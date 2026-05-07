// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Shard-0 listener bootstrap.
//!
//! Every shard on a node instantiates its own `Rc<IggyMessageBus>` on its
//! own compio runtime, but only shard 0 terminates inbound traffic: it
//! binds the replica listener, the plain-TCP client listener, and
//! optionally the WS, QUIC, TCP-TLS, and WSS client listeners. It also
//! dials higher-id peers for the replica plane. Each accepted or
//! dialed connection is handed to the delegate callback supplied by
//! the caller (typically wrapping `shard::coordinator::ShardZeroCoordinator`).
//! TCP and pre-upgrade WS callbacks duplicate the fd and ship it to
//! the owning shard via the inter-shard `ShardFrame` channel; TCP-TLS,
//! WSS, and QUIC callbacks install locally on shard 0 (the connection
//! state is not serialisable so cross-shard handover is not possible).
//!
//! Non-zero shards early-return `Ok(None)`; the launcher calls this helper
//! unconditionally per shard, so non-zero shards just have no listener
//! binding and rely on `send_to_*` slow-path forwarding to reach the
//! owning shard.

use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use iggy_common::IggyError;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use crate::connector::start as start_connector;
use crate::replica::listener::{bind as bind_replica_listener, run as run_replica_listener};
use crate::transports::quic::server_config_with_cert;
use crate::transports::tls::TlsServerCredentials;
use crate::{
    AcceptedClientFn, AcceptedQuicClientFn, AcceptedReplicaFn, AcceptedTlsClientFn,
    AcceptedWsClientFn, AcceptedWssClientFn, IggyMessageBus, client_listener,
};

/// QUIC server credentials passed by the bootstrap layer.
///
/// The cert chain is the leaf-first sequence rustls expects; the key
/// is the server's private key in DER form. Tests use rcgen to mint a
/// throwaway pair; production callers load real PKI material via
/// `core/server-ng`'s `[quic.certificate]` config section.
pub struct QuicServerCredentials {
    pub cert_chain: Vec<CertificateDer<'static>>,
    pub key_der: PrivateKeyDer<'static>,
}

/// Bound addresses returned to shard 0 after the listeners come up.
///
/// `ws`, `quic`, `tcp_tls`, and `wss` are populated only when the
/// corresponding `start_on_shard_zero` parameters are `Some`; an
/// unconfigured plane stays `None`.
#[derive(Debug, Clone)]
pub struct BoundPlanes {
    pub replica: SocketAddr,
    pub client: SocketAddr,
    pub ws: Option<SocketAddr>,
    pub quic: Option<SocketAddr>,
    pub tcp_tls: Option<SocketAddr>,
    pub wss: Option<SocketAddr>,
}

/// Boot-time check: every TCP-family listener address must occupy a
/// distinct `(ip, port)` slot.
///
/// TCP-family listeners (`replica`, `client`, `ws`, `tcp_tls`, `wss`)
/// share the TCP port space, so any pair sharing `(ip, port)` is a
/// bind-time conflict. The QUIC listener binds a UDP socket: it
/// occupies a separate port namespace and is allowed to share a port
/// with any TCP-family listener (a common operator choice for "QUIC
/// on 443 over UDP, HTTPS on 443 over TCP"). UDP-vs-UDP conflicts are
/// not possible today because QUIC is the only UDP listener.
///
/// # Panics
///
/// Panics with a message naming the conflicting pair. Boot-time
/// validation; surfaces operator misconfiguration loudly rather than
/// letting one listener silently lose a `EADDRINUSE` race against
/// another.
pub fn assert_listen_addrs_distinct(
    replica: SocketAddr,
    client: SocketAddr,
    ws: Option<SocketAddr>,
    _quic: Option<SocketAddr>,
    tcp_tls: Option<SocketAddr>,
    wss: Option<SocketAddr>,
) {
    let tcp_slots: [(&str, Option<SocketAddr>); 5] = [
        ("replica", Some(replica)),
        ("client", Some(client)),
        ("ws", ws),
        ("tcp_tls", tcp_tls),
        ("wss", wss),
    ];
    for i in 0..tcp_slots.len() {
        for j in (i + 1)..tcp_slots.len() {
            let (a_name, a) = tcp_slots[i];
            let (b_name, b) = tcp_slots[j];
            // Port 0 means "OS-assigned"; two slots at port 0 receive
            // distinct kernel ports at bind time, so they don't
            // conflict. Tests rely on this for loopback fixtures.
            if let (Some(a), Some(b)) = (a, b)
                && a == b
                && a.port() != 0
            {
                panic!("listener address conflict: {a_name} and {b_name} both bind to {a}");
            }
        }
    }
}

/// Bind the replica + client listeners on shard 0 and start the
/// outbound replica connector.
///
/// Optionally bind WS, QUIC, TCP-TLS, and WSS client listeners
/// alongside. Non-zero shards early-return `Ok(None)`.
///
/// Each accepted / dialed connection is handed to the supplied
/// delegate callback. The TCP callbacks (`on_accepted_replica`,
/// `on_accepted_client`) and the WS callback (`on_accepted_ws_client`)
/// are responsible for the dup-fd + inter-shard send. The QUIC,
/// TCP-TLS, and WSS callbacks
/// (`on_accepted_quic_client` / `on_accepted_tcp_tls_client` /
/// `on_accepted_wss_client`) install locally on shard 0; those planes
/// have no cross-shard handover (QUIC by transport design, TLS-family
/// because the rustls connection state is not serialisable).
///
/// `ws_listen_addr` / `on_accepted_ws_client` are paired: if either is
/// `Some`, both must be. Same for the QUIC trio
/// (`quic_listen_addr` / `quic_credentials` / `on_accepted_quic_client`)
/// and for the TCP-TLS / WSS trios
/// (`*_listen_addr` / `*_credentials` / `on_accepted_*_client`).
///
/// # Panics
///
/// Panics on TCP-family `(ip, port)` overlap among the populated
/// `replica`, `client`, `ws`, `tcp_tls`, `wss` slots — see
/// [`assert_listen_addrs_distinct`].
///
/// # Errors
///
/// Returns `IggyError::CannotBindToSocket` if any listener bind fails;
/// `IggyError::InvalidConfiguration` if any of the
/// listen-addr / credentials / callback trios is partially populated;
/// `IggyError::IoError` if a TLS-family server config cannot be built
/// from the supplied credentials.
#[allow(
    clippy::future_not_send,
    clippy::too_many_arguments,
    clippy::too_many_lines,
    clippy::similar_names
)]
pub async fn start_on_shard_zero(
    bus: &Rc<IggyMessageBus>,
    replica_listen_addr: SocketAddr,
    client_listen_addr: SocketAddr,
    ws_listen_addr: Option<SocketAddr>,
    quic_listen_addr: Option<SocketAddr>,
    quic_credentials: Option<QuicServerCredentials>,
    tcp_tls_listen_addr: Option<SocketAddr>,
    tcp_tls_credentials: Option<TlsServerCredentials>,
    wss_listen_addr: Option<SocketAddr>,
    wss_credentials: Option<TlsServerCredentials>,
    cluster_id: u128,
    self_id: u8,
    replica_count: u8,
    peers: Vec<(u8, SocketAddr)>,
    on_accepted_replica: AcceptedReplicaFn,
    on_accepted_client: AcceptedClientFn,
    on_accepted_ws_client: Option<AcceptedWsClientFn>,
    on_accepted_quic_client: Option<AcceptedQuicClientFn>,
    on_accepted_tcp_tls_client: Option<AcceptedTlsClientFn>,
    on_accepted_wss_client: Option<AcceptedWssClientFn>,
    reconnect_period: Duration,
) -> Result<Option<BoundPlanes>, IggyError> {
    if bus.shard_id() != 0 {
        return Ok(None);
    }

    assert_listen_addrs_distinct(
        replica_listen_addr,
        client_listen_addr,
        ws_listen_addr,
        quic_listen_addr,
        tcp_tls_listen_addr,
        wss_listen_addr,
    );

    let (replica_listener, replica_bound) = bind_replica_listener(replica_listen_addr).await?;
    let (clients_listener, client_bound) = client_listener::tcp::bind(client_listen_addr).await?;

    let token_for_replica = bus.token();
    let on_accepted_replica_for_listener = on_accepted_replica.clone();
    let listener_max_message_size = bus.config().max_message_size;
    let listener_handshake_grace = bus.config().handshake_grace;
    let replica_handle = compio::runtime::spawn(async move {
        run_replica_listener(
            replica_listener,
            token_for_replica,
            cluster_id,
            self_id,
            replica_count,
            on_accepted_replica_for_listener,
            listener_max_message_size,
            listener_handshake_grace,
        )
        .await;
    });
    bus.track_background(replica_handle);

    let token_for_client = bus.token();
    let client_handle = compio::runtime::spawn(async move {
        client_listener::tcp::run(clients_listener, token_for_client, on_accepted_client).await;
    });
    bus.track_background(client_handle);

    let ws_bound = match (ws_listen_addr, on_accepted_ws_client) {
        (Some(addr), Some(on_accepted_ws)) => {
            let (ws_listener, ws_bound) = client_listener::ws::bind(addr).await?;
            let token_for_ws = bus.token();
            let ws_handle = compio::runtime::spawn(async move {
                client_listener::ws::run(ws_listener, token_for_ws, on_accepted_ws).await;
            });
            bus.track_background(ws_handle);
            Some(ws_bound)
        }
        (None, None) => None,
        _ => {
            return Err(IggyError::InvalidConfiguration);
        }
    };

    let quic_bound = match (quic_listen_addr, quic_credentials, on_accepted_quic_client) {
        (Some(addr), Some(creds), Some(on_accepted_quic)) => {
            let server_config =
                server_config_with_cert(creds.cert_chain, creds.key_der, &bus.config().quic)
                    .map_err(|e| {
                        IggyError::IoError(format!("QUIC server config build failed: {e}"))
                    })?;
            let (endpoint, quic_bound) = client_listener::quic::bind(addr, server_config).await?;
            let token_for_quic = bus.token();
            let handshake_grace = bus.config().handshake_grace;
            let quic_handle = compio::runtime::spawn(async move {
                client_listener::quic::run(
                    endpoint,
                    token_for_quic,
                    on_accepted_quic,
                    handshake_grace,
                )
                .await;
            });
            bus.track_background(quic_handle);
            Some(quic_bound)
        }
        (None, None, None) => None,
        _ => {
            return Err(IggyError::InvalidConfiguration);
        }
    };

    let tcp_tls_bound = match (
        tcp_tls_listen_addr,
        tcp_tls_credentials,
        on_accepted_tcp_tls_client,
    ) {
        (Some(addr), Some(creds), Some(on_accepted_tls)) => {
            let (listener, server_config, tls_bound) =
                client_listener::tcp_tls::bind(addr, creds).await?;
            let token_for_tls = bus.token();
            let tls_handle = compio::runtime::spawn(async move {
                client_listener::tcp_tls::run(
                    listener,
                    server_config,
                    token_for_tls,
                    on_accepted_tls,
                )
                .await;
            });
            bus.track_background(tls_handle);
            Some(tls_bound)
        }
        (None, None, None) => None,
        _ => {
            return Err(IggyError::InvalidConfiguration);
        }
    };

    let wss_bound = match (wss_listen_addr, wss_credentials, on_accepted_wss_client) {
        (Some(addr), Some(creds), Some(on_accepted_wss)) => {
            let (listener, server_config, wss_bound) =
                client_listener::wss::bind(addr, creds).await?;
            let token_for_wss = bus.token();
            let wss_handle = compio::runtime::spawn(async move {
                client_listener::wss::run(listener, server_config, token_for_wss, on_accepted_wss)
                    .await;
            });
            bus.track_background(wss_handle);
            Some(wss_bound)
        }
        (None, None, None) => None,
        _ => {
            return Err(IggyError::InvalidConfiguration);
        }
    };

    start_connector(
        bus,
        cluster_id,
        self_id,
        peers,
        on_accepted_replica,
        reconnect_period,
    )
    .await;

    Ok(Some(BoundPlanes {
        replica: replica_bound,
        client: client_bound,
        ws: ws_bound,
        quic: quic_bound,
        tcp_tls: tcp_tls_bound,
        wss: wss_bound,
    }))
}

/// [`start_on_shard_zero`] defaulting `reconnect_period` to the bus's
/// [`crate::MessageBusConfig::reconnect_period`].
///
/// Leaves the WS, QUIC, TCP-TLS, and WSS listener slots unconfigured
/// (`None`). Convenience entry for TCP-only deployments and existing
/// tests; prefer the full [`start_on_shard_zero`] in production where
/// the optional planes come from `core/server-ng`'s config.
///
/// # Errors
///
/// Returns `IggyError::CannotBindToSocket` if either listener bind fails.
#[allow(clippy::future_not_send)]
#[allow(clippy::too_many_arguments)]
pub async fn start_on_shard_zero_default(
    bus: &Rc<IggyMessageBus>,
    replica_listen_addr: SocketAddr,
    client_listen_addr: SocketAddr,
    cluster_id: u128,
    self_id: u8,
    replica_count: u8,
    peers: Vec<(u8, SocketAddr)>,
    on_accepted_replica: AcceptedReplicaFn,
    on_accepted_client: AcceptedClientFn,
) -> Result<Option<BoundPlanes>, IggyError> {
    let reconnect_period = bus.config().reconnect_period;
    start_on_shard_zero(
        bus,
        replica_listen_addr,
        client_listen_addr,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        cluster_id,
        self_id,
        replica_count,
        peers,
        on_accepted_replica,
        on_accepted_client,
        None,
        None,
        None,
        None,
        reconnect_period,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    #[test]
    fn distinct_addrs_pass() {
        assert_listen_addrs_distinct(
            addr(9090),
            addr(8090),
            Some(addr(8092)),
            Some(addr(8080)),
            Some(addr(8093)),
            Some(addr(8094)),
        );
    }

    #[test]
    fn ws_unset_passes() {
        assert_listen_addrs_distinct(addr(9090), addr(8090), None, Some(addr(8080)), None, None);
    }

    #[test]
    fn quic_shares_port_with_tcp_replica_ok() {
        // QUIC on UDP and replica on TCP can share a port (separate
        // kernel namespaces). Operator choice "443 on TCP and UDP".
        assert_listen_addrs_distinct(
            addr(443),
            addr(8090),
            Some(addr(8092)),
            Some(addr(443)),
            None,
            None,
        );
    }

    #[test]
    fn quic_shares_port_with_tcp_client_ok() {
        assert_listen_addrs_distinct(
            addr(9090),
            addr(443),
            Some(addr(8092)),
            Some(addr(443)),
            None,
            None,
        );
    }

    #[test]
    fn quic_shares_port_with_tcp_tls_ok() {
        // QUIC's UDP namespace is also disjoint from the TLS-family
        // TCP listeners; "443 over UDP for QUIC + 443 over TCP for
        // TCP-TLS / WSS" is a valid co-location.
        assert_listen_addrs_distinct(
            addr(9090),
            addr(8090),
            None,
            Some(addr(443)),
            Some(addr(443)),
            None,
        );
    }

    #[test]
    #[should_panic(expected = "listener address conflict: replica and client")]
    fn replica_client_overlap_panics() {
        assert_listen_addrs_distinct(addr(8090), addr(8090), Some(addr(8092)), None, None, None);
    }

    #[test]
    #[should_panic(expected = "listener address conflict: replica and ws")]
    fn replica_ws_overlap_panics() {
        assert_listen_addrs_distinct(addr(9090), addr(8090), Some(addr(9090)), None, None, None);
    }

    #[test]
    #[should_panic(expected = "listener address conflict: client and ws")]
    fn client_ws_overlap_panics() {
        assert_listen_addrs_distinct(addr(9090), addr(8090), Some(addr(8090)), None, None, None);
    }

    #[test]
    #[should_panic(expected = "listener address conflict: client and tcp_tls")]
    fn client_tcp_tls_overlap_panics() {
        assert_listen_addrs_distinct(addr(9090), addr(8090), None, None, Some(addr(8090)), None);
    }

    #[test]
    #[should_panic(expected = "listener address conflict: ws and wss")]
    fn ws_wss_overlap_panics() {
        assert_listen_addrs_distinct(
            addr(9090),
            addr(8090),
            Some(addr(8092)),
            None,
            None,
            Some(addr(8092)),
        );
    }

    #[test]
    #[should_panic(expected = "listener address conflict: tcp_tls and wss")]
    fn tcp_tls_wss_overlap_panics() {
        assert_listen_addrs_distinct(
            addr(9090),
            addr(8090),
            None,
            None,
            Some(addr(8093)),
            Some(addr(8093)),
        );
    }

    #[test]
    #[should_panic(expected = "listener address conflict: replica and tcp_tls")]
    fn replica_tcp_tls_overlap_panics() {
        assert_listen_addrs_distinct(addr(9090), addr(8090), None, None, Some(addr(9090)), None);
    }
}
