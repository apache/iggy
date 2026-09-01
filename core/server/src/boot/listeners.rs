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

//! Shard 0 listener start-up and the accept-fn factories.

use crate::boot::credentials::{
    load_quic_server_credentials, load_tcp_tls_server_credentials, load_wss_server_credentials,
};
use crate::boot::topology::{
    TcpTopology, client_listeners, derived_address_misses_listener, derived_bind_ip,
    wildcard_listener_under_loopback_address,
};
use crate::cluster_meta::{ClusterRoster, self_advertised_address};
use crate::config_writer::{BoundAddresses, write_current_config};
use crate::http;
use crate::server_error::ServerError;
use crate::shell::ServerShard;
use configs::cluster::TransportPorts;
use configs::server::ServerConfig;
use message_bus::client_listener::{self, RequestHandler};
use message_bus::installer::conn_info::{ClientConnMeta, ClientTransportKind};
use message_bus::replica::io as replica_io;
use message_bus::replica::listener::{self as replica_listener};
use message_bus::transports::quic::server_config_with_cert;
use message_bus::{
    AcceptedClientFn, AcceptedQuicClientFn, AcceptedReplicaFn, AcceptedTlsClientFn,
    AcceptedWsClientFn, AcceptedWssClientFn, DialedReplicaFn, IggyMessageBus,
    MAX_INFLIGHT_REPLICA_HANDSHAKES, connector, installer,
};
use shard::metrics::ShardMetrics;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use tracing::{error, info, warn};

pub(in crate::boot) struct LocalClientAcceptFns {
    tcp: AcceptedClientFn,
    ws: AcceptedWsClientFn,
    quic: AcceptedQuicClientFn,
    tcp_tls: AcceptedTlsClientFn,
    wss: AcceptedWssClientFn,
}

#[allow(clippy::too_many_arguments)]
pub(in crate::boot) async fn start_tcp_runtime(
    shard: &Rc<ServerShard>,
    config: &ServerConfig,
    topology: &TcpTopology,
    roster: Rc<ClusterRoster>,
    accepted_replica: AcceptedReplicaFn,
    dialed_replica: DialedReplicaFn,
    accepted_clients: LocalClientAcceptFns,
    shard_metrics_all: &[ShardMetrics],
) -> Result<(), ServerError> {
    // HTTP is served over TCP but sits outside the replica_io / manual client
    // reactor, so it binds on its own: first, so its port is known when the
    // roster and `current_config.toml` are written, and served last, so no
    // request is answered before they are. Shard-0 gating comes from the sole
    // caller of this function.
    let http = topology
        .http_listen_addr
        .map(|addr| http::bind(addr, &config.http, &config.cluster))
        .transpose()?;

    let mut bound = if config.tcp.enabled && !config.tcp.tls.enabled {
        start_via_replica_io(
            shard,
            config,
            topology,
            accepted_replica,
            dialed_replica,
            accepted_clients,
        )
        .await?
    } else {
        start_manual_runtime(
            shard,
            config,
            topology,
            accepted_replica,
            dialed_replica,
            accepted_clients,
        )
        .await?
    };

    // Cluster metadata carries one host for all four transports, so a listener
    // the derived host does not reach is unreachable at it. Only a derived
    // address is judged, and never against the listener it was derived from.
    let declared = config.node.advertised_address.as_deref();
    let self_advertised = self_advertised_address(declared, derived_bind_ip(topology, config));
    // A roster entry answers this per node in cluster mode, so the derived
    // address is never served and none of these listeners are judged against it.
    let listeners = client_listeners(topology, config);
    if !config.cluster.enabled
        && let Some(derived_from) = listeners
            .iter()
            .find_map(|(key, listen_addr)| listen_addr.map(|_| *key))
    {
        for (key, listen_addr) in listeners {
            let Some(listen_addr) = listen_addr.filter(|_| key != derived_from) else {
                continue;
            };
            if derived_address_misses_listener(declared, &self_advertised, listen_addr) {
                warn!(
                    "{key} binds {listen_addr} but cluster metadata publishes {self_advertised}, \
                     derived from {derived_from}; a client reading that metadata would not reach \
                     this listener. Set node.advertised_address to the address clients dial."
                );
            } else if wildcard_listener_under_loopback_address(
                declared,
                &self_advertised,
                listen_addr,
            ) {
                warn!(
                    "{key} binds the wildcard {listen_addr} but cluster metadata publishes the \
                     loopback {self_advertised}, derived from {derived_from}; a client reaching \
                     this listener from another host is told an address that points back at \
                     itself. Set node.advertised_address to the address clients dial."
                );
            }
        }
    }

    bound.http = http.as_ref().map(|http| http.bound_addr);
    roster.bound_ports.publish(TransportPorts::from(&bound));
    write_current_config(config, Some(topology.self_replica_id), &bound).await?;

    if let Some(http) = http {
        http::start(
            http,
            shard,
            &config.http,
            config.metadata.clients_table_max,
            config.personal_access_token.max_tokens_per_user,
            Arc::clone(&config.system),
            roster,
            shard_metrics_all,
        )?;
    }

    Ok(())
}

// ws/wss bindings intentionally mirror the transport names (same convention as
// `replica_io::start_on_shard_zero`).
#[allow(clippy::similar_names)]
async fn start_via_replica_io(
    shard: &Rc<ServerShard>,
    config: &ServerConfig,
    topology: &TcpTopology,
    accepted_replica: AcceptedReplicaFn,
    dialed_replica: DialedReplicaFn,
    accepted_clients: LocalClientAcceptFns,
) -> Result<BoundAddresses, ServerError> {
    let replica_addr = topology
        .replica_listen_addr
        .expect("topology must include replica listener address");
    let quic_credentials = topology
        .quic_listen_addr
        .is_some()
        .then(|| load_quic_server_credentials(config))
        .transpose()?;
    let tcp_tls_credentials = topology
        .tcp_tls_listen_addr
        .is_some()
        .then(|| load_tcp_tls_server_credentials(config))
        .transpose()?;
    // `websocket.tls.enabled` upgrades the websocket address to a WSS
    // listener; the plain-WS listener must NOT also bind it (one port, one
    // handshake kind -- a plain upgrade parser fed a TLS ClientHello rejects
    // every connection with an httparse error).
    let wss_enabled = config.websocket.tls.enabled;
    let ws_listen_addr = (!wss_enabled).then_some(topology.ws_listen_addr).flatten();
    let wss_listen_addr = wss_enabled.then_some(topology.ws_listen_addr).flatten();
    let wss_credentials = wss_listen_addr
        .is_some()
        .then(|| load_wss_server_credentials(config))
        .transpose()?;

    let LocalClientAcceptFns {
        tcp,
        ws,
        quic,
        tcp_tls,
        wss,
    } = accepted_clients;

    let bound = replica_io::start_on_shard_zero(
        &shard.bus,
        replica_addr,
        topology.client_listen_addr,
        ws_listen_addr,
        topology.quic_listen_addr,
        quic_credentials,
        topology.tcp_tls_listen_addr,
        tcp_tls_credentials,
        wss_listen_addr,
        wss_credentials,
        topology.self_replica_id,
        topology.peers.clone(),
        accepted_replica,
        dialed_replica,
        tcp,
        ws_listen_addr.map(|_| ws),
        topology.quic_listen_addr.map(|_| quic),
        topology.tcp_tls_listen_addr.map(|_| tcp_tls),
        wss_listen_addr.map(|_| wss),
        shard.bus.config().reconnect_period,
    )
    .await
    .map_err(|source| {
        error!(
            replica_addr = %replica_addr,
            client_addr = %topology.client_listen_addr,
            error = %source,
            "failed to start server listeners via replica_io"
        );
        source
    })?;
    // `start_on_shard_zero` answers `None` only on a non-zero shard, and the
    // sole caller of this function is shard 0's listener block.
    let bound = bound.ok_or(ServerError::ListenersOffShardZero { shard_id: shard.id })?;

    if config.cluster.enabled {
        info!(
            shard = shard.id,
            replica = %bound.replica,
            tcp = %bound.client,
            tcp_tls = ?bound.tcp_tls,
            ws = ?bound.ws,
            quic = ?bound.quic,
            "server listeners started"
        );
    } else {
        info!(
            shard = shard.id,
            tcp = %bound.client,
            tcp_tls = ?bound.tcp_tls,
            ws = ?bound.ws,
            quic = ?bound.quic,
            "server client listeners started"
        );
    }

    Ok(BoundAddresses {
        tcp: Some(bound.client),
        tcp_tls: bound.tcp_tls,
        quic: bound.quic,
        // The WSS listener occupies the configured websocket address slot.
        websocket: bound.wss.or(bound.ws),
        http: None,
        replica: config.cluster.enabled.then_some(bound.replica),
    })
}

async fn start_manual_runtime(
    shard: &Rc<ServerShard>,
    config: &ServerConfig,
    topology: &TcpTopology,
    accepted_replica: AcceptedReplicaFn,
    dialed_replica: DialedReplicaFn,
    accepted_clients: LocalClientAcceptFns,
) -> Result<BoundAddresses, ServerError> {
    let bound_replica = if config.cluster.enabled {
        let replica_addr = topology
            .replica_listen_addr
            .expect("cluster-enabled topology must include replica listener address");
        let (replica_listener, bound_addr) =
            replica_listener::bind(replica_addr)
                .await
                .map_err(|source| {
                    error!(
                        replica_addr = %replica_addr,
                        error = %source,
                        "failed to bind replica listener"
                    );
                    source
                })?;
        let token = shard.bus.token();
        let replica_handle = compio::runtime::spawn(async move {
            replica_listener::run(replica_listener, token, accepted_replica).await;
        });
        shard.bus.track_background(replica_handle);
        connector::start(
            &shard.bus,
            topology.self_replica_id,
            topology.peers.clone(),
            dialed_replica,
            shard.bus.config().reconnect_period,
        )
        .await;
        Some(bound_addr)
    } else {
        None
    };

    let mut bound = start_client_listeners(shard, config, topology, &accepted_clients)?;
    bound.replica = bound_replica;

    if config.cluster.enabled {
        info!(
            shard = shard.id,
            replica = ?bound.replica,
            tcp = ?bound.tcp,
            tcp_tls = ?bound.tcp_tls,
            ws = ?bound.websocket,
            quic = ?bound.quic,
            "server listeners started"
        );
    } else {
        info!(
            shard = shard.id,
            tcp = ?bound.tcp,
            tcp_tls = ?bound.tcp_tls,
            ws = ?bound.websocket,
            quic = ?bound.quic,
            "server client listeners started"
        );
    }

    Ok(bound)
}

/// Replica delegation callbacks for shard 0's listener and connector.
///
/// Inbound: acquire a slot in the shard-0-global in-flight handshake cap
/// (drop the connection when full), then blind-delegate the raw fd
/// through the coordinator's round-robin. The fd lands on the target
/// shard's inbox as a [`shard::LifecycleFrame::ReplicaInboundSetup`]
/// frame; the owning shard runs the acceptor handshake and acks the
/// slot back. A failed delegation releases the slot immediately.
///
/// Outbound: delegate the dialed fd as
/// [`shard::LifecycleFrame::ReplicaOutboundSetup`] and mark the peer
/// dial-pending so the reconnect sweep skips it until the owning
/// shard's handshake outcome arrives (or the entry expires).
pub(in crate::boot) fn make_replica_delegation_fns(
    coord: Rc<shard::coordinator::ShardZeroCoordinator>,
    bus: &Rc<IggyMessageBus>,
) -> (AcceptedReplicaFn, DialedReplicaFn) {
    let inbound_bus = Rc::clone(bus);
    let inbound_coord = Rc::clone(&coord);
    let accepted: AcceptedReplicaFn = Rc::new(move |stream| {
        let Some(slot) = inbound_bus.try_acquire_replica_handshake_slot() else {
            warn!(
                cap = MAX_INFLIGHT_REPLICA_HANDSHAKES,
                "replica handshake in-flight cap reached; dropping inbound"
            );
            return;
        };
        match inbound_coord.delegate_replica_inbound(stream, slot) {
            Ok(target) => {
                info!(slot, target, "inbound replica connection delegated");
            }
            Err(error) => {
                inbound_bus.release_replica_handshake_slot(slot);
                warn!(
                    error = ?error,
                    "delegate_replica_inbound failed; dropping inbound replica connection"
                );
            }
        }
    });

    let outbound_bus = Rc::clone(bus);
    let dialed: DialedReplicaFn =
        Rc::new(
            move |stream, peer_id| match coord.delegate_replica_outbound(stream, peer_id) {
                Ok(target) => {
                    outbound_bus.mark_dial_pending(peer_id);
                    info!(peer_id, target, "outbound replica connection delegated");
                }
                Err(error) => {
                    warn!(
                        peer_id,
                        error = ?error,
                        "delegate_replica_outbound failed; dropping dialed replica connection"
                    );
                }
            },
        );

    (accepted, dialed)
}

/// Shard-0 client accept callbacks. TCP and WS clients are delegated via
/// the coordinator (round-robin to peer shards); QUIC and TCP-TLS install
/// locally on shard 0 because their per-connection state is not portable
/// across shards (`compio_quic` endpoint binds one UDP socket; rustls TLS
/// state ties to the post-handshake reactor).
// ws/wss bindings intentionally mirror the transport names (same convention as
// `replica_io::start_on_shard_zero`).
#[allow(clippy::similar_names)]
pub(in crate::boot) fn make_shard_zero_client_accept_fns(
    coord: Rc<shard::coordinator::ShardZeroCoordinator>,
    bus: &Rc<IggyMessageBus>,
    on_request: RequestHandler,
) -> LocalClientAcceptFns {
    let quic_bus = Rc::clone(bus);
    let tcp_tls_bus = Rc::clone(bus);
    let wss_bus = Rc::clone(bus);
    let quic_request = on_request.clone();
    let wss_request = on_request.clone();
    let tcp_tls_request = on_request;

    let tcp_coord = Rc::clone(&coord);
    let tcp = Rc::new(move |stream| match tcp_coord.delegate_client(stream) {
        Ok(client_id) => info!(client_id, "TCP client delegated"),
        Err(error) => warn!(error = ?error, "delegate_client failed; dropping TCP client"),
    });

    let ws_coord = Rc::clone(&coord);
    let ws = Rc::new(move |stream| match ws_coord.delegate_ws_client(stream) {
        Ok(client_id) => info!(client_id, "WS client delegated"),
        Err(error) => warn!(error = ?error, "delegate_ws_client failed; dropping WS client"),
    });

    // QUIC and TCP-TLS terminate locally on shard 0 but mint their client
    // ids through the coordinator's `client_seq`, the same counter the
    // delegated TCP/WS path uses. A separate counter here would let a
    // shard-0-local id collide with a delegated id that round-robined to
    // shard 0 (both encode target shard 0) in shard 0's connection
    // registry.
    let quic_coord = Rc::clone(&coord);
    let quic = Rc::new(move |accepted: message_bus::AcceptedQuicConn| {
        let meta = mint_client_meta(&quic_coord, accepted.peer_addr(), ClientTransportKind::Quic);
        installer::install_client_quic(&quic_bus, meta, accepted, quic_request.clone());
    });

    let tcp_tls_coord = Rc::clone(&coord);
    let tcp_tls = Rc::new(move |stream, tls_config| {
        let Some(meta) =
            client_meta_from_stream(&stream, &tcp_tls_coord, ClientTransportKind::TcpTls)
        else {
            return;
        };
        installer::install_client_tcp_tls(
            &tcp_tls_bus,
            meta,
            stream,
            tls_config,
            tcp_tls_request.clone(),
        );
    });

    // WSS terminates locally on shard 0 like TCP-TLS (rustls state is not
    // serialisable across the delegate path), minting ids through the same
    // coordinator counter.
    let wss_coord = coord;
    let wss = Rc::new(move |stream, tls_config| {
        let Some(meta) = client_meta_from_stream(&stream, &wss_coord, ClientTransportKind::Wss)
        else {
            return;
        };
        installer::install_client_wss(&wss_bus, meta, stream, tls_config, wss_request.clone());
    });

    LocalClientAcceptFns {
        tcp,
        ws,
        quic,
        tcp_tls,
        wss,
    }
}

fn client_meta_from_stream(
    stream: &compio::net::TcpStream,
    coord: &shard::coordinator::ShardZeroCoordinator,
    transport: ClientTransportKind,
) -> Option<ClientConnMeta> {
    let peer_addr = match stream.peer_addr() {
        Ok(peer_addr) => peer_addr,
        Err(error) => {
            warn!(error = %error, "dropping accepted client with unknown peer address");
            return None;
        }
    };
    Some(mint_client_meta(coord, peer_addr, transport))
}

fn mint_client_meta(
    coord: &shard::coordinator::ShardZeroCoordinator,
    peer_addr: SocketAddr,
    transport: ClientTransportKind,
) -> ClientConnMeta {
    ClientConnMeta::new(coord.mint_shard_zero_client_id(), peer_addr, transport)
}

fn start_client_listeners(
    shard: &Rc<ServerShard>,
    config: &ServerConfig,
    topology: &TcpTopology,
    accepted_clients: &LocalClientAcceptFns,
) -> Result<BoundAddresses, ServerError> {
    let mut bound = BoundAddresses::default();

    if config.tcp.enabled && !config.tcp.tls.enabled {
        let (listener, bound_addr) = client_listener::tcp::bind(topology.client_listen_addr)
            .map_err(|source| {
                error!(
                    addr = %topology.client_listen_addr,
                    error = %source,
                    "failed to bind TCP client listener"
                );
                source
            })?;
        let token = shard.bus.token();
        let accepted_client = accepted_clients.tcp.clone();
        let client_handle = compio::runtime::spawn(async move {
            client_listener::tcp::run(listener, token, accepted_client).await;
        });
        shard.bus.track_background(client_handle);
        bound.tcp = Some(bound_addr);
    }

    if let Some(ws_addr) = topology.ws_listen_addr {
        bound.websocket = Some(start_websocket_listener(
            shard,
            config,
            ws_addr,
            accepted_clients,
        )?);
    }

    if let Some(quic_addr) = topology.quic_listen_addr {
        let credentials = load_quic_server_credentials(config)?;
        let server_config = server_config_with_cert(
            credentials.cert_chain,
            credentials.key_der,
            &shard.bus.config().quic,
        )
        .map_err(|e| {
            let source =
                iggy_common::IggyError::IoError(format!("QUIC server config build failed: {e}"));
            error!(addr = %quic_addr, error = %source, "failed to build QUIC server config");
            source
        })?;
        let (endpoint, bound_addr) = client_listener::quic::bind(quic_addr, server_config)
            .map_err(|source| {
                error!(addr = %quic_addr, error = %source, "failed to bind QUIC listener");
                source
            })?;
        let token = shard.bus.token();
        let handshake_grace = shard.bus.config().handshake_grace;
        let accepted_quic = accepted_clients.quic.clone();
        let quic_handle = compio::runtime::spawn(async move {
            client_listener::quic::run(endpoint, token, accepted_quic, handshake_grace).await;
        });
        shard.bus.track_background(quic_handle);
        bound.quic = Some(bound_addr);
    }

    if config.tcp.enabled && config.tcp.tls.enabled {
        let credentials = load_tcp_tls_server_credentials(config)?;
        let (listener, tls_config, bound_addr) =
            client_listener::tcp_tls::bind(topology.client_listen_addr, credentials).map_err(
                |source| {
                    error!(
                        addr = %topology.client_listen_addr,
                        error = %source,
                        "failed to bind TCP TLS listener"
                    );
                    source
                },
            )?;
        let token = shard.bus.token();
        let accepted_tls = accepted_clients.tcp_tls.clone();
        let tls_handle = compio::runtime::spawn(async move {
            client_listener::tcp_tls::run(listener, tls_config, token, accepted_tls).await;
        });
        shard.bus.track_background(tls_handle);
        bound.tcp_tls = Some(bound_addr);
    }

    Ok(bound)
}

/// Bind the websocket client listener on `ws_addr`: WSS when
/// `websocket.tls.enabled` (the plain-WS accept loop must not also bind the
/// port -- a plain upgrade parser fed a TLS `ClientHello` rejects every
/// connection with an httparse error), plain WS otherwise.
fn start_websocket_listener(
    shard: &Rc<ServerShard>,
    config: &ServerConfig,
    ws_addr: SocketAddr,
    accepted_clients: &LocalClientAcceptFns,
) -> Result<SocketAddr, ServerError> {
    if config.websocket.tls.enabled {
        let credentials = load_wss_server_credentials(config)?;
        let (listener, tls_config, bound_addr) = client_listener::wss::bind(ws_addr, credentials)
            .map_err(|source| {
            error!(addr = %ws_addr, error = %source, "failed to bind WSS listener");
            source
        })?;
        let token = shard.bus.token();
        let accepted_wss = accepted_clients.wss.clone();
        let wss_handle = compio::runtime::spawn(async move {
            client_listener::wss::run(listener, tls_config, token, accepted_wss).await;
        });
        shard.bus.track_background(wss_handle);
        Ok(bound_addr)
    } else {
        let (listener, bound_addr) = client_listener::ws::bind(ws_addr).map_err(|source| {
            error!(addr = %ws_addr, error = %source, "failed to bind websocket listener");
            source
        })?;
        let token = shard.bus.token();
        let accepted_ws = accepted_clients.ws.clone();
        let ws_handle = compio::runtime::spawn(async move {
            client_listener::ws::run(listener, token, accepted_ws).await;
        });
        shard.bus.track_background(ws_handle);
        Ok(bound_addr)
    }
}
