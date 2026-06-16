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

//! Shared helpers for integration tests of the `message_bus` crate.
//!
//! Each integration test binary in `tests/` includes this module via
//! `mod common;` and uses the helpers to keep the tests compact.
//!
//! Note: integration tests live in separate binaries, so each binary that
//! does not consume every helper triggers `dead_code`. The crate-level
//! `#![allow(dead_code)]` below is intentional.

#![allow(dead_code)] // each test binary uses a subset

use iggy_binary_protocol::{Command2, GenericHeader, HEADER_SIZE};
use message_bus::ConnectionInstaller;
use message_bus::client_listener::RequestHandler;
use message_bus::replica::auth::ReplicaAuth;
use message_bus::replica::handshake::{ReplicaHandshakeCtx, ReplicaTlsCtx};
use message_bus::replica::listener::MessageHandler;
use message_bus::transports::tls::{
    AcceptAnyServerCert, REPLICA_ALPN, install_default_crypto_provider, self_signed_for_loopback,
};
use message_bus::{
    AcceptedClientFn, AcceptedQuicClientFn, AcceptedQuicConn, AcceptedReplicaFn,
    AcceptedTlsClientFn, AcceptedWsClientFn, AcceptedWssClientFn, ClientConnMeta,
    ClientTransportKind, DialedReplicaFn, IggyMessageBus, fd_transfer, installer,
};
use rustls::pki_types::ServerName;
use server_common::Message;
use std::cell::Cell;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;

/// Build a stub [`ClientConnMeta`] for tests that don't care about
/// peer addr / transport details. Uses `127.0.0.1:0` and the given
/// transport kind. Production install paths would surface the real
/// `SocketAddr` from the accept call site.
#[must_use]
pub fn test_client_meta(client_id: u128, transport: ClientTransportKind) -> ClientConnMeta {
    ClientConnMeta::new(client_id, "127.0.0.1:0".parse().unwrap(), transport)
}

/// Loopback address with OS-chosen port.
#[must_use]
pub fn loopback() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

/// Build a header-only consensus message with the given command.
///
/// Used by tests to fabricate `Request`, `Reply`, `Ping`, etc. directly.
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub fn header_only(command: Command2, cluster: u128, replica: u8) -> Message<GenericHeader> {
    Message::<GenericHeader>::new(HEADER_SIZE).transmute_header(|_, h: &mut GenericHeader| {
        h.command = command;
        h.cluster = cluster;
        h.replica = replica;
        h.size = HEADER_SIZE as u32;
    })
}

/// Install the replica handshake identity on a test bus. Production
/// bootstrap does this once per shard; tests call it before wiring the
/// listener / connector callbacks below.
pub fn set_replica_ctx(
    bus: &IggyMessageBus,
    cluster_id: u128,
    self_id: u8,
    replica_count: u8,
    auth: Option<ReplicaAuth>,
) {
    bus.set_replica_handshake_ctx(ReplicaHandshakeCtx {
        cluster_id,
        self_id,
        replica_count,
        auth: auth.map(Rc::new),
        tls: None,
    });
}

/// [`set_replica_ctx`] variant with a replica TLS context attached.
pub fn set_replica_ctx_with_tls(
    bus: &IggyMessageBus,
    cluster_id: u128,
    self_id: u8,
    replica_count: u8,
    auth: Option<ReplicaAuth>,
    tls: Rc<ReplicaTlsCtx>,
) {
    bus.set_replica_handshake_ctx(ReplicaHandshakeCtx {
        cluster_id,
        self_id,
        replica_count,
        auth: auth.map(Rc::new),
        tls: Some(tls),
    });
}

/// Build a self-signed-mode replica TLS context mirroring the production
/// `cluster.tls.self_signed` shape: TLS 1.3 only, `iggy-replica` ALPN,
/// throwaway per-node certificate, accept-any dialer verifier.
#[must_use]
pub fn self_signed_replica_tls_ctx(replica_count: u8) -> Rc<ReplicaTlsCtx> {
    install_default_crypto_provider();
    let creds = self_signed_for_loopback();
    let mut server =
        rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_no_client_auth()
            .with_single_cert(creds.cert_chain, creds.key_der)
            .expect("server config from self-signed credentials");
    server.alpn_protocols = vec![REPLICA_ALPN.to_vec()];
    let mut client =
        rustls::ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyServerCert))
            .with_no_client_auth();
    client.alpn_protocols = vec![REPLICA_ALPN.to_vec()];
    let peer_names = (0..replica_count)
        .map(|_| ServerName::try_from("localhost").expect("static server name"))
        .collect();
    Rc::new(ReplicaTlsCtx {
        server: Arc::new(server),
        client: Arc::new(client),
        peer_names,
    })
}

/// Build an [`AcceptedReplicaFn`] that runs the owning-shard inbound
/// path (acceptor handshake + install) locally on the given bus,
/// bypassing the coordinator but exercising the same admission (cap
/// slot) and handshake code the production owning shard runs. Requires
/// [`set_replica_ctx`] on the bus.
#[must_use]
pub fn install_replicas_locally(
    bus: Rc<IggyMessageBus>,
    on_message: MessageHandler,
) -> AcceptedReplicaFn {
    Rc::new(move |stream| {
        let Some(slot) = bus.try_acquire_replica_handshake_slot() else {
            return;
        };
        let release_bus = Rc::clone(&bus);
        installer::install_replica_inbound(
            &bus,
            stream,
            on_message.clone(),
            Box::new(move || {
                release_bus.release_replica_handshake_slot(slot);
            }),
        );
    })
}

/// Build a [`DialedReplicaFn`] that runs the owning-shard outbound path
/// (dialer handshake + install) locally on the given bus, including the
/// pending-dial mark / clear the production shard-0 callback and router
/// perform. Requires [`set_replica_ctx`] on the bus.
#[must_use]
pub fn install_dialed_replicas_locally(
    bus: Rc<IggyMessageBus>,
    on_message: MessageHandler,
) -> DialedReplicaFn {
    Rc::new(move |stream, peer_id| {
        bus.mark_dial_pending(peer_id);
        let clear_bus = Rc::clone(&bus);
        installer::install_replica_outbound(
            &bus,
            peer_id,
            stream,
            on_message.clone(),
            Box::new(move || {
                clear_bus.clear_dial_pending(peer_id);
            }),
        );
    })
}

/// Build an [`AcceptedClientFn`] that mints a local client id (top 16 bits =
/// `shard_id`, bottom 112 bits = per-call counter) and installs the client
/// stream directly on the given bus. Tests use this to bypass the shard-0
/// coordinator while keeping the same install plumbing the production path
/// exercises.
#[must_use]
pub fn install_clients_locally(
    bus: Rc<IggyMessageBus>,
    on_request: RequestHandler,
) -> AcceptedClientFn {
    let counter: Rc<Cell<u128>> = Rc::new(Cell::new(1));
    let shard_id = u128::from(bus.shard_id());
    Rc::new(move |stream| {
        let seq = counter.get();
        counter.set(seq.wrapping_add(1));
        let client_id = (shard_id << 112) | seq;
        let meta = test_client_meta(client_id, ClientTransportKind::Tcp);
        installer::install_client_tcp(&bus, meta, stream, on_request.clone());
    })
}

/// Build an [`AcceptedQuicClientFn`] that mints a local client id and
/// installs the QUIC connection directly on the given bus. Mirror of
/// [`install_clients_locally`] for the QUIC plane.
#[must_use]
pub fn install_quic_clients_locally(
    bus: Rc<IggyMessageBus>,
    on_request: RequestHandler,
) -> AcceptedQuicClientFn {
    let counter: Rc<Cell<u128>> = Rc::new(Cell::new(1));
    let shard_id = u128::from(bus.shard_id());
    Rc::new(move |conn: AcceptedQuicConn| {
        let seq = counter.get();
        counter.set(seq.wrapping_add(1));
        let client_id = (shard_id << 112) | seq;
        let meta = test_client_meta(client_id, ClientTransportKind::Quic);
        installer::install_client_quic(&bus, meta, conn, on_request.clone());
    })
}

/// Build an [`AcceptedWsClientFn`] that mints a local client id, dups
/// the accepted fd (mirroring the production cross-shard fd-ship
/// path), and hands it to [`ConnectionInstaller::install_client_ws_fd`]
/// on the given bus. Single-shard tests bypass the inter-shard
/// channel by dup'ing locally; the install path runs `accept_async`
/// and `install_client_ws` exactly as the production
/// owning-shard router would.
#[must_use]
pub fn install_ws_clients_locally(
    bus: Rc<IggyMessageBus>,
    on_request: RequestHandler,
) -> AcceptedWsClientFn {
    let counter: Rc<Cell<u128>> = Rc::new(Cell::new(1));
    let shard_id = u128::from(bus.shard_id());
    Rc::new(move |stream| {
        let seq = counter.get();
        counter.set(seq.wrapping_add(1));
        let client_id = (shard_id << 112) | seq;
        let fd = fd_transfer::dup_fd(&stream).expect("dup_fd");
        drop(stream);
        let meta = test_client_meta(client_id, ClientTransportKind::Ws);
        bus.install_client_ws_fd(fd, meta, on_request.clone());
    })
}

/// Build an [`AcceptedTlsClientFn`] that mints a local client id and
/// installs the accepted TCP-TLS stream directly on the given bus. The
/// install path drives the rustls handshake on its own task, mirroring
/// the production shard-0 coordinator.
#[must_use]
pub fn install_tls_clients_locally(
    bus: Rc<IggyMessageBus>,
    on_request: RequestHandler,
) -> AcceptedTlsClientFn {
    let counter: Rc<Cell<u128>> = Rc::new(Cell::new(1));
    let shard_id = u128::from(bus.shard_id());
    Rc::new(move |stream, config| {
        let seq = counter.get();
        counter.set(seq.wrapping_add(1));
        let client_id = (shard_id << 112) | seq;
        let meta = test_client_meta(client_id, ClientTransportKind::TcpTls);
        installer::install_client_tcp_tls(&bus, meta, stream, config, on_request.clone());
    })
}

/// Build an [`AcceptedWssClientFn`] that mints a local client id and
/// installs the accepted WSS stream directly on the given bus. The
/// install path drives both the rustls handshake and the WS HTTP-Upgrade
/// inside the transport's `run` body.
#[must_use]
pub fn install_wss_clients_locally(
    bus: Rc<IggyMessageBus>,
    on_request: RequestHandler,
) -> AcceptedWssClientFn {
    let counter: Rc<Cell<u128>> = Rc::new(Cell::new(1));
    let shard_id = u128::from(bus.shard_id());
    Rc::new(move |stream, config| {
        let seq = counter.get();
        counter.set(seq.wrapping_add(1));
        let client_id = (shard_id << 112) | seq;
        let meta = test_client_meta(client_id, ClientTransportKind::Wss);
        installer::install_client_wss(&bus, meta, stream, config, on_request.clone());
    })
}
