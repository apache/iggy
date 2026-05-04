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
//
//! End-to-end: a real rustls client connects to the consensus TCP-TLS
//! client listener on shard 0; the listener's callback hands the raw
//! stream + shared `Arc<rustls::ServerConfig>` to
//! `install_client_tcp_tls`, which drives the rustls handshake on its
//! own task before installing reader / writer tasks. The handler echoes
//! a Reply back via `bus.send_to_client`, the client reads the Reply.

mod common;

use async_channel::bounded;
use common::{header_only, install_tls_clients_locally, loopback};
use compio::net::TcpStream;
use iggy_binary_protocol::Command2;
use iggy_binary_protocol::consensus::MESSAGE_ALIGN;
use iggy_binary_protocol::consensus::iobuf::Frozen;
use iggy_binary_protocol::{GenericHeader, Message};
use message_bus::client_listener::RequestHandler;
use message_bus::client_listener::tcp_tls::{bind, run};
use message_bus::transports::tcp_tls::TcpTlsTransportConn;
use message_bus::transports::tls::{install_default_crypto_provider, self_signed_for_loopback};
use message_bus::transports::{ActorContext, TransportConn};
use message_bus::{FusedShutdown, IggyMessageBus, MessageBus, MessageBusConfig, Shutdown, framing};
use rustls::RootCertStore;
use rustls::pki_types::ServerName;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[compio::test]
async fn tcp_tls_client_listener_accepts_and_round_trips() {
    install_default_crypto_provider();

    let bus = Rc::new(IggyMessageBus::new(0));

    // Handler echoes a Reply back to the originating client_id via the
    // bus's send_to_client surface — same path a real dispatcher would
    // take. Spawned because the handler signature is synchronous.
    let bus_for_handler = Rc::clone(&bus);
    let on_request: RequestHandler = Rc::new(move |client_id, msg| {
        assert_eq!(msg.header().command, Command2::Request);
        let bus = Rc::clone(&bus_for_handler);
        compio::runtime::spawn(async move {
            let reply = header_only(Command2::Reply, 42, 0).into_frozen();
            bus.send_to_client(client_id, reply)
                .await
                .expect("server send_to_client");
        })
        .detach();
    });

    let creds = self_signed_for_loopback();
    let cert_chain = creds.cert_chain.clone();
    let (listener, server_cfg, server_addr) =
        bind(loopback(), creds).await.expect("tls listener bind");
    let token = bus.token();
    let on_accepted = install_tls_clients_locally(Rc::clone(&bus), on_request);
    let accept_handle = compio::runtime::spawn(async move {
        run(listener, server_cfg, token, on_accepted).await;
    });
    bus.track_background(accept_handle);

    // Client side: dial plaintext TCP, then drive the rustls client
    // through the same `TransportConn::run` shape.
    let mut roots = RootCertStore::empty();
    for cert in cert_chain {
        roots.add(cert).expect("trust self-signed cert");
    }
    let client_cfg: Arc<rustls::ClientConfig> = Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(Arc::new(roots))
            .with_no_client_auth(),
    );
    let server_name: ServerName<'static> = "localhost".try_into().expect("server name");

    let client_tcp = TcpStream::connect(server_addr).await.expect("client dial");
    let conn = TcpTlsTransportConn::new_client(client_tcp, client_cfg, server_name);

    let (out_tx, out_rx) = bounded::<Frozen<MESSAGE_ALIGN>>(8);
    let (in_tx, in_rx) = bounded::<Message<GenericHeader>>(8);
    let (client_shutdown, client_token) = Shutdown::new();
    let ctx = ActorContext {
        in_tx,
        rx: out_rx,
        shutdown: FusedShutdown::single(client_token),
        conn_shutdown: client_shutdown.clone(),
        max_batch: 16,
        max_message_size: framing::MAX_MESSAGE_SIZE,
        label: "test-client",
        peer: "test-client".to_owned(),
    };
    let client_handle = compio::runtime::spawn(async move { conn.run(ctx).await });

    let request = header_only(Command2::Request, 42, 0).into_frozen();
    out_tx.send(request).await.expect("client send");

    let reply = compio::time::timeout(Duration::from_secs(5), in_rx.recv())
        .await
        .expect("client must receive reply within 5 s")
        .expect("reply frame");
    assert_eq!(reply.header().command, Command2::Reply);
    assert_eq!(reply.header().cluster, 42);

    client_shutdown.trigger();
    let _ = compio::time::timeout(Duration::from_secs(5), client_handle).await;

    let outcome = bus.shutdown(Duration::from_secs(5)).await;
    assert_eq!(
        outcome.force, 0,
        "graceful shutdown should not force-cancel"
    );
}

#[compio::test]
async fn slow_tls_handshake_evicts_registry() {
    // W1 regression: a peer that completes the TCP accept but never
    // sends a ClientHello must not pin a registry slot indefinitely.
    // The installer plumbs `handshake_grace` from the bus config; the
    // transport's `run` body wraps the rustls accept in
    // `compio::time::timeout(handshake_grace, _)` and, on Elapsed,
    // returns so the install scopeguard removes the slot.
    install_default_crypto_provider();

    let cfg = MessageBusConfig {
        handshake_grace: Duration::from_millis(200),
        ..MessageBusConfig::default()
    };
    let bus = Rc::new(IggyMessageBus::with_tunables(0, cfg));

    let on_request: RequestHandler = Rc::new(|_, _| {
        panic!("handler must not fire: TLS handshake never completes");
    });

    let creds = self_signed_for_loopback();
    let (listener, server_cfg, server_addr) =
        bind(loopback(), creds).await.expect("tls listener bind");
    let token = bus.token();
    let on_accepted = install_tls_clients_locally(Rc::clone(&bus), on_request);
    let accept_handle = compio::runtime::spawn(async move {
        run(listener, server_cfg, token, on_accepted).await;
    });
    bus.track_background(accept_handle);

    // Dial plain TCP. Server side accepts and starts a rustls handshake
    // that never receives a ClientHello, so the handshake_grace timeout
    // is the only thing that bounds the install lifetime.
    let _slow = TcpStream::connect(server_addr).await.expect("dial");

    // Listener should install the slow client well before the 200 ms
    // grace expires.
    let install_deadline = Instant::now() + Duration::from_secs(1);
    while bus.clients().is_empty() {
        assert!(
            Instant::now() < install_deadline,
            "listener never installed slow client"
        );
        compio::time::sleep(Duration::from_millis(10)).await;
    }

    // Eviction must follow within handshake_grace plus a small
    // scopeguard tail; budget 2 s of headroom for CI flakes.
    let evict_deadline = Instant::now() + Duration::from_secs(2);
    while !bus.clients().is_empty() {
        assert!(
            Instant::now() < evict_deadline,
            "slow TLS handshake slot not evicted within deadline; len = {}",
            bus.clients().len()
        );
        compio::time::sleep(Duration::from_millis(20)).await;
    }

    let outcome = bus.shutdown(Duration::from_secs(5)).await;
    assert_eq!(
        outcome.force, 0,
        "graceful shutdown should not force-cancel"
    );
}
