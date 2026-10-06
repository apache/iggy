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

mod common;

use async_channel::bounded;
use common::{header_only, install_wss_clients_locally, loopback};
use compio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use compio::net::TcpStream;
use compio::tls::TlsConnector;
use iggy_binary_protocol::Command;
use iggy_binary_protocol::GenericHeader;
use message_bus::BusMessage;
use message_bus::client_listener::RequestHandler;
use message_bus::client_listener::wss::{bind, run};
use message_bus::transports::tls::{install_default_crypto_provider, self_signed_for_loopback};
use message_bus::transports::wss::WssTransportConn;
use message_bus::transports::{ActorContext, TransportConn};
use message_bus::{FusedShutdown, IggyMessageBus, MessageBus, MessageBusConfig, Shutdown, framing};
use rustls::RootCertStore;
use rustls::pki_types::ServerName;
use server_common::Message;
use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[compio::test]
async fn wss_client_listener_accepts_and_round_trips() {
    install_default_crypto_provider();

    let bus = Rc::new(IggyMessageBus::new(0));

    let bus_for_handler = Rc::clone(&bus);
    let on_request: RequestHandler = Rc::new(move |client_id, msg| {
        assert_eq!(msg.header().command, Command::Request);
        let bus = Rc::clone(&bus_for_handler);
        compio::runtime::spawn(async move {
            let reply = header_only(Command::Reply, 7, 0).into_frozen();
            bus.send_to_client(client_id, reply)
                .await
                .expect("server send_to_client");
        })
        .detach();
    });

    let creds = self_signed_for_loopback();
    let cert_chain = creds.cert_chain.clone();
    let (listener, server_cfg, server_addr) = bind(loopback(), creds).expect("wss listener bind");
    let token = bus.token();
    let on_accepted = install_wss_clients_locally(Rc::clone(&bus), on_request);
    let accept_handle = compio::runtime::spawn(async move {
        run(listener, server_cfg, token, on_accepted).await;
    });
    bus.track_background(accept_handle);

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
    let conn = WssTransportConn::new_client(client_tcp, client_cfg, server_name);

    let (out_tx, out_rx) = bounded::<BusMessage>(8);
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

    let request = header_only(Command::Request, 7, 0).into_frozen();
    out_tx.send(request.into()).await.expect("client send");

    let reply = compio::time::timeout(Duration::from_secs(5), in_rx.recv())
        .await
        .expect("client must receive reply within 5 s")
        .expect("reply frame");
    assert_eq!(reply.header().command, Command::Reply);
    assert_eq!(reply.header().cluster, 7);

    client_shutdown.trigger();
    let _ = compio::time::timeout(Duration::from_secs(5), client_handle).await;

    let outcome = bus.shutdown(Duration::from_secs(5)).await;
    assert_eq!(
        outcome.force, 0,
        "graceful shutdown should not force-cancel"
    );
}

#[compio::test]
async fn slow_handshake_evicts_registry() {
    // W1 regression for the WSS plane: a peer that completes the TCP
    // accept but never sends a TLS ClientHello must not pin a registry
    // slot. WSS shares one wall-clock budget across the TLS + WS
    // handshakes; the TLS step alone consumes the whole grace here, so
    // the timeout fires inside `tls_handshake` and the spawned
    // transport returns, triggering the install scopeguard.
    install_default_crypto_provider();

    let cfg = MessageBusConfig {
        handshake_grace: Duration::from_millis(200),
        ..MessageBusConfig::default()
    };
    let bus = Rc::new(IggyMessageBus::with_tunables(0, cfg));

    let on_request: RequestHandler = Rc::new(|_, _| {
        panic!("handler must not fire: WSS handshake never completes");
    });

    let creds = self_signed_for_loopback();
    let (listener, server_cfg, server_addr) = bind(loopback(), creds).expect("wss listener bind");
    let token = bus.token();
    let on_accepted = install_wss_clients_locally(Rc::clone(&bus), on_request);
    let accept_handle = compio::runtime::spawn(async move {
        run(listener, server_cfg, token, on_accepted).await;
    });
    bus.track_background(accept_handle);

    let _slow = TcpStream::connect(server_addr).await.expect("dial");

    let install_deadline = Instant::now() + Duration::from_secs(1);
    while bus.clients().is_empty() {
        assert!(
            Instant::now() < install_deadline,
            "listener never installed slow client"
        );
        compio::time::sleep(Duration::from_millis(10)).await;
    }

    let evict_deadline = Instant::now() + Duration::from_secs(2);
    while !bus.clients().is_empty() {
        assert!(
            Instant::now() < evict_deadline,
            "slow WSS handshake slot not evicted within deadline; len = {}",
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

#[compio::test]
async fn failed_and_interrupted_wss_handshakes_release_delegated_connections() {
    common::assert_failed_tls_installs_are_removed(message_bus::ClientTransportKind::Wss).await;
}

#[compio::test]
async fn stalled_rejected_and_interrupted_websocket_upgrades_release_delegated_connections() {
    const OWNER: u16 = 1;
    const FIRST_CLIENT_ID: u128 = (OWNER as u128) << 112 | 1;
    const HANDSHAKE_GRACE: Duration = Duration::from_secs(2);
    const TLS_DELAY: Duration = Duration::from_millis(1_200);
    const DEADLINE_MARGIN: Duration = Duration::from_millis(400);
    const IO_TIMEOUT: Duration = Duration::from_secs(3);
    const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);
    const UPGRADE_WAIT: Duration = Duration::from_millis(10);

    install_default_crypto_provider();
    let bus = Rc::new(IggyMessageBus::with_tunables(
        OWNER,
        MessageBusConfig {
            handshake_grace: HANDSHAKE_GRACE,
            ..MessageBusConfig::default()
        },
    ));
    let credentials = self_signed_for_loopback();
    let mut roots = RootCertStore::empty();
    for cert in &credentials.cert_chain {
        roots.add(cert.clone()).unwrap();
    }
    let connector = TlsConnector::from(Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    ));
    let (listener, config, address) = bind(loopback(), credentials).unwrap();
    let request_seen = Rc::new(Cell::new(false));
    let seen_by_handler = Rc::clone(&request_seen);
    let on_request: RequestHandler = Rc::new(move |_, _| seen_by_handler.set(true));
    let accepted = install_wss_clients_locally(Rc::clone(&bus), on_request);

    let tcp = TcpStream::connect(address).await.unwrap();
    accepted(listener.accept().await.unwrap().0, Arc::clone(&config));
    let installed_at = Instant::now();
    compio::time::sleep(TLS_DELAY).await;
    let mut tls = compio::time::timeout(IO_TIMEOUT, connector.connect("localhost", tcp))
        .await
        .unwrap()
        .unwrap();
    common::wait_for_client_count(&bus, 0).await;
    assert!(
        installed_at.elapsed() < HANDSHAKE_GRACE + DEADLINE_MARGIN,
        "TLS and WebSocket must share one deadline, elapsed {:?}",
        installed_at.elapsed()
    );
    assert!(bus.client_meta(FIRST_CLIENT_ID).is_none());
    let closed = compio::time::timeout(IO_TIMEOUT, tls.read(vec![0]))
        .await
        .unwrap()
        .0;
    assert!(closed.is_err() || closed.unwrap() == 0);

    let tcp = TcpStream::connect(address).await.unwrap();
    accepted(listener.accept().await.unwrap().0, Arc::clone(&config));
    let mut tls = compio::time::timeout(IO_TIMEOUT, connector.connect("localhost", tcp))
        .await
        .unwrap()
        .unwrap();
    assert!(bus.client_meta(FIRST_CLIENT_ID + 1).is_some());
    tls.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .0
        .unwrap();
    tls.flush().await.unwrap();
    common::wait_for_client_count(&bus, 0).await;
    assert!(bus.client_meta(FIRST_CLIENT_ID + 1).is_none());

    let tcp = TcpStream::connect(address).await.unwrap();
    accepted(listener.accept().await.unwrap().0, config);
    let mut tls = compio::time::timeout(IO_TIMEOUT, connector.connect("localhost", tcp))
        .await
        .unwrap()
        .unwrap();
    tls.write_all(b"GET / HTTP/1.1\r\n").await.0.unwrap();
    tls.flush().await.unwrap();
    // Let the server finish TLS and wait for the remaining upgrade headers.
    compio::time::sleep(UPGRADE_WAIT).await;
    assert!(bus.client_meta(FIRST_CLIENT_ID + 2).is_some());
    assert_eq!(
        bus.shutdown(SHUTDOWN_TIMEOUT).await.force,
        0,
        "pending WebSocket upgrade must observe shutdown without force-cancellation"
    );
    assert!(bus.clients().is_empty());
    assert!(bus.client_meta(FIRST_CLIENT_ID + 2).is_none());
    let closed = compio::time::timeout(IO_TIMEOUT, tls.read(vec![0]))
        .await
        .unwrap()
        .0;
    assert!(closed.is_err() || closed.unwrap() == 0);
    assert!(
        !request_seen.get(),
        "failed upgrades must not dispatch requests"
    );
}
