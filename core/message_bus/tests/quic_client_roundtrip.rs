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

//! End-to-end: a real QUIC client connects to the consensus QUIC client
//! listener, opens a bidi per request (matching the iggy SDK pattern),
//! sends a Request, the handler echoes a Reply back via
//! `bus.send_to_client`, the client reads the Reply off the same bidi.

mod common;

use async_channel::bounded;
use common::{header_only, install_quic_clients_locally, loopback};
use compio::BufResult;
use compio::io::AsyncWriteExt;
use compio_quic::{ClientBuilder, Endpoint};
use iggy_binary_protocol::Command2;
use message_bus::QuicTuning;
use message_bus::client_listener::RequestHandler;
use message_bus::client_listener::quic::{bind, run};
use message_bus::framing;
use message_bus::transports::quic::server_config_with_cert;
use message_bus::{IggyMessageBus, MessageBus};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use std::rc::Rc;
use std::time::Duration;

fn install_crypto_provider() {
    // Idempotent across same-process retries.
    let _ = rustls::crypto::ring::default_provider().install_default();
}

fn self_signed() -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).expect("rcgen");
    let cert_der = CertificateDer::from(cert.cert);
    let key_der: PrivateKeyDer<'static> =
        PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()).into();
    (cert_der, key_der)
}

#[allow(clippy::future_not_send)]
async fn client_endpoint(server_cert: CertificateDer<'static>) -> Endpoint {
    let builder = ClientBuilder::new_with_empty_roots()
        .with_custom_certificate(server_cert)
        .expect("trust cert")
        .with_no_crls();
    builder.bind("127.0.0.1:0").await.expect("client bind")
}

#[compio::test]
async fn request_reply_round_trip() {
    install_crypto_provider();

    let bus = Rc::new(IggyMessageBus::new(7));

    let bus_for_handler = bus.clone();
    let on_request: RequestHandler = Rc::new(move |client_id, msg| {
        assert_eq!(msg.header().command, Command2::Request);
        let bus = bus_for_handler.clone();
        compio::runtime::spawn(async move {
            let reply = header_only(Command2::Reply, 42, 0);
            bus.send_to_client(client_id, reply.into_frozen())
                .await
                .expect("send_to_client should succeed");
        })
        .detach();
    });

    let (cert, key) = self_signed();
    let server_cfg = server_config_with_cert(vec![cert.clone()], key, &QuicTuning::default())
        .expect("server config");
    let (endpoint, server_addr) = bind(loopback(), server_cfg).expect("bind");

    let token = bus.token();
    let on_accepted = install_quic_clients_locally(bus.clone(), on_request);
    let accept_handle = compio::runtime::spawn(async move {
        run(endpoint, token, on_accepted, Duration::from_secs(10)).await;
    });
    bus.track_background(accept_handle);

    // Dial as a real QUIC client.
    let client = client_endpoint(cert).await;
    let connecting = client
        .connect(server_addr, "localhost", None)
        .expect("connect");
    let connection = connecting.await.expect("client handshake");

    // One bidi per request: open, write Request, finish, read Reply.
    // Matches the iggy QUIC SDK's `open_bi -> write_all -> finish -> read`
    // pattern; the server's accept_bi loop accepts the bidi, dispatches
    // the request, writes the Reply, `finish()`-es the send half.
    let (mut send, mut recv) = connection.open_bi_wait().await.expect("open_bi");
    let request = header_only(Command2::Request, 42, 0).into_frozen();
    let BufResult(result, _) = send.write_all(request).await;
    result.expect("client write request");
    send.finish().expect("client finish");

    let reply = compio::time::timeout(
        Duration::from_secs(5),
        framing::read_message(&mut recv, framing::MAX_MESSAGE_SIZE),
    )
    .await
    .expect("client must receive reply within 5 s")
    .expect("reply frame");
    assert_eq!(reply.header().command, Command2::Reply);
    assert_eq!(reply.header().cluster, 42);

    let outcome = bus.shutdown(Duration::from_secs(2)).await;
    assert_eq!(
        outcome.force, 0,
        "graceful shutdown should not force-cancel"
    );
}

/// Regression test for the QUIC accept-loop head-of-line block. Before
/// the fix, the listener ran handshake + first `accept_bi` sequentially
/// inside its accept loop, so a single peer that completed the handshake
/// but never opened a bidi stream wedged every subsequent accept until
/// the 30 s idle timeout fired. The fix spawns one handshake task per
/// [`Incoming`] and now also defers `accept_bi` entirely to the
/// per-connection `run()`, so a connected-but-silent peer can never
/// block the listener. This test asserts the wedged peer does not block
/// a fast peer's request from being processed.
#[compio::test]
async fn slow_handshake_does_not_block_subsequent_accept() {
    install_crypto_provider();

    let bus = Rc::new(IggyMessageBus::new(7));

    let (request_tx, request_rx) = bounded::<()>(8);
    let request_tx = Rc::new(request_tx);
    let on_request: RequestHandler = Rc::new(move |_client_id, msg| {
        assert_eq!(msg.header().command, Command2::Request);
        let _ = request_tx.try_send(());
    });

    let (cert, key) = self_signed();
    let server_cfg = server_config_with_cert(vec![cert.clone()], key, &QuicTuning::default())
        .expect("server config");
    let (endpoint, server_addr) = bind(loopback(), server_cfg).expect("bind");

    let token = bus.token();
    let on_accepted = install_quic_clients_locally(bus.clone(), on_request);
    let accept_handle = compio::runtime::spawn(async move {
        run(endpoint, token, on_accepted, Duration::from_secs(10)).await;
    });
    bus.track_background(accept_handle);

    // Slow client: completes the QUIC handshake but never opens a bidi
    // stream. Without the listener-side fix, the accept loop blocked on
    // this peer's `accept_bi().await` and never moved on to the fast
    // client.
    let slow_client = client_endpoint(cert.clone()).await;
    let slow_connecting = slow_client
        .connect(server_addr, "localhost", None)
        .expect("slow connect");
    let slow_connection = compio::time::timeout(Duration::from_secs(2), slow_connecting)
        .await
        .expect("slow client handshake within 2 s")
        .expect("slow client handshake");

    // Fast client: handshake + open_bi + send Request must all finish
    // within 2 s, well below the 30 s QUIC idle timeout that gated the
    // pre-fix behaviour.
    let fast_client = client_endpoint(cert).await;
    let fast_connecting = fast_client
        .connect(server_addr, "localhost", None)
        .expect("fast connect");
    let fast_connection = compio::time::timeout(Duration::from_secs(2), fast_connecting)
        .await
        .expect("fast client handshake within 2 s")
        .expect("fast client handshake");
    let (mut send, _recv) =
        compio::time::timeout(Duration::from_secs(2), fast_connection.open_bi_wait())
            .await
            .expect("fast client open_bi within 2 s")
            .expect("open_bi");

    let request = header_only(Command2::Request, 1, 0).into_frozen();
    let BufResult(result, _) = send.write_all(request).await;
    result.expect("fast client write request");
    send.finish().expect("fast client finish");

    compio::time::timeout(Duration::from_secs(2), request_rx.recv())
        .await
        .expect("server must dispatch fast client's Request within 2 s")
        .expect("server-side request channel");

    drop(slow_connection);
    let _ = bus.shutdown(Duration::from_secs(2)).await;
}
