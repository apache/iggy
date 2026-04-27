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
//! listener, sends a Request, the handler echoes a Reply back via
//! `bus.send_to_client`, the client reads the Reply.

mod common;

use common::{header_only, install_quic_clients_locally, loopback};
use compio_quic::{ClientBuilder, Endpoint};
use iggy_binary_protocol::Command2;
use iggy_binary_protocol::consensus::MESSAGE_ALIGN;
use iggy_binary_protocol::consensus::iobuf::Frozen;
use message_bus::client_listener::RequestHandler;
use message_bus::client_listener_quic::{bind, run};
use message_bus::framing;
use message_bus::transports::quic::{QuicTransportConn, server_config_with_cert};
use message_bus::transports::{TransportConn, TransportReader, TransportWriter};
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
    let mut builder = ClientBuilder::new_with_empty_roots()
        .with_custom_certificate(server_cert)
        .expect("trust cert")
        .with_no_crls();
    builder = builder.with_alpn_protocols(&["iggy.consensus.v1"]);
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
    let server_cfg = server_config_with_cert(vec![cert.clone()], key).expect("server config");
    let (endpoint, server_addr) = bind(loopback(), server_cfg).await.expect("bind");

    let token = bus.token();
    let on_accepted = install_quic_clients_locally(bus.clone(), on_request);
    let accept_handle = compio::runtime::spawn(async move {
        run(endpoint, token, on_accepted).await;
    });
    bus.track_background(accept_handle);

    // Dial as a real QUIC client.
    let client = client_endpoint(cert).await;
    let connecting = client
        .connect(server_addr, "localhost", None)
        .expect("connect");
    let connection = connecting.await.expect("client handshake");
    let (send, recv) = connection.open_bi_wait().await.expect("open_bi");

    // Reuse the existing QuicTransportConn split for client-side framing.
    let conn = QuicTransportConn::new(connection, (send, recv));
    let (mut reader, mut writer) = conn.into_split();

    // Send a Request as a single-frame batch via the writer's atomic
    // send_batch (mirrors the production hot path).
    let request = header_only(Command2::Request, 42, 0).into_frozen();
    let mut batch: Vec<Frozen<MESSAGE_ALIGN>> = vec![request];
    writer.send_batch(&mut batch).await.expect("client send");
    assert!(batch.is_empty(), "send_batch must drain the Vec");

    // Read the Reply on the RecvStream.
    let reply = reader
        .read_message(framing::MAX_MESSAGE_SIZE)
        .await
        .expect("client read");
    assert_eq!(reply.header().command, Command2::Reply);
    assert_eq!(reply.header().cluster, 42);

    let outcome = bus.shutdown(Duration::from_secs(2)).await;
    assert_eq!(
        outcome.force, 0,
        "graceful shutdown should not force-cancel"
    );
}
