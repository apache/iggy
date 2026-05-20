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
//! End-to-end bootstrap: `replica::io::start_on_shard_zero` binds the
//! TCP-TLS plane (alongside replica + client TCP listeners) and the
//! caller dials the listener address surfaced via
//! [`message_bus::replica::io::BoundPlanes::tcp_tls`]. Mirrors the
//! `tcp_tls_client_listener.rs` listener-level test one level up the
//! stack: the validator, paired-presence trio, and `BoundPlanes`
//! growth all participate.

mod common;

use async_channel::bounded;
use common::{
    header_only, install_clients_locally, install_replicas_locally, install_tls_clients_locally,
    loopback,
};
use compio::net::TcpStream;
use iggy_binary_protocol::Command2;
use iggy_binary_protocol::consensus::MESSAGE_ALIGN;
use iggy_binary_protocol::consensus::iobuf::Frozen;
use iggy_binary_protocol::{GenericHeader, Message};
use message_bus::client_listener::RequestHandler;
use message_bus::connector::DEFAULT_RECONNECT_PERIOD;
use message_bus::replica::io::start_on_shard_zero;
use message_bus::replica::listener::MessageHandler;
use message_bus::transports::tcp_tls::TcpTlsTransportConn;
use message_bus::transports::tls::{install_default_crypto_provider, self_signed_for_loopback};
use message_bus::transports::{ActorContext, TransportConn};
use message_bus::{FusedShutdown, IggyMessageBus, MessageBus, Shutdown, framing};
use rustls::RootCertStore;
use rustls::pki_types::ServerName;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

const CLUSTER: u128 = 0xC0DE;

#[compio::test]
async fn start_on_shard_zero_tcp_tls_round_trip() {
    install_default_crypto_provider();

    let bus = Rc::new(IggyMessageBus::new(0));

    let on_message: MessageHandler = Rc::new(|_, _| {});
    let bus_for_handler = Rc::clone(&bus);
    let on_request: RequestHandler = Rc::new(move |client_id, msg| {
        assert_eq!(msg.header().command, Command2::Request);
        let bus = Rc::clone(&bus_for_handler);
        compio::runtime::spawn(async move {
            let reply = header_only(Command2::Reply, CLUSTER, 0).into_frozen();
            bus.send_to_client(client_id, reply)
                .await
                .expect("server send_to_client");
        })
        .detach();
    });

    let accepted_replica = install_replicas_locally(Rc::clone(&bus), on_message);
    let accepted_client = install_clients_locally(Rc::clone(&bus), Rc::clone(&on_request));
    let accepted_tls = install_tls_clients_locally(Rc::clone(&bus), on_request);

    let creds = self_signed_for_loopback();
    let cert_chain = creds.cert_chain.clone();

    let bound = start_on_shard_zero(
        &bus,
        loopback(),
        loopback(),
        None,
        None,
        None,
        Some(loopback()),
        Some(creds),
        None,
        None,
        CLUSTER,
        0,
        1,
        vec![],
        accepted_replica,
        accepted_client,
        None,
        None,
        Some(accepted_tls),
        None,
        DEFAULT_RECONNECT_PERIOD,
    )
    .await
    .expect("start_on_shard_zero must succeed")
    .expect("shard 0 must return BoundPlanes");

    let server_addr = bound.tcp_tls.expect("tcp_tls plane bound");
    assert_ne!(server_addr.port(), 0, "tcp_tls must bind a real port");

    // Client side: dial the bound TCP-TLS address, then drive the rustls
    // client through the same `TransportConn::run` shape the server uses.
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

    let request = header_only(Command2::Request, CLUSTER, 0).into_frozen();
    out_tx.send(request).await.expect("client send");

    let reply = compio::time::timeout(Duration::from_secs(5), in_rx.recv())
        .await
        .expect("client must receive reply within 5 s")
        .expect("reply frame");
    assert_eq!(reply.header().command, Command2::Reply);
    assert_eq!(reply.header().cluster, CLUSTER);

    client_shutdown.trigger();
    let _ = compio::time::timeout(Duration::from_secs(5), client_handle).await;

    let outcome = bus.shutdown(Duration::from_secs(5)).await;
    assert_eq!(
        outcome.force, 0,
        "graceful shutdown should not force-cancel"
    );
}
