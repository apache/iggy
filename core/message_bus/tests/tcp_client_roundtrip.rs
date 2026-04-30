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

//! End-to-end: a real TCP client connects to the consensus client listener,
//! sends a Request, the handler echoes a Reply back via `bus.send_to_client`,
//! the client reads the Reply.

mod common;

use common::{header_only, install_clients_locally, loopback};
use compio::net::TcpStream;
use iggy_binary_protocol::Command2;
use message_bus::client_listener::RequestHandler;
use message_bus::client_listener::tcp::{bind, run};
use message_bus::framing;
use message_bus::{IggyMessageBus, MessageBus};
use std::rc::Rc;
use std::time::Duration;

#[compio::test]
async fn request_reply_round_trip() {
    let bus = Rc::new(IggyMessageBus::new(7));

    // Handler echoes a Reply back via send_to_client.
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

    let (listener, addr) = bind(loopback()).await.expect("bind");
    let token = bus.token();
    let accept_delegate = install_clients_locally(bus.clone(), on_request);
    let accept_handle = compio::runtime::spawn(async move {
        run(listener, token, accept_delegate).await;
    });
    bus.track_background(accept_handle);

    // Dial as a raw TCP client.
    let mut client = TcpStream::connect(addr).await.expect("connect");

    let request = header_only(Command2::Request, 42, 0);
    framing::write_message(&mut client, request)
        .await
        .expect("client write");

    let reply = framing::read_message(&mut client, framing::MAX_MESSAGE_SIZE)
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

#[compio::test]
async fn unexpected_command_is_ignored() {
    let bus = Rc::new(IggyMessageBus::new(0));

    // Handler should never be called because only Request is accepted.
    let (tx, rx) = async_channel::bounded::<()>(1);
    let on_request: RequestHandler = Rc::new(move |_, _| {
        let _ = tx.try_send(());
    });

    let (listener, addr) = bind(loopback()).await.unwrap();
    let token = bus.token();
    let accept_delegate = install_clients_locally(bus.clone(), on_request);
    let accept_handle = compio::runtime::spawn(async move {
        run(listener, token, accept_delegate).await;
    });
    bus.track_background(accept_handle);

    let mut client = TcpStream::connect(addr).await.unwrap();
    let bogus = header_only(Command2::Ping, 0, 0);
    framing::write_message(&mut client, bogus).await.unwrap();

    // Give the read loop a chance to observe + ignore it.
    compio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        rx.try_recv().is_err(),
        "handler must not be called for non-Request commands"
    );

    bus.shutdown(Duration::from_secs(1)).await;
}
