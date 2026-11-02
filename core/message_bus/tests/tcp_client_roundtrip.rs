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
//! sends a Request, the handler echoes a Reply back via
//! `bus.send_to_client_instance`, the client reads the Reply.

mod common;

use common::{header_only, install_clients_locally, loopback};
use compio::net::{TcpSocket, TcpStream};
use iggy_binary_protocol::{Command, GenericHeader};
use message_bus::client_listener::RequestHandler;
use message_bus::client_listener::tcp::{bind, run};
use message_bus::framing;
use message_bus::{IggyMessageBus, InstanceToken, MessageBusConfig, SendError};
use server_common::Message;
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::{Duration, Instant};

#[compio::test]
async fn request_reply_round_trip() {
    let bus = Rc::new(IggyMessageBus::new(7));

    // Handler echoes a Reply back to the connection the Request arrived on.
    let bus_for_handler = bus.clone();
    let on_request: RequestHandler = Rc::new(move |client_id, msg| {
        assert_eq!(msg.header().command, Command::Request);
        let instance = bus_for_handler.client_instance(client_id);
        let reply = header_only(Command::Reply, 42, 0).into_frozen();
        bus_for_handler
            .send_to_client_instance(client_id, instance, reply.into())
            .expect("server send_to_client_instance");
    });

    let (listener, addr) = bind(loopback()).expect("bind");
    let token = bus.token();
    let accept_delegate = install_clients_locally(bus.clone(), on_request);
    let accept_handle = compio::runtime::spawn(async move {
        run(listener, token, accept_delegate).await;
    });
    bus.track_background(accept_handle);

    // Dial as a raw TCP client.
    let mut client = TcpStream::connect(addr).await.expect("connect");

    let request = header_only(Command::Request, 42, 0);
    framing::write_message(&mut client, request)
        .await
        .expect("client write");

    let reply = framing::read_message(&mut client, framing::MAX_MESSAGE_SIZE)
        .await
        .expect("client read");
    assert_eq!(reply.header().command, Command::Reply);
    assert_eq!(reply.header().cluster, 42);

    let outcome = bus.shutdown(Duration::from_secs(2)).await;
    assert_eq!(
        outcome.force, 0,
        "graceful shutdown should not force-cancel"
    );
}

#[compio::test]
async fn given_spoofed_wire_client_when_dispatched_should_keep_installed_transport_id() {
    const SHARD: u16 = 7;
    const VICTIM: u128 = (7_u128 << 112) | 1;
    const CALLER: u128 = (7_u128 << 112) | 2;
    let bus = Rc::new(IggyMessageBus::new(SHARD));
    let observed = Rc::new(RefCell::new(Vec::new()));
    let observations = Rc::clone(&observed);
    let handler_bus = Rc::clone(&bus);
    let on_request: RequestHandler = Rc::new(move |client_id, message| {
        let request = message
            .try_into_typed::<iggy_binary_protocol::RequestHeader>()
            .unwrap();
        observations
            .borrow_mut()
            .push((client_id, request.header().client));
        handler_bus
            .send_to_client_instance(
                client_id,
                handler_bus.client_instance(client_id),
                header_only(Command::Reply, 42, 0).into_frozen().into(),
            )
            .unwrap();
    });
    let (listener, address) = bind(loopback()).unwrap();
    let token = bus.token();
    let delegate = install_clients_locally(Rc::clone(&bus), on_request);
    bus.track_background(compio::runtime::spawn(async move {
        run(listener, token, delegate).await;
    }));
    let mut victim = TcpStream::connect(address).await.unwrap();
    let request = || {
        header_only(Command::Request, 42, 0)
            .transmute_header(
                |header, request: &mut iggy_binary_protocol::RequestHeader| {
                    request.command = header.command;
                    request.cluster = header.cluster;
                    request.size = header.size;
                    request.client = VICTIM;
                    request.operation = iggy_binary_protocol::Operation::Register;
                },
            )
            .into_generic()
    };
    framing::write_message(&mut victim, request())
        .await
        .unwrap();
    framing::read_message(&mut victim, framing::MAX_MESSAGE_SIZE)
        .await
        .unwrap();
    let victim_instance = bus.client_instance(VICTIM);
    let mut caller = TcpStream::connect(address).await.unwrap();
    framing::write_message(&mut caller, request())
        .await
        .unwrap();
    framing::read_message(&mut caller, framing::MAX_MESSAGE_SIZE)
        .await
        .unwrap();
    assert_eq!(*observed.borrow(), [(VICTIM, VICTIM), (CALLER, VICTIM)]);
    assert_eq!(bus.client_instance(VICTIM), victim_instance);
    assert_ne!(bus.client_instance(CALLER), victim_instance);
    bus.shutdown(Duration::from_secs(2)).await;
}

#[compio::test]
async fn unexpected_command_is_ignored() {
    let bus = Rc::new(IggyMessageBus::new(0));

    // Handler should never be called because only Request is accepted.
    let (tx, rx) = async_channel::bounded::<()>(1);
    let on_request: RequestHandler = Rc::new(move |_, _| {
        let _ = tx.try_send(());
    });

    let (listener, addr) = bind(loopback()).unwrap();
    let token = bus.token();
    let accept_delegate = install_clients_locally(bus.clone(), on_request);
    let accept_handle = compio::runtime::spawn(async move {
        run(listener, token, accept_delegate).await;
    });
    bus.track_background(accept_handle);

    let mut client = TcpStream::connect(addr).await.unwrap();
    let bogus = header_only(Command::Ping, 0, 0);
    framing::write_message(&mut client, bogus).await.unwrap();

    // Give the read loop a chance to observe + ignore it.
    compio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        rx.try_recv().is_err(),
        "handler must not be called for non-Request commands"
    );

    bus.shutdown(Duration::from_secs(1)).await;
}

/// A client that stops reading blocks its connection's socket writer, so the
/// replies behind it fill the queue and the next one is refused. The refusal
/// closes that connection: the server releases the blocked socket within
/// `close_peer_timeout`, the client keeps every complete reply under its own
/// request id and then reads end of stream or a reset instead of waiting for
/// the lost replies. Another connection keeps working.
#[compio::test]
#[allow(clippy::too_many_lines)]
async fn given_stalled_reader_when_reply_queue_refuses_should_close_only_that_connection() {
    const SHARD: u16 = 3;
    const QUEUE_CAPACITY: usize = 2;
    const MAX_BATCH: usize = 1;
    const CLOSE_PEER_TIMEOUT: Duration = Duration::from_millis(300);
    // Scheduling headroom on a loaded host, far below any SDK request timeout.
    const TEARDOWN_SLACK: Duration = Duration::from_secs(2);
    const POLL_INTERVAL: Duration = Duration::from_millis(5);
    // A fixed receive buffer turns off autotuning, so a reply larger than both
    // socket buffers keeps the server's writer blocked mid-write.
    const CLIENT_RECEIVE_BUFFER: u32 = 64 * 1024;
    const REPLY_SIZE: usize = 8 * 1024 * 1024;
    const MAX_REQUESTS: u128 = 64;
    const READ_BUDGET: Duration = Duration::from_secs(10);
    const HEALTHY_REQUEST: u128 = 1_000;

    let config = MessageBusConfig {
        client_queue_capacity: QUEUE_CAPACITY,
        max_batch: MAX_BATCH,
        close_peer_timeout: CLOSE_PEER_TIMEOUT,
        ..MessageBusConfig::default()
    };
    let bus = Rc::new(IggyMessageBus::with_tunables(SHARD, config));
    let outcomes: Rc<RefCell<Vec<Result<(), SendError>>>> = Rc::default();
    let stalled_instance: Rc<Cell<Option<InstanceToken>>> = Rc::default();
    let handler_bus = Rc::clone(&bus);
    let handler_outcomes = Rc::clone(&outcomes);
    let handler_instance = Rc::clone(&stalled_instance);
    // Replies in request order, and every send outcome is recorded as it happens.
    let on_request: RequestHandler = Rc::new(move |client_id, request| {
        let reply = sized_reply(request.header().cluster, REPLY_SIZE);
        let instance = handler_bus.client_instance(client_id);
        handler_instance.set(instance);
        let outcome =
            handler_bus.send_to_client_instance(client_id, instance, reply.into_frozen().into());
        handler_outcomes.borrow_mut().push(outcome);
    });
    let lost: Rc<RefCell<Vec<(u128, Instant)>>> = Rc::default();
    let lost_hook = Rc::clone(&lost);
    bus.set_client_connection_lost_fn(Rc::new(move |client_id| {
        lost_hook.borrow_mut().push((client_id, Instant::now()));
    }));

    let (listener, addr) = bind(loopback()).expect("bind");
    let token = bus.token();
    let accept_delegate = install_clients_locally(bus.clone(), on_request);
    let accept_handle = compio::runtime::spawn(async move {
        run(listener, token, accept_delegate).await;
    });
    bus.track_background(accept_handle);

    let socket = TcpSocket::new_v4().await.expect("socket");
    socket
        .set_recv_buffer_size(CLIENT_RECEIVE_BUFFER)
        .expect("receive buffer");
    let mut stalled = socket.connect(addr).await.expect("connect");
    let mut sent: u128 = 0;
    while !outcomes.borrow().iter().any(Result::is_err) {
        assert!(
            sent < MAX_REQUESTS,
            "a client that never reads must fill its reply queue"
        );
        framing::write_message(&mut stalled, header_only(Command::Request, sent, 0))
            .await
            .expect("client write");
        sent += 1;
        compio::time::sleep(POLL_INTERVAL).await;
    }
    let refused_at = Instant::now();
    let accepted = {
        let outcomes = outcomes.borrow();
        assert!(
            matches!(outcomes.last(), Some(Err(SendError::Backpressure))),
            "the first failed send must be the queue refusal, got {:?}",
            outcomes.last()
        );
        outcomes.len() - 1
    };

    while lost.borrow().is_empty() {
        assert!(
            refused_at.elapsed() <= CLOSE_PEER_TIMEOUT + TEARDOWN_SLACK,
            "the refused connection must be torn down within close_peer_timeout"
        );
        compio::time::sleep(POLL_INTERVAL).await;
    }
    let stalled_id = lost.borrow()[0].0;
    assert!(
        bus.clients().is_empty(),
        "the registry must drop the connection"
    );
    assert!(
        matches!(
            bus.send_to_client_instance(
                stalled_id,
                stalled_instance.get(),
                sized_reply(0, REPLY_SIZE).into_frozen().into()
            ),
            Err(SendError::ClientNotFound(_))
        ),
        "a later reply must not reach the closed connection"
    );

    let mut next_request: u128 = 0;
    let end = compio::time::timeout(READ_BUDGET, async {
        loop {
            match framing::read_message(&mut stalled, REPLY_SIZE).await {
                Ok(reply) => {
                    assert_eq!(reply.header().command, Command::Reply);
                    assert_eq!(
                        reply.header().cluster,
                        next_request,
                        "a complete reply must keep its request id"
                    );
                    next_request += 1;
                }
                Err(error) => break error,
            }
        }
    })
    .await;
    assert!(
        end.is_ok(),
        "the closed connection must end the read instead of leaving it to a timeout"
    );
    assert!(
        next_request <= u128::try_from(accepted).expect("count fits u128"),
        "only replies queued before the refusal can arrive"
    );

    let mut healthy = TcpStream::connect(addr).await.expect("connect healthy");
    framing::write_message(
        &mut healthy,
        header_only(Command::Request, HEALTHY_REQUEST, 0),
    )
    .await
    .expect("healthy write");
    let reply = framing::read_message(&mut healthy, REPLY_SIZE)
        .await
        .expect("healthy read");
    assert_eq!(reply.header().cluster, HEALTHY_REQUEST);
    assert_eq!(lost.borrow().len(), 1, "only the refused connection closes");

    bus.shutdown(Duration::from_secs(2)).await;
}

/// A Reply frame of `size` bytes that echoes `request`, the id the test
/// client stamps into the `cluster` field of its request.
fn sized_reply(request: u128, size: usize) -> Message<GenericHeader> {
    let size_field = u32::try_from(size).expect("reply size fits u32");
    Message::<GenericHeader>::new(size).transmute_header(|_, header: &mut GenericHeader| {
        header.command = Command::Reply;
        header.cluster = request;
        header.size = size_field;
    })
}
