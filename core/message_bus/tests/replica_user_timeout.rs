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

//! A peer that completes the replica handshake and then stops reading
//! never sends a FIN or RST. Its kernel only advertises a zero window, and
//! the dialer must close the link once its frames wait there for
//! `replica_user_timeout`.

#![cfg(target_os = "linux")]

mod common;

use common::{install_dialed_replicas_locally, set_replica_ctx, spawn_stalled_replica};
use iggy_binary_protocol::{Command, GenericHeader};
use message_bus::config::MessageBusConfig;
use message_bus::connector::start as start_connector;
use message_bus::replica::listener::MessageHandler;
use message_bus::{IggyMessageBus, MessageBus, SendError};
use server_common::Message;
use std::cell::Cell;
use std::rc::Rc;
use std::time::{Duration, Instant};

const CLUSTER: u128 = 0x7155;
const PEER: u8 = 1;
const USER_TIMEOUT: Duration = Duration::from_millis(500);
/// Large frames and a short queue fill the kernel buffers of both ends in
/// a few milliseconds without a large queue in memory.
const FRAME_SIZE: usize = 64 * 1024;
const QUEUE: usize = 16;
/// A live peer answers every zero-window probe, so without the user timeout
/// the link stays open. This budget only bounds a failing run.
const CLOSE_BUDGET: Duration = Duration::from_secs(10);
const CONNECT_BUDGET: Duration = Duration::from_secs(2);
const POLL: Duration = Duration::from_millis(1);
const SHUTDOWN: Duration = Duration::from_secs(2);

#[compio::test]
#[allow(clippy::future_not_send)]
async fn peer_that_stops_reading_loses_its_link() {
    let bus = Rc::new(IggyMessageBus::with_tunables(
        0,
        MessageBusConfig {
            peer_queue_capacity: QUEUE,
            replica_user_timeout: USER_TIMEOUT,
            ..MessageBusConfig::default()
        },
    ));
    set_replica_ctx(&bus, CLUSTER, 0, 2, None);
    let closed = Rc::new(Cell::new(None));
    let closed_by = Rc::clone(&closed);
    bus.set_connection_lost_fn(Rc::new(move |link| closed_by.set(Some(link.replica))));
    let (addr, stalled) = spawn_stalled_replica(&bus, PEER).await;
    let on_message: MessageHandler = Rc::new(|_, _| {});
    start_connector(
        &bus,
        0,
        vec![(PEER, addr)],
        install_dialed_replicas_locally(Rc::clone(&bus), on_message),
        bus.config().reconnect_period,
    )
    .await;

    let deadline = Instant::now() + CONNECT_BUDGET;
    while !bus.replicas().contains(PEER) {
        assert!(Instant::now() < deadline, "the stalled peer must connect");
        compio::time::sleep(POLL).await;
    }

    // Refill the queue whenever the writer drains it, so data always waits
    // behind the zero window once the buffers are full.
    let deadline = Instant::now() + CLOSE_BUDGET;
    while closed.get().is_none() {
        assert!(
            Instant::now() < deadline,
            "the link to a peer that stopped reading stayed open for {CLOSE_BUDGET:?}"
        );
        match bus.send_to_replica(PEER, frame().into_frozen()).await {
            Ok(()) => {}
            Err(SendError::Backpressure | SendError::ReplicaNotConnected(_)) => {
                compio::time::sleep(POLL).await;
            }
            Err(other) => panic!("unexpected send error: {other:?}"),
        }
    }
    assert_eq!(closed.get(), Some(PEER));

    bus.shutdown(SHUTDOWN).await;
    drop(stalled);
}

#[allow(clippy::cast_possible_truncation)]
fn frame() -> Message<GenericHeader> {
    Message::<GenericHeader>::new(FRAME_SIZE).transmute_header(|_, header: &mut GenericHeader| {
        header.command = Command::Prepare;
        header.cluster = CLUSTER;
        header.size = FRAME_SIZE as u32;
    })
}
