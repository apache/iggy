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

use iggy_binary_protocol::{Command2, GenericHeader, HEADER_SIZE, Message};
use message_bus::auth::{StaticSharedSecret, TokenSource};
use message_bus::client_listener::RequestHandler;
use message_bus::replica_listener::MessageHandler;
use message_bus::{AcceptedClientFn, AcceptedReplicaFn, IggyMessageBus, installer};
use std::cell::Cell;
use std::net::SocketAddr;
use std::rc::Rc;

/// Default zero-byte shared secret for integration tests. All replicas
/// authenticate against the same dummy source.
#[must_use]
pub fn test_token_source() -> Rc<dyn TokenSource> {
    Rc::new(StaticSharedSecret::zero())
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

/// Build an [`AcceptedReplicaFn`] that installs accepted replica streams
/// directly on the given bus. Mimics the pre-delegation behaviour for
/// single-shard tests that don't spin up a coordinator.
#[must_use]
pub fn install_replicas_locally(
    bus: Rc<IggyMessageBus>,
    on_message: MessageHandler,
) -> AcceptedReplicaFn {
    Rc::new(move |stream, peer_id| {
        installer::install_replica_stream(&bus, peer_id, stream, on_message.clone());
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
        installer::install_client_stream(&bus, client_id, stream, on_request.clone());
    })
}
