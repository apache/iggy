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

//! Panic-cleanup test for the connection installer.
//!
//! `compio::runtime::spawn` wraps the future with
//! `AssertUnwindSafe(future).catch_unwind()` (compio-runtime-0.11.0
//! `runtime/mod.rs:202-203`), so a panicking transport returns silently
//! to the runtime. Without a scopeguard, the post-loop cleanup that
//! evicts the registry slot and fires `notify_connection_lost` would
//! never run, leaking the slot and leaving the bus convinced the peer
//! is still up. These tests force a panic inside `conn.run` and assert
//! the cleanup ran on the unwind path.

mod common;

use message_bus::IggyMessageBus;
use message_bus::client_listener::RequestHandler;
use message_bus::installer::{install_client_conn, install_replica_conn};
use message_bus::replica_listener::MessageHandler;
use message_bus::transports::{ActorContext, TransportConn};
use std::cell::Cell;
use std::rc::Rc;
use std::time::{Duration, Instant};

const REPLICA_ID: u8 = 7;
const CLIENT_ID: u128 = 0x4242_4242_4242_4242_4242_4242_4242_4242;

/// Transport whose `run` body panics on first poll. Models a transport
/// that explodes after a partial decode, an assertion failure, or any
/// other unexpected branch the dispatcher does not catch.
struct PanickingConn;

impl TransportConn for PanickingConn {
    #[allow(clippy::future_not_send)]
    async fn run(self, _ctx: ActorContext) {
        panic!("intentional panic from PanickingConn::run");
    }
}

/// Transport whose `run` body parks until shutdown. Lets the test prove
/// a fresh install on the cleaned-up registry slot succeeds and stays
/// registered until tear-down.
struct ParkingConn;

impl TransportConn for ParkingConn {
    #[allow(clippy::future_not_send)]
    async fn run(self, ctx: ActorContext) {
        ctx.shutdown.wait().await;
    }
}

#[allow(clippy::future_not_send)]
async fn wait_until<F: Fn() -> bool>(deadline: Duration, label: &str, predicate: F) {
    let until = Instant::now() + deadline;
    while !predicate() {
        assert!(
            Instant::now() < until,
            "predicate '{label}' did not hold within {deadline:?}",
        );
        compio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[compio::test]
#[allow(clippy::future_not_send)]
async fn replica_panic_evicts_registry_and_notifies_loss() {
    let bus = Rc::new(IggyMessageBus::new(0));

    let lost_count: Rc<Cell<u32>> = Rc::new(Cell::new(0));
    let lost_peer: Rc<Cell<Option<u8>>> = Rc::new(Cell::new(None));
    {
        let lost_count = Rc::clone(&lost_count);
        let lost_peer = Rc::clone(&lost_peer);
        bus.set_connection_lost_fn(Rc::new(move |peer_id| {
            lost_count.set(lost_count.get() + 1);
            lost_peer.set(Some(peer_id));
        }));
    }

    let on_message: MessageHandler = Rc::new(|_, _| {});

    install_replica_conn(&bus, REPLICA_ID, PanickingConn, on_message.clone());

    {
        let bus = Rc::clone(&bus);
        wait_until(
            Duration::from_secs(2),
            "panicked replica slot evicted",
            move || !bus.replicas().contains(REPLICA_ID),
        )
        .await;
    }
    {
        let lost_count = Rc::clone(&lost_count);
        wait_until(
            Duration::from_secs(2),
            "notify_connection_lost fired",
            move || lost_count.get() >= 1,
        )
        .await;
    }
    assert_eq!(
        lost_count.get(),
        1,
        "notify_connection_lost must fire exactly once on panic",
    );
    assert_eq!(lost_peer.get(), Some(REPLICA_ID));

    install_replica_conn(&bus, REPLICA_ID, ParkingConn, on_message);
    {
        let bus = Rc::clone(&bus);
        wait_until(
            Duration::from_secs(2),
            "fresh install registers on cleaned slot",
            move || bus.replicas().contains(REPLICA_ID),
        )
        .await;
    }

    let outcome = bus.shutdown(Duration::from_secs(2)).await;
    assert_eq!(
        outcome.force, 0,
        "ParkingConn must wake on shutdown without forced cancel",
    );
}

#[compio::test]
#[allow(clippy::future_not_send)]
async fn client_panic_evicts_registry_slot() {
    let bus = Rc::new(IggyMessageBus::new(0));
    let on_request: RequestHandler = Rc::new(|_, _| {});

    install_client_conn(&bus, CLIENT_ID, PanickingConn, on_request.clone());
    {
        let bus = Rc::clone(&bus);
        wait_until(
            Duration::from_secs(2),
            "panicked client slot evicted",
            move || !bus.clients().contains(CLIENT_ID),
        )
        .await;
    }

    install_client_conn(&bus, CLIENT_ID, ParkingConn, on_request);
    {
        let bus = Rc::clone(&bus);
        wait_until(
            Duration::from_secs(2),
            "fresh install registers on cleaned slot",
            move || bus.clients().contains(CLIENT_ID),
        )
        .await;
    }

    let outcome = bus.shutdown(Duration::from_secs(2)).await;
    assert_eq!(
        outcome.force, 0,
        "ParkingConn must wake on shutdown without forced cancel",
    );
}

#[compio::test]
#[allow(clippy::future_not_send)]
async fn replica_panic_only_does_not_double_notify_after_dispatch_loop() {
    // Sanity: a panicking transport plus the dispatch task's separate
    // post-loop cleanup must still produce exactly one
    // `notify_connection_lost`. Pre-fix, the scopeguard wrapping the
    // transport's run path was missing entirely on the panic branch, so
    // only the dispatch task fired - or in the panic-after-insert race,
    // neither fired. With the fix, the shared `notified` cell still
    // dedups them.
    let bus = Rc::new(IggyMessageBus::new(0));

    let lost_count: Rc<Cell<u32>> = Rc::new(Cell::new(0));
    {
        let lost_count = Rc::clone(&lost_count);
        bus.set_connection_lost_fn(Rc::new(move |_peer_id| {
            lost_count.set(lost_count.get() + 1);
        }));
    }

    let on_message: MessageHandler = Rc::new(|_, _| {});
    install_replica_conn(&bus, REPLICA_ID, PanickingConn, on_message);

    {
        let lost_count = Rc::clone(&lost_count);
        wait_until(
            Duration::from_secs(2),
            "first notify_connection_lost",
            move || lost_count.get() >= 1,
        )
        .await;
    }

    // Both halves should have observed the abnormal close and run their
    // post-loop cleanup by now; give the second half a grace window so
    // any extra notify shows up before we assert.
    compio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        lost_count.get(),
        1,
        "exactly one notify_connection_lost per panicked install",
    );

    let _ = bus.shutdown(Duration::from_secs(2)).await;
}
