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

use common::test_client_meta;
use message_bus::client_listener::RequestHandler;
use message_bus::installer::{install_client_conn, install_replica_conn};
use message_bus::replica::listener::MessageHandler;
use message_bus::transports::{ActorContext, TransportConn};
use message_bus::{ClientTransportKind, IggyMessageBus};
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

/// Transport that mirrors the production TCP / QUIC shape: a reader
/// parked on the fused shutdown plus a writer task that panics. The
/// writer wraps its scopeguard around `conn_shutdown.trigger()`, so
/// the panic must propagate the conn-side shutdown into the reader's
/// `FusedShutdown::wait`, which then exits and lets `run` return so
/// the installer's transport-side scopeguard can evict the registry
/// slot. Without the writer scopeguard, the reader would park
/// indefinitely on the live socket and the slot would leak until the
/// bus-wide token fires.
struct WriterPanicConn;

impl TransportConn for WriterPanicConn {
    #[allow(clippy::future_not_send)]
    async fn run(self, ctx: ActorContext) {
        let ActorContext {
            shutdown,
            conn_shutdown,
            ..
        } = ctx;
        let reader_shutdown = shutdown.clone();
        let reader = compio::runtime::spawn(async move { reader_shutdown.wait().await });
        let writer_conn_shutdown = conn_shutdown;
        let writer = compio::runtime::spawn(async move {
            let _wake_reader = scopeguard::guard(writer_conn_shutdown, |s| s.trigger());
            panic!("intentional panic from WriterPanicConn writer task");
        });
        let _ = writer.await;
        let _ = reader.await;
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
    // Canonical post-shutdown leak gate for the bus's background-task
    // tracking. `IggyMessageBus::shutdown` drains every handle in
    // `track_background`'s vec; a leaked task or a missing cleanup
    // path here surfaces as residue. Pinned in this representative
    // panic-cleanup test rather than every test variant because all
    // client transports funnel through `install_client_conn`, so the
    // shared cleanup invariant trips here first.
    assert_eq!(
        bus.background_tasks_len(),
        0,
        "background_tasks vec must be empty post-shutdown",
    );
}

#[compio::test]
#[allow(clippy::future_not_send)]
async fn client_panic_evicts_registry_slot() {
    let bus = Rc::new(IggyMessageBus::new(0));
    let on_request: RequestHandler = Rc::new(|_, _| {});

    install_client_conn(
        &bus,
        test_client_meta(CLIENT_ID, ClientTransportKind::Tcp),
        PanickingConn,
        on_request.clone(),
    );
    {
        let bus = Rc::clone(&bus);
        wait_until(
            Duration::from_secs(2),
            "panicked client slot evicted",
            move || !bus.clients().contains(CLIENT_ID),
        )
        .await;
    }

    install_client_conn(
        &bus,
        test_client_meta(CLIENT_ID, ClientTransportKind::Tcp),
        ParkingConn,
        on_request,
    );
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

#[compio::test]
#[allow(clippy::future_not_send)]
async fn replica_writer_panic_evicts_registry_via_conn_shutdown() {
    // F11 regression coverage: a panicking writer task on a live
    // transport must trigger the per-connection `Shutdown` so the
    // reader (parked on the fused shutdown signal) wakes, the
    // transport's `run` returns, and the installer's transport-side
    // scopeguard evicts the registry slot. Pre-fix the reader stayed
    // parked on the live socket and the slot leaked until the bus-wide
    // token fired.
    let bus = Rc::new(IggyMessageBus::new(0));

    let lost_count: Rc<Cell<u32>> = Rc::new(Cell::new(0));
    {
        let lost_count = Rc::clone(&lost_count);
        bus.set_connection_lost_fn(Rc::new(move |_peer_id| {
            lost_count.set(lost_count.get() + 1);
        }));
    }

    let on_message: MessageHandler = Rc::new(|_, _| {});
    install_replica_conn(&bus, REPLICA_ID, WriterPanicConn, on_message);

    {
        let bus = Rc::clone(&bus);
        wait_until(
            Duration::from_secs(2),
            "writer-panicked replica slot evicted",
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

    let _ = bus.shutdown(Duration::from_secs(2)).await;
}

#[compio::test]
#[allow(clippy::future_not_send)]
async fn client_writer_panic_evicts_registry_via_conn_shutdown() {
    // F11 regression coverage on the client install path. Mirror of
    // the replica variant.
    let bus = Rc::new(IggyMessageBus::new(0));
    let on_request: RequestHandler = Rc::new(|_, _| {});

    install_client_conn(
        &bus,
        test_client_meta(CLIENT_ID, ClientTransportKind::Tcp),
        WriterPanicConn,
        on_request,
    );

    {
        let bus = Rc::clone(&bus);
        wait_until(
            Duration::from_secs(2),
            "writer-panicked client slot evicted",
            move || !bus.clients().contains(CLIENT_ID),
        )
        .await;
    }

    let _ = bus.shutdown(Duration::from_secs(2)).await;
}
