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

//! Replica plane install paths. TCP only by design.

use super::common::drain_rejected_registration;
use crate::IggyMessageBus;
use crate::lifecycle::{FusedShutdown, InstanceToken, Shutdown, ShutdownToken};
use crate::replica::listener::MessageHandler;
use crate::socket_opts::apply_nodelay_for_connection;
use crate::transports::tcp::TcpTransportConn;
use crate::transports::{ActorContext, TransportConn};
use async_channel::Receiver;
use compio::net::TcpStream;
use futures::FutureExt;
use iggy_binary_protocol::{GenericHeader, Message};
use std::cell::Cell;
use std::rc::Rc;
use tracing::{debug, info, warn};

/// TCP entry point: apply `TCP_NODELAY` on the raw stream and delegate
/// to the transport-generic install path.
///
/// Linux does not propagate `TCP_NODELAY` from the listener to the
/// accepted fd, so we toggle it here on every installed stream. A miss
/// means we stay Nagle-on for this peer, not a failure. Liveness is NOT
/// detected via `SO_KEEPALIVE` here — the replica plane observes peer
/// death via VSR heartbeats.
#[allow(clippy::future_not_send)]
pub fn install_replica_tcp(
    bus: &Rc<IggyMessageBus>,
    peer_id: u8,
    stream: TcpStream,
    on_message: MessageHandler,
) {
    if let Err(e) = apply_nodelay_for_connection(&stream) {
        warn!(replica = peer_id, "nodelay failed on delegated fd: {e}");
    }
    install_replica_conn(bus, peer_id, TcpTransportConn::new(stream), on_message);
}

/// Install a pre-wrapped replica connection on the bus.
///
/// Generic over [`TransportConn`] so alternate transports (WS via
/// shard-0 TLS terminator, QUIC via `compio-quic`) plug in behind the
/// same registry-insert + instance-token fencing + install-race
/// handling. TCP-specific socket options live in
/// [`install_replica_tcp`]; transports with no equivalent layer call
/// this entry directly with their already-configured connection.
#[allow(clippy::future_not_send, clippy::too_many_lines)]
pub fn install_replica_conn<C: TransportConn>(
    bus: &Rc<IggyMessageBus>,
    peer_id: u8,
    conn: C,
    on_message: MessageHandler,
) {
    if bus.replicas().contains(peer_id) {
        debug!(
            replica = peer_id,
            "replica already registered on this shard, dropping delegated fd"
        );
        drop(conn);
        return;
    }

    let (tx, rx) = async_channel::bounded(bus.peer_queue_capacity());
    let (in_tx, in_rx) =
        async_channel::bounded::<Message<GenericHeader>>(bus.peer_queue_capacity());

    // Writer and reader both observe abnormal close and used to fire
    // `notify_connection_lost` twice per disconnect, causing shard 0 to
    // broadcast two `ReplicaMappingClear` rounds and churn the mapping.
    // Shared one-shot guard: whichever post-loop runs first wins.
    let notified = Rc::new(Cell::new(false));
    // If the registry insert below races with a concurrent install for
    // the same peer id and loses, both spawned halves must skip their
    // post-loop cleanup: the loser's `replicas().remove` /
    // `close_peer_if_token_matches` calls would no-op against the winner's
    // generation token (so they can't evict the live entry), but
    // `notify_connection_lost` has no token guard and would still broadcast
    // a spurious mapping-clear round. `compio::runtime::JoinHandle::drop`
    // does not cancel the spawned task, so we have to tell the tasks to
    // stand down in-band.
    let install_aborted = Rc::new(Cell::new(false));

    // Generation token published by the registry on a successful insert.
    // Writer and reader post-loops release the slot only when the stored
    // token matches; a stale-install exit that wakes up after a later
    // reinstall would otherwise evict the new slot.
    let install_token: Rc<Cell<Option<InstanceToken>>> = Rc::new(Cell::new(None));

    // Per-connection shutdown used to kick the transport off its
    // `io_uring` read SQE when the registry insert below loses a race.
    // The bus-wide token cannot be triggered here (it would tear down
    // every other connection); closing the bus-side outbound sender
    // also does not reach a reader blocked on
    // [`crate::framing::read_message`]. The `Shutdown` is moved into
    // the registry entry on success so its `Sender` survives the
    // connection's lifetime: dropping the `Shutdown` would close the
    // broadcast channel and falsely wake the `ShutdownToken`'s
    // listeners. On insert race the loser receives the `Shutdown` back
    // via `RejectedRegistration` and triggers it before draining the
    // orphan tasks.
    let (conn_shutdown, conn_token) = Shutdown::new();

    // The transport observes a single fused shutdown signal in its
    // `ActorContext`. `FusedShutdown::wait` resolves on either the
    // bus-wide token or the per-connection token, with no spawned bridge
    // task and no third broadcast channel: the merge is folded into the
    // await site. The per-connection `Shutdown` (the producer half) is
    // moved into the registry entry on success so its `Sender` survives
    // the connection's lifetime and is triggered explicitly by the
    // insert-race rejection path.
    let transport_shutdown = FusedShutdown::new(bus.token(), conn_token.clone());

    let label: &'static str = "replica";
    let peer_fmt = format!("{peer_id}");
    let ctx = ActorContext {
        in_tx,
        rx,
        shutdown: transport_shutdown,
        conn_shutdown: conn_shutdown.clone(),
        max_batch: bus.config().max_batch,
        max_message_size: bus.config().max_message_size,
        label,
        peer: peer_fmt,
    };

    let bus_for_transport = Rc::clone(bus);
    let aborted_transport = Rc::clone(&install_aborted);
    let token_for_transport = Rc::clone(&install_token);
    let notified_transport = Rc::clone(&notified);
    let transport_handle = compio::runtime::spawn(async move {
        // Scopeguard so registry eviction + connection-lost notification fire
        // on PANIC as well as clean exit. compio's `spawn` wraps the future
        // with `AssertUnwindSafe(future).catch_unwind()` which silently
        // swallows panics; without this guard a panicking transport would
        // leak its registry slot and `notify_connection_lost` would never
        // fire, leaving the bus convinced the peer is still up until a
        // higher-layer timeout fallback notices.
        // Order-independent vs the dispatch-side scopeguard: `notified`
        // is a single shared `Cell<bool>` whose `replace(true)` runs at
        // most one `notify_connection_lost` regardless of which guard
        // unwinds first. Per F13 retraction.
        let _cleanup = scopeguard::guard((), |()| {
            if aborted_transport.get() || bus_for_transport.is_shutting_down() {
                return;
            }
            let Some(token) = token_for_transport.get() else {
                return;
            };
            if !bus_for_transport
                .replicas()
                .remove_if_token_matches(peer_id, token)
            {
                return;
            }
            if !notified_transport.replace(true) {
                bus_for_transport.notify_connection_lost(peer_id);
            }
        });
        conn.run(ctx).await;
    });

    let bus_for_dispatch = Rc::clone(bus);
    let bus_for_dispatch_guard = Rc::clone(bus);
    let bus_token_dispatch = bus.token();
    let bus_token_dispatch_guard = bus.token();
    let conn_token_dispatch = conn_token;
    let aborted_dispatch = Rc::clone(&install_aborted);
    let aborted_dispatch_guard = Rc::clone(&install_aborted);
    let token_for_dispatch = Rc::clone(&install_token);
    let token_for_dispatch_guard = Rc::clone(&install_token);
    let notified_dispatch = Rc::clone(&notified);
    let notified_dispatch_guard = Rc::clone(&notified);
    let close_peer_timeout = bus.config().close_peer_timeout;
    let dispatch_handle = compio::runtime::spawn(async move {
        // Scopeguard mirroring the transport-side cleanup so a panic
        // inside the dispatch loop (most likely from a panicking
        // `on_message` handler) still evicts the registry slot and fires
        // `notify_connection_lost`. compio's `spawn` wraps the future in
        // `AssertUnwindSafe(future).catch_unwind()` which silently
        // swallows panics; without this guard the bus would believe the
        // peer is still up until the transport notices a half-open
        // socket (peer EOF) or the bus-wide shutdown fires.
        //
        // Sync subset only: drop the registry entry (its stored
        // `Shutdown` drops with it, closing the per-connection broadcast
        // channel and waking the transport's `FusedShutdown.wait()`)
        // plus `notify_connection_lost`. The async drain
        // (`close_peer_if_token_matches`, which awaits the writer handle
        // under `close_peer_timeout`) runs only on clean exit below;
        // when both paths run, the second observes the registry slot
        // already gone and the `notified` flag already set, so it
        // becomes a no-op.
        // Order-independent vs the transport-side scopeguard: see the
        // matching note above; `notified.replace(true)` dedups across
        // both paths regardless of unwind order.
        let _dispatch_cleanup = scopeguard::guard((), |()| {
            if aborted_dispatch_guard.get() || bus_token_dispatch_guard.is_triggered() {
                return;
            }
            let Some(token) = token_for_dispatch_guard.get() else {
                return;
            };
            if bus_for_dispatch_guard
                .replicas()
                .remove_if_token_matches(peer_id, token)
                && !notified_dispatch_guard.replace(true)
            {
                bus_for_dispatch_guard.notify_connection_lost(peer_id);
            }
        });

        replica_dispatch_loop(
            peer_id,
            in_rx,
            &on_message,
            &bus_token_dispatch,
            &conn_token_dispatch,
            &aborted_dispatch,
        )
        .await;
        if aborted_dispatch.get() {
            debug!(
                replica = peer_id,
                "aborted replica install: skipping post-loop cleanup"
            );
            return;
        }
        if !bus_token_dispatch.is_triggered() {
            let Some(token) = token_for_dispatch.get() else {
                return;
            };
            let closed = bus_for_dispatch
                .replicas()
                .close_peer_if_token_matches(peer_id, token, close_peer_timeout)
                .await;
            if closed && !notified_dispatch.replace(true) {
                bus_for_dispatch.notify_connection_lost(peer_id);
            }
        }
        info!(replica = peer_id, "peer replica disconnected");
    });

    match bus.replicas().insert(
        peer_id,
        tx,
        transport_handle,
        dispatch_handle,
        conn_shutdown,
    ) {
        Ok(token) => {
            install_token.set(Some(token));
        }
        Err(rejected) => {
            // Tell both halves to stand down: the winner's entry is live and
            // must not be touched by this losing install.
            install_aborted.set(true);
            warn!(replica = peer_id, "replica registry insert raced");
            // `drain_rejected_registration` triggers the per-connection
            // shutdown (returned with `rejected`) to wake the transport
            // off its `io_uring` read SQE, then awaits transport /
            // dispatch handles. `compio::runtime::JoinHandle::drop` only
            // detaches, so without this drain the reader would outlive
            // the race on a half-open socket until peer EOF. Hand the
            // handle to `track_background` so `IggyMessageBus::shutdown`
            // awaits the drain before returning - `.detach()` would
            // orphan it and leak the half-closed socket across shutdown.
            let drain_handle =
                compio::runtime::spawn(drain_rejected_registration(rejected, close_peer_timeout));
            bus.track_background(drain_handle);
        }
    }
}

/// Dispatch loop for a delegated replica connection. Pulls inbound
/// consensus messages from the transport's per-connection inbound
/// channel and hands each off to the bus-installed handler.
///
/// `aborted` is set by the installer when the registry insert loses a
/// duplicate-replica-id race. The loop checks it before dispatching
/// each message so the losing dispatcher can never invoke `on_message`
/// with the replica id owned by the winning install — otherwise two
/// physical peers would feed the same VSR slot and break replication
/// safety.
///
/// `conn_token` is a per-connection shutdown the installer triggers
/// from the insert-race path; alongside the bus-wide token, both wake
/// this loop without waiting for the transport to drop its
/// `Sender<Message>`.
#[allow(clippy::future_not_send)]
async fn replica_dispatch_loop(
    replica_id: u8,
    in_rx: Receiver<Message<GenericHeader>>,
    on_message: &MessageHandler,
    token: &ShutdownToken,
    conn_token: &ShutdownToken,
    aborted: &Cell<bool>,
) {
    loop {
        futures::select! {
            () = token.wait().fuse() => {
                debug!(replica = replica_id, "replica dispatch loop shutting down");
                return;
            }
            () = conn_token.wait().fuse() => {
                debug!(replica = replica_id, "replica dispatch loop aborted by per-connection shutdown");
                return;
            }
            result = in_rx.recv().fuse() => {
                let Ok(msg) = result else {
                    debug!(replica = replica_id, "replica dispatch: inbound queue closed");
                    return;
                };
                if aborted.get() {
                    return;
                }
                on_message(replica_id, msg);
            }
        }
    }
}
