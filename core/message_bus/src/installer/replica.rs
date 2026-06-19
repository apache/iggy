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
use crate::lifecycle::{FusedShutdown, InstanceToken, Shutdown, ShutdownToken};
use crate::replica::auth::{ChannelBinding, EXPORTER_LABEL, EXPORTER_LEN};
use crate::replica::handshake::{self, ReplicaHandshakeCtx};
use crate::replica::listener::MessageHandler;
use crate::socket_opts::apply_nodelay_for_connection;
use crate::transports::tcp::TcpTransportConn;
use crate::transports::tcp_tls::TcpTlsTransportConn;
use crate::transports::{ActorContext, TransportConn};
use crate::{IggyMessageBus, ReplicaHandshakeDoneFn};
use async_channel::Receiver;
use compio::io::compat::AsyncStream;
use compio::io::{AsyncRead, AsyncWrite};
use compio::net::TcpStream;
use compio::tls::TlsStream;
use futures::FutureExt;
use iggy_binary_protocol::GenericHeader;
use server_common::Message;
use std::cell::Cell;
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Shared scaffold for both delegated install directions: shutdown
/// gate, handshake ctx fetch, `TCP_NODELAY`, then the direction's
/// handshake future (built by `make`) spawned under one
/// `handshake_grace` budget. Fires `on_done` (the shard-0 outcome ack)
/// on every exit path. The handshake runs in its own spawned task so a
/// slow or hostile peer never stalls the shard's frame pump; handshake
/// failures are logged at their cause site, only the grace timeout is
/// logged here.
///
/// The shutdown gate skips handshake work during teardown; a shutdown
/// that begins mid-handshake is caught again by the pre-install checks
/// in [`accept_and_install`] / [`dial_and_install`].
///
/// Linux does not propagate `TCP_NODELAY` from the listener to the
/// accepted fd, so we toggle it here on every installed stream. A miss
/// means we stay Nagle-on for this peer, not a failure. Liveness is NOT
/// detected via `SO_KEEPALIVE` here - the replica plane observes peer
/// death via VSR heartbeats.
fn spawn_replica_install<Fut>(
    bus: &Rc<IggyMessageBus>,
    stream: TcpStream,
    peer: String,
    on_done: ReplicaHandshakeDoneFn,
    make: impl FnOnce(Rc<IggyMessageBus>, ReplicaHandshakeCtx, TcpStream, String) -> Fut,
) where
    Fut: Future<Output = Result<(), ()>> + 'static,
{
    if bus.is_shutting_down() {
        on_done();
        return;
    }
    let Some(ctx) = bus.replica_handshake_ctx().cloned() else {
        warn!(
            peer = %peer,
            "dropping delegated replica connection: no handshake ctx installed"
        );
        on_done();
        return;
    };
    if let Err(e) = apply_nodelay_for_connection(&stream) {
        warn!(peer = %peer, "nodelay failed on delegated fd: {e}");
    }
    let grace = bus.config().handshake_grace;
    let fut = make(Rc::clone(bus), ctx, stream, peer.clone());
    let handle = compio::runtime::spawn(async move {
        if compio::time::timeout(grace, fut).await.is_err() {
            warn!(
                peer = %peer,
                grace = ?grace,
                "delegated replica handshake exceeded handshake_grace; closing connection"
            );
        }
        on_done();
    });
    bus.track_background(handle);
}

/// Owning-shard entry for a blind-delegated INBOUND replica connection.
///
/// When `cluster.tls` is configured, wraps the raw stream as a TLS
/// server first; the acceptor handshake then runs over the encrypted
/// stream. TLS wrap + replica handshake share ONE `handshake_grace`
/// budget, so a peer cannot stretch admission to two grace windows by
/// stalling between the steps.
///
/// Mirrors the WS-upgrade install path
/// (`ConnectionInstaller::install_client_ws_fd`). The peer id is unknown
/// until the handshake reads the `ReplicaHello`, hence no id parameter.
#[allow(clippy::future_not_send)]
pub fn install_replica_inbound(
    bus: &Rc<IggyMessageBus>,
    stream: TcpStream,
    on_message: MessageHandler,
    on_done: ReplicaHandshakeDoneFn,
) {
    let peer = stream
        .peer_addr()
        .map_or_else(|_| "unknown".to_string(), |addr| addr.to_string());
    spawn_replica_install(bus, stream, peer, on_done, |bus, ctx, stream, peer| {
        async move {
            match ctx.tls.clone() {
                None => {
                    accept_and_install(
                        &bus,
                        stream,
                        &ctx,
                        ChannelBinding::Plaintext,
                        TcpTransportConn::new,
                        on_message,
                        &peer,
                    )
                    .await
                }
                Some(tls) => {
                    // Handshake with futures-rustls directly (compio-tls's
                    // TlsAcceptor hides the rustls connection, so the
                    // exporter is unreachable through it), mirroring
                    // compio-tls's own adapter: compat-wrap, pin, accept,
                    // then convert into the compio TlsStream the pump runs
                    // on via compio-tls's public From impls.
                    let tls_stream = futures_rustls::TlsAcceptor::from(Arc::clone(&tls.server))
                        .accept(Box::pin(AsyncStream::new(stream)))
                        .await
                        .map_err(|e| warn!(peer = %peer, "replica TLS accept failed: {e}"))?;
                    let binding = exporter_binding(tls_stream.get_ref().1)
                        .map_err(|e| warn!(peer = %peer, "replica TLS exporter failed: {e}"))?;
                    let close_grace = bus.config().close_grace;
                    accept_and_install(
                        &bus,
                        TlsStream::from(tls_stream),
                        &ctx,
                        binding,
                        |tls_stream| {
                            TcpTlsTransportConn::from_established(tls_stream)
                                .with_close_grace(close_grace)
                        },
                        on_message,
                        &peer,
                    )
                    .await
                }
            }
        }
    });
}

/// Shared tail of the inbound path: run the acceptor handshake over
/// `stream` (plaintext or TLS) and install the connection built by
/// `make_conn` under the verified peer id.
#[allow(clippy::future_not_send)]
async fn accept_and_install<S, C, F>(
    bus: &Rc<IggyMessageBus>,
    mut stream: S,
    ctx: &ReplicaHandshakeCtx,
    binding: ChannelBinding,
    make_conn: F,
    on_message: MessageHandler,
    peer: &str,
) -> Result<(), ()>
where
    S: AsyncRead + AsyncWrite,
    C: TransportConn,
    F: FnOnce(S) -> C,
{
    let max_message_size = bus.config().max_message_size;
    let peer_id = handshake::acceptor_handshake(&mut stream, ctx, binding, max_message_size)
        .await
        .map_err(|e| warn!(peer = %peer, "delegated replica handshake failed: {e}"))?;
    if !bus.is_shutting_down() {
        install_replica_conn(bus, peer_id, make_conn(stream), on_message);
    }
    Ok(())
}

/// Derive the PSK channel binding from an established rustls session.
///
/// Both ends call this with the same label and empty context, so the
/// values agree exactly when the two PSK handshake halves run inside the
/// SAME TLS session; a relay MITM terminates two sessions and the MACs
/// fail. TLS 1.3 (the only version the replica plane negotiates) defines
/// the exporter for every session, so an error here is unexpected and
/// drops the connection.
fn exporter_binding<Data>(
    conn: &rustls::ConnectionCommon<Data>,
) -> Result<ChannelBinding, rustls::Error> {
    let exporter = conn.export_keying_material([0u8; EXPORTER_LEN], EXPORTER_LABEL, None)?;
    Ok(ChannelBinding::Tls(exporter))
}

/// Owning-shard entry for a delegated OUTBOUND replica connection: run
/// the dialer handshake under `handshake_grace` and install on success.
///
/// When `cluster.tls` is configured the dialer wraps the raw stream as a
/// TLS client first (SNI + certificate-verify name from the roster via
/// `ReplicaTlsCtx::peer_names`); TLS wrap + replica handshake share ONE
/// `handshake_grace` budget. The peer id is known upfront (configured
/// roster); failures are log-only - the shard-0 reconnect sweep redials
/// once the ack clears the pending entry.
#[allow(clippy::future_not_send)]
pub fn install_replica_outbound(
    bus: &Rc<IggyMessageBus>,
    peer_id: u8,
    stream: TcpStream,
    on_message: MessageHandler,
    on_done: ReplicaHandshakeDoneFn,
) {
    let peer = format!("replica {peer_id}");
    spawn_replica_install(bus, stream, peer, on_done, |bus, ctx, stream, peer| {
        async move {
            match ctx.tls.clone() {
                None => {
                    dial_and_install(
                        &bus,
                        stream,
                        &ctx,
                        peer_id,
                        ChannelBinding::Plaintext,
                        TcpTransportConn::new,
                        on_message,
                    )
                    .await
                }
                Some(tls) => {
                    let Some(peer_name) = tls.peer_names.get(usize::from(peer_id)) else {
                        warn!(
                            peer = %peer,
                            "no TLS peer name for replica id; dropping dialed connection"
                        );
                        return Err(());
                    };
                    // Direct futures-rustls handshake for exporter access;
                    // see the inbound twin for the rationale.
                    let tls_stream = futures_rustls::TlsConnector::from(Arc::clone(&tls.client))
                        .connect(peer_name.clone(), Box::pin(AsyncStream::new(stream)))
                        .await
                        .map_err(|e| warn!(peer = %peer, "replica TLS connect failed: {e}"))?;
                    let binding = exporter_binding(tls_stream.get_ref().1)
                        .map_err(|e| warn!(peer = %peer, "replica TLS exporter failed: {e}"))?;
                    let close_grace = bus.config().close_grace;
                    dial_and_install(
                        &bus,
                        TlsStream::from(tls_stream),
                        &ctx,
                        peer_id,
                        binding,
                        |tls_stream| {
                            TcpTlsTransportConn::from_established(tls_stream)
                                .with_close_grace(close_grace)
                        },
                        on_message,
                    )
                    .await
                }
            }
        }
    });
}

/// Shared tail of the outbound path: run the dialer handshake over
/// `stream` (plaintext or TLS) and install the connection built by
/// `make_conn` under the dialed peer id.
#[allow(clippy::future_not_send)]
async fn dial_and_install<S, C, F>(
    bus: &Rc<IggyMessageBus>,
    mut stream: S,
    ctx: &ReplicaHandshakeCtx,
    peer_id: u8,
    binding: ChannelBinding,
    make_conn: F,
    on_message: MessageHandler,
) -> Result<(), ()>
where
    S: AsyncRead + AsyncWrite,
    C: TransportConn,
    F: FnOnce(S) -> C,
{
    let max_message_size = bus.config().max_message_size;
    handshake::dialer_handshake(&mut stream, ctx, peer_id, binding, max_message_size).await?;
    if !bus.is_shutting_down() {
        install_replica_conn(bus, peer_id, make_conn(stream), on_message);
    }
    Ok(())
}

/// Install a pre-wrapped replica connection on the bus.
///
/// Generic over [`TransportConn`] so alternate transports (WS via
/// shard-0 TLS terminator, QUIC via `compio-quic`) plug in behind the
/// same registry-insert + instance-token fencing + install-race
/// handling. TCP-specific socket options live in the
/// `spawn_replica_install` scaffold; transports with no equivalent
/// layer call this entry directly with their already-configured
/// connection.
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

    // Atomically claim cross-shard ownership before spawning any task.
    // `mark_replica_owned` CAS-from-`OWNER_NONE` arbitrates parallel
    // inbound installs that target different shards: the loser shard
    // drops the fd here without ever touching the local registry, so
    // there is no orphan post-loop and no need for
    // `RejectedRegistration`-style drain on this path. The matching
    // local-`replicas().contains` check above handles same-shard
    // duplicates; same-shard reclaim during the previous install's
    // post-loop clear window is accepted by `try_claim`'s
    // `actual == shard_id` fallback (the live-registry guard in
    // `notify_connection_lost` prevents the stale post-loop from
    // clobbering the new entry).
    if !bus.mark_replica_owned(peer_id) {
        debug!(
            replica = peer_id,
            current_owner = ?bus.owning_shard(peer_id),
            "replica already owned by a different shard, dropping delegated fd"
        );
        drop(conn);
        return;
    }

    let (tx, rx) = async_channel::bounded(bus.peer_queue_capacity());
    let (in_tx, in_rx) =
        async_channel::bounded::<Message<GenericHeader>>(bus.peer_queue_capacity());

    // Writer and reader both observe abnormal close and used to fire
    // `notify_connection_lost` twice per disconnect, double-clearing the
    // owner-table slot and double-firing any installed test callback.
    // Shared one-shot guard: whichever post-loop runs first wins.
    let notified = Rc::new(Cell::new(false));
    // If the registry insert below races with a concurrent install for
    // the same peer id and loses, both spawned halves must skip their
    // post-loop cleanup: the loser's `replicas().remove` /
    // `close_peer_if_token_matches` calls would no-op against the winner's
    // generation token (so they can't evict the live entry), and
    // `notify_connection_lost` stands down whenever a live registry entry
    // exists - but the loser also drives `replica_dispatch_loop`, which
    // must never hand the winner's replica id to `on_message`.
    // `compio::runtime::JoinHandle::drop` does not cancel the spawned
    // task, so we have to tell the tasks to stand down in-band.
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
                bus_for_transport.notify_connection_lost(peer_id, "transport task exited");
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
                bus_for_dispatch_guard
                    .notify_connection_lost(peer_id, "dispatch task panicked or aborted");
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
                bus_for_dispatch.notify_connection_lost(
                    peer_id,
                    "dispatch loop ended (inbound channel closed)",
                );
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
            // Owner-table slot was already claimed at the top of this
            // function; nothing more to publish here. The matching
            // CAS-clear runs from `IggyMessageBus::notify_connection_lost`
            // on either of the post-loop guards firing.
            install_token.set(Some(token));
            let expected = bus.config().mesh_expected_peers;
            if expected > 0 {
                let connected = bus.owner_table().owned_count();
                if connected == expected {
                    info!(
                        expected,
                        "replica mesh complete: all peer connections established"
                    );
                } else {
                    info!(connected, expected, "replica mesh forming");
                }
            }
        }
        Err(rejected) => {
            // Defensive: on single-threaded compio there is no `.await`
            // between the contains-check above and this insert, so a
            // same-shard parallel install cannot interleave and this
            // branch is unreachable. Kept as belt-and-suspenders in case
            // a future refactor inserts an `.await` in the synchronous
            // setup block: we own the owner-table slot from the CAS at
            // the top, so unstamp it before draining the orphan to
            // avoid an owner-table entry with no live registry.
            install_aborted.set(true);
            let _ = bus.clear_replica_owned(peer_id);
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
