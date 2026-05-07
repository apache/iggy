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

//! TCP plaintext client install path + the transport-generic
//! `install_client_conn` reused by every other client transport
//! (TLS / WS / WSS / QUIC).

use super::common::drain_rejected_registration;
use super::conn_info::ClientConnMeta;
use crate::IggyMessageBus;
use crate::client_listener::RequestHandler;
use crate::lifecycle::{FusedShutdown, InstanceToken, Shutdown, ShutdownToken};
use crate::socket_opts::apply_nodelay_for_connection;
use crate::transports::tcp::TcpTransportConn;
use crate::transports::{ActorContext, TransportConn};
use async_channel::Receiver;
use compio::net::TcpStream;
use futures::FutureExt;
use iggy_binary_protocol::{Command2, GenericHeader, Message};
use std::cell::Cell;
use std::rc::Rc;
use tracing::{debug, info, warn};

/// TCP entry point for client installs. Applies socket options and
/// delegates to [`install_client_conn`]. See
/// [`super::replica::install_replica_tcp`] for the plane-symmetric docs.
#[allow(clippy::future_not_send)]
pub fn install_client_tcp(
    bus: &Rc<IggyMessageBus>,
    meta: ClientConnMeta,
    stream: TcpStream,
    on_request: RequestHandler,
) {
    let client_id = meta.client_id;
    if let Err(e) = apply_nodelay_for_connection(&stream) {
        warn!(
            client = client_id,
            "nodelay failed on delegated client fd: {e}"
        );
    }
    install_client_conn(bus, meta, TcpTransportConn::new(stream), on_request);
}

/// Install a pre-wrapped client connection on the bus. Generic over
/// [`TransportConn`]; plane-symmetric with
/// [`super::replica::install_replica_conn`].
#[allow(clippy::future_not_send, clippy::too_many_lines)]
pub fn install_client_conn<C: TransportConn>(
    bus: &Rc<IggyMessageBus>,
    meta: ClientConnMeta,
    conn: C,
    on_request: RequestHandler,
) {
    let client_id = meta.client_id;
    let (tx, rx) = async_channel::bounded(bus.peer_queue_capacity());
    let (in_tx, in_rx) =
        async_channel::bounded::<Message<GenericHeader>>(bus.peer_queue_capacity());

    // If the registry insert below loses a race for `client_id`, the
    // losing reader must NOT invoke `on_request` (it would route
    // responses through the wrong registry entry) and must NOT call
    // `close_peer` (it would evict the winner). See the replica path
    // for the same pattern.
    let install_aborted = Rc::new(Cell::new(false));

    // See replica path for the rationale on instance-token fencing.
    let install_token: Rc<Cell<Option<InstanceToken>>> = Rc::new(Cell::new(None));

    // Per-connection shutdown for fast reader wake on insert race; see
    // the replica installer for the full rationale.
    let (conn_shutdown, conn_token) = Shutdown::new();

    // Fuse bus-wide + per-connection shutdown directly at the await site:
    // the transport observes a single `FusedShutdown` whose `wait` resolves
    // on either source. No spawned bridge task, no third channel.
    let transport_shutdown = FusedShutdown::new(bus.token(), conn_token.clone());

    let label: &'static str = "client";
    let peer_fmt = format!("{client_id:#034x}");
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
    let transport_handle = compio::runtime::spawn(async move {
        // Scopeguard so the registry slot is evicted on PANIC as well as
        // clean exit. compio's `spawn` wraps the future with
        // `AssertUnwindSafe(future).catch_unwind()` which silently swallows
        // panics; without this guard a panicking client transport would
        // leak its slot and the next reconnect from the same client id
        // would be rejected as a duplicate.
        let _cleanup = scopeguard::guard((), |()| {
            if aborted_transport.get() || bus_for_transport.is_shutting_down() {
                return;
            }
            let Some(token) = token_for_transport.get() else {
                return;
            };
            bus_for_transport
                .clients()
                .remove_if_token_matches(client_id, token);
            bus_for_transport.remove_client_meta(client_id);
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
    let close_peer_timeout = bus.config().close_peer_timeout;
    let dispatch_handle = compio::runtime::spawn(async move {
        // Scopeguard mirroring the transport-side cleanup so a panic
        // inside the dispatch loop (most likely from a panicking
        // `on_request` handler) still evicts the registry slot and the
        // `client_meta` entry. compio's `spawn` wraps the future in
        // `AssertUnwindSafe(future).catch_unwind()` which silently
        // swallows panics; without this guard a panicking handler would
        // leave an orphan registry slot + `client_meta` until the
        // transport noticed a half-open socket.
        //
        // Sync subset only: drop the registry entry (its stored
        // `Shutdown` drops with it, closing the per-connection broadcast
        // channel and waking the transport's `FusedShutdown.wait()`),
        // and remove the `client_meta`. The async drain
        // (`close_peer_if_token_matches`, awaiting the writer handle
        // under `close_peer_timeout`) runs only on clean exit below;
        // when both paths run, the second observes the registry slot
        // already gone.
        let _dispatch_cleanup = scopeguard::guard((), |()| {
            if aborted_dispatch_guard.get() || bus_token_dispatch_guard.is_triggered() {
                return;
            }
            let Some(token) = token_for_dispatch_guard.get() else {
                return;
            };
            if bus_for_dispatch_guard
                .clients()
                .remove_if_token_matches(client_id, token)
            {
                bus_for_dispatch_guard.remove_client_meta(client_id);
            }
        });

        client_dispatch_loop(
            client_id,
            in_rx,
            &on_request,
            &bus_token_dispatch,
            &conn_token_dispatch,
            &aborted_dispatch,
        )
        .await;
        if aborted_dispatch.get() {
            debug!(
                client = client_id,
                "aborted client install: skipping post-loop cleanup"
            );
            return;
        }
        if !bus_token_dispatch.is_triggered() {
            let Some(token) = token_for_dispatch.get() else {
                return;
            };
            bus_for_dispatch
                .clients()
                .close_peer_if_token_matches(client_id, token, close_peer_timeout)
                .await;
        }
        info!(client = client_id, "consensus client disconnected");
    });

    match bus.clients().insert(
        client_id,
        tx,
        transport_handle,
        dispatch_handle,
        conn_shutdown,
    ) {
        Ok(token) => {
            install_token.set(Some(token));
            bus.insert_client_meta(Rc::new(meta));
        }
        Err(rejected) => {
            // Shard 0 mints client ids as `(target_shard << 112) | seq` with a
            // monotonic `seq` starting at 1, so wrap requires 2^112 mints and
            // a collision here is a bootstrap bug or a foreign id leaking
            // into the setup path. Flip `install_aborted` so the orphan
            // reader drops inbound frames instead of forwarding them via
            // `on_request` (which would route responses through the winner's
            // entry and silently misroute).
            install_aborted.set(true);
            warn!(
                client_id,
                "duplicate client id in registry, dropping delegated fd \
                 (shard 0 counter invariant violated)"
            );
            // See replica installer for the track_background rationale.
            let drain_handle =
                compio::runtime::spawn(drain_rejected_registration(rejected, close_peer_timeout));
            bus.track_background(drain_handle);
        }
    }
}

/// Dispatch loop for a delegated client connection. Rejects any command
/// other than `Request` (the client side of the consensus protocol only
/// speaks request/reply).
///
/// `aborted` is set by the installer when the registry insert loses a
/// duplicate-client-id race. The loop checks it before dispatching each
/// request so the losing dispatcher can never invoke `on_request` with
/// the client id owned by the winning install.
///
/// `conn_token` is a per-connection shutdown the installer triggers
/// from the insert-race path; alongside the bus-wide token, both wake
/// this loop without waiting for the transport to drop its
/// `Sender<Message>`.
#[allow(clippy::future_not_send)]
async fn client_dispatch_loop(
    client_id: u128,
    in_rx: Receiver<Message<GenericHeader>>,
    on_request: &RequestHandler,
    token: &ShutdownToken,
    conn_token: &ShutdownToken,
    aborted: &Cell<bool>,
) {
    loop {
        futures::select! {
            () = token.wait().fuse() => {
                debug!(client = client_id, "client dispatch loop shutting down");
                return;
            }
            () = conn_token.wait().fuse() => {
                debug!(client = client_id, "client dispatch loop aborted by per-connection shutdown");
                return;
            }
            result = in_rx.recv().fuse() => {
                let Ok(msg) = result else {
                    debug!(client = client_id, "client dispatch: inbound queue closed");
                    return;
                };
                if aborted.get() {
                    return;
                }
                let cmd = msg.header().command;
                if cmd != Command2::Request {
                    warn!(
                        client = client_id,
                        ?cmd,
                        "unexpected command from client, expected Request"
                    );
                    continue;
                }
                on_request(client_id, msg);
            }
        }
    }
}
