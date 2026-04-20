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

//! Connection installer trait.
//!
//! Shard 0 accepts / dials all TCP connections and ships the duplicated fd
//! to the owning shard via the inter-shard `ShardFrame` channel. The owning
//! shard's router handler wraps the fd on its own compio runtime and
//! registers the connection on its local bus. This trait exposes that
//! registration surface in a way the shard layer can call without knowing
//! the concrete bus type.

use crate::client_listener::RequestHandler;
use crate::fd_transfer;
use crate::framing;
use crate::replica_listener::MessageHandler;
use crate::socket_opts::apply_keepalive_for_connection;
use crate::{IggyMessageBus, lifecycle::ShutdownToken};
use compio::net::{OwnedReadHalf, TcpStream};
use futures::FutureExt;
use iggy_binary_protocol::Command2;
use std::cell::Cell;
use std::os::unix::io::RawFd;
use std::rc::Rc;
use tracing::{debug, info, warn};

/// Operations a shard needs to perform on its local bus when the router
/// receives an inter-shard connection-setup or mapping frame.
///
/// The production implementation is on `Rc<IggyMessageBus>`. The simulator
/// does not exercise this path; if it ever does, add a no-op impl on
/// `SharedSimOutbox`.
pub trait ConnectionInstaller {
    /// Wrap a duplicated TCP fd into a `TcpStream` on the local compio
    /// runtime, spawn writer + reader tasks, and register the replica
    /// connection on this shard.
    ///
    /// Takes ownership of `fd`. On registration failure the fd is closed
    /// via `close_fd` (by dropping the wrapping `TcpStream`).
    fn install_replica_fd(&self, fd: RawFd, replica_id: u8, on_message: MessageHandler);

    /// Same for an SDK client connection. The owning shard is already
    /// encoded in the top 16 bits of `client_id`.
    fn install_client_fd(&self, fd: RawFd, client_id: u128, on_request: RequestHandler);

    /// Update the replica -> owning shard mapping used by the `send_to_replica`
    /// slow path on non-owning shards.
    fn set_shard_mapping(&self, replica: u8, owning_shard: u16);

    /// Forget the replica -> owning shard mapping (e.g. after a connection
    /// loss, before the next allocate).
    fn remove_shard_mapping(&self, replica: u8);
}

impl ConnectionInstaller for Rc<IggyMessageBus> {
    fn install_replica_fd(&self, fd: RawFd, replica_id: u8, on_message: MessageHandler) {
        // SAFETY: Shard 0 duped the fd via `dup(2)` and sent it to this
        // shard via the inter-shard channel. Ownership is being transferred
        // to us; no other TcpStream holds it.
        let stream = unsafe { fd_transfer::wrap_duped_fd(fd) };
        install_replica_stream(self, replica_id, stream, on_message);
    }

    fn install_client_fd(&self, fd: RawFd, client_id: u128, on_request: RequestHandler) {
        // SAFETY: see install_replica_fd.
        let stream = unsafe { fd_transfer::wrap_duped_fd(fd) };
        install_client_stream(self, client_id, stream, on_request);
    }

    fn set_shard_mapping(&self, replica: u8, owning_shard: u16) {
        IggyMessageBus::set_shard_mapping(self, replica, owning_shard);
    }

    fn remove_shard_mapping(&self, replica: u8) {
        IggyMessageBus::remove_shard_mapping(self, replica);
    }
}

/// Install a pre-wrapped replica TCP stream on the bus. Shared by
/// `install_replica_fd` and, once shard 0 acts as the delegate target for
/// itself, the accept path.
#[allow(clippy::future_not_send)] // single-threaded compio
pub fn install_replica_stream(
    bus: &Rc<IggyMessageBus>,
    peer_id: u8,
    stream: TcpStream,
    on_message: MessageHandler,
) {
    if bus.replicas().contains(peer_id) {
        debug!(
            replica = peer_id,
            "replica already registered on this shard, dropping delegated fd"
        );
        drop(stream);
        return;
    }

    let cfg = bus.config();
    if let Err(e) = apply_keepalive_for_connection(
        &stream,
        cfg.keepalive_idle,
        cfg.keepalive_interval,
        cfg.keepalive_retries,
    ) {
        warn!(replica = peer_id, "keepalive failed on delegated fd: {e}");
    }

    let (read_half, write_half) = stream.into_split();
    let (tx, rx) = async_channel::bounded(bus.peer_queue_capacity());

    // Writer and reader both observe abnormal close and used to fire
    // `notify_connection_lost` twice per disconnect, causing shard 0 to
    // broadcast two `ReplicaMappingClear` rounds and churn the mapping.
    // Shared one-shot guard: whichever half exits first wins.
    let notified = Rc::new(Cell::new(false));
    // If the registry insert below races with a concurrent install for
    // the same peer id and loses, both spawned halves must skip their
    // post-loop cleanup: calling `replicas().remove` / `close_peer` or
    // `notify_connection_lost` would evict the winner's entry and
    // clobber its mapping broadcast. `compio::runtime::JoinHandle::drop`
    // does not cancel the spawned task, so we have to tell the tasks to
    // stand down in-band.
    let install_aborted = Rc::new(Cell::new(false));

    let writer_token = bus.token();
    let bus_for_writer = Rc::clone(bus);
    let writer_label = format!("{peer_id}");
    let notified_writer = Rc::clone(&notified);
    let aborted_writer = Rc::clone(&install_aborted);
    let max_batch = bus.config().max_batch;
    let writer_handle = compio::runtime::spawn(async move {
        crate::writer_task::run(
            rx,
            write_half,
            writer_token,
            "replica",
            writer_label,
            max_batch,
        )
        .await;
        if aborted_writer.get() || bus_for_writer.is_shutting_down() {
            return;
        }
        bus_for_writer.replicas().remove(peer_id);
        if !notified_writer.replace(true) {
            bus_for_writer.notify_connection_lost(peer_id);
        }
    });

    let bus_for_reader = Rc::clone(bus);
    let read_token = bus.token();
    let notified_reader = Rc::clone(&notified);
    let aborted_reader = Rc::clone(&install_aborted);
    let close_peer_timeout = bus.config().close_peer_timeout;
    let max_message_size = bus.config().max_message_size;
    let reader_handle = compio::runtime::spawn(async move {
        replica_read_loop(
            peer_id,
            read_half,
            &on_message,
            &read_token,
            &aborted_reader,
            max_message_size,
        )
        .await;
        if aborted_reader.get() {
            debug!(
                replica = peer_id,
                "aborted replica install: skipping post-loop cleanup"
            );
            return;
        }
        if !read_token.is_triggered() {
            bus_for_reader
                .replicas()
                .close_peer(peer_id, close_peer_timeout)
                .await;
            if !notified_reader.replace(true) {
                bus_for_reader.notify_connection_lost(peer_id);
            }
        }
        info!(replica = peer_id, "peer replica disconnected");
    });

    if bus
        .replicas()
        .insert(peer_id, tx, writer_handle, reader_handle)
        .is_err()
    {
        // Tell both halves to stand down: the winner's entry is live and
        // must not be touched by this losing install. Dropping the
        // `async_channel::Sender` (which `insert` already did on error)
        // will wake the writer with `Err(Closed)`; the writer then drops
        // its `OwnedWriteHalf`, the peer sees EOF, and the reader's
        // `read_message` returns with an error. Both halves exit and hit
        // their `install_aborted` check.
        install_aborted.set(true);
        warn!(replica = peer_id, "replica registry insert raced");
    }
}

/// Install a pre-wrapped client TCP stream on the bus.
#[allow(clippy::future_not_send)] // single-threaded compio
pub fn install_client_stream(
    bus: &Rc<IggyMessageBus>,
    client_id: u128,
    stream: TcpStream,
    on_request: RequestHandler,
) {
    let cfg = bus.config();
    if let Err(e) = apply_keepalive_for_connection(
        &stream,
        cfg.keepalive_idle,
        cfg.keepalive_interval,
        cfg.keepalive_retries,
    ) {
        warn!(
            client = client_id,
            "keepalive failed on delegated client fd: {e}"
        );
    }

    let (read_half, write_half) = stream.into_split();
    let (tx, rx) = async_channel::bounded(bus.peer_queue_capacity());

    // If the registry insert below loses a race for `client_id`, the
    // losing reader must NOT invoke `on_request` (it would route
    // responses through the wrong registry entry) and must NOT call
    // `close_peer` (it would evict the winner). See the replica path
    // above for the same pattern.
    let install_aborted = Rc::new(Cell::new(false));

    let writer_token = bus.token();
    let bus_for_writer = Rc::clone(bus);
    let writer_label = format!("{client_id:#034x}");
    let aborted_writer = Rc::clone(&install_aborted);
    let max_batch = bus.config().max_batch;
    let writer_handle = compio::runtime::spawn(async move {
        crate::writer_task::run(
            rx,
            write_half,
            writer_token,
            "client",
            writer_label,
            max_batch,
        )
        .await;
        if aborted_writer.get() || bus_for_writer.is_shutting_down() {
            return;
        }
        bus_for_writer.clients().remove(client_id);
    });

    let bus_for_reader = Rc::clone(bus);
    let read_token = bus.token();
    let aborted_reader = Rc::clone(&install_aborted);
    let close_peer_timeout = bus.config().close_peer_timeout;
    let max_message_size = bus.config().max_message_size;
    let reader_handle = compio::runtime::spawn(async move {
        client_read_loop(
            client_id,
            read_half,
            &on_request,
            &read_token,
            &aborted_reader,
            max_message_size,
        )
        .await;
        if aborted_reader.get() {
            debug!(
                client = client_id,
                "aborted client install: skipping post-loop cleanup"
            );
            return;
        }
        if !read_token.is_triggered() {
            bus_for_reader
                .clients()
                .close_peer(client_id, close_peer_timeout)
                .await;
        }
        info!(client = client_id, "consensus client disconnected");
    });

    if bus
        .clients()
        .insert(client_id, tx, writer_handle, reader_handle)
        .is_err()
    {
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
    }
}

/// Read loop for a delegated replica connection. Identical to the
/// `replica_listener` version but kept here to avoid cross-module coupling.
///
/// `aborted` is set by the installer when the registry insert loses a
/// duplicate-replica-id race. The loop checks it before dispatching each
/// message so the losing reader can never invoke `on_message` with the
/// replica id owned by the winning install — otherwise two physical peers
/// would feed the same VSR slot and break replication safety.
#[allow(clippy::future_not_send)]
async fn replica_read_loop(
    replica_id: u8,
    mut read_half: OwnedReadHalf<TcpStream>,
    on_message: &MessageHandler,
    token: &ShutdownToken,
    aborted: &Cell<bool>,
    max_message_size: usize,
) {
    loop {
        futures::select! {
            () = token.wait().fuse() => {
                debug!(replica = replica_id, "replica read loop shutting down");
                return;
            }
            result = framing::read_message(&mut read_half, max_message_size).fuse() => {
                match result {
                    Ok(msg) => {
                        if aborted.get() {
                            return;
                        }
                        on_message(replica_id, msg);
                    }
                    Err(e) => {
                        debug!(replica = replica_id, "read error: {e}");
                        return;
                    }
                }
            }
        }
    }
}

/// Read loop for a delegated client connection. Rejects any command other
/// than `Request` (the client side of the consensus protocol only speaks
/// request/reply).
///
/// `aborted` is set by the installer when the registry insert loses a
/// duplicate-client-id race. The loop checks it before dispatching each
/// request so the losing reader can never invoke `on_request` with the
/// client id owned by the winning install.
#[allow(clippy::future_not_send)]
async fn client_read_loop(
    client_id: u128,
    mut read_half: OwnedReadHalf<TcpStream>,
    on_request: &RequestHandler,
    token: &ShutdownToken,
    aborted: &Cell<bool>,
    max_message_size: usize,
) {
    loop {
        futures::select! {
            () = token.wait().fuse() => {
                debug!(client = client_id, "client read loop shutting down");
                return;
            }
            result = framing::read_message(&mut read_half, max_message_size).fuse() => {
                match result {
                    Ok(msg) => {
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
                    Err(e) => {
                        debug!(client = client_id, "client read error: {e}");
                        return;
                    }
                }
            }
        }
    }
}
