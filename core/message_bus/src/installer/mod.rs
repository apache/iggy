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

//! Connection installer trait + per-transport install paths.
//!
//! Shard 0 accepts / dials all TCP connections and ships the duplicated fd
//! to the owning shard via the inter-shard `ShardFrame` channel. The owning
//! shard's router handler wraps the fd on its own compio runtime and
//! registers the connection on its local bus. The [`ConnectionInstaller`]
//! trait exposes that registration surface in a way the shard layer can
//! call without knowing the concrete bus type.
//!
//! The per-transport `install_*` entry points live in the submodules
//! ([`replica`], [`tcp`], [`tcp_tls`], [`ws`], [`wss`], [`quic`]) and
//! are re-exported below so external call sites use stable
//! `crate::installer::install_*` paths.

mod common;

pub mod conn_info;
pub mod quic;
pub mod replica;
pub mod tcp;
pub mod tcp_tls;
pub mod ws;
pub mod wss;

pub use quic::install_client_quic;
pub use replica::{install_replica_conn, install_replica_inbound, install_replica_outbound};
pub use tcp::{install_client_conn, install_client_tcp};
pub use tcp_tls::install_client_tcp_tls;
pub use ws::install_client_ws;
pub use wss::install_client_wss;

use crate::client_listener::RequestHandler;
use crate::fd_transfer::{self, DupedFd};
use crate::installer::conn_info::ClientConnMeta;
use crate::replica::listener::MessageHandler;
use crate::{ClientConnectionLostFn, IggyMessageBus, ReplicaHandshakeDoneFn};
use std::rc::Rc;
use tracing::warn;

/// Operations a shard needs to perform on its local bus when the router
/// receives an inter-shard connection-setup frame.
///
/// The production implementation is on `Rc<IggyMessageBus>`. The simulator
/// does not exercise this path; if it ever does, add a no-op impl on
/// `SharedSimOutbox`.
pub trait ConnectionInstaller {
    /// Wrap a blind-delegated inbound replica fd into a `TcpStream` on
    /// the local compio runtime, run the acceptor handshake in a spawned
    /// task (bounded by `handshake_grace`), and register the connection
    /// on this shard on success. `on_done` is the shard-0 outcome ack;
    /// the install path fires it exactly once, on every exit path.
    ///
    /// Takes ownership of `fd`. On any failure the fd is closed by
    /// dropping the wrapping `TcpStream`.
    fn install_replica_inbound_fd(
        &self,
        fd: DupedFd,
        on_message: MessageHandler,
        on_done: ReplicaHandshakeDoneFn,
    );

    /// Same for a delegated outbound replica connection: runs the dialer
    /// handshake half toward the configured `replica_id`.
    fn install_replica_outbound_fd(
        &self,
        fd: DupedFd,
        replica_id: u8,
        on_message: MessageHandler,
        on_done: ReplicaHandshakeDoneFn,
    );

    /// Release a shard-0 in-flight inbound handshake slot. Routed here
    /// when shard 0's router processes the owning shard's
    /// handshake-outcome ack. Idempotent (the slot may have expired).
    fn release_replica_handshake_slot(&self, slot: u64);

    /// Clear a shard-0 pending-dial entry. Routed here when shard 0's
    /// router processes the owning shard's handshake-outcome ack.
    /// Idempotent (the entry may have expired).
    fn clear_replica_dial_pending(&self, replica_id: u8);

    /// Same for an SDK client connection. The owning shard is already
    /// encoded in the top 16 bits of `meta.client_id`. `meta` is stored
    /// on the bus and exposed via [`IggyMessageBus::client_meta`] for
    /// the lifetime of the connection.
    fn install_client_fd(&self, fd: DupedFd, meta: ClientConnMeta, on_request: RequestHandler);

    /// Same for an SDK WebSocket client's pre-upgrade TCP fd. The
    /// receiving shard wraps the fd, runs
    /// `compio_ws::accept_async_with_config` (configured by
    /// `MessageBusConfig::ws_config`) to drive the HTTP-Upgrade
    /// handshake, then installs WS reader / writer tasks via
    /// [`install_client_ws`] on success. On handshake failure
    /// the fd is closed by dropping the wrapping `TcpStream`. No
    /// subprotocol negotiation: the caller (server-ng) gates command
    /// access via the LOGIN allowlist.
    fn install_client_ws_fd(&self, fd: DupedFd, meta: ClientConnMeta, on_request: RequestHandler);

    /// Per-connection metadata stored at install time, or `None` if the client
    /// is not (or no longer) connected on this bus. Dispatch reads `peer_addr`
    /// + `transport` from it to seed the shard's session manager.
    fn client_meta(&self, client_id: u128) -> Option<Rc<ClientConnMeta>>;

    /// Register the callback fired when a client connection is lost, so
    /// dispatch can drive session removal + logout on disconnect.
    fn set_client_connection_lost_fn(&self, f: ClientConnectionLostFn);
}

impl ConnectionInstaller for Rc<IggyMessageBus> {
    fn install_replica_inbound_fd(
        &self,
        fd: DupedFd,
        on_message: MessageHandler,
        on_done: ReplicaHandshakeDoneFn,
    ) {
        let stream = fd_transfer::wrap_duped_fd(fd);
        install_replica_inbound(self, stream, on_message, on_done);
    }

    fn install_replica_outbound_fd(
        &self,
        fd: DupedFd,
        replica_id: u8,
        on_message: MessageHandler,
        on_done: ReplicaHandshakeDoneFn,
    ) {
        let stream = fd_transfer::wrap_duped_fd(fd);
        install_replica_outbound(self, replica_id, stream, on_message, on_done);
    }

    fn release_replica_handshake_slot(&self, slot: u64) {
        IggyMessageBus::release_replica_handshake_slot(self, slot);
    }

    fn clear_replica_dial_pending(&self, replica_id: u8) {
        self.clear_dial_pending(replica_id);
    }

    fn install_client_fd(&self, fd: DupedFd, meta: ClientConnMeta, on_request: RequestHandler) {
        let stream = fd_transfer::wrap_duped_fd(fd);
        install_client_tcp(self, meta, stream, on_request);
    }

    fn install_client_ws_fd(&self, fd: DupedFd, meta: ClientConnMeta, on_request: RequestHandler) {
        let stream = fd_transfer::wrap_duped_fd(fd);
        let bus = Self::clone(self);
        let cfg = bus.config();
        let ws_config = cfg.ws_config;
        let handshake_grace = cfg.handshake_grace;
        let handle = compio::runtime::spawn(async move {
            // compio-ws 0.4's `WebSocketStream` accept future is large
            // (~17 KB); box it to keep this spawned task's frame small.
            let outcome = compio::time::timeout(
                handshake_grace,
                Box::pin(compio_ws::accept_async_with_config(stream, ws_config)),
            )
            .await;
            match outcome {
                Ok(Ok(ws)) => {
                    if !bus.is_shutting_down() {
                        install_client_ws(&bus, meta, ws, on_request);
                    }
                }
                Ok(Err(e)) => {
                    warn!(client_id = meta.client_id, "WS upgrade failed: {e}");
                }
                Err(_elapsed) => {
                    warn!(
                        client_id = meta.client_id,
                        grace = ?handshake_grace,
                        "WS upgrade exceeded handshake_grace; closing connection"
                    );
                }
            }
        });
        self.track_background(handle);
    }

    fn client_meta(&self, client_id: u128) -> Option<Rc<ClientConnMeta>> {
        IggyMessageBus::client_meta(self, client_id)
    }

    fn set_client_connection_lost_fn(&self, f: ClientConnectionLostFn) {
        IggyMessageBus::set_client_connection_lost_fn(self, f);
    }
}
