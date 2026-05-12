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
pub use replica::{install_replica_conn, install_replica_tcp};
pub use tcp::{install_client_conn, install_client_tcp};
pub use tcp_tls::install_client_tcp_tls;
pub use ws::install_client_ws;
pub use wss::install_client_wss;

use crate::IggyMessageBus;
use crate::client_listener::RequestHandler;
use crate::fd_transfer::{self, DupedFd};
use crate::installer::conn_info::ClientConnMeta;
use crate::replica::listener::MessageHandler;
use std::rc::Rc;
use tracing::warn;

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
    /// by dropping the wrapping `TcpStream`; on caller-side failure (e.g.
    /// inter-shard send drops the setup frame) the `DupedFd` closes the
    /// fd on drop.
    fn install_replica_tcp_fd(&self, fd: DupedFd, replica_id: u8, on_message: MessageHandler);

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

    /// Update the replica -> owning shard mapping used by the `send_to_replica`
    /// slow path on non-owning shards.
    fn set_shard_mapping(&self, replica: u8, owning_shard: u16);

    /// Forget the replica -> owning shard mapping (e.g. after a connection
    /// loss, before the next allocate).
    fn remove_shard_mapping(&self, replica: u8);
}

impl ConnectionInstaller for Rc<IggyMessageBus> {
    fn install_replica_tcp_fd(&self, fd: DupedFd, replica_id: u8, on_message: MessageHandler) {
        let stream = fd_transfer::wrap_duped_fd(fd);
        install_replica_tcp(self, replica_id, stream, on_message);
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
            let outcome = compio::time::timeout(
                handshake_grace,
                compio_ws::accept_async_with_config(stream, ws_config),
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

    fn set_shard_mapping(&self, replica: u8, owning_shard: u16) {
        IggyMessageBus::set_shard_mapping(self, replica, owning_shard);
    }

    fn remove_shard_mapping(&self, replica: u8) {
        IggyMessageBus::remove_shard_mapping(self, replica);
    }
}
