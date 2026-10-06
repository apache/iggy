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

//! The server as a [`ShardHost`]: what production and the simulator's shell
//! mode build a shard on.
//!
//! One [`ServerHost`] per shard owns everything the request plane shares
//! across its transports: the per-client FIFO queues and their drain slots,
//! the [`SessionManager`], and the bus connection-lost hook that logs a
//! dropped connection out. All connections installed on the shard dispatch
//! through the same instance, which preserves ordering and disconnect
//! cleanup across transports: the destination shard installs it on delegated
//! TCP/WS/TCP-TLS/WSS connections, and shard 0 also on its local QUIC
//! connections.
//!
//! The host also owns a clone of the bus, so every adapter the bus installs,
//! on client and replica connections alike, keeps the bus alive: accepted,
//! since the bus lives for the process, rather than paid for with a weak
//! upgrade per replica frame.

use crate::consumer_group::lease::ConsumerGroupLiveness;
use crate::dispatch::submit::handle_metadata_submit;
use crate::dispatch::{
    ActiveClientRequests, ClientRequestQueues, enqueue_client_request, upgrade_shard_handle,
};
use crate::session_manager::SessionManager;
use crate::shell::{ShellBus, ShellShardHandle};
use ahash::{AHashMap, AHashSet};
use configs::server::ServerConfig;
use iggy_binary_protocol::{GenericHeader, PrepareHeader};
use journal::superblock::{PingPongSuperblock, SuperblockStore};
use journal::{Journal, JournalHandle};
use message_bus::MessageBus;
use server_common::Message;
use shard::{ListClientsReply, MetadataSubmit, ShardHost};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

/// The shard host the server runs; see the module docs.
///
/// Holds the late-bound [`ShellShardHandle`] rather than the shard itself:
/// the host exists before the shard it serves (the shard is built on it),
/// so every handler upgrades the weak self-reference per frame. Weak also
/// because the shard owns the host: a strong `Rc` here would close the
/// shard -> host -> shard cycle and keep the shard alive past shutdown.
pub struct ServerHost<B, MJ, S, SB = PingPongSuperblock>
where
    B: MessageBus,
{
    bus: B,
    shard_handle: ShellShardHandle<B, MJ, S, SB>,
    sessions: Rc<RefCell<SessionManager>>,
    /// Volatile leases shared by replica heartbeat ingress and shard 0's expiry task.
    consumer_group_liveness: Rc<RefCell<ConsumerGroupLiveness>>,
    server_config: Arc<ServerConfig>,
    max_tokens_per_user: u32,
    queues: ClientRequestQueues,
    active: ActiveClientRequests,
}

impl<B, MJ, S, SB> ServerHost<B, MJ, S, SB>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    /// Build the host for `shard_handle` against `bus`, over a fresh
    /// `SessionManager`, and install the bus connection-lost hook. The
    /// caller must set the weak self-reference in `shard_handle` once the
    /// shard is built, so the handlers can upgrade it per frame.
    pub fn new(
        bus: &B,
        shard_handle: &ShellShardHandle<B, MJ, S, SB>,
        server_config: Arc<ServerConfig>,
        max_tokens_per_user: u32,
    ) -> Self {
        let shard_handle = Rc::clone(shard_handle);
        let sessions = Rc::new(RefCell::new(SessionManager::new()));
        let queues: ClientRequestQueues = Rc::new(RefCell::new(AHashMap::new()));
        let queues_for_disconnect = Rc::clone(&queues);
        let sessions_for_disconnect = Rc::clone(&sessions);
        bus.set_client_connection_lost_fn(Rc::new(move |client_id| {
            // The socket is gone, so nothing will drain what a live drain task
            // left queued. The active slot is NOT released here: the transport
            // task runs this hook while a drain may be suspended at an `.await`,
            // and clearing the slot would let a frame the dispatch task still has
            // buffered spawn a second drain over the same queue. The drain task's
            // own guard covers every exit, the panic compio catches included.
            queues_for_disconnect.borrow_mut().remove(&client_id);
            sessions_for_disconnect
                .borrow_mut()
                .remove_connection(client_id);
        }));
        Self {
            bus: bus.clone(),
            shard_handle,
            sessions,
            consumer_group_liveness: Rc::default(),
            server_config,
            max_tokens_per_user,
            queues,
            active: Rc::new(RefCell::new(AHashSet::new())),
        }
    }

    /// The `SessionManager` the client-request path binds sessions into
    /// and the list-clients handler reads; the caller keeps it to reach
    /// locally-homed sessions.
    #[must_use]
    pub const fn sessions(&self) -> &Rc<RefCell<SessionManager>> {
        &self.sessions
    }

    /// The leases the metadata-submit path refreshes from replica
    /// heartbeats; shard 0's expiry task reads the same instance.
    pub(crate) const fn consumer_group_liveness(&self) -> &Rc<RefCell<ConsumerGroupLiveness>> {
        &self.consumer_group_liveness
    }
}

impl<B, MJ, S, SB> ShardHost for ServerHost<B, MJ, S, SB>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    fn on_replica_message(&self, _replica_id: u8, message: Message<GenericHeader>) {
        if let Some(shard) = upgrade_shard_handle(&self.shard_handle) {
            shard.dispatch(message);
        }
    }

    fn on_client_request(&self, client_id: u128, message: Message<GenericHeader>) {
        enqueue_client_request(
            &self.bus,
            &self.shard_handle,
            &self.sessions,
            &self.server_config,
            self.max_tokens_per_user,
            &self.queues,
            &self.active,
            client_id,
            message,
        );
    }

    fn on_metadata_submit(&self, submit: MetadataSubmit) {
        let Some(shard) = upgrade_shard_handle(&self.shard_handle) else {
            return;
        };
        handle_metadata_submit(
            shard,
            submit,
            &self.consumer_group_liveness,
            self.server_config
                .consumer_group
                .session_timeout
                .get_duration(),
        );
    }

    fn on_list_clients(&self, reply: ListClientsReply) {
        match reply {
            ListClientsReply::Clients(reply) => {
                let _ = reply.try_send(self.sessions.borrow().iter_clients().collect());
            }
            ListClientsReply::Sessions(reply) => {
                let _ = reply.try_send(
                    self.sessions
                        .borrow()
                        .iter_consumer_sessions(if self.server_config.heartbeat.enabled {
                            self.server_config
                                .consumer_group
                                .session_timeout
                                .get_duration()
                        } else {
                            Duration::MAX
                        })
                        .collect(),
                );
            }
            ListClientsReply::Count(reply) => {
                let _ = reply.try_send(self.sessions.borrow().client_count());
            }
        }
    }
}
