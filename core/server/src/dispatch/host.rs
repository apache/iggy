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
//! across its transports: the per-client FIFO queues and their drains, the
//! [`SessionManager`], and the bus connection-lost hook that logs a dropped
//! connection out. All connections installed on the shard dispatch through
//! the same instance, which keeps disconnect cleanup equal across transports:
//! the destination shard installs it on delegated TCP/WS/TCP-TLS/WSS
//! connections, and shard 0 also on its local QUIC connections. A drain orders
//! session changes and partition admissions. Later non-PING frames wait for a
//! preceding metadata commit; independent reads run in their own tasks (see
//! `crate::dispatch::ClientDrain`).
//!
//! The host also owns a clone of the bus, so every adapter the bus installs,
//! on client and replica connections alike, keeps the bus alive: accepted,
//! since the bus lives for the process, rather than paid for with a weak
//! upgrade per replica frame.

use crate::consumer_group::lease::ConsumerGroupLiveness;
use crate::dispatch::submit::{CompletionRelays, handle_metadata_submit};
use crate::dispatch::{
    ActiveClientRequests, ClientRequestQueues, enqueue_client_request, upgrade_shard_handle,
};
use crate::session_manager::SessionManager;
use crate::shell::{ShellBus, ShellShardHandle};
use ahash::AHashMap;
use configs::server::ServerConfig;
use iggy_binary_protocol::{Command, GenericHeader, PrepareHeader};
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
    /// Paces the partition completions that shard 0 of a backup relays.
    completion_relays: Rc<RefCell<CompletionRelays>>,
    server_config: Arc<ServerConfig>,
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
    ) -> Self {
        let shard_handle = Rc::clone(shard_handle);
        let sessions = Rc::new(RefCell::new(SessionManager::new()));
        let queues: ClientRequestQueues = Rc::new(RefCell::new(AHashMap::new()));
        let queues_for_disconnect = Rc::clone(&queues);
        let sessions_for_disconnect = Rc::clone(&sessions);
        bus.set_client_connection_lost_fn(Rc::new(move |client_id| {
            // The socket is gone, so nothing will drain what a live drain task
            // left queued. The drain entry is NOT removed here: the transport
            // task runs this hook while a drain may be suspended at an `.await`,
            // and removing the entry would let a frame the dispatch task still
            // has buffered spawn a second drain over the same queue. The drain
            // task's own guard covers every exit, the panic compio catches
            // included.
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
            completion_relays: Rc::new(RefCell::new(CompletionRelays::new(
                server_config
                    .cluster
                    .commit_broadcast_interval
                    .get_duration(),
            ))),
            server_config,
            queues,
            active: Rc::new(RefCell::new(AHashMap::new())),
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
    pub const fn consumer_group_liveness(&self) -> &Rc<RefCell<ConsumerGroupLiveness>> {
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
    fn on_replica_message(&self, replica_id: u8, message: Message<GenericHeader>) {
        if matches!(
            message.header().command,
            Command::PrimaryRebalance | Command::SegmentDelete
        ) && message.header().replica != replica_id
        {
            return;
        }
        if let Some(shard) = upgrade_shard_handle(&self.shard_handle) {
            shard.dispatch(message);
        }
    }

    fn on_client_request(&self, client_id: u128, message: Message<GenericHeader>) {
        if matches!(
            message.header().command,
            Command::PrimaryRebalance | Command::SegmentDelete
        ) {
            return;
        }
        enqueue_client_request(
            &self.bus,
            &self.shard_handle,
            &self.sessions,
            &self.server_config,
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
            self.server_config.heartbeat.session_timeout(),
            &self.completion_relays,
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
                        .iter_consumer_sessions(
                            if self.server_config.heartbeat.enabled {
                                self.server_config.heartbeat.session_timeout()
                            } else {
                                Duration::MAX
                            },
                            self.bus.monotonic_now(),
                        )
                        .collect(),
                );
            }
            ListClientsReply::Count(reply) => {
                let _ = reply.try_send(self.sessions.borrow().client_count());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::test_support::{FIRST_BOOT, SpyBus, test_shard};
    use iggy_binary_protocol::{
        CommitHeader, ConsensusHeader, HEADER_SIZE, PrimaryRebalanceHeader, PrimaryRebalanceKind,
        SegmentDeleteHeader, SegmentDeleteKind,
    };
    use journal::prepare_journal::PrepareJournal;
    use metadata::impls::metadata::IggySnapshot;
    use server_common::MessageBag;
    use server_common::sharding::METADATA_GROUP;
    use shard::{ShardFrame, shard_channel};

    const REBALANCE_KINDS: [PrimaryRebalanceKind; 7] = [
        PrimaryRebalanceKind::MetadataProbe,
        PrimaryRebalanceKind::MetadataReply,
        PrimaryRebalanceKind::PartitionProbe,
        PrimaryRebalanceKind::PartitionReply,
        PrimaryRebalanceKind::SourceProbe,
        PrimaryRebalanceKind::SourceReply,
        PrimaryRebalanceKind::PrimaryTransfer,
    ];

    fn rebalance_frame(kind: PrimaryRebalanceKind) -> Message<GenericHeader> {
        Message::<PrimaryRebalanceHeader>::new(HEADER_SIZE)
            .transmute_header(|_, header: &mut PrimaryRebalanceHeader| {
                header.command = Command::PrimaryRebalance;
                header.size = u32::try_from(HEADER_SIZE).unwrap();
                header.cluster = 1;
                header.replica = 2;
                header.nonce = 1;
                header.kind = kind;
                header.quorum_age_ns = PrimaryRebalanceHeader::NO_QUORUM_EVIDENCE;
                if matches!(
                    kind,
                    PrimaryRebalanceKind::PartitionProbe
                        | PrimaryRebalanceKind::PartitionReply
                        | PrimaryRebalanceKind::SourceProbe
                        | PrimaryRebalanceKind::SourceReply
                        | PrimaryRebalanceKind::PrimaryTransfer
                ) {
                    header.created_revision = 1;
                }
                header.ready = u8::from(matches!(
                    kind,
                    PrimaryRebalanceKind::MetadataReply
                        | PrimaryRebalanceKind::PartitionReply
                        | PrimaryRebalanceKind::SourceReply
                ));
                header.target = u8::from(kind == PrimaryRebalanceKind::PrimaryTransfer);
                header.seal();
            })
            .into_generic()
    }

    #[test]
    fn given_rebalance_ingress_when_sender_is_spoofed_should_drop_before_routing() {
        let bus = SpyBus::default();
        let mut shard = test_shard(&bus, 0, 3, FIRST_BOOT);
        let (sender, inbox, _reply_inbox) = shard_channel(0, 1, 1);
        shard.attach_senders(vec![sender]);
        let shard = Rc::new(shard);
        let handle = Rc::new(RefCell::new(Some(Rc::downgrade(&shard))));
        let host = ServerHost::new(&bus, &handle, Arc::new(ServerConfig::default()));
        for kind in REBALANCE_KINDS {
            host.on_replica_message(1, rebalance_frame(kind));
            assert!(
                inbox.try_recv().is_err(),
                "spoofed {kind:?} reached the owner"
            );
            host.on_replica_message(2, rebalance_frame(kind));
            let Ok(ShardFrame::Consensus {
                target_shard: 0,
                message: MessageBag::PrimaryRebalance(message),
            }) = inbox.try_recv()
            else {
                panic!("authenticated {kind:?} must reach the metadata owner");
            };
            assert_eq!(message.header().kind, kind);
            assert_eq!(message.header().replica, 2);
        }

        let commit = Message::<CommitHeader>::new(HEADER_SIZE).transmute_header(
            |_, header: &mut CommitHeader| {
                header.command = Command::Commit;
                header.size = u32::try_from(HEADER_SIZE).unwrap();
                header.replica = 0;
                header.group = METADATA_GROUP;
                header.seal();
            },
        );
        host.on_replica_message(1, commit.into_generic());
        assert!(matches!(
            inbox.try_recv(),
            Ok(ShardFrame::Consensus {
                message: MessageBag::Commit(_),
                ..
            })
        ));
    }

    #[test]
    fn given_segment_resolution_ingress_when_received_should_require_the_authenticated_replica() {
        let bus = SpyBus::default();
        let mut shard = test_shard(&bus, 0, 3, FIRST_BOOT);
        let (sender, inbox, _reply_inbox) = shard_channel(0, 1, 1);
        shard.attach_senders(vec![sender]);
        let shard = Rc::new(shard);
        let handle = Rc::new(RefCell::new(Some(Rc::downgrade(&shard))));
        let host = ServerHost::new(&bus, &handle, Arc::new(ServerConfig::default()));
        for kind in [
            SegmentDeleteKind::Query,
            SegmentDeleteKind::Resolved,
            SegmentDeleteKind::Retry,
        ] {
            let frame = Message::<SegmentDeleteHeader>::new(HEADER_SIZE).transmute_header(
                |_, header: &mut SegmentDeleteHeader| {
                    header.command = Command::SegmentDelete;
                    header.size = u32::try_from(HEADER_SIZE).unwrap();
                    header.cluster = 1;
                    header.replica = 2;
                    header.nonce = 1;
                    header.created_revision = 1;
                    header.kind = kind;
                    header.seal();
                },
            );
            host.on_replica_message(1, frame.clone().into_generic());
            assert!(inbox.try_recv().is_err());
            host.on_client_request(7, frame.clone().into_generic());
            assert!(host.queues.borrow().is_empty());
            host.on_replica_message(2, frame.into_generic());
            assert!(matches!(
                inbox.try_recv(),
                Ok(ShardFrame::Consensus {
                    target_shard: 0,
                    message: MessageBag::SegmentDelete(_),
                })
            ));
        }
    }

    #[compio::test]
    async fn given_rebalance_frame_on_client_connection_when_received_should_not_admit_it() {
        let bus = SpyBus::default();
        let handle: ShellShardHandle<SpyBus, PrepareJournal, IggySnapshot> = Rc::default();
        let host = ServerHost::new(&bus, &handle, Arc::new(ServerConfig::default()));
        for kind in REBALANCE_KINDS {
            host.on_client_request(7, rebalance_frame(kind));
            assert!(
                host.queues.borrow().is_empty(),
                "client {kind:?} was queued"
            );
            assert!(
                host.active.borrow().is_empty(),
                "client {kind:?} was admitted"
            );
        }
    }
}
