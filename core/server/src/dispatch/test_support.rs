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

//! Shared harness for the dispatch tree's unit tests: the [`SpyBus`] records
//! client- and replica-bound frames instead of writing to sockets, and the
//! builders below assemble minimal shards and wire frames.

use consensus::{LocalPipeline, VsrConsensus};
use iggy_binary_protocol::{Command, Operation, PrepareHeader, ReplyHeader, RoutedRequestHeader};
use iggy_common::variadic;
use journal::prepare_journal::PrepareJournal;
use message_bus::client_listener::RequestHandler;
use message_bus::fd_transfer::DupedFd;
use message_bus::installer::ConnectionInstaller;
use message_bus::installer::conn_info::ClientConnMeta;
use message_bus::replica::listener::MessageHandler;
use message_bus::{
    BusMessage, ClientConnectionLostFn, ClientForwardFn, ConnectionLostFn, JoinHandle, MessageBus,
    ReplicaForwardFn, ReplicaHandshakeDoneFn, SendError,
};
use metadata::impls::metadata::IggySnapshot;
use metadata::stm::stream::Streams;
use metadata::stm::user::Users;
use metadata::{IggyMetadata, MuxStateMachine};
use partitions::{IggyPartitions, PartitionPathLayout, PartitionsConfig};
use server_common::iobuf::Frozen;
use server_common::sharding::ShardId;
use server_common::{MESSAGE_ALIGN, Message};
use shard::shards_table::PapayaShardsTable;
use shard::{IggyShard, PartitionConsensusConfig, ReplicaTopology, ShardIdentity};
use std::cell::{Cell, RefCell};
use std::mem::size_of;
use std::rc::Rc;

pub type TestMux = MuxStateMachine<variadic!(Users, Streams)>;
pub type TestShard = IggyShard<SpyBus, PrepareJournal, IggySnapshot, TestMux, PapayaShardsTable>;
/// `(target client id, reply frame bytes)` per `send_to_client` call.
pub type RecordedReplies = Rc<RefCell<Vec<(u128, Vec<u8>)>>>;
/// `(target replica id, frame bytes)` per `send_to_replica` call.
pub type RecordedReplicaSends = Rc<RefCell<Vec<(u8, Vec<u8>)>>>;

/// Records every client-bound reply and replica-bound frame (target +
/// bytes) instead of writing to a socket, and counts connection-lost hook
/// installs; everything else is a no-op. The two `ShellBus` halves are
/// stubbed.
#[derive(Debug, Clone, Default)]
pub struct SpyBus {
    pub client_replies: RecordedReplies,
    pub replica_sends: RecordedReplicaSends,
    /// Installs of the client connection-lost hook, one per handler built
    /// on this bus.
    pub connection_lost_hooks: Rc<Cell<usize>>,
    /// Resolve [`MessageBus::sleep`] immediately instead of arming a real
    /// timer. The register forward is the only path here that races a
    /// timer, and its budget is five seconds -- too long to wait for in a
    /// unit test, and too long to shorten in production for one.
    pub instant_timers: Rc<Cell<bool>>,
}

impl SpyBus {
    /// Decode the single frame this bus sent to a replica.
    pub fn sole_replica_send<H: iggy_binary_protocol::ConsensusHeader>(&self) -> (u8, H) {
        let sends = self.replica_sends.borrow();
        assert_eq!(sends.len(), 1, "expected exactly one replica-bound frame");
        let (target, frame) = &sends[0];
        let mut aligned = server_common::iobuf::Owned::<MESSAGE_ALIGN>::zeroed(frame.len());
        aligned.as_mut_slice().copy_from_slice(frame);
        let header = *bytemuck::checked::try_from_bytes::<H>(&aligned.as_slice()[..size_of::<H>()])
            .expect("replica frame decodes into the expected header");
        (*target, header)
    }
}

#[allow(clippy::future_not_send)]
impl MessageBus for SpyBus {
    fn track_background(&self, _handle: JoinHandle<()>) {}
    async fn send_to_client(
        &self,
        client_id: u128,
        data: impl Into<BusMessage>,
    ) -> Result<(), SendError> {
        self.client_replies
            .borrow_mut()
            .push((client_id, data.into().into_contiguous().as_slice().to_vec()));
        Ok(())
    }
    async fn send_to_replica(
        &self,
        replica: u8,
        data: Frozen<MESSAGE_ALIGN>,
    ) -> Result<(), SendError> {
        self.replica_sends
            .borrow_mut()
            .push((replica, data.as_slice().to_vec()));
        Ok(())
    }
    async fn sleep(&self, duration: std::time::Duration) {
        if !self.instant_timers.get() {
            compio::time::sleep(duration).await;
        }
    }
    fn set_connection_lost_fn(&self, _f: ConnectionLostFn) {}
    fn set_replica_forward_fn(&self, _f: ReplicaForwardFn) {}
    fn set_client_forward_fn(&self, _f: ClientForwardFn) {}
}

impl ConnectionInstaller for SpyBus {
    fn install_replica_inbound_fd(
        &self,
        _fd: DupedFd,
        _on_message: MessageHandler,
        _on_done: ReplicaHandshakeDoneFn,
    ) {
    }
    fn install_replica_outbound_fd(
        &self,
        _fd: DupedFd,
        _replica_id: u8,
        _on_message: MessageHandler,
        _on_done: ReplicaHandshakeDoneFn,
    ) {
    }
    fn release_replica_handshake_slot(&self, _slot: u64) {}
    fn clear_replica_dial_pending(&self, _replica_id: u8) {}
    fn install_client_fd(&self, _fd: DupedFd, _meta: ClientConnMeta, _on_request: RequestHandler) {}
    fn install_client_ws_fd(
        &self,
        _fd: DupedFd,
        _meta: ClientConnMeta,
        _on_request: RequestHandler,
    ) {
    }
    fn client_meta(&self, _client_id: u128) -> Option<Rc<ClientConnMeta>> {
        None
    }
    fn set_client_connection_lost_fn(&self, _f: ClientConnectionLostFn) {
        self.connection_lost_hooks
            .set(self.connection_lost_hooks.get() + 1);
    }
}

/// Consensus incarnations standing for two successive boots of one node, as
/// far apart as the random draw at bootstrap makes them.
pub const FIRST_BOOT: u128 = 0x5EED_0001;
pub const SECOND_BOOT: u128 = 0x9E37_79B9_7F4A_7C15;

/// Shard 0 carrying a metadata consensus group of `replica_count`
/// replicas in which this node is `replica`. No journal: every test using
/// it either never proposes, or is a backup that cannot.
///
/// `incarnation` stands for one boot of this node: the shard seeds its
/// forward-nonce counter from it, so passing a different value models a
/// restart.
pub fn test_shard(bus: &SpyBus, replica: u8, replica_count: u8, incarnation: u128) -> TestShard {
    let consensus = VsrConsensus::new(
        1,
        replica,
        replica_count,
        server_common::sharding::METADATA_GROUP,
        bus.clone(),
        LocalPipeline::new(),
    );
    consensus.set_incarnation(incarnation);
    consensus.init();
    let metadata: IggyMetadata<_, PrepareJournal, IggySnapshot, TestMux> =
        IggyMetadata::new(Some(consensus), None, None, None, TestMux::default(), None);
    let partitions = IggyPartitions::new(
        ShardId::new(0),
        PartitionsConfig {
            messages_required_to_save: 1,
            size_of_messages_required_to_save: iggy_common::IggyByteSize::from(1024_u64),
            enforce_fsync: false,
            validate_checksum: true,
            segment_size: iggy_common::IggyByteSize::from(1_048_576_u64),
            preallocate_segments: false,
            encryptor: None,
            path_layout: PartitionPathLayout::default(),
        },
    );
    TestShard::without_inbox(
        ShardIdentity::new(0, "dispatch-test".to_string()),
        bus.clone(),
        metadata,
        partitions,
        PapayaShardsTable::new(),
        PartitionConsensusConfig::new(1, ReplicaTopology::new(replica, replica_count), bus.clone()),
    )
}

/// Minimal committed `Register` reply for `ClientTable::commit_register`
/// (reads only `client` and `commit`).
pub fn register_reply(client: u128, session: u64) -> Message<ReplyHeader> {
    let header_size = size_of::<ReplyHeader>();
    let mut reply = Message::<ReplyHeader>::new(header_size);
    let header = bytemuck::checked::try_from_bytes_mut::<ReplyHeader>(
        &mut reply.as_mut_slice()[..header_size],
    )
    .expect("zeroed bytes are a valid ReplyHeader");
    *header = ReplyHeader {
        client,
        request: 0,
        commit: session,
        command: Command::Reply,
        operation: Operation::Register,
        ..Default::default()
    };
    reply
}

pub fn request_message(
    operation: Operation,
    client: u128,
    session: u64,
    request: u64,
    body: &[u8],
) -> Message<RoutedRequestHeader> {
    let header_size = size_of::<RoutedRequestHeader>();
    let total = header_size + body.len();
    let mut message = Message::<RoutedRequestHeader>::new(total);
    {
        let slice = message.as_mut_slice();
        slice[header_size..total].copy_from_slice(body);
        let header =
            bytemuck::checked::from_bytes_mut::<RoutedRequestHeader>(&mut slice[..header_size]);
        *header = RoutedRequestHeader {
            command: Command::Request,
            operation,
            size: u32::try_from(total).expect("test request fits u32"),
            client,
            session,
            request,
            user_id: 0,
            group: server_common::sharding::METADATA_GROUP,
            ..Default::default()
        };
    }
    message
}

/// Raw prepare for the sibling op, standing in for the crate-private
/// `prepare_request` projection. `user_id` 0 skips the in-apply RBAC
/// gate (server-originated convention), so the op applies cleanly.
pub fn prepare_message(
    operation: Operation,
    client: u128,
    request: u64,
    body: &[u8],
) -> Message<PrepareHeader> {
    let header_size = size_of::<PrepareHeader>();
    let total = header_size + body.len();
    let mut message = Message::<PrepareHeader>::new(total);
    {
        let slice = message.as_mut_slice();
        slice[header_size..total].copy_from_slice(body);
        let header = bytemuck::checked::from_bytes_mut::<PrepareHeader>(&mut slice[..header_size]);
        *header = PrepareHeader {
            command: Command::Prepare,
            operation,
            size: u32::try_from(total).expect("test prepare fits u32"),
            op: 1,
            view: 0,
            client,
            request,
            user_id: 0,
            group: server_common::sharding::METADATA_GROUP,
            ..Default::default()
        };
    }
    // A real identity, not a placeholder: `on_replicate` recomputes it before the
    // prepare reaches the WAL, so an arbitrary value reads as transit corruption.
    consensus::seal_prepare_checksum(message)
}
