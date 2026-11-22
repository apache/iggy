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

use super::*;
use crate::impls::recovery::{ReplicaIdentity, recover};
use crate::stm::StateHandler;
use crate::stm::authz::GatedApply;
use crate::stm::consumer_group::JoinConsumerGroupRequest;
use crate::stm::lifecycle::{
    CompleteLifecycleRequest, LifecycleCompletion, apply_with_lifecycle_completion,
};
use crate::stm::stream::{Streams, StreamsInner};
use crate::stm::user::{PAT_TOKEN_HASH_BYTES, Users, UsersInner};
use consensus::{HistoryRecovery, LocalPipeline, Status};
use iggy_binary_protocol::codes::CREATE_USER_CODE;
use iggy_binary_protocol::primitives::metadata_changes::{
    WireMetadataChangeKind, WireMetadataChangeStatus,
};
use iggy_binary_protocol::primitives::permissions::{
    WireGlobalPermissions, WirePermissions, WireStreamPermissions,
};
use iggy_binary_protocol::requests::consumer_groups::CreateConsumerGroupRequest;
use iggy_binary_protocol::requests::streams::CreateStreamRequest;
use iggy_binary_protocol::requests::topics::{CreateTopicRequest, DeleteTopicRequest};
use iggy_binary_protocol::requests::users::{CreateUserRequest, DeleteUserRequest};
use iggy_binary_protocol::{RequestHeader, StartViewChangeHeader, StartViewHeader, WireOptions};
use iggy_common::MetadataChangeGroup;
use iggy_common::{
    Durability, GeoMode, HeaderValue, IggyExpiry, IggyTimestamp, OptionKey, OptionStorage,
    OptionValueCodec, PartitionResizePolicy, ServerKeyContext, StreamEncryptionKey,
    StreamKeyOrigin, TopicType, TopicUpdateOptions, UserStatus, UserUpdateOptions, WrapSet,
    check_critical_options, critical_keys_marker, descriptors_for, stream_wrap_set, variadic,
};
use journal::prepare_journal::{DEFAULT_SLOT_COUNT, PrepareJournal};
use journal::superblock::SuperblockContents;
use message_bus::{BusMessage, ConnectionLostFn, JoinHandle, ReplicaForwardFn, SendError};
use server_common::MESSAGE_ALIGN;
use server_common::iobuf::Frozen;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

mod commit_log;
mod consensus_tests;
mod journal_gating;
mod partitions_caps;
mod placement;
mod request_resolution;
mod sessions;
mod sessions_revocation;
mod snapshot;
mod state_transfer;
mod topic_admission;

#[allow(clippy::future_not_send)]
async fn settled_transfer_offer(
    metadata: &IggyMetadata<VsrConsensus<NoopBus>, PrepareJournal, (), TestMux>,
) -> Result<Rc<StateTransferOffer>, StateTransferUnavailable> {
    const TEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
    compio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match metadata.state_transfer_offer().await {
                Err(StateTransferUnavailable::CheckpointInProgress) => {
                    compio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
                result => return result,
            }
        }
    })
    .await
    .expect("concurrent snapshot writers settle")
}

type TestMux = MuxStateMachine<variadic!(Users, Streams)>;

#[derive(Default)]
struct CheckpointSuperblock {
    fail: Cell<bool>,
    writes: Cell<usize>,
}

#[allow(clippy::future_not_send)]
impl SuperblockStore for CheckpointSuperblock {
    async fn write(&self, _payload: &[u8]) -> io::Result<()> {
        self.writes.set(self.writes.get() + 1);
        if self.fail.get() {
            Err(io::Error::other("injected checkpoint superblock failure"))
        } else {
            Ok(())
        }
    }

    async fn read_latest(&self) -> io::Result<SuperblockContents> {
        Ok(SuperblockContents::Empty)
    }
}

/// Build a peer-shard-style `IggyMetadata` with `consensus`,
/// `journal`, and `snapshot` all `None`. Enough to test the
/// commit-notifier slot without standing up VSR / WAL infrastructure:
/// the test picks `()` for `C` / `J` / `S` since no notifier code path
/// touches their methods.
fn peer_metadata() -> IggyMetadata<(), (), (), TestMux> {
    IggyMetadata::new(None, None, None, None, TestMux::default(), None)
}

fn register_reply(client: u128, session: u64) -> Message<ReplyHeader> {
    build_reply_message(
        &PrepareHeader {
            client,
            op: session,
            operation: Operation::Register,
            ..Default::default()
        },
        &bytes::Bytes::new(),
    )
}

/// No-op bus: `prepare_request` builds a prepare without ever sending, so
/// every method is an unused stub.
#[derive(Debug, Default)]
struct NoopBus;

impl MessageBus for NoopBus {
    fn track_background(&self, _handle: JoinHandle<()>) {}
    async fn send_to_client(
        &self,
        _client_id: u128,
        _data: impl Into<BusMessage>,
    ) -> Result<(), SendError> {
        Ok(())
    }
    async fn send_to_replica(
        &self,
        _replica: u8,
        _data: Frozen<MESSAGE_ALIGN>,
    ) -> Result<(), SendError> {
        Ok(())
    }
    fn set_connection_lost_fn(&self, _f: ConnectionLostFn) {}
    fn set_replica_forward_fn(&self, _f: ReplicaForwardFn) {}
}

/// Single-node metadata plane whose `prepare_request` is callable. `J` is
/// `PrepareJournal` in name only (the value is `None`) to satisfy the
/// impl-block bound; no journal or snapshot is constructed.
fn metadata_plane() -> IggyMetadata<VsrConsensus<NoopBus>, PrepareJournal, (), TestMux> {
    let consensus = VsrConsensus::new(
        1,
        0,
        1,
        server_common::sharding::METADATA_GROUP,
        NoopBus,
        LocalPipeline::new(),
    );
    consensus.init();
    IggyMetadata::new(Some(consensus), None, None, None, TestMux::default(), None)
}

fn create_topic_request(client: u128, wire_user_id: u32) -> Message<RoutedRequestHeader> {
    create_named_topic_request(client, wire_user_id, WireIdentifier::numeric(0), "t")
}

fn create_named_topic_request(
    client: u128,
    wire_user_id: u32,
    stream_id: WireIdentifier,
    name: &str,
) -> Message<RoutedRequestHeader> {
    create_topic_request_with_count(client, wire_user_id, stream_id, name, 1)
}

fn create_topic_request_with_count(
    client: u128,
    wire_user_id: u32,
    stream_id: WireIdentifier,
    name: &str,
    partitions_count: u32,
) -> Message<RoutedRequestHeader> {
    create_topic_request_with_options(
        client,
        wire_user_id,
        stream_id,
        name,
        partitions_count,
        WireOptions::empty(),
    )
}

fn create_topic_request_with_options(
    client: u128,
    wire_user_id: u32,
    stream_id: WireIdentifier,
    name: &str,
    partitions_count: u32,
    options: WireOptions,
) -> Message<RoutedRequestHeader> {
    let body = CreateTopicRequest {
        stream_id,
        partitions_count,
        name: WireName::new(name).unwrap(),
        options,
    }
    .to_bytes();
    let header_size = size_of::<RoutedRequestHeader>();
    let total = header_size + body.len();
    let mut message = Message::<RoutedRequestHeader>::new(total);
    {
        let slice = message.as_mut_slice();
        slice[header_size..total].copy_from_slice(&body);
        let header =
            bytemuck::checked::from_bytes_mut::<RoutedRequestHeader>(&mut slice[..header_size]);
        *header = RoutedRequestHeader {
            command: Command::Request,
            operation: Operation::CreateTopic,
            size: u32::try_from(total).unwrap(),
            client,
            session: 1,
            request: 1,
            user_id: wire_user_id,
            group: server_common::sharding::METADATA_GROUP,
            ..Default::default()
        };
    }
    message
}

fn update_resource_request(client: u128, body: &[u8]) -> Message<RoutedRequestHeader> {
    let header_size = size_of::<RoutedRequestHeader>();
    let total = header_size + body.len();
    let mut message = Message::<RoutedRequestHeader>::new(total);
    let slice = message.as_mut_slice();
    slice[header_size..total].copy_from_slice(body);
    *bytemuck::checked::from_bytes_mut::<RoutedRequestHeader>(&mut slice[..header_size]) =
        RoutedRequestHeader {
            command: Command::Request,
            operation: Operation::UpdateResource,
            size: u32::try_from(total).unwrap(),
            client,
            session: 1,
            request: 1,
            group: server_common::sharding::METADATA_GROUP,
            ..Default::default()
        };
    message
}

fn placement_metadata() -> IggyMetadata<VsrConsensus<NoopBus>, PrepareJournal, (), TestMux> {
    placement_metadata_with_bus(NoopBus)
}

fn placement_metadata_with_bus<B: MessageBus>(
    bus: B,
) -> IggyMetadata<VsrConsensus<B>, PrepareJournal, (), TestMux> {
    let consensus = VsrConsensus::new(
        1,
        0,
        3,
        server_common::sharding::METADATA_GROUP,
        bus,
        LocalPipeline::new(),
    );
    consensus.init();
    let metadata = IggyMetadata::new(Some(consensus), None, None, None, TestMux::default(), None);
    for client in 1..=3 {
        metadata
            .client_table
            .borrow_mut()
            .commit_register(client, 0, [0x5a; 32], register_reply(client, 1))
            .unwrap();
    }
    metadata
}

fn placement_request(client: u128, name: &str, count: u32) -> Message<RoutedRequestHeader> {
    create_topic_request_with_count(client, 0, WireIdentifier::numeric(0), name, count)
}

fn assigned_views(prepare: &Message<PrepareHeader>) -> Vec<u32> {
    PersistedCreateTopicRequest::decode_from(prepare.body())
        .unwrap()
        .partitions
        .into_iter()
        .map(|partition| partition.initial_view)
        .collect()
}

fn prepare_topic_with(
    options: &iggy_common::TopicCreateOptions,
) -> Result<PersistedCreateTopicRequest, IggyError> {
    prepare_topic_on(&metadata_plane(), options)
}

fn prepare_topic_on(
    plane: &IggyMetadata<VsrConsensus<NoopBus>, PrepareJournal, (), TestMux>,
    options: &iggy_common::TopicCreateOptions,
) -> Result<PersistedCreateTopicRequest, IggyError> {
    const CLIENT: u128 = 1;
    const SESSION: u64 = 10;
    const ACTING_USER: u32 = 7;
    plane
        .client_table
        .borrow_mut()
        .commit_register(
            CLIENT,
            ACTING_USER,
            [0x5a; 32],
            register_reply(CLIENT, SESSION),
        )
        .expect("register commits");
    let prepare = plane.prepare_request(create_topic_request_with_options(
        CLIENT,
        ACTING_USER,
        WireIdentifier::numeric(0),
        "t",
        1,
        options.to_wire().expect("options encode"),
    ))?;
    let body = &prepare.as_slice()[size_of::<PrepareHeader>()..prepare.header().size as usize];
    Ok(PersistedCreateTopicRequest::decode_from(body)
        .expect("create topic with assignments prepare must decode"))
}

const HOUR_MICROS: u64 = 3_600_000_000;

/// Bus whose `send_to_client` parks forever while `stall` is set,
/// recording each parked send in `stall_hits`. Models a client whose
/// connection writer stalled, so an `on_ack` driver suspends at a wire
/// send — an await the test can then cancel the driver at.
#[derive(Debug, Default)]
struct StallBus {
    stall: std::cell::Cell<bool>,
    stall_hits: std::cell::Cell<u32>,
}

// Cell fields make the futures !Send; fine on the single-threaded shard.
#[allow(clippy::future_not_send)]
impl MessageBus for StallBus {
    fn track_background(&self, _handle: JoinHandle<()>) {}
    async fn send_to_client(
        &self,
        _client_id: u128,
        _data: impl Into<BusMessage>,
    ) -> Result<(), SendError> {
        if self.stall.get() {
            self.stall_hits.set(self.stall_hits.get() + 1);
            std::future::pending::<()>().await;
        }
        Ok(())
    }
    async fn send_to_replica(
        &self,
        _replica: u8,
        _data: Frozen<MESSAGE_ALIGN>,
    ) -> Result<(), SendError> {
        Ok(())
    }
    fn set_connection_lost_fn(&self, _f: ConnectionLostFn) {}
    fn set_replica_forward_fn(&self, _f: ReplicaForwardFn) {}
}

/// Bus that records the client id of every `send_to_client`.
#[derive(Debug, Default)]
struct ClientSendSpyBus {
    client_sends: RefCell<Vec<u128>>,
    client_replies: RefCell<Vec<BusMessage>>,
}

#[allow(clippy::future_not_send)]
impl MessageBus for ClientSendSpyBus {
    fn track_background(&self, _handle: JoinHandle<()>) {}
    async fn send_to_client(
        &self,
        client_id: u128,
        data: impl Into<BusMessage>,
    ) -> Result<(), SendError> {
        self.client_sends.borrow_mut().push(client_id);
        self.client_replies.borrow_mut().push(data.into());
        Ok(())
    }
    async fn send_to_replica(
        &self,
        _replica: u8,
        _data: Frozen<MESSAGE_ALIGN>,
    ) -> Result<(), SendError> {
        Ok(())
    }
    fn set_connection_lost_fn(&self, _f: ConnectionLostFn) {}
    fn set_replica_forward_fn(&self, _f: ReplicaForwardFn) {}
}

/// Clients the cap tests register, above those `placement_metadata`
/// registers as root.
const FIRST_CLIENT: u128 = 11;

/// A primary of three replicas, so its prepares wait in the pipeline, over
/// `users` and `streams`, with `clients` registered as `user_id`.
fn primary_with_clients<B: MessageBus>(
    bus: B,
    users: Users,
    streams: StreamsInner,
    user_id: u32,
    clients: impl IntoIterator<Item = u128>,
) -> IggyMetadata<VsrConsensus<B>, PrepareJournal, (), TestMux> {
    let mut metadata = placement_metadata_with_bus(bus);
    metadata.mux_stm = Rc::new(TestMux::new((users, (streams.into(), ()))));
    for client in clients {
        metadata
            .client_table
            .borrow_mut()
            .commit_register(client, user_id, [0x5a; 32], register_reply(client, 1))
            .unwrap();
    }
    metadata
}

/// What a view change that truncates the uncommitted suffix leaves on the
/// old primary: `handle_start_view` clears the pipeline and moves the head
/// to the announced op, here the commit.
fn drop_uncommitted_suffix<B: MessageBus>(consensus: &VsrConsensus<B>) {
    consensus.clear_pipeline();
    consensus.sequencer().set_sequence(consensus.commit_min());
}

const TOKEN_OWNER: u32 = 7;
const TOKEN_NAME: &str = "token";
const TOKEN_CAP: u32 = 1;

fn token_create(name: &str) -> CreatePersonalAccessTokenRequest {
    CreatePersonalAccessTokenRequest {
        user_id: TOKEN_OWNER,
        name: WireName::new(name).unwrap(),
        expiry: 0,
        token_hash: [b'a'; PAT_TOKEN_HASH_BYTES],
    }
}

fn token_delete() -> DeletePersonalAccessTokenRequest {
    DeletePersonalAccessTokenRequest {
        user_id: TOKEN_OWNER,
        name: WireName::new(TOKEN_NAME).unwrap(),
        only_if_expired: false,
    }
}

/// `TOKEN_OWNER` holding `TOKEN_NAME`, which fills `TOKEN_CAP`.
fn users_holding_token() -> UsersInner {
    let mut users = UsersInner::new();
    let created = StateHandler::apply(&token_create(TOKEN_NAME), &mut users, IggyTimestamp::now());
    assert_eq!(created.code, 0, "the fixture token must be created");
    users
}

/// A plane over `users` that caps each user at `TOKEN_CAP` tokens.
fn metadata_capped_over(
    users: UsersInner,
) -> IggyMetadata<VsrConsensus<NoopBus>, PrepareJournal, (), TestMux> {
    let mut metadata = metadata_plane();
    metadata.mux_stm = Rc::new(TestMux::new((
        users.into(),
        (StreamsInner::new().into(), ()),
    )));
    metadata.set_max_tokens_per_user(TOKEN_CAP);
    metadata
}

/// Slab ids of `users_with_topic_manager`: root takes 0.
const TOPIC_MANAGER: u32 = 1;
const UNGRANTED: u32 = 2;

/// Root, then `TOPIC_MANAGER` with `manage_topics` on stream 0, then
/// `UNGRANTED` with no permissions.
fn users_with_topic_manager() -> Users {
    let mut inner = UsersInner::new();
    let timestamp = IggyTimestamp::now();
    let no_grants = WireGlobalPermissions {
        manage_servers: false,
        read_servers: false,
        manage_users: false,
        read_users: false,
        manage_streams: false,
        read_streams: false,
        manage_topics: false,
        read_topics: false,
        poll_messages: false,
        send_messages: false,
    };
    for (username, streams) in [
        ("iggy", Vec::new()),
        (
            "manager",
            vec![WireStreamPermissions {
                stream_id: 0,
                manage_stream: false,
                read_stream: false,
                manage_topics: true,
                read_topics: false,
                poll_messages: false,
                send_messages: false,
                topics: Vec::new(),
            }],
        ),
        ("ungranted", Vec::new()),
    ] {
        let reply = StateHandler::apply(
            &CreateUserRequest {
                username: WireName::new(username).unwrap(),
                password: "hash".to_string(),
                status: UserStatus::Active.as_code(),
                permissions: Some(WirePermissions {
                    global: no_grants.clone(),
                    streams,
                }),
                options: WireOptions::empty(),
            },
            &mut inner,
            timestamp,
        );
        assert_eq!(reply.code, 0, "fixture user {username} must be created");
    }
    inner.into()
}

/// Stream 0 holding one topic with one partition.
fn stream_with_one_partition() -> StreamsInner {
    stream_with_one_partition_in_view(0)
}

fn stream_with_one_partition_in_view(view: u32) -> StreamsInner {
    let mut inner = StreamsInner::new();
    let _ = StateHandler::apply(
        &CreateStreamRequest {
            name: WireName::new("stream").unwrap(),
            options: WireOptions::empty(),
        },
        &mut inner,
        IggyTimestamp::now(),
    );
    create_topic_in_view(&mut inner, view);
    inner
}

/// Topic "existing" in stream 0, with one partition created in `view`.
fn create_topic_in_view(inner: &mut StreamsInner, view: u32) {
    let _ = StateHandler::apply(
        &PersistedCreateTopicRequest {
            request: CreateTopicRequest {
                stream_id: WireIdentifier::numeric(0),
                partitions_count: 1,
                name: WireName::new("existing").unwrap(),
                options: WireOptions::empty(),
            },
            derived_options: WireOptions::empty(),
            partitions: vec![CreatedPartitionAssignment {
                partition_id: 0,
                consensus_group_id: 1,
                initial_view: view,
                members: Vec::new(),
            }],
        },
        inner,
        IggyTimestamp::now(),
    );
}

/// Committed-reply fixture shaped like the commit path's output: a result
/// section carrying `code`, no payload.
fn committed_reply(
    client: u128,
    request: u64,
    operation: Operation,
    code: u32,
) -> Message<ReplyHeader> {
    let mut body = bytes::BytesMut::new();
    body.extend_from_slice(&1u32.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes());
    body.extend_from_slice(&code.to_le_bytes());
    let header_size = size_of::<ReplyHeader>();
    let total = header_size + body.len();
    let mut reply = Message::<ReplyHeader>::new(total);
    {
        let slice = reply.as_mut_slice();
        slice[header_size..total].copy_from_slice(&body);
        let header = bytemuck::checked::from_bytes_mut::<ReplyHeader>(&mut slice[..header_size]);
        *header = ReplyHeader {
            client,
            request,
            commit: request,
            size: u32::try_from(total).unwrap(),
            command: Command::Reply,
            operation,
            ..Default::default()
        };
    }
    reply
}

fn pat_request(
    client: u128,
    request: u64,
    operation: Operation,
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
            size: u32::try_from(total).unwrap(),
            client,
            session: 1,
            request,
            group: server_common::sharding::METADATA_GROUP,
            ..Default::default()
        };
    }
    message
}

fn create_stream_request(client: u128, request: u64, name: &str) -> Message<RoutedRequestHeader> {
    create_stream_request_with(client, request, name, WireOptions::empty())
}

fn create_stream_request_with(
    client: u128,
    request: u64,
    name: &str,
    options: WireOptions,
) -> Message<RoutedRequestHeader> {
    let body = iggy_binary_protocol::requests::streams::CreateStreamRequest {
        name: WireName::new(name).unwrap(),
        options,
    }
    .to_bytes();
    let header_size = size_of::<RoutedRequestHeader>();
    let total = header_size + body.len();
    let mut message = Message::<RoutedRequestHeader>::new(total);
    {
        let slice = message.as_mut_slice();
        slice[header_size..total].copy_from_slice(&body);
        let header =
            bytemuck::checked::from_bytes_mut::<RoutedRequestHeader>(&mut slice[..header_size]);
        *header = RoutedRequestHeader {
            command: Command::Request,
            operation: Operation::CreateStream,
            size: u32::try_from(total).unwrap(),
            client,
            session: 1,
            request,
            user_id: 0,
            group: server_common::sharding::METADATA_GROUP,
            ..Default::default()
        };
    }
    message
}

/// Bus that records every frame sent to a replica.
#[derive(Debug, Default)]
struct ReplicaSendSpyBus {
    replica_sends: RefCell<Vec<(u8, Frozen<MESSAGE_ALIGN>)>>,
}

#[allow(clippy::future_not_send)]
impl MessageBus for ReplicaSendSpyBus {
    fn track_background(&self, _handle: JoinHandle<()>) {}
    async fn send_to_client(
        &self,
        _client_id: u128,
        _data: impl Into<BusMessage>,
    ) -> Result<(), SendError> {
        Ok(())
    }
    async fn send_to_replica(
        &self,
        replica: u8,
        data: Frozen<MESSAGE_ALIGN>,
    ) -> Result<(), SendError> {
        self.replica_sends.borrow_mut().push((replica, data));
        Ok(())
    }
    fn set_connection_lost_fn(&self, _f: ConnectionLostFn) {}
    fn set_replica_forward_fn(&self, _f: ReplicaForwardFn) {}
}

/// `(target replica, op, prepare checksum)` of every `PrepareOk` sent on `bus`.
fn sent_prepare_oks(bus: &ReplicaSendSpyBus) -> Vec<(u8, u64, u128)> {
    bus.replica_sends
        .borrow()
        .iter()
        .filter_map(|(replica, frame)| {
            bytemuck::checked::try_from_bytes::<PrepareOkHeader>(frame.as_slice())
                .ok()
                .filter(|header| header.command == Command::PrepareOk)
                .map(|header| (*replica, header.op, header.prepare_checksum))
        })
        .collect()
}

/// The ops of the prepares sent to `replica`, in send order.
fn sent_prepare_ops(bus: &ReplicaSendSpyBus, replica: u8) -> Vec<u64> {
    bus.replica_sends
        .borrow()
        .iter()
        .filter(|(target, _)| *target == replica)
        .filter_map(|(_, frame)| {
            let header = frame.as_slice().get(..size_of::<PrepareHeader>())?;
            bytemuck::checked::try_from_bytes::<PrepareHeader>(header)
                .ok()
                .filter(|header| header.command == Command::Prepare)
                .map(|header| header.op)
        })
        .collect()
}

/// Lets the spawned tasks run once.
async fn yield_now() {
    let mut yielded = false;
    std::future::poll_fn(|context| {
        if yielded {
            return std::task::Poll::Ready(());
        }
        yielded = true;
        context.waker().wake_by_ref();
        std::task::Poll::Pending
    })
    .await;
}

/// The primary of a three-replica group at view 0, on a bus that records its
/// sends, with a session for each of `clients` and checkpoints armed.
#[allow(clippy::future_not_send)]
async fn primary_on_spy_bus(
    clients: impl IntoIterator<Item = u128>,
) -> (
    tempfile::TempDir,
    Rc<IggyMetadata<VsrConsensus<ReplicaSendSpyBus>, PrepareJournal, (), TestMux>>,
) {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join(crate::impls::METADATA_DIR)).unwrap();
    let journal = PrepareJournal::open(&dir.path().join("journal.wal"), 0)
        .await
        .unwrap();
    let consensus = VsrConsensus::new(
        1,
        0,
        3,
        server_common::sharding::METADATA_GROUP,
        ReplicaSendSpyBus::default(),
        LocalPipeline::new(),
    );
    consensus.init();
    let metadata = Rc::new(IggyMetadata::new(
        Some(consensus),
        Some(journal),
        None,
        None,
        TestMux::default(),
        Some(dir.path().to_path_buf()),
    ));
    for client in clients {
        metadata
            .client_table
            .borrow_mut()
            .commit_register(client, 0, [0x5a; 32], register_reply(client, 1))
            .unwrap();
    }
    (dir, metadata)
}

/// Adopts `view` from its primary with an empty log, as the shard does on a
/// `StartView`.
fn adopt_view(consensus: &VsrConsensus<ReplicaSendSpyBus>, view: u32) {
    let start_view = StartViewHeader {
        checksum: 0,
        checksum_body: 0,
        cluster: 1,
        size: u32::try_from(size_of::<StartViewHeader>()).unwrap(),
        view,
        release: 0,
        command: Command::StartView,
        replica: consensus.primary_index(view),
        reserved_frame: [0; 66],
        op: 0,
        commit: 0,
        group: server_common::sharding::METADATA_GROUP,
        partition_incarnation: 0,
        reserved: [0; 80],
        incarnation: 0,
    };
    let adopted = consensus.handle_start_view(PlaneKind::Metadata, &start_view, &[]);
    assert!(!adopted.is_empty(), "the replica adopts view {view}");
}

fn session_users() -> Users {
    const LAST_USER: u32 = 9;
    let mut users = UsersInner::new();
    for user_id in 0..=LAST_USER {
        let created = StateHandler::apply(
            &CreateUserRequest {
                username: WireName::new(format!("session-user-{user_id}")).unwrap(),
                password: "hash".to_owned(),
                status: UserStatus::Active.as_code(),
                permissions: None,
                options: WireOptions::empty(),
            },
            &mut users,
            IggyTimestamp::from(1),
        );
        assert_eq!(created.code, 0);
    }
    users.into()
}

/// The user of every session that [`join_group_member`] registers.
const GROUP_MEMBER_USER: u32 = 7;

#[allow(clippy::future_not_send)]
async fn metadata_with_group_member(
    client_id: u128,
) -> (
    tempfile::TempDir,
    IggyMetadata<VsrConsensus<NoopBus>, PrepareJournal, (), TestMux>,
) {
    let mut inner = StreamsInner::new();
    let timestamp = IggyTimestamp::now();
    let _ = StateHandler::apply(
        &CreateStreamRequest {
            name: WireName::new("stream").unwrap(),
            options: WireOptions::empty(),
        },
        &mut inner,
        timestamp,
    );
    let _ = StateHandler::apply(
        &PersistedCreateTopicRequest {
            request: CreateTopicRequest {
                stream_id: WireIdentifier::numeric(0),
                partitions_count: 1,
                name: WireName::new("topic").unwrap(),
                options: WireOptions::empty(),
            },
            derived_options: WireOptions::empty(),
            partitions: vec![CreatedPartitionAssignment {
                partition_id: 0,
                consensus_group_id: 1,
                initial_view: 0,
                members: Vec::new(),
            }],
        },
        &mut inner,
        timestamp,
    );
    let _ = StateHandler::apply(
        &CreateConsumerGroupRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            name: WireName::new("group").unwrap(),
        },
        &mut inner,
        timestamp,
    );
    let dir = tempfile::tempdir().unwrap();
    let mut metadata = metadata_plane();
    metadata.journal = Some(
        PrepareJournal::open(&dir.path().join("journal.wal"), 0)
            .await
            .unwrap(),
    );
    metadata.mux_stm = Rc::new(TestMux::new((session_users(), (inner.into(), ()))));
    metadata.mux_stm.users().ensure_root_user("root", "hash");
    join_group_member(&metadata, client_id).await;
    (dir, metadata)
}

/// Registers `client_id` as a session of `GROUP_MEMBER_USER` and commits
/// its join to the group of [`metadata_with_group_member`].
#[allow(clippy::future_not_send)]
async fn join_group_member(
    metadata: &IggyMetadata<VsrConsensus<NoopBus>, PrepareJournal, (), TestMux>,
    client_id: u128,
) {
    metadata
        .submit_register_in_process(
            client_id,
            GROUP_MEMBER_USER,
            RegisterRequest {
                user_created_at: 1,
                bind_verifier: [0x5a; 32],
            },
        )
        .await
        .unwrap();
    let consensus = metadata.consensus.as_ref().unwrap();
    let join = consensus::seal_prepare_capacity(
        build_prepare_message(
            consensus,
            &RoutedRequestHeader::default(),
            Operation::JoinConsumerGroup,
            &JoinConsumerGroupRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(0),
                group_id: WireIdentifier::numeric(0),
                client_id,
                session: metadata.client_table.borrow().get_epoch(client_id).unwrap(),
            }
            .to_bytes(),
        ),
        metadata.client_table_capacity(),
    );
    let join_op = join.header().op;
    consensus.pipeline_message(PlaneKind::Metadata, &join);
    metadata
        .journal
        .as_ref()
        .unwrap()
        .handle()
        .append(join)
        .await
        .unwrap();
    consensus.advance_commit_max(join_op);
    metadata.resume_stranded_commits().await;
    assert_eq!(consensus.commit_min(), join_op);
    assert_eq!(
        metadata
            .mux_stm
            .streams()
            .consumer_group_memberships(client_id)
            .len(),
        1
    );
}

/// The replica [`transfer_receiver`] builds: replica 0 of a 3-node cluster.
const TRANSFER_RECEIVER: ReplicaIdentity = ReplicaIdentity {
    cluster: 1,
    replica_id: 0,
    replica_count: 3,
};
/// The receiver's WAL head, which its certificate is bound to.
const TRANSFER_RECEIVER_HEAD: u64 = 4;
const CERTIFIED_VIEW: u32 = 0;
/// Recorded but never proven, so a restart proves the WAL through the
/// certificate rather than through the recorded view.
const ADOPTED_VIEW: u32 = 1;
/// The transferred snapshot's op, below the certified head.
const TRANSFER_FLOOR: u64 = 2;
/// Polls before a stepped install counts as hung. Each one waits a millisecond.
const INSTALL_POLL_BUDGET: usize = 10_000;

/// A state transfer receiver whose superblock records [`ADOPTED_VIEW`] and a
/// certificate for [`CERTIFIED_VIEW`] bound to op [`TRANSFER_RECEIVER_HEAD`].
/// `coordinated` gives it a snapshot coordinator. A simulator replica has none,
/// so its superblock pairs no checkpoint.
#[allow(clippy::future_not_send)]
async fn transfer_receiver(
    data_dir: &Path,
    coordinated: bool,
) -> IggyMetadata<VsrConsensus<NoopBus>, PrepareJournal, (), TestMux> {
    const CLIENT: u128 = 1;
    const SESSION: u64 = 1;
    const ACTING_USER: u32 = 7;
    let metadata_dir = data_dir.join(crate::impls::METADATA_DIR);
    std::fs::create_dir_all(&metadata_dir).unwrap();
    let journal = PrepareJournal::open(&metadata_dir.join("journal.wal"), 0)
        .await
        .unwrap();
    let superblock = PingPongSuperblock::open(&metadata_dir).await.unwrap();
    let consensus = VsrConsensus::new(
        TRANSFER_RECEIVER.cluster,
        TRANSFER_RECEIVER.replica_id,
        TRANSFER_RECEIVER.replica_count,
        server_common::sharding::METADATA_GROUP,
        NoopBus,
        LocalPipeline::new(),
    );
    consensus.init();
    let mut md = IggyMetadata::new(
        Some(consensus),
        Some(journal),
        None,
        Some(Rc::new(superblock)),
        TestMux::default(),
        coordinated.then(|| data_dir.to_path_buf()),
    );
    md.client_table
        .borrow_mut()
        .commit_register(
            CLIENT,
            ACTING_USER,
            [0x5a; 32],
            register_reply(CLIENT, SESSION),
        )
        .unwrap();
    {
        let consensus = md.consensus.as_ref().unwrap();
        for request in 1..=TRANSFER_RECEIVER_HEAD {
            let prepare = md
                .prepare_request(create_stream_request(
                    CLIENT,
                    request,
                    &format!("s{request}"),
                ))
                .expect("CreateStream is client-allowed");
            consensus.pipeline_message(PlaneKind::Metadata, &prepare);
            let (op, checksum) = (prepare.header().op, prepare.header().checksum);
            md.journal
                .as_ref()
                .unwrap()
                .handle()
                .append(prepare)
                .await
                .unwrap();
            consensus.extend_history_certificate(op, checksum);
        }
    }
    let consensus = md.consensus.as_mut().unwrap();
    consensus.set_view(ADOPTED_VIEW);
    consensus.set_log_view(ADOPTED_VIEW);
    assert!(
        md.persist_superblock_if_needed(md.consensus.as_ref().unwrap())
            .await
    );
    md
}

/// The metadata directory as a crash at this instant leaves it.
fn crash_image(data_dir: &Path) -> tempfile::TempDir {
    let image = tempfile::tempdir().unwrap();
    let image_dir = image.path().join(crate::impls::METADATA_DIR);
    std::fs::create_dir_all(&image_dir).unwrap();
    for entry in std::fs::read_dir(data_dir.join(crate::impls::METADATA_DIR)).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_file() {
            std::fs::copy(entry.path(), image_dir.join(entry.file_name())).unwrap();
        }
    }
    image
}

/// What a restart from `image` decides about its WAL, and the WAL head left
/// after that decision.
#[allow(clippy::future_not_send)]
async fn restarted_history(image: &Path) -> (HistoryRecovery, Option<u64>) {
    let recovered = recover::<crate::MuxStateMachine<()>>(
        image,
        TRANSFER_RECEIVER,
        DEFAULT_SLOT_COUNT,
        CLIENTS_TABLE_MAX,
        |_| {},
        ChangeLogCapacities::default(),
    )
    .await
    .unwrap();
    (recovered.history, recovered.journal.last_op())
}

/// The unvouched boundary is removed too. A durable snapshot can anchor the
/// certificate through that floor; without it, only the retained WAL can.
const fn transfer_restart(coordinated: bool) -> (HistoryRecovery, Option<u64>) {
    (
        HistoryRecovery::Certified {
            view: CERTIFIED_VIEW,
            truncate_from: Some(if coordinated {
                TRANSFER_FLOOR + 1
            } else {
                TRANSFER_FLOOR
            }),
        },
        Some(TRANSFER_FLOOR - 1),
    )
}
