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
use crate::iggy_index::{IGGY_INDEX_SIZE, IggyIndex, IggyIndexCache};
use crate::iggy_index_reader::IggyIndexReader;
use crate::offset_storage::{OffsetRecord, decode_offset_record, persist_offset};
use crate::poll_plan::{DiskFault, DiskReadOutcome, SealedSegmentHandle};
use crate::{IggyIndexWriter, MessagesWriter};
use bytes::{Bytes, BytesMut};
use compio::io::AsyncWriteAtExt;
use consensus::LocalPipeline;
use iggy_binary_protocol::batch::BATCH_MESSAGE_HEADER_SIZE;
use iggy_binary_protocol::{
    Command, CommitHeader, CommitReplyHeader, ConsensusHeader, HEADER_SIZE, ReplyHeader,
    StartViewHeader, WireConsumer, WireEncode, WireName,
};
use iggy_common::Aes256GcmEncryptor;
use iggy_common::Durability::{Persisted, Replicated};
use journal::DurableAppend;
use journal::durable_storage::{DiskStorage, DurableStorage, OpenMode, RegularFiles, StorageEntry};
use message_bus::{BusMessage, SendError};
use server_common::MESSAGE_ALIGN;
use server_common::iobuf::Owned;
use server_common::send_messages::{
    COMMAND_HEADER_SIZE, IggyMessage, IggyMessageHeader, IggyMessages, SendMessagesOwned,
    decode_batch_slice,
};
use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;

#[cfg(target_os = "linux")]
use std::os::unix::fs::MetadataExt;

use crate::{IggyPartition, PartitionsConfig};

const TEST_CLUSTER: u128 = 1;

const MESSAGE_LESS_CLIENT_ID: u128 = 42;
const MESSAGE_LESS_GROUP_ID: u64 = 7;
const MESSAGE_LESS_EXTERNAL_GROUP_ID: u32 = 5;
const MESSAGE_LESS_EXTERNAL_OFFSET: u64 = 9;

pub(super) fn checksummed_segment_prepare(
    op: u64,
    parent: u128,
    offset: u64,
    payload: &[u8],
) -> Message<PrepareHeader> {
    let namespace = IggyNamespace::new(1, 1, 0);
    let body =
        build_segment_record_with_payload(namespace, offset, Bytes::copy_from_slice(payload));
    let total = std::mem::size_of::<PrepareHeader>() + body.len();
    let mut prepare = Message::<PrepareHeader>::new(total);
    prepare.as_mut_slice()[std::mem::size_of::<PrepareHeader>()..].copy_from_slice(&body);
    prepare.transmute_header(|_, header: &mut PrepareHeader| {
        header.command = Command::Prepare;
        header.operation = Operation::SendMessages;
        header.cluster = TEST_CLUSTER;
        header.group = namespace.inner();
        header.op = op;
        header.parent = parent;
        header.client = 1;
        header.request = op;
        header.session = 1;
        header.retry_capacity = u32::try_from(consensus::PARTITION_DEDUP_CLIENTS_MAX).unwrap();
        header.size = u32::try_from(total).unwrap();
        header.checksum = header.identity_checksum();
    })
}

fn build_segment_record_with_payload(
    namespace: IggyNamespace,
    base_offset: u64,
    payload: Bytes,
) -> Vec<u8> {
    let mut batch = IggyMessages::with_capacity(1);
    batch.push(IggyMessage {
        header: IggyMessageHeader {
            payload_length: u32::try_from(payload.len()).unwrap(),
            ..Default::default()
        },
        payload,
        user_headers: None,
    });
    let mut owned =
        SendMessagesOwned::from_messages(namespace, &batch).expect("build send_messages batch");
    owned.header.base_offset = base_offset;
    owned.header.batch_checksum = owned.header.checksum_for_blob(&owned.blob);

    let mut record = vec![0u8; COMMAND_HEADER_SIZE + owned.blob.len()];
    owned.header.encode_into(&mut record[..COMMAND_HEADER_SIZE]);
    record[COMMAND_HEADER_SIZE..].copy_from_slice(&owned.blob);
    record
}

/// One-message segment record in on-disk layout `[256B command header][blob]`
/// stamped at `base_offset`, with a valid batch checksum so it decodes
/// through `decode_batch_slice` and matches an `Offset` poll.
pub fn build_segment_record(namespace: IggyNamespace, base_offset: u64) -> Vec<u8> {
    build_segment_record_with_payload(namespace, base_offset, Bytes::from_static(b"abcdefgh"))
}

fn test_partition() -> IggyPartition<IggyMessageBus> {
    test_partition_with_replicas(1)
}

pub fn test_partition_with_replicas(replica_count: u8) -> IggyPartition<IggyMessageBus> {
    test_partition_with_pipeline(replica_count, LocalPipeline::new())
}

pub fn test_partition_with_pipeline(
    replica_count: u8,
    pipeline: LocalPipeline,
) -> IggyPartition<IggyMessageBus> {
    let namespace = IggyNamespace::new(1, 1, 0);
    let consensus = VsrConsensus::new(
        TEST_CLUSTER,
        0,
        replica_count,
        namespace.inner(),
        IggyMessageBus::new(0),
        pipeline,
    );
    consensus.init();
    IggyPartition::with_in_memory_storage(
        Arc::new(PartitionStats::default()),
        consensus,
        IggyByteSize::from(1024 * 1024),
    )
}

pub fn repair_config() -> PartitionsConfig {
    PartitionsConfig {
        messages_required_to_save: 1,
        size_of_messages_required_to_save: IggyByteSize::from(1024 * 1024),
        validate_checksum: true,
        segment_size: IggyByteSize::from(1024 * 1024),
        preallocate_segments: false,
        key_context: Arc::default(),
        path_layout: crate::PartitionPathLayout::default(),
    }
}

/// A `SendMessages` request in the shape `convert_request_message` leaves at
/// [`ChecksumMode::Skip`]: canonical batch, `batch_checksum` zeroed.
pub fn checksumless_send_request(
    namespace: IggyNamespace,
    message_count: u32,
) -> Message<RoutedRequestHeader> {
    let mut batch = IggyMessages::with_capacity(message_count as usize);
    for _ in 0..message_count {
        batch.push(IggyMessage {
            header: IggyMessageHeader {
                payload_length: 8,
                ..Default::default()
            },
            payload: Bytes::from_static(b"abcdefgh"),
            user_headers: None,
        });
    }
    let mut owned =
        SendMessagesOwned::from_messages(namespace, &batch).expect("build send_messages batch");
    owned.header.batch_checksum = 0;

    let header_size = std::mem::size_of::<RoutedRequestHeader>();
    let total = header_size + COMMAND_HEADER_SIZE + owned.blob.len();
    let mut message = Message::<RoutedRequestHeader>::new(total);
    let body = &mut message.as_mut_slice()[header_size..];
    owned.header.encode_into(&mut body[..COMMAND_HEADER_SIZE]);
    body[COMMAND_HEADER_SIZE..].copy_from_slice(&owned.blob);
    message.transmute_header(|_, header: &mut RoutedRequestHeader| {
        header.command = Command::Request;
        header.operation = Operation::SendMessages;
        header.client = 1;
        header.session = 1;
        header.request = 1;
        header.group = namespace.inner();
        header.size = u32::try_from(total).expect("request size fits u32");
    })
}

/// In-memory superblock double: records every payload, counts attempts,
/// and injects write failures.
#[derive(Default)]
struct RecordingSuperblock {
    writes: std::cell::RefCell<Vec<Vec<u8>>>,
    attempts: std::cell::Cell<u32>,
    fail_writes: std::cell::Cell<bool>,
}

impl journal::superblock::SuperblockStore for RecordingSuperblock {
    async fn write(&self, payload: &[u8]) -> std::io::Result<()> {
        self.attempts.set(self.attempts.get() + 1);
        if self.fail_writes.get() {
            return Err(std::io::Error::other("injected superblock write failure"));
        }
        self.writes.borrow_mut().push(payload.to_vec());
        Ok(())
    }

    async fn read_latest(&self) -> std::io::Result<journal::superblock::SuperblockContents> {
        Ok(self
            .writes
            .borrow()
            .last()
            .map_or(journal::superblock::SuperblockContents::Empty, |bytes| {
                journal::superblock::SuperblockContents::Present(bytes.clone())
            }))
    }
}

async fn commit_recorded_loopback(partition: &mut IggyPartition<RecordingBus>) {
    partition.drive_persistence().await;
    let mut acknowledgments = Vec::new();
    partition
        .consensus()
        .drain_loopback_into(&mut acknowledgments);
    for ack in acknowledgments {
        partition
            .on_ack(
                ack.try_into_typed::<PrepareOkHeader>().unwrap(),
                &repair_config(),
            )
            .await;
    }
}

/// Partition whose consensus already advanced to `(view, log_view)` with
/// nothing marked durable, as after a view change and before the persist
/// gate runs.
fn partition_at_view(
    view: u32,
    log_view: u32,
) -> IggyPartition<IggyMessageBus, RecordingSuperblock> {
    let namespace = IggyNamespace::new(1, 1, 0);
    let mut consensus = VsrConsensus::new(
        TEST_CLUSTER,
        0,
        3,
        namespace.inner(),
        IggyMessageBus::new(0),
        LocalPipeline::new(),
    );
    consensus.set_view(view);
    consensus.set_log_view(log_view);
    consensus.init_as_backup();
    IggyPartition::with_in_memory_storage(
        Arc::new(PartitionStats::default()),
        consensus,
        IggyByteSize::from(1024 * 1024),
    )
}

/// A backup with a partition directory and offset stores, ready to install
/// what an origin offers. Keep the directory alive while the receiver runs.
fn transfer_receiver() -> (tempfile::TempDir, IggyPartition<RecordingBus>) {
    let directory = tempfile::tempdir().unwrap();
    let (mut receiver, _) = recording_partition_at(1, 3);
    receiver.set_partition_dir(directory.path().to_string_lossy().into_owned());
    set_offset_dirs_under(&mut receiver, directory.path());
    (directory, receiver)
}

/// Create the offset directory of every kind under `root/offsets`, as boot does, and point
/// `partition` at them.
fn set_offset_dirs_under<B: MessageBus, SB: SuperblockStore>(
    partition: &mut IggyPartition<B, SB>,
    root: &std::path::Path,
) {
    for (kind, name) in
        ConsumerKind::ALL
            .into_iter()
            .zip(["consumers", "groups", "external_groups"])
    {
        let dir = root.join("offsets").join(name);
        std::fs::create_dir_all(&dir).unwrap();
        partition.consumer_offset_dirs[kind.index()] = Some(dir.to_string_lossy().into_owned());
    }
}

/// A SOLO partition, the shape the offset reservation is scoped to.
fn solo_recording_partition() -> IggyPartition<IggyMessageBus, RecordingSuperblock> {
    let namespace = IggyNamespace::new(1, 1, 0);
    let consensus = VsrConsensus::new(
        TEST_CLUSTER,
        0,
        1,
        namespace.inner(),
        IggyMessageBus::new(0),
        LocalPipeline::new(),
    );
    consensus.init();
    IggyPartition::with_in_memory_storage(
        Arc::new(PartitionStats::default()),
        consensus,
        IggyByteSize::from(1024 * 1024),
    )
}

async fn publish_empty_retry_checkpoint(
    partition: &IggyPartition<IggyMessageBus, RecordingSuperblock>,
    through: u64,
) {
    let mut checkpoint = Box::new(partition_at_view(0, 0));
    checkpoint.set_partition_dir(partition.partition_dir.clone().unwrap());
    checkpoint.set_created_revision(partition.created_revision());
    checkpoint
        .consensus()
        .restore_commit_state(through, through);
    checkpoint.persist_retry_checkpoint(through).await.unwrap();
}

/// The `offset_frontier` of the most recent recorded write.
fn last_recorded_frontier(store: &RecordingSuperblock) -> u64 {
    let writes = store.writes.borrow();
    let bytes = writes.last().expect("a superblock write landed");
    consensus::VsrState::try_from(bytes.as_slice())
        .expect("recorded payload decodes as a VsrState")
        .offset_frontier
}

fn last_recorded_reservation(store: &RecordingSuperblock) -> u64 {
    let writes = store.writes.borrow();
    let bytes = writes.last().expect("a superblock write landed");
    consensus::VsrState::try_from(bytes.as_slice())
        .expect("recorded payload decodes as a VsrState")
        .offset_reserved
}

fn recorded_state(offset_frontier: u64, offset_reserved: u64) -> consensus::VsrState {
    consensus::VsrState {
        cluster: TEST_CLUSTER,
        replica_id: 0,
        replica_count: 1,
        view: 1,
        log_view: 1,
        commit_max: 0,
        checkpoint_op: 0,
        checkpoint_checksum: 0,
        offset_frontier,
        offset_reserved,
        adopted_view: None,
    }
}

fn test_lease(value: u32) -> NonZeroU32 {
    NonZeroU32::new(value).expect("a nonzero test lease")
}

fn partition_control_request<B: MessageBus>(
    partition: &IggyPartition<B>,
    operation: Operation,
    body: &[u8],
    metadata_op: u64,
) -> Message<RoutedRequestHeader> {
    let size = size_of::<RoutedRequestHeader>() + body.len();
    let mut request = Message::<RoutedRequestHeader>::new(size).transmute_header(
        |_, header: &mut RoutedRequestHeader| {
            header.command = Command::Request;
            header.operation = operation;
            header.client = AUTO_COMMIT_CLIENT_ID;
            header.session = 1;
            header.request = metadata_op;
            header.group = partition.consensus.group();
            header.metadata_watermark = metadata_op;
            header.partition_incarnation = partition.created_revision();
            header.size = u32::try_from(size).unwrap();
        },
    );
    request.as_mut_slice()[size_of::<RoutedRequestHeader>()..].copy_from_slice(body);
    request
}

/// Constructs a result awaiting owner acceptance, without reading messages or
/// updating progress. `Some` adds a placeholder fragment so completion takes
/// the nonempty path; these bytes are never decoded. `None` models an empty read.
fn poll_read_result<B: MessageBus>(
    partition: &IggyPartition<B>,
    consumer: PollingConsumer,
    auto_commit: bool,
    last_matching_offset: Option<u64>,
) -> PollReadResult {
    let mut fragments = PollFragments::new();
    if last_matching_offset.is_some() {
        fragments.push(crate::Fragment::whole(Owned::<4096>::zeroed(8).into()));
    }
    PollReadResult {
        context: PollContext {
            history: partition.poll_history,
            consumer,
            auto_commit,
            owner: match consumer {
                PollingConsumer::ConsumerGroup(group_id, _) => {
                    partition.consumer_group_owner(group_id as u64)
                }
                PollingConsumer::Consumer(..) => None,
            },
            _group_poll: None,
        },
        fragments,
        commit_offset: partition.offsets().commit_offset,
        last_matching_offset,
        message_count: u32::from(last_matching_offset.is_some()),
        truncated: false,
        read_error: None,
    }
}

fn retire_session_request(
    namespace: IggyNamespace,
    identity: SessionIdentity,
) -> Message<RoutedRequestHeader> {
    retire_sessions_request(namespace, &[identity])
}

fn retire_sessions_request(
    namespace: IggyNamespace,
    identities: &[SessionIdentity],
) -> Message<RoutedRequestHeader> {
    let mut body = BytesMut::new();
    for identity in identities {
        identity.encode(&mut body);
    }
    let watermark = identities
        .iter()
        .map(|identity| identity.metadata_watermark)
        .max()
        .unwrap();
    let size = size_of::<RoutedRequestHeader>() + body.len();
    let mut request = Message::<RoutedRequestHeader>::new(size).transmute_header(
        |_, header: &mut RoutedRequestHeader| {
            header.command = Command::Request;
            header.operation = Operation::RetireSession;
            header.cluster = TEST_CLUSTER;
            header.client = message_bus::AUTO_COMMIT_CLIENT_ID;
            header.session = 1;
            header.request = watermark;
            header.metadata_watermark = watermark;
            header.group = namespace.inner();
            header.size = u32::try_from(size).unwrap();
        },
    );
    request.as_mut_slice()[size_of::<RoutedRequestHeader>()..].copy_from_slice(&body);
    request
}

/// A prepare the partition still retains, as a peer serves it in repair.
fn retained_prepare<B: MessageBus>(
    partition: &IggyPartition<B>,
    op: u64,
) -> Message<PrepareHeader> {
    let retained = partition.log.journal().inner.repair_entry(op).unwrap();
    Message::<PrepareHeader>::try_from(Owned::copy_from_slice(retained.as_slice())).unwrap()
}

/// The op the emptied-table marker under `root` names, `None` without one.
fn emptied_marker_op(root: &std::path::Path) -> Option<u64> {
    let bytes = match std::fs::read(root.join("offsets/emptied")) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return None,
        Err(error) => panic!("cannot read the emptied marker: {error}"),
    };
    match decode_offset_record(&bytes) {
        OffsetRecord::Value {
            offset,
            checksummed: true,
        } => Some(offset),
        other => panic!("invalid emptied marker: {other:?}"),
    }
}

/// Complete `read`, replicate any automatic commit it admits, then commit
/// the whole log. Returns the refusal, if any.
async fn complete_poll_and_commit(
    partition: &mut IggyPartition<IggyMessageBus>,
    read: PollReadResult,
) -> Option<IggyError> {
    let refusal = match partition.complete_poll(read) {
        Ok(completion) => {
            if let Some(replication) = completion.replication {
                partition.replicate_poll_completion(replication).await;
            }
            None
        }
        Err(error) => Some(error),
    };
    commit_log(partition).await;
    refusal
}

/// An applied install ends a commit walk, so walk until the head applies.
async fn commit_log(partition: &mut IggyPartition<IggyMessageBus>) {
    let head = partition.consensus.sequencer().current_sequence();
    partition.consensus.advance_commit_max(head);
    for _ in 0..head {
        if partition.consensus.commit_min() == head || partition.fatal().is_some() {
            break;
        }
        partition.commit_journal(&repair_config()).await;
    }
}

fn retirement_payment_request(
    namespace: IggyNamespace,
    identities: Vec<SessionIdentity>,
    paid_through: u64,
) -> Message<RoutedRequestHeader> {
    let body = RetireSessionsRequest {
        identities,
        paid_through,
    }
    .to_bytes();
    let size = size_of::<RoutedRequestHeader>() + body.len();
    let mut request = Message::<RoutedRequestHeader>::new(size).transmute_header(
        |_, header: &mut RoutedRequestHeader| {
            header.command = Command::Request;
            header.operation = Operation::RetireSession;
            header.cluster = TEST_CLUSTER;
            header.client = message_bus::AUTO_COMMIT_CLIENT_ID;
            header.session = 1;
            header.request = paid_through;
            header.metadata_watermark = paid_through;
            header.group = namespace.inner();
            header.size = u32::try_from(size).unwrap();
        },
    );
    request.as_mut_slice()[size_of::<RoutedRequestHeader>()..].copy_from_slice(&body);
    request
}

/// Run the owner's I/O continuations the way the shard lane would, until
/// nothing is left to do.
async fn settle_partition_io<B: MessageBus, SB: SuperblockStore>(
    partition: &mut IggyPartition<B, SB>,
    config: &PartitionsConfig,
) {
    const STEPS_MAX: usize = 64;
    for _ in 0..STEPS_MAX {
        match partition.resume_io(config).await {
            crate::PartitionIoStep::Ready(plan) => {
                execute_partition_io_plan(partition, config, plan).await;
            }
            crate::PartitionIoStep::Progress => {}
            crate::PartitionIoStep::Pending => return,
            _ => panic!("unexpected partition I/O continuation"),
        }
    }
    panic!("partition I/O did not settle in {STEPS_MAX} steps");
}

async fn execute_partition_io_plan<B: MessageBus, SB: SuperblockStore>(
    partition: &mut IggyPartition<B, SB>,
    config: &PartitionsConfig,
    plan: crate::PartitionIoPlan,
) {
    let captured = partition.capture_io(plan, config).unwrap().unwrap();
    let result = captured.job.execute().await;
    partition.accept_io(captured.identity, result).unwrap();
    if let Some(gate) = captured.gate {
        gate.release();
    }
}

fn armed_session(to_op: u64, floor: u64) -> RepairSession {
    armed_fetch_session(to_op, to_op, floor)
}

fn armed_fetch_session(to_op: u64, fetch_to_op: u64, floor: u64) -> RepairSession {
    RepairSession {
        floor: Some(floor),
        ..floorless_session(to_op, fetch_to_op)
    }
}

/// Start the WAL writer without polling it. Until the returned future
/// runs, every lane turn sees the busy writer of a spawned writer task
/// that is still writing.
fn hold_wal_writer(
    persistence: &Rc<PartitionPersistence>,
) -> impl std::future::Future<Output = ()> + use<> {
    persistence.sync();
    assert!(persistence.start());
    Rc::clone(persistence).run()
}

fn primary_transfer_grant<B: MessageBus, SB: SuperblockStore>(
    partition: &IggyPartition<B, SB>,
) -> crate::PreparedPrimaryTransfer {
    const TEST_GRANT_TTL: std::time::Duration = std::time::Duration::from_secs(30);
    crate::PreparedPrimaryTransfer {
        created_revision: partition.created_revision(),
        expected_view: partition.consensus.view(),
        target: 1,
        expires_at: std::time::Instant::now() + TEST_GRANT_TTL,
    }
}

const COMMAND_KIND: u16 = 10;

fn command_prepare(op: u64, parent: u128, kind: u16, value: u64) -> Message<PrepareHeader> {
    let namespace = IggyNamespace::new(1, 1, 0);
    let payload = value.to_le_bytes();
    let body = crate::state_machine::MachineCommand {
        kind: crate::state_machine::CommandKind(kind),
        payload: &payload,
    }
    .encode()
    .unwrap();
    let total = size_of::<PrepareHeader>() + body.len();
    let mut prepare = Message::<PrepareHeader>::new(total);
    prepare.as_mut_slice()[size_of::<PrepareHeader>()..].copy_from_slice(&body);
    prepare.transmute_header(|_, header: &mut PrepareHeader| {
        header.command = Command::Prepare;
        header.operation = Operation::StateMachineCommand;
        header.cluster = TEST_CLUSTER;
        header.group = namespace.inner();
        header.op = op;
        header.parent = parent;
        header.client = message_bus::AUTO_COMMIT_CLIENT_ID;
        header.request = op;
        header.session = 1;
        header.retry_capacity = u32::try_from(consensus::PARTITION_DEDUP_CLIENTS_MAX).unwrap();
        header.size = u32::try_from(total).unwrap();
        header.checksum = header.identity_checksum();
    })
}

/// The primary's Commit heartbeat, handled as the shard's `on_commit` does.
async fn commit_on_heartbeat(
    partition: &mut IggyPartition<RecordingBus>,
    config: &PartitionsConfig,
    commit: u64,
) {
    let group = partition.namespace().inner();
    let heartbeat = Message::<CommitHeader>::new(size_of::<CommitHeader>()).transmute_header(
        |_, header: &mut CommitHeader| {
            header.command = Command::Commit;
            header.cluster = TEST_CLUSTER;
            header.group = group;
            header.commit = commit;
            header.size = u32::try_from(size_of::<CommitHeader>()).unwrap();
        },
    );
    assert!(matches!(
        partition.consensus().handle_commit(heartbeat.header()),
        consensus::CommitOutcome::Advanced
    ));
    partition.commit_journal(config).await;
}

/// A one-message `SendMessages` prepare for `op`, journaled through the
/// replicated-apply path (which stamps offsets and re-checksums), with the
/// sequencer advanced the way `on_replicate` does after a real append.
async fn journal_send_batch(partition: &mut IggyPartition<IggyMessageBus>, op: u64) {
    let record = build_segment_record(IggyNamespace::new(1, 1, 0), 0);
    journal_send_record(partition, op, &record).await;
}

/// [`journal_send_batch`] with the given `record` as the prepare body. The
/// record builder gives each message a random id, so partitions that must
/// write the same bytes share their records.
async fn journal_send_record(
    partition: &mut IggyPartition<IggyMessageBus>,
    op: u64,
    record: &[u8],
) {
    let namespace = IggyNamespace::new(1, 1, 0);
    let header_size = std::mem::size_of::<PrepareHeader>();
    let total = header_size + record.len();
    let mut message = Message::<PrepareHeader>::new(total);
    message.as_mut_slice()[header_size..].copy_from_slice(record);
    let message = message.transmute_header(|_, header: &mut PrepareHeader| {
        header.command = Command::Prepare;
        header.partition_incarnation = partition.created_revision();
        header.retry_capacity = u32::try_from(consensus::PARTITION_DEDUP_CLIENTS_MAX).unwrap();
        header.client = message_bus::AUTO_COMMIT_CLIENT_ID;
        header.session = 1;
        header.request = op;
        header.operation = Operation::SendMessages;
        header.op = op;
        header.timestamp = op;
        header.group = namespace.inner();
        header.size = u32::try_from(total).expect("prepare size fits u32");
    });
    partition
        .apply_replicated_operation(message)
        .await
        .expect("journal send batch");
    partition.consensus().sequencer().set_sequence(op);
}

/// A `StoreConsumerOffset` prepare for `op`, journaled and staged through
/// the replicated-apply path.
async fn journal_store_offset(
    partition: &mut IggyPartition<IggyMessageBus>,
    op: u64,
    consumer_id: u32,
    offset: u64,
) {
    let mut body = StoreConsumerOffsetRequest {
        consumer: WireConsumer::consumer(WireIdentifier::Numeric(consumer_id)),
        stream_id: WireIdentifier::Numeric(1),
        topic_id: WireIdentifier::Numeric(1),
        partition_id: Some(0),
        offset,
        ack: AckLevel::Quorum,
    }
    .to_bytes()
    .to_vec();
    body.extend_from_slice(&0u64.to_le_bytes());
    let header_size = std::mem::size_of::<PrepareHeader>();
    let total = header_size + body.len();
    let mut message = Message::<PrepareHeader>::new(total);
    message.as_mut_slice()[header_size..].copy_from_slice(&body);
    let message = message.transmute_header(|_, header: &mut PrepareHeader| {
        header.command = Command::Prepare;
        header.partition_incarnation = partition.created_revision();
        header.retry_capacity = u32::try_from(consensus::PARTITION_DEDUP_CLIENTS_MAX).unwrap();
        header.client = message_bus::AUTO_COMMIT_CLIENT_ID;
        header.session = 1;
        header.request = op;
        header.operation = Operation::StoreConsumerOffset;
        header.op = op;
        header.group = IggyNamespace::new(1, 1, 0).inner();
        header.size = u32::try_from(total).expect("prepare size fits u32");
    });
    partition
        .apply_replicated_operation(message)
        .await
        .expect("journal store offset");
    partition.consensus().sequencer().set_sequence(op);
}

/// A session at view 0 against peer 0 that has not learned a floor yet.
fn floorless_session(commit_to_op: u64, fetch_to_op: u64) -> RepairSession {
    RepairSession {
        nonce: 1,
        view: 0,
        from_op: 1,
        commit_to_op,
        fetch_to_op,
        floor: None,
        peer: 0,
        idle_ticks: 0,
        floor_pulled_from: 0,
        requested_from: Some(1),
    }
}

fn recording_partition() -> (IggyPartition<RecordingBus>, SentFrames) {
    recording_partition_at(0, 1)
}

/// A primary whose ops 1 to 3 commit an external group offset, an owner
/// install and a history fence, but no message. Keep the directory alive
/// while the origin runs.
async fn message_less_origin() -> (
    tempfile::TempDir,
    IggyPartition<RecordingBus>,
    TransitionPartitionHistoryRequest,
) {
    let directory = tempfile::tempdir().unwrap();
    let (mut origin, _) = recording_partition_at(0, 3);
    origin.set_partition_dir(directory.path().to_string_lossy().into_owned());
    set_offset_dirs_under(&mut origin, directory.path());
    origin
        .on_request(
            store_offset_request(
                MESSAGE_LESS_CLIENT_ID,
                1,
                ConsumerKind::ExternalGroup,
                MESSAGE_LESS_EXTERNAL_GROUP_ID,
                MESSAGE_LESS_EXTERNAL_OFFSET,
                AckLevel::Quorum,
            ),
            None,
        )
        .await;
    origin.consensus.advance_commit_max(1);
    origin.commit_journal(&repair_config()).await;
    install_test_owner(&mut origin, MESSAGE_LESS_GROUP_ID, MESSAGE_LESS_CLIENT_ID).await;
    let transition = TransitionPartitionHistoryRequest {
        incarnation: origin.created_revision(),
        metadata_op: origin.required_metadata_frontier + 1,
    };
    let fence = partition_control_request(
        &origin,
        Operation::TransitionPartitionHistory,
        &transition.to_bytes(),
        transition.metadata_op,
    );
    origin.on_request(fence, None).await;
    origin.consensus.advance_commit_max(3);
    origin.commit_journal(&repair_config()).await;
    assert!(origin.fatal().is_none(), "{:?}", origin.fatal());
    assert_eq!(origin.consensus().commit_min(), 3);
    assert!(origin.consumer_group_owner(MESSAGE_LESS_GROUP_ID).is_some());
    assert_eq!(origin.installed_history_transition(&transition), Some(3));
    assert_eq!(origin.offset_frontier(), 0);
    assert_ne!(origin.dedup().watermarks_sorted(), []);
    (directory, origin, transition)
}

/// One pass of the shard loop over a mounted partition: the maintenance
/// tick, the I/O lane, the WAL writer and its completion frames.
async fn run_shard_round(
    partition: &mut IggyPartition<RecordingBus>,
    completions: &RefCell<Vec<PersistenceCompletion>>,
    config: &PartitionsConfig,
) {
    if partition.needs_persistence_checkpoint() {
        partition.checkpoint_persistence(config).await;
    }
    settle_partition_io(partition, config).await;
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    persistence
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();
    for completion in completions.take() {
        partition.on_persistence_completed(completion, config).await;
    }
}

/// Replicate `origin`'s ops through `commit_op` to a replica that keeps
/// prepare history under `root`, checkpoint them, and restart that replica.
async fn restarted_replica_of(
    origin: &IggyPartition<RecordingBus>,
    root: &std::path::Path,
    replica: u8,
    commit_op: u64,
) -> (PartitionsConfig, IggyPartition<RecordingBus>) {
    let (config, mut donor) = replica_with_prepare_history(root, replica).await;
    commit_replicated_through(&mut donor, origin, &config, commit_op).await;
    let persistence = Rc::clone(donor.persistence.as_ref().unwrap());
    persistence.request_checkpoint();
    donor.checkpoint_persistence(&config).await;
    persistence
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();
    assert!(donor.fatal().is_none(), "{:?}", donor.fatal());
    assert_eq!(persistence.checkpoint_op(), commit_op);
    drop(persistence);
    drop(donor);

    let (config, donor) = replica_with_prepare_history(root, replica).await;
    assert_eq!(donor.consensus().commit_min(), commit_op);
    (config, donor)
}

/// Replicate `origin`'s ops past the commit point of `replica`, a replica
/// that keeps prepare history, and commit them through `commit_op`.
async fn commit_replicated_through(
    replica: &mut IggyPartition<RecordingBus>,
    origin: &IggyPartition<RecordingBus>,
    config: &PartitionsConfig,
    commit_op: u64,
) {
    const COMMIT_ROUNDS_MAX: usize = 8;
    let persistence = Rc::clone(replica.persistence.as_ref().unwrap());
    for op in replica.consensus().commit_min() + 1..=commit_op {
        replica.on_replicate(retained_prepare(origin, op)).await;
    }
    replica.consensus().advance_commit_max(commit_op);
    // Each applied op waits on persistence, so drive it between commits.
    for _ in 0..COMMIT_ROUNDS_MAX {
        replica.commit_journal(config).await;
        persistence
            .drain_with_timeout(compio::runtime::time::sleep)
            .await
            .unwrap();
        replica.drive_persistence().await;
        if replica.consensus().commit_min() == commit_op {
            break;
        }
    }
    assert_eq!(replica.consensus().commit_min(), commit_op);
}

/// A replica of three that keeps prepare history under `root`, opened the way boot
/// opens it, so a second call on the same root restarts it.
async fn replica_with_prepare_history(
    root: &std::path::Path,
    replica: u8,
) -> (PartitionsConfig, IggyPartition<RecordingBus>) {
    let mut config = repair_config();
    config.path_layout.streams_root = root.to_string_lossy().into_owned();
    let partition_dir = config.get_partition_path(1, 1, 0);
    let (mut partition, _) = recording_partition_at(replica, 3);
    partition.set_partition_dir(partition_dir.clone());
    // Boot loads the offset directories that partition creation made.
    set_offset_dirs_under(&mut partition, std::path::Path::new(&partition_dir));
    crate::partition_storage::configure_consumer_offsets_with_storage(
        &DiskStorage,
        &mut partition,
        &config,
        IggyNamespace::new(1, 1, 0),
        0,
    )
    .await
    .unwrap();
    partition.log.retire_front().unwrap();
    partition.install_empty_segment(&config, 0).await.unwrap();
    partition.open_persistence().await.unwrap();
    (config, partition)
}

fn assert_holds_message_less_state(
    receiver: &IggyPartition<RecordingBus>,
    origin: &IggyPartition<RecordingBus>,
    transition: &TransitionPartitionHistoryRequest,
) {
    assert_eq!(receiver.consensus().commit_min(), 3);
    assert_eq!(
        receiver.consumer_group_owner(MESSAGE_LESS_GROUP_ID),
        origin.consumer_group_owner(MESSAGE_LESS_GROUP_ID)
    );
    assert_eq!(receiver.installed_history_transition(transition), Some(3));
    assert_eq!(
        receiver.dedup().watermarks_sorted(),
        origin.dedup().watermarks_sorted()
    );
    assert_eq!(
        receiver.external_group_offset(MESSAGE_LESS_EXTERNAL_GROUP_ID),
        Some(MESSAGE_LESS_EXTERNAL_OFFSET)
    );
    assert_eq!(
        (receiver.offset_frontier(), receiver.mint_frontier()),
        (0, 0)
    );
    assert!(!receiver.offset_space_used());
}

/// Replace the fixture's initial segment with real files and offset stores.
/// Keep the returned directory alive until every read using those files ends.
async fn disk_poll_partition(
    config: &PartitionsConfig,
) -> (tempfile::TempDir, IggyPartition<IggyMessageBus>) {
    Box::pin(disk_poll_partition_with_replicas(config, 1)).await
}

async fn disk_poll_partition_with_replicas(
    config: &PartitionsConfig,
    replica_count: u8,
) -> (tempfile::TempDir, IggyPartition<IggyMessageBus>) {
    let directory = tempfile::tempdir().expect("create partition directory");
    let mut partition = test_partition_with_replicas(replica_count);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.log.retire_front().expect("retire empty segment");
    partition
        .install_empty_segment(config, 0)
        .await
        .expect("install segment with real writers");

    let consumer_path = directory.path().join("consumer_offsets");
    let group_path = directory.path().join("consumer_group_offsets");
    let external_path = directory.path().join("external_group_offsets");
    for path in [&consumer_path, &group_path, &external_path] {
        compio::fs::create_dir_all(path)
            .await
            .expect("create offsets directory");
    }
    partition.configure_consumer_offset_storage(
        [&consumer_path, &group_path, &external_path]
            .map(|path| path.to_string_lossy().into_owned()),
        ConsumerOffsets::with_capacity(1),
        ConsumerGroupOffsets::with_capacity(1),
    );
    (directory, partition)
}

/// A sealed prepare at `op` chained to `parent`, journaled. `request` tells two
/// entries at the same op apart.
async fn journal_sealed(
    partition: &IggyPartition<IggyMessageBus>,
    op: u64,
    parent: u128,
    request: u64,
) -> PrepareHeader {
    let prepare = sealed_prepare(op, parent, request);
    let header = *prepare.header();
    partition
        .log
        .journal()
        .inner
        .append(prepare.into_frozen())
        .await
        .expect("journal append");
    header
}

/// A repaired `SendMessages` prepare with an explicit chain identity, as a
/// serving peer ships it.
fn repaired_send_prepare(op: u64, parent: u128, checksum: u128) -> Message<PrepareHeader> {
    repaired_send_prepare_at(op, op, parent, checksum)
}

/// [`repaired_send_prepare`] with its batch stamped at `base_offset`.
fn repaired_send_prepare_at(
    op: u64,
    base_offset: u64,
    parent: u128,
    checksum: u128,
) -> Message<PrepareHeader> {
    let namespace = IggyNamespace::new(1, 1, 0);
    let record = build_segment_record(namespace, base_offset);
    let header_size = std::mem::size_of::<PrepareHeader>();
    let total = header_size + record.len();
    let mut message = Message::<PrepareHeader>::new(total);
    message.as_mut_slice()[header_size..].copy_from_slice(&record);
    message.transmute_header(|_, header: &mut PrepareHeader| {
        header.command = Command::Prepare;
        header.retry_capacity = u32::try_from(consensus::PARTITION_DEDUP_CLIENTS_MAX).unwrap();
        header.client = message_bus::AUTO_COMMIT_CLIENT_ID;
        header.session = 1;
        header.request = op;
        header.operation = Operation::SendMessages;
        header.op = op;
        header.parent = parent;
        header.checksum = checksum;
        header.group = namespace.inner();
        header.size = u32::try_from(total).expect("prepare size fits u32");
    })
}

/// The delete that offset cleanup sends for a group under its fence.
fn fenced_group_offset_delete(
    fence: &InstallConsumerGroupOwnerRequest,
) -> Message<RoutedRequestHeader> {
    delete_offset_request_with_ack(
        AUTO_COMMIT_CLIENT_ID,
        fence.metadata_op,
        ConsumerKind::ConsumerGroup,
        u32::try_from(fence.group_id).unwrap(),
        AckLevel::Quorum,
    )
    .transmute_header(|mut header, next: &mut RoutedRequestHeader| {
        header.owner_generation = fence.owner.generation;
        *next = header;
    })
}

/// Temp partition directory for the state-transfer fence specs below.
async fn transfer_fence_dir(label: &str) -> String {
    let dir = std::env::temp_dir().join(format!(
        "iggy-transfer-fence-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos(),
    ));
    compio::fs::create_dir_all(&dir)
        .await
        .expect("create temp partition dir");
    dir.to_string_lossy().into_owned()
}

/// Commit the unassigned owner that a group deletion installs as its fence.
async fn install_unassigned_test_owner(
    partition: &mut IggyPartition<IggyMessageBus>,
    group_id: u64,
) -> InstallConsumerGroupOwnerRequest {
    let mut fence = next_test_installation(partition, group_id, 0);
    fence.owner.session = 0;
    partition
        .on_request(owner_install_request(partition, &fence), None)
        .await;
    commit_log(partition).await;
    assert_eq!(partition.consumer_group_owner(group_id), Some(fence.owner));
    fence
}

/// The `parent` of each [`checksummed_segment_prepare`] for ops `1..=last`,
/// indexed by `op - 1`, chained as a primary links them.
pub(super) fn segment_chain_parents(last: u64, payload: &[u8]) -> Vec<u128> {
    let mut parents = Vec::new();
    let mut parent = 0;
    for op in 1..=last {
        parents.push(parent);
        parent = checksummed_segment_prepare(op, parent, 0, payload)
            .header()
            .checksum;
    }
    parents
}

/// Client-facing bus that records every `send_to_client` frame so tests
/// can assert on reply bytes without a connection registry (whose slot
/// guard would borrow the partition across `on_request(&mut self)`).
#[derive(Debug, Default)]
pub(super) struct RecordingBus {
    sent_to_clients: Rc<RefCell<Vec<(u128, Frozen<MESSAGE_ALIGN>)>>>,
    sent_to_replicas: Rc<RefCell<Vec<(u8, Frozen<MESSAGE_ALIGN>)>>>,
}

impl MessageBus for RecordingBus {
    fn track_background(&self, _handle: message_bus::JoinHandle<()>) {}

    async fn send_to_client(
        &self,
        client_id: u128,
        data: impl Into<BusMessage>,
    ) -> Result<(), SendError> {
        self.sent_to_clients
            .borrow_mut()
            .push((client_id, data.into().into_contiguous()));
        Ok(())
    }

    async fn send_to_replica(
        &self,
        replica: u8,
        data: Frozen<MESSAGE_ALIGN>,
    ) -> Result<(), SendError> {
        self.sent_to_replicas.borrow_mut().push((replica, data));
        Ok(())
    }

    fn set_connection_lost_fn(&self, _f: message_bus::ConnectionLostFn) {}
    fn set_replica_forward_fn(&self, _f: message_bus::ReplicaForwardFn) {}
}

pub(super) type SentFrames = Rc<RefCell<Vec<(u128, Frozen<MESSAGE_ALIGN>)>>>;

pub(super) fn recording_partition_at(
    replica: u8,
    replica_count: u8,
) -> (IggyPartition<RecordingBus>, SentFrames) {
    recording_partition_with_pipeline(replica, replica_count, LocalPipeline::new())
}

/// The offer `partition` serves once the I/O turn pinned its plan.
async fn serve_offer(
    partition: &mut IggyPartition<IggyMessageBus>,
) -> Rc<crate::state_transfer::PartitionStateTransferOffer> {
    let config = repair_config();
    assert!(matches!(
        partition.state_transfer_offer(&config).await,
        Err(PartitionTransferUnavailable::FlushPending)
    ));
    settle_partition_io(partition, &config).await;
    partition.state_transfer_offer(&config).await.unwrap()
}

/// `count` one-message records. Each call gives its messages new random
/// ids, so two calls give records of the same lengths but other bytes.
fn segment_records(count: u64) -> Vec<Vec<u8>> {
    (0..count)
        .map(|_| build_segment_record(IggyNamespace::new(1, 1, 0), 0))
        .collect()
}

/// One-message batches each segment of [`partition_with_sealed_segments`]
/// holds before it rolls.
const BATCHES_PER_SEGMENT: u64 = 2;

/// Ops the receivers of the reuse tests commit: two sealed segments and
/// one batch in the active segment.
const RECEIVER_OPS: u64 = 5;

/// Ops the origin of the reuse tests commits: three sealed segments and
/// one batch in the active segment.
const ORIGIN_OPS: u64 = 7;

/// Every write to this device fails with `ENOSPC`, which is how the
/// persist failure cases below inject a fault into one half of the flush
/// without any production-side plumbing.
#[cfg(target_os = "linux")]
const DEV_FULL: &str = "/dev/full";

/// A primary on disk whose segments roll every [`BATCHES_PER_SEGMENT`]
/// batches, with one op per record committed and written to its segments.
async fn partition_with_sealed_segments(
    records: &[Vec<u8>],
) -> (tempfile::TempDir, IggyPartition<IggyMessageBus>) {
    let config = repair_config();
    let (directory, mut partition) = Box::pin(disk_poll_partition_with_replicas(&config, 3)).await;
    let batch_len = build_segment_record(partition.namespace(), 0).len() as u64;
    let segment_size = IggyByteSize::from(BATCHES_PER_SEGMENT * batch_len);
    partition.runtime_options.segment_size = Some(segment_size);
    partition.log.active_segment_mut().max_size = segment_size;
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    Box::pin(commit_records(&mut partition, 1, records)).await;
    assert_eq!(partition.consensus().commit_min(), records.len() as u64);
    (directory, partition)
}

/// Journals `records` as the ops from `first_op` on, and commits and
/// writes each one before the next.
async fn commit_records(
    partition: &mut IggyPartition<IggyMessageBus>,
    first_op: u64,
    records: &[Vec<u8>],
) {
    let config = repair_config();
    for (op, record) in (first_op..).zip(records) {
        journal_send_record(partition, op, record).await;
        partition.consensus().advance_commit_max(op);
        settle_partition_io(partition, &config).await;
    }
}

#[cfg(target_os = "linux")]
fn log_inode(directory: &Path, start_offset: u64) -> u64 {
    std::fs::metadata(directory.join(format!("{start_offset:020}.log")))
        .unwrap()
        .ino()
}

/// Runs a reuse scan the way a session does: in a detached task, and then
/// accepted on the pump. Returns what the pump accepted.
async fn reuse_scan(
    receiver: &mut IggyPartition<IggyMessageBus>,
    manifest: &[consensus::StateArtifact],
) -> Vec<(u32, crate::state_transfer::StagedSegmentMeta)> {
    let scan = receiver.begin_reuse_scan(manifest);
    let _ = receiver.staging_work_ended.take().unwrap().await;
    let scanned = scan.result.borrow_mut().take().unwrap();
    receiver.accept_reuse_scan(scanned)
}

/// The bytes of each segment file in the partition directory, in offset
/// order.
fn segment_file_bytes(directory: &Path) -> Vec<Vec<u8>> {
    let mut paths: Vec<_> = std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "log"))
        .collect();
    paths.sort();
    paths
        .iter()
        .map(|path| std::fs::read(path).unwrap())
        .collect()
}

/// The offered bytes of each segment of `offer`.
fn offered_segment_bytes(
    offer: &crate::state_transfer::PartitionStateTransferOffer,
) -> Vec<Vec<u8>> {
    offer
        .segments
        .iter()
        .map(|source| {
            let mut bytes = std::fs::read(&source.log_path).unwrap();
            bytes.truncate(usize::try_from(source.entry.len).unwrap());
            bytes
        })
        .collect()
}

/// Installs `offer` the way the shard does: the reuse scan first, then a
/// spill of each segment that the scan did not adopt. Returns the manifest
/// indexes that the scan adopted.
async fn install_offer_with_reuse(
    receiver: &mut IggyPartition<IggyMessageBus>,
    offer: &crate::state_transfer::PartitionStateTransferOffer,
) -> Vec<u32> {
    let mut staged = vec![None; offer.segments.len()];
    let mut adopted = Vec::new();
    for (index, meta) in reuse_scan(receiver, &offer.manifest()).await {
        adopted.push(index);
        staged[index as usize] = Some(meta);
    }
    for (slot, source) in staged.iter_mut().zip(&offer.segments) {
        if slot.is_none() {
            let mut bytes = std::fs::read(&source.log_path).unwrap();
            bytes.truncate(usize::try_from(source.entry.len).unwrap());
            *slot = Some(
                receiver
                    .spill_transfer_segment(&source.entry, bytes, None)
                    .await
                    .unwrap(),
            );
        }
    }
    receiver
        .queue_state_transfer_install(
            offer.commit_op,
            staged.into_iter().map(Option::unwrap).collect(),
            &offer.offsets.1,
            0,
        )
        .unwrap();
    Box::pin(finish_install_io(receiver)).await.unwrap();
    adopted
}

async fn journal_prepare(partition: &IggyPartition<IggyMessageBus>, op: u64, operation: Operation) {
    let size = std::mem::size_of::<PrepareHeader>();
    let prepare =
        Message::<PrepareHeader>::new(size).transmute_header(|_, header: &mut PrepareHeader| {
            header.command = Command::Prepare;
            header.retry_capacity = u32::try_from(consensus::PARTITION_DEDUP_CLIENTS_MAX).unwrap();
            header.client = message_bus::AUTO_COMMIT_CLIENT_ID;
            header.session = 1;
            header.request = op;
            header.op = op;
            header.operation = operation;
            header.size = u32::try_from(size).expect("prepare header size fits in u32");
        });
    partition
        .log
        .journal()
        .inner
        .append(prepare.into_frozen())
        .await
        .expect("journal append");
}

fn sealed_prepare(op: u64, parent: u128, request: u64) -> Message<PrepareHeader> {
    let size = std::mem::size_of::<PrepareHeader>();
    Message::<PrepareHeader>::new(size).transmute_header(|_, header: &mut PrepareHeader| {
        header.command = Command::Prepare;
        header.client = message_bus::AUTO_COMMIT_CLIENT_ID;
        header.session = 1;
        header.request = request;
        header.op = op;
        header.parent = parent;
        header.operation = Operation::CreateStream;
        header.size = u32::try_from(size).expect("prepare header size fits in u32");
        header.checksum = header.identity_checksum();
    })
}

fn walked_ops(partition: &IggyPartition<IggyMessageBus>) -> Vec<u64> {
    partition
        .collect_committable_from_journal(COMMIT_WALK_OPS_MAX, &repair_config())
        .into_iter()
        .map(|header| header.op)
        .collect()
}

/// The successor of the installed owner, or the first owner of `group_id`.
fn next_test_installation<B: MessageBus>(
    partition: &IggyPartition<B>,
    group_id: u64,
    client_id: u128,
) -> InstallConsumerGroupOwnerRequest {
    InstallConsumerGroupOwnerRequest {
        incarnation: partition.created_revision(),
        group_id,
        owner: ConsumerGroupOwner {
            client_id,
            session: 1,
            generation: partition
                .consumer_group_owner(group_id)
                .map_or(1, |owner| owner.generation + 1),
        },
        metadata_op: partition.required_metadata_frontier + 1,
    }
}

fn history_send_request<B: MessageBus>(
    partition: &IggyPartition<B>,
    request: u64,
    metadata_op: u64,
) -> Message<RoutedRequestHeader> {
    let mut send = checksumless_send_request(partition.namespace(), 1);
    send.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
        .copy_from_slice(&build_segment_record(partition.namespace(), 0));
    send.transmute_header(|mut header, next: &mut RoutedRequestHeader| {
        header.request = request;
        header.metadata_watermark = metadata_op;
        *next = header;
    })
}

fn owner_install_request<B: MessageBus>(
    partition: &IggyPartition<B>,
    installation: &InstallConsumerGroupOwnerRequest,
) -> Message<RoutedRequestHeader> {
    partition_control_request(
        partition,
        Operation::InstallConsumerGroupOwner,
        &installation.to_bytes(),
        installation.metadata_op,
    )
}

fn owner_retirement_request<B: MessageBus>(
    partition: &IggyPartition<B>,
    catalog: &RetireConsumerGroupOwnersRequest,
) -> Message<RoutedRequestHeader> {
    partition_control_request(
        partition,
        Operation::RetireConsumerGroupOwners,
        &catalog.to_bytes(),
        catalog.metadata_op,
    )
}

/// Creates an empty partition with the supplied consensus queue capacities.
/// Its bus records sends without contacting clients or replicas; the returned
/// frame list contains client replies only.
fn recording_partition_with_pipeline(
    replica: u8,
    replica_count: u8,
    pipeline: LocalPipeline,
) -> (IggyPartition<RecordingBus>, SentFrames) {
    let namespace = IggyNamespace::new(1, 1, 0);
    let bus = RecordingBus::default();
    let sent_to_clients = bus.sent_to_clients.clone();
    let consensus = VsrConsensus::new(
        TEST_CLUSTER,
        replica,
        replica_count,
        namespace.inner(),
        bus,
        pipeline,
    );
    consensus.init();
    let mut partition = IggyPartition::with_in_memory_storage(
        Arc::new(PartitionStats::default()),
        consensus,
        IggyByteSize::from(1024 * 1024),
    );
    partition.set_runtime_options(TopicRuntimeOptions {
        durability: iggy_common::Durability::Persisted,
        consumer_offset_durability: iggy_common::Durability::Persisted,
        ..Default::default()
    });
    (partition, sent_to_clients)
}

/// A primary that committed and flushed op 1 after its WAL write, with
/// offset stores under its partition directory. Returns the checksum of
/// op 1 to chain later prepares.
async fn partition_with_committed_send(
    root: &std::path::Path,
    durability: iggy_common::Durability,
    replica_count: u8,
) -> (
    IggyPartition<RecordingBus>,
    SentFrames,
    PartitionsConfig,
    u128,
) {
    let mut config = repair_config();
    config.path_layout.streams_root = root.to_string_lossy().into_owned();
    let directory = std::path::PathBuf::from(config.get_partition_path(1, 1, 0));
    std::fs::create_dir_all(&directory).unwrap();
    let (mut partition, replies) = recording_partition_at(0, replica_count);
    partition.set_partition_dir(directory.to_string_lossy().into_owned());
    set_offset_dirs_under(&mut partition, &directory);
    partition.runtime_options.durability = durability;
    partition.runtime_options.consumer_offset_durability = durability;
    partition.open_persistence().await.unwrap();
    partition.log.retire_front().unwrap();
    partition.install_empty_segment(&config, 0).await.unwrap();
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    let prepare = checksummed_segment_prepare(1, 0, 0, b"first");
    let header = *prepare.header();
    persistence
        .append(prepare.clone().into_frozen(), durability.is_persisted())
        .unwrap();
    assert!(persistence.start());
    Rc::clone(&persistence).run().await;
    partition.consensus.with_pipeline_mut(|pipeline| {
        pipeline.push(PipelineEntry::new(header));
    });
    partition
        .append_repaired_send_messages(prepare)
        .await
        .unwrap();
    partition.consensus.advance_commit_max(header.op);
    partition.commit_journal(&config).await;
    assert!(partition.fatal().is_none());
    assert_eq!(partition.consensus.commit_min(), header.op);
    assert_eq!(partition.log.journal().inner.resident_count(), 0);
    (partition, replies, config, header.checksum)
}

fn delete_offset_request(
    client_id: u128,
    request_id: u64,
    consumer_id: u32,
) -> Message<RoutedRequestHeader> {
    delete_offset_request_with_ack(
        client_id,
        request_id,
        ConsumerKind::Consumer,
        consumer_id,
        AckLevel::Quorum,
    )
}

fn delete_offset_request_with_ack(
    client_id: u128,
    request_id: u64,
    kind: ConsumerKind,
    consumer_id: u32,
    ack: AckLevel,
) -> Message<RoutedRequestHeader> {
    let body = DeleteConsumerOffsetRequest {
        consumer: WireConsumer {
            kind: kind.as_code(),
            id: WireIdentifier::Numeric(consumer_id),
        },
        stream_id: WireIdentifier::Numeric(1),
        topic_id: WireIdentifier::Numeric(1),
        partition_id: Some(0),
        ack,
    }
    .to_bytes();
    let header_size = std::mem::size_of::<RoutedRequestHeader>();
    let total = header_size + body.len();
    let mut message = Message::<RoutedRequestHeader>::new(total);
    message.as_mut_slice()[header_size..].copy_from_slice(&body);
    message.transmute_header(|_, header: &mut RoutedRequestHeader| {
        header.command = Command::Request;
        header.operation = Operation::DeleteConsumerOffset;
        header.client = client_id;
        header.session = 1;
        header.request = request_id;
        header.group = IggyNamespace::new(1, 1, 0).inner();
        header.size = u32::try_from(total).expect("request size fits u32");
    })
}

fn store_offset_request(
    client_id: u128,
    request_id: u64,
    kind: ConsumerKind,
    consumer_id: u32,
    offset: u64,
    ack: AckLevel,
) -> Message<RoutedRequestHeader> {
    let body = StoreConsumerOffsetRequest {
        consumer: WireConsumer {
            kind: kind.as_code(),
            id: WireIdentifier::Numeric(consumer_id),
        },
        stream_id: WireIdentifier::Numeric(1),
        topic_id: WireIdentifier::Numeric(1),
        partition_id: Some(0),
        offset,
        ack,
    }
    .to_bytes();
    let header_size = std::mem::size_of::<RoutedRequestHeader>();
    let total = header_size + body.len();
    let mut message = Message::<RoutedRequestHeader>::new(total);
    message.as_mut_slice()[header_size..].copy_from_slice(&body);
    message.transmute_header(|_, header: &mut RoutedRequestHeader| {
        header.command = Command::Request;
        header.operation = Operation::StoreConsumerOffset;
        header.client = client_id;
        header.session = 1;
        header.request = request_id;
        header.group = IggyNamespace::new(1, 1, 0).inner();
        header.size = u32::try_from(total).expect("request size fits u32");
    })
}

pub(super) async fn install_test_owner<B: MessageBus>(
    partition: &mut IggyPartition<B>,
    group_id: u64,
    client_id: u128,
) -> u64 {
    let installation = next_test_installation(partition, group_id, client_id);
    let before = partition.consensus.sequencer().current_sequence();
    partition
        .on_request(owner_install_request(partition, &installation), None)
        .await;
    let op = partition.consensus.sequencer().current_sequence();
    assert_eq!(op, before + 1);
    assert_eq!(
        partition.installed_consumer_group_owner(&installation),
        None
    );
    partition.consensus.advance_commit_max(op);
    partition.commit_journal(&repair_config()).await;
    assert_eq!(
        partition.installed_consumer_group_owner(&installation),
        Some(op)
    );
    op
}

const FIRST_PAYLOAD: &[u8] = b"first-chunk";
const SECOND_PAYLOAD: &[u8] = b"second-chunk-is-longer";

/// Partition whose active segment carries real writers over the given
/// paths, both with fsync on. Point either path at [`DEV_FULL`] to make
/// that half's save fail.
struct PersistFixture {
    partition: IggyPartition<IggyMessageBus>,
    log_cursor: Rc<AtomicU64>,
    index_cursor: Rc<AtomicU64>,
}

impl PersistFixture {
    async fn new(log_path: &str, index_path: &str) -> Self {
        Self::with_replicas(log_path, index_path, 1).await
    }

    async fn with_replicas(log_path: &str, index_path: &str, replica_count: u8) -> Self {
        let log_cursor = Rc::new(AtomicU64::new(0));
        let index_cursor = Rc::new(AtomicU64::new(0));
        let messages_writer = MessagesWriter::new(log_path, log_cursor.clone(), true, false, None)
            .await
            .expect("open segment log writer");
        let index_writer = IggyIndexWriter::new(index_path, index_cursor.clone(), true, false)
            .await
            .expect("open segment index writer");

        let mut partition = test_partition_with_replicas(replica_count);
        partition.log.retire_back();
        partition.log.add_persisted_segment(
            Segment::new(0, IggyByteSize::from(1024 * 1024_u64)),
            SegmentStorage::default(),
            Some(Rc::new(messages_writer)),
            Some(Rc::new(index_writer)),
        );

        Self {
            partition,
            log_cursor,
            index_cursor,
        }
    }

    async fn persist(&mut self, payload: &[u8], offset: u64) -> Result<(), IggyError> {
        let result = self.capture(payload, offset)?.execute().await;
        self.partition.accept_materialization(result)
    }

    fn capture(
        &mut self,
        payload: &[u8],
        offset: u64,
    ) -> Result<crate::MaterializationIoJob, IggyError> {
        let index = IggyIndex::new(offset, offset + 1, self.segment_size());
        self.partition
            .prepare_materialization(
                vec![prepare_framed(payload)],
                IggyIndexCache::serialize(&index),
                1,
                0,
                true,
            )?
            .ok_or(IggyError::CannotWriteToFile)
    }

    fn cursors(&self) -> (u64, u64) {
        (
            self.log_cursor.load(Ordering::Relaxed),
            self.index_cursor.load(Ordering::Relaxed),
        )
    }

    fn segment_size(&self) -> u64 {
        self.partition.log.active_segment().size.as_bytes_u64()
    }
}

/// Journaled entry in the shape the persist path expects: a `PrepareHeader`
/// prefix it strips, followed by the bytes that reach the segment file.
fn prepare_framed(payload: &[u8]) -> Frozen<4096> {
    let mut bytes = vec![0u8; size_of::<PrepareHeader>()];
    bytes.extend_from_slice(payload);
    Owned::<4096>::copy_from_slice(&bytes).into()
}

fn file_len(path: &std::path::Path) -> u64 {
    std::fs::metadata(path).expect("stat file").len()
}

/// Runs the tick's superblock step between install steps, as the shard does.
async fn finish_install_io<SB: SuperblockStore>(
    partition: &mut IggyPartition<IggyMessageBus, SB>,
) -> Result<
    crate::state_transfer::PartitionInstallOutcome,
    crate::state_transfer::PartitionInstallError,
> {
    const STEPS_MAX: usize = 128;
    let config = repair_config();
    for _ in 0..STEPS_MAX {
        let _ = partition.persist_superblock_if_needed().await;
        match partition.resume_io(&config).await {
            crate::PartitionIoStep::Ready(plan) => {
                execute_partition_io_plan(partition, &config, plan).await;
            }
            crate::PartitionIoStep::Progress => {}
            crate::PartitionIoStep::Pending => {
                if let Some(persistence) = &partition.persistence {
                    persistence
                        .drain_with_timeout(compio::runtime::time::sleep)
                        .await
                        .unwrap();
                } else {
                    compio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
            }
            crate::PartitionIoStep::InstallFinished { outcome, .. } => return outcome,
            _ => panic!("unexpected install continuation"),
        }
    }
    panic!("install did not finish in {STEPS_MAX} steps");
}

/// [`DiskStorage`] whose read-only opens and link-following existence
/// checks of one path fail with `EIO`.
struct FailingPathStorage(std::path::PathBuf);

impl FailingPathStorage {
    fn check(&self, path: &std::path::Path) -> std::io::Result<()> {
        if path == self.0 {
            return Err(std::io::Error::from_raw_os_error(
                nix::errno::Errno::EIO as i32,
            ));
        }
        Ok(())
    }
}

impl DurableStorage for FailingPathStorage {
    type File = <DiskStorage as DurableStorage>::File;

    async fn open(&self, path: &std::path::Path, mode: OpenMode) -> std::io::Result<Self::File> {
        if mode == OpenMode::Read {
            self.check(path)?;
        }
        DiskStorage.open(path, mode).await
    }

    async fn create_directories(&self, path: &std::path::Path) -> std::io::Result<()> {
        DiskStorage.create_directories(path).await
    }

    async fn sync_directory(&self, path: &std::path::Path) -> std::io::Result<()> {
        DiskStorage.sync_directory(path).await
    }

    async fn rename(
        &self,
        source: &std::path::Path,
        target: &std::path::Path,
    ) -> std::io::Result<()> {
        DiskStorage.rename(source, target).await
    }

    async fn remove_file(&self, path: &std::path::Path) -> std::io::Result<()> {
        DiskStorage.remove_file(path).await
    }

    async fn hard_link(
        &self,
        source: &std::path::Path,
        target: &std::path::Path,
    ) -> std::io::Result<()> {
        DiskStorage.hard_link(source, target).await
    }

    async fn exists(&self, path: &std::path::Path) -> std::io::Result<bool> {
        DiskStorage.exists(path).await
    }

    async fn exists_following_links(&self, path: &std::path::Path) -> std::io::Result<bool> {
        self.check(path)?;
        DiskStorage.exists_following_links(path).await
    }

    async fn entries(&self, path: &std::path::Path) -> std::io::Result<Vec<StorageEntry>> {
        DiskStorage.entries(path).await
    }

    async fn regular_files(&self, path: &std::path::Path) -> std::io::Result<RegularFiles> {
        DiskStorage.regular_files(path).await
    }

    async fn remove_tree(&self, path: &std::path::Path) -> std::io::Result<()> {
        DiskStorage.remove_tree(path).await
    }
}

async fn partition_with_pending_durable_ack() -> (
    tempfile::TempDir,
    IggyPartition<RecordingBus>,
    PrepareHeader,
) {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition_at(0, 3);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.runtime_options.durability = iggy_common::Durability::Persisted;
    let log_path = directory.path().join("00000000000000000000.log");
    let index_path = directory.path().join("00000000000000000000.index");
    partition.log.messages_writers_mut()[0] = Some(Rc::new(
        MessagesWriter::new(
            log_path.to_str().unwrap(),
            Rc::new(AtomicU64::new(0)),
            true,
            false,
            None,
        )
        .await
        .unwrap(),
    ));
    partition.log.index_writers_mut()[0] = Some(Rc::new(
        IggyIndexWriter::new(
            index_path.to_str().unwrap(),
            Rc::new(AtomicU64::new(0)),
            true,
            false,
        )
        .await
        .unwrap(),
    ));
    partition.open_persistence().await.unwrap();
    let prepare = checksummed_segment_prepare(1, 0, 0, b"parked");
    let header = *prepare.header();
    partition.consensus().sequencer().set_sequence(header.op);
    partition
        .consensus()
        .set_last_prepare_checksum(header.checksum);
    partition
        .log
        .journal()
        .inner
        .append(prepare.clone().into_frozen())
        .await
        .unwrap();
    let persistence = partition.persistence.as_ref().unwrap();
    persistence.append(prepare.into_frozen(), true).unwrap();
    assert!(!partition.register_rebuilt_ack(&header));
    partition.start_persistence();
    persistence
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();

    (directory, partition, header)
}

mod consensus_tests;
mod consumer_group_ownership;
mod disk_poll;
mod empty_offsets;
mod history;
mod materialization;
mod materialization_io;
mod materialization_tail;
mod offset_barriers;
mod offset_lifecycle;
mod offset_recovery;
mod offset_replication;
mod offset_reservations;
mod offset_storage;
mod offsets;
mod persistence_checkpoint;
mod repair;
mod repair_floor;
mod repair_protocol;
mod retention;
mod session_retirement;
mod state_machines;
mod state_transfer_offer;
mod state_transfer_segments;
mod superblock;
mod transfer_ownership;
mod wal;
