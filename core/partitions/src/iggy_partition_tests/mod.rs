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

fn test_partition() -> IggyPartition<IggyMessageBus> {
    test_partition_with_replicas(1)
}

fn test_partition_with_replicas(replica_count: u8) -> IggyPartition<IggyMessageBus> {
    let namespace = IggyNamespace::new(1, 1, 0);
    let consensus = VsrConsensus::new(
        TEST_CLUSTER,
        0,
        replica_count,
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

fn repair_config() -> PartitionsConfig {
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
fn checksumless_send_request(
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
                ack.try_into_typed::<iggy_binary_protocol::PrepareOkHeader>()
                    .unwrap(),
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

#[path = "partition_history.rs"]
mod partition_history;
#[path = "partition_offsets.rs"]
mod partition_offsets;
#[path = "partition_ownership.rs"]
mod partition_ownership;
#[path = "partition_persistence.rs"]
mod partition_persistence;
#[path = "partition_repair.rs"]
mod partition_repair;
#[path = "partition_sessions.rs"]
mod partition_sessions;
#[path = "partition_superblock.rs"]
mod partition_superblock;
#[path = "retention_tests.rs"]
mod retention_tests;

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

#[compio::test]
async fn referenced_prepare_reads_reject_batch_and_payload_corruption_without_body_checksum() {
    for corrupt_at in [
        0,
        COMMAND_HEADER_SIZE,
        COMMAND_HEADER_SIZE + BATCH_MESSAGE_HEADER_SIZE,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let wal = directory.path().join("prepares-0");
        let prepare = checksummed_segment_prepare(1, 0, 0, b"payload");
        assert_eq!(prepare.header().checksum_body, 0);
        let group = prepare.header().group;
        let mut journal = journal::PartitionPrepareJournal::open(&wal, group, 0)
            .await
            .unwrap();
        journal
            .enable_segment_storage(
                journal::partition_journal::SegmentPosition::default(),
                1024 * 1024,
            )
            .await
            .unwrap();
        journal.append(prepare.clone().into_frozen()).await.unwrap();
        assert_eq!(
            journal.prepares().await.unwrap()[0].as_slice(),
            prepare.as_slice()
        );
        let path = directory.path().join("00000000000000000000.log");
        let mut body = std::fs::read(&path).unwrap();
        body[corrupt_at] ^= 1;
        std::fs::write(&path, body).unwrap();
        assert_eq!(
            journal.prepares().await.unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
        drop(journal);
        assert!(
            journal::PartitionPrepareJournal::open(&wal, group, 0)
                .await
                .is_err()
        );
    }
}

//     #[compio::test]
//     async fn replicated_transfer_keeps_the_evicted_checkpoint_checksum_with_an_inflight_tail() {
//         let origin_directory = tempfile::tempdir().unwrap();
//         let (mut origin, _) = recording_partition_at(0, 3);
//         origin.set_partition_dir(origin_directory.path().to_string_lossy().into_owned());
//         let committed = checksummed_segment_prepare(1, 0, 0, b"committed");
//         let inflight = checksummed_segment_prepare(2, committed.header().checksum, 1, b"inflight");
//         for prepare in [&committed, &inflight] {
//             origin
//                 .log
//                 .journal()
//                 .inner
//                 .append(prepare.clone().into_frozen())
//                 .await
//                 .unwrap();
//         }
//         origin.consensus().sequencer().set_sequence(2);
//         origin
//             .consensus()
//             .set_last_prepare_checksum(inflight.header().checksum);
//         origin.consensus().advance_commit_max(1);
//         origin.consensus().advance_commit_min(1);
//         origin
//             .dedup_mut()
//             .commit_capacity(usize::try_from(committed.header().retry_capacity).unwrap())
//             .unwrap();
//         // Retention can remove all polled segments while the repair ring still
//         // serves the checkpoint prepare and the primary keeps accepting writes.
//         origin.offset_space.committed_seeded = true;
//         origin.offset.store(0, Ordering::Relaxed);
//         origin.log.journal().inner.evict_prefix(1);
//         assert!(origin.persistence.is_none());
//         assert!(origin.log.journal().inner.header_by_op(1).is_none());
//         assert!(origin.log.journal().inner.repair_entry(1).is_some());
//
//         let offer = origin.state_transfer_offer(&repair_config()).await.unwrap();
//         let offsets = crate::state_transfer::ConsumerOffsetsWire::decode(&offer.offsets.1).unwrap();
//         assert_eq!(offsets.dedup_capacity, origin.dedup().capacity());
//         assert_eq!(offsets.prepare_checksum, Some(committed.header().checksum));
//         assert_eq!(offsets.checkpoint_prepare, committed.as_slice());
//         let (_receiver_directory, mut receiver) = transfer_receiver();
//         receiver
//             .install_state_transfer(
//                 &repair_config(),
//                 offer.commit_op,
//                 Vec::new(),
//                 &offer.offsets.1,
//             )
//             .await
//             .unwrap();
//         assert_eq!(receiver.consensus().commit_min(), 1);
//         assert_eq!(
//             receiver.consensus().last_prepare_checksum(),
//             committed.header().checksum
//         );
//     }

//     /// Steady writes keep an applied op resident on every round, and the
//     /// heartbeats a receiver takes during the pull move its `commit_max` past
//     /// whatever op an offer names. The plan pinned by the first flush carries
//     /// the build, a primary still applying later commits serves it, and the
//     /// receiver installs the pinned op and leaves the later ones to repair.
//     #[compio::test]
//     async fn given_expired_transfer_pin_when_hashing_finishes_should_request_a_fresh_plan() {
//         const RING_CAPACITY: usize = 1;
//         let config = repair_config();
//         let (_directory, mut partition) = Box::pin(disk_poll_partition(&config)).await;
//         partition.set_io_notifier(
//             Rc::new(|_, _| {}),
//             crate::largest_legal_job_charge().unwrap(),
//         );
//         partition.log.journal().inner.set_repair_retention(true);
//         partition
//             .log
//             .journal()
//             .inner
//             .set_ring_caps(RING_CAPACITY, u64::MAX);
//
//         journal_send_batch(&mut partition, 1).await;
//         partition.consensus().advance_commit_max(1);
//         settle_partition_io(&mut partition, &config).await;
//         assert!(matches!(
//             partition.state_transfer_offer(&config).await,
//             Err(crate::state_transfer::PartitionTransferUnavailable::FlushPending)
//         ));
//         settle_partition_io(&mut partition, &config).await;
//         let planned_len = partition.log.active_segment().size.as_bytes_u64();
//
//         for op in 2..=3 {
//             journal_send_batch(&mut partition, op).await;
//             partition.consensus().advance_commit_max(op);
//             settle_partition_io(&mut partition, &config).await;
//         }
//         assert_eq!(partition.consensus().commit_min(), 3);
//         assert_eq!(
//             partition.log.journal().inner.repair_retained_from(),
//             Some(3)
//         );
//
//         assert!(matches!(
//             partition.state_transfer_offer(&config).await,
//             Err(crate::state_transfer::PartitionTransferUnavailable::FlushPending)
//         ));
//         assert!(
//             !partition.segment_checksum_cache.borrow().is_empty(),
//             "an expired pin must keep completed checksum work"
//         );
//         settle_partition_io(&mut partition, &config).await;
//         let offer = partition.state_transfer_offer(&config).await.unwrap();
//         assert_eq!(offer.commit_op, 3);
//         assert!(offer.segments[0].entry.len > planned_len);
//         let offsets = crate::state_transfer::ConsumerOffsetsWire::decode(&offer.offsets.1).unwrap();
//         assert_eq!(offsets.next_offset, 3);
//     }

/// A tail whose bodies the ring released to segments does not keep a
/// plan pinned. The receiver would repair it from disk, which moves far
/// less per round than repair from memory, so the source re-pins once the
/// tail of the plan leaves memory.
#[compio::test]
async fn given_a_transfer_pin_whose_tail_left_memory_when_hashing_finishes_should_request_a_fresh_plan()
 {
    let root = tempfile::tempdir().unwrap();
    let (mut partition, _, config, mut parent) =
        partition_with_committed_send(root.path(), iggy_common::Durability::Replicated, 3).await;
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    let journal = &partition.log.journal().inner;
    journal.set_repair_retention(true);
    journal.set_ring_caps(crate::EVICTED_RING_CAPACITY, 1);
    assert!(matches!(
        partition.state_transfer_offer(&config).await,
        Err(crate::state_transfer::PartitionTransferUnavailable::FlushPending)
    ));
    settle_partition_io(&mut partition, &config).await;

    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    for op in 2..=3 {
        let prepare = checksummed_segment_prepare(op, parent, op - 1, b"tail");
        let header = *prepare.header();
        parent = header.checksum;
        persistence
            .append(prepare.clone().into_frozen(), false)
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
        partition.consensus.advance_commit_max(op);
        partition.commit_journal(&config).await;
        settle_partition_io(&mut partition, &config).await;
    }
    assert_eq!(partition.consensus().commit_min(), 3);
    assert!(
        partition.log.journal().inner.evicted_header(2).is_some(),
        "the byte cap released the body of op 2 to its segment"
    );

    let offer = partition.state_transfer_offer(&config).await;
    assert!(
        matches!(
            offer,
            Err(crate::state_transfer::PartitionTransferUnavailable::FlushPending)
        ),
        "{:?}",
        offer.map(|offer| offer.commit_op)
    );
    assert!(matches!(
        *partition.transfer_plan.borrow(),
        Some(TransferPlanSlot::Wanted)
    ));
}

#[compio::test]
async fn given_ops_committing_during_a_transfer_when_the_receiver_installs_should_land_on_the_pinned_op()
 {
    const THRESHOLD: u32 = 8;
    let mut config = repair_config();
    config.messages_required_to_save = THRESHOLD;
    let (_directory, mut partition) = Box::pin(disk_poll_partition(&config)).await;
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    journal_send_batch(&mut partition, 1).await;
    partition.consensus().advance_commit_max(1);
    settle_partition_io(&mut partition, &config).await;
    assert_eq!(partition.consensus().commit_min(), 1);
    assert!(partition.log.journal().inner.holds_op(1));

    assert!(matches!(
        partition.state_transfer_offer(&config).await,
        Err(crate::state_transfer::PartitionTransferUnavailable::FlushPending)
    ));
    settle_partition_io(&mut partition, &config).await;
    let planned_len = partition.log.active_segment().size.as_bytes_u64();
    assert!(planned_len > 0, "the transfer flush writes the applied op");

    journal_send_batch(&mut partition, 2).await;
    partition.consensus().advance_commit_max(2);
    settle_partition_io(&mut partition, &config).await;
    assert_eq!(partition.consensus().commit_min(), 2);
    assert!(partition.log.journal().inner.holds_op(2));
    journal_send_batch(&mut partition, 3).await;
    partition.consensus().advance_commit_max(3);
    assert_eq!(
        partition.consensus().commit_min(),
        2,
        "op 3 is not applied yet"
    );

    let offer = partition.state_transfer_offer(&config).await.unwrap();
    assert_eq!(offer.commit_op, 1);
    assert_eq!(offer.segments.len(), 1);
    assert_eq!(offer.segments[0].entry.len, planned_len);
    assert!(
        partition.transfer_plan.borrow().is_none(),
        "a finished build releases its plan"
    );

    let (_receiver_directory, mut receiver) = transfer_receiver();
    receiver
        .consensus()
        .advance_commit_max(partition.consensus().commit_min());
    let segment = &offer.segments[0];
    let mut bytes = std::fs::read(&segment.log_path).unwrap();
    bytes.truncate(usize::try_from(segment.entry.len).unwrap());
    let staged = receiver
        .spill_transfer_segment(&segment.entry, bytes, None)
        .await
        .unwrap();
    receiver
        .install_state_transfer(&config, offer.commit_op, vec![staged], &offer.offsets.1)
        .await
        .unwrap();
    assert_eq!(receiver.consensus().commit_min(), 1);
    assert_eq!(receiver.consensus().commit_max(), 2);
}

//     #[compio::test]
//     async fn transfer_plan_pins_before_commits_arriving_during_its_flush() {
//         let mut config = repair_config();
//         config.messages_required_to_save = 8;
//         let (_directory, mut partition) = Box::pin(disk_poll_partition(&config)).await;
//         partition.set_io_notifier(
//             Rc::new(|_, _| {}),
//             crate::largest_legal_job_charge().unwrap(),
//         );
//         journal_send_batch(&mut partition, 1).await;
//         partition.consensus().advance_commit_max(1);
//         settle_partition_io(&mut partition, &config).await;
//         assert_eq!(partition.consensus().commit_min(), 1);
//         assert!(matches!(
//             partition.state_transfer_offer(&config).await,
//             Err(PartitionTransferUnavailable::FlushPending)
//         ));
//         assert!(matches!(
//             partition.resume_io(&config).await,
//             crate::PartitionIoStep::Progress
//         ));
//         assert!(partition.maintenance.transfer_flush);
//
//         journal_send_batch(&mut partition, 2).await;
//         partition.consensus().advance_commit_max(2);
//         let headers = partition.collect_committable_from_journal(COMMIT_WALK_OPS_MAX, &config);
//         partition
//             .handle_committed_entries(headers, &config, false)
//             .await;
//         assert!(
//             partition.pending_commit.is_none(),
//             "the transfer must pin before selecting another commit"
//         );
//         settle_partition_io(&mut partition, &config).await;
//         let offer = partition.state_transfer_offer(&config).await.unwrap();
//         assert_eq!(offer.commit_op, 1);
//         assert_eq!(
//             partition.consensus().commit_min(),
//             2,
//             "commits resume once the plan is pinned"
//         );
//         assert!(!partition.maintenance.transfer_flush);
//     }

/// Removes every segment log of `partition`, then builds an offer. The
/// offer can only take each stamp from the checksum memo, and each stamp
/// must equal the manifest checksum of the bytes that the file held.
async fn assert_offer_reads_no_segment_file<B: MessageBus>(partition: &mut IggyPartition<B>) {
    let config = repair_config();
    let expected = segment_artifacts_on_disk(partition);
    let directory = partition.partition_dir().unwrap();
    for artifact in &expected {
        std::fs::remove_file(format!("{directory}/{:020}.log", artifact.frontier)).unwrap();
    }

    assert!(matches!(
        partition.state_transfer_offer(&config).await,
        Err(PartitionTransferUnavailable::FlushPending)
    ));
    settle_partition_io(partition, &config).await;
    let offer = partition.state_transfer_offer(&config).await.unwrap();

    let offered: Vec<_> = offer.segments.iter().map(|segment| segment.entry).collect();
    assert_eq!(offered, expected);
}

/// A save that the I/O byte cap splits into pieces feeds the memo the
/// bytes of every piece, in file order.
#[compio::test]
async fn given_saves_split_into_pieces_when_building_an_offer_should_not_read_the_segments() {
    const OPS: u64 = 4;
    let config = repair_config();
    let (directory, mut partition) = Box::pin(disk_poll_partition_with_replicas(&config, 3)).await;
    let job_charge_max = crate::largest_legal_job_charge().unwrap();
    partition.set_io_notifier(Rc::new(|_, _| {}), job_charge_max);
    for op in 1..=OPS {
        journal_send_batch(&mut partition, op).await;
    }
    let piece_charge = partition
        .log
        .journal()
        .inner
        .with_entries(|entries| {
            crate::io::materialization_allocation_charge(
                std::slice::from_ref(&entries[0]),
                1,
                crate::iggy_index::IGGY_INDEX_SIZE,
            )
        })
        .unwrap();
    partition.io_bytes_max.set(piece_charge);
    partition.consensus().advance_commit_max(OPS);
    settle_partition_io(&mut partition, &config).await;
    partition.io_bytes_max.set(job_charge_max);
    assert!(partition.fatal().is_none());
    assert_eq!(
        std::fs::metadata(directory.path().join(format!("{:020}.index", 0)))
            .unwrap()
            .len(),
        crate::iggy_index::IGGY_INDEX_SIZE as u64,
        "one save must write every batch, so the cap split it into pieces"
    );

    Box::pin(assert_offer_reads_no_segment_file(&mut partition)).await;
}

/// A save whose bodies the partition WAL already wrote feeds the memo the
/// same bytes that the segment file holds. The group has peers, so its
/// journal keeps the committed prepare that an offer carries.
#[compio::test]
async fn given_segment_bodies_written_by_the_wal_when_building_an_offer_should_not_read_them() {
    const REPLICAS: u8 = 3;
    for durability in [
        iggy_common::Durability::Replicated,
        iggy_common::Durability::Persisted,
    ] {
        let root = tempfile::tempdir().unwrap();
        let (mut partition, _replies, _config, _parent) =
            partition_with_committed_send(root.path(), durability, REPLICAS).await;
        assert!(
            partition
                .log
                .messages_writers()
                .last()
                .is_some_and(Option::is_none),
            "{durability:?}: the segment must have no writer, so the WAL wrote its bodies"
        );
        partition.set_io_notifier(
            Rc::new(|_, _| {}),
            crate::largest_legal_job_charge().unwrap(),
        );

        Box::pin(assert_offer_reads_no_segment_file(&mut partition)).await;
    }
}

/// One-message batches each segment of [`partition_with_sealed_segments`]
/// holds before it rolls.
const BATCHES_PER_SEGMENT: u64 = 2;

/// `count` one-message records. Each call gives its messages new random
/// ids, so two calls give records of the same lengths but other bytes.
fn segment_records(count: u64) -> Vec<Vec<u8>> {
    (0..count)
        .map(|_| build_segment_record(IggyNamespace::new(1, 1, 0), 0))
        .collect()
}

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

/// The manifest entry of each segment of `partition`, read from its file.
fn segment_artifacts_on_disk<B: MessageBus>(
    partition: &IggyPartition<B>,
) -> Vec<consensus::StateArtifact> {
    let directory = partition.partition_dir().unwrap();
    partition
        .log
        .segments()
        .iter()
        .map(|segment| {
            let bytes =
                std::fs::read(format!("{directory}/{:020}.log", segment.start_offset)).unwrap();
            assert_eq!(bytes.len() as u64, segment.size.as_bytes_u64());
            consensus::StateArtifact::for_bytes(
                consensus::state_manifest::artifact_kind::SEGMENT_LOG,
                segment.start_offset,
                &bytes,
            )
        })
        .collect()
}

/// The append feeds the checksum memo of each segment it writes, so the
/// offer takes every stamp from memory and reads no segment file. The test
/// removes the files before the build to prove that. The stamps must still
/// equal the manifest checksum of the bytes that the files held.
#[compio::test]
async fn given_segments_written_by_this_process_when_building_an_offer_should_not_read_them() {
    const OPS: u64 = 5;
    let (directory, mut partition) =
        Box::pin(partition_with_sealed_segments(&segment_records(OPS))).await;
    let expected = segment_artifacts_on_disk(&partition);
    assert_eq!(expected.len(), 3, "two sealed segments and the active one");
    for artifact in &expected {
        std::fs::remove_file(
            directory
                .path()
                .join(format!("{:020}.log", artifact.frontier)),
        )
        .unwrap();
    }

    let offer = Box::pin(serve_offer(&mut partition)).await;

    assert_eq!(offer.commit_op, OPS);
    let offered: Vec<_> = offer.segments.iter().map(|segment| segment.entry).collect();
    assert_eq!(offered, expected);
}

/// Ops the receivers of the reuse tests commit: two sealed segments and
/// one batch in the active segment.
const RECEIVER_OPS: u64 = 5;
/// Ops the origin of the reuse tests commits: three sealed segments and
/// one batch in the active segment.
const ORIGIN_OPS: u64 = 7;

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

#[cfg(target_os = "linux")]
fn log_inode(directory: &Path, start_offset: u64) -> u64 {
    std::fs::metadata(directory.join(format!("{start_offset:020}.log")))
        .unwrap()
        .ino()
}

//     /// A receiver that already holds sealed segments of an offer links them to
//     /// the staging names instead of pulling them. The install then removes the
//     /// old names, sweeps the other staging files and renames the links, so the
//     /// installed sealed segments are the files that the receiver wrote. The
//     /// install also stamps every installed segment with its checksum, which
//     /// lets the next transfer reuse them too.
//     #[cfg(target_os = "linux")]
//     #[compio::test]
//     async fn given_receiver_holding_sealed_segments_when_installing_an_offer_should_reuse_them_without_a_copy()
//      {
//         let records = segment_records(ORIGIN_OPS);
//         let (_origin_directory, mut origin) =
//             Box::pin(partition_with_sealed_segments(&records)).await;
//         let held = &records[..usize::try_from(RECEIVER_OPS).unwrap()];
//         let (directory, mut receiver) = Box::pin(partition_with_sealed_segments(held)).await;
//         let offer = Box::pin(serve_offer(&mut origin)).await;
//         let sealed = [0, BATCHES_PER_SEGMENT];
//         let written = sealed.map(|start_offset| log_inode(directory.path(), start_offset));
//
//         let adopted = Box::pin(install_offer_with_reuse(&mut receiver, &offer)).await;
//
//         assert_eq!(
//             adopted,
//             [0, 1],
//             "the two sealed segments that the receiver holds"
//         );
//         assert_eq!(
//             sealed.map(|start_offset| log_inode(directory.path(), start_offset)),
//             written,
//             "the installed sealed segments are the files that the receiver wrote"
//         );
//         assert_eq!(
//             segment_file_bytes(directory.path()),
//             offered_segment_bytes(&offer)
//         );
//         assert!(
//             std::fs::read_dir(directory.path())
//                 .unwrap()
//                 .all(|entry| !entry
//                     .unwrap()
//                     .path()
//                     .to_string_lossy()
//                     .ends_with(".staging")),
//             "the install renamed or swept every staging name"
//         );
//         assert_eq!(receiver.consensus().commit_min(), ORIGIN_OPS);
//         let memos = receiver.segment_checksum_cache.borrow();
//         for entry in offer.segments.iter().map(|source| source.entry) {
//             assert_eq!(
//                 memos
//                     .get(&entry.frontier)
//                     .and_then(|memo| memo.checksum_of(entry.len)),
//                 Some(entry.checksum),
//                 "segment {} keeps its installed checksum",
//                 entry.frontier
//             );
//         }
//     }

#[compio::test]
async fn given_cached_sealed_segment_corruption_when_installing_should_fetch_verified_bytes() {
    let records = segment_records(ORIGIN_OPS);
    let (_origin_directory, mut origin) = Box::pin(partition_with_sealed_segments(&records)).await;
    let (directory, mut receiver) = Box::pin(partition_with_sealed_segments(
        &records[..usize::try_from(RECEIVER_OPS).unwrap()],
    ))
    .await;
    let offer = Box::pin(serve_offer(&mut origin)).await;
    let entry = offer.segments[0].entry;
    let path = directory.path().join(format!("{:020}.log", entry.frontier));
    let mut corrupted = std::fs::read(&path).unwrap();
    *corrupted.last_mut().unwrap() ^= 1;
    std::fs::write(&path, &corrupted).unwrap();
    assert_eq!(corrupted.len() as u64, entry.len);
    assert_eq!(
        receiver.segment_checksum_cache.borrow()[&entry.frontier].checksum_of(entry.len),
        Some(entry.checksum),
        "the append checksum still describes the original bytes"
    );

    let adopted = Box::pin(install_offer_with_reuse(&mut receiver, &offer)).await;

    assert_eq!(adopted, [1], "the corrupt sealed segment must be fetched");
    assert_eq!(
        segment_file_bytes(directory.path()),
        offered_segment_bytes(&offer)
    );
    assert_eq!(receiver.consensus().commit_min(), ORIGIN_OPS);
}

#[compio::test]
async fn given_corrupt_sealed_index_when_reusing_should_rebuild_from_verified_log() {
    let records = segment_records(ORIGIN_OPS);
    let (_origin_directory, mut origin) = Box::pin(partition_with_sealed_segments(&records)).await;
    let (directory, mut receiver) = Box::pin(partition_with_sealed_segments(
        &records[..usize::try_from(RECEIVER_OPS).unwrap()],
    ))
    .await;
    let offer = Box::pin(serve_offer(&mut origin)).await;
    let source = &offer.segments[0];
    let expected = origin
        .spill_transfer_segment(
            &source.entry,
            std::fs::read(&source.log_path).unwrap(),
            None,
        )
        .await
        .unwrap();
    let expected_index = std::fs::read(&expected.index_staging).unwrap();
    let index_path = directory
        .path()
        .join(format!("{:020}.index", source.entry.frontier));
    let mut corrupted = std::fs::read(&index_path).unwrap();
    corrupted.fill(u8::MAX);
    std::fs::write(&index_path, &corrupted).unwrap();

    let adopted = Box::pin(install_offer_with_reuse(&mut receiver, &offer)).await;

    assert_eq!(adopted, [0, 1], "valid logs remain reusable");
    assert_eq!(std::fs::read(index_path).unwrap(), expected_index);
    assert_eq!(
        segment_file_bytes(directory.path()),
        offered_segment_bytes(&offer)
    );
}

#[compio::test]
async fn given_pending_reuse_scan_when_tearing_down_should_wait_for_its_task() {
    let (_directory, mut receiver) = Box::pin(partition_with_sealed_segments(&segment_records(
        RECEIVER_OPS,
    )))
    .await;
    let manifest = Box::pin(serve_offer(&mut receiver)).await.manifest();
    let (release, previous) = futures::channel::oneshot::channel::<()>();
    receiver.staging_work_ended = Some(previous);
    let scan = receiver.begin_reuse_scan(&manifest);
    let mut drain = Box::pin(receiver.capture_teardown().drain());

    assert!(
        futures::poll!(&mut drain).is_pending(),
        "teardown must wait even before the detached scan is first polled"
    );
    assert!(scan.stop());
    assert!(
        futures::poll!(&mut drain).is_pending(),
        "cancellation does not prove that the file task has settled"
    );
    drop(release);
    drain.await.unwrap();
    assert!(
        scan.result.borrow().is_some(),
        "the scan finished before teardown"
    );
}

#[compio::test]
async fn given_a_pending_spill_when_tearing_down_should_wait_for_its_task() {
    let (directory, mut receiver) = Box::pin(partition_with_sealed_segments(&segment_records(
        RECEIVER_OPS,
    )))
    .await;
    let offer = Box::pin(serve_offer(&mut receiver)).await;
    let source = &offer.segments[0];
    let bytes = std::fs::read(&source.log_path).unwrap();
    let (release, previous) = futures::channel::oneshot::channel::<()>();
    receiver.staging_work_ended = Some(previous);
    let spill = receiver.begin_transfer_segment_spill(&source.entry, bytes, None);
    let mut drain = Box::pin(receiver.capture_teardown().drain());

    assert!(futures::poll!(&mut drain).is_pending());
    assert!(spill.stop());
    assert!(
        futures::poll!(&mut drain).is_pending(),
        "cancellation must not release the staging writer before it settles"
    );
    drop(release);
    drain.await.unwrap();

    assert!(matches!(
        spill.result.borrow_mut().take().unwrap(),
        Err(crate::state_transfer::SpillError::Cancelled)
    ));
    assert!(
        !directory
            .path()
            .join("00000000000000000000.log.staging")
            .exists(),
        "the cancelled task must not write after teardown"
    );
}

#[compio::test]
async fn given_an_abandoned_spill_when_a_reuse_scan_starts_should_wait_for_the_writer() {
    let (_directory, mut receiver) = Box::pin(partition_with_sealed_segments(&segment_records(
        RECEIVER_OPS,
    )))
    .await;
    let offer = Box::pin(serve_offer(&mut receiver)).await;
    let source = &offer.segments[0];
    let bytes = std::fs::read(&source.log_path).unwrap();
    let (release, previous) = futures::channel::oneshot::channel::<()>();
    receiver.staging_work_ended = Some(previous);
    let spill = receiver.begin_transfer_segment_spill(&source.entry, bytes.clone(), None);
    assert!(spill.stop());
    let scan = receiver.begin_reuse_scan(&offer.manifest());
    server_common::yield_to_reactor().await;
    assert!(
        !scan.landed(),
        "a replacement scan must not overtake the abandoned staging writer"
    );

    drop(release);
    let _ = receiver.staging_work_ended.take().unwrap().await;

    assert!(matches!(
        spill.result.borrow_mut().take().unwrap(),
        Err(crate::state_transfer::SpillError::Cancelled)
    ));
    let reused = receiver.accept_reuse_scan(scan.result.borrow_mut().take().unwrap());
    let staged = &reused.iter().find(|(index, _)| *index == 0).unwrap().1;
    assert_eq!(std::fs::read(&staged.log_staging).unwrap(), bytes);
}

#[compio::test]
async fn given_singleton_materialization_when_writing_should_skip_transfer_checksums() {
    let config = repair_config();
    let (directory, mut partition) = Box::pin(disk_poll_partition(&config)).await;
    let record = build_segment_record(partition.namespace(), 0);
    Box::pin(commit_records(
        &mut partition,
        1,
        std::slice::from_ref(&record),
    ))
    .await;

    assert_eq!(partition.consensus().commit_min(), 1);
    assert_eq!(segment_file_bytes(directory.path())[0].len(), record.len());
    assert!(
        partition.segment_checksum_cache.borrow().is_empty(),
        "a singleton has no state-transfer peer to serve"
    );
}

/// A sealed segment takes no more appends, so its memo keeps only the
/// stamp, whether the appends or an offer read hashed it. The active
/// segment keeps its hasher for the bytes that it gains.
#[compio::test]
async fn given_sealed_segments_when_their_memos_cover_them_should_keep_no_hasher() {
    let (_directory, mut partition) = Box::pin(partition_with_sealed_segments(&segment_records(
        RECEIVER_OPS,
    )))
    .await;
    let active = partition.log.active_segment().start_offset;
    let hashing = |partition: &IggyPartition<IggyMessageBus>| -> Vec<u64> {
        partition
            .segment_checksum_cache
            .borrow()
            .iter()
            .filter(|(_, memo)| {
                matches!(
                    memo,
                    crate::state_transfer::SegmentChecksumMemo::Hashing { .. }
                )
            })
            .map(|(start_offset, _)| *start_offset)
            .collect()
    };
    assert_eq!(hashing(&partition), [active], "after the appends");

    partition.segment_checksum_cache.borrow_mut().clear();
    Box::pin(serve_offer(&mut partition)).await;

    assert_eq!(hashing(&partition), [active], "after an offer read");
    assert_eq!(
        partition.segment_checksum_cache.borrow().len(),
        partition.log.segments().len()
    );
}

/// A receiver links every sealed segment of an offer that it holds, however
/// many it holds, so the transfer pulls none of them.
#[cfg(unix)]
#[compio::test]
async fn given_many_held_sealed_segments_when_installing_an_offer_should_link_every_one() {
    const SEALED: u64 = 12;
    let records = segment_records(SEALED * BATCHES_PER_SEGMENT + 1);
    let (_origin_directory, mut origin) = Box::pin(partition_with_sealed_segments(&records)).await;
    let (_directory, mut receiver) = Box::pin(partition_with_sealed_segments(&records)).await;
    let offer = Box::pin(serve_offer(&mut origin)).await;

    let adopted = Box::pin(install_offer_with_reuse(&mut receiver, &offer)).await;

    assert_eq!(
        adopted,
        (0..u32::try_from(SEALED).unwrap()).collect::<Vec<_>>(),
        "every held sealed segment"
    );
}

#[cfg(target_os = "linux")]
#[compio::test]
async fn given_a_partial_reuse_scan_when_an_offer_advances_should_keep_verified_files() {
    for spill_other in [false, true] {
        let records = segment_records(ORIGIN_OPS + 1);
        let (_origin_directory, mut origin) =
            Box::pin(partition_with_sealed_segments(&records)).await;
        let (directory, mut receiver) = Box::pin(partition_with_sealed_segments(
            &records[..usize::try_from(RECEIVER_OPS).unwrap()],
        ))
        .await;
        let previous = Box::pin(serve_offer(&mut receiver)).await.manifest();
        let scan = receiver.begin_reuse_scan(&previous);
        let _ = receiver.staging_work_ended.take().unwrap().await;
        let mut scanned = scan.result.borrow_mut().take().unwrap();
        assert!(scanned.reused.len() > 1);
        // Model a deadline between artifacts using files verified by the real scan.
        for (_, unused) in scanned.reused.split_off(1) {
            std::fs::remove_file(unused.log_staging).unwrap();
            std::fs::remove_file(unused.index_staging).unwrap();
        }
        let first = scanned.reused[0].1.clone();
        receiver.accept_reuse_scan(scanned);
        let index = std::fs::File::open(&first.index_staging).unwrap();
        let verified_inode = index.metadata().unwrap().ino();
        let offer = Box::pin(serve_offer(&mut origin)).await;
        let manifest = offer.manifest();
        assert_ne!(manifest, previous, "the source kept accepting writes");
        let stopped = receiver.begin_reuse_scan(&manifest);
        assert!(stopped.stop());
        let _ = receiver.staging_work_ended.take().unwrap().await;
        receiver.accept_reuse_scan(stopped.result.borrow_mut().take().unwrap());
        if spill_other {
            let source = offer.segments.last().unwrap();
            let mut bytes = std::fs::read(&source.log_path).unwrap();
            bytes.truncate(usize::try_from(source.entry.len).unwrap());
            receiver
                .spill_transfer_segment(&source.entry, bytes, None)
                .await
                .unwrap();
        }

        let reused = reuse_scan(&mut receiver, &manifest).await;

        assert!(reused.iter().any(|(position, _)| *position == 0));
        assert_eq!(
            std::fs::metadata(&first.index_staging).unwrap().ino(),
            verified_inode,
            "spill_other={spill_other}: verified staging must not be rebuilt on every retry"
        );
        drop(index);
        Box::pin(install_offer_with_reuse(&mut receiver, &offer)).await;
        assert_eq!(
            segment_file_bytes(directory.path()),
            offered_segment_bytes(&offer)
        );
    }
}

#[compio::test]
async fn given_verified_staging_when_a_spill_replaces_it_should_revalidate_for_an_older_offer() {
    let (_directory, mut receiver) = Box::pin(partition_with_sealed_segments(&segment_records(
        RECEIVER_OPS,
    )))
    .await;
    let own = Box::pin(serve_offer(&mut receiver)).await;
    reuse_scan(&mut receiver, &own.manifest()).await;
    let (_origin_directory, mut origin) =
        Box::pin(partition_with_sealed_segments(&segment_records(ORIGIN_OPS))).await;
    let offer = Box::pin(serve_offer(&mut origin)).await;
    let source = &offer.segments[0];
    assert_ne!(source.entry.checksum, own.segments[0].entry.checksum);
    receiver
        .spill_transfer_segment(
            &source.entry,
            std::fs::read(&source.log_path).unwrap(),
            None,
        )
        .await
        .unwrap();

    let reused = reuse_scan(&mut receiver, &own.manifest()).await;

    let first = &reused.iter().find(|(index, _)| *index == 0).unwrap().1;
    assert_eq!(
        std::fs::read(&first.log_staging).unwrap(),
        std::fs::read(&own.segments[0].log_path).unwrap(),
        "an older offer must recover its verified bytes after a staging replacement"
    );
}

/// A scan that is asked to stop before its first entry stages nothing and
/// leaves no memo, so the next scan of the same segment set runs whole.
#[cfg(unix)]
#[compio::test]
async fn given_a_stopped_reuse_scan_when_it_lands_should_reuse_nothing_and_leave_no_memo() {
    let (_directory, mut receiver) = Box::pin(partition_with_sealed_segments(&segment_records(
        RECEIVER_OPS,
    )))
    .await;
    let manifest = Box::pin(serve_offer(&mut receiver)).await.manifest();
    let scan = receiver.begin_reuse_scan(&manifest);
    assert!(scan.stop());

    let _ = receiver.staging_work_ended.take().unwrap().await;

    let scanned = scan.result.borrow_mut().take().unwrap();
    assert_eq!(scanned.reused, Vec::new());
    assert!(scanned.memoized.is_empty(), "a stopped scan saw no entry");
    assert_eq!(receiver.accept_reuse_scan(scanned), Vec::new());
    assert!(receiver.reuse_scan_memo.borrow().is_none());
    assert_eq!(
        reuse_scan(&mut receiver, &manifest).await.len(),
        2,
        "the next scan links both held sealed segments"
    );
}

/// A link that fails leaves the staging names as they were, so the staged
/// copy of an earlier attempt still serves the transfer.
#[cfg(target_os = "linux")]
#[compio::test]
async fn given_a_failing_link_when_a_staged_copy_exists_should_adopt_the_copy() {
    let (directory, mut receiver) = Box::pin(partition_with_sealed_segments(&segment_records(
        RECEIVER_OPS,
    )))
    .await;
    let offer = Box::pin(serve_offer(&mut receiver)).await;
    let held = &offer.segments[0];
    receiver
        .spill_transfer_segment(&held.entry, std::fs::read(&held.log_path).unwrap(), None)
        .await
        .unwrap();
    let staged = directory
        .path()
        .join(format!("{:020}.log.staging", held.entry.frontier));
    let copy = std::fs::metadata(&staged).unwrap().ino();
    // A directory at the scratch name fails the link.
    std::fs::create_dir(
        directory
            .path()
            .join(format!("{:020}.log.link.staging", held.entry.frontier)),
    )
    .unwrap();

    let adopted = reuse_scan(&mut receiver, &offer.manifest()).await;

    assert!(
        adopted
            .iter()
            .any(|(_, meta)| meta.start_offset == held.entry.frontier && !meta.linked)
    );
    assert_eq!(
        std::fs::metadata(&staged).unwrap().ino(),
        copy,
        "the staging name still names the staged copy"
    );
}

/// A spill gives a staging name that links a live segment a new file.
/// Writing through the link would change the live segment.
#[cfg(target_os = "linux")]
#[compio::test]
async fn given_a_linked_staging_name_when_spilling_other_bytes_should_keep_the_live_segment() {
    let (directory, mut receiver) = Box::pin(partition_with_sealed_segments(&segment_records(
        RECEIVER_OPS,
    )))
    .await;
    let own = Box::pin(serve_offer(&mut receiver)).await;
    reuse_scan(&mut receiver, &own.manifest()).await;
    let paths = ["log", "index"].map(|kind| directory.path().join(format!("{:020}.{kind}", 0)));
    let held = paths.clone().map(|path| std::fs::read(path).unwrap());
    let staged_log = directory.path().join(format!("{:020}.log.staging", 0));
    assert_eq!(
        std::fs::metadata(&staged_log).unwrap().ino(),
        log_inode(directory.path(), 0),
        "the scan linked the first segment"
    );
    let (_origin_directory, mut origin) =
        Box::pin(partition_with_sealed_segments(&segment_records(ORIGIN_OPS))).await;
    let offer = Box::pin(serve_offer(&mut origin)).await;
    let source = &offer.segments[0];

    receiver
        .spill_transfer_segment(
            &source.entry,
            std::fs::read(&source.log_path).unwrap(),
            None,
        )
        .await
        .unwrap();

    assert!(
        paths.map(|path| std::fs::read(path).unwrap()) == held,
        "the live segment keeps its bytes"
    );
    assert_ne!(
        std::fs::metadata(&staged_log).unwrap().ino(),
        log_inode(directory.path(), 0)
    );
}

/// A sealed segment with the base offset and the length of an offered one
/// but other bytes fails the checksum match. The receiver then pulls the
/// offered segment, and the install replaces the file.
#[compio::test]
async fn given_receiver_segment_with_other_bytes_when_installing_an_offer_should_copy_it() {
    let (_origin_directory, mut origin) =
        Box::pin(partition_with_sealed_segments(&segment_records(ORIGIN_OPS))).await;
    let (directory, mut receiver) = Box::pin(partition_with_sealed_segments(&segment_records(
        RECEIVER_OPS,
    )))
    .await;
    let offer = Box::pin(serve_offer(&mut origin)).await;
    let held = segment_file_bytes(directory.path());
    let offered = offered_segment_bytes(&offer);
    assert_eq!(held[0].len(), offered[0].len());
    assert_ne!(held[0], offered[0]);

    let adopted = Box::pin(install_offer_with_reuse(&mut receiver, &offer)).await;

    assert!(
        adopted.is_empty(),
        "no held segment matches an offered checksum"
    );
    assert_eq!(segment_file_bytes(directory.path()), offered);
    assert_eq!(receiver.consensus().commit_min(), ORIGIN_OPS);
}

/// The segment that was active at an install keeps a hasher, so the
/// appends that seal it extend its checksum. The next transfer then links
/// it like any other sealed segment instead of pulling it again.
#[compio::test]
async fn given_installed_active_segment_when_appends_seal_it_should_reuse_it_in_the_next_transfer()
{
    let records = segment_records(ORIGIN_OPS + BATCHES_PER_SEGMENT);
    let (origin_held, appended) = records.split_at(usize::try_from(ORIGIN_OPS).unwrap());
    let (_origin_directory, mut origin) =
        Box::pin(partition_with_sealed_segments(origin_held)).await;
    let receiver_held = &records[..usize::try_from(RECEIVER_OPS).unwrap()];
    let (_directory, mut receiver) = Box::pin(partition_with_sealed_segments(receiver_held)).await;
    let first = Box::pin(serve_offer(&mut origin)).await;
    Box::pin(install_offer_with_reuse(&mut receiver, &first)).await;
    for partition in [&mut origin, &mut receiver] {
        Box::pin(commit_records(partition, ORIGIN_OPS + 1, appended)).await;
    }
    let second = Box::pin(serve_offer(&mut origin)).await;
    let active_at_install = first.segments.last().unwrap().entry.frontier;

    let adopted: Vec<u64> = reuse_scan(&mut receiver, &second.manifest())
        .await
        .into_iter()
        .map(|(_, meta)| meta.start_offset)
        .collect();

    assert_eq!(
        adopted,
        [
            0,
            BATCHES_PER_SEGMENT,
            2 * BATCHES_PER_SEGMENT,
            active_at_install
        ],
        "every sealed segment, the one active at the install included"
    );
}

#[compio::test]
async fn given_wal_lane_published_persisted_runs_when_idle_should_leave_the_tick_retry_set() {
    const RUNS: u64 = 4;
    const IDLE_TURNS_MAX: usize = 64;
    let root = tempfile::tempdir().unwrap();
    let (mut partition, _replies, config, mut parent) =
        partition_with_committed_send(root.path(), iggy_common::Durability::Persisted, 1).await;
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    let notified = Rc::new(std::cell::Cell::new(0_usize));
    let counter = Rc::clone(&notified);
    partition.set_io_notifier(
        Rc::new(move |_, _| counter.set(counter.get() + 1)),
        crate::largest_legal_job_charge().unwrap(),
    );
    for run in 0..RUNS {
        let first = 2 * run + 2;
        let mut prepares = Vec::new();
        for op in [first, first + 1] {
            let prepare = checksummed_segment_prepare(op, parent, op - 1, b"run");
            parent = prepare.header().checksum;
            persistence
                .append(prepare.clone().into_frozen(), true)
                .unwrap();
            prepares.push(prepare);
        }
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        for prepare in prepares {
            let header = *prepare.header();
            partition.consensus.with_pipeline_mut(|pipeline| {
                pipeline.push(PipelineEntry::new(header));
            });
            partition
                .append_repaired_send_messages(prepare)
                .await
                .unwrap();
        }
        partition.consensus().advance_commit_max(first + 1);
        let headers = partition.select_persistable_commits(&config);
        assert_eq!(headers.len(), 2);
        // Select inline, as `on_ack` does, so the lane only publishes the run.
        partition
            .handle_committed_entries(headers, &config, true)
            .await;
        assert!(
            partition.pending_commit.is_some(),
            "a Persisted run under a dispatcher must wait for a lane job"
        );
        settle_partition_io(&mut partition, &config).await;
        assert_eq!(partition.consensus().commit_min(), first + 1);
    }
    let mut idle_turns = 0;
    while idle_turns < IDLE_TURNS_MAX {
        notified.set(0);
        if !partition.needs_io_retry() {
            break;
        }
        idle_turns += 1;
        assert!(matches!(
            partition.resume_io(&config).await,
            crate::PartitionIoStep::Pending
        ));
    }
    assert!(partition.fatal().is_none());
    assert_eq!(
        idle_turns, 0,
        "an idle partition must not stay in the tick retry set"
    );
}

/// Installing wipes the journal, so an offer below a committed op this
/// replica holds would drop an op the quorum may have counted it for.
#[compio::test]
async fn given_a_held_committed_op_above_the_offer_when_installing_should_refuse() {
    let (_directory, mut receiver) = transfer_receiver();
    let held = checksummed_segment_prepare(2, 0, 1, b"held");
    receiver
        .log
        .journal()
        .inner
        .append(held.into_frozen())
        .await
        .unwrap();
    receiver.consensus().advance_commit_max(3);
    let offsets = crate::state_transfer::ConsumerOffsetsWire {
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        required_metadata_frontier: 0,
        prepare_checksum: None,
        checkpoint_prepare: Vec::new(),
        next_offset: 0,
        consumers: Vec::new(),
        groups: Vec::new(),
        external_groups: Vec::new(),
        dedup: Vec::new(),
        ..Default::default()
    };
    let refused = receiver
        .install_state_transfer(&repair_config(), 1, Vec::new(), &offsets.encode())
        .await;
    assert!(
        matches!(
            refused,
            Err(
                crate::state_transfer::PartitionInstallError::OfferErasesHeldOps {
                    commit_op: 1,
                    held_max: 3,
                }
            )
        ),
        "expected a held-op refusal, got {refused:?}"
    );
}

#[compio::test]
async fn given_a_no_wal_ack_above_commit_max_when_installing_should_preserve_the_held_op() {
    const HELD_OP: u64 = 2;
    for rebuilt in [false, true] {
        let (_directory, mut receiver) = transfer_receiver();
        receiver.runtime_options.durability = iggy_common::Durability::Replicated;
        let held = checksummed_segment_prepare(HELD_OP, 0, HELD_OP - 1, b"acknowledged");
        let header = *held.header();
        receiver
            .log
            .journal()
            .inner
            .append(held.into_frozen())
            .await
            .unwrap();
        receiver.consensus.sequencer().set_sequence(HELD_OP);
        if rebuilt {
            assert!(receiver.register_rebuilt_ack(&header));
        } else {
            assert!(receiver.send_prepare_ok(&header).await);
        }
        assert_eq!(receiver.consensus.commit_max(), 0);
        let offsets = crate::state_transfer::ConsumerOffsetsWire {
            dedup_capacity: consensus::CLIENTS_TABLE_MAX,
            ..Default::default()
        };
        let result = receiver
            .install_state_transfer(&repair_config(), 1, Vec::new(), &offsets.encode())
            .await;
        assert!(
            matches!(
                result,
                Err(
                    crate::state_transfer::PartitionInstallError::OfferErasesHeldOps {
                        commit_op: 1,
                        held_max: HELD_OP
                    }
                )
            ),
            "acknowledged operation was not protected: {result:?}"
        );
        assert_eq!(
            receiver
                .log
                .journal()
                .inner
                .header_by_op(HELD_OP)
                .unwrap()
                .checksum,
            header.checksum
        );
    }
}

#[compio::test]
async fn given_an_unacknowledged_no_wal_tail_when_installing_should_allow_repair_after_transfer() {
    let (_directory, mut receiver) = transfer_receiver();
    let tail = checksummed_segment_prepare(2, 0, 1, b"unacknowledged");
    receiver
        .log
        .journal()
        .inner
        .append(tail.into_frozen())
        .await
        .unwrap();
    receiver.consensus.sequencer().set_sequence(2);
    let offsets = crate::state_transfer::ConsumerOffsetsWire {
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        ..Default::default()
    };
    receiver
        .install_state_transfer(&repair_config(), 1, Vec::new(), &offsets.encode())
        .await
        .unwrap();
    assert!(receiver.log.journal().inner.is_empty());
    assert_eq!(receiver.consensus.commit_min(), 1);
}

/// An install below the head keeps the head, and an offer at or below the
/// known commit keeps the commit point, so only the journal wipe can tell a
/// suffix snapshot taken before the install that the bodies it offers are
/// gone.
#[compio::test]
async fn given_a_suffix_snapshot_when_an_install_keeps_the_head_should_invalidate_it() {
    const HEAD: u64 = 3;
    let (_directory, mut receiver) = transfer_receiver();
    receiver.consensus().sequencer().set_sequence(HEAD);
    receiver.consensus().advance_commit_max(HEAD);
    receiver
        .consensus()
        .set_local_dvc_suffix(consensus::DvcSuffix::empty());
    assert!(!receiver.consensus().local_dvc_suffix_stale());
    let offsets = crate::state_transfer::ConsumerOffsetsWire {
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        ..Default::default()
    };

    let installed = receiver
        .install_state_transfer(&repair_config(), 1, Vec::new(), &offsets.encode())
        .await;

    assert!(
        installed.is_ok(),
        "the offer erases no held op, got {installed:?}"
    );
    assert_eq!(receiver.consensus().sequencer().current_sequence(), HEAD);
    assert!(
        receiver.consensus().local_dvc_suffix_stale(),
        "a suffix snapshot taken before the install still matches the log, so the \
             next DoViewChange offers bodies the install wiped"
    );
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

/// A backup with a partition directory and offset stores, ready to install
/// what an origin offers. Keep the directory alive while the receiver runs.
fn transfer_receiver() -> (tempfile::TempDir, IggyPartition<RecordingBus>) {
    let directory = tempfile::tempdir().unwrap();
    let (mut receiver, _) = recording_partition_at(1, 3);
    receiver.set_partition_dir(directory.path().to_string_lossy().into_owned());
    set_offset_dirs_under(&mut receiver, directory.path());
    (directory, receiver)
}

/// A replica that needs state transfer while producers keep writing. The
/// serving primary builds its offer at its `commit_min`. The pull takes
/// seconds for a large active segment, and meanwhile the primary's commit
/// heartbeats raise the receiver's `commit_max` past the offer. The
/// receiver's journal stopped at the gap, so its sequencer is below the
/// offer and the install erases nothing it journaled. The committed tail
/// above the offer is what journal repair fetches after the install.
///
/// Pins `install_state_transfer`, not the repair floor check. Refusing
/// such an offer is a livelock: under sustained load every offer
/// is below `commit_max` by the time it lands, every round is refused with
/// "transfer frontier X is below the local commit frontier Y", and the
/// replica keeps serving reads from its stale prefix.
#[compio::test]
async fn given_commit_max_advanced_during_the_pull_when_installing_should_land_and_leave_the_tail_to_repair()
 {
    let origin_directory = tempfile::tempdir().unwrap();
    let receiver_directory = tempfile::tempdir().unwrap();
    let (mut origin, _) = recording_partition_at(0, 3);
    origin.set_partition_dir(origin_directory.path().to_string_lossy().into_owned());
    let committed = checksummed_segment_prepare(1, 0, 0, b"committed");
    origin
        .log
        .journal()
        .inner
        .append(committed.clone().into_frozen())
        .await
        .unwrap();
    origin.consensus().sequencer().set_sequence(1);
    origin
        .consensus()
        .set_last_prepare_checksum(committed.header().checksum);
    origin.consensus().advance_commit_max(1);
    origin.consensus().advance_commit_min(1);
    origin
        .dedup_mut()
        .commit_capacity(usize::try_from(committed.header().retry_capacity).unwrap())
        .unwrap();
    origin.offset_space.committed_seeded = true;
    origin.offset.store(0, Ordering::Relaxed);
    origin.log.journal().inner.evict_prefix(1);
    let offer = origin.state_transfer_offer(&repair_config()).await.unwrap();
    assert_eq!(offer.commit_op, 1);

    let (mut receiver, _) = recording_partition_at(1, 3);
    receiver.set_partition_dir(receiver_directory.path().to_string_lossy().into_owned());
    set_offset_dirs_under(&mut receiver, receiver_directory.path());
    // Learned from the primary's commit heartbeats while the pull ran. The
    // journal never got past the gap, so nothing above the offer is
    // journaled here.
    receiver.consensus().advance_commit_max(4);
    assert_eq!(receiver.consensus().sequencer().current_sequence(), 0);

    let installed = receiver
        .install_state_transfer(
            &repair_config(),
            offer.commit_op,
            Vec::new(),
            &offer.offsets.1,
        )
        .await;

    assert!(
        installed.is_ok(),
        "an offer that erases no journaled op must install, got {installed:?}"
    );
    assert_eq!(receiver.consensus().commit_min(), 1);
    assert_eq!(receiver.consensus().sequencer().current_sequence(), 1);
    assert_eq!(
        receiver.consensus().commit_max(),
        4,
        "the committed tail above the offer stays known for journal repair to fetch"
    );
}

/// Two batches: the first is what a lagging replica holds, the second what
/// the primary appended after it.
fn held_and_pulled_batches(held: &[u8]) -> (Vec<u8>, usize) {
    let first = checksummed_segment_prepare(1, 0, 0, held);
    let second = checksummed_segment_prepare(2, first.header().checksum, 1, b"pulled");
    let mut body = first.as_slice()[size_of::<PrepareHeader>()..].to_vec();
    let prefix_len = body.len();
    body.extend_from_slice(&second.as_slice()[size_of::<PrepareHeader>()..]);
    (body, prefix_len)
}

#[compio::test]
async fn given_a_held_prefix_when_spilling_the_artifact_should_stage_what_a_whole_pull_stages() {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition_at(1, 3);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    let (body, prefix_len) = held_and_pulled_batches(b"held");
    let artifact = consensus::StateArtifact::for_bytes(
        consensus::state_manifest::artifact_kind::SEGMENT_LOG,
        0,
        &body,
    );
    let held = directory.path().join("held.log");
    std::fs::write(&held, &body[..prefix_len]).unwrap();
    let (mut bytes, prefix) = crate::state_transfer::read_segment_prefix(
        held.to_str().unwrap(),
        artifact,
        prefix_len as u64,
        &Cell::new(false),
    )
    .await
    .unwrap();
    bytes.extend_from_slice(&body[prefix_len..]);

    let seeded = partition
        .spill_transfer_segment(&artifact, bytes, Some(prefix))
        .await
        .unwrap();
    let seeded_index = std::fs::read(&seeded.index_staging).unwrap();
    let pulled = partition
        .spill_transfer_segment(&artifact, body, None)
        .await
        .unwrap();
    assert_eq!(
        seeded, pulled,
        "the seeded spill must stage the same segment"
    );
    assert_eq!(
        seeded_index,
        std::fs::read(&pulled.index_staging).unwrap(),
        "the walk resumed after the prefix must rebuild the same index"
    );
}

#[compio::test]
async fn given_a_held_prefix_with_other_bytes_when_spilling_should_fail_the_checksum() {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition_at(1, 3);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    let (body, prefix_len) = held_and_pulled_batches(b"held");
    let (other, other_len) = held_and_pulled_batches(b"HELD");
    assert_eq!(prefix_len, other_len);
    let artifact = consensus::StateArtifact::for_bytes(
        consensus::state_manifest::artifact_kind::SEGMENT_LOG,
        0,
        &body,
    );
    let held = directory.path().join("held.log");
    std::fs::write(&held, &other[..prefix_len]).unwrap();
    let (mut bytes, prefix) = crate::state_transfer::read_segment_prefix(
        held.to_str().unwrap(),
        artifact,
        prefix_len as u64,
        &Cell::new(false),
    )
    .await
    .unwrap();
    bytes.extend_from_slice(&body[prefix_len..]);

    let spilled = partition
        .spill_transfer_segment(&artifact, bytes, Some(prefix))
        .await;
    assert!(
        matches!(
            spilled,
            Err(crate::state_transfer::SpillError::ManifestChecksum { frontier: 0 })
        ),
        "a valid prefix with other bytes must fail the manifest checksum, got {spilled:?}"
    );
}

#[compio::test]
async fn given_a_dropped_session_when_its_prefix_read_runs_should_stop_without_filling_a_buffer() {
    const REACTOR_TURNS_MAX: usize = 1024;
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition_at(1, 3);
    let (body, prefix_len) = held_and_pulled_batches(b"held");
    let artifact = consensus::StateArtifact::for_bytes(
        consensus::state_manifest::artifact_kind::SEGMENT_LOG,
        0,
        &body,
    );
    let held = directory.path().join("held.log");
    std::fs::write(&held, &body[..prefix_len]).unwrap();
    partition.transfer = Some(crate::state_transfer::PartitionTransferSession {
        nonce: 1,
        peer: 0,
        commit_op: 0,
        artifacts: vec![crate::state_transfer::TransferArtifact::Pending(
            consensus::ArtifactProgress {
                entry: artifact,
                buf: Vec::new(),
            },
        )],
        target_accepted: true,
        idle_ticks: 0,
        spill: None,
        scan: None,
        seed: Some(crate::state_transfer::PrefixSeed::Planned {
            artifact: 0,
            log_path: held.to_str().unwrap().to_owned(),
            len: prefix_len as u64,
        }),
        pulled_len: 0,
    });
    assert!(
        partition.drive_prefix_seed(),
        "the pull must wait for the read"
    );
    let Some(crate::state_transfer::PrefixSeed::Reading { read, .. }) = partition
        .transfer
        .as_ref()
        .and_then(|session| session.seed.as_ref())
    else {
        panic!("the read must start");
    };
    let landed = Rc::clone(&read.result);

    partition.transfer = None;
    for _ in 0..REACTOR_TURNS_MAX {
        if landed.borrow().is_some() {
            break;
        }
        server_common::yield_to_reactor().await;
    }

    let read = landed.borrow();
    assert!(
        matches!(
            read.as_ref(),
            Some(Err(error)) if error.kind() == std::io::ErrorKind::Interrupted
        ),
        "a read whose session dropped must stop before it fills a buffer, got {read:?}"
    );
}

/// A backup that fetches one segment artifact, whose prefix read landed.
fn fetching_with_a_landed_prefix_read() -> IggyPartition<RecordingBus> {
    let (mut partition, _) = recording_partition_at(1, 3);
    partition
        .consensus()
        .set_state_transfer_stage(consensus::StateTransferStage::AwaitingTarget);
    partition
        .consensus()
        .set_state_transfer_stage(consensus::StateTransferStage::Fetching);
    partition.transfer = Some(crate::state_transfer::PartitionTransferSession {
        nonce: 1,
        peer: 0,
        commit_op: 0,
        artifacts: vec![crate::state_transfer::TransferArtifact::Pending(
            consensus::ArtifactProgress {
                entry: consensus::StateArtifact::for_bytes(
                    consensus::state_manifest::artifact_kind::SEGMENT_LOG,
                    0,
                    b"segment",
                ),
                buf: Vec::new(),
            },
        )],
        target_accepted: true,
        idle_ticks: 0,
        spill: None,
        scan: None,
        seed: Some(crate::state_transfer::PrefixSeed::Reading {
            artifact: 0,
            read: crate::state_transfer::LocalWork {
                result: Rc::new(RefCell::new(Some(Err(std::io::Error::other(
                    "the read failed",
                ))))),
                ..crate::state_transfer::LocalWork::default()
            },
        }),
        pulled_len: 0,
    });
    partition
}

#[compio::test]
async fn given_a_landed_prefix_read_when_resuming_io_should_hand_the_pull_its_turn() {
    let mut partition = fetching_with_a_landed_prefix_read();

    let step = partition.resume_io(&repair_config()).await;

    assert!(
        matches!(step, crate::PartitionIoStep::TransferReady),
        "a landed read must resume the pull before its stall sweep"
    );
}

#[compio::test]
async fn transfer_establishes_wal_body_ownership_without_losing_the_active_index_writer() {
    for durability in [
        iggy_common::Durability::Replicated,
        iggy_common::Durability::Persisted,
    ] {
        for materialized in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let (mut partition, _) = recording_partition_at(1, 3);
            let partition_dir = directory.path().to_string_lossy().into_owned();
            partition.set_partition_dir(partition_dir.clone());
            partition.runtime_options.durability = durability;
            partition.runtime_options.consumer_offset_durability =
                iggy_common::Durability::Persisted;
            set_offset_dirs_under(&mut partition, directory.path());
            crate::state_transfer::mark_materialization_missing(&partition_dir, 0)
                .await
                .unwrap();
            partition.open_persistence().await.unwrap();
            assert!(
                partition
                    .persistence
                    .as_ref()
                    .unwrap()
                    .segment_checkpoint()
                    .is_none()
            );
            assert!(!partition.consensus().recovery_election_allowed());
            assert!(!partition.consensus().recovered_log_complete());
            let prepare = checksummed_segment_prepare(1, 0, 0, b"transferred");
            let mut staged = Vec::new();
            if materialized {
                let body = prepare.as_slice()[size_of::<PrepareHeader>()..].to_vec();
                let artifact = consensus::StateArtifact::for_bytes(
                    consensus::state_manifest::artifact_kind::SEGMENT_LOG,
                    0,
                    &body,
                );
                staged.push(
                    partition
                        .spill_transfer_segment(&artifact, body, None)
                        .await
                        .unwrap(),
                );
            }
            let offsets = crate::state_transfer::ConsumerOffsetsWire {
                owner_retirement: None,
                owners: Vec::new(),
                history_transition: None,
                dedup_capacity: consensus::CLIENTS_TABLE_MAX,
                required_metadata_frontier: 0,
                prepare_checksum: Some(prepare.header().checksum),
                checkpoint_prepare: prepare.as_slice().to_vec(),
                next_offset: 1,
                emptied_at: 0,
                consumers: Vec::new(),
                groups: Vec::new(),
                external_groups: Vec::new(),
                dedup: Vec::new(),
                retirement_paid_through: 0,
            };
            partition
                .install_state_transfer(&repair_config(), 1, staged, &offsets.encode())
                .await
                .unwrap();
            assert!(
                partition
                    .persistence
                    .as_ref()
                    .unwrap()
                    .segment_checkpoint()
                    .is_some()
            );
            assert!(
                partition
                    .log
                    .storages()
                    .iter()
                    .all(|storage| storage.messages_size.is_none())
            );
            assert!(partition.log.messages_writers().iter().all(Option::is_none));
            assert!(partition.log.index_writers().last().unwrap().is_some());
            assert_eq!(partition.consensus().commit_min(), 1);
            assert!(!partition.requires_state_transfer());
            assert!(
                partition.consensus().recovery_election_allowed(),
                "{durability:?}: the installed copy may vote and lead again"
            );
            assert_eq!(
                partition.consensus().recovered_log_complete(),
                durability.is_persisted()
            );
        }
    }
}

#[compio::test]
async fn given_commit_min_above_the_wal_head_when_checkpointing_should_not_report_one_due() {
    const WAL_HEAD: u64 = 1;
    for commit_min in [WAL_HEAD, WAL_HEAD + 1] {
        let directory = tempfile::tempdir().unwrap();
        let (mut partition, _) = recording_partition_at(0, 3);
        partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
        partition.open_persistence().await.unwrap();
        partition.set_io_notifier(
            Rc::new(|_, _| {}),
            crate::largest_legal_job_charge().unwrap(),
        );
        let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
        let prepare = checksummed_segment_prepare(WAL_HEAD, 0, 0, b"durable");
        persistence.append(prepare.into_frozen(), true).unwrap();
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        partition
            .consensus
            .restore_commit_state(commit_min, commit_min);
        persistence.request_checkpoint();

        let due = partition.needs_persistence_checkpoint();
        partition.checkpoint_persistence(&repair_config()).await;

        assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
        let started = partition.maintenance.checkpoint.is_some();
        assert_eq!(
            due, started,
            "commit_min {commit_min}: the tick starts a checkpoint whenever one is due"
        );
        assert_eq!(
            started,
            commit_min <= WAL_HEAD,
            "commit_min {commit_min}: receipts at commit_min need the WAL to hold it"
        );
    }
}

#[cfg(target_os = "linux")]
#[compio::test]
async fn checkpoint_index_sync_failure_fences_before_reclaiming_wal_history() {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition_at(0, 3);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.runtime_options.durability = iggy_common::Durability::Persisted;
    partition.open_persistence().await.unwrap();
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    let prepare = checksummed_segment_prepare(1, 0, 0, b"durable");
    persistence.append(prepare.into_frozen(), true).unwrap();
    assert!(persistence.start());
    Rc::clone(&persistence).run().await;
    assert!(persistence.is_durable_through(1));
    partition.consensus.restore_commit_state(1, 1);
    let writer = IggyIndexWriter::new("/dev/null", Rc::new(AtomicU64::new(0)), true, false)
        .await
        .unwrap();
    assert_eq!(
        writer
            .save_indexes_buffered_at(vec![1; 32], 0)
            .await
            .unwrap(),
        32
    );
    let active = partition.log.index_writers().len() - 1;
    partition.log.index_writers_mut()[active] = Some(Rc::new(writer));
    persistence.request_checkpoint();
    partition.checkpoint_persistence(&repair_config()).await;
    assert!(partition.fatal().is_some());
    assert!(!persistence.checkpoint_pending());
    assert!(persistence.is_durable_through(1));
}

// #[cfg(target_os = "linux")]
// #[compio::test]
// async fn checkpoint_index_sync_failure_fences_during_transfer_flush() {
//     let directory = tempfile::tempdir().unwrap();
//     let config = repair_config();
//     let (mut partition, _) = recording_partition_at(0, 3);
//     partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
//     partition.runtime_options.durability = iggy_common::Durability::Persisted;
//     partition.open_persistence().await.unwrap();
//     let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
//     let prepare = checksummed_segment_prepare(1, 0, 0, b"durable");
//     persistence.append(prepare.into_frozen(), true).unwrap();
//     assert!(persistence.start());
//     Rc::clone(&persistence).run().await;
//     partition.consensus.restore_commit_state(1, 1);
//     // fsync on /dev/null fails, standing in for a writeback error.
//     let writer = IggyIndexWriter::new("/dev/null", Rc::new(AtomicU64::new(0)), true, false)
//         .await
//         .unwrap();
//     let active = partition.log.index_writers().len() - 1;
//     partition.log.index_writers_mut()[active] = Some(Rc::new(writer));
//     partition.set_io_notifier(
//         Rc::new(|_, _| {}),
//         crate::largest_legal_job_charge().unwrap(),
//     );
//     // What `drive_transfer_plan` leaves behind when the flush block then
//     // falls through to `drive_checkpoint_io`.
//     partition.maintenance.transfer_flush = true;
//     partition.request_flush(1);
//     partition.maintenance.checkpoint = Some(PendingCheckpoint::Index(1));
//     let crate::PartitionIoStep::Ready(plan) = partition
//         .io_plan(crate::PartitionIoContinuation::Checkpoint)
//         .unwrap()
//     else {
//         panic!("the checkpoint index sync must plan");
//     };
//     let captured = partition.capture_io(plan, &config).unwrap().unwrap();
//     assert!(matches!(captured.job, crate::PartitionIoJob::IndexSync(_)));
//     let result = captured.job.execute().await;
//     assert!(partition.accept_io(captured.identity, result).is_err());
//     assert!(
//         partition.fatal().is_some(),
//         "a failed checkpoint index sync must fence inside a transfer flush too"
//     );
// }

// #[compio::test]
// async fn checkpoint_planning_and_capture_failures_fence_during_transfer_flush() {
//     for fail_capture in [false, true] {
//         let (mut partition, _) = recording_partition();
//         partition.set_io_notifier(
//             Rc::new(|_, _| {}),
//             crate::largest_legal_job_charge().unwrap(),
//         );
//         partition.maintenance.transfer_flush = true;
//         partition.maintenance.checkpoint = Some(PendingCheckpoint::Receipts(1, vec![1, 2, 3]));
//         if !fail_capture {
//             partition.io_bytes_max.set(0);
//         }
//         let config = repair_config();
//         let step = partition.resume_io(&config).await;
//         if fail_capture {
//             let crate::PartitionIoStep::Ready(plan) = step else {
//                 panic!("receipt checkpoint must reach capture");
//             };
//             let Err(error) = partition.capture_io(plan, &config) else {
//                 panic!("capture must fail without a partition directory");
//             };
//             assert!(
//                 matches!(&partition.maintenance.checkpoint, Some(PendingCheckpoint::Receipts(1, bytes)) if bytes == &[1, 2, 3])
//             );
//             partition.reject_io_capture(plan.continuation, error);
//         } else {
//             assert!(matches!(step, crate::PartitionIoStep::Pending));
//         }
//         assert!(
//             partition.fatal().is_some(),
//             "checkpoint failure must fence before transfer-flush recovery; fail_capture: {fail_capture}"
//         );
//     }
// }

//     #[cfg(target_os = "linux")]
//     #[compio::test]
//     async fn given_an_index_sync_failure_when_installing_state_transfer_should_refuse_backup_publication()
//      {
//         let directory = tempfile::tempdir().unwrap();
//         let (mut partition, _) = recording_partition_at(0, 3);
//         let partition_dir = directory.path().to_string_lossy().into_owned();
//         partition.set_partition_dir(partition_dir.clone());
//         partition.runtime_options.durability = iggy_common::Durability::Persisted;
//         for kind in ["consumers", "groups", "external_groups"] {
//             std::fs::create_dir_all(directory.path().join("offsets").join(kind)).unwrap();
//         }
//         partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
//             Some(format!("{partition_dir}/offsets/consumers"));
//         partition.consumer_offset_dirs[ConsumerKind::ConsumerGroup.index()] =
//             Some(format!("{partition_dir}/offsets/groups"));
//         partition.consumer_offset_dirs[ConsumerKind::ExternalGroup.index()] =
//             Some(format!("{partition_dir}/offsets/external_groups"));
//         partition.open_persistence().await.unwrap();
//         let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
//         let prepare = checksummed_segment_prepare(1, 0, 0, b"durable");
//         persistence
//             .append(prepare.clone().into_frozen(), true)
//             .unwrap();
//         assert!(persistence.start());
//         Rc::clone(&persistence).run().await;
//         partition.consensus.restore_commit_state(1, 1);
//         persistence.checkpoint(1);
//         assert!(persistence.start());
//         Rc::clone(&persistence).run().await;
//         assert_eq!(persistence.checkpoint_op(), 1);
//
//         let writer = IggyIndexWriter::new("/dev/null", Rc::new(AtomicU64::new(0)), true, false)
//             .await
//             .unwrap();
//         assert_eq!(
//             writer
//                 .save_indexes_buffered_at(vec![1; 32], 0)
//                 .await
//                 .unwrap(),
//             32
//         );
//         let active = partition.log.index_writers().len() - 1;
//         partition.log.index_writers_mut()[active] = Some(Rc::new(writer));
//         let offsets = crate::state_transfer::ConsumerOffsetsWire {
//             dedup_capacity: partition.dedup().capacity(),
//             required_metadata_frontier: 0,
//             prepare_checksum: Some(prepare.header().checksum),
//             checkpoint_prepare: prepare.as_slice().to_vec(),
//             next_offset: 1,
//             emptied_at: 0,
//             consumers: Vec::new(),
//             groups: Vec::new(),
//             external_groups: Vec::new(),
//             dedup: Vec::new(),
//             owners: Vec::new(),
//             history_transition: None,
//             owner_retirement: None,
//             retirement_paid_through: 0,
//         };
//
//         let result = partition
//             .install_state_transfer(&repair_config(), 1, Vec::new(), &offsets.encode())
//             .await;
//
//         assert!(matches!(
//             result,
//             Err(crate::state_transfer::PartitionInstallError::SwapIo { .. })
//         ));
//         assert!(partition.fatal().is_some());
//         assert!(!directory.path().join(".install-backup").exists());
//     }

#[compio::test]
async fn pending_wal_prefix_keeps_pipeline_replies_and_does_not_partially_flush() {
    for durability in [
        iggy_common::Durability::Replicated,
        iggy_common::Durability::Persisted,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let (mut partition, replies) = recording_partition_at(0, 3);
        partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
        partition.runtime_options.durability = durability;
        partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Persisted;
        let first = checksummed_segment_prepare(1, 0, 0, b"first");
        let second = checksummed_segment_prepare(2, first.header().checksum, 1, b"second");
        let segment_size =
            IggyByteSize::from((first.as_slice().len() - size_of::<PrepareHeader>()) as u64);
        partition.runtime_options.segment_size = Some(segment_size);
        partition.log.active_segment_mut().max_size = segment_size;
        partition.open_persistence().await.unwrap();
        partition.log.retire_front().unwrap();
        partition
            .install_empty_segment(&repair_config(), 0)
            .await
            .unwrap();
        let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
        persistence
            .append(first.clone().into_frozen(), durability.is_persisted())
            .unwrap();
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        persistence
            .append(second.clone().into_frozen(), durability.is_persisted())
            .unwrap();
        assert!(persistence.start());
        let writer = Rc::clone(&persistence).run();
        for prepare in [first, second] {
            partition.consensus.with_pipeline_mut(|pipeline| {
                pipeline.push(PipelineEntry::new(*prepare.header()));
            });
            partition
                .append_repaired_send_messages(prepare)
                .await
                .unwrap();
        }
        partition.consensus.restore_commit_state(0, 2);
        let config = repair_config();
        {
            let mut flush = Box::pin(partition.commit_messages_inner(&config, true, 2));
            assert!(matches!(
                futures::poll!(&mut flush),
                std::task::Poll::Ready(Ok(false))
            ));
        }
        assert_eq!(partition.log.journal().inner.resident_count(), 2);
        assert_eq!(partition.log.active_segment().size.as_bytes_u64(), 0);
        assert_eq!(partition.stats.messages_count_inconsistent(), 0);
        partition.commit_journal(&config).await;
        assert!(partition.fatal().is_none());
        assert_eq!(partition.consensus.commit_min(), 1);
        assert_eq!(partition.consensus.pipeline_head_header().unwrap().op, 2);
        assert_eq!(replies.borrow().len(), 1);
        writer.await;
        partition.commit_journal(&config).await;
        assert!(partition.fatal().is_none());
        assert_eq!(partition.consensus.commit_min(), 2);
        assert_eq!(partition.consensus.pipeline_len(), 0);
        assert_eq!(partition.stats.messages_count_inconsistent(), 2);
        assert_eq!(replies.borrow().len(), 2);
        let consensus::client_table::RequestStatus::Duplicate(cached) = partition
            .dedup
            .check_partition_request(1, 0, 1, 2, Operation::SendMessages)
        else {
            panic!("the committed reply must remain replayable");
        };
        assert!(
            replies.borrow()[1]
                .1
                .shares_allocation(&cached.into_wire_bytes())
        );
    }
}

#[compio::test]
async fn given_a_replicated_singleton_when_a_flush_write_completes_should_reply_without_a_tick() {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, replies) = recording_partition_at(0, 1);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.runtime_options.durability = iggy_common::Durability::Replicated;
    partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Replicated;
    partition.open_persistence().await.unwrap();
    let config = repair_config();
    partition.log.retire_front().unwrap();
    partition.install_empty_segment(&config, 0).await.unwrap();
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    let completion = Rc::new(Cell::new(None));
    let captured = Rc::clone(&completion);
    persistence.set_notifier(Rc::new(move |value| captured.set(Some(value))));

    // Hold the writer until the self-ack has reached the physical-write gate.
    persistence.sync();
    assert!(persistence.start());
    let writer = Rc::clone(&persistence).run();
    let mut request = checksumless_send_request(partition.namespace(), 2);
    let body = &mut request.as_mut_slice()[size_of::<RoutedRequestHeader>()..];
    let mut batch = BatchHeader::decode(body).unwrap();
    batch.batch_checksum = batch.checksum_for_blob(&body[COMMAND_HEADER_SIZE..]);
    batch.encode_into(&mut body[..COMMAND_HEADER_SIZE]);
    partition.on_request(request.clone(), None).await;
    assert_eq!(
        partition.consensus.pipeline_len(),
        1,
        "the send must be admitted: {:?}",
        replies
            .borrow()
            .iter()
            .map(|(_, reply)| bytemuck::checked::from_bytes::<ReplyHeader>(
                &reply.as_slice()[..size_of::<ReplyHeader>()]
            )
            .status)
            .collect::<Vec<_>>()
    );
    commit_recorded_loopback(&mut partition).await;
    let header = partition.consensus.pipeline_head_header().unwrap();
    assert_eq!(partition.consensus.commit_max(), header.op);
    assert_eq!(partition.consensus.commit_min(), 0);
    assert!(!persistence.is_written(&header));
    assert!(partition.pending_persisted_acks.borrow().is_empty());
    assert!(replies.borrow().is_empty());

    writer.await;
    assert!(persistence.failure().is_none());
    assert!(persistence.is_written(&header));
    assert_eq!(persistence.durable_op(), 0);
    assert!(replies.borrow().is_empty());
    let completed = completion
        .get()
        .expect("the buffered write must notify its owner");
    for stale in [
        PersistenceCompletion {
            instance: completed.instance + 1,
            ..completed
        },
        PersistenceCompletion {
            epoch: completed.epoch + 1,
            ..completed
        },
    ] {
        partition.on_persistence_completed(stale, &config).await;
        assert!(replies.borrow().is_empty());
        assert_eq!(partition.consensus.commit_min(), 0);
    }

    partition.materialization_missing = true;
    partition.on_persistence_completed(completed, &config).await;
    assert!(replies.borrow().is_empty());
    assert_eq!(partition.consensus.commit_min(), 0);
    partition.materialization_missing = false;

    partition.consensus().begin_state_transfer_await();
    partition.on_persistence_completed(completed, &config).await;
    assert!(replies.borrow().is_empty());
    assert_eq!(partition.consensus.commit_min(), 0);
    partition
        .consensus()
        .set_state_transfer_stage(consensus::StateTransferStage::Idle);

    partition.consensus().begin_view_probe();
    partition.on_persistence_completed(completed, &config).await;
    assert!(replies.borrow().is_empty());
    assert_eq!(partition.consensus.commit_min(), 0);
    partition.consensus().init();
    assert_eq!(partition.consensus.commit_max(), header.op);
    assert_eq!(partition.consensus.pipeline_len(), 1);

    partition.on_persistence_completed(completed, &config).await;

    assert!(partition.fatal().is_none());
    assert_eq!(
        partition.consensus.commit_min(),
        header.op,
        "a completed flush must release the self-acked reply without a consensus tick"
    );
    assert_eq!(partition.consensus.pipeline_len(), 0);
    assert_eq!(replies.borrow().len(), 1);
    assert_eq!(partition.stats.messages_count_inconsistent(), 2);
    assert_eq!(partition.offset.load(Ordering::Acquire), 1);
    let consensus::client_table::RequestStatus::Duplicate(cached) = partition
        .dedup
        .check_partition_request(1, 0, 1, 1, Operation::SendMessages)
    else {
        panic!("the completed send must retain its original receipt");
    };
    assert!(
        replies.borrow()[0]
            .1
            .shares_allocation(&cached.into_wire_bytes())
    );

    partition.on_persistence_completed(completed, &config).await;
    assert_eq!(replies.borrow().len(), 1);
    partition.on_request(request, None).await;
    assert_eq!(replies.borrow().len(), 2);
    assert!(
        replies.borrow()[0]
            .1
            .shares_allocation(&replies.borrow()[1].1)
    );
    assert_eq!(partition.stats.messages_count_inconsistent(), 2);
    assert_eq!(partition.offset.load(Ordering::Acquire), 1);
    assert_eq!(persistence.head(), header.op);
}

#[compio::test]
async fn mixed_durability_replies_before_body_writes_below_the_flush_threshold() {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, replies) = recording_partition_at(0, 3);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.runtime_options.durability = iggy_common::Durability::Replicated;
    partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Persisted;
    partition.open_persistence().await.unwrap();
    let mut config = repair_config();
    config.messages_required_to_save = 2;
    partition.log.retire_front().unwrap();
    partition.install_empty_segment(&config, 0).await.unwrap();
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    let first = checksummed_segment_prepare(1, 0, 0, b"first");
    let second = checksummed_segment_prepare(2, first.header().checksum, 1, b"second");
    persistence
        .append(first.clone().into_frozen(), false)
        .unwrap();
    assert!(persistence.start());
    let writer = Rc::clone(&persistence).run();
    for (index, prepare) in [first, second].into_iter().enumerate() {
        let header = *prepare.header();
        if index > 0 {
            persistence
                .append(prepare.clone().into_frozen(), false)
                .unwrap();
        }
        partition.consensus.with_pipeline_mut(|pipeline| {
            pipeline.push(PipelineEntry::new(header));
        });
        partition
            .append_repaired_send_messages(prepare)
            .await
            .unwrap();
        partition.consensus.advance_commit_max(header.op);
        if index == 0 {
            let write_lock = partition.write_lock.clone();
            let _guard = write_lock.lock().await;
            let mut commit = Box::pin(partition.commit_journal(&config));
            assert!(
                futures::poll!(&mut commit).is_ready(),
                "a below-threshold reply must not acquire a later materialization requirement while waiting for the append lock"
            );
        } else {
            partition.commit_journal(&config).await;
        }
        assert!(partition.fatal().is_none());
        assert_eq!(partition.consensus.commit_min(), 1);
        assert_eq!(
            replies.borrow().len(),
            1,
            "only the below-threshold send can reply"
        );
        assert_eq!(partition.log.active_segment().size.as_bytes_u64(), 0);
        assert_eq!(partition.log.journal().inner.resident_count(), index + 1);
        assert!(!persistence.is_written(&header));
    }
    assert_eq!(partition.consensus.pipeline_head_header().unwrap().op, 2);
    {
        let mut flush = Box::pin(partition.commit_messages_inner(&config, true, 2));
        assert!(matches!(
            futures::poll!(&mut flush),
            std::task::Poll::Ready(Ok(false))
        ));
    }
    writer.await;
    assert_eq!(persistence.durable_op(), 0);
    partition.commit_journal(&config).await;
    assert!(partition.fatal().is_none());
    assert_eq!(partition.consensus.commit_min(), 2);
    assert_eq!(partition.consensus.pipeline_len(), 0);
    assert_eq!(replies.borrow().len(), 2);
    assert_eq!(partition.log.journal().inner.resident_count(), 0);
    assert_eq!(partition.stats.messages_count_inconsistent(), 2);
}

/// Under a dispatcher a Replicated reply needs neither its body write nor
/// the flush, and the flush that follows takes only written bodies.
#[compio::test]
async fn dispatched_replicated_send_replies_before_its_write_and_flushes_written_bodies() {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, replies) = recording_partition_at(0, 3);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.runtime_options.durability = iggy_common::Durability::Replicated;
    partition.open_persistence().await.unwrap();
    let mut config = repair_config();
    config.messages_required_to_save = 1;
    partition.log.retire_front().unwrap();
    partition.install_empty_segment(&config, 0).await.unwrap();
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    let first = checksummed_segment_prepare(1, 0, 0, b"first");
    let second = checksummed_segment_prepare(2, first.header().checksum, 1, b"second");
    persistence
        .append(first.clone().into_frozen(), false)
        .unwrap();
    assert!(persistence.start());
    Rc::clone(&persistence).run().await;
    persistence
        .append(second.clone().into_frozen(), false)
        .unwrap();
    assert!(persistence.start());
    let writer = Rc::clone(&persistence).run();
    for prepare in [first, second] {
        partition.consensus.with_pipeline_mut(|pipeline| {
            pipeline.push(PipelineEntry::new(*prepare.header()));
        });
        partition
            .append_repaired_send_messages(prepare)
            .await
            .unwrap();
    }
    partition.consensus.advance_commit_max(2);

    partition.commit_journal(&config).await;
    assert!(partition.fatal().is_none());
    assert_eq!(partition.consensus.commit_min(), 2);
    assert_eq!(
        replies.borrow().len(),
        2,
        "a Replicated reply must not wait for its body write"
    );
    assert_eq!(partition.offset.load(Ordering::Acquire), 1);

    settle_partition_io(&mut partition, &config).await;
    assert!(partition.fatal().is_none());
    assert_eq!(
        partition.log.journal().inner.resident_count(),
        1,
        "the flush must leave the unwritten body resident"
    );
    assert_eq!(
        partition.offset.load(Ordering::Acquire),
        1,
        "a flush behind the commits must not hide them from polls"
    );
    assert_eq!(partition.stats.current_offset(), 1);

    writer.await;
    settle_partition_io(&mut partition, &config).await;
    assert!(partition.fatal().is_none());
    assert_eq!(partition.log.journal().inner.resident_count(), 0);
    assert!(!partition.history_writes_pending());
    assert_eq!(partition.offset.load(Ordering::Acquire), 1);
    assert_eq!(partition.stats.messages_count_inconsistent(), 2);
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

//     #[compio::test]
//     async fn a_pending_checkpoint_holds_persisted_commits_and_passes_replicated_ones() {
//         for dispatched in [false, true] {
//             for durability in [
//                 iggy_common::Durability::Replicated,
//                 iggy_common::Durability::Persisted,
//             ] {
//                 let root = tempfile::tempdir().unwrap();
//                 let (mut partition, replies, config, parent) =
//                     partition_with_committed_send(root.path(), durability, 3).await;
//                 let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
//                 // The body is written, and durable under Persisted, before the
//                 // checkpoint starts. Only the checkpoint can hold the reply.
//                 let second = checksummed_segment_prepare(2, parent, 1, b"second");
//                 let header = *second.header();
//                 persistence
//                     .append(second.clone().into_frozen(), durability.is_persisted())
//                     .unwrap();
//                 assert!(persistence.start());
//                 Rc::clone(&persistence).run().await;
//                 partition.consensus.with_pipeline_mut(|pipeline| {
//                     pipeline.push(PipelineEntry::new(header));
//                 });
//                 partition
//                     .append_repaired_send_messages(second)
//                     .await
//                     .unwrap();
//                 persistence.checkpoint(1);
//                 assert!(partition.persistence_checkpoint_pending());
//
//                 if dispatched {
//                     partition.set_io_notifier(
//                         Rc::new(|_, _| {}),
//                         crate::largest_legal_job_charge().unwrap(),
//                     );
//                 }
//                 partition.consensus.advance_commit_max(header.op);
//                 partition.commit_journal(&config).await;
//                 if dispatched {
//                     settle_partition_io(&mut partition, &config).await;
//                 }
//                 assert!(partition.fatal().is_none());
//                 assert!(partition.persistence_checkpoint_pending());
//                 let held = durability.is_persisted();
//                 assert_eq!(
//                     partition.consensus.commit_min(),
//                     if held { 1 } else { header.op },
//                     "{durability:?}"
//                 );
//                 assert_eq!(replies.borrow().len(), if held { 1 } else { 2 });
//
//                 partition.start_persistence();
//                 persistence.drain().await.unwrap();
//                 assert_eq!(persistence.checkpoint_op(), 1);
//                 partition.commit_journal(&config).await;
//                 if dispatched {
//                     settle_partition_io(&mut partition, &config).await;
//                 }
//                 assert!(partition.fatal().is_none());
//                 assert_eq!(partition.consensus.commit_min(), header.op);
//                 assert_eq!(replies.borrow().len(), 2);
//             }
//         }
//     }

//     /// A due checkpoint starts at `commit_min`, so until it is pending a
//     /// dispatched Replicated commit waits for its body write.
//     #[compio::test]
//     async fn a_due_checkpoint_holds_dispatched_replicated_commits_at_the_written_head() {
//         let root = tempfile::tempdir().unwrap();
//         let (mut partition, replies, config, parent) =
//             partition_with_committed_send(root.path(), iggy_common::Durability::Replicated, 3)
//                 .await;
//         partition.set_io_notifier(
//             Rc::new(|_, _| {}),
//             crate::largest_legal_job_charge().unwrap(),
//         );
//         let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
//         let second = checksummed_segment_prepare(2, parent, 1, b"second");
//         let header = *second.header();
//         persistence
//             .append(second.clone().into_frozen(), false)
//             .unwrap();
//         // Hold the writer so that the second body stays unwritten.
//         assert!(persistence.start());
//         partition.consensus.with_pipeline_mut(|pipeline| {
//             pipeline.push(PipelineEntry::new(header));
//         });
//         partition
//             .append_repaired_send_messages(second)
//             .await
//             .unwrap();
//         partition.consensus.advance_commit_max(header.op);
//
//         persistence.request_checkpoint();
//         partition.commit_journal(&config).await;
//         assert!(partition.fatal().is_none());
//         assert_eq!(
//             partition.consensus.commit_min(),
//             header.op - 1,
//             "a due checkpoint must hold commits at the written head"
//         );
//         assert_eq!(replies.borrow().len(), 1);
//
//         partition.checkpoint_persistence(&config).await;
//         settle_partition_io(&mut partition, &config).await;
//         assert!(partition.fatal().is_none());
//         assert!(partition.persistence_checkpoint_pending());
//         partition.commit_journal(&config).await;
//         assert_eq!(
//             partition.consensus.commit_min(),
//             header.op,
//             "a pending checkpoint must let commits pass the writer"
//         );
//         assert_eq!(replies.borrow().len(), 2);
//
//         Rc::clone(&persistence).run().await;
//         settle_partition_io(&mut partition, &config).await;
//         assert!(partition.fatal().is_none());
//         assert_eq!(persistence.checkpoint_op(), header.op - 1);
//         assert_eq!(partition.log.journal().inner.resident_count(), 0);
//     }

//     #[compio::test]
//     async fn given_retention_waiting_for_checkpoint_when_it_finishes_should_resume_without_another_request()
//      {
//         let root = tempfile::tempdir().unwrap();
//         let (mut partition, _, config, _) =
//             partition_with_committed_send(root.path(), iggy_common::Durability::Persisted, 3).await;
//         partition.log.segments_mut()[0].sealed = true;
//         partition.install_empty_segment(&config, 1).await.unwrap();
//         partition.set_io_notifier(
//             Rc::new(|_, _| {}),
//             crate::largest_legal_job_charge().unwrap(),
//         );
//         let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
//         assert_eq!(persistence.segment_checkpoint().unwrap().next_offset, 0);
//         persistence.sync();
//         assert!(persistence.start());
//
//         partition.remove_sealed_segments_up_to(0).await;
//         settle_partition_io(&mut partition, &config).await;
//         assert_eq!(partition.maintenance.retention, Some(0));
//         assert_eq!(partition.log.segments().len(), 2);
//         partition.checkpoint_persistence(&config).await;
//         settle_partition_io(&mut partition, &config).await;
//         assert!(persistence.checkpoint_pending());
//
//         Rc::clone(&persistence).run().await;
//         settle_partition_io(&mut partition, &config).await;
//         assert!(partition.fatal().is_none());
//         assert_eq!(partition.log.segments().len(), 1);
//         assert_eq!(partition.log.segments()[0].start_offset, 1);
//         assert_eq!(partition.maintenance.retention, None);
//     }

//     /// A dispatched checkpoint covers `commit_min`, which may lead the writer.
//     /// It flushes every body through that op first, so it waits for the writer.
//     #[compio::test]
//     async fn a_dispatched_checkpoint_waits_for_the_writer_before_covering_a_body() {
//         let root = tempfile::tempdir().unwrap();
//         let (mut partition, replies, config, parent) =
//             partition_with_committed_send(root.path(), iggy_common::Durability::Replicated, 3)
//                 .await;
//         partition.set_io_notifier(
//             Rc::new(|_, _| {}),
//             crate::largest_legal_job_charge().unwrap(),
//         );
//         let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
//         let second = checksummed_segment_prepare(2, parent, 1, b"second");
//         let header = *second.header();
//         persistence
//             .append(second.clone().into_frozen(), false)
//             .unwrap();
//         // Hold the writer so that the second body stays unwritten.
//         assert!(persistence.start());
//         partition.consensus.with_pipeline_mut(|pipeline| {
//             pipeline.push(PipelineEntry::new(header));
//         });
//         partition
//             .append_repaired_send_messages(second)
//             .await
//             .unwrap();
//         partition.consensus.advance_commit_max(header.op);
//         partition.commit_journal(&config).await;
//         assert_eq!(partition.consensus.commit_min(), header.op);
//         assert_eq!(replies.borrow().len(), 2);
//
//         persistence.request_checkpoint();
//         partition.checkpoint_persistence(&config).await;
//         settle_partition_io(&mut partition, &config).await;
//         assert!(partition.fatal().is_none());
//         assert!(
//             !partition.persistence_checkpoint_pending(),
//             "a checkpoint must not cover an unwritten body"
//         );
//         assert_eq!(partition.log.journal().inner.resident_count(), 1);
//
//         Rc::clone(&persistence).run().await;
//         settle_partition_io(&mut partition, &config).await;
//         persistence.drain().await.unwrap();
//         settle_partition_io(&mut partition, &config).await;
//         assert!(partition.fatal().is_none());
//         assert_eq!(partition.log.journal().inner.resident_count(), 0);
//         assert_eq!(persistence.checkpoint_op(), header.op);
//     }

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

//     #[compio::test]
//     async fn given_busy_wal_when_primary_transfer_drains_should_fence_admission_and_release_pause()
//     {
//         for expire in [false, true] {
//             let directory = tempfile::tempdir().unwrap();
//             let (mut partition, replies) = recording_partition_at(0, 3);
//             partition.set_created_revision(7);
//             partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
//             partition.open_persistence().await.unwrap();
//             let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
//             let writer = hold_wal_writer(&persistence);
//             let request = primary_transfer_grant(&partition);
//             for (created_revision, expected_view, expires_at) in [
//                 (
//                     request.created_revision + 1,
//                     request.expected_view,
//                     request.expires_at,
//                 ),
//                 (
//                     request.created_revision,
//                     request.expected_view + 1,
//                     request.expires_at,
//                 ),
//                 (
//                     request.created_revision,
//                     request.expected_view,
//                     std::time::Instant::now(),
//                 ),
//             ] {
//                 assert!(
//                     !partition.defer_primary_transfer(crate::PreparedPrimaryTransfer {
//                         created_revision,
//                         expected_view,
//                         target: request.target,
//                         expires_at,
//                     })
//                 );
//                 assert!(partition.transition.is_none());
//                 assert!(persistence.has_capacity(size_of::<PrepareHeader>()));
//             }
//             assert!(partition.defer_primary_transfer(request));
//             let config = repair_config();
//             assert!(matches!(
//                 partition.resume_io(&config).await,
//                 crate::PartitionIoStep::Pending
//             ));
//             assert!(!persistence.has_capacity(size_of::<PrepareHeader>()));
//             partition
//                 .on_request(
//                     store_offset_request(42, 1, ConsumerKind::Consumer, 7, 0, AckLevel::NoAck),
//                     None,
//                 )
//                 .await;
//             let status = bytemuck::checked::from_bytes::<ReplyHeader>(
//                 &replies.borrow()[0].1.as_slice()[..size_of::<ReplyHeader>()],
//             )
//             .status;
//             assert_eq!(status, IggyError::TransientNotAccepted.as_code());
//             assert_eq!(partition.consensus.sequencer().current_sequence(), 0);
//             assert!(!partition.defer_primary_transfer(primary_transfer_grant(&partition)));
//
//             if expire {
//                 let Some(PendingPartitionTransition::PrimaryTransfer { request, .. }) =
//                     &mut partition.transition
//                 else {
//                     panic!("the held writer must keep its grant");
//                 };
//                 request.expires_at = std::time::Instant::now();
//                 assert!(matches!(
//                     partition.resume_io(&config).await,
//                     crate::PartitionIoStep::Pending
//                 ));
//                 assert!(partition.transition.is_none());
//                 assert!(persistence.has_capacity(size_of::<PrepareHeader>()));
//             }
//             writer.await;
//             let step = partition.resume_io(&config).await;
//             if expire {
//                 assert!(matches!(step, crate::PartitionIoStep::Pending));
//             } else {
//                 let crate::PartitionIoStep::PrimaryTransfer(request) = step else {
//                     panic!("settled WAL must release the transfer continuation");
//                 };
//                 assert_eq!(request.created_revision, 7);
//                 assert_eq!(request.expected_view, 0);
//                 assert_eq!(request.target, 1);
//             }
//             assert!(persistence.has_capacity(size_of::<PrepareHeader>()));
//             assert_eq!(partition.consensus.view(), 0);
//             assert!(partition.transition.is_none());
//             assert!(partition.fatal().is_none());
//         }
//     }

// #[compio::test]
// async fn given_accepted_no_ack_write_when_primary_transfer_drains_should_finish_its_reply() {
//     const CLIENT: u128 = 42;
//     const CONSUMER: u32 = 7;
//     const STEPS_MAX: usize = 32;
//     let directory = tempfile::tempdir().unwrap();
//     let (mut partition, replies) = recording_partition_at(0, 3);
//     set_offset_dirs_under(&mut partition, directory.path());
//     partition.stats.increment_messages_count(1);
//     partition.set_io_notifier(
//         Rc::new(|_, _| {}),
//         crate::largest_legal_job_charge().unwrap(),
//     );
//     let store = store_offset_request(
//         CLIENT,
//         1,
//         ConsumerKind::Consumer,
//         CONSUMER,
//         0,
//         AckLevel::NoAck,
//     );
//     partition
//         .apply_consumer_offset_no_ack(
//             Box::new(*store.header()),
//             ConsumerKind::Consumer,
//             CONSUMER,
//             Some(0),
//             None,
//         )
//         .await;
//     let config = repair_config();
//     let captured = loop {
//         match partition.resume_io(&config).await {
//             crate::PartitionIoStep::Progress => {}
//             crate::PartitionIoStep::Ready(plan) => {
//                 break partition.capture_io(plan, &config).unwrap().unwrap();
//             }
//             _ => panic!("accepted store must reach its file write"),
//         }
//     };
//     assert_eq!(
//         captured.identity.continuation,
//         crate::PartitionIoContinuation::NoAck
//     );
//     assert!(partition.defer_primary_transfer(primary_transfer_grant(&partition)));
//     assert!(matches!(
//         partition.resume_io(&config).await,
//         crate::PartitionIoStep::Pending
//     ));
//     assert!(replies.borrow().is_empty());
//     let result = captured.job.execute().await;
//     assert!(matches!(
//         partition.resume_io(&config).await,
//         crate::PartitionIoStep::Pending
//     ));
//     partition.accept_io(captured.identity, result).unwrap();
//     if let Some(gate) = captured.gate {
//         gate.release();
//     }
//     let mut finished = false;
//     for _ in 0..STEPS_MAX {
//         match partition.resume_io(&config).await {
//             crate::PartitionIoStep::Progress => {}
//             crate::PartitionIoStep::Ready(plan) => {
//                 execute_partition_io_plan(&mut partition, &config, plan).await;
//             }
//             crate::PartitionIoStep::PrimaryTransfer(_) => {
//                 finished = true;
//                 break;
//             }
//             _ => panic!("accepted store must settle before transfer"),
//         }
//     }
//     assert!(finished);
//     assert_eq!(
//         partition.get_consumer_offset(PollingConsumer::Consumer(
//             usize::try_from(CONSUMER).unwrap(),
//             0,
//         )),
//         Some(0)
//     );
//     assert_eq!(replies.borrow().len(), 1);
//     assert_eq!(
//         bytemuck::checked::from_bytes::<ReplyHeader>(
//             &replies.borrow()[0].1.as_slice()[..size_of::<ReplyHeader>()],
//         )
//         .status,
//         0
//     );
//     assert!(
//         !bytemuck::checked::from_bytes::<ReplyHeader>(
//             &replies.borrow()[0].1.as_slice()[..size_of::<ReplyHeader>()],
//         )
//         .has_committed_result()
//     );
//     assert!(partition.no_ack.is_empty());
//     assert_eq!(partition.consensus.view(), 0);
// }

// #[compio::test]
// async fn given_unapplied_commit_when_primary_transfer_drains_should_apply_before_handoff() {
//     const STEPS_MAX: usize = 64;
//     let directory = tempfile::tempdir().unwrap();
//     let (mut partition, _replies, config, parent) =
//         partition_with_committed_send(directory.path(), iggy_common::Durability::Replicated, 3)
//             .await;
//     partition.set_io_notifier(
//         Rc::new(|_, _| {}),
//         crate::largest_legal_job_charge().unwrap(),
//     );
//     let prepare = checksummed_segment_prepare(2, parent, 1, b"committed-before-transfer");
//     let header = *prepare.header();
//     let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
//     persistence
//         .append(prepare.clone().into_frozen(), false)
//         .unwrap();
//     assert!(persistence.start());
//     Rc::clone(&persistence).run().await;
//     partition
//         .consensus
//         .with_pipeline_mut(|pipeline| pipeline.push(PipelineEntry::new(header)));
//     partition
//         .append_repaired_send_messages(prepare)
//         .await
//         .unwrap();
//     partition.consensus.sequencer().set_sequence(header.op);
//     partition
//         .consensus
//         .set_last_prepare_checksum(header.checksum);
//     assert_eq!(partition.consensus.commit_min(), 1);
//     assert!(partition.defer_primary_transfer(primary_transfer_grant(&partition)));
//     assert!(
//         matches!(
//             partition.resume_io(&config).await,
//             crate::PartitionIoStep::Pending
//         ),
//         "an accepted prepare must finish before the primary hands off"
//     );
//     partition.consensus.advance_commit_max(header.op);
//     let mut finished = false;
//     for _ in 0..STEPS_MAX {
//         match partition.resume_io(&config).await {
//             crate::PartitionIoStep::Progress => {}
//             crate::PartitionIoStep::Ready(plan) => {
//                 execute_partition_io_plan(&mut partition, &config, plan).await;
//             }
//             crate::PartitionIoStep::PrimaryTransfer(request) => {
//                 assert_eq!(partition.consensus.commit_min(), header.op);
//                 assert!(
//                     !partition
//                         .consensus
//                         .request_primary_transfer(request.expected_view, request.target)
//                         .is_empty()
//                 );
//                 finished = true;
//                 break;
//             }
//             _ => panic!("committed prefix must drain under the admission fence"),
//         }
//     }
//     assert!(finished);
//     assert_eq!(partition.stats.messages_count_inconsistent(), 2);
//     assert!(partition.fatal().is_none());
// }

//     #[compio::test]
//     async fn given_queued_requests_when_primary_transfer_drains_should_refuse_them_and_hand_off() {
//         const GROUP_ID: u64 = 8;
//         const OWNER_ID: u128 = 42;
//         const ADMITTED_CLIENT: u128 = 100;
//         const QUEUED_CLIENT: u128 = 101;
//         const STEPS_MAX: usize = 64;
//         let directory = tempfile::tempdir().unwrap();
//         let (mut partition, replies) =
//             recording_partition_with_pipeline(0, 3, LocalPipeline::with_capacities(1, 2));
//         install_test_owner(&mut partition, GROUP_ID, OWNER_ID).await;
//         set_offset_dirs_under(&mut partition, directory.path());
//         partition.set_io_notifier(
//             Rc::new(|_, _| {}),
//             crate::largest_legal_job_charge().unwrap(),
//         );
//         partition.stats.increment_messages_count(1);
//         for (client_id, consumer_id) in [(ADMITTED_CLIENT, 7), (QUEUED_CLIENT, 9)] {
//             let store = store_offset_request(
//                 client_id,
//                 1,
//                 ConsumerKind::Consumer,
//                 consumer_id,
//                 0,
//                 AckLevel::Quorum,
//             )
//             .transmute_header(|mut header, next: &mut RoutedRequestHeader| {
//                 header.metadata_watermark = partition.required_metadata_frontier;
//                 *next = header;
//             });
//             partition.on_request(store, None).await;
//         }
//         let installation = next_test_installation(&partition, GROUP_ID, OWNER_ID + 1);
//         let (sender, receiver) = consensus::oneshot_channel();
//         partition
//             .on_request(
//                 owner_install_request(&partition, &installation),
//                 Some(sender),
//             )
//             .await;
//         assert_eq!(partition.consensus.pipeline_len(), 1);
//         assert_eq!(partition.consensus.request_queue_len(), 2);
//         assert_eq!(partition.pending_owner_install.get(), Some(installation));
//
//         assert!(partition.defer_primary_transfer(primary_transfer_grant(&partition)));
//         partition
//             .consensus
//             .advance_commit_max(partition.consensus.sequencer().current_sequence());
//         let config = repair_config();
//         let mut transfer = None;
//         for _ in 0..STEPS_MAX {
//             match partition.resume_io(&config).await {
//                 crate::PartitionIoStep::Progress => {}
//                 crate::PartitionIoStep::Ready(plan) => {
//                     execute_partition_io_plan(&mut partition, &config, plan).await;
//                 }
//                 crate::PartitionIoStep::PrimaryTransfer(request) => {
//                     transfer = Some(request);
//                     break;
//                 }
//                 crate::PartitionIoStep::Pending => break,
//                 _ => panic!("the drain must only commit and hand off"),
//             }
//         }
//         let Some(request) = transfer else {
//             panic!(
//                 "a queued request must not hold the handoff: {} queued, {} prepared, commit {}/{}",
//                 partition.consensus.request_queue_len(),
//                 partition.consensus.pipeline_len(),
//                 partition.consensus.commit_min(),
//                 partition.consensus.commit_max(),
//             );
//         };
//         assert!(
//             !partition
//                 .consensus
//                 .request_primary_transfer(request.expected_view, request.target)
//                 .is_empty()
//         );
//         assert_eq!(
//             receiver.await.unwrap().header().status,
//             IggyError::TransientNotAccepted.as_code()
//         );
//         let statuses = |client_id| {
//             replies
//                 .borrow()
//                 .iter()
//                 .filter(|(client, _)| *client == client_id)
//                 .map(|(_, frame)| {
//                     bytemuck::checked::from_bytes::<ReplyHeader>(
//                         &frame.as_slice()[..size_of::<ReplyHeader>()],
//                     )
//                     .status
//                 })
//                 .collect::<Vec<_>>()
//         };
//         assert_eq!(statuses(ADMITTED_CLIENT), [0]);
//         assert_eq!(
//             statuses(QUEUED_CLIENT),
//             [IggyError::TransientNotAccepted.as_code()]
//         );
//         assert!(
//             partition.pending_owner_install.get().is_none(),
//             "a refused install must release its latch"
//         );
//         assert!(partition.fatal().is_none());
//     }
//
//     #[compio::test]
//     async fn given_planned_primary_transfer_when_a_view_frame_arrives_should_preserve_real_transition()
//      {
//         let (mut partition, _) = recording_partition_at(0, 3);
//         assert!(partition.defer_primary_transfer(primary_transfer_grant(&partition)));
//         let message = Message::<StartViewHeader>::new(size_of::<StartViewHeader>())
//             .transmute_header(|_, header: &mut StartViewHeader| {
//                 header.command = Command::StartView;
//                 header.size = u32::try_from(size_of::<StartViewHeader>()).unwrap();
//                 header.view = 1;
//                 header.replica = 1;
//             });
//         partition.defer_view_transition(server_common::MessageBag::StartView(message));
//         partition.cancel_primary_transfer();
//         assert!(matches!(
//             partition.transition,
//             Some(PendingPartitionTransition::View { .. })
//         ));
//         assert!(matches!(partition.resume_io(&repair_config()).await,
//             crate::PartitionIoStep::Transition(server_common::MessageBag::StartView(message))
//             if message.header().view == 1));
//     }

//     #[compio::test]
//     async fn given_backup_replica_when_granted_a_primary_transfer_should_refuse_and_keep_committing()
//      {
//         const BACKUP: u8 = 2;
//         for target in [1, BACKUP] {
//             let root = tempfile::tempdir().unwrap();
//             let mut config = repair_config();
//             config.path_layout.streams_root = root.path().to_string_lossy().into_owned();
//             let directory = std::path::PathBuf::from(config.get_partition_path(1, 1, 0));
//             std::fs::create_dir_all(&directory).unwrap();
//             let (mut backup, _) = recording_partition_at(BACKUP, 3);
//             backup.set_partition_dir(directory.to_string_lossy().into_owned());
//             set_offset_dirs_under(&mut backup, &directory);
//             backup.open_persistence().await.unwrap();
//             backup.log.retire_front().unwrap();
//             backup.install_empty_segment(&config, 0).await.unwrap();
//             backup.set_io_notifier(
//                 Rc::new(|_, _| {}),
//                 crate::largest_legal_job_charge().unwrap(),
//             );
//             let persistence = Rc::clone(backup.persistence.as_ref().unwrap());
//             let writer = hold_wal_writer(&persistence);
//             let first = checksummed_segment_prepare(1, 0, 0, b"before-grant");
//             let parent = first.header().checksum;
//             backup.on_replicate(first).await;
//             assert!(
//                 !backup.defer_primary_transfer(crate::PreparedPrimaryTransfer {
//                     target,
//                     ..primary_transfer_grant(&backup)
//                 }),
//                 "backup {BACKUP} accepted a grant for target {target}"
//             );
//             assert!(backup.transition.is_none());
//             assert!(persistence.has_capacity(size_of::<PrepareHeader>()));
//
//             // Replica 1 acks op 2, so the primary commits it. An unfenced
//             // backup journals the forwarded prepare and commits it as well.
//             backup
//                 .on_replicate(checksummed_segment_prepare(2, parent, 1, b"after-grant"))
//                 .await;
//             writer.await;
//             commit_on_heartbeat(&mut backup, &config, 2).await;
//             settle_partition_io(&mut backup, &config).await;
//             assert_eq!(backup.consensus.sequencer().current_sequence(), 2);
//             assert_eq!(persistence.written_op(), 2);
//             assert_eq!(backup.consensus.commit_min(), 2);
//         }
//     }

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

/// Under sustained produce load every lane turn sees a busy WAL writer.
/// Retention of a sealed segment that a published checkpoint covers must
/// not wait for that writer.
#[compio::test]
async fn given_busy_wal_writer_when_retaining_a_checkpointed_segment_should_not_wait() {
    const RECORDS_PER_SEGMENT: u64 = 2;

    let root = tempfile::tempdir().unwrap();
    let mut config = repair_config();
    config.path_layout.streams_root = root.path().to_string_lossy().into_owned();
    let directory = std::path::PathBuf::from(config.get_partition_path(1, 1, 0));
    std::fs::create_dir_all(&directory).unwrap();
    let (mut partition, _) = recording_partition_at(1, 3);
    partition.set_partition_dir(directory.to_string_lossy().into_owned());
    partition.runtime_options.durability = iggy_common::Durability::Replicated;
    partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Replicated;
    let record_len = checksummed_segment_prepare(1, 0, 0, b"payload")
        .as_slice()
        .len()
        - size_of::<PrepareHeader>();
    let segment_size = IggyByteSize::from(RECORDS_PER_SEGMENT * record_len as u64);
    partition.runtime_options.segment_size = Some(segment_size);
    partition.log.active_segment_mut().max_size = segment_size;
    partition.open_persistence().await.unwrap();
    partition.log.retire_front().unwrap();
    partition.install_empty_segment(&config, 0).await.unwrap();
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    let layout = |partition: &IggyPartition<RecordingBus>| {
        partition
            .log
            .segments()
            .iter()
            .map(|segment| (segment.start_offset, segment.end_offset, segment.sealed))
            .collect::<Vec<_>>()
    };

    // Sustained produce load: a new append restarts the writer before
    // each lane turn, so no turn sees an idle writer.
    let mut parent = 0;
    let mut writer = hold_wal_writer(&persistence);
    for offset in 0..=RECORDS_PER_SEGMENT {
        let op = offset + 1;
        let prepare = checksummed_segment_prepare(op, parent, offset, b"payload");
        parent = prepare.header().checksum;
        partition.on_replicate(prepare).await;
        writer.await;
        writer = hold_wal_writer(&persistence);
        commit_on_heartbeat(&mut partition, &config, op).await;
        settle_partition_io(&mut partition, &config).await;
    }
    assert_eq!(layout(&partition), vec![(0, 1, true), (2, 2, false)]);

    // A due checkpoint, as the shard tick starts it. The byte trigger needs
    // half of a WAL budget of at least 128 MiB, so the test requests it.
    persistence.request_checkpoint();
    assert!(partition.needs_persistence_checkpoint());
    partition.checkpoint_persistence(&config).await;
    settle_partition_io(&mut partition, &config).await;

    // The load stops: the writer completes the checkpoint and goes idle.
    writer.await;
    settle_partition_io(&mut partition, &config).await;
    assert!(partition.fatal().is_none());
    assert!(
        persistence
            .segment_checkpoint()
            .is_some_and(|checkpoint| checkpoint.next_offset > RECORDS_PER_SEGMENT - 1),
        "the checkpoint covers the sealed segment"
    );
    assert_eq!(layout(&partition), vec![(0, 1, true), (2, 2, false)]);
    assert!(directory.join("00000000000000000000.log").is_file());
    assert_eq!(
        partition.stats.messages_count_inconsistent(),
        RECORDS_PER_SEGMENT + 1
    );
    let writer = hold_wal_writer(&persistence);
    assert!(!persistence.is_quiescent());
    partition
        .remove_sealed_segments_up_to(RECORDS_PER_SEGMENT - 1)
        .await;
    let crate::PartitionIoStep::Ready(plan) = partition.resume_io(&config).await else {
        panic!("a busy writer must not block retention of a checkpointed sealed segment");
    };
    assert_eq!(plan.continuation, crate::PartitionIoContinuation::Retention);
    execute_partition_io_plan(&mut partition, &config, plan).await;
    assert_eq!(layout(&partition), vec![(2, 2, false)]);
    writer.await;
}

/// Past the byte cap the evicted ring releases the bodies that the WAL
/// wrote to segments and keeps their headers. Each body reads back from
/// its segment as the replicated prepare, after the WAL checkpoint too:
/// ops 1 and 2 in one read from the first segment, op 3 from the second.
/// Retention of a segment forgets the ops in it.
#[compio::test]
async fn given_bodies_past_the_ring_byte_cap_when_read_back_should_return_the_replicated_prepares()
{
    const RECORDS_PER_SEGMENT: u64 = 2;
    const OPS: u64 = 4;
    const PAYLOAD: [u8; 4096] = [7; 4096];

    let root = tempfile::tempdir().unwrap();
    let mut config = repair_config();
    config.path_layout.streams_root = root.path().to_string_lossy().into_owned();
    let directory = std::path::PathBuf::from(config.get_partition_path(1, 1, 0));
    std::fs::create_dir_all(&directory).unwrap();
    let (mut partition, _) = recording_partition_at(1, 3);
    partition.set_partition_dir(directory.to_string_lossy().into_owned());
    partition.runtime_options.durability = iggy_common::Durability::Replicated;
    partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Replicated;
    let prepare_len = checksummed_segment_prepare(1, 0, 0, &PAYLOAD)
        .as_slice()
        .len();
    let segment_size =
        IggyByteSize::from(RECORDS_PER_SEGMENT * (prepare_len - size_of::<PrepareHeader>()) as u64);
    partition.runtime_options.segment_size = Some(segment_size);
    partition.log.active_segment_mut().max_size = segment_size;
    partition.open_persistence().await.unwrap();
    partition.log.retire_front().unwrap();
    partition.install_empty_segment(&config, 0).await.unwrap();
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    // Room for one body, but not for two.
    partition
        .log
        .journal()
        .inner
        .set_ring_caps(crate::EVICTED_RING_CAPACITY, (prepare_len * 3 / 2) as u64);
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());

    let mut prepares = Vec::new();
    let mut parent = 0;
    let mut writer = hold_wal_writer(&persistence);
    for offset in 0..OPS {
        let op = offset + 1;
        let prepare = checksummed_segment_prepare(op, parent, offset, &PAYLOAD);
        parent = prepare.header().checksum;
        prepares.push(prepare.clone());
        partition.on_replicate(prepare).await;
        writer.await;
        writer = hold_wal_writer(&persistence);
        commit_on_heartbeat(&mut partition, &config, op).await;
        settle_partition_io(&mut partition, &config).await;
    }
    assert_eq!(
        partition.log.journal().inner.repair_retained_from(),
        Some(1),
        "repair serves every committed op"
    );

    persistence.request_checkpoint();
    partition.checkpoint_persistence(&config).await;
    settle_partition_io(&mut partition, &config).await;
    writer.await;
    settle_partition_io(&mut partition, &config).await;
    let released: Vec<_> = prepares[..prepares.len() - 1]
        .iter()
        .map(|prepare| {
            partition
                .log
                .journal()
                .inner
                .evicted_header(prepare.header().op)
                .expect("the byte cap released this body")
        })
        .collect();
    let mut bodies = crate::ReleasedRead::new(&partition.log, released.clone())
        .read()
        .await;
    for (evicted, prepare) in released.iter().zip(&prepares) {
        let body = bodies
            .take(evicted)
            .expect("the read brings every body back");
        assert_eq!(body.as_slice(), prepare.as_slice());
    }

    partition
        .remove_sealed_segments_up_to(RECORDS_PER_SEGMENT - 1)
        .await;
    let crate::PartitionIoStep::Ready(plan) = partition.resume_io(&config).await else {
        panic!("retention of the checkpointed segment is due");
    };
    execute_partition_io_plan(&mut partition, &config, plan).await;
    assert_eq!(
        partition.log.journal().inner.repair_retained_from(),
        Some(RECORDS_PER_SEGMENT + 1),
        "repair answers RangeEvicted for the ops of the removed segment"
    );
}
//
//     #[compio::test]
//     async fn given_transfer_flush_when_tick_promotes_a_queued_send_should_ack_it_on_the_primary() {
//         let root = tempfile::tempdir().unwrap();
//         let (mut partition, _replies, mut config, parent) =
//             partition_with_committed_send(root.path(), iggy_common::Durability::Replicated, 3)
//                 .await;
//         partition.set_io_notifier(
//             Rc::new(|_, _| {}),
//             crate::largest_legal_job_charge().unwrap(),
//         );
//         let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
//         config.messages_required_to_save = 8;
//         let prepare = checksummed_segment_prepare(2, parent, 1, b"resident");
//         let header = *prepare.header();
//         persistence
//             .append(prepare.clone().into_frozen(), false)
//             .unwrap();
//         assert!(persistence.start());
//         Rc::clone(&persistence).run().await;
//         partition.consensus.with_pipeline_mut(|pipeline| {
//             pipeline.push(PipelineEntry::new(header));
//         });
//         partition
//             .append_repaired_send_messages(prepare)
//             .await
//             .unwrap();
//         partition.consensus.advance_commit_max(2);
//         partition.commit_journal(&config).await;
//         assert_eq!(partition.consensus.commit_min(), 2);
//         assert_eq!(partition.log.journal().inner.resident_count(), 1);
//         partition.consensus.sequencer().set_sequence(2);
//         partition
//             .consensus
//             .set_last_prepare_checksum(header.checksum);
//         for _ in 0..2 {
//             let order = partition.next_local_request_order().unwrap();
//             let request = consensus::RequestEntry::with_sender(
//                 checksumless_send_request(partition.namespace(), 1),
//                 None,
//             );
//             assert!(partition.queue_local_request(request, order).is_ok());
//         }
//
//         assert!(matches!(
//             partition.state_transfer_offer(&config).await,
//             Err(PartitionTransferUnavailable::FlushPending)
//         ));
//         assert!(partition.drive_transfer_plan());
//         assert!(partition.maintenance.transfer_flush);
//
//         partition.resume_queued_requests().await;
//         settle_partition_io(&mut partition, &config).await;
//         partition.resume_queued_requests().await;
//
//         assert!(
//             partition
//                 .consensus
//                 .with_pipeline(|pipeline| pipeline.entry_by_op(3).is_some()
//                     && pipeline.entry_by_op(4).is_some()),
//             "both queued sends must be promoted"
//         );
//         let mut loopback = Vec::new();
//         partition.consensus().drain_loopback_into(&mut loopback);
//         let acked: Vec<u64> = loopback
//             .into_iter()
//             .map(|ack| ack.try_into_typed::<PrepareOkHeader>().unwrap().header().op)
//             .collect();
//         assert!(
//             acked.contains(&4),
//             "control: the primary acks a send promoted after the flush: {acked:?}"
//         );
//         assert!(
//             acked.contains(&3),
//             "a send promoted during the transfer flush must retain its self-ack: {acked:?}"
//         );
//         assert!(persistence.head() >= 4);
//     }

//     #[compio::test]
//     async fn committed_offset_mutations_obey_checkpoint_dependencies() {
//         const CLIENT: u128 = 42;
//         const CONSUMER: u32 = 7;
//         let kind = ConsumerKind::Consumer;
//         for delete in [false, true] {
//             let root = tempfile::tempdir().unwrap();
//             let (mut partition, replies, config, parent) =
//                 partition_with_committed_send(root.path(), iggy_common::Durability::Replicated, 3)
//                     .await;
//             let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
//             let request = if delete {
//                 let initial = PendingConsumerOffsetCommit::upsert(kind, CONSUMER, 0);
//                 partition
//                     .persist_consumer_offset_commit(initial)
//                     .await
//                     .unwrap();
//                 partition.apply_consumer_offset_commit(initial);
//                 delete_offset_request(CLIENT, 1, CONSUMER)
//             } else {
//                 store_offset_request(CLIENT, 1, kind, CONSUMER, 0, AckLevel::Quorum)
//             };
//             let store = request.transmute_header(|request, header: &mut PrepareHeader| {
//                 *header = PrepareHeader {
//                     command: Command::Prepare,
//                     operation: request.operation,
//                     cluster: TEST_CLUSTER,
//                     group: request.group,
//                     size: request.size,
//                     client: CLIENT,
//                     user_id: request.user_id,
//                     session: request.session,
//                     request: request.request,
//                     retry_capacity: u32::try_from(consensus::PARTITION_DEDUP_CLIENTS_MAX).unwrap(),
//                     op: 2,
//                     parent,
//                     ..PrepareHeader::default()
//                 };
//                 header.checksum = header.identity_checksum();
//             });
//             let header = *store.header();
//             persistence
//                 .append(store.clone().into_frozen(), false)
//                 .unwrap();
//             partition.consensus.with_pipeline_mut(|pipeline| {
//                 pipeline.push(PipelineEntry::new(header));
//             });
//             partition
//                 .log
//                 .journal()
//                 .inner
//                 .append(store.into_frozen())
//                 .await
//                 .unwrap();
//             if delete {
//                 partition.stage_consumer_offset_delete(header.op, kind, CONSUMER);
//             } else {
//                 partition.stage_consumer_offset_upsert(header.op, kind, CONSUMER, 0, false);
//             }
//             persistence.checkpoint(1);
//             partition.consensus.advance_commit_max(header.op);
//             let path = partition.persisted_offset_path(kind, CONSUMER).unwrap();
//
//             partition.commit_journal(&config).await;
//             assert!(partition.fatal().is_none());
//             assert_eq!(
//                 partition.consensus.commit_min(),
//                 if delete { 1 } else { header.op },
//                 "only a delete must wait for the checkpoint"
//             );
//             assert!(std::path::Path::new(&path).exists());
//             assert_eq!(replies.borrow().len(), if delete { 1 } else { 2 });
//
//             partition.start_persistence();
//             persistence.drain().await.unwrap();
//             assert!(!partition.persistence_checkpoint_pending());
//             partition.commit_journal(&config).await;
//             assert!(partition.fatal().is_none());
//             assert_eq!(partition.consensus.commit_min(), header.op);
//             assert_eq!(std::path::Path::new(&path).is_file(), !delete);
//             assert_eq!(replies.borrow().len(), 2);
//         }
//     }

//     #[compio::test]
//     async fn no_ack_offset_writes_continue_during_a_pending_checkpoint() {
//         const CONSUMER: u32 = 7;
//         let kind = ConsumerKind::Consumer;
//         let root = tempfile::tempdir().unwrap();
//         let (mut partition, _replies, _config, _parent) =
//             partition_with_committed_send(root.path(), iggy_common::Durability::Replicated, 1)
//                 .await;
//         let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
//         persistence.checkpoint(1);
//         let path = partition.persisted_offset_path(kind, CONSUMER).unwrap();
//         let (sender, receiver) = consensus::oneshot_channel();
//         let mut receiver = std::pin::pin!(receiver);
//
//         partition
//             .on_request(
//                 store_offset_request(42, 1, kind, CONSUMER, 0, AckLevel::NoAck),
//                 Some(sender),
//             )
//             .await;
//         assert!(partition.fatal().is_none());
//         assert!(std::path::Path::new(&path).exists());
//         let reply = receiver.as_mut().await.unwrap();
//         assert_eq!(reply.header().status, 0);
//
//         partition.start_persistence();
//         persistence.drain().await.unwrap();
//         partition.resume_queued_requests().await;
//         assert!(std::path::Path::new(&path).is_file());
//     }

#[compio::test]
async fn given_small_written_prefix_when_flush_is_due_should_wait_without_rebuilding_the_tail() {
    const THROUGH: u64 = 9;
    const WRITTEN: u64 = 3;
    let root = tempfile::tempdir().unwrap();
    let (mut partition, _, config, mut parent) =
        partition_with_committed_send(root.path(), iggy_common::Durability::Replicated, 3).await;
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    for op in 2..=THROUGH {
        let prepare = checksummed_segment_prepare(op, parent, op - 1, b"queued");
        parent = prepare.header().checksum;
        persistence
            .append(prepare.clone().into_frozen(), false)
            .unwrap();
        partition
            .append_repaired_send_messages(prepare)
            .await
            .unwrap();
        if op == WRITTEN {
            assert!(persistence.start());
            Rc::clone(&persistence).run().await;
        }
    }
    let writer = hold_wal_writer(&persistence);
    partition.consensus.advance_commit_max(THROUGH);
    for op in 2..=THROUGH {
        partition.consensus.advance_commit_min(op);
    }
    partition.consensus.sequencer().set_sequence(THROUGH);
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    partition.defer_flush(THROUGH);
    partition.resume_io(&config).await;
    assert!(
        partition.materialization.is_none(),
        "two written entries must not rebuild six retained entries"
    );
    assert_eq!(
        partition.log.journal().inner.resident_count(),
        usize::try_from(THROUGH - 1).unwrap()
    );
    writer.await;
    settle_partition_io(&mut partition, &config).await;
    assert!(partition.fatal().is_none());
    assert_eq!(partition.log.journal().inner.resident_count(), 0);
}

#[compio::test]
async fn given_a_discarded_suffix_with_an_offset_gap_when_truncated_should_reuse_the_retained_frontier()
 {
    let mut partition = partition_at_view(0, 0);
    let retained = checksummed_segment_prepare(1, 0, 0, b"retained");
    let parent = retained.header().checksum;
    partition
        .append_repaired_send_messages(retained)
        .await
        .unwrap();
    let discarded = checksummed_segment_prepare(3, parent, 2, b"discarded");
    partition
        .append_repaired_send_messages(discarded)
        .await
        .unwrap();
    partition.consensus.sequencer().set_sequence(3);

    partition.truncate_uncommitted_from(2).await.unwrap();

    assert_eq!(partition.dirty_offset.load(Ordering::Relaxed), 0);
    let replacement = checksummed_segment_prepare(2, parent, 1, b"replacement");
    partition
        .append_received_send_messages_to_journal(replacement)
        .await
        .unwrap();
    assert_eq!(partition.dirty_offset.load(Ordering::Relaxed), 1);
    assert!(partition.log.journal().inner.holds_op(1));
    assert!(partition.log.journal().inner.holds_op(2));
    assert!(!partition.log.journal().inner.holds_op(3));
}

#[compio::test]
async fn live_wal_rollback_preserves_replacement_indexes_and_pollable_bodies() {
    let root = tempfile::tempdir().unwrap();
    let mut config = repair_config();
    config.path_layout.streams_root = root.path().to_string_lossy().into_owned();
    let directory = std::path::PathBuf::from(config.get_partition_path(1, 1, 0));
    std::fs::create_dir_all(&directory).unwrap();
    let mut partition = partition_at_view(0, 0);
    partition.set_partition_dir(directory.to_string_lossy().into_owned());
    partition.runtime_options.durability = iggy_common::Durability::Persisted;
    let first = checksummed_segment_prepare(1, 0, 0, b"first");
    let parent = first.header().checksum;
    let segment_size =
        IggyByteSize::from((first.as_slice().len() - size_of::<PrepareHeader>()) as u64);
    partition.runtime_options.segment_size = Some(segment_size);
    partition.log.active_segment_mut().max_size = segment_size;
    partition.open_persistence().await.unwrap();
    partition.log.retire_front().unwrap();
    partition
        .install_empty_segment(&repair_config(), 0)
        .await
        .unwrap();
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    persistence
        .append(first.clone().into_frozen(), true)
        .unwrap();
    assert!(persistence.start());
    Rc::clone(&persistence).run().await;
    partition
        .append_repaired_send_messages(first)
        .await
        .unwrap();
    partition.consensus.restore_commit_state(0, 1);
    partition.commit_journal(&repair_config()).await;
    let old = checksummed_segment_prepare(2, parent, 1, b"discarded");
    persistence.append(old.clone().into_frozen(), true).unwrap();
    assert!(persistence.start());
    Rc::clone(&persistence).run().await;
    partition.append_repaired_send_messages(old).await.unwrap();
    partition.truncate_uncommitted_from(2).await.unwrap();
    let replacement = checksummed_segment_prepare(2, parent, 1, b"replacement");
    let expected = replacement.as_slice()[size_of::<PrepareHeader>()..].to_vec();
    persistence
        .append(replacement.clone().into_frozen(), true)
        .unwrap();
    assert!(persistence.start());
    Rc::clone(&persistence).run().await;
    partition
        .append_repaired_send_messages(replacement)
        .await
        .unwrap();
    partition.consensus.advance_commit_max(2);
    partition.commit_journal(&repair_config()).await;
    assert!(partition.fatal().is_none());
    assert_eq!(partition.consensus.commit_min(), 2);
    assert_eq!(
        std::fs::read(directory.join("00000000000000000001.log")).unwrap(),
        expected
    );
    assert_eq!(
        std::fs::metadata(directory.join("00000000000000000001.index"))
            .unwrap()
            .len(),
        IGGY_INDEX_SIZE as u64
    );
    assert_eq!(partition.log.journal().inner.resident_count(), 0);
    let args = PollingArgs::new(iggy_common::PollingStrategy::offset(1), 1, false);
    let result = partition
        .build_poll_plan(PollingConsumer::Consumer(1, 0), &args, true)
        .execute()
        .await;
    let completion = partition.complete_poll(result).unwrap();
    let polled: Vec<_> = completion
        .fragments
        .iter()
        .flat_map(|fragment| fragment.as_slice().iter().copied())
        .collect();
    let batch = decode_batch_slice(&polled).unwrap();
    assert_eq!(batch.header.base_offset, 1);
    assert_eq!(batch.message_count(), 1);
    assert_eq!(batch.iter().next().unwrap().payload, b"replacement");
    persistence.request_checkpoint();
    partition.checkpoint_persistence(&config).await;
    persistence
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();
    assert!(persistence.failure().is_none());
    assert_eq!(persistence.checkpoint_op(), 2);
}

#[test]
fn given_topic_durability_when_installed_should_tell_consensus_whether_restarts_keep_acks() {
    let mut partition = test_partition();
    assert!(
        !partition.consensus().recovered_log_complete(),
        "the default `replicated` policy acknowledges before the write"
    );
    for (durability, consumer_offset_durability, complete) in [
        (Persisted, Persisted, true),
        (Replicated, Persisted, false),
        (Persisted, Replicated, false),
        (Replicated, Replicated, false),
    ] {
        partition.set_runtime_options(TopicRuntimeOptions {
            durability,
            consumer_offset_durability,
            ..TopicRuntimeOptions::default()
        });
        assert_eq!(
            partition.consensus().recovered_log_complete(),
            complete,
            "{durability:?} messages, {consumer_offset_durability:?} offsets"
        );
    }
}

// pub(super) fn test_partition() -> IggyPartition<IggyMessageBus> {
//     test_partition_with_replicas(1)
// }

// #[compio::test]
// async fn given_snapshot_transfer_when_writes_evict_its_tail_should_release_only_confirmed_progress_and_expire_idle_peers()
//  {
//     const PEER: u8 = 1;
//     const NONCE: u128 = 1;
//     const BYTES: usize = 64 * 1024;
//     let mut partition = test_partition_with_replicas(3);
//     let budget = Arc::new(crate::RepairMemoryBudget::new(BYTES));
//     partition.proof_walk_memory = Some(budget.reservation());
//     let journal = &partition.log.journal().inner;
//     journal.set_ring_caps(1, u64::MAX);
//     assert!(partition.retain_transfer_tail(PEER, NONCE, 0));
//     assert!(partition.transfer_tail_available(PEER, NONCE, 1));
//     for op in 1..=8 {
//         let prepare = checksummed_segment_prepare(op, 0, op - 1, b"tail");
//         journal.append(prepare.into_frozen()).await.unwrap();
//         journal.evict_prefix(1);
//     }
//     assert_eq!(journal.repair_retained_from(), Some(2));
//     partition.advance_transfer_tail(PEER, consensus::ReplicationProgress::Busy, 9);
//     assert_eq!(journal.repair_retained_from(), Some(2));
//     partition.advance_transfer_tail(
//         PEER,
//         consensus::ReplicationProgress::Ready { received_op: 4 },
//         10,
//     );
//     assert_eq!(journal.repair_retained_from(), Some(5));
//     partition.expire_transfer_tails(19, 10);
//     assert!(partition.touch_transfer_tail(PEER, NONCE, 19));
//     partition.renew_transfer_tail_for_repair(PEER, 28);
//     partition.expire_transfer_tails(29, 10);
//     assert_eq!(
//         journal.repair_retained_from(),
//         Some(5),
//         "a pull keeps the suffix without proving its prefix"
//     );
//     partition.expire_transfer_tails(38, 10);
//     assert_eq!(journal.repair_retained_from(), Some(8));
//     assert!(!partition.touch_transfer_tail(PEER, NONCE, 29));
//     assert!(
//         !partition.retain_transfer_tail(PEER, NONCE, 29),
//         "an expired nonce must not rebuild a different manifest"
//     );
//     assert!(partition.retain_transfer_tail(PEER, NONCE + 1, 29));
//     assert!(
//         !partition.transfer_tail_available(PEER, NONCE + 1, 1),
//         "an old snapshot cannot promise already-missing operations"
//     );
//     assert!(partition.transfer_tail_available(PEER, NONCE + 1, 8));
//     assert!(budget.reservation().try_add(BYTES));
// }

// fn test_partition_with_replicas(replica_count: u8) -> IggyPartition<IggyMessageBus> {
//     let namespace = IggyNamespace::new(1, 1, 0);
//     let consensus = VsrConsensus::new(
//         TEST_CLUSTER,
//         0,
//         replica_count,
//         namespace.inner(),
//         IggyMessageBus::new(0),
//         LocalPipeline::new(),
//     );
//     consensus.init();
//     IggyPartition::with_in_memory_storage(
//         Arc::new(PartitionStats::default()),
//         consensus,
//         IggyByteSize::from(1024 * 1024),
//     )
// }

//     #[test]
//     fn given_new_partition_when_created_revision_exceeds_session_end_should_require_retirement() {
//         let mut partition = test_partition();
//         let ended = SessionIdentity {
//             client_id: 1,
//             session: 2,
//             metadata_watermark: 3,
//         };
//         partition.set_created_revision(7);
//
//         assert!(
//             !partition.session_retired(ended),
//             "a Streams revision cannot certify an applied metadata retirement barrier"
//         );
//     }

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

// /// A SOLO partition, the shape the offset reservation is scoped to.
// fn solo_recording_partition() -> IggyPartition<IggyMessageBus, RecordingSuperblock> {
//     let namespace = IggyNamespace::new(1, 1, 0);
//     let consensus = VsrConsensus::new(
//         TEST_CLUSTER,
//         0,
//         1,
//         namespace.inner(),
//         IggyMessageBus::new(0),
//         LocalPipeline::new(),
//     );
//     consensus.init();
//     IggyPartition::with_in_memory_storage(
//         Arc::new(PartitionStats::default()),
//         consensus,
//         IggyByteSize::from(1024 * 1024),
//     )
// }

// #[compio::test]
// async fn missing_materialization_does_not_restore_wal_checkpoint_as_applied() {
//     let directory = tempfile::tempdir().unwrap();
//     let mut partition = partition_at_view(1, 1);
//     partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
//     partition.runtime_options.durability = iggy_common::Durability::Persisted;
//     let mut journal = journal::PartitionPrepareJournal::open(
//         &directory.path().join("prepares-0"),
//         partition.consensus().group(),
//         0,
//     )
//     .await
//     .unwrap();
//     journal.reset(7, Some(1234)).await.unwrap();
//     drop(journal);
//     crate::state_transfer::mark_materialization_missing(directory.path().to_str().unwrap(), 0)
//         .await
//         .unwrap();
//     publish_empty_retry_checkpoint(&partition, 7).await;
//     partition.open_persistence().await.unwrap();
//     assert!(partition.requires_state_transfer());
//     assert_eq!(partition.consensus().commit_min(), 0);
//     assert_eq!(partition.consensus().sequencer().current_sequence(), 0);
//     assert!(partition.consensus().is_transferring());
//     assert_eq!(partition.persistence.as_ref().unwrap().checkpoint_op(), 7);
//     partition.commit_journal(&repair_config()).await;
//     assert_eq!(partition.consensus().commit_min(), 0);
// }

//     #[cfg(target_os = "linux")]
//     #[compio::test]
//     async fn given_persisted_preallocation_when_rotating_should_reserve_without_extending() {
//         const SEGMENT_BYTES: u64 = 1024 * 1024;
//         const BLOCK_BYTES: u64 = 512;
//
//         for preallocate in [false, true] {
//             let directory = tempfile::tempdir().unwrap();
//             let probe = tempfile::tempfile_in(directory.path()).unwrap();
//             let preallocation_supported = match nix::fcntl::fallocate(
//                 &probe,
//                 nix::fcntl::FallocateFlags::FALLOC_FL_KEEP_SIZE,
//                 0,
//                 i64::try_from(SEGMENT_BYTES).unwrap(),
//             ) {
//                 Ok(()) => true,
//                 Err(nix::errno::Errno::EOPNOTSUPP | nix::errno::Errno::ENOSYS) => false,
//                 Err(error) => panic!("preallocation probe failed: {error}"),
//             };
//             let mut partition = partition_at_view(0, 0);
//             partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
//             partition.runtime_options.durability = iggy_common::Durability::Persisted;
//             partition.runtime_options.preallocate_segments = Some(preallocate);
//             partition.runtime_options.segment_size = Some(IggyByteSize::from(SEGMENT_BYTES));
//             partition.open_persistence().await.unwrap();
//             partition.log.retire_front().unwrap();
//             partition
//                 .install_empty_segment(&repair_config(), 0)
//                 .await
//                 .unwrap();
//             let empty =
//                 std::fs::metadata(directory.path().join("00000000000000000000.log")).unwrap();
//             assert_eq!(empty.len(), 0);
//             if preallocation_supported {
//                 assert_eq!(
//                     empty.blocks() * BLOCK_BYTES >= SEGMENT_BYTES,
//                     preallocate,
//                     "empty active segment allocation must follow preallocate_segments={preallocate}"
//                 );
//             }
//             let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
//             let namespace = partition.namespace();
//             let bodies = [
//                 build_segment_record_with_payload(
//                     namespace,
//                     0,
//                     Bytes::from(vec![1; usize::try_from(SEGMENT_BYTES).unwrap()]),
//                 ),
//                 build_segment_record(namespace, 1),
//             ];
//             let mut parent = 0;
//             for (offset, body) in bodies.iter().enumerate() {
//                 let total = size_of::<PrepareHeader>() + body.len();
//                 let mut prepare = Message::<PrepareHeader>::new(total);
//                 prepare.as_mut_slice()[size_of::<PrepareHeader>()..].copy_from_slice(body);
//                 let prepare = prepare.transmute_header(|_, header: &mut PrepareHeader| {
//                     header.command = Command::Prepare;
//                     header.operation = Operation::SendMessages;
//                     header.cluster = TEST_CLUSTER;
//                     header.group = namespace.inner();
//                     header.op = u64::try_from(offset).unwrap() + 1;
//                     header.parent = parent;
//                     header.size = u32::try_from(total).unwrap();
//                     header.checksum_body = u128::from(iggy_common::calculate_checksum(body));
//                     header.checksum = header.identity_checksum();
//                 });
//                 parent = prepare.header().checksum;
//                 persistence.append(prepare.into_frozen(), true).unwrap();
//             }
//             partition.start_persistence();
//             persistence
//                 .drain_with_timeout(compio::runtime::time::sleep)
//                 .await
//                 .unwrap();
//
//             let rotated = directory.path().join("00000000000000000001.log");
//             let metadata = std::fs::metadata(&rotated).unwrap();
//             assert_eq!(std::fs::read(&rotated).unwrap(), bodies[1]);
//             assert_eq!(metadata.len(), bodies[1].len() as u64);
//             if preallocation_supported {
//                 assert_eq!(
//                     metadata.blocks() * BLOCK_BYTES >= SEGMENT_BYTES,
//                     preallocate,
//                     "rotated segment allocation must follow preallocate_segments={preallocate}"
//                 );
//             }
//             assert_eq!(partition.log.active_segment().size.as_bytes_u64(), 0);
//         }
//     }

#[compio::test]
async fn retransmission_catches_up_sequencer_after_wal_admission_backpressure() {
    let mut partition = partition_at_view(1, 1);
    let prepare = Message::<PrepareHeader>::new(size_of::<PrepareHeader>()).transmute_header(
        |_, header: &mut PrepareHeader| {
            header.command = Command::Prepare;
            header.operation = Operation::StoreConsumerOffset;
            header.view = 1;
            header.replica = 1;
            header.group = partition.consensus().group();
            header.cluster = TEST_CLUSTER;
            header.op = 1;
            header.timestamp = 1;
            header.size = u32::try_from(size_of::<PrepareHeader>()).unwrap();
            header.checksum = header.identity_checksum();
        },
    );
    let checksum = prepare.header().checksum;
    partition
        .log
        .journal()
        .inner
        .append(prepare.clone().into_frozen())
        .await
        .unwrap();
    assert_eq!(partition.consensus().sequencer().current_sequence(), 0);
    partition.on_replicate(prepare).await;
    assert_eq!(partition.consensus().sequencer().current_sequence(), 1);
    assert_eq!(partition.consensus().last_prepare_checksum(), checksum);
}

#[compio::test]
async fn recovered_empty_wal_reopens_elections_but_new_missing_history_stays_fenced() {
    let intact = tempfile::tempdir().unwrap();
    let mut partition = Box::new(partition_at_view(0, 0));
    partition.set_partition_dir(intact.path().to_string_lossy().into_owned());
    partition.runtime_options.durability = iggy_common::Durability::Persisted;
    let group = partition.consensus().group();
    let journal =
        journal::PartitionPrepareJournal::open(&intact.path().join("prepares-0"), group, 0)
            .await
            .unwrap();
    assert!(!journal.recovered_frontier());
    drop(journal);
    partition.consensus.set_recovery_election_allowed(false);
    partition.open_persistence().await.unwrap();
    assert_eq!(partition.persistence.as_ref().unwrap().head(), 0);
    assert!(partition.consensus.recovery_election_allowed());
    assert!(!partition.requires_state_transfer());
    drop(partition);

    let missing = tempfile::tempdir().unwrap();
    let wal = missing.path().join("prepares-0");
    PartitionPersistence::fence_missing_history(&wal, 0)
        .await
        .unwrap();
    assert!(
        !wal.exists(),
        "the durable fence must precede replacement WAL creation"
    );
    let replacement = journal::PartitionPrepareJournal::open(&wal, group, 0)
        .await
        .unwrap();
    drop(replacement);
    for _ in 0..2 {
        let mut replacement = Box::new(partition_at_view(0, 0));
        replacement.set_partition_dir(missing.path().to_string_lossy().into_owned());
        replacement.runtime_options.durability = iggy_common::Durability::Persisted;
        replacement.consensus.set_recovery_election_allowed(false);
        replacement.open_persistence().await.unwrap();
        assert!(!replacement.consensus.recovery_election_allowed());
        assert!(replacement.requires_state_transfer());
        assert!(
            crate::state_transfer::materialization_is_missing(missing.path().to_str().unwrap(), 0)
                .await
                .unwrap()
        );
    }
}

#[compio::test]
async fn recovered_committed_tail_refuses_admission_until_receipts_and_capacity_are_restored() {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, replies) = recording_partition_at(0, 1);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.set_dedup_clients_max(3);
    partition.log.retire_front().unwrap();
    partition
        .install_empty_segment(&repair_config(), 0)
        .await
        .unwrap();
    let mut journal = journal::PartitionPrepareJournal::open(
        &directory.path().join("prepares-0"),
        partition.consensus().group(),
        0,
    )
    .await
    .unwrap();
    journal
        .append(checksummed_segment_prepare(1, 0, 0, b"abcdefgh").into_frozen())
        .await
        .unwrap();
    drop(journal);
    partition.open_persistence().await.unwrap();
    assert_eq!(partition.consensus.commit_min(), 0);
    assert_eq!(partition.consensus.commit_max(), 1);
    assert_eq!(partition.consensus.pipeline_len(), 0);
    let mut retry = checksumless_send_request(partition.namespace(), 1);
    retry.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
        .copy_from_slice(&build_segment_record(partition.namespace(), 0));
    partition.on_request(retry.clone(), None).await;
    {
        let replies = replies.borrow();
        assert_eq!(replies.len(), 1);
        let header = bytemuck::checked::from_bytes::<ReplyHeader>(
            &replies[0].1.as_slice()[..size_of::<ReplyHeader>()],
        );
        assert_eq!(header.status, IggyError::TransientNotAccepted.as_code());
    }
    assert_eq!(partition.consensus.pipeline_len(), 0);
    assert_eq!(
        partition.dedup.capacity(),
        3,
        "refused admission cannot seal the new local capacity"
    );
    partition.commit_journal(&repair_config()).await;
    assert!(
        partition.fatal.is_none(),
        "recovery commit failed: {:?}",
        partition.fatal
    );
    assert_eq!(
        partition.consensus.commit_min(),
        1,
        "pending checkpoint: {}, resident headers: {:?}",
        partition.persistence_checkpoint_pending(),
        partition
            .collect_committable_from_journal(COMMIT_WALK_OPS_MAX, &repair_config())
            .iter()
            .map(|header| header.op)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        partition.dedup.capacity(),
        consensus::PARTITION_DEDUP_CLIENTS_MAX
    );
    partition.on_request(retry, None).await;
    let replies = replies.borrow();
    assert_eq!(replies.len(), 2);
    let header = bytemuck::checked::from_bytes::<ReplyHeader>(
        &replies[1].1.as_slice()[..size_of::<ReplyHeader>()],
    );
    assert_eq!(header.status, 0);
    assert_eq!(header.op, 1, "recovery retry must replay the committed op");
    assert_eq!(
        partition.persistence.as_ref().unwrap().head(),
        1,
        "retry must not append another batch"
    );
}

#[compio::test]
async fn committed_pipeline_entries_allow_another_clients_write_before_receipt_fold() {
    let (mut partition, replies) = recording_partition_at(0, 3);
    let mut first = checksumless_send_request(partition.namespace(), 1);
    first.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
        .copy_from_slice(&build_segment_record(partition.namespace(), 0));
    partition.on_request(first.clone(), None).await;
    partition.consensus.advance_commit_max(1);
    assert_eq!(partition.consensus.commit_min(), 0);
    assert_eq!(
        partition.consensus.pipeline_len(),
        1,
        "initial admission failed: fatal={:?}, replies={:?}",
        partition.fatal,
        replies.borrow()
    );

    let retry = first.deep_copy();
    let second = first.transmute_header(|header, next: &mut RoutedRequestHeader| {
        *next = header;
        next.client = 2;
    });
    partition.on_request(second, None).await;
    assert!(replies.borrow().is_empty(), "both writes remain in flight");
    assert_eq!(partition.consensus.sequencer().current_sequence(), 2);
    assert_eq!(partition.consensus.pipeline_len(), 2);

    partition.consensus.advance_commit_max(2);
    partition.commit_journal(&repair_config()).await;
    assert_eq!(partition.consensus.commit_min(), 2);
    assert_eq!(partition.dedup.count(), 2);
    assert!(partition.fatal.is_none(), "{:?}", partition.fatal);
    assert_eq!(replies.borrow().len(), 2);
    partition.on_request(retry, None).await;
    let replies = replies.borrow();
    assert_eq!(replies.len(), 3);
    for (_, bytes) in replies.iter() {
        let header = bytemuck::checked::from_bytes::<ReplyHeader>(
            &bytes.as_slice()[..size_of::<ReplyHeader>()],
        );
        assert!(header.has_committed_result());
    }
    assert_eq!(
        replies[2].1.as_slice(),
        replies[0].1.as_slice(),
        "a live retry keeps the original proof"
    );
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

// #[compio::test]
// async fn given_ended_sessions_when_retiring_batch_should_use_one_partition_commit() {
//     const LIVE_CLIENT: u128 = 3;
//     let (mut partition, _) = recording_partition_at(0, 3);
//     for client_id in 1..=LIVE_CLIENT {
//         let mut send = checksumless_send_request(partition.namespace(), 1).transmute_header(
//             |header, next: &mut RoutedRequestHeader| {
//                 *next = header;
//                 next.client = client_id;
//             },
//         );
//         send.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
//             .copy_from_slice(&build_segment_record(partition.namespace(), 0));
//         partition.on_request(send, None).await;
//     }
//     partition.consensus.advance_commit_max(3);
//     partition.commit_journal(&repair_config()).await;
//     let identities = [
//         SessionIdentity {
//             client_id: 1,
//             session: 1,
//             metadata_watermark: 10,
//         },
//         SessionIdentity {
//             client_id: 2,
//             session: 1,
//             metadata_watermark: 11,
//         },
//     ];
//     partition
//         .on_request(
//             retire_sessions_request(partition.namespace(), &identities),
//             None,
//         )
//         .await;
//     assert_eq!(partition.consensus.sequencer().current_sequence(), 4);
//     partition.consensus.advance_commit_max(4);
//     partition.commit_journal(&repair_config()).await;
//     assert!(partition.fatal().is_none());
//     assert_eq!(
//         partition.dedup.count(),
//         1,
//         "one barrier must retire both ended sessions"
//     );
//     assert!(
//         partition.dedup.contains(LIVE_CLIENT),
//         "active sessions keep their retry receipts"
//     );
//     for identity in identities {
//         assert!(partition.session_retired(identity));
//     }
// }

// #[compio::test]
// async fn retire_session_fence_is_durable_before_metadata_can_finalize() {
//     for (durability, consumer_offset_durability) in [
//         (
//             iggy_common::Durability::Replicated,
//             iggy_common::Durability::Replicated,
//         ),
//         (
//             iggy_common::Durability::Persisted,
//             iggy_common::Durability::Replicated,
//         ),
//         (
//             iggy_common::Durability::Replicated,
//             iggy_common::Durability::Persisted,
//         ),
//     ] {
//         let directory = tempfile::tempdir().unwrap();
//         let (mut partition, _) = recording_partition_at(0, 1);
//         partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
//         partition.runtime_options.durability = durability;
//         partition.runtime_options.consumer_offset_durability = consumer_offset_durability;
//         partition.open_persistence().await.unwrap();
//         let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
//         // Hold the writer so that no fence can become durable.
//         persistence.sync();
//         assert!(persistence.start());
//         let identity = SessionIdentity {
//             client_id: 1,
//             session: 1,
//             metadata_watermark: 1,
//         };
//         partition
//             .on_request(
//                 retire_session_request(partition.namespace(), identity),
//                 None,
//             )
//             .await;
//         let header = partition.consensus.pipeline_head_header().unwrap();
//         assert_eq!(header.operation, Operation::RetireSession);
//         commit_recorded_loopback(&mut partition).await;
//         assert!(
//             partition
//                 .pending_persisted_acks
//                 .borrow()
//                 .contains_key(&header.op),
//             "{durability:?} messages, {consumer_offset_durability:?} offsets"
//         );
//         assert_eq!(partition.consensus.commit_min(), 0);
//         assert!(!partition.session_retired(identity));
//         Rc::clone(&persistence).run().await;
//         assert!(persistence.is_durable(&header));
//         partition.drive_persistence().await;
//         commit_recorded_loopback(&mut partition).await;
//         assert!(partition.session_retired(identity));
//         let namespace = partition.namespace();
//         drop(partition);
//         drop(persistence);
//         let (recovered, prepares) =
//             crate::persistence::PartitionPersistence::open_with_storage(
//                 &directory.path().join("prepares-0"),
//                 namespace.inner(),
//                 0,
//                 journal::durable_storage::DiskStorage,
//             )
//             .await
//             .unwrap();
//         assert!(recovered.is_durable(&header));
//         assert!(
//             prepares.iter().any(|prepare| *prepare.header() == header),
//             "recovery must retain the barrier metadata finalization relies on"
//         );
//     }
// }

// #[compio::test]
// async fn given_interleaved_sends_and_retirements_when_committing_should_preserve_live_receipts()
// {
//     const CLIENTS: u128 = 3;
//     const ENDING_FRONTIER: u64 = 10;
//     let (mut partition, replies) = recording_partition_at(0, 3);
//     for client_id in 1..=CLIENTS {
//         let mut send = checksumless_send_request(partition.namespace(), 1).transmute_header(
//             |header, next: &mut RoutedRequestHeader| {
//                 *next = header;
//                 next.client = client_id;
//             },
//         );
//         send.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
//             .copy_from_slice(&build_segment_record(partition.namespace(), 0));
//         partition.on_request(send, None).await;
//     }
//     partition
//         .consensus
//         .advance_commit_max(u64::try_from(CLIENTS).unwrap());
//     partition.commit_journal(&repair_config()).await;
//     for client_id in 1..CLIENTS {
//         let identity = SessionIdentity {
//             client_id,
//             session: 1,
//             metadata_watermark: ENDING_FRONTIER + u64::try_from(client_id).unwrap(),
//         };
//         partition
//             .on_request(
//                 retire_session_request(partition.namespace(), identity),
//                 None,
//             )
//             .await;
//         if client_id == 1 {
//             let mut send = checksumless_send_request(partition.namespace(), 1)
//                 .transmute_header(|header, next: &mut RoutedRequestHeader| {
//                     *next = header;
//                     next.client = CLIENTS;
//                     next.request = 2;
//                 });
//             send.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
//                 .copy_from_slice(&build_segment_record(partition.namespace(), 0));
//             partition.on_request(send, None).await;
//         }
//     }
//     let committed_op = u64::try_from(CLIENTS * 2).unwrap();
//     partition.consensus.advance_commit_max(committed_op);
//     partition.commit_journal(&repair_config()).await;
//     assert!(partition.fatal.is_none(), "{:?}", partition.fatal);
//     assert_eq!(partition.consensus.commit_min(), committed_op);
//     assert_eq!(partition.dedup.count(), 1);
//     assert!(partition.dedup.contains(CLIENTS));
//     assert_eq!(
//         replies.borrow().len(),
//         usize::try_from(CLIENTS + 1).unwrap()
//     );
//     for client_id in 1..CLIENTS {
//         assert!(partition.session_retired(SessionIdentity {
//             client_id,
//             session: 1,
//             metadata_watermark: ENDING_FRONTIER + u64::try_from(client_id).unwrap(),
//         }));
//     }
// }

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

//     #[compio::test]
//     async fn given_retirement_debt_when_session_requests_arrive_should_refuse_them_until_the_payment_commits()
//      {
//         const DEBT_OP: u64 = 40;
//         const FINALIZED_CLIENT: u128 = 1;
//         const NEW_CLIENT: u128 = 2;
//         let (mut partition, _replies) = recording_partition_at(0, 3);
//         let namespace = partition.namespace();
//         let send = |client_id: u128, metadata_watermark: u64| {
//             let mut send = checksumless_send_request(namespace, 1).transmute_header(
//                 |header, next: &mut RoutedRequestHeader| {
//                     *next = header;
//                     next.client = client_id;
//                     next.metadata_watermark = metadata_watermark;
//                 },
//             );
//             send.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
//                 .copy_from_slice(&build_segment_record(namespace, 0));
//             send
//         };
//         partition.on_request(send(FINALIZED_CLIENT, 0), None).await;
//         partition.consensus.advance_commit_max(1);
//         partition.commit_journal(&repair_config()).await;
//
//         assert_eq!(
//             partition.observe_retirement_debt(DEBT_OP),
//             RetirementDebtStatus::Unpaid(Some(vec![(FINALIZED_CLIENT, 1)]))
//         );
//         // The first request is the committed one's retry, a cached hit.
//         for request in [send(FINALIZED_CLIENT, 0), send(NEW_CLIENT, DEBT_OP)] {
//             let (sender, receiver) = consensus::oneshot_channel();
//             partition.on_request(request, Some(sender)).await;
//             assert_eq!(
//                 receiver.await.unwrap().header().status,
//                 IggyError::TransientNotAccepted.as_code()
//             );
//         }
//         assert_eq!(partition.consensus.sequencer().current_sequence(), 1);
//
//         partition
//             .on_request(
//                 retirement_payment_request(
//                     namespace,
//                     vec![SessionIdentity {
//                         client_id: FINALIZED_CLIENT,
//                         session: 1,
//                         metadata_watermark: DEBT_OP,
//                     }],
//                     DEBT_OP,
//                 ),
//                 None,
//             )
//             .await;
//         assert!(partition.owes_session_retirement(), "prepared is not paid");
//         partition.consensus.advance_commit_max(2);
//         partition.commit_journal(&repair_config()).await;
//         assert!(partition.fatal.is_none(), "{:?}", partition.fatal);
//         assert!(!partition.owes_session_retirement());
//         assert_eq!(
//             partition.observe_retirement_debt(DEBT_OP),
//             RetirementDebtStatus::Paid
//         );
//         assert!(!partition.dedup.contains(FINALIZED_CLIENT));
//         assert_eq!(partition.required_metadata_frontier, DEBT_OP);
//
//         let (sender, receiver) = consensus::oneshot_channel();
//         partition
//             .on_request(send(FINALIZED_CLIENT, 0), Some(sender))
//             .await;
//         assert_eq!(
//             receiver.await.unwrap().header().status,
//             IggyError::TransientNotAccepted.as_code(),
//             "the paid frontier keeps a finalized session's stale request out"
//         );
//         partition.on_request(send(NEW_CLIENT, DEBT_OP), None).await;
//         assert_eq!(partition.consensus.sequencer().current_sequence(), 3);
//
//         let wire = crate::state_transfer::ConsumerOffsetsWire::decode(
//             &crate::state_transfer::ConsumerOffsetsWire {
//                 retirement_paid_through: partition.retirement_paid_through,
//                 ..Default::default()
//             }
//             .encode(),
//         )
//         .unwrap();
//         assert_eq!(wire.retirement_paid_through, DEBT_OP);
//     }

// Pins the barrier count of a burst of ended sessions: the sweep sends the
// oldest `MAX_SESSIONS_PER_RETIREMENT` identities in one barrier, so one
// partition commits `ceil(K / MAX_SESSIONS_PER_RETIREMENT)` barriers for K
// sessions, and each commit releases the dedup slots of its whole batch.
//     #[compio::test]
//     async fn given_more_ended_sessions_than_one_batch_when_retiring_should_commit_one_barrier_per_batch()
//      {
//         const BATCH: usize = iggy_binary_protocol::requests::system::MAX_SESSIONS_PER_RETIREMENT;
//         const SESSIONS: usize = BATCH + 1;
//         const ENDING_FRONTIER: u64 = 10;
//         let (mut partition, _replies) = recording_partition_at(0, 3);
//         partition.set_dedup_clients_max(SESSIONS);
//         let mut op = 0;
//         let identities: Vec<SessionIdentity> = (1..=SESSIONS)
//             .map(|index| SessionIdentity {
//                 client_id: u128::try_from(index).unwrap(),
//                 session: 1,
//                 metadata_watermark: ENDING_FRONTIER + u64::try_from(index).unwrap(),
//             })
//             .collect();
//         for identity in &identities {
//             let mut send = checksumless_send_request(partition.namespace(), 1).transmute_header(
//                 |header, next: &mut RoutedRequestHeader| {
//                     *next = header;
//                     next.client = identity.client_id;
//                 },
//             );
//             send.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
//                 .copy_from_slice(&build_segment_record(partition.namespace(), 0));
//             partition.on_request(send, None).await;
//             op += 1;
//             partition.consensus.advance_commit_max(op);
//             partition.commit_journal(&repair_config()).await;
//         }
//         assert!(partition.fatal.is_none(), "{:?}", partition.fatal);
//         assert_eq!(partition.dedup.count(), SESSIONS);
//
//         let mut barriers = 0;
//         for batch in identities.chunks(BATCH) {
//             let held = partition.dedup.count();
//             partition
//                 .on_request(retire_sessions_request(partition.namespace(), batch), None)
//                 .await;
//             assert_eq!(
//                 partition.dedup.count(),
//                 held,
//                 "a prepared barrier must not release capacity"
//             );
//             assert!(
//                 batch
//                     .iter()
//                     .all(|identity| !partition.session_retired(*identity))
//             );
//             op += 1;
//             partition.consensus.advance_commit_max(op);
//             partition.commit_journal(&repair_config()).await;
//             barriers += 1;
//             assert_eq!(partition.consensus.commit_min(), op);
//             assert_eq!(partition.dedup.count(), held - batch.len());
//             assert!(
//                 batch
//                     .iter()
//                     .all(|identity| partition.session_retired(*identity))
//             );
//             assert!(
//                 identities[(barriers * BATCH).min(SESSIONS)..]
//                     .iter()
//                     .all(|identity| !partition.session_retired(*identity)),
//                 "a barrier must not retire a session outside its batch"
//             );
//         }
//         assert_eq!(barriers, SESSIONS.div_ceil(BATCH));
//         assert_eq!(partition.dedup.count(), 0);
//     }

// #[compio::test]
// async fn retirement_excludes_pending_target_writes_and_reports_during_unrelated_commits() {
//     const ENDING_FRONTIER: u64 = 10;
//     let (mut partition, replies) = recording_partition_at(0, 3);
//     let identity = SessionIdentity {
//         client_id: 1,
//         session: 1,
//         metadata_watermark: ENDING_FRONTIER,
//     };
//     let mut send = checksumless_send_request(partition.namespace(), 1);
//     send.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
//         .copy_from_slice(&build_segment_record(partition.namespace(), 0));
//     partition.on_request(send.clone(), None).await;
//     partition
//         .on_request(
//             retire_session_request(partition.namespace(), identity),
//             None,
//         )
//         .await;
//     assert_eq!(
//         partition.consensus.sequencer().current_sequence(),
//         1,
//         "retirement must wait for the target write"
//     );
//     partition.consensus.advance_commit_max(1);
//     partition.commit_journal(&repair_config()).await;
//     partition
//         .on_request(
//             retire_session_request(partition.namespace(), identity),
//             None,
//         )
//         .await;
//     let retry = send
//         .clone()
//         .transmute_header(|header, next: &mut RoutedRequestHeader| {
//             *next = header;
//             next.request = 2;
//         });
//     partition.on_request(retry, None).await;
//     assert_eq!(
//         partition.consensus.sequencer().current_sequence(),
//         2,
//         "target write must wait for its retirement"
//     );
//     let rejected = *bytemuck::checked::from_bytes::<ReplyHeader>(
//         &replies.borrow().last().unwrap().1.as_slice()[..size_of::<ReplyHeader>()],
//     );
//     assert_eq!(rejected.status, IggyError::TransientNotAccepted.as_code());
//     partition.consensus.advance_commit_max(2);
//     partition.commit_journal(&repair_config()).await;
//     assert_eq!(partition.dedup.count(), 0);
//     let unrelated = send.transmute_header(|header, next: &mut RoutedRequestHeader| {
//         *next = header;
//         next.client = 2;
//         next.metadata_watermark = ENDING_FRONTIER;
//     });
//     partition.on_request(unrelated, None).await;
//     partition.consensus.advance_commit_max(3);
//     assert_eq!(partition.consensus.commit_min(), 2);
//     assert!(
//         partition.session_retired(identity),
//         "unrelated commits must not block the completed retirement"
//     );
//     partition.commit_journal(&repair_config()).await;
//     assert_eq!(partition.dedup.count(), 1);
//     assert!(!partition.dedup.contains(identity.client_id));
// }

fn send_request_for_client(
    namespace: IggyNamespace,
    client_id: u128,
) -> Message<RoutedRequestHeader> {
    let mut send = checksumless_send_request(namespace, 1).transmute_header(
        |header, next: &mut RoutedRequestHeader| {
            *next = header;
            next.client = client_id;
        },
    );
    send.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
        .copy_from_slice(&build_segment_record(namespace, 0));
    send
}

#[compio::test]
async fn given_in_flight_send_when_storing_committed_offset_should_admit_independent_requests() {
    let (mut partition, _) = recording_partition_at(0, 3);
    partition.stats.increment_messages_count(1);
    let namespace = partition.namespace();
    partition
        .on_request(send_request_for_client(namespace, 1), None)
        .await;
    partition
        .on_request(
            store_offset_request(42, 1, ConsumerKind::Consumer, 7, 0, AckLevel::Quorum),
            None,
        )
        .await;
    partition
        .on_request(send_request_for_client(namespace, 2), None)
        .await;
    assert_eq!(partition.consensus.pipeline_len(), 3);
    assert_eq!(partition.consensus.request_queue_len(), 0);
}

#[compio::test]
async fn given_in_flight_send_when_storing_its_offset_should_preserve_the_dependency() {
    let (mut partition, replies) = recording_partition_at(0, 3);
    let namespace = partition.namespace();
    partition
        .on_request(send_request_for_client(namespace, 1), None)
        .await;
    partition
        .on_request(
            store_offset_request(42, 1, ConsumerKind::Consumer, 7, 0, AckLevel::Quorum),
            None,
        )
        .await;
    assert_eq!(partition.consensus.pipeline_len(), 1);
    assert_eq!(partition.consensus.request_queue_len(), 1);
    assert!(
        replies.borrow().is_empty(),
        "the store must await its earlier send, not be rejected"
    );
}

#[compio::test]
async fn given_unstaged_auto_commit_when_another_key_completes_should_admit_only_independent_keys()
{
    let (mut partition, _) = recording_partition_at(0, 3);
    install_test_owner(&mut partition, 7, 42).await;
    install_test_owner(&mut partition, 8, 42).await;
    partition.stats.increment_messages_count(1);
    let namespace = partition.namespace();
    let request = send_request_for_client(namespace, 1).transmute_header(
        |mut header, next: &mut RoutedRequestHeader| {
            header.metadata_watermark = partition.required_metadata_frontier;
            *next = header;
        },
    );
    partition.on_request(request, None).await;
    let first = partition
        .complete_poll(poll_read_result(
            &partition,
            PollingConsumer::ConsumerGroup(7, 1),
            true,
            Some(0),
        ))
        .unwrap();
    assert!(first.replication.is_some());
    let independent = partition
        .complete_poll(poll_read_result(
            &partition,
            PollingConsumer::ConsumerGroup(8, 1),
            true,
            Some(0),
        ))
        .unwrap();
    assert!(independent.replication.is_some());
    let dependent = partition
        .complete_poll(poll_read_result(
            &partition,
            PollingConsumer::ConsumerGroup(7, 1),
            true,
            Some(0),
        ))
        .unwrap();
    assert!(dependent.replication.is_none());
    assert_eq!(partition.consensus.pipeline_len(), 3);
    assert_eq!(partition.consensus.request_queue_len(), 1);
}

// #[compio::test]
// async fn queued_retirement_excludes_a_later_target_write() {
//     let (mut partition, replies) =
//         recording_partition_with_pipeline(0, 3, LocalPipeline::with_capacities(1, 4));
//     let mut unrelated = checksumless_send_request(partition.namespace(), 1).transmute_header(
//         |header, next: &mut RoutedRequestHeader| {
//             *next = header;
//             next.client = 2;
//         },
//     );
//     unrelated.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
//         .copy_from_slice(&build_segment_record(partition.namespace(), 0));
//     partition.on_request(unrelated, None).await;
//     let identity = SessionIdentity {
//         client_id: 1,
//         session: 1,
//         metadata_watermark: 10,
//     };
//     let second_identity = SessionIdentity {
//         client_id: 3,
//         ..identity
//     };
//     partition
//         .on_request(
//             retire_sessions_request(partition.namespace(), &[identity, second_identity]),
//             None,
//         )
//         .await;
//     assert_eq!(
//         partition
//             .consensus
//             .with_pipeline(LocalPipeline::request_queue_len),
//         1
//     );
//     let mut target = checksumless_send_request(partition.namespace(), 1).transmute_header(
//         |old, header: &mut RoutedRequestHeader| {
//             *header = old;
//             header.client = second_identity.client_id;
//         },
//     );
//     target.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
//         .copy_from_slice(&build_segment_record(partition.namespace(), 0));
//     partition.on_request(target, None).await;
//     assert_eq!(
//         partition
//             .consensus
//             .with_pipeline(LocalPipeline::request_queue_len),
//         1,
//         "target cannot queue behind its retirement"
//     );
//     let rejected = *bytemuck::checked::from_bytes::<ReplyHeader>(
//         &replies.borrow()[0].1.as_slice()[..size_of::<ReplyHeader>()],
//     );
//     assert_eq!(rejected.status, IggyError::TransientNotAccepted.as_code());
// }

//     #[compio::test]
//     async fn given_foreign_session_retirement_when_replicated_or_repaired_should_preserve_current_history()
//      {
//         const INCARNATION: u64 = 7;
//         const COMMITTED_ELSEWHERE: u64 = 14;
//         const NEXT_OP: u64 = 1;
//         let (mut primary, _) = recording_partition_at(0, 3);
//         primary.set_created_revision(INCARNATION);
//         let identity = SessionIdentity {
//             client_id: 1,
//             session: 1,
//             metadata_watermark: 1,
//         };
//         primary
//             .on_request(retire_session_request(primary.namespace(), identity), None)
//             .await;
//         let bytes = primary.log.journal().inner.repair_entry(NEXT_OP).unwrap();
//         let mut valid = Message::<PrepareHeader>::new(bytes.len());
//         valid.as_mut_slice().copy_from_slice(bytes.as_slice());
//         assert_eq!(valid.header().partition_incarnation, INCARNATION);
//
//         for repaired in [false, true] {
//             let (mut backup, _) = recording_partition_at(1, 3);
//             backup.set_created_revision(INCARNATION);
//             if repaired {
//                 backup.repair = Some(armed_session(NEXT_OP, 0));
//             }
//             for foreign in [0, INCARNATION - 1, INCARNATION + 1] {
//                 for commit in [0, COMMITTED_ELSEWHERE] {
//                     let message = consensus::seal_prepare_checksum(valid.clone().transmute_header(
//                         |mut original, header: &mut PrepareHeader| {
//                             original.partition_incarnation = foreign;
//                             original.commit = commit;
//                             original.op = commit + NEXT_OP;
//                             *header = original;
//                         },
//                     ));
//                     if repaired {
//                         backup.apply_repaired_prepare(message).await;
//                     } else {
//                         backup.on_replicate(message).await;
//                     }
//                     assert_eq!(backup.consensus().commit_max(), 0);
//                     assert_eq!(backup.consensus().sequencer().current_sequence(), 0);
//                     assert!(
//                         backup
//                             .log
//                             .journal()
//                             .inner
//                             .header_by_op(commit + NEXT_OP)
//                             .is_none()
//                     );
//                     assert!(!backup.session_retired(identity));
//                 }
//             }
//             if repaired {
//                 backup.apply_repaired_prepare(valid.clone()).await;
//             } else {
//                 backup.on_replicate(valid.clone()).await;
//             }
//             assert!(
//                 backup.log.journal().inner.header_by_op(NEXT_OP).is_some(),
//                 "a retirement of the hosted incarnation must still enter the journal"
//             );
//         }
//     }

//     #[compio::test]
//     async fn given_deleted_history_when_retirement_is_replicated_or_repaired_should_commit_it() {
//         const FENCE_OP: u64 = 1;
//         const RETIREMENT_OP: u64 = 2;
//         let config = repair_config();
//         let directory = tempfile::tempdir().unwrap();
//         let (mut primary, _) = recording_partition_at(0, 3);
//         primary.set_partition_dir(directory.path().to_string_lossy().into_owned());
//         let transition = TransitionPartitionHistoryRequest {
//             incarnation: primary.created_revision(),
//             metadata_op: 10,
//         };
//         primary
//             .on_request(
//                 partition_control_request(
//                     &primary,
//                     Operation::TransitionPartitionHistory,
//                     &transition.to_bytes(),
//                     transition.metadata_op,
//                 ),
//                 None,
//             )
//             .await;
//         primary.consensus.advance_commit_max(FENCE_OP);
//         primary.commit_journal(&config).await;
//         assert!(primary.history_deleted());
//         let bytes = primary.log.journal().inner.repair_entry(FENCE_OP).unwrap();
//         let fence =
//             Message::<PrepareHeader>::try_from(Owned::copy_from_slice(bytes.as_slice())).unwrap();
//         let identity = SessionIdentity {
//             client_id: 1,
//             session: 1,
//             metadata_watermark: 20,
//         };
//         assert!(primary.session_retired(identity));
//         primary
//             .on_request(retire_session_request(primary.namespace(), identity), None)
//             .await;
//         let bytes = primary
//             .log
//             .journal()
//             .inner
//             .repair_entry(RETIREMENT_OP)
//             .expect("retirement must remain journaled after the terminal fence");
//         let retirement =
//             Message::<PrepareHeader>::try_from(Owned::copy_from_slice(bytes.as_slice())).unwrap();
//
//         for repaired in [false, true] {
//             let directory = tempfile::tempdir().unwrap();
//             let (mut backup, _) = recording_partition_at(1, 3);
//             backup.set_partition_dir(directory.path().to_string_lossy().into_owned());
//             backup.on_replicate(fence.clone()).await;
//             backup.consensus.advance_commit_max(FENCE_OP);
//             backup.commit_journal(&config).await;
//             assert!(backup.history_deleted());
//             if repaired {
//                 backup.repair = Some(armed_session(RETIREMENT_OP, 0));
//                 backup.apply_repaired_prepare(retirement.clone()).await;
//             } else {
//                 backup.on_replicate(retirement.clone()).await;
//             }
//             assert!(
//                 backup.log.journal().inner.holds_op(RETIREMENT_OP),
//                 "retirement after the delete fence must reach the backup, repaired={repaired}"
//             );
//             backup.consensus.advance_commit_max(RETIREMENT_OP);
//             backup.commit_journal(&config).await;
//             assert_eq!(backup.consensus.commit_min(), RETIREMENT_OP);
//             assert!(backup.session_retired(identity));
//             assert!(backup.fatal().is_none());
//         }
//     }

// #[compio::test]
// async fn given_unpublished_delete_fence_when_checking_retirement_should_wait_for_commit() {
//     let mut partition = test_partition();
//     let identity = SessionIdentity {
//         client_id: 1,
//         session: 1,
//         metadata_watermark: 20,
//     };
//     partition
//         .apply_history_transition(
//             TransitionPartitionHistoryRequest {
//                 incarnation: partition.created_revision(),
//                 metadata_op: 10,
//             },
//             1,
//         )
//         .unwrap();
//     assert!(!partition.session_retired(identity));
//     partition.consensus.advance_commit_max(1);
//     partition.consensus.advance_commit_min(1);
//     assert!(partition.session_retired(identity));
// }

//     #[compio::test]
//     async fn given_queued_session_retirement_when_promoted_at_a_live_incarnation_should_prepare_it()
//     {
//         const INCARNATION: u64 = 7;
//         let (mut partition, replies) =
//             recording_partition_with_pipeline(0, 1, LocalPipeline::with_capacities(1, 1));
//         partition.set_created_revision(INCARNATION);
//         partition.runtime_options.durability = iggy_common::Durability::Replicated;
//         partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Replicated;
//         // The send holds the only prepare slot, so the retirement has to queue.
//         let mut send = checksumless_send_request(partition.namespace(), 1).transmute_header(
//             |header, next: &mut RoutedRequestHeader| {
//                 *next = header;
//                 next.client = 2;
//                 next.partition_incarnation = INCARNATION;
//             },
//         );
//         send.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
//             .copy_from_slice(&build_segment_record(partition.namespace(), 0));
//         partition.on_request(send, None).await;
//         let identity = SessionIdentity {
//             client_id: 1,
//             session: 1,
//             metadata_watermark: 1,
//         };
//         partition
//             .on_request(
//                 retire_session_request(partition.namespace(), identity),
//                 None,
//             )
//             .await;
//         assert_eq!(partition.consensus().request_queue_len(), 1);
//
//         commit_recorded_loopback(&mut partition).await;
//
//         assert_eq!(partition.consensus().request_queue_len(), 0);
//         let promoted = partition.consensus.pipeline_head_header().unwrap();
//         assert_eq!(promoted.operation, Operation::RetireSession);
//         assert_eq!(promoted.partition_incarnation, INCARNATION);
//         for (_, reply) in replies.borrow().iter() {
//             let header = bytemuck::checked::from_bytes::<ReplyHeader>(
//                 &reply.as_slice()[..size_of::<ReplyHeader>()],
//             );
//             assert_ne!(header.status, IggyError::HistoryUnavailable.as_code());
//         }
//     }

// The history is live, so the incarnation stamp is the only thing that
// tells admission this send was aimed at an older partition with this id.
//     #[compio::test]
//     async fn given_live_history_when_send_names_an_older_incarnation_should_refuse_it() {
//         const INCARNATION: u64 = 7;
//         const OLDER_INCARNATION: u64 = 5;
//         let (mut partition, _) = recording_partition_at(0, 3);
//         partition.set_created_revision(INCARNATION);
//         partition.runtime_options.durability = iggy_common::Durability::Replicated;
//         let namespace = partition.namespace();
//         let send_at = |incarnation: u64| {
//             let mut send = checksumless_send_request(namespace, 1).transmute_header(
//                 |header, next: &mut RoutedRequestHeader| {
//                     *next = header;
//                     next.partition_incarnation = incarnation;
//                 },
//             );
//             send.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
//                 .copy_from_slice(&build_segment_record(namespace, 0));
//             send
//         };
//         assert!(!partition.history_deleted());
//
//         let (sender, refusal) = consensus::oneshot_channel();
//         partition
//             .on_request(send_at(OLDER_INCARNATION), Some(sender))
//             .await;
//         // Checked before the await: a wrongly admitted send never gets a reply.
//         assert_eq!(partition.consensus.sequencer().current_sequence(), 0);
//         assert_eq!(
//             refusal.await.unwrap().header().status,
//             IggyError::HistoryUnavailable.as_code()
//         );
//
//         partition.on_request(send_at(INCARNATION), None).await;
//         assert_eq!(partition.consensus.sequencer().current_sequence(), 1);
//     }

/// A sender drops its context on `HistoryUnavailable`. A caller context from
/// a node ahead of the dispatching one can name an incarnation this replica
/// has not materialized yet, so only applied metadata can prove it gone.
#[compio::test]
async fn given_send_ahead_of_dispatch_metadata_when_admitting_should_report_not_accepted() {
    const INCARNATION: u64 = 7;
    const NEWER_INCARNATION: u64 = 9;
    const DISPATCH_WATERMARK: u64 = 10;
    let (mut partition, _) = recording_partition_at(0, 3);
    partition.set_created_revision(INCARNATION);
    partition.runtime_options.durability = iggy_common::Durability::Replicated;
    let namespace = partition.namespace();
    let send_requiring = |minimum_metadata_op: u64| {
        let mut send = checksumless_send_request(namespace, 1).transmute_header(
            |header, next: &mut RoutedRequestHeader| {
                *next = header;
                next.partition_incarnation = NEWER_INCARNATION;
                next.minimum_metadata_op = minimum_metadata_op;
                next.metadata_watermark = DISPATCH_WATERMARK;
            },
        );
        send.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
            .copy_from_slice(&build_segment_record(namespace, 0));
        send
    };

    for (minimum_metadata_op, expected) in [
        (DISPATCH_WATERMARK + 1, IggyError::TransientNotAccepted),
        (DISPATCH_WATERMARK, IggyError::HistoryUnavailable),
    ] {
        let (sender, refusal) = consensus::oneshot_channel();
        partition
            .on_request(send_requiring(minimum_metadata_op), Some(sender))
            .await;
        // Checked before the await: a wrongly admitted send never gets a reply.
        assert_eq!(partition.consensus.sequencer().current_sequence(), 0);
        assert_eq!(
            IggyError::from_code(refusal.await.unwrap().header().status),
            expected,
            "a send requiring metadata op {minimum_metadata_op} dispatched at {DISPATCH_WATERMARK}"
        );
    }
}

// #[compio::test]
// async fn uncertified_log_view_requires_transfer_before_voting() {
//     let directory = tempfile::tempdir().unwrap();
//     let mut partition = partition_at_view(2, 2);
//     partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
//     partition.runtime_options.durability = iggy_common::Durability::Persisted;
//     let mut journal = journal::PartitionPrepareJournal::open(
//         &directory.path().join("prepares-0"),
//         partition.consensus().group(),
//         0,
//     )
//     .await
//     .unwrap();
//     journal.reset(7, Some(1234)).await.unwrap();
//     journal.certify_log_view(1, 7, 1234).await.unwrap();
//     drop(journal);
//     publish_empty_retry_checkpoint(&partition, 7).await;
//     partition.open_persistence().await.unwrap();
//     assert!(partition.requires_state_transfer());
//     assert!(partition.consensus().is_transferring());
//     assert_eq!(partition.consensus().commit_min(), 0);
//     assert_eq!(partition.consensus().sequencer().current_sequence(), 0);
// }

/// The primary commits op 2 on this backup's ack and applies it later, so
/// the offer pins op 1. The backup entered its transfer before it heard
/// that op 2 committed, and installing the offer must not erase op 2.
#[compio::test]
async fn given_an_acked_op_above_the_known_commit_when_installing_a_lower_offer_should_refuse() {
    let directory = tempfile::tempdir().unwrap();
    let (mut backup, _) = recording_partition_at(1, 3);
    let sent = backup.consensus().message_bus().sent_to_replicas.clone();
    backup.set_partition_dir(directory.path().to_string_lossy().into_owned());
    set_offset_dirs_under(&mut backup, directory.path());
    backup.open_persistence().await.unwrap();

    let first = checksummed_segment_prepare(1, 0, 0, b"first");
    let second = checksummed_segment_prepare(2, first.header().checksum, 1, b"second")
        .transmute_header(|original, header: &mut PrepareHeader| {
            *header = original;
            header.commit = 1;
            header.checksum = header.identity_checksum();
        });
    let third = checksummed_segment_prepare(3, second.header().checksum, 2, b"third")
        .transmute_header(|original, header: &mut PrepareHeader| {
            *header = original;
            header.commit = 2;
            header.checksum = header.identity_checksum();
        });
    backup.on_replicate(first.clone()).await;
    backup.on_replicate(second).await;
    backup
        .persistence
        .as_ref()
        .unwrap()
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();
    backup.drive_persistence().await;
    let acked: Vec<_> = sent
        .borrow()
        .iter()
        .filter_map(|(_, frame)| {
            bytemuck::checked::try_from_bytes::<PrepareOkHeader>(frame.as_slice())
                .ok()
                .filter(|header| header.command == Command::PrepareOk)
                .map(|header| header.op)
        })
        .collect();
    assert_eq!(acked, vec![1, 2], "the primary counts this ack toward op 2");
    assert_eq!(backup.consensus().commit_max(), 1);

    backup.consensus().begin_state_transfer_await();
    backup.on_replicate(third).await;
    assert!(!backup.log.journal().inner.holds_op(3));
    assert_eq!(
        backup.consensus().commit_max(),
        1,
        "a transferring backup drops the prepare that names commit 2"
    );

    let body = first.as_slice()[size_of::<PrepareHeader>()..].to_vec();
    let artifact = consensus::StateArtifact::for_bytes(
        consensus::state_manifest::artifact_kind::SEGMENT_LOG,
        0,
        &body,
    );
    let staged = backup
        .spill_transfer_segment(&artifact, body, None)
        .await
        .unwrap();
    let offsets = crate::state_transfer::ConsumerOffsetsWire {
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        required_metadata_frontier: 0,
        prepare_checksum: Some(first.header().checksum),
        checkpoint_prepare: first.as_slice().to_vec(),
        next_offset: 1,
        emptied_at: 0,
        consumers: Vec::new(),
        groups: Vec::new(),
        external_groups: Vec::new(),
        dedup: Vec::new(),
        owners: Vec::new(),
        history_transition: None,
        owner_retirement: None,
        retirement_paid_through: 0,
    };
    let installed = backup
        .install_state_transfer(&repair_config(), 1, vec![staged], &offsets.encode())
        .await;
    let wal_head = backup.persistence.as_ref().unwrap().head();
    assert!(
        matches!(
            installed,
            Err(
                crate::state_transfer::PartitionInstallError::OfferErasesHeldOps {
                    commit_op: 1,
                    ..
                }
            )
        ),
        "the install erased op 2, which this backup acknowledged: {installed:?}, \
             holds_op(2) = {}, WAL head = {wal_head}",
        backup.log.journal().inner.holds_op(2),
    );
    assert!(backup.log.journal().inner.holds_op(2));
    assert_eq!(wal_head, 2);
}

/// The CREATE claim carries the recorded frontier, writes inside an open
/// backoff window, and counts a failure only outside that window.
#[compio::test]
async fn given_a_create_claim_when_the_store_fails_should_write_through_backoff() {
    let store = Rc::new(RecordingSuperblock::default());
    let mut partition = solo_recording_partition();
    let recovered = recorded_state(40, 40);
    partition.set_superblock(store.clone(), Some(&recovered));
    partition.set_offset_reservation_lease(test_lease(16));

    assert!(partition.reserve_offsets_through(50).await);
    assert_eq!(store.attempts.get(), 1);
    assert_eq!(
        last_recorded_frontier(&store),
        40,
        "the claim carries the recorded frontier forward"
    );
    assert_eq!(
        last_recorded_reservation(&store),
        67,
        "the claim runs one past the offset plus the lease"
    );

    store.fail_writes.set(true);
    let window_end = partition.consensus().clock_realtime_micros() + 1_000_000;
    partition.superblock_retry_after_micros.set(window_end);
    assert!(!partition.reserve_offsets_through(100).await);
    assert_eq!(
        store.attempts.get(),
        2,
        "the CREATE claim writes inside an open backoff window"
    );
    assert_eq!(
        partition.superblock_write_failures(),
        0,
        "a failure inside the window is not counted"
    );
    assert_eq!(
        partition.superblock_retry_after_micros.get(),
        window_end,
        "a failure inside the window does not move it"
    );

    partition.superblock_retry_after_micros.set(0);
    assert!(!partition.reserve_offsets_through(100).await);
    assert_eq!(store.attempts.get(), 3);
    assert_eq!(
        partition.superblock_write_failures(),
        1,
        "a failure outside the window is counted once"
    );
    assert_ne!(
        partition.superblock_retry_after_micros.get(),
        0,
        "the counted failure arms the next window"
    );
}

/// Under a dispatcher the forced flush only requests owner work. The inline
/// path in `commit_messages_inner` stays with undispatched owners.
#[compio::test]
async fn given_a_dispatcher_when_flushing_should_leave_the_flush_to_the_owner() {
    let (mut partition, _replies) = recording_partition_at(0, 3);
    let mut config = repair_config();
    config.messages_required_to_save = u32::MAX;
    let prepare = checksummed_segment_prepare(1, 0, 0, b"committed");
    partition.consensus.with_pipeline_mut(|pipeline| {
        pipeline.push(PipelineEntry::new(*prepare.header()));
    });
    partition
        .append_repaired_send_messages(prepare)
        .await
        .unwrap();
    partition.consensus.sequencer().set_sequence(1);
    partition.consensus.advance_commit_max(1);
    partition.commit_journal(&config).await;
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );

    assert!(!partition.flush_committed_messages(&config).await.unwrap());
    assert_eq!(partition.maintenance.flush_through, Some(1));
    assert!(
        partition.materialization.is_none(),
        "the dispatched flush must not begin materialization inline"
    );
}

#[compio::test]
async fn given_rebuilt_replicated_prepare_above_a_wal_hole_when_acknowledging_should_wait() {
    let directory = tempfile::tempdir().unwrap();
    let (mut backup, _) = recording_partition_at(1, 3);
    backup.set_partition_dir(directory.path().to_string_lossy().into_owned());
    backup.runtime_options.durability = iggy_common::Durability::Replicated;
    backup.open_persistence().await.unwrap();
    let sent = backup.consensus().message_bus().sent_to_replicas.clone();
    let first = checksummed_segment_prepare(1, 0, 0, b"first");
    let prepare = checksummed_segment_prepare(2, first.header().checksum, 1, b"adopted");
    let header = *prepare.header();
    backup.consensus().sequencer().set_sequence(header.op);
    backup
        .log
        .journal()
        .inner
        .append(prepare.into_frozen())
        .await
        .unwrap();

    assert!(!backup.register_rebuilt_ack(&header));
    backup.acknowledge_prepare(2).await;
    assert!(
        sent.borrow().is_empty(),
        "the WAL has not accepted the missing predecessor"
    );
    assert!(backup.pending_persisted_acks.borrow().contains_key(&2));
    assert_eq!(backup.persistence.as_ref().unwrap().head(), 0);

    backup
        .log
        .journal()
        .inner
        .append(first.into_frozen())
        .await
        .unwrap();
    backup.persist_repaired_prefix();
    backup
        .persistence
        .as_ref()
        .unwrap()
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();
    backup.drive_persistence().await;
    let sent = sent.borrow();
    assert_eq!(sent.len(), 1);
    let ack = bytemuck::checked::from_bytes::<PrepareOkHeader>(sent[0].1.as_slice());
    assert_eq!(ack.command, Command::PrepareOk);
    assert_eq!(ack.op, 2);
    assert!(backup.pending_persisted_acks.borrow().is_empty());
}

#[compio::test]
async fn given_unflushed_wal_when_reporting_replication_progress_should_honor_topic_durability() {
    for durability in [
        iggy_common::Durability::Replicated,
        iggy_common::Durability::Persisted,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let (mut backup, _) = recording_partition_at(1, 3);
        backup.set_partition_dir(directory.path().to_string_lossy().into_owned());
        backup.runtime_options.durability = durability;
        backup.open_persistence().await.unwrap();
        let prepare = checksummed_segment_prepare(1, 0, 0, b"written");
        let header = *prepare.header();
        backup.consensus().sequencer().set_sequence(header.op);
        backup
            .log
            .journal()
            .inner
            .append(prepare.clone().into_frozen())
            .await
            .unwrap();
        let persistence = Rc::clone(backup.persistence.as_ref().unwrap());
        persistence.append(prepare.into_frozen(), false).unwrap();
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        assert!(persistence.is_written(&header));
        assert!(!persistence.is_durable(&header));

        let expected = Some(consensus::BackupPrefix {
            held_op: header.op,
            received_op: u64::from(!durability.is_persisted()),
        });
        assert_eq!(
            backup.replication_progress(),
            expected,
            "{durability:?}: the held prefix finds gaps, and the received prefix follows \
                 the prepare acknowledgment contract"
        );
        backup.maintenance.flush_through = Some(header.op);
        assert_eq!(
            backup.replication_progress(),
            expected,
            "{durability:?}: ordinary materialization does not replace the held history"
        );
    }
}

#[compio::test]
async fn given_unacked_repair_above_a_wal_hole_when_installing_should_allow_recovery() {
    let directory = tempfile::tempdir().unwrap();
    let (mut backup, _) = recording_partition_at(1, 3);
    backup.set_partition_dir(directory.path().to_string_lossy().into_owned());
    set_offset_dirs_under(&mut backup, directory.path());
    backup.runtime_options.durability = iggy_common::Durability::Persisted;
    backup.open_persistence().await.unwrap();
    let first = checksummed_segment_prepare(1, 0, 0, b"first");
    backup.on_replicate(first.clone()).await;
    backup
        .persistence
        .as_ref()
        .unwrap()
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();
    backup.drive_persistence().await;
    // Op 3 is committed but unacked here: the WAL hole at op 2 holds back
    // its ack, so it must not block the install.
    backup.consensus().advance_commit_max(3);
    let third = checksummed_segment_prepare(3, 0, 2, b"unacked");
    let header = *third.header();
    backup
        .log
        .journal()
        .inner
        .append(third.into_frozen())
        .await
        .unwrap();
    assert!(!backup.register_rebuilt_ack(&header));
    assert_eq!(backup.persistence.as_ref().unwrap().head(), 1);

    let body = first.as_slice()[size_of::<PrepareHeader>()..].to_vec();
    let artifact = consensus::StateArtifact::for_bytes(
        consensus::state_manifest::artifact_kind::SEGMENT_LOG,
        0,
        &body,
    );
    let staged = backup
        .spill_transfer_segment(&artifact, body, None)
        .await
        .unwrap();
    let offsets = crate::state_transfer::ConsumerOffsetsWire {
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        prepare_checksum: Some(first.header().checksum),
        checkpoint_prepare: first.as_slice().to_vec(),
        next_offset: 1,
        ..Default::default()
    };
    backup.consensus().begin_state_transfer_await();
    let installed = backup
        .install_state_transfer(&repair_config(), 1, vec![staged], &offsets.encode())
        .await
        .unwrap();
    assert_eq!(installed.applied_commit_op, 1);
    assert!(!backup.log.journal().inner.holds_op(3));
    assert_eq!(backup.persistence.as_ref().unwrap().head(), 1);
}

// #[compio::test]
// async fn promotion_waits_for_the_matching_local_prepare_to_be_durable() {
//     let directory = tempfile::tempdir().unwrap();
//     let mut partition = partition_at_view(1, 1);
//     partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
//     partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Persisted;
//     partition.open_persistence().await.unwrap();
//     let prepare = Message::<PrepareHeader>::new(size_of::<PrepareHeader>()).transmute_header(
//         |_, header: &mut PrepareHeader| {
//             header.command = Command::Prepare;
//             header.operation = Operation::StoreConsumerOffset;
//             header.view = 1;
//             header.replica = 1;
//             header.group = partition.consensus().group();
//             header.cluster = TEST_CLUSTER;
//             header.op = 1;
//             header.timestamp = 1;
//             header.size = u32::try_from(size_of::<PrepareHeader>()).unwrap();
//             header.checksum = header.identity_checksum();
//         },
//     );
//     let header = *prepare.header();
//     let persistence = partition.persistence.as_ref().unwrap();
//     persistence.append(prepare.into_frozen(), true).unwrap();
//     assert!(!partition.register_rebuilt_ack(&header));
//     assert!(partition.pending_persisted_acks.borrow().contains_key(&1));
//     partition.start_persistence();
//     persistence
//         .drain_with_timeout(compio::runtime::time::sleep)
//         .await
//         .unwrap();
//     assert!(partition.register_rebuilt_ack(&header));
// }

#[compio::test]
async fn given_committed_prepare_when_retransmitted_should_ack_only_matching_history() {
    let mut prepares = Vec::new();
    for client_id in [1, 2] {
        let (mut primary, _) = recording_partition_at(0, 3);
        primary
            .on_request(
                retire_session_request(
                    primary.namespace(),
                    SessionIdentity {
                        client_id,
                        session: 1,
                        metadata_watermark: 1,
                    },
                ),
                None,
            )
            .await;
        let retained = primary.log.journal().inner.repair_entry(1).unwrap();
        prepares.push(
            Message::<PrepareHeader>::try_from(Owned::copy_from_slice(retained.as_slice()))
                .unwrap(),
        );
    }
    for durability in [
        iggy_common::Durability::Replicated,
        iggy_common::Durability::Persisted,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let (mut partition, _) = recording_partition_at(1, 3);
        let sent = partition.consensus().message_bus().sent_to_replicas.clone();
        partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
        partition.runtime_options.durability = durability;
        partition.open_persistence().await.unwrap();
        let prepare = &prepares[0];
        partition.on_replicate(prepare.clone()).await;
        partition
            .persistence
            .as_ref()
            .unwrap()
            .drain_with_timeout(compio::runtime::time::sleep)
            .await
            .unwrap();
        partition.drive_persistence().await;
        partition.consensus.advance_commit_max(prepare.header().op);
        partition.commit_journal(&repair_config()).await;
        assert!(partition.fatal.is_none(), "{:?}", partition.fatal);
        assert_eq!(partition.consensus.commit_min(), prepare.header().op);
        let offset = partition.offset.load(Ordering::Acquire);
        sent.borrow_mut().clear();

        partition.on_replicate(prepare.clone()).await;
        let acknowledgements: Vec<_> = sent
            .borrow()
            .iter()
            .filter_map(|(replica, frame)| {
                bytemuck::checked::try_from_bytes::<PrepareOkHeader>(frame.as_slice())
                    .ok()
                    .filter(|header| header.command == Command::PrepareOk)
                    .map(|header| (*replica, header.op, header.prepare_checksum))
            })
            .collect();
        assert_eq!(
            acknowledgements,
            vec![(0, prepare.header().op, prepare.header().checksum)],
            "a committed retransmit must acknowledge the retained prepare under {durability:?}"
        );
        assert_eq!(partition.offset.load(Ordering::Acquire), offset);
        assert_eq!(partition.consensus.commit_min(), prepare.header().op);
        assert_eq!(partition.consensus.applied_log_contradictions(), 0);
        sent.borrow_mut().clear();

        partition.on_replicate(prepares[1].clone()).await;
        assert!(
            sent.borrow().is_empty(),
            "a conflicting committed prepare must never be acknowledged"
        );
        assert_eq!(partition.offset.load(Ordering::Acquire), offset);
        assert_eq!(
            partition.consensus.applied_log_contradictions(),
            1,
            "a different prepare at an applied op is a contradiction, not an old retransmit"
        );
    }
}

#[compio::test]
async fn live_retransmits_fill_an_announced_gap_only_after_the_matching_prefix() {
    for durability in [
        iggy_common::Durability::Replicated,
        iggy_common::Durability::Persisted,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let (mut partition, _) = recording_partition_at(1, 3);
        let sent = partition.consensus().message_bus().sent_to_replicas.clone();
        partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
        partition.runtime_options.durability = durability;
        partition.open_persistence().await.unwrap();
        let first = checksummed_segment_prepare(1, 0, 0, b"first");
        let second = checksummed_segment_prepare(2, first.header().checksum, 1, b"second");
        let third = checksummed_segment_prepare(3, second.header().checksum, 2, b"third");
        let start = Message::<StartViewHeader>::new(size_of::<StartViewHeader>()).transmute_header(
            |_, header: &mut StartViewHeader| {
                header.command = Command::StartView;
                header.cluster = TEST_CLUSTER;
                header.group = partition.namespace().inner();
                header.op = third.header().op;
                header.size = u32::try_from(size_of::<StartViewHeader>()).unwrap();
            },
        );
        partition
            .consensus()
            .handle_start_view(PlaneKind::Partitions, start.header(), &[]);
        assert!(!partition.consensus().view_log_is_pending());
        partition.repair = Some(armed_fetch_session(0, 3, 0));
        partition.apply_repaired_prepare(first.clone()).await;
        assert!(
            !partition.log.journal().inner.holds_op(1),
            "unattributed repair still needs a canonical header above commit"
        );
        partition.repair = None;

        partition.on_replicate(third.clone()).await;
        let fork = first
            .clone()
            .transmute_header(|original, header: &mut PrepareHeader| {
                *header = original;
                header.parent = u128::MAX;
                header.checksum = header.identity_checksum();
            });
        partition.on_replicate(fork).await;
        assert!(!partition.log.journal().inner.holds_op(1));
        assert!(!partition.log.journal().inner.holds_op(3));
        assert!(sent.borrow().is_empty());

        partition.on_replicate(first).await;
        partition.on_replicate(third.clone()).await;
        assert!(
            !partition.log.journal().inner.holds_op(3),
            "op 2 is still missing"
        );
        assert_eq!(partition.consensus().sequencer().current_sequence(), 3);
        partition.on_replicate(second.clone()).await;
        assert_eq!(partition.consensus().sequencer().current_sequence(), 3);
        partition.on_replicate(third.clone()).await;
        if let Some(persistence) = &partition.persistence {
            persistence
                .drain_with_timeout(compio::runtime::time::sleep)
                .await
                .unwrap();
        }
        partition.drive_persistence().await;
        assert_eq!(
            partition.consensus().last_prepare_checksum(),
            third.header().checksum
        );
        assert!(!partition.consensus().view_log_is_pending());
        let acked: Vec<_> = sent
            .borrow()
            .iter()
            .filter_map(|(_, frame)| {
                bytemuck::checked::try_from_bytes::<PrepareOkHeader>(frame.as_slice())
                    .ok()
                    .filter(|header| header.command == Command::PrepareOk)
                    .map(|header| header.op)
            })
            .collect();
        assert_eq!(
            acked,
            vec![1, 2, 3],
            "every held body must eventually be acknowledged under {durability:?}"
        );
    }
}

#[compio::test]
async fn given_a_backup_that_cannot_journal_a_prepare_when_it_arrives_should_neither_forward_nor_ack()
 {
    const MIDDLE: u8 = 1;
    const TAIL: u8 = 2;
    /// Led by replica 0 in a group of three, like view 0, so only the view
    /// number distinguishes a prepare from another view.
    const LATER_VIEW: u32 = 3;
    let first = checksummed_segment_prepare(1, 0, 0, b"first");
    let above_gap = checksummed_segment_prepare(2, first.header().checksum, 1, b"second");
    let later_view = first
        .clone()
        .transmute_header(|original, header: &mut PrepareHeader| {
            *header = original;
            header.view = LATER_VIEW;
            header.checksum = header.identity_checksum();
        });
    let cases = [
        (MIDDLE, true, &first),
        (MIDDLE, false, &above_gap),
        (MIDDLE, true, &later_view),
        (TAIL, true, &first),
        (TAIL, false, &above_gap),
    ];
    for (replica, awaiting_target, prepare) in cases {
        let (mut backup, _) = recording_partition_at(replica, 3);
        let sent = backup.consensus().message_bus().sent_to_replicas.clone();
        if awaiting_target {
            backup.consensus().begin_state_transfer_await();
        }
        let header = *prepare.header();
        let prepare = prepare.clone();
        backup.on_replicate(prepare).await;

        let case = format!(
            "replica {replica}, awaiting target {awaiting_target}, op {}, view {}",
            header.op, header.view
        );
        assert!(!backup.log.journal().inner.holds_op(header.op), "{case}");
        assert!(
            sent.borrow().is_empty(),
            "a backup never forwards or acknowledges refused data: {case}"
        );
    }
}

/// A backup that awaits a transfer target relays nothing outside `Normal`
/// status, where its view is not settled, and nothing at or below its
/// commit point, where the prepare is a late copy of a committed op.
#[compio::test]
async fn given_a_backup_outside_normal_status_or_at_its_commit_point_when_a_prepare_arrives_should_not_relay_it()
 {
    const MIDDLE: u8 = 1;
    const OP: u64 = 1;
    type Setup = fn(&IggyPartition<RecordingBus>);
    let prepare = checksummed_segment_prepare(OP, 0, 0, b"first");
    let cases: [(&str, Setup); 2] = [
        ("outside normal status", |backup| {
            backup.consensus().begin_view_probe();
        }),
        ("at its commit point", |backup| {
            backup.consensus().restore_commit_state(OP, OP);
        }),
    ];
    let mut relayed = Vec::new();
    for (case, setup) in cases {
        let (mut backup, _) = recording_partition_at(MIDDLE, 3);
        let sent = backup.consensus().message_bus().sent_to_replicas.clone();
        setup(&backup);
        backup.consensus().begin_state_transfer_await();
        backup.on_replicate(prepare.clone()).await;
        if !sent.borrow().is_empty() {
            relayed.push(case);
        }
    }
    assert!(relayed.is_empty(), "relayed a prepare {relayed:?}");
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

//     #[compio::test]
//     async fn pending_wal_acks_wait_for_the_superblock_prepass() {
//         let directory = tempfile::tempdir().unwrap();
//         let mut partition = partition_at_view(3, 3);
//         partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
//         partition.runtime_options.durability = iggy_common::Durability::Persisted;
//         partition.open_persistence().await.unwrap();
//         let prepare = checksummed_segment_prepare(1, 0, 0, b"pending").transmute_header(
//             |mut original, header: &mut PrepareHeader| {
//                 original.view = 3;
//                 original.checksum = original.identity_checksum();
//                 *header = original;
//             },
//         );
//         let header = *prepare.header();
//         partition.consensus().sequencer().set_sequence(header.op);
//         partition
//             .consensus()
//             .set_last_prepare_checksum(header.checksum);
//         partition
//             .log
//             .journal()
//             .inner
//             .append(prepare.clone().into_frozen())
//             .await
//             .unwrap();
//         let persistence = partition.persistence.as_ref().unwrap();
//         persistence.append(prepare.into_frozen(), true).unwrap();
//         assert!(!partition.register_rebuilt_ack(&header));
//         persistence.certify_log_view(3, header.op, header.checksum);
//         partition.start_persistence();
//         persistence
//             .drain_with_timeout(compio::runtime::time::sleep)
//             .await
//             .unwrap();
//         let store = Rc::new(RecordingSuperblock::default());
//         partition.set_superblock(store.clone(), None);
//         assert!(partition.consensus().needs_superblock_persist());
//
//         partition.drive_persistence().await;
//
//         assert_eq!(
//             store.attempts.get(),
//             0,
//             "the serial ACK drain must not issue a superblock write"
//         );
//         assert!(
//             partition
//                 .pending_persisted_acks
//                 .borrow()
//                 .contains_key(&header.op)
//         );
//         assert!(partition.persist_superblock_if_needed().await);
//         partition.drive_persistence().await;
//         assert_eq!(store.attempts.get(), 1);
//         assert!(partition.pending_persisted_acks.borrow().is_empty());
//     }

// #[compio::test]
// async fn pending_prepare_ack_uses_topic_durability_after_superblock_completion() {
//     for durability in [
//         iggy_common::Durability::Replicated,
//         iggy_common::Durability::Persisted,
//     ] {
//         for dispatched in [false, true] {
//             let directory = tempfile::tempdir().unwrap();
//             let mut partition = partition_at_view(0, 0);
//             partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
//             partition.runtime_options.durability = durability;
//             partition.open_persistence().await.unwrap();
//             let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
//             let prepare = checksummed_segment_prepare(1, 0, 0, b"pending");
//             let header = *prepare.header();
//             partition.consensus.sequencer().set_sequence(header.op);
//             partition
//                 .consensus
//                 .set_last_prepare_checksum(header.checksum);
//             partition
//                 .log
//                 .journal()
//                 .inner
//                 .append(prepare.clone().into_frozen())
//                 .await
//                 .unwrap();
//             persistence.append(prepare.into_frozen(), false).unwrap();
//             assert!(persistence.start());
//             Rc::clone(&persistence).run().await;
//             assert!(!persistence.is_durable(&header));
//             partition.consensus.set_view(3);
//             let store = Rc::new(RecordingSuperblock::default());
//             partition.set_superblock(store, None);
//             partition.set_io_notifier(
//                 Rc::new(|_, _| {}),
//                 crate::largest_legal_job_charge().unwrap(),
//             );
//             assert!(!partition.send_prepare_ok(&header).await);
//             assert!(
//                 partition
//                     .pending_persisted_acks
//                     .borrow()
//                     .contains_key(&header.op)
//             );
//             assert!(!partition.persist_superblock_if_needed().await);
//             let config = repair_config();
//             let crate::PartitionIoStep::Ready(plan) = partition.resume_io(&config).await else {
//                 panic!("the superblock write must be ready");
//             };
//             assert_eq!(
//                 plan.continuation,
//                 crate::PartitionIoContinuation::Superblock
//             );
//             execute_partition_io_plan(&mut partition, &config, plan).await;
//             assert!(!partition.consensus.needs_superblock_persist());
//             assert!(!persistence.is_durable(&header));
//             if dispatched {
//                 partition.resume_pending_prepare_ack().await;
//             } else {
//                 partition.drive_persistence().await;
//             }
//             assert_eq!(
//                 partition
//                     .pending_persisted_acks
//                     .borrow()
//                     .contains_key(&header.op),
//                 durability == iggy_common::Durability::Persisted,
//                 "{durability:?}, dispatched: {dispatched}"
//             );
//         }
//     }
// }

#[compio::test]
async fn persisted_ack_survives_recovery_transfer_and_a_rewound_head() {
    let (_directory, mut partition, header) = partition_with_pending_durable_ack().await;
    partition.consensus().begin_view_probe();
    partition.drive_persistence().await;
    assert_eq!(partition.pending_persisted_acks.borrow().len(), 1);

    partition.consensus().init();
    partition.consensus().begin_state_transfer_await();
    partition.drive_persistence().await;
    assert_eq!(partition.pending_persisted_acks.borrow().len(), 1);

    partition
        .consensus()
        .set_state_transfer_stage(consensus::StateTransferStage::Idle);
    partition
        .consensus()
        .sequencer()
        .set_sequence(header.op - 1);
    partition.drive_persistence().await;
    assert_eq!(partition.pending_persisted_acks.borrow().len(), 1);
    let mut acknowledgments = Vec::new();
    partition
        .consensus()
        .drain_loopback_into(&mut acknowledgments);
    assert!(acknowledgments.is_empty());

    partition.consensus().sequencer().set_sequence(header.op);
    partition.drive_persistence().await;
    partition.drive_persistence().await;
    partition
        .consensus()
        .drain_loopback_into(&mut acknowledgments);
    assert!(partition.pending_persisted_acks.borrow().is_empty());
    assert_eq!(acknowledgments.len(), 1);
    let ack = bytemuck::checked::from_bytes::<PrepareOkHeader>(acknowledgments[0].as_slice());
    assert_eq!(ack.op, header.op);
    assert_eq!(ack.prepare_checksum, header.checksum);
}

#[compio::test]
async fn persisted_ack_remains_fenced_after_a_local_commit_failure() {
    let (_directory, mut partition, header) = partition_with_pending_durable_ack().await;
    partition.fatal = Some(FatalCommit {
        namespace_raw: partition.namespace().inner(),
        op: header.op,
        operation: Some(Operation::StoreConsumerOffset),
    });
    partition.persistence.as_ref().unwrap().request_checkpoint();
    partition
        .consensus()
        .restore_commit_state(header.op, header.op);
    partition.checkpoint_persistence(&repair_config()).await;
    partition.drive_persistence().await;
    partition.acknowledge_prepare(header.op).await;
    let mut acknowledgments = Vec::new();
    partition
        .consensus()
        .drain_loopback_into(&mut acknowledgments);
    assert!(
        acknowledgments.is_empty(),
        "a fenced partition must never acknowledge"
    );
    assert_eq!(partition.pending_persisted_acks.borrow().len(), 1);
    assert_eq!(
        partition.fatal().unwrap().operation,
        Some(Operation::StoreConsumerOffset)
    );
}

//     #[compio::test]
//     async fn checkpoint_receipts_survive_deferred_completion_and_state_transfer() {
//         for install_before_completion in [false, true] {
//             let root = tempfile::tempdir().unwrap();
//             let mut config = repair_config();
//             config.path_layout.streams_root = root.path().to_string_lossy().into_owned();
//             let directory = std::path::PathBuf::from(config.get_partition_path(1, 1, 0));
//             std::fs::create_dir_all(&directory).unwrap();
//             let mut partition = partition_at_view(0, 0);
//             partition.set_partition_dir(directory.to_string_lossy().into_owned());
//             set_offset_dirs_under(&mut partition, &directory);
//             partition.runtime_options.durability = iggy_common::Durability::Persisted;
//             partition.open_persistence().await.unwrap();
//             partition.log.retire_front().unwrap();
//             partition.install_empty_segment(&config, 0).await.unwrap();
//             let prepare = checksummed_segment_prepare(1, 0, 0, b"checkpoint");
//             let header = *prepare.header();
//             let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
//             persistence
//                 .append(prepare.clone().into_frozen(), true)
//                 .unwrap();
//             assert!(persistence.start());
//             Rc::clone(&persistence).run().await;
//             partition
//                 .append_repaired_send_messages(prepare.clone())
//                 .await
//                 .unwrap();
//             partition.consensus.restore_commit_state(0, header.op);
//             partition.commit_journal(&config).await;
//             assert!(partition.fatal().is_none());
//             assert_eq!(partition.consensus.commit_min(), header.op);
//             let completion = Rc::new(std::cell::Cell::new(None));
//             let captured = Rc::clone(&completion);
//             persistence.set_notifier(Rc::new(move |value| captured.set(Some(value))));
//             let previous = directory.join("prepares-0/receipts-0.checkpoint");
//             std::fs::write(&previous, b"obsolete receipt").unwrap();
//             persistence.sync();
//             assert!(persistence.start());
//             persistence.request_checkpoint();
//
//             partition.checkpoint_persistence(&config).await;
//
//             assert!(partition.fatal().is_none());
//             assert_eq!(persistence.checkpoint_op(), 0);
//             assert!(
//                 previous.exists(),
//                 "unpublished checkpoints cannot reclaim receipts"
//             );
//             assert!(directory.join("prepares-0/receipts-1.checkpoint").exists());
//             Rc::clone(&persistence).run().await;
//             assert!(
//                 persistence.failure().is_none(),
//                 "{:?}",
//                 persistence.failure()
//             );
//             assert_eq!(persistence.checkpoint_op(), header.op);
//             let checkpoint_op = if install_before_completion {
//                 partition.maintenance.checkpoint = Some(PendingCheckpoint::Wal(header.op));
//                 assert!(matches!(
//                     partition.drive_checkpoint_io(&config).unwrap(),
//                     Some(crate::PartitionIoStep::Progress)
//                 ));
//                 assert!(matches!(
//                     partition.maintenance.checkpoint,
//                     Some(PendingCheckpoint::Reclaim(1))
//                 ));
//                 let transferred =
//                     checksummed_segment_prepare(2, header.checksum, 1, b"transferred");
//                 let mut segment_body = prepare.as_slice()[size_of::<PrepareHeader>()..].to_vec();
//                 segment_body
//                     .extend_from_slice(&transferred.as_slice()[size_of::<PrepareHeader>()..]);
//                 let artifact = consensus::StateArtifact::for_bytes(
//                     consensus::state_manifest::artifact_kind::SEGMENT_LOG,
//                     0,
//                     &segment_body,
//                 );
//                 let staged = partition
//                     .spill_transfer_segment(&artifact, segment_body, None)
//                     .await
//                     .unwrap();
//                 let offsets = crate::state_transfer::ConsumerOffsetsWire {
//                     owner_retirement: None,
//                     dedup_capacity: partition.dedup.capacity(),
//                     required_metadata_frontier: 0,
//                     prepare_checksum: Some(transferred.header().checksum),
//                     checkpoint_prepare: transferred.as_slice().to_vec(),
//                     next_offset: 2,
//                     emptied_at: 0,
//                     consumers: Vec::new(),
//                     groups: Vec::new(),
//                     external_groups: Vec::new(),
//                     dedup: partition.dedup.watermarks_sorted(),
//                     owners: Vec::new(),
//                     history_transition: None,
//                     retirement_paid_through: 0,
//                 };
//                 partition
//                     .install_state_transfer(&config, 2, vec![staged], &offsets.encode())
//                     .await
//                     .unwrap();
//                 settle_partition_io(&mut partition, &config).await;
//                 2
//             } else {
//                 header.op
//             };
//             let current = directory.join(format!("prepares-0/receipts-{checkpoint_op}.checkpoint"));
//             assert!(current.exists());
//             let completion = completion.get().unwrap();
//             assert!(persistence.accepts_completion(completion));
//             partition
//                 .on_persistence_completed(completion, &config)
//                 .await;
//             assert!(!previous.exists());
//             assert!(
//                 current.exists(),
//                 "completion must retain the published checkpoint"
//             );
//             drop(partition);
//             drop(persistence);
//             let mut reopened = partition_at_view(0, 0);
//             reopened.set_partition_dir(directory.to_string_lossy().into_owned());
//             let (recovered, _) = crate::persistence::PartitionPersistence::open_with_storage(
//                 &directory.join("prepares-0"),
//                 reopened.namespace().inner(),
//                 reopened.created_revision(),
//                 journal::durable_storage::DiskStorage,
//             )
//             .await
//             .unwrap();
//             assert_eq!(recovered.checkpoint_op(), checkpoint_op);
//             reopened
//                 .restore_retry_checkpoint(recovered.checkpoint_op())
//                 .await
//                 .unwrap();
//             let consensus::client_table::RequestStatus::Duplicate(reply) = reopened
//                 .dedup
//                 .check_request(1, 1, 1, Operation::SendMessages)
//             else {
//                 panic!("checkpoint must retain the committed result for a retry");
//             };
//             assert!(reply.header().has_committed_result());
//         }
//     }

#[compio::test]
async fn receipt_publication_failure_preserves_the_paired_wal_frontier() {
    let (directory, mut partition, header) = partition_with_pending_durable_ack().await;
    partition
        .consensus()
        .restore_commit_state(header.op, header.op);
    assert!(
        partition
            .commit_messages_inner(&repair_config(), true, header.op)
            .await
            .unwrap(),
        "the committed message must materialize before injecting the receipt publication fault"
    );
    let persistence = partition.persistence.as_ref().unwrap().clone();
    let frontier = directory.path().join("prepares-0/frontier");
    let before = std::fs::read(&frontier).unwrap();
    let temporary = directory
        .path()
        .join(format!("prepares-0/receipts-{}.checkpoint.tmp", header.op));
    std::fs::create_dir(temporary).unwrap();
    persistence.request_checkpoint();
    partition.checkpoint_persistence(&repair_config()).await;
    assert!(
        partition.fatal().is_some(),
        "failed receipt publication must fence the partition"
    );
    assert_eq!(
        persistence.checkpoint_op(),
        0,
        "the WAL prefix must remain recoverable"
    );
    assert_eq!(persistence.head(), header.op);
    assert_eq!(std::fs::read(&frontier).unwrap(), before);
    assert!(
        !directory
            .path()
            .join(format!("prepares-0/receipts-{}.checkpoint", header.op))
            .exists()
    );
}

//     #[compio::test]
//     async fn consumer_offset_open_failure_does_not_poison_the_wal_and_can_be_retried() {
//         let directory = tempfile::tempdir().unwrap();
//         let (mut partition, _) = recording_partition_at(0, 3);
//         partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
//         partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Persisted;
//         partition.open_persistence().await.unwrap();
//         let path = directory.path().join("consumer-offset");
//         std::fs::create_dir(&path).unwrap();
//         assert!(matches!(
//             partition
//                 .write_consumer_offset(path.to_str().unwrap(), 7, false)
//                 .await,
//             Err(IggyError::CannotOpenConsumerOffsetsFile(_))
//         ));
//         partition.drive_persistence().await;
//         assert!(partition.persistence.as_ref().unwrap().failure().is_none());
//         assert!(partition.fatal.is_none());
//
//         std::fs::remove_dir(&path).unwrap();
//         partition
//             .write_consumer_offset(path.to_str().unwrap(), 7, false)
//             .await
//             .unwrap();
//         assert_eq!(
//             std::fs::read(&path).unwrap(),
//             crate::offset_storage::encode_offset_record(7)
//         );
//         assert!(
//             partition
//                 .persistence
//                 .as_ref()
//                 .unwrap()
//                 .take_offset_file(path.to_str().unwrap())
//                 .is_some()
//         );
//     }

//     #[cfg(target_os = "linux")]
//     #[compio::test]
//     async fn consumer_offset_write_failure_keeps_its_error_kind_and_original_writer() {
//         let directory = tempfile::tempdir().unwrap();
//         let (mut partition, _) = recording_partition_at(0, 3);
//         partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
//         partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Persisted;
//         partition.open_persistence().await.unwrap();
//         assert!(matches!(
//             partition.write_consumer_offset(DEV_FULL, 7, false).await,
//             Err(IggyError::CannotWriteToFile)
//         ));
//         let persistence = partition.persistence.as_ref().unwrap();
//         assert_eq!(
//             persistence.failure().unwrap().kind(),
//             std::io::ErrorKind::StorageFull
//         );
//         assert!(persistence.take_offset_file(DEV_FULL).is_some());
//         partition.drive_persistence().await;
//         assert!(
//             partition.fatal.is_some(),
//             "a failed write must remain fenced"
//         );
//         assert_eq!(
//             partition.fatal.as_ref().unwrap().operation,
//             Some(Operation::StoreConsumerOffset)
//         );
//     }
//
//     #[cfg(target_os = "linux")]
//     #[compio::test]
//     async fn workerless_offset_write_failure_invalidates_its_retained_descriptor() {
//         let (partition, _) = recording_partition_at(0, 3);
//         assert!(partition.persistence.is_none());
//         assert!(matches!(
//             partition.write_consumer_offset(DEV_FULL, 7, false).await,
//             Err(IggyError::CannotWriteToFile)
//         ));
//         assert!(partition.workerless_offset_files.take(DEV_FULL).is_none());
//         assert!(
//             partition.fatal.is_none(),
//             "workerless errors retain their caller's policy"
//         );
//     }
//
//     #[compio::test]
//     async fn workerless_offsets_reuse_a_writer_and_delete_reopens_the_recreated_path() {
//         let directory = tempfile::tempdir().unwrap();
//         let (mut partition, _) = recording_partition_at(0, 3);
//         let offset_dir = directory.path().join("offsets");
//         partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
//             Some(offset_dir.to_string_lossy().into_owned());
//         // Persisted offsets bypass the workerless descriptor cache.
//         partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Replicated;
//         let kind = ConsumerKind::Consumer;
//         let consumer = 7;
//         let path = partition.persisted_offset_path(kind, consumer).unwrap();
//         let store = PendingConsumerOffsetCommit::upsert(kind, consumer, 3);
//         partition
//             .persist_consumer_offset_commit(store)
//             .await
//             .unwrap();
//         let (file, permit) = partition.workerless_offset_files.checkout(&path).unwrap();
//         assert!(file.is_some(), "first write retained its descriptor");
//         partition
//             .workerless_offset_files
//             .put(&path, file.unwrap(), permit);
//         partition
//             .persist_consumer_offset_commit(PendingConsumerOffsetCommit::upsert(kind, consumer, 5))
//             .await
//             .unwrap();
//         assert_eq!(
//             std::fs::read(&path).unwrap(),
//             crate::offset_storage::encode_offset_record(5)
//         );
//         partition
//             .persist_consumer_offset_commit(PendingConsumerOffsetCommit::delete(kind, consumer))
//             .await
//             .unwrap();
//         assert!(partition.workerless_offset_files.take(&path).is_none());
//         assert!(!std::path::Path::new(&path).exists());
//         partition
//             .persist_consumer_offset_commit(store)
//             .await
//             .unwrap();
//         assert_eq!(
//             std::fs::read(&path).unwrap(),
//             crate::offset_storage::encode_offset_record(3)
//         );
//         partition.invalidate_poll_history();
//         assert!(partition.workerless_offset_files.take(&path).is_none());
//     }

#[compio::test]
async fn checkpoint_paths_include_offset_parent_directories() {
    let directory = tempfile::tempdir().unwrap();
    let mut partition = partition_at_view(1, 1);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] = Some(
        directory
            .path()
            .join("offsets/consumers")
            .to_string_lossy()
            .into_owned(),
    );
    partition.consumer_offset_dirs[ConsumerKind::ConsumerGroup.index()] = Some(
        directory
            .path()
            .join("offsets/groups")
            .to_string_lossy()
            .into_owned(),
    );
    partition.runtime_options.durability = iggy_common::Durability::Persisted;
    partition.open_persistence().await.unwrap();
    let (_, directories) = partition.persistence_checkpoint_files(&repair_config());
    assert!(directories.contains(&directory.path().join("offsets")));
    assert!(directories.contains(&directory.path().join("offsets/consumers")));
    assert!(directories.contains(&directory.path().join("offsets/groups")));
    assert!(directories.contains(&directory.path().to_path_buf()));

    let (_, clean) = partition.persistence_checkpoint_files(&repair_config());
    assert_eq!(clean, [directory.path().to_path_buf()]);

    let kind = ConsumerKind::ExternalGroup;
    let external = directory.path().join("offsets/external_groups");
    partition.consumer_offset_dirs[kind.index()] = Some(external.to_string_lossy().into_owned());
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    persistence.mark_offset_dirty(kind.index(), 7, true);
    persistence.mark_offset_dirty(kind.index(), 7, false);
    let (files, deleted) = partition.persistence_checkpoint_files(&repair_config());
    assert!(files.is_empty(), "the last key was deleted");
    assert_eq!(
        deleted,
        [
            external,
            directory.path().join("offsets"),
            directory.path().to_path_buf()
        ]
    );

    persistence.reset(0, None);
    let (_, reset) = partition.persistence_checkpoint_files(&repair_config());
    assert_eq!(reset.len(), ConsumerKind::COUNT + 2);
}

// async fn publish_empty_retry_checkpoint(
//     partition: &IggyPartition<IggyMessageBus, RecordingSuperblock>,
//     through: u64,
// ) {
//     let mut checkpoint = Box::new(partition_at_view(0, 0));
//     checkpoint.set_partition_dir(partition.partition_dir.clone().unwrap());
//     checkpoint.set_created_revision(partition.created_revision());
//     checkpoint
//         .consensus()
//         .restore_commit_state(through, through);
//     checkpoint.persist_retry_checkpoint(through).await.unwrap();
// }

// #[compio::test]
// async fn receipt_checkpoint_excludes_consumer_offsets_and_restores_retry_protection() {
//     const CHECKPOINT_OP: u64 = 1;
//     const METADATA_FRONTIER: u64 = 17;
//     const CLIENT: u128 = 42;
//     const SESSION: u64 = 4;
//     const CONSUMER_ID: u32 = 7;
//     let directory = tempfile::tempdir().unwrap();
//     let mut partition = test_partition();
//     partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
//     std::fs::create_dir(directory.path().join("prepares-0")).unwrap();
//     partition.seed_recovered_consumer_offset(ConsumerKind::Consumer, CONSUMER_ID, 10, 10);
//     partition.consumer_offsets.pin().insert(
//         CONSUMER_ID as usize,
//         ConsumerOffset::new(ConsumerKind::Consumer, CONSUMER_ID, 10, String::new()),
//     );
//     partition.seed_recovered_consumer_offset(ConsumerKind::ConsumerGroup, CONSUMER_ID, 11, 11);
//     partition.consumer_group_offsets.pin().insert(
//         ConsumerGroupId(CONSUMER_ID as usize),
//         ConsumerOffset::new(ConsumerKind::ConsumerGroup, CONSUMER_ID, 11, String::new()),
//     );
//     partition.required_metadata_frontier = METADATA_FRONTIER;
//     partition
//         .dedup_mut()
//         .commit_capacity(consensus::PARTITION_DEDUP_CLIENTS_MAX)
//         .unwrap();
//     let reply = consensus::build_reply_message(
//         &PrepareHeader {
//             client: CLIENT,
//             request: 1,
//             session: SESSION,
//             op: CHECKPOINT_OP,
//             operation: Operation::StoreConsumerOffset,
//             ..Default::default()
//         },
//         &Bytes::new(),
//     );
//     partition
//         .dedup_mut()
//         .commit_partition_reply(1, SESSION, reply)
//         .unwrap();
//     partition
//         .consensus()
//         .restore_commit_state(CHECKPOINT_OP, CHECKPOINT_OP);
//     let path = partition
//         .persist_retry_checkpoint(CHECKPOINT_OP)
//         .await
//         .unwrap();
//     let bytes = std::fs::read(path).unwrap();
//     let content = consensus::le_cursor::split_verified_trailer(&bytes).unwrap();
//     let mut cursor = consensus::le_cursor::LeCursor::new(content);
//     cursor.take(size_of::<[u8; 4]>()).unwrap();
//     assert_eq!(cursor.u64().unwrap(), partition.consensus().group());
//     assert_eq!(cursor.u64().unwrap(), partition.created_revision());
//     assert_eq!(cursor.u64().unwrap(), CHECKPOINT_OP);
//     let wire = crate::state_transfer::ConsumerOffsetsWire::decode(cursor.remaining()).unwrap();
//     assert!(wire.consumers.is_empty() && wire.groups.is_empty());
//     assert_eq!(wire.checkpoint_prepare, b"");
//     assert_eq!(wire.required_metadata_frontier, METADATA_FRONTIER);
//     let mut recovered = test_partition();
//     recovered.set_partition_dir(directory.path().to_string_lossy().into_owned());
//     recovered
//         .restore_retry_checkpoint(CHECKPOINT_OP)
//         .await
//         .unwrap();
//     assert_eq!(
//         recovered.dedup().watermarks_sorted(),
//         partition.dedup().watermarks_sorted()
//     );
//     assert_eq!(recovered.required_metadata_frontier, METADATA_FRONTIER);
// }

#[test]
fn given_delayed_poll_after_stream_recreation_when_replying_should_keep_original_encryptor() {
    let encryptor = |seed: u8| {
        Arc::new(EncryptorKind::Aes256Gcm(
            Aes256GcmEncryptor::new(&[seed; 32]).expect("valid key"),
        ))
    };
    let (original_key, replacement_key) = (encryptor(1), encryptor(2));
    let mut original = test_partition();
    original.set_encryptor(Some(Arc::clone(&original_key)));
    let read = poll_read_result(&original, PollingConsumer::Consumer(1, 0), false, Some(0));
    let completion = original.complete_poll(read).expect("completes");

    // The stream is deleted and its id reused before the reply is built.
    drop(original);
    let mut recreated = test_partition();
    recreated.set_encryptor(Some(Arc::clone(&replacement_key)));

    let pinned = completion.encryptor.expect("an encrypted stream's poll");
    assert!(Arc::ptr_eq(&pinned, &original_key));
    assert!(!Arc::ptr_eq(&pinned, &replacement_key));
    let mut plaintext = test_partition();
    let read = poll_read_result(&plaintext, PollingConsumer::Consumer(1, 0), false, None);
    assert!(
        plaintext
            .complete_poll(read)
            .expect("completes")
            .encryptor
            .is_none()
    );
}

#[test]
fn given_empty_poll_of_encrypted_stream_when_replying_should_not_carry_its_key() {
    let encryptor = Arc::new(EncryptorKind::Aes256Gcm(
        Aes256GcmEncryptor::new(&[3; 32]).expect("valid key"),
    ));
    let mut partition = test_partition();
    partition.set_encryptor(Some(Arc::clone(&encryptor)));

    let empty = poll_read_result(&partition, PollingConsumer::Consumer(1, 0), false, None);
    let completion = partition.complete_poll(empty).expect("completes");

    assert!(completion.fragments.is_empty());
    assert!(completion.encryptor.is_none());
    assert_eq!(Arc::strong_count(&encryptor), 2);
    let read = poll_read_result(&partition, PollingConsumer::Consumer(1, 0), false, Some(0));
    let pinned = partition
        .complete_poll(read)
        .expect("completes")
        .encryptor
        .expect("a poll with fragments carries its stream's key");
    assert!(Arc::ptr_eq(&pinned, &encryptor));
}

#[compio::test]
async fn given_idle_owner_when_polled_or_view_changed_should_require_drain_evidence() {
    const GROUP_ID: u64 = 7;
    const CLIENT_ID: u128 = 42;
    let mut partition = test_partition();
    install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
    assert!(partition.consumer_group_drained(GROUP_ID));
    let read = partition
        .build_poll_plan(
            PollingConsumer::ConsumerGroup(usize::try_from(GROUP_ID).unwrap(), 0),
            &PollingArgs::new(iggy_common::PollingStrategy::next(), 1, false),
            false,
        )
        .execute_resident();
    assert!(!partition.consumer_group_drained(GROUP_ID));
    assert!(read.fragments.is_empty());
    partition.complete_poll(read).unwrap();
    assert!(partition.consumer_group_drained(GROUP_ID));
    let read = poll_read_result(
        &partition,
        PollingConsumer::ConsumerGroup(usize::try_from(GROUP_ID).unwrap(), 0),
        false,
        Some(0),
    );
    partition.complete_poll(read).unwrap();
    assert!(!partition.consumer_group_drained(GROUP_ID));
    partition.consensus.set_view(1);
    partition.resynchronize_consumer_offset_reservations();
    assert!(!partition.consumer_group_drained(GROUP_ID));
    install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
    assert!(partition.consumer_group_drained(GROUP_ID));
}

#[compio::test]
async fn given_recorded_last_polled_when_owner_install_applies_should_clear_it() {
    const GROUP_ID: u64 = 7;
    const CLIENT_ID: u128 = 42;
    const POLLED_OFFSET: u64 = 0;
    let mut partition = test_partition();
    install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
    let read = poll_read_result(
        &partition,
        PollingConsumer::ConsumerGroup(usize::try_from(GROUP_ID).unwrap(), 0),
        false,
        Some(POLLED_OFFSET),
    );
    partition.complete_poll(read).unwrap();
    assert_eq!(
        partition.group_offset_state(GROUP_ID).0,
        Some(POLLED_OFFSET)
    );

    install_test_owner(&mut partition, GROUP_ID, CLIENT_ID + 1).await;

    assert_eq!(
        partition.group_offset_state(GROUP_ID).0,
        None,
        "a new owner must not inherit the last polled offset"
    );
}

#[compio::test]
async fn given_busy_pipeline_when_owner_fence_arrives_should_queue_and_refuse_late_poll() {
    const GROUP_ID: u64 = 7;
    const CLIENT_ID: u128 = 42;
    let mut partition = test_partition();
    install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
    let late = poll_read_result(
        &partition,
        PollingConsumer::ConsumerGroup(usize::try_from(GROUP_ID).unwrap(), 0),
        false,
        Some(0),
    );
    let mut send = checksumless_send_request(partition.namespace(), 1);
    send.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
        .copy_from_slice(&build_segment_record(partition.namespace(), 0));
    let send = send.transmute_header(|mut header, next: &mut RoutedRequestHeader| {
        header.metadata_watermark = partition.required_metadata_frontier;
        *next = header;
    });
    partition.on_request(send, None).await;
    let send_op = partition.consensus.sequencer().current_sequence();
    let installation = InstallConsumerGroupOwnerRequest {
        incarnation: partition.created_revision(),
        group_id: GROUP_ID,
        owner: ConsumerGroupOwner {
            client_id: CLIENT_ID + 1,
            session: 1,
            generation: 2,
        },
        metadata_op: partition.required_metadata_frontier + 1,
    };
    let request = partition_control_request(
        &partition,
        Operation::InstallConsumerGroupOwner,
        &installation.to_bytes(),
        installation.metadata_op,
    );
    partition.on_request(request, None).await;
    assert!(
        partition.consensus.sequencer().current_sequence() > send_op
            || partition.consensus.request_queue_len() > 0
    );
    assert!(matches!(
        partition.complete_poll(late),
        Err(IggyError::TransientNotAccepted)
    ));
    assert_eq!(partition.group_offset_state(GROUP_ID).0, None);
    for _ in 0..2 {
        let op = partition.consensus.sequencer().current_sequence();
        partition.consensus.advance_commit_max(op);
        partition.commit_journal(&repair_config()).await;
    }
    assert_eq!(
        partition.consumer_group_owner(GROUP_ID),
        Some(installation.owner)
    );
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
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

/// Journal installs that replace one client in two groups, apply neither
/// and change the view. A view change can leave this state, and the rebuilt
/// latch then names only the second install. Returns the first install.
async fn unapplied_installs_after_view_change(
    partition: &mut IggyPartition<IggyMessageBus>,
) -> InstallConsumerGroupOwnerRequest {
    const GROUPS: [u64; 2] = [7, 8];
    const REPLACED_CLIENT: u128 = 42;
    for group_id in GROUPS {
        install_test_owner(partition, group_id, REPLACED_CLIENT).await;
    }
    let first = next_test_installation(partition, GROUPS[0], REPLACED_CLIENT + 1);
    let second = InstallConsumerGroupOwnerRequest {
        metadata_op: first.metadata_op + 1,
        ..next_test_installation(partition, GROUPS[1], REPLACED_CLIENT + 1)
    };
    for installation in [first, second] {
        let prepare = partition.pipeline_local_request(
            owner_install_request(partition, &installation),
            None,
            None,
        );
        partition.on_replicate(prepare).await;
    }
    partition.consensus.set_view(1);
    first
}

/// A store under an unassigned owner fails at apply on every replica, so
/// the read that would prepare it must not complete.
#[compio::test]
async fn given_unassigned_captured_owner_when_group_read_completes_should_refuse_before_auto_commit()
 {
    const GROUP_ID: u64 = 7;
    const CLIENT_ID: u128 = 42;
    let mut partition = test_partition();
    install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
    let mut unassigned = next_test_installation(&partition, GROUP_ID, 0);
    unassigned.owner.session = 0;
    partition
        .on_request(owner_install_request(&partition, &unassigned), None)
        .await;
    commit_log(&mut partition).await;
    let op = partition.consensus.sequencer().current_sequence();
    assert_eq!(
        partition.consumer_group_owner(GROUP_ID),
        Some(unassigned.owner)
    );
    partition.stats.increment_messages_count(1);
    let read = poll_read_result(
        &partition,
        PollingConsumer::ConsumerGroup(usize::try_from(GROUP_ID).unwrap(), 0),
        true,
        Some(0),
    );

    let refusal = complete_poll_and_commit(&mut partition, read).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(partition.consensus.sequencer().current_sequence(), op);
    assert!(
        matches!(
            refusal,
            Some(IggyError::ConsumerGroupPartitionNotOwned(client, 0))
                if client == unassigned.owner.client_id
        ),
        "{refusal:?}"
    );
    assert_eq!(partition.group_offset_state(GROUP_ID), (None, None));
}

/// A view change can leave an install that this replica has not applied.
/// The replaced owner's read must wait for it even without a commit, and
/// the successor is served once it applies.
#[compio::test]
async fn given_unapplied_install_after_view_change_when_replaced_owner_read_completes_should_refuse()
 {
    const GROUP_ID: u64 = 7;
    const CLIENT_ID: u128 = 42;
    let consumer = PollingConsumer::ConsumerGroup(usize::try_from(GROUP_ID).unwrap(), 0);
    let mut partition = test_partition();
    install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
    let successor = next_test_installation(&partition, GROUP_ID, CLIENT_ID + 1);
    let prepare =
        partition.pipeline_local_request(owner_install_request(&partition, &successor), None, None);
    partition.on_replicate(prepare).await;
    partition.consensus.set_view(1);
    let read = poll_read_result(&partition, consumer, false, Some(0));

    assert!(matches!(
        partition.complete_poll(read),
        Err(IggyError::TransientNotAccepted)
    ));
    assert_eq!(partition.group_offset_state(GROUP_ID), (None, None));

    commit_log(&mut partition).await;
    assert_eq!(
        partition.consumer_group_owner(GROUP_ID),
        Some(successor.owner)
    );
    let read = poll_read_result(&partition, consumer, false, Some(0));
    partition.complete_poll(read).unwrap();
    assert_eq!(partition.group_offset_state(GROUP_ID).0, Some(0));
}

/// A view change can leave unapplied installs for several groups, and the
/// rebuilt latch holds only the last one. A store prepared under an earlier
/// group's replaced owner would fail at apply on every replica.
#[compio::test]
async fn given_unapplied_installs_for_two_groups_after_view_change_when_first_group_auto_commits_should_refuse_without_fatal_commit()
 {
    let mut partition = test_partition();
    let first = unapplied_installs_after_view_change(&mut partition).await;
    partition.stats.increment_messages_count(1);
    let head = partition.consensus.sequencer().current_sequence();
    let read = poll_read_result(
        &partition,
        PollingConsumer::ConsumerGroup(usize::try_from(first.group_id).unwrap(), 0),
        true,
        Some(0),
    );

    let refusal = complete_poll_and_commit(&mut partition, read).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(partition.consensus.sequencer().current_sequence(), head);
    assert!(
        matches!(refusal, Some(IggyError::TransientNotAccepted)),
        "{refusal:?}"
    );
    assert_eq!(
        partition.consumer_group_owner(first.group_id),
        Some(first.owner)
    );
    assert_eq!(partition.group_offset_state(first.group_id), (None, None));
}

/// The same state as above, but the replaced owner stores explicitly. Its
/// store would land behind the first install and fail at apply on every
/// replica, so admission waits until every unapplied install has applied.
#[compio::test]
async fn given_unapplied_installs_for_two_groups_after_view_change_when_replaced_owner_stores_should_refuse_without_fatal_commit()
 {
    let mut partition = test_partition();
    let first = unapplied_installs_after_view_change(&mut partition).await;
    partition.stats.increment_messages_count(1);
    let replaced = partition.consumer_group_owner(first.group_id).unwrap();
    let head = partition.consensus.sequencer().current_sequence();
    let group = u32::try_from(first.group_id).unwrap();
    let store = |owner: ConsumerGroupOwner, metadata_watermark: u64| {
        store_offset_request(
            owner.client_id,
            1,
            ConsumerKind::ConsumerGroup,
            group,
            0,
            AckLevel::Quorum,
        )
        .transmute_header(|mut header, next: &mut RoutedRequestHeader| {
            header.metadata_watermark = metadata_watermark;
            header.owner_generation = owner.generation;
            *next = header;
        })
    };
    let (sender, receiver) = consensus::oneshot_channel();

    let frontier = partition.required_metadata_frontier;
    partition
        .on_request(store(replaced, frontier), Some(sender))
        .await;
    commit_log(&mut partition).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(partition.consensus.sequencer().current_sequence(), head);
    assert_eq!(
        receiver.await.unwrap().header().status,
        IggyError::TransientNotAccepted.as_code()
    );
    assert_eq!(
        partition.consumer_group_owner(first.group_id),
        Some(first.owner)
    );
    assert_eq!(partition.group_offset_state(first.group_id), (None, None));

    let frontier = partition.required_metadata_frontier;
    partition
        .on_request(store(first.owner, frontier), None)
        .await;
    commit_log(&mut partition).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(partition.group_offset_state(first.group_id).1, Some(0));
}

/// A store without a poll position carries owner 0. It binds to the
/// installed owner only from that owner's client and session, and only
/// when the writer routed by metadata that already decided the install.
/// Any other owner-0 store stays refused, so a store never lands under an
/// owner its writer did not see.
#[compio::test]
async fn given_owner_zero_store_when_admitted_should_bind_only_the_installed_owner_it_routed_by() {
    const GROUP_ID: u64 = 7;
    const CLIENT_ID: u128 = (1 << 64) + 42;
    const STORED_OFFSET: u64 = 0;
    let group = u32::try_from(GROUP_ID).unwrap();
    let mut partition = test_partition();
    partition.stats.increment_messages_count(1);
    install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
    let installed = partition.consumer_group_owners[&GROUP_ID].installation;
    let frontier = partition.required_metadata_frontier;
    assert!(installed.metadata_op <= frontier);
    let store = |request: u64, session: u64, minimum_metadata_op: u64| {
        store_offset_request(
            CLIENT_ID,
            request,
            ConsumerKind::ConsumerGroup,
            group,
            STORED_OFFSET,
            AckLevel::Quorum,
        )
        .transmute_header(|mut header, next: &mut RoutedRequestHeader| {
            header.session = session;
            header.metadata_watermark = frontier;
            header.minimum_metadata_op = minimum_metadata_op;
            *next = header;
        })
    };
    let head = partition.consensus.sequencer().current_sequence();

    for (request, session, minimum_metadata_op) in [
        (1, installed.owner.session, installed.metadata_op - 1),
        (2, installed.owner.session + 1, installed.metadata_op),
    ] {
        let (sender, receiver) = consensus::oneshot_channel();
        partition
            .on_request(store(request, session, minimum_metadata_op), Some(sender))
            .await;
        assert_eq!(
            receiver.await.unwrap().header().status,
            IggyError::ConsumerGroupPartitionNotOwned(CLIENT_ID, 0).as_code(),
            "session {session} routed by op {minimum_metadata_op} must not bind"
        );
    }
    assert_eq!(partition.consensus.sequencer().current_sequence(), head);

    partition
        .on_request(
            store(3, installed.owner.session, installed.metadata_op),
            None,
        )
        .await;
    assert_eq!(partition.consensus.sequencer().current_sequence(), head + 1);
    commit_log(&mut partition).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(
        partition.group_offset_state(GROUP_ID).1,
        Some(STORED_OFFSET)
    );
}

//     /// An install wipes the log above its commit op. A barrier left at an
//     /// install it dropped would hold the installed owner until commits pass that
//     /// op, and the auto-commits it refuses are all an idle group would commit.
//     #[compio::test]
//     async fn given_barrier_above_the_offer_when_state_transfer_publishes_should_serve_the_installed_owner()
//      {
//         const GROUP_ID: u64 = 7;
//         const CLIENT_ID: u128 = 42;
//         let directory = tempfile::tempdir().unwrap();
//         let mut partition = test_partition();
//         partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
//         set_offset_dirs_under(&mut partition, directory.path());
//         let installed_op = install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
//         let installed = partition.consumer_group_owners[&GROUP_ID];
//         let successor = next_test_installation(&partition, GROUP_ID, CLIENT_ID + 1);
//         let prepare = partition.pipeline_local_request(
//             owner_install_request(&partition, &successor),
//             None,
//             None,
//         );
//         partition.apply_replicated_operation(prepare).await.unwrap();
//         assert_eq!(
//             partition.acknowledged_op.get(),
//             installed_op,
//             "the successor may only be discarded if it was never acknowledged"
//         );
//         partition.consensus.set_view(1);
//         partition.resynchronize_consumer_offset_reservations();
//         assert_eq!(partition.owner_install_barrier, installed_op + 1);
//
//         let offsets = crate::state_transfer::ConsumerOffsetsWire {
//             dedup_capacity: partition.dedup().capacity(),
//             required_metadata_frontier: installed.installation.metadata_op,
//             owners: vec![installed],
//             ..Default::default()
//         };
//         partition
//             .install_state_transfer(
//                 &repair_config(),
//                 installed_op,
//                 Vec::new(),
//                 &offsets.encode(),
//             )
//             .await
//             .unwrap();
//         partition.stats.increment_messages_count(1);
//         let read = poll_read_result(
//             &partition,
//             PollingConsumer::ConsumerGroup(usize::try_from(GROUP_ID).unwrap(), 0),
//             true,
//             Some(0),
//         );
//
//         let refusal = complete_poll_and_commit(&mut partition, read).await;
//         assert!(refusal.is_none(), "{refusal:?}");
//         assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
//         assert_eq!(partition.group_offset_state(GROUP_ID), (Some(0), Some(0)));
//     }

#[compio::test]
async fn given_later_install_in_the_log_when_an_earlier_install_publishes_should_keep_the_group_fenced()
 {
    const GROUP_ID: u64 = 7;
    const CLIENT_ID: u128 = 42;
    let mut partition = test_partition();
    install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
    partition.stats.increment_messages_count(1);
    let earlier = next_test_installation(&partition, GROUP_ID, CLIENT_ID + 1);
    partition
        .on_request(owner_install_request(&partition, &earlier), None)
        .await;
    let earlier_op = partition.consensus.sequencer().current_sequence();
    // Admission refuses a second install while one is pending, but a view
    // change can leave a committed install that has not applied ahead of a
    // newer one, so the newer install enters the log directly.
    let later = InstallConsumerGroupOwnerRequest {
        owner: ConsumerGroupOwner {
            client_id: CLIENT_ID + 2,
            session: 1,
            generation: earlier.owner.generation + 1,
        },
        metadata_op: earlier.metadata_op + 1,
        ..earlier
    };
    let prepare =
        partition.pipeline_local_request(owner_install_request(&partition, &later), None, None);
    partition.on_replicate(prepare).await;
    let later_op = earlier_op + 1;
    partition.consensus.set_view(1);
    partition.resynchronize_consumer_offset_reservations();
    assert_eq!(partition.pending_owner_install.get(), Some(later));

    partition.consensus.advance_commit_max(earlier_op);
    partition.commit_journal(&repair_config()).await;
    assert_eq!(
        partition.consumer_group_owner(GROUP_ID),
        Some(earlier.owner)
    );
    assert_eq!(partition.pending_owner_install.get(), Some(later));

    let store = store_offset_request(
        earlier.owner.client_id,
        1,
        ConsumerKind::ConsumerGroup,
        u32::try_from(GROUP_ID).unwrap(),
        0,
        AckLevel::Quorum,
    )
    .transmute_header(|mut header, next: &mut RoutedRequestHeader| {
        header.metadata_watermark = partition.required_metadata_frontier;
        header.owner_generation = earlier.owner.generation;
        *next = header;
    });
    let (sender, receiver) = consensus::oneshot_channel();
    partition.on_request(store, Some(sender)).await;
    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        later_op,
        "an offset under the replaced owner must not land behind the later install"
    );
    assert_eq!(
        receiver.await.unwrap().header().status,
        IggyError::TransientNotAccepted.as_code()
    );

    partition.consensus.advance_commit_max(later_op);
    partition.commit_journal(&repair_config()).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(partition.consumer_group_owner(GROUP_ID), Some(later.owner));
    assert!(partition.pending_owner_install.get().is_none());
}

/// Repair can journal an install ahead of a lower op, so the tail header is
/// not the highest op. The first scan of the next view must still find the
/// install, or a store under the replaced owner lands behind it and fails at
/// apply on every replica.
#[compio::test]
async fn given_install_repaired_ahead_of_lower_op_when_view_starts_should_refuse_replaced_owner_store()
 {
    const GROUP_ID: u64 = 7;
    const CLIENT_ID: u128 = 42;
    const CONSUMER_ID: u32 = 9;
    let mut origin = test_partition();
    let mut partition = test_partition();
    install_test_owner(&mut origin, GROUP_ID, CLIENT_ID).await;
    let installed_op = install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
    let replaced = partition.consumer_group_owner(GROUP_ID).unwrap();
    for replica in [&origin, &partition] {
        replica.stats.increment_messages_count(1);
    }
    let consumer_store = store_offset_request(
        CLIENT_ID + 2,
        1,
        ConsumerKind::Consumer,
        CONSUMER_ID,
        0,
        AckLevel::Quorum,
    )
    .transmute_header(|mut header, next: &mut RoutedRequestHeader| {
        header.metadata_watermark = origin.required_metadata_frontier;
        *next = header;
    });
    origin.on_request(consumer_store, None).await;
    let successor = next_test_installation(&origin, GROUP_ID, CLIENT_ID + 1);
    origin
        .on_request(owner_install_request(&origin, &successor), None)
        .await;
    let head = origin.consensus.sequencer().current_sequence();
    assert_eq!(head, installed_op + 2);

    partition.repair = Some(armed_session(head, installed_op));
    for op in [head, head - 1] {
        partition
            .apply_repaired_prepare(retained_prepare(&origin, op))
            .await;
    }
    assert_eq!(partition.consensus.sequencer().current_sequence(), head);
    // A view start keeps the journal order and rebuilds the pipeline from it.
    partition.consensus.set_view(1);
    partition.consensus.with_pipeline_mut(|pipeline| {
        for op in installed_op + 1..=head {
            let header = partition.log.journal().inner.header_by_op(op).unwrap();
            pipeline.push(PipelineEntry::new(header));
        }
    });

    let store = store_offset_request(
        replaced.client_id,
        1,
        ConsumerKind::ConsumerGroup,
        u32::try_from(GROUP_ID).unwrap(),
        0,
        AckLevel::Quorum,
    )
    .transmute_header(|mut header, next: &mut RoutedRequestHeader| {
        header.metadata_watermark = partition.required_metadata_frontier;
        header.owner_generation = replaced.generation;
        *next = header;
    });
    let (sender, receiver) = consensus::oneshot_channel();
    partition.on_request(store, Some(sender)).await;
    commit_log(&mut partition).await;

    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(partition.consensus.sequencer().current_sequence(), head);
    assert_eq!(
        receiver.await.unwrap().header().status,
        IggyError::TransientNotAccepted.as_code()
    );
    assert_eq!(
        partition.consumer_group_owner(GROUP_ID),
        Some(successor.owner)
    );
}

#[compio::test]
async fn given_pending_install_when_the_same_install_arrives_should_refuse_the_copy() {
    const GROUP_ID: u64 = 7;
    const CLIENT_ID: u128 = 42;
    let mut partition = test_partition();
    install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
    let installation = next_test_installation(&partition, GROUP_ID, CLIENT_ID + 1);
    partition
        .on_request(owner_install_request(&partition, &installation), None)
        .await;
    let install_op = partition.consensus.sequencer().current_sequence();
    assert!(partition.consumer_group_owner_admitted(&installation));
    let (sender, receiver) = consensus::oneshot_channel();
    partition
        .on_request(
            owner_install_request(&partition, &installation),
            Some(sender),
        )
        .await;
    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        install_op
    );
    assert_eq!(
        receiver.await.unwrap().header().status,
        IggyError::TransientNotAccepted.as_code()
    );

    partition.consensus.advance_commit_max(install_op);
    partition.commit_journal(&repair_config()).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(
        partition.consumer_group_owner(GROUP_ID),
        Some(installation.owner)
    );
    assert!(partition.pending_owner_install.get().is_none());
    assert!(partition.consumer_group_owner_admitted(&installation));
    assert_eq!(
        partition.installed_consumer_group_owner(&installation),
        Some(install_op)
    );
}

#[compio::test]
async fn given_shared_flag_when_fences_publish_should_raise_it() {
    const GROUP_ID: u64 = 7;
    const CLIENT_ID: u128 = 42;
    const METADATA_OP: u64 = 10;
    let config = repair_config();
    let mut partition = test_partition();
    let published = Rc::new(Cell::new(false));
    partition.set_fence_published_flag(Rc::clone(&published));

    let installation = next_test_installation(&partition, GROUP_ID, CLIENT_ID);
    partition
        .on_request(owner_install_request(&partition, &installation), None)
        .await;
    let install_op = partition.consensus.sequencer().current_sequence();
    assert!(!published.get(), "an admitted install is not published");
    partition.consensus.advance_commit_max(install_op);
    partition.commit_journal(&config).await;
    assert_eq!(
        partition.installed_consumer_group_owner(&installation),
        Some(install_op)
    );
    assert!(published.replace(false));

    let transition = TransitionPartitionHistoryRequest {
        incarnation: partition.created_revision(),
        metadata_op: METADATA_OP,
    };
    let fence = partition_control_request(
        &partition,
        Operation::TransitionPartitionHistory,
        &transition.to_bytes(),
        METADATA_OP,
    );
    partition.on_request(fence, None).await;
    let fence_op = partition.consensus.sequencer().current_sequence();
    assert!(!published.get(), "an admitted fence is not published");
    partition.consensus.advance_commit_max(fence_op);
    partition.commit_journal(&config).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(
        partition.installed_history_transition(&transition),
        Some(fence_op)
    );
    assert!(published.get());
}

#[compio::test]
async fn given_pending_install_when_a_send_arrives_should_admit_it() {
    const GROUP_ID: u64 = 7;
    const CLIENT_ID: u128 = 42;
    let mut partition = test_partition();
    install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
    let installation = next_test_installation(&partition, GROUP_ID, CLIENT_ID + 1);
    partition
        .on_request(owner_install_request(&partition, &installation), None)
        .await;
    let install_op = partition.consensus.sequencer().current_sequence();
    let mut send = checksumless_send_request(partition.namespace(), 1);
    send.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
        .copy_from_slice(&build_segment_record(partition.namespace(), 0));
    let send = send.transmute_header(|mut header, next: &mut RoutedRequestHeader| {
        header.metadata_watermark = partition.required_metadata_frontier;
        *next = header;
    });
    partition.on_request(send, None).await;
    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        install_op + 1
    );

    partition.consensus.advance_commit_max(install_op + 1);
    partition.commit_journal(&repair_config()).await;
    partition.commit_journal(&repair_config()).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(partition.consensus.commit_min(), install_op + 1);
    assert_eq!(
        partition.consumer_group_owner(GROUP_ID),
        Some(installation.owner)
    );
}

#[compio::test]
async fn given_send_committed_behind_install_when_the_install_publishes_should_prove_it() {
    const GROUP_ID: u64 = 7;
    const CLIENT_ID: u128 = 42;
    let config = repair_config();
    let mut partition = test_partition();
    let installation = next_test_installation(&partition, GROUP_ID, CLIENT_ID);
    partition
        .on_request(owner_install_request(&partition, &installation), None)
        .await;
    let install_op = partition.consensus.sequencer().current_sequence();
    let send = history_send_request(&partition, 1, partition.required_metadata_frontier);
    partition.on_request(send, None).await;
    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        install_op + 1
    );
    partition.consensus.advance_commit_max(install_op + 1);

    let selected = partition.select_persistable_commits(&config);
    assert!(partition.select_commit(selected, &config, true));
    loop {
        match partition.drive_commit().unwrap() {
            CommitStep::Progress => {}
            CommitStep::Publish => break,
            CommitStep::Ready | CommitStep::Pending => {
                panic!("an install-only walk needs no I/O before it publishes")
            }
        }
    }
    assert_eq!(
        partition.consumer_group_owner(GROUP_ID),
        Some(installation.owner)
    );
    assert_eq!(partition.consensus.commit_min(), install_op - 1);
    assert_eq!(
        partition.installed_consumer_group_owner(&installation),
        None,
        "an applied install proves nothing before its walk publishes"
    );

    partition.commit_journal(&config).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(partition.consensus.commit_min(), install_op);
    assert_eq!(partition.consensus.commit_max(), install_op + 1);
    assert_eq!(
        partition.installed_consumer_group_owner(&installation),
        Some(install_op),
        "a published install must not wait for later writes to apply"
    );
}

#[compio::test]
async fn given_full_queues_when_an_install_is_refused_should_not_block_its_successor() {
    const GROUP_ID: u64 = 8;
    const CLIENT_ID: u128 = 42;
    const CONSUMER_IDS: [u32; 2] = [7, 9];
    let (mut partition, _) =
        recording_partition_with_pipeline(0, 3, LocalPipeline::with_capacities(1, 1));
    install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
    partition.stats.increment_messages_count(1);
    for (client_id, consumer_id) in (100..).zip(CONSUMER_IDS) {
        let store = store_offset_request(
            client_id,
            1,
            ConsumerKind::Consumer,
            consumer_id,
            0,
            AckLevel::Quorum,
        )
        .transmute_header(|mut header, next: &mut RoutedRequestHeader| {
            header.metadata_watermark = partition.required_metadata_frontier;
            *next = header;
        });
        partition.on_request(store, None).await;
    }
    assert_eq!(partition.consensus.request_queue_len(), 1);

    let refused = next_test_installation(&partition, GROUP_ID, CLIENT_ID + 1);
    let (sender, receiver) = consensus::oneshot_channel();
    partition
        .on_request(owner_install_request(&partition, &refused), Some(sender))
        .await;
    assert_eq!(
        receiver.await.unwrap().header().status,
        IggyError::TransientNotAccepted.as_code()
    );
    assert!(partition.pending_owner_install.get().is_none());

    drop(partition.consensus.pop_queued_request());
    // Metadata replaced the refused install, so only its successor arrives.
    let successor = InstallConsumerGroupOwnerRequest {
        owner: ConsumerGroupOwner {
            client_id: CLIENT_ID + 2,
            session: 1,
            generation: refused.owner.generation + 1,
        },
        metadata_op: refused.metadata_op + 1,
        ..refused
    };
    partition
        .on_request(owner_install_request(&partition, &successor), None)
        .await;
    assert_eq!(partition.consensus.request_queue_len(), 1);
    assert_eq!(partition.pending_owner_install.get(), Some(successor));
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
}

#[compio::test]
async fn given_owner_capacity_when_installing_new_group_should_refuse_before_commit() {
    let mut partition = test_partition();
    let incarnation = partition.created_revision();
    for group_id in 0..u64::from(crate::state_transfer::CONSUMER_OFFSETS_ENTRIES_MAX) {
        partition.consumer_group_owners.insert(
            group_id,
            InstalledConsumerGroupOwner {
                installation: InstallConsumerGroupOwnerRequest {
                    incarnation,
                    group_id,
                    owner: ConsumerGroupOwner {
                        client_id: 0,
                        session: 0,
                        generation: 1,
                    },
                    metadata_op: 1,
                },
                partition_op: 1,
            },
        );
    }
    let mut installation = InstallConsumerGroupOwnerRequest {
        incarnation,
        group_id: u64::from(crate::state_transfer::CONSUMER_OFFSETS_ENTRIES_MAX),
        owner: ConsumerGroupOwner {
            client_id: 0,
            session: 0,
            generation: 2,
        },
        metadata_op: 2,
    };
    let (sender, receiver) = consensus::oneshot_channel();
    partition
        .on_request(
            partition_control_request(
                &partition,
                Operation::InstallConsumerGroupOwner,
                &installation.to_bytes(),
                installation.metadata_op,
            ),
            Some(sender),
        )
        .await;
    assert_eq!(
        receiver.await.unwrap().header().status,
        IggyError::TransientNotAccepted.as_code()
    );
    assert_eq!(partition.consensus.sequencer().current_sequence(), 0);
    assert!(partition.pending_owner_install.get().is_none());
    assert!(partition.fatal().is_none());
    installation.group_id = 0;
    partition
        .on_request(
            partition_control_request(
                &partition,
                Operation::InstallConsumerGroupOwner,
                &installation.to_bytes(),
                installation.metadata_op,
            ),
            None,
        )
        .await;
    assert_eq!(partition.consensus.sequencer().current_sequence(), 1);
    partition.consensus.advance_commit_max(1);
    partition.commit_journal(&repair_config()).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(partition.consumer_group_owner(0), Some(installation.owner));
}

/// A user revocation can decide two owners at one op, and a reader can
/// install the first. The second must fail admission, not its commit.
#[compio::test]
async fn given_two_owner_decisions_in_one_op_when_the_second_install_arrives_should_refuse_at_admission()
 {
    const GROUP_ID: u64 = 7;
    const DECISION_OP: u64 = 5;
    let mut partition = test_partition();
    let config = repair_config();
    let incarnation = partition.created_revision();
    let installation = |client_id, generation| InstallConsumerGroupOwnerRequest {
        incarnation,
        group_id: GROUP_ID,
        owner: ConsumerGroupOwner {
            client_id,
            session: 1,
            generation,
        },
        metadata_op: generation,
    };
    let installed = installation(42, DECISION_OP);
    partition
        .on_request(owner_install_request(&partition, &installed), None)
        .await;
    assert_eq!(partition.consensus.sequencer().current_sequence(), 1);
    partition.consensus.advance_commit_max(1);
    partition.commit_journal(&config).await;
    assert_eq!(
        partition.consumer_group_owner(GROUP_ID),
        Some(installed.owner)
    );

    let (sender, receiver) = consensus::oneshot_channel();
    partition
        .on_request(
            owner_install_request(&partition, &installation(43, DECISION_OP)),
            Some(sender),
        )
        .await;
    // A prepared install answers only at its commit, so check the
    // sequencer before awaiting the reply.
    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        1,
        "a conflicting install must not be prepared"
    );
    assert_eq!(
        receiver.await.unwrap().header().status,
        IggyError::InvalidCommand.as_code()
    );
    assert!(partition.pending_owner_install.get().is_none());
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(
        partition.consumer_group_owner(GROUP_ID),
        Some(installed.owner)
    );

    let higher = installation(45, DECISION_OP + 1);
    for (admitted, owner) in [
        (installation(44, DECISION_OP - 1), installed.owner),
        (installed, installed.owner),
        (higher, higher.owner),
    ] {
        let op = partition.consensus.sequencer().current_sequence()
            + u64::from(admitted.owner.generation > installed.owner.generation);
        partition
            .on_request(owner_install_request(&partition, &admitted), None)
            .await;
        assert_eq!(
            partition.consensus.sequencer().current_sequence(),
            op,
            "{admitted:?}"
        );
        partition.consensus.advance_commit_max(op);
        partition.commit_journal(&config).await;
        assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
        assert_eq!(
            partition.consumer_group_owner(GROUP_ID),
            Some(owner),
            "{admitted:?}"
        );
    }
}
#[compio::test]
async fn given_superseded_or_applied_owner_installs_when_resubmitted_should_not_consume_ops() {
    const GROUP_ID: u64 = 7;
    const FIRST_CLIENT: u128 = 42;
    let mut partition = test_partition();
    install_test_owner(&mut partition, GROUP_ID, FIRST_CLIENT).await;
    let stale = partition.consumer_group_owners[&GROUP_ID].installation;
    let installed_op = install_test_owner(&mut partition, GROUP_ID, FIRST_CLIENT + 1).await;
    let installed = partition.consumer_group_owners[&GROUP_ID].installation;
    for installation in [stale, installed] {
        let (sender, receiver) = consensus::oneshot_channel();
        partition
            .on_request(
                owner_install_request(&partition, &installation),
                Some(sender),
            )
            .await;
        assert_eq!(
            partition.consensus.sequencer().current_sequence(),
            installed_op,
            "an applied or superseded installation must not consume a log entry"
        );
        assert_eq!(
            receiver.await.unwrap().header().status,
            IggyError::TransientNotAccepted.as_code()
        );
        assert!(partition.consumer_group_owner_admitted(&installation));
        assert!(partition.pending_owner_install.get().is_none());
        assert_eq!(
            partition.consumer_group_owner(GROUP_ID),
            Some(installed.owner)
        );
    }
    let foreign = InstallConsumerGroupOwnerRequest {
        incarnation: installed.incarnation + 1,
        ..stale
    };
    assert!(!partition.consumer_group_owner_admitted(&foreign));
    install_test_owner(&mut partition, GROUP_ID, FIRST_CLIENT + 2).await;
    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        installed_op + 1
    );
}
#[compio::test]
async fn given_confirmed_group_deletion_when_retired_and_restarted_should_reject_late_owner() {
    const LIVE_GROUP_ID: u64 = 1;
    const DELETED_GROUP_ID: u64 = 2;
    const CLIENT_ID: u128 = 42;
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir(directory.path().join("prepares-0")).unwrap();
    let mut partition = test_partition();
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    install_test_owner(&mut partition, LIVE_GROUP_ID, CLIENT_ID).await;
    install_test_owner(&mut partition, DELETED_GROUP_ID, CLIENT_ID).await;
    let late = partition.consumer_group_owners[&DELETED_GROUP_ID].installation;
    let mut unassigned = late;
    unassigned.owner = ConsumerGroupOwner {
        client_id: 0,
        session: 0,
        generation: late.owner.generation + 1,
    };
    unassigned.metadata_op = partition.required_metadata_frontier + 1;
    let request = partition_control_request(
        &partition,
        Operation::InstallConsumerGroupOwner,
        &unassigned.to_bytes(),
        unassigned.metadata_op,
    );
    partition.on_request(request, None).await;
    partition
        .consensus
        .advance_commit_max(partition.consensus.sequencer().current_sequence());
    partition.commit_journal(&repair_config()).await;
    let catalog = RetireConsumerGroupOwnersRequest {
        incarnation: partition.created_revision(),
        metadata_op: partition.required_metadata_frontier + 1,
        next_group_id: DELETED_GROUP_ID + 1,
        live_group_ids: vec![LIVE_GROUP_ID],
    };
    let request = partition_control_request(
        &partition,
        Operation::RetireConsumerGroupOwners,
        &catalog.to_bytes(),
        catalog.metadata_op,
    );
    partition.on_request(request, None).await;
    let request = partition_control_request(
        &partition,
        Operation::InstallConsumerGroupOwner,
        &late.to_bytes(),
        late.metadata_op,
    );
    partition.on_request(request, None).await;
    let checkpoint = partition.consensus.sequencer().current_sequence();
    partition.consensus.advance_commit_max(checkpoint);
    partition.commit_journal(&repair_config()).await;
    settle_partition_io(&mut partition, &repair_config()).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(partition.consumer_group_owner(DELETED_GROUP_ID), None);
    assert_eq!(partition.consumer_group_owners.len(), 1);
    partition
        .persist_retry_checkpoint(checkpoint)
        .await
        .unwrap();
    drop(partition);
    let mut recovered = test_partition();
    recovered.set_partition_dir(directory.path().to_string_lossy().into_owned());
    recovered
        .restore_retry_checkpoint(checkpoint)
        .await
        .unwrap();
    recovered
        .consensus
        .restore_commit_state(checkpoint, checkpoint);
    recovered.consensus.sequencer().set_sequence(checkpoint);
    assert_eq!(recovered.owner_retirement, Some(catalog));
    assert_eq!(recovered.consumer_group_owner(DELETED_GROUP_ID), None);
    assert!(recovered.consumer_group_owner(LIVE_GROUP_ID).is_some());
    let request = partition_control_request(
        &recovered,
        Operation::InstallConsumerGroupOwner,
        &late.to_bytes(),
        late.metadata_op,
    );
    recovered.on_request(request, None).await;
    assert_eq!(
        recovered.consensus.sequencer().current_sequence(),
        checkpoint
    );
    install_test_owner(&mut recovered, DELETED_GROUP_ID + 1, CLIENT_ID).await;
    assert_eq!(recovered.consumer_group_owners.len(), 2);
}
#[compio::test]
async fn given_retired_owner_with_group_offset_when_catalog_arrives_should_refuse_until_offset_is_deleted()
 {
    const GROUP_ID: u64 = 7;
    const CLIENT_ID: u128 = 42;
    const STORED_OFFSET: u64 = 7;
    let mut partition = test_partition();
    install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
    let fence = install_unassigned_test_owner(&mut partition, GROUP_ID).await;
    let offset_key = u32::try_from(GROUP_ID).unwrap();
    partition.seed_recovered_consumer_offset(
        ConsumerKind::ConsumerGroup,
        offset_key,
        STORED_OFFSET,
        STORED_OFFSET,
    );
    let catalog = RetireConsumerGroupOwnersRequest {
        incarnation: partition.created_revision(),
        metadata_op: partition.required_metadata_frontier + 1,
        next_group_id: GROUP_ID + 1,
        live_group_ids: Vec::new(),
    };
    let head = partition.consensus.sequencer().current_sequence();

    let (sender, receiver) = consensus::oneshot_channel();
    partition
        .on_request(owner_retirement_request(&partition, &catalog), Some(sender))
        .await;
    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        head,
        "a catalog must wait while a group it retires has an offset"
    );
    assert_eq!(
        receiver.await.unwrap().header().status,
        IggyError::TransientNotAccepted.as_code()
    );

    partition
        .on_request(fenced_group_offset_delete(&fence), None)
        .await;
    commit_log(&mut partition).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert!(
        !partition
            .durable_consumer_offsets
            .contains(ConsumerKind::ConsumerGroup, offset_key)
    );

    partition
        .on_request(owner_retirement_request(&partition, &catalog), None)
        .await;
    assert_eq!(partition.consensus.sequencer().current_sequence(), head + 2);
    commit_log(&mut partition).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert!(!partition.consumer_group_owners.contains_key(&GROUP_ID));
    assert_eq!(partition.owner_retirement, Some(catalog));
}

/// A boot loads offset files that can be newer than its checkpoint, so two
/// replicas can apply one catalog with different offsets.
#[test]
fn given_replicas_with_different_offset_files_when_catalog_applies_should_keep_identical_owners() {
    const LIVE_GROUP_ID: u64 = 1;
    const RETIRED_GROUP_ID: u64 = 2;
    const STORED_OFFSET: u64 = 7;
    let mut with_offset = test_partition();
    let mut without_offset = test_partition();
    let live = InstallConsumerGroupOwnerRequest {
        incarnation: with_offset.created_revision(),
        group_id: LIVE_GROUP_ID,
        owner: ConsumerGroupOwner {
            client_id: 42,
            session: 1,
            generation: 1,
        },
        metadata_op: 1,
    };
    let retired = InstallConsumerGroupOwnerRequest {
        group_id: RETIRED_GROUP_ID,
        owner: ConsumerGroupOwner {
            client_id: 0,
            session: 0,
            generation: 2,
        },
        metadata_op: 2,
        ..live
    };
    for (partition_op, installation) in (1..).zip([live, retired]) {
        for replica in [&mut with_offset, &mut without_offset] {
            replica
                .apply_consumer_group_owner(installation, partition_op)
                .unwrap();
        }
    }
    with_offset.seed_recovered_consumer_offset(
        ConsumerKind::ConsumerGroup,
        u32::try_from(RETIRED_GROUP_ID).unwrap(),
        STORED_OFFSET,
        STORED_OFFSET,
    );
    let catalog = RetireConsumerGroupOwnersRequest {
        incarnation: live.incarnation,
        metadata_op: retired.metadata_op + 1,
        next_group_id: RETIRED_GROUP_ID + 1,
        live_group_ids: vec![LIVE_GROUP_ID],
    };

    for replica in [&mut with_offset, &mut without_offset] {
        replica
            .apply_consumer_group_owner_retirement(catalog.clone())
            .unwrap();
    }
    let owners = [&with_offset, &without_offset].map(|replica| {
        replica
            .consumer_group_owners
            .keys()
            .copied()
            .collect::<Vec<_>>()
    });
    assert_eq!(owners[0], owners[1], "replicas diverged on one catalog");
    assert_eq!(owners[0], [LIVE_GROUP_ID]);
}

/// A failed `NoAck` write strands a durable group offset. The reconciler
/// sends no delete for it until a restart, a transfer or a later write
/// clears the mark, and the other deleted groups must not wait for that.
#[compio::test]
async fn given_stranded_offset_of_deleted_group_when_retiring_should_keep_it_until_the_mark_clears()
{
    const STRANDED_GROUP_ID: u64 = 7;
    const FREE_GROUP_ID: u64 = 8;
    const CLIENT_ID: u128 = 42;
    const STORED_OFFSET: u64 = 7;
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir(directory.path().join("prepares-0")).unwrap();
    let mut partition = test_partition();
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    install_test_owner(&mut partition, STRANDED_GROUP_ID, CLIENT_ID).await;
    install_test_owner(&mut partition, FREE_GROUP_ID, CLIENT_ID).await;
    let offset_key = u32::try_from(STRANDED_GROUP_ID).unwrap();
    partition.seed_recovered_consumer_offset(
        ConsumerKind::ConsumerGroup,
        offset_key,
        STORED_OFFSET,
        STORED_OFFSET,
    );
    partition
        .consumer_offset_capacity_for(ConsumerKind::ConsumerGroup)
        .record_stranded(offset_key);
    let fence = install_unassigned_test_owner(&mut partition, STRANDED_GROUP_ID).await;
    install_unassigned_test_owner(&mut partition, FREE_GROUP_ID).await;
    let is_live = |group_id| group_id > FREE_GROUP_ID;

    assert_eq!(
        partition.dead_group_offset_keys(is_live),
        Vec::<(ConsumerKind, u32)>::new()
    );
    assert_eq!(
        partition.retirement_kept_groups(is_live),
        Some(vec![STRANDED_GROUP_ID]),
        "the free group must not wait for a stranded offset"
    );
    let keeping = RetireConsumerGroupOwnersRequest {
        incarnation: partition.created_revision(),
        metadata_op: partition.required_metadata_frontier + 1,
        next_group_id: FREE_GROUP_ID + 1,
        live_group_ids: vec![STRANDED_GROUP_ID],
    };
    partition
        .on_request(owner_retirement_request(&partition, &keeping), None)
        .await;
    commit_log(&mut partition).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(
        partition
            .consumer_group_owners
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        [STRANDED_GROUP_ID]
    );
    let checkpoint = partition.consensus.commit_min();
    partition
        .persist_retry_checkpoint(checkpoint)
        .await
        .unwrap();
    drop(partition);

    // The restart loads the offset file again, with no stranded mark.
    let mut recovered = test_partition();
    recovered.set_partition_dir(directory.path().to_string_lossy().into_owned());
    recovered
        .restore_retry_checkpoint(checkpoint)
        .await
        .unwrap();
    recovered
        .consensus
        .restore_commit_state(checkpoint, checkpoint);
    recovered.consensus.sequencer().set_sequence(checkpoint);
    recovered.seed_recovered_consumer_offset(
        ConsumerKind::ConsumerGroup,
        offset_key,
        STORED_OFFSET,
        STORED_OFFSET,
    );
    assert_eq!(recovered.retirement_kept_groups(is_live), None);
    assert_eq!(
        recovered.dead_group_offset_keys(is_live),
        [(ConsumerKind::ConsumerGroup, offset_key)]
    );
    recovered
        .on_request(fenced_group_offset_delete(&fence), None)
        .await;
    commit_log(&mut recovered).await;
    assert_eq!(recovered.retirement_kept_groups(is_live), Some(Vec::new()));
    let retiring = RetireConsumerGroupOwnersRequest {
        live_group_ids: Vec::new(),
        ..keeping
    };
    recovered
        .on_request(owner_retirement_request(&recovered, &retiring), None)
        .await;
    commit_log(&mut recovered).await;
    assert!(recovered.fatal().is_none(), "{:?}", recovered.fatal());
    assert!(recovered.consumer_group_owners.is_empty());
    assert_eq!(recovered.owner_retirement, Some(retiring));
}

/// A boot that cannot read a group offset file, or a `NoAck` delete whose
/// directory sync fails, strands the key with no durable entry. The group
/// retires at once, and when a later boot loads that file, the cleanup of
/// a retired group deletes it with no owner row.
#[compio::test]
async fn given_unloaded_offset_of_deleted_group_when_a_later_boot_loads_it_should_stay_deletable() {
    const UNREAD_GROUP_ID: u64 = 7;
    const UNSYNCED_GROUP_ID: u64 = 8;
    const FREE_GROUP_ID: u64 = 9;
    const CLIENT_ID: u128 = 42;
    const STORED_OFFSET: u64 = 7;
    const STRANDED_GROUP_IDS: [u64; 2] = [UNREAD_GROUP_ID, UNSYNCED_GROUP_ID];
    const DELETED_GROUP_IDS: [u64; 3] = [UNREAD_GROUP_ID, UNSYNCED_GROUP_ID, FREE_GROUP_ID];
    let kind = ConsumerKind::ConsumerGroup;
    let keys = STRANDED_GROUP_IDS.map(|group_id| u32::try_from(group_id).unwrap());
    let [unread_key, unsynced_key] = keys;
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir(directory.path().join("prepares-0")).unwrap();
    let group_offsets = directory.path().join("groups");
    std::fs::create_dir(&group_offsets).unwrap();
    let mut partition = test_partition();
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.consumer_offset_dirs[kind.index()] =
        Some(group_offsets.to_string_lossy().into_owned());
    partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Persisted;
    assert!(partition.seed_stranded_consumer_offset(kind, unread_key));
    for group_id in DELETED_GROUP_IDS {
        install_test_owner(&mut partition, group_id, CLIENT_ID).await;
    }
    let stored = PendingConsumerOffsetCommit::upsert(kind, unsynced_key, STORED_OFFSET);
    partition
        .persist_consumer_offset_commit(stored)
        .await
        .unwrap();
    partition.apply_consumer_offset_commit(stored);
    partition
        .consumer_offset_dir_sync_fault
        .set(Some(kind.index()));
    let delete = delete_offset_request_with_ack(CLIENT_ID, 1, kind, unsynced_key, AckLevel::NoAck);
    partition
        .apply_consumer_offset_no_ack(Box::new(*delete.header()), kind, unsynced_key, None, None)
        .await;
    partition.consumer_offset_dir_sync_fault.set(None);
    assert!(!group_offsets.join(unsynced_key.to_string()).exists());
    for key in keys {
        assert!(!partition.durable_consumer_offsets.contains(kind, key));
        assert!(
            partition
                .consumer_offset_capacity_for(kind)
                .is_stranded(key)
        );
    }
    for group_id in DELETED_GROUP_IDS {
        install_unassigned_test_owner(&mut partition, group_id).await;
    }
    let is_live = |group_id| group_id > FREE_GROUP_ID;

    assert_eq!(
        partition.retirement_kept_groups(is_live),
        Some(Vec::new()),
        "a stranded key with no durable entry must not keep its deleted group"
    );
    let retiring = RetireConsumerGroupOwnersRequest {
        incarnation: partition.created_revision(),
        metadata_op: partition.required_metadata_frontier + 1,
        next_group_id: FREE_GROUP_ID + 1,
        live_group_ids: Vec::new(),
    };
    partition
        .on_request(owner_retirement_request(&partition, &retiring), None)
        .await;
    commit_log(&mut partition).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert!(partition.consumer_group_owners.is_empty());
    let checkpoint = partition.consensus.commit_min();
    partition
        .persist_retry_checkpoint(checkpoint)
        .await
        .unwrap();
    drop(partition);

    // The later boot reads the first file, and a crash undid the unsynced
    // unlink of the second.
    let mut recovered = test_partition();
    recovered.set_partition_dir(directory.path().to_string_lossy().into_owned());
    recovered
        .restore_retry_checkpoint(checkpoint)
        .await
        .unwrap();
    recovered
        .consensus
        .restore_commit_state(checkpoint, checkpoint);
    recovered.consensus.sequencer().set_sequence(checkpoint);
    for key in keys {
        recovered.seed_recovered_consumer_offset(kind, key, STORED_OFFSET, STORED_OFFSET);
    }
    assert!(recovered.consumer_group_owners.is_empty());
    assert_eq!(recovered.owner_retirement, Some(retiring));
    assert_eq!(
        recovered.dead_group_offset_keys(is_live),
        keys.map(|key| (kind, key))
    );
    for key in keys {
        let cleanup =
            delete_offset_request_with_ack(AUTO_COMMIT_CLIENT_ID, 1, kind, key, AckLevel::Quorum);
        recovered.on_request(cleanup, None).await;
    }
    commit_log(&mut recovered).await;
    assert!(recovered.fatal().is_none(), "{:?}", recovered.fatal());
    assert_eq!(
        recovered.durable_consumer_offset_count(kind),
        0,
        "the cleanup must delete the offsets of retired groups"
    );
}

/// The old primary had no copy of the group offset and prepared a catalog
/// that retires the group. This replica holds a copy, leads the next view
/// before that catalog applies, and admits the delete under the fence. The
/// catalog then commits first and prunes the row.
#[compio::test]
async fn given_unapplied_catalog_retiring_group_when_its_durable_offset_is_deleted_should_not_fence()
 {
    const GROUP_ID: u64 = 7;
    const CLIENT_ID: u128 = 42;
    const STORED_OFFSET: u64 = 7;
    let kind = ConsumerKind::ConsumerGroup;
    let key = u32::try_from(GROUP_ID).unwrap();
    let mut partition = test_partition();
    install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
    let fence = install_unassigned_test_owner(&mut partition, GROUP_ID).await;
    partition.seed_recovered_consumer_offset(kind, key, STORED_OFFSET, STORED_OFFSET);
    let catalog = RetireConsumerGroupOwnersRequest {
        incarnation: partition.created_revision(),
        metadata_op: partition.required_metadata_frontier + 1,
        next_group_id: GROUP_ID + 1,
        live_group_ids: Vec::new(),
    };
    let prepare = partition.pipeline_local_request(
        owner_retirement_request(&partition, &catalog),
        None,
        None,
    );
    let catalog_op = prepare.header().op;
    partition.on_replicate(prepare).await;
    partition.consensus.set_view(1);

    partition
        .on_request(fenced_group_offset_delete(&fence), None)
        .await;
    let delete_op = partition.consensus.sequencer().current_sequence();
    assert_eq!(
        delete_op,
        catalog_op + 1,
        "the delete must be prepared behind the catalog"
    );
    commit_log(&mut partition).await;

    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(partition.consensus.commit_min(), delete_op);
    assert!(!partition.durable_consumer_offsets.contains(kind, key));
    assert_eq!(partition.owner_retirement, Some(catalog));
}

/// A group with no owner row passes the owner check only for the
/// auto-commit delete of its offset, and only once the installed catalog
/// retires the group.
#[compio::test]
async fn given_retired_group_without_row_when_other_requests_arrive_should_refuse() {
    const GROUP_ID: u64 = 7;
    const UNRETIRED_KEY: u32 = 9;
    const CLIENT_ID: u128 = 42;
    let kind = ConsumerKind::ConsumerGroup;
    let key = u32::try_from(GROUP_ID).unwrap();
    let mut partition = test_partition();
    partition.stats.increment_messages_count(1);
    install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
    install_unassigned_test_owner(&mut partition, GROUP_ID).await;
    let catalog = RetireConsumerGroupOwnersRequest {
        incarnation: partition.created_revision(),
        metadata_op: partition.required_metadata_frontier + 1,
        next_group_id: GROUP_ID + 1,
        live_group_ids: Vec::new(),
    };
    partition
        .on_request(owner_retirement_request(&partition, &catalog), None)
        .await;
    commit_log(&mut partition).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert!(partition.consumer_group_retired(GROUP_ID));
    assert!(!partition.consumer_group_retired(u64::from(UNRETIRED_KEY)));
    partition.seed_recovered_consumer_offset(kind, key, 0, 0);
    partition.seed_recovered_consumer_offset(kind, UNRETIRED_KEY, 0, 0);
    let frontier = partition.required_metadata_frontier;
    let head = partition.consensus.sequencer().current_sequence();

    // A current watermark keeps the frontier check from refusing it first.
    let client_delete = delete_offset_request_with_ack(CLIENT_ID, 1, kind, key, AckLevel::Quorum)
        .transmute_header(|mut header, next: &mut RoutedRequestHeader| {
            header.metadata_watermark = frontier;
            *next = header;
        });
    partition.on_request(client_delete, None).await;
    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        head,
        "a client delete of a retired group must be refused"
    );
    partition
        .on_request(
            store_offset_request(AUTO_COMMIT_CLIENT_ID, 2, kind, key, 0, AckLevel::Quorum),
            None,
        )
        .await;
    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        head,
        "a store of a retired group must be refused"
    );
    partition
        .on_request(
            delete_offset_request_with_ack(
                AUTO_COMMIT_CLIENT_ID,
                3,
                kind,
                UNRETIRED_KEY,
                AckLevel::Quorum,
            ),
            None,
        )
        .await;
    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        head,
        "a delete for a group that has no row and no retiring catalog must be refused"
    );
    partition
        .on_request(
            delete_offset_request_with_ack(AUTO_COMMIT_CLIENT_ID, 4, kind, key, AckLevel::Quorum),
            None,
        )
        .await;
    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        head + 1,
        "the cleanup delete of a retired group passes"
    );
}

/// Each primary keeps the deleted groups it saw with a durable stranded
/// offset. So after a view change, a catalog can keep a group that an
/// earlier catalog in the log retired.
#[test]
fn given_catalog_keeping_a_retired_group_when_applied_should_keep_it_retired() {
    const LIVE_GROUP_ID: u64 = 1;
    const RETIRED_GROUP_ID: u64 = 2;
    let mut partition = test_partition();
    let live = InstallConsumerGroupOwnerRequest {
        incarnation: partition.created_revision(),
        group_id: LIVE_GROUP_ID,
        owner: ConsumerGroupOwner {
            client_id: 42,
            session: 1,
            generation: 1,
        },
        metadata_op: 1,
    };
    let retired = InstallConsumerGroupOwnerRequest {
        group_id: RETIRED_GROUP_ID,
        owner: ConsumerGroupOwner {
            client_id: 0,
            session: 0,
            generation: 2,
        },
        metadata_op: 2,
        ..live
    };
    for (partition_op, installation) in (1..).zip([live, retired]) {
        partition
            .apply_consumer_group_owner(installation, partition_op)
            .unwrap();
    }
    let retiring = RetireConsumerGroupOwnersRequest {
        incarnation: live.incarnation,
        metadata_op: retired.metadata_op + 1,
        next_group_id: RETIRED_GROUP_ID + 1,
        live_group_ids: vec![LIVE_GROUP_ID],
    };
    let keeping = RetireConsumerGroupOwnersRequest {
        live_group_ids: vec![LIVE_GROUP_ID, RETIRED_GROUP_ID],
        ..retiring
    };
    let later = RetireConsumerGroupOwnersRequest {
        metadata_op: retiring.metadata_op + 1,
        ..keeping.clone()
    };

    for catalog in [retiring, keeping, later] {
        let applied = partition.apply_consumer_group_owner_retirement(catalog);
        assert!(
            applied.is_ok(),
            "{applied:?}: a catalog that keeps a retired group must not fence the partition"
        );
    }
    partition.apply_consumer_group_owner(retired, 3).unwrap();
    assert_eq!(
        partition
            .consumer_group_owners
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        [LIVE_GROUP_ID],
        "a retired group stays retired"
    );
}

#[test]
fn given_a_parked_poll_when_a_group_owner_is_installed_or_retired_should_notify_it() {
    // A parked group read learned that it lost its partition only from a
    // maintenance sweep, so a fetch over many partitions waited for it.
    let mut partition = test_partition();
    let notified = Rc::new(Cell::new(0_u32));
    let counter = Rc::clone(&notified);
    partition.set_poll_notifier(Some(Rc::new(move || counter.set(counter.get() + 1))));
    let install = InstallConsumerGroupOwnerRequest {
        incarnation: partition.created_revision(),
        group_id: 7,
        owner: ConsumerGroupOwner {
            client_id: 1,
            session: 1,
            generation: 1,
        },
        metadata_op: 1,
    };
    partition
        .apply_consumer_group_owner(install, 1)
        .expect("the owner installs");
    assert_eq!(
        notified.get(),
        1,
        "an installed owner wakes the parked reads"
    );
    partition
        .apply_consumer_group_owner(
            InstallConsumerGroupOwnerRequest {
                owner: ConsumerGroupOwner {
                    client_id: 0,
                    session: 0,
                    generation: 2,
                },
                metadata_op: install.metadata_op + 1,
                ..install
            },
            2,
        )
        .expect("the unassigned owner installs");
    partition
        .apply_consumer_group_owner_retirement(RetireConsumerGroupOwnersRequest {
            incarnation: install.incarnation,
            metadata_op: install.metadata_op + 2,
            next_group_id: install.group_id + 1,
            live_group_ids: Vec::new(),
        })
        .expect("the retirement applies");
    assert_eq!(
        notified.get(),
        3,
        "each owner change wakes the parked reads"
    );
}

//     #[test]
//     fn given_recorded_last_polled_when_retirement_prunes_the_group_should_clear_it() {
//         const GROUP_ID: u32 = 7;
//         const POLLED_OFFSET: u64 = 3;
//         let mut partition = test_partition();
//         let fence = InstallConsumerGroupOwnerRequest {
//             incarnation: partition.created_revision(),
//             group_id: u64::from(GROUP_ID),
//             owner: ConsumerGroupOwner {
//                 client_id: 0,
//                 session: 0,
//                 generation: 1,
//             },
//             metadata_op: 1,
//         };
//         partition.apply_consumer_group_owner(fence, 1).unwrap();
//         // The fence install clears the entry, so the test seeds it after.
//         partition.last_polled_offsets.pin().insert(
//             ConsumerGroupId(GROUP_ID as usize),
//             ConsumerOffset::new(
//                 ConsumerKind::ConsumerGroup,
//                 GROUP_ID,
//                 POLLED_OFFSET,
//                 String::new(),
//             ),
//         );
//         let catalog = RetireConsumerGroupOwnersRequest {
//             incarnation: fence.incarnation,
//             metadata_op: fence.metadata_op + 1,
//             next_group_id: fence.group_id + 1,
//             live_group_ids: Vec::new(),
//         };
//
//         partition
//             .apply_consumer_group_owner_retirement(catalog)
//             .unwrap();
//
//         assert!(partition.consumer_group_owners.is_empty());
//         assert_eq!(
//             partition.group_offset_state(fence.group_id).0,
//             None,
//             "a retired group must not keep its last polled offset"
//         );
//     }

// A replay starts from the owners of an older checkpoint, and the offset
// files can already lack the offset that a later delete removed. A catalog
// committed ahead of that delete would then prune the owner it needs.
//     #[compio::test]
//     async fn given_retirement_racing_offset_cleanup_when_replayed_from_older_checkpoint_should_not_fence_partition()
//      {
//         const GROUP_ID: u64 = 7;
//         const CLIENT_ID: u128 = 42;
//         const STORED_OFFSET: u64 = 7;
//         let directory = tempfile::tempdir().unwrap();
//         std::fs::create_dir(directory.path().join("prepares-0")).unwrap();
//         let mut partition = test_partition();
//         partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
//         install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
//         let fence = install_unassigned_test_owner(&mut partition, GROUP_ID).await;
//         let checkpoint = partition.consensus.commit_min();
//         partition
//             .persist_retry_checkpoint(checkpoint)
//             .await
//             .unwrap();
//         partition.seed_recovered_consumer_offset(
//             ConsumerKind::ConsumerGroup,
//             u32::try_from(GROUP_ID).unwrap(),
//             STORED_OFFSET,
//             STORED_OFFSET,
//         );
//         let catalog = RetireConsumerGroupOwnersRequest {
//             incarnation: partition.created_revision(),
//             metadata_op: partition.required_metadata_frontier + 1,
//             next_group_id: GROUP_ID + 1,
//             live_group_ids: Vec::new(),
//         };
//         // The reconciler can send the catalog in the pass that deletes the
//         // offset, and again in a later pass.
//         let passes = [
//             vec![
//                 owner_retirement_request(&partition, &catalog),
//                 fenced_group_offset_delete(&fence),
//             ],
//             vec![owner_retirement_request(&partition, &catalog)],
//         ];
//         let mut tail = Vec::new();
//         for pass in passes {
//             for request in pass {
//                 partition.on_request(request, None).await;
//             }
//             // The commit walk evicts what it applied, so take the tail first.
//             let first = checkpoint + 1 + tail.len() as u64;
//             for op in first..=partition.consensus.sequencer().current_sequence() {
//                 let journaled = partition.log.journal().inner.repair_entry(op).unwrap();
//                 tail.push(
//                     Message::<PrepareHeader>::try_from(Owned::copy_from_slice(
//                         journaled.as_slice(),
//                     ))
//                     .unwrap(),
//                 );
//             }
//             commit_log(&mut partition).await;
//             assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
//         }
//         let head = partition.consensus.commit_min();
//         assert_eq!(head, checkpoint + tail.len() as u64);
//         drop(partition);
//
//         let mut recovered = test_partition();
//         recovered.set_partition_dir(directory.path().to_string_lossy().into_owned());
//         recovered
//             .restore_retry_checkpoint(checkpoint)
//             .await
//             .unwrap();
//         for prepare in tail {
//             recovered.apply_replicated_operation(prepare).await.unwrap();
//         }
//         recovered.consensus.sequencer().set_sequence(head);
//         recovered.consensus.restore_commit_state(checkpoint, head);
//         commit_log(&mut recovered).await;
//         assert!(recovered.fatal().is_none(), "{:?}", recovered.fatal());
//         assert_eq!(recovered.consensus.commit_min(), head);
//         assert_eq!(recovered.consumer_group_owner(GROUP_ID), None);
//         assert_eq!(recovered.owner_retirement, Some(catalog));
//     }

/// An install for a group that a catalog retires assigns the row again
/// when it commits ahead of the catalog, and the catalog apply then fences
/// the partition. After a view change the latch names only the last
/// install of the log, so the barrier must hold the catalog too.
#[compio::test]
async fn given_unapplied_install_for_retired_group_when_catalog_arrives_should_refuse_it() {
    const LIVE_GROUP_ID: u64 = 1;
    const RETIRED_GROUP_ID: u64 = 2;
    const CLIENT_ID: u128 = 42;
    let mut partition = test_partition();
    install_test_owner(&mut partition, RETIRED_GROUP_ID, CLIENT_ID).await;
    install_unassigned_test_owner(&mut partition, RETIRED_GROUP_ID).await;
    let catalog = RetireConsumerGroupOwnersRequest {
        incarnation: partition.created_revision(),
        metadata_op: partition.required_metadata_frontier + 1,
        next_group_id: RETIRED_GROUP_ID + 1,
        live_group_ids: vec![LIVE_GROUP_ID],
    };
    let obsolete = next_test_installation(&partition, RETIRED_GROUP_ID, CLIENT_ID);
    partition
        .on_request(owner_install_request(&partition, &obsolete), None)
        .await;
    let obsolete_op = partition.consensus.sequencer().current_sequence();
    assert_eq!(partition.pending_owner_install.get(), Some(obsolete));

    let (sender, receiver) = consensus::oneshot_channel();
    partition
        .on_request(owner_retirement_request(&partition, &catalog), Some(sender))
        .await;
    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        obsolete_op,
        "a catalog must wait for a pending install of a group it retires"
    );
    assert_eq!(
        receiver.await.unwrap().header().status,
        IggyError::TransientNotAccepted.as_code()
    );

    let live = next_test_installation(&partition, LIVE_GROUP_ID, CLIENT_ID);
    let prepare =
        partition.pipeline_local_request(owner_install_request(&partition, &live), None, None);
    partition.on_replicate(prepare).await;
    partition.consensus.set_view(1);
    partition.resynchronize_consumer_offset_reservations();
    assert_eq!(partition.pending_owner_install.get(), Some(live));
    let (sender, receiver) = consensus::oneshot_channel();
    partition
        .on_request(owner_retirement_request(&partition, &catalog), Some(sender))
        .await;
    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        obsolete_op + 1,
        "a catalog must wait for every install the log retains"
    );
    assert_eq!(
        receiver.await.unwrap().header().status,
        IggyError::TransientNotAccepted.as_code()
    );

    commit_log(&mut partition).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(
        partition.consumer_group_owner(RETIRED_GROUP_ID),
        Some(obsolete.owner)
    );
}

//     #[compio::test]
//     async fn given_installed_owner_when_checkpoint_and_restart_should_commit_group_offset() {
//         const GROUP_ID: u32 = 7;
//         const CLIENT_ID: u128 = 42;
//         let directory = tempfile::tempdir().unwrap();
//         std::fs::create_dir(directory.path().join("prepares-0")).unwrap();
//         let mut partition = test_partition();
//         partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
//         let checkpoint = install_test_owner(&mut partition, u64::from(GROUP_ID), CLIENT_ID).await;
//         let owner = partition.consumer_group_owner(u64::from(GROUP_ID)).unwrap();
//         partition
//             .persist_retry_checkpoint(checkpoint)
//             .await
//             .unwrap();
//         drop(partition);
//
//         let mut recovered = test_partition();
//         recovered.set_partition_dir(directory.path().to_string_lossy().into_owned());
//         recovered
//             .restore_retry_checkpoint(checkpoint)
//             .await
//             .unwrap();
//         assert_eq!(
//             recovered.consumer_group_owner(u64::from(GROUP_ID)),
//             Some(owner),
//             "checkpoint recovery must retain the installed owner before replaying group offsets",
//         );
//         recovered
//             .consensus
//             .restore_commit_state(checkpoint, checkpoint);
//         recovered.consensus.sequencer().set_sequence(checkpoint);
//         let mut send = checksumless_send_request(recovered.namespace(), 1);
//         send.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
//             .copy_from_slice(&build_segment_record(recovered.namespace(), 0));
//         let send = send.transmute_header(|mut header, next: &mut RoutedRequestHeader| {
//             header.metadata_watermark = recovered.required_metadata_frontier;
//             *next = header;
//         });
//         recovered.on_request(send, None).await;
//         recovered.consensus.advance_commit_max(checkpoint + 1);
//         recovered.commit_journal(&repair_config()).await;
//         assert!(recovered.fatal().is_none(), "{:?}", recovered.fatal());
//         let request = store_offset_request(
//             CLIENT_ID,
//             1,
//             ConsumerKind::ConsumerGroup,
//             GROUP_ID,
//             0,
//             AckLevel::Quorum,
//         )
//         .transmute_header(|mut header, next: &mut RoutedRequestHeader| {
//             header.metadata_watermark = recovered.required_metadata_frontier;
//             header.owner_generation = owner.generation;
//             *next = header;
//         });
//         recovered.on_request(request, None).await;
//         let offset_op = recovered.consensus.sequencer().current_sequence();
//         assert_eq!(offset_op, checkpoint + 2);
//         recovered.consensus.advance_commit_max(offset_op);
//         recovered.commit_journal(&repair_config()).await;
//         assert!(recovered.fatal().is_none(), "{:?}", recovered.fatal());
//         assert_eq!(recovered.consensus.commit_min(), offset_op);
//         assert_eq!(
//             recovered
//                 .durable_consumer_offsets
//                 .get(ConsumerKind::ConsumerGroup, GROUP_ID)
//                 .map(|state| state.committed_offset),
//             Some(0),
//         );
//     }
//
//     #[compio::test]
//     async fn given_deleted_history_when_checkpoint_and_restart_should_preserve_the_fence() {
//         for dispatched in [false, true] {
//             const METADATA_OP: u64 = 10;
//             let directory = tempfile::tempdir().unwrap();
//             let mut config = repair_config();
//             config.path_layout.streams_root = directory.path().to_string_lossy().into_owned();
//             let path = config.get_partition_path(1, 1, 0);
//             std::fs::create_dir_all(&path).unwrap();
//             std::fs::create_dir(std::path::Path::new(&path).join("prepares-0")).unwrap();
//             let mut partition = test_partition();
//             partition.set_partition_dir(path.clone());
//             if dispatched {
//                 partition.set_io_notifier(
//                     Rc::new(|_, _| {}),
//                     crate::largest_legal_job_charge().unwrap(),
//                 );
//             }
//             let transition = TransitionPartitionHistoryRequest {
//                 incarnation: partition.created_revision(),
//                 metadata_op: METADATA_OP,
//             };
//             let request = partition_control_request(
//                 &partition,
//                 Operation::TransitionPartitionHistory,
//                 &transition.to_bytes(),
//                 METADATA_OP,
//             );
//             partition.on_request(request, None).await;
//             let checkpoint = partition.consensus.sequencer().current_sequence();
//             assert_eq!(checkpoint, 1);
//             partition.consensus.advance_commit_max(checkpoint);
//             partition.commit_journal(&config).await;
//             if dispatched {
//                 settle_partition_io(&mut partition, &config).await;
//             }
//             assert_eq!(partition.consensus.commit_min(), checkpoint);
//             assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
//             assert!(partition.history_deleted());
//             partition
//                 .persist_retry_checkpoint(checkpoint)
//                 .await
//                 .unwrap();
//             drop(partition);
//
//             let mut recovered = test_partition();
//             recovered.set_partition_dir(path);
//             recovered
//                 .restore_retry_checkpoint(checkpoint)
//                 .await
//                 .unwrap();
//             recovered
//                 .consensus
//                 .restore_commit_state(checkpoint, checkpoint);
//             assert!(recovered.history_deleted());
//             assert_eq!(
//                 recovered.installed_history_transition(&transition),
//                 Some(checkpoint)
//             );
//         }
//     }
//
//     #[compio::test]
//     async fn given_fence_awaiting_directory_sync_when_checkpoint_is_due_should_recover_the_fence() {
//         const FIRST_GROUP_ID: u64 = 1;
//         const SECOND_GROUP_ID: u64 = 2;
//         const CLIENT_ID: u128 = 42;
//         // Only the second resident control op makes the flush due, so the owner
//         // install leaves the segment names dirty for the fence to sync.
//         const CONTROL_OPS_PER_FLUSH: u32 = 2;
//         for operation in [
//             Operation::InstallConsumerGroupOwner,
//             Operation::TransitionPartitionHistory,
//         ] {
//             let root = tempfile::tempdir().unwrap();
//             let mut config = repair_config();
//             config.path_layout.streams_root = root.path().to_string_lossy().into_owned();
//             let directory = std::path::PathBuf::from(config.get_partition_path(1, 1, 0));
//             std::fs::create_dir_all(&directory).unwrap();
//             let (mut partition, _) = recording_partition_at(0, 1);
//             partition.set_partition_dir(directory.to_string_lossy().into_owned());
//             set_offset_dirs_under(&mut partition, &directory);
//             partition.runtime_options.messages_required_to_save = Some(CONTROL_OPS_PER_FLUSH);
//             partition.open_persistence().await.unwrap();
//             partition.log.retire_front().unwrap();
//             partition.install_empty_segment(&config, 0).await.unwrap();
//             partition.set_io_notifier(
//                 Rc::new(|_, _| {}),
//                 crate::largest_legal_job_charge().unwrap(),
//             );
//             let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
//
//             let first = next_test_installation(&partition, FIRST_GROUP_ID, CLIENT_ID);
//             partition
//                 .on_request(owner_install_request(&partition, &first), None)
//                 .await;
//             persistence.drain().await.unwrap();
//             partition.consensus.advance_commit_max(1);
//             partition.commit_journal(&config).await;
//             assert_eq!(partition.consensus.commit_min(), 1);
//
//             let second = next_test_installation(&partition, SECOND_GROUP_ID, CLIENT_ID);
//             let body = if operation == Operation::InstallConsumerGroupOwner {
//                 second.to_bytes()
//             } else {
//                 TransitionPartitionHistoryRequest {
//                     incarnation: partition.created_revision(),
//                     metadata_op: second.metadata_op,
//                 }
//                 .to_bytes()
//             };
//             let fence_applied = |partition: &IggyPartition<RecordingBus>| {
//                 if operation == Operation::InstallConsumerGroupOwner {
//                     partition.consumer_group_owner(SECOND_GROUP_ID) == Some(second.owner)
//                 } else {
//                     partition.history_deleted()
//                 }
//             };
//             partition
//                 .on_request(
//                     partition_control_request(&partition, operation, &body, second.metadata_op),
//                     None,
//                 )
//                 .await;
//             persistence.drain().await.unwrap();
//             partition.consensus.advance_commit_max(2);
//             partition.commit_journal(&config).await;
//             assert!(
//                 matches!(
//                     partition
//                         .pending_commit
//                         .as_ref()
//                         .map(|pending| pending.phase),
//                     Some(CommitPhase::SegmentDirectory)
//                 ),
//                 "{operation:?} must wait for the directory sync"
//             );
//             assert_eq!(partition.consensus.commit_min(), 1);
//             assert!(
//                 fence_applied(&partition),
//                 "{operation:?} applies before its commit publishes"
//             );
//
//             // Shard ticks: one while the directory sync is pending, one after it.
//             persistence.request_checkpoint();
//             if partition.needs_persistence_checkpoint() {
//                 partition.checkpoint_persistence(&config).await;
//             }
//             settle_partition_io(&mut partition, &config).await;
//             assert_eq!(partition.consensus.commit_min(), 2);
//             if partition.needs_persistence_checkpoint() {
//                 partition.checkpoint_persistence(&config).await;
//             }
//             settle_partition_io(&mut partition, &config).await;
//             persistence.drain().await.unwrap();
//             settle_partition_io(&mut partition, &config).await;
//             assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
//             drop(partition);
//             drop(persistence);
//
//             let (mut recovered, _) = recording_partition_at(0, 1);
//             recovered.set_partition_dir(directory.to_string_lossy().into_owned());
//             set_offset_dirs_under(&mut recovered, &directory);
//             let recovery = recovered.open_persistence().await;
//             assert!(
//                 recovery.is_ok(),
//                 "{operation:?}: recovery refused the published checkpoint: {recovery:?}"
//             );
//             assert_eq!(
//                 recovered
//                     .persistence
//                     .as_ref()
//                     .map(|persistence| persistence.checkpoint_op()),
//                 Some(2),
//                 "{operation:?}: the deferred checkpoint must run once the fence publishes"
//             );
//             assert_eq!(
//                 recovered.consumer_group_owner(FIRST_GROUP_ID),
//                 Some(first.owner)
//             );
//             assert!(fence_applied(&recovered), "{operation:?}");
//         }
//     }

// #[compio::test]
// async fn receipt_checkpoint_recovery_refuses_missing_corrupt_and_foreign_artifacts() {
//     const CHECKPOINT_OP: u64 = 7;
//     let directory = tempfile::tempdir().unwrap();
//     let mut partition = partition_at_view(1, 1);
//     partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
//     partition.set_dedup_clients_max(3);
//     std::fs::create_dir(directory.path().join("prepares-0")).unwrap();
//     let path = directory.path().join("prepares-0/receipts-7.checkpoint");
//     assert_eq!(
//         partition
//             .restore_retry_checkpoint(CHECKPOINT_OP)
//             .await
//             .unwrap_err()
//             .kind(),
//         std::io::ErrorKind::NotFound
//     );
//     publish_empty_retry_checkpoint(&partition, CHECKPOINT_OP).await;
//     let valid = std::fs::read(&path).unwrap();
//     for offset in [0, 4, 12, 20] {
//         let mut foreign = valid.clone();
//         foreign[offset] ^= 1;
//         let content_len = foreign.len() - size_of::<u64>();
//         let checksum = consensus::state_artifact_checksum(&foreign[..content_len]);
//         foreign[content_len..].copy_from_slice(&checksum.to_le_bytes());
//         std::fs::write(&path, &foreign).unwrap();
//         assert_eq!(
//             partition
//                 .restore_retry_checkpoint(CHECKPOINT_OP)
//                 .await
//                 .unwrap_err()
//                 .kind(),
//             std::io::ErrorKind::InvalidData
//         );
//         assert_eq!(std::fs::read(&path).unwrap(), foreign);
//         assert_eq!(
//             partition.dedup().capacity(),
//             3,
//             "refusal must preserve the live protection table"
//         );
//     }
//     for end in 0..valid.len() {
//         std::fs::write(&path, &valid[..end]).unwrap();
//         assert!(
//             partition
//                 .restore_retry_checkpoint(CHECKPOINT_OP)
//                 .await
//                 .is_err(),
//             "accepted truncated receipt artifact at {end}"
//         );
//     }
//     std::fs::write(&path, &valid).unwrap();
//     partition
//         .restore_retry_checkpoint(CHECKPOINT_OP)
//         .await
//         .unwrap();
//     assert_eq!(
//         partition.dedup().capacity(),
//         consensus::PARTITION_DEDUP_CLIENTS_MAX
//     );
// }

//     #[compio::test]
//     async fn receipt_recovery_reclaims_only_unreferenced_artifacts_after_validation() {
//         const CHECKPOINT_OP: u64 = 7;
//         const OBSOLETE_OP: u64 = 3;
//         const ORPHAN_OP: u64 = 99;
//         let directory = tempfile::tempdir().unwrap();
//         let mut partition = partition_at_view(1, 1);
//         partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
//         let prepares = directory.path().join("prepares-0");
//         std::fs::create_dir(&prepares).unwrap();
//         for through in [OBSOLETE_OP, CHECKPOINT_OP, ORPHAN_OP] {
//             publish_empty_retry_checkpoint(&partition, through).await;
//         }
//         let current = prepares.join("receipts-7.checkpoint");
//         let valid = std::fs::read(&current).unwrap();
//         let retained = [
//             "receipts-unknown.checkpoint",
//             "prepare-99.log",
//             "receipts-4.checkpoint-dir",
//         ];
//         for name in retained {
//             std::fs::write(prepares.join(name), b"unrelated").unwrap();
//         }
//         std::fs::write(prepares.join("receipts-8.checkpoint.tmp"), b"unfinished").unwrap();
//         std::fs::create_dir(prepares.join("receipts-9.checkpoint")).unwrap();
//         std::fs::write(&current, b"corrupt").unwrap();
//         assert!(
//             partition
//                 .restore_retry_checkpoint(CHECKPOINT_OP)
//                 .await
//                 .is_err()
//         );
//         assert!(prepares.join("receipts-3.checkpoint").exists());
//         assert!(prepares.join("receipts-99.checkpoint").exists());
//
//         std::fs::write(&current, &valid).unwrap();
//         partition
//             .restore_retry_checkpoint(CHECKPOINT_OP)
//             .await
//             .unwrap();
//         assert_eq!(std::fs::read(&current).unwrap(), valid);
//         for name in [
//             "receipts-3.checkpoint",
//             "receipts-99.checkpoint",
//             "receipts-8.checkpoint.tmp",
//         ] {
//             assert!(
//                 !prepares.join(name).exists(),
//                 "obsolete artifact retained: {name}"
//             );
//         }
//         for name in retained {
//             assert!(
//                 prepares.join(name).exists(),
//                 "unrelated entry removed: {name}"
//             );
//         }
//         assert!(prepares.join("receipts-9.checkpoint").is_dir());
//
//         // A receipt published before a failed WAL publication remains orphaned at boot.
//         publish_empty_retry_checkpoint(&partition, ORPHAN_OP).await;
//         partition
//             .restore_retry_checkpoint(CHECKPOINT_OP)
//             .await
//             .unwrap();
//         assert!(!prepares.join("receipts-99.checkpoint").exists());
//         assert!(current.exists());
//         partition.restore_retry_checkpoint(0).await.unwrap();
//         assert!(!current.exists());
//     }
//
//     #[compio::test]
//     async fn checkpoint_only_recovery_restores_the_prepare_chain_anchor() {
//         let directory = tempfile::tempdir().unwrap();
//         let mut partition = partition_at_view(1, 1);
//         partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
//         partition.runtime_options.durability = iggy_common::Durability::Persisted;
//         let mut journal = journal::PartitionPrepareJournal::open(
//             &directory.path().join("prepares-0"),
//             partition.consensus().group(),
//             0,
//         )
//         .await
//         .unwrap();
//         journal.reset(7, Some(1234)).await.unwrap();
//         journal.certify_log_view(1, 7, 1234).await.unwrap();
//         drop(journal);
//         publish_empty_retry_checkpoint(&partition, 7).await;
//         partition.open_persistence().await.unwrap();
//         assert_eq!(partition.consensus().sequencer().current_sequence(), 7);
//         assert_eq!(partition.consensus().commit_min(), 7);
//         assert_eq!(partition.consensus().last_prepare_checksum(), 1234);
//     }

/// In-memory superblock double: records every payload, counts attempts,
/// and injects write failures.
//     #[derive(Default)]
//     struct RecordingSuperblock {
//         writes: RefCell<Vec<Vec<u8>>>,
//         attempts: Cell<u32>,
//         fail_writes: Cell<bool>,
//     }
//
//     impl journal::superblock::SuperblockStore for RecordingSuperblock {
//         async fn write(&self, payload: &[u8]) -> std::io::Result<()> {
//             self.attempts.set(self.attempts.get() + 1);
//             if self.fail_writes.get() {
//                 return Err(std::io::Error::other("injected superblock write failure"));
//             }
//             self.writes.borrow_mut().push(payload.to_vec());
//             Ok(())
//         }
//
//         async fn read_latest(&self) -> std::io::Result<journal::superblock::SuperblockContents> {
//             Ok(self
//                 .writes
//                 .borrow()
//                 .last()
//                 .map_or(journal::superblock::SuperblockContents::Empty, |bytes| {
//                     journal::superblock::SuperblockContents::Present(bytes.clone())
//                 }))
//         }
//     }

#[compio::test]
async fn given_storeless_partition_when_persist_gate_runs_should_mark_current_view_durable() {
    let partition = partition_at_view(2, 1);
    assert!(partition.consensus().needs_superblock_persist());

    assert!(partition.persist_superblock_if_needed().await);

    assert!(
        !partition.consensus().needs_superblock_persist(),
        "a storeless partition must record durable = current, or the dispatch \
             tripwire would fire on its first view-scoped send"
    );
}

#[test]
fn given_held_commits_when_more_are_deferred_should_keep_the_latest_per_target() {
    let (partition, _) = recording_partition();
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    let commit = |target: Option<u8>, timestamp_monotonic: u64| consensus::VsrAction::SendCommit {
        view: 0,
        commit: 1,
        group: partition.namespace().inner(),
        timestamp_monotonic,
        target,
    };

    partition.defer_wire_actions(vec![commit(None, 1), commit(Some(1), 2)]);
    partition.defer_wire_actions(vec![commit(Some(2), 3), commit(Some(1), 4)]);
    partition.defer_wire_actions(vec![commit(None, 5)]);

    let held: Vec<_> = partition
        .pending_wire_actions
        .borrow()
        .as_ref()
        .expect("the commits are held")
        .actions
        .iter()
        .map(|action| match action {
            consensus::VsrAction::SendCommit {
                target,
                timestamp_monotonic,
                ..
            } => (*target, *timestamp_monotonic),
            other => panic!("held a non-commit action: {other:?}"),
        })
        .collect();
    assert_eq!(
        held,
        [(None, 5), (Some(1), 4), (Some(2), 3)],
        "a Commit to one backup must neither displace the heartbeat to all nor \
             another backup's Commit"
    );
}

#[compio::test]
async fn given_a_tick_extension_when_it_writes_should_advance_the_ceiling_a_full_lease() {
    let store = Rc::new(RecordingSuperblock::default());
    let mut partition = solo_recording_partition();
    partition.set_superblock(store.clone(), None);
    partition.set_offset_reservation_lease(test_lease(16));

    assert!(partition.reserve_offsets_through(0).await);
    assert_eq!(last_recorded_reservation(&store), 17);
    partition.note_append_live();

    // Half the block consumed, which is where the trigger fires.
    partition.dirty_offset.store(11, Ordering::Relaxed);
    assert!(partition.needs_offset_reservation_extension());
    assert!(partition.extend_offset_reservation().await);
    assert_eq!(
        last_recorded_reservation(&store),
        33,
        "a full lease past the ceiling of 17, not past the append point of 12"
    );

    // And the trigger is genuinely satisfied for a full block of appends,
    // rather than re-firing after another half.
    for offset in 12..=24 {
        partition.dirty_offset.store(offset, Ordering::Relaxed);
        assert!(
            !partition.needs_offset_reservation_extension(),
            "offset {offset} still sits a full half-lease under the new ceiling"
        );
    }
}

/// A storeless partition (in-memory, simulated) reserves nothing at all, so
/// the tick must never reach a write for one.
#[test]
fn given_a_storeless_partition_when_ticking_should_not_extend() {
    let mut partition = solo_recording_partition();
    partition.set_offset_reservation_lease(test_lease(16));
    // Without this the assert would pass on `!append_live` alone, leaving
    // the store check it is named for untested.
    partition.note_append_live();
    assert!(partition.superblock.is_none(), "the premise: no store");
    assert!(!partition.needs_offset_reservation_extension());
}

/// Inside an open backoff window the ADMITTED path refuses without touching
/// the disk the last writer just found broken. Every producer retry otherwise
/// re-runs a full atomic replace, which starves the shard pump for as long as
/// the fault lasts.
#[compio::test]
async fn given_an_open_backoff_window_when_preflighting_a_send_should_refuse_without_writing() {
    let store = Rc::new(RecordingSuperblock::default());
    let mut partition = solo_recording_partition();
    partition.set_superblock(store.clone(), None);
    partition.set_offset_reservation_lease(test_lease(16));
    partition
        .superblock_retry_after_micros
        .set(partition.consensus().clock_realtime_micros() + 1_000_000);

    assert_eq!(
        partition.reserve_offsets_through_retryable(0).await,
        crate::PartitionIoVerdict::Failed,
        "a claim that would need a write is refused inside the window"
    );
    assert_eq!(
        store.attempts.get(),
        0,
        "and refused without any I/O at all"
    );

    // Isolated bootstrap still attempts its required initial claim.
    assert!(partition.reserve_offsets_through(0).await);
    assert_eq!(store.attempts.get(), 1);

    // A batch the record already covers owes the disk nothing, so the open
    // window must not refuse it either.
    assert_eq!(
        partition.reserve_offsets_through_retryable(0).await,
        crate::PartitionIoVerdict::Ready,
        "the coverage fast path wins over the backoff window"
    );
    assert_eq!(store.attempts.get(), 1);
}

/// A journaled offset is not a committed one. Seeding the committed bit at
/// the append would serve a resident offset 0 to a consumer before the first
/// commit and let the frontier persist name data no quorum agreed on.
#[test]
fn given_a_journaled_offset_when_uncommitted_should_not_seed_the_committed_counter() {
    let mut partition = solo_recording_partition();

    partition.note_append_live();
    partition.dirty_offset.store(0, Ordering::Relaxed);
    assert!(partition.offset_space.append_live);
    assert!(
        !partition.offset_space.committed_seeded,
        "the append moves the append counter alone"
    );
    assert_eq!(partition.mint_frontier(), 1, "the next mint continues it");
    assert_eq!(
        partition.offset_frontier(),
        0,
        "and the frontier a persist would record still names no data"
    );

    partition.note_committed_seeded();
    assert_eq!(
        partition.offset_frontier(),
        1,
        "commit is what publishes the offset"
    );
}

/// Fail-closed: offsets the record does not cover would be confirmed to a
/// client with nothing durable saying they were handed out.
/// The fence ahead of the pipeline must bound a mint the ordinary send path
/// actually takes. `convert_request_message` runs at `ChecksumMode::Skip`, so
/// a verifying decode of its output fails and the ceiling would come back
/// `None`, dropping every solo send back to the fence at the mint, where a
/// refusal fences the partition and exits the node.
#[test]
fn given_a_checksumless_send_when_bounding_the_mint_should_read_the_batch_header() {
    let partition = solo_recording_partition();
    let namespace = IggyNamespace::from_raw(partition.consensus().group());
    let message = checksumless_send_request(namespace, 3);
    let body = &message.as_slice()
        [std::mem::size_of::<RoutedRequestHeader>()..message.header().size as usize];

    assert!(
        decode_batch_slice(body).is_err(),
        "the premise: a Skip-converted body does not survive a verifying decode"
    );
    assert_eq!(
        partition.request_mint_ceiling(&message),
        Some(2),
        "the ceiling is the batch's LAST offset: three messages from a frontier \
             of 0 mint 0, 1 and 2, and the claim adds the exclusive successor itself"
    );
}

/// Nothing is reserved above one replica, so the ceiling is not computed
/// there either.
#[test]
fn given_a_replicated_group_when_bounding_the_mint_should_not_compute_a_ceiling() {
    let partition = partition_at_view(1, 1);
    assert!(partition.consensus().replica_count() > 1);
    let namespace = IggyNamespace::from_raw(partition.consensus().group());
    assert_eq!(
        partition.request_mint_ceiling(&checksumless_send_request(namespace, 3)),
        None
    );
}

//     #[compio::test]
//     async fn given_a_crafted_offset_name_when_admitting_should_not_reserve_offsets() {
//         let store = Rc::new(RecordingSuperblock::default());
//         let mut partition = solo_recording_partition();
//         partition.set_superblock(store.clone(), None);
//         partition.set_offset_reservation_lease(test_lease(16));
//
//         // The consumer prefix is three bytes. Its name overlaps the batch
//         // length and count fields, making this non-send body a valid header.
//         let mut name = vec![0u8; 255];
//         name[29..37].copy_from_slice(&256u64.to_le_bytes());
//         name[45..49].copy_from_slice(&[0x7F; 4]);
//         let body = StoreConsumerOffsetRequest {
//             consumer: WireConsumer {
//                 kind: ConsumerKind::Consumer.as_code(),
//                 id: WireIdentifier::String(
//                     WireName::new(String::from_utf8(name).unwrap()).unwrap(),
//                 ),
//             },
//             stream_id: WireIdentifier::Numeric(1),
//             topic_id: WireIdentifier::Numeric(1),
//             partition_id: Some(0),
//             offset: 0,
//             ack: AckLevel::NoAck,
//         }
//         .to_bytes();
//         assert!(BatchHeader::decode(&body).is_ok());
//         let header_size = std::mem::size_of::<RoutedRequestHeader>();
//         let total = header_size + body.len();
//         let mut message = Message::<RoutedRequestHeader>::new(total);
//         message.as_mut_slice()[header_size..].copy_from_slice(&body);
//         let message = message.transmute_header(|_, header: &mut RoutedRequestHeader| {
//             header.command = Command::Request;
//             header.operation = Operation::StoreConsumerOffset;
//             header.client = 1;
//             header.session = 1;
//             header.request = 1;
//             header.group = IggyNamespace::new(1, 1, 0).inner();
//             header.size = u32::try_from(total).unwrap();
//         });
//
//         assert_eq!(partition.request_mint_ceiling(&message), None);
//         assert_eq!(
//             partition.admit_reserved_send(&message).await,
//             crate::PartitionIoVerdict::Ready
//         );
//         assert!(store.writes.borrow().is_empty());
//         assert_eq!(partition.durable_offset_reserved.get(), 0);
//     }

#[test]
fn given_a_rearmed_advance_when_requesting_shutdown_should_replace_it() {
    let mut partition = solo_recording_partition();
    partition.set_superblock(Rc::new(RecordingSuperblock::default()), None);
    let advance = SuperblockIntent::Advance {
        frontier: 0,
        reserved: 64,
    };
    partition.superblock_intent.set(Some(advance));
    let collapse = SuperblockIntent::Collapse {
        frontier: 0,
        reserved: 0,
    };

    assert_eq!(
        partition.request_superblock_intent(collapse),
        crate::PartitionIoVerdict::Pending
    );
    assert!(
        partition.superblock_intent.get() == Some(collapse),
        "shutdown must replace the failed Advance"
    );
    assert_eq!(
        partition.request_superblock_intent(advance),
        crate::PartitionIoVerdict::Pending
    );
    assert!(
        partition.superblock_intent.get() == Some(collapse),
        "an Advance must not replace the final write"
    );
}

//     #[compio::test]
//     async fn given_failing_superblock_when_fencing_should_refuse() {
//         let store = Rc::new(RecordingSuperblock::default());
//         let mut partition = solo_recording_partition();
//         partition.set_superblock(store.clone(), None);
//         partition.set_offset_reservation_lease(test_lease(4));
//         store.fail_writes.set(true);
//
//         assert!(
//             !partition.reserve_offsets_through(0).await,
//             "an unrecordable claim must refuse the append"
//         );
//     }

/// The point of extending from the tick: after it runs, the append path finds
/// the ceiling already covering it and writes nothing. Without this the two
/// fsyncs land in front of a produce, inside the frame pump the consensus
/// tick shares.
#[compio::test]
async fn given_a_consumed_block_when_the_tick_extends_should_leave_the_append_path_writeless() {
    let store = Rc::new(RecordingSuperblock::default());
    let mut partition = solo_recording_partition();
    partition.set_superblock(store.clone(), None);
    partition.set_offset_reservation_lease(test_lease(16));

    // First append pays for the first claim, as it must: nothing durable yet.
    assert!(partition.reserve_offsets_through(0).await);
    assert_eq!(store.attempts.get(), 1);
    partition.set_offset_space_used(true);

    // Inside the block there is nothing to do, from either caller.
    partition.dirty_offset.store(4, Ordering::Relaxed);
    assert!(
        !partition.needs_offset_reservation_extension(),
        "a full block of headroom needs no extension"
    );

    // Past half the block the tick takes the write.
    partition.dirty_offset.store(11, Ordering::Relaxed);
    assert!(
        partition.needs_offset_reservation_extension(),
        "under half a block of headroom the tick must extend"
    );
    assert!(partition.extend_offset_reservation().await);
    assert_eq!(
        store.attempts.get(),
        2,
        "the tick wrote, not the append path"
    );

    // And now the append path is writeless across the rest of the old block.
    let before = store.attempts.get();
    for offset in 12..=16 {
        assert!(partition.reserve_offsets_through(offset).await);
    }
    assert_eq!(
        store.attempts.get(),
        before,
        "every append after the extension must take the fence's fast path"
    );
}

/// The extension must not fire for a partition that has never minted, or boot
/// would write a superblock per idle partition for nothing.
#[test]
fn given_an_untouched_partition_when_ticking_should_not_extend_the_reservation() {
    let mut partition = solo_recording_partition();
    partition.set_superblock(Rc::new(RecordingSuperblock::default()), None);
    assert!(!partition.offset_space.append_live);
    assert!(!partition.needs_offset_reservation_extension());
}

/// A boot that consumed a reservation has ZERO headroom -- the append point
/// sits exactly at the ceiling -- so the extension has to fire before the
/// first produce rather than after it.
#[test]
fn given_a_reservation_seeded_boot_when_ticking_should_extend_before_the_first_produce() {
    let mut partition = solo_recording_partition();
    let recovered = recorded_state(0, 65_537);
    partition.set_superblock(Rc::new(RecordingSuperblock::default()), Some(&recovered));
    partition.restore_offset_frontier(Some(&recovered));
    assert_eq!(partition.mint_frontier(), 65_537);
    assert!(
        partition.needs_offset_reservation_extension(),
        "the seeded append point is at the ceiling, so the next append would \
             otherwise pay for the write"
    );
}

/// A replicated group pays nothing for a protection it cannot use: nothing
/// seeds its counter from the reservation, its chain never gets re-anchored,
/// and an ack there means a quorum journaled the batch.
#[compio::test]
async fn given_a_replicated_group_when_fencing_should_not_write_at_all() {
    let store = Rc::new(RecordingSuperblock::default());
    let mut partition = partition_at_view(1, 1);
    assert!(partition.consensus().replica_count() > 1);
    partition.set_superblock(store.clone(), None);
    partition.set_offset_reservation_lease(test_lease(16));
    store.fail_writes.set(true);

    assert!(
        partition.reserve_offsets_through(1_000).await,
        "the fence is inert above one replica, so not even a failing store can \
             refuse the append"
    );
    assert_eq!(store.attempts.get(), 0, "and it attempts no write");
}

/// The reservation seeds the APPEND counter and only on a solo group. The
/// whole fix: after a crash below the flush thresholds it is the only witness
/// that those offsets were confirmed.
#[test]
fn given_a_recorded_reservation_when_solo_should_seed_the_append_counter() {
    let mut partition = solo_recording_partition();
    assert_eq!(
        partition.consensus().replica_count(),
        1,
        "the reservation seed is scoped to solo groups"
    );
    let store = Rc::new(RecordingSuperblock::default());

    partition.set_superblock(store.clone(), None);
    partition.restore_offset_frontier(None);
    assert_eq!(
        partition.mint_frontier(),
        0,
        "nothing recorded, nothing seeded"
    );

    // The shape a crash before the first flush leaves: the fence recorded a
    // block, and no message ever reached a segment, so the frontier is 0.
    let recovered = recorded_state(0, 65_537);
    partition.set_superblock(store, Some(&recovered));
    partition.restore_offset_frontier(Some(&recovered));
    assert_eq!(
        partition.mint_frontier(),
        65_537,
        "the first mint must land above every offset the reservation covered"
    );
    assert_eq!(
        partition.offset_frontier(),
        0,
        "the committed frontier must not inherit the reservation, nor the \
             append counter's own liveness: nothing was flushed, so the partition \
             holds no offset at all"
    );
}

/// A replicated group must not take the jump: a backup rejects any prepare
/// whose `base_offset` does not continue its own counter, so an append point a
/// lease block above the group has every peer refuse the batch.
#[test]
fn given_a_recorded_reservation_when_replicated_should_not_seed_the_append_counter() {
    let mut partition = partition_at_view(1, 1);
    assert!(partition.consensus().replica_count() > 1);
    let recovered = recorded_state(0, 70_000);
    partition.set_superblock(Rc::new(RecordingSuperblock::default()), Some(&recovered));
    partition.restore_offset_frontier(Some(&recovered));
    assert_eq!(
        partition.mint_frontier(),
        0,
        "a replicated group's offsets are the group's to decide"
    );
}

#[test]
fn given_persisted_singleton_when_restoring_should_use_the_frontier_without_a_reservation_gap() {
    let mut partition = solo_recording_partition();
    partition.set_runtime_options(TopicRuntimeOptions {
        durability: iggy_common::Durability::Persisted,
        ..TopicRuntimeOptions::default()
    });
    let recovered = recorded_state(40, 70_000);
    partition.set_superblock(Rc::new(RecordingSuperblock::default()), Some(&recovered));
    partition.restore_offset_frontier(Some(&recovered));
    assert_eq!(partition.offset_frontier(), 40);
    assert_eq!(partition.mint_frontier(), 40);
}

/// The committed frontier still seeds both counters: it is a claim about data
/// every replica shares, so it is not gated on the replica count.
#[test]
fn given_a_recorded_frontier_when_replicated_should_seed_both_counters() {
    let mut partition = partition_at_view(1, 1);
    assert!(partition.consensus().replica_count() > 1);
    let recovered = recorded_state(40, 70_000);
    partition.set_superblock(Rc::new(RecordingSuperblock::default()), Some(&recovered));
    partition.restore_offset_frontier(Some(&recovered));
    assert_eq!(partition.offset_frontier(), 40);
    assert_eq!(
        partition.mint_frontier(),
        40,
        "the reservation is ignored here, so the append point is the frontier"
    );
}

/// The rewind fence asks whether an offer would destroy something this replica
/// COMMITTED. An append point standing a lease block above that -- what a
/// reservation-seeded boot leaves until the first append -- claims no
/// messages, so it must not turn a legitimate offer inside the block into a
/// refusal the replica would then cycle on forever.
#[compio::test]
async fn given_an_append_point_above_every_message_when_an_offer_arrives_should_not_call_it_a_rewind()
 {
    let partition_dir = transfer_fence_dir("append-point-is-not-data").await;
    let mut partition = test_partition();
    partition.set_partition_dir(partition_dir.clone());
    partition.set_offset_space_used(true);
    partition.dirty_offset.store(69_999, Ordering::Relaxed);
    assert_eq!(partition.mint_frontier(), 70_000);
    assert_eq!(
        partition.offset_frontier(),
        1,
        "nothing committed: the append point speaks for no messages"
    );

    let offer = crate::state_transfer::ConsumerOffsetsWire {
        owner_retirement: None,
        owners: Vec::new(),
        history_transition: None,
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        required_metadata_frontier: 0,
        prepare_checksum: None,
        checkpoint_prepare: Vec::new(),
        next_offset: 1_030,
        emptied_at: 0,
        consumers: Vec::new(),
        groups: Vec::new(),
        external_groups: Vec::new(),
        dedup: Vec::new(),
        retirement_paid_through: 0,
    };
    let outcome = partition
        .install_state_transfer(&repair_config(), 12, Vec::new(), &offer.encode())
        .await;
    assert!(
        !matches!(
            outcome,
            Err(crate::state_transfer::PartitionInstallError::OfferRewindsDurableData { .. })
        ),
        "the offer destroys nothing this replica holds, so the fence must let it \
             through: got {outcome:?}"
    );

    let _ = std::fs::remove_dir_all(&partition_dir);
}

/// After a clean shutdown the segments account for every confirmed offset,
/// so collapsing the reservation keeps the offset space dense across an
/// ordinary restart instead of jumping a lease block every time.
#[compio::test]
async fn given_a_flushed_partition_when_collapsing_should_drop_the_reservation_to_the_frontier() {
    let store = Rc::new(RecordingSuperblock::default());
    let mut partition = solo_recording_partition();
    partition.set_superblock(store.clone(), Some(&recorded_state(0, 65_537)));
    // A flushed partition: the append point and the committed head agree.
    partition.set_offset_space_used(true);
    partition.offset.store(24, Ordering::Release);
    partition.dirty_offset.store(24, Ordering::Relaxed);

    assert!(
        partition
            .complete_superblock_intent(partition.collapse_superblock_intent().unwrap())
            .await
    );
    assert_eq!(last_recorded_frontier(&store), 25);
    assert_eq!(
        last_recorded_reservation(&store),
        25,
        "a clean stop leaves no claim above what the segments prove"
    );

    // And the restart that follows mints where it left off, not a block up.
    let recovered = recorded_state(25, 25);
    let mut restarted = solo_recording_partition();
    restarted.set_superblock(store.clone(), Some(&recovered));
    restarted.restore_offset_frontier(Some(&recovered));
    assert_eq!(
        restarted.mint_frontier(),
        25,
        "the collapsed record puts the append point back at the frontier"
    );
}

/// The shard keeps ticking until every partition finished its shutdown. An
/// extension after the collapse would make the restart resume a lease block
/// above the data.
#[compio::test]
async fn given_a_collapsed_shutdown_when_the_tick_runs_should_not_extend_the_reservation() {
    const SHUTDOWN_SETTLES_MAX: usize = 4;
    let store = Rc::new(RecordingSuperblock::default());
    let mut partition = solo_recording_partition();
    partition.set_superblock(store.clone(), Some(&recorded_state(0, 65_537)));
    partition.set_offset_space_used(true);
    partition.offset.store(24, Ordering::Release);
    partition.dirty_offset.store(24, Ordering::Relaxed);
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    partition.begin_shutdown_io();
    for _ in 0..SHUTDOWN_SETTLES_MAX {
        if partition.shutdown_io_complete() {
            break;
        }
        settle_partition_io(&mut partition, &repair_config()).await;
    }
    assert!(partition.shutdown_io_complete());
    assert_eq!(last_recorded_reservation(&store), 25);

    if partition.needs_offset_reservation_extension() {
        let _ = partition.extend_offset_reservation().await;
    }
    for _ in 0..SHUTDOWN_SETTLES_MAX {
        settle_partition_io(&mut partition, &repair_config()).await;
    }
    assert_eq!(
        last_recorded_reservation(&store),
        25,
        "the collapse must stay the last superblock write of the shutdown"
    );
}

/// The graceful stop must not undo the crash protection. A boot that read a
/// reservation back and then took no traffic has a committed frontier of 0
/// while the reservation is the only record that offsets were confirmed, so a
/// collapse reading the committed frontier would write it away -- and a clean
/// stop is the runbook answer to an incident, which would make it the one
/// action that re-opens the defect.
#[compio::test]
async fn given_an_unspent_reservation_when_collapsing_should_leave_it_standing() {
    let store = Rc::new(RecordingSuperblock::default());
    let recovered = recorded_state(0, 65_537);
    let mut partition = solo_recording_partition();
    partition.set_superblock(store.clone(), Some(&recovered));
    partition.restore_offset_frontier(Some(&recovered));

    assert!(partition.collapse_superblock_intent().is_none());
    assert_eq!(
        store.attempts.get(),
        0,
        "the reservation already covers the append point, so there is nothing to \
             collapse and nothing to write"
    );

    // The restart after the clean stop still resumes above every offset the
    // crashed incarnation confirmed.
    let mut restarted = solo_recording_partition();
    restarted.set_superblock(store, Some(&recovered));
    restarted.restore_offset_frontier(Some(&recovered));
    assert_eq!(restarted.mint_frontier(), 65_537);
}

/// An install is the one place the reservation may come down: left high, it
/// re-seeds the counter above the group and every replicated prepare fails
/// the `base_offset == dirty_offset + 1` check.
#[compio::test]
async fn given_install_frontier_when_recorded_should_set_the_reservation_down_to_it() {
    let store = Rc::new(RecordingSuperblock::default());
    let mut partition = partition_at_view(1, 1);
    partition.set_superblock(store.clone(), Some(&recorded_state(0, 70_000)));

    assert!(
        partition
            .complete_superblock_intent(SuperblockIntent::Install(1_030))
            .await
    );
    assert_eq!(
        last_recorded_reservation(&store),
        1_030,
        "the install's frontier replaces the stale reservation"
    );
    assert_eq!(last_recorded_frontier(&store), 1_030);
}

/// The install's frontier write describes the state the install is about
/// to adopt. Once its failure aborts the install, the replica keeps its old
/// state, so the same write landing later would record a frontier and a
/// reservation for an install that never happened.
#[compio::test]
async fn given_failed_install_frontier_write_when_install_aborts_should_never_write_it_later() {
    // Between the recorded frontier and reservation, so a late write would
    // move both.
    const OFFER_FRONTIER: u64 = 150;
    const STEPS_MAX: usize = 32;
    let directory = tempfile::tempdir().unwrap();
    let mut partition = partition_at_view(1, 1);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    set_offset_dirs_under(&mut partition, directory.path());
    let store = Rc::new(RecordingSuperblock::default());
    let recovered = recorded_state(100, 200);
    partition.set_superblock(Rc::clone(&store), Some(&recovered));
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    store.fail_writes.set(true);
    let wire = crate::state_transfer::ConsumerOffsetsWire {
        dedup_capacity: consensus::PARTITION_DEDUP_CLIENTS_MAX,
        next_offset: OFFER_FRONTIER,
        ..Default::default()
    };
    partition
        .queue_state_transfer_install(1, Vec::new(), &wire.encode(), 1)
        .unwrap();

    let config = repair_config();
    let mut aborted = false;
    for _ in 0..STEPS_MAX {
        match partition.resume_io(&config).await {
            crate::PartitionIoStep::Ready(plan) => {
                execute_partition_io_plan(&mut partition, &config, plan).await;
            }
            crate::PartitionIoStep::InstallFinished { outcome, .. } => {
                assert!(
                    matches!(
                        outcome,
                        Err(
                            crate::state_transfer::PartitionInstallError::FrontierNotDurable {
                                frontier: OFFER_FRONTIER
                            }
                        )
                    ),
                    "the failed frontier write must abort the install: got {outcome:?}"
                );
                aborted = true;
                break;
            }
            crate::PartitionIoStep::Progress | crate::PartitionIoStep::Pending => {}
            _ => panic!("unexpected install continuation"),
        }
    }
    assert!(aborted);
    assert_eq!(
        store.attempts.get(),
        1,
        "the install must try its frontier write"
    );

    store.fail_writes.set(false);
    partition.superblock_retry_after_micros.set(0);
    settle_partition_io(&mut partition, &config).await;
    assert_eq!(
        store.attempts.get(),
        1,
        "an aborted install must never write its frontier later"
    );
    assert_eq!(
        partition.durable_offset_frontier.get(),
        recovered.offset_frontier
    );
    assert_eq!(
        partition.durable_offset_reserved.get(),
        recovered.offset_reserved
    );
}

/// The reservation can never sit below the frontier: "offsets under N exist"
/// is stronger than "offsets under N may have been handed out".
#[compio::test]
async fn given_reservation_below_the_frontier_when_written_should_clamp_it_up() {
    let store = Rc::new(RecordingSuperblock::default());
    let mut partition = partition_at_view(1, 1);
    partition.set_superblock(store.clone(), None);
    partition.set_offset_space_used(true);
    partition.offset.store(99, Ordering::Release);

    assert!(
        partition
            .complete_superblock_intent(SuperblockIntent::Advance {
                frontier: 100,
                reserved: 0
            })
            .await
    );
    assert_eq!(last_recorded_frontier(&store), 100);
    assert_eq!(
        last_recorded_reservation(&store),
        100,
        "the reservation is clamped up to the frontier it accompanies"
    );
}

/// The fence path persists the frontier while the live counter still sits
/// at its pre-install value, so an advance that maxes against the counter
/// alone erases the record and then quarantines the segments that were its
/// only other witness. Boot re-mints from 0 against a group at N after that.
#[compio::test]
async fn given_record_above_live_counter_when_advancing_should_keep_the_record() {
    let mut partition = partition_at_view(1, 1);
    let store = Rc::new(RecordingSuperblock::default());
    partition.set_superblock(store.clone(), None);

    assert!(
        partition
            .complete_superblock_intent(SuperblockIntent::Advance {
                frontier: 9_000,
                reserved: 0
            })
            .await
    );
    assert_eq!(last_recorded_frontier(&store), 9_000);
    assert_eq!(
        partition.offset_frontier(),
        0,
        "a partition that never minted reports a zero frontier, which is the \
             value the fence would otherwise persist"
    );

    assert!(
        partition
            .complete_superblock_intent(SuperblockIntent::Advance {
                frontier: partition.offset_frontier(),
                reserved: 0
            })
            .await
    );

    assert_eq!(
        last_recorded_frontier(&store),
        9_000,
        "the advance direction must not lower the durable frontier"
    );
}

/// Attaching a store seeds the last-written frontier from the record
/// itself, so an advance maxes against what boot read off disk even before
/// this replica has written anything. The sibling test reaches that state by
/// WRITING first, which cannot catch an attach site that skips the seed.
#[compio::test]
async fn given_attached_record_when_advancing_should_keep_the_recorded_frontier() {
    let mut partition = partition_at_view(1, 1);
    let store = Rc::new(RecordingSuperblock::default());
    let recovered = consensus::VsrState {
        cluster: TEST_CLUSTER,
        replica_id: 0,
        replica_count: 3,
        view: 1,
        log_view: 1,
        commit_max: 0,
        checkpoint_op: 0,
        checkpoint_checksum: 0,
        offset_frontier: 4_200,
        offset_reserved: 0,
        adopted_view: None,
    };
    partition.set_superblock(store.clone(), Some(&recovered));
    assert_eq!(partition.offset_frontier(), 0, "nothing minted locally");

    assert!(
        partition
            .complete_superblock_intent(SuperblockIntent::Advance {
                frontier: partition.offset_frontier(),
                reserved: 0
            })
            .await
    );

    assert_eq!(
        last_recorded_frontier(&store),
        4_200,
        "the first write after an attach must not lower the record it was attached to"
    );
}

// #[compio::test]
// async fn quarantine_finishes_after_a_failed_pending_or_running_superblock_write() {
//     const STEPS_MAX: usize = 16;
//     for running in [false, true] {
//         let mut partition = partition_at_view(1, 1);
//         let store = Rc::new(RecordingSuperblock::default());
//         store.fail_writes.set(true);
//         partition.set_superblock(Rc::clone(&store), None);
//         partition.set_io_notifier(
//             Rc::new(|_, _| {}),
//             crate::largest_legal_job_charge().unwrap(),
//         );
//         assert!(!partition.persist_superblock_if_needed().await);
//         let config = repair_config();
//         let captured = if running {
//             let crate::PartitionIoStep::Ready(plan) = partition.resume_io(&config).await else {
//                 panic!("view persistence must be ready");
//             };
//             Some(partition.capture_io(plan, &config).unwrap().unwrap())
//         } else {
//             None
//         };
//         partition.begin_quarantine(partition.offset_frontier());
//         if let Some(captured) = captured {
//             let result = captured.job.execute().await;
//             partition.accept_io(captured.identity, result).unwrap();
//             captured.gate.unwrap().release();
//         }
//         let mut finished = false;
//         for _ in 0..STEPS_MAX {
//             match partition.resume_io(&config).await {
//                 crate::PartitionIoStep::Ready(plan) => {
//                     execute_partition_io_plan(&mut partition, &config, plan).await;
//                 }
//                 crate::PartitionIoStep::QuarantineFinished(outcome) => {
//                     assert!(outcome.is_ok());
//                     finished = true;
//                     break;
//                 }
//                 crate::PartitionIoStep::Progress | crate::PartitionIoStep::Pending => {}
//                 _ => panic!("unexpected quarantine continuation"),
//             }
//         }
//         assert!(finished);
//         assert_eq!(store.attempts.get(), 1 + u32::from(running));
//         assert!(partition.shutdown_io_complete());
//     }
// }

#[compio::test]
async fn given_undurable_view_when_sending_prepare_ok_should_withhold_until_persisted() {
    let bus = RecordingBus::default();
    let replica_frames = bus.sent_to_replicas.clone();
    let mut consensus = VsrConsensus::new(
        TEST_CLUSTER,
        0,
        3,
        IggyNamespace::new(1, 1, 0).inner(),
        bus,
        LocalPipeline::new(),
    );
    consensus.set_view(1);
    consensus.set_log_view(1);
    consensus.init_as_backup();
    let mut partition: IggyPartition<RecordingBus, RecordingSuperblock> =
        IggyPartition::with_in_memory_storage(
            Arc::new(PartitionStats::default()),
            consensus,
            IggyByteSize::from(1024 * 1024),
        );
    let store = Rc::new(RecordingSuperblock::default());
    store.fail_writes.set(true);
    partition.set_superblock(store.clone(), None);
    // The ack path drops an op past the local head, so the head must cover it.
    partition.consensus().sequencer().set_sequence(1);
    let size = std::mem::size_of::<PrepareHeader>();
    let prepare =
        Message::<PrepareHeader>::new(size).transmute_header(|_, header: &mut PrepareHeader| {
            header.command = Command::Prepare;
            header.op = 1;
            // Current view: an older-view prepare is fenced as deposed-primary
            // traffic and would never reach the ack send under test.
            header.view = 1;
            header.size = u32::try_from(size).expect("prepare header size fits in u32");
        });
    let header = *prepare.header();

    partition.send_prepare_ok(&header).await;

    assert!(
        replica_frames.borrow().is_empty(),
        "an ack must not leave while the advanced view is not durable"
    );
    assert_eq!(store.attempts.get(), 1);

    // Outwait the write-failure backoff (base 10 ms doubled once by the
    // first failure), then retry with the store healthy: the ack must
    // persist first and then go out.
    store.fail_writes.set(false);
    compio::time::sleep(std::time::Duration::from_millis(50)).await;
    partition.send_prepare_ok(&header).await;

    assert_eq!(
        replica_frames.borrow().len(),
        1,
        "the retried ack must go out once the view persisted"
    );
    assert!(!partition.consensus().needs_superblock_persist());
    assert!(
        partition.pending_persisted_acks.borrow().is_empty(),
        "a partition without a WAL has no persistence driver to drain queued acks"
    );
}

//     #[compio::test]
//     async fn given_failing_superblock_when_persist_gate_runs_should_withhold_and_back_off() {
//         for (view, log_view) in [(1, 1), (2, 0)] {
//             let case = format!("view {view}, log_view {log_view}");
//             let mut partition = partition_at_view(view, log_view);
//             let store = Rc::new(RecordingSuperblock::default());
//             store.fail_writes.set(true);
//             partition.set_superblock(store.clone(), None);
//
//             assert!(
//                 !partition.persist_superblock_if_needed().await,
//                 "{case}: a failed write must withhold the send"
//             );
//             assert_eq!(store.attempts.get(), 1, "{case}");
//             assert!(
//                 !partition.persist_superblock_if_needed().await,
//                 "{case}: the backoff window must withhold without retrying the write"
//             );
//             assert_eq!(store.attempts.get(), 1, "{case}");
//             assert!(
//                 partition.consensus().needs_superblock_persist(),
//                 "{case}: the view stays undurable until a write lands"
//             );
//         }
//     }

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

// #[test]
// fn recovered_history_keeps_the_observed_batch_read_floor() {
//     let (mut partition, _) = recording_partition();
//     assert_eq!(partition.widest_committed_batch(), 0);
//     partition.widest_batch_bytes.set(4096);
//     assert_eq!(partition.widest_committed_batch(), 4096);
//     partition.recovered_durable_offset = Some(100);
//     assert_eq!(partition.widest_committed_batch(), 4096);
// }

#[test]
fn active_disk_poll_keeps_the_sparse_index_offset() {
    let (mut partition, _) = recording_partition();
    partition.log.ensure_indexes();
    let index = partition.log.active_indexes_mut().unwrap();
    index.insert(0, 0, 0);
    index.insert(100, 100, 6400);
    let query = MessageLookup::Offset {
        offset: 150,
        count: 10,
        ceiling: 200,
    };
    assert_eq!(partition.disk_poll_start(&query), (0, 6400, Some(100)));
}

fn recording_partition() -> (IggyPartition<RecordingBus>, SentFrames) {
    recording_partition_at(0, 1)
}

pub(super) fn recording_partition_at(
    replica: u8,
    replica_count: u8,
) -> (IggyPartition<RecordingBus>, SentFrames) {
    recording_partition_with_pipeline(replica, replica_count, LocalPipeline::new())
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

#[compio::test]
async fn given_committed_delete_fence_when_retrying_success_should_refuse_and_keep_watermark() {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition_at(0, 3);
    partition.runtime_options.durability = iggy_common::Durability::Replicated;
    let config = repair_config();
    let mut old = checksumless_send_request(partition.namespace(), 1);
    old.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
        .copy_from_slice(&build_segment_record(partition.namespace(), 0));
    partition.on_request(old.clone(), None).await;
    assert_eq!(partition.consensus.sequencer().current_sequence(), 1);
    partition.consensus.advance_commit_max(1);
    partition.commit_journal(&config).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(partition.consensus.commit_min(), 1);
    assert_eq!(partition.dedup.count(), 1);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    let transition = TransitionPartitionHistoryRequest {
        incarnation: partition.created_revision(),
        metadata_op: 10,
    };
    let fence = partition_control_request(
        &partition,
        Operation::TransitionPartitionHistory,
        &transition.to_bytes(),
        transition.metadata_op,
    );
    partition.on_request(fence, None).await;
    assert_eq!(partition.installed_history_transition(&transition), None);
    partition.consensus.advance_commit_max(2);
    partition.commit_journal(&config).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(partition.installed_history_transition(&transition), Some(2));
    assert_eq!(partition.offset_frontier(), 1);
    assert_eq!(partition.dedup.count(), 1);

    for request in [1, 2] {
        let retry = old
            .clone()
            .transmute_header(|mut header, next: &mut RoutedRequestHeader| {
                header.request = request;
                header.metadata_watermark = transition.metadata_op;
                *next = header;
            });
        let (sender, receiver) = consensus::oneshot_channel();
        partition.on_request(retry, Some(sender)).await;
        assert_eq!(
            receiver.await.unwrap().header().status,
            IggyError::HistoryUnavailable.as_code()
        );
        assert_eq!(partition.consensus.sequencer().current_sequence(), 2);
    }
    assert_eq!(partition.offset_frontier(), 1);
}

#[compio::test]
async fn given_stale_offset_repair_when_incarnation_changed_should_preserve_the_current_journal() {
    const INCARNATION: u64 = 7;
    let mut source = test_partition();
    source.set_created_revision(INCARNATION);
    journal_store_offset(&mut source, 2, 9, 42).await;
    let bytes = source.log.journal().inner.repair_entry(2).unwrap();
    let mut valid = Message::<PrepareHeader>::new(bytes.len());
    valid.as_mut_slice().copy_from_slice(bytes.as_slice());

    let mut partition = test_partition();
    partition.set_created_revision(INCARNATION);
    partition.consensus.restore_commit_state(1, 1);
    partition.consensus.sequencer().set_sequence(1);
    partition.repair = Some(armed_session(2, 1));
    for incarnation in [INCARNATION - 1, INCARNATION + 1] {
        let stale = valid
            .clone()
            .transmute_header(|mut original, header: &mut PrepareHeader| {
                original.partition_incarnation = incarnation;
                *header = original;
            });
        partition.apply_repaired_prepare(stale).await;
        assert!(partition.log.journal().inner.header_by_op(2).is_none());
        assert!(partition.pending_consumer_offset_commits.is_empty());
        assert_eq!(partition.consensus.sequencer().current_sequence(), 1);
    }

    partition.apply_repaired_prepare(valid).await;
    partition.consensus.advance_commit_max(2);
    partition.commit_journal(&repair_config()).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(partition.consensus.commit_min(), 2);
    assert_eq!(
        partition.get_consumer_offset(PollingConsumer::Consumer(9, 0)),
        Some(42)
    );
}

#[compio::test]
async fn given_invalid_repaired_send_when_ingesting_should_not_poison_the_journal() {
    const INCARNATION: u64 = 7;
    let mut partition = test_partition();
    partition.set_created_revision(INCARNATION);
    for (incarnation, corrupt) in [
        (INCARNATION - 1, false),
        (INCARNATION + 1, false),
        (INCARNATION, true),
    ] {
        let mut message = repaired_send_prepare(2, 0, 0).transmute_header(
            |mut original, header: &mut PrepareHeader| {
                original.partition_incarnation = incarnation;
                *header = original;
            },
        );
        if corrupt {
            let last = message.as_slice().len() - 1;
            message.as_mut_slice()[last] ^= 1;
        }
        assert!(
            partition
                .append_repaired_send_messages(message)
                .await
                .is_err()
        );
        assert!(partition.log.journal().inner.header_by_op(2).is_none());
        assert_eq!(partition.log.journal().info.messages_count, 0);
    }
    let valid = repaired_send_prepare(2, 0, 0).transmute_header(
        |mut original, header: &mut PrepareHeader| {
            original.partition_incarnation = INCARNATION;
            *header = original;
        },
    );
    assert!(partition.append_repaired_send_messages(valid).await.is_ok());
    assert!(partition.log.journal().inner.header_by_op(2).is_some());
}

#[compio::test]
async fn given_repair_window_with_delete_fence_when_completing_should_close_the_history() {
    const COMPLETION_STEPS_MAX: usize = 8;
    let prepares = fenced_history_prepares(1).await;
    let to_op = u64::try_from(prepares.len()).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let mut config = repair_config();
    config.path_layout.streams_root = directory.path().to_string_lossy().into_owned();
    let path = config.get_partition_path(1, 1, 0);
    std::fs::create_dir_all(&path).unwrap();
    let (mut replica, _) = recording_partition_at(1, 3);
    replica.set_partition_dir(path.clone());
    set_offset_dirs_under(&mut replica, std::path::Path::new(&path));
    replica.log.retire_front().unwrap();
    replica.install_empty_segment(&config, 0).await.unwrap();
    replica.open_persistence().await.unwrap();
    let persistence = Rc::clone(replica.persistence.as_ref().unwrap());
    replica.consensus().advance_commit_max(to_op);
    replica.repair = Some(armed_session(to_op, 0));
    for prepare in prepares {
        replica.apply_repaired_prepare(prepare).await;
    }

    let mut conclusion = RepairConclusion::InProgress;
    for _ in 0..COMPLETION_STEPS_MAX {
        conclusion = replica.complete_repair(&config).await;
        persistence
            .drain_with_timeout(compio::runtime::time::sleep)
            .await
            .unwrap();
        replica.drive_persistence().await;
        if conclusion == RepairConclusion::Done {
            break;
        }
    }
    assert!(
        persistence.failure().is_none(),
        "{:?}",
        persistence.failure()
    );
    assert!(replica.fatal().is_none(), "{:?}", replica.fatal());
    assert_eq!(conclusion, RepairConclusion::Done);
    assert_eq!(replica.consensus().commit_min(), to_op);
    assert!(replica.history_deleted());
    assert_eq!(replica.offset_frontier(), 1);
}

/// The prepares a primary journals for `sends_before` sends and the delete
/// fence that closes their history.
async fn fenced_history_prepares(sends_before: u64) -> Vec<Message<PrepareHeader>> {
    const METADATA_OP: u64 = 10;
    let config = repair_config();
    let directory = tempfile::tempdir().unwrap();
    let (mut primary, _) = recording_partition_at(0, 3);
    primary.runtime_options.durability = iggy_common::Durability::Replicated;
    primary.set_partition_dir(directory.path().to_string_lossy().into_owned());
    for request in 1..=sends_before {
        let send = history_send_request(&primary, request, METADATA_OP);
        primary.on_request(send, None).await;
    }
    primary.consensus.advance_commit_max(sends_before);
    primary.commit_journal(&config).await;
    let transition = TransitionPartitionHistoryRequest {
        incarnation: primary.created_revision(),
        metadata_op: METADATA_OP,
    };
    let fence = partition_control_request(
        &primary,
        Operation::TransitionPartitionHistory,
        &transition.to_bytes(),
        METADATA_OP,
    );
    primary.on_request(fence, None).await;
    primary.consensus.advance_commit_max(sends_before + 1);
    primary.commit_journal(&config).await;
    assert!(primary.fatal().is_none(), "{:?}", primary.fatal());
    assert!(primary.history_deleted());
    let head = primary.consensus.sequencer().current_sequence();
    assert_eq!(head, sends_before + 1);
    (1..=head)
        .map(|op| {
            let journaled = primary.log.journal().inner.repair_entry(op).unwrap();
            Message::<PrepareHeader>::try_from(Owned::copy_from_slice(journaled.as_slice()))
                .unwrap()
        })
        .collect()
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

// async fn commit_recorded_loopback(partition: &mut IggyPartition<RecordingBus>) {
//     partition.drive_persistence().await;
//     let mut acknowledgments = Vec::new();
//     partition
//         .consensus()
//         .drain_loopback_into(&mut acknowledgments);
//     for ack in acknowledgments {
//         partition
//             .on_ack(
//                 ack.try_into_typed::<PrepareOkHeader>().unwrap(),
//                 &repair_config(),
//             )
//             .await;
//     }
// }

//     /// A `SendMessages` request in the shape `convert_request_message` leaves at
//     /// [`ChecksumMode::Skip`]: canonical batch, `batch_checksum` zeroed.
//     fn checksumless_send_request(
//         namespace: IggyNamespace,
//         message_count: u32,
//     ) -> Message<RoutedRequestHeader> {
//         let mut batch = IggyMessages::with_capacity(message_count as usize);
//         for _ in 0..message_count {
//             batch.push(IggyMessage {
//                 header: IggyMessageHeader {
//                     payload_length: 8,
//                     ..Default::default()
//                 },
//                 payload: Bytes::from_static(b"abcdefgh"),
//                 user_headers: None,
//             });
//         }
//         let mut owned =
//             SendMessagesOwned::from_messages(namespace, &batch).expect("build send_messages batch");
//         owned.header.batch_checksum = 0;
//
//         let header_size = std::mem::size_of::<RoutedRequestHeader>();
//         let total = header_size + COMMAND_HEADER_SIZE + owned.blob.len();
//         let mut message = Message::<RoutedRequestHeader>::new(total);
//         let body = &mut message.as_mut_slice()[header_size..];
//         owned.header.encode_into(&mut body[..COMMAND_HEADER_SIZE]);
//         body[COMMAND_HEADER_SIZE..].copy_from_slice(&owned.blob);
//         message.transmute_header(|_, header: &mut RoutedRequestHeader| {
//             header.command = Command::Request;
//             header.operation = Operation::SendMessages;
//             header.client = 1;
//             header.session = 1;
//             header.request = 1;
//             header.group = namespace.inner();
//             header.size = u32::try_from(total).expect("request size fits u32");
//         })
//     }

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

#[compio::test]
async fn given_transfer_above_cap_when_file_write_fails_should_refuse_until_retry_succeeds() {
    let dir = tempfile::tempdir().expect("transfer directory");
    let consumers = dir.path().join("consumers");
    let groups = dir.path().join("groups");
    std::fs::File::create(&groups).expect("block group-file creation");
    let mut partition = test_partition();
    partition.set_partition_dir(dir.path().to_string_lossy().into_owned());
    partition.set_consumer_offsets_max(1);
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
        Some(consumers.to_string_lossy().into_owned());
    partition.consumer_offset_dirs[ConsumerKind::ConsumerGroup.index()] =
        Some(groups.to_string_lossy().into_owned());
    partition.consumer_offset_dirs[ConsumerKind::ExternalGroup.index()] = Some(
        dir.path()
            .join("external_groups")
            .to_string_lossy()
            .into_owned(),
    );
    let wire = crate::state_transfer::ConsumerOffsetsWire {
        owner_retirement: None,
        owners: Vec::new(),
        history_transition: None,
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        required_metadata_frontier: 0,
        prepare_checksum: None,
        checkpoint_prepare: Vec::new(),
        next_offset: 10,
        emptied_at: 0,
        consumers: vec![(7, 1), (8, 2)],
        groups: vec![(9, 3)],
        external_groups: Vec::new(),
        dedup: Vec::new(),
        retirement_paid_through: 0,
    };
    partition
        .install_state_transfer(&repair_config(), 12, Vec::new(), &wire.encode())
        .await
        .expect_err("incomplete offset state must not advance the commit floor");
    assert_eq!(partition.consensus.commit_min(), 0);
    assert_eq!(
        partition.log.segments().len(),
        1,
        "offset staging failure must preserve the existing segment chain"
    );
    assert_eq!(
        partition.durable_consumer_offset_count(ConsumerKind::Consumer),
        0
    );
    assert_eq!(
        partition.durable_consumer_offset_count(ConsumerKind::ConsumerGroup),
        0
    );
    std::fs::remove_file(&groups).expect("remove file-write fault");
    partition
        .install_state_transfer(&repair_config(), 12, Vec::new(), &wire.encode())
        .await
        .expect("retry must install accepted state above the local cap");
    assert_eq!(partition.consensus.commit_min(), 12);
    assert_eq!(
        partition.durable_consumer_offset_count(ConsumerKind::Consumer),
        2
    );
    assert_eq!(
        partition.durable_consumer_offset_count(ConsumerKind::ConsumerGroup),
        1
    );
    assert!(
        partition
            .reserve_consumer_offset(ConsumerKind::Consumer, 10)
            .is_err()
    );
    assert!(
        partition
            .durable_consumer_offsets
            .covers(ConsumerKind::ConsumerGroup, 9, 3)
    );
    assert_eq!(
        partition
            .offsets_wire_snapshot_for_test()
            .expect("durable snapshot"),
        vec![(7, 1), (8, 2)]
    );
}

#[compio::test]
async fn durability_combinations_share_body_ownership_and_keep_materialization_thresholds() {
    const SEGMENT_BYTES: u64 = 1024;
    for replicas in [1, 3] {
        for durability in [
            iggy_common::Durability::Replicated,
            iggy_common::Durability::Persisted,
        ] {
            for offset_durability in [
                iggy_common::Durability::Replicated,
                iggy_common::Durability::Persisted,
            ] {
                for byte_threshold in [false, true] {
                    let directory = tempfile::tempdir().unwrap();
                    let (mut partition, _) = recording_partition_at(0, replicas);
                    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
                    partition.runtime_options.durability = durability;
                    partition.runtime_options.consumer_offset_durability = offset_durability;
                    partition.runtime_options.preallocate_segments = Some(false);
                    partition.runtime_options.segment_size =
                        Some(IggyByteSize::from(SEGMENT_BYTES));
                    partition.log.segments_mut()[0].max_size = IggyByteSize::from(SEGMENT_BYTES);
                    partition.open_persistence().await.unwrap();
                    let persistence = partition.persistence.clone();
                    let owned = true;
                    assert_eq!(persistence.is_some(), owned);
                    let mut config = repair_config();
                    config.messages_required_to_save = if byte_threshold { u32::MAX } else { 2 };
                    partition.log.retire_front().unwrap();
                    partition.install_empty_segment(&config, 0).await.unwrap();
                    assert_eq!(partition.log.messages_writers()[0].is_none(), owned);
                    let mut expected = Vec::new();
                    let mut parent = 0;
                    for op in 1..=3 {
                        let namespace = partition.namespace();
                        let request = checksumless_send_request(namespace, 1);
                        let prepare =
                            request.transmute_header(|old, header: &mut PrepareHeader| {
                                header.command = Command::Prepare;
                                header.operation = Operation::SendMessages;
                                header.cluster = TEST_CLUSTER;
                                header.group = namespace.inner();
                                header.op = op;
                                header.parent = parent;
                                header.timestamp = op;
                                header.size = old.size;
                                header.retry_capacity =
                                    u32::try_from(consensus::PARTITION_DEDUP_CLIENTS_MAX).unwrap();
                                header.client = AUTO_COMMIT_CLIENT_ID;
                                header.session = 1;
                                header.request = op;
                            });
                        let prepare = partition
                            .stamp_and_append_messages(prepare)
                            .await
                            .unwrap()
                            .prepare;
                        let header = *bytemuck::checked::from_bytes::<PrepareHeader>(
                            &prepare.as_slice()[..size_of::<PrepareHeader>()],
                        );
                        parent = header.checksum;
                        expected
                            .extend_from_slice(&prepare.as_slice()[size_of::<PrepareHeader>()..]);
                        partition.consensus().sequencer().set_sequence(op);
                        assert!(
                            partition.submit_prepare_persistence(prepare, Operation::SendMessages)
                        );
                        if replicas > 1 {
                            assert_eq!(
                                partition.send_prepare_ok(&header).await,
                                !durability.is_persisted()
                            );
                        }
                        if let Some(persistence) = &persistence {
                            persistence
                                .drain_with_timeout(compio::runtime::time::sleep)
                                .await
                                .unwrap();
                            assert!(partition.send_prepare_ok(&header).await);
                        }
                        partition.consensus().advance_commit_max(op);
                        if op == 1 && byte_threshold {
                            config.size_of_messages_required_to_save = IggyByteSize::from(
                                2 * partition.log.journal().info.size.as_bytes_u64(),
                            );
                        }
                        partition.commit_messages(&config, op).await.unwrap();
                        if op == 1 {
                            assert_eq!(
                                partition
                                    .log
                                    .segments()
                                    .iter()
                                    .any(|segment| segment.size.as_bytes_u64() > 0),
                                replicas == 1 && durability.is_persisted(),
                                "replicas={replicas} durability={durability:?} offsets={offset_durability:?} byte_threshold={byte_threshold}",
                            );
                        } else if op == 2 {
                            assert_eq!(partition.log.journal().info.messages_count, 0);
                        }
                    }
                    if let Some(persistence) = &persistence {
                        assert_eq!(
                            persistence.durable_op(),
                            if durability.is_persisted() { 3 } else { 0 }
                        );
                    }
                    partition.flush_committed_messages(&config).await.unwrap();
                    let mut actual = Vec::new();
                    for segment in partition.log.segments() {
                        let public = directory
                            .path()
                            .join(format!("{:020}.log", segment.start_offset));
                        actual.extend(std::fs::read(&public).unwrap());
                        let metadata = std::fs::metadata(public).unwrap();
                        assert_eq!(metadata.len(), segment.size.as_bytes_u64());
                        #[cfg(target_os = "linux")]
                        if owned && metadata.len() > 0 {
                            let wal = directory
                                .path()
                                .join(format!("prepares-{}", partition.created_revision()));
                            assert!(
                                std::fs::read_dir(wal).unwrap().any(|entry| entry
                                    .unwrap()
                                    .metadata()
                                    .unwrap()
                                    .ino()
                                    == metadata.ino())
                            );
                        }
                    }
                    assert_eq!(
                        actual, expected,
                        "replicas={replicas} durability={durability:?} offsets={offset_durability:?} byte_threshold={byte_threshold}"
                    );
                    assert_eq!(partition.log.journal().info.messages_count, 0);
                    if let Some(persistence) = persistence {
                        assert!(persistence.segment_checkpoint().is_some());
                        assert_eq!(persistence.durable_op(), 3);
                    }
                }
            }
        }
    }
}

#[compio::test]
async fn wal_backpressure_preserves_replay_outcomes() {
    for expected_status in [
        0,
        IggyError::TransientNotCommitted.as_code(),
        IggyError::TransientNotAccepted.as_code(),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let (mut partition, replies) = recording_partition_at(0, 3);
        partition.runtime_options.durability = iggy_common::Durability::Persisted;
        partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
        partition.open_persistence().await.unwrap();
        let mut request = checksumless_send_request(partition.namespace(), 1);
        request.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
            .copy_from_slice(&build_segment_record(partition.namespace(), 0));
        let header = *request.header();
        let mut cached_reply = None;
        if expected_status == 0 {
            let prepare = PrepareHeader {
                client: header.client,
                session: header.session,
                request: header.request,
                request_checksum: header.request_checksum,
                operation: header.operation,
                op: 1,
                group: header.group,
                ..Default::default()
            };
            let reply = build_reply_message(
                &prepare,
                &send_messages_reply_body(
                    header.group,
                    Some(CommittedBatchStats {
                        base_offset: 0,
                        message_count: 1,
                        size_bytes: 0,
                    }),
                ),
            );
            cached_reply = Some(
                partition
                    .dedup
                    .commit_partition_reply(header.user_id, header.session, reply)
                    .unwrap()
                    .into_wire_bytes(),
            );
        } else if expected_status == IggyError::TransientNotCommitted.as_code() {
            let prepare =
                request
                    .clone()
                    .transmute_header(|request, prepare: &mut PrepareHeader| {
                        prepare.command = Command::Prepare;
                        prepare.operation = request.operation;
                        prepare.client = request.client;
                        prepare.user_id = request.user_id;
                        prepare.request = request.request;
                        prepare.session = request.session;
                        prepare.request_checksum = request.request_checksum;
                        prepare.retry_capacity =
                            u32::try_from(consensus::PARTITION_DEDUP_CLIENTS_MAX).unwrap();
                        prepare.op = 1;
                        prepare.size = request.size;
                    });
            partition
                .consensus()
                .pipeline_message(PlaneKind::Partitions, &prepare);
        }
        let pipeline_len = partition.consensus().pipeline_len();
        partition
            .persistence
            .as_ref()
            .unwrap()
            .exhaust_capacity_for_test();

        partition.on_request(request, None).await;

        let replies = replies.borrow();
        assert_eq!(
            replies.len(),
            1,
            "every retry must receive its known outcome"
        );
        let reply = bytemuck::checked::from_bytes::<ReplyHeader>(
            &replies[0].1.as_slice()[..size_of::<ReplyHeader>()],
        );
        assert_eq!(reply.status, expected_status);
        if let Some(cached) = cached_reply {
            assert!(replies[0].1.shares_allocation(&cached));
        }
        assert_eq!(partition.consensus().pipeline_len(), pipeline_len);
        assert_eq!(partition.persistence.as_ref().unwrap().head(), 0);
    }
}

#[compio::test]
async fn wal_backpressure_on_one_backup_does_not_stop_the_other_backup() {
    let (mut primary, _) = recording_partition_at(0, 3);
    let namespace = primary.namespace();
    let request = checksumless_send_request(namespace, 1);
    let prepare = request.transmute_header(|old, header: &mut PrepareHeader| {
        header.command = Command::Prepare;
        header.operation = Operation::SendMessages;
        header.cluster = TEST_CLUSTER;
        header.group = namespace.inner();
        header.op = 1;
        header.timestamp = 1;
        header.size = old.size;
    });
    let forwarded = primary
        .stamp_and_append_messages(prepare)
        .await
        .unwrap()
        .prepare;
    let message = Message::<PrepareHeader>::try_from(server_common::iobuf::Owned::copy_from_slice(
        forwarded.as_slice(),
    ))
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let (mut backup, _) = recording_partition_at(1, 3);
    backup.runtime_options.durability = iggy_common::Durability::Persisted;
    backup.set_partition_dir(directory.path().to_string_lossy().into_owned());
    backup.open_persistence().await.unwrap();
    backup
        .persistence
        .as_ref()
        .unwrap()
        .exhaust_capacity_for_test();
    let stalled_header = *message.header();
    backup.on_replicate(message).await;
    assert_eq!(backup.consensus().sequencer().current_sequence(), 1);
    assert_eq!(backup.persistence.as_ref().unwrap().head(), 0);
    assert!(
        backup
            .consensus()
            .message_bus()
            .sent_to_replicas
            .borrow()
            .is_empty()
    );
    let message =
        Message::<PrepareHeader>::try_from(Owned::copy_from_slice(forwarded.as_slice())).unwrap();
    primary.on_replicate(message).await;
    let healthy_copy = {
        let sent = primary.consensus().message_bus().sent_to_replicas.borrow();
        let (_, frame) = sent
            .iter()
            .find(|(target, _)| *target == 2)
            .expect("the primary sends directly to the other backup");
        Message::<PrepareHeader>::try_from(Owned::copy_from_slice(frame.as_slice())).unwrap()
    };
    let (mut healthy, _) = recording_partition_at(2, 3);
    healthy.on_replicate(healthy_copy).await;
    assert!(healthy.log.journal().inner.holds_op(1));

    assert!(!backup.register_rebuilt_ack(&stalled_header));
    assert!(backup.pending_persisted_acks.borrow().contains_key(&1));
    backup.drive_persistence().await;
    assert!(
        backup.pending_persisted_acks.borrow().contains_key(&1),
        "the WAL has not accepted the parked ack's prepare yet"
    );
    backup
        .persistence
        .as_ref()
        .unwrap()
        .release_capacity_for_test();
    let request = checksumless_send_request(namespace, 2);
    let prepare = request.transmute_header(|old, header: &mut PrepareHeader| {
        header.command = Command::Prepare;
        header.operation = Operation::SendMessages;
        header.cluster = TEST_CLUSTER;
        header.group = namespace.inner();
        header.op = 2;
        header.timestamp = 2;
        header.size = old.size;
    });
    let forwarded = primary
        .stamp_and_append_messages(prepare)
        .await
        .unwrap()
        .prepare;
    let message =
        Message::<PrepareHeader>::try_from(Owned::copy_from_slice(forwarded.as_slice())).unwrap();
    backup.on_replicate(message).await;
    let persistence = backup.persistence.as_ref().unwrap();
    assert_eq!(persistence.head(), 2);
    assert_eq!(backup.consensus().sequencer().current_sequence(), 2);
    assert!(persistence.failure().is_none());
    persistence
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();
    assert!(persistence.is_durable_through(2));
    backup.drive_persistence().await;
    assert!(
        backup
            .consensus()
            .message_bus()
            .sent_to_replicas
            .borrow()
            .iter()
            .any(|(target, message)| {
                *target == 0
                    && message.len() == size_of::<PrepareOkHeader>()
                    && bytemuck::checked::from_bytes::<PrepareOkHeader>(message.as_slice()).op == 1
            }),
        "the parked ack must be delivered without a prepare retransmit"
    );
}

/// The primary sends prepares before it commits any, so this backup's WAL
/// fills and refuses the last two, which stay journaled. The cluster then
/// commits past the WAL head. The backup must not commit past its WAL
/// head, and its own maintenance must reclaim the WAL so the refused
/// prepares persist and commit, with no capacity released from outside.
#[compio::test]
async fn given_full_backup_wal_when_cluster_commits_past_its_head_should_reclaim_and_catch_up() {
    // Four bodies of this size fit the smallest WAL, and a fifth does not.
    const PAYLOAD_BYTES: usize = 30 * 1024 * 1024;
    const WAL_HEAD: u64 = 4;
    const OPS: u64 = WAL_HEAD + 2;
    const ROUNDS_MAX: usize = 16;
    let payload = vec![0; PAYLOAD_BYTES];
    for durability in [
        iggy_common::Durability::Replicated,
        iggy_common::Durability::Persisted,
    ] {
        let directory = tempfile::tempdir().unwrap();
        // Checkpoints sync the segment paths the layout names.
        let mut config = repair_config();
        config.path_layout.streams_root = directory.path().to_string_lossy().into_owned();
        let partition_dir = config.get_partition_path(1, 1, 0);
        std::fs::create_dir_all(&partition_dir).unwrap();
        let (mut backup, _) = recording_partition_at(1, 3);
        set_offset_dirs_under(&mut backup, std::path::Path::new(&partition_dir));
        backup.set_partition_dir(partition_dir);
        backup.runtime_options.durability = durability;
        backup.log.retire_front().unwrap();
        backup.install_empty_segment(&config, 0).await.unwrap();
        backup
            .open_persistence_with_capacity(
                journal::partition_journal::PARTITION_WAL_CAPACITY_MIN,
                std::time::Duration::ZERO,
            )
            .await
            .unwrap();
        backup.set_io_notifier(
            Rc::new(|_, _| {}),
            crate::largest_legal_job_charge().unwrap(),
        );
        let completions = Rc::new(RefCell::new(Vec::new()));
        let captured = Rc::clone(&completions);
        backup.set_persistence_notifier(Rc::new(move |completion| {
            captured.borrow_mut().push(completion);
        }));
        let persistence = Rc::clone(backup.persistence.as_ref().unwrap());

        let mut parent = 0;
        let mut frame_bytes = 0;
        for op in 1..=OPS {
            let prepare = checksummed_segment_prepare(op, parent, op - 1, &payload);
            parent = prepare.header().checksum;
            frame_bytes = prepare.as_slice().len();
            backup.on_replicate(prepare).await;
            backup.commit_journal(&config).await;
            run_shard_round(&mut backup, &completions, &config).await;
        }
        assert_eq!(
            backup.consensus().sequencer().current_sequence(),
            OPS,
            "{durability:?}: the backup journals every prepare"
        );
        assert_eq!(
            persistence.head(),
            WAL_HEAD,
            "{durability:?}: the WAL refuses prepares past its capacity"
        );
        assert!(!persistence.has_capacity(frame_bytes), "{durability:?}");

        // A commit heartbeat: the other backup's acks committed every op.
        backup.consensus().advance_commit_max(OPS);
        backup.commit_journal(&config).await;

        for _ in 0..ROUNDS_MAX {
            assert!(
                backup.consensus().commit_min() <= persistence.head(),
                "{durability:?}: commit {} passed the WAL head {}",
                backup.consensus().commit_min(),
                persistence.head()
            );
            if backup.consensus().commit_min() == OPS {
                break;
            }
            run_shard_round(&mut backup, &completions, &config).await;
            assert!(
                backup.fatal().is_none(),
                "{durability:?}: {:?}",
                backup.fatal()
            );
        }
        assert_eq!(
            (backup.consensus().commit_min(), persistence.head()),
            (OPS, OPS),
            "{durability:?}: wedged with checkpoint {}, WAL capacity {}, checkpoint due {}",
            persistence.checkpoint_op(),
            persistence.has_capacity(frame_bytes),
            backup.needs_persistence_checkpoint()
        );
        assert!(
            persistence.checkpoint_op() >= WAL_HEAD,
            "{durability:?}: only a checkpoint can free the full WAL"
        );
    }
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

#[compio::test]
async fn persisted_singleton_with_failed_segment_directory_sync_withholds_reply() {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, replies) = recording_partition_at(0, 1);
    partition.runtime_options.durability = iggy_common::Durability::Persisted;
    partition.set_partition_dir(
        directory
            .path()
            .join("missing")
            .to_string_lossy()
            .into_owned(),
    );
    partition.consensus().advance_commit_max(1);
    let header = PrepareHeader {
        retry_capacity: u32::try_from(consensus::PARTITION_DEDUP_CLIENTS_MAX).unwrap(),
        session: 1,
        command: Command::Prepare,
        operation: Operation::SendMessages,
        op: 1,
        group: partition.consensus().group(),
        client: 1,
        request: 1,
        ..PrepareHeader::default()
    };
    partition
        .handle_committed_entries(vec![header], &repair_config(), true)
        .await;
    assert!(partition.fatal().is_some());
    assert_eq!(partition.consensus().commit_min(), 0);
    assert!(replies.borrow().is_empty());
}

#[compio::test]
async fn persisted_singleton_skips_directory_sync_after_names_are_published() {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition_at(0, 1);
    partition.runtime_options.durability = iggy_common::Durability::Persisted;
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    for op in 1..=2 {
        partition.consensus().advance_commit_max(op);
        let prepare = checksummed_segment_prepare(op, 0, op - 1, b"payload");
        let header = *prepare.header();
        partition
            .log
            .journal()
            .inner
            .append(prepare.into_frozen())
            .await
            .unwrap();
        partition
            .handle_committed_entries(vec![header], &repair_config(), true)
            .await;
        assert!(partition.fatal().is_none());
        assert_eq!(partition.consensus().commit_min(), op);
        assert!(!partition.segment_names_dirty.get());
        if op == 1 {
            std::fs::remove_dir(directory.path()).unwrap();
        }
    }
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    assert!(partition.segment_names_dirty.get());
}

#[compio::test]
async fn given_committed_deletes_when_drained_together_should_sync_directory_once_before_replies() {
    let dir = tempfile::tempdir().unwrap();
    let (mut partition, sent) = recording_partition_at(0, 3);
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
        Some(dir.path().to_string_lossy().into_owned());
    partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Persisted;
    let mut drained = Vec::new();
    for id in 1..=2 {
        partition
            .persist_consumer_offset_commit(PendingConsumerOffsetCommit::upsert(
                ConsumerKind::Consumer,
                id,
                0,
            ))
            .await
            .unwrap();
        partition.stage_consumer_offset_delete(u64::from(id), ConsumerKind::Consumer, id);
        let header = PrepareHeader {
            retry_capacity: u32::try_from(consensus::PARTITION_DEDUP_CLIENTS_MAX).unwrap(),
            session: 1,
            op: u64::from(id),
            operation: Operation::DeleteConsumerOffset,
            client: 42,
            request: u64::from(id),
            ..Default::default()
        };
        drained.push(header);
    }
    partition.consensus.restore_commit_state(0, 2);
    partition
        .handle_committed_entries(drained, &repair_config(), true)
        .await;
    assert_eq!(partition.offset_dir_sync_count.get(), 1);
    assert_eq!(partition.consensus.commit_min(), 2);
    assert_eq!(sent.borrow().len(), 2);
    assert!(!dir.path().join("1").exists());
    assert!(!dir.path().join("2").exists());
}

#[compio::test]
async fn deferred_views_keep_newer_progress_without_replacing_history_transitions() {
    let (mut partition, _) = recording_partition_at(0, 5);
    let start_change = |view, replica| {
        server_common::MessageBag::StartViewChange(
            Message::<iggy_binary_protocol::StartViewChangeHeader>::new(size_of::<
                iggy_binary_protocol::StartViewChangeHeader,
            >())
            .transmute_header(
                |_, header: &mut iggy_binary_protocol::StartViewChangeHeader| {
                    header.command = Command::StartViewChange;
                    header.size =
                        u32::try_from(size_of::<iggy_binary_protocol::StartViewChangeHeader>())
                            .unwrap();
                    header.view = view;
                    header.replica = replica;
                },
            ),
        )
    };
    let start_view = |view| {
        server_common::MessageBag::StartView(
            Message::<iggy_binary_protocol::StartViewHeader>::new(size_of::<
                iggy_binary_protocol::StartViewHeader,
            >())
            .transmute_header(
                |_, header: &mut iggy_binary_protocol::StartViewHeader| {
                    header.command = Command::StartView;
                    header.size =
                        u32::try_from(size_of::<iggy_binary_protocol::StartViewHeader>()).unwrap();
                    header.view = view;
                },
            ),
        )
    };
    partition.defer_view_transition(start_change(1, 2));
    partition.defer_view_transition(start_view(1));
    partition.defer_view_transition(start_change(1, 1));
    partition.defer_view_transition(start_change(1, 2));
    partition.defer_view_transition(start_change(1, 5));
    partition.defer_view_transition(start_view(0));
    for replica in [1, 2] {
        assert!(matches!(partition.resume_io(&repair_config()).await,
                crate::PartitionIoStep::Transition(server_common::MessageBag::StartViewChange(message))
                if message.header().view == 1 && message.header().replica == replica));
    }
    assert!(matches!(partition.resume_io(&repair_config()).await,
            crate::PartitionIoStep::Transition(server_common::MessageBag::StartView(message))
            if message.header().view == 1));
    assert!(partition.deferred_views.is_empty());
    partition.defer_view_transition(start_view(1));
    partition.defer_view_transition(start_change(2, 1));
    assert!(matches!(partition.resume_io(&repair_config()).await,
            crate::PartitionIoStep::Transition(server_common::MessageBag::StartView(message))
            if message.header().view == 1));
    assert!(matches!(partition.resume_io(&repair_config()).await,
            crate::PartitionIoStep::Transition(server_common::MessageBag::StartViewChange(message))
            if message.header().view == 2));
    partition.transition = Some(PendingPartitionTransition::Truncate {
        ops: 3..=u64::MAX,
        actions: Vec::new(),
        peer: None,
        drain: None,
    });
    partition.defer_view_transition(start_view(3));
    assert!(matches!(
        partition.transition.as_ref(),
        Some(PendingPartitionTransition::Truncate { ops, .. }) if *ops == (3..=u64::MAX)
    ));
    partition.defer_view_transition(start_view(4));
    partition.transition = None;
    assert!(matches!(partition.resume_io(&repair_config()).await,
            crate::PartitionIoStep::Transition(server_common::MessageBag::StartView(message))
            if message.header().view == 4));
    assert!(partition.deferred_views.is_empty());
}

#[compio::test]
async fn missing_materialization_allows_deferred_recovery_control() {
    let (mut partition, _) = recording_partition();
    partition.materialization_missing = true;
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    let message = Message::<iggy_binary_protocol::RequestStartViewHeader>::new(size_of::<
        iggy_binary_protocol::RequestStartViewHeader,
    >());
    partition.defer_view_transition(server_common::MessageBag::RequestStartView(message));
    assert!(
        matches!(
            partition.resume_io(&repair_config()).await,
            crate::PartitionIoStep::Transition(server_common::MessageBag::RequestStartView(_))
        ),
        "recovery control must not be blocked by the state it repairs"
    );
}

#[compio::test]
async fn transfer_install_waits_for_captured_writer_before_replacing_history() {
    let directory = tempfile::tempdir().unwrap();
    let config = repair_config();
    let mut partition = partition_at_view(1, 1);
    partition.set_superblock(Rc::new(RecordingSuperblock::default()), None);
    partition.set_partition_dir(directory.path().to_str().unwrap().to_owned());
    partition.configure_consumer_offset_storage(
        ["consumers", "groups", "external_groups"]
            .map(|name| directory.path().join(name).to_str().unwrap().to_owned()),
        ConsumerOffsets::with_capacity(1),
        ConsumerGroupOffsets::with_capacity(1),
    );
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    assert!(!partition.persist_superblock_if_needed().await);
    let crate::PartitionIoStep::Ready(plan) = partition.resume_io(&config).await else {
        panic!("the current view must reach its superblock write");
    };
    let captured = partition.capture_io(plan, &config).unwrap().unwrap();
    let old_history = partition.poll_history;
    for stage in [
        consensus::StateTransferStage::AwaitingTarget,
        consensus::StateTransferStage::Fetching,
        consensus::StateTransferStage::Installing,
    ] {
        partition.consensus().set_state_transfer_stage(stage);
    }
    let offsets = crate::state_transfer::ConsumerOffsetsWire {
        dedup_capacity: consensus::PARTITION_DEDUP_CLIENTS_MAX,
        ..Default::default()
    }
    .encode();
    partition
        .queue_state_transfer_install(1, Vec::new(), &offsets, 1)
        .unwrap();
    partition.begin_shutdown_io();
    assert!(matches!(
        partition.queue_state_transfer_install(2, Vec::new(), &offsets, 1),
        Err(crate::state_transfer::PartitionInstallError::ShuttingDown)
    ));
    assert!(matches!(
        partition.resume_io(&config).await,
        crate::PartitionIoStep::Pending
    ));
    let result = captured.job.execute().await;
    assert!(matches!(
        partition.resume_io(&config).await,
        crate::PartitionIoStep::Pending
    ));
    assert_eq!(partition.poll_history, old_history);
    assert_eq!(partition.consensus().commit_min(), 0);
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    partition.accept_io(captured.identity, result).unwrap();
    captured.gate.unwrap().release();

    let outcome = finish_install_io(&mut partition).await.unwrap();
    assert_eq!(outcome.applied_commit_op, 1);
    assert_eq!(partition.consensus().commit_min(), 1);
    assert_ne!(partition.poll_history, old_history);
    assert!(partition.fatal().is_none());
    assert_eq!(
        std::fs::metadata(directory.path().join("00000000000000000000.log"))
            .unwrap()
            .len(),
        0
    );
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

#[compio::test]
async fn installed_offsets_survive_a_materialization_marker_unlink_failure() {
    const MARKER: &str = "materialization.missing";
    for wal in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let mut partition = partition_at_view(1, 1);
        partition.set_partition_dir(directory.path().to_str().unwrap().to_owned());
        if wal {
            partition.runtime_options.durability = iggy_common::Durability::Persisted;
            partition.open_persistence().await.unwrap();
        }
        let consumers = directory.path().join("consumers");
        let dirs = ["consumers", "groups", "external_groups"]
            .map(|name| directory.path().join(name).to_str().unwrap().to_owned());
        for dir in &dirs {
            std::fs::create_dir(dir).unwrap();
        }
        partition.configure_consumer_offset_storage(
            dirs,
            ConsumerOffsets::with_capacity(1),
            ConsumerGroupOffsets::with_capacity(1),
        );
        partition.materialization_missing = true;
        let marker = directory.path().join(MARKER);
        std::fs::create_dir(&marker).unwrap();
        let prepare = checksummed_segment_prepare(1, 0, 8, b"installed");
        let wire = crate::state_transfer::ConsumerOffsetsWire {
            dedup_capacity: consensus::PARTITION_DEDUP_CLIENTS_MAX,
            prepare_checksum: Some(prepare.header().checksum),
            checkpoint_prepare: prepare.as_slice().to_vec(),
            next_offset: 9,
            consumers: vec![(7, 4)],
            ..Default::default()
        };
        partition.set_io_notifier(
            Rc::new(|_, _| {}),
            crate::largest_legal_job_charge().unwrap(),
        );
        partition
            .queue_state_transfer_install(1, Vec::new(), &wire.encode(), 1)
            .unwrap();
        assert!(finish_install_io(&mut partition).await.is_err());
        assert_eq!(partition.consensus.commit_min(), 1);
        assert!(
            partition.fatal().is_none(),
            "marker cleanup must not fence a published install"
        );
        assert!(partition.materialization_missing);
        assert!(marker.is_dir());
        assert!(
            consumers.join("7").is_file(),
            "published offsets must survive cleanup failure"
        );
        assert_eq!(
            partition.durable_consumer_offset_count(ConsumerKind::Consumer),
            1
        );
    }
}

/// A fresh backup's sequencer is still 0 between the install's WAL reset and
/// the publication of the installed consensus.
#[compio::test]
async fn given_a_fresh_backup_when_the_tick_runs_during_an_install_should_certify_the_installed_head()
 {
    let directory = tempfile::tempdir().unwrap();
    let mut partition = partition_at_view(1, 1);
    partition.set_partition_dir(directory.path().to_str().unwrap().to_owned());
    partition.runtime_options.durability = iggy_common::Durability::Persisted;
    partition.open_persistence().await.unwrap();
    let dirs = ["consumers", "groups", "external_groups"]
        .map(|name| directory.path().join(name).to_str().unwrap().to_owned());
    for dir in &dirs {
        std::fs::create_dir(dir).unwrap();
    }
    partition.configure_consumer_offset_storage(
        dirs,
        ConsumerOffsets::with_capacity(1),
        ConsumerGroupOffsets::with_capacity(1),
    );
    let prepare = checksummed_segment_prepare(1, 0, 8, b"installed");
    let wire = crate::state_transfer::ConsumerOffsetsWire {
        dedup_capacity: consensus::PARTITION_DEDUP_CLIENTS_MAX,
        prepare_checksum: Some(prepare.header().checksum),
        checkpoint_prepare: prepare.as_slice().to_vec(),
        next_offset: 9,
        ..Default::default()
    };
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    partition
        .queue_state_transfer_install(1, Vec::new(), &wire.encode(), 1)
        .unwrap();

    let outcome = finish_install_io(&mut partition).await;
    assert_eq!(outcome.unwrap().applied_commit_op, 1);
    assert!(partition.fatal().is_none());
    assert_eq!(
        partition.persistence.as_ref().unwrap().certified_log_view(),
        Some(1)
    );
}

#[compio::test]
async fn install_refuses_an_unreadable_offset_directory() {
    let directory = tempfile::tempdir().unwrap();
    let mut partition = partition_at_view(1, 1);
    partition.set_partition_dir(directory.path().to_str().unwrap().to_owned());
    let consumers = directory.path().join("consumers");
    std::fs::write(&consumers, b"not a directory").unwrap();
    partition.consumer_offset_dirs = ["consumers", "groups", "external_groups"]
        .map(|name| Some(directory.path().join(name).to_str().unwrap().to_owned()));
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    partition
        .queue_state_transfer_install(
            1,
            Vec::new(),
            &crate::state_transfer::ConsumerOffsetsWire {
                dedup_capacity: consensus::PARTITION_DEDUP_CLIENTS_MAX,
                ..Default::default()
            }
            .encode(),
            1,
        )
        .unwrap();
    let outcome = finish_install_io(&mut partition).await;
    assert!(
        outcome.is_err(),
        "an unreadable offset directory must fail installation: {outcome:?}"
    );
}

#[compio::test]
async fn given_old_offsets_when_install_replaces_them_should_keep_cleanup_keys_and_batch_renames() {
    const OLD_CONSUMER: u32 = 99;
    const INCOMING_OFFSETS: u32 = 33;
    const STEPS_MAX: usize = 128;
    let directory = tempfile::tempdir().unwrap();
    let mut partition = partition_at_view(1, 1);
    partition.set_partition_dir(directory.path().to_str().unwrap().to_owned());
    partition.open_persistence().await.unwrap();
    let dirs = ["consumers", "groups", "external_groups"]
        .map(|name| directory.path().join(name).to_str().unwrap().to_owned());
    for dir in &dirs {
        std::fs::create_dir(dir).unwrap();
    }
    let consumers = dirs[0].clone();
    partition.configure_consumer_offset_storage(
        dirs,
        ConsumerOffsets::with_capacity(1),
        ConsumerGroupOffsets::with_capacity(1),
    );
    let old = PendingConsumerOffsetCommit::upsert(ConsumerKind::Consumer, OLD_CONSUMER, 4);
    partition.persist_consumer_offset_commit(old).await.unwrap();
    partition.apply_consumer_offset_commit(old);
    let prepare = checksummed_segment_prepare(1, 0, 8, b"installed");
    let wire = crate::state_transfer::ConsumerOffsetsWire {
        dedup_capacity: consensus::PARTITION_DEDUP_CLIENTS_MAX,
        prepare_checksum: Some(prepare.header().checksum),
        checkpoint_prepare: prepare.as_slice().to_vec(),
        next_offset: 9,
        consumers: (0..INCOMING_OFFSETS).map(|id| (id, 4)).collect(),
        ..Default::default()
    };
    partition.materialization_missing = true;
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    partition
        .queue_state_transfer_install(1, Vec::new(), &wire.encode(), 1)
        .unwrap();
    let config = repair_config();
    let mut cleaned = false;
    let mut batches = Vec::new();
    let mut installed = false;
    for _ in 0..STEPS_MAX {
        let _ = partition.persist_superblock_if_needed().await;
        match partition.resume_io(&config).await {
            crate::PartitionIoStep::Ready(plan) => {
                let captured = partition.capture_io(plan, &config).unwrap().unwrap();
                if let crate::PartitionIoJob::OffsetCleanup {
                    directory, known, ..
                } = &captured.job
                    && *directory == consumers
                {
                    assert!(
                        known.contains(&OLD_CONSUMER),
                        "a directory scan failure must still leave a known cleanup path"
                    );
                    cleaned = true;
                }
                if let crate::PartitionIoJob::Transfer(crate::io::TransferFileJob::CommitOffsets(
                    paths,
                )) = &captured.job
                {
                    batches.push(paths.len());
                }
                let result = captured.job.execute().await;
                partition.accept_io(captured.identity, result).unwrap();
                if let Some(gate) = captured.gate {
                    gate.release();
                }
                captured.quiescence.settle(captured.identity);
            }
            crate::PartitionIoStep::Progress => {}
            crate::PartitionIoStep::Pending => {
                partition
                    .persistence
                    .as_ref()
                    .unwrap()
                    .drain_with_timeout(compio::runtime::time::sleep)
                    .await
                    .unwrap();
            }
            crate::PartitionIoStep::InstallFinished { outcome, .. } => {
                outcome.unwrap();
                installed = true;
                break;
            }
            _ => panic!("unexpected install continuation"),
        }
    }
    assert!(installed && cleaned);
    assert_eq!(batches, [16, 16, 1]);
    assert!(
        !std::path::Path::new(&consumers)
            .join(OLD_CONSUMER.to_string())
            .exists()
    );
    assert_eq!(
        partition.durable_consumer_offset_count(ConsumerKind::Consumer),
        INCOMING_OFFSETS as usize
    );
    for id in 0..INCOMING_OFFSETS {
        assert_eq!(
            crate::offset_storage::read_offset_max(&format!("{consumers}/{id}"), 0)
                .await
                .unwrap()
                .offset,
            4
        );
    }
}

/// The durable table does not list a file the partition failed to load or
/// unlink, so the install cleanup has to budget for its stranded id and
/// then clear it.
#[compio::test]
async fn given_durable_and_stranded_offsets_when_install_cleans_old_offsets_should_reserve_and_release_both()
 {
    const DURABLE: u32 = 7;
    const STRANDED: u32 = 42;
    const STEPS_MAX: usize = 128;
    let (_directory, mut partition) = transfer_receiver();
    for kind in ConsumerKind::ALL {
        partition
            .durable_consumer_offsets
            .record_explicit(kind, DURABLE, 0, 0);
        partition
            .consumer_offset_capacity_for(kind)
            .record_stranded(STRANDED);
    }
    let file_phase_charge = crate::io::file_phase_charge::<PingPongSuperblock>(
        std::iter::once(partition.partition_dir.as_deref())
            .chain(ConsumerKind::ALL.map(|kind| partition.consumer_offsets_dir(kind)))
            .flatten(),
    )
    .unwrap();
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    // An offer without offsets retains no ids, so only the cleanup
    // reservation can cover the known set.
    let wire = crate::state_transfer::ConsumerOffsetsWire {
        dedup_capacity: consensus::PARTITION_DEDUP_CLIENTS_MAX,
        next_offset: 1,
        ..Default::default()
    };
    partition
        .queue_state_transfer_install(1, Vec::new(), &wire.encode(), 1)
        .unwrap();
    let config = repair_config();
    let mut cleanups = 0;
    let mut installed = false;
    for _ in 0..STEPS_MAX {
        match partition.resume_io(&config).await {
            crate::PartitionIoStep::Ready(plan) => {
                let captured = partition.capture_io(plan, &config).unwrap().unwrap();
                if let crate::PartitionIoJob::OffsetCleanup { known, .. } = &captured.job {
                    assert_eq!(
                        *known,
                        HashSet::from([DURABLE, STRANDED]),
                        "the cleanup must know the durable and the stranded file"
                    );
                    assert!(
                        plan.allocation_charge
                            >= file_phase_charge + known.capacity() * size_of::<u32>(),
                        "the plan must reserve the known set on top of the file phase"
                    );
                    cleanups += 1;
                }
                let result = captured.job.execute().await;
                partition.accept_io(captured.identity, result).unwrap();
                if let Some(gate) = captured.gate {
                    gate.release();
                }
            }
            crate::PartitionIoStep::Progress => {}
            crate::PartitionIoStep::InstallFinished { outcome, .. } => {
                outcome.unwrap();
                installed = true;
                break;
            }
            _ => panic!("unexpected install continuation"),
        }
    }
    assert!(installed);
    assert_eq!(cleanups, ConsumerKind::COUNT);
    for kind in ConsumerKind::ALL {
        assert_eq!(
            partition.stranded_consumer_offset_count(kind),
            0,
            "the install must release the stranded {kind:?} id"
        );
    }
}

#[compio::test]
#[allow(clippy::too_many_lines)]
async fn queued_delete_observes_held_no_ack_store_success_and_failures() {
    const CLIENT: u128 = 42;
    const CONSUMER: u32 = 7;
    const MAX_STEPS: usize = 256;
    for delete_ack in [AckLevel::NoAck, AckLevel::Quorum] {
        for (fail_write, fail_barrier) in [(false, false), (true, false), (false, true)] {
            let directory = tempfile::tempdir().unwrap();
            let offset_directory = directory.path().join("consumers");
            let (mut partition, replies) = recording_partition();
            partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
                Some(offset_directory.to_string_lossy().into_owned());
            partition.runtime_options.consumer_offset_durability =
                iggy_common::Durability::Persisted;
            partition.stats.increment_messages_count(1);
            partition.set_io_notifier(
                Rc::new(|_, _| {}),
                crate::largest_legal_job_charge().unwrap(),
            );
            let config = repair_config();
            partition
                .on_request(
                    store_offset_request(
                        CLIENT,
                        1,
                        ConsumerKind::Consumer,
                        CONSUMER,
                        0,
                        AckLevel::NoAck,
                    ),
                    None,
                )
                .await;
            let first = loop {
                match partition.resume_io(&config).await {
                    crate::PartitionIoStep::Progress => {}
                    crate::PartitionIoStep::Ready(plan) => {
                        break partition.capture_io(plan, &config).unwrap().unwrap();
                    }
                    _ => panic!("admitted store did not reach file execution"),
                }
            };
            assert_eq!(
                first.identity.continuation,
                crate::PartitionIoContinuation::NoAck
            );
            partition
                .on_request(
                    delete_offset_request_with_ack(
                        CLIENT,
                        2,
                        ConsumerKind::Consumer,
                        CONSUMER,
                        delete_ack,
                    ),
                    None,
                )
                .await;
            assert!(
                replies.borrow().is_empty(),
                "delete must wait for the new key's predecessor"
            );
            assert_eq!(
                partition.occupied_consumer_offset_count(ConsumerKind::Consumer),
                1
            );
            assert!(
                !partition
                    .consumer_offsets
                    .pin()
                    .contains_key(&(CONSUMER as usize))
            );
            assert!(matches!(
                partition.resume_io(&config).await,
                crate::PartitionIoStep::Pending
            ));
            if fail_write {
                std::fs::write(&offset_directory, b"not a directory").unwrap();
            }
            if fail_barrier {
                partition.consumer_offset_dir_sync_fault.set(Some(0));
            }
            let result = first.job.execute().await;
            partition.accept_io(first.identity, result).unwrap();
            assert!(replies.borrow().is_empty());
            assert_eq!(
                partition
                    .consumer_offsets
                    .pin()
                    .contains_key(&(CONSUMER as usize)),
                !fail_write
            );
            if fail_write {
                assert!(
                    partition
                        .consumer_offset_capacity_for(ConsumerKind::Consumer)
                        .is_stranded(CONSUMER)
                );
                std::fs::remove_file(&offset_directory).unwrap();
                std::fs::create_dir(&offset_directory).unwrap();
            }
            for _ in 0..MAX_STEPS {
                let mut loopbacks = Vec::new();
                partition.consensus.drain_loopback_into(&mut loopbacks);
                for ack in loopbacks {
                    partition
                        .on_ack(ack.try_into_typed().unwrap(), &config)
                        .await;
                }
                match partition.resume_io(&config).await {
                    crate::PartitionIoStep::Progress => {}
                    crate::PartitionIoStep::Ready(plan) => {
                        let captured = partition.capture_io(plan, &config).unwrap().unwrap();
                        let directory_barrier =
                            matches!(&captured.job, crate::PartitionIoJob::OffsetDirectories(_));
                        let result = captured.job.execute().await;
                        partition.accept_io(captured.identity, result).unwrap();
                        if directory_barrier {
                            partition.consumer_offset_dir_sync_fault.set(None);
                        }
                    }
                    crate::PartitionIoStep::Pending if replies.borrow().len() == 2 => break,
                    _ => panic!("ordered offset work stalled before both replies"),
                }
            }
            assert_eq!(replies.borrow().len(), 2);
            let statuses: Vec<_> = replies
                .borrow()
                .iter()
                .map(|(_, reply)| {
                    bytemuck::checked::from_bytes::<ReplyHeader>(
                        &reply.as_slice()[..size_of::<ReplyHeader>()],
                    )
                    .status
                })
                .collect();
            assert_eq!(statuses[0] == 0, !fail_write && !fail_barrier);
            assert_eq!(
                statuses[1], 0,
                "delete must clean visible or stranded predecessor state"
            );
            assert!(!offset_directory.join(CONSUMER.to_string()).exists());
            assert!(
                !partition
                    .consumer_offsets
                    .pin()
                    .contains_key(&(CONSUMER as usize))
            );
            assert_eq!(
                partition.occupied_consumer_offset_count(ConsumerKind::Consumer),
                0
            );
            assert!(
                partition.fatal().is_none(),
                "NoAck failure remains request-scoped"
            );
        }
    }
}

#[compio::test]
async fn given_offset_phases_when_barrier_settles_should_publish_ordered_effects_and_release_replies()
 {
    const CLIENT: u128 = 42;
    const CONSUMER: u32 = 7;
    const INITIAL_OFFSET: u64 = 9;
    let kind = ConsumerKind::Consumer;
    'cases: for (fail_barrier, fail_write) in [(false, false), (true, false), (false, true)] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join(CONSUMER.to_string());
        let (mut partition, bus_replies) = recording_partition_at(0, 3);
        partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
            Some(directory.path().to_string_lossy().into_owned());
        partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Persisted;
        let initial = PendingConsumerOffsetCommit::upsert(kind, CONSUMER, INITIAL_OFFSET);
        partition
            .persist_consumer_offset_commit(initial)
            .await
            .unwrap();
        partition.apply_consumer_offset_commit(initial);
        let mutations = [
            PendingConsumerOffsetCommit::upsert(kind, CONSUMER, 3),
            PendingConsumerOffsetCommit::upsert(kind, CONSUMER + 1, 4),
            PendingConsumerOffsetCommit::upsert_auto_commit(kind, CONSUMER + 2, 5),
            PendingConsumerOffsetCommit::upsert_auto_commit(kind, CONSUMER, 2),
            PendingConsumerOffsetCommit::delete(kind, CONSUMER),
            PendingConsumerOffsetCommit::upsert_auto_commit(kind, CONSUMER, 1),
        ];
        let mut receivers = Vec::new();
        let mut parent = 0;
        for (index, pending) in mutations.into_iter().enumerate() {
            let op = u64::try_from(index + 1).unwrap();
            let client = if pending.auto_commit {
                AUTO_COMMIT_CLIENT_ID
            } else {
                CLIENT
            };
            let request = match pending.mutation {
                PendingConsumerOffsetMutation::Upsert(offset) => store_offset_request(
                    client,
                    op,
                    kind,
                    pending.consumer_id,
                    offset,
                    AckLevel::Quorum,
                ),
                PendingConsumerOffsetMutation::Delete => {
                    delete_offset_request(client, op, CONSUMER)
                }
            };
            let prepare = request.transmute_header(|request, header: &mut PrepareHeader| {
                *header = PrepareHeader {
                    command: Command::Prepare,
                    operation: request.operation,
                    cluster: TEST_CLUSTER,
                    group: request.group,
                    size: request.size,
                    client,
                    user_id: request.user_id,
                    session: request.session,
                    request: op,
                    retry_capacity: u32::try_from(consensus::PARTITION_DEDUP_CLIENTS_MAX).unwrap(),
                    op,
                    parent,
                    ..PrepareHeader::default()
                };
                header.checksum = header.identity_checksum();
            });
            parent = prepare.header().checksum;
            let (entry, receiver) = PipelineEntry::with_subscriber(*prepare.header());
            receivers.push(receiver);
            partition
                .consensus
                .with_pipeline_mut(|pipeline| pipeline.push(entry));
            partition
                .log
                .journal()
                .inner
                .append(prepare.into_frozen())
                .await
                .unwrap();
            match pending.mutation {
                PendingConsumerOffsetMutation::Upsert(offset) => {
                    partition.stage_consumer_offset_upsert(
                        op,
                        kind,
                        pending.consumer_id,
                        offset,
                        pending.auto_commit,
                    );
                }
                PendingConsumerOffsetMutation::Delete => {
                    partition.stage_consumer_offset_delete(op, kind, CONSUMER);
                }
            }
        }
        let through_op = u64::try_from(mutations.len()).unwrap();
        partition.consensus.restore_commit_state(0, through_op);
        let config = repair_config();
        let selected = partition.select_persistable_commits(&config);
        assert_eq!(selected.len(), mutations.len());
        assert!(partition.select_commit(selected, &config, true));
        if fail_barrier {
            partition
                .consumer_offset_dir_sync_fault
                .set(Some(kind.index()));
        }
        let mut written_offsets = Vec::new();
        let mut deletes = 0;
        let mut barriers = 0;
        let mut batches = 0;
        loop {
            match partition.drive_commit().unwrap() {
                CommitStep::Progress => {}
                CommitStep::Pending => panic!("the test owns every submitted job"),
                CommitStep::Publish => break,
                CommitStep::Ready => {
                    assert_eq!(partition.consensus.pipeline_len(), mutations.len());
                    assert!(partition.consensus.with_pipeline(|pipeline| {
                        (1..=through_op)
                            .all(|op| pipeline.entry_by_op(op).unwrap().has_reply_sender())
                    }));
                    let job = partition.capture_commit_phase(&config).unwrap().unwrap();
                    let writes_offset = matches!(
                        &job,
                        crate::io::PartitionIoJob::OffsetWrite(_)
                            | crate::io::PartitionIoJob::OffsetBatch(_)
                    );
                    if let crate::io::PartitionIoJob::OffsetBatch(writes) = &job {
                        assert_eq!(writes.len(), 3);
                        batches += 1;
                    }
                    let deletes_offset = matches!(&job, crate::io::PartitionIoJob::OffsetDelete(_));
                    let syncs_directory =
                        matches!(&job, crate::io::PartitionIoJob::OffsetDirectories(_));
                    let mut result = job.execute().await;
                    if fail_write {
                        let crate::PartitionIoResult::OffsetBatch(results) = &mut result else {
                            panic!("the first file phase must batch independent offsets");
                        };
                        results[0].outcome = Err(IggyError::CannotWriteToFile);
                        results[0].write_error =
                            Some(std::io::Error::other("injected first write failure"));
                    }
                    assert_eq!(partition.consensus.commit_min(), 0);
                    assert_eq!(
                        partition
                            .durable_consumer_offsets
                            .get(kind, CONSUMER)
                            .unwrap()
                            .committed_offset,
                        INITIAL_OFFSET
                    );
                    for receiver in &mut receivers {
                        assert!(futures::poll!(&mut *receiver).is_pending());
                    }
                    if writes_offset {
                        written_offsets.push(
                            crate::offset_storage::read_offset_max(
                                path.to_str().unwrap(),
                                u64::MIN,
                            )
                            .await
                            .unwrap()
                            .offset,
                        );
                    }
                    if deletes_offset {
                        deletes += 1;
                        assert!(!path.exists());
                    }
                    if syncs_directory {
                        barriers += 1;
                    }
                    if let Err(error) = partition.accept_commit_phase(result) {
                        if fail_write {
                            assert!(matches!(error, IggyError::CannotWriteToFile));
                            for consumer in [CONSUMER + 1, CONSUMER + 2] {
                                let path = directory.path().join(consumer.to_string());
                                assert!(
                                    path.is_file(),
                                    "successful sibling offset {consumer} must remain on disk"
                                );
                            }
                            let pending = partition.pending_commit.as_ref().unwrap();
                            assert_eq!(pending.offset_effects.len(), 2);
                            assert_eq!(
                                pending.next_entry, 0,
                                "the fence must name the first failed op"
                            );
                            assert_eq!(
                                partition.consumer_offset_dirs_touched[kind.index()]
                                    .get()
                                    .unwrap()
                                    .0,
                                3
                            );
                            assert_eq!(partition.consensus.commit_min(), 0);
                            assert!(bus_replies.borrow().is_empty());
                            for receiver in &mut receivers {
                                assert!(futures::poll!(&mut *receiver).is_pending());
                            }
                            continue 'cases;
                        }
                        assert!(fail_barrier && syncs_directory, "{error}");
                        break;
                    }
                }
            }
        }
        assert_eq!(
            written_offsets,
            vec![3, 1],
            "coverage must follow the reset and deletion"
        );
        assert_eq!(batches, 1, "independent keys must share one file job");
        assert_eq!(deletes, 1);
        assert_eq!(barriers, 1);
        assert!(bus_replies.borrow().is_empty());
        if fail_barrier {
            assert!(partition.fatal().is_some());
            assert_eq!(partition.consensus.commit_min(), 0);
            assert_eq!(partition.consensus.pipeline_len(), mutations.len());
            assert_eq!(
                partition.log.journal().inner.resident_count(),
                mutations.len()
            );
            assert_eq!(
                partition
                    .durable_consumer_offsets
                    .get(kind, CONSUMER)
                    .unwrap()
                    .committed_offset,
                INITIAL_OFFSET
            );
            for receiver in &mut receivers {
                assert!(futures::poll!(&mut *receiver).is_pending());
            }
        } else {
            assert!(matches!(
                partition.resume_io(&config).await,
                crate::PartitionIoStep::Progress
            ));
            assert_eq!(partition.consensus.commit_min(), through_op);
            assert_eq!(partition.consensus.pipeline_len(), 0);
            assert!(partition.pending_consumer_offset_commits.is_empty());
            assert_eq!(
                partition
                    .durable_consumer_offsets
                    .get(kind, CONSUMER)
                    .unwrap()
                    .committed_offset,
                1
            );
            for mut receiver in receivers {
                let std::task::Poll::Ready(Ok(reply)) = futures::poll!(&mut receiver) else {
                    panic!("one continuation must deliver the whole committed reply batch");
                };
                assert_eq!(reply.header().status, 0);
            }
        }
    }
}

/// `AckLevel::NoAck` stores apply on the primary only and never replicate, so
/// which replicas hold an offset is not agreed and a committed delete can
/// legitimately find nothing. Erroring on that fails the committed apply,
/// fences the partition, then crash-loops on every replay of the op.
///
/// Both kinds, because they are separate maps with separate directories.
#[compio::test]
async fn given_absent_offset_file_when_delete_commits_should_skip_directory_sync() {
    for (op, kind) in [
        (1, ConsumerKind::Consumer),
        (2, ConsumerKind::ConsumerGroup),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let (mut partition, sent) = recording_partition_at(0, 3);
        let missing = Some(dir.path().join("missing").to_string_lossy().into_owned());
        partition.consumer_offset_dirs[ConsumerKind::Consumer.index()].clone_from(&missing);
        partition.consumer_offset_dirs[ConsumerKind::ConsumerGroup.index()] = missing;
        if kind == ConsumerKind::ConsumerGroup {
            install_test_owner(&mut partition, 7, 42).await;
        }
        partition.stage_consumer_offset_delete(op, kind, 7);
        if kind == ConsumerKind::ConsumerGroup {
            partition
                .pending_consumer_offset_commits
                .get_mut(&op)
                .unwrap()
                .owner_generation = 1;
        }
        partition.consensus.sequencer().set_sequence(op);
        partition.consensus.advance_commit_max(op);
        let header = PrepareHeader {
            retry_capacity: u32::try_from(consensus::PARTITION_DEDUP_CLIENTS_MAX).unwrap(),
            session: 1,
            op,
            operation: Operation::DeleteConsumerOffset,
            client: 42,
            request: 1,
            ..Default::default()
        };
        partition
            .handle_committed_entries(vec![header], &repair_config(), true)
            .await;
        assert!(partition.fatal.is_none(), "{kind:?} delete must not fence");
        assert_eq!(partition.consensus.commit_min(), op);
        assert_eq!(partition.offset_dir_sync_count.get(), 0);
        assert_eq!(sent.borrow().len(), 1);
    }
}

#[compio::test]
async fn given_one_offset_directory_sync_failure_when_flushing_should_sync_the_other_directory() {
    let dir = tempfile::tempdir().unwrap();
    let invalid = dir.path().join("file");
    std::fs::write(&invalid, b"not a directory").unwrap();
    let (mut partition, _) = recording_partition();
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
        Some(invalid.join("child").to_string_lossy().into_owned());
    partition.consumer_offset_dirs[ConsumerKind::ConsumerGroup.index()] =
        Some(dir.path().to_string_lossy().into_owned());
    partition.consumer_offset_dirs[ConsumerKind::ExternalGroup.index()] =
        Some(dir.path().to_string_lossy().into_owned());
    for kind in ConsumerKind::ALL {
        partition.mark_consumer_offset_dir_dirty(kind);
    }
    assert_eq!(
        partition
            .flush_consumer_offset_directories_for([true; ConsumerKind::COUNT])
            .await,
        [true, false, false]
    );
    assert!(partition.consumer_offset_dirs_dirty[0].get());
    assert!(!partition.consumer_offset_dirs_dirty[1].get());
    assert!(!partition.consumer_offset_dirs_dirty[2].get());
    assert_eq!(partition.offset_dir_sync_count.get(), 2);
}

#[compio::test]
async fn given_covered_auto_commit_when_persisting_should_not_sync_directory() {
    let dir = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition();
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
        Some(dir.path().to_string_lossy().into_owned());
    partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Persisted;
    let path = dir.path().join("7");
    persist_offset(path.to_str().unwrap(), 10, true)
        .await
        .unwrap();
    for offset in [5, 7] {
        partition
            .persist_consumer_offset_commit(PendingConsumerOffsetCommit::upsert_auto_commit(
                ConsumerKind::Consumer,
                7,
                offset,
            ))
            .await
            .unwrap();
        assert!(!partition.consumer_offset_dirs_dirty[0].get());
    }
    partition
        .persist_consumer_offset_commit(PendingConsumerOffsetCommit::upsert_auto_commit(
            ConsumerKind::Consumer,
            7,
            11,
        ))
        .await
        .unwrap();
    assert!(partition.consumer_offset_dirs_dirty[0].get());
}

#[compio::test]
async fn given_empty_transfer_frontier_when_incoming_offsets_are_clamped_out_should_remove_old_files()
 {
    let dir = tempfile::tempdir().unwrap();
    let mut partition = test_partition();
    partition.set_partition_dir(dir.path().to_string_lossy().into_owned());
    let consumers = dir.path().join("offsets/consumers");
    let groups = dir.path().join("offsets/groups");
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
        Some(consumers.to_string_lossy().into_owned());
    partition.consumer_offset_dirs[ConsumerKind::ConsumerGroup.index()] =
        Some(groups.to_string_lossy().into_owned());
    partition.consumer_offset_dirs[ConsumerKind::ExternalGroup.index()] = Some(
        dir.path()
            .join("offsets/external_groups")
            .to_string_lossy()
            .into_owned(),
    );
    for path in [consumers.join("7"), groups.join("8")] {
        persist_offset(path.to_str().unwrap(), 5, true)
            .await
            .unwrap();
    }
    let wire = crate::state_transfer::ConsumerOffsetsWire {
        owner_retirement: None,
        owners: Vec::new(),
        history_transition: None,
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        required_metadata_frontier: 0,
        prepare_checksum: None,
        checkpoint_prepare: Vec::new(),
        next_offset: 0,
        emptied_at: 0,
        consumers: vec![(7, 5)],
        groups: vec![(8, 5)],
        external_groups: Vec::new(),
        dedup: Vec::new(),
        retirement_paid_through: 0,
    };
    partition
        .install_state_transfer(&repair_config(), 1, Vec::new(), &wire.encode())
        .await
        .unwrap();
    assert!(!consumers.join("7").exists());
    assert!(!groups.join("8").exists());
    assert_eq!(
        partition.durable_consumer_offset_count(ConsumerKind::Consumer),
        0
    );
    assert_eq!(
        partition.durable_consumer_offset_count(ConsumerKind::ConsumerGroup),
        0
    );
}

/// A Kafka commit can name an offset past the transferred frontier, or any offset on an empty
/// partition. The install must not clamp or drop it.
#[compio::test]
async fn given_empty_transfer_frontier_when_external_offsets_arrive_should_keep_them_as_sent() {
    let dir = tempfile::tempdir().unwrap();
    let mut partition = test_partition();
    partition.set_partition_dir(dir.path().to_string_lossy().into_owned());
    let offsets = dir.path().join("offsets");
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
        Some(offsets.join("consumers").to_string_lossy().into_owned());
    partition.consumer_offset_dirs[ConsumerKind::ConsumerGroup.index()] =
        Some(offsets.join("groups").to_string_lossy().into_owned());
    partition.consumer_offset_dirs[ConsumerKind::ExternalGroup.index()] = Some(
        offsets
            .join("external_groups")
            .to_string_lossy()
            .into_owned(),
    );
    let wire = crate::state_transfer::ConsumerOffsetsWire {
        dedup_capacity: partition.dedup().capacity(),
        external_groups: vec![(8, 0), (9, 1 << 40)],
        ..Default::default()
    };

    partition
        .install_state_transfer(&repair_config(), 1, Vec::new(), &wire.encode())
        .await
        .unwrap();

    assert_eq!(partition.external_group_offset(8), Some(0));
    assert_eq!(partition.external_group_offset(9), Some(1 << 40));
    assert_eq!(
        partition.durable_consumer_offset_count(ConsumerKind::ExternalGroup),
        2
    );
    assert!(offsets.join("external_groups/9").exists());
}

#[compio::test]
async fn given_empty_partition_when_storing_an_external_group_offset_should_skip_the_range_check() {
    let (mut partition, sent_to_clients) = recording_partition();
    for (request, offset) in [(1, 0), (2, 1_000)] {
        partition
            .on_request(
                store_offset_request(
                    42,
                    request,
                    ConsumerKind::ExternalGroup,
                    5,
                    offset,
                    AckLevel::NoAck,
                ),
                None,
            )
            .await;
        assert_eq!(partition.external_group_offset(5), Some(offset));
    }

    install_test_owner(&mut partition, 5, 42).await;
    let request = store_offset_request(42, 3, ConsumerKind::ConsumerGroup, 5, 0, AckLevel::NoAck)
        .transmute_header(|mut header, next: &mut RoutedRequestHeader| {
            header.metadata_watermark = partition.required_metadata_frontier;
            header.owner_generation = partition.consumer_group_owner(5).unwrap().generation;
            *next = header;
        });
    partition.on_request(request, None).await;

    let sent = sent_to_clients.borrow();
    let denied = sent
        .iter()
        .find_map(|(_, frame)| {
            let header = bytemuck::checked::try_from_bytes::<ReplyHeader>(
                &frame.as_slice()[..std::mem::size_of::<ReplyHeader>()],
            )
            .ok()?;
            (header.request == 3).then_some(*header)
        })
        .expect("range denial reply");
    assert_eq!(denied.status, IggyError::InvalidOffset(0).as_code());
    assert_eq!(
        partition.durable_consumer_offset_count(ConsumerKind::ExternalGroup),
        1
    );
    assert!(partition.consumer_group_offsets.pin().is_empty());
}

//     /// Kafka never lets a committed offset block retention, and neither does its gateway.
//     #[compio::test]
//     async fn given_an_external_group_offset_when_finding_the_retention_barrier_should_ignore_it() {
//         let (mut partition, _) = recording_partition();
//         partition.stats.increment_messages_count(1);
//         partition
//             .on_request(
//                 store_offset_request(42, 1, ConsumerKind::ExternalGroup, 5, 0, AckLevel::NoAck),
//                 None,
//             )
//             .await;
//         assert_eq!(partition.external_group_offset(5), Some(0));
//         assert_eq!(partition.min_committed_offset(), None);
//
//         install_test_owner(&mut partition, 6, 42).await;
//         let request =
//             store_offset_request(42, 2, ConsumerKind::ConsumerGroup, 6, 0, AckLevel::NoAck)
//                 .transmute_header(|mut header, next: &mut RoutedRequestHeader| {
//                     header.metadata_watermark = partition.required_metadata_frontier;
//                     header.owner_generation = partition.consumer_group_owner(6).unwrap().generation;
//                     *next = header;
//                 });
//         partition.on_request(request, None).await;
//         assert_eq!(
//             partition.min_committed_offset(),
//             Some((0, ConsumerKind::ConsumerGroup, 6))
//         );
//     }
//
//     #[compio::test]
//     async fn given_offsets_past_the_partition_end_when_recovered_should_clamp_only_iggy_kinds() {
//         let dir = tempfile::tempdir().unwrap();
//         let config = PartitionsConfig {
//             path_layout: crate::PartitionPathLayout {
//                 streams_root: dir.path().to_string_lossy().into_owned(),
//             },
//             ..repair_config()
//         };
//         let namespace = IggyNamespace::new(1, 1, 0);
//         let group_file = format!("{}/8", config.get_consumer_group_offsets_path(1, 1, 0));
//         let external_file = format!("{}/9", config.get_external_group_offsets_path(1, 1, 0));
//         for path in [&group_file, &external_file] {
//             persist_offset(path, 1_000, true).await.unwrap();
//         }
//         let (mut partition, _) = recording_partition();
//
//         crate::partition_storage::configure_consumer_offsets_with_storage(
//             &DiskStorage,
//             &mut partition,
//             &config,
//             namespace,
//             10,
//         )
//         .await
//         .unwrap();
//
//         assert_eq!(partition.external_group_offset(9), Some(1_000));
//         let group = partition
//             .consumer_group_offsets
//             .pin()
//             .get(&ConsumerGroupId(8))
//             .map(|offset| offset.offset.load(Ordering::Relaxed));
//         assert_eq!(group, Some(10));
//     }
//
//     #[compio::test]
//     async fn given_a_deleted_group_when_reconciling_should_list_its_external_offset() {
//         let (partition, _) = recording_partition();
//         partition.seed_recovered_consumer_offset(ConsumerKind::ExternalGroup, 7, 11, 11);
//         partition.seed_recovered_consumer_offset(ConsumerKind::ConsumerGroup, 9, 11, 11);
//         partition.consumer_group_offsets.pin().insert(
//             ConsumerGroupId(9),
//             ConsumerOffset::new(ConsumerKind::ConsumerGroup, 9, 11, String::new()),
//         );
//
//         assert_eq!(
//             partition.dead_group_offset_keys(|_| false),
//             vec![
//                 (ConsumerKind::ExternalGroup, 7),
//                 (ConsumerKind::ConsumerGroup, 9)
//             ]
//         );
//         assert_eq!(partition.dead_group_offset_keys(|_| true), []);
//     }

/// The reconciler deletes a bounded number of dead keys per pass. Ordered
/// by group id, a backlog of dead consumer group keys cannot hold every
/// external group key back by whole passes.
#[compio::test]
async fn given_dead_keys_of_both_kinds_when_listed_should_order_them_by_group_id() {
    let (partition, _) = recording_partition();
    for group_id in [1, 2, 3] {
        partition.seed_recovered_consumer_offset(ConsumerKind::ConsumerGroup, group_id, 11, 11);
        partition.consumer_group_offsets.pin().insert(
            ConsumerGroupId(group_id as usize),
            ConsumerOffset::new(ConsumerKind::ConsumerGroup, group_id, 11, String::new()),
        );
    }
    for group_id in [1, 2] {
        partition.seed_recovered_consumer_offset(ConsumerKind::ExternalGroup, group_id, 11, 11);
    }

    assert_eq!(
        partition.dead_group_offset_keys(|_| false),
        vec![
            (ConsumerKind::ConsumerGroup, 1),
            (ConsumerKind::ExternalGroup, 1),
            (ConsumerKind::ConsumerGroup, 2),
            (ConsumerKind::ExternalGroup, 2),
            (ConsumerKind::ConsumerGroup, 3),
        ]
    );
}

//     /// External group offsets can be stored and deleted on a partition that never took a
//     /// message. Its commit floor then moves with no bytes behind it, and the offer carries
//     /// exactly that: no segment, frontier 0 and no offset.
//     #[compio::test]
//     async fn given_external_offset_stored_then_deleted_on_empty_partition_when_offer_requested_should_serve_it()
//      {
//         let directory = tempfile::tempdir().unwrap();
//         let (mut origin, _) = recording_partition_at(0, 3);
//         origin.set_partition_dir(directory.path().to_string_lossy().into_owned());
//         origin.stage_consumer_offset_upsert(1, ConsumerKind::ExternalGroup, 5, 1 << 40, false);
//         origin.apply_staged_consumer_offset_commit(1).await.unwrap();
//         origin.stage_consumer_offset_delete(2, ConsumerKind::ExternalGroup, 5);
//         origin.apply_staged_consumer_offset_commit(2).await.unwrap();
//         origin.consensus().sequencer().set_sequence(2);
//         origin.consensus().advance_commit_max(2);
//         for op in 1..=2 {
//             origin.consensus().advance_commit_min(op);
//         }
//         assert_eq!(origin.external_group_offset(5), None);
//
//         let offer = origin.state_transfer_offer(&repair_config()).await.unwrap();
//
//         assert_eq!(offer.commit_op, 2);
//         assert!(offer.segments.is_empty());
//         let offsets = crate::state_transfer::ConsumerOffsetsWire::decode(&offer.offsets.1).unwrap();
//         assert_eq!(offsets.next_offset, 0);
//         assert_eq!(offsets.external_groups, []);
//     }
//
//     /// The sender snapshot of a real offer carries an external group offset, and the receiver
//     /// installs it unclamped although it sits far past the origin's only message.
//     #[compio::test]
//     async fn given_external_offset_on_origin_when_receiver_installs_its_offer_should_hold_it_unclamped()
//      {
//         let origin_directory = tempfile::tempdir().unwrap();
//         let receiver_directory = tempfile::tempdir().unwrap();
//         let (mut origin, _) = recording_partition_at(0, 3);
//         origin.set_partition_dir(origin_directory.path().to_string_lossy().into_owned());
//         // One message whose segment retention removed: the counter backs the empty chain.
//         origin.set_offset_space_used(true);
//         origin.offset.store(0, Ordering::Release);
//         origin.stage_consumer_offset_upsert(1, ConsumerKind::ExternalGroup, 5, 1 << 40, false);
//         origin.apply_staged_consumer_offset_commit(1).await.unwrap();
//         origin.consensus().sequencer().set_sequence(1);
//         origin.consensus().advance_commit_max(1);
//         origin.consensus().advance_commit_min(1);
//         origin
//             .dedup_mut()
//             .commit_capacity(consensus::CLIENTS_TABLE_MAX)
//             .unwrap();
//         let offer = origin.state_transfer_offer(&repair_config()).await.unwrap();
//         let (mut receiver, _) = recording_partition_at(1, 3);
//         receiver.set_partition_dir(receiver_directory.path().to_string_lossy().into_owned());
//         set_offset_dirs_under(&mut receiver, receiver_directory.path());
//
//         receiver
//             .install_state_transfer(
//                 &repair_config(),
//                 offer.commit_op,
//                 Vec::new(),
//                 &offer.offsets.1,
//             )
//             .await
//             .unwrap();
//
//         assert_eq!(receiver.external_group_offset(5), Some(1 << 40));
//         assert!(
//             receiver_directory
//                 .path()
//                 .join("offsets/external_groups/5")
//                 .exists()
//         );
//     }
//
//     /// A restarted replica that only holds an external group offset reads 0 as
//     /// its next offset, so the message rewind check cannot protect it. An offer
//     /// that holds nothing, such as one from a primary that lost its directory,
//     /// must not unlink that offset.
//     #[compio::test]
//     async fn given_receiver_with_only_external_offsets_when_offered_nothing_should_refuse() {
//         let directory = tempfile::tempdir().unwrap();
//         let (mut receiver, _) = recording_partition_at(1, 3);
//         receiver.set_partition_dir(directory.path().to_string_lossy().into_owned());
//         set_offset_dirs_under(&mut receiver, directory.path());
//         receiver.stage_consumer_offset_upsert(1, ConsumerKind::ExternalGroup, 7, 1 << 40, false);
//         receiver
//             .apply_staged_consumer_offset_commit(1)
//             .await
//             .unwrap();
//         let offset_file = directory.path().join("offsets/external_groups/7");
//         assert!(offset_file.exists());
//
//         let nothing = crate::state_transfer::ConsumerOffsetsWire {
//             dedup_capacity: receiver.dedup().capacity(),
//             ..Default::default()
//         };
//         let refused = receiver
//             .install_state_transfer(&repair_config(), 5, Vec::new(), &nothing.encode())
//             .await;
//
//         assert!(
//             matches!(
//                 refused,
//                 Err(
//                     crate::state_transfer::PartitionInstallError::OfferErasesDurableOffsets {
//                         local_offsets: 1
//                     }
//                 )
//             ),
//             "expected the empty offer to be refused, got {refused:?}"
//         );
//         assert_eq!(receiver.external_group_offset(7), Some(1 << 40));
//         assert!(offset_file.exists());
//     }
//
//     /// A replica holding an offset whose delete no peer retains catches up by
//     /// transfer. The donor's empty offer names the delete that emptied its table,
//     /// and the receiver keeps that op to serve it in turn.
//     #[compio::test]
//     async fn given_counter_zero_replica_that_missed_an_evicted_offset_delete_when_transferring_should_install_and_drop_the_deleted_offset()
//      {
//         let donor_directory = tempfile::tempdir().unwrap();
//         let (mut donor, _) = recording_partition_at(0, 3);
//         donor.set_partition_dir(donor_directory.path().to_string_lossy().into_owned());
//         set_offset_dirs_under(&mut donor, donor_directory.path());
//         donor
//             .on_request(
//                 store_offset_request(
//                     MESSAGE_LESS_CLIENT_ID,
//                     1,
//                     ConsumerKind::ExternalGroup,
//                     MESSAGE_LESS_EXTERNAL_GROUP_ID,
//                     MESSAGE_LESS_EXTERNAL_OFFSET,
//                     AckLevel::Quorum,
//                 ),
//                 None,
//             )
//             .await;
//         donor.consensus().advance_commit_max(1);
//         donor.commit_journal(&repair_config()).await;
//         let (receiver_directory, mut receiver) = transfer_receiver();
//         receiver.on_replicate(retained_prepare(&donor, 1)).await;
//         receiver.consensus().advance_commit_max(1);
//         receiver.commit_journal(&repair_config()).await;
//         let offset_file = receiver_directory.path().join(format!(
//             "offsets/external_groups/{MESSAGE_LESS_EXTERNAL_GROUP_ID}"
//         ));
//         assert!(offset_file.exists());
//         donor
//             .on_request(
//                 delete_offset_request_with_ack(
//                     MESSAGE_LESS_CLIENT_ID,
//                     2,
//                     ConsumerKind::ExternalGroup,
//                     MESSAGE_LESS_EXTERNAL_GROUP_ID,
//                     AckLevel::Quorum,
//                 ),
//                 None,
//             )
//             .await;
//         donor.consensus().advance_commit_max(2);
//         donor.commit_journal(&repair_config()).await;
//         assert_eq!(
//             donor.external_group_offset(MESSAGE_LESS_EXTERNAL_GROUP_ID),
//             None
//         );
//
//         // No peer retains the delete any more, so repair cannot reach it.
//         receiver.consensus().advance_commit_max(2);
//         receiver.repair = Some(armed_session(2, 2));
//         assert_eq!(
//             receiver.complete_repair(&repair_config()).await,
//             RepairConclusion::FloorRefused { floor: 2, to_op: 2 }
//         );
//         let offer = donor.state_transfer_offer(&repair_config()).await.unwrap();
//         let installed = receiver
//             .install_state_transfer(
//                 &repair_config(),
//                 offer.commit_op,
//                 Vec::new(),
//                 &offer.offsets.1,
//             )
//             .await
//             .unwrap();
//
//         assert_eq!(installed.applied_commit_op, 2);
//         assert_eq!(receiver.consensus().commit_min(), 2);
//         assert_eq!(
//             receiver.external_group_offset(MESSAGE_LESS_EXTERNAL_GROUP_ID),
//             None
//         );
//         assert!(!offset_file.exists());
//         assert_eq!(emptied_marker_op(receiver_directory.path()), Some(2));
//         assert_eq!(receiver.durable_consumer_offsets.emptied_at(), 2);
//     }

/// A receiver that already applied the op its donor's table was emptied at
/// cannot have missed that delete, so the empty offer does not explain the
/// offset it holds.
#[compio::test]
async fn given_receiver_at_the_emptied_op_when_offered_nothing_should_refuse() {
    const EMPTIED_AT: u64 = 2;
    const OFFER_COMMIT_OP: u64 = 5;
    let directory = tempfile::tempdir().unwrap();
    let mut receiver = external_offset_receiver(directory.path(), EMPTIED_AT).await;
    let emptied = crate::state_transfer::ConsumerOffsetsWire {
        dedup_capacity: receiver.dedup().capacity(),
        emptied_at: EMPTIED_AT,
        ..Default::default()
    };

    let refused = receiver
        .install_state_transfer(
            &repair_config(),
            OFFER_COMMIT_OP,
            Vec::new(),
            &emptied.encode(),
        )
        .await;

    assert!(
        matches!(
            refused,
            Err(
                crate::state_transfer::PartitionInstallError::OfferErasesDurableOffsets {
                    local_offsets: 1
                }
            )
        ),
        "expected the empty offer to be refused, got {refused:?}"
    );
    assert_eq!(
        receiver.external_group_offset(MESSAGE_LESS_EXTERNAL_GROUP_ID),
        Some(MESSAGE_LESS_EXTERNAL_OFFSET)
    );
}

/// An emptied op past the offer's own op is not one its donor applied, so it
/// proves nothing about the table at the offer.
#[compio::test]
async fn given_emptied_op_past_the_offer_when_offered_nothing_should_refuse() {
    const OFFER_COMMIT_OP: u64 = 5;
    let directory = tempfile::tempdir().unwrap();
    let mut receiver = external_offset_receiver(directory.path(), 1).await;
    let ahead = crate::state_transfer::ConsumerOffsetsWire {
        dedup_capacity: receiver.dedup().capacity(),
        emptied_at: OFFER_COMMIT_OP + 1,
        ..Default::default()
    };

    let refused = receiver
        .install_state_transfer(
            &repair_config(),
            OFFER_COMMIT_OP,
            Vec::new(),
            &ahead.encode(),
        )
        .await;

    assert!(
        matches!(
            refused,
            Err(
                crate::state_transfer::PartitionInstallError::OfferErasesDurableOffsets {
                    local_offsets: 1
                }
            )
        ),
        "expected the empty offer to be refused, got {refused:?}"
    );
    assert_eq!(emptied_marker_op(directory.path()), None);
}

/// An installed table that holds offsets keeps no marker, neither the
/// receiver's nor one the offer names, so none can outlive the installed files.
#[compio::test]
async fn given_emptied_receiver_when_installing_offsets_should_remove_its_marker() {
    const OFFER_COMMIT_OP: u64 = 5;
    const OFFER_EMPTIED_AT: u64 = 4;
    let directory = tempfile::tempdir().unwrap();
    let (mut receiver, _) = recording_partition_at(1, 3);
    receiver.set_partition_dir(directory.path().to_string_lossy().into_owned());
    set_offset_dirs_under(&mut receiver, directory.path());
    let marker = directory.path().join("offsets/emptied");
    persist_offset(&marker.to_string_lossy(), 1, true)
        .await
        .unwrap();
    let offsets = crate::state_transfer::ConsumerOffsetsWire {
        dedup_capacity: receiver.dedup().capacity(),
        emptied_at: OFFER_EMPTIED_AT,
        external_groups: vec![(MESSAGE_LESS_EXTERNAL_GROUP_ID, MESSAGE_LESS_EXTERNAL_OFFSET)],
        ..Default::default()
    };

    receiver
        .install_state_transfer(
            &repair_config(),
            OFFER_COMMIT_OP,
            Vec::new(),
            &offsets.encode(),
        )
        .await
        .unwrap();

    assert_eq!(emptied_marker_op(directory.path()), None);
    assert_eq!(
        receiver.external_group_offset(MESSAGE_LESS_EXTERNAL_GROUP_ID),
        Some(MESSAGE_LESS_EXTERNAL_OFFSET)
    );
}

/// A store into an emptied table retires the marker before its file lands,
/// so a crash that loses the file cannot leave a marker claiming the table
/// is still empty.
#[compio::test]
async fn given_emptied_marker_when_an_offset_is_stored_again_should_remove_the_marker() {
    let (directory, mut partition) = emptied_external_origin().await;
    assert_eq!(emptied_marker_op(directory.path()), Some(2));

    partition
        .on_request(
            store_offset_request(
                MESSAGE_LESS_CLIENT_ID,
                3,
                ConsumerKind::ExternalGroup,
                MESSAGE_LESS_EXTERNAL_GROUP_ID,
                MESSAGE_LESS_EXTERNAL_OFFSET,
                AckLevel::Quorum,
            ),
            None,
        )
        .await;
    partition.consensus().advance_commit_max(3);
    partition.commit_journal(&repair_config()).await;

    assert_eq!(partition.consensus().commit_min(), 3);
    assert_eq!(emptied_marker_op(directory.path()), None);
    let offer = partition
        .state_transfer_offer(&repair_config())
        .await
        .unwrap();
    let offsets = crate::state_transfer::ConsumerOffsetsWire::decode(&offer.offsets.1).unwrap();
    assert_eq!(offsets.emptied_at, 0);
}

/// The node-local store skips the commit walk, and it too retires the marker
/// before its file lands.
#[compio::test]
async fn given_no_ack_store_in_flight_when_a_send_is_queued_should_keep_the_emptied_marker_cleared()
{
    let config = repair_config();
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition();
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.log.retire_front().unwrap();
    partition.install_empty_segment(&config, 0).await.unwrap();
    set_offset_dirs_under(&mut partition, directory.path());
    commit_external_store_then_delete(&mut partition).await;
    assert_eq!(emptied_marker_op(directory.path()), Some(2));
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    let store = store_offset_request(
        MESSAGE_LESS_CLIENT_ID,
        3,
        ConsumerKind::ExternalGroup,
        MESSAGE_LESS_EXTERNAL_GROUP_ID,
        MESSAGE_LESS_EXTERNAL_OFFSET,
        AckLevel::NoAck,
    );

    partition
        .apply_consumer_offset_no_ack(
            Box::new(*store.header()),
            ConsumerKind::ExternalGroup,
            MESSAGE_LESS_EXTERNAL_GROUP_ID,
            Some(MESSAGE_LESS_EXTERNAL_OFFSET),
            None,
        )
        .await;
    assert!(matches!(partition.drive_no_ack(), NoAckStep::Progress));
    let store_job = partition.capture_no_ack_phase().unwrap();

    let send = history_send_request(&partition, 1, partition.required_metadata_frontier);
    partition.on_request(send, None).await;
    partition.consensus().advance_commit_max(3);
    assert_eq!(partition.select_persistable_commits(&config), []);
    partition.accept_no_ack_phase(store_job.execute().await);

    assert_eq!(
        partition.external_group_offset(MESSAGE_LESS_EXTERNAL_GROUP_ID),
        Some(MESSAGE_LESS_EXTERNAL_OFFSET)
    );
    assert_eq!(emptied_marker_op(directory.path()), None);
    assert_eq!(partition.durable_consumer_offsets.emptied_at(), 0);

    settle_partition_io(&mut partition, &config).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(partition.consensus().commit_min(), 3);
    assert_eq!(emptied_marker_op(directory.path()), None);
    assert_eq!(partition.durable_consumer_offsets.emptied_at(), 0);
    assert!(matches!(
        partition.state_transfer_offer(&config).await,
        Err(crate::state_transfer::PartitionTransferUnavailable::FlushPending)
    ));
    settle_partition_io(&mut partition, &config).await;
    let offer = partition.state_transfer_offer(&config).await.unwrap();
    let offsets = crate::state_transfer::ConsumerOffsetsWire::decode(&offer.offsets.1).unwrap();
    assert_eq!(offsets.emptied_at, 0);
}

/// A stranded file may hold an offset the table never loaded, so a delete of
/// the last loaded offset does not mark the table emptied.
#[compio::test]
async fn given_stranded_offset_when_a_delete_empties_the_table_should_not_mark_it_emptied() {
    const STRANDED_GROUP_ID: u32 = MESSAGE_LESS_EXTERNAL_GROUP_ID + 1;
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition_at(0, 3);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    set_offset_dirs_under(&mut partition, directory.path());
    assert!(
        partition.seed_stranded_consumer_offset(ConsumerKind::ExternalGroup, STRANDED_GROUP_ID)
    );

    commit_external_store_then_delete(&mut partition).await;

    assert_eq!(
        partition.external_group_offset(MESSAGE_LESS_EXTERNAL_GROUP_ID),
        None
    );
    assert_eq!(emptied_marker_op(directory.path()), None);
    assert_eq!(partition.durable_consumer_offsets.emptied_at(), 0);
}

/// A delete that leaves another offset does not empty the table. Only the
/// delete of the last offset marks it, with that delete's op.
#[compio::test]
async fn given_two_offsets_when_both_are_deleted_should_mark_only_the_last_delete() {
    const LAST_DELETE_OP: u64 = 4;
    let (directory, mut origin) = external_origin_that_deleted_one_of_two_groups().await;
    assert_eq!(emptied_marker_op(directory.path()), None);
    assert_eq!(origin.durable_consumer_offsets.emptied_at(), 0);

    origin
        .on_request(
            delete_offset_request_with_ack(
                MESSAGE_LESS_CLIENT_ID,
                LAST_DELETE_OP,
                ConsumerKind::ExternalGroup,
                LIVE_EXTERNAL_GROUP_ID,
                AckLevel::Quorum,
            ),
            None,
        )
        .await;
    origin.consensus().advance_commit_max(LAST_DELETE_OP);
    origin.commit_journal(&repair_config()).await;

    assert_eq!(origin.consensus().commit_min(), LAST_DELETE_OP);
    assert_eq!(emptied_marker_op(directory.path()), Some(LAST_DELETE_OP));
    assert_eq!(origin.durable_consumer_offsets.emptied_at(), LAST_DELETE_OP);
}

/// A donor that restarts after emptying its table still proves it: boot reads
/// the marker back, and the offer at the checkpointed op names the delete.
#[compio::test]
async fn given_table_emptied_by_a_delete_when_the_donor_restarts_should_offer_the_emptied_op() {
    const EMPTIED_AT: u64 = 2;
    let (_origin_directory, origin) = emptied_external_origin().await;
    let donor_directory = tempfile::tempdir().unwrap();
    let (config, mut donor) = Box::pin(restarted_replica_of(
        &origin,
        donor_directory.path(),
        1,
        EMPTIED_AT,
    ))
    .await;
    // A view change makes the restarted replica the primary.
    donor.consensus.set_view(1);

    let offer = donor.state_transfer_offer(&config).await.unwrap();

    let offsets = crate::state_transfer::ConsumerOffsetsWire::decode(&offer.offsets.1).unwrap();
    assert_eq!(
        (offer.commit_op, offsets.emptied_at),
        (EMPTIED_AT, EMPTIED_AT)
    );
    assert_eq!(offsets.external_groups, []);
}

/// A donor whose boot lost its offset files holds an empty table, so its
/// delete of a key that table lacks removes nothing. Its empty offer must
/// not name that delete, or a receiver behind it erases an offset that the
/// origin still holds.
#[compio::test]
async fn given_donor_that_lost_its_offset_files_when_it_deletes_an_absent_offset_should_not_erase_the_receiver_offsets()
 {
    const CHECKPOINT_OP: u64 = 2;
    const DELETE_OP: u64 = 3;
    let (_origin_directory, origin) = external_origin_that_deleted_one_of_two_groups().await;

    for tear_files in [true, false] {
        let donor_directory = tempfile::tempdir().unwrap();
        let (config, donor) = Box::pin(restarted_replica_of(
            &origin,
            donor_directory.path(),
            1,
            CHECKPOINT_OP,
        ))
        .await;
        drop(donor);
        let offsets_dir = std::path::PathBuf::from(config.get_offsets_path(1, 1, 0));
        if tear_files {
            for group_id in [MESSAGE_LESS_EXTERNAL_GROUP_ID, LIVE_EXTERNAL_GROUP_ID] {
                let file = offsets_dir.join(format!("external_groups/{group_id}"));
                std::fs::write(file, []).unwrap();
            }
        } else {
            std::fs::remove_dir_all(&offsets_dir).unwrap();
        }
        let (config, mut donor) = replica_with_prepare_history(donor_directory.path(), 1).await;
        assert_eq!(donor.external_group_offset(LIVE_EXTERNAL_GROUP_ID), None);
        commit_replicated_through(&mut donor, &origin, &config, DELETE_OP).await;
        // A view change makes the restarted replica the primary.
        donor.consensus.set_view(1);
        let offer = donor.state_transfer_offer(&config).await.unwrap();
        let (receiver_directory, mut receiver) = transfer_receiver();
        for op in 1..=CHECKPOINT_OP {
            receiver.on_replicate(retained_prepare(&origin, op)).await;
        }
        receiver.consensus().advance_commit_max(CHECKPOINT_OP);
        receiver.commit_journal(&repair_config()).await;
        assert_eq!(receiver.consensus().commit_min(), CHECKPOINT_OP);

        let refused = receiver
            .install_state_transfer(
                &repair_config(),
                offer.commit_op,
                Vec::new(),
                &offer.offsets.1,
            )
            .await;

        assert!(
            matches!(
                refused,
                Err(
                    crate::state_transfer::PartitionInstallError::OfferErasesDurableOffsets {
                        local_offsets: 2
                    }
                )
            ),
            "files torn: {tear_files}, expected the empty offer to be refused, got {refused:?}"
        );
        assert_eq!(
            receiver.external_group_offset(LIVE_EXTERNAL_GROUP_ID),
            Some(MESSAGE_LESS_EXTERNAL_OFFSET)
        );
        assert!(
            receiver_directory
                .path()
                .join(format!("offsets/external_groups/{LIVE_EXTERNAL_GROUP_ID}"))
                .exists()
        );
    }
}

/// Boot lost the file of one of the donor's two offsets, so its delete of
/// that key removes nothing and the table still holds the other. That
/// delete must not mark the table emptied.
#[compio::test]
async fn given_donor_that_lost_one_of_two_offset_files_when_it_deletes_that_offset_should_not_mark_the_table_emptied()
 {
    const CHECKPOINT_OP: u64 = 2;
    const DELETE_OP: u64 = 3;
    let (_origin_directory, origin) = external_origin_that_deleted_one_of_two_groups().await;
    let donor_directory = tempfile::tempdir().unwrap();
    let (config, donor) = Box::pin(restarted_replica_of(
        &origin,
        donor_directory.path(),
        1,
        CHECKPOINT_OP,
    ))
    .await;
    drop(donor);
    let lost_file = format!(
        "{}/{MESSAGE_LESS_EXTERNAL_GROUP_ID}",
        config.get_external_group_offsets_path(1, 1, 0)
    );
    std::fs::write(lost_file, []).unwrap();
    let (config, mut donor) = replica_with_prepare_history(donor_directory.path(), 1).await;
    assert_eq!(
        donor.external_group_offset(MESSAGE_LESS_EXTERNAL_GROUP_ID),
        None
    );

    commit_replicated_through(&mut donor, &origin, &config, DELETE_OP).await;

    assert_eq!(
        donor.external_group_offset(LIVE_EXTERNAL_GROUP_ID),
        Some(MESSAGE_LESS_EXTERNAL_OFFSET)
    );
    let partition_dir = config.get_partition_path(1, 1, 0);
    assert_eq!(
        emptied_marker_op(std::path::Path::new(&partition_dir)),
        None
    );
    assert_eq!(donor.durable_consumer_offsets.emptied_at(), 0);
}

/// The simulator's restart image keeps the emptied-at op, as boot reads the
/// marker back from disk.
#[compio::test]
async fn given_emptied_table_when_retained_across_a_restart_should_keep_the_emptied_op() {
    let (_directory, mut origin) = emptied_external_origin().await;
    let retained = origin.take_retained_state();
    let (mut restarted, _) = recording_partition_at(0, 3);

    restarted.adopt_retained_log(retained);

    assert_eq!(restarted.durable_consumer_offsets.emptied_at(), 2);
}

/// A crash that beats the directory sync can bring back an offset file the
/// emptying delete unlinked. Boot then drops the marker instead of trusting it.
#[compio::test]
async fn given_emptied_marker_beside_an_offset_file_when_recovering_should_remove_the_marker() {
    let dir = tempfile::tempdir().unwrap();
    let config = PartitionsConfig {
        path_layout: crate::PartitionPathLayout {
            streams_root: dir.path().to_string_lossy().into_owned(),
        },
        ..repair_config()
    };
    let external_file = format!("{}/9", config.get_external_group_offsets_path(1, 1, 0));
    persist_offset(&external_file, 1_000, true).await.unwrap();
    let offsets_dir = config.get_offsets_path(1, 1, 0);
    persist_offset(&format!("{offsets_dir}/emptied"), 2, true)
        .await
        .unwrap();
    let (mut partition, _) = recording_partition();

    crate::partition_storage::configure_consumer_offsets_with_storage(
        &DiskStorage,
        &mut partition,
        &config,
        IggyNamespace::new(1, 1, 0),
        10,
    )
    .await
    .unwrap();

    assert_eq!(partition.external_group_offset(9), Some(1_000));
    let partition_dir = std::path::Path::new(&offsets_dir).parent().unwrap();
    assert_eq!(emptied_marker_op(partition_dir), None);
}

/// An offset file that boot cannot read strands its key, and it may hold an
/// offset the table never loaded. Boot then drops the marker.
#[compio::test]
async fn given_emptied_marker_beside_a_stranded_offset_file_when_recovering_should_remove_the_marker()
 {
    let dir = tempfile::tempdir().unwrap();
    let config = PartitionsConfig {
        path_layout: crate::PartitionPathLayout {
            streams_root: dir.path().to_string_lossy().into_owned(),
        },
        ..repair_config()
    };
    let external_file = format!("{}/9", config.get_external_group_offsets_path(1, 1, 0));
    persist_offset(&external_file, 1_000, true).await.unwrap();
    let offsets_dir = config.get_offsets_path(1, 1, 0);
    persist_offset(&format!("{offsets_dir}/emptied"), 2, true)
        .await
        .unwrap();
    let (mut partition, _) = recording_partition();

    crate::partition_storage::configure_consumer_offsets_with_storage(
        &FailingPathStorage(external_file.into()),
        &mut partition,
        &config,
        IggyNamespace::new(1, 1, 0),
        10,
    )
    .await
    .unwrap();

    assert_eq!(
        partition.stranded_consumer_offset_count(ConsumerKind::ExternalGroup),
        1
    );
    let partition_dir = std::path::Path::new(&offsets_dir).parent().unwrap();
    assert_eq!(emptied_marker_op(partition_dir), None);
    assert_eq!(partition.durable_consumer_offsets.emptied_at(), 0);
}

/// A kind directory whose presence boot cannot check may hold offsets, so
/// the load fails instead of starting that kind empty.
#[compio::test]
async fn given_failed_offset_directory_check_when_recovering_should_fail_the_load() {
    let dir = tempfile::tempdir().unwrap();
    let config = PartitionsConfig {
        path_layout: crate::PartitionPathLayout {
            streams_root: dir.path().to_string_lossy().into_owned(),
        },
        ..repair_config()
    };
    for (kind, path) in [
        (
            ConsumerKind::Consumer,
            config.get_consumer_offsets_path(1, 1, 0),
        ),
        (
            ConsumerKind::ConsumerGroup,
            config.get_consumer_group_offsets_path(1, 1, 0),
        ),
        (
            ConsumerKind::ExternalGroup,
            config.get_external_group_offsets_path(1, 1, 0),
        ),
    ] {
        let (mut partition, _) = recording_partition();

        let Err(error) = crate::partition_storage::configure_consumer_offsets_with_storage(
            &FailingPathStorage(path.into()),
            &mut partition,
            &config,
            IggyNamespace::new(1, 1, 0),
            0,
        )
        .await
        else {
            panic!("{kind}: a failed directory check must fail the load");
        };

        assert!(
            matches!(
                &error,
                crate::PartitionRecoveryError::ConsumerOffsetsLoad { consumer_kind, .. }
                    if *consumer_kind == kind.as_str()
            ),
            "{kind}: {error:?}"
        );
    }
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

/// A replica whose only offset is one external group's, committed through
/// `commit_min`.
async fn external_offset_receiver(
    root: &std::path::Path,
    commit_min: u64,
) -> IggyPartition<RecordingBus> {
    let (mut receiver, _) = recording_partition_at(1, 3);
    receiver.set_partition_dir(root.to_string_lossy().into_owned());
    set_offset_dirs_under(&mut receiver, root);
    receiver.stage_consumer_offset_upsert(
        1,
        ConsumerKind::ExternalGroup,
        MESSAGE_LESS_EXTERNAL_GROUP_ID,
        MESSAGE_LESS_EXTERNAL_OFFSET,
        false,
    );
    receiver
        .apply_staged_consumer_offset_commit(1)
        .await
        .unwrap();
    receiver.consensus().sequencer().set_sequence(commit_min);
    receiver.consensus().advance_commit_max(commit_min);
    for op in 1..=commit_min {
        receiver.consensus().advance_commit_min(op);
    }
    receiver
}

/// A primary with offset directories whose only external group offset was
/// stored at op 1 and deleted at op 2.
async fn emptied_external_origin() -> (tempfile::TempDir, IggyPartition<RecordingBus>) {
    let directory = tempfile::tempdir().unwrap();
    let (mut origin, _) = recording_partition_at(0, 3);
    origin.set_partition_dir(directory.path().to_string_lossy().into_owned());
    set_offset_dirs_under(&mut origin, directory.path());
    commit_external_store_then_delete(&mut origin).await;
    (directory, origin)
}

async fn commit_external_store_then_delete(primary: &mut IggyPartition<RecordingBus>) {
    let store = store_offset_request(
        MESSAGE_LESS_CLIENT_ID,
        1,
        ConsumerKind::ExternalGroup,
        MESSAGE_LESS_EXTERNAL_GROUP_ID,
        MESSAGE_LESS_EXTERNAL_OFFSET,
        AckLevel::Quorum,
    );
    let delete = delete_offset_request_with_ack(
        MESSAGE_LESS_CLIENT_ID,
        2,
        ConsumerKind::ExternalGroup,
        MESSAGE_LESS_EXTERNAL_GROUP_ID,
        AckLevel::Quorum,
    );
    for (op, request) in [(1, store), (2, delete)] {
        primary.on_request(request, None).await;
        primary.consensus().advance_commit_max(op);
        primary.commit_journal(&repair_config()).await;
    }
    assert!(primary.fatal().is_none(), "{:?}", primary.fatal());
    assert_eq!(primary.consensus().commit_min(), 2);
}

const LIVE_EXTERNAL_GROUP_ID: u32 = MESSAGE_LESS_EXTERNAL_GROUP_ID + 1;

/// A primary with offset directories that stored the offsets of external
/// groups `MESSAGE_LESS_EXTERNAL_GROUP_ID` at op 1 and
/// `LIVE_EXTERNAL_GROUP_ID` at op 2, then deleted the first at op 3.
async fn external_origin_that_deleted_one_of_two_groups()
-> (tempfile::TempDir, IggyPartition<RecordingBus>) {
    let directory = tempfile::tempdir().unwrap();
    let (mut origin, _) = recording_partition_at(0, 3);
    origin.set_partition_dir(directory.path().to_string_lossy().into_owned());
    set_offset_dirs_under(&mut origin, directory.path());
    let store = |op, group_id| {
        store_offset_request(
            MESSAGE_LESS_CLIENT_ID,
            op,
            ConsumerKind::ExternalGroup,
            group_id,
            MESSAGE_LESS_EXTERNAL_OFFSET,
            AckLevel::Quorum,
        )
    };
    let delete = delete_offset_request_with_ack(
        MESSAGE_LESS_CLIENT_ID,
        3,
        ConsumerKind::ExternalGroup,
        MESSAGE_LESS_EXTERNAL_GROUP_ID,
        AckLevel::Quorum,
    );
    for (op, request) in [
        (1, store(1, MESSAGE_LESS_EXTERNAL_GROUP_ID)),
        (2, store(2, LIVE_EXTERNAL_GROUP_ID)),
        (3, delete),
    ] {
        origin.on_request(request, None).await;
        origin.consensus().advance_commit_max(op);
        origin.commit_journal(&repair_config()).await;
    }
    assert_eq!(origin.consensus().commit_min(), 3);
    (directory, origin)
}

// A message-less replica that falls behind catches up only by transfer. A donor that
// restarted after a WAL checkpoint still serves the prepare at that op, which a
// receiver keeping prepare history needs to install the offer.
//     #[compio::test]
//     async fn given_message_less_donor_restarted_after_checkpoint_when_persistent_replica_transfers_should_hold_its_state()
//      {
//         let (_origin_directory, origin, transition) = message_less_origin().await;
//         let commit_op = origin.consensus().commit_min();
//         let donor_directory = tempfile::tempdir().unwrap();
//         let (config, mut donor) = Box::pin(restarted_replica_of(
//             &origin,
//             donor_directory.path(),
//             1,
//             commit_op,
//         ))
//         .await;
//         // A view change makes the restarted replica the primary.
//         donor.consensus.set_view(1);
//         let offer = donor.state_transfer_offer(&config).await.unwrap();
//         let receiver_directory = tempfile::tempdir().unwrap();
//         let (receiver_config, mut receiver) =
//             replica_with_prepare_history(receiver_directory.path(), 2).await;
//         receiver
//             .install_state_transfer(
//                 &receiver_config,
//                 offer.commit_op,
//                 Vec::new(),
//                 &offer.offsets.1,
//             )
//             .await
//             .unwrap();
//
//         assert_holds_message_less_state(&receiver, &origin, &transition);
//         assert!(
//             receiver
//                 .log
//                 .journal()
//                 .inner
//                 .repair_entry(commit_op)
//                 .is_some()
//         );
//     }

//     #[compio::test]
//     async fn given_durable_adopted_partition_headers_when_reopening_should_keep_body_certification_separate()
//      {
//         const BACKUP: u8 = 1;
//         const ADOPTED: u32 = 2;
//         const HELD: u64 = 1;
//         const HEAD: u64 = 3;
//         let directory = tempfile::tempdir().unwrap();
//         let (config, mut partition) = replica_with_prepare_history(directory.path(), BACKUP).await;
//         let partition_dir = config.get_partition_path(1, 1, 0);
//         let store = Rc::new(PingPongSuperblock::open(&partition_dir).await.unwrap());
//         partition.set_superblock(store.clone(), None);
//         let held = checksummed_segment_prepare(HELD, 0, 0, b"retained");
//         partition.on_replicate(held.clone()).await;
//         let persistence = partition.persistence.as_ref().unwrap().clone();
//         persistence
//             .drain_with_timeout(compio::runtime::time::sleep)
//             .await
//             .unwrap();
//         assert_eq!(persistence.head(), HELD);
//         assert_eq!(persistence.certified_log_view(), Some(0));
//         assert!(
//             partition
//                 .complete_superblock_intent(SuperblockIntent::Advance {
//                     frontier: partition.offset_frontier(),
//                     reserved: 0
//                 })
//                 .await
//         );
//         let mut parent = *held.header();
//         let mut headers = Vec::new();
//         for op in HELD + 1..=HEAD {
//             let mut header = PrepareHeader {
//                 op,
//                 request: op,
//                 timestamp: op,
//                 view: ADOPTED,
//                 parent: parent.checksum,
//                 ..parent
//             };
//             header.checksum = header.identity_checksum();
//             parent = header;
//             headers.push(header);
//         }
//         headers.reverse();
//         let start_view = Message::<iggy_binary_protocol::StartViewHeader>::new(size_of::<
//             iggy_binary_protocol::StartViewHeader,
//         >())
//         .transmute_header(|_, header: &mut iggy_binary_protocol::StartViewHeader| {
//             header.command = Command::StartView;
//             header.cluster = TEST_CLUSTER;
//             header.group = partition.namespace().inner();
//             header.view = ADOPTED;
//             header.replica = 2;
//             header.op = HEAD;
//             header.commit = HEAD - 1;
//             header.size =
//                 u32::try_from(size_of::<iggy_binary_protocol::StartViewHeader>()).unwrap();
//         });
//         partition.consensus().handle_start_view(
//             consensus::PlaneKind::Partitions,
//             start_view.header(),
//             bytemuck::cast_slice(&headers),
//         );
//         assert!(partition.consensus().view_log_unproven());
//         assert!(
//             partition.persist_superblock_if_needed().await,
//             "DVC identity can persist while the adopted bodies are missing"
//         );
//         assert_eq!(persistence.certified_log_view(), Some(0));
//         assert!(!partition.register_rebuilt_ack(&headers[0]));
//         let segment_size = partition.log.active_segment().max_size;
//         drop(persistence);
//         drop(partition);
//         drop(store);
//
//         let (store, latest) = PingPongSuperblock::open_with_latest(&partition_dir)
//             .await
//             .unwrap();
//         let journal::superblock::SuperblockContents::Present(bytes) = latest else {
//             panic!("adopted record must be durable");
//         };
//         let state = consensus::VsrState::try_from(bytes.as_slice()).unwrap();
//         assert_eq!(state.log_view, 0);
//         state
//             .validate_group(IggyNamespace::new(1, 1, 0).inner(), 0)
//             .unwrap();
//         let (mut reopened, _) = recording_partition_at(BACKUP, 3);
//         reopened.consensus.set_view(state.view);
//         reopened.consensus.set_log_view(state.log_view);
//         reopened
//             .consensus
//             .mark_superblock_durable(state.view, state.log_view);
//         reopened.set_superblock(Rc::new(store), Some(&state));
//         reopened.set_partition_dir(partition_dir);
//         reopened.log.active_segment_mut().max_size = segment_size;
//         // Open the files already referenced by the WAL; creating an empty segment
//         // would erase the retained body this restart is meant to preserve.
//         reopened.open_persistence().await.unwrap();
//         assert!(!reopened.requires_state_transfer());
//         let persistence = reopened.persistence.as_ref().unwrap();
//         assert_eq!(persistence.head(), HELD);
//         assert_eq!(persistence.checksum(HELD), Some(held.header().checksum));
//         assert_eq!(persistence.certified_log_view(), Some(0));
//         assert_eq!(
//             reopened.consensus().adopted_view_head(),
//             Some((ADOPTED, HEAD))
//         );
//         assert_eq!(reopened.consensus().log_view_claim(), ADOPTED);
//         assert_eq!(reopened.consensus().sequencer().current_sequence(), HEAD);
//         assert!(reopened.consensus().view_log_unproven());
//         assert!(!reopened.consensus().view_log_entry_proven(HEAD));
//         assert!(!reopened.register_rebuilt_ack(&headers[0]));
//     }

//     /// An install below the head of an adopted `StartView` leaves the WAL without
//     /// that view's log. A restart before repair must restore the adopted head, or
//     /// the replica votes for the view with a head at the install op.
//     #[compio::test]
//     async fn given_an_install_below_the_adopted_head_when_reopening_should_resume_the_adopted_view()
//     {
//         const BACKUP: u8 = 1;
//         const ADOPTED: u32 = 2;
//         const HELD: u64 = 1;
//         const HEAD: u64 = 3;
//         let directory = tempfile::tempdir().unwrap();
//         let (config, mut partition) = replica_with_prepare_history(directory.path(), BACKUP).await;
//         let partition_dir = config.get_partition_path(1, 1, 0);
//         let store = Rc::new(PingPongSuperblock::open(&partition_dir).await.unwrap());
//         partition.set_superblock(store.clone(), None);
//         let held = checksummed_segment_prepare(HELD, 0, 0, b"retained");
//         partition.on_replicate(held.clone()).await;
//         let persistence = partition.persistence.as_ref().unwrap().clone();
//         persistence
//             .drain_with_timeout(compio::runtime::time::sleep)
//             .await
//             .unwrap();
//         let mut parent = *held.header();
//         let mut headers = Vec::new();
//         for op in HELD + 1..=HEAD {
//             let mut header = PrepareHeader {
//                 op,
//                 request: op,
//                 timestamp: op,
//                 view: ADOPTED,
//                 parent: parent.checksum,
//                 ..parent
//             };
//             header.checksum = header.identity_checksum();
//             parent = header;
//             headers.push(header);
//         }
//         headers.reverse();
//         let start_view = Message::<iggy_binary_protocol::StartViewHeader>::new(size_of::<
//             iggy_binary_protocol::StartViewHeader,
//         >())
//         .transmute_header(|_, header: &mut iggy_binary_protocol::StartViewHeader| {
//             header.command = Command::StartView;
//             header.cluster = TEST_CLUSTER;
//             header.group = partition.namespace().inner();
//             header.view = ADOPTED;
//             header.replica = 2;
//             header.op = HEAD;
//             header.commit = HEAD - 1;
//             header.size =
//                 u32::try_from(size_of::<iggy_binary_protocol::StartViewHeader>()).unwrap();
//         });
//         partition.consensus().handle_start_view(
//             consensus::PlaneKind::Partitions,
//             start_view.header(),
//             bytemuck::cast_slice(&headers),
//         );
//         assert!(partition.persist_superblock_if_needed().await);
//
//         let body = held.as_slice()[size_of::<PrepareHeader>()..].to_vec();
//         let artifact = consensus::StateArtifact::for_bytes(
//             consensus::state_manifest::artifact_kind::SEGMENT_LOG,
//             0,
//             &body,
//         );
//         let staged = partition
//             .spill_transfer_segment(&artifact, body, None)
//             .await
//             .unwrap();
//         let offsets = crate::state_transfer::ConsumerOffsetsWire {
//             dedup_capacity: consensus::CLIENTS_TABLE_MAX,
//             prepare_checksum: Some(held.header().checksum),
//             checkpoint_prepare: held.as_slice().to_vec(),
//             next_offset: 1,
//             ..Default::default()
//         };
//         partition.consensus().begin_state_transfer_await();
//         partition
//             .install_state_transfer(&repair_config(), HELD, vec![staged], &offsets.encode())
//             .await
//             .unwrap();
//         assert_eq!(partition.consensus().sequencer().current_sequence(), HEAD);
//         assert!(partition.consensus().view_log_unproven());
//         let certified_after_install = persistence.certified_log_view();
//         let segment_size = partition.log.active_segment().max_size;
//         drop(persistence);
//         drop(partition);
//         drop(store);
//
//         let (store, latest) = PingPongSuperblock::open_with_latest(&partition_dir)
//             .await
//             .unwrap();
//         let journal::superblock::SuperblockContents::Present(bytes) = latest else {
//             panic!("adopted record must be durable");
//         };
//         let state = consensus::VsrState::try_from(bytes.as_slice()).unwrap();
//         let (mut reopened, _) = recording_partition_at(BACKUP, 3);
//         reopened.consensus.set_view(state.view);
//         reopened.consensus.set_log_view(state.log_view);
//         reopened
//             .consensus
//             .mark_superblock_durable(state.view, state.log_view);
//         reopened.set_superblock(Rc::new(store), Some(&state));
//         reopened.set_partition_dir(partition_dir);
//         reopened.log.active_segment_mut().max_size = segment_size;
//         reopened.open_persistence().await.unwrap();
//         assert!(
//             !reopened.requires_state_transfer(),
//             "the certificate after the install is below the persisted log view"
//         );
//         assert_eq!(
//             reopened.consensus().adopted_view_head(),
//             Some((ADOPTED, HEAD))
//         );
//         assert_eq!(
//             (
//                 reopened.consensus().sequencer().current_sequence(),
//                 reopened.consensus().view_log_unproven(),
//             ),
//             (HEAD, true),
//             "the restart dropped the adopted head: certified view after the install \
//              {certified_after_install:?}, state log_view {}",
//             state.log_view
//         );
//     }

//     /// The WAL drops a divergent run before the journal does. The canonical
//     /// tail cannot extend that WAL branch and exists only in the journal.
//     /// A crash before refill leaves the committed prefix uncertified,
//     /// so the replica claims no commit point and waits for a state transfer.
//     #[compio::test]
//     async fn given_a_crash_after_a_divergent_run_truncation_when_reopening_should_keep_the_committed_prefix_and_await_a_transfer()
//      {
//         const BACKUP: u8 = 1;
//         const APPLIED: u64 = 2;
//         const FIRST: u64 = APPLIED + 1;
//         const LAST: u64 = FIRST + 1;
//         const HEAD: u64 = LAST + 2;
//         const TRUNCATION_STEPS_MAX: usize = 8;
//         const COMMIT_ROUNDS_MAX: usize = 8;
//         let parents = segment_chain_parents(HEAD, b"canonical");
//         let canonical: Vec<_> = (1..=HEAD)
//             .map(|op| {
//                 let parent = parents[usize::try_from(op - 1).unwrap()];
//                 checksummed_segment_prepare(op, parent, op - 1, b"canonical")
//             })
//             .collect();
//         let mut parent = canonical[usize::try_from(APPLIED - 1).unwrap()]
//             .header()
//             .checksum;
//         let stale: Vec<_> = (FIRST..=LAST)
//             .map(|op| {
//                 let prepare = checksummed_segment_prepare(op, parent, op - 1, b"stale");
//                 parent = prepare.header().checksum;
//                 prepare
//             })
//             .collect();
//
//         for deferred in [false, true] {
//             let directory = tempfile::tempdir().unwrap();
//             let (config, mut partition) =
//                 replica_with_prepare_history(directory.path(), BACKUP).await;
//             let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
//             for prepare in canonical
//                 .iter()
//                 .take(usize::try_from(APPLIED).unwrap())
//                 .chain(&stale)
//             {
//                 partition.on_replicate(prepare.clone()).await;
//             }
//             partition.consensus().advance_commit_max(APPLIED);
//             for _ in 0..COMMIT_ROUNDS_MAX {
//                 partition.commit_journal(&config).await;
//                 persistence
//                     .drain_with_timeout(compio::runtime::time::sleep)
//                     .await
//                     .unwrap();
//                 partition.drive_persistence().await;
//                 if partition.consensus().commit_min() == APPLIED {
//                     break;
//                 }
//             }
//             assert_eq!(partition.consensus().commit_min(), APPLIED);
//             assert_eq!(persistence.head(), LAST, "the WAL holds the stale run");
//             for prepare in canonical.iter().skip(usize::try_from(LAST).unwrap()) {
//                 partition
//                     .log
//                     .journal()
//                     .inner
//                     .append(prepare.clone().into_frozen())
//                     .await
//                     .unwrap();
//             }
//             let segment_size = partition.log.active_segment().max_size;
//             // Fails the resident removal after the WAL truncation is durable.
//             partition.log.active_segment_mut().size = IggyByteSize::from(u64::MAX);
//
//             if deferred {
//                 partition.defer_view_truncation(FIRST..=LAST, Vec::new(), None);
//                 for _ in 0..TRUNCATION_STEPS_MAX {
//                     if !partition.view_truncation_pending() {
//                         break;
//                     }
//                     partition.resume_io(&config).await;
//                     persistence
//                         .drain_with_timeout(compio::runtime::time::sleep)
//                         .await
//                         .unwrap();
//                 }
//                 assert!(!partition.view_truncation_pending());
//             } else {
//                 assert!(
//                     partition
//                         .remove_divergent_prepares(FIRST..=LAST)
//                         .await
//                         .is_err()
//                 );
//             }
//             assert_eq!(persistence.head(), APPLIED, "deferred {deferred}");
//             assert!(
//                 (FIRST..=HEAD).all(|op| partition.log.journal().inner.holds_op(op)),
//                 "deferred {deferred}: the crash comes before the journal removed anything"
//             );
//             drop(persistence);
//             drop(partition);
//
//             let (mut reopened, _) = recording_partition_at(BACKUP, 3);
//             reopened.set_partition_dir(config.get_partition_path(1, 1, 0));
//             reopened.log.active_segment_mut().max_size = segment_size;
//             // Reuse the files the WAL references; installing an empty segment
//             // here would truncate the committed message bodies before recovery.
//             reopened.open_persistence().await.unwrap();
//             let wal = reopened.persistence.as_ref().unwrap();
//             assert_eq!(
//                 wal.head(),
//                 APPLIED,
//                 "deferred {deferred}: no op above the committed prefix"
//             );
//             for prepare in canonical.iter().take(usize::try_from(APPLIED).unwrap()) {
//                 let header = prepare.header();
//                 assert_eq!(
//                     wal.checksum(header.op),
//                     Some(header.checksum),
//                     "deferred {deferred}: committed op {} must stay in the WAL",
//                     header.op
//                 );
//             }
//             assert!(reopened.requires_state_transfer(), "deferred {deferred}");
//             assert_eq!(
//                 (
//                     reopened.consensus().status(),
//                     reopened.consensus().state_transfer_stage(),
//                 ),
//                 (
//                     consensus::Status::Recovering,
//                     consensus::StateTransferStage::AwaitingTarget,
//                 ),
//                 "deferred {deferred}"
//             );
//             assert_eq!(
//                 reopened.consensus().commit_min(),
//                 0,
//                 "deferred {deferred}: the old certificate cannot vouch for the prefix"
//             );
//         }
//     }

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

/// The first boot after an upgrade creates the external group offsets
/// directory. When that fails, the error has to say why, or the operator
/// sees a path and nothing else.
#[compio::test]
async fn given_unwritable_offsets_dir_when_recovering_should_report_why_the_external_dir_failed() {
    let directory = tempfile::tempdir().unwrap();
    let config = PartitionsConfig {
        path_layout: crate::PartitionPathLayout {
            streams_root: directory.path().to_string_lossy().into_owned(),
        },
        ..repair_config()
    };
    // A file where the external group directory belongs fails its creation.
    // One at `offsets/` would fail the consumer directory check first.
    std::fs::create_dir_all(config.get_offsets_path(1, 1, 0)).unwrap();
    std::fs::write(
        config.get_external_group_offsets_path(1, 1, 0),
        b"not a directory",
    )
    .unwrap();
    let (mut partition, _) = recording_partition_at(0, 3);
    partition.set_partition_dir(config.get_partition_path(1, 1, 0));

    let error = crate::partition_storage::configure_consumer_offsets_with_storage(
        &DiskStorage,
        &mut partition,
        &config,
        IggyNamespace::new(1, 1, 0),
        0,
    )
    .await
    .expect_err("the external group directory cannot be created");

    let crate::PartitionRecoveryError::CreateOffsetsDirectory { source, .. } = &error else {
        panic!("expected CreateOffsetsDirectory, got {error:?}");
    };
    assert!(
        error.to_string().ends_with(&source.to_string()),
        "the message must carry the I/O error: {error}"
    );
}

/// A partition made before external groups existed has no directory for them, and every WAL
/// checkpoint syncs each offset directory. Boot must create it before the first checkpoint,
/// or that checkpoint fails and fences the partition.
#[compio::test]
async fn given_layout_without_external_groups_dir_when_checkpointing_after_boot_should_succeed() {
    let directory = tempfile::tempdir().unwrap();
    let config = PartitionsConfig {
        path_layout: crate::PartitionPathLayout {
            streams_root: directory.path().to_string_lossy().into_owned(),
        },
        ..repair_config()
    };
    for dir in [
        config.get_consumer_offsets_path(1, 1, 0),
        config.get_consumer_group_offsets_path(1, 1, 0),
    ] {
        std::fs::create_dir_all(dir).unwrap();
    }
    let external_dir = config.get_external_group_offsets_path(1, 1, 0);
    let (mut partition, _) = recording_partition_at(0, 3);
    partition.set_partition_dir(config.get_partition_path(1, 1, 0));
    partition.runtime_options.durability = iggy_common::Durability::Persisted;
    crate::partition_storage::configure_consumer_offsets_with_storage(
        &DiskStorage,
        &mut partition,
        &config,
        IggyNamespace::new(1, 1, 0),
        0,
    )
    .await
    .unwrap();
    assert!(std::path::Path::new(&external_dir).is_dir());
    partition.open_persistence().await.unwrap();
    partition.log.retire_front().unwrap();
    partition.install_empty_segment(&config, 0).await.unwrap();
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    persistence
        .append(
            checksummed_segment_prepare(1, 0, 0, b"durable").into_frozen(),
            true,
        )
        .unwrap();
    assert!(persistence.start());
    Rc::clone(&persistence).run().await;
    partition.consensus.restore_commit_state(1, 1);

    persistence.request_checkpoint();
    partition.checkpoint_persistence(&config).await;
    persistence
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();

    assert!(partition.fatal().is_none());
    assert!(persistence.failure().is_none());
    assert_eq!(persistence.checkpoint_op(), 1);
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

#[compio::test]
async fn given_committed_store_when_offset_persist_fails_should_set_fatal_commit() {
    let dir = tempfile::tempdir().expect("temporary offset parent");
    let parent = dir.path().join("not-a-directory");
    std::fs::File::create(&parent).expect("create invalid directory fixture");
    let (mut partition, _) = recording_partition_at(0, 3);
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
        Some(parent.to_string_lossy().into_owned());
    partition.stats.increment_messages_count(1);
    partition
        .on_request(
            store_offset_request(42, 1, ConsumerKind::Consumer, 7, 0, AckLevel::Quorum),
            None,
        )
        .await;
    let header = partition
        .log
        .journal()
        .inner
        .header_by_op(1)
        .expect("prepared offset op");
    partition.consensus.restore_commit_state(0, 1);
    partition
        .handle_committed_entries(vec![header], &repair_config(), true)
        .await;
    let fatal = partition
        .fatal
        .as_ref()
        .expect("committed persistence failure fences partition");
    assert_eq!(fatal.operation, Some(Operation::StoreConsumerOffset));
    assert_eq!(fatal.op, 1);
    assert_eq!(
        partition.durable_consumer_offset_count(ConsumerKind::Consumer),
        0
    );
}

#[test]
fn given_reserved_owner_trailer_when_sizing_an_offset_prepare_should_match_its_projected_frame() {
    let partition = test_partition();
    let automatic = partition
        .build_poll_auto_commit_request(ConsumerKind::Consumer, 7, 0)
        .expect("auto-commit request");
    let explicit = store_offset_request(42, 1, ConsumerKind::Consumer, 7, 0, AckLevel::Quorum);
    for (label, request) in [("auto-commit", automatic), ("client", explicit)] {
        let sized = IggyPartition::<IggyMessageBus>::prepared_request_size(&request);
        let prepare = request.project(partition.consensus());
        assert_eq!(
            sized,
            prepare.as_slice().len(),
            "{label} store must be sized as the prepare it projects to"
        );
    }
}

#[compio::test]
async fn given_queued_offset_when_wal_capacity_is_exhausted_should_deny_before_minting() {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, replies) = recording_partition_at(0, 3);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.open_persistence().await.unwrap();
    partition.stats.increment_messages_count(1);
    let request = store_offset_request(42, 1, ConsumerKind::Consumer, 7, 0, AckLevel::Quorum);
    let retry = request.deep_copy();
    assert!(
        partition
            .consensus
            .push_queued_request(consensus::RequestEntry::with_sender(request, None))
            .is_ok()
    );
    partition
        .persistence
        .as_ref()
        .unwrap()
        .exhaust_capacity_for_test();
    partition.drain_request_queue_into_prepares(1).await;
    assert_eq!(partition.consensus.sequencer().current_sequence(), 0);
    assert_eq!(partition.persistence.as_ref().unwrap().head(), 0);
    assert!(partition.fatal().is_none());
    assert_eq!(replies.borrow().len(), 1);
    let reply = replies.borrow()[0].clone();
    let header = bytemuck::checked::from_bytes::<ReplyHeader>(
        &reply.1.as_slice()[..size_of::<ReplyHeader>()],
    );
    assert_eq!(header.status, IggyError::TransientNotAccepted.as_code());

    partition
        .persistence
        .as_ref()
        .unwrap()
        .release_capacity_for_test();
    partition.on_request(retry, None).await;
    assert_eq!(partition.consensus.sequencer().current_sequence(), 1);
    assert!(partition.fatal().is_none());
}

#[compio::test]
async fn given_queued_group_store_when_its_owner_is_replaced_should_refuse_it_at_promotion() {
    const GROUP_ID: u32 = 7;
    const CLIENT_ID: u128 = (1 << 64) + 42;
    let mut partition = test_partition();
    partition.stats.increment_messages_count(1);
    install_test_owner(&mut partition, u64::from(GROUP_ID), CLIENT_ID).await;
    let replaced = partition.consumer_group_owner(u64::from(GROUP_ID)).unwrap();
    install_test_owner(&mut partition, u64::from(GROUP_ID), CLIENT_ID + 1).await;
    let store = store_offset_request(
        replaced.client_id,
        1,
        ConsumerKind::ConsumerGroup,
        GROUP_ID,
        0,
        AckLevel::Quorum,
    )
    .transmute_header(|mut header, next: &mut RoutedRequestHeader| {
        header.metadata_watermark = partition.required_metadata_frontier;
        header.owner_generation = replaced.generation;
        *next = header;
    });
    let head = partition.consensus.sequencer().current_sequence();
    let (sender, receiver) = consensus::oneshot_channel();
    assert!(
        partition
            .consensus
            .push_queued_request(consensus::RequestEntry::with_sender(store, Some(sender)))
            .is_ok()
    );

    partition.drain_request_queue_into_prepares(1).await;

    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        head,
        "a store of a replaced owner must not be prepared"
    );
    assert_eq!(
        receiver.await.unwrap().header().status,
        IggyError::ConsumerGroupPartitionNotOwned(replaced.client_id, 0).as_code()
    );
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
}

#[compio::test]
async fn given_queued_auto_commit_when_already_covered_should_reply_with_committed_proof() {
    const CONSUMER_ID: u32 = 7;
    let mut partition = test_partition();
    partition.stats.increment_messages_count(1);
    let store = partition
        .build_poll_auto_commit_request(ConsumerKind::Consumer, CONSUMER_ID, 0)
        .unwrap();
    let (sender, receiver) = consensus::oneshot_channel();
    assert!(
        partition
            .consensus
            .push_queued_request(consensus::RequestEntry::with_sender(store, Some(sender)))
            .is_ok()
    );
    partition.seed_recovered_consumer_offset(ConsumerKind::Consumer, CONSUMER_ID, 0, 0);
    let head = partition.consensus.sequencer().current_sequence();

    partition.drain_request_queue_into_prepares(1).await;

    let reply = receiver.await.unwrap();
    assert_eq!(reply.header().status, 0);
    assert!(reply.header().has_committed_result());
    assert_eq!(partition.consensus.sequencer().current_sequence(), head);
    assert!(partition.consensus.pop_queued_request().is_none());
}

#[compio::test]
async fn given_queued_auto_commit_when_its_owner_is_replaced_should_refuse_it_at_promotion() {
    const GROUP_ID: u32 = 7;
    const CLIENT_ID: u128 = 42;
    let mut partition = test_partition();
    partition.stats.increment_messages_count(1);
    install_test_owner(&mut partition, u64::from(GROUP_ID), CLIENT_ID).await;
    let store = partition
        .build_poll_auto_commit_request(ConsumerKind::ConsumerGroup, GROUP_ID, 0)
        .unwrap();
    let generation = store.header().owner_generation;
    install_test_owner(&mut partition, u64::from(GROUP_ID), CLIENT_ID + 1).await;
    assert!(
        partition
            .consumer_group_owner(u64::from(GROUP_ID))
            .unwrap()
            .generation
            > generation
    );
    let head = partition.consensus.sequencer().current_sequence();
    assert!(
        partition
            .consensus
            .push_queued_request(consensus::RequestEntry::with_sender(store, None),)
            .is_ok()
    );

    partition.drain_request_queue_into_prepares(1).await;

    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        head,
        "an internal auto-commit must retain the owner generation it captured"
    );
    assert!(partition.consensus.pop_queued_request().is_none());
    assert_eq!(
        partition.group_offset_state(u64::from(GROUP_ID)),
        (None, None)
    );
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
}

#[compio::test]
async fn given_queued_capacity_denial_when_promoting_should_use_the_slot_for_next_existing_key() {
    let (mut partition, _) = recording_partition_at(0, 3);
    partition.set_consumer_offsets_max(1);
    partition.seed_recovered_consumer_offset(ConsumerKind::Consumer, 7, 0, 0);
    partition.consumer_offsets.pin().insert(
        7,
        ConsumerOffset::new(ConsumerKind::Consumer, 7, 0, String::new()),
    );
    partition.stats.increment_messages_count(1);
    for (request_id, consumer_id) in [(1, 8), (2, 7)] {
        assert!(
            partition
                .consensus
                .push_queued_request(consensus::RequestEntry::with_sender(
                    store_offset_request(
                        42,
                        request_id,
                        ConsumerKind::Consumer,
                        consumer_id,
                        0,
                        AckLevel::Quorum
                    ),
                    None,
                ))
                .is_ok()
        );
    }
    partition.drain_request_queue_into_prepares(1).await;
    assert_eq!(partition.consensus.pipeline_len(), 1);
    assert_eq!(
        partition
            .pending_consumer_offset_commits
            .get(&1)
            .expect("existing update projected")
            .consumer_id,
        7
    );
    assert!(partition.consensus.pop_queued_request().is_none());
}

#[compio::test]
async fn given_same_view_truncation_when_resynchronizing_should_release_discarded_reservations() {
    let mut partition = test_partition();
    journal_prepare(&partition, 1, Operation::StoreConsumerOffset).await;
    journal_prepare(&partition, 2, Operation::StoreConsumerOffset).await;
    partition.consensus.sequencer().set_sequence(2);
    partition.stage_consumer_offset_upsert(1, ConsumerKind::Consumer, 7, 0, false);
    partition.stage_consumer_offset_upsert(2, ConsumerKind::Consumer, 8, 0, false);
    partition.truncate_uncommitted_from(2).await.unwrap();
    partition.resynchronize_consumer_offset_reservations();
    assert!(partition.pending_consumer_offset_commits.contains_key(&1));
    assert!(!partition.pending_consumer_offset_commits.contains_key(&2));
    assert_eq!(
        partition.occupied_consumer_offset_count(ConsumerKind::Consumer),
        1
    );
    assert!(
        !partition
            .consumer_offset_capacity_for(ConsumerKind::Consumer)
            .is_uncertain()
    );
}

/// A primary-elect defers dropping the suffix its new view discards. An
/// acknowledgement that arrives first scans the new view and latches an
/// install from that suffix. A latch left on the dropped install refuses
/// every later install until another view change. Uncertain accounting
/// holds rescans for the shard tick, and the rebuild does not wait for it.
#[compio::test]
async fn given_ack_scan_before_deferred_truncation_when_resynchronizing_should_release_discarded_install()
 {
    const GROUP_ID: u64 = 7;
    const CLIENT_ID: u128 = 42;
    let config = repair_config();
    for uncertain in [false, true] {
        let mut partition = test_partition();
        let installed_op = install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
        let discarded = next_test_installation(&partition, GROUP_ID, CLIENT_ID + 1);
        partition
            .on_request(owner_install_request(&partition, &discarded), None)
            .await;
        let discarded_op = partition.consensus.sequencer().current_sequence();
        assert_eq!(discarded_op, installed_op + 1);

        partition.consensus.set_view(1);
        let stale_ack = Message::<PrepareOkHeader>::new(size_of::<PrepareOkHeader>())
            .transmute_header(|_, header: &mut PrepareOkHeader| {
                header.command = Command::PrepareOk;
                header.cluster = TEST_CLUSTER;
                header.group = partition.consensus.group();
                header.op = installed_op;
                header.operation = Operation::InstallConsumerGroupOwner;
                header.size = u32::try_from(size_of::<PrepareOkHeader>()).unwrap();
            });
        partition.on_ack(stale_ack, &config).await;
        assert_eq!(partition.pending_owner_install.get(), Some(discarded));

        partition.defer_view_truncation(discarded_op..=u64::MAX, Vec::new(), None);
        assert!(matches!(
            partition.resume_io(&config).await,
            crate::PartitionIoStep::ViewApplied { .. }
        ));
        // The view starts on the retained head with an empty pipeline.
        partition.consensus.sequencer().set_sequence(installed_op);
        partition.consensus.with_pipeline_mut(LocalPipeline::clear);
        if uncertain {
            for capacity in &partition.consumer_offset_capacities {
                capacity.mark_uncertain();
            }
        }
        partition.commit_journal(&config).await;
        assert!(
            !partition.consumer_group_owner_admitted(&discarded),
            "uncertain {uncertain}: the latch still names the dropped install: {:?}",
            partition.pending_owner_install.get()
        );

        let successor = InstallConsumerGroupOwnerRequest {
            owner: ConsumerGroupOwner {
                client_id: CLIENT_ID + 2,
                session: 1,
                generation: discarded.owner.generation + 1,
            },
            metadata_op: discarded.metadata_op + 1,
            ..discarded
        };
        partition
            .on_request(owner_install_request(&partition, &successor), None)
            .await;
        assert_eq!(
            partition.pending_owner_install.get(),
            Some(successor),
            "uncertain {uncertain}"
        );
        commit_log(&mut partition).await;
        assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
        assert_eq!(
            partition.consumer_group_owner(GROUP_ID),
            Some(successor.owner),
            "uncertain {uncertain}"
        );
    }
}

#[compio::test]
async fn given_promotion_denial_budget_when_no_more_commits_arrive_should_resume_queued_work() {
    let (mut partition, sent) = recording_partition_at(0, 3);
    partition.set_consumer_offsets_max(1);
    partition.seed_recovered_consumer_offset(ConsumerKind::Consumer, 7, 0, 0);
    partition.consumer_offsets.pin().insert(
        7,
        ConsumerOffset::new(ConsumerKind::Consumer, 7, 0, String::new()),
    );
    partition.stats.increment_messages_count(1);
    for (request, id) in [8, 9, 10, 11, 12, 7].into_iter().enumerate() {
        partition
            .consensus
            .push_queued_request(consensus::RequestEntry::with_sender(
                store_offset_request(
                    42,
                    request as u64 + 1,
                    ConsumerKind::Consumer,
                    id,
                    0,
                    AckLevel::Quorum,
                ),
                None,
            ))
            .unwrap();
    }
    partition.drain_request_queue_into_prepares(1).await;
    assert_eq!(sent.borrow().len(), PROMOTION_DENIALS_MAX);
    assert_eq!(partition.consensus.request_queue_len(), 2);
    assert_eq!(partition.consensus.pipeline_len(), 0);
    assert!(partition.queued_requests_ready());
    partition.resume_queued_requests().await;
    assert_eq!(partition.consensus.request_queue_len(), 0);
    assert_eq!(partition.consensus.pipeline_len(), 1);
    assert!(!partition.queued_requests_ready());
}

#[compio::test]
async fn given_covered_store_with_dirty_directory_when_sync_fails_should_fence_its_own_operation() {
    let dir = tempfile::tempdir().unwrap();
    let (mut partition, sent) = recording_partition_at(0, 3);
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
        Some(dir.path().to_string_lossy().into_owned());
    partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Persisted;
    let pending = PendingConsumerOffsetCommit::upsert_auto_commit(ConsumerKind::Consumer, 7, 10);
    partition
        .persist_consumer_offset_commit(pending)
        .await
        .unwrap();
    partition.apply_consumer_offset_commit(pending);
    partition.consumer_offset_dir_sync_fault.set(Some(0));
    partition.stage_consumer_offset_upsert(1, ConsumerKind::Consumer, 7, 5, true);
    partition.consensus.restore_commit_state(0, 1);
    let header = PrepareHeader {
        retry_capacity: u32::try_from(consensus::PARTITION_DEDUP_CLIENTS_MAX).unwrap(),
        session: 1,
        op: 1,
        operation: Operation::StoreConsumerOffset,
        client: 42,
        request: 1,
        ..Default::default()
    };
    partition
        .handle_committed_entries(vec![header], &repair_config(), true)
        .await;
    assert_eq!(partition.fatal.as_ref().unwrap().op, 1);
    assert_eq!(
        partition.fatal.as_ref().unwrap().operation,
        Some(Operation::StoreConsumerOffset)
    );
    assert_eq!(partition.consensus.commit_min(), 0);
    assert!(sent.borrow().is_empty());
    assert!(partition.consumer_offset_dirs_dirty[0].get());
}

#[compio::test]
async fn given_repeated_admitted_deletes_when_committed_should_be_idempotent() {
    let (mut partition, _) = recording_partition();
    partition.consumer_offsets.pin().insert(
        7,
        ConsumerOffset::new(ConsumerKind::Consumer, 7, 0, String::new()),
    );
    partition.seed_recovered_consumer_offset(ConsumerKind::Consumer, 7, 0, 0);
    for op in 1..=2 {
        partition.stage_consumer_offset_delete(op, ConsumerKind::Consumer, 7);
    }
    for op in 1..=2 {
        partition
            .apply_staged_consumer_offset_commit(op)
            .await
            .expect("admitted delete converges");
    }
    assert_eq!(
        partition.occupied_consumer_offset_count(ConsumerKind::Consumer),
        0
    );
}

#[compio::test]
async fn given_queued_auto_commit_when_view_changes_should_release_its_provisional_slot() {
    let namespace = IggyNamespace::new(1, 1, 0);
    let consensus = VsrConsensus::new(
        TEST_CLUSTER,
        0,
        3,
        namespace.inner(),
        RecordingBus::default(),
        LocalPipeline::with_capacities(1, 2),
    );
    consensus.init();
    let mut partition: IggyPartition<RecordingBus> = IggyPartition::with_in_memory_storage(
        Arc::new(PartitionStats::default()),
        consensus,
        IggyByteSize::from(1024 * 1024),
    );
    partition.set_runtime_options(TopicRuntimeOptions {
        durability: iggy_common::Durability::Persisted,
        consumer_offset_durability: iggy_common::Durability::Persisted,
        ..Default::default()
    });
    partition.stats.increment_messages_count(1);
    partition.set_consumer_offsets_max(2);
    partition
        .on_request(
            store_offset_request(42, 1, ConsumerKind::Consumer, 7, 0, AckLevel::Quorum),
            None,
        )
        .await;
    let result = poll_read_result(&partition, PollingConsumer::Consumer(8, 0), true, Some(0));
    let completion = partition
        .complete_poll(result)
        .expect("queue automatic commit");
    assert!(completion.replication.is_none());
    assert_eq!(
        partition.get_consumer_offset(PollingConsumer::Consumer(8, 0)),
        Some(0)
    );
    assert_eq!(
        partition.occupied_consumer_offset_count(ConsumerKind::Consumer),
        2
    );
    assert_eq!(partition.consensus.request_queue_len(), 1);
    partition.consensus.set_view(3);
    partition.resynchronize_consumer_offset_reservations();
    assert_eq!(partition.consensus.request_queue_len(), 0);
    assert_eq!(
        partition.occupied_consumer_offset_count(ConsumerKind::Consumer),
        1
    );
}

#[compio::test]
async fn given_auto_commit_guard_when_pump_admits_should_transfer_to_journal_without_reply() {
    let (mut partition, sent) = recording_partition_at(0, 3);
    partition.stats.increment_messages_count(1);
    partition.set_consumer_offsets_max(1);
    let result = poll_read_result(&partition, PollingConsumer::Consumer(7, 0), true, Some(0));
    let completion = partition
        .complete_poll(result)
        .expect("accept automatic commit");
    assert!(partition.pending_consumer_offset_commits.is_empty());
    partition
        .replicate_poll_completion(completion.replication.expect("assigned prepare"))
        .await;
    assert_eq!(partition.pending_consumer_offset_commits.len(), 1);
    assert_eq!(
        partition.occupied_consumer_offset_count(ConsumerKind::Consumer),
        1
    );
    assert!(sent.borrow().is_empty());
    partition
        .apply_staged_consumer_offset_commit(1)
        .await
        .unwrap();
    assert_eq!(
        partition.durable_consumer_offset_count(ConsumerKind::Consumer),
        1
    );
    assert!(sent.borrow().is_empty());
}

/// Auto-commit ops carry no segment bytes, so no message threshold ever
/// flushes them; a consume-only partition must still evict them at the
/// same message-count bound instead of holding one op per poll forever.
#[compio::test]
async fn given_only_consumer_offset_ops_when_committed_should_keep_the_journal_bounded() {
    const THRESHOLD: u32 = 8;
    let mut partition = test_partition();
    let mut config = repair_config();
    config.messages_required_to_save = THRESHOLD;
    let last_op = 3 * u64::from(THRESHOLD);
    for op in 1..=last_op {
        journal_store_offset(&mut partition, op, 7, op).await;
        partition.consensus().advance_commit_max(op);
        partition.commit_journal(&config).await;
        assert!(partition.fatal().is_none());
        assert_eq!(partition.consensus().commit_min(), op);
        assert!(
            partition.log.journal().inner.resident_count() < THRESHOLD as usize,
            "op {op}: the resident journal must evict at the threshold"
        );
    }
    assert_eq!(
        partition.get_consumer_offset(PollingConsumer::Consumer(7, 0)),
        Some(last_op),
        "every committed offset must survive the evictions"
    );
    assert_eq!(
        partition.log.active_segment().size.as_bytes_u64(),
        0,
        "offset ops never reach the segment"
    );
}

/// A below-threshold batch committed earlier sits in front of the offset
/// ops. The control-op bound flushes it as a small chunk, the way the
/// shutdown flush would, and evicts the whole prefix behind it.
#[compio::test]
async fn given_small_message_tail_when_offset_ops_reach_the_bound_should_flush_it_and_evict() {
    const THRESHOLD: u32 = 8;
    let mut config = repair_config();
    config.messages_required_to_save = THRESHOLD;
    let (directory, mut partition) = Box::pin(disk_poll_partition(&config)).await;
    journal_send_batch(&mut partition, 1).await;
    partition.consensus().advance_commit_max(1);
    partition.commit_journal(&config).await;
    assert_eq!(partition.consensus().commit_min(), 1);
    assert_eq!(
        partition.log.active_segment().size.as_bytes_u64(),
        0,
        "one message stays resident below the threshold"
    );

    let last_op = 1 + u64::from(THRESHOLD);
    for op in 2..=last_op {
        journal_store_offset(&mut partition, op, 7, op).await;
    }
    partition.consensus().advance_commit_max(last_op);
    partition.commit_journal(&config).await;

    assert!(partition.fatal().is_none());
    assert_eq!(partition.consensus().commit_min(), last_op);
    let one_record = build_segment_record(IggyNamespace::new(1, 1, 0), 0).len() as u64;
    assert_eq!(
        partition.log.active_segment().size.as_bytes_u64(),
        one_record,
        "the resident tail lands in the segment"
    );
    assert!(
        partition.log.journal().inner.is_empty(),
        "the committed prefix, offset ops included, is evicted"
    );
    assert_eq!(partition.log.journal().info.messages_count, 0);
    assert_eq!(
        partition.get_consumer_offset(PollingConsumer::Consumer(7, 0)),
        Some(last_op)
    );

    let offset_path = partition
        .persisted_offset_path(ConsumerKind::Consumer, 7)
        .unwrap();
    drop(partition);
    let segment = std::fs::read(directory.path().join("00000000000000000000.log")).unwrap();
    assert_eq!(segment.len() as u64, one_record);
    let batch = decode_batch_slice(&segment).unwrap();
    assert_eq!(batch.header.base_offset, 0);
    assert_eq!(batch.message_count(), 1);
    assert_eq!(batch.iter().next().unwrap().payload, b"abcdefgh");
    let index = IggyIndexReader::new(
        directory
            .path()
            .join("00000000000000000000.index")
            .to_str()
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        index.load_last().await.unwrap(),
        Some(IggyIndex::new(0, batch.header.base_timestamp, 0))
    );
    assert_eq!(
        crate::offset_storage::decode_offset_record(&std::fs::read(offset_path).unwrap()),
        crate::offset_storage::OffsetRecord::Value {
            offset: last_op,
            checksummed: true,
        }
    );
}

#[compio::test]
async fn given_new_external_group_offset_when_committed_should_request_reconciliation() {
    let (mut partition, _) = recording_partition();
    let epoch = partition.consumer_group_offsets_reconcile_epoch.clone();
    let initial = epoch.get();
    partition.stage_consumer_offset_upsert(1, ConsumerKind::ExternalGroup, 7, 1, false);
    partition
        .apply_staged_consumer_offset_commit(1)
        .await
        .unwrap();
    assert!(
        epoch.get() > initial,
        "a new external group key must be reconciled against deleted groups"
    );
}

#[compio::test]
async fn given_group_offset_updates_when_key_already_exists_should_keep_reconciliation_idle() {
    let (mut partition, _) = recording_partition();
    let epoch = partition.consumer_group_offsets_reconcile_epoch.clone();
    let initial = epoch.get();
    partition.stage_consumer_offset_upsert(1, ConsumerKind::ConsumerGroup, 7, 1, true);
    partition
        .apply_staged_consumer_offset_commit(1)
        .await
        .unwrap();
    assert!(epoch.get() > initial);
    let after_create = epoch.get();
    assert_eq!(partition.dead_group_offset_keys(|_| true), []);
    assert_eq!(epoch.get(), after_create);
    partition.stage_consumer_offset_upsert(2, ConsumerKind::ConsumerGroup, 7, 2, true);
    partition
        .apply_staged_consumer_offset_commit(2)
        .await
        .unwrap();
    assert_eq!(epoch.get(), after_create);
    partition.consumer_group_offsets.pin().insert(
        ConsumerGroupId(8),
        ConsumerOffset::new(ConsumerKind::ConsumerGroup, 8, 3, String::new()),
    );
    partition.stage_consumer_offset_upsert(3, ConsumerKind::ConsumerGroup, 8, 3, true);
    partition
        .apply_staged_consumer_offset_commit(3)
        .await
        .unwrap();
    assert!(
        epoch.get() > after_create,
        "first durable commit must discover an eager map key"
    );
}

#[compio::test]
async fn given_primary_view_change_when_map_pressure_arrives_should_reclaim_only_unprotected_key() {
    let (mut partition, _) = recording_partition_at(0, 3);
    partition.set_consumer_offsets_max(4);
    partition.stats.increment_messages_count(1);
    partition.seed_recovered_consumer_offset(ConsumerKind::Consumer, 7, 0, 0);
    partition
        .on_request(
            store_offset_request(42, 1, ConsumerKind::Consumer, 8, 0, AckLevel::Quorum),
            None,
        )
        .await;
    let held = partition
        .consumer_offset_capacity_for(ConsumerKind::Consumer)
        .reserve_provisional(9, &partition.durable_consumer_offsets)
        .unwrap();
    for id in 7..=10 {
        partition.consumer_offsets.pin().insert(
            id as usize,
            ConsumerOffset::new(ConsumerKind::Consumer, id, 0, String::new()),
        );
    }
    partition.consensus.set_view(3);
    partition.resynchronize_consumer_offset_reservations();
    assert_eq!(partition.consumer_offsets.len(), 4);
    partition.reclaim_phantom_offsets(ConsumerKind::Consumer, 4);
    assert_eq!(partition.consumer_offsets.len(), 3);
    assert!(!partition.consumer_offsets.pin().contains_key(&10));
    for id in 7..=9 {
        assert!(partition.consumer_offsets.pin().contains_key(&id));
    }
    drop(held);
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

#[test]
fn given_read_result_when_owner_accepts_should_advance_before_replication() {
    let (mut partition, _) = recording_partition();
    // Let Next compare its starting offset with the committed frontier (0)
    // instead of taking the shortcut for a partition that has never had data.
    partition.offset_space.committed_seeded = true;
    let consumer_id = 7;
    let partition_id = 0;
    let auto_commit = true;
    let consumer = PollingConsumer::Consumer(consumer_id, partition_id);
    let read_result = poll_read_result(&partition, consumer, auto_commit, Some(0));
    assert_eq!(partition.get_consumer_offset(consumer), None);

    // Acceptance advances local progress and assigns replication work, but
    // the automatic commit has not yet been staged in the journal.
    let completion = partition.complete_poll(read_result).expect("accept poll");
    assert_eq!(partition.get_consumer_offset(consumer), Some(0));
    assert!(completion.replication.is_some());
    assert!(partition.pending_consumer_offset_commits.is_empty());

    // Next already starts after offset 0, before replication runs.
    let validate_checksum = false;
    let next_result = partition
        .build_poll_plan(
            consumer,
            &PollingArgs::new(iggy_common::PollingStrategy::next(), 1, auto_commit),
            validate_checksum,
        )
        .execute_resident();
    assert!(next_result.fragments.is_empty());
}

#[test]
fn given_capacity_refusal_when_poll_completes_should_preserve_existing_progress() {
    let (mut partition, _) = recording_partition();
    let durable_consumer_id = 8;
    let polling_consumer_id = 7;
    let partition_id = 0;
    let auto_commit = true;

    // Another consumer owns the only durable slot. The polling consumer has
    // local progress at 4, but cannot admit an automatic commit through 9.
    partition.set_consumer_offsets_max(1);
    partition.seed_recovered_consumer_offset(ConsumerKind::Consumer, durable_consumer_id, 0, 0);
    partition.apply_local_poll_offset(ConsumerKind::Consumer, polling_consumer_id, 4);
    let consumer = PollingConsumer::Consumer(polling_consumer_id as usize, partition_id);
    let read_result = poll_read_result(&partition, consumer, auto_commit, Some(9));

    assert!(matches!(
        partition.complete_poll(read_result),
        Err(IggyError::TooManyConsumerOffsets)
    ));
    assert_eq!(partition.get_consumer_offset(consumer), Some(4));
    assert_eq!(partition.consensus.pipeline_len(), 0);
}

/// A backup answering a read cannot originate the offset prepare, so
/// admitting the commit would advance local progress alone. The primary
/// would still hold the old offset and hand the same messages out again.
/// A normal backup names the primary as the place to retry, and a backup
/// in transfer cannot name it.
#[test]
fn given_backup_when_auto_commit_poll_completes_should_reject_without_progress() {
    for (transferring, expected) in [
        (false, IggyError::NotPrimary),
        (true, IggyError::TransientNotAccepted),
    ] {
        let (mut partition, _) = recording_partition_at(1, 3);
        if transferring {
            partition.consensus.begin_state_transfer_await();
        }
        let consumer = PollingConsumer::Consumer(7, 0);
        let read_result = poll_read_result(&partition, consumer, true, Some(9));

        assert_eq!(partition.complete_poll(read_result).unwrap_err(), expected);
        assert_eq!(partition.get_consumer_offset(consumer), None);
        assert_eq!(partition.consensus.pipeline_len(), 0);
    }
}

/// A backup whose durable table already covers the offset has nothing to
/// replicate, so there is no divergence to prevent and the refusal must
/// not reach it. Otherwise a caught-up follower would fail reads over a
/// commit the group already agreed.
#[test]
fn given_backup_when_auto_commit_offset_is_already_durable_should_be_accepted() {
    let (mut partition, _) = recording_partition_at(1, 3);
    let consumer = PollingConsumer::Consumer(7, 0);
    partition.seed_recovered_consumer_offset(ConsumerKind::Consumer, 7, 9, 9);
    let read_result = poll_read_result(&partition, consumer, true, Some(9));

    let completion = partition
        .complete_poll(read_result)
        .expect("an already-durable offset admits no commit");
    assert!(completion.replication.is_none());
    assert_eq!(partition.consensus.pipeline_len(), 0);
}

/// An empty read never reaches automatic-commit admission and mutates no
/// progress, so the refusal above must not spread to it: a backup has to
/// keep answering the tail of a partition it is caught up on.
#[test]
fn given_backup_when_empty_auto_commit_poll_completes_should_be_accepted() {
    let (mut partition, _) = recording_partition_at(1, 3);
    let consumer = PollingConsumer::Consumer(7, 0);
    let read_result = poll_read_result(&partition, consumer, true, None);

    let completion = partition
        .complete_poll(read_result)
        .expect("an empty read admits no commit");
    assert!(completion.replication.is_none());
    assert_eq!(partition.get_consumer_offset(consumer), None);
}

#[test]
fn given_primary_when_role_changes_before_auto_commit_completion_should_reject_without_progress() {
    for transferring in [false, true] {
        let (mut partition, _) = recording_partition_at(0, 3);
        let consumer = PollingConsumer::ConsumerGroup(7, 0);
        let read_result = poll_read_result(&partition, consumer, true, Some(9));
        if transferring {
            partition.consensus.begin_state_transfer_await();
        } else {
            partition.consensus.begin_view_probe();
        }
        assert!(matches!(
            partition.complete_poll(read_result),
            Err(IggyError::TransientNotAccepted)
        ));
        assert_eq!(partition.group_offset_state(7), (None, None));
        assert_eq!(partition.consensus.pipeline_len(), 0);
    }
}

// A retirement debt is paid only from an empty pipeline, so an automatic
// commit waits for the payment like a session request does.
// #[test]
// fn given_retirement_debt_when_auto_commit_poll_completes_should_reject_without_progress() {
//     const DEBT_OP: u64 = 40;
//     for owed in [false, true] {
//         let (mut partition, _) = recording_partition_at(0, 3);
//         let consumer = PollingConsumer::Consumer(7, 0);
//         if owed {
//             assert!(matches!(
//                 partition.observe_retirement_debt(DEBT_OP),
//                 RetirementDebtStatus::Unpaid(Some(_))
//             ));
//         }
//         let read_result = poll_read_result(&partition, consumer, true, Some(9));
//         let completion = partition.complete_poll(read_result);
//         if owed {
//             assert!(matches!(completion, Err(IggyError::TransientNotAccepted)));
//             assert_eq!(partition.get_consumer_offset(consumer), None);
//             assert_eq!(partition.consensus.pipeline_len(), 0);
//         } else {
//             assert!(completion.unwrap().replication.is_some());
//         }
//     }
// }

/// `on_replicate` refuses prepares during shutdown, retirement and a
/// transition. An automatic commit admitted then would hold a pipeline
/// slot that no journal backs, and every later op would wait behind it.
#[test]
fn given_primary_refusing_prepares_when_auto_commit_poll_completes_should_reject_without_progress()
{
    let refusals: [fn(&mut IggyPartition<RecordingBus>); 3] = [
        IggyPartition::begin_shutdown_io,
        |partition| partition.io_active.retire(),
        |partition| {
            let message = Message::<iggy_binary_protocol::RequestStartViewHeader>::new(size_of::<
                iggy_binary_protocol::RequestStartViewHeader,
            >(
            ));
            partition.defer_view_transition(server_common::MessageBag::RequestStartView(message));
        },
    ];
    for refuse in refusals {
        let (mut partition, _) = recording_partition();
        let consumer = PollingConsumer::Consumer(7, 0);
        let read_result = poll_read_result(&partition, consumer, true, Some(9));
        refuse(&mut partition);
        assert!(matches!(
            partition.complete_poll(read_result),
            Err(IggyError::TransientNotAccepted)
        ));
        assert_eq!(partition.get_consumer_offset(consumer), None);
        assert_eq!(partition.consensus.pipeline_len(), 0);
    }
}

/// Shutdown promotes no queued request, because `on_replicate` would drop
/// its prepare and `DrainAccepted` would wait for it until the pump drain
/// times out, skipping the final flush.
#[compio::test]
async fn given_queued_request_when_shutdown_drains_should_deny_it_and_complete() {
    const STEPS_MAX: usize = 32;
    let (mut partition, replies) = recording_partition();
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    let order = partition.next_local_request_order().unwrap();
    let request = consensus::RequestEntry::with_sender(
        checksumless_send_request(partition.namespace(), 1),
        None,
    );
    assert!(partition.queue_local_request(request, order).is_ok());

    partition.begin_shutdown_io();
    let config = repair_config();
    for _ in 0..STEPS_MAX {
        if partition.shutdown_io_complete() {
            break;
        }
        partition.resume_io(&config).await;
    }

    assert!(partition.shutdown_io_complete());
    assert_eq!(partition.consensus.pipeline_len(), 0);
    assert_eq!(partition.consensus.request_queue_len(), 0);
    let replies = replies.borrow();
    assert_eq!(replies.len(), 1);
    let reply = bytemuck::checked::from_bytes::<ReplyHeader>(
        &replies[0].1.as_slice()[..size_of::<ReplyHeader>()],
    );
    assert_eq!(reply.status, IggyError::TransientNotAccepted.as_code());
}

#[compio::test]
async fn given_shutdown_waiting_for_io_when_interrupted_should_notify_and_keep_resources() {
    let directory = tempfile::tempdir().unwrap();
    let mut partition = partition_at_view(1, 1);
    partition.set_partition_dir(directory.path().to_str().unwrap().to_owned());
    partition.runtime_options.durability = iggy_common::Durability::Persisted;
    partition.open_persistence().await.unwrap();
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    assert!(!persistence.certify_log_view(2, 0, 0));
    assert!(persistence.start());
    partition.set_superblock(Rc::new(RecordingSuperblock::default()), None);
    let notifications = Rc::new(Cell::new(0));
    let observed = Rc::clone(&notifications);
    partition.set_io_notifier(
        Rc::new(move |_, _| observed.set(observed.get() + 1)),
        crate::largest_legal_job_charge().unwrap(),
    );
    assert!(!partition.persist_superblock_if_needed().await);
    let config = repair_config();
    let crate::PartitionIoStep::Ready(plan) = partition.resume_io(&config).await else {
        panic!("view persistence must be ready");
    };
    let captured = partition.capture_io(plan, &config).unwrap().unwrap();

    partition.begin_shutdown_io();
    assert!(matches!(
        partition.resume_io(&config).await,
        crate::PartitionIoStep::Pending
    ));
    assert!(!partition.shutdown_io_complete());
    notifications.set(0);

    captured.quiescence.interrupt();
    partition.interrupt_io(captured.identity);
    assert!(notifications.get() > 0, "the shutdown owner must be ready");
    partition.resume_io(&config).await;

    assert!(
        !partition.shutdown_io_complete(),
        "accepted WAL work must drain"
    );
    assert!(
        partition.needs_io_retry(),
        "the retained file identity must not suppress WAL deadline checks"
    );
    Rc::clone(&persistence).run().await;
    partition.resume_io(&config).await;
    assert!(persistence.is_quiescent());
    assert!(partition.shutdown_io_complete());
    assert!(!partition.needs_io_retry());
    assert_eq!(partition.io_active.get(), Some(captured.identity));
    assert_eq!(captured.quiescence.get(), Some(captured.identity));
    assert!(partition.superblock_lock.try_acquire().is_none());
}

#[compio::test]
async fn shutdown_flushes_committed_messages_with_an_unacknowledged_pipeline_tail() {
    for (replica_count, store_offset) in [(3, false), (1, true), (3, true)] {
        let directory = tempfile::tempdir().unwrap();
        let (mut partition, replies) = recording_partition_at(0, replica_count);
        let mut config = repair_config();
        config.messages_required_to_save = u32::MAX;
        partition.set_partition_dir(directory.path().to_str().unwrap().to_owned());
        partition.log.retire_front().unwrap();
        partition.install_empty_segment(&config, 0).await.unwrap();
        let first = checksummed_segment_prepare(1, 0, 0, b"committed");
        let second = checksummed_segment_prepare(2, first.header().checksum, 1, b"pending");
        let mut expected = first.as_slice()[size_of::<PrepareHeader>()..].to_vec();
        let tail = second.as_slice()[size_of::<PrepareHeader>()..].to_vec();
        for prepare in [first, second] {
            partition.consensus.with_pipeline_mut(|pipeline| {
                pipeline.push(PipelineEntry::new(*prepare.header()));
            });
            partition
                .append_repaired_send_messages(prepare)
                .await
                .unwrap();
        }
        partition.consensus.sequencer().set_sequence(2);
        partition.consensus.advance_commit_max(1);
        partition.commit_journal(&config).await;
        assert_eq!(partition.consensus.commit_min(), 1);
        assert_eq!(partition.consensus.pipeline_len(), 1);
        assert!(partition.persistence.is_none());
        partition.set_io_notifier(
            Rc::new(|_, _| {}),
            crate::largest_legal_job_charge().unwrap(),
        );
        replies.borrow_mut().clear();
        if store_offset {
            partition
                .on_request(
                    store_offset_request(42, 1, ConsumerKind::Consumer, 7, 1, AckLevel::NoAck),
                    None,
                )
                .await;
            assert_eq!(partition.no_ack.len(), usize::from(replica_count == 1));
            assert_eq!(
                partition.consensus.request_queue_len(),
                usize::from(replica_count > 1)
            );
            assert!(replies.borrow().is_empty());
        }
        partition.begin_shutdown_io();
        settle_partition_io(&mut partition, &config).await;
        assert!(
            partition.shutdown_io_complete(),
            "shutdown stalled: replica_count={replica_count}, store_offset={store_offset}"
        );
        if store_offset {
            let replies = replies.borrow();
            assert_eq!(replies.len(), 1);
            let reply = bytemuck::checked::from_bytes::<ReplyHeader>(
                &replies[0].1.as_slice()[..size_of::<ReplyHeader>()],
            );
            assert_eq!(reply.status, IggyError::TransientNotAccepted.as_code());
            assert!(partition.consumer_offsets.pin().is_empty());
            assert_eq!(
                partition.occupied_consumer_offset_count(ConsumerKind::Consumer),
                0
            );
        }
        let path = directory.path().join("00000000000000000000.log");
        assert_eq!(std::fs::read(&path).unwrap(), expected);

        partition.consensus.advance_commit_max(2);
        assert!(
            !partition.shutdown_io_complete(),
            "a late commit must invalidate completion"
        );
        settle_partition_io(&mut partition, &config).await;
        expected.extend(tail);
        assert!(partition.shutdown_io_complete());
        assert_eq!(std::fs::read(&path).unwrap(), expected);
    }
}

#[compio::test]
async fn shutdown_keeps_wal_enqueue_paused_after_a_late_commit_flush() {
    const STEPS_MAX: usize = 64;
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition_at(1, 3);
    let mut config = repair_config();
    config.messages_required_to_save = u32::MAX;
    partition.set_partition_dir(directory.path().to_str().unwrap().to_owned());
    partition.runtime_options.durability = iggy_common::Durability::Persisted;
    partition.open_persistence().await.unwrap();
    partition.log.retire_front().unwrap();
    partition.install_empty_segment(&config, 0).await.unwrap();
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    let first = checksummed_segment_prepare(1, 0, 0, b"first");
    let second = checksummed_segment_prepare(2, first.header().checksum, 1, b"second");
    let tail = checksummed_segment_prepare(3, second.header().checksum, 2, b"uncommitted");
    for prepare in [&first, &second] {
        persistence
            .append(prepare.clone().into_frozen(), true)
            .unwrap();
    }
    assert!(persistence.start());
    Rc::clone(&persistence).run().await;
    for prepare in [first, second, tail] {
        partition
            .append_repaired_send_messages(prepare)
            .await
            .unwrap();
    }
    partition.consensus.sequencer().set_sequence(3);
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    partition.begin_shutdown_io();

    for commit_max in 1..=3 {
        partition.consensus.advance_commit_max(commit_max);
        for _ in 0..STEPS_MAX {
            settle_partition_io(&mut partition, &config).await;
            if partition.shutdown_io_complete() {
                break;
            }
            persistence
                .drain_with_timeout(compio::runtime::time::sleep)
                .await
                .unwrap();
        }
        assert!(
            partition.shutdown_io_complete(),
            "shutdown must finish the flush of commit {commit_max}"
        );
        assert!(persistence.is_quiescent());
        assert_eq!(persistence.head(), 2);
        partition.acknowledge_prepare(3).await;
        assert_eq!(
            persistence.head(),
            2,
            "shutdown must keep resident repair history out of the WAL after flushing commit {commit_max}"
        );
        assert!(persistence.is_quiescent());
    }
}

#[compio::test]
async fn shutdown_ignores_commit_heartbeats_beyond_the_local_journal() {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition_at(1, 3);
    let mut config = repair_config();
    config.messages_required_to_save = u32::MAX;
    partition.set_partition_dir(directory.path().to_str().unwrap().to_owned());
    partition.log.retire_front().unwrap();
    partition.install_empty_segment(&config, 0).await.unwrap();
    let prepare = checksummed_segment_prepare(1, 0, 0, b"committed");
    let expected = prepare.as_slice()[size_of::<PrepareHeader>()..].to_vec();
    partition
        .append_repaired_send_messages(prepare)
        .await
        .unwrap();
    partition.consensus.sequencer().set_sequence(1);
    partition.consensus.advance_commit_max(1);
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    partition.begin_shutdown_io();
    settle_partition_io(&mut partition, &config).await;
    assert!(partition.shutdown_io_complete());
    assert_eq!(
        std::fs::read(directory.path().join("00000000000000000000.log")).unwrap(),
        expected
    );

    for commit_max in 2..=4 {
        partition.consensus.advance_commit_max(commit_max);
        assert!(
            partition.shutdown_io_complete(),
            "remote commit {commit_max} must not invalidate the local flush"
        );
        assert!(matches!(
            partition.resume_io(&config).await,
            crate::PartitionIoStep::Pending
        ));
        assert!(partition.shutdown_io_complete());
    }
}

#[compio::test]
async fn shutdown_with_missing_materialization_drains_wal_without_collapsing_reservation() {
    let directory = tempfile::tempdir().unwrap();
    let mut partition = partition_at_view(1, 1);
    let store = Rc::new(RecordingSuperblock::default());
    let recovered = recorded_state(1, 100);
    partition.set_superblock(Rc::clone(&store), Some(&recovered));
    partition.set_partition_dir(directory.path().to_str().unwrap().to_owned());
    partition.runtime_options.durability = iggy_common::Durability::Persisted;
    partition.open_persistence().await.unwrap();
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    persistence.certify_log_view(1, 0, 0);
    assert!(persistence.start());
    partition.materialization_missing = true;
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    partition.begin_shutdown_io();
    settle_partition_io(&mut partition, &repair_config()).await;
    assert!(!partition.shutdown_io_complete());
    Rc::clone(&persistence).run().await;
    settle_partition_io(&mut partition, &repair_config()).await;
    assert!(partition.shutdown_io_complete());
    assert!(persistence.is_quiescent());
    assert_eq!(store.attempts.get(), 0);
    assert_eq!(
        partition.durable_offset_reserved.get(),
        recovered.offset_reserved
    );
}

#[compio::test]
async fn shutdown_fences_a_failed_flush_already_requested_by_transfer() {
    let mut partition = test_partition();
    let config = repair_config();
    let prepare = checksummed_segment_prepare(1, 0, 0, b"committed");
    partition
        .append_repaired_send_messages(prepare)
        .await
        .unwrap();
    partition.consensus.restore_commit_state(1, 1);
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    partition.maintenance.transfer_flush = true;
    partition.request_flush(1);
    partition.begin_shutdown_io();
    partition.reject_io_capture(
        crate::PartitionIoContinuation::Materialization,
        IggyError::CannotWriteToFile,
    );
    assert!(partition.fatal().is_some());
    settle_partition_io(&mut partition, &config).await;
    assert!(partition.shutdown_io_complete());
}

//     #[test]
//     fn given_group_read_without_auto_commit_when_history_changes_should_not_record_last_polled() {
//         let (mut partition, _) = recording_partition();
//         let group_id = 7;
//         let member_id = 1;
//         let auto_commit = false;
//         let consumer = PollingConsumer::ConsumerGroup(group_id, member_id);
//         let old_result = poll_read_result(&partition, consumer, auto_commit, Some(0));
//
//         // Group reads still record last_polled with automatic commits disabled.
//         // Retiring the history must prevent even that local progress update.
//         partition.invalidate_poll_history();
//         assert!(matches!(
//             partition.complete_poll(old_result),
//             Err(IggyError::TransientNotAccepted)
//         ));
//         let (last_polled, committed) = partition.group_offset_state(group_id as u64);
//         assert_eq!(
//             last_polled, None,
//             "the old read must not record group progress"
//         );
//         assert_eq!(committed, None, "automatic commits are disabled");
//     }

/// A replica applies the delete fence from its partition log, and its
/// metadata can apply the delete later. Until then reads planned before or
/// after the fence pass every metadata check. Completion refuses them with
/// or without automatic commits, also when the offset is already durable.
#[compio::test]
async fn given_deleted_history_when_poll_completes_should_refuse_without_progress() {
    const GROUP_ID: u64 = 7;
    const CLIENT_ID: u128 = 42;
    const CONSUMER_ID: u32 = 8;
    const DURABLE_CONSUMER_ID: u32 = 9;
    const METADATA_OP: u64 = 10;
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition();
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
    partition.seed_recovered_consumer_offset(ConsumerKind::Consumer, DURABLE_CONSUMER_ID, 0, 0);
    let group = PollingConsumer::ConsumerGroup(usize::try_from(GROUP_ID).unwrap(), 0);
    let consumer = |id: u32| PollingConsumer::Consumer(usize::try_from(id).unwrap(), 0);
    let reads = |partition: &IggyPartition<RecordingBus>| {
        [
            (group, false),
            (group, true),
            (consumer(CONSUMER_ID), false),
            (consumer(CONSUMER_ID), true),
            (consumer(DURABLE_CONSUMER_ID), true),
        ]
        .map(|(polling, auto_commit)| poll_read_result(partition, polling, auto_commit, Some(0)))
    };
    let planned_before_fence = reads(&partition);
    let transition = TransitionPartitionHistoryRequest {
        incarnation: partition.created_revision(),
        metadata_op: METADATA_OP,
    };
    let fence = partition_control_request(
        &partition,
        Operation::TransitionPartitionHistory,
        &transition.to_bytes(),
        METADATA_OP,
    );
    partition.on_request(fence, None).await;
    let fence_op = partition.consensus.sequencer().current_sequence();
    partition.consensus.advance_commit_max(fence_op);
    partition.commit_journal(&repair_config()).await;
    assert_eq!(
        partition.installed_history_transition(&transition),
        Some(fence_op)
    );

    for read in planned_before_fence.into_iter().chain(reads(&partition)) {
        let (polling, auto_commit) = (read.context.consumer, read.context.auto_commit);
        let completion = partition.complete_poll(read);
        assert!(
            matches!(completion, Err(IggyError::HistoryUnavailable)),
            "{polling:?} with auto_commit {auto_commit} read the deleted history: {:?}",
            completion.map(|accepted| accepted.fragments.len())
        );
    }
    assert_eq!(partition.group_offset_state(GROUP_ID), (None, None));
    for id in [CONSUMER_ID, DURABLE_CONSUMER_ID] {
        assert_eq!(partition.get_consumer_offset(consumer(id)), None);
    }
    assert_eq!(partition.consensus.sequencer().current_sequence(), fence_op);
    assert_eq!(partition.consensus.pipeline_len(), 0);
    assert_eq!(partition.consensus.request_queue_len(), 0);
}

#[compio::test]
async fn given_stale_poll_when_reservations_need_resync_should_reject_before_reconciliation() {
    let mut partition = test_partition();
    partition.set_consumer_offsets_max(1);
    let discarded_consumer_id = 8;
    let discarded_operation = 1;
    journal_prepare(
        &partition,
        discarded_operation,
        Operation::StoreConsumerOffset,
    )
    .await;
    partition
        .consensus
        .sequencer()
        .set_sequence(discarded_operation);
    partition.stage_consumer_offset_upsert(
        discarded_operation,
        ConsumerKind::Consumer,
        discarded_consumer_id,
        0,
        false,
    );
    let consumer = PollingConsumer::Consumer(7, 0);
    let auto_commit = true;
    let stale_result = poll_read_result(&partition, consumer, auto_commit, Some(0));

    // Truncation removes the pending operation but leaves its capacity
    // reservation for reconciliation. The old read cannot belong to the
    // replacement history or trigger that maintenance.
    partition.invalidate_poll_history();
    partition
        .truncate_uncommitted_from(discarded_operation)
        .await
        .unwrap();
    assert!(partition.pending_consumer_offset_commits.is_empty());
    assert!(partition.offset_reservations_need_resync.get());
    assert_eq!(
        partition.occupied_consumer_offset_count(ConsumerKind::Consumer),
        1
    );

    assert!(matches!(
        partition.complete_poll(stale_result),
        Err(IggyError::TransientNotAccepted)
    ));
    assert_eq!(
        partition.occupied_consumer_offset_count(ConsumerKind::Consumer),
        1,
        "rejecting a stale read must not reconcile the discarded reservation"
    );
    assert!(partition.offset_reservations_need_resync.get());
    assert_eq!(partition.get_consumer_offset(consumer), None);
    assert_eq!(partition.consensus.pipeline_len(), 0);

    // A valid completion still reconciles before admission, freeing the
    // only slot so the owner can admit an automatic commit for this consumer.
    let fresh_result = poll_read_result(&partition, consumer, auto_commit, Some(0));
    let completion = partition
        .complete_poll(fresh_result)
        .expect("accept fresh poll");
    assert!(completion.replication.is_some());
    assert!(!partition.offset_reservations_need_resync.get());
    assert_eq!(partition.get_consumer_offset(consumer), Some(0));
    assert_eq!(
        partition.occupied_consumer_offset_count(ConsumerKind::Consumer),
        1
    );
}

#[compio::test]
async fn given_full_wal_when_poll_completes_should_reject_without_progress() {
    // A primary with persisted offsets needs write-ahead log capacity before
    // accepting an automatic commit. Use real persistence, then exhaust it.
    let directory = tempfile::tempdir().unwrap();
    let primary_replica = 0;
    let replica_count = 3;
    let (mut partition, _) = recording_partition_at(primary_replica, replica_count);
    partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Persisted;
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.open_persistence().await.unwrap();
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    let group_id = 7;
    install_test_owner(&mut partition, group_id as u64, 42).await;
    let member_id = 1;
    let auto_commit = true;
    let consumer = PollingConsumer::ConsumerGroup(group_id, member_id);
    let read_result = poll_read_result(&partition, consumer, auto_commit, Some(0));
    let operation_before_poll = partition.consensus.sequencer().current_sequence();
    persistence.exhaust_capacity_for_test();

    // Refusal must leave progress, queues, operation assignment, and
    // occupied capacity unchanged.
    assert!(matches!(
        partition.complete_poll(read_result),
        Err(IggyError::TransientNotAccepted)
    ));
    let (last_polled, committed) = partition.group_offset_state(group_id as u64);
    assert_eq!(last_polled, None);
    assert_eq!(committed, None);
    assert_eq!(partition.consensus.pipeline_len(), 0);
    assert_eq!(partition.consensus.request_queue_len(), 0);
    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        operation_before_poll
    );
    assert_eq!(
        partition
            .consumer_offset_capacity_for(ConsumerKind::ConsumerGroup)
            .occupied(&partition.durable_consumer_offsets),
        0
    );

    // The same offset becomes acceptable when capacity returns. These are
    // local progress updates; the assigned replication has not run yet.
    persistence.release_capacity_for_test();
    let retry_result = poll_read_result(&partition, consumer, auto_commit, Some(0));
    let retry_completion = partition.complete_poll(retry_result).expect("accept retry");
    assert!(retry_completion.replication.is_some());
    let (last_polled, committed) = partition.group_offset_state(group_id as u64);
    assert_eq!(last_polled, Some(0));
    assert_eq!(committed, Some(0));
    assert_eq!(partition.consensus.pipeline_len(), 1);
    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        operation_before_poll + 1
    );
}

#[test]
fn given_pending_poll_when_materialization_is_missing_should_reject_without_progress() {
    let group_id = 7;
    let member_id = 1;
    let consumer = PollingConsumer::ConsumerGroup(group_id, member_id);
    for auto_commit in [false, true] {
        let (mut partition, _) = recording_partition();
        let read_result = poll_read_result(&partition, consumer, auto_commit, Some(9));

        // The partition can lose its materialized data after planning even
        // when its history identity has not changed.
        partition.materialization_missing = true;
        assert!(matches!(
            partition.complete_poll(read_result),
            Err(IggyError::TransientNotAccepted)
        ));
        let (last_polled, committed) = partition.group_offset_state(group_id as u64);
        assert_eq!(
            last_polled, None,
            "a rejected read must not record group progress"
        );
        assert_eq!(committed, None, "a rejected read must not commit an offset");
        assert_eq!(partition.consensus.pipeline_len(), 0);
        assert_eq!(partition.consensus.request_queue_len(), 0);
    }
}

#[test]
fn given_empty_read_when_partition_is_replaced_should_reject_old_result() {
    let (partition, _) = recording_partition();
    let consumer_id = 7;
    let partition_id = 0;
    let auto_commit = true;
    let consumer = PollingConsumer::Consumer(consumer_id, partition_id);
    let old_empty_result = poll_read_result(&partition, consumer, auto_commit, None);

    // The same namespace does not give a replacement ownership of
    // the old instance's results, even when they contain no messages.
    let (mut replacement, _) = recording_partition();
    assert!(matches!(
        replacement.complete_poll(old_empty_result),
        Err(IggyError::TransientNotAccepted)
    ));
}

/// Disk reads of one group can finish out of order, and each carries its
/// own automatic commit. Both are admitted, so ordering has to be settled
/// where progress is recorded rather than by the order they complete in.
#[compio::test]
async fn given_group_reads_completing_in_reverse_order_should_keep_progress_monotone() {
    let (mut partition, replies) = recording_partition_at(0, 3);
    let group_id = 7;
    install_test_owner(&mut partition, group_id as u64, 42).await;
    replies.borrow_mut().clear();
    let member_id = 1;
    let auto_commit = true;
    let consumer = PollingConsumer::ConsumerGroup(group_id, member_id);
    let earlier_result = poll_read_result(&partition, consumer, auto_commit, Some(4));
    let later_result = poll_read_result(&partition, consumer, auto_commit, Some(9));

    let later = partition
        .complete_poll(later_result)
        .expect("accept later poll")
        .replication
        .expect("sequence first auto-commit");
    assert!(
        partition
            .complete_poll(earlier_result)
            .expect("accept earlier poll")
            .replication
            .is_none()
    );
    let (last_polled, committed) = partition.group_offset_state(group_id as u64);
    assert_eq!(
        last_polled,
        Some(9),
        "the slower read must not rewind group progress"
    );
    assert_eq!(
        committed,
        Some(9),
        "the slower read must not rewind its offset"
    );
    assert_eq!(partition.consensus.pipeline_len(), 1);
    assert_eq!(
        partition
            .consensus
            .with_pipeline(consensus::LocalPipeline::request_queue_len),
        1
    );
    let offset_op = later.prepare.header().op;
    partition.on_replicate(later.prepare).await;
    drop(later.reservation);
    partition.consensus.advance_commit_max(offset_op);
    partition.commit_journal(&repair_config()).await;
    assert_eq!(
        partition.consensus.commit_min(),
        offset_op,
        "the queued offset is already covered"
    );
    assert_eq!(partition.consensus.pipeline_len(), 0);
    assert_eq!(
        partition
            .consensus
            .with_pipeline(consensus::LocalPipeline::request_queue_len),
        0
    );
    assert!(partition.durable_consumer_offsets.covers(
        ConsumerKind::ConsumerGroup,
        u32::try_from(group_id).unwrap(),
        9
    ));
    assert!(
        replies.borrow().is_empty(),
        "automatic no-ops must not emit unsolicited replies"
    );
}

#[test]
fn given_pending_auto_commit_when_history_changes_should_release_only_its_queue_entry() {
    let (mut partition, _) = recording_partition();
    partition.set_consumer_offsets_max(1);
    let automatic_consumer_id = 7;
    let explicit_consumer_id = 8;
    let explicit_client_id = 42;

    // Only the automatic request holds the provisional slot. Queue an
    // explicit store beside it to prove that retiring the history does not
    // discard unrelated client requests.
    let automatic_reservation = partition
        .consumer_offset_capacity_for(ConsumerKind::Consumer)
        .reserve_provisional(automatic_consumer_id, &partition.durable_consumer_offsets)
        .unwrap();
    let automatic_request = partition
        .build_poll_auto_commit_request(ConsumerKind::Consumer, automatic_consumer_id, 0)
        .unwrap();
    partition
        .consensus
        .push_queued_request(consensus::RequestEntry::with_auto_commit(
            automatic_request,
            AutoCommitRequestContext {
                reservation: automatic_reservation,
            },
        ))
        .unwrap();
    let explicit_request = store_offset_request(
        explicit_client_id,
        1,
        ConsumerKind::Consumer,
        explicit_consumer_id,
        0,
        AckLevel::Quorum,
    );
    partition
        .consensus
        .push_queued_request(consensus::RequestEntry::new(explicit_request))
        .unwrap();
    assert_eq!(
        partition.occupied_consumer_offset_count(ConsumerKind::Consumer),
        1
    );

    partition.invalidate_poll_history();
    assert_eq!(
        partition.occupied_consumer_offset_count(ConsumerKind::Consumer),
        0
    );
    let retained_request = partition
        .consensus
        .pop_queued_request()
        .expect("explicit request survives");
    assert_eq!(retained_request.message.header().client, explicit_client_id);
    assert!(partition.consensus.pop_queued_request().is_none());
}

/// Promotion does not check a queued offset mutation against the poll
/// history, so an install must leave none queued to run against the
/// installed offsets.
#[compio::test]
async fn given_queued_offset_mutations_when_install_publishes_should_drop_them_and_keep_installed_offsets()
 {
    const STORED: u32 = 7;
    const DELETED: u32 = 8;
    const INSTALLED_OP: u64 = 1;
    const INSTALLED_OFFSET: u64 = 4;
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) =
        recording_partition_with_pipeline(0, 3, LocalPipeline::with_capacities(1, 2));
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    set_offset_dirs_under(&mut partition, directory.path());
    partition.open_persistence().await.unwrap();
    queue_offset_mutations_then_arm_transfer(&mut partition, STORED, DELETED).await;

    let checkpoint = checksummed_segment_prepare(INSTALLED_OP, 0, INSTALLED_OFFSET, b"installed");
    let installed_state = crate::state_transfer::ConsumerOffsetsWire {
        dedup_capacity: consensus::PARTITION_DEDUP_CLIENTS_MAX,
        prepare_checksum: Some(checkpoint.header().checksum),
        checkpoint_prepare: checkpoint.as_slice().to_vec(),
        next_offset: INSTALLED_OFFSET + 1,
        consumers: vec![(STORED, INSTALLED_OFFSET), (DELETED, INSTALLED_OFFSET)],
        ..Default::default()
    };
    partition
        .install_state_transfer(
            &repair_config(),
            INSTALLED_OP,
            Vec::new(),
            &installed_state.encode(),
        )
        .await
        .unwrap();

    assert_eq!(partition.consensus().request_queue_len(), 0);
    for consumer in [STORED, DELETED] {
        assert_eq!(
            partition.get_consumer_offset(PollingConsumer::Consumer(consumer as usize, 0)),
            Some(INSTALLED_OFFSET),
            "consumer {consumer} must keep its installed offset"
        );
    }
}

/// Converge wipes the journal that held the pipeline's prepares, and a
/// queued mutation must not run against the converged offsets.
#[compio::test]
async fn given_queued_offset_mutations_when_failed_install_converges_should_clear_the_pipeline() {
    const STORED: u32 = 7;
    const DELETED: u32 = 8;
    const OFFERED_OP: u64 = 12;
    const OFFERED_NEXT_OFFSET: u64 = 2;
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) =
        recording_partition_with_pipeline(0, 3, LocalPipeline::with_capacities(1, 2));
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    set_offset_dirs_under(&mut partition, directory.path());
    queue_offset_mutations_then_arm_transfer(&mut partition, STORED, DELETED).await;

    // Staged files that do not exist fail the swap after preflight, and
    // without a WAL the partition converges instead of fencing.
    let missing_staged_segment = crate::state_transfer::StagedSegmentMeta {
        start_offset: 0,
        end_offset: 0,
        index_size: 0,
        size: 8,
        start_timestamp: 0,
        end_timestamp: 0,
        max_timestamp: 0,
        log_staging: directory.path().join("00000000000000000000.log.staging"),
        index_staging: directory.path().join("00000000000000000000.index.staging"),
        checksum: 0,
        linked: false,
    };
    let offered_state = crate::state_transfer::ConsumerOffsetsWire {
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        next_offset: OFFERED_NEXT_OFFSET,
        ..Default::default()
    };
    let install_error = partition
        .install_state_transfer(
            &repair_config(),
            OFFERED_OP,
            vec![missing_staged_segment],
            &offered_state.encode(),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            install_error,
            crate::state_transfer::PartitionInstallError::SwapIo { .. }
        ),
        "unexpected failure: {install_error:?}"
    );

    assert_eq!(
        partition.consensus().request_queue_len(),
        0,
        "request queue"
    );
    assert_eq!(partition.consensus().pipeline_len(), 0, "prepare queue");
}

/// Admits an explicit store and a delete while a filler store holds the
/// only prepare slot, so both wait in the request queue, then arms a
/// state transfer behind them.
async fn queue_offset_mutations_then_arm_transfer(
    partition: &mut IggyPartition<RecordingBus>,
    stored: u32,
    deleted: u32,
) {
    // A key of its own, so the mutations queue for the full pipeline alone.
    let filler = stored.max(deleted) + 1;
    // One message makes offset 0 storable, and a delete on a replicated
    // partition needs a durable entry to remove.
    partition.stats.increment_messages_count(1);
    partition.seed_recovered_consumer_offset(ConsumerKind::Consumer, deleted, 0, 0);
    partition
        .on_request(
            store_offset_request(42, 1, ConsumerKind::Consumer, filler, 0, AckLevel::Quorum),
            None,
        )
        .await;
    partition
        .on_request(
            store_offset_request(43, 1, ConsumerKind::Consumer, stored, 0, AckLevel::Quorum),
            None,
        )
        .await;
    partition
        .on_request(delete_offset_request(44, 1, deleted), None)
        .await;
    assert_eq!(partition.consensus().pipeline_len(), 1);
    assert_eq!(partition.consensus().request_queue_len(), 2);

    for stage in [
        consensus::StateTransferStage::AwaitingTarget,
        consensus::StateTransferStage::Fetching,
        consensus::StateTransferStage::Installing,
    ] {
        partition.consensus().set_state_transfer_stage(stage);
    }
}

//     #[compio::test]
//     async fn given_read_when_state_is_installed_should_reject_the_previous_history() {
//         // State installation writes replacement offset tables, so the fixture
//         // needs filesystem paths even though its message log starts in memory.
//         let directory = tempfile::tempdir().unwrap();
//         let (mut partition, _) = recording_partition();
//         partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
//         partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] = Some(
//             directory
//                 .path()
//                 .join("consumers")
//                 .to_string_lossy()
//                 .into_owned(),
//         );
//         partition.consumer_offset_dirs[ConsumerKind::ConsumerGroup.index()] = Some(
//             directory
//                 .path()
//                 .join("groups")
//                 .to_string_lossy()
//                 .into_owned(),
//         );
//         partition.consumer_offset_dirs[ConsumerKind::ExternalGroup.index()] = Some(
//             directory
//                 .path()
//                 .join("external_groups")
//                 .to_string_lossy()
//                 .into_owned(),
//         );
//         let group_id = 7;
//         let member_id = 1;
//         let auto_commit_enabled = true;
//         let auto_commit_disabled = false;
//         let consumer = PollingConsumer::ConsumerGroup(group_id, member_id);
//         let old_automatic_result =
//             poll_read_result(&partition, consumer, auto_commit_enabled, Some(4));
//         let old_manual_result =
//             poll_read_result(&partition, consumer, auto_commit_disabled, Some(4));
//
//         // Replace the history after both reads have captured its old identity.
//         // Offset 4 is below the new frontier, so its value alone cannot establish
//         // that either result still belongs to the installed history.
//         let installed_state = crate::state_transfer::ConsumerOffsetsWire {
//             owner_retirement: None,
//             owners: Vec::new(),
//             history_transition: None,
//             dedup_capacity: consensus::CLIENTS_TABLE_MAX,
//             required_metadata_frontier: 0,
//             prepare_checksum: None,
//             checkpoint_prepare: Vec::new(),
//             next_offset: 10,
//             emptied_at: 0,
//             consumers: Vec::new(),
//             groups: Vec::new(),
//             external_groups: Vec::new(),
//             dedup: Vec::new(),
//             retirement_paid_through: 0,
//         };
//         let installed_commit_operation = 12;
//         partition
//             .install_state_transfer(
//                 &repair_config(),
//                 installed_commit_operation,
//                 Vec::new(),
//                 &installed_state.encode(),
//             )
//             .await
//             .unwrap();
//
//         for old_result in [old_automatic_result, old_manual_result] {
//             assert!(matches!(
//                 partition.complete_poll(old_result),
//                 Err(IggyError::TransientNotAccepted)
//             ));
//         }
//         let (last_polled, committed) = partition.group_offset_state(group_id as u64);
//         assert_eq!(
//             last_polled, None,
//             "old reads must not restore group progress"
//         );
//         assert_eq!(committed, None, "old reads must not commit an offset");
//         assert_eq!(partition.consensus.pipeline_len(), 0);
//     }

/// An install unlinks the segment files a suspended poll may still hold
/// open, so the poll has to reopen by path instead of reading the old inode.
#[compio::test]
async fn given_in_flight_sealed_poll_when_state_is_installed_should_wipe_its_read_state() {
    let (directory, mut partition) = transfer_receiver();
    let namespace = partition.namespace();
    let partition_dir = directory.path().to_string_lossy().into_owned();
    let log_path = format!("{partition_dir}/{:0>20}.log", 0u64);
    let index_path = format!("{partition_dir}/{:0>20}.index", 0u64);
    partition.log.segments_mut()[0].sealed = true;
    partition.log.storages_mut()[0] = SegmentStorage::new(&log_path, &index_path, 0, 0, false)
        .await
        .unwrap();
    let record = build_segment_record(namespace, 0);
    let record_len = record.len() as u64;
    std::fs::write(&log_path, record).unwrap();
    partition.log.touch_sealed_read_state(0);
    let handle = Rc::clone(&partition.log.sealed_read_state()[0]);
    let poll = || DiskReadPlan {
        partition_dir: PartitionDirResolution::Resolved(partition_dir.clone()),
        bytes_per_message: None,
        widest_batch_bytes: 0,
        validate_checksum: true,
        segments: vec![DiskSegment {
            start_offset: 0,
            persisted: record_len,
            read_state: Rc::clone(&handle),
            sealed: true,
        }],
        start_position: 0,
        start_index_offset: None,
        namespace_raw: namespace.inner(),
    };
    let lookup = MessageLookup::Offset {
        offset: 0,
        count: 1,
        ceiling: u64::MAX,
    };
    assert!(matches!(
        poll().read_disk(lookup).await,
        DiskReadOutcome::Matched { .. }
    ));
    assert!(handle.fd.borrow().is_some());
    assert!(handle.index.borrow().is_some());

    let installed_state = crate::state_transfer::ConsumerOffsetsWire {
        dedup_capacity: consensus::PARTITION_DEDUP_CLIENTS_MAX,
        next_offset: 10,
        ..Default::default()
    };
    partition
        .install_state_transfer(&repair_config(), 12, Vec::new(), &installed_state.encode())
        .await
        .unwrap();

    assert!(
        handle.fd.borrow().is_none(),
        "install must drop the fd the poll cached"
    );
    assert!(
        handle.index.borrow().is_none(),
        "install must drop the index the poll cached"
    );
    assert!(
        !handle.tracked.get(),
        "a resumed walk must not cache into the old handle"
    );
    assert!(
        !matches!(
            poll().read_disk(lookup).await,
            DiskReadOutcome::Matched { .. }
        ),
        "a resumed walk must not serve the unlinked segment"
    );
}

#[compio::test]
async fn given_read_when_failed_install_converges_should_reject_the_previous_history() {
    // Valid offset table paths let installation reach the segment swap.
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition();
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] = Some(
        directory
            .path()
            .join("consumers")
            .to_string_lossy()
            .into_owned(),
    );
    partition.consumer_offset_dirs[ConsumerKind::ConsumerGroup.index()] = Some(
        directory
            .path()
            .join("groups")
            .to_string_lossy()
            .into_owned(),
    );
    partition.consumer_offset_dirs[ConsumerKind::ExternalGroup.index()] = Some(
        directory
            .path()
            .join("external_groups")
            .to_string_lossy()
            .into_owned(),
    );
    partition.log.journal().inner.set_repair_retention(true);
    let first = checksummed_segment_prepare(1, 0, 0, b"old first");
    let second = checksummed_segment_prepare(2, first.header().checksum, 1, b"old second");
    for prepare in [first, second] {
        partition
            .append_repaired_send_messages(prepare)
            .await
            .unwrap();
    }
    partition.evict_committed_prefix(1);
    assert!(partition.log.journal().inner.repair_entry(1).is_some());
    assert!(partition.log.journal().inner.holds_op(2));
    assert_eq!(partition.log.journal().info.messages_count, 1);
    let group_id = 7;
    let member_id = 1;
    let auto_commit = true;
    let consumer = PollingConsumer::ConsumerGroup(group_id, member_id);
    let old_result = poll_read_result(&partition, consumer, auto_commit, Some(4));
    for kind in ConsumerKind::ALL {
        let path = partition.persisted_offset_path(kind, 42).unwrap();
        persist_offset(&path, 0, true).await.unwrap();
        partition
            .consumer_offset_capacity_for(kind)
            .record_stranded(42);
    }

    // Describe staged files without creating them. The swap fails after
    // preflight, forcing recovery to replace the served log with an empty one.
    let missing_staged_segment = crate::state_transfer::StagedSegmentMeta {
        start_offset: 0,
        end_offset: 0,
        index_size: 0,
        size: 8,
        start_timestamp: 0,
        end_timestamp: 0,
        max_timestamp: 0,
        log_staging: directory.path().join("00000000000000000000.log.staging"),
        index_staging: directory.path().join("00000000000000000000.index.staging"),
        checksum: 0,
        linked: false,
    };
    let offered_state = crate::state_transfer::ConsumerOffsetsWire {
        owner_retirement: None,
        owners: Vec::new(),
        history_transition: None,
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        required_metadata_frontier: 0,
        prepare_checksum: None,
        checkpoint_prepare: Vec::new(),
        next_offset: 2,
        emptied_at: 0,
        consumers: Vec::new(),
        groups: Vec::new(),
        external_groups: Vec::new(),
        dedup: Vec::new(),
        retirement_paid_through: 0,
    };
    let offered_commit_operation = 12;
    let install_error = partition
        .install_state_transfer(
            &repair_config(),
            offered_commit_operation,
            vec![missing_staged_segment],
            &offered_state.encode(),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            install_error,
            crate::state_transfer::PartitionInstallError::SwapIo { .. }
        ),
        "unexpected failure: {install_error:?}"
    );

    for kind in ConsumerKind::ALL {
        assert!(
            !std::path::Path::new(&partition.persisted_offset_path(kind, 42).unwrap()).exists()
        );
        assert_eq!(
            partition
                .consumer_offset_capacity_for(kind)
                .stranded_count(),
            0,
            "convergence must release removed offset files for {kind:?}"
        );
    }

    // Failure did not preserve the old history: recovery left an empty log,
    // and its owner must reject the result captured before installation.
    assert!(
        partition
            .log
            .journal()
            .inner
            .repair_retained_from()
            .is_none()
    );
    assert!(partition.log.journal().inner.repair_entry(1).is_none());
    assert!(!partition.log.journal().inner.holds_op(2));
    assert_eq!(partition.log.journal().info.messages_count, 0);
    assert_eq!(partition.log.journal().info.size.as_bytes_u64(), 0);
    assert_eq!(partition.log.segments().len(), 1);
    assert_eq!(partition.log.active_segment().size.as_bytes_u64(), 0);
    assert!(matches!(
        partition.complete_poll(old_result),
        Err(IggyError::TransientNotAccepted)
    ));
    let (last_polled, committed) = partition.group_offset_state(group_id as u64);
    assert_eq!(last_polled, None);
    assert_eq!(committed, None);
}

#[compio::test]
async fn given_read_when_install_preflight_fails_should_keep_the_existing_history() {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition();
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    let group_id = 7;
    install_test_owner(&mut partition, group_id as u64, 42).await;
    let member_id = 1;
    let auto_commit = false;
    let consumer = PollingConsumer::ConsumerGroup(group_id, member_id);
    let pending_result = poll_read_result(&partition, consumer, auto_commit, Some(4));

    // Empty offset bytes fail decoding before installation mutates served
    // state. The result's original history identity must remain acceptable.
    let offered_commit_operation = 12;
    assert!(
        partition
            .install_state_transfer(&repair_config(), offered_commit_operation, Vec::new(), &[],)
            .await
            .is_err()
    );
    partition
        .complete_poll(pending_result)
        .expect("unmodified history remains valid");
    let (last_polled, committed) = partition.group_offset_state(group_id as u64);
    assert_eq!(
        last_polled,
        Some(4),
        "the read still belongs to the served history"
    );
    assert_eq!(committed, None, "automatic commits are disabled");
}

#[compio::test]
async fn given_full_prepare_queue_when_poll_completes_should_queue_its_context_and_advance() {
    let primary_replica = 0;
    let replica_count = 3;
    let prepare_capacity = 1;
    let request_capacity = 1;
    let pipeline = LocalPipeline::with_capacities(prepare_capacity, request_capacity);
    let (mut partition, _) =
        recording_partition_with_pipeline(primary_replica, replica_count, pipeline);
    let client_id = 42;
    let explicit_consumer_id = 7;
    let group_id = 8;
    let installed_op = install_test_owner(&mut partition, group_id as u64, client_id).await;

    // One visible message makes offset 0 valid. Its explicit store occupies
    // the only prepare slot while the request queue remains available.
    partition.stats.increment_messages_count(1);
    let mut explicit = store_offset_request(
        client_id,
        1,
        ConsumerKind::Consumer,
        explicit_consumer_id,
        0,
        AckLevel::Quorum,
    );
    explicit = explicit.transmute_header(|mut header, next: &mut RoutedRequestHeader| {
        header.metadata_watermark = partition.required_metadata_frontier;
        *next = header;
    });
    partition.on_request(explicit, None).await;
    let member_id = 1;
    let auto_commit = true;
    let consumer = PollingConsumer::ConsumerGroup(group_id, member_id);
    let read_result = poll_read_result(&partition, consumer, auto_commit, Some(0));

    // Acceptance can advance local progress after queueing the automatic
    // commit, even though no operation or replication work is assigned yet.
    let completion = partition
        .complete_poll(read_result)
        .expect("request queue has room");
    assert!(completion.replication.is_none());
    let (last_polled, committed) = partition.group_offset_state(group_id as u64);
    assert_eq!(last_polled, Some(0));
    assert_eq!(committed, Some(0));
    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        installed_op + 1
    );

    // The queued request owns the capacity reservation. Taking it out of
    // the queue must keep that slot occupied.
    let queued_request = partition
        .consensus
        .pop_queued_request()
        .expect("automatic commit queued");
    let queued_context = queued_request
        .auto_commit()
        .expect("request carries its context");
    assert_eq!(
        queued_context.reservation.kind(),
        ConsumerKind::ConsumerGroup
    );
    assert_eq!(queued_context.reservation.consumer_id() as usize, group_id);
    assert_eq!(
        partition.occupied_consumer_offset_count(ConsumerKind::ConsumerGroup),
        1
    );
    drop(queued_request);
    assert_eq!(
        partition.occupied_consumer_offset_count(ConsumerKind::ConsumerGroup),
        0
    );
}

#[compio::test]
async fn given_full_prepare_and_request_queues_when_poll_completes_should_preserve_progress() {
    let primary_replica = 0;
    let replica_count = 3;
    let prepare_capacity = 1;
    let request_capacity = 1;
    let pipeline = LocalPipeline::with_capacities(prepare_capacity, request_capacity);
    let (mut partition, _) =
        recording_partition_with_pipeline(primary_replica, replica_count, pipeline);
    let client_id = 42;
    let prepared_consumer_id = 7;
    let queued_consumer_id = 9;

    // Fill both admission paths: the first explicit store owns the prepare
    // slot, and the second waits in the only request slot.
    partition.stats.increment_messages_count(1);
    partition
        .on_request(
            store_offset_request(
                client_id,
                1,
                ConsumerKind::Consumer,
                prepared_consumer_id,
                0,
                AckLevel::Quorum,
            ),
            None,
        )
        .await;
    partition
        .consensus
        .push_queued_request(consensus::RequestEntry::new(store_offset_request(
            client_id,
            2,
            ConsumerKind::Consumer,
            queued_consumer_id,
            0,
            AckLevel::Quorum,
        )))
        .unwrap();
    let group_id = 8;
    let member_id = 1;
    let auto_commit = true;
    let consumer = PollingConsumer::ConsumerGroup(group_id, member_id);
    let read_result = poll_read_result(&partition, consumer, auto_commit, Some(0));

    // The group cannot queue its automatic commit. Rejection must leave its
    // progress empty, release its reservation, and retain the existing work.
    assert!(matches!(
        partition.complete_poll(read_result),
        Err(IggyError::TransientNotAccepted)
    ));
    let (last_polled, committed) = partition.group_offset_state(group_id as u64);
    assert_eq!(last_polled, None);
    assert_eq!(committed, None);
    assert_eq!(
        partition.occupied_consumer_offset_count(ConsumerKind::ConsumerGroup),
        0
    );
    assert_eq!(partition.consensus.request_queue_len(), 1);
    assert_eq!(partition.consensus.sequencer().current_sequence(), 1);
}

#[test]
fn given_full_live_map_when_polling_existing_key_should_reclaim_only_for_new_keys() {
    let (mut partition, _) = recording_partition();
    partition.set_consumer_offsets_max(2);
    partition.offset_space.committed_seeded = true;
    partition.seed_recovered_consumer_offset(ConsumerKind::Consumer, 7, 0, 0);
    for id in 7..=8 {
        partition.consumer_offsets.pin().insert(
            id as usize,
            ConsumerOffset::new(ConsumerKind::Consumer, id, 0, String::new()),
        );
    }
    let args = PollingArgs::new(iggy_common::PollingStrategy::first(), 1, true);
    let _ = partition.build_poll_plan(PollingConsumer::Consumer(7, 0), &args, false);
    assert_eq!(partition.consumer_offsets.len(), 2);
    let _ = partition.build_poll_plan(PollingConsumer::Consumer(9, 0), &args, false);
    assert_eq!(partition.consumer_offsets.len(), 1);
    assert!(partition.consumer_offsets.pin().contains_key(&7));
}

#[compio::test]
async fn given_missing_retained_header_when_journal_progresses_should_retry_without_view_change() {
    let mut partition = test_partition();
    journal_prepare(&partition, 2, Operation::SendMessages).await;
    partition.consensus.sequencer().set_sequence(2);
    partition.consensus.set_view(1);
    partition.resynchronize_consumer_offset_reservations();
    assert!(
        partition
            .consumer_offset_capacity_for(ConsumerKind::Consumer)
            .is_uncertain()
    );
    assert!(!partition.offset_reservations_need_resync.get());
    let failed_scan = partition.offset_reservations_scan_state;
    for op in 3..=20 {
        journal_prepare(&partition, op, Operation::SendMessages).await;
        partition.consensus.sequencer().set_sequence(op);
        partition.offset_reservations_need_resync.set(true);
        partition.resynchronize_consumer_offset_reservations();
        assert_eq!(partition.offset_reservations_scan_state, failed_scan);
    }
    partition.retry_consumer_offset_reservations();
    assert_ne!(partition.offset_reservations_scan_state, failed_scan);
    assert!(
        partition
            .consumer_offset_capacity_for(ConsumerKind::Consumer)
            .is_uncertain()
    );
    partition.seed_recovered_consumer_offset(ConsumerKind::Consumer, 7, 0, 0);
    assert!(partition.auto_commit_admission_ready(ConsumerKind::Consumer, 7));
    assert!(!partition.auto_commit_admission_ready(ConsumerKind::Consumer, 8));
    partition.log.journal().inner.clear_all();
    for op in 1..=3 {
        journal_prepare(&partition, op, Operation::SendMessages).await;
    }
    partition.consensus.sequencer().set_sequence(3);
    partition.resynchronize_consumer_offset_reservations();
    assert!(
        partition
            .consumer_offset_capacity_for(ConsumerKind::Consumer)
            .is_uncertain()
    );
    partition.retry_consumer_offset_reservations();
    assert!(
        !partition
            .consumer_offset_capacity_for(ConsumerKind::Consumer)
            .is_uncertain()
    );
}

#[compio::test]
async fn given_evicted_committed_prefix_when_promoted_should_preserve_staging_without_latching() {
    let mut partition = test_partition();
    partition.consensus.restore_commit_state(0, 5000);
    partition.consensus.sequencer().set_sequence(5001);
    partition.stage_consumer_offset_upsert(4999, ConsumerKind::Consumer, 7, 0, false);
    journal_prepare(&partition, 5001, Operation::SendMessages).await;
    partition.consensus.set_view(1);
    partition.resynchronize_consumer_offset_reservations();
    assert!(
        !partition
            .consumer_offset_capacity_for(ConsumerKind::Consumer)
            .is_uncertain()
    );
    assert!(
        partition
            .pending_consumer_offset_commits
            .contains_key(&4999)
    );
    assert_eq!(
        partition.occupied_consumer_offset_count(ConsumerKind::Consumer),
        1
    );
}

#[test]
fn given_truncated_journal_when_sequencer_is_ahead_should_not_latch_capacity() {
    let (mut partition, _) = recording_partition();
    partition.consensus.sequencer().set_sequence(100);
    partition.offset_reservations_need_resync.set(true);
    partition.resynchronize_consumer_offset_reservations();
    assert!(
        !partition
            .consumer_offset_capacity_for(ConsumerKind::Consumer)
            .is_uncertain()
    );
    assert!(
        partition
            .reserve_consumer_offset(ConsumerKind::Consumer, 7)
            .is_ok()
    );
}

#[test]
fn given_full_backup_phantom_map_when_new_consumer_polls_should_reclaim_without_promotion() {
    let (mut partition, _) = recording_partition_at(1, 3);
    partition.set_consumer_offsets_max(2);
    partition.offset_space.committed_seeded = true;
    for id in 1..=2 {
        partition.apply_local_poll_offset(ConsumerKind::Consumer, id, 0);
    }
    let args = PollingArgs::new(iggy_common::PollingStrategy::first(), 1, true);
    let _ = partition.build_poll_plan(PollingConsumer::Consumer(3, 0), &args, false);
    assert_eq!(partition.consumer_offsets.len(), 1);
    let retained_id = *partition.consumer_offsets.pin().keys().next().unwrap();
    assert!(retained_id == 1 || retained_id == 2);
    assert!(
        partition
            .check_local_poll_key(ConsumerKind::Consumer, 3)
            .is_ok()
    );
    partition.apply_local_poll_offset(ConsumerKind::Consumer, 3, 0);
    assert!(partition.consumer_offsets.pin().contains_key(&retained_id));
    assert_eq!(partition.consumer_offsets.len(), 2);
    assert!(!partition.consensus.is_primary());
}

#[compio::test]
async fn given_full_offset_table_when_storing_new_id_should_reject_before_replication() {
    let (mut partition, sent_to_clients) = recording_partition();
    let directory = tempfile::tempdir().unwrap();
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
        Some(directory.path().to_string_lossy().into_owned());
    partition.set_consumer_offsets_max(1);
    partition.stats.increment_messages_count(1);

    partition
        .on_request(
            store_offset_request(42, 1, ConsumerKind::Consumer, 7, 0, AckLevel::Quorum),
            None,
        )
        .await;
    commit_recorded_loopback(&mut partition).await;
    assert_eq!(
        partition.durable_consumer_offset_count(ConsumerKind::Consumer),
        1
    );

    partition
        .on_request(
            store_offset_request(42, 2, ConsumerKind::Consumer, 8, 0, AckLevel::Quorum),
            None,
        )
        .await;
    let sent = sent_to_clients.borrow();
    let denied = sent
        .iter()
        .find_map(|(_, frame)| {
            let header = bytemuck::checked::try_from_bytes::<ReplyHeader>(
                &frame.as_slice()[..std::mem::size_of::<ReplyHeader>()],
            )
            .ok()?;
            (header.request == 2).then_some(*header)
        })
        .expect("capacity denial reply");
    assert_eq!(denied.status, IggyError::TooManyConsumerOffsets.as_code());
    assert_eq!(denied.op, 0);
    assert!(partition.consumer_offsets.pin().get(&8).is_none());
    assert_eq!(partition.consensus().pipeline_len(), 0);
}

#[compio::test]
async fn given_full_offset_table_when_updating_existing_id_should_succeed() {
    let (mut partition, _) = recording_partition();
    let directory = tempfile::tempdir().unwrap();
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
        Some(directory.path().to_string_lossy().into_owned());
    partition.set_consumer_offsets_max(1);
    partition.stats.increment_messages_count(1);
    for request_id in 1..=2 {
        partition
            .on_request(
                store_offset_request(
                    42,
                    request_id,
                    ConsumerKind::Consumer,
                    7,
                    0,
                    AckLevel::Quorum,
                ),
                None,
            )
            .await;
        commit_recorded_loopback(&mut partition).await;
    }
    assert_eq!(
        partition.durable_consumer_offset_count(ConsumerKind::Consumer),
        1
    );
    assert_eq!(partition.consumer_offsets.pin().len(), 1);
}

#[compio::test]
async fn given_solo_persisted_offset_when_wal_is_pending_should_withhold_reply() {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, replies) = recording_partition();
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] = Some(
        directory
            .path()
            .join("consumers")
            .to_string_lossy()
            .into_owned(),
    );
    partition.stats.increment_messages_count(1);
    partition.open_persistence().await.unwrap();
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    persistence.set_group_commit_delay(std::time::Duration::from_millis(1));
    // Hold the worker until the self-ack path has had a chance to commit.
    persistence.sync();
    assert!(persistence.start());

    partition
        .on_request(
            store_offset_request(42, 1, ConsumerKind::Consumer, 7, 0, AckLevel::Quorum),
            None,
        )
        .await;
    commit_recorded_loopback(&mut partition).await;

    assert_eq!(persistence.head(), 1);
    assert_eq!(persistence.durable_op(), 0);
    assert!(
        replies.borrow().is_empty(),
        "reply preceded the WAL barrier"
    );
    assert_eq!(partition.consensus().commit_min(), 0);

    Rc::clone(&persistence).run().await;
    commit_recorded_loopback(&mut partition).await;

    assert_eq!(persistence.durable_op(), 1);
    assert_eq!(partition.consensus().commit_min(), 1);
    assert_eq!(replies.borrow().len(), 1);
    assert_eq!(
        partition.get_consumer_offset(PollingConsumer::Consumer(7, 0)),
        Some(0)
    );
}

// A two-replica primary whose WAL refused its own prepare for op 1, which
// is journaled and pipelined with no WAL entry and no armed self-ack.

/// A backup's `PrepareOk` for `header`, in the primary's current view.
fn backup_prepare_ok(
    partition: &IggyPartition<RecordingBus>,
    replica: u8,
    header: &PrepareHeader,
) -> Message<PrepareOkHeader> {
    let consensus = partition.consensus();
    Message::<PrepareOkHeader>::new(size_of::<PrepareOkHeader>()).transmute_header(
        |_, ack: &mut PrepareOkHeader| {
            ack.command = Command::PrepareOk;
            ack.cluster = consensus.cluster();
            ack.replica = replica;
            ack.view = consensus.view();
            ack.op = header.op;
            ack.prepare_checksum = header.checksum;
            ack.operation = header.operation;
            ack.group = header.group;
            ack.size = u32::try_from(size_of::<PrepareOkHeader>()).unwrap();
            ack.seal();
        },
    )
}

/// A two-replica primary whose WAL refused its own prepare for op 1, which
/// is journaled and pipelined with no WAL entry and no armed self-ack.
#[allow(clippy::future_not_send)]
async fn primary_with_refused_offset_prepare(
    directory: &Path,
) -> (IggyPartition<RecordingBus>, SentFrames, PrepareHeader) {
    let (mut partition, replies) = recording_partition_at(0, 2);
    partition.set_partition_dir(directory.to_string_lossy().into_owned());
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
        Some(directory.join("consumers").to_string_lossy().into_owned());
    partition.stats.increment_messages_count(1);
    partition.open_persistence().await.unwrap();
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());

    // Exhaust WAL capacity between admission and the submit under
    // `write_lock`, so the WAL refuses the prepare.
    let write_lock = Arc::clone(&partition.write_lock);
    let parked = write_lock.lock().await;
    let request = store_offset_request(42, 1, ConsumerKind::Consumer, 7, 0, AckLevel::Quorum);
    let mut admission = Box::pin(partition.on_request(request, None));
    assert!(futures::poll!(&mut admission).is_pending());
    persistence.exhaust_capacity_for_test();
    drop(parked);
    admission.await;
    persistence.release_capacity_for_test();
    let header = partition
        .consensus()
        .pipeline_head_header()
        .expect("op 1 was admitted");
    assert_eq!(
        persistence.head(),
        0,
        "the WAL refused the primary's own prepare"
    );
    assert!(partition.pending_persisted_acks.borrow().is_empty());
    (partition, replies, header)
}

fn answer_partition_heartbeat(
    partition: &IggyPartition<RecordingBus>,
    action: &consensus::VsrAction,
) {
    if let consensus::VsrAction::SendCommit {
        view,
        group,
        timestamp_monotonic,
        ..
    } = action
    {
        let reply = Message::<CommitReplyHeader>::new(HEADER_SIZE).transmute_header(
            |_, header: &mut CommitReplyHeader| {
                header.command = Command::CommitReply;
                header.size = u32::try_from(HEADER_SIZE).unwrap();
                header.cluster = partition.consensus().cluster();
                header.view = *view;
                header.replica = 1;
                header.group = *group;
                header.partition_incarnation = partition.consensus().partition_incarnation();
                header.timestamp_monotonic = *timestamp_monotonic;
                header.seal();
            },
        );
        partition.consensus().handle_commit_reply(reply.header());
    }
}

#[compio::test]
async fn given_an_armed_self_ack_when_the_local_write_is_retried_should_not_send_a_second_prepare_ok()
 {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _replies, header) =
        Box::pin(primary_with_refused_offset_prepare(directory.path())).await;
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    partition
        .pending_persisted_acks
        .borrow_mut()
        .insert(header.op, header);
    partition.persist_repaired_prefix();
    persistence
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();

    partition.retry_local_write(header.op).await;
    partition.drive_persistence().await;
    let mut acknowledgments = Vec::new();
    partition
        .consensus()
        .drain_loopback_into(&mut acknowledgments);
    assert_eq!(
        acknowledgments.len(),
        1,
        "the armed self-ack fires once, so the retry must not ack on its own"
    );
}

#[compio::test]
async fn given_a_wal_still_full_at_the_first_retry_when_capacity_frees_should_resubmit_and_commit()
{
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _replies, header) =
        Box::pin(primary_with_refused_offset_prepare(directory.path())).await;
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    partition
        .on_ack(backup_prepare_ok(&partition, 1, &header), &repair_config())
        .await;
    commit_recorded_loopback(&mut partition).await;
    persistence.exhaust_capacity_for_test();

    let mut retries = 0;
    for _ in 0..consensus::TimeoutManager::PREPARE_TICKS * 200 {
        if retries == 1 {
            persistence.release_capacity_for_test();
        }
        for action in partition.consensus().tick(PlaneKind::Partitions) {
            answer_partition_heartbeat(&partition, &action);
            if let consensus::VsrAction::RetryLocalWrite { op } = action {
                partition.retry_local_write(op).await;
                retries += 1;
            }
        }
        commit_recorded_loopback(&mut partition).await;
    }
    persistence
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();
    commit_recorded_loopback(&mut partition).await;

    assert!(
        retries > 1,
        "the timer must keep retrying, retries={retries}"
    );
    assert_eq!(
        persistence.head(),
        header.op,
        "a retry after capacity frees must re-submit the op the full WAL refused"
    );
    assert_eq!(partition.consensus().commit_min(), header.op);
}

#[compio::test]
async fn given_a_self_ack_queued_in_loopback_when_the_prepare_timeout_keeps_firing_should_not_queue_another()
 {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _replies, header) =
        Box::pin(primary_with_refused_offset_prepare(directory.path())).await;
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    partition
        .on_ack(backup_prepare_ok(&partition, 1, &header), &repair_config())
        .await;
    commit_recorded_loopback(&mut partition).await;
    partition.retry_local_write(header.op).await;
    persistence
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();
    partition.drive_persistence().await;

    for _ in 0..consensus::TimeoutManager::PREPARE_TICKS * 40 {
        for action in partition.consensus().tick(PlaneKind::Partitions) {
            answer_partition_heartbeat(&partition, &action);
            if let consensus::VsrAction::RetryLocalWrite { op } = action {
                partition.retry_local_write(op).await;
            }
        }
    }
    let mut acknowledgments = Vec::new();
    partition
        .consensus()
        .drain_loopback_into(&mut acknowledgments);
    assert_eq!(
        acknowledgments.len(),
        1,
        "an undrained self-ack already carries the primary's vote"
    );
}

#[compio::test]
async fn given_a_replica_that_lost_primacy_when_the_local_write_is_retried_should_neither_submit_nor_ack()
 {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _replies, header) =
        Box::pin(primary_with_refused_offset_prepare(directory.path())).await;
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    partition.consensus.set_view(1);
    assert!(!partition.consensus().is_primary());

    partition.retry_local_write(header.op).await;
    assert_eq!(persistence.head(), 0, "a non-primary must not re-submit");
    assert!(partition.pending_persisted_acks.borrow().is_empty());
    let mut acknowledgments = Vec::new();
    partition
        .consensus()
        .drain_loopback_into(&mut acknowledgments);
    assert!(acknowledgments.is_empty());
}

#[compio::test]
async fn given_a_wal_that_refused_the_primary_prepare_when_the_prepare_timeout_fires_should_retry_locally_and_commit()
 {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, replies, header) =
        Box::pin(primary_with_refused_offset_prepare(directory.path())).await;
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());

    // Two replicas need both votes, so the backup's ack alone commits nothing.
    partition
        .on_ack(backup_prepare_ok(&partition, 1, &header), &repair_config())
        .await;
    commit_recorded_loopback(&mut partition).await;
    assert_eq!(partition.consensus().commit_min(), 0);

    let mut retried = false;
    for _ in 0..=consensus::TimeoutManager::PREPARE_TICKS {
        for action in partition.consensus().tick(PlaneKind::Partitions) {
            match action {
                consensus::VsrAction::RetryLocalWrite { op } => {
                    partition.retry_local_write(op).await;
                    retried = true;
                }
                consensus::VsrAction::RetransmitPrepares { targets } => {
                    assert!(
                        targets.iter().all(|(_, replicas)| !replicas.contains(&0)),
                        "a send to self is a no-op, so it cannot be the remedy: {targets:?}"
                    );
                }
                _ => {}
            }
        }
    }
    assert!(
        retried,
        "the prepare timeout must retry the primary's own write"
    );
    assert_eq!(
        persistence.head(),
        header.op,
        "the retry re-submitted op 1 to the WAL"
    );

    persistence
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();
    commit_recorded_loopback(&mut partition).await;
    assert_eq!(partition.consensus().commit_min(), header.op);
    assert_eq!(
        replies.borrow().len(),
        1,
        "the client learns the outcome from a reply"
    );
    assert_eq!(
        partition.get_consumer_offset(PollingConsumer::Consumer(7, 0)),
        Some(0)
    );
}

#[compio::test]
async fn given_replicated_partition_when_quorum_offset_is_stored_should_enter_vsr() {
    let (mut partition, sent_to_clients) = recording_partition_at(0, 3);
    partition.stats.increment_messages_count(1);
    partition
        .on_request(
            store_offset_request(42, 1, ConsumerKind::Consumer, 7, 0, AckLevel::Quorum),
            None,
        )
        .await;

    assert_eq!(partition.consensus().pipeline_len(), 1);
    assert_eq!(partition.pending_consumer_offset_commits.len(), 1);
    assert_eq!(
        partition.durable_consumer_offset_count(ConsumerKind::Consumer),
        0,
        "durable membership begins at commit"
    );
    assert!(sent_to_clients.borrow().is_empty());
}

#[compio::test]
async fn given_pending_offset_prepare_when_view_changes_should_rebuild_capacity_from_journal() {
    let (mut partition, _) = recording_partition_at(0, 3);
    partition.set_consumer_offsets_max(1);
    partition.stats.increment_messages_count(1);
    partition
        .on_request(
            store_offset_request(42, 1, ConsumerKind::Consumer, 7, 0, AckLevel::Quorum),
            None,
        )
        .await;
    assert_eq!(partition.pending_consumer_offset_commits.len(), 1);

    partition.pending_consumer_offset_commits.clear();
    partition
        .consumer_offset_capacity_for(ConsumerKind::Consumer)
        .set_pending_count(7, 0);
    partition.consensus.set_view(1);
    partition.resynchronize_consumer_offset_reservations();

    assert_eq!(partition.pending_consumer_offset_commits.len(), 1);
    assert!(
        partition
            .reserve_consumer_offset(ConsumerKind::Consumer, 8)
            .is_err(),
        "the retained prepare must keep the only slot reserved"
    );
}

#[test]
fn given_phantom_and_eager_offsets_when_snapshotting_should_export_committed_durable_value_only() {
    let (partition, _) = recording_partition();
    partition.consumer_offsets.pin().insert(
        7,
        ConsumerOffset::new(ConsumerKind::Consumer, 7, 9, String::new()),
    );
    partition.consumer_offsets.pin().insert(
        8,
        ConsumerOffset::new(ConsumerKind::Consumer, 8, 11, String::new()),
    );
    partition.seed_recovered_consumer_offset(ConsumerKind::Consumer, 7, 5, 5);

    assert_eq!(
        partition
            .offsets_wire_snapshot_for_test()
            .expect("durable key exists in live map"),
        vec![(7, 5)],
        "the eager value and the follower-local phantom must not enter the artifact"
    );
}

#[test]
fn given_durable_offset_missing_from_live_map_when_snapshotting_should_refuse() {
    let (partition, _) = recording_partition();
    partition.seed_recovered_consumer_offset(ConsumerKind::Consumer, 7, 5, 5);
    assert!(matches!(
        partition.offsets_wire_snapshot_for_test(),
        Err(
            crate::state_transfer::PartitionTransferUnavailable::ConsumerOffsetStateInconsistent {
                kind: ConsumerKind::Consumer,
                consumer_id: 7,
            }
        )
    ));
}

#[compio::test]
async fn given_no_reply_owner_when_partition_commits_should_not_fire_http_slot() {
    let bus = Rc::new(IggyMessageBus::new(0));
    let namespace = IggyNamespace::new(1, 1, 0);
    let consensus = VsrConsensus::new(
        TEST_CLUSTER,
        0,
        1,
        namespace.inner(),
        Rc::clone(&bus),
        LocalPipeline::new(),
    );
    consensus.init();
    let mut partition: IggyPartition<Rc<IggyMessageBus>> = IggyPartition::with_in_memory_storage(
        Arc::new(PartitionStats::default()),
        consensus,
        IggyByteSize::from(1024 * 1024),
    );
    let mut request = checksumless_send_request(namespace, 1);
    request.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
        .copy_from_slice(&build_segment_record(namespace, 0));
    let header = *request.header();
    bus.clients().insert_in_process(header.client).unwrap();
    let (_slot, mut receiver) = bus
        .clients()
        .install_reply_slot(header.client, header.request)
        .unwrap();
    partition.on_request(request.clone(), None).await;
    assert_eq!(partition.consensus.pipeline_len(), 1);
    partition.consensus.advance_commit_max(1);
    partition.commit_journal(&repair_config()).await;
    assert!(partition.fatal.is_none(), "{:?}", partition.fatal);
    assert_eq!(partition.consensus.commit_min(), 1);
    assert!(
        matches!(receiver.try_recv(), Ok(None)),
        "a committed reply without its owner must not fill an HTTP slot"
    );
    let (reply, answered) = consensus::oneshot_channel();
    partition.on_request(request, Some(reply)).await;
    let replay = answered.await.unwrap();
    assert_eq!(replay.header().status, 0);
    assert_eq!(replay.header().op, 1);
    assert!(
        matches!(receiver.try_recv(), Ok(None)),
        "the legitimate replay answers its carried receiver"
    );
}

#[compio::test]
async fn given_no_reply_owner_when_partition_denies_should_not_fire_http_slot() {
    const CLIENT: u128 = 42;
    const REQUEST: u64 = 7;
    let bus = Rc::new(IggyMessageBus::new(0));
    let namespace = IggyNamespace::new(1, 1, 0);
    let consensus = VsrConsensus::new(
        TEST_CLUSTER,
        0,
        1,
        namespace.inner(),
        Rc::clone(&bus),
        LocalPipeline::new(),
    );
    consensus.init();
    let mut partition: IggyPartition<Rc<IggyMessageBus>> = IggyPartition::with_in_memory_storage(
        Arc::new(PartitionStats::default()),
        consensus,
        IggyByteSize::from(1024 * 1024),
    );
    bus.clients().insert_in_process(CLIENT).unwrap();
    let (_slot, mut receiver) = bus.clients().install_reply_slot(CLIENT, REQUEST).unwrap();
    partition
        .on_request(delete_offset_request(CLIENT, REQUEST, 5), None)
        .await;
    assert_eq!(partition.consensus().pipeline_len(), 0);
    assert!(
        matches!(receiver.try_recv(), Ok(None)),
        "an originless denial must not take another request's slot"
    );
    let (reply, answered) = consensus::oneshot_channel();
    partition
        .on_request(delete_offset_request(CLIENT, REQUEST, 5), Some(reply))
        .await;
    assert_eq!(
        answered.await.unwrap().header().status,
        IggyError::ConsumerOffsetNotFound(0).as_code()
    );
    assert!(matches!(receiver.try_recv(), Ok(None)));
}

/// Deleting a consumer offset that was never stored must answer with a
/// typed deny reply (empty body, `status` = `ConsumerOffsetNotFound`,
/// `op` 0) before consensus: nothing may enter the pipeline, and an
/// awaited client write must fail fast instead of waiting out its reply
/// timeout. Once the offset exists, the same request must pass the gate
/// into the pipeline without a deny.
#[compio::test]
async fn on_request_delete_of_missing_offset_replies_typed_deny() {
    let (mut partition, sent_to_clients) = recording_partition();
    let client_id: u128 = 42;
    let consumer_id: u32 = 5;

    partition
        .on_request(delete_offset_request(client_id, 7, consumer_id), None)
        .await;

    {
        let sent = sent_to_clients.borrow();
        assert_eq!(sent.len(), 1, "exactly one deny reply");
        let (reply_client, frame) = &sent[0];
        assert_eq!(*reply_client, client_id);
        let header = bytemuck::checked::try_from_bytes::<ReplyHeader>(
            &frame.as_slice()[..std::mem::size_of::<ReplyHeader>()],
        )
        .expect("deny frame starts with a valid reply header");
        assert_eq!(header.command, Command::Reply);
        assert_eq!(
            header.status,
            IggyError::ConsumerOffsetNotFound(0).as_code()
        );
        assert_eq!(header.op, 0, "a deny commits nothing");
        assert_eq!(header.request, 7);
        assert_eq!(
            header.size as usize,
            std::mem::size_of::<ReplyHeader>(),
            "deny reply body must be empty"
        );
    }
    assert_eq!(
        partition.consensus().pipeline_len(),
        0,
        "denied delete must not replicate"
    );
    assert!(partition.pending_consumer_offset_commits.is_empty());

    // Existing offset: the gate passes and the delete enters the pipeline.
    partition.consumer_offsets.pin().insert(
        consumer_id as usize,
        ConsumerOffset::new(ConsumerKind::Consumer, consumer_id, 3, String::new()),
    );
    partition
        .on_request(delete_offset_request(client_id, 8, consumer_id), None)
        .await;
    assert_eq!(
        partition.consensus().pipeline_len(),
        1,
        "existing offset delete must replicate"
    );
}

#[compio::test]
async fn on_request_delete_on_stale_backup_replies_routing_denial_before_not_found() {
    for (transferring, expected) in [
        (false, IggyError::NotPrimary),
        (true, IggyError::TransientNotAccepted),
    ] {
        let (mut partition, sent_to_clients) = recording_partition_at(1, 3);
        if transferring {
            partition.consensus.begin_state_transfer_await();
        }
        let client_id = 42;

        partition
            .on_request(delete_offset_request(client_id, 7, 5), None)
            .await;

        let sent = sent_to_clients.borrow();
        assert_eq!(sent.len(), 1, "exactly one routing denial");
        let (reply_client, frame) = &sent[0];
        assert_eq!(*reply_client, client_id);
        let header = bytemuck::checked::try_from_bytes::<ReplyHeader>(
            &frame.as_slice()[..std::mem::size_of::<ReplyHeader>()],
        )
        .expect("deny frame starts with a valid reply header");
        assert_eq!(header.status, expected.as_code());
        assert_eq!(header.op, 0, "the backup admitted nothing");
        assert_eq!(
            partition.consensus().pipeline_len(),
            0,
            "a backup must not replicate the delete"
        );
    }
}

// A server auto-commit persists monotonically. Disk-tier polls replicate
// their offsets in IO-completion order, so the last committed op can carry
// a lower offset than an earlier one; the file must keep the max or a
// restart reloads the rewound value and re-delivers. An explicit client
// store still overwrites, so a deliberate offset reset holds.

fn unique_temp_offset_dir() -> String {
    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "iggy-offset-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos(),
    ));
    dir.to_string_lossy().into_owned()
}

/// A server auto-commit persists monotonically. Disk-tier polls replicate
/// their offsets in IO-completion order, so the last committed op can carry
/// a lower offset than an earlier one; the file must keep the max or a
/// restart reloads the rewound value and re-delivers. An explicit client
/// store still overwrites, so a deliberate offset reset holds.
#[compio::test]
async fn auto_commit_offset_persists_monotonically_explicit_store_rewinds() {
    let mut partition = test_partition();
    let dir = unique_temp_offset_dir();
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] = Some(dir.clone());
    let consumer_id: u32 = 5;
    let path = format!("{dir}/{consumer_id}");
    let read_disk = |p: &str| -> u64 {
        let bytes = std::fs::read(p).expect("offset file exists");
        match crate::offset_storage::decode_offset_record(&bytes) {
            crate::offset_storage::OffsetRecord::Value { offset, .. } => offset,
            other => panic!("offset file must hold a readable value, got {other:?}"),
        }
    };

    // Reordered auto-commits: the later op (109) trails the earlier (114).
    partition
        .persist_consumer_offset_commit(PendingConsumerOffsetCommit::upsert_auto_commit(
            ConsumerKind::Consumer,
            consumer_id,
            114,
        ))
        .await
        .expect("auto-commit persist 114");
    partition
        .persist_consumer_offset_commit(PendingConsumerOffsetCommit::upsert_auto_commit(
            ConsumerKind::Consumer,
            consumer_id,
            109,
        ))
        .await
        .expect("auto-commit persist 109");
    assert_eq!(
        read_disk(&path),
        114,
        "auto-commit must not rewind the file on IO-completion reorder"
    );

    assert!(
        partition
            .durable_consumer_offsets
            .covers(ConsumerKind::Consumer, consumer_id, 114),
        "committed high-water covers the persisted offset"
    );
    assert!(
        !partition
            .durable_consumer_offsets
            .covers(ConsumerKind::Consumer, consumer_id, 115),
        "an advancing offset is not covered and must submit"
    );

    // An explicit client store may deliberately rewind.
    partition
        .persist_consumer_offset_commit(PendingConsumerOffsetCommit::upsert(
            ConsumerKind::Consumer,
            consumer_id,
            109,
        ))
        .await
        .expect("explicit store persist 109");
    assert_eq!(read_disk(&path), 109, "explicit store may rewind the file");
    assert!(
        !partition
            .durable_consumer_offsets
            .covers(ConsumerKind::Consumer, consumer_id, 114),
        "explicit rewind lowers the high-water so a later auto-commit may re-advance"
    );

    // The accepted edge: an auto-commit racing the explicit rewind
    // re-advances the file past it.
    partition
        .persist_consumer_offset_commit(PendingConsumerOffsetCommit::upsert_auto_commit(
            ConsumerKind::Consumer,
            consumer_id,
            114,
        ))
        .await
        .expect("auto-commit persist 114 after rewind");
    assert_eq!(
        read_disk(&path),
        114,
        "auto-commit re-advances past a rewind"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Promotion from the request queue must keep an explicit store's
/// overwrite, or a client could never move its cursor back.
#[compio::test]
async fn given_queued_explicit_store_below_committed_offset_when_promoted_should_rewind_it() {
    const CLIENT: u128 = 42;
    const FILLER_CLIENT: u128 = 43;
    const CONSUMER: u32 = 7;
    const FILLER_CONSUMER: u32 = 8;
    for (kind, consumer) in [
        (
            ConsumerKind::Consumer,
            PollingConsumer::Consumer(CONSUMER as usize, 0),
        ),
        (
            ConsumerKind::ConsumerGroup,
            PollingConsumer::ConsumerGroup(CONSUMER as usize, 0),
        ),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let (mut partition, _) =
            recording_partition_with_pipeline(0, 1, LocalPipeline::with_capacities(1, 1));
        set_offset_dirs_under(&mut partition, directory.path());
        partition.stats.increment_messages_count(2);
        partition.stats.set_current_offset(1);
        if kind == ConsumerKind::ConsumerGroup {
            for (group_id, client_id) in [(CONSUMER, CLIENT), (FILLER_CONSUMER, FILLER_CLIENT)] {
                let installation =
                    next_test_installation(&partition, u64::from(group_id), client_id);
                partition
                    .on_request(owner_install_request(&partition, &installation), None)
                    .await;
                commit_recorded_loopback(&mut partition).await;
                assert!(
                    partition
                        .installed_consumer_group_owner(&installation)
                        .is_some()
                );
            }
        }
        let owned = |partition: &IggyPartition<_>, request: Message<RoutedRequestHeader>| {
            request.transmute_header(|mut header, next: &mut RoutedRequestHeader| {
                let consumer_id = if header.client == CLIENT {
                    CONSUMER
                } else {
                    FILLER_CONSUMER
                };
                header.metadata_watermark = partition.required_metadata_frontier;
                header.owner_generation = partition
                    .consumer_group_owner(u64::from(consumer_id))
                    .map_or(0, |owner| owner.generation);
                *next = header;
            })
        };
        let applied = partition.consensus().commit_min();
        let request = owned(
            &partition,
            store_offset_request(CLIENT, 1, kind, CONSUMER, 1, AckLevel::Quorum),
        );
        partition.on_request(request, None).await;
        commit_recorded_loopback(&mut partition).await;
        assert_eq!(partition.get_consumer_offset(consumer), Some(1));

        // The filler holds the only prepare slot, so the rewind has to queue.
        let filler = owned(
            &partition,
            store_offset_request(FILLER_CLIENT, 1, kind, FILLER_CONSUMER, 0, AckLevel::Quorum),
        );
        partition.on_request(filler, None).await;
        let rewind = owned(
            &partition,
            store_offset_request(CLIENT, 2, kind, CONSUMER, 0, AckLevel::Quorum),
        );
        partition.on_request(rewind, None).await;
        assert_eq!(partition.consensus().request_queue_len(), 1);
        commit_recorded_loopback(&mut partition).await;
        commit_recorded_loopback(&mut partition).await;

        assert_eq!(partition.consensus().commit_min(), applied + 3, "{kind:?}");
        assert_eq!(
            partition.get_consumer_offset(consumer),
            Some(0),
            "a promoted {kind:?} store must overwrite, not fold to the max"
        );
    }
}
#[compio::test]
async fn wal_backed_cold_offsets_skip_covered_values_and_write_advances() {
    let (directory, partition, _) = partition_with_pending_durable_ack().await;
    let path = directory.path().join("offset");
    let path = path.to_str().unwrap();
    persist_offset(path, 114, false).await.unwrap();
    assert_eq!(
        partition
            .write_cold_consumer_offset(path, 109, true)
            .await
            .unwrap(),
        (114, false)
    );
    assert_eq!(
        partition
            .write_cold_consumer_offset(path, 114, true)
            .await
            .unwrap(),
        (114, false)
    );
    assert!(
        partition
            .persistence
            .as_ref()
            .unwrap()
            .take_offset_file(path)
            .is_none()
    );
    assert_eq!(
        partition
            .write_cold_consumer_offset(path, 115, true)
            .await
            .unwrap(),
        (115, true)
    );
    assert_eq!(
        crate::offset_storage::read_offset_max(path, 0)
            .await
            .unwrap()
            .offset,
        115
    );
}

/// The persisted-offset tracker is cold after a restart; the first
/// auto-commit folds against the file once (so a pre-existing higher value
/// wins, exactly like the old per-commit read-modify-write) and warms the
/// tracker with the on-disk value, not the op's. A delete drops both the
/// file and the tracker entry so a later auto-commit starts a fresh fold.
#[compio::test]
async fn auto_commit_cold_key_folds_against_file_once() {
    let mut partition = test_partition();
    let dir = unique_temp_offset_dir();
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] = Some(dir.clone());
    let consumer_id: u32 = 5;
    let path = format!("{dir}/{consumer_id}");
    let read_disk = |p: &str| -> u64 {
        let bytes = std::fs::read(p).expect("offset file exists");
        match crate::offset_storage::decode_offset_record(&bytes) {
            crate::offset_storage::OffsetRecord::Value { offset, .. } => offset,
            other => panic!("offset file must hold a readable value, got {other:?}"),
        }
    };

    // Simulate the previous process run: the file already holds 114.
    persist_offset(&path, 114, false)
        .await
        .expect("seed offset file");
    assert!(
        !partition
            .durable_consumer_offsets
            .covers(ConsumerKind::Consumer, consumer_id, 1),
        "a cold key is never covered; the first submit must go through"
    );

    partition
        .persist_consumer_offset_commit(PendingConsumerOffsetCommit::upsert_auto_commit(
            ConsumerKind::Consumer,
            consumer_id,
            109,
        ))
        .await
        .expect("auto-commit persist 109 on cold key");
    assert_eq!(
        read_disk(&path),
        114,
        "cold-key fold must not rewind the pre-existing on-disk value"
    );
    assert!(
        partition
            .durable_consumer_offsets
            .covers(ConsumerKind::Consumer, consumer_id, 114),
        "tracker warms with the on-disk value, not the trailing op offset"
    );

    partition
        .persist_consumer_offset_commit(PendingConsumerOffsetCommit::delete(
            ConsumerKind::Consumer,
            consumer_id,
        ))
        .await
        .expect("delete persisted offset");
    assert!(!std::path::Path::new(&path).exists(), "file unlinked");
    assert!(
        !partition
            .durable_consumer_offsets
            .covers(ConsumerKind::Consumer, consumer_id, 1),
        "delete drops the tracker entry with the file"
    );

    partition
        .persist_consumer_offset_commit(PendingConsumerOffsetCommit::upsert_auto_commit(
            ConsumerKind::Consumer,
            consumer_id,
            7,
        ))
        .await
        .expect("auto-commit persist 7 after delete");
    assert_eq!(read_disk(&path), 7, "post-delete auto-commit starts fresh");

    let _ = std::fs::remove_dir_all(&dir);
}

#[compio::test]
async fn given_dead_group_when_reclaimed_should_keep_capacity_until_replicated_delete() {
    let (mut partition, _) = recording_partition();
    partition.consumer_group_offsets.pin().insert(
        ConsumerGroupId(7),
        ConsumerOffset::new(ConsumerKind::ConsumerGroup, 7, 11, String::new()),
    );
    partition.seed_recovered_consumer_offset(ConsumerKind::ConsumerGroup, 7, 11, 11);
    assert_eq!(
        partition.dead_group_offset_keys(|_| false),
        vec![(ConsumerKind::ConsumerGroup, 7)]
    );
    assert_eq!(partition.consumer_group_offset_ids(), vec![7]);
    assert_eq!(
        partition.occupied_consumer_offset_count(ConsumerKind::ConsumerGroup),
        1
    );
    partition.stage_consumer_offset_delete(1, ConsumerKind::ConsumerGroup, 7);
    partition
        .apply_staged_consumer_offset_commit(1)
        .await
        .expect("delete commits");
    assert_eq!(
        partition.occupied_consumer_offset_count(ConsumerKind::ConsumerGroup),
        0
    );
    assert_eq!(partition.consumer_group_offset_ids(), [] as [u64; 0]);
}

#[compio::test]
async fn given_known_stranded_group_file_when_reconciling_should_not_submit_delete_loop() {
    let dir = tempfile::tempdir().unwrap();
    let group_dir = dir.path().join("groups");
    std::fs::create_dir_all(group_dir.join("7")).unwrap();
    let (mut partition, _) = recording_partition();
    partition.consumer_offset_dirs[ConsumerKind::ConsumerGroup.index()] =
        Some(group_dir.to_string_lossy().into_owned());
    // In the live map, so the reclaim walk actually meets the key and the
    // stranded filter is what keeps it out of the delete log.
    partition.seed_recovered_consumer_offset(ConsumerKind::ConsumerGroup, 7, 11, 11);
    partition.consumer_group_offsets.pin().insert(
        ConsumerGroupId(7),
        ConsumerOffset::new(ConsumerKind::ConsumerGroup, 7, 11, String::new()),
    );
    partition
        .consumer_offset_capacity_for(ConsumerKind::ConsumerGroup)
        .record_stranded(7);
    assert!(
        partition
            .consumer_offset_capacity_for(ConsumerKind::ConsumerGroup)
            .is_stranded(7)
    );
    assert_eq!(
        partition.dead_group_offset_keys(|_| true),
        Vec::<(ConsumerKind, u32)>::new()
    );
    assert!(
        partition.dead_group_offset_keys(|_| false).is_empty(),
        "automatic cleanup must not resubmit a known failed unlink"
    );
    partition
        .consumer_offset_capacity_for(ConsumerKind::ConsumerGroup)
        .clear_stranded(7);
    assert_eq!(
        partition.dead_group_offset_keys(|_| false),
        vec![(ConsumerKind::ConsumerGroup, 7)],
        "the same dead key is reclaimed once it is no longer stranded"
    );
    assert_eq!(partition.offsets_wire_snapshot_for_test().unwrap(), []);
}

#[test]
fn given_local_only_key_when_replicated_should_require_durable_membership_to_delete() {
    let (partition, _) = recording_partition_at(0, 3);
    partition.seed_stranded_consumer_offset(ConsumerKind::ConsumerGroup, 7);
    assert!(
        partition
            .ensure_consumer_offset_exists(ConsumerKind::ConsumerGroup, 7)
            .is_err(),
        "a stranded file is not a replicated key"
    );
    partition.consumer_group_offsets.pin().insert(
        ConsumerGroupId(8),
        ConsumerOffset::new(ConsumerKind::ConsumerGroup, 8, 0, String::new()),
    );
    assert!(
        partition
            .ensure_consumer_offset_exists(ConsumerKind::ConsumerGroup, 8)
            .is_err(),
        "a local poll cursor must not produce a missing-key delete on an older peer"
    );
    partition
        .durable_consumer_offsets
        .record_explicit(ConsumerKind::ConsumerGroup, 9, 0, 0);
    assert!(
        partition
            .ensure_consumer_offset_exists(ConsumerKind::ConsumerGroup, 9)
            .is_ok()
    );
    assert!(
        partition
            .ensure_consumer_offset_exists(ConsumerKind::ConsumerGroup, 10)
            .is_err(),
        "an unknown key still answers not found"
    );

    let (single, _) = recording_partition();
    single.seed_stranded_consumer_offset(ConsumerKind::ConsumerGroup, 7);
    assert!(
        single
            .ensure_consumer_offset_exists(ConsumerKind::ConsumerGroup, 7)
            .is_ok()
    );
}

#[compio::test]
async fn given_committed_delete_when_unlink_fails_should_preserve_state_and_report_failure() {
    let dir = tempfile::tempdir().unwrap();
    let group_dir = dir.path().join("groups");
    std::fs::create_dir_all(group_dir.join("7")).unwrap();
    let (mut partition, _) = recording_partition();
    partition.consumer_offset_dirs[ConsumerKind::ConsumerGroup.index()] =
        Some(group_dir.to_string_lossy().into_owned());
    partition.consumer_group_offsets.pin().insert(
        ConsumerGroupId(7),
        ConsumerOffset::new(ConsumerKind::ConsumerGroup, 7, 11, String::new()),
    );
    partition.seed_recovered_consumer_offset(ConsumerKind::ConsumerGroup, 7, 11, 11);
    partition.stage_consumer_offset_delete(1, ConsumerKind::ConsumerGroup, 7);

    // A failed file mutation cannot become a successful logical delete.
    assert!(
        partition
            .apply_staged_consumer_offset_commit(1)
            .await
            .is_err()
    );
    assert_eq!(partition.consumer_group_offset_ids(), vec![7]);
    assert_eq!(
        partition.durable_consumer_offset_count(ConsumerKind::ConsumerGroup),
        1
    );
    assert!(partition.pending_consumer_offset_commits.contains_key(&1));
    assert!(
        partition
            .consumer_offset_capacity_for(ConsumerKind::ConsumerGroup)
            .is_stranded(7)
    );
    assert!(partition.fatal.is_none());
}

#[compio::test]
async fn given_quorum_store_when_unrelated_dirty_directory_is_gone_should_succeed() {
    let dir = tempfile::tempdir().unwrap();
    let (mut partition, sent) = recording_partition();
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
        Some(dir.path().join("consumers").to_string_lossy().into_owned());
    partition.consumer_offset_dirs[ConsumerKind::ConsumerGroup.index()] = Some(
        dir.path()
            .join("missing-groups")
            .to_string_lossy()
            .into_owned(),
    );
    partition.consumer_offset_dirs_dirty[1].set(true);
    partition.stats.increment_messages_count(1);

    partition
        .on_request(
            store_offset_request(42, 1, ConsumerKind::Consumer, 7, 0, AckLevel::Quorum),
            None,
        )
        .await;
    commit_recorded_loopback(&mut partition).await;

    assert!(partition.consumer_offsets.pin().contains_key(&7));
    assert!(
        partition
            .durable_consumer_offsets
            .covers(ConsumerKind::Consumer, 7, 0)
    );
    let reply = sent.borrow();
    let header = reply
        .first()
        .and_then(|(_, message)| {
            bytemuck::checked::try_from_bytes::<ReplyHeader>(
                &message.as_slice()[..size_of::<ReplyHeader>()],
            )
            .ok()
        })
        .expect("success reply");
    assert_eq!(header.status, 0);
}

#[compio::test]
async fn given_no_ack_explicit_store_when_submitted_should_apply_without_replication() {
    let dir = tempfile::tempdir().unwrap();
    let (mut partition, sent) = recording_partition();
    partition.set_partition_dir(dir.path().to_string_lossy().into_owned());
    partition.open_persistence().await.unwrap();
    partition.stats.increment_messages_count(1);
    partition
        .on_request(
            store_offset_request(42, 1, ConsumerKind::Consumer, 7, 0, AckLevel::NoAck),
            None,
        )
        .await;
    assert_eq!(
        partition.get_consumer_offset(PollingConsumer::Consumer(7, 0)),
        Some(0)
    );
    assert_eq!(
        partition.durable_consumer_offset_count(ConsumerKind::Consumer),
        1
    );
    assert_eq!(partition.consensus().pipeline_len(), 0);
    assert_eq!(partition.persistence.as_ref().unwrap().head(), 0);
    let sent = sent.borrow();
    let header = bytemuck::checked::from_bytes::<ReplyHeader>(
        &sent[0].1.as_slice()[..size_of::<ReplyHeader>()],
    );
    assert_eq!(header.status, 0);
    assert_eq!(header.op, 0);
}

#[compio::test]
async fn given_stale_dirt_on_the_other_kind_when_its_sync_fails_should_not_fence_the_walk() {
    let dir = tempfile::tempdir().unwrap();
    let (mut partition, sent) = recording_partition_at(0, 3);
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
        Some(dir.path().join("consumers").to_string_lossy().into_owned());
    partition.consumer_offset_dirs[ConsumerKind::ConsumerGroup.index()] =
        Some(dir.path().join("groups").to_string_lossy().into_owned());
    partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Persisted;
    // Dirt a NoAck request left on the groups directory, whose sync now
    // fails. This walk writes consumer offsets only.
    partition.consumer_offset_dirs_dirty[1].set(true);
    partition.consumer_offset_dir_sync_fault.set(Some(1));
    partition
        .persist_consumer_offset_commit(PendingConsumerOffsetCommit::upsert(
            ConsumerKind::Consumer,
            1,
            0,
        ))
        .await
        .unwrap();
    partition.stage_consumer_offset_delete(1, ConsumerKind::Consumer, 1);
    let header = PrepareHeader {
        retry_capacity: u32::try_from(consensus::PARTITION_DEDUP_CLIENTS_MAX).unwrap(),
        session: 1,
        op: 1,
        operation: Operation::DeleteConsumerOffset,
        client: 42,
        request: 1,
        ..Default::default()
    };
    partition.consensus.restore_commit_state(0, 1);
    partition
        .handle_committed_entries(vec![header], &repair_config(), true)
        .await;
    assert!(
        partition.fatal.is_none(),
        "a failure on a kind this walk did not write must not fence it"
    );
    assert_eq!(partition.consensus.commit_min(), 1);
    assert_eq!(sent.borrow().len(), 1);
    assert!(!partition.consumer_offset_dirs_dirty[0].get());
    assert!(partition.consumer_offset_dirs_dirty[1].get());

    // The same failure on the kind the walk wrote fences it.
    partition.consumer_offset_dir_sync_fault.set(Some(0));
    partition
        .persist_consumer_offset_commit(PendingConsumerOffsetCommit::upsert(
            ConsumerKind::Consumer,
            2,
            0,
        ))
        .await
        .unwrap();
    partition.stage_consumer_offset_delete(2, ConsumerKind::Consumer, 2);
    let header = PrepareHeader {
        retry_capacity: u32::try_from(consensus::PARTITION_DEDUP_CLIENTS_MAX).unwrap(),
        session: 1,
        op: 2,
        operation: Operation::DeleteConsumerOffset,
        client: 42,
        request: 2,
        ..Default::default()
    };
    partition.consensus.advance_commit_max(2);
    partition
        .handle_committed_entries(vec![header], &repair_config(), true)
        .await;
    assert!(
        partition.fatal.is_some(),
        "a sync failure on a written kind fences the walk"
    );
    assert_eq!(partition.consensus.commit_min(), 1);
}

#[compio::test]
async fn given_no_ack_delete_sync_failure_when_retried_should_retry_barrier_and_succeed() {
    let dir = tempfile::tempdir().unwrap();
    let (mut partition, sent) = recording_partition();
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
        Some(dir.path().to_string_lossy().into_owned());
    partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Persisted;
    let stored = PendingConsumerOffsetCommit::upsert(ConsumerKind::Consumer, 7, 0);
    partition
        .persist_consumer_offset_commit(stored)
        .await
        .unwrap();
    partition.apply_consumer_offset_commit(stored);
    partition.consumer_offset_dir_sync_fault.set(Some(0));
    partition
        .apply_consumer_offset_no_ack(
            Box::new(*delete_offset_request(42, 1, 7).header()),
            ConsumerKind::Consumer,
            7,
            None,
            None,
        )
        .await;
    let status = |index: usize| {
        let frames = sent.borrow();
        let bytes = frames[index].1.as_slice();
        let start = std::mem::offset_of!(ReplyHeader, status);
        u32::from_le_bytes(bytes[start..start + 4].try_into().unwrap())
    };
    assert_eq!(status(0), IggyError::CannotSyncFile.as_code());
    assert!(!dir.path().join("7").exists());
    assert!(
        partition
            .ensure_consumer_offset_exists(ConsumerKind::Consumer, 7)
            .is_ok()
    );
    partition.consumer_offset_dir_sync_fault.set(None);
    partition
        .apply_consumer_offset_no_ack(
            Box::new(*delete_offset_request(42, 2, 7).header()),
            ConsumerKind::Consumer,
            7,
            None,
            None,
        )
        .await;
    assert_eq!(status(1), 0);
    assert!(!partition.consumer_offset_dirs_dirty[0].get());
    assert!(
        !partition
            .consumer_offset_capacity_for(ConsumerKind::Consumer)
            .is_stranded(7)
    );
}

/// Both the commit walk and a no-ack store apply through
/// `apply_consumer_offset_commit`, so its stamps hold for both.
#[test]
fn given_client_stores_and_auto_commits_when_applied_should_stamp_only_client_stores() {
    let (partition, _) = recording_partition();
    let consumer = PollingConsumer::Consumer(7, 0);
    let before = partition.consensus.message_bus().monotonic_micros();
    partition.apply_consumer_offset_commit(PendingConsumerOffsetCommit::upsert_auto_commit(
        ConsumerKind::Consumer,
        7,
        3,
    ));
    assert!(
        !partition.consumer_offset_stored_since(consumer, before),
        "the commit of a read is no client store"
    );
    partition.apply_consumer_offset_commit(PendingConsumerOffsetCommit::explicit(
        ConsumerKind::Consumer,
        7,
        Some(3),
    ));
    assert_eq!(partition.get_consumer_offset(consumer), Some(3));
    assert!(
        partition.consumer_offset_stored_since(consumer, before),
        "a client store of the value it held stamps the offset"
    );
    assert!(
        !partition.consumer_offset_stored_since(PollingConsumer::ConsumerGroup(7, 0), before),
        "the stamp names its kind"
    );
    let other = PollingConsumer::Consumer(8, 0);
    assert!(!partition.consumer_offset_stored_since(other, before));
    partition.apply_consumer_offset_commit(PendingConsumerOffsetCommit::explicit(
        ConsumerKind::Consumer,
        7,
        None,
    ));
    assert_eq!(partition.get_consumer_offset(consumer), None);
    assert!(
        partition.consumer_offset_stored_since(other, before),
        "an offset without a stamp of its own counts the last delete"
    );
}

/// The primary stages a client store when it admits it and stamps it when
/// it commits. A parked read that finishes in between would commit at a
/// higher op than the store, so its monotone apply would pass over the
/// rewind.
#[test]
fn given_a_staged_client_store_when_checking_since_admission_should_report_it_before_it_applies() {
    let (mut partition, _) = recording_partition();
    let consumer = PollingConsumer::Consumer(7, 0);
    let other = PollingConsumer::Consumer(8, 0);
    let before = partition.consensus.message_bus().monotonic_micros();
    partition.stage_consumer_offset_upsert(1, ConsumerKind::Consumer, 7, 3, true);
    assert!(
        !partition.consumer_offset_stored_since(consumer, before),
        "a staged commit of a read is no client store"
    );
    partition.stage_consumer_offset_upsert(2, ConsumerKind::Consumer, 7, 1, false);
    assert_eq!(
        partition.get_consumer_offset(consumer),
        None,
        "the staged store is not applied yet"
    );
    assert!(
        partition.consumer_offset_stored_since(consumer, before),
        "a staged client store fences a read that finishes before it applies"
    );
    assert!(
        !partition.consumer_offset_stored_since(other, before),
        "the staged store names its consumer"
    );
    partition.stage_consumer_offset_delete(3, ConsumerKind::Consumer, 8);
    assert!(
        partition.consumer_offset_stored_since(other, before),
        "a staged delete fences as a staged store does"
    );
}

#[test]
fn given_follower_when_group_is_deleted_should_wait_for_ordered_reclamation() {
    let (partition, _) = recording_partition_at(1, 3);
    partition.consumer_group_offsets.pin().insert(
        ConsumerGroupId(7),
        ConsumerOffset::new(ConsumerKind::ConsumerGroup, 7, 11, String::new()),
    );
    partition.seed_recovered_consumer_offset(ConsumerKind::ConsumerGroup, 7, 11, 11);
    assert_eq!(partition.dead_group_offset_keys(|_| false), []);
    assert_eq!(
        partition.durable_consumer_offset_count(ConsumerKind::ConsumerGroup),
        1
    );
}

/// One-message segment record in on-disk layout `[256B command header][blob]`
/// stamped at `base_offset`, with a valid batch checksum so it decodes
/// through `decode_batch_slice` and matches an `Offset` poll.
fn build_segment_record(namespace: IggyNamespace, base_offset: u64) -> Vec<u8> {
    build_segment_record_with_payload(namespace, base_offset, Bytes::from_static(b"abcdefgh"))
}

/// Fail-closed disk read: an unreadable EARLIER segment must stop the walk
/// (return `Faulted`) rather than skip forward and serve a LATER segment's
/// messages, which would punch a silent gap into the poll. The second
/// segment holds a real, matchable batch at a higher offset; before the
/// fix, a missing first segment did `continue` and the walk served that
/// batch (offset 5 in response to an offset-0 poll) - the exact skip.
#[compio::test]
async fn read_disk_faults_closed_when_earlier_segment_unreadable() {
    let namespace = IggyNamespace::new(1, 1, 0);

    // Unique temp dir; the first segment file is deliberately never created.
    let dir = std::env::temp_dir().join(format!(
        "iggy-read-disk-faulted-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos(),
    ));
    compio::fs::create_dir_all(&dir)
        .await
        .expect("create temp partition dir");
    let partition_dir = dir.to_string_lossy().into_owned();

    // Second segment starts at offset 5 and holds a valid batch there.
    let later_record = build_segment_record(namespace, 5);
    let later_path = format!("{partition_dir}/{:0>20}.log", 5u64);
    let later_len = later_record.len() as u64;
    {
        let mut file = compio::fs::File::create(&later_path)
            .await
            .expect("create later segment file");
        let (written, _) = file.write_all_at(later_record, 0).await.into();
        written.expect("write later segment record");
        file.sync_all().await.expect("flush later segment file");
    }

    // First segment claims persisted bytes but its file is absent, so the
    // open exhausts retries -> the walk must fault-close before segment two.
    let plan = DiskReadPlan {
        partition_dir: PartitionDirResolution::Resolved(partition_dir),
        bytes_per_message: None,
        widest_batch_bytes: 0,
        validate_checksum: true,
        segments: vec![
            DiskSegment {
                start_offset: 0,
                persisted: 512,
                read_state: SealedSegmentHandle::default(),
                sealed: false,
            },
            DiskSegment {
                start_offset: 5,
                persisted: later_len,
                read_state: SealedSegmentHandle::default(),
                sealed: false,
            },
        ],
        start_position: 0,
        start_index_offset: None,
        namespace_raw: namespace.inner(),
    };

    let outcome = plan
        .read_disk(MessageLookup::Offset {
            offset: 0,
            count: 10,
            ceiling: u64::MAX,
        })
        .await;

    assert!(
        matches!(outcome, DiskReadOutcome::Faulted(DiskFault::Retryable)),
        "unreadable first segment must fault-close, not skip forward to the later segment",
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Sizing the first read from the requested count makes it routinely
/// narrower than one batch, which the walk answers by re-reading the same
/// position four times as wide. A partition whose mean message is small
/// and whose next batch is not must still serve that batch.
#[compio::test]
async fn read_disk_serves_a_batch_wider_than_the_sized_chunk() {
    let namespace = IggyNamespace::new(1, 1, 0);

    let dir = std::env::temp_dir().join(format!(
        "iggy-read-disk-wide-batch-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos(),
    ));
    compio::fs::create_dir_all(&dir)
        .await
        .expect("create temp partition dir");
    let partition_dir = dir.to_string_lossy().into_owned();

    let payload = Bytes::from(vec![0x5Au8; 256 << 10]);
    let record = build_segment_record_with_payload(namespace, 0, payload.clone());
    let record_len = record.len() as u64;
    let path = format!("{partition_dir}/{:0>20}.log", 0u64);
    {
        let mut file = compio::fs::File::create(&path)
            .await
            .expect("create segment file");
        let (written, _) = file.write_all_at(record, 0).await.into();
        written.expect("write segment record");
        file.sync_all().await.expect("flush segment file");
    }

    // One byte per message floors the first read at 64 KiB, a quarter of
    // the batch waiting at offset 0.
    let plan = DiskReadPlan {
        partition_dir: PartitionDirResolution::Resolved(partition_dir),
        bytes_per_message: Some(1),
        widest_batch_bytes: 0,
        validate_checksum: true,
        segments: vec![DiskSegment {
            start_offset: 0,
            persisted: record_len,
            read_state: SealedSegmentHandle::default(),
            sealed: false,
        }],
        start_position: 0,
        start_index_offset: None,
        namespace_raw: namespace.inner(),
    };

    let outcome = plan
        .read_disk(MessageLookup::Offset {
            offset: 0,
            count: 1,
            ceiling: u64::MAX,
        })
        .await;

    let DiskReadOutcome::Matched {
        fragments, matched, ..
    } = outcome
    else {
        panic!("a batch wider than the first read must still be served");
    };
    assert_eq!(matched, 1);
    let served: u64 = fragments.iter().map(|fragment| fragment.len() as u64).sum();
    assert_eq!(served, record_len);

    let _ = std::fs::remove_dir_all(&dir);
}

/// Fail-closed disk read on a CORRUPT (present-but-undecodable) batch in an
/// EARLIER segment: like a missing/unreadable segment, the walk must stop
/// (`Faulted`) rather than skip past the garbage and serve a LATER
/// segment's valid batch at a higher offset, which would punch a silent gap
/// into the poll. The first segment's file exists and claims persisted bytes
/// but holds non-decodable data; the second segment holds a real batch at
/// offset 5.
#[compio::test]
async fn read_disk_faults_closed_when_earlier_segment_corrupt() {
    let namespace = IggyNamespace::new(1, 1, 0);

    let dir = std::env::temp_dir().join(format!(
        "iggy-read-disk-corrupt-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos(),
    ));
    compio::fs::create_dir_all(&dir)
        .await
        .expect("create temp partition dir");
    let partition_dir = dir.to_string_lossy().into_owned();

    // First segment (start_offset 0): garbage bytes that never decode into a
    // complete batch.
    let corrupt_record = vec![0xABu8; 512];
    let corrupt_len = corrupt_record.len() as u64;
    let corrupt_path = format!("{partition_dir}/{:0>20}.log", 0u64);
    {
        let mut file = compio::fs::File::create(&corrupt_path)
            .await
            .expect("create corrupt segment file");
        let (written, _) = file.write_all_at(corrupt_record, 0).await.into();
        written.expect("write corrupt segment record");
        file.sync_all().await.expect("flush corrupt segment file");
    }

    // Second segment (start_offset 5): a valid, matchable batch.
    let later_record = build_segment_record(namespace, 5);
    let later_path = format!("{partition_dir}/{:0>20}.log", 5u64);
    let later_len = later_record.len() as u64;
    {
        let mut file = compio::fs::File::create(&later_path)
            .await
            .expect("create later segment file");
        let (written, _) = file.write_all_at(later_record, 0).await.into();
        written.expect("write later segment record");
        file.sync_all().await.expect("flush later segment file");
    }

    let plan = DiskReadPlan {
        partition_dir: PartitionDirResolution::Resolved(partition_dir),
        bytes_per_message: None,
        widest_batch_bytes: 0,
        validate_checksum: true,
        segments: vec![
            DiskSegment {
                start_offset: 0,
                persisted: corrupt_len,
                read_state: SealedSegmentHandle::default(),
                sealed: false,
            },
            DiskSegment {
                start_offset: 5,
                persisted: later_len,
                read_state: SealedSegmentHandle::default(),
                sealed: false,
            },
        ],
        start_position: 0,
        start_index_offset: None,
        namespace_raw: namespace.inner(),
    };

    let outcome = plan
        .read_disk(MessageLookup::Offset {
            offset: 0,
            count: 10,
            ceiling: u64::MAX,
        })
        .await;

    assert!(
        matches!(outcome, DiskReadOutcome::Faulted(DiskFault::Unreadable)),
        "corrupt earlier segment must fault-close, not skip forward to the later segment",
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// A segment whose bytes decode cleanly but do not match their own
/// `batch_checksum`: bit rot at rest, not a torn write. Unverified, the batch is
/// served and a consumer reads data provably not what was written.
///
/// Detection only, per the operator knob: the poll fails closed and reports, with
/// no attempt to repair.
#[compio::test]
async fn read_disk_faults_closed_on_batch_checksum_mismatch() {
    let namespace = IggyNamespace::new(1, 1, 0);
    let dir = std::env::temp_dir().join(format!(
        "iggy-read-disk-bitrot-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos(),
    ));
    compio::fs::create_dir_all(&dir)
        .await
        .expect("create temp partition dir");
    let partition_dir = dir.to_string_lossy().into_owned();

    // Structurally valid with one payload byte flipped, so every length and
    // offset still decodes and only the checksum disagrees.
    let mut record = build_segment_record(namespace, 0);
    let last = record.len() - 1;
    record[last] ^= 0x01;
    let record_len = record.len() as u64;
    let path = format!("{partition_dir}/{:0>20}.log", 0u64);
    {
        let mut file = compio::fs::File::create(&path)
            .await
            .expect("create segment file");
        let (written, _) = file.write_all_at(record, 0).await.into();
        written.expect("write segment record");
        file.sync_all().await.expect("flush segment file");
    }

    let plan = |validate_checksum| DiskReadPlan {
        partition_dir: PartitionDirResolution::Resolved(partition_dir.clone()),
        bytes_per_message: None,
        widest_batch_bytes: 0,
        validate_checksum,
        segments: vec![DiskSegment {
            start_offset: 0,
            persisted: record_len,
            read_state: SealedSegmentHandle::default(),
            sealed: false,
        }],
        start_position: 0,
        start_index_offset: None,
        namespace_raw: namespace.inner(),
    };
    let query = MessageLookup::Offset {
        offset: 0,
        count: 10,
        ceiling: u64::MAX,
    };

    let outcome = plan(true).read_disk(query).await;
    assert!(
        matches!(outcome, DiskReadOutcome::Faulted(DiskFault::Unreadable)),
        "a batch that fails its own checksum must fault-close"
    );

    // What the opt-out costs. The shipped default is `true` because of it.
    let outcome = plan(false).read_disk(query).await;
    assert!(
        matches!(outcome, DiskReadOutcome::Matched { .. }),
        "verification off is an explicit opt-out: the corrupt batch is served"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// A disk plan over one segment of a simulated (file-less) partition that
/// persisted `persisted` bytes.
fn file_less_plan(persisted: u64) -> DiskReadPlan {
    DiskReadPlan {
        partition_dir: PartitionDirResolution::NoFiles,
        bytes_per_message: None,
        widest_batch_bytes: 0,
        segments: vec![DiskSegment {
            start_offset: 0,
            persisted,
            read_state: SealedSegmentHandle::default(),
            sealed: false,
        }],
        start_position: 0,
        start_index_offset: None,
        namespace_raw: IggyNamespace::new(1, 1, 0).inner(),
        validate_checksum: true,
    }
}

/// A simulated (file-less) partition has no segment files by design. Until
/// a flush persists anything, the journal is its only tier, so a disk poll
/// must stay `Empty` and the caller serves the journal.
#[compio::test]
async fn read_disk_serves_journal_when_partition_has_no_files() {
    let outcome = file_less_plan(0)
        .read_disk(MessageLookup::Offset {
            offset: 0,
            count: 10,
            ceiling: u64::MAX,
        })
        .await;

    assert!(
        matches!(outcome, DiskReadOutcome::Empty),
        "file-less (simulated) storage must serve the journal tier, not fault",
    );
}

/// A flush evicts what it persisted from the journal, and a file-less
/// partition cannot read those bytes back. Serving the journal forward
/// would answer a poll below the resident tail with that tail and skip the
/// flushed offsets, so the poll must fail closed.
#[compio::test]
async fn read_disk_faults_closed_when_file_less_partition_flushed() {
    let outcome = file_less_plan(512)
        .read_disk(MessageLookup::Offset {
            offset: 0,
            count: 10,
            ceiling: u64::MAX,
        })
        .await;

    assert!(
        matches!(outcome, DiskReadOutcome::Faulted(DiskFault::Unreadable)),
        "flushed offsets of a file-less partition must fault-close, not serve the journal",
    );
}

/// A live partition whose dir is transiently unresolvable (mid-rotation)
/// may hold disk-resident data the walk cannot reach; the poll must
/// fault-close instead of letting the journal-forward skip those offsets.
#[compio::test]
async fn read_disk_faults_closed_when_partition_dir_unresolvable() {
    let plan = DiskReadPlan {
        partition_dir: PartitionDirResolution::Unresolvable,
        bytes_per_message: None,
        widest_batch_bytes: 0,
        segments: vec![DiskSegment {
            start_offset: 0,
            persisted: 512,
            read_state: SealedSegmentHandle::default(),
            sealed: false,
        }],
        start_position: 0,
        start_index_offset: None,
        namespace_raw: IggyNamespace::new(1, 1, 0).inner(),
        validate_checksum: true,
    };

    let outcome = plan
        .read_disk(MessageLookup::Offset {
            offset: 0,
            count: 10,
            ceiling: u64::MAX,
        })
        .await;

    assert!(
        matches!(outcome, DiskReadOutcome::Faulted(DiskFault::Retryable)),
        "unresolvable dir over file-backed data must fault-close, not serve the journal",
    );
}

/// A sealed-segment poll opens the file once and caches the read fd; a later
/// poll of the same segment reuses the cached descriptor. Proven by
/// unlinking the file after the first read: a fresh open-by-path would now
/// fail, so a successful second read can only come from the cached fd (which
/// reads the still-open, unlinked inode).
#[compio::test]
async fn read_disk_caches_and_reuses_sealed_segment_fd() {
    let namespace = IggyNamespace::new(1, 1, 0);

    let dir = std::env::temp_dir().join(format!(
        "iggy-read-disk-fdcache-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos(),
    ));
    compio::fs::create_dir_all(&dir)
        .await
        .expect("create temp partition dir");
    let partition_dir = dir.to_string_lossy().into_owned();

    let record = build_segment_record(namespace, 0);
    let record_len = record.len() as u64;
    let path = format!("{partition_dir}/{:0>20}.log", 0u64);
    {
        let mut file = compio::fs::File::create(&path)
            .await
            .expect("create segment file");
        let (written, _) = file.write_all_at(record, 0).await.into();
        written.expect("write segment record");
        file.sync_all().await.expect("flush segment file");
    }

    let handle = SealedSegmentHandle::default();
    // The pump touches the poll's start segment before cloning its handle
    // into the plan, so a cache-eligible handle is always tracked.
    handle.tracked.set(true);
    assert!(handle.fd.borrow().is_none(), "fd cache slot starts empty");

    let plan = DiskReadPlan {
        partition_dir: PartitionDirResolution::Resolved(partition_dir.clone()),
        bytes_per_message: None,
        widest_batch_bytes: 0,
        validate_checksum: true,
        segments: vec![DiskSegment {
            start_offset: 0,
            persisted: record_len,
            read_state: Rc::clone(&handle),
            sealed: true,
        }],
        start_position: 0,
        start_index_offset: None,
        namespace_raw: namespace.inner(),
    };
    let first = plan
        .read_disk(MessageLookup::Offset {
            offset: 0,
            count: 1,
            ceiling: u64::MAX,
        })
        .await;
    assert!(
        matches!(first, DiskReadOutcome::Matched { .. }),
        "first sealed poll must match the batch",
    );
    assert!(
        handle.fd.borrow().is_some(),
        "first sealed poll must populate the read-fd cache slot",
    );

    // Unlink the file: a fresh open-by-path would fail now, so the second
    // read succeeding proves the cached fd was reused.
    std::fs::remove_file(&path).expect("unlink segment file");

    let plan = DiskReadPlan {
        partition_dir: PartitionDirResolution::Resolved(partition_dir.clone()),
        bytes_per_message: None,
        widest_batch_bytes: 0,
        validate_checksum: true,
        segments: vec![DiskSegment {
            start_offset: 0,
            persisted: record_len,
            read_state: Rc::clone(&handle),
            sealed: true,
        }],
        start_position: 0,
        start_index_offset: None,
        namespace_raw: namespace.inner(),
    };
    let second = plan
        .read_disk(MessageLookup::Offset {
            offset: 0,
            count: 1,
            ceiling: u64::MAX,
        })
        .await;
    assert!(
        matches!(second, DiskReadOutcome::Matched { .. }),
        "cached fd must serve the read after the segment path is unlinked",
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// An untracked handle (a sealed segment the walk crosses without being the
/// poll's start segment, or a slot evicted mid-poll) opens its file
/// transiently: the read succeeds but no fd is retained, so the sealed LRU
/// cap stays a true bound on resident descriptors.
#[compio::test]
async fn read_disk_does_not_retain_fd_for_untracked_handle() {
    let namespace = IggyNamespace::new(1, 1, 0);

    let dir = std::env::temp_dir().join(format!(
        "iggy-read-disk-untracked-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos(),
    ));
    compio::fs::create_dir_all(&dir)
        .await
        .expect("create temp partition dir");
    let partition_dir = dir.to_string_lossy().into_owned();

    let record = build_segment_record(namespace, 0);
    let record_len = record.len() as u64;
    let path = format!("{partition_dir}/{:0>20}.log", 0u64);
    {
        let mut file = compio::fs::File::create(&path)
            .await
            .expect("create segment file");
        let (written, _) = file.write_all_at(record, 0).await.into();
        written.expect("write segment record");
        file.sync_all().await.expect("flush segment file");
    }

    let handle = SealedSegmentHandle::default();
    let plan = DiskReadPlan {
        partition_dir: PartitionDirResolution::Resolved(partition_dir.clone()),
        bytes_per_message: None,
        widest_batch_bytes: 0,
        validate_checksum: true,
        segments: vec![DiskSegment {
            start_offset: 0,
            persisted: record_len,
            read_state: Rc::clone(&handle),
            sealed: true,
        }],
        start_position: 0,
        start_index_offset: None,
        namespace_raw: namespace.inner(),
    };
    let outcome = plan
        .read_disk(MessageLookup::Offset {
            offset: 0,
            count: 1,
            ceiling: u64::MAX,
        })
        .await;
    assert!(
        matches!(outcome, DiskReadOutcome::Matched { .. }),
        "the transient open must still serve the read",
    );
    assert!(
        handle.fd.borrow().is_none(),
        "an untracked handle must not retain the fd",
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// A sealed-segment poll reloads the dropped sparse index from the `.index`
/// file and resolves the start byte from it, skipping the full-segment scan.
/// Proven by prefixing the `.log` with bytes a scan from position 0 would
/// fault on: only an index that jumps straight to the batch reads it.
#[compio::test]
async fn read_disk_reloads_sealed_index_to_skip_scan() {
    let namespace = IggyNamespace::new(1, 1, 0);

    let dir = std::env::temp_dir().join(format!(
        "iggy-read-disk-idxreload-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos(),
    ));
    compio::fs::create_dir_all(&dir)
        .await
        .expect("create temp partition dir");
    let partition_dir = dir.to_string_lossy().into_owned();

    // `.log`: an undecodable prefix (a scan from byte 0 faults on it) then a
    // valid batch at offset 5. `.index`: one sparse entry mapping offset 5
    // to the batch's byte position, so the poll jumps past the prefix.
    let prefix = vec![0xABu8; 512];
    let prefix_len = prefix.len() as u64;
    let batch = build_segment_record(namespace, 5);
    let mut log_bytes = prefix;
    log_bytes.extend_from_slice(&batch);
    let log_len = log_bytes.len() as u64;
    let log_path = format!("{partition_dir}/{:0>20}.log", 0u64);
    {
        let mut file = compio::fs::File::create(&log_path)
            .await
            .expect("create segment log");
        let (written, _) = file.write_all_at(log_bytes, 0).await.into();
        written.expect("write segment log");
        file.sync_all().await.expect("flush segment log");
    }

    let index_bytes = crate::iggy_index::IggyIndexCache::serialize(
        &crate::iggy_index::IggyIndex::new(5, 0, prefix_len),
    );
    let index_path = format!("{partition_dir}/{:0>20}.index", 0u64);
    {
        let mut file = compio::fs::File::create(&index_path)
            .await
            .expect("create segment index");
        let (written, _) = file.write_all_at(index_bytes, 0).await.into();
        written.expect("write segment index");
        file.sync_all().await.expect("flush segment index");
    }

    let handle = SealedSegmentHandle::default();
    let plan = DiskReadPlan {
        partition_dir: PartitionDirResolution::Resolved(partition_dir.clone()),
        bytes_per_message: None,
        widest_batch_bytes: 0,
        validate_checksum: true,
        segments: vec![DiskSegment {
            start_offset: 0,
            persisted: log_len,
            read_state: Rc::clone(&handle),
            sealed: true,
        }],
        // Byte 0, exactly what disk_poll_start returns for a sealed segment
        // whose resident index was dropped.
        start_position: 0,
        start_index_offset: None,
        namespace_raw: namespace.inner(),
    };
    let outcome = plan
        .read_disk(MessageLookup::Offset {
            offset: 5,
            count: 1,
            ceiling: u64::MAX,
        })
        .await;
    assert!(
        matches!(outcome, DiskReadOutcome::Matched { .. }),
        "the reloaded sparse index must skip the prefix; a scan from byte 0 would fault",
    );
    assert!(
        handle.index.borrow().is_some(),
        "the sealed poll must cache the reloaded sparse index",
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// A sparse index past `SEALED_INDEX_RESIDENT_MAX_BYTES` (a dense flush
/// cadence can make it track every message, hundreds of MB per segment) is
/// binary-searched on file instead of materialized: the poll still resolves
/// the exact start byte (proven by the poison prefix a byte-0 scan would
/// fault on) while the handle's index slot stays empty.
#[compio::test]
async fn read_disk_resolves_oversized_index_without_materializing() {
    let namespace = IggyNamespace::new(1, 1, 0);

    let dir = std::env::temp_dir().join(format!(
        "iggy-read-disk-bigidx-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos(),
    ));
    compio::fs::create_dir_all(&dir)
        .await
        .expect("create temp partition dir");
    let partition_dir = dir.to_string_lossy().into_owned();

    let index_size = crate::iggy_index::IGGY_INDEX_SIZE as u64;
    let entry_count = crate::poll_plan::SEALED_INDEX_RESIDENT_MAX_BYTES / index_size + 1;
    let target_offset = entry_count - 1;

    let prefix = vec![0xABu8; 512];
    let prefix_len = prefix.len() as u64;
    let batch = build_segment_record(namespace, target_offset);
    let mut log_bytes = prefix;
    log_bytes.extend_from_slice(&batch);
    let log_len = log_bytes.len() as u64;
    let log_path = format!("{partition_dir}/{:0>20}.log", 0u64);
    {
        let mut file = compio::fs::File::create(&log_path)
            .await
            .expect("create segment log");
        let (written, _) = file.write_all_at(log_bytes, 0).await.into();
        written.expect("write segment log");
        file.sync_all().await.expect("flush segment log");
    }

    // Every entry below the target points at byte 0 (the poison prefix),
    // so only an exact lower-bound hit on the last entry reads the batch.
    let mut index_bytes =
        Vec::with_capacity(usize::try_from(entry_count * index_size).expect("fits in usize"));
    for entry in 0..entry_count {
        index_bytes.extend_from_slice(&entry.to_le_bytes());
        index_bytes.extend_from_slice(&entry.to_le_bytes());
        let position = if entry == target_offset {
            prefix_len
        } else {
            0
        };
        index_bytes.extend_from_slice(&position.to_le_bytes());
    }
    let index_path = format!("{partition_dir}/{:0>20}.index", 0u64);
    {
        let mut file = compio::fs::File::create(&index_path)
            .await
            .expect("create segment index");
        let (written, _) = file.write_all_at(index_bytes, 0).await.into();
        written.expect("write segment index");
        file.sync_all().await.expect("flush segment index");
    }

    let handle = SealedSegmentHandle::default();
    let plan = DiskReadPlan {
        partition_dir: PartitionDirResolution::Resolved(partition_dir.clone()),
        bytes_per_message: None,
        widest_batch_bytes: 0,
        validate_checksum: true,
        segments: vec![DiskSegment {
            start_offset: 0,
            persisted: log_len,
            read_state: Rc::clone(&handle),
            sealed: true,
        }],
        start_position: 0,
        start_index_offset: None,
        namespace_raw: namespace.inner(),
    };
    let outcome = plan
        .read_disk(MessageLookup::Offset {
            offset: target_offset,
            count: 1,
            ceiling: u64::MAX,
        })
        .await;
    assert!(
        matches!(outcome, DiskReadOutcome::Matched { .. }),
        "the on-file lower bound must resolve past the poison prefix",
    );
    assert!(
        handle.index.borrow().is_none(),
        "an index past the resident cap must never be materialized",
    );

    let _ = std::fs::remove_dir_all(&dir);
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

//     #[compio::test]
//     async fn given_a_fenced_window_when_the_anchor_proves_it_should_release_the_walk() {
//         let mut partition = test_partition();
//         let first = journal_sealed(&partition, 1, 0, 1).await;
//         let second = journal_sealed(&partition, 2, first.checksum, 2).await;
//         partition.consensus.restore_commit_state(0, 2);
//         partition.fence_view_window(2, None);
//         assert!(
//             walked_ops(&partition).is_empty(),
//             "the walk applied an entry the view has not vouched for"
//         );
//
//         partition.offer_view_anchor(sealed_prepare(3, second.checksum, 3).header());
//         assert_eq!(
//             partition.prove_view_window().await,
//             Some(ViewWindowVerdict::Proven)
//         );
//         assert!(!partition.view_fenced());
//         assert_eq!(walked_ops(&partition), vec![1, 2]);
//     }

#[compio::test]
async fn given_an_old_views_entry_under_the_anchor_when_proving_without_a_dispatcher_should_drop_it_and_stay_fenced()
 {
    let mut partition = test_partition();
    let first = journal_sealed(&partition, 1, 0, 1).await;
    // The view's op 2, which the anchor chains to, is not what is held.
    let canonical_second = sealed_prepare(2, first.checksum, 2);
    journal_sealed(&partition, 2, first.checksum, 99).await;
    partition.consensus.restore_commit_state(0, 2);
    partition.fence_view_window(
        2,
        Some(*sealed_prepare(3, canonical_second.header().checksum, 3).header()),
    );

    assert_eq!(
        partition.prove_view_window().await,
        Some(ViewWindowVerdict::Break { op: 2 })
    );
    assert!(!partition.log.journal().inner.holds_op(2));
    assert!(partition.log.journal().inner.holds_op(1));
    assert!(
        walked_ops(&partition).is_empty(),
        "op 1 is not proven until op 2 is refetched, so the walk must still hold"
    );
    assert!(partition.view_fenced());
}

//     #[compio::test]
//     async fn given_a_hole_in_the_fenced_window_when_proving_should_truncate_nothing() {
//         let mut partition = test_partition();
//         journal_sealed(&partition, 1, 0, 1).await;
//         let third = journal_sealed(&partition, 3, 7, 3).await;
//         partition.consensus.restore_commit_state(0, 3);
//         partition.fence_view_window(3, Some(*sealed_prepare(4, third.checksum, 4).header()));
//
//         assert_eq!(
//             partition.prove_view_window().await,
//             Some(ViewWindowVerdict::Hole { op: 2 })
//         );
//         assert!(
//             partition.log.journal().inner.holds_op(1) && partition.log.journal().inner.holds_op(3),
//             "a hole is no evidence against the entries around it"
//         );
//         assert_eq!(partition.view_fence_hole(), Some(2));
//         assert!(partition.view_fenced());
//     }
//
//     #[compio::test]
//     async fn given_a_recovered_commit_point_when_fenced_should_walk_the_committed_prefix() {
//         let mut partition = test_partition();
//         let first = journal_sealed(&partition, 1, 0, 1).await;
//         let second = journal_sealed(&partition, 2, first.checksum, 2).await;
//         journal_sealed(&partition, 3, second.checksum, 3).await;
//         partition.consensus.restore_commit_state(0, 3);
//         partition.recovered_commit = 2;
//         partition.fence_view_window(3, None);
//
//         assert!(partition.view_fenced());
//         assert_eq!(
//             walked_ops(&partition),
//             vec![1, 2],
//             "the recovered WAL already named ops 1 and 2 committed"
//         );
//     }

#[compio::test]
async fn given_a_wal_naming_a_commit_point_when_restarted_and_fenced_should_commit_the_recovered_prefix()
 {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition_at(1, 3);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.log.retire_front().unwrap();
    partition
        .install_empty_segment(&repair_config(), 0)
        .await
        .unwrap();
    let mut journal = journal::PartitionPrepareJournal::open(
        &directory.path().join("prepares-0"),
        partition.consensus().group(),
        0,
    )
    .await
    .unwrap();
    let mut parent = 0;
    for op in 1..=4u64 {
        let mut prepare = checksummed_segment_prepare(op, parent, op - 1, b"recovered");
        prepare = prepare.transmute_header(|old, header: &mut PrepareHeader| {
            *header = old;
            header.commit = op - 1;
            header.checksum = header.identity_checksum();
        });
        parent = prepare.header().checksum;
        journal.append(prepare.into_frozen()).await.unwrap();
    }
    drop(journal);
    partition.open_persistence().await.unwrap();
    assert_eq!(partition.consensus.commit_min(), 0);
    assert_eq!(partition.consensus.commit_max(), 3);

    partition.fence_view_window(4, None);
    assert!(partition.view_fenced());
    partition.commit_journal(&repair_config()).await;

    assert!(partition.fatal.is_none(), "{:?}", partition.fatal);
    assert_eq!(
        partition.consensus.commit_min(),
        3,
        "the recovered WAL named ops 1..=3 committed"
    );
    assert!(
        partition.view_fenced(),
        "op 4 stays held until the view's log proves it"
    );
}

#[compio::test]
async fn given_a_fenced_window_when_its_hole_fills_should_report_it_held() {
    let mut partition = test_partition();
    let first = journal_sealed(&partition, 1, 0, 1).await;
    partition.consensus.restore_commit_state(0, 3);
    partition.fence_view_window(3, Some(*sealed_prepare(4, 9, 4).header()));
    assert_eq!(partition.held_through(), 1);
    assert!(!partition.view_fence_held());

    let second = journal_sealed(&partition, 2, first.checksum, 2).await;
    assert!(!partition.view_fence_held());
    journal_sealed(&partition, 3, second.checksum, 3).await;
    assert_eq!(partition.held_through(), 3);
    assert!(partition.view_fence_held());
}

#[compio::test]
async fn given_an_anchor_from_another_view_when_offered_should_refuse_it() {
    let mut partition = test_partition();
    let first = journal_sealed(&partition, 1, 0, 1).await;
    partition.consensus.restore_commit_state(0, 1);
    partition.fence_view_window(1, None);
    let mut other_view = *sealed_prepare(2, first.checksum, 2).header();
    other_view.view = partition.consensus.view() + 1;
    assert!(
        !partition.offer_view_anchor(&other_view),
        "an anchor from a view the fence was not installed in vouches for the wrong log"
    );
    assert!(partition.view_fence_unanchored());
}

#[compio::test]
async fn given_an_unsealed_anchor_when_fencing_should_stay_unanchored() {
    let mut partition = test_partition();
    journal_sealed(&partition, 1, 0, 1).await;
    partition.consensus.restore_commit_state(0, 1);
    // What a `StartView` carries at a commit point the primary holds no
    // header for: checksum and parent both zero.
    let blank = PrepareHeader {
        command: Command::Prepare,
        op: 2,
        ..Default::default()
    };
    partition.fence_view_window(1, Some(blank));
    assert!(
        partition.view_fence_unanchored(),
        "a blank anchor links any window below it"
    );
    assert!(!partition.offer_view_anchor(&blank));
}

/// A Break leaves `last_prepare_checksum` naming the entry it dropped, and the
/// refetch fills the op without moving the head; unless the proof refreshes
/// it, certification keeps failing on a quiet partition.
#[compio::test]
async fn given_a_stale_last_prepare_checksum_when_the_window_proves_should_certify() {
    let (_directory, mut partition, held) = partition_with_pending_durable_ack().await;
    let persistence = partition.persistence.as_ref().unwrap().clone();
    let view = partition.consensus().view() + 1;
    partition.consensus.set_view(view);
    partition.consensus().set_log_view(view);
    partition.consensus().restore_commit_state(0, 1);
    partition
        .consensus()
        .set_last_prepare_checksum(held.checksum ^ 1);
    let anchor = checksummed_segment_prepare(2, held.checksum, 0, b"anchor");
    partition.fence_view_window(1, Some(*anchor.header()));
    assert_eq!(
        partition.prove_view_window().await,
        Some(ViewWindowVerdict::Proven)
    );
    partition.ensure_wal_view();
    persistence
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();
    assert_eq!(persistence.certified_log_view(), Some(view));
}

#[compio::test]
async fn given_a_fenced_wal_when_certifying_should_vouch_only_for_the_applied_prefix() {
    let (_directory, mut partition, held) = partition_with_pending_durable_ack().await;
    let persistence = partition.persistence.as_ref().unwrap().clone();
    let view = partition.consensus().view() + 1;
    partition.consensus.set_view(view);
    partition.consensus().set_log_view(view);
    partition.consensus().sequencer().set_sequence(4);

    // A window reaching the suffix the next `DoViewChange` reports: muted.
    partition.consensus().restore_commit_state(held.op, 3);
    partition.fence_view_window(3, None);
    assert!(!partition.ensure_wal_view());
    persistence
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();
    assert_ne!(persistence.certified_log_view(), Some(view));

    // WAL holds nothing above `commit_min`, window below the suffix: certified.
    partition.consensus().advance_commit_max(4);
    partition.ensure_wal_view();
    persistence
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();
    assert_eq!(persistence.certified_log_view(), Some(view));
}

#[compio::test]
async fn given_an_unanchored_fence_when_ticking_should_ask_for_headers_at_the_pace_given() {
    let mut partition = test_partition();
    journal_sealed(&partition, 1, 0, 1).await;
    partition.consensus.restore_commit_state(0, 1);
    partition.fence_view_window(1, None);
    let due: Vec<bool> = (0..6).map(|_| partition.view_fence_probe_due(3)).collect();
    assert_eq!(due, vec![false, false, true, false, false, true]);
}

#[compio::test]
async fn given_a_cached_hole_when_the_missing_op_arrives_should_bypass_the_cache() {
    let mut partition = test_partition();
    journal_sealed(&partition, 1, 0, 1).await;
    let third = journal_sealed(&partition, 3, 7, 3).await;
    partition.consensus.restore_commit_state(0, 3);
    partition.fence_view_window(3, Some(*sealed_prepare(4, third.checksum, 4).header()));
    assert_eq!(
        partition.prove_view_window().await,
        Some(ViewWindowVerdict::Hole { op: 2 })
    );
    assert_eq!(partition.view_fence.and_then(|fence| fence.hole), Some(2));
    // Op 3 chains to parent 7, which the arriving op 2 is not.
    journal_sealed(&partition, 2, 0, 99).await;
    assert_eq!(
        partition.prove_view_window().await,
        Some(ViewWindowVerdict::Break { op: 2 }),
        "the cache must be bypassed once the missing op arrives"
    );
}

/// The fence holds entries back from the WAL too: a stale entry can sit at the
/// WAL head, and appending the view's successor on top of it fails the WAL's
/// parent check, which fences the whole partition.
#[compio::test]
async fn given_a_stale_wal_head_when_fenced_should_not_persist_repaired_successors() {
    let (_directory, mut partition, stale) = partition_with_pending_durable_ack().await;
    let persistence = partition.persistence.as_ref().unwrap().clone();
    assert_eq!(persistence.head(), stale.op);
    let canonical = checksummed_segment_prepare(1, 0, 7, b"canonical");
    let successor = checksummed_segment_prepare(2, canonical.header().checksum, 0, b"successor");
    partition
        .log
        .journal()
        .inner
        .append(successor.clone().into_frozen())
        .await
        .unwrap();
    partition.consensus().sequencer().set_sequence(2);
    partition.consensus().restore_commit_state(0, 2);
    partition.fence_view_window(1, Some(*successor.header()));

    partition.persist_repaired_prefix();
    assert_eq!(
        persistence.head(),
        stale.op,
        "a repaired successor reached the WAL above an unproven entry"
    );
    assert!(persistence.failure().is_none());
}

/// With an I/O dispatcher the Break is handed to the lane: the stale entry
/// stays resident and repair frames are refused until the cut lands, then the
/// hole is refetched at once.
#[compio::test]
async fn given_a_dispatcher_when_the_proof_breaks_should_defer_the_drop_to_the_lane() {
    let (_directory, mut partition, stale) = partition_with_pending_durable_ack().await;
    let persistence = partition.persistence.as_ref().unwrap().clone();
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    let view = partition.consensus().view() + 1;
    partition.consensus.set_view(view);
    partition.consensus().set_log_view(view);
    partition.consensus().restore_commit_state(0, 1);
    let canonical = checksummed_segment_prepare(1, 0, 7, b"canonical");
    let anchor = checksummed_segment_prepare(2, canonical.header().checksum, 0, b"anchor");
    partition.repair = Some(RepairSession {
        view,
        ..armed_session(2, 0)
    });
    partition.fence_view_window(1, Some(*anchor.header()));

    assert_eq!(
        partition.prove_view_window().await,
        Some(ViewWindowVerdict::Break { op: 1 })
    );
    assert!(
        partition.log.journal().inner.holds_op(1),
        "the lane, not the proof, truncates a mounted partition"
    );
    assert_eq!(partition.unproven_drop_from, Some(1));
    assert!(partition.refuses_prepares());
    partition.apply_repaired_prepare(canonical.clone()).await;
    partition.apply_repaired_prepare(anchor.clone()).await;
    assert!(
        !partition.log.journal().inner.holds_op(2),
        "a repaired body landed under a pending cut"
    );
    assert_eq!(
        partition
            .log
            .journal()
            .inner
            .repair_header(1)
            .map(|header| header.checksum),
        Some(stale.checksum),
        "a repaired body journaled before the cut would outlive it in the WAL"
    );

    let config = repair_config();
    let mut applied = false;
    for _ in 0..64 {
        match partition.resume_io(&config).await {
            crate::PartitionIoStep::Ready(plan) => {
                execute_partition_io_plan(&mut partition, &config, plan).await;
            }
            crate::PartitionIoStep::Progress => {}
            crate::PartitionIoStep::ViewApplied { .. } => {
                applied = true;
                break;
            }
            crate::PartitionIoStep::Pending => {
                persistence
                    .drain_with_timeout(compio::runtime::time::sleep)
                    .await
                    .unwrap();
            }
            _ => panic!("unexpected partition I/O continuation"),
        }
    }
    assert!(applied, "the lane never ran the truncation");
    assert!(!partition.log.journal().inner.holds_op(1));
    assert_eq!(partition.unproven_drop_from, None);
    assert_eq!(persistence.head(), 0);
    assert_eq!(
        partition.repair.map(|session| session.requested_from),
        Some(None),
        "the hole must be refetched without waiting out the retry timer"
    );
    assert!(partition.view_fenced());
    assert!(partition.fatal.is_none());
}

/// A proof that breaks while other history work holds the lane drops
/// nothing yet; the anchor that triggered it must not reach the WAL on top
/// of the stale head it does not chain to. Its retransmit skips the proof,
/// since the fence already holds the anchor, so the WAL itself has to refuse.
#[compio::test]
async fn given_busy_history_when_an_anchor_breaks_the_window_should_not_append_it_to_the_wal() {
    let (_directory, mut partition, _stale) = partition_with_pending_durable_ack().await;
    let persistence = partition.persistence.as_ref().unwrap().clone();
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    let view = partition.consensus().view() + 1;
    partition.consensus.set_view(view);
    partition.consensus().set_log_view(view);
    partition.consensus().restore_commit_state(0, 1);
    partition.fence_view_window(1, None);
    partition.maintenance.flush_through = Some(1);
    assert!(partition.history_is_busy());
    let canonical = checksummed_segment_prepare(1, 0, 7, b"canonical");
    let anchor = checksummed_segment_prepare(2, canonical.header().checksum, 0, b"anchor")
        .transmute_header(|mut original, header: &mut PrepareHeader| {
            original.view = view;
            original.checksum = original.identity_checksum();
            *header = original;
        });
    let sequence = partition.consensus().sequencer().current_sequence();
    partition.on_replicate(anchor.clone()).await;
    assert!(!partition.view_fence_unanchored());
    assert!(partition.view_fenced());
    assert_eq!(partition.unproven_drop_from, None);
    assert!(
        !partition.log.journal().inner.holds_op(2),
        "the anchor was journaled above a window its proof broke"
    );
    assert_eq!(
        partition.consensus().sequencer().current_sequence(),
        sequence
    );
    partition.on_replicate(anchor).await;
    let _ = persistence
        .drain_with_timeout(compio::runtime::time::sleep)
        .await;
    assert!(
        persistence.failure().is_none(),
        "the anchor was appended on a WAL head it does not chain to: {:?}",
        persistence.failure()
    );
    assert_eq!(persistence.head(), 1);
    assert!(partition.fatal.is_none());
}

/// The lane's cut is pending but nothing else refuses the frame: the gate
/// on the pending drop alone keeps a repaired body out of the journal.
#[compio::test]
async fn given_a_pending_unproven_drop_when_a_repair_frame_arrives_should_not_journal_it() {
    let mut partition = test_partition();
    partition.repair = Some(armed_session(3, 0));
    partition.unproven_drop_from = Some(1);
    assert!(!partition.refuses_prepares());

    partition
        .apply_repaired_prepare(repaired_send_prepare(1, 0, 1))
        .await;
    assert_eq!(
        partition.log.journal().inner.resident_count(),
        0,
        "a repaired body was journaled under a pending cut"
    );
    assert_eq!(partition.consensus().sequencer().current_sequence(), 0);
}

/// A repair commit floor (`complete_repair` taking the peer's
/// `RangeEvicted` floor) can raise the fence floor past the hole the last
/// proof stopped at. The cached hole then names an op outside the window
/// and must not keep the proof waiting on it.
#[compio::test]
async fn given_a_cached_hole_under_a_raised_floor_when_proving_should_not_wait_on_it() {
    let mut partition = test_partition();
    journal_sealed(&partition, 1, 0, 1).await;
    let third = journal_sealed(&partition, 3, 7, 3).await;
    partition.consensus.restore_commit_state(0, 3);
    partition.fence_view_window(3, Some(*sealed_prepare(4, third.checksum, 4).header()));
    assert_eq!(
        partition.prove_view_window().await,
        Some(ViewWindowVerdict::Hole { op: 2 })
    );

    partition.consensus.set_commit_floor(2);
    assert_eq!(partition.view_fence_hole(), None);
    assert_eq!(
        partition.prove_view_window().await,
        Some(ViewWindowVerdict::Proven),
        "the proof waited on a hole below the raised floor"
    );
    assert!(!partition.view_fenced());
}

/// A prepare received while the window was fenced is journaled but neither
/// persisted nor acked. The primary resends it only while it lacks quorum,
/// after a retransmit timeout, so proving the window releases its ack
/// without waiting for one.
#[compio::test]
async fn given_a_prepare_received_while_fenced_when_the_window_is_proven_should_ack_it_without_a_retransmit()
 {
    let (_directory, mut partition, held) = partition_with_pending_durable_ack().await;
    let persistence = partition.persistence.as_ref().unwrap().clone();
    let sent = partition.consensus().message_bus().sent_to_replicas.clone();
    let view = partition.consensus().view() + 1;
    partition.consensus.set_view(view);
    partition.consensus().set_log_view(view);
    partition.consensus().restore_commit_state(0, 1);
    // The held op 1 was journaled behind the append counter's back; wake
    // the counter so the live successor continues it at offset 1.
    partition.offset_space.append_live = true;
    partition.dirty_offset.store(0, Ordering::Relaxed);
    let successor = checksummed_segment_prepare(2, held.checksum, 1, b"successor")
        .transmute_header(|mut original, header: &mut PrepareHeader| {
            original.view = view;
            original.checksum = original.identity_checksum();
            *header = original;
        });
    partition.fence_view_window(1, Some(*successor.header()));
    partition.on_replicate(successor).await;
    assert!(partition.log.journal().inner.holds_op(2));
    assert_eq!(
        persistence.head(),
        1,
        "a fenced replica persisted a prepare above the window"
    );
    sent.borrow_mut().clear();

    assert_eq!(
        partition.prove_view_window().await,
        Some(ViewWindowVerdict::Proven)
    );
    for _ in 0..4 {
        persistence
            .drain_with_timeout(compio::runtime::time::sleep)
            .await
            .unwrap();
        partition.drive_persistence().await;
    }
    let acked: Vec<u64> = sent
        .borrow()
        .iter()
        .filter_map(|(_, frame)| {
            let header =
                bytemuck::checked::try_from_bytes::<PrepareOkHeader>(frame.as_slice()).ok()?;
            (header.command == Command::PrepareOk).then_some(header.op)
        })
        .collect();
    assert!(
        acked.contains(&2),
        "the fence lifted without acking op 2: {acked:?}"
    );
    assert_eq!(persistence.head(), 2);
    assert!(partition.pending_persisted_acks.borrow().is_empty());
    assert!(partition.fatal.is_none());
}

/// Only a prepare of the view that proved the window was withheld by the
/// fence. An older view's entry above it is not the view's, and its ack
/// would vouch for a log the view never sent.
#[compio::test]
async fn given_an_older_views_entry_above_the_window_when_proven_should_not_queue_its_ack() {
    let mut partition = test_partition();
    let first = journal_sealed(&partition, 1, 0, 1).await;
    let view = partition.consensus().view() + 1;
    partition.consensus.set_view(view);
    let in_view = |op: u64, parent: u128| {
        sealed_prepare(op, parent, op).transmute_header(
            |mut original, header: &mut PrepareHeader| {
                original.view = view;
                original.checksum = original.identity_checksum();
                *header = original;
            },
        )
    };
    let anchor = in_view(2, first.checksum);
    let older = journal_sealed(&partition, 2, first.checksum, 2).await;
    let current = in_view(3, older.checksum);
    partition
        .log
        .journal()
        .inner
        .append(current.into_frozen())
        .await
        .expect("journal append");
    partition.consensus().sequencer().set_sequence(3);
    partition.consensus.restore_commit_state(0, 1);
    partition.fence_view_window(1, Some(*anchor.header()));

    assert_eq!(
        partition.prove_view_window().await,
        Some(ViewWindowVerdict::Proven)
    );
    let queued: Vec<u64> = partition
        .pending_persisted_acks
        .borrow()
        .keys()
        .copied()
        .collect();
    assert_eq!(queued, vec![3], "an older view's entry had its ack queued");
}

#[compio::test]
async fn given_a_fenced_window_when_acking_should_withhold_the_ack() {
    let (mut partition, _) = recording_partition_at(1, 3);
    let prepare = sealed_prepare(1, 0, 1);
    let first = *prepare.header();
    partition
        .log
        .journal()
        .inner
        .append(prepare.into_frozen())
        .await
        .expect("journal append");
    partition.consensus.sequencer().set_sequence(1);
    partition.consensus.restore_commit_state(0, 1);
    assert!(
        partition.send_prepare_ok(&first).await,
        "an unfenced backup acks what it holds"
    );
    partition.fence_view_window(1, None);
    assert!(
        !partition.send_prepare_ok(&first).await,
        "an ack above an unproven window can be wiped when the window breaks"
    );
}

#[compio::test]
async fn given_a_staged_offset_under_commit_max_when_its_entry_is_truncated_should_drop_the_staging()
 {
    let mut partition = test_partition();
    journal_prepare(&partition, 1, Operation::StoreConsumerOffset).await;
    journal_prepare(&partition, 2, Operation::StoreConsumerOffset).await;
    partition.consensus.sequencer().set_sequence(2);
    partition.consensus.restore_commit_state(0, 2);
    partition.stage_consumer_offset_upsert(2, ConsumerKind::Consumer, 8, 0, false);
    partition.truncate_uncommitted_from(2).await.unwrap();
    assert!(
        !partition.pending_consumer_offset_commits.contains_key(&2),
        "the stale cursor would be applied in place of the view's op 2"
    );
}

/// Walking through the head advances `commit_min` past a resident entry only
/// `on_ack` can pop and answer, after which every later ack finds the drain
/// empty and no reply is ever shipped.
#[compio::test]
async fn given_a_pipeline_head_when_walking_the_journal_should_stop_below_it() {
    let partition = test_partition();
    for op in 1..=4 {
        journal_prepare(&partition, op, Operation::CreateStream).await;
    }
    partition.consensus.restore_commit_state(0, 4);
    partition.consensus.pipeline_message(
        PlaneKind::Partitions,
        &pipeline_prepare(3, Operation::CreateStream),
    );

    let ops: Vec<u64> = partition
        .collect_committable_from_journal(COMMIT_WALK_OPS_MAX, &repair_config())
        .into_iter()
        .map(|entry| entry.op)
        .collect();
    assert_eq!(
        ops,
        vec![1, 2],
        "the walk stops at the op the pipeline holds, leaving 3 to on_ack"
    );
}

/// A backup journals replicated prepares and never populates a pipeline, so an
/// absent head must mean NO ceiling. Read as a ceiling of zero it would stop
/// every backup's commit walk.
#[compio::test]
async fn given_an_empty_pipeline_when_walking_the_journal_should_not_cap() {
    let partition = test_partition();
    for op in 1..=3 {
        journal_prepare(&partition, op, Operation::CreateStream).await;
    }
    partition.consensus.restore_commit_state(0, 3);
    assert!(partition.consensus.pipeline_head_header().is_none());

    let ops: Vec<u64> = partition
        .collect_committable_from_journal(COMMIT_WALK_OPS_MAX, &repair_config())
        .into_iter()
        .map(|entry| entry.op)
        .collect();
    assert_eq!(ops, vec![1, 2, 3], "a backup walks its whole committed run");
}

#[compio::test]
async fn persisted_commit_walk_preserves_pipeline_gaps_and_wal_ceiling() {
    let directory = tempfile::tempdir().unwrap();
    let mut partition = partition_at_view(0, 0);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.runtime_options.durability = iggy_common::Durability::Persisted;
    partition.open_persistence().await.unwrap();
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    let mut parent = 0;
    for op in 1..=4 {
        let prepare = checksummed_segment_prepare(op, parent, op - 1, b"payload");
        parent = prepare.header().checksum;
        if op >= 3 {
            partition
                .consensus
                .pipeline_message(PlaneKind::Partitions, &prepare);
        }
        let frozen = prepare.into_frozen();
        if op <= 3 {
            persistence.append(frozen.clone(), true).unwrap();
        }
        partition.log.journal().inner.append(frozen).await.unwrap();
    }
    assert!(persistence.start());
    Rc::clone(&persistence).run().await;
    assert!(persistence.failure().is_none());
    persistence.exhaust_capacity_for_test();
    partition.consensus.restore_commit_state(0, 4);

    assert_eq!(partition.select_persistable_commits(&repair_config()), []);
    let journaled =
        partition.collect_committable_from_journal(COMMIT_WALK_OPS_MAX, &repair_config());
    assert_eq!(
        journaled.iter().map(|entry| entry.op).collect::<Vec<_>>(),
        vec![1, 2]
    );
    partition.consensus.advance_commit_min(1);
    partition.consensus.advance_commit_min(2);
    let drained = partition.select_persistable_commits(&repair_config());
    assert_eq!(
        drained.iter().map(|entry| entry.op).collect::<Vec<_>>(),
        vec![3]
    );
    assert_eq!(partition.consensus.pipeline_head_header().unwrap().op, 3);
    partition
        .handle_committed_entries(drained, &repair_config(), true)
        .await;
    assert_eq!(partition.consensus.commit_min(), 3);
    assert_eq!(partition.select_persistable_commits(&repair_config()), []);
    assert_eq!(partition.consensus.pipeline_head_header().unwrap().op, 4);
    assert_eq!(
        partition.collect_committable_from_journal(COMMIT_WALK_OPS_MAX, &repair_config()),
        []
    );
    persistence.retire();
}

fn pipeline_prepare(op: u64, operation: Operation) -> Message<PrepareHeader> {
    let size = std::mem::size_of::<PrepareHeader>();
    Message::<PrepareHeader>::new(size).transmute_header(|_, header: &mut PrepareHeader| {
        header.command = Command::Prepare;
        header.op = op;
        header.operation = operation;
        header.retry_capacity = u32::try_from(consensus::PARTITION_DEDUP_CLIENTS_MAX).unwrap();
        header.size = u32::try_from(size).expect("prepare header size fits in u32");
    })
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

//     #[compio::test]
//     async fn given_paused_repair_when_valid_frames_arrive_should_refresh_activity_without_writing()
//     {
//         const IDLE_TICKS: u32 = 7;
//         let mut partition = test_partition();
//         partition.repair = Some(armed_session(3, 0));
//         partition.repair.as_mut().unwrap().idle_ticks = IDLE_TICKS;
//         partition.repair_attempts = 2;
//         partition.transition = Some(PendingPartitionTransition::View { drain: None });
//
//         partition
//             .apply_repaired_prepare(repaired_send_prepare(4, 0, 4))
//             .await;
//         assert_eq!(partition.repair.as_ref().unwrap().idle_ticks, IDLE_TICKS);
//         assert_eq!(partition.repair_attempts, 2);
//         partition
//             .apply_repaired_prepare(repaired_send_prepare(1, 0, 1))
//             .await;
//         assert_eq!(partition.repair.as_ref().unwrap().idle_ticks, 0);
//         assert_eq!(partition.repair_attempts, 0);
//         assert_eq!(partition.log.journal().inner.resident_count(), 0);
//         assert_eq!(partition.consensus().sequencer().current_sequence(), 0);
//     }

#[compio::test]
async fn given_held_repair_frame_when_peer_re_serves_it_should_keep_stall_budget() {
    const IDLE_TICKS: u32 = 7;
    const ATTEMPTS: u32 = 2;
    let mut partition = test_partition();
    partition.repair = Some(armed_session(3, 0));
    partition
        .apply_repaired_prepare(repaired_send_prepare(1, 0, 1))
        .await;
    assert!(partition.log.journal().inner.holds_op(1));
    partition.repair.as_mut().unwrap().idle_ticks = IDLE_TICKS;
    partition.repair_attempts = ATTEMPTS;

    partition
        .apply_repaired_prepare(repaired_send_prepare(1, 0, 1))
        .await;

    assert_eq!(
        partition.repair.as_ref().unwrap().idle_ticks,
        IDLE_TICKS,
        "a held frame must not restart the stall clock"
    );
    assert_eq!(
        partition.repair_attempts, ATTEMPTS,
        "a held frame must not refund the stall budget"
    );
}

/// A frame that passes the window filters but that the journal refuses
/// stays not held, so the peer serves it again every round. It is no
/// progress, so the budget must still run out.
#[compio::test]
async fn given_unjournalable_repair_frame_when_peer_re_serves_it_should_spend_stall_budget() {
    let mut partition = test_partition();
    partition.repair = Some(armed_session(3, 0));

    let mut verdicts = Vec::new();
    for _ in 0..=crate::types::REPAIR_MAX_STALL_RETRIES {
        partition
            .apply_repaired_prepare(pipeline_prepare(1, Operation::SendMessages))
            .await;
        assert!(!partition.log.journal().inner.holds_op(1));
        verdicts.push(partition.burn_repair_attempt());
    }

    assert_eq!(
        verdicts.iter().position(|&rotate| rotate),
        Some(verdicts.len() - 1),
        "a frame the journal refuses must not refund the budget: {verdicts:?}"
    );
}

/// Without a prepare WAL a refused floor stays in progress while the
/// window is incomplete. Every round loses op `FLOOR + 1`, so the leading
/// edge below the anchor stays open and the refusal never turns final,
/// while the rest of each round re-serves frames this journal already
/// holds. Held frames are no progress, so the budget must run out and
/// rotate the peer. Above a committed prefix, so the walk mark that the
/// first round records must not pass for progress in the rounds after it.
#[compio::test]
async fn given_walless_refused_floor_when_peer_re_serves_held_frames_should_rotate_session() {
    const COMMIT_MIN: u64 = 5;
    const FLOOR: u64 = 10;
    const SERVED_THROUGH: u64 = 12;
    const COMMIT_TO_OP: u64 = SERVED_THROUGH + 1;
    let (mut backup, _) = recording_partition_at(1, 3);
    backup
        .consensus()
        .restore_commit_state(COMMIT_MIN, COMMIT_MIN);
    backup.consensus().advance_commit_max(COMMIT_TO_OP);
    backup.repair = Some(armed_session(COMMIT_TO_OP, FLOOR));
    let config = repair_config();

    let mut verdicts = Vec::new();
    for _ in 0..=crate::types::REPAIR_MAX_STALL_RETRIES {
        for op in FLOOR + 2..=SERVED_THROUGH {
            backup
                .apply_repaired_prepare(checksummed_segment_prepare(op, 0, op - 1, b"repaired"))
                .await;
        }
        assert_eq!(
            backup.complete_repair(&config).await,
            RepairConclusion::InProgress
        );
        verdicts.push(backup.burn_repair_attempt());
    }

    assert_eq!(
        verdicts.iter().position(|&rotate| rotate),
        Some(verdicts.len() - 1),
        "only the round past the budget rotates: {verdicts:?}"
    );
}

#[compio::test]
async fn given_a_repair_larger_than_the_wal_when_a_prefix_is_durable_should_apply_it_before_fetching_the_rest()
 {
    const OPS: u64 = 3;
    const PAYLOAD_BYTES: usize = 48 * 1024 * 1024;
    let root = tempfile::tempdir().unwrap();
    let mut config = repair_config();
    config.segment_size = IggyByteSize::from(4 * PAYLOAD_BYTES as u64);
    config.path_layout.streams_root = root.path().to_string_lossy().into_owned();
    let directory = std::path::PathBuf::from(config.get_partition_path(1, 1, 0));
    std::fs::create_dir_all(&directory).unwrap();
    let (mut backup, _) = recording_partition_at(1, 3);
    backup.set_partition_dir(directory.to_string_lossy().into_owned());
    set_offset_dirs_under(&mut backup, &directory);
    backup.runtime_options.durability = iggy_common::Durability::Persisted;
    backup.runtime_options.segment_size = Some(config.segment_size);
    let payload = vec![1; PAYLOAD_BYTES];
    let parents = segment_chain_parents(OPS, &payload);
    let prepare = |op: u64| {
        checksummed_segment_prepare(
            op,
            parents[usize::try_from(op - 1).unwrap()],
            op - 1,
            &payload,
        )
    };
    let frame_bytes = prepare(1).total_len();
    backup.log.retire_front().unwrap();
    backup.install_empty_segment(&config, 0).await.unwrap();
    backup
        .open_persistence_with_capacity(
            journal::partition_journal::PARTITION_WAL_CAPACITY_MIN,
            std::time::Duration::ZERO,
        )
        .await
        .unwrap();
    backup.consensus().advance_commit_max(OPS);
    backup.repair = Some(RepairSession {
        floor: None,
        ..armed_session(OPS, 0)
    });
    let persistence = Rc::clone(backup.persistence.as_ref().unwrap());
    assert!(persistence.segment_checkpoint().is_some());

    persistence.append(prepare(1).into_frozen(), true).unwrap();
    assert!(persistence.start());
    backup.apply_repaired_prepare(prepare(1)).await;
    Rc::clone(&persistence).run().await;
    assert!(persistence.is_durable_through(1));
    persistence.append(prepare(2).into_frozen(), true).unwrap();
    assert!(persistence.start());
    backup.apply_repaired_prepare(prepare(2)).await;
    assert!(!persistence.has_capacity(frame_bytes));
    assert!(!persistence.is_durable_through(2));
    assert!(!backup.repair_fetch_ready());

    assert_eq!(
        backup.complete_repair(&config).await,
        RepairConclusion::InProgress
    );
    assert_eq!(
        backup.consensus().commit_min(),
        1,
        "the verified durable prefix must apply before the rest of the window can fit"
    );
    assert!(backup.repair.is_some());
    Rc::clone(&persistence).run().await;
    settle_partition_io(&mut backup, &config).await;
    assert_eq!(
        backup.complete_repair(&config).await,
        RepairConclusion::InProgress
    );
    assert_eq!(backup.consensus().commit_min(), 2);
    backup.checkpoint_persistence(&config).await;
    settle_partition_io(&mut backup, &config).await;
    persistence
        .drain_with_timeout(|timeout| backup.consensus.message_bus().sleep(timeout))
        .await
        .unwrap();
    settle_partition_io(&mut backup, &config).await;
    assert!(
        persistence.has_capacity(frame_bytes),
        "applying and checkpointing the prefix must reclaim WAL capacity"
    );
    assert!(backup.repair_fetch_ready());

    backup.apply_repaired_prepare(prepare(3)).await;
    backup.persist_repaired_prefix();
    persistence
        .drain_with_timeout(|timeout| backup.consensus.message_bus().sleep(timeout))
        .await
        .unwrap();
    settle_partition_io(&mut backup, &config).await;
    assert_eq!(
        backup.complete_repair(&config).await,
        RepairConclusion::Done,
        "commit={}, head={}, durable={}, sequence={}, fatal={:?}",
        backup.consensus().commit_min(),
        persistence.head(),
        persistence.durable_op(),
        backup.consensus().sequencer().current_sequence(),
        backup.fatal(),
    );
    assert_eq!(backup.consensus().commit_min(), OPS);
    assert!(backup.fatal().is_none());
}

/// A bounded commit walk can leave already fetched frames for the next
/// round. Advancing the walk keeps the peer's repair budget intact.
#[compio::test]
async fn given_advancing_repair_walk_when_peer_re_serves_held_frames_should_keep_peer() {
    const ROUNDS: u32 = crate::types::REPAIR_MAX_STALL_RETRIES + 1;
    const COMMIT_TO_OP: u64 = COMMIT_WALK_OPS_MAX as u64 * ROUNDS as u64;
    let (mut backup, _) = recording_partition_at(1, 3);
    backup.consensus().advance_commit_max(COMMIT_TO_OP);
    backup.repair = Some(RepairSession {
        floor: None,
        ..armed_session(COMMIT_TO_OP, 0)
    });
    let config = repair_config();
    let parents = segment_chain_parents(COMMIT_TO_OP, b"repaired");

    for round in 0..ROUNDS {
        for op in backup.consensus().commit_min() + 1..=COMMIT_TO_OP {
            let parent = parents[usize::try_from(op - 1).unwrap()];
            backup
                .apply_repaired_prepare(checksummed_segment_prepare(
                    op,
                    parent,
                    op - 1,
                    b"repaired",
                ))
                .await;
        }
        backup.commit_journal(&config).await;
        assert!(
            !backup.burn_repair_attempt(),
            "round {round} rotated away although the walk reached {}",
            backup.consensus().commit_min()
        );
    }

    assert_eq!(backup.consensus().commit_min(), COMMIT_TO_OP);
}

/// A walk restarts the budget once: the mark moves with it, so the rounds
/// after the walk stops spend a whole budget and no more.
#[test]
fn given_credited_walk_when_later_stall_rounds_find_no_progress_should_rotate_at_budget() {
    const COMMIT_MIN: u64 = 5;
    let (mut backup, _) = recording_partition_at(1, 3);
    backup
        .consensus()
        .restore_commit_state(COMMIT_MIN, COMMIT_MIN + 1);
    assert!(!backup.burn_repair_attempt());
    backup.consensus().advance_commit_min(COMMIT_MIN + 1);

    let verdicts: Vec<bool> = (0..=crate::types::REPAIR_MAX_STALL_RETRIES)
        .map(|_| backup.burn_repair_attempt())
        .collect();

    assert_eq!(
        verdicts.iter().position(|&rotate| rotate),
        Some(verdicts.len() - 1),
        "only the round past the budget that follows the walk rotates: {verdicts:?}"
    );
}

/// A stall round in a new view starts a fresh count, as on the metadata
/// plane: the rotations of an earlier view say nothing about the ring of
/// this one.
#[test]
fn given_burned_repair_budget_when_view_changes_should_start_fresh_counts() {
    let (mut backup, _) = recording_partition_at(1, 3);
    for _ in 0..crate::types::REPAIR_MAX_STALL_RETRIES {
        assert!(!backup.burn_repair_attempt());
    }
    backup.note_repair_rotation();
    let next_view = backup.consensus().view() + 1;
    backup.consensus.set_view(next_view);

    let verdicts: Vec<bool> = (0..=crate::types::REPAIR_MAX_STALL_RETRIES)
        .map(|_| backup.burn_repair_attempt())
        .collect();

    assert_eq!(
        (
            verdicts.iter().position(|&rotate| rotate),
            backup.repair_rotations()
        ),
        (Some(verdicts.len() - 1), 0),
        "the new view must restart the stall rounds and the rotations: {verdicts:?}"
    );
}

#[compio::test]
async fn given_lower_backfill_when_applying_repaired_prepare_should_not_rewind_parent() {
    // The call-site regression for the WAL frontier rewind: a repaired op
    // below the DVC-adopted head must leave BOTH halves of the frontier
    // alone. The old code left the sequencer at the head and rewound
    // `last_prepare_checksum` to the backfilled entry, so the next prepare
    // parented past a committed op and recovery refused the WAL.
    const CHECKSUM_1: u128 = 0x11;
    const CHECKSUM_2: u128 = 0x22;
    const CHECKSUM_3: u128 = 0x33;
    let mut partition = test_partition();
    partition.repair = Some(armed_session(3, 0));

    partition
        .apply_repaired_prepare(repaired_send_prepare(1, 0, CHECKSUM_1))
        .await;
    partition
        .apply_repaired_prepare(repaired_send_prepare(3, CHECKSUM_2, CHECKSUM_3))
        .await;
    // The hole at op 2 stalls the frontier advance.
    assert_eq!(partition.consensus().sequencer().current_sequence(), 1);

    // The state a DoViewChange merge leaves: head 3, parented on its
    // checksum, with the hole at op 2 still unfilled locally.
    partition.consensus().sequencer().set_sequence(3);
    partition.consensus().set_last_prepare_checksum(CHECKSUM_3);

    partition
        .apply_repaired_prepare(repaired_send_prepare(2, CHECKSUM_1, CHECKSUM_2))
        .await;

    assert!(
        partition.log.journal().inner.header_by_op(2).is_some(),
        "the backfill must be journaled"
    );
    assert_eq!(partition.consensus().sequencer().current_sequence(), 3);
    assert_eq!(
        partition.consensus().last_prepare_checksum(),
        CHECKSUM_3,
        "a lower backfill must not rewind the parent of the next prepare"
    );
}

#[compio::test]
async fn given_gap_closure_when_applying_repaired_prepare_should_adopt_new_head_checksum() {
    // The frame that closes a gap is not the new head: the frontier walks
    // to the highest contiguous op and the checksum must come from THAT
    // journal entry, not from the repair frame that happened to arrive
    // last.
    const CHECKSUM_1: u128 = 0x11;
    const CHECKSUM_2: u128 = 0x22;
    const CHECKSUM_3: u128 = 0x33;
    let mut partition = test_partition();
    partition.repair = Some(armed_session(3, 0));

    partition
        .apply_repaired_prepare(repaired_send_prepare(1, 0, CHECKSUM_1))
        .await;
    partition
        .apply_repaired_prepare(repaired_send_prepare(3, CHECKSUM_2, CHECKSUM_3))
        .await;
    assert_eq!(partition.consensus().sequencer().current_sequence(), 1);
    assert_eq!(partition.consensus().last_prepare_checksum(), CHECKSUM_1);

    partition
        .apply_repaired_prepare(repaired_send_prepare(2, CHECKSUM_1, CHECKSUM_2))
        .await;

    assert_eq!(partition.consensus().sequencer().current_sequence(), 3);
    assert_eq!(
        partition.consensus().last_prepare_checksum(),
        CHECKSUM_3,
        "a gap closure must adopt the checksum of the new contiguous head"
    );
}

#[compio::test]
async fn given_prior_view_repair_when_a_new_view_started_should_discard_it() {
    let mut partition = test_partition();
    partition.repair = Some(armed_fetch_session(0, 1, 0));
    partition.consensus.set_view(1);

    partition
        .apply_repaired_prepare(repaired_send_prepare(1, 0, 0x11))
        .await;

    assert!(
        partition.repair.is_none(),
        "the prior-view session is obsolete"
    );
    assert!(
        partition.log.journal().inner.header_by_op(1).is_none(),
        "a delayed prior-view body must not enter the new view's journal"
    );
}

#[compio::test]
async fn given_session_remint_when_attempts_burned_should_survive_on_partition() {
    let mut partition = test_partition();
    for round in 0..consensus::STATE_TRANSFER_MAX_STALL_RETRIES {
        assert!(!partition.burn_transfer_attempt());
        // A re-minted session must not reset the budget: it lives on the
        // partition precisely because arming sites mint fresh sessions.
        partition.transfer = Some(crate::state_transfer::PartitionTransferSession {
            nonce: u128::from(round),
            peer: 0,
            commit_op: 0,
            artifacts: Vec::new(),
            target_accepted: false,
            idle_ticks: 0,
            spill: None,
            scan: None,
            seed: None,
            pulled_len: 0,
        });
    }
    assert!(partition.burn_transfer_attempt(), "budget exhausts");
    partition.note_transfer_progress();
    assert!(!partition.burn_transfer_attempt(), "progress resets it");
}

#[compio::test]
async fn given_repeated_failures_when_only_generation_advances_should_keep_counting() {
    let mut partition = test_partition();
    // A committing primary advances its generation every round; the
    // consecutive count must keep growing regardless, or a
    // deterministic local failure retries at network round-trip rate
    // forever. Only a completed install resets it.
    assert_eq!(partition.record_transfer_failure(), 1);
    assert_eq!(partition.record_transfer_failure(), 2);
    partition.note_transfer_progress();
    assert_eq!(
        partition.record_transfer_failure(),
        3,
        "received chunks are not install progress"
    );
    partition.note_transfer_installed();
    assert_eq!(partition.record_transfer_failure(), 1, "install resets");
}

//     #[compio::test]
//     async fn given_local_ops_below_repair_floor_when_completing_should_apply_them_and_cancel_transfer()
//      {
//         let mut partition = test_partition();
//         for op in 1..=3 {
//             journal_send_batch(&mut partition, op).await;
//         }
//         partition.consensus().advance_commit_max(3);
//         partition.repair = Some(armed_session(3, 2));
//         partition.transfer_rearm = Some(crate::state_transfer::PendingTransferRearm {
//             peer: 0,
//             after_ticks: 1,
//         });
//         assert_eq!(partition.recovered_durable_offset, None);
//
//         let conclusion = partition.complete_repair(&repair_config()).await;
//
//         assert_eq!(conclusion, RepairConclusion::Done);
//         assert_eq!(partition.consensus().commit_min(), 3);
//         assert!(partition.repair.is_none());
//         assert!(partition.transfer_rearm.is_none());
//     }
//
//     #[compio::test]
//     async fn given_no_repaired_batch_when_window_never_arrived_should_refuse_commit_floor() {
//         let mut partition = test_partition();
//         partition.consensus().advance_commit_max(8);
//         partition.repair = Some(armed_session(8, 5));
//
//         let conclusion = partition.complete_repair(&repair_config()).await;
//
//         assert_eq!(
//             conclusion,
//             RepairConclusion::InProgress,
//             "an incomplete window is not a definitive refusal"
//         );
//         assert_eq!(partition.consensus().commit_min(), 0);
//         assert!(
//             partition.repair.is_some(),
//             "session must stay armed for retry"
//         );
//     }

/// A backup whose prepare WAL holds `1..=wal_head`, durable and committed,
/// with nothing resident above it: where a rejoining backup stands after
/// its replayed history was walked and flushed, or after an install.
async fn backup_with_durable_wal(
    directory: &std::path::Path,
    wal_head: u64,
) -> IggyPartition<RecordingBus> {
    let (mut backup, _) = recording_partition_at(1, 3);
    backup.set_partition_dir(directory.to_string_lossy().into_owned());
    backup.open_persistence().await.unwrap();
    let persistence = Rc::clone(backup.persistence.as_ref().unwrap());
    let mut parent = 0;
    for op in 1..=wal_head {
        let prepare = checksummed_segment_prepare(op, parent, op - 1, b"durable");
        parent = prepare.header().checksum;
        persistence.append(prepare.into_frozen(), true).unwrap();
    }
    assert!(persistence.start());
    Rc::clone(&persistence).run().await;
    assert!(persistence.is_durable_through(wal_head));
    backup.consensus().restore_commit_state(wal_head, wal_head);
    backup.consensus().sequencer().set_sequence(wal_head);
    backup.recovered_durable_offset = Some(wal_head - 1);
    backup
}

#[compio::test]
async fn given_durable_unproven_history_when_resuming_io_should_not_select_or_apply_it() {
    const HEAD: u64 = 3;
    let directory = tempfile::tempdir().unwrap();
    let config = repair_config();
    let (mut backup, _) = recording_partition_at(1, 3);
    backup.set_partition_dir(directory.path().to_string_lossy().into_owned());
    backup.open_persistence().await.unwrap();
    let persistence = Rc::clone(backup.persistence.as_ref().unwrap());
    let mut parent = 0;
    let mut headers = Vec::new();
    for op in 1..=HEAD {
        let prepare = checksummed_segment_prepare(op, parent, op - 1, b"old-view");
        parent = prepare.header().checksum;
        headers.push(*prepare.header());
        backup
            .log
            .journal()
            .inner
            .append(prepare.clone().into_frozen())
            .await
            .unwrap();
        persistence.append(prepare.into_frozen(), true).unwrap();
    }
    assert!(persistence.start());
    Rc::clone(&persistence).run().await;
    assert!(persistence.is_durable_through(HEAD));
    backup.consensus().advance_commit_max(HEAD);
    backup.consensus().fence_view_log(HEAD + 1, HEAD + 1);

    assert!(!backup.select_commit(headers.clone(), &config, false));
    assert_eq!(
        backup.collect_committable_from_journal(COMMIT_WALK_OPS_MAX, &config),
        []
    );
    for _ in 0..HEAD {
        backup.resume_io(&config).await;
        assert_eq!(backup.consensus().commit_min(), 0);
        assert!(!backup.has_pending_commit());
    }

    backup.consensus().settle_view_log_proof();
    assert_eq!(
        backup.collect_committable_from_journal(COMMIT_WALK_OPS_MAX, &config),
        headers
    );
    backup.commit_journal(&config).await;
    assert_eq!(backup.consensus().commit_min(), HEAD);
}
//
//     #[compio::test]
//     async fn given_unproven_history_when_repair_floor_connects_should_require_state_transfer() {
//         const FLOOR: u64 = 2;
//         const HEAD: u64 = 3;
//         let (mut backup, _) = recording_partition_at(1, 3);
//         backup.consensus().advance_commit_max(HEAD);
//         backup.repair = Some(armed_session(HEAD, FLOOR));
//         backup
//             .apply_repaired_prepare(checksummed_segment_prepare(HEAD, 0, 0, b"connected"))
//             .await;
//         backup.consensus().fence_view_log(FLOOR, FLOOR);
//
//         assert_eq!(
//             backup.complete_repair(&repair_config()).await,
//             RepairConclusion::FloorRefused {
//                 floor: FLOOR,
//                 to_op: HEAD
//             }
//         );
//         assert_eq!(backup.consensus().commit_min(), 0);
//         assert!(backup.consensus().view_log_unproven());
//         assert!(backup.repair.is_none());
//     }

/// The repair walk of an unproven log runs from the adopted head down and
/// commits nothing before it reaches `commit_min`. A walk within the WAL
/// byte budget keeps repairing. Once it holds more, it journals nothing
/// more and hands the window to state transfer.
#[compio::test]
async fn given_an_unproven_walk_past_the_wal_byte_budget_when_completing_should_require_state_transfer()
 {
    const PAYLOAD_BYTES: usize = 4 * 1024 * 1024;
    let budget = journal::partition_journal::PARTITION_WAL_CAPACITY_MIN;
    let payload = vec![7_u8; PAYLOAD_BYTES];
    let prepare_bytes = u64::try_from(
        checksummed_segment_prepare(1, 0, 0, &payload)
            .as_slice()
            .len(),
    )
    .unwrap();
    let journaled = budget / prepare_bytes + 1;
    let head = journaled + 2;
    let parents = segment_chain_parents(head, &payload);
    let prepare = |op: u64| {
        let parent = parents[usize::try_from(op - 1).unwrap()];
        checksummed_segment_prepare(op, parent, op - 1, &payload)
    };
    let root = tempfile::tempdir().unwrap();
    let (mut backup, _) = recording_partition_at(1, 3);
    backup.set_partition_dir(root.path().to_string_lossy().into_owned());
    backup
        .open_persistence_with_capacity(budget, std::time::Duration::ZERO)
        .await
        .unwrap();
    backup.consensus().advance_commit_max(head);
    backup.consensus().sequencer().set_sequence(head);
    backup.consensus().fence_view_log(head, head);
    backup.repair = Some(floorless_session(head, head));

    backup.apply_repaired_prepare(prepare(head)).await;
    assert_eq!(
        backup.complete_repair(&repair_config()).await,
        RepairConclusion::InProgress,
        "a walk within the budget keeps repairing"
    );
    for op in (1..head).rev() {
        backup.apply_repaired_prepare(prepare(op)).await;
    }

    let journal = &backup.log.journal().inner;
    let lowest_journaled = head - journaled + 1;
    assert!(
        journal.header_by_op(lowest_journaled).is_some(),
        "the body that crosses the budget is journaled"
    );
    assert!(
        journal.header_by_op(lowest_journaled - 1).is_none(),
        "the walk journals nothing past the budget"
    );
    assert_eq!(
        backup.complete_repair(&repair_config()).await,
        RepairConclusion::FloorRefused {
            floor: 0,
            to_op: head
        }
    );
    assert_eq!(backup.consensus().commit_min(), 0);
    assert!(backup.repair.is_none());
}

#[compio::test]
async fn given_walks_sharing_a_node_budget_when_it_is_full_should_transfer_and_release_discarded_bodies()
 {
    const HEAD: u64 = 3;
    const PAYLOAD: &[u8] = b"repair body";
    let parents = segment_chain_parents(HEAD, PAYLOAD);
    let prepare = |op: u64| {
        checksummed_segment_prepare(
            op,
            parents[usize::try_from(op - 1).unwrap()],
            op - 1,
            PAYLOAD,
        )
    };
    let capacity = prepare(HEAD).retained_allocation_size().unwrap();
    let budget = Arc::new(crate::RepairMemoryBudget::new(capacity));
    let backup = || {
        let (mut partition, _) = recording_partition_at(1, 3);
        partition.proof_walk_memory = Some(budget.reservation());
        partition.consensus().advance_commit_max(HEAD);
        partition.consensus().sequencer().set_sequence(HEAD);
        partition.consensus().fence_view_log(HEAD, HEAD);
        partition.repair = Some(floorless_session(HEAD, HEAD));
        partition
    };
    let mut first = backup();
    let mut second = backup();
    first.apply_repaired_prepare(prepare(HEAD)).await;
    assert!(first.log.journal().inner.header_by_op(HEAD).is_some());
    assert!(
        !budget.reservation().try_add(1),
        "the first walk owns the available allocation"
    );

    second.apply_repaired_prepare(prepare(HEAD)).await;
    assert!(
        second.log.journal().inner.is_empty(),
        "an over-budget body must not be journaled"
    );
    assert_eq!(
        second.complete_repair(&repair_config()).await,
        RepairConclusion::FloorRefused {
            floor: 0,
            to_op: HEAD
        },
        "memory pressure must hand this walk to state transfer"
    );

    first.truncate_uncommitted_from(1).await.unwrap();
    assert!(
        budget.reservation().try_add(capacity),
        "discarding the window releases its reservation"
    );

    let mut third = backup();
    third.apply_repaired_prepare(prepare(HEAD)).await;
    assert!(third.log.journal().inner.header_by_op(HEAD).is_some());
    drop(third);
    assert!(
        budget.reservation().try_add(capacity),
        "dropping a partition releases its reservation"
    );
}

#[compio::test]
async fn given_a_primary_elect_when_repair_memory_is_exhausted_should_request_transfer() {
    const VIEW: u32 = 4;
    const HEAD: u64 = 3;
    let (mut partition, _) = recording_partition_at(1, 3);
    let prepare = checksummed_segment_prepare(HEAD, 0, HEAD - 1, b"repair");
    let body = &prepare.as_slice()[..size_of::<PrepareHeader>()];
    let size = size_of::<iggy_binary_protocol::DoViewChangeHeader>() + body.len();
    let mut dvc = Message::<iggy_binary_protocol::GenericHeader>::new(size);
    dvc.as_mut_slice()[size_of::<iggy_binary_protocol::DoViewChangeHeader>()..]
        .copy_from_slice(body);
    let dvc = dvc.transmute_header(|_, header: &mut iggy_binary_protocol::DoViewChangeHeader| {
        header.command = Command::DoViewChange;
        header.size = u32::try_from(size).unwrap();
        header.group = partition.consensus.group();
        header.cluster = TEST_CLUSTER;
        header.view = VIEW;
        header.log_view = 1;
        header.replica = 0;
        header.op = HEAD;
        header.commit = HEAD;
        header.present_bitset = 1;
    });
    partition
        .consensus
        .handle_do_view_change(consensus::PlaneKind::Partitions, dvc.header(), body);
    assert!(partition.consensus.is_primary_elect());
    partition.consensus.fence_view_log(HEAD, HEAD);
    partition.repair = Some(RepairSession {
        view: VIEW,
        ..floorless_session(HEAD, HEAD)
    });
    let budget = Arc::new(crate::RepairMemoryBudget::new(1));
    partition.proof_walk_memory = Some(budget.reservation());
    partition.apply_repaired_prepare(prepare).await;
    assert!(partition.log.journal().inner.is_empty());
    assert_eq!(
        partition.complete_repair(&repair_config()).await,
        RepairConclusion::FloorRefused {
            floor: 0,
            to_op: HEAD
        },
        "primary election must not discard the memory-pressure signal"
    );
}

/// The serving peer evicted every op below `RETAINED_FROM` and retains more
/// than one repair chunk above it. Every request that starts below
/// `RETAINED_FROM` gets `RangeEvicted` and the same first chunk, so the
/// refused window can never complete. With a prepare WAL attached, a floor
/// above the WAL head can never be accepted, so the first refusal is final.
#[compio::test]
async fn given_floor_above_wal_head_when_refused_window_exceeds_one_chunk_should_refuse_commit_floor()
 {
    // The default `[cluster] repair_chunk_max`, `shard::REPAIR_CHUNK_MAX`.
    const CHUNK_OPS: u64 = 128;
    const WAL_HEAD: u64 = 2;
    const RETAINED_FROM: u64 = 11;
    // One op more than the chunk served from `RETAINED_FROM` covers.
    const COMMIT_TO_OP: u64 = RETAINED_FROM + CHUNK_OPS;
    // One round past the stall budget, so a peer rotation would be due.
    const ROUNDS: u32 = crate::types::REPAIR_MAX_STALL_RETRIES + 1;
    let directory = tempfile::tempdir().unwrap();
    let mut backup = backup_with_durable_wal(directory.path(), WAL_HEAD).await;
    backup.consensus().advance_commit_max(COMMIT_TO_OP);
    backup.repair = Some(RepairSession {
        floor: None,
        ..armed_session(COMMIT_TO_OP, 0)
    });
    let config = repair_config();

    let mut request = WAL_HEAD + 1..=COMMIT_TO_OP;
    let mut rounds = Vec::new();
    for _ in 0..ROUNDS {
        // The serve path clamps the request to its retained range, reports
        // that with `RangeEvicted`, and serves at most one chunk.
        let from_op = (*request.start()).max(RETAINED_FROM);
        if from_op > *request.start() {
            backup.repair.as_mut().unwrap().floor = Some(RETAINED_FROM - 1);
        }
        for op in from_op..=(*request.end()).min(from_op + CHUNK_OPS - 1) {
            backup
                .apply_repaired_prepare(checksummed_segment_prepare(op, 0, op - 1, b"repaired"))
                .await;
        }
        let conclusion = backup.complete_repair(&config).await;
        if conclusion != RepairConclusion::InProgress {
            rounds.push((request, conclusion, false));
            break;
        }
        // The stall retry burns one attempt, then re-requests from the live
        // commit point, where the pull after `RepairDone` starts too.
        let rotate = backup.burn_repair_attempt();
        rounds.push((request, conclusion, rotate));
        if rotate {
            break;
        }
        request = backup.consensus().commit_min() + 1..=backup.repair.unwrap().fetch_to_op;
    }

    assert_eq!(
        rounds.last().map(|(_, conclusion, _)| *conclusion),
        Some(RepairConclusion::FloorRefused {
            floor: RETAINED_FROM - 1,
            to_op: COMMIT_TO_OP,
        }),
        "a floor above the WAL head can never be accepted, so its refusal is final; \
             rounds as (request, conclusion, rotate): {rounds:?}"
    );
    assert!(backup.repair.is_none());
}

/// Can a WAL-backed partition accept a floor above its WAL head? This window
/// connects: ops 3 to 10 carried no messages, so its first batch starts right
/// after the recovered durable end. Its last frame is missing, so a refusal
/// cannot come from the window-completeness check of the refused path.
#[compio::test]
async fn given_wal_attached_when_connected_floor_above_wal_head_should_refuse_commit_floor() {
    const WAL_HEAD: u64 = 2;
    const FLOOR: u64 = 10;
    const SERVED_THROUGH: u64 = 12;
    const COMMIT_TO_OP: u64 = SERVED_THROUGH + 1;
    let directory = tempfile::tempdir().unwrap();
    let mut backup = backup_with_durable_wal(directory.path(), WAL_HEAD).await;
    backup.consensus().advance_commit_max(COMMIT_TO_OP);
    backup.repair = Some(armed_session(COMMIT_TO_OP, FLOOR));
    for (offset, op) in (WAL_HEAD..).zip(FLOOR + 1..=SERVED_THROUGH) {
        backup
            .apply_repaired_prepare(checksummed_segment_prepare(op, 0, offset, b"repaired"))
            .await;
    }

    let conclusion = backup.complete_repair(&repair_config()).await;

    assert_eq!(
        conclusion,
        RepairConclusion::FloorRefused {
            floor: FLOOR,
            to_op: COMMIT_TO_OP,
        },
        "the WAL cannot become durable through ops the peer no longer retains"
    );
    assert!(backup.repair.is_none());
    assert_eq!(
        backup.consensus().commit_min(),
        WAL_HEAD,
        "a refused floor must not lift commit_min over ops the WAL never persisted"
    );
}

/// The other half of the question: boot replay leaves every op in
/// `(commit_min, head]` resident, so a floor at or below the WAL head is
/// dropped before the connection check, and the replayed ops are walked.
/// This window can never connect (no repaired batch, message ops in it), so
/// a floor that reached the check would be refused instead.
#[compio::test]
async fn given_boot_replayed_wal_when_floor_at_or_below_wal_head_should_walk_resident_ops() {
    const WAL_HEAD: u64 = 4;
    const RECOVERED_COMMIT: u64 = 2;
    let config = repair_config();
    for floor in RECOVERED_COMMIT + 1..=WAL_HEAD {
        let directory = tempfile::tempdir().unwrap();
        let (mut backup, _) = recording_partition_at(1, 3);
        backup.set_partition_dir(directory.path().to_string_lossy().into_owned());
        backup.log.retire_front().unwrap();
        backup.install_empty_segment(&config, 0).await.unwrap();
        let mut wal = journal::PartitionPrepareJournal::open(
            &directory.path().join("prepares-0"),
            backup.consensus().group(),
            0,
        )
        .await
        .unwrap();
        let mut parent = 0;
        for op in 1..=WAL_HEAD {
            // A prepare carries the primary's commit point at send time.
            let prepare = checksummed_segment_prepare(op, parent, op - 1, b"replayed")
                .transmute_header(|original, header: &mut PrepareHeader| {
                    *header = original;
                    header.commit = RECOVERED_COMMIT.min(op - 1);
                    header.checksum = header.identity_checksum();
                });
            parent = prepare.header().checksum;
            wal.append(prepare.into_frozen()).await.unwrap();
        }
        drop(wal);
        backup.open_persistence().await.unwrap();
        backup.commit_journal(&config).await;
        assert_eq!(backup.consensus().commit_min(), RECOVERED_COMMIT);
        assert!(
            backup
                .log
                .journal()
                .inner
                .repaired_window_shape(RECOVERED_COMMIT, WAL_HEAD)
                .complete,
            "boot replay must leave (commit_min, head] resident"
        );
        backup.consensus().advance_commit_max(WAL_HEAD);
        backup.repair = Some(armed_session(WAL_HEAD, floor));

        let conclusion = backup.complete_repair(&config).await;

        assert_eq!(
            conclusion,
            RepairConclusion::Done,
            "floor {floor} at or below the WAL head must be dropped, not refused"
        );
        assert_eq!(backup.consensus().commit_min(), WAL_HEAD);
    }
}

/// The window above the floor is complete and carries no messages, so no
/// batch anchors the floor. The jump would skip ops 1 to 5, which this
/// journal never held, and nothing shows that they carried no messages,
/// whether or not this replica committed messages before.
#[compio::test]
async fn given_no_batch_above_floor_when_window_carries_no_messages_should_refuse_commit_floor() {
    const COMMITTED_OFFSET: u64 = 99;
    for committed_messages in [false, true] {
        let mut partition = test_partition();
        if committed_messages {
            partition.set_offset_space_used(true);
            partition.offset.store(COMMITTED_OFFSET, Ordering::Release);
        }
        partition.consensus().advance_commit_max(8);
        // Stands in for any op without messages, such as a consumer
        // offset store.
        for op in 6..=8 {
            journal_prepare(&partition, op, Operation::CreateStream).await;
        }
        partition.repair = Some(armed_session(8, 5));

        let conclusion = partition.complete_repair(&repair_config()).await;

        assert_eq!(
            (conclusion, partition.consensus().commit_min()),
            (RepairConclusion::FloorRefused { floor: 5, to_op: 8 }, 0),
            "committed messages before the window: {committed_messages}"
        );
        assert!(
            partition.repair.is_none(),
            "a definitive refusal hands recovery to state transfer"
        );
    }
}

/// The committed window above the floor carries no messages, and the only
/// batch above the floor belongs to the uncommitted suffix. A view change
/// can still replace that op, so its batch must not anchor the floor, even
/// at offset 0, which would connect it here.
#[compio::test]
async fn given_suffix_batch_above_commit_to_op_when_committed_window_carries_no_messages_should_refuse_commit_floor()
 {
    const FLOOR: u64 = 5;
    const COMMIT_TO_OP: u64 = 8;
    const SUFFIX_OP: u64 = COMMIT_TO_OP + 1;
    let mut partition = test_partition();
    partition.consensus().advance_commit_max(COMMIT_TO_OP);
    for op in FLOOR + 1..=COMMIT_TO_OP {
        journal_prepare(&partition, op, Operation::CreateStream).await;
    }
    partition
        .log
        .journal()
        .inner
        .append(checksummed_segment_prepare(SUFFIX_OP, 0, 0, b"suffix").into_frozen())
        .await
        .expect("journal append");
    partition.repair = Some(armed_fetch_session(COMMIT_TO_OP, SUFFIX_OP, FLOOR));

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(
        (conclusion, partition.consensus().commit_min()),
        (
            RepairConclusion::FloorRefused {
                floor: FLOOR,
                to_op: COMMIT_TO_OP
            },
            0
        ),
        "the batch at op {SUFFIX_OP} must not anchor the floor"
    );
}

#[compio::test]
async fn given_no_repaired_batch_when_window_holds_message_op_should_refuse_commit_floor() {
    let mut partition = test_partition();
    partition.consensus().advance_commit_max(8);
    journal_prepare(&partition, 6, Operation::SendMessages).await;
    for op in 7..=8 {
        journal_prepare(&partition, op, Operation::CreateStream).await;
    }
    partition.repair = Some(armed_session(8, 5));

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(
        conclusion,
        RepairConclusion::FloorRefused { floor: 5, to_op: 8 },
        "a complete window with an unanchored message op can never connect"
    );
    assert_eq!(partition.consensus().commit_min(), 0);
    assert!(
        partition.repair.is_none(),
        "a definitive refusal hands recovery to state transfer"
    );
}

#[compio::test]
async fn given_no_repaired_batch_when_window_fully_evicted_should_refuse_commit_floor() {
    let mut partition = test_partition();
    partition.consensus().advance_commit_max(8);
    partition.repair = Some(armed_session(8, 8));

    let conclusion = partition.complete_repair(&repair_config()).await;

    // Everything the peer retained was evicted: a retry re-raises the
    // identical empty window every round (the wedge state transfer
    // exists to break), so this refusal is definitive.
    assert_eq!(
        conclusion,
        RepairConclusion::FloorRefused { floor: 8, to_op: 8 }
    );
    assert_eq!(partition.consensus().commit_min(), 0);
    assert!(partition.repair.is_none());
}

#[compio::test]
async fn given_empty_committed_window_with_a_suffix_fetch_should_escape_to_state_transfer() {
    let mut partition = test_partition();
    partition.consensus().advance_commit_max(5);
    partition.repair = Some(armed_fetch_session(5, 9, 5));

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(
        conclusion,
        RepairConclusion::FloorRefused { floor: 5, to_op: 5 },
        "the uncommitted fetch ceiling must not postpone a definitive committed-floor refusal"
    );
    assert!(partition.repair.is_none());
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

#[compio::test]
async fn given_committed_suffix_evicted_when_completing_repair_should_close_the_session() {
    // A successful suffix repair is what lets the group commit past
    // `commit_to_op`, and the commit walk's flush then evicts exactly the
    // suffix headers. The completion verdict must survive that eviction:
    // judged from resident headers alone, the fully successful session
    // would report itself incomplete forever, stay armed, and block every
    // later re-arm for this partition until a view change.
    let mut partition = test_partition();
    for op in 1..=3 {
        journal_send_batch(&mut partition, op).await;
    }
    partition.consensus().advance_commit_max(3);
    partition.repair = Some(armed_fetch_session(2, 3, 0));
    partition.commit_journal(&repair_config()).await;
    partition.log.journal().inner.evict_prefix(3);

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(conclusion, RepairConclusion::Done);
    assert!(
        partition.repair.is_none(),
        "a fully committed suffix fetch must not stay armed after its \
             headers are flushed out of the resident journal"
    );
}

#[compio::test]
async fn given_suffix_fetch_when_its_view_is_discarded_should_clear_the_session() {
    let mut partition = test_partition();
    partition.repair = Some(armed_fetch_session(0, 3, 0));
    partition.consensus.set_view(1);

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(conclusion, RepairConclusion::Done);
    assert!(
        partition.repair.is_none(),
        "a discarded view must not leave its suffix fetch blocking future repair"
    );
}

#[compio::test]
async fn given_repaired_batch_above_durable_end_when_floor_arrives_should_refuse_commit_floor() {
    let mut partition = test_partition();
    partition.consensus().advance_commit_max(8);
    // No recovered segments (durable end None) and the served window's
    // first batch starts at offset 8: ops below the floor are neither
    // locally durable nor repaired.
    partition.repair = Some(armed_session(8, 5));
    partition
        .apply_repaired_prepare(repaired_send_prepare(8, 0, 0x88))
        .await;

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(
        conclusion,
        RepairConclusion::InProgress,
        "with the window incomplete, later frames can still land below \
             the anchor and connect"
    );
    assert_eq!(partition.consensus().commit_min(), 0);
    assert!(partition.repair.is_some());
}

/// The repair window `(floor, to_op]` is longer than one repair chunk and
/// only its first chunk arrived. The anchor (op 7, offset 7) starts above
/// the recovered durable end (offset 3) and every op between the floor and
/// it is resident, so the refusal is final and must hand recovery to state
/// transfer.
#[compio::test]
async fn given_a_resident_leading_edge_above_durable_end_when_the_window_is_longer_than_a_chunk_should_refuse_commit_floor()
 {
    let mut partition = test_partition();
    partition.consensus().advance_commit_max(300);
    partition.recovered_durable_offset = Some(3);
    partition.repair = Some(armed_session(300, 5));
    journal_prepare(&partition, 6, Operation::CreateStream).await;
    partition
        .apply_repaired_prepare(repaired_send_prepare(7, 0, 0x77))
        .await;
    for op in 8..=133 {
        journal_prepare(&partition, op, Operation::CreateStream).await;
    }

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(
        conclusion,
        RepairConclusion::FloorRefused {
            floor: 5,
            to_op: 300
        },
        "a resident leading edge fixes where the window starts, so the \
             refusal is final"
    );
    assert!(
        partition.repair.is_none(),
        "a definitive refusal hands recovery to state transfer"
    );
}

/// A stall rotation drops the session that repaired ops 6 and 7, and the
/// next session learns the floor. The batches are resident whichever
/// session journaled them, so they anchor the check.
#[compio::test]
async fn given_a_connecting_batch_from_an_earlier_session_when_the_floor_arrives_should_accept_it()
{
    let mut partition = test_partition();
    partition.consensus().advance_commit_max(300);
    partition.recovered_durable_offset = Some(3);
    partition.repair = Some(floorless_session(300, 300));
    partition
        .apply_repaired_prepare(repaired_send_prepare_at(6, 4, 0, 0x66))
        .await;
    partition
        .apply_repaired_prepare(repaired_send_prepare_at(7, 5, 0x66, 0x77))
        .await;
    partition.repair = None;
    partition.repair = Some(armed_session(300, 5));

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(conclusion, RepairConclusion::InProgress);
    assert_eq!(
        partition.consensus().commit_min(),
        7,
        "the window connects at offset 4, so the floor stands and the walk \
             commits the resident batches above it"
    );
    assert!(partition.repair.is_some());
}

/// The same shape as above with batches that start past the durable end:
/// resident from an earlier session or not, nothing below them is
/// missing, so the refusal is final.
#[compio::test]
async fn given_a_disconnected_batch_from_an_earlier_session_when_the_floor_arrives_should_refuse_commit_floor()
 {
    let mut partition = test_partition();
    partition.consensus().advance_commit_max(300);
    partition.recovered_durable_offset = Some(3);
    partition.repair = Some(floorless_session(300, 300));
    partition
        .apply_repaired_prepare(repaired_send_prepare(6, 0, 0x66))
        .await;
    partition
        .apply_repaired_prepare(repaired_send_prepare(7, 0x66, 0x77))
        .await;
    partition.repair = None;
    partition.repair = Some(armed_session(300, 5));

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(
        conclusion,
        RepairConclusion::FloorRefused {
            floor: 5,
            to_op: 300
        }
    );
    assert_eq!(partition.consensus().commit_min(), 0);
    assert!(partition.repair.is_none());
}

/// The anchor sits directly above the floor, so the leading edge is empty
/// and the refusal is final at once.
#[compio::test]
async fn given_the_anchor_directly_above_the_floor_when_it_does_not_connect_should_refuse_commit_floor()
 {
    let mut partition = test_partition();
    partition.consensus().advance_commit_max(300);
    partition.recovered_durable_offset = Some(3);
    partition.repair = Some(armed_session(300, 5));
    partition
        .apply_repaired_prepare(repaired_send_prepare(6, 0, 0x66))
        .await;

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(
        conclusion,
        RepairConclusion::FloorRefused {
            floor: 5,
            to_op: 300
        }
    );
    assert!(partition.repair.is_none());
}

/// A resident batch whose stamped `base_offset` no longer matches its batch
/// checksum says nothing about where the window begins, so it must not
/// anchor the floor however well the forged offset would connect.
#[compio::test]
async fn given_a_resident_batch_with_a_corrupt_base_offset_when_anchoring_should_yield_no_anchor() {
    const FLOOR: u64 = 5;
    const OP: u64 = FLOOR + 1;
    let partition = test_partition();
    let mut prepare = repaired_send_prepare_at(OP, OP, 0, 0x66);
    let base_offset_at = std::mem::size_of::<PrepareHeader>() + 8;
    prepare.as_mut_slice()[base_offset_at..base_offset_at + 8].copy_from_slice(&0u64.to_le_bytes());
    partition
        .log
        .journal()
        .inner
        .append(prepare.into_frozen())
        .await
        .expect("journal append");

    assert_eq!(partition.repair_window_anchor(FLOOR, 300), None);
}

#[compio::test]
async fn given_a_resident_batch_with_an_intact_checksum_when_anchoring_should_read_its_base_offset()
{
    const FLOOR: u64 = 5;
    const OP: u64 = FLOOR + 1;
    let partition = test_partition();
    partition
        .log
        .journal()
        .inner
        .append(repaired_send_prepare_at(OP, OP, 0, 0x66).into_frozen())
        .await
        .expect("journal append");

    assert_eq!(
        partition.repair_window_anchor(FLOOR, 300),
        Some(RepairAnchor {
            op: OP,
            base_offset: OP
        })
    );
}

/// An op below the anchor is still missing, so a frame for it may yet
/// connect the window.
#[compio::test]
async fn given_a_hole_below_the_anchor_when_the_window_is_incomplete_should_keep_repairing() {
    let mut partition = test_partition();
    partition.consensus().advance_commit_max(300);
    partition.recovered_durable_offset = Some(3);
    partition.repair = Some(armed_session(300, 5));
    journal_prepare(&partition, 6, Operation::CreateStream).await;
    partition
        .apply_repaired_prepare(repaired_send_prepare(8, 0, 0x88))
        .await;

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(conclusion, RepairConclusion::InProgress);
    assert!(partition.repair.is_some());
}

/// Round one repaired op 4 and lost op 5. Before the stall retry the peer
/// evicted through op 5, so the floor lands at 5 and the retry serves only
/// ops 6 to 10. Op 4 connects to the durable end, but the floor claims
/// recovered state stands in for op 5, which this replica holds nowhere.
/// Accepting that floor commits and serves a log missing offset 5.
#[compio::test]
async fn given_a_connecting_batch_below_a_raised_floor_when_the_floor_arrives_should_refuse_it() {
    let mut partition = test_partition();
    partition.consensus().advance_commit_max(10);
    partition.recovered_durable_offset = Some(3);
    partition.repair = Some(armed_session(10, 5));
    partition
        .apply_repaired_prepare(repaired_send_prepare(4, 0, 0x44))
        .await;
    for op in 6..=10u64 {
        partition
            .apply_repaired_prepare(repaired_send_prepare(op, 0, 0x100 + u128::from(op)))
            .await;
    }

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(
        conclusion,
        RepairConclusion::FloorRefused {
            floor: 5,
            to_op: 10
        },
        "a batch at or below the floor must not anchor the connect check"
    );
    assert!(
        partition.consensus().commit_min() < 5,
        "the floor must not skip op 5"
    );
}

/// The first stream repaired op 4 and the walk committed through it. The
/// stall retry then learns a floor at 6, above the still-missing op 5.
/// The committed op 4 must not stand in for the window above the floor.
#[compio::test]
async fn given_a_committed_batch_below_the_floor_when_the_floor_arrives_should_keep_repairing() {
    let mut partition = test_partition();
    partition.consensus().advance_commit_max(300);
    partition.recovered_durable_offset = Some(3);
    partition.repair = Some(armed_session(300, 6));
    for op in 1..=3 {
        journal_prepare(&partition, op, Operation::CreateStream).await;
    }
    partition
        .apply_repaired_prepare(repaired_send_prepare(4, 0, 0x44))
        .await;
    partition.commit_journal(&repair_config()).await;
    assert_eq!(partition.consensus().commit_min(), 4);

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(conclusion, RepairConclusion::InProgress);
    assert_eq!(
        partition.consensus().commit_min(),
        4,
        "the floor must not skip op 5"
    );
    assert!(partition.repair.is_some());
}

/// Ops up to the floor were resident but uncommitted, so the walk inside
/// the refusal passes the floor. With lazy flush their headers stay
/// resident and the leading edge reads as complete, but a floor below
/// `commit_min` moves nothing and is no reason for a state transfer.
#[compio::test]
async fn given_lazy_flush_when_the_walk_passes_a_disconnected_floor_should_keep_repairing() {
    let mut partition = test_partition();
    partition.consensus().advance_commit_max(300);
    partition.recovered_durable_offset = Some(3);
    partition.repair = Some(armed_session(300, 5));
    for op in 1..=6 {
        journal_prepare(&partition, op, Operation::CreateStream).await;
    }
    partition
        .apply_repaired_prepare(repaired_send_prepare(7, 0, 0x77))
        .await;
    let lazy = PartitionsConfig {
        messages_required_to_save: 10_000,
        size_of_messages_required_to_save: IggyByteSize::from(512 * 1024 * 1024),
        ..repair_config()
    };

    let conclusion = partition.complete_repair(&lazy).await;

    assert_eq!(conclusion, RepairConclusion::InProgress);
    assert_eq!(partition.consensus().commit_min(), 7);
    assert!(
        partition.log.journal().inner.holds_op(6),
        "lazy flush keeps the committed headers resident"
    );
    assert!(partition.repair.is_some());
}

/// The resident run past the floor is longer than one capped commit walk.
/// The floor is moot from the start, because this journal holds every op
/// up to it. One walk stops below the floor, and a floor judged there
/// reads as disconnected and spends a state transfer on ops already held.
#[compio::test]
async fn given_resident_run_longer_than_one_walk_when_it_passes_a_disconnected_floor_should_keep_repairing()
 {
    const FLOOR: u64 = COMMIT_WALK_OPS_MAX as u64 + 6;
    let mut partition = test_partition();
    partition.consensus().advance_commit_max(300);
    partition.recovered_durable_offset = Some(3);
    partition.repair = Some(armed_session(300, FLOOR));
    for op in 1..=FLOOR + 1 {
        journal_prepare(&partition, op, Operation::CreateStream).await;
    }
    partition
        .apply_repaired_prepare(repaired_send_prepare(FLOOR + 2, 0, 0x77))
        .await;

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(conclusion, RepairConclusion::InProgress);
    assert_eq!(
        partition.consensus().commit_min(),
        COMMIT_WALK_OPS_MAX as u64,
        "one capped walk, the rest resumes on the next"
    );
    assert!(partition.repair.is_some());
}

/// As above, with the whole repaired window resident, which would make a
/// refusal final. A resident run that reaches the floor drops the floor
/// before any capped walk, so the walk cap cannot turn it into a refusal.
#[compio::test]
async fn given_resident_run_longer_than_one_walk_when_the_window_is_complete_should_keep_repairing()
{
    const FLOOR: u64 = COMMIT_WALK_OPS_MAX as u64 + 6;
    let mut partition = test_partition();
    partition.consensus().advance_commit_max(FLOOR + 2);
    partition.recovered_durable_offset = Some(3);
    partition.repair = Some(armed_session(FLOOR + 2, FLOOR));
    for op in 1..=FLOOR {
        journal_prepare(&partition, op, Operation::CreateStream).await;
    }
    for op in FLOOR + 1..=FLOOR + 2 {
        partition
            .apply_repaired_prepare(repaired_send_prepare(op, 0, u128::from(op)))
            .await;
    }

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(conclusion, RepairConclusion::InProgress);
    assert!(partition.repair.is_some(), "no state transfer for held ops");
}

/// Repair frames arrive out of order. Op 7 connects at offset 4, op 9 does
/// not, and the window is judged by the lower one whichever lands first.
#[compio::test]
async fn given_batches_applied_out_of_order_when_the_floor_arrives_should_anchor_on_the_lowest() {
    for ops in [[9, 7], [7, 9]] {
        let mut partition = test_partition();
        partition.consensus().advance_commit_max(300);
        partition.recovered_durable_offset = Some(3);
        partition.repair = Some(armed_session(300, 5));
        for op in ops {
            let base_offset = if op == 7 { 4 } else { 6 };
            partition
                .apply_repaired_prepare(repaired_send_prepare_at(
                    op,
                    base_offset,
                    0,
                    u128::from(op),
                ))
                .await;
        }

        let conclusion = partition.complete_repair(&repair_config()).await;

        assert_eq!(conclusion, RepairConclusion::InProgress, "order {ops:?}");
        assert_eq!(
            partition.consensus().commit_min(),
            5,
            "order {ops:?}: the lowest batch connects, so the floor stands"
        );
    }
}

/// A floor holds the walk and the first served chunk carries no message,
/// so nothing anchors the window yet. Pulling again from `commit_min + 1`
/// re-serves that same chunk forever; the pull must start past it.
#[compio::test]
async fn given_a_floor_holding_the_walk_when_the_first_chunk_has_no_batch_should_pull_past_it() {
    const FLOOR: u64 = 5;
    // One full shard repair chunk above the floor.
    const FIRST_CHUNK_LAST: u64 = FLOOR + 128;
    const TO_OP: u64 = FIRST_CHUNK_LAST + 10;
    const SEND_OP: u64 = FIRST_CHUNK_LAST + 7;
    let mut partition = test_partition();
    partition.consensus().advance_commit_max(TO_OP);
    partition.recovered_durable_offset = Some(3);
    partition.repair = Some(armed_session(TO_OP, FLOOR));
    for op in FLOOR + 1..=FIRST_CHUNK_LAST {
        journal_prepare(&partition, op, Operation::CreateStream).await;
    }

    for _ in 0..2 {
        assert_eq!(
            partition.complete_repair(&repair_config()).await,
            RepairConclusion::InProgress
        );
        assert_eq!(partition.consensus().commit_min(), 0);
    }
    assert_eq!(partition.repair_pull_from(), Some(FIRST_CHUNK_LAST + 1));
    assert_eq!(partition.next_floor_pull(), Some(FIRST_CHUNK_LAST + 1));
    assert_eq!(
        partition.next_floor_pull(),
        None,
        "nothing new is resident, so the next pull waits for the stall retry"
    );

    for op in FIRST_CHUNK_LAST + 1..=TO_OP {
        if op == SEND_OP {
            partition
                .apply_repaired_prepare(repaired_send_prepare_at(op, 200, 0, u128::from(op)))
                .await;
        } else {
            journal_prepare(&partition, op, Operation::CreateStream).await;
        }
    }

    assert_eq!(
        partition.complete_repair(&repair_config()).await,
        RepairConclusion::FloorRefused {
            floor: FLOOR,
            to_op: TO_OP
        }
    );
}

/// A backup without a WAL that repairs `(0, to_op]` and has walked ops
/// `1..=walked_to`, each with one message, op `n` at offset `n - 1`.
async fn walless_backup_walked_to(walked_to: u64, to_op: u64) -> IggyPartition<RecordingBus> {
    let (mut backup, _) = recording_partition_at(1, 3);
    backup.consensus().advance_commit_max(to_op);
    backup.repair = Some(RepairSession {
        floor: None,
        ..armed_session(to_op, 0)
    });
    serve_batches(&mut backup, 1..=walked_to, 0).await;
    assert_eq!(
        backup.complete_repair(&repair_config()).await,
        RepairConclusion::InProgress
    );
    assert_eq!(backup.consensus().commit_min(), walked_to);
    backup
}

/// Serves `ops` as one-message batches at consecutive offsets from
/// `first_offset`.
async fn serve_batches(
    backup: &mut IggyPartition<RecordingBus>,
    ops: std::ops::RangeInclusive<u64>,
    first_offset: u64,
) {
    let parents = segment_chain_parents(*ops.end(), b"repaired");
    for (op, offset) in ops.zip(first_offset..) {
        let parent = parents[usize::try_from(op - 1).unwrap()];
        backup
            .apply_repaired_prepare(checksummed_segment_prepare(op, parent, offset, b"repaired"))
            .await;
    }
}

/// The peer's ring slid past the ops after the walked prefix. The batches
/// walked before the floor rose say nothing about the ops between them
/// and the floor.
#[compio::test]
async fn given_batches_walked_before_floor_rise_when_window_completes_should_refuse_commit_floor() {
    const WALKED_TO: u64 = 4;
    const FLOOR: u64 = 8;
    const TO_OP: u64 = 12;
    let mut backup = walless_backup_walked_to(WALKED_TO, TO_OP).await;

    backup.note_repair_floor(FLOOR);
    serve_batches(&mut backup, FLOOR + 1..=TO_OP, FLOOR).await;
    let conclusion = backup.complete_repair(&repair_config()).await;

    assert_eq!(
        backup.consensus().commit_min(),
        WALKED_TO,
        "the floor must not lift commit_min over ops {}..={FLOOR}, which carried messages",
        WALKED_TO + 1
    );
    assert_eq!(
        conclusion,
        RepairConclusion::FloorRefused {
            floor: FLOOR,
            to_op: TO_OP
        }
    );
}

/// A batch at or below the floor never anchors it, even one that starts
/// at the stand-in bound, because the jump skips it. Both edges of the
/// skipped window: the op after the walked prefix, and the floor itself.
#[compio::test]
async fn given_batch_at_or_below_floor_at_stand_in_bound_when_window_completes_should_refuse_commit_floor()
 {
    const WALKED_TO: u64 = 4;
    const FLOOR: u64 = 8;
    const TO_OP: u64 = 12;
    for skipped_op in [WALKED_TO + 1, FLOOR] {
        let mut backup = walless_backup_walked_to(WALKED_TO, TO_OP).await;
        backup.recovered_durable_offset = Some(WALKED_TO - 1);

        backup.note_repair_floor(FLOOR);
        serve_batches(&mut backup, skipped_op..=skipped_op, WALKED_TO).await;
        serve_batches(&mut backup, FLOOR + 1..=TO_OP, WALKED_TO + 1).await;
        let conclusion = backup.complete_repair(&repair_config()).await;

        assert_eq!(
            conclusion,
            RepairConclusion::FloorRefused {
                floor: FLOOR,
                to_op: TO_OP
            },
            "the batch at op {skipped_op} must not anchor the floor"
        );
        assert!(
            backup.consensus().commit_min() < FLOOR,
            "the floor must not lift commit_min over the batch at op {skipped_op}"
        );
    }
}

/// Ops between the walked prefix and the floor carried no messages, so
/// the first batch above the floor starts at the stand-in bound, one past
/// the recovered durable end.
#[compio::test]
async fn given_batch_above_floor_at_stand_in_bound_when_window_completes_should_accept_commit_floor()
 {
    const WALKED_TO: u64 = 4;
    const FLOOR: u64 = 8;
    const TO_OP: u64 = 12;
    let mut backup = walless_backup_walked_to(WALKED_TO, TO_OP).await;
    backup.recovered_durable_offset = Some(WALKED_TO - 1);

    backup.note_repair_floor(FLOOR);
    serve_batches(&mut backup, FLOOR + 1..=TO_OP, WALKED_TO).await;
    let conclusion = backup.complete_repair(&repair_config()).await;

    assert_eq!(
        conclusion,
        RepairConclusion::Done,
        "a first batch above the floor at the stand-in bound connects the window"
    );
    assert_eq!(backup.consensus().commit_min(), TO_OP);
}

/// The batches above the floor were journaled before the floor settled: in
/// a round without a floor, under a lower floor before the peer's ring
/// slid, or under the same floor in a round whose `RepairDone` was lost.
/// The peer re-serves them, the journal holds them, and they still anchor
/// the floor. Ops 5 to 8 carried no messages, so the first batch above the
/// floor starts at the stand-in bound.
#[compio::test]
async fn given_batches_journaled_before_floor_settled_when_window_completes_should_accept_commit_floor()
 {
    const WALKED_TO: u64 = 4;
    const LOWER_FLOOR: u64 = 6;
    const FLOOR: u64 = 8;
    const TO_OP: u64 = 12;
    for earlier_floor in [None, Some(LOWER_FLOOR), Some(FLOOR)] {
        let mut backup = walless_backup_walked_to(WALKED_TO, TO_OP).await;
        backup.recovered_durable_offset = Some(WALKED_TO - 1);
        if let Some(floor) = earlier_floor {
            backup.note_repair_floor(floor);
        }
        serve_batches(&mut backup, FLOOR + 1..=TO_OP, WALKED_TO).await;

        backup.note_repair_floor(FLOOR);
        serve_batches(&mut backup, FLOOR + 1..=TO_OP, WALKED_TO).await;
        let conclusion = backup.complete_repair(&repair_config()).await;

        assert_eq!(
            (conclusion, backup.consensus().commit_min()),
            (RepairConclusion::Done, TO_OP),
            "batches journaled under the earlier floor {earlier_floor:?} must anchor the floor"
        );
    }
}

/// A second floor in one session rises over ops whose messages never
/// arrived. The batches that anchored the first floor are walked and sit
/// below the second one, so they prove nothing about it.
#[compio::test]
async fn given_second_floor_above_walked_batches_when_window_completes_should_refuse_commit_floor()
{
    const WALKED_TO: u64 = 4;
    const FIRST_FLOOR: u64 = 6;
    const FIRST_TO: u64 = 8;
    const FLOOR: u64 = 12;
    const TO_OP: u64 = 16;
    // Ops 9 to 12 carried offsets 6 to 9, which never arrive here.
    const FIRST_OFFSET_ABOVE_FLOOR: u64 = 10;
    let mut backup = walless_backup_walked_to(WALKED_TO, TO_OP).await;
    backup.recovered_durable_offset = Some(WALKED_TO - 1);
    backup.note_repair_floor(FIRST_FLOOR);
    serve_batches(&mut backup, FIRST_FLOOR + 1..=FIRST_TO, WALKED_TO).await;
    assert_eq!(
        backup.complete_repair(&repair_config()).await,
        RepairConclusion::InProgress
    );
    assert_eq!(backup.consensus().commit_min(), FIRST_TO);

    backup.note_repair_floor(FLOOR);
    serve_batches(&mut backup, FLOOR + 1..=TO_OP, FIRST_OFFSET_ABOVE_FLOOR).await;
    let conclusion = backup.complete_repair(&repair_config()).await;

    assert_eq!(
        (conclusion, backup.consensus().commit_min()),
        (
            RepairConclusion::FloorRefused {
                floor: FLOOR,
                to_op: TO_OP
            },
            FIRST_TO
        ),
        "the second floor must not lift commit_min over ops {}..={FLOOR}",
        FIRST_TO + 1
    );
}

/// A `RangeEvicted` floor arrives above `commit_min`, and the walk passes
/// it before `RepairDone` lands: ops resident just above the commit point
/// committed and their headers were evicted. Verifying that moot floor
/// refused every round (evicted headers never complete the window) while
/// the walk inside the refusal ran `commit_min` to the fetch ceiling, and
/// the reply handler asked the peer for `fetch_to_op + 1 ..= fetch_to_op`.
#[compio::test]
async fn given_floor_below_commit_min_when_completing_repair_should_ignore_the_floor() {
    let mut partition = test_partition();
    partition.consensus().restore_commit_state(7, 8);
    // Disconnected on its face: no batch above the floor ties it to the
    // boot-recovered segments. Meaningless below commit_min.
    partition.recovered_durable_offset = Some(10);
    journal_prepare(&partition, 8, Operation::CreateStream).await;
    partition.repair = Some(armed_session(8, 5));

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(conclusion, RepairConclusion::Done);
    assert_eq!(partition.consensus().commit_min(), 8);
    assert!(
        partition.repair.is_none(),
        "nothing is left to fetch, so the session must close instead of \
             re-requesting past its ceiling"
    );
}

#[compio::test]
async fn given_floor_clamped_to_commit_min_when_completing_repair_should_still_refuse() {
    let mut partition = test_partition();
    partition.consensus().restore_commit_state(5, 5);
    // The peer retains nothing below op 10, so it cannot serve the suffix
    // either. Only the raw floor tells this apart from a moot one.
    partition.repair = Some(armed_fetch_session(5, 9, 9));

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(
        conclusion,
        RepairConclusion::FloorRefused { floor: 5, to_op: 5 },
        "a floor the clamp pulls down to commit_min still names an evicted \
             range and must escape to state transfer"
    );
    assert!(partition.repair.is_none());
}

#[compio::test]
async fn given_moot_floor_with_unfetched_suffix_when_completing_repair_should_keep_repairing() {
    let mut partition = test_partition();
    partition.consensus().restore_commit_state(5, 5);
    // The peer retains from op 6, everything this replica still needs, so
    // the suffix fetch must go on. Escaping to state transfer here copied
    // segments for a window the peer can serve.
    partition.repair = Some(armed_fetch_session(5, 9, 5));

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(conclusion, RepairConclusion::InProgress);
    let session = partition.repair.expect("the suffix fetch stays armed");
    assert!(
        partition.consensus().commit_min() < session.fetch_to_op,
        "the stall retry must still have a range to ask for"
    );
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

/// An offer whose frontier sits below this replica's own offset counter is
/// refused: installing it would rewind the counter, and the next replicated
/// prepare is re-stamped from it, so this replica would persist different
/// bytes (and a different `batch_checksum`) than the rest of the group.
#[compio::test]
async fn given_offer_below_local_counter_when_installed_should_refuse_rewind() {
    let partition_dir = transfer_fence_dir("rewind-refused").await;
    let mut partition = test_partition();
    partition.set_partition_dir(partition_dir.clone());
    partition.set_offset_space_used(true);
    partition.offset.store(99, Ordering::Release);
    // Committed and resident: the threshold-gated flush leaves exactly this
    // shape, and the fence has to count it as data.
    {
        let info = &mut partition.log.journal_mut().info;
        info.messages_count = 100;
        info.current_offset = 99;
    }

    let behind = crate::state_transfer::ConsumerOffsetsWire {
        owner_retirement: None,
        owners: Vec::new(),
        history_transition: None,
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        required_metadata_frontier: 0,
        prepare_checksum: None,
        checkpoint_prepare: Vec::new(),
        next_offset: 50,
        emptied_at: 0,
        consumers: Vec::new(),
        groups: Vec::new(),
        external_groups: Vec::new(),
        dedup: Vec::new(),
        retirement_paid_through: 0,
    };
    let refused = partition
        .install_state_transfer(&repair_config(), 12, Vec::new(), &behind.encode())
        .await;
    assert!(
        matches!(
            refused,
            Err(
                crate::state_transfer::PartitionInstallError::OfferRewindsDurableData {
                    offer_next_offset: 50,
                    local_next_offset: 100,
                }
            )
        ),
        "expected a rewind refusal, got {refused:?}"
    );

    let _ = std::fs::remove_dir_all(&partition_dir);
}

/// A chain installed EMPTY at frontier N holds no sized segment and no
/// journal entry, so a fence reading only held bytes reads 0 and skips
/// itself, letting a stale offer rewind the counter under offsets this
/// replica already claimed. The committed frontier is what carries N.
#[compio::test]
async fn given_an_empty_chain_installed_at_a_frontier_when_a_stale_offer_arrives_should_refuse() {
    let partition_dir = transfer_fence_dir("empty-install-rewind").await;
    let mut partition = test_partition();
    partition.set_partition_dir(partition_dir.clone());
    // What an install of an all-GC'd origin leaves: the counter at the group
    // frontier, nothing on disk, nothing resident.
    partition.set_offset_space_used(true);
    partition.offset.store(4_095, Ordering::Release);
    partition.dirty_offset.store(4_095, Ordering::Relaxed);
    assert_eq!(
        partition.held_offset_frontier(),
        4_096,
        "the committed arm has to carry a frontier no byte on disk names"
    );

    let stale = crate::state_transfer::ConsumerOffsetsWire {
        owner_retirement: None,
        owners: Vec::new(),
        history_transition: None,
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        required_metadata_frontier: 0,
        prepare_checksum: None,
        checkpoint_prepare: Vec::new(),
        next_offset: 1_000,
        emptied_at: 0,
        consumers: Vec::new(),
        groups: Vec::new(),
        external_groups: Vec::new(),
        dedup: Vec::new(),
        retirement_paid_through: 0,
    };
    let refused = partition
        .install_state_transfer(&repair_config(), 12, Vec::new(), &stale.encode())
        .await;
    assert!(
        matches!(
            refused,
            Err(
                crate::state_transfer::PartitionInstallError::OfferRewindsDurableData {
                    offer_next_offset: 1_000,
                    local_next_offset: 4_096,
                }
            )
        ),
        "expected a rewind refusal, got {refused:?}"
    );

    let _ = std::fs::remove_dir_all(&partition_dir);
}

/// Primary-by-index at view 0 with nothing committed refuses to serve: an
/// empty group is trivially "caught up", so this gate is the only thing
/// separating a real primary from a phantom whose directory vanished, whose
/// zero-segment offer at frontier 0 would make a data-holding receiver
/// unlink its chain.
#[compio::test]
async fn given_nothing_committed_when_offer_requested_should_refuse() {
    let partition_dir = transfer_fence_dir("nothing-committed").await;
    let mut partition = test_partition();
    partition.set_partition_dir(partition_dir.clone());
    assert_eq!(partition.consensus().commit_max(), 0);

    let refused = partition.state_transfer_offer(&repair_config()).await;
    assert!(
        matches!(
            refused,
            Err(crate::state_transfer::PartitionTransferUnavailable::NothingCommitted)
        ),
        "expected a NothingCommitted refusal, got {refused:?}"
    );
    assert!(
        refused.is_err_and(|reason| reason.transient()),
        "the refusal must be transient: the requester rotates rather than \
             charging its failure count"
    );

    let _ = std::fs::remove_dir_all(&partition_dir);
}

const MESSAGE_LESS_CLIENT_ID: u128 = 42;
const MESSAGE_LESS_GROUP_ID: u64 = 7;
const MESSAGE_LESS_EXTERNAL_GROUP_ID: u32 = 5;
const MESSAGE_LESS_EXTERNAL_OFFSET: u64 = 9;

/// A partition that never took a message still commits owner, history and
/// retry state, so a replica behind the repair window can only recover by
/// transfer. The caught-up primary serves its empty chain at frontier 0,
/// and the receiver lands on that state without claiming an offset.
#[compio::test]
async fn given_message_less_primary_when_offer_requested_should_serve_its_committed_state() {
    let (_origin_directory, mut origin, transition) = message_less_origin().await;

    let offer = origin.state_transfer_offer(&repair_config()).await.unwrap();
    assert!(offer.segments.is_empty());
    let (_receiver_directory, mut receiver) = transfer_receiver();
    let incarnation = receiver.created_revision();
    receiver
        .install_state_transfer(
            &repair_config(),
            offer.commit_op,
            Vec::new(),
            &offer.offsets.1,
        )
        .await
        .unwrap();

    assert_holds_message_less_state(&receiver, &origin, &transition);
    assert_eq!(receiver.created_revision(), incarnation);
}

/// A replica that never took a message and lags past every op its peers
/// retain cannot repair, so it catches up from the primary's offer.
#[compio::test]
async fn given_lagging_message_less_replica_when_window_fully_evicted_should_catch_up_by_transfer()
{
    let (_origin_directory, mut origin, transition) = message_less_origin().await;
    let (_receiver_directory, mut receiver) = transfer_receiver();
    receiver.consensus().advance_commit_max(3);
    // An empty serving journal reports eviction from its commit point.
    receiver.repair = Some(armed_session(3, 3));

    let conclusion = receiver.complete_repair(&repair_config()).await;
    assert_eq!(
        conclusion,
        RepairConclusion::FloorRefused { floor: 3, to_op: 3 }
    );
    let offer = origin.state_transfer_offer(&repair_config()).await.unwrap();
    receiver
        .install_state_transfer(
            &repair_config(),
            offer.commit_op,
            Vec::new(),
            &offer.offsets.1,
        )
        .await
        .unwrap();

    assert_holds_message_less_state(&receiver, &origin, &transition);
}

/// A complete window without messages says nothing about the ops below
/// its floor when the replica never took a message. Op 1 stored an
/// external offset, so jumping the floor would drop it unseen.
#[compio::test]
async fn given_lagging_message_less_replica_when_window_partly_evicted_should_catch_up_by_transfer()
{
    let (_origin_directory, mut origin, transition) = message_less_origin().await;
    let (_receiver_directory, mut receiver) = transfer_receiver();
    receiver.consensus().advance_commit_max(3);
    // The serving peer evicted op 1 and still retains ops 2 and 3.
    receiver.repair = Some(armed_session(3, 1));
    for op in [2, 3] {
        receiver
            .apply_repaired_prepare(retained_prepare(&origin, op))
            .await;
    }

    let conclusion = receiver.complete_repair(&repair_config()).await;
    assert_eq!(
        (conclusion, receiver.consensus().commit_min()),
        (RepairConclusion::FloorRefused { floor: 1, to_op: 3 }, 0)
    );
    let offer = origin.state_transfer_offer(&repair_config()).await.unwrap();
    receiver
        .install_state_transfer(
            &repair_config(),
            offer.commit_op,
            Vec::new(),
            &offer.offsets.1,
        )
        .await
        .unwrap();

    assert_holds_message_less_state(&receiver, &origin, &transition);
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

/// A prepare the partition still retains, as a peer serves it in repair.
fn retained_prepare<B: MessageBus>(
    partition: &IggyPartition<B>,
    op: u64,
) -> Message<PrepareHeader> {
    let retained = partition.log.journal().inner.repair_entry(op).unwrap();
    Message::<PrepareHeader>::try_from(Owned::copy_from_slice(retained.as_slice())).unwrap()
}

/// A backup restarts with origin op 1 replayed from its WAL but not
/// committed, and its peers retain only op 3. A floor at op 2 would skip
/// that op 1 and the op 2 nobody serves, so a replica with no committed
/// message walks op 1, refuses the floor and catches up by transfer.
#[compio::test]
async fn given_restarted_message_less_backup_when_floor_skips_unserved_op_should_refuse_and_transfer()
 {
    let (_origin_directory, mut origin, transition) = message_less_origin().await;
    for durability in [
        iggy_common::Durability::Replicated,
        iggy_common::Durability::Persisted,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let open_backup = async || {
            let (mut backup, _) = recording_partition_at(1, 3);
            backup.set_partition_dir(directory.path().to_string_lossy().into_owned());
            set_offset_dirs_under(&mut backup, directory.path());
            backup.runtime_options.durability = durability;
            backup.runtime_options.consumer_offset_durability = durability;
            backup.open_persistence().await.unwrap();
            backup
        };
        let mut backup = open_backup().await;
        backup.on_replicate(retained_prepare(&origin, 1)).await;
        // Replicated durability writes op 1 without a barrier, and recovery
        // drops what was never synced, so sync it before the crash.
        let persistence = backup.persistence.as_ref().unwrap();
        persistence.sync();
        backup.start_persistence();
        persistence
            .drain_with_timeout(compio::runtime::time::sleep)
            .await
            .unwrap();
        assert!(persistence.is_durable_through(1), "{durability:?}");
        drop(backup);

        let mut receiver = open_backup().await;
        assert_eq!(
            receiver.consensus().sequencer().current_sequence(),
            1,
            "{durability:?}"
        );
        assert!(receiver.log.journal().inner.holds_op(1), "{durability:?}");
        assert_eq!(receiver.consensus().commit_min(), 0, "{durability:?}");
        receiver.consensus().advance_commit_max(3);
        receiver.repair = Some(armed_session(3, 2));
        receiver
            .apply_repaired_prepare(retained_prepare(&origin, 3))
            .await;

        let conclusion = receiver.complete_repair(&repair_config()).await;
        assert_eq!(
            conclusion,
            RepairConclusion::FloorRefused { floor: 2, to_op: 3 },
            "{durability:?}"
        );
        assert_eq!(receiver.consensus().commit_min(), 1, "{durability:?}");
        assert_eq!(
            receiver.external_group_offset(MESSAGE_LESS_EXTERNAL_GROUP_ID),
            Some(MESSAGE_LESS_EXTERNAL_OFFSET),
            "{durability:?}"
        );
        let offer = origin.state_transfer_offer(&repair_config()).await.unwrap();
        receiver
            .install_state_transfer(
                &repair_config(),
                offer.commit_op,
                Vec::new(),
                &offer.offsets.1,
            )
            .await
            .unwrap();
        assert_holds_message_less_state(&receiver, &origin, &transition);
    }
}

fn batch_stats(base_offset: u64, message_count: u32) -> CommittedBatchStats {
    CommittedBatchStats {
        base_offset,
        message_count,
        size_bytes: 128,
    }
}

#[test]
fn given_send_messages_when_offsets_resolved_should_confirm_base_offset() {
    let namespace = IggyNamespace::new(3, 7, 5);
    let stats = batch_stats(42, 3);

    let body = send_messages_reply_body(namespace.inner(), Some(stats));
    let (response, consumed) = SendMessagesResponse::decode(&body).unwrap();

    assert_eq!(consumed, body.len());
    assert_eq!(
        response.confirmations,
        vec![SendMessagesConfirmationResponse {
            stream_id: 3,
            topic_id: 7,
            partition_id: 5,
            base_offset: 42,
        }]
    );
}

#[test]
fn given_send_messages_when_offsets_unavailable_should_reply_zero_confirmations() {
    let namespace = IggyNamespace::new(1, 1, 0);

    let body = send_messages_reply_body(namespace.inner(), None);

    assert_eq!(&body[..], &[0, 0, 0, 0]);
    let (response, _) = SendMessagesResponse::decode(&body).unwrap();
    assert_eq!(response.confirmations, []);
}

#[test]
fn given_batch_stats_when_end_offset_derived_should_span_the_message_run() {
    assert_eq!(batch_stats(9, 1).end_offset(), 9);
    assert_eq!(batch_stats(9, 4).end_offset(), 12);
}

#[test]
fn given_result_framed_operation_when_committed_should_reply_empty_result_section() {
    assert_eq!(
        &committed_reply_body(Operation::StoreConsumerOffset)[..],
        &[0, 0, 0, 0]
    );
}

#[test]
fn given_unframed_operation_when_committed_should_reply_empty_body() {
    assert!(committed_reply_body(Operation::DeleteSegments).is_empty());
}

/// Every write to this device fails with `ENOSPC`, which is how the
/// persist failure cases below inject a fault into one half of the flush
/// without any production-side plumbing.
#[cfg(target_os = "linux")]
const DEV_FULL: &str = "/dev/full";

#[cfg(target_os = "linux")]
#[compio::test]
async fn given_an_unsyncable_messages_writer_when_installing_should_refuse_the_barrier() {
    let directory = tempfile::tempdir().unwrap();
    let wal = directory.path().join("wal");
    let (persistence, _) = PartitionPersistence::open(&wal, 42, 7).await.unwrap();
    let messages_writer =
        MessagesWriter::new("/dev/null", Rc::new(AtomicU64::new(0)), false, false, None)
            .await
            .unwrap();
    let index_path = directory.path().join("segment.index");
    let index_writer = IggyIndexWriter::new(
        index_path.to_str().unwrap(),
        Rc::new(AtomicU64::new(0)),
        false,
        false,
    )
    .await
    .unwrap();
    let mut partition = test_partition();
    partition.log.add_persisted_segment(
        Segment::new(0, IggyByteSize::from(1024 * 1024_u64)),
        SegmentStorage::default(),
        Some(Rc::new(messages_writer)),
        Some(Rc::new(index_writer)),
    );
    partition.persistence = Some(Rc::clone(&persistence));

    partition.barrier_install_files_locked();
    assert!(persistence.start());
    Rc::clone(&persistence).run().await;
    assert!(persistence.failure().is_some());
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

#[compio::test]
async fn given_large_saved_appends_when_accepted_should_keep_the_checksum_of_the_written_bytes() {
    const PAYLOAD_BYTES: usize = 12 * 1024 * 1024;
    let directory = tempfile::tempdir().unwrap();
    let log_path = directory.path().join("segment.log");
    let index_path = directory.path().join("segment.index");
    let mut fixture =
        PersistFixture::with_replicas(log_path.to_str().unwrap(), index_path.to_str().unwrap(), 3)
            .await;
    fixture
        .partition
        .set_partition_dir(directory.path().to_str().unwrap().to_owned());
    let payload = vec![0x5a; PAYLOAD_BYTES];
    for offset in 0..2 {
        let job = fixture.capture(&payload, offset).unwrap();
        assert!(
            fixture.partition.segment_checksum_cache.borrow().is_empty(),
            "the in-flight append owns its hasher"
        );
        let result = job.execute().await;
        assert!(
            fixture.partition.segment_checksum_cache.borrow().is_empty(),
            "file execution cannot publish a stamp before acceptance"
        );
        fixture.partition.accept_materialization(result).unwrap();
        let written = std::fs::read(&log_path).unwrap();
        let memos = fixture.partition.segment_checksum_cache.borrow();
        assert_eq!(
            memos
                .get(&0)
                .and_then(|memo| memo.checksum_of(written.len() as u64)),
            Some(consensus::state_artifact_checksum(&written)),
            "append {offset} must hash exactly the bytes accepted on disk"
        );
    }
}

#[compio::test]
async fn reference_wal_backlog_keeps_disk_poll_seek_bounded() {
    const RECORDS: u64 = 8;
    const PAYLOAD_BYTES: usize = 512 * 1024;
    const SEGMENT_BYTES: u64 = 16 * 1024 * 1024;
    for durability in [
        iggy_common::Durability::Replicated,
        iggy_common::Durability::Persisted,
    ] {
        for (span, expected_indexes) in [
            (iggy_common::DEFAULT_SIZE_OF_MESSAGES_REQUIRED_TO_SAVE, 4),
            (
                3 * iggy_common::DEFAULT_SIZE_OF_MESSAGES_REQUIRED_TO_SAVE,
                2,
            ),
            (
                iggy_common::DEFAULT_SIZE_OF_MESSAGES_REQUIRED_TO_SAVE / 4,
                8,
            ),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let namespace = IggyNamespace::new(1, 1, 0);
            let mut config = repair_config();
            config.size_of_messages_required_to_save = IggyByteSize::from(span);
            config.path_layout.streams_root = directory
                .path()
                .join("streams")
                .to_string_lossy()
                .into_owned();
            let partition_path = config.get_partition_path(1, 1, 0);
            std::fs::create_dir_all(&partition_path).unwrap();
            let (mut partition, _) = recording_partition_at(0, 1);
            set_offset_dirs_under(&mut partition, std::path::Path::new(&partition_path));
            partition.set_partition_dir(partition_path);
            partition.runtime_options.durability = durability;
            partition.runtime_options.segment_size = Some(IggyByteSize::from(SEGMENT_BYTES));
            partition.log.active_segment_mut().max_size = IggyByteSize::from(SEGMENT_BYTES);
            partition.open_persistence().await.unwrap();
            partition.log.retire_front().unwrap();
            partition.install_empty_segment(&config, 0).await.unwrap();
            let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
            assert!(persistence.segment_checkpoint().is_some());
            let mut parent = 0;
            let mut expected = Vec::new();
            let mut last_position = 0;
            for ordinal in 0..RECORDS {
                let payload = vec![u8::try_from(ordinal).unwrap(); PAYLOAD_BYTES];
                let prepare = checksummed_segment_prepare(ordinal + 1, parent, ordinal, &payload);
                parent = prepare.header().checksum;
                last_position = expected.len() as u64;
                expected.extend_from_slice(&prepare.as_slice()[size_of::<PrepareHeader>()..]);
                persistence
                    .append(prepare.clone().into_frozen(), durability.is_persisted())
                    .unwrap();
                partition
                    .append_repaired_send_messages(prepare)
                    .await
                    .unwrap();
            }
            assert!(persistence.start());
            Rc::clone(&persistence).run().await;
            partition.consensus.restore_commit_state(RECORDS, RECORDS);
            partition.consensus.sequencer().set_sequence(RECORDS);
            assert!(
                partition
                    .commit_messages_inner(&config, true, RECORDS)
                    .await
                    .unwrap()
            );
            assert_eq!(
                std::fs::read(config.get_messages_path(1, 1, 0, 0)).unwrap(),
                expected
            );
            assert_eq!(
                file_len(std::path::Path::new(&config.get_index_path(1, 1, 0, 0)))
                    / crate::iggy_index::IGGY_INDEX_SIZE as u64,
                expected_indexes
            );
            let args =
                PollingArgs::new(iggy_common::PollingStrategy::offset(RECORDS - 1), 1, false);
            let plan = partition.build_poll_plan(PollingConsumer::Consumer(1, 0), &args, true);
            let PollTier::Disk { disk, .. } = &plan.tier else {
                panic!("the flushed backlog must be polled from disk");
            };
            assert!(
                last_position.saturating_sub(disk.start_position) <= span,
                "the last poll must not rescan the whole coalesced WAL backlog: {} bytes skipped",
                last_position.saturating_sub(disk.start_position)
            );
            let result = plan.execute().await;
            assert_eq!(result.last_matching_offset, Some(RECORDS - 1));
            let completion = partition.complete_poll(result).unwrap();
            let polled: Vec<_> = completion
                .fragments
                .iter()
                .flat_map(|fragment| fragment.as_slice().iter().copied())
                .collect();
            assert_eq!(polled, expected[usize::try_from(last_position).unwrap()..]);
            persistence.request_checkpoint();
            partition.checkpoint_persistence(&config).await;
            persistence.drain().await.unwrap();
            assert_eq!(persistence.checkpoint_op(), RECORDS);
            let checkpoint = persistence.segment_checkpoint();
            drop(partition);
            drop(persistence);
            let recovered = crate::segment_recovery::load_persisted_segments_with_checkpoint(
                &config,
                namespace,
                IggyByteSize::from(SEGMENT_BYTES),
                durability.is_persisted(),
                &PartitionStats::default(),
                checkpoint,
            )
            .await
            .unwrap();
            assert_eq!(recovered.len(), 1);
            assert_eq!(recovered[0].segment.end_offset, RECORDS - 1);
            assert_eq!(
                recovered[0].segment.size.as_bytes_u64(),
                expected.len() as u64
            );
            assert_eq!(
                std::fs::read(config.get_messages_path(1, 1, 0, 0)).unwrap(),
                expected
            );
        }
    }
}

#[compio::test]
async fn given_bounded_save_when_pieces_settle_should_publish_one_index_and_advance_together() {
    const RECORDS: u64 = 3;
    const PAYLOAD_BYTES: usize = 512;
    for split in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let log_path = directory.path().join("segment.log");
        let index_path = directory.path().join("segment.index");
        let mut fixture =
            PersistFixture::new(log_path.to_str().unwrap(), index_path.to_str().unwrap()).await;
        let mut expected = Vec::new();
        let mut parent = 0;
        let mut single_charge = 0;
        for ordinal in 0..RECORDS {
            let payload = vec![u8::try_from(ordinal).unwrap(); PAYLOAD_BYTES];
            let prepare = checksummed_segment_prepare(ordinal + 1, parent, ordinal, &payload);
            parent = prepare.header().checksum;
            let frozen = prepare.into_frozen();
            expected.extend_from_slice(&frozen.as_slice()[size_of::<PrepareHeader>()..]);
            single_charge = crate::io::materialization_allocation_charge(
                std::slice::from_ref(&frozen),
                1,
                crate::iggy_index::IGGY_INDEX_SIZE,
            )
            .unwrap();
            fixture
                .partition
                .log
                .journal()
                .inner
                .append(frozen)
                .await
                .unwrap();
        }
        let bytes_max = if split {
            single_charge
        } else {
            single_charge * usize::try_from(RECORDS).unwrap()
        };
        fixture.partition.io_bytes_max.set(bytes_max);
        fixture.partition.consensus.advance_commit_max(RECORDS);
        assert!(
            fixture
                .partition
                .begin_materialization(
                    RECORDS,
                    iggy_common::DEFAULT_SIZE_OF_MESSAGES_REQUIRED_TO_SAVE
                )
                .unwrap()
        );
        let mut jobs = 0;
        loop {
            match fixture.partition.drive_materialization().unwrap() {
                MaterializationStep::Progress => {}
                MaterializationStep::Pending => panic!("no WAL or task can block this fixture"),
                MaterializationStep::Complete => break,
                MaterializationStep::Ready => {
                    let job = fixture
                        .partition
                        .capture_materialization(&repair_config())
                        .unwrap()
                        .unwrap();
                    let crate::PartitionIoJob::Materialize(save) = &job else {
                        panic!("segment has room for every record");
                    };
                    assert!(save.allocation_charge() <= bytes_max);
                    let completes = save.completes;
                    assert_eq!(save.indexes.is_empty(), !completes);
                    assert_eq!(
                        fixture.cursors(),
                        (0, 0),
                        "a partial save cannot expose either cursor"
                    );
                    let result = job.execute().await;
                    assert_eq!(
                        fixture.cursors(),
                        (0, 0),
                        "execution cannot publish progress"
                    );
                    fixture
                        .partition
                        .accept_materialization_phase(result)
                        .unwrap();
                    jobs += 1;
                    if !completes {
                        assert_eq!(fixture.cursors(), (0, 0));
                        assert_eq!(fixture.segment_size(), 0);
                        assert_eq!(file_len(&index_path), 0);
                        assert_eq!(
                            fixture
                                .partition
                                .log
                                .journal()
                                .inner
                                .with_entries(VecDeque::len),
                            usize::try_from(RECORDS).unwrap()
                        );
                    }
                }
            }
        }
        assert_eq!(jobs, if split { RECORDS } else { 1 });
        assert_eq!(
            fixture.cursors(),
            (
                expected.len() as u64,
                crate::iggy_index::IGGY_INDEX_SIZE as u64
            )
        );
        assert_eq!(std::fs::read(&log_path).unwrap(), expected);
        assert_eq!(
            file_len(&index_path),
            crate::iggy_index::IGGY_INDEX_SIZE as u64
        );
        assert_eq!(
            fixture.partition.log.active_segment().end_offset,
            RECORDS - 1
        );
        assert!(fixture.partition.log.journal().inner.is_empty());
    }
}

//     #[cfg(target_os = "linux")]
//     #[compio::test]
//     async fn given_index_save_failure_when_persisting_should_leave_both_cursors_and_segment_size_untouched()
//      {
//         let dir = tempfile::tempdir().expect("temp dir");
//         let log_path = dir.path().join("segment.log");
//         let mut fixture =
//             PersistFixture::new(log_path.to_str().expect("utf-8 path"), DEV_FULL).await;
//
//         let result = fixture.persist(FIRST_PAYLOAD, 0).await;
//
//         assert!(
//             matches!(result, Err(IggyError::CannotSaveIndexToSegment)),
//             "index save over {DEV_FULL} must fail, got {result:?}"
//         );
//         assert_eq!(
//             file_len(&log_path),
//             FIRST_PAYLOAD.len() as u64,
//             "the segment bytes landed; only the cursor is withheld"
//         );
//         assert_eq!(
//             fixture.cursors(),
//             (0, 0),
//             "neither cursor may advance when the index half failed"
//         );
//         assert_eq!(fixture.segment_size(), 0, "segment size must not advance");
//     }
//
//     #[cfg(target_os = "linux")]
//     #[compio::test]
//     async fn given_log_save_failure_when_persisting_should_leave_both_cursors_and_segment_size_untouched()
//      {
//         let dir = tempfile::tempdir().expect("temp dir");
//         let index_path = dir.path().join("segment.index");
//         let mut fixture =
//             PersistFixture::new(DEV_FULL, index_path.to_str().expect("utf-8 path")).await;
//
//         let result = fixture.persist(FIRST_PAYLOAD, 0).await;
//
//         assert!(
//             matches!(result, Err(IggyError::CannotWriteToFile)),
//             "segment save over {DEV_FULL} must fail, got {result:?}"
//         );
//         assert_eq!(
//             file_len(&index_path),
//             IGGY_INDEX_SIZE as u64,
//             "the index entry landed; only the cursor is withheld"
//         );
//         assert_eq!(
//             fixture.cursors(),
//             (0, 0),
//             "neither cursor may advance when the segment half failed"
//         );
//         assert_eq!(fixture.segment_size(), 0, "segment size must not advance");
//     }
//
//     #[cfg(target_os = "linux")]
//     #[compio::test]
//     async fn given_both_saves_failing_when_persisting_should_leave_both_cursors_untouched() {
//         let mut fixture = PersistFixture::new(DEV_FULL, DEV_FULL).await;
//
//         let result = fixture.persist(FIRST_PAYLOAD, 0).await;
//
//         assert!(
//             matches!(result, Err(IggyError::CannotWriteToFile)),
//             "a failed segment save must win over the index error, got {result:?}"
//         );
//         assert_eq!(fixture.cursors(), (0, 0), "no cursor may advance");
//         assert_eq!(fixture.segment_size(), 0, "segment size must not advance");
//     }
//
//     /// The two saves run concurrently, so each must read its own cursor
//     /// without observing the other's advance: the second chunk lands exactly
//     /// where the first one ended, and its index entry says so.
//     #[compio::test]
//     async fn given_two_successful_persists_when_reading_back_should_place_second_chunk_at_first_chunk_end()
//      {
//         let dir = tempfile::tempdir().expect("temp dir");
//         let log_path = dir.path().join("segment.log");
//         let index_path = dir.path().join("segment.index");
//         let mut fixture = PersistFixture::new(
//             log_path.to_str().expect("utf-8 path"),
//             index_path.to_str().expect("utf-8 path"),
//         )
//         .await;
//
//         fixture
//             .persist(FIRST_PAYLOAD, 0)
//             .await
//             .expect("first persist");
//         fixture
//             .persist(SECOND_PAYLOAD, 7)
//             .await
//             .expect("second persist");
//
//         let log_bytes = (FIRST_PAYLOAD.len() + SECOND_PAYLOAD.len()) as u64;
//         let index_bytes = 2 * IGGY_INDEX_SIZE as u64;
//         assert_eq!(
//             fixture.cursors(),
//             (log_bytes, index_bytes),
//             "both cursors must cover both persists"
//         );
//         assert_eq!(file_len(&log_path), log_bytes, "segment file length");
//         assert_eq!(file_len(&index_path), index_bytes, "index file length");
//         assert_eq!(fixture.segment_size(), log_bytes, "segment size");
//
//         let reader = IggyIndexReader::new(index_path.to_str().expect("utf-8 path"))
//             .await
//             .expect("open index reader");
//         let last = reader
//             .load_last()
//             .await
//             .expect("read last index entry")
//             .expect("index entry present");
//         assert_eq!(
//             last,
//             IggyIndex::new(7, 8, FIRST_PAYLOAD.len() as u64),
//             "the second entry must address the first chunk's end"
//         );
//     }

#[compio::test]
async fn given_a_fenced_window_when_flushing_should_write_nothing_above_commit_min() {
    let dir = tempfile::tempdir().expect("temp dir");
    let log_path = dir.path().join("segment.log");
    let index_path = dir.path().join("segment.index");
    let mut fixture = PersistFixture::new(
        log_path.to_str().expect("utf-8 path"),
        index_path.to_str().expect("utf-8 path"),
    )
    .await;
    let partition = &mut fixture.partition;
    partition.log.journal().inner.set_repair_retention(true);
    partition.repair = Some(armed_session(3, 0));
    for op in 1..=3 {
        partition
            .apply_repaired_prepare(repaired_send_prepare(
                op,
                u128::from(op - 1),
                u128::from(op),
            ))
            .await;
    }
    partition.consensus().advance_commit_max(3);
    partition.fence_view_window(3, None);

    partition
        .flush_committed_messages(&repair_config())
        .await
        .expect("flush");
    assert!(
        std::fs::read(&log_path).unwrap().is_empty(),
        "a forced flush wrote unproven entries to the segment"
    );
    assert!((1..=3).all(|op| partition.log.journal().inner.holds_op(op)));
}

#[compio::test]
async fn owned_materialization_keeps_physical_cursors_private_until_acceptance() {
    let directory = tempfile::tempdir().unwrap();
    let log_path = directory.path().join("segment.log");
    let index_path = directory.path().join("segment.index");
    let mut fixture =
        PersistFixture::new(log_path.to_str().unwrap(), index_path.to_str().unwrap()).await;
    let job = fixture.capture(FIRST_PAYLOAD, 0).unwrap();
    let result = job.execute().await;
    assert_eq!(std::fs::read(&log_path).unwrap(), FIRST_PAYLOAD);
    assert_eq!(file_len(&index_path), IGGY_INDEX_SIZE as u64);
    assert_eq!(
        fixture.cursors(),
        (0, 0),
        "file completion is not owner acceptance"
    );
    assert_eq!(fixture.segment_size(), 0);
    fixture.partition.accept_materialization(result).unwrap();
    assert_eq!(
        fixture.cursors(),
        (FIRST_PAYLOAD.len() as u64, IGGY_INDEX_SIZE as u64)
    );
    assert_eq!(fixture.segment_size(), FIRST_PAYLOAD.len() as u64);
}

#[compio::test]
async fn completed_materialization_cannot_advance_a_recreated_writer_at_the_same_path() {
    let directory = tempfile::tempdir().unwrap();
    let log_path = directory.path().join("segment.log");
    let index_path = directory.path().join("segment.index");
    let mut fixture =
        PersistFixture::new(log_path.to_str().unwrap(), index_path.to_str().unwrap()).await;
    let result = fixture.capture(FIRST_PAYLOAD, 0).unwrap().execute().await;
    std::fs::remove_file(&index_path).unwrap();
    let replacement = IggyIndexWriter::new(
        index_path.to_str().unwrap(),
        Rc::new(AtomicU64::new(0)),
        true,
        false,
    )
    .await
    .unwrap();
    let active = fixture.partition.log.index_writers().len() - 1;
    fixture.partition.log.index_writers_mut()[active] = Some(Rc::new(replacement));
    assert!(fixture.partition.accept_materialization(result).is_err());
    assert_eq!(fixture.cursors(), (0, 0));
    assert_eq!(fixture.segment_size(), 0);
}

#[compio::test]
async fn given_flushed_repair_ahead_of_commit_min_when_replayed_should_persist_only_new_batches() {
    let dir = tempfile::tempdir().expect("temp dir");
    let log_path = dir.path().join("segment.log");
    let index_path = dir.path().join("segment.index");
    let mut fixture = PersistFixture::new(
        log_path.to_str().expect("utf-8 path"),
        index_path.to_str().expect("utf-8 path"),
    )
    .await;
    let partition = &mut fixture.partition;
    partition.log.journal().inner.set_repair_retention(true);
    partition.repair = Some(armed_session(4, 0));
    let prepares: Vec<_> = (1..=4)
        .map(|op| repaired_send_prepare(op, u128::from(op - 1), u128::from(op)).into_frozen())
        .collect();
    let replay = |op: usize| {
        let bytes = prepares[op - 1].as_slice();
        let mut message = Message::<PrepareHeader>::new(bytes.len());
        message.as_mut_slice().copy_from_slice(bytes);
        message
    };
    for op in 1..=3 {
        partition.apply_repaired_prepare(replay(op)).await;
    }
    partition.consensus().advance_commit_max(3);
    partition
        .flush_committed_messages(&repair_config())
        .await
        .expect("flush before the commit walk catches up");
    assert_eq!(partition.consensus().commit_min(), 0);
    assert_eq!(partition.recovered_durable_offset, None);
    let original = std::fs::read(&log_path).expect("read initial segment");
    let original_index = std::fs::read(&index_path).expect("read initial index");

    for op in 2..=3 {
        partition.apply_repaired_prepare(replay(op)).await;
    }
    partition
        .flush_committed_messages(&repair_config())
        .await
        .expect("flush replayed batches");
    assert_eq!(std::fs::read(&log_path).unwrap(), original);
    assert_eq!(std::fs::read(&index_path).unwrap(), original_index);

    let next = replay(4);
    let mut expected = original;
    expected.extend_from_slice(&next.as_slice()[size_of::<PrepareHeader>()..]);
    partition.apply_repaired_prepare(next).await;
    partition.consensus().advance_commit_max(4);
    partition
        .flush_committed_messages(&repair_config())
        .await
        .expect("flush the new batch");
    assert_eq!(std::fs::read(&log_path).unwrap(), expected);
    // The commit walk needs resident headers after the earlier flush.
    for op in 1..=4 {
        partition.apply_repaired_prepare(replay(op)).await;
    }
    partition.commit_journal(&repair_config()).await;
    assert_eq!(partition.consensus().commit_min(), 4);
    assert!(partition.fatal.is_none());
    assert_eq!(std::fs::read(&log_path).unwrap(), expected);
}

/// Base offsets of the batches in a segment log, in file order.
fn segment_base_offsets(mut log: &[u8]) -> Vec<u64> {
    let mut offsets = Vec::new();
    while !log.is_empty() {
        let batch = decode_batch_slice(log).expect("segment holds whole batches");
        offsets.push(batch.header.base_offset);
        log = &log[batch.header.total_size()..];
    }
    offsets
}

/// A repaired op can arrive before the ops below it. Writing it first puts
/// its offsets past a hole, and the flush skips every batch that ends at or
/// below the segment end, so the lower ops would read as persisted and be
/// dropped.
#[compio::test]
async fn given_repaired_op_above_a_hole_when_flushing_should_hold_it_until_the_hole_fills() {
    let dir = tempfile::tempdir().expect("temp dir");
    let log_path = dir.path().join("segment.log");
    let index_path = dir.path().join("segment.index");
    let mut fixture = PersistFixture::new(
        log_path.to_str().expect("utf-8 path"),
        index_path.to_str().expect("utf-8 path"),
    )
    .await;
    let partition = &mut fixture.partition;
    partition.repair = Some(armed_session(6, 0));
    for op in [1, 2, 3, 6] {
        partition
            .apply_repaired_prepare(repaired_send_prepare(
                op,
                u128::from(op - 1),
                u128::from(op),
            ))
            .await;
    }
    partition.consensus().advance_commit_max(6);
    for _ in 0..2 {
        partition
            .flush_committed_messages(&repair_config())
            .await
            .expect("flush below the hole");
    }
    assert_eq!(
        segment_base_offsets(&std::fs::read(&log_path).unwrap()),
        [1, 2, 3]
    );
    assert!(partition.log.journal().inner.holds_op(6));

    for op in [4, 5] {
        partition
            .apply_repaired_prepare(repaired_send_prepare(
                op,
                u128::from(op - 1),
                u128::from(op),
            ))
            .await;
    }
    partition
        .flush_committed_messages(&repair_config())
        .await
        .expect("flush the filled window");
    assert_eq!(
        segment_base_offsets(&std::fs::read(&log_path).unwrap()),
        [1, 2, 3, 4, 5, 6]
    );
    assert!(partition.log.journal().inner.is_empty());
}

/// Repair that journals `[1, 2, 3, 6, 4, 5]` must still reach the segment
/// in op order through the I/O owner.
#[compio::test]
async fn given_out_of_order_repair_when_the_io_owner_flushes_should_write_in_op_order() {
    const FLUSH_ROUNDS_MAX: usize = 8;
    let config = repair_config();
    let (directory, mut partition) = Box::pin(disk_poll_partition(&config)).await;
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    partition.repair = Some(armed_session(6, 0));
    for op in [1, 2, 3, 6, 4, 5] {
        partition
            .apply_repaired_prepare(repaired_send_prepare(
                op,
                u128::from(op - 1),
                u128::from(op),
            ))
            .await;
    }
    partition.consensus().advance_commit_max(6);

    let mut flushed = false;
    for _ in 0..FLUSH_ROUNDS_MAX {
        if partition.flush_committed_messages(&config).await.unwrap() {
            flushed = true;
            break;
        }
        settle_partition_io(&mut partition, &config).await;
    }
    assert!(flushed, "the flush must finish");
    assert!(partition.fatal().is_none());
    assert_eq!(
        segment_base_offsets(
            &std::fs::read(directory.path().join("00000000000000000000.log")).unwrap()
        ),
        [1, 2, 3, 4, 5, 6]
    );
}

/// An install leaves no evicted op behind and moves the commit point past
/// the history it brings, so only the commit point tells the flush where
/// the next op may start.
#[compio::test]
async fn given_installed_history_when_flushing_the_next_ops_should_write_them() {
    const INSTALLED_OPS: u64 = 3;
    let directory = tempfile::tempdir().unwrap();
    let mut partition = test_partition();
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    set_offset_dirs_under(&mut partition, directory.path());
    let mut prepares = Vec::new();
    let mut parent = 0;
    for offset in 0..INSTALLED_OPS {
        let prepare = checksummed_segment_prepare(offset + 1, parent, offset, b"transferred");
        parent = prepare.header().checksum;
        prepares.push(prepare);
    }
    let mut staged = Vec::new();
    for (offset, prepare) in (0u64..).zip(&prepares) {
        let body = prepare.as_slice()[size_of::<PrepareHeader>()..].to_vec();
        let artifact = consensus::StateArtifact::for_bytes(
            consensus::state_manifest::artifact_kind::SEGMENT_LOG,
            offset,
            &body,
        );
        staged.push(
            partition
                .spill_transfer_segment(&artifact, body, None)
                .await
                .unwrap(),
        );
    }
    let last = prepares.last().unwrap();
    let offsets = crate::state_transfer::ConsumerOffsetsWire {
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        required_metadata_frontier: 0,
        prepare_checksum: Some(last.header().checksum),
        checkpoint_prepare: last.as_slice().to_vec(),
        next_offset: INSTALLED_OPS,
        emptied_at: 0,
        consumers: Vec::new(),
        groups: Vec::new(),
        external_groups: Vec::new(),
        dedup: Vec::new(),
        owners: Vec::new(),
        history_transition: None,
        owner_retirement: None,
        retirement_paid_through: 0,
    };
    partition
        .install_state_transfer(&repair_config(), INSTALLED_OPS, staged, &offsets.encode())
        .await
        .unwrap();
    assert_eq!(partition.consensus().commit_min(), INSTALLED_OPS);

    for op in [INSTALLED_OPS + 1, INSTALLED_OPS + 2] {
        journal_send_batch(&mut partition, op).await;
    }
    partition.consensus().advance_commit_max(INSTALLED_OPS + 2);
    assert!(
        partition
            .flush_committed_messages(&repair_config())
            .await
            .unwrap()
    );
    assert!(partition.log.journal().inner.is_empty());
    assert_eq!(
        partition.log.active_segment().end_offset,
        INSTALLED_OPS + 1,
        "offsets 3 and 4 follow the installed 0..=2"
    );
}

#[cfg(target_os = "linux")]
#[compio::test]
async fn given_a_persist_failure_on_a_committed_op_should_fence_the_partition_not_panic() {
    // The op is cluster-committed and the local write cannot be made, so
    // the replica is divergent. This used to `panic!`, which the pump
    // task swallows: the shard stopped serving every partition it owned
    // while the process reported healthy. The partition must fence itself
    // instead, so the shard's tick can see the fault and stop the server.
    let dir = tempfile::tempdir().expect("temp dir");
    let log_path = dir.path().join("segment.log");
    let log_cursor = Rc::new(AtomicU64::new(0));
    let index_cursor = Rc::new(AtomicU64::new(0));
    let messages_writer = MessagesWriter::new(
        log_path.to_str().expect("utf-8 path"),
        log_cursor,
        true,
        false,
        None,
    )
    .await
    .expect("open segment log writer");
    // The index half writes to a device that is always full, so the
    // persist fails the way a full disk fails.
    let index_writer = IggyIndexWriter::new(DEV_FULL, index_cursor, true, false)
        .await
        .expect("open segment index writer");

    let mut partition = test_partition();
    partition.log.add_persisted_segment(
        Segment::new(0, IggyByteSize::from(1024 * 1024_u64)),
        SegmentStorage::default(),
        Some(Rc::new(messages_writer)),
        Some(Rc::new(index_writer)),
    );

    journal_send_batch(&mut partition, 1).await;
    partition.consensus().advance_commit_max(1);

    // Would abort the test process before the fence existed.
    partition.commit_journal(&repair_config()).await;

    let fault = partition
        .fatal()
        .expect("a failed commit of a cluster-committed op must fence the partition");
    assert_eq!(fault.op, 1);
    assert_eq!(fault.operation, Some(Operation::SendMessages));

    // The fence holds: a fenced partition must not advance again, or the
    // pump's tail drain walks it into the `advance_commit_min` assert.
    let commit_min = partition.consensus().commit_min();
    partition.commit_journal(&repair_config()).await;
    assert_eq!(
        partition.consensus().commit_min(),
        commit_min,
        "a fenced partition must not advance on a later commit"
    );
}

#[compio::test]
async fn given_a_shutdown_flush_failure_when_fencing_should_keep_an_earlier_commit_fault() {
    let mut partition = test_partition();

    // A flush failure on a healthy partition fences it, so the pump's
    // post-flush scan turns the exit non-zero instead of reporting a
    // clean shutdown over unpersisted cluster-committed data.
    partition.fence_flush_failure();
    let fault = partition
        .fatal()
        .expect("a failed shutdown flush must fence the partition");
    assert_eq!(fault.op, partition.consensus().commit_min());
    assert_eq!(fault.operation, Some(Operation::SendMessages));

    // A partition the commit path already fenced keeps that fault: it
    // names the op that first diverged.
    let commit_fault = FatalCommit {
        namespace_raw: fault.namespace_raw,
        op: 42,
        operation: Some(Operation::StoreConsumerOffset),
    };
    partition.fatal = Some(commit_fault);
    partition.fence_flush_failure();
    let kept = partition.fatal().expect("the fence must hold");
    assert_eq!(kept.op, 42);
    assert_eq!(kept.operation, Some(Operation::StoreConsumerOffset));
}

//     /// A half that failed never advanced its cursor, so the retry rewrites
//     /// the same positions: one copy of the batch, one index entry.
//     #[cfg(target_os = "linux")]
//     #[compio::test]
//     async fn given_failed_persist_when_retried_with_a_healthy_writer_should_overwrite_the_same_positions()
//      {
//         let dir = tempfile::tempdir().expect("temp dir");
//         let log_path = dir.path().join("segment.log");
//         let index_path = dir.path().join("segment.index");
//         let mut fixture =
//             PersistFixture::new(log_path.to_str().expect("utf-8 path"), DEV_FULL).await;
//
//         assert!(
//             fixture.persist(FIRST_PAYLOAD, 0).await.is_err(),
//             "index save over {DEV_FULL} must fail"
//         );
//
//         // The committed prefix stays resident, so the retry re-persists the
//         // identical bytes; only the broken index writer is swapped out.
//         let index_writer = IggyIndexWriter::new(
//             index_path.to_str().expect("utf-8 path"),
//             fixture.index_cursor.clone(),
//             true,
//             false,
//         )
//         .await
//         .expect("open replacement index writer");
//         let active = fixture.partition.log.index_writers().len() - 1;
//         fixture.partition.log.index_writers_mut()[active] = Some(Rc::new(index_writer));
//
//         fixture
//             .persist(FIRST_PAYLOAD, 0)
//             .await
//             .expect("retry persist");
//
//         assert_eq!(
//             file_len(&log_path),
//             FIRST_PAYLOAD.len() as u64,
//             "the retry must overwrite the batch, not append a second copy"
//         );
//         assert_eq!(
//             file_len(&index_path),
//             IGGY_INDEX_SIZE as u64,
//             "the retry must write exactly one index entry"
//         );
//         assert_eq!(
//             fixture.cursors(),
//             (FIRST_PAYLOAD.len() as u64, IGGY_INDEX_SIZE as u64),
//             "both cursors must advance once the retry succeeded"
//         );
//     }

/// Eviction is what makes a second copy reachable at all:
/// `apply_repaired_prepare` refuses an op the journal still holds, so a
/// re-delivered repair frame is only re-journaled once the flush that
/// persisted it has evicted it. `commit_min` is what lets the frame past
/// the other guard: the flush persists the whole committed prefix while
/// the walk advances `commit_min` by at most [`COMMIT_WALK_OPS_MAX`], so
/// every op above the walk's reach is persisted, evicted, and still
/// re-deliverable. The flush is the last gate, and a durable line frozen at
/// boot cannot see its own writes -- a rejoining backup then diverged from
/// the group by exactly the ops it repaired.
///
/// Journaled through the repair path, the one that keeps a batch's original
/// offsets; `apply_replicated_operation` re-stamps from the local counter
/// and so cannot collide with itself.
#[compio::test]
async fn given_offsets_already_persisted_when_flushed_again_should_not_append_a_second_copy() {
    const CHECKSUM: u128 = 0x5a;
    // One op past the walk budget, so the last one stays above `commit_min`
    // after the flush that persists it.
    const OPS: u64 = COMMIT_WALK_OPS_MAX as u64 + 1;
    let mut partition = test_partition();
    let record_len = build_segment_record(IggyNamespace::new(1, 1, 0), 1).len() as u64;
    let chained = |op: u64| if op == 1 { 0 } else { CHECKSUM };
    partition.repair = Some(armed_session(OPS, 0));

    for op in 1..=OPS {
        partition
            .apply_repaired_prepare(repaired_send_prepare(op, chained(op), CHECKSUM))
            .await;
    }
    partition.consensus().advance_commit_max(OPS);
    partition.commit_journal(&repair_config()).await;
    assert_eq!(partition.consensus().commit_min(), OPS - 1);
    assert_eq!(
        partition.log.active_segment().size.as_bytes_u64(),
        record_len * (OPS - 1)
    );
    assert_eq!(
        partition
            .collect_committable_from_journal(COMMIT_WALK_OPS_MAX, &repair_config())
            .iter()
            .map(|entry| entry.op)
            .collect::<Vec<_>>(),
        vec![OPS]
    );
    partition
        .flush_committed_messages(&repair_config())
        .await
        .unwrap();
    assert_eq!(
        partition.log.active_segment().size.as_bytes_u64(),
        record_len * OPS,
        "the first flush persists the whole committed prefix"
    );
    assert!(
        partition.consensus().commit_min() < OPS,
        "the premise: the walk leaves the last op above `commit_min`, which \
             is what lets the repair ingest re-deliver it"
    );

    partition.repair = Some(armed_session(OPS, 0));
    partition
        .apply_repaired_prepare(repaired_send_prepare(OPS, chained(OPS), CHECKSUM))
        .await;
    partition.commit_journal(&repair_config()).await;
    assert_eq!(
        partition.log.active_segment().size.as_bytes_u64(),
        record_len * OPS,
        "offsets the segment already holds must not be appended twice"
    );
}

#[cfg(test)]
mod review_4092_tests {
    use super::*;
    use super::{checksummed_segment_prepare, recording_partition_at};

    /// ENOSPC on 28 is the raw errno; `io::ErrorKind::StorageFull` is unstable.
    const ENOSPC: i32 = 28;

    /// `tick_partitions` turns a partition's `fatal()` into a server shutdown
    /// (`shard/src/lib.rs:7378-7382`). A refused offset write leaves prior bytes
    /// intact and nothing undefined, so it should fence the partition at worst,
    /// the way `mark_materialization_missing` and `partitions.tombstone` already
    /// do for an unserviceable namespace.
    #[compio::test]
    #[ignore = "PR #4092 review: a refused consumer-offset write raises `FatalCommit`, which the shard pump converts into a whole-node shutdown"]
    async fn given_a_full_disk_when_driving_persistence_then_only_the_partition_should_fence() {
        let directory = tempfile::tempdir().unwrap();
        let (mut partition, _replies) = recording_partition_at(0, 3);
        partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
        partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Persisted;
        partition.open_persistence().await.unwrap();
        let persistence = Rc::clone(partition.persistence.as_ref().unwrap());

        persistence.fail_operation(
            std::io::Error::from_raw_os_error(ENOSPC),
            Operation::StoreConsumerOffset,
        );
        partition.drive_persistence().await;

        assert!(
            partition.fatal().is_none(),
            "a refused consumer-offset write raised FatalCommit, which the shard pump converts into a whole-node shutdown; every other partition on the core is taken down with it, including topics with no persisted policy"
        );
    }

    /// Contested between reviewers: distsys argued the pre-checks leave only a
    /// genuinely divergent prepare here, where fail-closed is defensible; storage
    /// argued the same errno classification fix covers both call sites. Recorded
    /// so the decision is explicit rather than implied by a missing test.
    #[compio::test]
    #[ignore = "PR #4092 review: CONTESTED between reviewers -- whether a divergent prepare at an already-accepted op should latch the whole partition"]
    async fn given_a_divergent_prepare_when_submitting_then_the_partition_should_not_latch() {
        let directory = tempfile::tempdir().unwrap();
        let (mut partition, _replies) = recording_partition_at(0, 3);
        partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
        partition.runtime_options.durability = iggy_common::Durability::Persisted;
        partition.open_persistence().await.unwrap();
        let persistence = Rc::clone(partition.persistence.as_ref().unwrap());

        let first = checksummed_segment_prepare(1, 0, 0, b"first");
        assert!(partition.submit_prepare_persistence(first.into_frozen(), Operation::SendMessages));

        // Same op, different bytes: `append` answers InvalidData, which is not
        // `WouldBlock`, so `:1192-1193` latches the whole partition.
        let divergent = checksummed_segment_prepare(1, 0, 0, b"divergent");
        assert!(
            !partition.submit_prepare_persistence(divergent.into_frozen(), Operation::SendMessages)
        );
        assert!(
            persistence.failure().is_none(),
            "a single refused prepare latched the partition permanently; `failure` has no clearing path, so every later durability query answers false"
        );
    }
}
