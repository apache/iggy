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

include!("persistence_transfer.rs");
include!("persistence_wal.rs");
include!("persistence_consensus.rs");
include!("persistence_ownership.rs");
include!("persistence_counters.rs");
include!("persistence_materialization.rs");
include!("persistence_materialization_io.rs");
include!("persistence_offset_barrier.rs");
include!("persistence_offset_empty.rs");
include!("persistence_offset_recovery.rs");
include!("persistence_offset_lifecycle.rs");
include!("persistence_offset_replication.rs");
include!("persistence_offset_storage.rs");
include!("persistence_disk_poll.rs");
include!("persistence_repair.rs");
include!("persistence_repair_floor.rs");
include!("persistence_state_transfer.rs");
include!("persistence_materialization_tail.rs");
