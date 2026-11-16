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

//! A partition that places its batches: its prepares and its segments hold
//! placed batches, and the messages are in the object store.

use super::*;
use iggy_binary_protocol::batch::BatchHeader;
use iggy_binary_protocol::placement::{PLACED_BATCH_SIZE, PlacedBatch, Placement};

/// The placed batch of a one-message batch whose blob is the `index`-th one
/// of the object `7`, with the offsets still to be stamped.
fn placed_record(index: u64) -> (Vec<u8>, Placement) {
    let whole = build_segment_record(IggyNamespace::new(1, 1, 0), 0);
    let mut header = BatchHeader::decode(&whole).unwrap();
    header.batch_checksum = 0;
    let placement = Placement::of(7, index * 4096, &whole[COMMAND_HEADER_SIZE..]);
    let mut record = vec![0; PLACED_BATCH_SIZE];
    PlacedBatch::new(header, placement).encode_into(&mut record);
    (record, placement)
}

fn placed_records(count: u64) -> (Vec<Vec<u8>>, Vec<Placement>) {
    (0..count).map(placed_record).unzip()
}

async fn placing_partition() -> (tempfile::TempDir, IggyPartition<IggyMessageBus>) {
    let config = repair_config();
    let (directory, mut partition) = Box::pin(disk_poll_partition_with_replicas(&config, 3)).await;
    partition.set_places_batches(true);
    (directory, partition)
}

/// The placed batches a segment file holds, back to back.
fn placed_batches_in(bytes: &[u8]) -> Vec<PlacedBatch> {
    assert_eq!(bytes.len() % PLACED_BATCH_SIZE, 0, "a cut placed batch");
    bytes
        .chunks(PLACED_BATCH_SIZE)
        .map(|placed| PlacedBatch::decode(placed).unwrap())
        .collect()
}

#[compio::test]
async fn given_a_placing_partition_when_placed_batches_commit_should_stamp_and_store_them_in_its_segment()
 {
    let (directory, mut partition) = Box::pin(placing_partition()).await;
    let (records, placements) = placed_records(3);

    Box::pin(commit_records(&mut partition, 1, &records)).await;

    assert_eq!(partition.offset.load(Ordering::Acquire), 2);
    let segments = segment_file_bytes(directory.path());
    assert_eq!(segments.len(), 1);
    let stored = placed_batches_in(&segments[0]);
    assert_eq!(stored.len(), 3);
    for (index, (placed, placement)) in stored.iter().zip(&placements).enumerate() {
        let op = index as u64 + 1;
        assert_eq!(placed.header.base_offset, index as u64);
        // `journal_send_record` stamps each prepare with its op.
        assert_eq!(placed.header.base_timestamp, op);
        assert_eq!(placed.header.message_count, 1);
        assert_eq!(placed.header.batch_checksum, 0);
        assert_eq!(placed.placement, *placement);
    }
    let segment = partition.log.active_segment();
    assert_eq!(segment.size.as_bytes_u64(), 3 * PLACED_BATCH_SIZE as u64);
    assert_eq!(segment.end_offset, 2);
}

#[compio::test]
async fn given_a_placing_partition_when_a_whole_batch_arrives_should_refuse_it() {
    let (_directory, mut partition) = Box::pin(placing_partition()).await;
    let whole = checksummed_segment_prepare(1, 0, 0, b"abcdefgh");

    let applied = Box::pin(partition.apply_replicated_operation(whole)).await;

    assert!(applied.is_err());
    assert_eq!(partition.dirty_offset.load(Ordering::Relaxed), 0);
}

#[compio::test]
async fn given_a_partition_of_whole_batches_when_a_placed_batch_arrives_should_refuse_it() {
    let config = repair_config();
    let (_directory, mut partition) = Box::pin(disk_poll_partition_with_replicas(&config, 3)).await;
    assert!(!partition.places_batches());
    let (record, _) = placed_record(0);
    let header_size = std::mem::size_of::<PrepareHeader>();
    let total = header_size + record.len();
    let mut placed = Message::<PrepareHeader>::new(total);
    placed.as_mut_slice()[header_size..].copy_from_slice(&record);
    let placed = placed.transmute_header(|_, header: &mut PrepareHeader| {
        header.command = Command::Prepare;
        header.partition_incarnation = partition.created_revision();
        header.operation = Operation::SendMessages;
        header.op = 1;
        header.timestamp = 1;
        header.group = IggyNamespace::new(1, 1, 0).inner();
        header.size = u32::try_from(total).unwrap();
    });

    let applied = Box::pin(partition.apply_replicated_operation(placed)).await;

    assert!(applied.is_err());
}

/// A prepare as a backup receives it: the primary stamped the placed batch
/// with the offset and with the prepare's timestamp.
fn stamped_placed_prepare(
    partition: &IggyPartition<IggyMessageBus>,
    op: u64,
    offset: u64,
    record: &[u8],
) -> Message<PrepareHeader> {
    let header_size = std::mem::size_of::<PrepareHeader>();
    let total = header_size + record.len();
    let mut message = Message::<PrepareHeader>::new(total);
    message.as_mut_slice()[header_size..].copy_from_slice(record);
    let message = message.transmute_header(|_, header: &mut PrepareHeader| {
        header.command = Command::Prepare;
        header.partition_incarnation = partition.created_revision();
        header.operation = Operation::SendMessages;
        header.op = op;
        header.timestamp = 1_000 + op;
        header.group = IggyNamespace::new(1, 1, 0).inner();
        header.size = u32::try_from(total).unwrap();
    });
    let (message, ..) =
        server_common::send_messages::stamp_prepare_for_persistence(message, offset, 1_000 + op)
            .unwrap();
    message
}

#[compio::test]
async fn given_a_backup_when_it_receives_a_stamped_placed_batch_should_journal_it_at_its_offsets() {
    let (_directory, mut partition) = Box::pin(placing_partition()).await;
    let (records, _) = placed_records(2);
    let first = stamped_placed_prepare(&partition, 1, 0, &records[0]);
    let second = stamped_placed_prepare(&partition, 2, 1, &records[1]);

    Box::pin(partition.append_received_send_messages_to_journal(first))
        .await
        .unwrap();
    Box::pin(partition.append_received_send_messages_to_journal(second))
        .await
        .unwrap();

    assert_eq!(partition.dirty_offset.load(Ordering::Relaxed), 1);
    assert_eq!(
        partition.log.active_segment().current_position,
        2 * PLACED_BATCH_SIZE as u64
    );
}

#[compio::test]
async fn given_a_backup_when_a_placed_batch_fails_its_checksum_should_refuse_it() {
    let (_directory, mut partition) = Box::pin(placing_partition()).await;
    let (records, _) = placed_records(1);
    let mut damaged = stamped_placed_prepare(&partition, 1, 0, &records[0]);
    // Inside the placement: where the blob starts in its object.
    damaged.as_mut_slice()[std::mem::size_of::<PrepareHeader>() + COMMAND_HEADER_SIZE + 16] ^= 1;

    let appended = Box::pin(partition.append_received_send_messages_to_journal(damaged)).await;

    assert!(appended.is_err());
    assert_eq!(partition.dirty_offset.load(Ordering::Relaxed), 0);
}

#[compio::test]
async fn given_a_backup_when_a_placed_batch_carries_another_offset_should_refuse_it() {
    let (_directory, mut partition) = Box::pin(placing_partition()).await;
    let (records, _) = placed_records(1);
    let ahead = stamped_placed_prepare(&partition, 1, 5, &records[0]);

    let appended = Box::pin(partition.append_received_send_messages_to_journal(ahead)).await;

    assert!(appended.is_err());
}

/// The segments hold what the WAL counts, so a segment of placed batches
/// fills by their size and not by the size of the batches they stand for.
#[compio::test]
async fn given_a_segment_that_fits_two_placed_batches_when_a_third_commits_should_roll() {
    let (directory, mut partition) = Box::pin(placing_partition()).await;
    let segment_size = IggyByteSize::from(2 * PLACED_BATCH_SIZE as u64);
    partition.runtime_options.segment_size = Some(segment_size);
    partition.log.active_segment_mut().max_size = segment_size;
    let (records, _) = placed_records(5);

    Box::pin(commit_records(&mut partition, 1, &records)).await;

    let starts: Vec<_> = partition
        .log
        .segments()
        .iter()
        .map(|segment| segment.start_offset)
        .collect();
    assert_eq!(starts, [0, 2, 4]);
    let stored: Vec<_> = segment_file_bytes(directory.path())
        .iter()
        .map(|bytes| placed_batches_in(bytes).len())
        .collect();
    assert_eq!(stored, [2, 2, 1]);
}
