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

/// A placing partition under `root`, where boot recovery finds its files,
/// with segments that fit two placed batches and `count` batches committed.
async fn placing_partition_on_disk(
    root: &Path,
    count: u64,
) -> (PartitionsConfig, IggyPartition<IggyMessageBus>) {
    let mut config = repair_config();
    config.path_layout.streams_root = root.to_string_lossy().into_owned();
    let directory = config.get_partition_path(1, 1, 0);
    std::fs::create_dir_all(&directory).unwrap();
    let mut partition = test_partition_with_replicas(3);
    partition.set_places_batches(true);
    partition.set_partition_dir(directory);
    partition.log.retire_front().unwrap();
    partition.install_empty_segment(&config, 0).await.unwrap();
    partition.runtime_options.segment_size = Some(placed_segment_size());
    partition.log.active_segment_mut().max_size = placed_segment_size();
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    let (records, _) = placed_records(count);
    Box::pin(commit_records(&mut partition, 1, &records)).await;
    (config, partition)
}

fn placed_segment_size() -> IggyByteSize {
    IggyByteSize::from(2 * PLACED_BATCH_SIZE as u64)
}

async fn recover(
    config: &PartitionsConfig,
) -> Result<crate::RecoveredLog, crate::PartitionRecoveryError> {
    crate::load_placed_segments(
        config,
        IggyNamespace::new(1, 1, 0),
        placed_segment_size(),
        &PartitionStats::default(),
        None,
    )
    .await
}

/// `(start offset, end offset, size)` of each recovered segment.
fn bounds(recovered: &crate::RecoveredLog) -> Vec<(u64, u64, u64)> {
    recovered
        .segments
        .iter()
        .map(|recovered| {
            let segment = &recovered.segment;
            (
                segment.start_offset,
                segment.end_offset,
                segment.size.as_bytes_u64(),
            )
        })
        .collect()
}

fn log_path(config: &PartitionsConfig, start_offset: u64) -> std::path::PathBuf {
    Path::new(&config.get_partition_path(1, 1, 0)).join(format!("{start_offset:020}.log"))
}

#[compio::test]
async fn given_segments_of_placed_batches_when_booting_should_recover_their_bounds() {
    let root = tempfile::tempdir().unwrap();
    let (config, partition) = Box::pin(placing_partition_on_disk(root.path(), 5)).await;
    drop(partition);
    let placed = PLACED_BATCH_SIZE as u64;

    let recovered = Box::pin(recover(&config)).await.unwrap();

    assert_eq!(
        bounds(&recovered),
        [(0, 1, 2 * placed), (2, 3, 2 * placed), (4, 4, placed)]
    );
    let last = &recovered.segments[2].segment;
    assert!(!last.sealed);
    // `journal_send_record` stamps each prepare with its op.
    assert_eq!(last.start_timestamp, 5);
    assert_eq!(recovered.segments[0].segment.start_timestamp, 1);
    assert_eq!(recovered.segments[0].segment.end_timestamp, 2);
}

/// A segment of whole batches is not what this recovery reads, and it must
/// not take one for a segment that merely lost its tail.
#[compio::test]
async fn given_a_segment_of_whole_batches_when_recovering_placed_batches_should_recover_nothing_from_it()
 {
    let root = tempfile::tempdir().unwrap();
    let (config, partition) = Box::pin(placing_partition_on_disk(root.path(), 1)).await;
    drop(partition);
    let whole = build_segment_record(IggyNamespace::new(1, 1, 0), 0);
    std::fs::write(log_path(&config, 0), whole).unwrap();

    let recovered = Box::pin(recover(&config)).await.unwrap();

    assert_eq!(bounds(&recovered), [(0, 0, 0)]);
}

#[compio::test]
async fn given_a_torn_last_placed_batch_when_booting_should_recover_the_batches_before_it() {
    let root = tempfile::tempdir().unwrap();
    let (config, partition) = Box::pin(placing_partition_on_disk(root.path(), 3)).await;
    drop(partition);
    // The last segment holds one placed batch. A crash cut the next one.
    let path = log_path(&config, 2);
    let mut bytes = std::fs::read(&path).unwrap();
    assert_eq!(bytes.len(), PLACED_BATCH_SIZE);
    let (torn, _) = placed_record(3);
    bytes.extend_from_slice(&torn[..PLACED_BATCH_SIZE - 7]);
    std::fs::write(&path, &bytes).unwrap();
    let placed = PLACED_BATCH_SIZE as u64;

    let recovered = Box::pin(recover(&config)).await.unwrap();

    assert_eq!(bounds(&recovered), [(0, 1, 2 * placed), (2, 2, placed)]);
    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        placed,
        "the torn tail is cut off"
    );
}

/// The batch after the damaged one was appended later, so cutting the
/// segment at the damage would erase it.
#[compio::test]
async fn given_a_damaged_placed_batch_before_an_intact_one_when_booting_should_refuse_the_partition()
 {
    let root = tempfile::tempdir().unwrap();
    let (mut config, partition) = Box::pin(placing_partition_on_disk(root.path(), 0)).await;
    drop(partition);
    // One segment that holds three placed batches, the middle one damaged.
    config.segment_size = IggyByteSize::from(4 * PLACED_BATCH_SIZE as u64);
    let mut bytes = Vec::new();
    for offset in 0..3 {
        let (record, _) = placed_record(offset);
        let mut placed = PlacedBatch::decode(&record).unwrap();
        placed.header.base_offset = offset;
        placed.header.base_timestamp = 1 + offset;
        let mut stamped = vec![0; PLACED_BATCH_SIZE];
        placed.encode_into(&mut stamped);
        bytes.extend_from_slice(&stamped);
    }
    bytes[PLACED_BATCH_SIZE + COMMAND_HEADER_SIZE + 16] ^= 1;
    std::fs::write(log_path(&config, 0), &bytes).unwrap();

    let recovered = Box::pin(recover(&config)).await;

    assert!(matches!(
        recovered,
        Err(crate::PartitionRecoveryError::Refused {
            reason: crate::PartitionRecoveryRefusal::InteriorDamage { .. },
            ..
        })
    ));
    assert_eq!(std::fs::read(log_path(&config, 0)).unwrap(), bytes);
}

#[compio::test]
async fn given_a_placed_batch_at_another_offset_when_booting_should_refuse_the_partition() {
    let root = tempfile::tempdir().unwrap();
    let (config, partition) = Box::pin(placing_partition_on_disk(root.path(), 0)).await;
    drop(partition);
    let (record, _) = placed_record(0);
    let mut placed = PlacedBatch::decode(&record).unwrap();
    placed.header.base_offset = 9;
    let mut stamped = vec![0; PLACED_BATCH_SIZE];
    placed.encode_into(&mut stamped);
    std::fs::write(log_path(&config, 0), &stamped).unwrap();

    let recovered = Box::pin(recover(&config)).await;

    assert!(matches!(
        recovered,
        Err(crate::PartitionRecoveryError::Refused {
            reason: crate::PartitionRecoveryRefusal::OffsetDiscontinuity { .. },
            ..
        })
    ));
}

#[compio::test]
async fn given_a_rebooted_placing_partition_when_more_placed_batches_commit_should_continue_its_offsets()
 {
    let root = tempfile::tempdir().unwrap();
    let (config, partition) = Box::pin(placing_partition_on_disk(root.path(), 3)).await;
    drop(partition);
    let directory = config.get_partition_path(1, 1, 0);
    let recovered = Box::pin(recover(&config)).await.unwrap();
    let mut partition = test_partition_with_replicas(3);
    partition.set_places_batches(true);
    partition.set_partition_dir(directory.clone());
    partition.runtime_options.segment_size = Some(placed_segment_size());
    partition.log.retire_front().unwrap();
    crate::partition_storage::hydrate_partition_log(&mut partition, &directory, 1, 1, 0, recovered)
        .await
        .unwrap();
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    // The server restores the offsets from the recovered segments.
    let recovered_end = partition.log.active_segment().end_offset;
    assert_eq!(recovered_end, 2);
    partition.offset.store(recovered_end, Ordering::Release);
    partition
        .dirty_offset
        .store(recovered_end, Ordering::Relaxed);
    partition.set_offset_space_used(true);

    // The test partition keeps no WAL, so its ops start over. The offsets
    // are what the segments carry across the reboot.
    let (records, _) = placed_records(2);
    Box::pin(commit_records(&mut partition, 1, &records)).await;

    assert_eq!(partition.offset.load(Ordering::Acquire), 4);
    let stored: Vec<_> = segment_file_bytes(Path::new(&directory))
        .iter()
        .flat_map(|bytes| placed_batches_in(bytes))
        .map(|placed| placed.header.base_offset)
        .collect();
    assert_eq!(stored, [0, 1, 2, 3, 4]);
}

/// A placing partition whose segments fit two placed batches.
pub(super) async fn placing_partition_of_small_segments()
-> (tempfile::TempDir, IggyPartition<IggyMessageBus>) {
    let (directory, mut partition) = Box::pin(placing_partition()).await;
    partition.runtime_options.segment_size = Some(placed_segment_size());
    partition.log.active_segment_mut().max_size = placed_segment_size();
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    (directory, partition)
}

#[compio::test]
async fn given_a_placing_origin_when_a_placing_replica_installs_its_offer_should_hold_the_same_placed_batches()
 {
    // Two partitions on the test's own stack leave too little of it for the
    // install.
    let (origin_directory, origin) = Box::pin(placing_partition_of_small_segments()).await;
    let mut origin = Box::new(origin);
    let (records, placements) = placed_records(5);
    Box::pin(commit_records(&mut origin, 1, &records)).await;
    let offer = Box::pin(serve_offer(&mut origin)).await;
    let (receiver_directory, receiver) = Box::pin(placing_partition_of_small_segments()).await;
    let mut receiver = Box::new(receiver);

    Box::pin(install_offer_with_reuse(&mut receiver, &offer)).await;

    assert_eq!(
        segment_file_bytes(receiver_directory.path()),
        segment_file_bytes(origin_directory.path())
    );
    let installed: Vec<_> = segment_file_bytes(receiver_directory.path())
        .iter()
        .flat_map(|bytes| placed_batches_in(bytes))
        .collect();
    assert_eq!(installed.len(), 5);
    for (index, (placed, placement)) in installed.iter().zip(&placements).enumerate() {
        assert_eq!(placed.header.base_offset, index as u64);
        assert_eq!(placed.placement, *placement);
    }
    let starts: Vec<_> = receiver
        .log
        .segments()
        .iter()
        .map(|segment| (segment.start_offset, segment.end_offset))
        .collect();
    assert_eq!(starts, [(0, 1), (2, 3), (4, 4)]);
}

/// The offer of a placing origin that committed `count` placed batches. The
/// origin is dropped, and its directory holds the segments the offer names.
async fn placed_offer(
    count: u64,
) -> (
    tempfile::TempDir,
    Rc<crate::state_transfer::PartitionStateTransferOffer>,
) {
    let (directory, mut origin) = Box::pin(placing_partition_of_small_segments()).await;
    let (records, _) = placed_records(count);
    Box::pin(commit_records(&mut origin, 1, &records)).await;
    let offer = Box::pin(serve_offer(&mut origin)).await;
    (directory, offer)
}

/// A replica walks a transferred segment as what its own segments hold, so
/// placed batches never land in a partition of whole batches.
#[compio::test]
async fn given_a_placing_origin_when_a_replica_of_whole_batches_spills_its_segment_should_refuse_it()
 {
    let (_origin_directory, offer) = Box::pin(placed_offer(2)).await;
    let config = repair_config();
    let (_receiver_directory, receiver) =
        Box::pin(disk_poll_partition_with_replicas(&config, 3)).await;
    let entry = offer.segments[0].entry;
    let bytes = offered_segment_bytes(&offer).remove(0);

    let spilled = Box::pin(receiver.spill_transfer_segment(&entry, bytes, None)).await;

    assert!(matches!(
        spilled,
        Err(crate::state_transfer::SpillError::Walk(_))
    ));
}

#[compio::test]
async fn given_a_damaged_placed_batch_in_a_transferred_segment_when_spilling_should_refuse_it() {
    let (_origin_directory, offer) = Box::pin(placed_offer(2)).await;
    let (_receiver_directory, receiver) = Box::pin(placing_partition_of_small_segments()).await;
    let mut entry = offer.segments[0].entry;
    let mut bytes = offered_segment_bytes(&offer).remove(0);
    // Inside the second placement, with a checksum that covers the damage, so
    // only the walk can tell.
    bytes[PLACED_BATCH_SIZE + COMMAND_HEADER_SIZE + 16] ^= 1;
    entry.checksum = consensus::state_artifact_checksum(&bytes);

    let spilled = Box::pin(receiver.spill_transfer_segment(&entry, bytes, None)).await;

    assert!(matches!(
        spilled,
        Err(crate::state_transfer::SpillError::Walk(_))
    ));
}

/// A prepare a primary replicates for a one-message batch it placed in the
/// object store, chained to `parent`.
fn placed_segment_prepare(op: u64, parent: u128, offset: u64) -> Message<PrepareHeader> {
    let whole = checksummed_segment_prepare(op, parent, offset, b"payload");
    let header_size = size_of::<PrepareHeader>();
    let batch = BatchHeader::decode(&whole.as_slice()[header_size..]).unwrap();
    let blob = &whole.as_slice()[header_size + COMMAND_HEADER_SIZE..];
    let placed = PlacedBatch::new(batch, Placement::of(7, op * 4096, blob));
    let total = header_size + PLACED_BATCH_SIZE;
    let mut prepare = Message::<PrepareHeader>::new(total);
    placed.encode_into(&mut prepare.as_mut_slice()[header_size..]);
    let template = *whole.header();
    prepare.transmute_header(|_, header: &mut PrepareHeader| {
        *header = template;
        header.size = u32::try_from(total).unwrap();
        header.checksum = header.identity_checksum();
    })
}

/// Room for two placed batches in a segment.
const PLACED_PER_SEGMENT: u64 = 2;

/// A backup that places its batches, with a prepare WAL under `root`.
async fn placing_backup_with_wal(root: &Path) -> (PartitionsConfig, IggyPartition<RecordingBus>) {
    let mut config = repair_config();
    config.path_layout.streams_root = root.to_string_lossy().into_owned();
    let directory = config.get_partition_path(1, 1, 0);
    std::fs::create_dir_all(&directory).unwrap();
    let (mut partition, _) = recording_partition_at(1, 3);
    partition.set_places_batches(true);
    partition.set_partition_dir(directory);
    partition.runtime_options.durability = iggy_common::Durability::Replicated;
    partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Replicated;
    partition.runtime_options.segment_size = Some(placed_segment_size());
    partition.log.active_segment_mut().max_size = placed_segment_size();
    partition.open_persistence().await.unwrap();
    partition.log.retire_front().unwrap();
    partition.install_empty_segment(&config, 0).await.unwrap();
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    (config, partition)
}

/// Replicate and commit one placed batch per offset, each as op
/// `offset + 1`. Returns the checksum the next prepare chains to.
async fn replicate_placed(
    partition: &mut IggyPartition<RecordingBus>,
    config: &PartitionsConfig,
    offsets: std::ops::RangeInclusive<u64>,
    mut parent: u128,
) -> u128 {
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    for offset in offsets {
        let op = offset + 1;
        let prepare = placed_segment_prepare(op, parent, offset);
        parent = prepare.header().checksum;
        let writer = hold_wal_writer(&persistence);
        Box::pin(partition.on_replicate(prepare)).await;
        writer.await;
        Box::pin(commit_on_heartbeat(partition, config, op)).await;
        Box::pin(settle_partition_io(partition, config)).await;
    }
    parent
}

fn stored_offsets(directory: &str) -> Vec<u64> {
    segment_file_bytes(Path::new(directory))
        .iter()
        .flat_map(|bytes| placed_batches_in(bytes))
        .map(|placed| placed.header.base_offset)
        .collect()
}

/// With a WAL the WAL writes the placed batch into the segment, and the
/// partition checks what it wrote before it counts the batch as stored.
#[compio::test]
async fn given_a_backup_with_a_wal_when_placed_batches_commit_should_hold_them_in_its_segments() {
    let root = tempfile::tempdir().unwrap();
    let (config, mut partition) = Box::pin(placing_backup_with_wal(root.path())).await;
    let directory = config.get_partition_path(1, 1, 0);

    Box::pin(replicate_placed(&mut partition, &config, 0..=2, 0)).await;

    assert!(partition.fatal().is_none());
    assert_eq!(partition.consensus().commit_min(), 3);
    assert_eq!(partition.offset.load(Ordering::Acquire), 2);
    assert_eq!(stored_offsets(&directory), [0, 1, 2]);
    let segments: Vec<_> = partition
        .log
        .segments()
        .iter()
        .map(|segment| (segment.start_offset, segment.size.as_bytes_u64()))
        .collect();
    let placed = PLACED_BATCH_SIZE as u64;
    assert_eq!(segments, [(0, PLACED_PER_SEGMENT * placed), (2, placed)]);
}

/// The server's order at boot: the WAL is read first, and recovery cuts the
/// segments back to its checkpoint before the WAL replays what follows.
#[compio::test]
async fn given_a_restarted_backup_with_a_wal_when_more_placed_batches_commit_should_continue_from_what_it_held()
 {
    let root = tempfile::tempdir().unwrap();
    let (config, mut partition) = Box::pin(placing_backup_with_wal(root.path())).await;
    let directory = config.get_partition_path(1, 1, 0);
    let parent = Box::pin(replicate_placed(&mut partition, &config, 0..=2, 0)).await;
    hold_wal_writer(partition.persistence.as_ref().unwrap()).await;
    drop(partition);

    let wal = Path::new(&directory).join("prepares-0");
    let recovered_wal = PartitionPersistence::open(&wal, IggyNamespace::new(1, 1, 0).inner(), 0)
        .await
        .unwrap();
    let recovered = crate::load_placed_segments(
        &config,
        IggyNamespace::new(1, 1, 0),
        placed_segment_size(),
        &PartitionStats::default(),
        recovered_wal.0.segment_checkpoint(),
    )
    .await
    .unwrap();
    let (mut partition, _) = recording_partition_at(1, 3);
    partition.set_places_batches(true);
    partition.set_partition_dir(directory.clone());
    partition.runtime_options.durability = iggy_common::Durability::Replicated;
    partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Replicated;
    partition.runtime_options.segment_size = Some(placed_segment_size());
    partition.log.retire_front().unwrap();
    crate::partition_storage::hydrate_partition_log(&mut partition, &directory, 1, 1, 0, recovered)
        .await
        .unwrap();
    partition
        .open_persistence_with_recovered(
            journal::partition_journal::PARTITION_WAL_BYTES_MAX,
            std::time::Duration::ZERO,
            Some(recovered_wal),
        )
        .await
        .unwrap();
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );

    Box::pin(replicate_placed(&mut partition, &config, 3..=4, parent)).await;

    assert!(partition.fatal().is_none());
    assert_eq!(partition.consensus().commit_min(), 5);
    assert_eq!(stored_offsets(&directory), [0, 1, 2, 3, 4]);
}

/// A view change discards the placed batches that did not commit. Their
/// offsets go back to the batches the new view replicates in their place.
#[compio::test]
async fn given_uncommitted_placed_batches_when_a_view_change_discards_them_should_take_the_replacements_at_their_offsets()
 {
    let root = tempfile::tempdir().unwrap();
    let (config, mut partition) = Box::pin(placing_backup_with_wal(root.path())).await;
    let directory = config.get_partition_path(1, 1, 0);
    let committed = Box::pin(replicate_placed(&mut partition, &config, 0..=0, 0)).await;
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    let mut parent = committed;
    for offset in 1..=2 {
        let prepare = placed_segment_prepare(offset + 1, parent, offset);
        parent = prepare.header().checksum;
        let writer = hold_wal_writer(&persistence);
        Box::pin(partition.on_replicate(prepare)).await;
        writer.await;
    }
    assert_eq!(partition.dirty_offset.load(Ordering::Relaxed), 2);
    assert_eq!(partition.offset.load(Ordering::Acquire), 0);

    Box::pin(partition.truncate_uncommitted_from(2))
        .await
        .unwrap();

    assert_eq!(partition.dirty_offset.load(Ordering::Relaxed), 0);
    assert_eq!(
        partition.log.active_segment().current_position,
        PLACED_BATCH_SIZE as u64
    );
    Box::pin(replicate_placed(&mut partition, &config, 1..=1, committed)).await;
    assert!(partition.fatal().is_none());
    assert_eq!(partition.consensus().commit_min(), 2);
    assert_eq!(partition.offset.load(Ordering::Acquire), 1);
    assert_eq!(stored_offsets(&directory), [0, 1]);
}

/// The request the node's upload path hands a partition for a client's send
/// of `count` messages, once their blob is stored.
fn placed_client_request(
    partition: &IggyPartition<IggyMessageBus>,
    request: u64,
    count: u32,
) -> Message<RoutedRequestHeader> {
    let admitted = checksumless_send_request(partition.namespace(), count).transmute_header(
        |mut header, next: &mut RoutedRequestHeader| {
            header.request = request;
            header.metadata_watermark = partition.required_metadata_frontier;
            *next = header;
        },
    );
    server_common::send_messages::place_request(&admitted, 7, request * 4096).unwrap()
}

/// A primary alone in its group, so a request commits without a backup.
async fn placing_primary() -> (tempfile::TempDir, IggyPartition<IggyMessageBus>) {
    let config = repair_config();
    let (directory, mut partition) = Box::pin(disk_poll_partition_with_replicas(&config, 1)).await;
    partition.set_places_batches(true);
    (directory, partition)
}

#[compio::test]
async fn given_a_placing_primary_when_the_upload_path_hands_it_placed_requests_should_commit_them()
{
    let (directory, mut partition) = Box::pin(placing_primary()).await;
    let config = repair_config();

    for (request, count) in [(1, 2), (2, 3)] {
        let placed = placed_client_request(&partition, request, count);
        Box::pin(partition.on_placed_request(placed, None)).await;
        assert_eq!(
            partition.consensus().sequencer().current_sequence(),
            request,
            "the placed request became a prepare"
        );
        partition.consensus().advance_commit_max(request);
        Box::pin(settle_partition_io(&mut partition, &config)).await;
    }

    assert_eq!(partition.offset.load(Ordering::Acquire), 4);
    let stored: Vec<_> = segment_file_bytes(directory.path())
        .iter()
        .flat_map(|bytes| placed_batches_in(bytes))
        .map(|placed| {
            (
                placed.header.base_offset,
                placed.header.message_count,
                placed.placement.position,
            )
        })
        .collect();
    assert_eq!(stored, [(0, 2, 4096), (2, 3, 8192)]);
}

/// A send that still carries its messages was stored nowhere, and a
/// placing partition has no place for the messages.
#[compio::test]
async fn given_a_placing_primary_when_a_send_arrives_with_its_messages_should_admit_nothing() {
    let (_directory, mut partition) = Box::pin(placing_primary()).await;
    let config = repair_config();
    let whole = checksumless_send_request(partition.namespace(), 1).transmute_header(
        |mut header, next: &mut RoutedRequestHeader| {
            header.metadata_watermark = partition.required_metadata_frontier;
            *next = header;
        },
    );

    Box::pin(partition.on_request(whole, None)).await;
    Box::pin(settle_partition_io(&mut partition, &config)).await;

    assert_eq!(partition.consensus().sequencer().current_sequence(), 0);
    assert_eq!(partition.dirty_offset.load(Ordering::Relaxed), 0);
}

#[compio::test]
async fn given_a_primary_of_whole_batches_when_a_placed_request_arrives_should_admit_nothing() {
    let config = repair_config();
    let (_directory, mut partition) = Box::pin(disk_poll_partition_with_replicas(&config, 1)).await;
    let placed = placed_client_request(&partition, 1, 1);

    Box::pin(partition.on_placed_request(placed, None)).await;
    Box::pin(settle_partition_io(&mut partition, &config)).await;

    assert_eq!(partition.consensus().sequencer().current_sequence(), 0);
}

/// Only a placed batch for this partition goes in through the placed entry.
#[compio::test]
async fn given_a_placing_primary_when_the_placed_entry_gets_another_body_should_admit_nothing() {
    let (_directory, mut partition) = Box::pin(placing_primary()).await;
    let config = repair_config();
    let whole = checksumless_send_request(partition.namespace(), 1);
    let mut damaged = placed_client_request(&partition, 1, 1);
    damaged.as_mut_slice()[size_of::<RoutedRequestHeader>() + COMMAND_HEADER_SIZE + 16] ^= 1;
    let elsewhere = IggyNamespace::new(1, 1, 5);
    let other_partition = server_common::send_messages::place_request(
        &checksumless_send_request(elsewhere, 1).transmute_header(
            |mut header, next: &mut RoutedRequestHeader| {
                header.group = partition.namespace().inner();
                *next = header;
            },
        ),
        7,
        0,
    )
    .unwrap();

    for request in [whole, damaged, other_partition] {
        Box::pin(partition.on_placed_request(request, None)).await;
        Box::pin(settle_partition_io(&mut partition, &config)).await;
    }

    assert_eq!(partition.consensus().sequencer().current_sequence(), 0);
}

/// A store whose every read fails, so a poll that passes must not have read.
struct NoStore;

impl crate::RemoteObjects for NoStore {
    fn get_range<'a>(
        &'a self,
        key: &'a str,
        _range: std::ops::Range<u64>,
    ) -> futures::future::LocalBoxFuture<
        'a,
        Result<iggy_object_store::ObjectRange, iggy_object_store::ObjectStoreError>,
    > {
        Box::pin(std::future::ready(Err(
            iggy_object_store::ObjectStoreError::NotFound {
                key: key.to_owned(),
            },
        )))
    }
}

pub(super) const FRESH_OBJECT: u128 = 0x51;

/// The same batches for two partitions: as whole records, and as placed
/// records whose blobs sit back to back in one fresh object.
pub(super) fn whole_and_placed(count: u64) -> (Vec<Vec<u8>>, Vec<Vec<u8>>, bytes::Bytes) {
    whole_and_placed_in(FRESH_OBJECT, count)
}

/// [`whole_and_placed`] with the blobs in the fresh object `object_id`.
pub(super) fn whole_and_placed_in(
    object_id: u128,
    count: u64,
) -> (Vec<Vec<u8>>, Vec<Vec<u8>>, bytes::Bytes) {
    let whole = segment_records(count);
    let mut object = iggy_object_store::FreshObject::new();
    let placed = whole
        .iter()
        .map(|record| {
            let mut header = BatchHeader::decode(record).unwrap();
            header.batch_checksum = 0;
            let blob = &record[COMMAND_HEADER_SIZE..];
            let placement = Placement::of(object_id, object.push(blob), blob);
            let mut placed = vec![0; PLACED_BATCH_SIZE];
            PlacedBatch::new(header, placement).encode_into(&mut placed);
            placed
        })
        .collect();
    (whole, placed, object.into_bytes())
}

/// A store that holds `object` as the fresh object the placed records name.
pub(super) async fn store_with(object: bytes::Bytes) -> Rc<iggy_object_store::MemoryStore> {
    use iggy_object_store::ObjectStore;
    let store = Rc::new(iggy_object_store::MemoryStore::new());
    store
        .put(
            &super::remote_poll::layout().fresh_key(FRESH_OBJECT),
            iggy_object_store::PutBody::from_bytes(object),
            &iggy_object_store::PutOptions::default(),
        )
        .await
        .unwrap();
    store
}

/// What every poll of [`super::remote_poll::polled_all`] returns from a
/// partition of whole batches that committed `records`.
async fn polls_of_whole_batches(records: &[Vec<u8>]) -> Vec<(Option<u64>, Vec<u8>)> {
    let config = repair_config();
    let (_directory, mut partition) = Box::pin(disk_poll_partition_with_replicas(&config, 3)).await;
    let batch_len = records[0].len() as u64;
    let segment_size = IggyByteSize::from(BATCHES_PER_SEGMENT * batch_len);
    partition.runtime_options.segment_size = Some(segment_size);
    partition.log.active_segment_mut().max_size = segment_size;
    Box::pin(commit_records(&mut partition, 1, records)).await;
    Box::pin(super::remote_poll::polled_all(&mut partition)).await
}

/// A placing partition of small segments that committed `placed`.
async fn placing_partition_with(
    placed: &[Vec<u8>],
) -> (tempfile::TempDir, IggyPartition<IggyMessageBus>) {
    let (directory, mut partition) = Box::pin(placing_partition_of_small_segments()).await;
    Box::pin(commit_records(&mut partition, 1, placed)).await;
    (directory, partition)
}

/// The messages of a placed batch are in the object store, and a poll puts
/// the batch back together byte for byte.
#[compio::test]
async fn given_the_same_batches_placed_when_polling_should_return_the_bytes_of_a_partition_of_whole_batches()
 {
    let (whole, placed, object) = whole_and_placed(RECEIVER_OPS);
    let expected = Box::pin(polls_of_whole_batches(&whole)).await;
    assert_eq!(expected[0].0, Some(RECEIVER_OPS - 1));
    let (_directory, mut partition) = Box::pin(placing_partition_with(&placed)).await;
    super::remote_poll::attach(&mut partition, Box::pin(store_with(object)).await);
    assert_eq!(
        partition.log.segments().len(),
        3,
        "the batches span segments"
    );

    let polled = Box::pin(super::remote_poll::polled_all(&mut partition)).await;

    assert_eq!(polled, expected);
}

#[compio::test]
async fn given_a_fresh_object_this_shard_stored_when_polling_should_not_ask_the_store() {
    let (whole, placed, object) = whole_and_placed(RECEIVER_OPS);
    let expected = Box::pin(polls_of_whole_batches(&whole)).await;
    let (_directory, mut partition) = Box::pin(placing_partition_with(&placed)).await;
    let remote = Rc::new(crate::RemoteSegments::new(
        Rc::new(NoStore),
        super::remote_poll::layout(),
    ));
    remote.cache_fresh_object(FRESH_OBJECT, object);
    partition.set_remote_segments(remote);

    let polled = Box::pin(super::remote_poll::polled_all(&mut partition)).await;

    assert_eq!(polled, expected);
}

async fn poll_error(partition: &mut IggyPartition<IggyMessageBus>) -> IggyError {
    let args = PollingArgs::new(iggy_common::PollingStrategy::offset(0), 10, false);
    let result = partition
        .build_poll_plan(PollingConsumer::Consumer(1, 0), &args, false)
        .execute()
        .await;
    assert_eq!(result.message_count, 0);
    result.read_error.unwrap()
}

/// A poll that skipped the batch would hand the consumer a gap.
#[compio::test]
async fn given_messages_that_are_not_the_placed_ones_when_polling_should_fail_the_read() {
    let (_whole, placed, object) = whole_and_placed(2);
    let (_directory, mut partition) = Box::pin(placing_partition_with(&placed)).await;
    let mut damaged = object.to_vec();
    let last = damaged.len() - 1;
    damaged[last] ^= 1;
    super::remote_poll::attach(&mut partition, Box::pin(store_with(damaged.into())).await);

    assert_eq!(
        Box::pin(poll_error(&mut partition)).await,
        IggyError::TieredReadFailed
    );
}

#[compio::test]
async fn given_a_missing_fresh_object_when_polling_should_fail_the_read() {
    let (_whole, placed, _object) = whole_and_placed(2);
    let (_directory, mut partition) = Box::pin(placing_partition_with(&placed)).await;
    super::remote_poll::attach(&mut partition, Rc::new(NoStore));

    assert_eq!(
        Box::pin(poll_error(&mut partition)).await,
        IggyError::TieredReadFailed
    );
}

/// A shard connects its store a moment after it starts.
#[compio::test]
async fn given_no_store_yet_when_polling_should_ask_for_a_retry() {
    let (_whole, placed, _object) = whole_and_placed(2);
    let (_directory, mut partition) = Box::pin(placing_partition_with(&placed)).await;

    assert_eq!(
        Box::pin(poll_error(&mut partition)).await,
        IggyError::TransientNotAccepted
    );
}

pub(super) type MemoryWriter =
    crate::ExecutorLongTermWriter<iggy_object_store::InlineTierExecutor<SharedStore>>;

/// A store that the rewrites and the polls of a test both reach.
#[derive(Clone)]
pub(super) struct SharedStore(pub(super) Rc<iggy_object_store::MemoryStore>);

impl iggy_object_store::ObjectStore for SharedStore {
    async fn put(
        &self,
        key: &str,
        body: iggy_object_store::PutBody,
        options: &iggy_object_store::PutOptions,
    ) -> Result<(), iggy_object_store::ObjectStoreError> {
        self.0.put(key, body, options).await
    }

    async fn get_range(
        &self,
        key: &str,
        range: std::ops::Range<u64>,
    ) -> Result<iggy_object_store::ObjectRange, iggy_object_store::ObjectStoreError> {
        self.0.get_range(key, range).await
    }

    async fn head(
        &self,
        key: &str,
    ) -> Result<iggy_object_store::ObjectMeta, iggy_object_store::ObjectStoreError> {
        self.0.head(key).await
    }

    async fn delete(
        &self,
        keys: &[String],
    ) -> Result<iggy_object_store::DeleteOutcome, iggy_object_store::ObjectStoreError> {
        self.0.delete(keys).await
    }

    async fn list(
        &self,
        request: &iggy_object_store::ListRequest<'_>,
    ) -> Result<iggy_object_store::ListPage, iggy_object_store::ObjectStoreError> {
        self.0.list(request).await
    }
}

pub(super) fn writer(store: &Rc<iggy_object_store::MemoryStore>) -> MemoryWriter {
    crate::ExecutorLongTermWriter::new(
        iggy_object_store::InlineTierExecutor::new(SharedStore(Rc::clone(store))),
        super::remote_poll::layout(),
    )
}

/// A primary alone in its group that committed `placed`, whose polls and
/// rewrites read `store`.
pub(super) async fn placing_primary_with(
    placed: &[Vec<u8>],
    store: &Rc<iggy_object_store::MemoryStore>,
) -> (tempfile::TempDir, IggyPartition<IggyMessageBus>) {
    let (directory, mut partition) = Box::pin(placing_primary()).await;
    partition.register_builtin_state_machines().unwrap();
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    super::remote_poll::attach(
        &mut partition,
        Rc::clone(store) as Rc<dyn crate::RemoteObjects>,
    );
    Box::pin(commit_records(&mut partition, 1, placed)).await;
    (directory, partition)
}

/// One pass: start a rewrite, run it, hand its result back, and commit the
/// segment it stored to the long-term log as op `op`.
async fn rewrite_pass(
    partition: &mut IggyPartition<IggyMessageBus>,
    writer: &MemoryWriter,
    op: u64,
) -> Option<iggy_object_store::ManifestSegment> {
    use crate::LongTermWriter;
    let (id, rewrite, scope) = partition.begin_segment_rewrite()?;
    let outcome = writer.write(rewrite, scope).await;
    let segment = partition.finish_segment_rewrite(id, outcome)?;
    assert_eq!(partition.unpublished_segment(), Some(segment));
    commit_published_segment(partition, &segment, op);
    Some(segment)
}

/// Commit the command of `segment` to the long-term log as op `op`.
pub(super) fn commit_published_segment(
    partition: &mut IggyPartition<IggyMessageBus>,
    segment: &iggy_object_store::ManifestSegment,
    op: u64,
) {
    let payload = crate::long_term::PublishSegment(*segment).encode();
    let applied = partition
        .state_machines
        .apply(
            op,
            crate::state_machine::MachineCommand {
                kind: crate::long_term::PUBLISH_SEGMENT,
                payload: &payload,
            },
        )
        .unwrap();
    assert_eq!(applied, crate::state_machine::Applied::Changed);
}

pub(super) async fn object_bytes(store: &iggy_object_store::MemoryStore, key: &str) -> Vec<u8> {
    use iggy_object_store::ObjectStore;
    let size = store.head(key).await.unwrap().size;
    store.get_range(key, 0..size).await.unwrap().bytes.to_vec()
}

pub(super) fn long_term_key(
    start_offset: u64,
    end_offset: u64,
    object: iggy_object_store::SegmentObject,
) -> String {
    super::remote_poll::layout().long_term_key(&iggy_object_store::LongTermKey {
        prefix: crate::object_prefix(IggyNamespace::new(1, 1, 0), 0),
        start_offset,
        end_offset,
        object,
    })
}

/// The bytes of the segment a partition of whole batches holds once it
/// committed `records`.
async fn log_of_whole_batches(records: &[Vec<u8>]) -> Vec<u8> {
    let config = repair_config();
    let (directory, mut partition) = Box::pin(disk_poll_partition_with_replicas(&config, 3)).await;
    Box::pin(commit_records(&mut partition, 1, records)).await;
    segment_file_bytes(directory.path()).concat()
}

/// A reader of the bucket gets the partition's log without a placement or a
/// fresh object: the long-term segment is the `.log` of a partition that
/// kept the same batches whole.
#[compio::test]
async fn given_committed_placed_batches_when_rewriting_should_store_the_segment_a_partition_of_whole_batches_holds()
 {
    let (whole, placed, object) = whole_and_placed(RECEIVER_OPS);
    let expected_log = Box::pin(log_of_whole_batches(&whole)).await;
    let store = Box::pin(store_with(object)).await;
    let (_directory, mut partition) = Box::pin(placing_primary_with(&placed, &store)).await;
    let writer = writer(&store);
    assert_eq!(partition.long_term_floor(), None);

    let segment = Box::pin(rewrite_pass(&mut partition, &writer, 100))
        .await
        .unwrap();

    assert_eq!(
        (segment.start_offset, segment.end_offset),
        (0, RECEIVER_OPS - 1)
    );
    assert_eq!(segment.size, expected_log.len() as u64);
    assert_eq!(segment.crc32c, crc32c::crc32c(&expected_log));
    assert_eq!(
        Box::pin(object_bytes(
            &store,
            &long_term_key(0, RECEIVER_OPS - 1, iggy_object_store::SegmentObject::Log)
        ))
        .await,
        expected_log
    );
    let index = Box::pin(object_bytes(
        &store,
        &long_term_key(0, RECEIVER_OPS - 1, iggy_object_store::SegmentObject::Index),
    ))
    .await;
    assert_eq!(index.len(), crate::iggy_index::IGGY_INDEX_SIZE);
    assert_eq!(partition.long_term_floor(), Some(RECEIVER_OPS - 1));
    assert_eq!(partition.long_term_segments(), [segment]);
    assert_eq!(partition.unpublished_segment(), None);
    // Nothing is left above the floor.
    assert!(partition.begin_segment_rewrite().is_none());
}

#[compio::test]
async fn given_batches_committed_after_a_rewrite_when_rewriting_again_should_continue_at_the_floor()
{
    let (_whole, placed, object) = whole_and_placed(4);
    let store = Box::pin(store_with(object)).await;
    let (_directory, mut partition) = Box::pin(placing_primary_with(&placed[..3], &store)).await;
    let writer = writer(&store);
    let first = Box::pin(rewrite_pass(&mut partition, &writer, 100))
        .await
        .unwrap();
    assert_eq!((first.start_offset, first.end_offset), (0, 2));

    Box::pin(commit_records(&mut partition, 4, &placed[3..])).await;
    let second = Box::pin(rewrite_pass(&mut partition, &writer, 101))
        .await
        .unwrap();

    assert_eq!((second.start_offset, second.end_offset), (3, 3));
    assert_eq!(partition.long_term_segments(), [first, second]);
}

/// The segment is stored, and its command has yet to commit. The primary
/// rewrites nothing until it does, and keeps the segment to send again.
#[compio::test]
async fn given_a_stored_segment_whose_command_is_not_committed_when_a_pass_runs_should_wait_for_it()
{
    use crate::LongTermWriter;
    let (_whole, placed, object) = whole_and_placed(2);
    let store = Box::pin(store_with(object)).await;
    let (_directory, mut partition) = Box::pin(placing_primary_with(&placed, &store)).await;
    let writer = writer(&store);
    let (id, rewrite, scope) = partition.begin_segment_rewrite().unwrap();
    assert!(partition.begin_segment_rewrite().is_none(), "one at a time");
    let outcome = writer.write(rewrite, scope).await;

    let segment = partition.finish_segment_rewrite(id, outcome).unwrap();

    assert_eq!(partition.unpublished_segment(), Some(segment));
    assert!(partition.begin_segment_rewrite().is_none());
    assert_eq!(partition.long_term_floor(), None);
}

#[compio::test]
async fn given_a_backup_when_a_pass_runs_should_rewrite_nothing() {
    let (_whole, placed, _object) = whole_and_placed(2);
    let (_directory, mut partition) = Box::pin(placing_partition()).await;
    partition.register_builtin_state_machines().unwrap();
    Box::pin(commit_records(&mut partition, 1, &placed)).await;
    let view = partition.consensus.view() + 1;
    partition.consensus.set_view(view);
    assert!(!partition.consensus.is_primary());

    assert!(partition.begin_segment_rewrite().is_none());
}

#[compio::test]
async fn given_a_partition_of_whole_batches_when_a_pass_runs_should_rewrite_nothing() {
    let config = repair_config();
    let (_directory, mut partition) = Box::pin(disk_poll_partition_with_replicas(&config, 1)).await;
    Box::pin(commit_records(&mut partition, 1, &segment_records(2))).await;

    assert!(partition.begin_segment_rewrite().is_none());
}

/// The fresh object is gone, so the batches cannot be read. Nothing is
/// stored, and the next pass tries again.
#[compio::test]
async fn given_batches_that_cannot_be_read_when_rewriting_should_store_nothing_and_try_again() {
    use crate::LongTermWriter;
    let (_whole, placed, _object) = whole_and_placed(2);
    let store = Rc::new(iggy_object_store::MemoryStore::new());
    let (_directory, mut partition) = Box::pin(placing_primary_with(&placed, &store)).await;
    let writer = writer(&store);
    let (id, rewrite, scope) = partition.begin_segment_rewrite().unwrap();

    let outcome = writer.write(rewrite, scope).await;

    assert!(matches!(outcome, Ok(Err(crate::RewriteError::Read(_)))));
    assert_eq!(partition.finish_segment_rewrite(id, outcome), None);
    assert_eq!(store.keys(), Vec::<String>::new());
    assert!(partition.begin_segment_rewrite().is_some());
}

/// What a primary leaves behind when it loses primacy between storing a
/// segment and committing it: the segment of `placed` in `store`, and no
/// record of it anywhere.
async fn store_segment_of_a_deposed_primary(
    placed: &[Vec<u8>],
    store: &Rc<iggy_object_store::MemoryStore>,
) {
    use crate::LongTermWriter;
    let (_directory, mut partition) = Box::pin(placing_primary_with(placed, store)).await;
    let (_, rewrite, scope) = partition.begin_segment_rewrite().unwrap();
    writer(store).write(rewrite, scope).await.unwrap().unwrap();
}

/// A key names where its segment ends, so the segment a deposed primary
/// left from the same offset is another object. The primary stores its own
/// in one pass, and the other one stays in the store with nothing naming it.
#[compio::test]
async fn given_a_segment_another_primary_stored_from_the_same_offset_when_rewriting_should_store_its_own_and_list_only_that_one()
 {
    use iggy_object_store::SegmentObject;
    let (whole, placed, object) = whole_and_placed(4);
    let expected_log = Box::pin(log_of_whole_batches(&whole)).await;
    let store = Box::pin(store_with(object)).await;
    Box::pin(store_segment_of_a_deposed_primary(&placed[..2], &store)).await;
    let orphan = Box::pin(object_bytes(
        &store,
        &long_term_key(0, 1, SegmentObject::Log),
    ))
    .await;
    let (_directory, mut partition) = Box::pin(placing_primary_with(&placed, &store)).await;

    let segment = Box::pin(rewrite_pass(&mut partition, &writer(&store), 100))
        .await
        .unwrap();

    assert_eq!((segment.start_offset, segment.end_offset), (0, 3));
    assert_eq!(partition.long_term_segments(), [segment]);
    assert_eq!(partition.long_term_floor(), Some(3));
    assert_eq!(
        Box::pin(object_bytes(
            &store,
            &long_term_key(0, 3, SegmentObject::Log)
        ))
        .await,
        expected_log
    );
    assert_eq!(
        Box::pin(object_bytes(
            &store,
            &long_term_key(0, 1, SegmentObject::Log)
        ))
        .await,
        orphan
    );
    assert_eq!(long_term_logs(&store), 2);
    let polled = Box::pin(super::remote_poll::polled_all(&mut partition)).await;
    assert_eq!(polled, Box::pin(polls_of_whole_batches(&whole)).await);
}

/// A segment another primary stored that ends where this one does holds the
/// same bytes under the same key. The rewrite takes it as stored and puts
/// nothing in its place.
#[compio::test]
async fn given_a_segment_another_primary_stored_with_the_same_offsets_when_rewriting_should_take_it_as_its_own()
 {
    let (_whole, placed, object) = whole_and_placed(2);
    let store = Box::pin(store_with(object)).await;
    Box::pin(store_segment_of_a_deposed_primary(&placed, &store)).await;
    let keys = store.keys();
    let (_directory, mut partition) = Box::pin(placing_primary_with(&placed, &store)).await;

    let segment = Box::pin(rewrite_pass(&mut partition, &writer(&store), 100))
        .await
        .unwrap();

    assert_eq!((segment.start_offset, segment.end_offset), (0, 1));
    assert_eq!(store.keys(), keys);
    assert_eq!(long_term_logs(&store), 1);
}

/// The offsets at or below the long-term floor come from the long-term
/// segment and the ones above it from the placed batches, in one poll. The
/// fresh object of the rewritten batches is gone, so nothing below the
/// floor can have come from it.
#[compio::test]
async fn given_a_long_term_floor_inside_the_log_when_polling_across_it_should_return_every_batch_once()
 {
    use iggy_object_store::ObjectStore;
    const SECOND_OBJECT: u128 = 0x52;
    let (first_whole, first_placed, first_object) = whole_and_placed(3);
    let (second_whole, second_placed, second_object) = whole_and_placed_in(SECOND_OBJECT, 2);
    let whole: Vec<_> = first_whole.into_iter().chain(second_whole).collect();
    let expected = Box::pin(polls_of_whole_batches(&whole)).await;
    let store = Box::pin(store_with(first_object)).await;
    let layout = super::remote_poll::layout();
    store
        .put(
            &layout.fresh_key(SECOND_OBJECT),
            iggy_object_store::PutBody::from_bytes(second_object),
            &iggy_object_store::PutOptions::default(),
        )
        .await
        .unwrap();
    let (_directory, mut partition) = Box::pin(placing_primary_with(&first_placed, &store)).await;
    let rewritten = Box::pin(rewrite_pass(&mut partition, &writer(&store), 100))
        .await
        .unwrap();
    assert_eq!(rewritten.end_offset, 2);
    Box::pin(commit_records(&mut partition, 4, &second_placed)).await;
    let deleted = store
        .delete(&[layout.fresh_key(FRESH_OBJECT)])
        .await
        .unwrap();
    assert!(deleted.failed.is_empty());
    // What the rewrite read stays in memory, and must not be what answers.
    super::remote_poll::attach(
        &mut partition,
        Rc::clone(&store) as Rc<dyn crate::RemoteObjects>,
    );

    let polled = Box::pin(super::remote_poll::polled_all(&mut partition)).await;

    assert_eq!(polled, expected);
}

/// The age and size of placed batches say nothing of their messages, so
/// expiry and the size cap remove a local segment only once the long-term
/// log trimmed its offsets, wherever the floor is.
#[compio::test]
async fn given_expired_placed_batches_when_cleaning_should_keep_what_the_long_term_log_did_not_trim()
 {
    let (_whole, placed, object) = whole_and_placed(6);
    let store = Box::pin(store_with(object)).await;
    let (_directory, mut partition) = Box::pin(placing_primary_with(&[], &store)).await;
    partition.runtime_options.segment_size = Some(placed_segment_size());
    partition.log.active_segment_mut().max_size = placed_segment_size();
    let config = repair_config();
    let starts = |partition: &IggyPartition<IggyMessageBus>| -> Vec<u64> {
        partition
            .log
            .segments()
            .iter()
            .map(|segment| segment.start_offset)
            .collect()
    };
    let far_future = IggyTimestamp::from(u64::MAX / 2);
    let expiry = IggyExpiry::from(1);

    Box::pin(commit_records(&mut partition, 1, &placed[..3])).await;
    assert_eq!(starts(&partition), [0, 2]);
    Box::pin(partition.clean_expired_segments(far_future, expiry, Some(0))).await;
    settle_partition_io(&mut partition, &config).await;
    assert_eq!(starts(&partition), [0, 2], "no long-term segment yet");

    let rewritten = Box::pin(rewrite_pass(&mut partition, &writer(&store), 100))
        .await
        .unwrap();
    assert_eq!(rewritten.end_offset, 2);
    Box::pin(commit_records(&mut partition, 4, &placed[3..])).await;
    assert_eq!(starts(&partition), [0, 2, 4, 6]);
    Box::pin(partition.clean_expired_segments(far_future, expiry, Some(0))).await;
    settle_partition_io(&mut partition, &config).await;

    assert!(partition.fatal().is_none());
    assert_eq!(
        starts(&partition),
        [0, 2, 4, 6],
        "the long-term log holds offsets 0 to 2 and trimmed none"
    );
}

/// A client that deletes segments names an offset, and the removal it leads
/// to must stop where retention stops.
#[compio::test]
async fn given_placed_batches_when_removing_segments_up_to_an_offset_should_keep_those_above_the_long_term_floor()
 {
    let (_whole, placed, object) = whole_and_placed(6);
    let store = Box::pin(store_with(object)).await;
    let (_directory, mut partition) = Box::pin(placing_primary_with(&[], &store)).await;
    partition.runtime_options.segment_size = Some(placed_segment_size());
    partition.log.active_segment_mut().max_size = placed_segment_size();
    let config = repair_config();
    let starts = |partition: &IggyPartition<IggyMessageBus>| -> Vec<u64> {
        partition
            .log
            .segments()
            .iter()
            .map(|segment| segment.start_offset)
            .collect()
    };

    Box::pin(commit_records(&mut partition, 1, &placed[..3])).await;
    Box::pin(partition.remove_sealed_segments_up_to(u64::MAX)).await;
    settle_partition_io(&mut partition, &config).await;
    assert_eq!(starts(&partition), [0, 2], "no long-term segment yet");

    let rewritten = Box::pin(rewrite_pass(&mut partition, &writer(&store), 100))
        .await
        .unwrap();
    assert_eq!(rewritten.end_offset, 2);
    Box::pin(commit_records(&mut partition, 4, &placed[3..])).await;
    Box::pin(partition.remove_sealed_segments_up_to(u64::MAX)).await;
    settle_partition_io(&mut partition, &config).await;

    assert!(partition.fatal().is_none());
    assert_eq!(starts(&partition), [2, 4, 6]);
}

/// A reader of the bucket finds the partition's segments in its manifest.
#[compio::test]
async fn given_a_published_long_term_segment_when_building_the_manifest_should_list_it() {
    let (_whole, placed, object) = whole_and_placed(3);
    let store = Box::pin(store_with(object)).await;
    let (_directory, mut partition) = Box::pin(placing_primary_with(&placed, &store)).await;
    let now = IggyTimestamp::now();
    let empty = partition.manifest(now).unwrap();
    assert_eq!((empty.floor, empty.segments.len()), (None, 0));
    let segment = Box::pin(rewrite_pass(&mut partition, &writer(&store), 100))
        .await
        .unwrap();

    let manifest = partition.manifest(now).unwrap();

    assert_eq!(
        manifest.prefix,
        crate::object_prefix(IggyNamespace::new(1, 1, 0), 0)
    );
    assert_eq!(manifest.floor, Some(2));
    assert_eq!(manifest.trim_point, None);
    assert_eq!(manifest.segments, [segment]);
    let (_, published, _) = partition.begin_manifest_publish(now).unwrap();
    assert_eq!(published, manifest);
}

/// The fresh objects the partition names above its long-term floor.
async fn objects_in_use(partition: &IggyPartition<IggyMessageBus>) -> Vec<u128> {
    partition
        .fresh_objects_in_use()
        .expect("the partition places its batches")
        .read()
        .await
        .unwrap()
        .into_iter()
        .collect()
}

#[compio::test]
async fn given_a_partition_of_whole_batches_when_asking_for_its_fresh_objects_should_name_none() {
    let config = repair_config();
    let (_directory, partition) = Box::pin(disk_poll_partition_with_replicas(&config, 3)).await;

    assert!(partition.fresh_objects_in_use().is_none());
}

/// A batch in the journal may still commit, so its object is needed whether
/// it is committed or not.
#[compio::test]
async fn given_placed_batches_only_in_the_journal_when_asking_for_fresh_objects_should_name_theirs()
{
    let (_directory, mut partition) = Box::pin(placing_partition()).await;
    let (records, placements) = placed_records(2);
    for (index, record) in records.iter().enumerate() {
        let index = index as u64;
        let prepare = stamped_placed_prepare(&partition, index + 1, index, record);
        Box::pin(partition.append_received_send_messages_to_journal(prepare))
            .await
            .unwrap();
    }

    assert_eq!(
        Box::pin(objects_in_use(&partition)).await,
        vec![placements[0].object_id]
    );
}

/// The rewrite moves the floor past the batches it stored, and the batches
/// committed after it name their object again.
#[compio::test]
async fn given_a_long_term_floor_when_asking_for_fresh_objects_should_name_those_of_the_batches_above_it()
 {
    let (_whole, placed, object) = whole_and_placed(4);
    let store = Box::pin(store_with(object)).await;
    let (_directory, mut partition) = Box::pin(placing_primary_with(&placed[..3], &store)).await;
    let named = PlacedBatch::decode(&placed[0]).unwrap().placement.object_id;
    assert_eq!(Box::pin(objects_in_use(&partition)).await, vec![named]);

    let writer = writer(&store);
    Box::pin(rewrite_pass(&mut partition, &writer, 100))
        .await
        .unwrap();
    assert_eq!(partition.long_term_floor(), Some(2));
    assert_eq!(
        Box::pin(objects_in_use(&partition)).await,
        Vec::<u128>::new()
    );

    Box::pin(commit_records(&mut partition, 4, &placed[3..])).await;
    assert_eq!(Box::pin(objects_in_use(&partition)).await, vec![named]);
}

/// Send the command of `segment` as the primary does, and commit it on the
/// replica as it stands then.
async fn commit_segment_command(
    partition: &mut IggyPartition<IggyMessageBus>,
    segment: &iggy_object_store::ManifestSegment,
    lose_primacy_first: bool,
) {
    let request = partition.publish_segment_request(segment, 1).unwrap();
    Box::pin(commit_long_term_request(
        partition,
        request,
        lose_primacy_first,
    ))
    .await;
    assert_eq!(partition.long_term_segments(), [*segment]);
}

/// Take `request` as the primary does, and commit it on the replica as it
/// stands then.
pub(super) async fn commit_long_term_request(
    partition: &mut IggyPartition<IggyMessageBus>,
    request: Message<RoutedRequestHeader>,
    lose_primacy_first: bool,
) {
    Box::pin(partition.on_request(request, None)).await;
    let head = partition.consensus().sequencer().current_sequence();
    if lose_primacy_first {
        lose_primacy(partition);
    }
    partition.consensus().advance_commit_max(head);
    let config = repair_config();
    for _ in 0..4 {
        Box::pin(partition.commit_journal(&config)).await;
    }
}

/// A leader of three whose long-term log holds the offsets `0`, `1` and
/// `2..=3` as three segments, each committed by its own command.
pub(super) async fn leader_with_three_long_term_segments() -> (
    tempfile::TempDir,
    IggyPartition<IggyMessageBus>,
    [iggy_object_store::ManifestSegment; 3],
) {
    use crate::LongTermWriter;
    let (_whole, placed, object) = whole_and_placed(4);
    let store = Box::pin(store_with(object)).await;
    let (directory, mut partition) = Box::pin(placing_leader_of_three_with(&placed, &store)).await;
    let mut segments = Vec::new();
    for (request, up_to) in (1..).zip([0, 1, 3]) {
        let (id, mut rewrite, scope) = partition.begin_segment_rewrite().unwrap();
        rewrite.up_to = up_to;
        let outcome = writer(&store).write(rewrite, scope).await;
        let segment = partition.finish_segment_rewrite(id, outcome).unwrap();
        let command = partition
            .publish_segment_request(&segment, request)
            .unwrap();
        Box::pin(commit_long_term_request(&mut partition, command, false)).await;
        segments.push(segment);
    }
    assert_eq!(partition.long_term_segments(), segments);
    assert!(partition.take_long_term_changed());
    (directory, partition, segments.try_into().unwrap())
}

/// The segment that holds the bytes of `first` and then of `second`.
pub(super) fn merged_of(
    first: &iggy_object_store::ManifestSegment,
    second: &iggy_object_store::ManifestSegment,
) -> iggy_object_store::ManifestSegment {
    iggy_object_store::ManifestSegment {
        end_offset: second.end_offset,
        end_timestamp: second.end_timestamp,
        max_timestamp: first.max_timestamp.max(second.max_timestamp),
        size: first.size + second.size,
        crc32c: crc32c::crc32c_combine(
            first.crc32c,
            second.crc32c,
            usize::try_from(second.size).unwrap(),
        ),
        hash: [7; iggy_object_store::SEGMENT_HASH_LEN],
        ..*first
    }
}

/// A replace and a trim change what the manifest lists as a publish does,
/// so the primary learns of their commits the same way.
#[compio::test]
async fn given_a_primary_when_a_replace_and_a_trim_commit_should_note_that_the_log_changed() {
    let (_directory, mut partition, [first, second, third]) =
        Box::pin(leader_with_three_long_term_segments()).await;
    let merged = merged_of(&first, &second);

    let replace = partition.replace_segments_request(&merged, 10).unwrap();
    Box::pin(commit_long_term_request(&mut partition, replace, false)).await;

    assert_eq!(partition.long_term_segments(), [merged, third]);
    assert_eq!(partition.long_term_floor(), Some(3));
    assert!(partition.take_long_term_changed());
    assert!(!partition.take_long_term_changed(), "taken once");

    let trim = partition
        .trim_segments_request(merged.end_offset, 11)
        .unwrap();
    Box::pin(commit_long_term_request(&mut partition, trim, false)).await;

    assert_eq!(partition.long_term_segments(), [third]);
    assert_eq!(partition.long_term_floor(), Some(3));
    assert!(partition.take_long_term_changed());
}

#[compio::test]
async fn given_a_backup_when_a_replace_or_a_trim_commits_should_apply_it_and_not_note_that_the_log_changed()
 {
    let (_directory, mut partition, [first, second, third]) =
        Box::pin(leader_with_three_long_term_segments()).await;
    let merged = merged_of(&first, &second);
    let replace = partition.replace_segments_request(&merged, 10).unwrap();
    let trim = partition
        .trim_segments_request(merged.end_offset, 11)
        .unwrap();
    Box::pin(partition.on_request(replace, None)).await;

    Box::pin(commit_long_term_request(&mut partition, trim, true)).await;

    assert_eq!(partition.long_term_segments(), [third]);
    assert!(!partition.take_long_term_changed());
}

/// A trim below the first segment and a replace sent twice commit and leave
/// the log as it was. The manifest in the store already says what they
/// would, so neither asks for another.
#[compio::test]
async fn given_a_primary_when_a_command_that_changes_nothing_commits_should_not_note_that_the_log_changed()
 {
    let (_directory, mut partition, [first, second, third]) =
        Box::pin(leader_with_three_long_term_segments()).await;
    let merged = merged_of(&first, &second);
    let replace = partition.replace_segments_request(&merged, 10).unwrap();
    Box::pin(commit_long_term_request(&mut partition, replace, false)).await;
    let trim = partition.trim_segments_request(1, 11).unwrap();
    Box::pin(commit_long_term_request(&mut partition, trim, false)).await;
    assert_eq!(partition.long_term_segments(), [third]);
    assert!(partition.take_long_term_changed());

    let trim_again = partition.trim_segments_request(1, 12).unwrap();
    Box::pin(commit_long_term_request(&mut partition, trim_again, false)).await;
    assert!(!partition.take_long_term_changed());

    // The last segment stays whatever the offset, so this one removes
    // nothing either.
    let trim_everything = partition.trim_segments_request(u64::MAX, 13).unwrap();
    Box::pin(commit_long_term_request(
        &mut partition,
        trim_everything,
        false,
    ))
    .await;
    assert_eq!(partition.long_term_segments(), [third]);
    assert!(!partition.take_long_term_changed());
}

#[compio::test]
async fn given_a_primary_when_a_replace_commits_twice_should_note_that_the_log_changed_once() {
    let (_directory, mut partition, [first, second, third]) =
        Box::pin(leader_with_three_long_term_segments()).await;
    let merged = merged_of(&first, &second);
    let replace = partition.replace_segments_request(&merged, 10).unwrap();
    Box::pin(commit_long_term_request(&mut partition, replace, false)).await;
    assert!(partition.take_long_term_changed());

    let replace_again = partition.replace_segments_request(&merged, 11).unwrap();
    Box::pin(commit_long_term_request(
        &mut partition,
        replace_again,
        false,
    ))
    .await;

    assert_eq!(partition.long_term_segments(), [merged, third]);
    assert!(!partition.take_long_term_changed());
}

/// A replace leaves the floor and the trim point where they were and lists
/// one segment where the manifest in the store lists several. A reader
/// would look for the objects of those, which are deleted later, so the
/// manifest is stored again.
#[compio::test]
async fn given_a_replace_at_an_unchanged_floor_when_a_manifest_pass_runs_should_publish_the_shorter_list()
 {
    const STORED: crate::ManifestPublishOutcome = Ok(Ok(()));
    let (_directory, mut partition, [first, second, third]) =
        Box::pin(leader_with_three_long_term_segments()).await;
    let now = IggyTimestamp::now();
    let (id, manifest, _scope) = partition.begin_manifest_publish(now).unwrap();
    assert_eq!(manifest.segments, [first, second, third]);
    assert!(!partition.finish_manifest_publish(id, &STORED, now));
    assert!(
        partition.begin_manifest_publish(now).is_none(),
        "the manifest in the store is the current one"
    );
    let merged = merged_of(&first, &second);
    let replace = partition.replace_segments_request(&merged, 10).unwrap();
    Box::pin(commit_long_term_request(&mut partition, replace, false)).await;

    let (id, manifest, _scope) = partition.begin_manifest_publish(now).unwrap();

    assert_eq!(manifest.floor, Some(3));
    assert_eq!(manifest.trim_point, None);
    assert_eq!(manifest.segments, [merged, third]);
    assert!(!partition.finish_manifest_publish(id, &STORED, now));
    assert!(partition.begin_manifest_publish(now).is_none());
}

/// No publish starts while one runs. A replace that commits meanwhile moves
/// neither the floor nor the trim point, and the manifest being stored
/// still lists what it replaced, so the end of the publish asks for the
/// next one.
#[compio::test]
async fn given_a_replace_during_a_manifest_publish_when_it_ends_should_ask_for_another() {
    const STORED: crate::ManifestPublishOutcome = Ok(Ok(()));
    let (_directory, mut partition, [first, second, _]) =
        Box::pin(leader_with_three_long_term_segments()).await;
    let now = IggyTimestamp::now();
    let (id, _, _scope) = partition.begin_manifest_publish(now).unwrap();
    let replace = partition
        .replace_segments_request(&merged_of(&first, &second), 10)
        .unwrap();
    Box::pin(commit_long_term_request(&mut partition, replace, false)).await;

    assert!(partition.finish_manifest_publish(id, &STORED, now));
}

/// A reader takes the trim point as the offset below which the partition
/// holds nothing, and refuses a manifest whose segments start at or below
/// it.
#[compio::test]
async fn given_a_trimmed_long_term_log_when_building_the_manifest_should_carry_the_trim_point() {
    let (_directory, mut partition, [_, second, third]) =
        Box::pin(leader_with_three_long_term_segments()).await;
    let now = IggyTimestamp::now();
    assert_eq!(partition.long_term_trim_point(), None);
    let (id, _, _scope) = partition.begin_manifest_publish(now).unwrap();
    partition.finish_manifest_publish(id, &Ok(Ok(())), now);
    let trim = partition.trim_segments_request(0, 10).unwrap();
    Box::pin(commit_long_term_request(&mut partition, trim, false)).await;

    let (_, manifest, _scope) = partition
        .begin_manifest_publish(now)
        .expect("a trim is a change of the manifest");

    assert_eq!(partition.long_term_trim_point(), Some(0));
    assert_eq!(manifest.trim_point, Some(0));
    assert_eq!(manifest.floor, Some(3));
    assert_eq!(manifest.segments, [second, third]);
    assert_eq!(
        iggy_object_store::PartitionManifest::decode(&manifest.encode()).unwrap(),
        manifest
    );
}

/// The primary publishes the manifest and rewrites, so it is the one that
/// learns of a commit to the long-term log.
#[compio::test]
async fn given_a_primary_when_the_command_of_a_segment_commits_should_note_that_the_log_changed() {
    use crate::LongTermWriter;
    let (_whole, placed, object) = whole_and_placed(2);
    let store = Box::pin(store_with(object)).await;
    let (_directory, mut partition) = Box::pin(placing_leader_of_three_with(&placed, &store)).await;
    let (id, rewrite, scope) = partition.begin_segment_rewrite().unwrap();
    let outcome = writer(&store).write(rewrite, scope).await;
    let segment = partition.finish_segment_rewrite(id, outcome).unwrap();
    assert!(!partition.take_long_term_changed());

    Box::pin(commit_segment_command(&mut partition, &segment, false)).await;

    assert!(partition.take_long_term_changed());
    assert!(!partition.take_long_term_changed(), "taken once");
}

#[compio::test]
async fn given_a_backup_when_the_command_of_a_segment_commits_should_not_note_that_the_log_changed()
{
    use crate::LongTermWriter;
    let (_whole, placed, object) = whole_and_placed(2);
    let store = Box::pin(store_with(object)).await;
    let (_directory, mut partition) = Box::pin(placing_leader_of_three_with(&placed, &store)).await;
    let (id, rewrite, scope) = partition.begin_segment_rewrite().unwrap();
    let outcome = writer(&store).write(rewrite, scope).await;
    let segment = partition.finish_segment_rewrite(id, outcome).unwrap();

    Box::pin(commit_segment_command(&mut partition, &segment, true)).await;

    assert!(!partition.take_long_term_changed());
}

/// The log refuses a segment past its limit. A segment stored then would
/// wait for its command forever, and hold back every rewrite after it.
#[compio::test]
async fn given_a_full_long_term_log_when_a_pass_runs_should_start_no_rewrite_until_it_is_shorter() {
    let (_whole, placed, object) = whole_and_placed(2);
    let store = Box::pin(store_with(object)).await;
    let (_directory, mut partition) = Box::pin(placing_primary_with(&placed, &store)).await;
    let segments = crate::long_term::LONG_TERM_SEGMENTS_MAX as u64;
    for offset in 0..segments {
        let payload = crate::long_term::PublishSegment(iggy_object_store::ManifestSegment {
            start_offset: offset,
            end_offset: offset,
            start_timestamp: 0,
            end_timestamp: 0,
            max_timestamp: 0,
            size: 1,
            crc32c: 0,
            hash: [0; iggy_object_store::SEGMENT_HASH_LEN],
        })
        .encode();
        let applied = partition
            .state_machines
            .apply(
                100 + offset,
                crate::state_machine::MachineCommand {
                    kind: crate::long_term::PUBLISH_SEGMENT,
                    payload: &payload,
                },
            )
            .unwrap();
        assert_eq!(applied, crate::state_machine::Applied::Changed);
    }
    // One committed message above the floor, which a rewrite would take.
    partition.dirty_offset.store(segments, Ordering::Release);
    partition.offset.store(segments, Ordering::Release);
    assert_eq!(partition.long_term_lag(), 1);

    assert!(partition.begin_segment_rewrite().is_none());
    assert!(!partition.segment_rewrite_running());

    // A trim makes the log shorter, and the next pass rewrites again.
    let payload = crate::long_term::TrimSegments { up_to_offset: 0 }.encode();
    let applied = partition
        .state_machines
        .apply(
            100 + segments,
            crate::state_machine::MachineCommand {
                kind: crate::long_term::TRIM_SEGMENTS,
                payload: &payload,
            },
        )
        .unwrap();
    assert_eq!(applied, crate::state_machine::Applied::Changed);
    let (_, rewrite, _) = partition.begin_segment_rewrite().unwrap();
    assert_eq!((rewrite.start_offset, rewrite.up_to), (segments, segments));
}

/// A segment that ends before the commit point its rewrite started at has
/// committed messages behind it, as one cut at the byte cap does.
#[compio::test]
async fn given_a_segment_that_ends_before_the_commit_point_when_stored_should_say_more_is_behind() {
    use crate::LongTermWriter;
    let (_whole, placed, object) = whole_and_placed(4);
    let store = Box::pin(store_with(object)).await;
    let (_directory, mut partition) = Box::pin(placing_primary_with(&placed, &store)).await;
    let writer = writer(&store);
    let (id, mut rewrite, scope) = partition.begin_segment_rewrite().unwrap();
    assert_eq!(rewrite.up_to, 3);
    // Stands in for the byte cap, which no test reaches.
    rewrite.up_to = 1;
    let outcome = writer.write(rewrite, scope).await;
    let first = partition.finish_segment_rewrite(id, outcome).unwrap();
    assert_eq!(first.end_offset, 1);
    assert!(!partition.stored_segment_reached_commit());
    commit_published_segment(&mut partition, &first, 100);

    let second = Box::pin(rewrite_pass(&mut partition, &writer, 101))
        .await
        .unwrap();
    assert_eq!(second.end_offset, 3);
    assert!(partition.stored_segment_reached_commit());
}

/// A segment committed while the manifest was being stored is not in it,
/// and no publish starts while one runs, so the end of the publish asks for
/// the next one.
#[compio::test]
async fn given_a_segment_published_during_a_manifest_publish_when_it_ends_should_ask_for_another() {
    const STORED: crate::ManifestPublishOutcome = Ok(Ok(()));
    let (_whole, placed, object) = whole_and_placed(4);
    let store = Box::pin(store_with(object)).await;
    let (_directory, mut partition) = Box::pin(placing_primary_with(&placed[..2], &store)).await;
    let writer = writer(&store);
    let now = IggyTimestamp::now();
    Box::pin(rewrite_pass(&mut partition, &writer, 100)).await;
    let (id, manifest, _scope) = partition.begin_manifest_publish(now).unwrap();
    assert_eq!(manifest.floor, Some(1));
    Box::pin(commit_records(&mut partition, 3, &placed[2..])).await;
    Box::pin(rewrite_pass(&mut partition, &writer, 101)).await;
    assert!(
        partition.begin_manifest_publish(now).is_none(),
        "one at a time"
    );

    assert!(partition.finish_manifest_publish(id, &STORED, now));

    let (id, manifest, _scope) = partition.begin_manifest_publish(now).unwrap();
    assert_eq!(manifest.floor, Some(3));
    assert!(
        !partition.finish_manifest_publish(id, &STORED, now),
        "the log stood still"
    );
}

/// Reads `store`, and counts the reads.
struct CountedStore {
    store: Rc<iggy_object_store::MemoryStore>,
    reads: std::cell::Cell<usize>,
}

impl crate::RemoteObjects for CountedStore {
    fn get_range<'a>(
        &'a self,
        key: &'a str,
        range: std::ops::Range<u64>,
    ) -> futures::future::LocalBoxFuture<
        'a,
        Result<iggy_object_store::ObjectRange, iggy_object_store::ObjectStoreError>,
    > {
        self.reads.set(self.reads.get() + 1);
        crate::RemoteObjects::get_range(&*self.store, key, range)
    }
}

/// The batches of one poll sit back to back in one fresh object, so the
/// poll asks the store once for all of them and not once a batch.
#[compio::test]
async fn given_batches_of_one_fresh_object_when_polling_them_should_ask_the_store_once() {
    let (whole, placed, object) = whole_and_placed(RECEIVER_OPS);
    let expected = Box::pin(polls_of_whole_batches(&whole)).await;
    let (_directory, mut partition) = Box::pin(placing_partition_with(&placed)).await;
    let counted = Rc::new(CountedStore {
        store: Box::pin(store_with(object)).await,
        reads: std::cell::Cell::new(0),
    });
    super::remote_poll::attach(
        &mut partition,
        Rc::clone(&counted) as Rc<dyn crate::RemoteObjects>,
    );

    let polled = Box::pin(super::remote_poll::polled_all(&mut partition)).await;

    assert_eq!(polled, expected);
    const { assert!(RECEIVER_OPS > 2, "the poll reads several batches") };
    assert_eq!(counted.reads.get(), 1);
}

/// The objects a busy shard stored can leave the cache no room for what a
/// poll fetches. The poll takes its batches from its own read all the same.
#[compio::test]
async fn given_a_cache_without_room_when_polling_batches_of_one_fresh_object_should_ask_the_store_once()
 {
    let (_whole, placed, object) = whole_and_placed(RECEIVER_OPS);
    let (_directory, mut partition) = Box::pin(placing_partition_with(&placed)).await;
    let counted = Rc::new(CountedStore {
        store: Box::pin(store_with(object)).await,
        reads: std::cell::Cell::new(0),
    });
    let mut remote = crate::RemoteSegments::new(
        Rc::clone(&counted) as Rc<dyn crate::RemoteObjects>,
        super::remote_poll::layout(),
    );
    remote.set_fresh_cache_bytes(0);
    partition.set_remote_segments(Rc::new(remote));

    let args = PollingArgs::new(iggy_common::PollingStrategy::offset(0), 10, false);
    let result = partition
        .build_poll_plan(PollingConsumer::Consumer(1, 0), &args, false)
        .execute()
        .await;

    assert_eq!(result.read_error, None);
    assert_eq!(u64::from(result.message_count), RECEIVER_OPS);
    assert_eq!(counted.reads.get(), 1);
}

/// What a reader with nothing but the bucket finds for the partition: the
/// offsets of every batch in the segments its manifest lists, and the bytes
/// of those segments back to back. Each segment and each batch passes the
/// checks a reader can make on its own.
pub(super) async fn read_from_the_bucket_alone(
    store: &iggy_object_store::MemoryStore,
) -> (
    iggy_object_store::PartitionManifest,
    Vec<(u64, u32)>,
    Vec<u8>,
) {
    use iggy_binary_protocol::batch::BATCH_HEADER_SIZE;
    use iggy_object_store::{LongTermKey, PartitionManifest, SegmentObject};

    let layout = super::remote_poll::layout();
    let prefix = crate::object_prefix(IggyNamespace::new(1, 1, 0), 0);
    let manifest = PartitionManifest::decode(
        &Box::pin(object_bytes(store, &layout.manifest_key(&prefix))).await,
    )
    .expect("the manifest decodes");
    let mut batches = Vec::new();
    let mut log = Vec::new();
    for segment in &manifest.segments {
        let key = layout.long_term_key(&LongTermKey {
            prefix: manifest.prefix,
            start_offset: segment.start_offset,
            end_offset: segment.end_offset,
            object: SegmentObject::Log,
        });
        let bytes = Box::pin(object_bytes(store, &key)).await;
        assert_eq!(bytes.len() as u64, segment.size);
        assert_eq!(crc32c::crc32c(&bytes), segment.crc32c);
        let mut rest = bytes.as_slice();
        while rest.len() >= BATCH_HEADER_SIZE {
            let total = BatchHeader::decode(rest).unwrap().total_size();
            let batch = server_common::send_messages::decode_batch_slice(&rest[..total])
                .expect("a batch of a long-term segment passes its checksums");
            batches.push((batch.header.base_offset, batch.header.message_count));
            rest = &rest[total..];
        }
        assert!(rest.is_empty(), "the segment ends with a whole batch");
        assert_eq!(batches.first().map(|first| first.0), Some(0));
        let (last_offset, last_count) = *batches.last().unwrap();
        assert_eq!(last_offset + u64::from(last_count) - 1, segment.end_offset);
        log.extend_from_slice(&bytes);
    }
    (manifest, batches, log)
}

/// Store the partition's manifest, as its publish pass does.
pub(super) async fn publish_manifest(
    partition: &mut IggyPartition<IggyMessageBus>,
    store: &Rc<iggy_object_store::MemoryStore>,
) {
    use crate::ManifestPublisher;
    let publisher = crate::ExecutorManifestPublisher::new(
        iggy_object_store::InlineTierExecutor::new(SharedStore(Rc::clone(store))),
        super::remote_poll::layout(),
    );
    let now = IggyTimestamp::now();
    let (id, manifest, scope) = partition
        .begin_manifest_publish(now)
        .expect("the manifest changed");
    let outcome = publisher.publish(manifest, scope).await;
    partition.finish_manifest_publish(id, &outcome, now);
}

/// Another cluster reads the partition from the bucket and from nothing
/// else. The manifest and the segments it lists give it every committed
/// message once, in order, as the bytes a partition that kept its batches
/// whole holds. The fresh objects are not among what it reads.
#[compio::test]
async fn given_rewritten_batches_when_reading_the_bucket_alone_should_find_every_committed_message_once()
 {
    let (whole, placed, object) = whole_and_placed(RECEIVER_OPS);
    let expected_log = Box::pin(log_of_whole_batches(&whole)).await;
    let store = Box::pin(store_with(object)).await;
    let (_directory, mut partition) = Box::pin(placing_primary_with(&placed[..3], &store)).await;
    let writer = writer(&store);
    Box::pin(rewrite_pass(&mut partition, &writer, 100))
        .await
        .unwrap();
    Box::pin(publish_manifest(&mut partition, &store)).await;

    // The first three are published, and the other two not yet committed.
    let (manifest, batches, _) = Box::pin(read_from_the_bucket_alone(&store)).await;
    assert_eq!(manifest.floor, Some(2));
    assert_eq!(batches, [(0, 1), (1, 1), (2, 1)]);

    Box::pin(commit_records(&mut partition, 4, &placed[3..])).await;
    // Committed, and not in a long-term segment: the reader does not see
    // them, and what it saw before has not changed.
    let (_, unchanged, _) = Box::pin(read_from_the_bucket_alone(&store)).await;
    assert_eq!(unchanged, batches);

    Box::pin(rewrite_pass(&mut partition, &writer, 101))
        .await
        .unwrap();
    Box::pin(publish_manifest(&mut partition, &store)).await;

    let (manifest, batches, log) = Box::pin(read_from_the_bucket_alone(&store)).await;
    assert_eq!(manifest.segments.len(), 2);
    assert_eq!(manifest.floor, Some(RECEIVER_OPS - 1));
    assert_eq!(
        batches,
        (0..RECEIVER_OPS)
            .map(|offset| (offset, 1))
            .collect::<Vec<_>>()
    );
    assert_eq!(log, expected_log);
}

/// A replica of a group of three that leads view 0 and committed `placed`,
/// whose polls and rewrites read `store`.
pub(super) async fn placing_leader_of_three_with(
    placed: &[Vec<u8>],
    store: &Rc<iggy_object_store::MemoryStore>,
) -> (tempfile::TempDir, IggyPartition<IggyMessageBus>) {
    let (directory, mut partition) = Box::pin(placing_partition()).await;
    partition.register_builtin_state_machines().unwrap();
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    super::remote_poll::attach(
        &mut partition,
        Rc::clone(store) as Rc<dyn crate::RemoteObjects>,
    );
    Box::pin(commit_records(&mut partition, 1, placed)).await;
    assert!(partition.consensus.is_primary());
    (directory, partition)
}

/// Move the partition to the next view this replica does not lead, and then
/// to the next one it leads.
pub(super) fn lose_primacy(partition: &mut IggyPartition<IggyMessageBus>) {
    let view = partition.consensus.view() + 1;
    partition.consensus.set_view(view);
    assert!(!partition.consensus.is_primary());
}

pub(super) fn regain_primacy(partition: &mut IggyPartition<IggyMessageBus>) {
    while !partition.consensus.is_primary() {
        let view = partition.consensus.view() + 1;
        partition.consensus.set_view(view);
    }
}

/// The log objects of long-term segments the store holds.
pub(super) fn long_term_logs(store: &iggy_object_store::MemoryStore) -> usize {
    let layout = super::remote_poll::layout();
    store
        .keys()
        .iter()
        .filter_map(|key| layout.parse_long_term_key(key))
        .filter(|key| key.object == iggy_object_store::SegmentObject::Log)
        .count()
}

/// Primacy moves while a rewrite runs. Its result belongs to a view that is
/// over: the replica keeps nothing of it and sends no command for it. The
/// segment it stored is found by whoever leads next, this replica included,
/// and is published once.
#[compio::test]
async fn given_a_rewrite_in_flight_when_primacy_moves_should_drop_its_result_and_converge_later() {
    use crate::LongTermWriter;
    let (_whole, placed, object) = whole_and_placed(3);
    let store = Box::pin(store_with(object)).await;
    let (_directory, mut partition) = Box::pin(placing_leader_of_three_with(&placed, &store)).await;
    let writer = writer(&store);
    let (id, rewrite, scope) = partition.begin_segment_rewrite().unwrap();
    let outcome = writer.write(rewrite, scope).await;
    let stored = *outcome.as_ref().unwrap().as_ref().unwrap();

    lose_primacy(&mut partition);

    assert_eq!(partition.finish_segment_rewrite(id, outcome), None);
    assert_eq!(partition.unpublished_segment(), None);
    assert!(partition.begin_segment_rewrite().is_none());
    assert!(!partition.segment_rewrite_running());
    assert_eq!(partition.long_term_floor(), None);

    regain_primacy(&mut partition);
    let mut published = None;
    for op in 100..103 {
        published = Box::pin(rewrite_pass(&mut partition, &writer, op)).await;
        if published.is_some() {
            break;
        }
    }

    assert_eq!(published, Some(stored));
    assert_eq!(partition.long_term_segments(), [stored]);
    assert_eq!(long_term_logs(&store), 1);
}

/// Primacy moves after the segment is stored and before its command
/// commits. The replica does not send the command of an earlier view again:
/// it rewrites from the floor, finds the segment in the store, and commits
/// that one.
#[compio::test]
async fn given_a_stored_segment_without_its_command_when_primacy_moves_and_returns_should_publish_it_once()
 {
    use crate::LongTermWriter;
    let (_whole, placed, object) = whole_and_placed(3);
    let store = Box::pin(store_with(object)).await;
    let (_directory, mut partition) = Box::pin(placing_leader_of_three_with(&placed, &store)).await;
    let writer = writer(&store);
    let (id, rewrite, scope) = partition.begin_segment_rewrite().unwrap();
    let outcome = writer.write(rewrite, scope).await;
    let stored = partition.finish_segment_rewrite(id, outcome).unwrap();
    assert_eq!(partition.unpublished_segment(), Some(stored));

    lose_primacy(&mut partition);
    assert_eq!(
        partition.unpublished_segment(),
        None,
        "a backup sends no command"
    );
    regain_primacy(&mut partition);
    assert_eq!(
        partition.unpublished_segment(),
        None,
        "what an earlier view stored is found again, not remembered"
    );

    let mut published = None;
    for op in 100..103 {
        published = Box::pin(rewrite_pass(&mut partition, &writer, op)).await;
        if published.is_some() {
            break;
        }
    }

    assert_eq!(published, Some(stored));
    assert_eq!(partition.long_term_floor(), Some(2));
    assert_eq!(long_term_logs(&store), 1);
}

/// Primacy moved while the messages of a send were on their way to the
/// store. The object is stored, and the replica that stored it no longer
/// leads: it prepares nothing for the request, so nothing commits where
/// those messages sit and the producer sends them again to the primary.
#[compio::test]
async fn given_a_replica_that_lost_primacy_when_a_placed_request_arrives_should_admit_nothing() {
    let config = repair_config();
    let (_directory, mut partition) = Box::pin(placing_partition()).await;
    let placed = placed_client_request(&partition, 1, 2);
    assert!(partition.takes_placed_sends() && partition.has_request_room());
    let view = partition.consensus.view() + 1;
    partition.consensus.set_view(view);
    assert!(!partition.consensus.is_primary());
    assert!(!partition.takes_placed_sends());

    Box::pin(partition.on_placed_request(placed, None)).await;
    Box::pin(settle_partition_io(&mut partition, &config)).await;

    assert_eq!(partition.consensus().sequencer().current_sequence(), 0);
    assert_eq!(partition.log.active_segment().current_position, 0);
}

/// A backup whose ring released the body of a committed placed batch still
/// acknowledges the retransmit of that op. Only the placed batch's own
/// checksum can tell it from another batch under the same header.
#[compio::test]
async fn given_a_released_committed_placed_batch_when_retransmitted_should_ack_only_the_same_prepare()
 {
    const LATER_VIEW: u32 = 3;

    let root = tempfile::tempdir().unwrap();
    let (config, mut partition) = Box::pin(placing_backup_with_wal(root.path())).await;
    let sent = partition.consensus().message_bus().sent_to_replicas.clone();
    // No room for a body, so only the newest one stays in memory.
    partition
        .log
        .journal()
        .inner
        .set_ring_caps(crate::EVICTED_RING_CAPACITY, 1);
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    let released = placed_segment_prepare(1, 0, 0);
    let in_memory = placed_segment_prepare(2, released.header().checksum, 1);
    let mut writer = hold_wal_writer(&persistence);
    for prepare in [&released, &in_memory] {
        Box::pin(partition.on_replicate(prepare.clone())).await;
        writer.await;
        writer = hold_wal_writer(&persistence);
        Box::pin(commit_on_heartbeat(
            &mut partition,
            &config,
            prepare.header().op,
        ))
        .await;
        Box::pin(settle_partition_io(&mut partition, &config)).await;
    }
    writer.await;
    let journal = &partition.log.journal().inner;
    assert!(
        journal.evicted_header(1).is_some() && journal.repair_entry(1).is_none(),
        "the ring keeps op 1 as a header only"
    );

    partition.consensus.set_view(LATER_VIEW);
    partition.consensus.set_log_view(LATER_VIEW);
    if !partition.persist_superblock_if_needed().await {
        persistence
            .drain_with_timeout(compio::runtime::time::sleep)
            .await
            .unwrap();
    }
    assert!(partition.persist_superblock_if_needed().await);
    let resent = |prepare: &Message<PrepareHeader>| {
        prepare
            .clone()
            .transmute_header(|original, header: &mut PrepareHeader| {
                *header = original;
                header.view = LATER_VIEW;
            })
    };
    let acknowledged = || -> Vec<(u64, u128)> {
        sent.borrow_mut()
            .drain(..)
            .filter_map(|(_, frame)| {
                bytemuck::checked::try_from_bytes::<PrepareOkHeader>(frame.as_slice())
                    .ok()
                    .filter(|header| header.command == Command::PrepareOk)
                    .map(|header| (header.op, header.prepare_checksum))
            })
            .collect()
    };
    sent.borrow_mut().clear();

    for (prepare, body) in [(&in_memory, "in memory"), (&released, "in its segment")] {
        Box::pin(partition.on_replicate(resent(prepare))).await;
        assert_eq!(
            acknowledged(),
            vec![(prepare.header().op, prepare.header().checksum)],
            "a committed placed retransmit must be acknowledged when its body is {body}"
        );
    }

    let mut other_placement = released.clone();
    let placement_byte = size_of::<PrepareHeader>() + PLACED_BATCH_SIZE - 1;
    other_placement.as_mut_slice()[placement_byte] ^= 1;
    Box::pin(partition.on_replicate(resent(&other_placement))).await;
    assert!(
        acknowledged().is_empty(),
        "a placed batch that fails its checksum must not be acknowledged"
    );
}
