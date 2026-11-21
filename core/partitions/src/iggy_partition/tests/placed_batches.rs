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
async fn placing_partition_of_small_segments() -> (tempfile::TempDir, IggyPartition<IggyMessageBus>)
{
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
    let (origin_directory, mut origin) = Box::pin(placing_partition_of_small_segments()).await;
    let (records, placements) = placed_records(5);
    Box::pin(commit_records(&mut origin, 1, &records)).await;
    let offer = Box::pin(serve_offer(&mut origin)).await;
    let (receiver_directory, mut receiver) = Box::pin(placing_partition_of_small_segments()).await;

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

const FRESH_OBJECT: u128 = 0x51;

/// The same batches for two partitions: as whole records, and as placed
/// records whose blobs sit back to back in one fresh object.
fn whole_and_placed(count: u64) -> (Vec<Vec<u8>>, Vec<Vec<u8>>, bytes::Bytes) {
    let whole = segment_records(count);
    let mut object = iggy_object_store::FreshObject::new();
    let placed = whole
        .iter()
        .map(|record| {
            let mut header = BatchHeader::decode(record).unwrap();
            header.batch_checksum = 0;
            let blob = &record[COMMAND_HEADER_SIZE..];
            let placement = Placement::of(FRESH_OBJECT, object.push(blob), blob);
            let mut placed = vec![0; PLACED_BATCH_SIZE];
            PlacedBatch::new(header, placement).encode_into(&mut placed);
            placed
        })
        .collect();
    (whole, placed, object.into_bytes())
}

/// A store that holds `object` as the fresh object the placed records name.
async fn store_with(object: bytes::Bytes) -> Rc<iggy_object_store::MemoryStore> {
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

/// Bytes of the buffer a poll holds one batch of [`whole_and_placed`] in.
fn held_batch_bytes(whole: &[Vec<u8>]) -> usize {
    size_of::<PrepareHeader>() + whole[0].len()
}

/// Poll from `offset` on with `max_bytes` of read buffers, and return the
/// last offset, the bytes and whether the read was truncated.
async fn polled_within(
    partition: &mut IggyPartition<IggyMessageBus>,
    offset: u64,
    max_bytes: usize,
) -> (Option<u64>, Vec<u8>, bool) {
    let args = PollingArgs::new(iggy_common::PollingStrategy::offset(offset), 10, false);
    let result = partition
        .build_poll_plan(PollingConsumer::Consumer(1, 0), &args, false)
        .execute_with_limit(max_bytes, usize::MAX)
        .await;
    assert_eq!(result.read_error, None);
    let truncated = result.truncated();
    let last_matching_offset = result.last_matching_offset;
    let bytes = partition
        .complete_poll(result)
        .unwrap()
        .fragments
        .iter()
        .flat_map(|fragment| fragment.as_slice().iter().copied())
        .collect();
    (last_matching_offset, bytes, truncated)
}

/// A placing partition that committed [`RECEIVER_OPS`] batches whose
/// messages its store holds, with the size of the buffer of one batch and
/// the bytes a partition of whole batches returns for a poll of all of them.
async fn placing_partition_to_poll() -> (
    tempfile::TempDir,
    IggyPartition<IggyMessageBus>,
    usize,
    Vec<u8>,
) {
    let (whole, placed, object) = whole_and_placed(RECEIVER_OPS);
    let (last, expected) = Box::pin(polls_of_whole_batches(&whole))
        .await
        .swap_remove(0);
    assert_eq!(last, Some(RECEIVER_OPS - 1));
    let (directory, mut partition) = Box::pin(placing_partition_with(&placed)).await;
    super::remote_poll::attach(&mut partition, Box::pin(store_with(object)).await);
    (directory, partition, held_batch_bytes(&whole), expected)
}

/// A poll holds every batch it reads until it answers, so its read buffers
/// are capped as those of a poll of whole batches are. It stops between two
/// batches, and the next poll goes on from there.
#[compio::test]
async fn given_a_cap_inside_the_run_when_polling_placed_batches_should_return_the_batches_that_fit_and_continue_from_them()
 {
    let (_directory, mut partition, held, expected) = Box::pin(placing_partition_to_poll()).await;
    // Two batches and half of the third.
    let max_bytes = 2 * held + held / 2;

    let mut polled = Vec::new();
    let mut polls = Vec::new();
    let mut offset = 0;
    while offset < RECEIVER_OPS {
        let (last, bytes, truncated) =
            Box::pin(polled_within(&mut partition, offset, max_bytes)).await;
        let last = last.expect("each poll returns a batch");
        polls.push((last, truncated));
        polled.extend(bytes);
        offset = last + 1;
    }

    const { assert!(RECEIVER_OPS == 5) };
    assert_eq!(polls, [(1, true), (3, true), (4, false)]);
    assert_eq!(
        polled, expected,
        "the polls together return every batch once, in order"
    );
}

/// The batch that holds the first message of a poll is returned whatever the
/// cap, or the same poll would return nothing each time it is sent.
#[compio::test]
async fn given_a_cap_below_one_batch_when_polling_placed_batches_should_return_one_batch_a_poll() {
    let (_directory, mut partition, held, expected) = Box::pin(placing_partition_to_poll()).await;

    let mut polled = Vec::new();
    for offset in 0..RECEIVER_OPS {
        let (last, bytes, truncated) =
            Box::pin(polled_within(&mut partition, offset, held - 1)).await;
        assert_eq!(last, Some(offset));
        assert_eq!(truncated, offset + 1 < RECEIVER_OPS, "the poll at {offset}");
        polled.extend(bytes);
    }

    assert_eq!(polled, expected);
}

#[compio::test]
async fn given_a_cap_that_covers_the_run_when_polling_placed_batches_should_return_them_all_untruncated()
 {
    let (_directory, mut partition, held, expected) = Box::pin(placing_partition_to_poll()).await;
    let max_bytes = usize::try_from(RECEIVER_OPS).unwrap() * held;

    let (last, bytes, truncated) = Box::pin(polled_within(&mut partition, 0, max_bytes)).await;

    assert_eq!(last, Some(RECEIVER_OPS - 1));
    assert!(!truncated);
    assert_eq!(bytes, expected);
}

/// The selection of a reply is capped apart from the read buffers, and a
/// poll that stops at it says so too.
#[compio::test]
async fn given_a_reply_that_is_full_when_polling_placed_batches_should_stop_and_report_truncation()
{
    let (_directory, mut partition, _held, expected) = Box::pin(placing_partition_to_poll()).await;
    let args = PollingArgs::new(iggy_common::PollingStrategy::offset(0), 10, false);

    let result = partition
        .build_poll_plan(PollingConsumer::Consumer(1, 0), &args, false)
        .execute_with_limit(usize::MAX, 1)
        .await;

    assert_eq!(result.read_error, None);
    assert_eq!(result.last_matching_offset, Some(0));
    assert!(result.truncated());
    assert!(result.encoded_bytes() < expected.len());
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
