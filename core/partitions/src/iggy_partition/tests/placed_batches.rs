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

/// Reading through a placement comes later. Until then a poll says so, and
/// does not answer as though the partition held nothing.
#[compio::test]
async fn given_a_placing_partition_when_a_poll_completes_should_refuse_it_as_unavailable() {
    let (_directory, mut partition) = Box::pin(placing_partition()).await;
    let (records, _) = placed_records(1);
    Box::pin(commit_records(&mut partition, 1, &records)).await;

    let completed = partition.complete_poll(poll_read_result(
        &partition,
        PollingConsumer::Consumer(7, 0),
        false,
        None,
    ));

    assert!(matches!(completed, Err(IggyError::FeatureUnavailable)));
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
