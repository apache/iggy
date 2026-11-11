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

use super::segment_chain::{PREFIX, tiered_partition};
use super::*;
use crate::segment_chain::{
    ChainError, ChainRecord, ObjectPrefix, RECORD_SIZE, append_record, chain_path, read_chain,
};
use crate::{LocalEnvelope, SegmentEviction, SegmentLocation};
use iggy_common::{IggyExpiry, IggyTimestamp};

/// Every sealed segment is outside an envelope that keeps nothing.
pub(super) const KEEP_NOTHING: LocalEnvelope = LocalEnvelope {
    age: IggyExpiry::NeverExpire,
    size: Some(0),
};

/// Sealed segments of a partition with [`RECEIVER_OPS`] one-message ops.
const SEALED: [u64; 2] = [0, 2];

pub(super) fn confirm<B: MessageBus>(partition: &mut IggyPartition<B>, starts: &[u64]) {
    for start_offset in starts {
        let record = partition.sealed_segment_record(*start_offset).unwrap();
        assert!(partition.confirm_segment_hash(*start_offset, record.end_offset, &record.hash));
    }
}

pub(super) async fn evict<B: MessageBus>(
    partition: &mut IggyPartition<B>,
    floor: u64,
    envelope: LocalEnvelope,
) -> SegmentEviction {
    let config = repair_config();
    let eviction = Box::pin(partition.evict_segments(IggyTimestamp::now(), floor, envelope)).await;
    Box::pin(settle_partition_io(partition, &config)).await;
    eviction
}

fn locations<B: MessageBus>(partition: &IggyPartition<B>) -> Vec<SegmentLocation> {
    partition
        .log
        .segments()
        .iter()
        .map(|segment| segment.location)
        .collect()
}

fn log_exists(directory: &Path, start_offset: u64) -> bool {
    directory.join(format!("{start_offset:020}.log")).exists()
}

pub(super) async fn chain_records(directory: &Path) -> Vec<ChainRecord> {
    read_chain(&directory.to_string_lossy())
        .await
        .unwrap()
        .unwrap()
        .records
}

/// A tiered partition under `root`, at the path boot recovery looks in.
pub(super) async fn tiered_partition_on_disk(
    root: &Path,
) -> (PartitionsConfig, IggyPartition<IggyMessageBus>) {
    let mut config = repair_config();
    config.path_layout.streams_root = root.to_string_lossy().into_owned();
    let directory = config.get_partition_path(1, 1, 0);
    std::fs::create_dir_all(&directory).unwrap();
    let mut partition = test_partition_with_replicas(3);
    partition.set_partition_dir(directory);
    partition.log.retire_front().unwrap();
    partition.install_empty_segment(&config, 0).await.unwrap();
    let batch_len = build_segment_record(partition.namespace(), 0).len() as u64;
    let segment_size = IggyByteSize::from(BATCHES_PER_SEGMENT * batch_len);
    partition.runtime_options.segment_size = Some(segment_size);
    partition.log.active_segment_mut().max_size = segment_size;
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    Box::pin(partition.open_segment_chain(PREFIX))
        .await
        .unwrap();
    Box::pin(commit_records(
        &mut partition,
        1,
        &segment_records(RECEIVER_OPS),
    ))
    .await;
    (config, partition)
}

/// The partition boot rebuilds from what `config` names on disk.
pub(super) async fn reboot(config: &PartitionsConfig) -> IggyPartition<IggyMessageBus> {
    let directory = config.get_partition_path(1, 1, 0);
    let recovered = crate::segment_recovery::load_persisted_segments(
        config,
        IggyNamespace::new(1, 1, 0),
        config.segment_size,
        false,
        &PartitionStats::default(),
    )
    .await
    .unwrap();
    let mut partition = test_partition_with_replicas(3);
    partition.set_partition_dir(directory.clone());
    partition.log.retire_front().unwrap();
    crate::partition_storage::hydrate_partition_log(&mut partition, &directory, 1, 1, 0, recovered)
        .await
        .unwrap();
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    partition
}

#[compio::test]
async fn given_confirmed_segments_below_the_floor_when_evicting_should_record_each_and_unlink_its_log()
 {
    let (directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    confirm(&mut partition, &SEALED);
    let size = partition.stats.size_bytes_inconsistent();
    let sealed = SEALED.map(|start| partition.sealed_segment_record(start).unwrap());

    Box::pin(evict(&mut partition, u64::MAX, KEEP_NOTHING)).await;

    assert_eq!(
        locations(&partition),
        [
            SegmentLocation::Remote,
            SegmentLocation::Remote,
            SegmentLocation::Local
        ]
    );
    for start_offset in SEALED {
        assert!(!log_exists(directory.path(), start_offset));
        assert!(
            directory
                .path()
                .join(format!("{start_offset:020}.index"))
                .exists()
        );
    }
    assert!(log_exists(
        directory.path(),
        partition.log.active_segment().start_offset
    ));
    assert_eq!(
        chain_records(directory.path()).await,
        [
            ChainRecord::Sealed(sealed[0]),
            ChainRecord::Sealed(sealed[1]),
            ChainRecord::Remote(sealed[0]),
            ChainRecord::Remote(sealed[1]),
        ]
    );
    assert_eq!(
        partition.stats.size_bytes_inconsistent(),
        size,
        "an evicted segment still counts toward the partition's size"
    );
    assert!(
        partition.log.storages()[0]
            .segment_and_index_paths()
            .0
            .is_none()
    );
}

#[compio::test]
async fn given_a_floor_inside_the_sealed_run_when_evicting_should_stop_above_it() {
    let (directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    confirm(&mut partition, &SEALED);
    let floor = partition.sealed_segment_record(0).unwrap().end_offset;

    Box::pin(evict(&mut partition, floor, KEEP_NOTHING)).await;

    assert_eq!(
        locations(&partition),
        [
            SegmentLocation::Remote,
            SegmentLocation::Local,
            SegmentLocation::Local
        ]
    );
    assert!(log_exists(directory.path(), 2));
}

/// A replica whose bytes differ from what the floor names keeps them, and so
/// does every segment behind the first unmatched one.
#[compio::test]
async fn given_a_segment_whose_hash_no_advance_matched_when_evicting_should_stop_at_it() {
    let (directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let first = partition.sealed_segment_record(0).unwrap();
    assert!(!partition.confirm_segment_hash(0, first.end_offset, &[0; 32]));
    assert!(!partition.confirm_segment_hash(0, first.end_offset + 1, &first.hash));
    confirm(&mut partition, &SEALED[1..]);

    let eviction = Box::pin(evict(&mut partition, u64::MAX, KEEP_NOTHING)).await;

    assert_eq!(eviction, SegmentEviction::default());
    assert!(
        locations(&partition)
            .iter()
            .all(|location| *location == SegmentLocation::Local)
    );
    assert!(log_exists(directory.path(), 0));
    assert_eq!(chain_records(directory.path()).await.len(), 2);
}

#[compio::test]
async fn given_segments_inside_the_local_envelope_when_evicting_should_keep_them() {
    let (_directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    confirm(&mut partition, &SEALED);
    let last_sealed = partition.log.segments()[1].size.as_bytes_u64();

    let unbounded = LocalEnvelope {
        age: IggyExpiry::NeverExpire,
        size: None,
    };
    Box::pin(evict(&mut partition, u64::MAX, unbounded)).await;
    assert!(
        locations(&partition)
            .iter()
            .all(|location| *location == SegmentLocation::Local)
    );

    let one_segment = LocalEnvelope {
        age: IggyExpiry::NeverExpire,
        size: Some(last_sealed),
    };
    Box::pin(evict(&mut partition, u64::MAX, one_segment)).await;
    assert_eq!(
        locations(&partition),
        [
            SegmentLocation::Remote,
            SegmentLocation::Local,
            SegmentLocation::Local
        ]
    );
}

#[compio::test]
async fn given_a_partition_without_a_chain_when_evicting_should_evict_nothing() {
    let (directory, mut partition) = Box::pin(partition_with_sealed_segments(&segment_records(
        RECEIVER_OPS,
    )))
    .await;

    let eviction = Box::pin(evict(&mut partition, u64::MAX, KEEP_NOTHING)).await;

    assert_eq!(eviction, SegmentEviction::default());
    assert!(log_exists(directory.path(), 0));
}

/// A crash between the record's fsync and the unlink: boot keeps the segment
/// local, and the resumed eviction unlinks it without a second record.
#[compio::test]
async fn given_an_eviction_cut_short_after_its_record_when_rebooted_should_resume_without_a_second_record()
 {
    let root = tempfile::tempdir().unwrap();
    let (config, partition) = Box::pin(tiered_partition_on_disk(root.path())).await;
    let directory = PathBuf::from(config.get_partition_path(1, 1, 0));
    let first = partition.sealed_segment_record(0).unwrap();
    let path = chain_path(&directory.to_string_lossy());
    let valid_len = std::fs::metadata(&path).unwrap().len();
    append_record(&path, valid_len, &ChainRecord::Remote(first))
        .await
        .unwrap();
    drop(partition);

    let mut partition = Box::pin(reboot(&config)).await;
    assert_eq!(partition.log.segments()[0].location, SegmentLocation::Local);
    assert_eq!(partition.sealed_segment_record(0), Some(first));
    confirm(&mut partition, &[0]);
    Box::pin(evict(&mut partition, first.end_offset, KEEP_NOTHING)).await;

    assert_eq!(
        partition.log.segments()[0].location,
        SegmentLocation::Remote
    );
    assert!(!log_exists(&directory, 0));
    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        valid_len + RECORD_SIZE as u64,
        "the record a crash left durable is not appended again"
    );
}

/// After an eviction finished, boot serves the segment as remote again.
#[compio::test]
async fn given_an_evicted_segment_when_rebooted_should_keep_it_remote() {
    let root = tempfile::tempdir().unwrap();
    let (config, mut partition) = Box::pin(tiered_partition_on_disk(root.path())).await;
    confirm(&mut partition, &SEALED);
    Box::pin(evict(&mut partition, u64::MAX, KEEP_NOTHING)).await;
    let before: Vec<_> = partition
        .log
        .segments()
        .iter()
        .map(|segment| (segment.start_offset, segment.end_offset, segment.location))
        .collect();
    drop(partition);

    let partition = Box::pin(reboot(&config)).await;

    let after: Vec<_> = partition
        .log
        .segments()
        .iter()
        .map(|segment| (segment.start_offset, segment.end_offset, segment.location))
        .collect();
    assert_eq!(after, before);
    assert_eq!(partition.object_prefix(), Some(PREFIX));
}

/// Retention raises the trim point before it unlinks, so boot neither refuses
/// the moved front nor brings back the remote segment it removed.
#[compio::test]
async fn given_retention_of_an_evicted_segment_when_rebooted_should_accept_the_new_front() {
    let root = tempfile::tempdir().unwrap();
    let (config, mut partition) = Box::pin(tiered_partition_on_disk(root.path())).await;
    let directory = PathBuf::from(config.get_partition_path(1, 1, 0));
    confirm(&mut partition, &SEALED);
    let first_end = partition.sealed_segment_record(0).unwrap().end_offset;
    Box::pin(evict(&mut partition, first_end, KEEP_NOTHING)).await;

    Box::pin(partition.remove_sealed_segments_up_to(first_end)).await;
    Box::pin(settle_partition_io(&mut partition, &config)).await;

    assert_eq!(partition.chain_trim_point(), Some(first_end));
    assert!(partition.sealed_segment_record(0).is_none());
    assert_eq!(
        chain_records(&directory).await.last(),
        Some(&ChainRecord::TrimPoint(first_end))
    );
    assert!(!directory.join(format!("{:020}.index", 0)).exists());
    drop(partition);

    let partition = Box::pin(reboot(&config)).await;
    assert_eq!(partition.log.segments()[0].start_offset, first_end + 1);
    assert_eq!(partition.chain_trim_point(), Some(first_end));
}

#[compio::test]
async fn given_a_chain_of_another_incarnation_when_adopting_should_refuse_it() {
    let (directory, partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let chain = read_chain(&directory.path().to_string_lossy())
        .await
        .unwrap()
        .unwrap();
    drop(partition);

    let mut other = Box::new(test_partition_with_replicas(3));
    other.set_partition_dir(directory.path().to_string_lossy().into_owned());
    other.set_created_revision(PREFIX.created_revision + 1);
    assert!(matches!(
        other.adopt_segment_chain(&chain),
        Err(ChainError::Prefix { recorded }) if recorded == PREFIX
    ));
    let moved = ObjectPrefix {
        partition_id: PREFIX.partition_id + 1,
        ..PREFIX
    };
    let moved_chain = crate::segment_chain::Chain {
        prefix: moved,
        ..chain
    };
    other.set_created_revision(PREFIX.created_revision);
    assert!(other.adopt_segment_chain(&moved_chain).is_err());
    assert!(other.object_prefix().is_none());
}

/// The WAL links its checkpoint-cursor segment and boot reads it, so a sealed
/// segment the checkpoint has not passed stays until the checkpoint moves on.
#[compio::test]
async fn given_the_wal_checkpoint_in_a_sealed_segment_when_evicting_should_wait_for_it_to_pass() {
    const RECORDS_PER_SEGMENT: u64 = 2;
    let root = tempfile::tempdir().unwrap();
    let mut config = repair_config();
    config.path_layout.streams_root = root.path().to_string_lossy().into_owned();
    let directory = PathBuf::from(config.get_partition_path(1, 1, 0));
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
    Box::pin(partition.open_segment_chain(PREFIX))
        .await
        .unwrap();
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    let mut parent = 0;
    for offset in 0..=RECORDS_PER_SEGMENT {
        let op = offset + 1;
        let prepare = checksummed_segment_prepare(op, parent, offset, b"payload");
        parent = prepare.header().checksum;
        let writer = hold_wal_writer(&persistence);
        Box::pin(partition.on_replicate(prepare)).await;
        writer.await;
        Box::pin(commit_on_heartbeat(&mut partition, &config, op)).await;
        Box::pin(settle_partition_io(&mut partition, &config)).await;
    }
    let checkpoint = persistence.segment_checkpoint().unwrap();
    assert_eq!(
        checkpoint.start_offset, 0,
        "the checkpoint is in the sealed segment"
    );
    confirm(&mut partition, &[0]);

    Box::pin(evict(&mut partition, u64::MAX, KEEP_NOTHING)).await;
    assert_eq!(partition.log.segments()[0].location, SegmentLocation::Local);
    assert!(directory.join(format!("{:020}.log", 0)).is_file());
    assert!(partition.needs_persistence_checkpoint());

    let writer = hold_wal_writer(&persistence);
    partition.checkpoint_persistence(&config).await;
    Box::pin(settle_partition_io(&mut partition, &config)).await;
    writer.await;
    Box::pin(settle_partition_io(&mut partition, &config)).await;
    let moved = persistence.segment_checkpoint().unwrap();
    assert!(
        moved.start_offset > 0,
        "the checkpoint moved past the sealed segment: {moved:?}"
    );
    Box::pin(evict(&mut partition, u64::MAX, KEEP_NOTHING)).await;
    assert_eq!(
        partition.log.segments()[0].location,
        SegmentLocation::Remote
    );
    assert!(!directory.join(format!("{:020}.log", 0)).exists());
}

/// The record is durable before the unlink, so a failed unlink costs only a
/// retry, which appends nothing.
#[compio::test]
async fn given_an_unlink_failing_after_the_record_when_evicting_should_retry_it_without_a_second_record()
 {
    let (directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    confirm(&mut partition, &SEALED[..1]);
    let floor = partition.sealed_segment_record(0).unwrap().end_offset;
    let log = directory.path().join(format!("{:020}.log", 0));
    let bytes = std::fs::read(&log).unwrap();
    // Unlinking a directory fails even for root.
    std::fs::remove_file(&log).unwrap();
    std::fs::create_dir(&log).unwrap();

    Box::pin(evict(&mut partition, floor, KEEP_NOTHING)).await;

    assert!(partition.fatal().is_none());
    assert_eq!(partition.log.segments()[0].location, SegmentLocation::Local);
    let records = chain_records(directory.path()).await;
    assert!(
        matches!(records.last(), Some(ChainRecord::Remote(record)) if record.start_offset == 0)
    );

    std::fs::remove_dir(&log).unwrap();
    std::fs::write(&log, bytes).unwrap();
    Box::pin(evict(&mut partition, floor, KEEP_NOTHING)).await;

    assert_eq!(
        partition.log.segments()[0].location,
        SegmentLocation::Remote
    );
    assert!(!log.exists());
    assert_eq!(chain_records(directory.path()).await, records);
}
