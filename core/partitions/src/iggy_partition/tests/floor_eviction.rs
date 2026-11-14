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

use super::eviction::{KEEP_NOTHING, chain_records, evict, reboot, tiered_partition_on_disk};
use super::segment_chain::{PREFIX, tiered_partition};
use super::*;
use crate::SegmentLocation;
use crate::segment_chain::{ChainRecord, chain_path, hash_segment_file};
use crate::state_machine::MachineCommand;
use crate::tiered_floor::{ADVANCE_TIERED_FLOOR, AdvanceTieredFloor};

/// Sealed segments of a partition with [`RECEIVER_OPS`] one-message ops, as
/// `(first offset, last offset)`.
const SEALED: [(u64, u64); 2] = [(0, 1), (2, 3)];

/// The advance the primary commits once the store holds the segment from
/// `start` to `up_to`, with the hash of the bytes this replica holds.
pub(super) async fn advance_over(
    directory: &Path,
    (start, up_to): (u64, u64),
) -> AdvanceTieredFloor {
    let log = directory.join(format!("{start:020}.log"));
    let size = std::fs::metadata(&log).unwrap().len();
    let hashes = hash_segment_file(&log.to_string_lossy(), size)
        .await
        .unwrap();
    AdvanceTieredFloor {
        start,
        up_to,
        hash: hashes.blake3,
    }
}

/// Apply `advance` as the commit path does: to the floor machine, and then to
/// the partition's own bookkeeping.
pub(super) async fn apply(
    partition: &mut IggyPartition<IggyMessageBus>,
    op: u64,
    advance: AdvanceTieredFloor,
) {
    let _ = partition.register_builtin_state_machines();
    let payload = advance.encode();
    let applied = partition
        .state_machines
        .apply(
            op,
            MachineCommand {
                kind: ADVANCE_TIERED_FLOOR,
                payload: &payload,
            },
        )
        .unwrap();
    assert_eq!(applied, crate::state_machine::Applied::Changed);
    partition.note_tiered_advance(advance);
    Box::pin(settle_partition_io(partition, &repair_config())).await;
}

fn locations(partition: &IggyPartition<IggyMessageBus>) -> Vec<SegmentLocation> {
    partition
        .log
        .segments()
        .iter()
        .map(|segment| segment.location)
        .collect()
}

const LOCAL: SegmentLocation = SegmentLocation::Local;
const REMOTE: SegmentLocation = SegmentLocation::Remote;

#[compio::test]
async fn given_an_envelope_that_keeps_nothing_when_advances_apply_should_evict_each_segment_they_confirm()
 {
    let (directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    partition.set_local_envelope(Some(KEEP_NOTHING));
    let first = Box::pin(advance_over(directory.path(), SEALED[0])).await;
    let second = Box::pin(advance_over(directory.path(), SEALED[1])).await;

    Box::pin(apply(&mut partition, 100, first)).await;

    assert_eq!(partition.tiered_floor(), Some(1));
    assert_eq!(locations(&partition), [REMOTE, LOCAL, LOCAL]);
    assert!(
        chain_records(directory.path())
            .await
            .contains(&ChainRecord::Advanced(first))
    );

    Box::pin(apply(&mut partition, 101, second)).await;

    assert_eq!(locations(&partition), [REMOTE, REMOTE, LOCAL]);
    assert_eq!(partition.tiered_divergences(), Vec::<u64>::new());
}

#[compio::test]
async fn given_no_envelope_when_an_advance_applies_should_confirm_and_wait_for_the_cleaner() {
    let (directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let first = Box::pin(advance_over(directory.path(), SEALED[0])).await;

    Box::pin(apply(&mut partition, 100, first)).await;
    assert_eq!(locations(&partition), [LOCAL, LOCAL, LOCAL]);

    // The cleaner's pass brings the envelope and evicts what is confirmed.
    partition.set_local_envelope(Some(KEEP_NOTHING));
    partition.stage_eviction(IggyTimestamp::now());
    Box::pin(settle_partition_io(&mut partition, &repair_config())).await;
    assert_eq!(locations(&partition), [REMOTE, LOCAL, LOCAL]);
}

#[compio::test]
async fn given_an_advance_that_contradicts_the_local_segment_when_it_applies_should_keep_the_segment_and_report_it()
 {
    let (directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    partition.set_local_envelope(Some(KEEP_NOTHING));
    let other_bytes = AdvanceTieredFloor {
        hash: [9; 32],
        ..Box::pin(advance_over(directory.path(), SEALED[0])).await
    };
    // The store's segment ends at offset 2, inside this replica's second one.
    let other_end = AdvanceTieredFloor {
        start: 2,
        up_to: 2,
        hash: [9; 32],
    };

    Box::pin(apply(&mut partition, 100, other_bytes)).await;
    Box::pin(apply(&mut partition, 101, other_end)).await;

    assert_eq!(partition.tiered_floor(), Some(2));
    assert_eq!(partition.tiered_divergences(), [1, 2]);
    assert_eq!(locations(&partition), [LOCAL, LOCAL, LOCAL]);
    // Nothing later clears a divergence, so the segments never leave.
    Box::pin(evict(&mut partition, u64::MAX, KEEP_NOTHING)).await;
    assert_eq!(locations(&partition), [LOCAL, LOCAL, LOCAL]);
}

#[compio::test]
async fn given_advances_before_the_chain_exists_when_tiering_is_enabled_should_record_and_settle_them()
 {
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
    Box::pin(commit_records(
        &mut partition,
        1,
        &segment_records(RECEIVER_OPS),
    ))
    .await;
    partition.set_local_envelope(Some(KEEP_NOTHING));
    let first = Box::pin(advance_over(directory.path(), SEALED[0])).await;

    Box::pin(apply(&mut partition, 100, first)).await;
    assert!(!partition.is_tiered());
    assert_eq!(locations(&partition), [LOCAL, LOCAL, LOCAL]);

    partition.enable_tiering(PREFIX);
    Box::pin(settle_partition_io(&mut partition, &config)).await;

    // The advance is recorded before the segment's hash from disk settles it.
    assert_eq!(locations(&partition), [REMOTE, LOCAL, LOCAL]);
}

#[compio::test]
async fn given_a_recorded_advance_when_the_replica_restarts_should_still_confirm_its_segment() {
    let root = tempfile::tempdir().unwrap();
    let (config, mut partition) = Box::pin(tiered_partition_on_disk(root.path())).await;
    let directory = PathBuf::from(config.get_partition_path(1, 1, 0));
    let first = Box::pin(advance_over(&directory, SEALED[0])).await;
    Box::pin(apply(&mut partition, 100, first)).await;
    assert_eq!(locations(&partition), [LOCAL, LOCAL, LOCAL]);
    drop(partition);

    let mut rebooted = Box::pin(reboot(&config)).await;

    // The floor comes back with the machine's snapshot, which this bare
    // reboot does not restore, and nothing leaves above the floor.
    rebooted.set_local_envelope(Some(KEEP_NOTHING));
    rebooted.stage_eviction(IggyTimestamp::now());
    Box::pin(settle_partition_io(&mut rebooted, &repair_config())).await;
    assert_eq!(locations(&rebooted)[..2], [LOCAL, LOCAL]);

    Box::pin(evict(&mut rebooted, u64::MAX, KEEP_NOTHING)).await;
    assert_eq!(
        locations(&rebooted)[..2],
        [REMOTE, LOCAL],
        "only the segment an advance confirmed may leave"
    );
}

#[compio::test]
async fn given_a_committed_advance_when_it_applies_should_reach_the_partition_once() {
    let mut partition = Box::new(test_partition_with_replicas(1));
    partition.register_builtin_state_machines().unwrap();
    let advance = AdvanceTieredFloor {
        start: 0,
        up_to: 9,
        hash: [7; 32],
    };
    // A refused advance changes nothing, so the partition never hears of it.
    let gap = AdvanceTieredFloor {
        start: 20,
        up_to: 29,
        hash: [7; 32],
    };
    for (request, advance) in [(1, advance), (2, gap)] {
        let request = partition
            .advance_tiered_floor_request(&advance, request)
            .unwrap();
        Box::pin(partition.on_request(request, None)).await;
    }

    crate::state_machine::tests::commit_all(&mut partition).await;

    assert_eq!(partition.tiered_floor(), Some(9));
    assert_eq!(partition.pending_advances, [advance]);
}

/// A partition with an open chain and a WAL that a checkpoint is due on, and
/// its directory.
async fn partition_due_a_checkpoint() -> (
    tempfile::TempDir,
    PartitionsConfig,
    PathBuf,
    IggyPartition<RecordingBus>,
) {
    let root = tempfile::tempdir().unwrap();
    let mut config = repair_config();
    config.path_layout.streams_root = root.path().to_string_lossy().into_owned();
    let directory = PathBuf::from(config.get_partition_path(1, 1, 0));
    std::fs::create_dir_all(&directory).unwrap();
    let (mut partition, _) = recording_partition_at(1, 3);
    partition.set_partition_dir(directory.to_string_lossy().into_owned());
    partition.runtime_options.durability = iggy_common::Durability::Replicated;
    partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Replicated;
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
    let prepare = checksummed_segment_prepare(1, 0, 0, b"payload");
    let writer = hold_wal_writer(&persistence);
    Box::pin(partition.on_replicate(prepare)).await;
    writer.await;
    Box::pin(commit_on_heartbeat(&mut partition, &config, 1)).await;
    Box::pin(settle_partition_io(&mut partition, &config)).await;
    persistence.request_checkpoint();
    assert!(partition.needs_persistence_checkpoint());
    (root, config, directory, partition)
}

const UNSEALED_ADVANCE: AdvanceTieredFloor = AdvanceTieredFloor {
    start: 0,
    up_to: 0,
    hash: [7; 32],
};

/// The checkpoint keeps the floor and lets the WAL drop the advance, so a
/// checkpoint taken before the chain holds the advance would leave a restarted
/// replica with a floor it can never match against its segments.
#[compio::test]
async fn given_an_advance_not_yet_in_the_chain_when_a_checkpoint_is_due_should_wait_for_its_record()
{
    let (_root, config, directory, mut partition) = Box::pin(partition_due_a_checkpoint()).await;
    let advance = UNSEALED_ADVANCE;

    partition.note_tiered_advance(advance);

    assert!(!partition.needs_persistence_checkpoint());
    Box::pin(settle_partition_io(&mut partition, &config)).await;
    assert_eq!(
        chain_records(&directory).await,
        [ChainRecord::Advanced(advance)]
    );
    assert!(partition.needs_persistence_checkpoint());
}

/// A checkpoint can still run while the partition shuts down, so the advances
/// it would outrun are recorded first. The rest of the chain's work is left
/// for the next start.
#[compio::test]
async fn given_an_advance_not_yet_in_the_chain_when_shutting_down_should_still_record_it() {
    let (_root, config, directory, mut partition) = Box::pin(partition_due_a_checkpoint()).await;
    partition.note_tiered_advance(UNSEALED_ADVANCE);

    partition.begin_shutdown_io();

    assert!(partition.checkpoint_waits_for_advances());
    Box::pin(settle_partition_io(&mut partition, &config)).await;
    assert_eq!(
        chain_records(&directory).await,
        [ChainRecord::Advanced(UNSEALED_ADVANCE)]
    );
    assert!(!partition.checkpoint_waits_for_advances());
    assert_eq!(partition.pending_tiering(), None);
}

/// Put a directory where the chain file is, so every append to it fails.
fn break_chain_file(directory: &Path) -> PathBuf {
    let chain = PathBuf::from(chain_path(&directory.to_string_lossy()));
    let kept = chain.with_extension("kept");
    std::fs::rename(&chain, &kept).unwrap();
    std::fs::create_dir(&chain).unwrap();
    kept
}

/// A failed append is retried after a delay that grows to a minute, and the
/// WAL must not grow for as long as the chain file stays unwritable.
#[compio::test]
async fn given_an_advance_whose_record_fails_when_a_checkpoint_is_due_should_not_wait_for_the_retry()
 {
    let (_root, config, directory, mut partition) = Box::pin(partition_due_a_checkpoint()).await;
    let kept = break_chain_file(&directory);
    partition.note_tiered_advance(UNSEALED_ADVANCE);
    assert!(!partition.needs_persistence_checkpoint());

    Box::pin(settle_partition_io(&mut partition, &config)).await;

    assert_eq!(partition.pending_advances, [UNSEALED_ADVANCE]);
    assert_eq!(
        partition.pending_tiering(),
        Some(PREFIX),
        "the retry is armed"
    );
    assert!(partition.needs_persistence_checkpoint());

    // The shutdown does not wait for the retry either.
    partition.begin_shutdown_io();
    Box::pin(settle_partition_io(&mut partition, &config)).await;
    assert_eq!(partition.pending_tiering(), None);
    assert!(!partition.checkpoint_waits_for_advances());
    std::fs::remove_dir(chain_path(&directory.to_string_lossy())).unwrap();
    std::fs::rename(kept, chain_path(&directory.to_string_lossy())).unwrap();
    assert_eq!(chain_records(&directory).await, []);
}

/// While segments wait to be hashed the lane always has a next job, so the
/// queue is never found idle and emptied as a whole.
#[compio::test]
async fn given_a_recorded_advance_when_the_next_job_is_chosen_should_leave_the_queue() {
    let config = repair_config();
    let records = segment_records(RECEIVER_OPS);
    let (directory, mut partition) = Box::pin(tiered_partition(&records, records.len())).await;
    let first = Box::pin(advance_over(directory.path(), SEALED[0])).await;
    let second = Box::pin(advance_over(directory.path(), SEALED[1])).await;
    partition.note_tiered_advance(first);
    partition.note_tiered_advance(second);

    let crate::PartitionIoStep::Ready(plan) = partition.resume_io(&config).await else {
        panic!("the first advance is ready to record");
    };
    Box::pin(execute_partition_io_plan(&mut partition, &config, plan)).await;
    assert_eq!(
        chain_records(directory.path()).await,
        [ChainRecord::Advanced(first)]
    );
    assert!(matches!(
        partition.resume_io(&config).await,
        crate::PartitionIoStep::Ready(_)
    ));

    assert_eq!(partition.pending_advances, [second]);
}
