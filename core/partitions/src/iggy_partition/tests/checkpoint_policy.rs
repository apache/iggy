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

//! The checkpoint rule of [`crate::CheckpointDelays`]: when a partition comes
//! due, what a pop of the shard queue requests, and how a graceful stop holds
//! a partition until its checkpoint starts.

use super::*;

use std::path::Path;
use std::time::Duration;

use consensus::TICK_INTERVAL;
use server_common::sharding::ShardId;

use crate::iggy_partition::PartitionShutdownPhase;
use crate::iggy_partitions::QUEUED_CHECKPOINTS_IN_FLIGHT_MAX;
use crate::{CheckpointDelays, CheckpointTrigger, IggyPartitions, PartitionPersistence};

/// The shipped delays.
const IDLE_DELAY: Duration = Duration::from_secs(30);
const MAX_DELAY: Duration = Duration::from_mins(5);
/// The ops of `message_less_origin`.
const ORIGIN_OPS: u64 = 3;
/// The starts that the shard tick allows the queue.
const STARTS_PER_TICK: usize = 16;

fn delays() -> CheckpointDelays {
    CheckpointDelays::new(IDLE_DELAY, MAX_DELAY)
}

fn ticks(delay: Duration) -> u64 {
    u64::try_from(delay.as_nanos() / TICK_INTERVAL.as_nanos()).unwrap()
}

fn advance(partition: &IggyPartition<RecordingBus>, ticks: u64) {
    let clock = partition.consensus().tick_clock();
    for _ in 0..ticks {
        clock.advance();
    }
}

fn now(partition: &IggyPartition<RecordingBus>) -> u64 {
    partition.consensus().tick_clock().now()
}

/// A replica that keeps prepare history under `root`, with the ops of
/// `origin` journaled through `journaled` and committed through `committed`.
async fn replica_of(
    root: &Path,
    origin: &IggyPartition<RecordingBus>,
    committed: u64,
    journaled: u64,
) -> (PartitionsConfig, IggyPartition<RecordingBus>) {
    let (config, mut replica) = Box::pin(replica_with_prepare_history(root, 1)).await;
    Box::pin(commit_replicated_through(
        &mut replica,
        origin,
        &config,
        committed,
    ))
    .await;
    for op in committed + 1..=journaled {
        Box::pin(journal_uncommitted(&mut replica, origin, op)).await;
    }
    (config, replica)
}

async fn journal_uncommitted(
    replica: &mut IggyPartition<RecordingBus>,
    origin: &IggyPartition<RecordingBus>,
    op: u64,
) {
    replica.on_replicate(retained_prepare(origin, op)).await;
    Rc::clone(replica.persistence.as_ref().unwrap())
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();
    assert_eq!(replica.consensus().commit_min(), op - 1);
}

/// Runs the stop of `partition` to its end, as the shard drain does.
async fn finish_stop(partition: &mut IggyPartition<RecordingBus>, config: &PartitionsConfig) {
    const ROUNDS_MAX: usize = 8;
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    for _ in 0..ROUNDS_MAX {
        Box::pin(settle_partition_io(partition, config)).await;
        if partition.shutdown_io_complete() {
            return;
        }
        persistence
            .drain_with_timeout(compio::runtime::time::sleep)
            .await
            .unwrap();
        partition.drive_persistence().await;
    }
    panic!("the stop did not end in {ROUNDS_MAX} rounds");
}

/// The WAL that the next boot of the partition under `config` opens, and
/// the prepares it loads.
async fn reopened_wal(
    config: &PartitionsConfig,
    created_revision: u64,
) -> (Rc<PartitionPersistence>, Vec<Message<PrepareHeader>>) {
    let directory =
        Path::new(&config.get_partition_path(1, 1, 0)).join(format!("prepares-{created_revision}"));
    PartitionPersistence::open(
        &directory,
        IggyNamespace::new(1, 1, 0).inner(),
        created_revision,
    )
    .await
    .unwrap()
}

fn ops_of(prepares: &[Message<PrepareHeader>]) -> Vec<u64> {
    prepares.iter().map(|prepare| prepare.header().op).collect()
}

/// Boots the partition under `config` again in the order of the server: the
/// WAL, the receipts at its checkpoint, the segments that the WAL vouches
/// for, then the replay of the WAL.
async fn reboot(config: &PartitionsConfig, created_revision: u64) -> IggyPartition<RecordingBus> {
    let directory = config.get_partition_path(1, 1, 0);
    let wal = reopened_wal(config, created_revision).await;
    let (mut partition, _) = recording_partition_at(1, 3);
    partition.set_partition_dir(directory.clone());
    Box::pin(partition.restore_retry_checkpoint(wal.0.checkpoint_op()))
        .await
        .unwrap();
    let segments = crate::segment_recovery::load_persisted_segments_with_checkpoint(
        config,
        IggyNamespace::new(1, 1, 0),
        partition.effective_segment_size(),
        false,
        &PartitionStats::default(),
        wal.0.segment_checkpoint(),
    )
    .await
    .unwrap();
    partition.log.retire_front().unwrap();
    crate::partition_storage::hydrate_partition_log(&mut partition, &directory, 1, 1, 0, segments)
        .await
        .unwrap();
    Box::pin(partition.open_persistence_with_recovered(
        journal::partition_journal::PARTITION_WAL_BYTES_MAX,
        Duration::ZERO,
        Some(wal),
    ))
    .await
    .unwrap();
    partition
}

#[compio::test]
async fn given_ops_when_journaled_should_come_due_one_idle_delay_after_the_last() {
    let (_origin_directory, origin, _) = Box::pin(message_less_origin()).await;
    let root = tempfile::tempdir().unwrap();
    let (config, mut replica) = Box::pin(replica_with_prepare_history(root.path(), 1)).await;
    replica.reset_checkpoint_stamps();
    assert_eq!(
        replica.checkpoint_deadline(delays()),
        None,
        "a fresh replica"
    );

    Box::pin(commit_replicated_through(&mut replica, &origin, &config, 1)).await;
    let first = now(&replica);
    assert_eq!(
        replica.checkpoint_deadline(delays()),
        Some(first + ticks(IDLE_DELAY))
    );

    advance(&replica, ticks(IDLE_DELAY) - 1);
    journal_uncommitted(&mut replica, &origin, 2).await;
    assert_eq!(
        replica.checkpoint_deadline(delays()),
        Some(now(&replica) + ticks(IDLE_DELAY)),
        "the last op moves the idle deadline"
    );
    assert_eq!(replica.checkpoint_first_op_tick(), Some(first));
}

/// A writer that never pauses for the idle delay still gets a checkpoint at
/// the max delay after its first op.
#[compio::test]
async fn given_ops_that_never_pause_when_ticks_pass_should_come_due_at_the_max_delay() {
    let (_origin_directory, origin, _) = Box::pin(message_less_origin()).await;
    let root = tempfile::tempdir().unwrap();
    let (_config, replica) = Box::pin(replica_of(root.path(), &origin, 1, 1)).await;
    let first = now(&replica);
    let age_deadline = first + ticks(MAX_DELAY);

    while now(&replica) < age_deadline {
        if (now(&replica) - first).is_multiple_of(ticks(IDLE_DELAY) - 1) {
            replica.stamp_checkpoint_op();
        }
        assert_eq!(
            replica.take_due_checkpoint(now(&replica), delays()),
            None,
            "due early at tick {}",
            now(&replica)
        );
        advance(&replica, 1);
    }

    assert_eq!(replica.checkpoint_deadline(delays()), Some(age_deadline));
    assert_eq!(
        replica.take_due_checkpoint(now(&replica), delays()),
        Some(CheckpointTrigger::Age)
    );
    assert!(replica.in_checkpoint_queue());
    assert_eq!(replica.checkpoint_deadline(delays()), None);
}

#[compio::test]
async fn given_a_due_partition_when_nothing_is_committed_above_its_checkpoint_should_stay_out_of_the_queue()
 {
    let (_origin_directory, origin, _) = Box::pin(message_less_origin()).await;
    let root = tempfile::tempdir().unwrap();
    let (config, mut replica) = Box::pin(replica_of(root.path(), &origin, 2, 2)).await;
    Rc::clone(replica.persistence.as_ref().unwrap()).request_checkpoint();
    assert_eq!(
        run_checkpoint(&mut replica, &config).await,
        Some(CheckpointTrigger::Demand)
    );
    assert_eq!(
        replica.checkpoint_deadline(delays()),
        None,
        "the checkpoint covered every op"
    );

    journal_uncommitted(&mut replica, &origin, ORIGIN_OPS).await;
    advance(&replica, ticks(IDLE_DELAY));
    assert_eq!(replica.take_due_checkpoint(now(&replica), delays()), None);
    assert!(!replica.in_checkpoint_queue());
    assert_eq!(
        replica.checkpoint_deadline(delays()),
        Some(now(&replica) + ticks(IDLE_DELAY)),
        "an uncommitted op arms the deadline again"
    );

    Box::pin(commit_replicated_through(
        &mut replica,
        &origin,
        &config,
        ORIGIN_OPS,
    ))
    .await;
    advance(&replica, ticks(IDLE_DELAY));
    assert_eq!(
        replica.take_due_checkpoint(now(&replica), delays()),
        Some(CheckpointTrigger::Idle)
    );
}

/// The checkpoint of a queue entry covers `commit_min`, the receipts point,
/// and the WAL keeps one record there. Ops above it keep a deadline from the
/// start.
#[compio::test]
async fn given_a_queued_partition_when_popped_and_started_should_checkpoint_through_commit_min() {
    let (_origin_directory, origin, _) = Box::pin(message_less_origin()).await;
    let root = tempfile::tempdir().unwrap();
    let (config, mut replica) = Box::pin(replica_of(root.path(), &origin, 2, ORIGIN_OPS)).await;
    advance(&replica, ticks(IDLE_DELAY));
    assert_eq!(
        replica.take_due_checkpoint(now(&replica), delays()),
        Some(CheckpointTrigger::Idle)
    );

    assert!(replica.request_queued_checkpoint(CheckpointTrigger::Idle));
    assert!(!replica.in_checkpoint_queue());
    assert!(replica.queued_checkpoint_in_flight());
    let started = now(&replica);
    assert_eq!(
        run_checkpoint(&mut replica, &config).await,
        Some(CheckpointTrigger::Idle)
    );
    assert_eq!(
        replica.persistence.as_ref().unwrap().checkpoint_op(),
        replica.consensus().commit_min()
    );
    assert!(!replica.queued_checkpoint_in_flight());
    assert_eq!(
        replica.checkpoint_deadline(delays()),
        Some(started + ticks(IDLE_DELAY)),
        "the uncommitted op restarts the stamps at the start"
    );

    let created_revision = replica.created_revision();
    drop(replica);
    let (wal, prepares) = reopened_wal(&config, created_revision).await;
    let ops = ops_of(&prepares);
    assert_eq!((wal.checkpoint_op(), ops), (2, vec![2, ORIGIN_OPS]));
}

/// A pop requests a checkpoint only if one could start and cover a commit. A
/// request behind a running checkpoint would stop the run-ahead after it
/// ends, with nothing left to cover.
#[compio::test]
async fn given_a_popped_partition_when_no_checkpoint_can_start_should_request_nothing() {
    let (_origin_directory, origin, _) = Box::pin(message_less_origin()).await;
    let root = tempfile::tempdir().unwrap();
    let (config, mut replica) = Box::pin(replica_of(root.path(), &origin, 2, 2)).await;
    dispatch_io(&replica);
    Rc::clone(replica.persistence.as_ref().unwrap()).request_checkpoint();
    assert!(replica.needs_persistence_checkpoint());
    assert!(replica.checkpoint_persistence(&config).await.is_some());

    replica.join_checkpoint_queue();
    assert!(
        !replica.request_queued_checkpoint(CheckpointTrigger::Idle),
        "a checkpoint runs"
    );
    assert!(!replica.in_checkpoint_queue());
    finish_checkpoint(&mut replica, &config).await;
    Box::pin(commit_replicated_through(
        &mut replica,
        &origin,
        &config,
        ORIGIN_OPS,
    ))
    .await;
    assert!(
        !replica.needs_persistence_checkpoint(),
        "no request waited behind the checkpoint"
    );

    Rc::clone(replica.persistence.as_ref().unwrap()).request_checkpoint();
    assert!(run_checkpoint(&mut replica, &config).await.is_some());
    replica.join_checkpoint_queue();
    replica.hold_shutdown_for_checkpoint();
    assert!(
        !replica.request_queued_checkpoint(CheckpointTrigger::Idle),
        "commit_min is at the checkpoint"
    );
    assert!(!replica.needs_persistence_checkpoint());
    assert!(
        !replica.holds_shutdown_for_checkpoint(),
        "a stop does not wait for a start that cannot come"
    );
}

#[compio::test]
async fn given_an_op_journaled_while_a_checkpoint_runs_should_keep_a_deadline() {
    let (_origin_directory, origin, _) = Box::pin(message_less_origin()).await;
    let root = tempfile::tempdir().unwrap();
    let (config, mut replica) = Box::pin(replica_of(root.path(), &origin, 2, 2)).await;
    dispatch_io(&replica);
    Rc::clone(replica.persistence.as_ref().unwrap()).request_checkpoint();
    assert!(replica.checkpoint_persistence(&config).await.is_some());
    assert_eq!(replica.checkpoint_deadline(delays()), None);

    journal_uncommitted(&mut replica, &origin, ORIGIN_OPS).await;
    assert_eq!(
        replica.checkpoint_deadline(delays()),
        Some(now(&replica) + ticks(IDLE_DELAY))
    );
    advance(&replica, ticks(IDLE_DELAY));
    assert!(
        replica.maintenance.checkpoint.is_some(),
        "the checkpoint runs"
    );
    assert_eq!(replica.take_due_checkpoint(now(&replica), delays()), None);
    assert_eq!(
        replica.checkpoint_deadline(delays()),
        Some(now(&replica) + ticks(IDLE_DELAY)),
        "a running checkpoint arms the deadline again"
    );

    finish_checkpoint(&mut replica, &config).await;
    Box::pin(commit_replicated_through(
        &mut replica,
        &origin,
        &config,
        ORIGIN_OPS,
    ))
    .await;
    advance(&replica, ticks(IDLE_DELAY));
    assert_eq!(
        replica.take_due_checkpoint(now(&replica), delays()),
        Some(CheckpointTrigger::Idle)
    );
}

/// `insert` installs the shard clock and stamps a partition whose boot
/// reloaded ops above its checkpoint. A clean partition gets no stamps, so it
/// does not wake one idle delay after the boot.
#[compio::test]
async fn given_a_boot_when_inserted_should_stamp_only_a_partition_with_ops_above_its_checkpoint() {
    let (_origin_directory, origin, _) = Box::pin(message_less_origin()).await;
    for (journaled, stamped) in [(ORIGIN_OPS, true), (2, false)] {
        let root = tempfile::tempdir().unwrap();
        let (config, mut replica) = Box::pin(replica_of(root.path(), &origin, 2, journaled)).await;
        Rc::clone(replica.persistence.as_ref().unwrap()).request_checkpoint();
        assert!(run_checkpoint(&mut replica, &config).await.is_some());
        drop(replica);

        let (config, rebooted) = Box::pin(replica_with_prepare_history(root.path(), 1)).await;
        let namespace = IggyNamespace::new(1, 1, 0);
        let partitions =
            IggyPartitions::new(ShardId::new(0), config).with_checkpoint_delays(delays());
        let clock = partitions.tick_clock();
        for _ in 0..ticks(IDLE_DELAY) {
            clock.advance();
        }
        partitions.insert(namespace, rebooted);

        let rebooted = partitions.peek_by_ns(&namespace).unwrap();
        assert_eq!(
            rebooted.checkpoint_deadline(delays()),
            stamped.then(|| clock.now() + ticks(IDLE_DELAY)),
            "journaled through {journaled}"
        );
    }
}

/// The stop holds a partition it serves before the drain of its writers, so
/// the exit cannot cut a checkpoint that starts late. The start ends the
/// hold, and the drain finishes the checkpoint.
#[compio::test]
async fn given_a_held_partition_when_stopping_should_drain_only_after_its_checkpoint_starts() {
    let (_origin_directory, origin, _) = Box::pin(message_less_origin()).await;
    let root = tempfile::tempdir().unwrap();
    let (config, mut replica) = Box::pin(replica_of(root.path(), &origin, 2, 2)).await;
    replica.join_checkpoint_queue();
    replica.hold_shutdown_for_checkpoint();
    replica.begin_shutdown_io();
    Box::pin(settle_partition_io(&mut replica, &config)).await;
    assert!(matches!(
        replica.maintenance.shutdown,
        Some(PartitionShutdownPhase::CheckpointHold)
    ));
    assert!(!replica.shutdown_io_complete());

    assert!(replica.request_queued_checkpoint(CheckpointTrigger::Stop));
    assert_eq!(
        replica.checkpoint_persistence(&config).await,
        Some(CheckpointTrigger::Stop)
    );
    assert!(!replica.holds_shutdown_for_checkpoint());
    finish_checkpoint(&mut replica, &config).await;
    finish_stop(&mut replica, &config).await;

    let created_revision = replica.created_revision();
    drop(replica);
    let (wal, prepares) = reopened_wal(&config, created_revision).await;
    let ops = ops_of(&prepares);
    assert_eq!((wal.checkpoint_op(), ops), (2, vec![2]));
}

/// Past the hold no checkpoint starts, because the exit can cut it. A
/// request from before the stop does not start one either.
#[compio::test]
async fn given_a_stopping_partition_without_a_hold_should_start_no_checkpoint() {
    let (_origin_directory, origin, _) = Box::pin(message_less_origin()).await;
    for held in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let (config, mut replica) = Box::pin(replica_of(root.path(), &origin, 2, 2)).await;
        Rc::clone(replica.persistence.as_ref().unwrap()).request_checkpoint();
        if held {
            replica.hold_shutdown_for_checkpoint();
        }
        replica.begin_shutdown_io();
        Box::pin(settle_partition_io(&mut replica, &config)).await;
        if held {
            assert!(!replica.shutdown_io_complete(), "the hold waits");
            assert!(replica.release_checkpoint_hold(), "the shard ends the hold");
        }

        assert!(!replica.needs_persistence_checkpoint(), "held {held}");
        assert_eq!(replica.checkpoint_persistence(&config).await, None);
        finish_stop(&mut replica, &config).await;
        assert_eq!(replica.persistence.as_ref().unwrap().checkpoint_op(), 0);
    }
}

/// The replay hands the segment record of the checkpoint op to the ring, so
/// a restarted partition can release that body once a newer op arrives. The
/// WAL has to keep the record through the checkpoint rewrite for that.
#[compio::test]
async fn given_a_checkpointed_message_op_when_the_wal_reopens_should_keep_its_segment_record() {
    let root = tempfile::tempdir().unwrap();
    let mut config = repair_config();
    config.path_layout.streams_root = root.path().to_string_lossy().into_owned();
    let directory = config.get_partition_path(1, 1, 0);
    std::fs::create_dir_all(&directory).unwrap();
    let (mut partition, _) = recording_partition_at(1, 3);
    partition.set_partition_dir(directory);
    partition.runtime_options.durability = iggy_common::Durability::Replicated;
    partition.open_persistence().await.unwrap();
    partition.log.retire_front().unwrap();
    partition.install_empty_segment(&config, 0).await.unwrap();
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    let writer = hold_wal_writer(&persistence);
    Box::pin(partition.on_replicate(checksummed_segment_prepare(1, 0, 0, b"payload"))).await;
    writer.await;
    Box::pin(commit_on_heartbeat(&mut partition, &config, 1)).await;
    Box::pin(settle_partition_io(&mut partition, &config)).await;
    assert!(persistence.segment_reference(1).is_some());

    persistence.request_checkpoint();
    assert!(run_checkpoint(&mut partition, &config).await.is_some());
    let created_revision = partition.created_revision();
    drop(persistence);
    drop(partition);

    let (wal, prepares) = reopened_wal(&config, created_revision).await;
    let ops = ops_of(&prepares);
    assert_eq!((wal.checkpoint_op(), ops), (1, vec![1]));
    assert!(wal.segment_reference(1).is_some());
}

/// A table partition whose WAL holds op 1, committed above its checkpoint at
/// 0. A checkpoint start on the I/O lane only enters its first phase.
async fn committed_wal_partition(directory: &Path) -> IggyPartition<RecordingBus> {
    std::fs::create_dir_all(directory).unwrap();
    let (mut partition, _) = recording_partition_at(0, 3);
    partition.set_partition_dir(directory.to_string_lossy().into_owned());
    partition.open_persistence().await.unwrap();
    dispatch_io(&partition);
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
    partition
}

fn checkpoint_table() -> IggyPartitions<RecordingBus> {
    IggyPartitions::new(ShardId::new(0), repair_config()).with_checkpoint_delays(delays())
}

fn advance_table(partitions: &IggyPartitions<RecordingBus>, ticks: u64) {
    for _ in 0..ticks {
        partitions.tick_clock().advance();
    }
}

/// Queues every partition of `namespaces` that is due, as the shard tick
/// does.
fn queue_due(partitions: &IggyPartitions<RecordingBus>, namespaces: &[IggyNamespace]) {
    let now = partitions.tick_clock().now();
    for namespace in namespaces {
        let partition = partitions.peek_by_ns(namespace).unwrap();
        if let Some(trigger) = partition.take_due_checkpoint(now, delays()) {
            partitions.enqueue_checkpoint(*namespace, trigger);
        }
    }
}

#[compio::test]
async fn given_more_due_partitions_than_the_cap_when_popping_should_pace_the_requests() {
    const PARTITIONS: usize = QUEUED_CHECKPOINTS_IN_FLIGHT_MAX + STARTS_PER_TICK;
    const FENCED: usize = 3;
    let root = tempfile::tempdir().unwrap();
    let partitions = checkpoint_table();
    let mut namespaces = Vec::with_capacity(PARTITIONS);
    for index in 0..PARTITIONS {
        let namespace = IggyNamespace::new(1, 1, index);
        let partition = committed_wal_partition(&root.path().join(index.to_string())).await;
        partitions.insert(namespace, partition);
        namespaces.push(namespace);
    }
    advance_table(&partitions, ticks(IDLE_DELAY));
    queue_due(&partitions, &namespaces);
    assert_eq!(partitions.checkpoint_queue_len(), PARTITIONS);

    for tick in 1..=QUEUED_CHECKPOINTS_IN_FLIGHT_MAX / STARTS_PER_TICK {
        assert_eq!(
            partitions.request_queued_checkpoints(STARTS_PER_TICK),
            STARTS_PER_TICK
        );
        assert_eq!(
            partitions.queued_checkpoints_in_flight(),
            tick * STARTS_PER_TICK
        );
    }
    assert_eq!(
        partitions.request_queued_checkpoints(STARTS_PER_TICK),
        0,
        "the cap of requested or running checkpoints holds"
    );
    assert_eq!(
        partitions.checkpoint_queue_len(),
        PARTITIONS - QUEUED_CHECKPOINTS_IN_FLIGHT_MAX
    );

    // A fenced partition starts no checkpoint, so it gives its slot back.
    for namespace in &namespaces[..FENCED] {
        partitions
            .get_io_owner(namespace)
            .unwrap()
            .fence_flush_failure();
    }
    assert_eq!(
        partitions.request_queued_checkpoints(STARTS_PER_TICK),
        FENCED
    );
    assert_eq!(
        partitions.queued_checkpoints_in_flight(),
        QUEUED_CHECKPOINTS_IN_FLIGHT_MAX
    );
}

/// The partitions of [`plan_stop`]: two requests that wait for their start,
/// three queue entries, two other partitions with a committed op above their
/// checkpoint (the second one older), and a clean one.
struct StopTable {
    partitions: IggyPartitions<RecordingBus>,
    waiting: [IggyNamespace; 2],
    queued: [IggyNamespace; 3],
    newer: IggyNamespace,
    older: IggyNamespace,
    clean: IggyNamespace,
    _root: tempfile::TempDir,
}

async fn stop_table() -> StopTable {
    let root = tempfile::tempdir().unwrap();
    let partitions = checkpoint_table();
    let namespace = |index| IggyNamespace::new(1, 1, index);
    let insert = async |index: usize| {
        let partition = committed_wal_partition(&root.path().join(index.to_string())).await;
        partitions.insert(namespace(index), partition);
        namespace(index)
    };
    let mut queued_first = Vec::new();
    for index in 0..5 {
        queued_first.push(insert(index).await);
    }
    advance_table(&partitions, 1);
    let older = insert(5).await;
    advance_table(&partitions, 1);
    let newer = insert(6).await;
    let clean_directory = root.path().join("clean");
    std::fs::create_dir_all(&clean_directory).unwrap();
    let (mut clean, _) = recording_partition_at(0, 3);
    clean.set_partition_dir(clean_directory.to_string_lossy().into_owned());
    clean.open_persistence().await.unwrap();
    partitions.insert(namespace(7), clean);

    advance_table(&partitions, ticks(IDLE_DELAY));
    queue_due(&partitions, &queued_first);
    assert_eq!(partitions.request_queued_checkpoints(2), 2);
    StopTable {
        partitions,
        waiting: [queued_first[0], queued_first[1]],
        queued: [queued_first[2], queued_first[3], queued_first[4]],
        newer,
        older,
        clean: namespace(7),
        _root: root,
    }
}

fn held(partitions: &IggyPartitions<RecordingBus>, namespace: IggyNamespace) -> bool {
    partitions
        .peek_by_ns(&namespace)
        .unwrap()
        .holds_shutdown_for_checkpoint()
}

/// The stop serves the waiting requests first, then the queue, then the
/// partition with the oldest first op, up to its reach. The rest passes the
/// hold at once.
#[compio::test]
async fn given_a_stop_when_planning_should_hold_what_its_reach_can_serve_in_order() {
    let table = stop_table().await;
    let partitions = &table.partitions;

    assert_eq!(partitions.plan_stop_checkpoints(6), 1, "the newer one");

    for namespace in table.waiting.iter().chain(&table.queued) {
        assert!(held(partitions, *namespace));
    }
    assert!(held(partitions, table.older));
    assert!(!held(partitions, table.newer));
    assert!(!held(partitions, table.clean));
    assert_eq!(partitions.checkpoint_queue_len(), 4);

    let short = stop_table().await;
    assert_eq!(
        short.partitions.plan_stop_checkpoints(3),
        4,
        "two queue entries and both others"
    );
    for namespace in short.waiting.iter().chain(&short.queued[..1]) {
        assert!(held(&short.partitions, *namespace));
    }
    for namespace in short.queued[1..].iter().chain([&short.older, &short.newer]) {
        assert!(!held(&short.partitions, *namespace));
    }
}

/// At the deadline the shard ends every hold. It counts the partitions
/// whose checkpoint did not start, and no checkpoint starts for them.
#[compio::test]
async fn given_held_partitions_when_the_stop_deadline_passes_should_release_and_count_the_unstarted()
 {
    let table = stop_table().await;
    let partitions = &table.partitions;
    assert_eq!(partitions.plan_stop_checkpoints(6), 1);
    for namespace in partitions.namespaces() {
        partitions
            .get_io_owner(namespace)
            .unwrap()
            .begin_shutdown_io();
    }
    assert_eq!(partitions.request_queued_checkpoints(STARTS_PER_TICK), 4);
    let config = partitions.config().clone();
    let older = partitions.get_io_owner(&table.older).unwrap();
    assert!(older.needs_persistence_checkpoint());
    assert_eq!(
        older.checkpoint_persistence(&config).await,
        Some(CheckpointTrigger::Stop)
    );

    assert_eq!(partitions.release_stop_checkpoints(), 5);
    for namespace in partitions.namespaces() {
        let partition = partitions.get_io_owner(namespace).unwrap();
        assert!(!partition.holds_shutdown_for_checkpoint());
        assert!(
            !partition.needs_persistence_checkpoint(),
            "partition {}",
            namespace.inner()
        );
    }
}

/// Runs the started checkpoint for at most `steps` turns of the I/O lane or
/// the WAL writer. Returns whether it ended.
async fn step_checkpoint(
    partition: &mut IggyPartition<RecordingBus>,
    config: &PartitionsConfig,
    steps: usize,
) -> bool {
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    let ended = |partition: &IggyPartition<RecordingBus>| {
        partition.maintenance.checkpoint.is_none() && !partition.persistence_checkpoint_pending()
    };
    for _ in 0..steps {
        if ended(partition) {
            return true;
        }
        match partition.resume_io(config).await {
            crate::PartitionIoStep::Ready(plan) => {
                execute_partition_io_plan(partition, config, plan).await;
            }
            crate::PartitionIoStep::Progress => {}
            crate::PartitionIoStep::Pending => {
                persistence
                    .drain_with_timeout(compio::runtime::time::sleep)
                    .await
                    .unwrap();
                partition.drive_persistence().await;
            }
            _ => panic!("unexpected partition I/O continuation"),
        }
        assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    }
    ended(partition)
}

/// A crash at any step of a queued checkpoint keeps every committed message.
/// The next boot finds either the old checkpoint and replays both messages
/// from the WAL, or the new checkpoint with its receipts and both messages in
/// the segment.
#[compio::test]
async fn given_a_crash_at_any_step_of_a_queued_checkpoint_when_rebooted_should_keep_every_committed_op()
 {
    const COMMITTED: u64 = 2;
    const STEPS_MAX: usize = 32;
    for crash_at in 0..STEPS_MAX {
        let root = tempfile::tempdir().unwrap();
        let (config, mut partition) = Box::pin(replica_with_prepare_history(root.path(), 1)).await;
        // The messages stay resident, so the checkpoint itself flushes them.
        partition.runtime_options.messages_required_to_save =
            Some(u32::try_from(COMMITTED + 1).unwrap());
        dispatch_io(&partition);
        let mut parent = 0;
        for op in 1..=COMMITTED {
            parent = replicate_send(&mut partition, op, parent).await;
        }
        Box::pin(commit_on_heartbeat(&mut partition, &config, COMMITTED)).await;
        Box::pin(settle_partition_io(&mut partition, &config)).await;
        assert_eq!(partition.consensus().commit_min(), COMMITTED);
        advance(&partition, ticks(IDLE_DELAY));
        let trigger = partition.take_due_checkpoint(now(&partition), delays());
        assert_eq!(trigger, Some(CheckpointTrigger::Idle));
        assert!(partition.request_queued_checkpoint(CheckpointTrigger::Idle));
        assert_eq!(partition.checkpoint_persistence(&config).await, trigger);

        let ended = step_checkpoint(&mut partition, &config, crash_at).await;
        let created_revision = partition.created_revision();
        drop(partition);

        let (wal, prepares) = reopened_wal(&config, created_revision).await;
        let ops = ops_of(&prepares);
        let checkpoint = wal.checkpoint_op();
        assert!(
            checkpoint == 0 || checkpoint == COMMITTED,
            "crash at step {crash_at}: checkpoint {checkpoint}"
        );
        assert_eq!(
            ops,
            (checkpoint.max(1)..=COMMITTED).collect::<Vec<_>>(),
            "crash at step {crash_at}"
        );
        drop(wal);
        let rebooted = Box::pin(reboot(&config, created_revision)).await;
        let journal = &rebooted.log.journal().inner;
        let stored: u64 = rebooted
            .log
            .segments()
            .iter()
            .filter(|segment| segment.size.as_bytes_u64() > 0)
            .map(|segment| segment.end_offset - segment.start_offset + 1)
            .sum();
        assert_eq!(
            (
                stored + journal.resident_messages(),
                rebooted.consensus().sequencer().current_sequence()
            ),
            (COMMITTED, COMMITTED),
            "crash at step {crash_at}: (messages in segments and journal, op head)"
        );
        assert!(rebooted.consensus().commit_min() >= checkpoint);
        if ended {
            assert_eq!(checkpoint, COMMITTED);
            return;
        }
    }
    panic!("the checkpoint did not end in {STEPS_MAX} steps");
}

/// On the I/O lane, a commit that flushes ops below a started checkpoint
/// evicts them before the checkpoint rewrites the WAL. So the ring entries
/// keep the body positions that the rewrite drops, and a shed releases their
/// bodies.
#[compio::test]
async fn given_a_started_checkpoint_on_the_io_lane_when_a_commit_flushes_ops_below_it_should_evict_them_before_the_wal_rewrite()
 {
    // The checkpoint covers ops 1 and 2. Op 3 fills the flush threshold, so
    // its commit flushes and evicts all three.
    const THROUGH: u64 = 2;
    const LAST: u64 = THROUGH + 1;
    const STEPS_MAX: usize = 32;
    let root = tempfile::tempdir().unwrap();
    let (config, mut partition) = Box::pin(replica_with_prepare_history(root.path(), 1)).await;
    partition.runtime_options.messages_required_to_save = Some(u32::try_from(LAST).unwrap());
    dispatch_io(&partition);
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    let mut parent = 0;
    for op in 1..=THROUGH {
        parent = replicate_send(&mut partition, op, parent).await;
    }
    Box::pin(commit_on_heartbeat(&mut partition, &config, THROUGH)).await;
    Box::pin(settle_partition_io(&mut partition, &config)).await;
    assert_eq!(partition.consensus().commit_min(), THROUGH);
    assert_eq!(
        partition.log.journal().inner.resident_count(),
        usize::try_from(THROUGH).unwrap()
    );

    persistence.request_checkpoint();
    assert!(partition.checkpoint_persistence(&config).await.is_some());
    replicate_send(&mut partition, LAST, parent).await;
    Box::pin(commit_on_heartbeat(&mut partition, &config, LAST)).await;
    assert!(
        partition.has_pending_commit(),
        "the commit flush waits for the lane"
    );
    // The flush ends inside the commit, so the eviction waits for the commit
    // to publish. The rewrite must not be queued before that.
    for _ in 0..STEPS_MAX {
        if !partition.has_pending_commit() {
            break;
        }
        assert!(
            !persistence.checkpoint_pending() && persistence.checkpoint_op() < THROUGH,
            "the WAL rewrite started while the commit had not evicted"
        );
        step_checkpoint(&mut partition, &config, 1).await;
        // The writer runs beside the lane, so a rewrite queued in this turn
        // publishes before the next one.
        persistence
            .drain_with_timeout(compio::runtime::time::sleep)
            .await
            .unwrap();
    }
    assert_eq!(partition.consensus().commit_min(), LAST, "the commit ended");
    assert_eq!(partition.log.journal().inner.resident_count(), 0);

    assert!(step_checkpoint(&mut partition, &config, STEPS_MAX).await);
    assert_eq!(persistence.checkpoint_op(), THROUGH);
    assert!(
        persistence.segment_reference(1).is_none(),
        "the rewrite dropped op 1"
    );
    assert!(
        persistence.segment_reference(THROUGH).is_some(),
        "the rewrite keeps the checkpoint op"
    );
    let journal = &partition.log.journal().inner;
    journal.release_ring_bodies(usize::MAX, None);
    for op in 1..=THROUGH {
        assert!(
            journal.evicted_header(op).is_some(),
            "op {op} kept its body position"
        );
    }
}
