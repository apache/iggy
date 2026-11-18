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

//! The reload budget of a shard: which partitions it queues checkpoints for,
//! in which order, and when it stops.

use super::*;

use server_common::sharding::ShardId;

use crate::IggyPartitions;

/// The starts that the shard tick allows the checkpoint queue.
const STARTS_PER_TICK: usize = 16;

fn namespace(partition_id: usize) -> IggyNamespace {
    IggyNamespace::new(1, 1, partition_id)
}

/// A persisted replica of `partition_id` with one send above checkpoint 0,
/// committed when `commit` is set.
async fn replica_with_send(
    root: &std::path::Path,
    partition_id: usize,
    commit: bool,
) -> (PartitionsConfig, IggyPartition<RecordingBus>) {
    let (config, mut partition) =
        Box::pin(replica_with_prepare_history_in(root, 1, partition_id)).await;
    dispatch_io(&partition);
    replicate_send(&mut partition, 1, 0).await;
    if commit {
        Box::pin(commit_on_heartbeat(&mut partition, &config, 1)).await;
    }
    Box::pin(settle_partition_io(&mut partition, &config)).await;
    (config, partition)
}

struct ReloadTable {
    config: PartitionsConfig,
    table: IggyPartitions<RecordingBus>,
}

impl ReloadTable {
    fn new(config: PartitionsConfig, bytes_max: u64) -> Self {
        let table =
            IggyPartitions::new(ShardId::new(0), config.clone()).with_wal_reload_budget(bytes_max);
        Self { config, table }
    }

    /// Inserts `partition` one tick after the last insert, which stamps its
    /// first op at that tick, and settles it as the shard tick does.
    fn insert(&self, partition_id: usize, partition: IggyPartition<RecordingBus>) {
        self.table.tick_clock().advance();
        self.table.insert(namespace(partition_id), partition);
        self.settle(partition_id);
    }

    fn settle(&self, partition_id: usize) {
        self.table.settle(&namespace(partition_id), None);
    }

    fn partition(&self, partition_id: usize) -> &mut IggyPartition<RecordingBus> {
        self.table.get_mut_by_ns(&namespace(partition_id)).unwrap()
    }

    /// Stamps the first op of a partition again one tick later, as a new
    /// stamp after a checkpoint start does.
    fn restamp(&self, partition_id: usize) {
        self.table.tick_clock().advance();
        self.partition(partition_id).reset_checkpoint_stamps();
        self.settle(partition_id);
    }

    /// The reload step of the next shard tick.
    fn queue_next_tick(&self) -> usize {
        self.table.tick_clock().advance();
        self.table.queue_reload_checkpoints()
    }

    fn queued(&self, partition_ids: &[usize]) -> Vec<bool> {
        partition_ids
            .iter()
            .map(|id| self.partition(*id).in_checkpoint_queue())
            .collect()
    }
}

/// The partitions with committed sends `partition_ids`, inserted in that
/// order, and the bytes that each reloads. `budget` maps those bytes to the
/// share of the shard.
async fn reload_table(
    root: &std::path::Path,
    partition_ids: &[usize],
    budget: impl FnOnce(u64) -> u64,
) -> (ReloadTable, u64) {
    let mut partitions = Vec::with_capacity(partition_ids.len());
    let mut config = None;
    for id in partition_ids {
        let (partition_config, partition) = replica_with_send(root, *id, true).await;
        config = Some(partition_config);
        partitions.push((*id, partition));
    }
    let reload = partitions[0].1.persistence.as_ref().unwrap().reload_bytes();
    assert!(reload > 0);
    let table = ReloadTable::new(config.unwrap(), budget(reload));
    for (id, partition) in partitions {
        assert_eq!(
            partition.persistence.as_ref().unwrap().reload_bytes(),
            reload
        );
        table.insert(id, partition);
    }
    (table, reload)
}

/// Past the share, the oldest first ops go first, until the queued bytes
/// cover the excess. The queue starts them with the reload trigger. A
/// checkpoint that ended keeps counting until the tick settles its share.
#[compio::test]
async fn given_an_excess_when_queueing_reload_checkpoints_should_take_the_oldest_until_covered() {
    let root = tempfile::tempdir().unwrap();
    // Two partitions and one byte past the share: the excess takes two.
    let (table, _) = reload_table(root.path(), &[0, 1, 2], |reload| 2 * reload - 1).await;

    assert_eq!(table.queue_next_tick(), 2);
    assert_eq!(table.queued(&[0, 1, 2]), vec![true, true, false]);
    assert_eq!(table.queue_next_tick(), 0);
    assert_eq!(table.table.checkpoint_queue_len(), 2);

    assert_eq!(table.table.request_queued_checkpoints(STARTS_PER_TICK), 2);
    assert_eq!(
        run_checkpoint(table.partition(0), &table.config).await,
        Some(CheckpointTrigger::Reload)
    );
    assert_eq!(
        table
            .partition(0)
            .persistence
            .as_ref()
            .unwrap()
            .reload_bytes(),
        0
    );
    assert_eq!(table.queue_next_tick(), 0);
    table.settle(0);
    assert_eq!(table.queue_next_tick(), 0);
    assert_eq!(table.queued(&[0, 1, 2]), vec![false, false, false]);
}

/// A partition that waits in the queue for another trigger counts toward
/// the excess, and only once, also when a new op stamps it again while it
/// waits.
#[compio::test]
async fn given_partitions_queued_by_other_triggers_when_queueing_reload_checkpoints_should_count_each_once()
 {
    let root = tempfile::tempdir().unwrap();
    let (_, third) = replica_with_send(root.path(), 3, true).await;
    let (_, fourth) = replica_with_send(root.path(), 4, true).await;
    // One byte under the bytes of one partition: the excess takes two.
    let (table, _) = reload_table(root.path(), &[0, 1, 2], |reload| reload + 1).await;
    table.partition(0).join_checkpoint_queue();
    table
        .table
        .enqueue_checkpoint(namespace(0), CheckpointTrigger::Demand);

    assert_eq!(table.queue_next_tick(), 1);
    assert_eq!(table.queued(&[0, 1, 2]), vec![true, true, false]);
    assert_eq!(table.table.checkpoint_queue_len(), 2);

    table.restamp(0);
    table.insert(3, third);
    table.insert(4, fourth);
    assert_eq!(table.queue_next_tick(), 2);
    assert_eq!(
        table.queued(&[0, 1, 2, 3, 4]),
        vec![true, true, true, true, false]
    );
}

/// An entry whose partition holds a newer stamp is stale and takes no
/// checkpoint. A partition with only uncommitted ops is armed again, as at
/// its deadline, and waits for the next tick.
#[compio::test]
async fn given_stale_and_uncommitted_entries_when_queueing_reload_checkpoints_should_take_only_committed_holders()
 {
    let root = tempfile::tempdir().unwrap();
    let (_, uncommitted) = replica_with_send(root.path(), 3, false).await;
    // One byte past the share of three: the excess takes one.
    let (table, reload) = reload_table(root.path(), &[0, 1, 2], |reload| 3 * reload - 1).await;
    table.restamp(0);
    assert_eq!(table.queue_next_tick(), 1);
    assert_eq!(table.queued(&[0, 1, 2]), vec![false, true, false]);

    let (table, _) = reload_table(root.path(), &[4], |_| reload - 1).await;
    table.insert(3, uncommitted);
    let stamp = table.partition(3).checkpoint_first_op_tick();
    assert_eq!(table.queue_next_tick(), 1);
    assert_eq!(table.queued(&[3, 4]), vec![false, true]);
    assert!(table.partition(3).checkpoint_first_op_tick() > stamp);
    assert_eq!(table.table.reload_order_len(), 1);
}

/// Every tick drops the stale entries at the front, also below the share,
/// and a push drops every stale entry once the order holds two per
/// partition.
#[compio::test]
async fn given_stamps_that_go_stale_when_ticking_should_keep_the_reload_order_bounded() {
    const PARTITIONS: usize = 2;
    const RESTAMPS: usize = 5;
    let root = tempfile::tempdir().unwrap();
    let (table, _) = reload_table(root.path(), &[0, 1], |_| u64::MAX).await;
    table.restamp(0);
    assert_eq!(table.table.reload_order_len(), PARTITIONS + 1);
    assert_eq!(table.queue_next_tick(), 0);
    assert_eq!(table.table.reload_order_len(), PARTITIONS);

    for _ in 0..RESTAMPS {
        table.restamp(1);
    }
    assert!(table.table.reload_order_len() <= 2 * PARTITIONS);
}

/// A new op stamps a partition whose requested checkpoint still waits for
/// its start, and the reload budget queues it again. The request takes one
/// start slot only.
#[compio::test]
async fn given_a_request_that_waits_for_its_start_when_the_reload_budget_queues_it_again_should_take_one_slot()
 {
    let root = tempfile::tempdir().unwrap();
    let (_, uncommitted) = replica_with_send(root.path(), 1, false).await;
    // A share one byte under the bytes of one partition: the uncommitted
    // bytes, which no checkpoint can cover yet, keep an excess past what the
    // request covers.
    let (table, _) = reload_table(root.path(), &[0], |reload| reload - 1).await;
    table.insert(1, uncommitted);
    assert_eq!(table.queue_next_tick(), 1);
    assert_eq!(table.table.request_queued_checkpoints(STARTS_PER_TICK), 1);

    table.restamp(0);
    assert_eq!(table.queue_next_tick(), 1);
    table.table.request_queued_checkpoints(STARTS_PER_TICK);
    assert_eq!(table.table.queued_checkpoints_in_flight(), 1);
}
