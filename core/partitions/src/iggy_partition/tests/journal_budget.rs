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

//! The journal budget of a shard: which committed resident messages count as
//! relief from a flush, when the shed asks that flush, and when it drops or
//! releases for relief that does not come.

use super::*;

use server_common::sharding::ShardId;

use crate::iggy_partitions::RELIEF_WAIT_TICKS;
use crate::{IggyPartitions, RepairRingBudget};

/// Sends that the replica commits and keeps resident.
const RESIDENT_OPS: u64 = 3;
/// Above every op these tests send, so no commit flushes by itself.
const MESSAGES_REQUIRED_TO_SAVE: u32 = 64;
/// Commands that the backup commits.
const COMMAND_OPS: u64 = 4;
/// A budget that every committed op passes.
const TINY_BUDGET_BYTES: usize = 1;
/// The starts that the shard tick allows the checkpoint queue.
const STARTS_PER_TICK: usize = 16;

fn namespace() -> IggyNamespace {
    IggyNamespace::new(1, 1, 0)
}

fn advance_table(table: &IggyPartitions<RecordingBus>, ticks: u64) {
    for _ in 0..ticks {
        table.tick_clock().advance();
    }
}

struct BudgetedReplica {
    config: PartitionsConfig,
    table: IggyPartitions<RecordingBus>,
    budget: Rc<RepairRingBudget>,
    /// The checksum of the last op, the parent of the next.
    parent: u128,
}

impl BudgetedReplica {
    fn partition(&self) -> &mut IggyPartition<RecordingBus> {
        self.table.get_mut_by_ns(&namespace()).unwrap()
    }

    /// Replicates and commits the sends from the last op through `through`,
    /// and leaves the commit to the I/O lane.
    async fn commit_sends(&mut self, through: u64) {
        let partition = self.table.get_mut_by_ns(&namespace()).unwrap();
        for op in partition.consensus().commit_min() + 1..=through {
            self.parent = replicate_send(partition, op, self.parent).await;
        }
        Box::pin(commit_on_heartbeat(partition, &self.config, through)).await;
    }

    async fn settle_io(&self) {
        Box::pin(settle_partition_io(self.partition(), &self.config)).await;
    }
}

/// A replica that committed [`RESIDENT_OPS`] one-message sends and holds them
/// all resident, in a shard table with a budget of [`TINY_BUDGET_BYTES`].
async fn budgeted_replica(root: &std::path::Path) -> BudgetedReplica {
    let (config, mut partition) = Box::pin(replica_with_prepare_history(root, 1)).await;
    partition.runtime_options.messages_required_to_save = Some(MESSAGES_REQUIRED_TO_SAVE);
    dispatch_io(&partition);
    let budget = Rc::new(RepairRingBudget::new(TINY_BUDGET_BYTES));
    let table =
        IggyPartitions::new(ShardId::new(0), config.clone()).with_repair_rings(Rc::clone(&budget));
    table.insert(namespace(), partition);
    let mut replica = BudgetedReplica {
        config,
        table,
        budget,
        parent: 0,
    };
    replica.commit_sends(RESIDENT_OPS).await;
    replica.settle_io().await;
    assert_eq!(
        replica.partition().log.journal().inner.resident_messages(),
        RESIDENT_OPS
    );
    replica
}

/// Committed resident messages past the budget count as relief. The shed
/// asks one flush for them and keeps counting them for
/// [`RELIEF_WAIT_TICKS`], so nothing is dropped for them in that time. After
/// it, the flush that did not land stops counting.
#[compio::test]
async fn given_an_asked_flush_that_does_not_land_when_the_wait_passes_should_stop_counting_its_relief()
 {
    let root = tempfile::tempdir().unwrap();
    let BudgetedReplica { table, budget, .. } = budgeted_replica(root.path()).await;
    let resident = budget.usage().resident_bytes;
    assert!(resident > TINY_BUDGET_BYTES);

    let shed = table.shed_repair_rings().unwrap();
    assert_eq!((shed.flushes_asked, shed.usage.relief_bytes), (1, resident));
    assert_eq!(budget.costly_excess(), None);

    advance_table(&table, RELIEF_WAIT_TICKS - 1);
    let shed = table.shed_repair_rings().unwrap();
    assert_eq!(
        (
            shed.flushes_asked,
            shed.usage.relief_bytes,
            shed.usage.relief_excluded_bytes
        ),
        (0, resident, 0)
    );
    assert_eq!(budget.costly_excess(), None);

    advance_table(&table, 1);
    let shed = table.shed_repair_rings().unwrap();
    assert_eq!(
        (shed.usage.relief_bytes, shed.usage.relief_excluded_bytes),
        (0, resident)
    );
    assert_eq!(budget.costly_excess(), Some(resident - TINY_BUDGET_BYTES));
}

/// The asked flush lands: the messages leave the resident journal for the
/// ring, the relief ends, and the next shed releases their bodies to the
/// segment before it drops anything.
#[compio::test]
async fn given_an_asked_flush_when_it_lands_should_move_the_relief_to_releasable_ring_bodies() {
    let root = tempfile::tempdir().unwrap();
    let replica = budgeted_replica(root.path()).await;
    let (table, budget) = (&replica.table, &replica.budget);
    assert_eq!(table.shed_repair_rings().unwrap().flushes_asked, 1);

    replica.settle_io().await;
    assert_eq!(
        replica.partition().log.journal().inner.resident_messages(),
        0
    );
    let usage = budget.usage();
    assert_eq!(
        (
            usage.resident_bytes,
            usage.relief_bytes,
            usage.relief_excluded_bytes
        ),
        (0, 0, 0)
    );

    let shed = table.shed_repair_rings().unwrap();
    let ring_ops = usize::try_from(RESIDENT_OPS).unwrap();
    assert_eq!(
        (
            shed.flushes_asked,
            shed.bodies_released,
            shed.entries_dropped
        ),
        (0, ring_ops - 1, ring_ops - 1)
    );
}

/// A backup on disk under `root` that committed [`COMMAND_OPS`] commands, and
/// the checksum of the last one, the parent of the next op.
async fn backup_with_commands(
    root: &std::path::Path,
) -> (PartitionsConfig, Box<IggyPartition<RecordingBus>>, u128) {
    let mut config = repair_config();
    config.path_layout.streams_root = root.to_string_lossy().into_owned();
    let directory = std::path::PathBuf::from(config.get_partition_path(1, 1, 0));
    std::fs::create_dir_all(&directory).unwrap();
    let mut backup =
        backup_with_counter(&config, &directory, iggy_common::Durability::Persisted).await;
    let mut parent = 0;
    for op in 1..=COMMAND_OPS {
        let command = command_prepare(op, parent, COMMAND_KIND, op);
        parent = command.header().checksum;
        Box::pin(backup.on_replicate(command)).await;
    }
    Box::pin(commit_on_heartbeat(&mut backup, &config, COMMAND_OPS)).await;
    Box::pin(settle_partition_io(&mut backup, &config)).await;
    assert_eq!(
        backup.log.journal().inner.command_ops(),
        (1..=COMMAND_OPS).collect::<Vec<_>>()
    );
    (config, backup, parent)
}

/// Held commands leave memory only once a checkpoint covers them. The shed
/// queues that checkpoint once, also while the request waits for its start,
/// and after it releases the oldest commands for the excess alone.
#[compio::test]
async fn given_held_commands_past_the_budget_when_shedding_should_checkpoint_them_then_release_only_the_excess()
 {
    const START_WAIT_TICKS: usize = 3;
    let root = tempfile::tempdir().unwrap();
    let (config, backup, _) = backup_with_commands(root.path()).await;
    let journal = &backup.log.journal().inner;
    let probe = Rc::new(RepairRingBudget::new(usize::MAX));
    journal.attach_ring_budget(Rc::clone(&probe), namespace());
    let held = probe.usage().command_bytes;
    let command = held / usize::try_from(COMMAND_OPS).unwrap();
    // One and a half commands past the budget, so the excess takes two.
    let budget = Rc::new(RepairRingBudget::new(held - command - command / 2));
    let table =
        IggyPartitions::new(ShardId::new(0), config.clone()).with_repair_rings(Rc::clone(&budget));
    table.insert(namespace(), *backup);

    let shed = table.shed_repair_rings().unwrap();
    assert_eq!((shed.commands_released, shed.checkpoints_asked), (0, 1));
    assert_eq!(table.shed_repair_rings().unwrap().checkpoints_asked, 0);
    assert_eq!(table.request_queued_checkpoints(STARTS_PER_TICK), 1);
    let mut asked = 0;
    for _ in 0..START_WAIT_TICKS {
        asked += table.shed_repair_rings().unwrap().checkpoints_asked;
        table.request_queued_checkpoints(STARTS_PER_TICK);
    }
    assert_eq!((asked, table.queued_checkpoints_in_flight()), (0, 1));
    let partition = table.get_mut_by_ns(&namespace()).unwrap();
    run_checkpoint(partition, &config).await;
    assert_eq!(
        partition.persistence.as_ref().unwrap().checkpoint_op(),
        COMMAND_OPS
    );

    let shed = table.shed_repair_rings().unwrap();
    assert_eq!((shed.commands_released, shed.checkpoints_asked), (2, 0));
    assert_eq!(
        table
            .get_by_ns(&namespace())
            .unwrap()
            .log
            .journal()
            .inner
            .command_ops(),
        vec![3, 4]
    );
}

/// A view change reads the command at the commit point from memory, and a
/// transfer donor sends it. Past any budget the shed keeps it in the floor,
/// and lets it go once a later command commits, with no checkpoint asked for
/// the new one.
#[compio::test]
async fn given_a_command_at_the_commit_point_when_shedding_past_the_budget_should_keep_it_in_the_floor()
 {
    let root = tempfile::tempdir().unwrap();
    let (config, backup, parent) = backup_with_commands(root.path()).await;
    let budget = Rc::new(RepairRingBudget::new(TINY_BUDGET_BYTES));
    let table =
        IggyPartitions::new(ShardId::new(0), config.clone()).with_repair_rings(Rc::clone(&budget));
    table.insert(namespace(), *backup);
    assert_eq!(table.shed_repair_rings().unwrap().checkpoints_asked, 1);
    assert_eq!(table.request_queued_checkpoints(STARTS_PER_TICK), 1);
    run_checkpoint(table.get_mut_by_ns(&namespace()).unwrap(), &config).await;

    let shed = table.shed_repair_rings().unwrap();
    let partition = table.get_by_ns(&namespace()).unwrap();
    let journal = &partition.log.journal().inner;
    assert_eq!(journal.command_ops(), vec![COMMAND_OPS]);
    assert_eq!(
        shed.commands_released,
        usize::try_from(COMMAND_OPS - 1).unwrap()
    );
    assert!(
        journal
            .repair_headers_in(COMMAND_OPS..=COMMAND_OPS)
            .contains_key(&COMMAND_OPS)
    );
    assert!(partition.offsets_wire_snapshot_for_test().is_ok());
    assert_eq!(shed.usage.floor_bytes, shed.usage.command_bytes);
    assert_eq!(budget.costly_excess(), None);
    let shed = table.shed_repair_rings().unwrap();
    assert_eq!((shed.commands_released, shed.checkpoints_asked), (0, 0));

    let next = COMMAND_OPS + 1;
    let partition = table.get_mut_by_ns(&namespace()).unwrap();
    Box::pin(partition.on_replicate(command_prepare(next, parent, COMMAND_KIND, next))).await;
    Box::pin(commit_on_heartbeat(partition, &config, next)).await;
    Box::pin(settle_partition_io(partition, &config)).await;
    let shed = table.shed_repair_rings().unwrap();
    assert_eq!((shed.commands_released, shed.checkpoints_asked), (1, 0));
    assert_eq!(
        table
            .get_by_ns(&namespace())
            .unwrap()
            .log
            .journal()
            .inner
            .command_ops(),
        vec![next]
    );
    assert_eq!(shed.usage.floor_bytes, shed.usage.command_bytes);
}

/// Commits that land while an asked flush runs stay resident after it. The
/// landing ends the ask, so the shed asks again at once for them instead of
/// waiting out the old stamp.
#[compio::test]
async fn given_relief_that_stays_after_a_flush_lands_when_shedding_should_ask_again() {
    const LATER_OPS: u64 = 2;
    let root = tempfile::tempdir().unwrap();
    let mut replica = budgeted_replica(root.path()).await;
    assert_eq!(replica.table.shed_repair_rings().unwrap().flushes_asked, 1);

    replica.commit_sends(RESIDENT_OPS + LATER_OPS).await;
    replica.settle_io().await;
    assert_eq!(
        replica.partition().log.journal().inner.resident_messages(),
        LATER_OPS
    );

    assert_eq!(replica.table.shed_repair_rings().unwrap().flushes_asked, 1);
}

/// A partition that cannot take a flush, here a Replicated commit ahead of
/// the WAL writer, counts its relief for [`RELIEF_WAIT_TICKS`] from the
/// first skip. A later skip does not restart that wait. Once the WAL catches
/// up, the shed asks.
#[compio::test]
async fn given_a_partition_that_cannot_flush_when_shedding_should_count_its_relief_only_until_the_wait_passes()
 {
    let root = tempfile::tempdir().unwrap();
    let replica = budgeted_replica(root.path()).await;
    let partition = replica.partition();
    partition.runtime_options.durability = iggy_common::Durability::Replicated;
    let next = RESIDENT_OPS + 1;
    let unwritten = checksummed_segment_prepare(next, replica.parent, next - 1, b"payload");
    Box::pin(partition.on_replicate(unwritten)).await;
    Box::pin(commit_on_heartbeat(partition, &replica.config, next)).await;
    replica.settle_io().await;
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    assert_eq!(
        (partition.consensus().commit_min(), persistence.written_op()),
        (next, RESIDENT_OPS)
    );
    let relief = replica.budget.usage().relief_bytes;
    assert_eq!(relief, replica.budget.usage().resident_bytes);

    let shed = replica.table.shed_repair_rings().unwrap();
    assert_eq!((shed.flushes_asked, shed.usage.relief_bytes), (0, relief));
    advance_table(&replica.table, RELIEF_WAIT_TICKS - 1);
    let shed = replica.table.shed_repair_rings().unwrap();
    assert_eq!((shed.flushes_asked, shed.usage.relief_bytes), (0, relief));
    advance_table(&replica.table, 1);
    let shed = replica.table.shed_repair_rings().unwrap();
    assert_eq!(
        (shed.flushes_asked, shed.usage.relief_excluded_bytes),
        (0, relief)
    );

    persistence
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();
    replica.partition().drive_persistence().await;
    assert_eq!(persistence.written_op(), next);
    let shed = replica.table.shed_repair_rings().unwrap();
    assert_eq!(
        (shed.flushes_asked, shed.usage.relief_excluded_bytes),
        (1, 0)
    );
}
