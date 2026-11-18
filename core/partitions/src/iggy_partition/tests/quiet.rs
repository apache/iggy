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

//! [`IggyPartition::quiet_until`]: a partition sleeps only while no shard
//! tick has work for it, and its deadline is the first tick that has some.

use super::*;
use consensus::PlaneKind;

type Disturb = fn(&mut IggyPartition<IggyMessageBus>);

/// A caught-up backup of a three-replica group in view 1.
async fn quiet_backup() -> IggyPartition<IggyMessageBus> {
    let namespace = IggyNamespace::new(1, 1, 0);
    let mut consensus = VsrConsensus::new(
        TEST_CLUSTER,
        0,
        3,
        namespace.inner(),
        IggyMessageBus::new(0),
        LocalPipeline::new(),
    );
    consensus.set_view(1);
    consensus.set_log_view(1);
    consensus.init_as_backup();
    let partition = IggyPartition::with_in_memory_storage(
        Arc::new(PartitionStats::default()),
        consensus,
        IggyByteSize::from(1024 * 1024),
    );
    assert!(partition.persist_superblock_if_needed().await);
    partition
}

fn tick(partition: &IggyPartition<IggyMessageBus>, grid_tick: bool) -> Vec<consensus::VsrAction> {
    partition.consensus.tick_clock().advance();
    partition
        .consensus
        .tick_with_grid(PlaneKind::Partitions, false, grid_tick, false)
}

#[compio::test]
async fn given_a_caught_up_backup_when_ticks_pass_should_have_work_first_at_its_deadline() {
    let partition = Box::pin(quiet_backup()).await;
    let clock = partition.consensus.tick_clock();
    let deadline = partition
        .quiet_until()
        .expect("a caught-up backup is quiet");
    assert!(
        deadline > clock.now() && deadline < u64::MAX,
        "a backup waits for a heartbeat"
    );

    while clock.now() + 1 < deadline {
        let actions = tick(&partition, true);
        assert!(actions.is_empty(), "work at tick {}", clock.now());
        assert_eq!(partition.quiet_until(), Some(deadline));
    }
    let actions = tick(&partition, true);
    assert!(
        !actions.is_empty() || partition.quiet_until() != Some(deadline),
        "the heartbeat timeout ran at its tick"
    );
}

#[compio::test]
async fn given_a_new_primary_when_its_heartbeat_waits_for_the_grid_should_stay_awake() {
    let partition = test_partition_with_replicas(3);
    assert!(partition.persist_superblock_if_needed().await);
    assert_eq!(partition.quiet_until(), None, "the first heartbeat is due");

    tick(&partition, false);
    assert_eq!(partition.quiet_until(), None, "no grid tick yet");

    tick(&partition, true);
    let clock = partition.consensus.tick_clock();
    let deadline = partition.quiet_until().expect("the heartbeat left");
    assert!(deadline > clock.now() && deadline < u64::MAX);

    partition.consensus.request_eager_commit();
    assert_eq!(partition.quiet_until(), None, "an eager Commit is due");
}

#[compio::test]
async fn given_a_quiet_backup_when_one_state_gives_a_tick_work_should_stay_awake() {
    let cases: [(&str, Disturb); 18] = [
        ("commit fault", |partition| partition.fence_flush_failure()),
        ("view change", |partition| {
            partition.consensus.begin_view_probe();
        }),
        ("state transfer", |partition| {
            partition.consensus.begin_state_transfer_await();
        }),
        ("commit behind", |partition| {
            partition.consensus.advance_commit_max(1);
        }),
        ("repair session", |partition| {
            partition.set_repair(Some(armed_session(1, 0)));
        }),
        ("transfer re-arm", |partition| {
            partition.transfer_rearm = Some(PendingTransferRearm {
                peer: 1,
                after_ticks: 3,
            });
        }),
        ("gap debounce", |partition| partition.gap_ticks.set(1)),
        ("proof walk memory", |partition| {
            let mut memory = Arc::new(crate::RepairMemoryBudget::new(1)).reservation();
            assert!(memory.try_add(1));
            partition.proof_walk_memory = Some(memory);
        }),
        ("view fence", |partition| {
            partition.fence_view_window(1, None);
        }),
        ("missing materialization", |partition| {
            partition.materialization_missing = true;
        }),
        ("superblock write failure", |partition| {
            partition.superblock_write_failures.set(1);
        }),
        ("persisted ack to send", |partition| {
            partition
                .pending_persisted_acks
                .borrow_mut()
                .insert(1, PrepareHeader::default());
        }),
        ("primary transfer", |partition| {
            partition.transition = Some(PendingPartitionTransition::PrimaryTransfer {
                request: primary_transfer_grant(partition),
                drain: None,
            });
        }),
        ("shutdown drain", IggyPartition::begin_shutdown_io),
        ("local I/O work", |partition| {
            assert!(partition.io_active.begin_local_work());
        }),
        ("retiring I/O", |partition| partition.io_active.retire()),
        ("retirement debt", |partition| {
            let _ = partition.observe_retirement_debt(1);
        }),
        ("gap drops to report", |partition| {
            partition.prepare_gap_drops.set(1);
        }),
    ];
    for (name, disturb) in cases {
        let mut partition = Box::pin(quiet_backup()).await;
        assert!(partition.quiet_until().is_some(), "{name}: quiet before");
        disturb(&mut partition);
        assert_eq!(partition.quiet_until(), None, "{name}");
    }
}
