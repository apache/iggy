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

use super::*;

use std::rc::Rc;

use iggy_binary_protocol::PrepareHeader;
use server_common::sharding::IggyNamespace;

#[compio::test]
async fn certified_empty_wal_without_superblock_preserves_its_log_view() {
    const CREATED_VIEW: u32 = 2;
    const LOG_VIEW: u32 = 5;
    let directory = tempfile::tempdir().unwrap();
    let mut partition = partition_at_view(CREATED_VIEW, 0);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.runtime_options.durability = iggy_common::Durability::Persisted;
    let wal = directory.path().join("prepares-0");
    let mut journal =
        journal::PartitionPrepareJournal::open(&wal, partition.consensus().group(), 0)
            .await
            .unwrap();
    journal.certify_log_view(LOG_VIEW, 0, 0).await.unwrap();
    drop(journal);

    partition.open_persistence().await.unwrap();

    assert_eq!(partition.consensus().view(), LOG_VIEW);
    assert_eq!(partition.consensus().log_view(), LOG_VIEW);
    assert!(!partition.requires_state_transfer());
    drop(partition);
    let journal =
        journal::PartitionPrepareJournal::open(&wal, IggyNamespace::new(1, 1, 0).inner(), 0)
            .await
            .unwrap();
    assert_eq!(journal.certified_log_view(), Some(LOG_VIEW));
}

#[compio::test]
async fn pending_wal_acks_wait_for_the_superblock_prepass() {
    let directory = tempfile::tempdir().unwrap();
    let mut partition = partition_at_view(3, 3);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.runtime_options.durability = iggy_common::Durability::Persisted;
    partition.open_persistence().await.unwrap();
    let prepare = checksummed_segment_prepare(1, 0, 0, b"pending").transmute_header(
        |mut original, header: &mut PrepareHeader| {
            original.view = 3;
            original.checksum = original.identity_checksum();
            *header = original;
        },
    );
    let header = *prepare.header();
    partition.consensus().sequencer().set_sequence(header.op);
    partition
        .consensus()
        .set_last_prepare_checksum(header.checksum);
    partition
        .log
        .journal()
        .inner
        .append(prepare.clone().into_frozen())
        .await
        .unwrap();
    let persistence = partition.persistence.as_ref().unwrap();
    persistence.append(prepare.into_frozen(), true).unwrap();
    assert!(!partition.register_rebuilt_ack(&header));
    persistence.certify_log_view(3, header.op, header.checksum);
    partition.start_persistence();
    persistence
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();
    let store = Rc::new(RecordingSuperblock::default());
    partition.set_superblock(store.clone(), None);
    assert!(partition.consensus().needs_superblock_persist());

    partition.drive_persistence().await;

    assert_eq!(
        store.attempts.get(),
        0,
        "the serial ACK drain must not issue a superblock write"
    );
    assert!(
        partition
            .pending_persisted_acks
            .borrow()
            .contains_key(&header.op)
    );
    assert!(partition.persist_superblock_if_needed().await);
    partition.drive_persistence().await;
    assert_eq!(store.attempts.get(), 1);
    assert!(partition.pending_persisted_acks.borrow().is_empty());
}

#[compio::test]
async fn pending_prepare_ack_uses_topic_durability_after_superblock_completion() {
    for durability in [
        iggy_common::Durability::Replicated,
        iggy_common::Durability::Persisted,
    ] {
        for dispatched in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let mut partition = partition_at_view(0, 0);
            partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
            partition.runtime_options.durability = durability;
            partition.open_persistence().await.unwrap();
            let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
            let prepare = checksummed_segment_prepare(1, 0, 0, b"pending");
            let header = *prepare.header();
            partition.consensus.sequencer().set_sequence(header.op);
            partition
                .consensus
                .set_last_prepare_checksum(header.checksum);
            partition
                .log
                .journal()
                .inner
                .append(prepare.clone().into_frozen())
                .await
                .unwrap();
            persistence.append(prepare.into_frozen(), false).unwrap();
            assert!(persistence.start());
            Rc::clone(&persistence).run().await;
            assert!(!persistence.is_durable(&header));
            partition.consensus.set_view(3);
            let store = Rc::new(RecordingSuperblock::default());
            partition.set_superblock(store, None);
            partition.set_io_notifier(
                Rc::new(|_, _| {}),
                crate::largest_legal_job_charge().unwrap(),
            );
            assert!(!partition.send_prepare_ok(&header).await);
            assert!(
                partition
                    .pending_persisted_acks
                    .borrow()
                    .contains_key(&header.op)
            );
            assert!(!partition.persist_superblock_if_needed().await);
            let config = repair_config();
            let crate::PartitionIoStep::Ready(plan) = partition.resume_io(&config).await else {
                panic!("the superblock write must be ready");
            };
            assert_eq!(
                plan.continuation,
                crate::PartitionIoContinuation::Superblock
            );
            execute_partition_io_plan(&mut partition, &config, plan).await;
            assert!(!partition.consensus.needs_superblock_persist());
            assert!(!persistence.is_durable(&header));
            if dispatched {
                partition.resume_pending_prepare_ack().await;
            } else {
                partition.drive_persistence().await;
            }
            assert_eq!(
                partition
                    .pending_persisted_acks
                    .borrow()
                    .contains_key(&header.op),
                durability == iggy_common::Durability::Persisted,
                "{durability:?}, dispatched: {dispatched}"
            );
        }
    }
}

/// One superblock write per block, not per batch: a fence writing per append
/// would put two fsyncs in front of every produce.
#[compio::test]
async fn given_appends_inside_the_block_when_fencing_should_write_the_superblock_once() {
    let store = Rc::new(RecordingSuperblock::default());
    let mut partition = solo_recording_partition();
    partition.set_superblock(store.clone(), None);
    partition.set_offset_reservation_lease(test_lease(16));

    assert!(partition.reserve_offsets_through(0).await);
    assert_eq!(store.attempts.get(), 1, "the first offset claims a block");
    assert_eq!(
        last_recorded_reservation(&store),
        17,
        "the claim runs one past the offset plus the lease"
    );

    for offset in 1..=16 {
        assert!(partition.reserve_offsets_through(offset).await);
    }
    assert_eq!(
        store.attempts.get(),
        1,
        "every offset inside the block is covered by the claim already on disk"
    );

    assert!(partition.reserve_offsets_through(17).await);
    assert_eq!(
        store.attempts.get(),
        2,
        "the first offset past the block extends it"
    );
    assert_eq!(last_recorded_reservation(&store), 34);
}

#[compio::test]
async fn given_failing_superblock_when_fencing_should_refuse() {
    let store = Rc::new(RecordingSuperblock::default());
    let mut partition = solo_recording_partition();
    partition.set_superblock(store.clone(), None);
    partition.set_offset_reservation_lease(test_lease(4));
    store.fail_writes.set(true);

    assert!(
        !partition.reserve_offsets_through(0).await,
        "an unrecordable claim must refuse the append"
    );
}

#[compio::test]
async fn quarantine_finishes_after_a_failed_pending_or_running_superblock_write() {
    const STEPS_MAX: usize = 16;
    for running in [false, true] {
        let mut partition = partition_at_view(1, 1);
        let store = Rc::new(RecordingSuperblock::default());
        store.fail_writes.set(true);
        partition.set_superblock(Rc::clone(&store), None);
        partition.set_io_notifier(
            Rc::new(|_, _| {}),
            crate::largest_legal_job_charge().unwrap(),
        );
        assert!(!partition.persist_superblock_if_needed().await);
        let config = repair_config();
        let captured = if running {
            let crate::PartitionIoStep::Ready(plan) = partition.resume_io(&config).await else {
                panic!("view persistence must be ready");
            };
            Some(partition.capture_io(plan, &config).unwrap().unwrap())
        } else {
            None
        };
        partition.begin_quarantine(partition.offset_frontier());
        if let Some(captured) = captured {
            let result = captured.job.execute().await;
            partition.accept_io(captured.identity, result).unwrap();
            captured.gate.unwrap().release();
        }
        let mut finished = false;
        for _ in 0..STEPS_MAX {
            match partition.resume_io(&config).await {
                crate::PartitionIoStep::Ready(plan) => {
                    execute_partition_io_plan(&mut partition, &config, plan).await;
                }
                crate::PartitionIoStep::QuarantineFinished(outcome) => {
                    assert!(outcome.is_ok());
                    finished = true;
                    break;
                }
                crate::PartitionIoStep::Progress | crate::PartitionIoStep::Pending => {}
                _ => panic!("unexpected quarantine continuation"),
            }
        }
        assert!(finished);
        assert_eq!(store.attempts.get(), 1 + u32::from(running));
        assert!(partition.shutdown_io_complete());
    }
}

#[compio::test]
async fn given_failing_superblock_when_persist_gate_runs_should_withhold_and_back_off() {
    for (view, log_view) in [(1, 1), (2, 0)] {
        let case = format!("view {view}, log_view {log_view}");
        let mut partition = partition_at_view(view, log_view);
        let store = Rc::new(RecordingSuperblock::default());
        store.fail_writes.set(true);
        partition.set_superblock(store.clone(), None);

        assert!(
            !partition.persist_superblock_if_needed().await,
            "{case}: a failed write must withhold the send"
        );
        assert_eq!(store.attempts.get(), 1, "{case}");
        assert!(
            !partition.persist_superblock_if_needed().await,
            "{case}: the backoff window must withhold without retrying the write"
        );
        assert_eq!(store.attempts.get(), 1, "{case}");
        assert!(
            partition.consensus().needs_superblock_persist(),
            "{case}: the view stays undurable until a write lands"
        );
    }
}
