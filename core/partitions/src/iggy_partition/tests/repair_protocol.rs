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

#[compio::test]
async fn given_an_old_views_entry_under_the_anchor_when_proving_without_a_dispatcher_should_drop_it_and_stay_fenced()
 {
    let mut partition = test_partition();
    let first = journal_sealed(&partition, 1, 0, 1).await;
    // The view's op 2, which the anchor chains to, is not what is held.
    let canonical_second = sealed_prepare(2, first.checksum, 2);
    journal_sealed(&partition, 2, first.checksum, 99).await;
    partition.consensus.restore_commit_state(0, 2);
    partition.fence_view_window(
        2,
        Some(*sealed_prepare(3, canonical_second.header().checksum, 3).header()),
    );

    assert_eq!(
        partition.prove_view_window().await,
        Some(ViewWindowVerdict::Break { op: 2 })
    );
    assert!(!partition.log.journal().inner.holds_op(2));
    assert!(partition.log.journal().inner.holds_op(1));
    assert!(
        walked_ops(&partition).is_empty(),
        "op 1 is not proven until op 2 is refetched, so the walk must still hold"
    );
    assert!(partition.view_fenced());
}

#[compio::test]
async fn given_a_wal_naming_a_commit_point_when_restarted_and_fenced_should_commit_the_recovered_prefix()
 {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition_at(1, 3);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.log.retire_front().unwrap();
    partition
        .install_empty_segment(&repair_config(), 0)
        .await
        .unwrap();
    let mut journal = journal::PartitionPrepareJournal::open(
        &directory.path().join("prepares-0"),
        partition.consensus().group(),
        0,
    )
    .await
    .unwrap();
    let mut parent = 0;
    for op in 1..=4u64 {
        let mut prepare = checksummed_segment_prepare(op, parent, op - 1, b"recovered");
        prepare = prepare.transmute_header(|old, header: &mut PrepareHeader| {
            *header = old;
            header.commit = op - 1;
            header.checksum = header.identity_checksum();
        });
        parent = prepare.header().checksum;
        journal.append(prepare.into_frozen()).await.unwrap();
    }
    drop(journal);
    partition.open_persistence().await.unwrap();
    assert_eq!(partition.consensus.commit_min(), 0);
    assert_eq!(partition.consensus.commit_max(), 3);

    partition.fence_view_window(4, None);
    assert!(partition.view_fenced());
    partition.commit_journal(&repair_config()).await;

    assert!(partition.fatal.is_none(), "{:?}", partition.fatal);
    assert_eq!(
        partition.consensus.commit_min(),
        3,
        "the recovered WAL named ops 1..=3 committed"
    );
    assert!(
        partition.view_fenced(),
        "op 4 stays held until the view's log proves it"
    );
}

#[compio::test]
async fn given_a_fenced_window_when_its_hole_fills_should_report_it_held() {
    let mut partition = test_partition();
    let first = journal_sealed(&partition, 1, 0, 1).await;
    partition.consensus.restore_commit_state(0, 3);
    partition.fence_view_window(3, Some(*sealed_prepare(4, 9, 4).header()));
    assert_eq!(partition.held_through(), 1);
    assert!(!partition.view_fence_held());

    let second = journal_sealed(&partition, 2, first.checksum, 2).await;
    assert!(!partition.view_fence_held());
    journal_sealed(&partition, 3, second.checksum, 3).await;
    assert_eq!(partition.held_through(), 3);
    assert!(partition.view_fence_held());
}

#[compio::test]
async fn given_an_anchor_from_another_view_when_offered_should_refuse_it() {
    let mut partition = test_partition();
    let first = journal_sealed(&partition, 1, 0, 1).await;
    partition.consensus.restore_commit_state(0, 1);
    partition.fence_view_window(1, None);
    let mut other_view = *sealed_prepare(2, first.checksum, 2).header();
    other_view.view = partition.consensus.view() + 1;
    assert!(
        !partition.offer_view_anchor(&other_view),
        "an anchor from a view the fence was not installed in vouches for the wrong log"
    );
    assert!(partition.view_fence_unanchored());
}

#[compio::test]
async fn given_an_unsealed_anchor_when_fencing_should_stay_unanchored() {
    let mut partition = test_partition();
    journal_sealed(&partition, 1, 0, 1).await;
    partition.consensus.restore_commit_state(0, 1);
    // What a `StartView` carries at a commit point the primary holds no
    // header for: checksum and parent both zero.
    let blank = PrepareHeader {
        command: Command::Prepare,
        op: 2,
        ..Default::default()
    };
    partition.fence_view_window(1, Some(blank));
    assert!(
        partition.view_fence_unanchored(),
        "a blank anchor links any window below it"
    );
    assert!(!partition.offer_view_anchor(&blank));
}

/// A Break leaves `last_prepare_checksum` naming the entry it dropped, and the
/// refetch fills the op without moving the head; unless the proof refreshes
/// it, certification keeps failing on a quiet partition.
#[compio::test]
async fn given_a_stale_last_prepare_checksum_when_the_window_proves_should_certify() {
    let (_directory, mut partition, held) = partition_with_pending_durable_ack().await;
    let persistence = partition.persistence.as_ref().unwrap().clone();
    let view = partition.consensus().view() + 1;
    partition.consensus.set_view(view);
    partition.consensus().set_log_view(view);
    partition.consensus().restore_commit_state(0, 1);
    partition
        .consensus()
        .set_last_prepare_checksum(held.checksum ^ 1);
    let anchor = checksummed_segment_prepare(2, held.checksum, 0, b"anchor");
    partition.fence_view_window(1, Some(*anchor.header()));
    assert_eq!(
        partition.prove_view_window().await,
        Some(ViewWindowVerdict::Proven)
    );
    partition.ensure_wal_view();
    persistence
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();
    assert_eq!(persistence.certified_log_view(), Some(view));
}

#[compio::test]
async fn given_a_fenced_wal_when_certifying_should_vouch_only_for_the_applied_prefix() {
    let (_directory, mut partition, held) = partition_with_pending_durable_ack().await;
    let persistence = partition.persistence.as_ref().unwrap().clone();
    let view = partition.consensus().view() + 1;
    partition.consensus.set_view(view);
    partition.consensus().set_log_view(view);
    partition.consensus().sequencer().set_sequence(4);

    // A window reaching the suffix the next `DoViewChange` reports: muted.
    partition.consensus().restore_commit_state(held.op, 3);
    partition.fence_view_window(3, None);
    assert!(!partition.ensure_wal_view());
    persistence
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();
    assert_ne!(persistence.certified_log_view(), Some(view));

    // WAL holds nothing above `commit_min`, window below the suffix: certified.
    partition.consensus().advance_commit_max(4);
    partition.ensure_wal_view();
    persistence
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();
    assert_eq!(persistence.certified_log_view(), Some(view));
}

#[compio::test]
async fn given_an_unanchored_fence_when_ticking_should_ask_for_headers_at_the_pace_given() {
    let mut partition = test_partition();
    journal_sealed(&partition, 1, 0, 1).await;
    partition.consensus.restore_commit_state(0, 1);
    partition.fence_view_window(1, None);
    let due: Vec<bool> = (0..6).map(|_| partition.view_fence_probe_due(3)).collect();
    assert_eq!(due, vec![false, false, true, false, false, true]);
}

#[compio::test]
async fn given_a_cached_hole_when_the_missing_op_arrives_should_bypass_the_cache() {
    let mut partition = test_partition();
    journal_sealed(&partition, 1, 0, 1).await;
    let third = journal_sealed(&partition, 3, 7, 3).await;
    partition.consensus.restore_commit_state(0, 3);
    partition.fence_view_window(3, Some(*sealed_prepare(4, third.checksum, 4).header()));
    assert_eq!(
        partition.prove_view_window().await,
        Some(ViewWindowVerdict::Hole { op: 2 })
    );
    assert_eq!(partition.view_fence.and_then(|fence| fence.hole), Some(2));
    // Op 3 chains to parent 7, which the arriving op 2 is not.
    journal_sealed(&partition, 2, 0, 99).await;
    assert_eq!(
        partition.prove_view_window().await,
        Some(ViewWindowVerdict::Break { op: 2 }),
        "the cache must be bypassed once the missing op arrives"
    );
}

/// The fence holds entries back from the WAL too: a stale entry can sit at the
/// WAL head, and appending the view's successor on top of it fails the WAL's
/// parent check, which fences the whole partition.
#[compio::test]
async fn given_a_stale_wal_head_when_fenced_should_not_persist_repaired_successors() {
    let (_directory, mut partition, stale) = partition_with_pending_durable_ack().await;
    let persistence = partition.persistence.as_ref().unwrap().clone();
    assert_eq!(persistence.head(), stale.op);
    let canonical = checksummed_segment_prepare(1, 0, 7, b"canonical");
    let successor = checksummed_segment_prepare(2, canonical.header().checksum, 0, b"successor");
    partition
        .log
        .journal()
        .inner
        .append(successor.clone().into_frozen())
        .await
        .unwrap();
    partition.consensus().sequencer().set_sequence(2);
    partition.consensus().restore_commit_state(0, 2);
    partition.fence_view_window(1, Some(*successor.header()));

    partition.persist_repaired_prefix();
    assert_eq!(
        persistence.head(),
        stale.op,
        "a repaired successor reached the WAL above an unproven entry"
    );
    assert!(persistence.failure().is_none());
}

/// With an I/O dispatcher the Break is handed to the lane: the stale entry
/// stays resident and repair frames are refused until the cut lands, then the
/// hole is refetched at once.
#[compio::test]
async fn given_a_dispatcher_when_the_proof_breaks_should_defer_the_drop_to_the_lane() {
    let (_directory, mut partition, stale) = partition_with_pending_durable_ack().await;
    let persistence = partition.persistence.as_ref().unwrap().clone();
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    let view = partition.consensus().view() + 1;
    partition.consensus.set_view(view);
    partition.consensus().set_log_view(view);
    partition.consensus().restore_commit_state(0, 1);
    let canonical = checksummed_segment_prepare(1, 0, 7, b"canonical");
    let anchor = checksummed_segment_prepare(2, canonical.header().checksum, 0, b"anchor");
    partition.repair = Some(RepairSession {
        view,
        ..armed_session(2, 0)
    });
    partition.fence_view_window(1, Some(*anchor.header()));

    assert_eq!(
        partition.prove_view_window().await,
        Some(ViewWindowVerdict::Break { op: 1 })
    );
    assert!(
        partition.log.journal().inner.holds_op(1),
        "the lane, not the proof, truncates a mounted partition"
    );
    assert_eq!(partition.unproven_drop_from, Some(1));
    assert!(partition.refuses_prepares());
    partition.apply_repaired_prepare(canonical.clone()).await;
    partition.apply_repaired_prepare(anchor.clone()).await;
    assert!(
        !partition.log.journal().inner.holds_op(2),
        "a repaired body landed under a pending cut"
    );
    assert_eq!(
        partition
            .log
            .journal()
            .inner
            .repair_header(1)
            .map(|header| header.checksum),
        Some(stale.checksum),
        "a repaired body journaled before the cut would outlive it in the WAL"
    );

    let config = repair_config();
    let mut applied = false;
    for _ in 0..64 {
        match partition.resume_io(&config).await {
            crate::PartitionIoStep::Ready(plan) => {
                execute_partition_io_plan(&mut partition, &config, plan).await;
            }
            crate::PartitionIoStep::Progress => {}
            crate::PartitionIoStep::ViewApplied { .. } => {
                applied = true;
                break;
            }
            crate::PartitionIoStep::Pending => {
                persistence
                    .drain_with_timeout(compio::runtime::time::sleep)
                    .await
                    .unwrap();
            }
            _ => panic!("unexpected partition I/O continuation"),
        }
    }
    assert!(applied, "the lane never ran the truncation");
    assert!(!partition.log.journal().inner.holds_op(1));
    assert_eq!(partition.unproven_drop_from, None);
    assert_eq!(persistence.head(), 0);
    assert_eq!(
        partition.repair.map(|session| session.requested_from),
        Some(None),
        "the hole must be refetched without waiting out the retry timer"
    );
    assert!(partition.view_fenced());
    assert!(partition.fatal.is_none());
}

/// A proof that breaks while other history work holds the lane drops
/// nothing yet; the anchor that triggered it must not reach the WAL on top
/// of the stale head it does not chain to. Its retransmit skips the proof,
/// since the fence already holds the anchor, so the WAL itself has to refuse.
#[compio::test]
async fn given_busy_history_when_an_anchor_breaks_the_window_should_not_append_it_to_the_wal() {
    let (_directory, mut partition, _stale) = partition_with_pending_durable_ack().await;
    let persistence = partition.persistence.as_ref().unwrap().clone();
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    let view = partition.consensus().view() + 1;
    partition.consensus.set_view(view);
    partition.consensus().set_log_view(view);
    partition.consensus().restore_commit_state(0, 1);
    partition.fence_view_window(1, None);
    partition.maintenance.flush_through = Some(1);
    assert!(partition.history_is_busy());
    let canonical = checksummed_segment_prepare(1, 0, 7, b"canonical");
    let anchor = checksummed_segment_prepare(2, canonical.header().checksum, 0, b"anchor")
        .transmute_header(|mut original, header: &mut PrepareHeader| {
            original.view = view;
            original.checksum = original.identity_checksum();
            *header = original;
        });
    let sequence = partition.consensus().sequencer().current_sequence();
    partition.on_replicate(anchor.clone()).await;
    assert!(!partition.view_fence_unanchored());
    assert!(partition.view_fenced());
    assert_eq!(partition.unproven_drop_from, None);
    assert!(
        !partition.log.journal().inner.holds_op(2),
        "the anchor was journaled above a window its proof broke"
    );
    assert_eq!(
        partition.consensus().sequencer().current_sequence(),
        sequence
    );
    partition.on_replicate(anchor).await;
    let _ = persistence
        .drain_with_timeout(compio::runtime::time::sleep)
        .await;
    assert!(
        persistence.failure().is_none(),
        "the anchor was appended on a WAL head it does not chain to: {:?}",
        persistence.failure()
    );
    assert_eq!(persistence.head(), 1);
    assert!(partition.fatal.is_none());
}

/// The lane's cut is pending but nothing else refuses the frame: the gate
/// on the pending drop alone keeps a repaired body out of the journal.
#[compio::test]
async fn given_a_pending_unproven_drop_when_a_repair_frame_arrives_should_not_journal_it() {
    let mut partition = test_partition();
    partition.repair = Some(armed_session(3, 0));
    partition.unproven_drop_from = Some(1);
    assert!(!partition.refuses_prepares());

    partition
        .apply_repaired_prepare(repaired_send_prepare(1, 0, 1))
        .await;
    assert_eq!(
        partition.log.journal().inner.resident_count(),
        0,
        "a repaired body was journaled under a pending cut"
    );
    assert_eq!(partition.consensus().sequencer().current_sequence(), 0);
}

/// A repair commit floor (`complete_repair` taking the peer's
/// `RangeEvicted` floor) can raise the fence floor past the hole the last
/// proof stopped at. The cached hole then names an op outside the window
/// and must not keep the proof waiting on it.
#[compio::test]
async fn given_a_cached_hole_under_a_raised_floor_when_proving_should_not_wait_on_it() {
    let mut partition = test_partition();
    journal_sealed(&partition, 1, 0, 1).await;
    let third = journal_sealed(&partition, 3, 7, 3).await;
    partition.consensus.restore_commit_state(0, 3);
    partition.fence_view_window(3, Some(*sealed_prepare(4, third.checksum, 4).header()));
    assert_eq!(
        partition.prove_view_window().await,
        Some(ViewWindowVerdict::Hole { op: 2 })
    );

    partition.consensus.set_commit_floor(2);
    assert_eq!(partition.view_fence_hole(), None);
    assert_eq!(
        partition.prove_view_window().await,
        Some(ViewWindowVerdict::Proven),
        "the proof waited on a hole below the raised floor"
    );
    assert!(!partition.view_fenced());
}

/// A prepare received while the window was fenced is journaled but neither
/// persisted nor acked. The primary resends it only while it lacks quorum,
/// after a retransmit timeout, so proving the window releases its ack
/// without waiting for one.
#[compio::test]
async fn given_a_prepare_received_while_fenced_when_the_window_is_proven_should_ack_it_without_a_retransmit()
 {
    let (_directory, mut partition, held) = partition_with_pending_durable_ack().await;
    let persistence = partition.persistence.as_ref().unwrap().clone();
    let sent = partition.consensus().message_bus().sent_to_replicas.clone();
    let view = partition.consensus().view() + 1;
    partition.consensus.set_view(view);
    partition.consensus().set_log_view(view);
    partition.consensus().restore_commit_state(0, 1);
    // The held op 1 was journaled behind the append counter's back; wake
    // the counter so the live successor continues it at offset 1.
    partition.offset_space.append_live = true;
    partition.dirty_offset.store(0, Ordering::Relaxed);
    let successor = checksummed_segment_prepare(2, held.checksum, 1, b"successor")
        .transmute_header(|mut original, header: &mut PrepareHeader| {
            original.view = view;
            original.checksum = original.identity_checksum();
            *header = original;
        });
    partition.fence_view_window(1, Some(*successor.header()));
    partition.on_replicate(successor).await;
    assert!(partition.log.journal().inner.holds_op(2));
    assert_eq!(
        persistence.head(),
        1,
        "a fenced replica persisted a prepare above the window"
    );
    sent.borrow_mut().clear();

    assert_eq!(
        partition.prove_view_window().await,
        Some(ViewWindowVerdict::Proven)
    );
    for _ in 0..4 {
        persistence
            .drain_with_timeout(compio::runtime::time::sleep)
            .await
            .unwrap();
        partition.drive_persistence().await;
    }
    let acked: Vec<u64> = sent
        .borrow()
        .iter()
        .filter_map(|(_, frame)| {
            let header =
                bytemuck::checked::try_from_bytes::<PrepareOkHeader>(frame.as_slice()).ok()?;
            (header.command == Command::PrepareOk).then_some(header.op)
        })
        .collect();
    assert!(
        acked.contains(&2),
        "the fence lifted without acking op 2: {acked:?}"
    );
    assert_eq!(persistence.head(), 2);
    assert!(partition.pending_persisted_acks.borrow().is_empty());
    assert!(partition.fatal.is_none());
}

/// Only a prepare of the view that proved the window was withheld by the
/// fence. An older view's entry above it is not the view's, and its ack
/// would vouch for a log the view never sent.
#[compio::test]
async fn given_an_older_views_entry_above_the_window_when_proven_should_not_queue_its_ack() {
    let mut partition = test_partition();
    let first = journal_sealed(&partition, 1, 0, 1).await;
    let view = partition.consensus().view() + 1;
    partition.consensus.set_view(view);
    let in_view = |op: u64, parent: u128| {
        sealed_prepare(op, parent, op).transmute_header(
            |mut original, header: &mut PrepareHeader| {
                original.view = view;
                original.checksum = original.identity_checksum();
                *header = original;
            },
        )
    };
    let anchor = in_view(2, first.checksum);
    let older = journal_sealed(&partition, 2, first.checksum, 2).await;
    let current = in_view(3, older.checksum);
    partition
        .log
        .journal()
        .inner
        .append(current.into_frozen())
        .await
        .expect("journal append");
    partition.consensus().sequencer().set_sequence(3);
    partition.consensus.restore_commit_state(0, 1);
    partition.fence_view_window(1, Some(*anchor.header()));

    assert_eq!(
        partition.prove_view_window().await,
        Some(ViewWindowVerdict::Proven)
    );
    let queued: Vec<u64> = partition
        .pending_persisted_acks
        .borrow()
        .keys()
        .copied()
        .collect();
    assert_eq!(queued, vec![3], "an older view's entry had its ack queued");
}

#[compio::test]
async fn given_a_fenced_window_when_acking_should_withhold_the_ack() {
    let (mut partition, _) = recording_partition_at(1, 3);
    let prepare = sealed_prepare(1, 0, 1);
    let first = *prepare.header();
    partition
        .log
        .journal()
        .inner
        .append(prepare.into_frozen())
        .await
        .expect("journal append");
    partition.consensus.sequencer().set_sequence(1);
    partition.consensus.restore_commit_state(0, 1);
    assert!(
        partition.send_prepare_ok(&first).await,
        "an unfenced backup acks what it holds"
    );
    partition.fence_view_window(1, None);
    assert!(
        !partition.send_prepare_ok(&first).await,
        "an ack above an unproven window can be wiped when the window breaks"
    );
}

#[compio::test]
async fn given_a_staged_offset_under_commit_max_when_its_entry_is_truncated_should_drop_the_staging()
 {
    let mut partition = test_partition();
    journal_prepare(&partition, 1, Operation::StoreConsumerOffset).await;
    journal_prepare(&partition, 2, Operation::StoreConsumerOffset).await;
    partition.consensus.sequencer().set_sequence(2);
    partition.consensus.restore_commit_state(0, 2);
    partition.stage_consumer_offset_upsert(2, ConsumerKind::Consumer, 8, 0, false);
    partition.truncate_uncommitted_from(2).await.unwrap();
    assert!(
        !partition.pending_consumer_offset_commits.contains_key(&2),
        "the stale cursor would be applied in place of the view's op 2"
    );
}

/// Walking through the head advances `commit_min` past a resident entry only
/// `on_ack` can pop and answer, after which every later ack finds the drain
/// empty and no reply is ever shipped.
#[compio::test]
async fn given_a_pipeline_head_when_walking_the_journal_should_stop_below_it() {
    let partition = test_partition();
    for op in 1..=4 {
        journal_prepare(&partition, op, Operation::CreateStream).await;
    }
    partition.consensus.restore_commit_state(0, 4);
    partition.consensus.pipeline_message(
        PlaneKind::Partitions,
        &pipeline_prepare(3, Operation::CreateStream),
    );

    let ops: Vec<u64> = partition
        .collect_committable_from_journal(COMMIT_WALK_OPS_MAX, &repair_config())
        .into_iter()
        .map(|entry| entry.op)
        .collect();
    assert_eq!(
        ops,
        vec![1, 2],
        "the walk stops at the op the pipeline holds, leaving 3 to on_ack"
    );
}

/// A backup journals replicated prepares and never populates a pipeline, so an
/// absent head must mean NO ceiling. Read as a ceiling of zero it would stop
/// every backup's commit walk.
#[compio::test]
async fn given_an_empty_pipeline_when_walking_the_journal_should_not_cap() {
    let partition = test_partition();
    for op in 1..=3 {
        journal_prepare(&partition, op, Operation::CreateStream).await;
    }
    partition.consensus.restore_commit_state(0, 3);
    assert!(partition.consensus.pipeline_head_header().is_none());

    let ops: Vec<u64> = partition
        .collect_committable_from_journal(COMMIT_WALK_OPS_MAX, &repair_config())
        .into_iter()
        .map(|entry| entry.op)
        .collect();
    assert_eq!(ops, vec![1, 2, 3], "a backup walks its whole committed run");
}

#[compio::test]
async fn persisted_commit_walk_preserves_pipeline_gaps_and_wal_ceiling() {
    let directory = tempfile::tempdir().unwrap();
    let mut partition = partition_at_view(0, 0);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.runtime_options.durability = iggy_common::Durability::Persisted;
    partition.open_persistence().await.unwrap();
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    let mut parent = 0;
    for op in 1..=4 {
        let prepare = checksummed_segment_prepare(op, parent, op - 1, b"payload");
        parent = prepare.header().checksum;
        if op >= 3 {
            partition
                .consensus
                .pipeline_message(PlaneKind::Partitions, &prepare);
        }
        let frozen = prepare.into_frozen();
        if op <= 3 {
            persistence.append(frozen.clone(), true).unwrap();
        }
        partition.log.journal().inner.append(frozen).await.unwrap();
    }
    assert!(persistence.start());
    Rc::clone(&persistence).run().await;
    assert!(persistence.failure().is_none());
    persistence.exhaust_capacity_for_test();
    partition.consensus.restore_commit_state(0, 4);

    assert_eq!(partition.select_persistable_commits(&repair_config()), []);
    let journaled =
        partition.collect_committable_from_journal(COMMIT_WALK_OPS_MAX, &repair_config());
    assert_eq!(
        journaled.iter().map(|entry| entry.op).collect::<Vec<_>>(),
        vec![1, 2]
    );
    partition.consensus.advance_commit_min(1);
    partition.consensus.advance_commit_min(2);
    let drained = partition.select_persistable_commits(&repair_config());
    assert_eq!(
        drained.iter().map(|entry| entry.op).collect::<Vec<_>>(),
        vec![3]
    );
    assert_eq!(partition.consensus.pipeline_head_header().unwrap().op, 3);
    partition
        .handle_committed_entries(drained, &repair_config(), true)
        .await;
    assert_eq!(partition.consensus.commit_min(), 3);
    assert_eq!(partition.select_persistable_commits(&repair_config()), []);
    assert_eq!(partition.consensus.pipeline_head_header().unwrap().op, 4);
    assert_eq!(
        partition.collect_committable_from_journal(COMMIT_WALK_OPS_MAX, &repair_config()),
        []
    );
    persistence.retire();
}

fn pipeline_prepare(op: u64, operation: Operation) -> Message<PrepareHeader> {
    let size = std::mem::size_of::<PrepareHeader>();
    Message::<PrepareHeader>::new(size).transmute_header(|_, header: &mut PrepareHeader| {
        header.command = Command::Prepare;
        header.op = op;
        header.operation = operation;
        header.retry_capacity = u32::try_from(consensus::PARTITION_DEDUP_CLIENTS_MAX).unwrap();
        header.size = u32::try_from(size).expect("prepare header size fits in u32");
    })
}

#[compio::test]
async fn given_held_repair_frame_when_peer_re_serves_it_should_keep_stall_budget() {
    const IDLE_TICKS: u32 = 7;
    const ATTEMPTS: u32 = 2;
    let mut partition = test_partition();
    partition.repair = Some(armed_session(3, 0));
    partition
        .apply_repaired_prepare(repaired_send_prepare(1, 0, 1))
        .await;
    assert!(partition.log.journal().inner.holds_op(1));
    partition.repair.as_mut().unwrap().idle_ticks = IDLE_TICKS;
    partition.repair_attempts = ATTEMPTS;

    partition
        .apply_repaired_prepare(repaired_send_prepare(1, 0, 1))
        .await;

    assert_eq!(
        partition.repair.as_ref().unwrap().idle_ticks,
        IDLE_TICKS,
        "a held frame must not restart the stall clock"
    );
    assert_eq!(
        partition.repair_attempts, ATTEMPTS,
        "a held frame must not refund the stall budget"
    );
}

/// A frame that passes the window filters but that the journal refuses
/// stays not held, so the peer serves it again every round. It is no
/// progress, so the budget must still run out.
#[compio::test]
async fn given_unjournalable_repair_frame_when_peer_re_serves_it_should_spend_stall_budget() {
    let mut partition = test_partition();
    partition.repair = Some(armed_session(3, 0));

    let mut verdicts = Vec::new();
    for _ in 0..=crate::types::REPAIR_MAX_STALL_RETRIES {
        partition
            .apply_repaired_prepare(pipeline_prepare(1, Operation::SendMessages))
            .await;
        assert!(!partition.log.journal().inner.holds_op(1));
        verdicts.push(partition.burn_repair_attempt());
    }

    assert_eq!(
        verdicts.iter().position(|&rotate| rotate),
        Some(verdicts.len() - 1),
        "a frame the journal refuses must not refund the budget: {verdicts:?}"
    );
}

/// Without a prepare WAL a refused floor stays in progress while the
/// window is incomplete. Every round loses op `FLOOR + 1`, so the leading
/// edge below the anchor stays open and the refusal never turns final,
/// while the rest of each round re-serves frames this journal already
/// holds. Held frames are no progress, so the budget must run out and
/// rotate the peer. Above a committed prefix, so the walk mark that the
/// first round records must not pass for progress in the rounds after it.
#[compio::test]
async fn given_walless_refused_floor_when_peer_re_serves_held_frames_should_rotate_session() {
    const COMMIT_MIN: u64 = 5;
    const FLOOR: u64 = 10;
    const SERVED_THROUGH: u64 = 12;
    const COMMIT_TO_OP: u64 = SERVED_THROUGH + 1;
    let (mut backup, _) = recording_partition_at(1, 3);
    backup
        .consensus()
        .restore_commit_state(COMMIT_MIN, COMMIT_MIN);
    backup.consensus().advance_commit_max(COMMIT_TO_OP);
    backup.repair = Some(armed_session(COMMIT_TO_OP, FLOOR));
    let config = repair_config();

    let mut verdicts = Vec::new();
    for _ in 0..=crate::types::REPAIR_MAX_STALL_RETRIES {
        for op in FLOOR + 2..=SERVED_THROUGH {
            backup
                .apply_repaired_prepare(checksummed_segment_prepare(op, 0, op - 1, b"repaired"))
                .await;
        }
        assert_eq!(
            backup.complete_repair(&config).await,
            RepairConclusion::InProgress
        );
        verdicts.push(backup.burn_repair_attempt());
    }

    assert_eq!(
        verdicts.iter().position(|&rotate| rotate),
        Some(verdicts.len() - 1),
        "only the round past the budget rotates: {verdicts:?}"
    );
}

#[compio::test]
async fn given_a_repair_larger_than_the_wal_when_a_prefix_is_durable_should_apply_it_before_fetching_the_rest()
 {
    const OPS: u64 = 3;
    const PAYLOAD_BYTES: usize = 48 * 1024 * 1024;
    let root = tempfile::tempdir().unwrap();
    let mut config = repair_config();
    config.segment_size = IggyByteSize::from(4 * PAYLOAD_BYTES as u64);
    config.path_layout.streams_root = root.path().to_string_lossy().into_owned();
    let directory = std::path::PathBuf::from(config.get_partition_path(1, 1, 0));
    std::fs::create_dir_all(&directory).unwrap();
    let (mut backup, _) = recording_partition_at(1, 3);
    backup.set_partition_dir(directory.to_string_lossy().into_owned());
    set_offset_dirs_under(&mut backup, &directory);
    backup.runtime_options.durability = iggy_common::Durability::Persisted;
    backup.runtime_options.segment_size = Some(config.segment_size);
    let payload = vec![1; PAYLOAD_BYTES];
    let parents = segment_chain_parents(OPS, &payload);
    let prepare = |op: u64| {
        checksummed_segment_prepare(
            op,
            parents[usize::try_from(op - 1).unwrap()],
            op - 1,
            &payload,
        )
    };
    let frame_bytes = prepare(1).total_len();
    backup.log.retire_front().unwrap();
    backup.install_empty_segment(&config, 0).await.unwrap();
    backup
        .open_persistence_with_capacity(
            journal::partition_journal::PARTITION_WAL_CAPACITY_MIN,
            std::time::Duration::ZERO,
        )
        .await
        .unwrap();
    backup.consensus().advance_commit_max(OPS);
    backup.repair = Some(RepairSession {
        floor: None,
        ..armed_session(OPS, 0)
    });
    let persistence = Rc::clone(backup.persistence.as_ref().unwrap());
    assert!(persistence.segment_checkpoint().is_some());

    persistence.append(prepare(1).into_frozen(), true).unwrap();
    assert!(persistence.start());
    backup.apply_repaired_prepare(prepare(1)).await;
    Rc::clone(&persistence).run().await;
    assert!(persistence.is_durable_through(1));
    persistence.append(prepare(2).into_frozen(), true).unwrap();
    assert!(persistence.start());
    backup.apply_repaired_prepare(prepare(2)).await;
    assert!(!persistence.has_capacity(frame_bytes));
    assert!(!persistence.is_durable_through(2));
    assert!(!backup.repair_fetch_ready());

    assert_eq!(
        backup.complete_repair(&config).await,
        RepairConclusion::InProgress
    );
    assert_eq!(
        backup.consensus().commit_min(),
        1,
        "the verified durable prefix must apply before the rest of the window can fit"
    );
    assert!(backup.repair.is_some());
    Rc::clone(&persistence).run().await;
    settle_partition_io(&mut backup, &config).await;
    assert_eq!(
        backup.complete_repair(&config).await,
        RepairConclusion::InProgress
    );
    assert_eq!(backup.consensus().commit_min(), 2);
    backup.checkpoint_persistence(&config).await;
    settle_partition_io(&mut backup, &config).await;
    persistence
        .drain_with_timeout(|timeout| backup.consensus.message_bus().sleep(timeout))
        .await
        .unwrap();
    settle_partition_io(&mut backup, &config).await;
    assert!(
        persistence.has_capacity(frame_bytes),
        "applying and checkpointing the prefix must reclaim WAL capacity"
    );
    assert!(backup.repair_fetch_ready());

    backup.apply_repaired_prepare(prepare(3)).await;
    backup.persist_repaired_prefix();
    persistence
        .drain_with_timeout(|timeout| backup.consensus.message_bus().sleep(timeout))
        .await
        .unwrap();
    settle_partition_io(&mut backup, &config).await;
    assert_eq!(
        backup.complete_repair(&config).await,
        RepairConclusion::Done,
        "commit={}, head={}, durable={}, sequence={}, fatal={:?}",
        backup.consensus().commit_min(),
        persistence.head(),
        persistence.durable_op(),
        backup.consensus().sequencer().current_sequence(),
        backup.fatal(),
    );
    assert_eq!(backup.consensus().commit_min(), OPS);
    assert!(backup.fatal().is_none());
}

/// A bounded commit walk can leave already fetched frames for the next
/// round. Advancing the walk keeps the peer's repair budget intact.
#[compio::test]
async fn given_advancing_repair_walk_when_peer_re_serves_held_frames_should_keep_peer() {
    const ROUNDS: u32 = crate::types::REPAIR_MAX_STALL_RETRIES + 1;
    const COMMIT_TO_OP: u64 = COMMIT_WALK_OPS_MAX as u64 * ROUNDS as u64;
    let (mut backup, _) = recording_partition_at(1, 3);
    backup.consensus().advance_commit_max(COMMIT_TO_OP);
    backup.repair = Some(RepairSession {
        floor: None,
        ..armed_session(COMMIT_TO_OP, 0)
    });
    let config = repair_config();
    let parents = segment_chain_parents(COMMIT_TO_OP, b"repaired");

    for round in 0..ROUNDS {
        for op in backup.consensus().commit_min() + 1..=COMMIT_TO_OP {
            let parent = parents[usize::try_from(op - 1).unwrap()];
            backup
                .apply_repaired_prepare(checksummed_segment_prepare(
                    op,
                    parent,
                    op - 1,
                    b"repaired",
                ))
                .await;
        }
        backup.commit_journal(&config).await;
        assert!(
            !backup.burn_repair_attempt(),
            "round {round} rotated away although the walk reached {}",
            backup.consensus().commit_min()
        );
    }

    assert_eq!(backup.consensus().commit_min(), COMMIT_TO_OP);
}

/// A walk restarts the budget once: the mark moves with it, so the rounds
/// after the walk stops spend a whole budget and no more.
#[test]
fn given_credited_walk_when_later_stall_rounds_find_no_progress_should_rotate_at_budget() {
    const COMMIT_MIN: u64 = 5;
    let (mut backup, _) = recording_partition_at(1, 3);
    backup
        .consensus()
        .restore_commit_state(COMMIT_MIN, COMMIT_MIN + 1);
    assert!(!backup.burn_repair_attempt());
    backup.consensus().advance_commit_min(COMMIT_MIN + 1);

    let verdicts: Vec<bool> = (0..=crate::types::REPAIR_MAX_STALL_RETRIES)
        .map(|_| backup.burn_repair_attempt())
        .collect();

    assert_eq!(
        verdicts.iter().position(|&rotate| rotate),
        Some(verdicts.len() - 1),
        "only the round past the budget that follows the walk rotates: {verdicts:?}"
    );
}

/// A stall round in a new view starts a fresh count, as on the metadata
/// plane: the rotations of an earlier view say nothing about the ring of
/// this one.
#[test]
fn given_burned_repair_budget_when_view_changes_should_start_fresh_counts() {
    let (mut backup, _) = recording_partition_at(1, 3);
    for _ in 0..crate::types::REPAIR_MAX_STALL_RETRIES {
        assert!(!backup.burn_repair_attempt());
    }
    backup.note_repair_rotation();
    let next_view = backup.consensus().view() + 1;
    backup.consensus.set_view(next_view);

    let verdicts: Vec<bool> = (0..=crate::types::REPAIR_MAX_STALL_RETRIES)
        .map(|_| backup.burn_repair_attempt())
        .collect();

    assert_eq!(
        (
            verdicts.iter().position(|&rotate| rotate),
            backup.repair_rotations()
        ),
        (Some(verdicts.len() - 1), 0),
        "the new view must restart the stall rounds and the rotations: {verdicts:?}"
    );
}

#[compio::test]
async fn given_lower_backfill_when_applying_repaired_prepare_should_not_rewind_parent() {
    // The call-site regression for the WAL frontier rewind: a repaired op
    // below the DVC-adopted head must leave BOTH halves of the frontier
    // alone. The old code left the sequencer at the head and rewound
    // `last_prepare_checksum` to the backfilled entry, so the next prepare
    // parented past a committed op and recovery refused the WAL.
    const CHECKSUM_1: u128 = 0x11;
    const CHECKSUM_2: u128 = 0x22;
    const CHECKSUM_3: u128 = 0x33;
    let mut partition = test_partition();
    partition.repair = Some(armed_session(3, 0));

    partition
        .apply_repaired_prepare(repaired_send_prepare(1, 0, CHECKSUM_1))
        .await;
    partition
        .apply_repaired_prepare(repaired_send_prepare(3, CHECKSUM_2, CHECKSUM_3))
        .await;
    // The hole at op 2 stalls the frontier advance.
    assert_eq!(partition.consensus().sequencer().current_sequence(), 1);

    // The state a DoViewChange merge leaves: head 3, parented on its
    // checksum, with the hole at op 2 still unfilled locally.
    partition.consensus().sequencer().set_sequence(3);
    partition.consensus().set_last_prepare_checksum(CHECKSUM_3);

    partition
        .apply_repaired_prepare(repaired_send_prepare(2, CHECKSUM_1, CHECKSUM_2))
        .await;

    assert!(
        partition.log.journal().inner.header_by_op(2).is_some(),
        "the backfill must be journaled"
    );
    assert_eq!(partition.consensus().sequencer().current_sequence(), 3);
    assert_eq!(
        partition.consensus().last_prepare_checksum(),
        CHECKSUM_3,
        "a lower backfill must not rewind the parent of the next prepare"
    );
}

#[compio::test]
async fn given_gap_closure_when_applying_repaired_prepare_should_adopt_new_head_checksum() {
    // The frame that closes a gap is not the new head: the frontier walks
    // to the highest contiguous op and the checksum must come from THAT
    // journal entry, not from the repair frame that happened to arrive
    // last.
    const CHECKSUM_1: u128 = 0x11;
    const CHECKSUM_2: u128 = 0x22;
    const CHECKSUM_3: u128 = 0x33;
    let mut partition = test_partition();
    partition.repair = Some(armed_session(3, 0));

    partition
        .apply_repaired_prepare(repaired_send_prepare(1, 0, CHECKSUM_1))
        .await;
    partition
        .apply_repaired_prepare(repaired_send_prepare(3, CHECKSUM_2, CHECKSUM_3))
        .await;
    assert_eq!(partition.consensus().sequencer().current_sequence(), 1);
    assert_eq!(partition.consensus().last_prepare_checksum(), CHECKSUM_1);

    partition
        .apply_repaired_prepare(repaired_send_prepare(2, CHECKSUM_1, CHECKSUM_2))
        .await;

    assert_eq!(partition.consensus().sequencer().current_sequence(), 3);
    assert_eq!(
        partition.consensus().last_prepare_checksum(),
        CHECKSUM_3,
        "a gap closure must adopt the checksum of the new contiguous head"
    );
}

#[compio::test]
async fn given_prior_view_repair_when_a_new_view_started_should_discard_it() {
    let mut partition = test_partition();
    partition.repair = Some(armed_fetch_session(0, 1, 0));
    partition.consensus.set_view(1);

    partition
        .apply_repaired_prepare(repaired_send_prepare(1, 0, 0x11))
        .await;

    assert!(
        partition.repair.is_none(),
        "the prior-view session is obsolete"
    );
    assert!(
        partition.log.journal().inner.header_by_op(1).is_none(),
        "a delayed prior-view body must not enter the new view's journal"
    );
}

#[compio::test]
async fn given_session_remint_when_attempts_burned_should_survive_on_partition() {
    let mut partition = test_partition();
    for round in 0..consensus::STATE_TRANSFER_MAX_STALL_RETRIES {
        assert!(!partition.burn_transfer_attempt());
        // A re-minted session must not reset the budget: it lives on the
        // partition precisely because arming sites mint fresh sessions.
        partition.transfer = Some(crate::state_transfer::PartitionTransferSession {
            nonce: u128::from(round),
            peer: 0,
            commit_op: 0,
            artifacts: Vec::new(),
            target_accepted: false,
            idle_ticks: 0,
            spill: None,
            scan: None,
            seed: None,
            pulled_len: 0,
        });
    }
    assert!(partition.burn_transfer_attempt(), "budget exhausts");
    partition.note_transfer_progress();
    assert!(!partition.burn_transfer_attempt(), "progress resets it");
}

#[compio::test]
async fn given_repeated_failures_when_only_generation_advances_should_keep_counting() {
    let mut partition = test_partition();
    // A committing primary advances its generation every round; the
    // consecutive count must keep growing regardless, or a
    // deterministic local failure retries at network round-trip rate
    // forever. Only a completed install resets it.
    assert_eq!(partition.record_transfer_failure(), 1);
    assert_eq!(partition.record_transfer_failure(), 2);
    partition.note_transfer_progress();
    assert_eq!(
        partition.record_transfer_failure(),
        3,
        "received chunks are not install progress"
    );
    partition.note_transfer_installed();
    assert_eq!(partition.record_transfer_failure(), 1, "install resets");
}
