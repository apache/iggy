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
async fn given_storeless_partition_when_persist_gate_runs_should_mark_current_view_durable() {
    let partition = partition_at_view(2, 1);
    assert!(partition.consensus().needs_superblock_persist());

    assert!(partition.persist_superblock_if_needed().await);

    assert!(
        !partition.consensus().needs_superblock_persist(),
        "a storeless partition must record durable = current, or the dispatch \
             tripwire would fire on its first view-scoped send"
    );
}

#[test]
fn given_held_commits_when_more_are_deferred_should_keep_the_latest_per_target() {
    let (partition, _) = recording_partition();
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    let commit = |target: Option<u8>, timestamp_monotonic: u64| consensus::VsrAction::SendCommit {
        view: 0,
        commit: 1,
        group: partition.namespace().inner(),
        timestamp_monotonic,
        target,
    };

    partition.defer_wire_actions(vec![commit(None, 1), commit(Some(1), 2)]);
    partition.defer_wire_actions(vec![commit(Some(2), 3), commit(Some(1), 4)]);
    partition.defer_wire_actions(vec![commit(None, 5)]);

    let held: Vec<_> = partition
        .pending_wire_actions
        .borrow()
        .as_ref()
        .expect("the commits are held")
        .actions
        .iter()
        .map(|action| match action {
            consensus::VsrAction::SendCommit {
                target,
                timestamp_monotonic,
                ..
            } => (*target, *timestamp_monotonic),
            other => panic!("held a non-commit action: {other:?}"),
        })
        .collect();
    assert_eq!(
        held,
        [(None, 5), (Some(1), 4), (Some(2), 3)],
        "a Commit to one backup must neither displace the heartbeat to all nor \
             another backup's Commit"
    );
}

/// The tick claims a lease past the CEILING. Extending past the append point
/// instead buys back only the headroom the trigger had left -- about half a
/// lease -- and doubles the write rate the default lease is sized for.
#[compio::test]
async fn given_a_tick_extension_when_it_writes_should_advance_the_ceiling_a_full_lease() {
    let store = Rc::new(RecordingSuperblock::default());
    let mut partition = solo_recording_partition();
    partition.set_superblock(store.clone(), None);
    partition.set_offset_reservation_lease(test_lease(16));

    assert!(partition.reserve_offsets_through(0).await);
    assert_eq!(last_recorded_reservation(&store), 17);
    partition.note_append_live();

    // Half the block consumed, which is where the trigger fires.
    partition.dirty_offset.store(11, Ordering::Relaxed);
    assert!(partition.needs_offset_reservation_extension());
    assert!(partition.extend_offset_reservation().await);
    assert_eq!(
        last_recorded_reservation(&store),
        33,
        "a full lease past the ceiling of 17, not past the append point of 12"
    );

    // And the trigger is genuinely satisfied for a full block of appends,
    // rather than re-firing after another half.
    for offset in 12..=24 {
        partition.dirty_offset.store(offset, Ordering::Relaxed);
        assert!(
            !partition.needs_offset_reservation_extension(),
            "offset {offset} still sits a full half-lease under the new ceiling"
        );
    }
}

/// A storeless partition (in-memory, simulated) reserves nothing at all, so
/// the tick must never reach a write for one.
#[test]
fn given_a_storeless_partition_when_ticking_should_not_extend() {
    let mut partition = solo_recording_partition();
    partition.set_offset_reservation_lease(test_lease(16));
    // Without this the assert would pass on `!append_live` alone, leaving
    // the store check it is named for untested.
    partition.note_append_live();
    assert!(partition.superblock.is_none(), "the premise: no store");
    assert!(!partition.needs_offset_reservation_extension());
}

/// Inside an open backoff window the ADMITTED path refuses without touching
/// the disk the last writer just found broken. Every producer retry otherwise
/// re-runs a full atomic replace, which starves the shard pump for as long as
/// the fault lasts.
#[compio::test]
async fn given_an_open_backoff_window_when_preflighting_a_send_should_refuse_without_writing() {
    let store = Rc::new(RecordingSuperblock::default());
    let mut partition = solo_recording_partition();
    partition.set_superblock(store.clone(), None);
    partition.set_offset_reservation_lease(test_lease(16));
    partition
        .superblock_retry_after_micros
        .set(partition.consensus().clock_realtime_micros() + 1_000_000);

    assert_eq!(
        partition.reserve_offsets_through_retryable(0).await,
        crate::PartitionIoVerdict::Failed,
        "a claim that would need a write is refused inside the window"
    );
    assert_eq!(
        store.attempts.get(),
        0,
        "and refused without any I/O at all"
    );

    // Isolated bootstrap still attempts its required initial claim.
    assert!(partition.reserve_offsets_through(0).await);
    assert_eq!(store.attempts.get(), 1);

    // A batch the record already covers owes the disk nothing, so the open
    // window must not refuse it either.
    assert_eq!(
        partition.reserve_offsets_through_retryable(0).await,
        crate::PartitionIoVerdict::Ready,
        "the coverage fast path wins over the backoff window"
    );
    assert_eq!(store.attempts.get(), 1);
}

/// A journaled offset is not a committed one. Seeding the committed bit at
/// the append would serve a resident offset 0 to a consumer before the first
/// commit and let the frontier persist name data no quorum agreed on.
#[test]
fn given_a_journaled_offset_when_uncommitted_should_not_seed_the_committed_counter() {
    let mut partition = solo_recording_partition();

    partition.note_append_live();
    partition.dirty_offset.store(0, Ordering::Relaxed);
    assert!(partition.offset_space.append_live);
    assert!(
        !partition.offset_space.committed_seeded,
        "the append moves the append counter alone"
    );
    assert_eq!(partition.mint_frontier(), 1, "the next mint continues it");
    assert_eq!(
        partition.offset_frontier(),
        0,
        "and the frontier a persist would record still names no data"
    );

    partition.note_committed_seeded();
    assert_eq!(
        partition.offset_frontier(),
        1,
        "commit is what publishes the offset"
    );
}

/// Fail-closed: offsets the record does not cover would be confirmed to a
/// client with nothing durable saying they were handed out.
/// The fence ahead of the pipeline must bound a mint the ordinary send path
/// actually takes. `convert_request_message` runs at `ChecksumMode::Skip`, so
/// a verifying decode of its output fails and the ceiling would come back
/// `None`, dropping every solo send back to the fence at the mint, where a
/// refusal fences the partition and exits the node.
#[test]
fn given_a_checksumless_send_when_bounding_the_mint_should_read_the_batch_header() {
    let partition = solo_recording_partition();
    let namespace = IggyNamespace::from_raw(partition.consensus().group());
    let message = checksumless_send_request(namespace, 3);
    let body = &message.as_slice()
        [std::mem::size_of::<RoutedRequestHeader>()..message.header().size as usize];

    assert!(
        decode_batch_slice(body).is_err(),
        "the premise: a Skip-converted body does not survive a verifying decode"
    );
    assert_eq!(
        partition.request_mint_ceiling(&message),
        Some(2),
        "the ceiling is the batch's LAST offset: three messages from a frontier \
             of 0 mint 0, 1 and 2, and the claim adds the exclusive successor itself"
    );
}

/// Nothing is reserved above one replica, so the ceiling is not computed
/// there either.
#[test]
fn given_a_replicated_group_when_bounding_the_mint_should_not_compute_a_ceiling() {
    let partition = partition_at_view(1, 1);
    assert!(partition.consensus().replica_count() > 1);
    let namespace = IggyNamespace::from_raw(partition.consensus().group());
    assert_eq!(
        partition.request_mint_ceiling(&checksumless_send_request(namespace, 3)),
        None
    );
}

#[test]
fn given_a_rearmed_advance_when_requesting_shutdown_should_replace_it() {
    let mut partition = solo_recording_partition();
    partition.set_superblock(Rc::new(RecordingSuperblock::default()), None);
    let advance = SuperblockIntent::Advance {
        frontier: 0,
        reserved: 64,
    };
    partition.superblock_intent.set(Some(advance));
    let collapse = SuperblockIntent::Collapse {
        frontier: 0,
        reserved: 0,
    };

    assert_eq!(
        partition.request_superblock_intent(collapse),
        crate::PartitionIoVerdict::Pending
    );
    assert!(
        partition.superblock_intent.get() == Some(collapse),
        "shutdown must replace the failed Advance"
    );
    assert_eq!(
        partition.request_superblock_intent(advance),
        crate::PartitionIoVerdict::Pending
    );
    assert!(
        partition.superblock_intent.get() == Some(collapse),
        "an Advance must not replace the final write"
    );
}

/// The point of extending from the tick: after it runs, the append path finds
/// the ceiling already covering it and writes nothing. Without this the two
/// fsyncs land in front of a produce, inside the frame pump the consensus
/// tick shares.
#[compio::test]
async fn given_a_consumed_block_when_the_tick_extends_should_leave_the_append_path_writeless() {
    let store = Rc::new(RecordingSuperblock::default());
    let mut partition = solo_recording_partition();
    partition.set_superblock(store.clone(), None);
    partition.set_offset_reservation_lease(test_lease(16));

    // First append pays for the first claim, as it must: nothing durable yet.
    assert!(partition.reserve_offsets_through(0).await);
    assert_eq!(store.attempts.get(), 1);
    partition.set_offset_space_used(true);

    // Inside the block there is nothing to do, from either caller.
    partition.dirty_offset.store(4, Ordering::Relaxed);
    assert!(
        !partition.needs_offset_reservation_extension(),
        "a full block of headroom needs no extension"
    );

    // Past half the block the tick takes the write.
    partition.dirty_offset.store(11, Ordering::Relaxed);
    assert!(
        partition.needs_offset_reservation_extension(),
        "under half a block of headroom the tick must extend"
    );
    assert!(partition.extend_offset_reservation().await);
    assert_eq!(
        store.attempts.get(),
        2,
        "the tick wrote, not the append path"
    );

    // And now the append path is writeless across the rest of the old block.
    let before = store.attempts.get();
    for offset in 12..=16 {
        assert!(partition.reserve_offsets_through(offset).await);
    }
    assert_eq!(
        store.attempts.get(),
        before,
        "every append after the extension must take the fence's fast path"
    );
}

/// The extension must not fire for a partition that has never minted, or boot
/// would write a superblock per idle partition for nothing.
#[test]
fn given_an_untouched_partition_when_ticking_should_not_extend_the_reservation() {
    let mut partition = solo_recording_partition();
    partition.set_superblock(Rc::new(RecordingSuperblock::default()), None);
    assert!(!partition.offset_space.append_live);
    assert!(!partition.needs_offset_reservation_extension());
}

/// A boot that consumed a reservation has ZERO headroom -- the append point
/// sits exactly at the ceiling -- so the extension has to fire before the
/// first produce rather than after it.
#[test]
fn given_a_reservation_seeded_boot_when_ticking_should_extend_before_the_first_produce() {
    let mut partition = solo_recording_partition();
    let recovered = recorded_state(0, 65_537);
    partition.set_superblock(Rc::new(RecordingSuperblock::default()), Some(&recovered));
    partition.restore_offset_frontier(Some(&recovered));
    assert_eq!(partition.mint_frontier(), 65_537);
    assert!(
        partition.needs_offset_reservation_extension(),
        "the seeded append point is at the ceiling, so the next append would \
             otherwise pay for the write"
    );
}

/// A replicated group pays nothing for a protection it cannot use: nothing
/// seeds its counter from the reservation, its chain never gets re-anchored,
/// and an ack there means a quorum journaled the batch.
#[compio::test]
async fn given_a_replicated_group_when_fencing_should_not_write_at_all() {
    let store = Rc::new(RecordingSuperblock::default());
    let mut partition = partition_at_view(1, 1);
    assert!(partition.consensus().replica_count() > 1);
    partition.set_superblock(store.clone(), None);
    partition.set_offset_reservation_lease(test_lease(16));
    store.fail_writes.set(true);

    assert!(
        partition.reserve_offsets_through(1_000).await,
        "the fence is inert above one replica, so not even a failing store can \
             refuse the append"
    );
    assert_eq!(store.attempts.get(), 0, "and it attempts no write");
}

/// The reservation seeds the APPEND counter and only on a solo group. The
/// whole fix: after a crash below the flush thresholds it is the only witness
/// that those offsets were confirmed.
#[test]
fn given_a_recorded_reservation_when_solo_should_seed_the_append_counter() {
    let mut partition = solo_recording_partition();
    assert_eq!(
        partition.consensus().replica_count(),
        1,
        "the reservation seed is scoped to solo groups"
    );
    let store = Rc::new(RecordingSuperblock::default());

    partition.set_superblock(store.clone(), None);
    partition.restore_offset_frontier(None);
    assert_eq!(
        partition.mint_frontier(),
        0,
        "nothing recorded, nothing seeded"
    );

    // The shape a crash before the first flush leaves: the fence recorded a
    // block, and no message ever reached a segment, so the frontier is 0.
    let recovered = recorded_state(0, 65_537);
    partition.set_superblock(store, Some(&recovered));
    partition.restore_offset_frontier(Some(&recovered));
    assert_eq!(
        partition.mint_frontier(),
        65_537,
        "the first mint must land above every offset the reservation covered"
    );
    assert_eq!(
        partition.offset_frontier(),
        0,
        "the committed frontier must not inherit the reservation, nor the \
             append counter's own liveness: nothing was flushed, so the partition \
             holds no offset at all"
    );
}

/// A replicated group must not take the jump: a backup rejects any prepare
/// whose `base_offset` does not continue its own counter, so an append point a
/// lease block above the group has every peer refuse the batch.
#[test]
fn given_a_recorded_reservation_when_replicated_should_not_seed_the_append_counter() {
    let mut partition = partition_at_view(1, 1);
    assert!(partition.consensus().replica_count() > 1);
    let recovered = recorded_state(0, 70_000);
    partition.set_superblock(Rc::new(RecordingSuperblock::default()), Some(&recovered));
    partition.restore_offset_frontier(Some(&recovered));
    assert_eq!(
        partition.mint_frontier(),
        0,
        "a replicated group's offsets are the group's to decide"
    );
}

#[test]
fn given_persisted_singleton_when_restoring_should_use_the_frontier_without_a_reservation_gap() {
    let mut partition = solo_recording_partition();
    partition.set_runtime_options(TopicRuntimeOptions {
        durability: iggy_common::Durability::Persisted,
        ..TopicRuntimeOptions::default()
    });
    let recovered = recorded_state(40, 70_000);
    partition.set_superblock(Rc::new(RecordingSuperblock::default()), Some(&recovered));
    partition.restore_offset_frontier(Some(&recovered));
    assert_eq!(partition.offset_frontier(), 40);
    assert_eq!(partition.mint_frontier(), 40);
}

/// The committed frontier still seeds both counters: it is a claim about data
/// every replica shares, so it is not gated on the replica count.
#[test]
fn given_a_recorded_frontier_when_replicated_should_seed_both_counters() {
    let mut partition = partition_at_view(1, 1);
    assert!(partition.consensus().replica_count() > 1);
    let recovered = recorded_state(40, 70_000);
    partition.set_superblock(Rc::new(RecordingSuperblock::default()), Some(&recovered));
    partition.restore_offset_frontier(Some(&recovered));
    assert_eq!(partition.offset_frontier(), 40);
    assert_eq!(
        partition.mint_frontier(),
        40,
        "the reservation is ignored here, so the append point is the frontier"
    );
}

/// The rewind fence asks whether an offer would destroy something this replica
/// COMMITTED. An append point standing a lease block above that -- what a
/// reservation-seeded boot leaves until the first append -- claims no
/// messages, so it must not turn a legitimate offer inside the block into a
/// refusal the replica would then cycle on forever.
#[compio::test]
async fn given_an_append_point_above_every_message_when_an_offer_arrives_should_not_call_it_a_rewind()
 {
    let partition_dir = transfer_fence_dir("append-point-is-not-data").await;
    let mut partition = test_partition();
    partition.set_partition_dir(partition_dir.clone());
    partition.set_offset_space_used(true);
    partition.dirty_offset.store(69_999, Ordering::Relaxed);
    assert_eq!(partition.mint_frontier(), 70_000);
    assert_eq!(
        partition.offset_frontier(),
        1,
        "nothing committed: the append point speaks for no messages"
    );

    let offer = crate::state_transfer::ConsumerOffsetsWire {
        producers: Vec::new(),
        owner_retirement: None,
        owners: Vec::new(),
        history_transition: None,
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        required_metadata_frontier: 0,
        prepare_checksum: None,
        checkpoint_prepare: Vec::new(),
        next_offset: 1_030,
        emptied_at: 0,
        consumers: Vec::new(),
        groups: Vec::new(),
        external_groups: Vec::new(),
        dedup: Vec::new(),
        retirement_paid_through: 0,
        machines: None,
    };
    let outcome = partition
        .install_state_transfer(&repair_config(), 12, Vec::new(), &offer.encode())
        .await;
    assert!(
        !matches!(
            outcome,
            Err(crate::state_transfer::PartitionInstallError::OfferRewindsDurableData { .. })
        ),
        "the offer destroys nothing this replica holds, so the fence must let it \
             through: got {outcome:?}"
    );

    let _ = std::fs::remove_dir_all(&partition_dir);
}

/// After a clean shutdown the segments account for every confirmed offset,
/// so collapsing the reservation keeps the offset space dense across an
/// ordinary restart instead of jumping a lease block every time.
#[compio::test]
async fn given_a_flushed_partition_when_collapsing_should_drop_the_reservation_to_the_frontier() {
    let store = Rc::new(RecordingSuperblock::default());
    let mut partition = solo_recording_partition();
    partition.set_superblock(store.clone(), Some(&recorded_state(0, 65_537)));
    // A flushed partition: the append point and the committed head agree.
    partition.set_offset_space_used(true);
    partition.offset.store(24, Ordering::Release);
    partition.dirty_offset.store(24, Ordering::Relaxed);

    assert!(
        partition
            .complete_superblock_intent(partition.collapse_superblock_intent().unwrap())
            .await
    );
    assert_eq!(last_recorded_frontier(&store), 25);
    assert_eq!(
        last_recorded_reservation(&store),
        25,
        "a clean stop leaves no claim above what the segments prove"
    );

    // And the restart that follows mints where it left off, not a block up.
    let recovered = recorded_state(25, 25);
    let mut restarted = solo_recording_partition();
    restarted.set_superblock(store.clone(), Some(&recovered));
    restarted.restore_offset_frontier(Some(&recovered));
    assert_eq!(
        restarted.mint_frontier(),
        25,
        "the collapsed record puts the append point back at the frontier"
    );
}

/// The shard keeps ticking until every partition finished its shutdown. An
/// extension after the collapse would make the restart resume a lease block
/// above the data.
#[compio::test]
async fn given_a_collapsed_shutdown_when_the_tick_runs_should_not_extend_the_reservation() {
    const SHUTDOWN_SETTLES_MAX: usize = 4;
    let store = Rc::new(RecordingSuperblock::default());
    let mut partition = solo_recording_partition();
    partition.set_superblock(store.clone(), Some(&recorded_state(0, 65_537)));
    partition.set_offset_space_used(true);
    partition.offset.store(24, Ordering::Release);
    partition.dirty_offset.store(24, Ordering::Relaxed);
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    partition.begin_shutdown_io();
    for _ in 0..SHUTDOWN_SETTLES_MAX {
        if partition.shutdown_io_complete() {
            break;
        }
        settle_partition_io(&mut partition, &repair_config()).await;
    }
    assert!(partition.shutdown_io_complete());
    assert_eq!(last_recorded_reservation(&store), 25);

    if partition.needs_offset_reservation_extension() {
        let _ = partition.extend_offset_reservation().await;
    }
    for _ in 0..SHUTDOWN_SETTLES_MAX {
        settle_partition_io(&mut partition, &repair_config()).await;
    }
    assert_eq!(
        last_recorded_reservation(&store),
        25,
        "the collapse must stay the last superblock write of the shutdown"
    );
}

/// The graceful stop must not undo the crash protection. A boot that read a
/// reservation back and then took no traffic has a committed frontier of 0
/// while the reservation is the only record that offsets were confirmed, so a
/// collapse reading the committed frontier would write it away -- and a clean
/// stop is the runbook answer to an incident, which would make it the one
/// action that re-opens the defect.
#[compio::test]
async fn given_an_unspent_reservation_when_collapsing_should_leave_it_standing() {
    let store = Rc::new(RecordingSuperblock::default());
    let recovered = recorded_state(0, 65_537);
    let mut partition = solo_recording_partition();
    partition.set_superblock(store.clone(), Some(&recovered));
    partition.restore_offset_frontier(Some(&recovered));

    assert!(partition.collapse_superblock_intent().is_none());
    assert_eq!(
        store.attempts.get(),
        0,
        "the reservation already covers the append point, so there is nothing to \
             collapse and nothing to write"
    );

    // The restart after the clean stop still resumes above every offset the
    // crashed incarnation confirmed.
    let mut restarted = solo_recording_partition();
    restarted.set_superblock(store, Some(&recovered));
    restarted.restore_offset_frontier(Some(&recovered));
    assert_eq!(restarted.mint_frontier(), 65_537);
}

/// An install is the one place the reservation may come down: left high, it
/// re-seeds the counter above the group and every replicated prepare fails
/// the `base_offset == dirty_offset + 1` check.
#[compio::test]
async fn given_install_frontier_when_recorded_should_set_the_reservation_down_to_it() {
    let store = Rc::new(RecordingSuperblock::default());
    let mut partition = partition_at_view(1, 1);
    partition.set_superblock(store.clone(), Some(&recorded_state(0, 70_000)));

    assert!(
        partition
            .complete_superblock_intent(SuperblockIntent::Install(1_030))
            .await
    );
    assert_eq!(
        last_recorded_reservation(&store),
        1_030,
        "the install's frontier replaces the stale reservation"
    );
    assert_eq!(last_recorded_frontier(&store), 1_030);
}

/// The install's frontier write describes the state the install is about
/// to adopt. Once its failure aborts the install, the replica keeps its old
/// state, so the same write landing later would record a frontier and a
/// reservation for an install that never happened.
#[compio::test]
async fn given_failed_install_frontier_write_when_install_aborts_should_never_write_it_later() {
    // Between the recorded frontier and reservation, so a late write would
    // move both.
    const OFFER_FRONTIER: u64 = 150;
    const STEPS_MAX: usize = 32;
    let directory = tempfile::tempdir().unwrap();
    let mut partition = partition_at_view(1, 1);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    set_offset_dirs_under(&mut partition, directory.path());
    let store = Rc::new(RecordingSuperblock::default());
    let recovered = recorded_state(100, 200);
    partition.set_superblock(Rc::clone(&store), Some(&recovered));
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    store.fail_writes.set(true);
    let wire = crate::state_transfer::ConsumerOffsetsWire {
        producers: Vec::new(),
        dedup_capacity: consensus::PARTITION_DEDUP_CLIENTS_MAX,
        next_offset: OFFER_FRONTIER,
        ..Default::default()
    };
    partition
        .queue_state_transfer_install(1, Vec::new(), &wire.encode(), 1)
        .unwrap();

    let config = repair_config();
    let mut aborted = false;
    for _ in 0..STEPS_MAX {
        match partition.resume_io(&config).await {
            crate::PartitionIoStep::Ready(plan) => {
                execute_partition_io_plan(&mut partition, &config, plan).await;
            }
            crate::PartitionIoStep::InstallFinished { outcome, .. } => {
                assert!(
                    matches!(
                        outcome,
                        Err(
                            crate::state_transfer::PartitionInstallError::FrontierNotDurable {
                                frontier: OFFER_FRONTIER
                            }
                        )
                    ),
                    "the failed frontier write must abort the install: got {outcome:?}"
                );
                aborted = true;
                break;
            }
            crate::PartitionIoStep::Progress | crate::PartitionIoStep::Pending => {}
            _ => panic!("unexpected install continuation"),
        }
    }
    assert!(aborted);
    assert_eq!(
        store.attempts.get(),
        1,
        "the install must try its frontier write"
    );

    store.fail_writes.set(false);
    partition.superblock_retry_after_micros.set(0);
    settle_partition_io(&mut partition, &config).await;
    assert_eq!(
        store.attempts.get(),
        1,
        "an aborted install must never write its frontier later"
    );
    assert_eq!(
        partition.durable_offset_frontier.get(),
        recovered.offset_frontier
    );
    assert_eq!(
        partition.durable_offset_reserved.get(),
        recovered.offset_reserved
    );
}

/// The reservation can never sit below the frontier: "offsets under N exist"
/// is stronger than "offsets under N may have been handed out".
#[compio::test]
async fn given_reservation_below_the_frontier_when_written_should_clamp_it_up() {
    let store = Rc::new(RecordingSuperblock::default());
    let mut partition = partition_at_view(1, 1);
    partition.set_superblock(store.clone(), None);
    partition.set_offset_space_used(true);
    partition.offset.store(99, Ordering::Release);

    assert!(
        partition
            .complete_superblock_intent(SuperblockIntent::Advance {
                frontier: 100,
                reserved: 0
            })
            .await
    );
    assert_eq!(last_recorded_frontier(&store), 100);
    assert_eq!(
        last_recorded_reservation(&store),
        100,
        "the reservation is clamped up to the frontier it accompanies"
    );
}

/// The fence path persists the frontier while the live counter still sits
/// at its pre-install value, so an advance that maxes against the counter
/// alone erases the record and then quarantines the segments that were its
/// only other witness. Boot re-mints from 0 against a group at N after that.
#[compio::test]
async fn given_record_above_live_counter_when_advancing_should_keep_the_record() {
    let mut partition = partition_at_view(1, 1);
    let store = Rc::new(RecordingSuperblock::default());
    partition.set_superblock(store.clone(), None);

    assert!(
        partition
            .complete_superblock_intent(SuperblockIntent::Advance {
                frontier: 9_000,
                reserved: 0
            })
            .await
    );
    assert_eq!(last_recorded_frontier(&store), 9_000);
    assert_eq!(
        partition.offset_frontier(),
        0,
        "a partition that never minted reports a zero frontier, which is the \
             value the fence would otherwise persist"
    );

    assert!(
        partition
            .complete_superblock_intent(SuperblockIntent::Advance {
                frontier: partition.offset_frontier(),
                reserved: 0
            })
            .await
    );

    assert_eq!(
        last_recorded_frontier(&store),
        9_000,
        "the advance direction must not lower the durable frontier"
    );
}

/// Attaching a store seeds the last-written frontier from the record
/// itself, so an advance maxes against what boot read off disk even before
/// this replica has written anything. The sibling test reaches that state by
/// WRITING first, which cannot catch an attach site that skips the seed.
#[compio::test]
async fn given_attached_record_when_advancing_should_keep_the_recorded_frontier() {
    let mut partition = partition_at_view(1, 1);
    let store = Rc::new(RecordingSuperblock::default());
    let recovered = consensus::VsrState {
        cluster: TEST_CLUSTER,
        replica_id: 0,
        replica_count: 3,
        view: 1,
        log_view: 1,
        commit_max: 0,
        checkpoint_op: 0,
        checkpoint_checksum: 0,
        offset_frontier: 4_200,
        offset_reserved: 0,
        adopted_view: None,
    };
    partition.set_superblock(store.clone(), Some(&recovered));
    assert_eq!(partition.offset_frontier(), 0, "nothing minted locally");

    assert!(
        partition
            .complete_superblock_intent(SuperblockIntent::Advance {
                frontier: partition.offset_frontier(),
                reserved: 0
            })
            .await
    );

    assert_eq!(
        last_recorded_frontier(&store),
        4_200,
        "the first write after an attach must not lower the record it was attached to"
    );
}

#[compio::test]
async fn given_undurable_view_when_sending_prepare_ok_should_withhold_until_persisted() {
    let bus = RecordingBus::default();
    let replica_frames = bus.sent_to_replicas.clone();
    let mut consensus = VsrConsensus::new(
        TEST_CLUSTER,
        0,
        3,
        IggyNamespace::new(1, 1, 0).inner(),
        bus,
        LocalPipeline::new(),
    );
    consensus.set_view(1);
    consensus.set_log_view(1);
    consensus.init_as_backup();
    let mut partition: IggyPartition<RecordingBus, RecordingSuperblock> =
        IggyPartition::with_in_memory_storage(
            Arc::new(PartitionStats::default()),
            consensus,
            IggyByteSize::from(1024 * 1024),
        );
    let store = Rc::new(RecordingSuperblock::default());
    store.fail_writes.set(true);
    partition.set_superblock(store.clone(), None);
    // The ack path drops an op past the local head, so the head must cover it.
    partition.consensus().sequencer().set_sequence(1);
    let size = std::mem::size_of::<PrepareHeader>();
    let prepare =
        Message::<PrepareHeader>::new(size).transmute_header(|_, header: &mut PrepareHeader| {
            header.command = Command::Prepare;
            header.op = 1;
            // Current view: an older-view prepare is fenced as deposed-primary
            // traffic and would never reach the ack send under test.
            header.view = 1;
            header.size = u32::try_from(size).expect("prepare header size fits in u32");
        });
    let header = *prepare.header();

    partition.send_prepare_ok(&header).await;

    assert!(
        replica_frames.borrow().is_empty(),
        "an ack must not leave while the advanced view is not durable"
    );
    assert_eq!(store.attempts.get(), 1);

    // Outwait the write-failure backoff (base 10 ms doubled once by the
    // first failure), then retry with the store healthy: the ack must
    // persist first and then go out.
    store.fail_writes.set(false);
    compio::time::sleep(std::time::Duration::from_millis(50)).await;
    partition.send_prepare_ok(&header).await;

    assert_eq!(
        replica_frames.borrow().len(),
        1,
        "the retried ack must go out once the view persisted"
    );
    assert!(!partition.consensus().needs_superblock_persist());
    assert!(
        partition.pending_persisted_acks.borrow().is_empty(),
        "a partition without a WAL has no persistence driver to drain queued acks"
    );
}

#[test]
fn active_disk_poll_keeps_the_sparse_index_offset() {
    let (mut partition, _) = recording_partition();
    partition.log.ensure_indexes();
    let index = partition.log.active_indexes_mut().unwrap();
    index.insert(0, 0, 0);
    index.insert(100, 100, 6400);
    let query = MessageLookup::Offset {
        offset: 150,
        count: 10,
        ceiling: 200,
    };
    assert_eq!(partition.disk_poll_start(&query), (0, 6400, Some(100)));
}

#[compio::test]
async fn given_committed_delete_fence_when_retrying_success_should_refuse_and_keep_watermark() {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition_at(0, 3);
    partition.runtime_options.durability = iggy_common::Durability::Replicated;
    let config = repair_config();
    let mut old = checksumless_send_request(partition.namespace(), 1);
    old.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
        .copy_from_slice(&build_segment_record(partition.namespace(), 0));
    partition.on_request(old.clone(), None).await;
    assert_eq!(partition.consensus.sequencer().current_sequence(), 1);
    partition.consensus.advance_commit_max(1);
    partition.commit_journal(&config).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(partition.consensus.commit_min(), 1);
    assert_eq!(partition.dedup.count(), 1);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    let transition = TransitionPartitionHistoryRequest {
        incarnation: partition.created_revision(),
        metadata_op: 10,
    };
    let fence = partition_control_request(
        &partition,
        Operation::TransitionPartitionHistory,
        &transition.to_bytes(),
        transition.metadata_op,
    );
    partition.on_request(fence, None).await;
    assert_eq!(partition.installed_history_transition(&transition), None);
    partition.consensus.advance_commit_max(2);
    partition.commit_journal(&config).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(partition.installed_history_transition(&transition), Some(2));
    assert_eq!(partition.offset_frontier(), 1);
    assert_eq!(partition.dedup.count(), 1);

    for request in [1, 2] {
        let retry = old
            .clone()
            .transmute_header(|mut header, next: &mut RoutedRequestHeader| {
                header.request = request;
                header.metadata_watermark = transition.metadata_op;
                *next = header;
            });
        let (sender, receiver) = consensus::oneshot_channel();
        partition.on_request(retry, Some(sender)).await;
        assert_eq!(
            receiver.await.unwrap().header().status,
            IggyError::HistoryUnavailable.as_code()
        );
        assert_eq!(partition.consensus.sequencer().current_sequence(), 2);
    }
    assert_eq!(partition.offset_frontier(), 1);
}

#[compio::test]
async fn given_stale_offset_repair_when_incarnation_changed_should_preserve_the_current_journal() {
    const INCARNATION: u64 = 7;
    let mut source = test_partition();
    source.set_created_revision(INCARNATION);
    journal_store_offset(&mut source, 2, 9, 42).await;
    let bytes = source.log.journal().inner.repair_entry(2).unwrap();
    let mut valid = Message::<PrepareHeader>::new(bytes.len());
    valid.as_mut_slice().copy_from_slice(bytes.as_slice());

    let mut partition = test_partition();
    partition.set_created_revision(INCARNATION);
    partition.consensus.restore_commit_state(1, 1);
    partition.consensus.sequencer().set_sequence(1);
    partition.repair = Some(armed_session(2, 1));
    for incarnation in [INCARNATION - 1, INCARNATION + 1] {
        let stale = valid
            .clone()
            .transmute_header(|mut original, header: &mut PrepareHeader| {
                original.partition_incarnation = incarnation;
                *header = original;
            });
        partition.apply_repaired_prepare(stale).await;
        assert!(partition.log.journal().inner.header_by_op(2).is_none());
        assert!(partition.pending_consumer_offset_commits.is_empty());
        assert_eq!(partition.consensus.sequencer().current_sequence(), 1);
    }

    partition.apply_repaired_prepare(valid).await;
    partition.consensus.advance_commit_max(2);
    partition.commit_journal(&repair_config()).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(partition.consensus.commit_min(), 2);
    assert_eq!(
        partition.get_consumer_offset(PollingConsumer::Consumer(9, 0)),
        Some(42)
    );
}

#[compio::test]
async fn given_invalid_repaired_send_when_ingesting_should_not_poison_the_journal() {
    const INCARNATION: u64 = 7;
    let mut partition = test_partition();
    partition.set_created_revision(INCARNATION);
    for (incarnation, corrupt) in [
        (INCARNATION - 1, false),
        (INCARNATION + 1, false),
        (INCARNATION, true),
    ] {
        let mut message = repaired_send_prepare(2, 0, 0).transmute_header(
            |mut original, header: &mut PrepareHeader| {
                original.partition_incarnation = incarnation;
                *header = original;
            },
        );
        if corrupt {
            let last = message.as_slice().len() - 1;
            message.as_mut_slice()[last] ^= 1;
        }
        assert!(
            partition
                .append_repaired_send_messages(message)
                .await
                .is_err()
        );
        assert!(partition.log.journal().inner.header_by_op(2).is_none());
        assert_eq!(partition.log.journal().info.messages_count, 0);
    }
    let valid = repaired_send_prepare(2, 0, 0).transmute_header(
        |mut original, header: &mut PrepareHeader| {
            original.partition_incarnation = INCARNATION;
            *header = original;
        },
    );
    assert!(partition.append_repaired_send_messages(valid).await.is_ok());
    assert!(partition.log.journal().inner.header_by_op(2).is_some());
}

#[compio::test]
async fn given_repair_window_with_delete_fence_when_completing_should_close_the_history() {
    const COMPLETION_STEPS_MAX: usize = 8;
    let prepares = fenced_history_prepares(1).await;
    let to_op = u64::try_from(prepares.len()).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let mut config = repair_config();
    config.path_layout.streams_root = directory.path().to_string_lossy().into_owned();
    let path = config.get_partition_path(1, 1, 0);
    std::fs::create_dir_all(&path).unwrap();
    let (mut replica, _) = recording_partition_at(1, 3);
    replica.set_partition_dir(path.clone());
    set_offset_dirs_under(&mut replica, std::path::Path::new(&path));
    replica.log.retire_front().unwrap();
    replica.install_empty_segment(&config, 0).await.unwrap();
    replica.open_persistence().await.unwrap();
    let persistence = Rc::clone(replica.persistence.as_ref().unwrap());
    replica.consensus().advance_commit_max(to_op);
    replica.repair = Some(armed_session(to_op, 0));
    for prepare in prepares {
        replica.apply_repaired_prepare(prepare).await;
    }

    let mut conclusion = RepairConclusion::InProgress;
    for _ in 0..COMPLETION_STEPS_MAX {
        conclusion = replica.complete_repair(&config).await;
        persistence
            .drain_with_timeout(compio::runtime::time::sleep)
            .await
            .unwrap();
        replica.drive_persistence().await;
        if conclusion == RepairConclusion::Done {
            break;
        }
    }
    assert!(
        persistence.failure().is_none(),
        "{:?}",
        persistence.failure()
    );
    assert!(replica.fatal().is_none(), "{:?}", replica.fatal());
    assert_eq!(conclusion, RepairConclusion::Done);
    assert_eq!(replica.consensus().commit_min(), to_op);
    assert!(replica.history_deleted());
    assert_eq!(replica.offset_frontier(), 1);
}

/// The prepares a primary journals for `sends_before` sends and the delete
/// fence that closes their history.
async fn fenced_history_prepares(sends_before: u64) -> Vec<Message<PrepareHeader>> {
    const METADATA_OP: u64 = 10;
    let config = repair_config();
    let directory = tempfile::tempdir().unwrap();
    let (mut primary, _) = recording_partition_at(0, 3);
    primary.runtime_options.durability = iggy_common::Durability::Replicated;
    primary.set_partition_dir(directory.path().to_string_lossy().into_owned());
    for request in 1..=sends_before {
        let send = history_send_request(&primary, request, METADATA_OP);
        primary.on_request(send, None).await;
    }
    primary.consensus.advance_commit_max(sends_before);
    primary.commit_journal(&config).await;
    let transition = TransitionPartitionHistoryRequest {
        incarnation: primary.created_revision(),
        metadata_op: METADATA_OP,
    };
    let fence = partition_control_request(
        &primary,
        Operation::TransitionPartitionHistory,
        &transition.to_bytes(),
        METADATA_OP,
    );
    primary.on_request(fence, None).await;
    primary.consensus.advance_commit_max(sends_before + 1);
    primary.commit_journal(&config).await;
    assert!(primary.fatal().is_none(), "{:?}", primary.fatal());
    assert!(primary.history_deleted());
    let head = primary.consensus.sequencer().current_sequence();
    assert_eq!(head, sends_before + 1);
    (1..=head)
        .map(|op| {
            let journaled = primary.log.journal().inner.repair_entry(op).unwrap();
            Message::<PrepareHeader>::try_from(Owned::copy_from_slice(journaled.as_slice()))
                .unwrap()
        })
        .collect()
}
