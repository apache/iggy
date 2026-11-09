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

/// A backup whose prepare WAL holds `1..=wal_head`, durable and committed,
/// with nothing resident above it: where a rejoining backup stands after
/// its replayed history was walked and flushed, or after an install.
async fn backup_with_durable_wal(
    directory: &std::path::Path,
    wal_head: u64,
) -> IggyPartition<RecordingBus> {
    let (mut backup, _) = recording_partition_at(1, 3);
    backup.set_partition_dir(directory.to_string_lossy().into_owned());
    backup.open_persistence().await.unwrap();
    let persistence = Rc::clone(backup.persistence.as_ref().unwrap());
    let mut parent = 0;
    for op in 1..=wal_head {
        let prepare = checksummed_segment_prepare(op, parent, op - 1, b"durable");
        parent = prepare.header().checksum;
        persistence.append(prepare.into_frozen(), true).unwrap();
    }
    assert!(persistence.start());
    Rc::clone(&persistence).run().await;
    assert!(persistence.is_durable_through(wal_head));
    backup.consensus().restore_commit_state(wal_head, wal_head);
    backup.consensus().sequencer().set_sequence(wal_head);
    backup.recovered_durable_offset = Some(wal_head - 1);
    backup
}

#[compio::test]
async fn given_durable_unproven_history_when_resuming_io_should_not_select_or_apply_it() {
    const HEAD: u64 = 3;
    let directory = tempfile::tempdir().unwrap();
    let config = repair_config();
    let (mut backup, _) = recording_partition_at(1, 3);
    backup.set_partition_dir(directory.path().to_string_lossy().into_owned());
    backup.open_persistence().await.unwrap();
    let persistence = Rc::clone(backup.persistence.as_ref().unwrap());
    let mut parent = 0;
    let mut headers = Vec::new();
    for op in 1..=HEAD {
        let prepare = checksummed_segment_prepare(op, parent, op - 1, b"old-view");
        parent = prepare.header().checksum;
        headers.push(*prepare.header());
        backup
            .log
            .journal()
            .inner
            .append(prepare.clone().into_frozen())
            .await
            .unwrap();
        persistence.append(prepare.into_frozen(), true).unwrap();
    }
    assert!(persistence.start());
    Rc::clone(&persistence).run().await;
    assert!(persistence.is_durable_through(HEAD));
    backup.consensus().advance_commit_max(HEAD);
    backup.consensus().fence_view_log(HEAD + 1, HEAD + 1);

    assert!(!backup.select_commit(headers.clone(), &config, false));
    assert_eq!(
        backup.collect_committable_from_journal(COMMIT_WALK_OPS_MAX, &config),
        []
    );
    for _ in 0..HEAD {
        backup.resume_io(&config).await;
        assert_eq!(backup.consensus().commit_min(), 0);
        assert!(!backup.has_pending_commit());
    }

    backup.consensus().settle_view_log_proof();
    assert_eq!(
        backup.collect_committable_from_journal(COMMIT_WALK_OPS_MAX, &config),
        headers
    );
    backup.commit_journal(&config).await;
    assert_eq!(backup.consensus().commit_min(), HEAD);
}

/// The repair walk of an unproven log runs from the adopted head down and
/// commits nothing before it reaches `commit_min`. A walk within the WAL
/// byte budget keeps repairing. Once it holds more, it journals nothing
/// more and hands the window to state transfer.
#[compio::test]
async fn given_an_unproven_walk_past_the_wal_byte_budget_when_completing_should_require_state_transfer()
 {
    const PAYLOAD_BYTES: usize = 4 * 1024 * 1024;
    let budget = journal::partition_journal::PARTITION_WAL_CAPACITY_MIN;
    let payload = vec![7_u8; PAYLOAD_BYTES];
    let prepare_bytes = u64::try_from(
        checksummed_segment_prepare(1, 0, 0, &payload)
            .as_slice()
            .len(),
    )
    .unwrap();
    let journaled = budget / prepare_bytes + 1;
    let head = journaled + 2;
    let parents = segment_chain_parents(head, &payload);
    let prepare = |op: u64| {
        let parent = parents[usize::try_from(op - 1).unwrap()];
        checksummed_segment_prepare(op, parent, op - 1, &payload)
    };
    let root = tempfile::tempdir().unwrap();
    let (mut backup, _) = recording_partition_at(1, 3);
    backup.set_partition_dir(root.path().to_string_lossy().into_owned());
    backup
        .open_persistence_with_capacity(budget, std::time::Duration::ZERO)
        .await
        .unwrap();
    backup.consensus().advance_commit_max(head);
    backup.consensus().sequencer().set_sequence(head);
    backup.consensus().fence_view_log(head, head);
    backup.repair = Some(floorless_session(head, head));

    backup.apply_repaired_prepare(prepare(head)).await;
    assert_eq!(
        backup.complete_repair(&repair_config()).await,
        RepairConclusion::InProgress,
        "a walk within the budget keeps repairing"
    );
    for op in (1..head).rev() {
        backup.apply_repaired_prepare(prepare(op)).await;
    }

    let journal = &backup.log.journal().inner;
    let lowest_journaled = head - journaled + 1;
    assert!(
        journal.header_by_op(lowest_journaled).is_some(),
        "the body that crosses the budget is journaled"
    );
    assert!(
        journal.header_by_op(lowest_journaled - 1).is_none(),
        "the walk journals nothing past the budget"
    );
    assert_eq!(
        backup.complete_repair(&repair_config()).await,
        RepairConclusion::FloorRefused {
            floor: 0,
            to_op: head
        }
    );
    assert_eq!(backup.consensus().commit_min(), 0);
    assert!(backup.repair.is_none());
}

#[compio::test]
async fn given_walks_sharing_a_node_budget_when_it_is_full_should_transfer_and_release_discarded_bodies()
 {
    const HEAD: u64 = 3;
    const PAYLOAD: &[u8] = b"repair body";
    let parents = segment_chain_parents(HEAD, PAYLOAD);
    let prepare = |op: u64| {
        checksummed_segment_prepare(
            op,
            parents[usize::try_from(op - 1).unwrap()],
            op - 1,
            PAYLOAD,
        )
    };
    let capacity = prepare(HEAD).retained_allocation_size().unwrap();
    let budget = Arc::new(crate::RepairMemoryBudget::new(capacity));
    let backup = || {
        let (mut partition, _) = recording_partition_at(1, 3);
        partition.proof_walk_memory = Some(budget.reservation());
        partition.consensus().advance_commit_max(HEAD);
        partition.consensus().sequencer().set_sequence(HEAD);
        partition.consensus().fence_view_log(HEAD, HEAD);
        partition.repair = Some(floorless_session(HEAD, HEAD));
        partition
    };
    let mut first = backup();
    let mut second = backup();
    first.apply_repaired_prepare(prepare(HEAD)).await;
    assert!(first.log.journal().inner.header_by_op(HEAD).is_some());
    assert!(
        !budget.reservation().try_add(1),
        "the first walk owns the available allocation"
    );

    second.apply_repaired_prepare(prepare(HEAD)).await;
    assert!(
        second.log.journal().inner.is_empty(),
        "an over-budget body must not be journaled"
    );
    assert_eq!(
        second.complete_repair(&repair_config()).await,
        RepairConclusion::FloorRefused {
            floor: 0,
            to_op: HEAD
        },
        "memory pressure must hand this walk to state transfer"
    );

    first.truncate_uncommitted_from(1).await.unwrap();
    assert!(
        budget.reservation().try_add(capacity),
        "discarding the window releases its reservation"
    );

    let mut third = backup();
    third.apply_repaired_prepare(prepare(HEAD)).await;
    assert!(third.log.journal().inner.header_by_op(HEAD).is_some());
    drop(third);
    assert!(
        budget.reservation().try_add(capacity),
        "dropping a partition releases its reservation"
    );
}

/// The primary of a new view acts on its machines only once it applied
/// every op of the log the view started with, here three commands. The
/// view starts with them uncommitted in its rebuilt pipeline, so nothing
/// else holds a new command back: no committed op is left unapplied.
#[compio::test]
async fn given_a_primary_behind_its_view_start_when_a_command_arrives_should_refuse_it_until_applied()
 {
    const VIEW: u32 = 4;
    const HEAD: u64 = 3;
    let (partition, _) = recording_partition_at(1, 3);
    let mut partition = Box::new(partition);
    partition
        .register_state_machine(Box::new(
            crate::state_machine::registry::tests::CounterMachine::new(1, &[COMMAND_KIND]),
        ))
        .unwrap();
    let mut parent = 0;
    let mut headers = Vec::new();
    for op in 1..=HEAD {
        // A view-change suffix runs down the log with falling timestamps.
        let prepare = command_prepare(op, parent, COMMAND_KIND, op).transmute_header(
            |mut header, next: &mut PrepareHeader| {
                header.timestamp = op;
                header.checksum = header.identity_checksum();
                *next = header;
            },
        );
        parent = prepare.header().checksum;
        headers.push(*prepare.header());
        Box::pin(partition.on_replicate(prepare)).await;
    }
    let body: Vec<u8> = headers
        .iter()
        .rev()
        .flat_map(|header| bytemuck::bytes_of(header).to_vec())
        .collect();
    let size = size_of::<iggy_binary_protocol::DoViewChangeHeader>() + body.len();
    let mut dvc = Message::<iggy_binary_protocol::GenericHeader>::new(size);
    dvc.as_mut_slice()[size_of::<iggy_binary_protocol::DoViewChangeHeader>()..]
        .copy_from_slice(&body);
    let dvc = dvc.transmute_header(|_, header: &mut iggy_binary_protocol::DoViewChangeHeader| {
        header.command = Command::DoViewChange;
        header.size = u32::try_from(size).unwrap();
        header.group = partition.consensus.group();
        header.cluster = TEST_CLUSTER;
        header.view = VIEW;
        header.log_view = 0;
        header.replica = 0;
        header.op = HEAD;
        header.commit = 0;
        header.present_bitset = 0b111;
    });
    partition.consensus.handle_do_view_change(
        consensus::PlaneKind::Partitions,
        dvc.header(),
        &dvc.as_slice()[size_of::<iggy_binary_protocol::DoViewChangeHeader>()..],
    );
    assert!(partition.consensus.is_primary_elect());
    // Stands in for the shard's proof walk and pipeline rebuild.
    partition.consensus.settle_view_log_proof();
    let _ = partition
        .consensus
        .start_pending_view(consensus::PlaneKind::Partitions);
    partition.consensus.with_pipeline_mut(|pipeline| {
        for header in &headers {
            pipeline.push(consensus::PipelineEntry::new(*header));
        }
    });
    assert!(partition.consensus.is_primary() && partition.consensus.is_normal());
    assert_eq!(partition.consensus.commit_min(), 0);
    assert_eq!(partition.consensus.commit_max(), 0);
    assert!(!partition.state_machines_current());

    let command = |partition: &IggyPartition<RecordingBus>, request| {
        partition
            .command_request(
                crate::state_machine::CommandKind(COMMAND_KIND),
                &1u64.to_le_bytes(),
                request,
            )
            .unwrap()
    };
    let (sender, refusal) = consensus::oneshot_channel();
    Box::pin(partition.on_request(command(&partition, 1), Some(sender))).await;
    // Checked before the await: an admitted command gets no reply here.
    assert_eq!(partition.consensus.sequencer().current_sequence(), HEAD);
    assert_eq!(
        refusal.await.unwrap().header().status,
        IggyError::TransientNotAccepted.as_code()
    );

    partition.consensus.advance_commit_max(HEAD);
    Box::pin(partition.commit_journal(&repair_config())).await;
    assert_eq!(partition.consensus.commit_min(), HEAD);
    assert_eq!(partition.state_machines().applied_op(), HEAD);
    assert!(partition.state_machines_current());
    Box::pin(partition.on_request(command(&partition, 2), None)).await;
    assert_eq!(partition.consensus.sequencer().current_sequence(), HEAD + 1);
}

#[compio::test]
async fn given_a_primary_elect_when_repair_memory_is_exhausted_should_request_transfer() {
    const VIEW: u32 = 4;
    const HEAD: u64 = 3;
    let (mut partition, _) = recording_partition_at(1, 3);
    let prepare = checksummed_segment_prepare(HEAD, 0, HEAD - 1, b"repair");
    let body = &prepare.as_slice()[..size_of::<PrepareHeader>()];
    let size = size_of::<iggy_binary_protocol::DoViewChangeHeader>() + body.len();
    let mut dvc = Message::<iggy_binary_protocol::GenericHeader>::new(size);
    dvc.as_mut_slice()[size_of::<iggy_binary_protocol::DoViewChangeHeader>()..]
        .copy_from_slice(body);
    let dvc = dvc.transmute_header(|_, header: &mut iggy_binary_protocol::DoViewChangeHeader| {
        header.command = Command::DoViewChange;
        header.size = u32::try_from(size).unwrap();
        header.group = partition.consensus.group();
        header.cluster = TEST_CLUSTER;
        header.view = VIEW;
        header.log_view = 1;
        header.replica = 0;
        header.op = HEAD;
        header.commit = HEAD;
        header.present_bitset = 1;
    });
    partition
        .consensus
        .handle_do_view_change(consensus::PlaneKind::Partitions, dvc.header(), body);
    assert!(partition.consensus.is_primary_elect());
    partition.consensus.fence_view_log(HEAD, HEAD);
    partition.repair = Some(RepairSession {
        view: VIEW,
        ..floorless_session(HEAD, HEAD)
    });
    let budget = Arc::new(crate::RepairMemoryBudget::new(1));
    partition.proof_walk_memory = Some(budget.reservation());
    partition.apply_repaired_prepare(prepare).await;
    assert!(partition.log.journal().inner.is_empty());
    assert_eq!(
        partition.complete_repair(&repair_config()).await,
        RepairConclusion::FloorRefused {
            floor: 0,
            to_op: HEAD
        },
        "primary election must not discard the memory-pressure signal"
    );
}

/// The serving peer evicted every op below `RETAINED_FROM` and retains more
/// than one repair chunk above it. Every request that starts below
/// `RETAINED_FROM` gets `RangeEvicted` and the same first chunk, so the
/// refused window can never complete. With a prepare WAL attached, a floor
/// above the WAL head can never be accepted, so the first refusal is final.
#[compio::test]
async fn given_floor_above_wal_head_when_refused_window_exceeds_one_chunk_should_refuse_commit_floor()
 {
    // The default `[cluster] repair_chunk_max`, `shard::REPAIR_CHUNK_MAX`.
    const CHUNK_OPS: u64 = 128;
    const WAL_HEAD: u64 = 2;
    const RETAINED_FROM: u64 = 11;
    // One op more than the chunk served from `RETAINED_FROM` covers.
    const COMMIT_TO_OP: u64 = RETAINED_FROM + CHUNK_OPS;
    // One round past the stall budget, so a peer rotation would be due.
    const ROUNDS: u32 = crate::types::REPAIR_MAX_STALL_RETRIES + 1;
    let directory = tempfile::tempdir().unwrap();
    let mut backup = backup_with_durable_wal(directory.path(), WAL_HEAD).await;
    backup.consensus().advance_commit_max(COMMIT_TO_OP);
    backup.repair = Some(RepairSession {
        floor: None,
        ..armed_session(COMMIT_TO_OP, 0)
    });
    let config = repair_config();

    let mut request = WAL_HEAD + 1..=COMMIT_TO_OP;
    let mut rounds = Vec::new();
    for _ in 0..ROUNDS {
        // The serve path clamps the request to its retained range, reports
        // that with `RangeEvicted`, and serves at most one chunk.
        let from_op = (*request.start()).max(RETAINED_FROM);
        if from_op > *request.start() {
            backup.repair.as_mut().unwrap().floor = Some(RETAINED_FROM - 1);
        }
        for op in from_op..=(*request.end()).min(from_op + CHUNK_OPS - 1) {
            backup
                .apply_repaired_prepare(checksummed_segment_prepare(op, 0, op - 1, b"repaired"))
                .await;
        }
        let conclusion = backup.complete_repair(&config).await;
        if conclusion != RepairConclusion::InProgress {
            rounds.push((request, conclusion, false));
            break;
        }
        // The stall retry burns one attempt, then re-requests from the live
        // commit point, where the pull after `RepairDone` starts too.
        let rotate = backup.burn_repair_attempt();
        rounds.push((request, conclusion, rotate));
        if rotate {
            break;
        }
        request = backup.consensus().commit_min() + 1..=backup.repair.unwrap().fetch_to_op;
    }

    assert_eq!(
        rounds.last().map(|(_, conclusion, _)| *conclusion),
        Some(RepairConclusion::FloorRefused {
            floor: RETAINED_FROM - 1,
            to_op: COMMIT_TO_OP,
        }),
        "a floor above the WAL head can never be accepted, so its refusal is final; \
             rounds as (request, conclusion, rotate): {rounds:?}"
    );
    assert!(backup.repair.is_none());
}

/// Can a WAL-backed partition accept a floor above its WAL head? This window
/// connects: ops 3 to 10 carried no messages, so its first batch starts right
/// after the recovered durable end. Its last frame is missing, so a refusal
/// cannot come from the window-completeness check of the refused path.
#[compio::test]
async fn given_wal_attached_when_connected_floor_above_wal_head_should_refuse_commit_floor() {
    const WAL_HEAD: u64 = 2;
    const FLOOR: u64 = 10;
    const SERVED_THROUGH: u64 = 12;
    const COMMIT_TO_OP: u64 = SERVED_THROUGH + 1;
    let directory = tempfile::tempdir().unwrap();
    let mut backup = backup_with_durable_wal(directory.path(), WAL_HEAD).await;
    backup.consensus().advance_commit_max(COMMIT_TO_OP);
    backup.repair = Some(armed_session(COMMIT_TO_OP, FLOOR));
    for (offset, op) in (WAL_HEAD..).zip(FLOOR + 1..=SERVED_THROUGH) {
        backup
            .apply_repaired_prepare(checksummed_segment_prepare(op, 0, offset, b"repaired"))
            .await;
    }

    let conclusion = backup.complete_repair(&repair_config()).await;

    assert_eq!(
        conclusion,
        RepairConclusion::FloorRefused {
            floor: FLOOR,
            to_op: COMMIT_TO_OP,
        },
        "the WAL cannot become durable through ops the peer no longer retains"
    );
    assert!(backup.repair.is_none());
    assert_eq!(
        backup.consensus().commit_min(),
        WAL_HEAD,
        "a refused floor must not lift commit_min over ops the WAL never persisted"
    );
}

/// The other half of the question: boot replay leaves every op in
/// `(commit_min, head]` resident, so a floor at or below the WAL head is
/// dropped before the connection check, and the replayed ops are walked.
/// This window can never connect (no repaired batch, message ops in it), so
/// a floor that reached the check would be refused instead.
#[compio::test]
async fn given_boot_replayed_wal_when_floor_at_or_below_wal_head_should_walk_resident_ops() {
    const WAL_HEAD: u64 = 4;
    const RECOVERED_COMMIT: u64 = 2;
    let config = repair_config();
    for floor in RECOVERED_COMMIT + 1..=WAL_HEAD {
        let directory = tempfile::tempdir().unwrap();
        let (mut backup, _) = recording_partition_at(1, 3);
        backup.set_partition_dir(directory.path().to_string_lossy().into_owned());
        backup.log.retire_front().unwrap();
        backup.install_empty_segment(&config, 0).await.unwrap();
        let mut wal = journal::PartitionPrepareJournal::open(
            &directory.path().join("prepares-0"),
            backup.consensus().group(),
            0,
        )
        .await
        .unwrap();
        let mut parent = 0;
        for op in 1..=WAL_HEAD {
            // A prepare carries the primary's commit point at send time.
            let prepare = checksummed_segment_prepare(op, parent, op - 1, b"replayed")
                .transmute_header(|original, header: &mut PrepareHeader| {
                    *header = original;
                    header.commit = RECOVERED_COMMIT.min(op - 1);
                    header.checksum = header.identity_checksum();
                });
            parent = prepare.header().checksum;
            wal.append(prepare.into_frozen()).await.unwrap();
        }
        drop(wal);
        backup.open_persistence().await.unwrap();
        backup.commit_journal(&config).await;
        assert_eq!(backup.consensus().commit_min(), RECOVERED_COMMIT);
        assert!(
            backup
                .log
                .journal()
                .inner
                .repaired_window_shape(RECOVERED_COMMIT, WAL_HEAD)
                .complete,
            "boot replay must leave (commit_min, head] resident"
        );
        backup.consensus().advance_commit_max(WAL_HEAD);
        backup.repair = Some(armed_session(WAL_HEAD, floor));

        let conclusion = backup.complete_repair(&config).await;

        assert_eq!(
            conclusion,
            RepairConclusion::Done,
            "floor {floor} at or below the WAL head must be dropped, not refused"
        );
        assert_eq!(backup.consensus().commit_min(), WAL_HEAD);
    }
}

/// The window above the floor is complete and carries no messages, so no
/// batch anchors the floor. The jump would skip ops 1 to 5, which this
/// journal never held, and nothing shows that they carried no messages,
/// whether or not this replica committed messages before.
#[compio::test]
async fn given_no_batch_above_floor_when_window_carries_no_messages_should_refuse_commit_floor() {
    const COMMITTED_OFFSET: u64 = 99;
    for committed_messages in [false, true] {
        let mut partition = test_partition();
        if committed_messages {
            partition.set_offset_space_used(true);
            partition.offset.store(COMMITTED_OFFSET, Ordering::Release);
        }
        partition.consensus().advance_commit_max(8);
        // Stands in for any op without messages, such as a consumer
        // offset store.
        for op in 6..=8 {
            journal_prepare(&partition, op, Operation::CreateStream).await;
        }
        partition.repair = Some(armed_session(8, 5));

        let conclusion = partition.complete_repair(&repair_config()).await;

        assert_eq!(
            (conclusion, partition.consensus().commit_min()),
            (RepairConclusion::FloorRefused { floor: 5, to_op: 8 }, 0),
            "committed messages before the window: {committed_messages}"
        );
        assert!(
            partition.repair.is_none(),
            "a definitive refusal hands recovery to state transfer"
        );
    }
}

/// The committed window above the floor carries no messages, and the only
/// batch above the floor belongs to the uncommitted suffix. A view change
/// can still replace that op, so its batch must not anchor the floor, even
/// at offset 0, which would connect it here.
#[compio::test]
async fn given_suffix_batch_above_commit_to_op_when_committed_window_carries_no_messages_should_refuse_commit_floor()
 {
    const FLOOR: u64 = 5;
    const COMMIT_TO_OP: u64 = 8;
    const SUFFIX_OP: u64 = COMMIT_TO_OP + 1;
    let mut partition = test_partition();
    partition.consensus().advance_commit_max(COMMIT_TO_OP);
    for op in FLOOR + 1..=COMMIT_TO_OP {
        journal_prepare(&partition, op, Operation::CreateStream).await;
    }
    partition
        .log
        .journal()
        .inner
        .append(checksummed_segment_prepare(SUFFIX_OP, 0, 0, b"suffix").into_frozen())
        .await
        .expect("journal append");
    partition.repair = Some(armed_fetch_session(COMMIT_TO_OP, SUFFIX_OP, FLOOR));

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(
        (conclusion, partition.consensus().commit_min()),
        (
            RepairConclusion::FloorRefused {
                floor: FLOOR,
                to_op: COMMIT_TO_OP
            },
            0
        ),
        "the batch at op {SUFFIX_OP} must not anchor the floor"
    );
}

#[compio::test]
async fn given_no_repaired_batch_when_window_holds_message_op_should_refuse_commit_floor() {
    let mut partition = test_partition();
    partition.consensus().advance_commit_max(8);
    journal_prepare(&partition, 6, Operation::SendMessages).await;
    for op in 7..=8 {
        journal_prepare(&partition, op, Operation::CreateStream).await;
    }
    partition.repair = Some(armed_session(8, 5));

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(
        conclusion,
        RepairConclusion::FloorRefused { floor: 5, to_op: 8 },
        "a complete window with an unanchored message op can never connect"
    );
    assert_eq!(partition.consensus().commit_min(), 0);
    assert!(
        partition.repair.is_none(),
        "a definitive refusal hands recovery to state transfer"
    );
}

#[compio::test]
async fn given_no_repaired_batch_when_window_fully_evicted_should_refuse_commit_floor() {
    let mut partition = test_partition();
    partition.consensus().advance_commit_max(8);
    partition.repair = Some(armed_session(8, 8));

    let conclusion = partition.complete_repair(&repair_config()).await;

    // Everything the peer retained was evicted: a retry re-raises the
    // identical empty window every round (the wedge state transfer
    // exists to break), so this refusal is definitive.
    assert_eq!(
        conclusion,
        RepairConclusion::FloorRefused { floor: 8, to_op: 8 }
    );
    assert_eq!(partition.consensus().commit_min(), 0);
    assert!(partition.repair.is_none());
}

#[compio::test]
async fn given_empty_committed_window_with_a_suffix_fetch_should_escape_to_state_transfer() {
    let mut partition = test_partition();
    partition.consensus().advance_commit_max(5);
    partition.repair = Some(armed_fetch_session(5, 9, 5));

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(
        conclusion,
        RepairConclusion::FloorRefused { floor: 5, to_op: 5 },
        "the uncommitted fetch ceiling must not postpone a definitive committed-floor refusal"
    );
    assert!(partition.repair.is_none());
}

#[compio::test]
async fn given_committed_suffix_evicted_when_completing_repair_should_close_the_session() {
    // A successful suffix repair is what lets the group commit past
    // `commit_to_op`, and the commit walk's flush then evicts exactly the
    // suffix headers. The completion verdict must survive that eviction:
    // judged from resident headers alone, the fully successful session
    // would report itself incomplete forever, stay armed, and block every
    // later re-arm for this partition until a view change.
    let mut partition = test_partition();
    for op in 1..=3 {
        journal_send_batch(&mut partition, op).await;
    }
    partition.consensus().advance_commit_max(3);
    partition.repair = Some(armed_fetch_session(2, 3, 0));
    partition.commit_journal(&repair_config()).await;
    partition.log.journal().inner.evict_prefix(3);

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(conclusion, RepairConclusion::Done);
    assert!(
        partition.repair.is_none(),
        "a fully committed suffix fetch must not stay armed after its \
             headers are flushed out of the resident journal"
    );
}

#[compio::test]
async fn given_suffix_fetch_when_its_view_is_discarded_should_clear_the_session() {
    let mut partition = test_partition();
    partition.repair = Some(armed_fetch_session(0, 3, 0));
    partition.consensus.set_view(1);

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(conclusion, RepairConclusion::Done);
    assert!(
        partition.repair.is_none(),
        "a discarded view must not leave its suffix fetch blocking future repair"
    );
}

#[compio::test]
async fn given_repaired_batch_above_durable_end_when_floor_arrives_should_refuse_commit_floor() {
    let mut partition = test_partition();
    partition.consensus().advance_commit_max(8);
    // No recovered segments (durable end None) and the served window's
    // first batch starts at offset 8: ops below the floor are neither
    // locally durable nor repaired.
    partition.repair = Some(armed_session(8, 5));
    partition
        .apply_repaired_prepare(repaired_send_prepare(8, 0, 0x88))
        .await;

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(
        conclusion,
        RepairConclusion::InProgress,
        "with the window incomplete, later frames can still land below \
             the anchor and connect"
    );
    assert_eq!(partition.consensus().commit_min(), 0);
    assert!(partition.repair.is_some());
}

/// The repair window `(floor, to_op]` is longer than one repair chunk and
/// only its first chunk arrived. The anchor (op 7, offset 7) starts above
/// the recovered durable end (offset 3) and every op between the floor and
/// it is resident, so the refusal is final and must hand recovery to state
/// transfer.
#[compio::test]
async fn given_a_resident_leading_edge_above_durable_end_when_the_window_is_longer_than_a_chunk_should_refuse_commit_floor()
 {
    let mut partition = test_partition();
    partition.consensus().advance_commit_max(300);
    partition.recovered_durable_offset = Some(3);
    partition.repair = Some(armed_session(300, 5));
    journal_prepare(&partition, 6, Operation::CreateStream).await;
    partition
        .apply_repaired_prepare(repaired_send_prepare(7, 0, 0x77))
        .await;
    for op in 8..=133 {
        journal_prepare(&partition, op, Operation::CreateStream).await;
    }

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(
        conclusion,
        RepairConclusion::FloorRefused {
            floor: 5,
            to_op: 300
        },
        "a resident leading edge fixes where the window starts, so the \
             refusal is final"
    );
    assert!(
        partition.repair.is_none(),
        "a definitive refusal hands recovery to state transfer"
    );
}

/// A stall rotation drops the session that repaired ops 6 and 7, and the
/// next session learns the floor. The batches are resident whichever
/// session journaled them, so they anchor the check.
#[compio::test]
async fn given_a_connecting_batch_from_an_earlier_session_when_the_floor_arrives_should_accept_it()
{
    let mut partition = test_partition();
    partition.consensus().advance_commit_max(300);
    partition.recovered_durable_offset = Some(3);
    partition.repair = Some(floorless_session(300, 300));
    partition
        .apply_repaired_prepare(repaired_send_prepare_at(6, 4, 0, 0x66))
        .await;
    partition
        .apply_repaired_prepare(repaired_send_prepare_at(7, 5, 0x66, 0x77))
        .await;
    partition.repair = None;
    partition.repair = Some(armed_session(300, 5));

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(conclusion, RepairConclusion::InProgress);
    assert_eq!(
        partition.consensus().commit_min(),
        7,
        "the window connects at offset 4, so the floor stands and the walk \
             commits the resident batches above it"
    );
    assert!(partition.repair.is_some());
}

/// The same connecting window as above, on a partition with a state
/// machine: the jump would skip the commands committed below the floor
/// without applying them, so the window goes to state transfer, which
/// carries the machines' snapshot.
#[compio::test]
async fn given_state_machines_when_a_connecting_floor_arrives_should_refuse_the_jump() {
    let mut partition = test_partition();
    partition
        .register_state_machine(Box::new(
            crate::state_machine::registry::tests::CounterMachine::new(1, &[COMMAND_KIND]),
        ))
        .unwrap();
    partition.consensus().advance_commit_max(300);
    partition.recovered_durable_offset = Some(3);
    partition.repair = Some(floorless_session(300, 300));
    partition
        .apply_repaired_prepare(repaired_send_prepare_at(6, 4, 0, 0x66))
        .await;
    partition
        .apply_repaired_prepare(repaired_send_prepare_at(7, 5, 0x66, 0x77))
        .await;
    partition.repair = None;
    partition.repair = Some(armed_session(300, 5));

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(
        conclusion,
        RepairConclusion::FloorRefused {
            floor: 5,
            to_op: 300
        }
    );
    assert_eq!(partition.consensus().commit_min(), 0);
    assert!(partition.repair.is_none());
}

/// The same shape as above with batches that start past the durable end:
/// resident from an earlier session or not, nothing below them is
/// missing, so the refusal is final.
#[compio::test]
async fn given_a_disconnected_batch_from_an_earlier_session_when_the_floor_arrives_should_refuse_commit_floor()
 {
    let mut partition = test_partition();
    partition.consensus().advance_commit_max(300);
    partition.recovered_durable_offset = Some(3);
    partition.repair = Some(floorless_session(300, 300));
    partition
        .apply_repaired_prepare(repaired_send_prepare(6, 0, 0x66))
        .await;
    partition
        .apply_repaired_prepare(repaired_send_prepare(7, 0x66, 0x77))
        .await;
    partition.repair = None;
    partition.repair = Some(armed_session(300, 5));

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(
        conclusion,
        RepairConclusion::FloorRefused {
            floor: 5,
            to_op: 300
        }
    );
    assert_eq!(partition.consensus().commit_min(), 0);
    assert!(partition.repair.is_none());
}

/// The anchor sits directly above the floor, so the leading edge is empty
/// and the refusal is final at once.
#[compio::test]
async fn given_the_anchor_directly_above_the_floor_when_it_does_not_connect_should_refuse_commit_floor()
 {
    let mut partition = test_partition();
    partition.consensus().advance_commit_max(300);
    partition.recovered_durable_offset = Some(3);
    partition.repair = Some(armed_session(300, 5));
    partition
        .apply_repaired_prepare(repaired_send_prepare(6, 0, 0x66))
        .await;

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(
        conclusion,
        RepairConclusion::FloorRefused {
            floor: 5,
            to_op: 300
        }
    );
    assert!(partition.repair.is_none());
}

/// A resident batch whose stamped `base_offset` no longer matches its batch
/// checksum says nothing about where the window begins, so it must not
/// anchor the floor however well the forged offset would connect.
#[compio::test]
async fn given_a_resident_batch_with_a_corrupt_base_offset_when_anchoring_should_yield_no_anchor() {
    const FLOOR: u64 = 5;
    const OP: u64 = FLOOR + 1;
    let partition = test_partition();
    let mut prepare = repaired_send_prepare_at(OP, OP, 0, 0x66);
    let base_offset_at = std::mem::size_of::<PrepareHeader>() + 8;
    prepare.as_mut_slice()[base_offset_at..base_offset_at + 8].copy_from_slice(&0u64.to_le_bytes());
    partition
        .log
        .journal()
        .inner
        .append(prepare.into_frozen())
        .await
        .expect("journal append");

    assert_eq!(partition.repair_window_anchor(FLOOR, 300), None);
}

#[compio::test]
async fn given_a_resident_batch_with_an_intact_checksum_when_anchoring_should_read_its_base_offset()
{
    const FLOOR: u64 = 5;
    const OP: u64 = FLOOR + 1;
    let partition = test_partition();
    partition
        .log
        .journal()
        .inner
        .append(repaired_send_prepare_at(OP, OP, 0, 0x66).into_frozen())
        .await
        .expect("journal append");

    assert_eq!(
        partition.repair_window_anchor(FLOOR, 300),
        Some(RepairAnchor {
            op: OP,
            base_offset: OP
        })
    );
}

/// An op below the anchor is still missing, so a frame for it may yet
/// connect the window.
#[compio::test]
async fn given_a_hole_below_the_anchor_when_the_window_is_incomplete_should_keep_repairing() {
    let mut partition = test_partition();
    partition.consensus().advance_commit_max(300);
    partition.recovered_durable_offset = Some(3);
    partition.repair = Some(armed_session(300, 5));
    journal_prepare(&partition, 6, Operation::CreateStream).await;
    partition
        .apply_repaired_prepare(repaired_send_prepare(8, 0, 0x88))
        .await;

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(conclusion, RepairConclusion::InProgress);
    assert!(partition.repair.is_some());
}

/// Round one repaired op 4 and lost op 5. Before the stall retry the peer
/// evicted through op 5, so the floor lands at 5 and the retry serves only
/// ops 6 to 10. Op 4 connects to the durable end, but the floor claims
/// recovered state stands in for op 5, which this replica holds nowhere.
/// Accepting that floor commits and serves a log missing offset 5.
#[compio::test]
async fn given_a_connecting_batch_below_a_raised_floor_when_the_floor_arrives_should_refuse_it() {
    let mut partition = test_partition();
    partition.consensus().advance_commit_max(10);
    partition.recovered_durable_offset = Some(3);
    partition.repair = Some(armed_session(10, 5));
    partition
        .apply_repaired_prepare(repaired_send_prepare(4, 0, 0x44))
        .await;
    for op in 6..=10u64 {
        partition
            .apply_repaired_prepare(repaired_send_prepare(op, 0, 0x100 + u128::from(op)))
            .await;
    }

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(
        conclusion,
        RepairConclusion::FloorRefused {
            floor: 5,
            to_op: 10
        },
        "a batch at or below the floor must not anchor the connect check"
    );
    assert!(
        partition.consensus().commit_min() < 5,
        "the floor must not skip op 5"
    );
}

/// The first stream repaired op 4 and the walk committed through it. The
/// stall retry then learns a floor at 6, above the still-missing op 5.
/// The committed op 4 must not stand in for the window above the floor.
#[compio::test]
async fn given_a_committed_batch_below_the_floor_when_the_floor_arrives_should_keep_repairing() {
    let mut partition = test_partition();
    partition.consensus().advance_commit_max(300);
    partition.recovered_durable_offset = Some(3);
    partition.repair = Some(armed_session(300, 6));
    for op in 1..=3 {
        journal_prepare(&partition, op, Operation::CreateStream).await;
    }
    partition
        .apply_repaired_prepare(repaired_send_prepare(4, 0, 0x44))
        .await;
    partition.commit_journal(&repair_config()).await;
    assert_eq!(partition.consensus().commit_min(), 4);

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(conclusion, RepairConclusion::InProgress);
    assert_eq!(
        partition.consensus().commit_min(),
        4,
        "the floor must not skip op 5"
    );
    assert!(partition.repair.is_some());
}

/// Ops up to the floor were resident but uncommitted, so the walk inside
/// the refusal passes the floor. With lazy flush their headers stay
/// resident and the leading edge reads as complete, but a floor below
/// `commit_min` moves nothing and is no reason for a state transfer.
#[compio::test]
async fn given_lazy_flush_when_the_walk_passes_a_disconnected_floor_should_keep_repairing() {
    let mut partition = test_partition();
    partition.consensus().advance_commit_max(300);
    partition.recovered_durable_offset = Some(3);
    partition.repair = Some(armed_session(300, 5));
    for op in 1..=6 {
        journal_prepare(&partition, op, Operation::CreateStream).await;
    }
    partition
        .apply_repaired_prepare(repaired_send_prepare(7, 0, 0x77))
        .await;
    let lazy = PartitionsConfig {
        messages_required_to_save: 10_000,
        size_of_messages_required_to_save: IggyByteSize::from(512 * 1024 * 1024),
        ..repair_config()
    };

    let conclusion = partition.complete_repair(&lazy).await;

    assert_eq!(conclusion, RepairConclusion::InProgress);
    assert_eq!(partition.consensus().commit_min(), 7);
    assert!(
        partition.log.journal().inner.holds_op(6),
        "lazy flush keeps the committed headers resident"
    );
    assert!(partition.repair.is_some());
}

/// The resident run past the floor is longer than one capped commit walk.
/// The floor is moot from the start, because this journal holds every op
/// up to it. One walk stops below the floor, and a floor judged there
/// reads as disconnected and spends a state transfer on ops already held.
#[compio::test]
async fn given_resident_run_longer_than_one_walk_when_it_passes_a_disconnected_floor_should_keep_repairing()
 {
    const FLOOR: u64 = COMMIT_WALK_OPS_MAX as u64 + 6;
    let mut partition = test_partition();
    partition.consensus().advance_commit_max(300);
    partition.recovered_durable_offset = Some(3);
    partition.repair = Some(armed_session(300, FLOOR));
    for op in 1..=FLOOR + 1 {
        journal_prepare(&partition, op, Operation::CreateStream).await;
    }
    partition
        .apply_repaired_prepare(repaired_send_prepare(FLOOR + 2, 0, 0x77))
        .await;

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(conclusion, RepairConclusion::InProgress);
    assert_eq!(
        partition.consensus().commit_min(),
        COMMIT_WALK_OPS_MAX as u64,
        "one capped walk, the rest resumes on the next"
    );
    assert!(partition.repair.is_some());
}

/// As above, with the whole repaired window resident, which would make a
/// refusal final. A resident run that reaches the floor drops the floor
/// before any capped walk, so the walk cap cannot turn it into a refusal.
#[compio::test]
async fn given_resident_run_longer_than_one_walk_when_the_window_is_complete_should_keep_repairing()
{
    const FLOOR: u64 = COMMIT_WALK_OPS_MAX as u64 + 6;
    let mut partition = test_partition();
    partition.consensus().advance_commit_max(FLOOR + 2);
    partition.recovered_durable_offset = Some(3);
    partition.repair = Some(armed_session(FLOOR + 2, FLOOR));
    for op in 1..=FLOOR {
        journal_prepare(&partition, op, Operation::CreateStream).await;
    }
    for op in FLOOR + 1..=FLOOR + 2 {
        partition
            .apply_repaired_prepare(repaired_send_prepare(op, 0, u128::from(op)))
            .await;
    }

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(conclusion, RepairConclusion::InProgress);
    assert!(partition.repair.is_some(), "no state transfer for held ops");
}

/// Repair frames arrive out of order. Op 7 connects at offset 4, op 9 does
/// not, and the window is judged by the lower one whichever lands first.
#[compio::test]
async fn given_batches_applied_out_of_order_when_the_floor_arrives_should_anchor_on_the_lowest() {
    for ops in [[9, 7], [7, 9]] {
        let mut partition = test_partition();
        partition.consensus().advance_commit_max(300);
        partition.recovered_durable_offset = Some(3);
        partition.repair = Some(armed_session(300, 5));
        for op in ops {
            let base_offset = if op == 7 { 4 } else { 6 };
            partition
                .apply_repaired_prepare(repaired_send_prepare_at(
                    op,
                    base_offset,
                    0,
                    u128::from(op),
                ))
                .await;
        }

        let conclusion = partition.complete_repair(&repair_config()).await;

        assert_eq!(conclusion, RepairConclusion::InProgress, "order {ops:?}");
        assert_eq!(
            partition.consensus().commit_min(),
            5,
            "order {ops:?}: the lowest batch connects, so the floor stands"
        );
    }
}

/// A floor holds the walk and the first served chunk carries no message,
/// so nothing anchors the window yet. Pulling again from `commit_min + 1`
/// re-serves that same chunk forever; the pull must start past it.
#[compio::test]
async fn given_a_floor_holding_the_walk_when_the_first_chunk_has_no_batch_should_pull_past_it() {
    const FLOOR: u64 = 5;
    // One full shard repair chunk above the floor.
    const FIRST_CHUNK_LAST: u64 = FLOOR + 128;
    const TO_OP: u64 = FIRST_CHUNK_LAST + 10;
    const SEND_OP: u64 = FIRST_CHUNK_LAST + 7;
    let mut partition = test_partition();
    partition.consensus().advance_commit_max(TO_OP);
    partition.recovered_durable_offset = Some(3);
    partition.repair = Some(armed_session(TO_OP, FLOOR));
    for op in FLOOR + 1..=FIRST_CHUNK_LAST {
        journal_prepare(&partition, op, Operation::CreateStream).await;
    }

    for _ in 0..2 {
        assert_eq!(
            partition.complete_repair(&repair_config()).await,
            RepairConclusion::InProgress
        );
        assert_eq!(partition.consensus().commit_min(), 0);
    }
    assert_eq!(partition.repair_pull_from(), Some(FIRST_CHUNK_LAST + 1));
    assert_eq!(partition.next_floor_pull(), Some(FIRST_CHUNK_LAST + 1));
    assert_eq!(
        partition.next_floor_pull(),
        None,
        "nothing new is resident, so the next pull waits for the stall retry"
    );

    for op in FIRST_CHUNK_LAST + 1..=TO_OP {
        if op == SEND_OP {
            partition
                .apply_repaired_prepare(repaired_send_prepare_at(op, 200, 0, u128::from(op)))
                .await;
        } else {
            journal_prepare(&partition, op, Operation::CreateStream).await;
        }
    }

    assert_eq!(
        partition.complete_repair(&repair_config()).await,
        RepairConclusion::FloorRefused {
            floor: FLOOR,
            to_op: TO_OP
        }
    );
}

/// A backup without a WAL that repairs `(0, to_op]` and has walked ops
/// `1..=walked_to`, each with one message, op `n` at offset `n - 1`.
async fn walless_backup_walked_to(walked_to: u64, to_op: u64) -> IggyPartition<RecordingBus> {
    let (mut backup, _) = recording_partition_at(1, 3);
    backup.consensus().advance_commit_max(to_op);
    backup.repair = Some(RepairSession {
        floor: None,
        ..armed_session(to_op, 0)
    });
    serve_batches(&mut backup, 1..=walked_to, 0).await;
    assert_eq!(
        backup.complete_repair(&repair_config()).await,
        RepairConclusion::InProgress
    );
    assert_eq!(backup.consensus().commit_min(), walked_to);
    backup
}

/// Serves `ops` as one-message batches at consecutive offsets from
/// `first_offset`.
async fn serve_batches(
    backup: &mut IggyPartition<RecordingBus>,
    ops: std::ops::RangeInclusive<u64>,
    first_offset: u64,
) {
    let parents = segment_chain_parents(*ops.end(), b"repaired");
    for (op, offset) in ops.zip(first_offset..) {
        let parent = parents[usize::try_from(op - 1).unwrap()];
        backup
            .apply_repaired_prepare(checksummed_segment_prepare(op, parent, offset, b"repaired"))
            .await;
    }
}

/// The peer's ring slid past the ops after the walked prefix. The batches
/// walked before the floor rose say nothing about the ops between them
/// and the floor.
#[compio::test]
async fn given_batches_walked_before_floor_rise_when_window_completes_should_refuse_commit_floor() {
    const WALKED_TO: u64 = 4;
    const FLOOR: u64 = 8;
    const TO_OP: u64 = 12;
    let mut backup = walless_backup_walked_to(WALKED_TO, TO_OP).await;

    backup.note_repair_floor(FLOOR);
    serve_batches(&mut backup, FLOOR + 1..=TO_OP, FLOOR).await;
    let conclusion = backup.complete_repair(&repair_config()).await;

    assert_eq!(
        backup.consensus().commit_min(),
        WALKED_TO,
        "the floor must not lift commit_min over ops {}..={FLOOR}, which carried messages",
        WALKED_TO + 1
    );
    assert_eq!(
        conclusion,
        RepairConclusion::FloorRefused {
            floor: FLOOR,
            to_op: TO_OP
        }
    );
}

/// A batch at or below the floor never anchors it, even one that starts
/// at the stand-in bound, because the jump skips it. Both edges of the
/// skipped window: the op after the walked prefix, and the floor itself.
#[compio::test]
async fn given_batch_at_or_below_floor_at_stand_in_bound_when_window_completes_should_refuse_commit_floor()
 {
    const WALKED_TO: u64 = 4;
    const FLOOR: u64 = 8;
    const TO_OP: u64 = 12;
    for skipped_op in [WALKED_TO + 1, FLOOR] {
        let mut backup = walless_backup_walked_to(WALKED_TO, TO_OP).await;
        backup.recovered_durable_offset = Some(WALKED_TO - 1);

        backup.note_repair_floor(FLOOR);
        serve_batches(&mut backup, skipped_op..=skipped_op, WALKED_TO).await;
        serve_batches(&mut backup, FLOOR + 1..=TO_OP, WALKED_TO + 1).await;
        let conclusion = backup.complete_repair(&repair_config()).await;

        assert_eq!(
            conclusion,
            RepairConclusion::FloorRefused {
                floor: FLOOR,
                to_op: TO_OP
            },
            "the batch at op {skipped_op} must not anchor the floor"
        );
        assert!(
            backup.consensus().commit_min() < FLOOR,
            "the floor must not lift commit_min over the batch at op {skipped_op}"
        );
    }
}

/// Ops between the walked prefix and the floor carried no messages, so
/// the first batch above the floor starts at the stand-in bound, one past
/// the recovered durable end.
#[compio::test]
async fn given_batch_above_floor_at_stand_in_bound_when_window_completes_should_accept_commit_floor()
 {
    const WALKED_TO: u64 = 4;
    const FLOOR: u64 = 8;
    const TO_OP: u64 = 12;
    let mut backup = walless_backup_walked_to(WALKED_TO, TO_OP).await;
    backup.recovered_durable_offset = Some(WALKED_TO - 1);

    backup.note_repair_floor(FLOOR);
    serve_batches(&mut backup, FLOOR + 1..=TO_OP, WALKED_TO).await;
    let conclusion = backup.complete_repair(&repair_config()).await;

    assert_eq!(
        conclusion,
        RepairConclusion::Done,
        "a first batch above the floor at the stand-in bound connects the window"
    );
    assert_eq!(backup.consensus().commit_min(), TO_OP);
}

/// The batches above the floor were journaled before the floor settled: in
/// a round without a floor, under a lower floor before the peer's ring
/// slid, or under the same floor in a round whose `RepairDone` was lost.
/// The peer re-serves them, the journal holds them, and they still anchor
/// the floor. Ops 5 to 8 carried no messages, so the first batch above the
/// floor starts at the stand-in bound.
#[compio::test]
async fn given_batches_journaled_before_floor_settled_when_window_completes_should_accept_commit_floor()
 {
    const WALKED_TO: u64 = 4;
    const LOWER_FLOOR: u64 = 6;
    const FLOOR: u64 = 8;
    const TO_OP: u64 = 12;
    for earlier_floor in [None, Some(LOWER_FLOOR), Some(FLOOR)] {
        let mut backup = walless_backup_walked_to(WALKED_TO, TO_OP).await;
        backup.recovered_durable_offset = Some(WALKED_TO - 1);
        if let Some(floor) = earlier_floor {
            backup.note_repair_floor(floor);
        }
        serve_batches(&mut backup, FLOOR + 1..=TO_OP, WALKED_TO).await;

        backup.note_repair_floor(FLOOR);
        serve_batches(&mut backup, FLOOR + 1..=TO_OP, WALKED_TO).await;
        let conclusion = backup.complete_repair(&repair_config()).await;

        assert_eq!(
            (conclusion, backup.consensus().commit_min()),
            (RepairConclusion::Done, TO_OP),
            "batches journaled under the earlier floor {earlier_floor:?} must anchor the floor"
        );
    }
}

/// A second floor in one session rises over ops whose messages never
/// arrived. The batches that anchored the first floor are walked and sit
/// below the second one, so they prove nothing about it.
#[compio::test]
async fn given_second_floor_above_walked_batches_when_window_completes_should_refuse_commit_floor()
{
    const WALKED_TO: u64 = 4;
    const FIRST_FLOOR: u64 = 6;
    const FIRST_TO: u64 = 8;
    const FLOOR: u64 = 12;
    const TO_OP: u64 = 16;
    // Ops 9 to 12 carried offsets 6 to 9, which never arrive here.
    const FIRST_OFFSET_ABOVE_FLOOR: u64 = 10;
    let mut backup = walless_backup_walked_to(WALKED_TO, TO_OP).await;
    backup.recovered_durable_offset = Some(WALKED_TO - 1);
    backup.note_repair_floor(FIRST_FLOOR);
    serve_batches(&mut backup, FIRST_FLOOR + 1..=FIRST_TO, WALKED_TO).await;
    assert_eq!(
        backup.complete_repair(&repair_config()).await,
        RepairConclusion::InProgress
    );
    assert_eq!(backup.consensus().commit_min(), FIRST_TO);

    backup.note_repair_floor(FLOOR);
    serve_batches(&mut backup, FLOOR + 1..=TO_OP, FIRST_OFFSET_ABOVE_FLOOR).await;
    let conclusion = backup.complete_repair(&repair_config()).await;

    assert_eq!(
        (conclusion, backup.consensus().commit_min()),
        (
            RepairConclusion::FloorRefused {
                floor: FLOOR,
                to_op: TO_OP
            },
            FIRST_TO
        ),
        "the second floor must not lift commit_min over ops {}..={FLOOR}",
        FIRST_TO + 1
    );
}

/// A `RangeEvicted` floor arrives above `commit_min`, and the walk passes
/// it before `RepairDone` lands: ops resident just above the commit point
/// committed and their headers were evicted. Verifying that moot floor
/// refused every round (evicted headers never complete the window) while
/// the walk inside the refusal ran `commit_min` to the fetch ceiling, and
/// the reply handler asked the peer for `fetch_to_op + 1 ..= fetch_to_op`.
#[compio::test]
async fn given_floor_below_commit_min_when_completing_repair_should_ignore_the_floor() {
    let mut partition = test_partition();
    partition.consensus().restore_commit_state(7, 8);
    // Disconnected on its face: no batch above the floor ties it to the
    // boot-recovered segments. Meaningless below commit_min.
    partition.recovered_durable_offset = Some(10);
    journal_prepare(&partition, 8, Operation::CreateStream).await;
    partition.repair = Some(armed_session(8, 5));

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(conclusion, RepairConclusion::Done);
    assert_eq!(partition.consensus().commit_min(), 8);
    assert!(
        partition.repair.is_none(),
        "nothing is left to fetch, so the session must close instead of \
             re-requesting past its ceiling"
    );
}

#[compio::test]
async fn given_floor_clamped_to_commit_min_when_completing_repair_should_still_refuse() {
    let mut partition = test_partition();
    partition.consensus().restore_commit_state(5, 5);
    // The peer retains nothing below op 10, so it cannot serve the suffix
    // either. Only the raw floor tells this apart from a moot one.
    partition.repair = Some(armed_fetch_session(5, 9, 9));

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(
        conclusion,
        RepairConclusion::FloorRefused { floor: 5, to_op: 5 },
        "a floor the clamp pulls down to commit_min still names an evicted \
             range and must escape to state transfer"
    );
    assert!(partition.repair.is_none());
}

#[compio::test]
async fn given_moot_floor_with_unfetched_suffix_when_completing_repair_should_keep_repairing() {
    let mut partition = test_partition();
    partition.consensus().restore_commit_state(5, 5);
    // The peer retains from op 6, everything this replica still needs, so
    // the suffix fetch must go on. Escaping to state transfer here copied
    // segments for a window the peer can serve.
    partition.repair = Some(armed_fetch_session(5, 9, 5));

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(conclusion, RepairConclusion::InProgress);
    let session = partition.repair.expect("the suffix fetch stays armed");
    assert!(
        partition.consensus().commit_min() < session.fetch_to_op,
        "the stall retry must still have a range to ask for"
    );
}
