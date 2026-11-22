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

/// Reproduces the single-node "metadata prepare queue is full" wedge
///
/// `checkpoint_if_needed` runs inside `on_replicate`, once per submit.
/// Under a concurrent login/create burst, several `on_replicate` futures
/// cross the forced-checkpoint boundary (journal `remaining_capacity <=
/// CHECKPOINT_MARGIN`, i.e. op `SLOT_COUNT - MARGIN = 960`) together, and
/// every one of them runs a full checkpoint concurrently. The concurrent
/// `journal.drain()` calls race on the one fixed `wal.tmp`: the losers
/// surface `snapshot I/O error: No such file or directory` (or a short
/// read after the winner's reopen). Fatally, `on_replicate` then dropped
/// the loser's prepare — AFTER `pipeline_message` had pushed the pipeline
/// entry and pre-advanced the sequencer — so the op was never journaled,
/// never acked, never committed. The commit frontier gaps permanently:
/// logins first bounce `NotCaughtUp`, in-flight clients wedge
/// (`InProgress` on logout), and once the pipeline fills every submit is
/// rejected `PipelineFull` forever.
///
/// Correct behavior: checkpoints are single-flight, a failed or skipped
/// checkpoint never discards a pipelined prepare, all racers' ops are
/// journaled and commit, and the caught-up gate reopens.
#[compio::test]
async fn concurrent_checkpoint_boundary_must_not_drop_prepares() {
    const CLIENT: u128 = 1;
    const SESSION: u64 = 1;
    const ACTING_USER: u32 = 7;
    /// One op below the forced-checkpoint trigger: the journal holds
    /// 1024 slots and forces a checkpoint when 64 or fewer remain.
    const FILL: u64 = 960;

    let dir = tempfile::tempdir().unwrap();
    // Bootstrap creates the metadata dir; the coordinator's snapshot
    // persist expects it to exist.
    std::fs::create_dir_all(dir.path().join(crate::impls::METADATA_DIR)).unwrap();
    let journal =
        journal::prepare_journal::PrepareJournal::open(&dir.path().join("journal.wal"), 0)
            .await
            .unwrap();
    let consensus = VsrConsensus::new(
        1,
        0,
        1,
        server_common::sharding::METADATA_GROUP,
        NoopBus,
        LocalPipeline::new(),
    );
    consensus.init();
    // `data_dir` present => SnapshotCoordinator armed, checkpoints live.
    let md: IggyMetadata<_, journal::prepare_journal::PrepareJournal, (), TestMux> =
        IggyMetadata::new(
            Some(consensus),
            Some(journal),
            None,
            None,
            TestMux::default(),
            Some(dir.path().to_path_buf()),
        );
    let consensus = md.consensus.as_ref().unwrap();
    md.client_table
        .borrow_mut()
        .commit_register(
            CLIENT,
            ACTING_USER,
            [0x5a; 32],
            register_reply(CLIENT, SESSION),
        )
        .unwrap();

    // Fill to one op under the boundary through the real primary path,
    // acking each op so `commit_min` tracks `last_op` and the pipeline
    // stays shallow — the steady state the production server was in.
    let mut loopback = Vec::new();
    for i in 1..=FILL {
        let prepare = md
            .prepare_request(create_stream_request(CLIENT, i, &format!("s{i}")))
            .expect("CreateStream is client-allowed");
        consensus.pipeline_message(PlaneKind::Metadata, &prepare);
        md.on_replicate(prepare).await;
        loopback.clear();
        consensus.drain_loopback_into(&mut loopback);
        let ack = loopback
            .pop()
            .expect("one self-ack per prepare")
            .try_into_typed::<PrepareOkHeader>()
            .expect("loopback holds self PrepareOks");
        md.on_ack(ack).await;
    }
    assert_eq!(consensus.commit_min(), FILL);
    assert_eq!(
        md.journal.as_ref().unwrap().remaining_capacity(),
        Some(64),
        "fill must stop exactly at the forced-checkpoint boundary"
    );

    // Three submits race across the boundary — the concurrent
    // login/create burst from the incident. Every racer sees
    // `remaining_capacity <= CHECKPOINT_MARGIN` before any drain
    // completes.
    let md_ref = &md;
    let race = |request: u64, name: String| async move {
        let prepare = md_ref
            .prepare_request(create_stream_request(CLIENT, request, &name))
            .expect("CreateStream is client-allowed");
        consensus.pipeline_message(PlaneKind::Metadata, &prepare);
        md_ref.on_replicate(prepare).await;
    };
    futures::join!(
        race(FILL + 1, format!("s{}", FILL + 1)),
        race(FILL + 2, format!("s{}", FILL + 2)),
        race(FILL + 3, format!("s{}", FILL + 3)),
    );

    // Every racer's prepare must be durably journaled: a dropped one is
    // unrepairable (nothing re-prepares it) and gaps the frontier.
    let journal = md.journal.as_ref().unwrap();
    for op in FILL + 1..=FILL + 3 {
        assert!(
            journal
                .header(usize::try_from(op).expect("test ops fit in usize"))
                .is_some(),
            "op {op} vanished from the WAL: a failed checkpoint dropped a pipelined prepare"
        );
    }
    // The checkpoint itself must have happened — once: WAL reclaimed,
    // snapshot on disk.
    assert!(
        journal.remaining_capacity().unwrap() > 900,
        "checkpoint must have drained the snapshotted prefix, got {:?}",
        journal.remaining_capacity()
    );
    assert!(
        dir.path()
            .join(crate::impls::METADATA_DIR)
            .join("snapshot.bin")
            .exists(),
        "checkpoint must persist the snapshot"
    );

    // The self-acks commit all three racers; any gap here is the
    // production wedge (commit frontier pinned, PipelineFull forever).
    loopback.clear();
    consensus.drain_loopback_into(&mut loopback);
    assert_eq!(loopback.len(), 3, "one self-ack per racer");
    for message in loopback {
        let ack = message
            .try_into_typed::<PrepareOkHeader>()
            .expect("loopback holds self PrepareOks");
        md.on_ack(ack).await;
    }
    assert_eq!(
        consensus.commit_min(),
        FILL + 3,
        "commit frontier must cross the checkpoint boundary"
    );
    assert!(
        is_caught_up_primary(consensus),
        "caught-up gate must reopen after the boundary"
    );
}

/// Each submit claims its op and then waits on `journal_gate`. A submit that
/// arrives after a release must not take the gate ahead of the submits that
/// wait for it: a backup takes only the next op, so it drops a prepare that
/// comes early, and the op waits for a retransmit. The test holds the gate,
/// as a slow WAL append does. The submits run in spawned tasks, as in the
/// server, because a `join!` driver polls its futures in arrival order and
/// hides the overtake.
#[compio::test]
async fn given_a_submit_waiting_on_the_journal_gate_when_another_submit_arrives_should_send_in_op_order()
 {
    let (_dir, metadata) = primary_on_spy_bus([1, 2]).await;
    let submit = |client: u128| {
        let metadata = Rc::clone(&metadata);
        compio::runtime::spawn(async move {
            metadata
                .on_request(create_stream_request(client, 1, &format!("s{client}")))
                .await;
        })
    };
    let consensus = metadata.consensus.as_ref().unwrap();

    let held = metadata.journal_gate.try_acquire().unwrap();
    let waiting = submit(1);
    yield_now().await;
    assert_eq!(
        consensus.pipeline_len(),
        1,
        "the first submit claims op 1 and waits on the gate"
    );
    let late = submit(2);
    drop(held);
    waiting.await.unwrap();
    late.await.unwrap();

    for backup in 1..=2 {
        assert_eq!(
            sent_prepare_ops(consensus.message_bus(), backup),
            [1, 2],
            "backup {backup} takes only the next op"
        );
    }
}

/// An in-process submit runs `on_replicate` on the caller's task, so it can
/// sit on `journal_gate` while the shard adopts a `StartView`. Granted the
/// gate afterwards, its prepare is the next op of the adopted journal and
/// the follower path would take it for one of the new view. A checkpoint it
/// ran first would keep the reconcile of the view waiting on the gate.
#[compio::test]
async fn given_a_submit_waiting_on_the_journal_gate_when_a_view_is_adopted_meanwhile_should_drop_the_prepare()
 {
    const CLIENT: u128 = 1;
    let (_dir, metadata) = primary_on_spy_bus([CLIENT]).await;
    let consensus = metadata.consensus.as_ref().unwrap();
    metadata.set_checkpoint_margin(usize::MAX);
    let coordinator = metadata.coordinator.as_ref().unwrap();
    coordinator.checkpoint_pending.set(true);

    let held = metadata.journal_gate.try_acquire().unwrap();
    let waiting = compio::runtime::spawn({
        let metadata = Rc::clone(&metadata);
        async move {
            metadata
                .submit_request_in_process(create_stream_request(CLIENT, 1, "stale"))
                .await
        }
    });
    yield_now().await;
    assert_eq!(
        consensus.pipeline_len(),
        1,
        "the submit claims op 1 and waits on the gate"
    );
    adopt_view(consensus, 1);
    drop(held);
    let reply = waiting.await.unwrap().unwrap();

    assert_eq!(
        iggy_binary_protocol::result_code(reply.body()),
        Some(IggyError::TransientNotCommitted.as_code()),
        "the caller learns the outcome is unknown and retries"
    );
    assert!(
        metadata
            .journal
            .as_ref()
            .unwrap()
            .handle()
            .header(1)
            .is_none(),
        "a prepare of view 0 must not enter the journal of view 1"
    );
    assert!(
        coordinator.checkpoint_pending.get(),
        "a prepare of view 0 must not run a checkpoint in view 1"
    );
    for backup in 1..=2 {
        assert!(
            sent_prepare_ops(consensus.message_bus(), backup).is_empty(),
            "backup {backup} must not be sent a prepare of the view that ended"
        );
    }
}

/// The submit holds `journal_gate` across the forced checkpoint, and the
/// snapshot write yields to the shard. A `StartView` adopted meanwhile makes
/// this replica a backup of the new view before the prepare is appended.
#[compio::test]
async fn given_a_submit_inside_its_checkpoint_when_a_view_is_adopted_meanwhile_should_drop_the_prepare()
 {
    const CLIENT: u128 = 1;
    let (_dir, metadata) = primary_on_spy_bus([CLIENT]).await;
    let consensus = metadata.consensus.as_ref().unwrap();
    // An interrupted checkpoint stays pending, and the next prepare retries it.
    metadata.set_checkpoint_margin(usize::MAX);
    metadata
        .coordinator
        .as_ref()
        .unwrap()
        .checkpoint_pending
        .set(true);

    let checkpointing = compio::runtime::spawn({
        let metadata = Rc::clone(&metadata);
        async move {
            metadata
                .submit_request_in_process(create_stream_request(CLIENT, 1, "stale"))
                .await
        }
    });
    yield_now().await;
    assert!(
        metadata.checkpoint_lock.try_acquire().is_none(),
        "the submit must be inside its checkpoint"
    );
    adopt_view(consensus, 1);
    let reply = checkpointing.await.unwrap().unwrap();

    assert_eq!(
        iggy_binary_protocol::result_code(reply.body()),
        Some(IggyError::TransientNotCommitted.as_code()),
        "the caller learns the outcome is unknown and retries"
    );
    assert!(
        metadata
            .journal
            .as_ref()
            .unwrap()
            .handle()
            .header(1)
            .is_none(),
        "a prepare of view 0 must not enter the journal of view 1"
    );
    assert_eq!(
        consensus.sequencer().current_sequence(),
        0,
        "a prepare of view 0 must not move the sequencer of view 1"
    );
    for backup in 1..=2 {
        assert!(
            sent_prepare_ops(consensus.message_bus(), backup).is_empty(),
            "backup {backup} must not be sent a prepare of the view that ended"
        );
    }
}

/// A prepare granted the gate during a view change still lands, on the
/// follower path when another replica leads the next view. That view can be
/// adopted while the prepare appends, and its op is not one of the view's log.
#[compio::test]
async fn given_a_submit_inside_its_append_when_a_view_is_adopted_meanwhile_should_keep_the_sequencer_of_the_view()
 {
    const CLIENT: u128 = 1;
    const NEXT_VIEW: u32 = 2;
    let (_dir, metadata) = primary_on_spy_bus([CLIENT]).await;
    let consensus = metadata.consensus.as_ref().unwrap();
    let journal = metadata.journal.as_ref().unwrap();

    let held = metadata.journal_gate.try_acquire().unwrap();
    let appending = compio::runtime::spawn({
        let metadata = Rc::clone(&metadata);
        async move {
            metadata
                .submit_request_in_process(create_stream_request(CLIENT, 1, "stale"))
                .await
        }
    });
    yield_now().await;
    assert_eq!(
        consensus.pipeline_len(),
        1,
        "the submit claims op 1 and waits on the gate"
    );
    // A primary joins only a vote of a replica that is two views ahead.
    let vote = StartViewChangeHeader {
        checksum: 0,
        checksum_body: 0,
        cluster: 1,
        size: u32::try_from(size_of::<StartViewChangeHeader>()).unwrap(),
        view: NEXT_VIEW,
        release: 0,
        command: Command::StartViewChange,
        replica: 1,
        reserved_frame: [0; 66],
        group: server_common::sharding::METADATA_GROUP,
        partition_incarnation: 0,
        reserved: [0; 112],
    };
    consensus.handle_start_view_change(PlaneKind::Metadata, &vote);
    assert_eq!(
        (consensus.status(), consensus.view()),
        (Status::ViewChange, NEXT_VIEW),
        "the replica must be changing to view {NEXT_VIEW}"
    );
    drop(held);
    yield_now().await;
    assert!(
        journal.handle().header(1).is_none(),
        "the write must still be in flight"
    );
    adopt_view(consensus, NEXT_VIEW);
    appending.await.unwrap().unwrap();

    assert!(
        journal.handle().header(1).is_some(),
        "the write in flight lands, for the reconcile of the view to remove"
    );
    assert_eq!(
        consensus.sequencer().current_sequence(),
        0,
        "a prepare of view 0 must not move the sequencer of view {NEXT_VIEW}"
    );
}
