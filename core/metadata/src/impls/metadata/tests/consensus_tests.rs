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

/// Reproduces the `commit_min must advance sequentially` shard-0 crash.
///
/// `on_ack` drains (pops) the committable prefix off the pipeline and only
/// then applies it, with awaits in between (journal read, wire send). Any
/// driver of `on_ack` that is dropped at one of those awaits — a hyper
/// HTTP handler future canceled by peer disconnect (`http/state.rs`), or
/// any parked in-process submitter — strands the popped-but-unapplied
/// entries: nothing can re-apply them (`repair_primary_self_acks` is
/// re-ack-only, above `commit_max`), `commit_min` is pinned below
/// `commit_max` (every login rejected `NotCaughtUp`), and the next commit
/// that quorums panics the shard.
///
/// The test parks a driver mid-commit exactly there, cancels it, and then
/// delivers the next ack. Correct behavior: the stranded op is still in
/// the pipeline and the late ack commits it and everything after it, in
/// order. Broken behavior: panic "expected 2, got 3".
#[compio::test]
async fn dropped_on_ack_driver_must_not_lose_popped_commits() {
    use std::future::Future;

    const CLIENT: u128 = 1;
    // The session is the Register op's commit number; it must be > 0 and
    // sort at-or-under the commits of the three ops below (1..=3).
    const SESSION: u64 = 1;
    const ACTING_USER: u32 = 7;

    let dir = tempfile::tempdir().unwrap();
    let journal =
        journal::prepare_journal::PrepareJournal::open(&dir.path().join("journal.wal"), 0)
            .await
            .unwrap();
    let consensus = VsrConsensus::new(
        1,
        0,
        1,
        server_common::sharding::METADATA_GROUP,
        StallBus::default(),
        LocalPipeline::new(),
    );
    consensus.init();
    let md: IggyMetadata<_, journal::prepare_journal::PrepareJournal, (), TestMux> =
        IggyMetadata::new(
            Some(consensus),
            Some(journal),
            None,
            None,
            TestMux::default(),
            None,
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

    // Three prepares through the real primary path: pipeline entry, WAL
    // append, self-ack onto the loopback queue.
    for (i, name) in ["s1", "s2", "s3"].iter().enumerate() {
        let prepare = md
            .prepare_request(create_stream_request(CLIENT, i as u64 + 1, name))
            .expect("CreateStream is client-allowed");
        consensus.pipeline_message(PlaneKind::Metadata, &prepare);
        md.on_replicate(prepare).await;
    }
    let mut loopback = Vec::new();
    consensus.drain_loopback_into(&mut loopback);
    let mut acks = loopback
        .into_iter()
        .map(|message| {
            message
                .try_into_typed::<PrepareOkHeader>()
                .expect("loopback holds self PrepareOks")
        })
        .collect::<Vec<_>>();
    assert_eq!(acks.len(), 3, "one self-ack per replicated prepare");
    let ack3 = acks.pop().unwrap();
    let ack2 = acks.pop().unwrap();
    let ack1 = acks.pop().unwrap();

    // Ack op 2 first: quorum for op 2 alone, but the contiguous prefix
    // still starts at the un-acked op 1, so nothing commits yet.
    md.on_ack(ack2).await;
    assert_eq!(consensus.commit_max(), 0);
    assert_eq!(consensus.commit_min(), 0);

    // Ack op 1: the quorum walk covers ops 1..=2, so this single driver
    // commits both. Poll it by hand until it parks at a stalled wire
    // send mid-`on_ack`, then cancel it — the moral equivalent of hyper
    // dropping an HTTP handler future on peer disconnect.
    consensus.message_bus().stall.set(true);
    {
        let mut driver = Box::pin(md.on_ack(ack1));
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        let mut parked_at_send = false;
        for _ in 0..1_000 {
            assert!(
                driver.as_mut().poll(&mut cx).is_pending(),
                "driver must park at the stalled wire send, not complete"
            );
            if consensus.message_bus().stall_hits.get() > 0 {
                parked_at_send = true;
                break;
            }
            // Let the runtime process the journal-read completion the
            // driver is waiting on, then poll again.
            compio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        assert!(
            parked_at_send,
            "driver never reached a wire send (commit_min={})",
            consensus.commit_min()
        );
        // Cancel mid-`on_ack`, with at least op 1 applied and the reply
        // send in flight.
        drop(driver);
    }
    consensus.message_bus().stall.set(false);
    assert_eq!(consensus.commit_max(), 2, "quorum walk advanced commit_max");
    assert!(
        consensus.commit_min() >= 1,
        "driver applied op 1 before parking at the wire send"
    );

    // Whatever the canceled driver left behind must still be
    // committable: delivering the ack for op 3 has to commit every
    // remaining op, in order. The broken commit path lost op 2 with the
    // dropped driver (popped, never applied) and panics here with
    // "commit_min must advance sequentially: expected 2, got 3".
    md.on_ack(ack3).await;
    assert_eq!(
        consensus.commit_min(),
        3,
        "late ack must commit the stranded op 2 and then op 3"
    );
    assert_eq!(consensus.commit_max(), 3);
    assert!(
        is_caught_up_primary(consensus),
        "gate must reopen once the prefix is fully applied"
    );
}

/// A backup admits the prepare its JOURNAL needs next, even when its
/// sequencer has run ahead of the journal.
///
/// The two frontiers legitimately disagree: `on_start_view` sets the sequencer
/// to the view's announced head, deliberately ahead of what this replica
/// holds, because the bodies arrive afterwards by retransmit or repair. Gating
/// admission on the sequencer therefore rejected exactly the prepare that
/// would heal the log: a backup with journal head 44 refusing op 45 because
/// its adopted head said to expect 46. The primary retransmits that op
/// forever, every backup logs an out-of-order gap, it never reaches a commit
/// quorum, and its client is never answered. Systematic for any rejoining
/// replica, so the deterministic simulator wedged on every metadata workload
/// under crash/restart injection until this was gated on the journal.
#[compio::test]
async fn backup_admits_the_prepare_its_journal_needs_despite_a_leading_sequencer() {
    const CLIENT: u128 = 1;
    const SESSION: u64 = 1;
    const ACTING_USER: u32 = 7;
    /// Stands in for a head adopted from a `StartView` whose bodies have not
    /// arrived, so it sits well above the empty journal.
    const ADOPTED_HEAD: u64 = 5;

    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join(crate::impls::METADATA_DIR)).unwrap();
    let journal =
        journal::prepare_journal::PrepareJournal::open(&dir.path().join("journal.wal"), 0)
            .await
            .unwrap();
    // Replica 1 of 3 at view 0, so `primary_index(0) == 0` makes this a backup
    // and `on_replicate` takes the gap-check branch.
    let consensus = VsrConsensus::new(
        1,
        1,
        3,
        server_common::sharding::METADATA_GROUP,
        NoopBus,
        LocalPipeline::new(),
    );
    consensus.init();
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
    assert!(
        consensus.is_follower(),
        "replica 1 of 3 at view 0 must be a backup for this to exercise the gap check"
    );
    md.client_table
        .borrow_mut()
        .commit_register(
            CLIENT,
            ACTING_USER,
            [0x5a; 32],
            register_reply(CLIENT, SESSION),
        )
        .unwrap();

    // Minted while the sequencer is still at 0, so it carries op 1: exactly
    // what the empty journal needs next.
    let prepare = md
        .prepare_request(create_stream_request(CLIENT, 1, "s1"))
        .expect("CreateStream is client-allowed");
    assert_eq!(prepare.header().op, 1, "the first prepare must be op 1");

    // Now run the sequencer ahead, as adopting a started view does.
    consensus.sequencer().set_sequence(ADOPTED_HEAD);
    let journal = md.journal.as_ref().unwrap();
    assert_eq!(
        journal.last_op(),
        None,
        "the journal must still be empty, else the divergence under test is absent"
    );

    md.on_replicate(prepare).await;

    assert!(
        journal.header(1).is_some(),
        "backup dropped the prepare its journal needed next because its sequencer \
         was ahead; the primary's retransmit of this op can never be accepted, so \
         the op never commits and its client never gets a reply"
    );
}

/// The walk cap bounds ONE `commit_journal` call, not the backlog.
///
/// The resident `(commit_min, commit_max]` run after a repair window or a
/// rejoin is the whole backlog, and the walk applies each op with no await
/// the pump can interleave, so an uncapped call holds the shard for all of
/// it. Every caller is re-driven every tick, so stopping short loses
/// nothing; a cap that did NOT resume would pin `commit_min` until the next
/// op to commit trips `advance_commit_min`'s sequential assert.
#[compio::test]
async fn commit_journal_stops_at_the_walk_cap_and_resumes_on_the_next_call() {
    const CLIENT: u128 = 1;
    const SESSION: u64 = 1;
    const ACTING_USER: u32 = 7;
    /// The cap, as an op count.
    const CAP: u64 = COMMIT_WALK_OPS_MAX as u64;
    /// One op past the cap, so the first call must stop short and the
    /// second must have something left to finish.
    const OPS: u64 = CAP + 1;

    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join(crate::impls::METADATA_DIR)).unwrap();
    let journal =
        journal::prepare_journal::PrepareJournal::open(&dir.path().join("journal.wal"), 0)
            .await
            .unwrap();
    // Replica 1 of 3 at view 0: a backup, so `on_replicate` journals each
    // prepare without the primary's pipeline commit path running under it.
    let consensus = VsrConsensus::new(
        1,
        1,
        3,
        server_common::sharding::METADATA_GROUP,
        NoopBus,
        LocalPipeline::new(),
    );
    consensus.init();
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

    for request in 1..=OPS {
        let prepare = md
            .prepare_request(create_stream_request(
                CLIENT,
                request,
                &format!("s{request}"),
            ))
            .expect("CreateStream is client-allowed");
        md.on_replicate(prepare).await;
    }
    let journal = md.journal.as_ref().unwrap();
    assert_eq!(journal.last_op(), Some(OPS), "every op must be resident");
    assert_eq!(consensus.commit_min(), 0, "no commit heartbeat has landed");

    // What a repair window or a rejoin leaves behind: the whole run
    // committed by the group and resident here, none of it walked.
    consensus.advance_commit_max(OPS);

    assert_eq!(md.commit_journal().await, CommitWalkStop::WalkCap);
    assert_eq!(
        consensus.commit_min(),
        CAP,
        "one call walked the whole backlog; the pump is blocked for as long \
         as the resident run is, however long that is"
    );

    assert_eq!(md.commit_journal().await, CommitWalkStop::CaughtUp);
    assert_eq!(
        consensus.commit_min(),
        OPS,
        "the walk did not resume where it stopped, so `commit_min` is pinned \
         below `commit_max` with no other re-driver"
    );
}

/// The walk leaves the pipeline head to `on_ack` and says so.
///
/// A primary-elect inherits a committed head at `commit_min + 1` and the
/// walk moves nothing over it. `tick_metadata`'s stall detector truncates
/// on `BodyMissing` only, so this has to come back as `PipelineHead` with
/// the prepare still journaled for the pipeline driver to read.
#[compio::test]
async fn given_committed_pipeline_head_at_next_op_when_walking_should_stop_as_pipeline_head() {
    const CLIENT: u128 = 1;
    const SESSION: u64 = 1;
    const ACTING_USER: u32 = 7;

    let dir = tempfile::tempdir().unwrap();
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
    let md: IggyMetadata<_, journal::prepare_journal::PrepareJournal, (), TestMux> =
        IggyMetadata::new(
            Some(consensus),
            Some(journal),
            None,
            None,
            TestMux::default(),
            None,
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

    let prepare = md
        .prepare_request(create_stream_request(CLIENT, 1, "s1"))
        .expect("CreateStream is client-allowed");
    consensus.pipeline_message(PlaneKind::Metadata, &prepare);
    md.on_replicate(prepare).await;
    // Committed with no driver to apply it: the inherited-head shape.
    consensus.advance_commit_max(1);

    assert_eq!(md.commit_journal().await, CommitWalkStop::PipelineHead);
    assert_eq!(
        consensus.commit_min(),
        0,
        "the walk applied the pipeline head"
    );
    assert!(
        md.journal.as_ref().unwrap().handle().header(1).is_some(),
        "the head's prepare must stay journaled for the pipeline driver"
    );

    md.resume_stranded_commits().await;
    assert_eq!(
        consensus.commit_min(),
        1,
        "the pipeline driver commits the head the walk left to it"
    );
}

/// A committed op with no resident header is a plain gap: the walk names it
/// as `HeaderMissing`, never as a missing body that needs truncating.
#[compio::test]
async fn given_committed_op_without_header_when_walking_should_stop_as_header_missing() {
    const CLIENT: u128 = 1;
    const SESSION: u64 = 1;
    const ACTING_USER: u32 = 7;
    const RESIDENT_OPS: u64 = 2;

    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join(crate::impls::METADATA_DIR)).unwrap();
    let journal =
        journal::prepare_journal::PrepareJournal::open(&dir.path().join("journal.wal"), 0)
            .await
            .unwrap();
    // A backup, so no pipeline head caps the walk.
    let consensus = VsrConsensus::new(
        1,
        1,
        3,
        server_common::sharding::METADATA_GROUP,
        NoopBus,
        LocalPipeline::new(),
    );
    consensus.init();
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

    for request in 1..=RESIDENT_OPS {
        let prepare = md
            .prepare_request(create_stream_request(
                CLIENT,
                request,
                &format!("s{request}"),
            ))
            .expect("CreateStream is client-allowed");
        md.on_replicate(prepare).await;
    }
    consensus.advance_commit_max(RESIDENT_OPS + 1);

    assert_eq!(
        md.commit_journal().await,
        CommitWalkStop::HeaderMissing {
            op: RESIDENT_OPS + 1
        }
    );
    assert_eq!(
        consensus.commit_min(),
        RESIDENT_OPS,
        "the walk applies the resident run before stopping at the gap"
    );
}

/// A backup re-acks the retransmit of an op it already applied.
///
/// A `DoViewChange` caps its commit at the sender's head, so a backup that
/// learned a commit without holding the op hands the next primary a lower
/// commit than the cluster reached. That primary restarts the op as
/// uncommitted and retransmits it. With the third replica down, this backup's
/// ack is the only quorum the op can get: dropping the retransmit keeps the
/// primary below its recovery barrier, refusing every metadata request.
///
/// A prepare naming a different prepare at the applied op is no retransmit:
/// it contradicts a committed op, so it is refused and counted.
#[compio::test]
async fn given_an_applied_op_when_the_primary_retransmits_it_should_ack_only_the_journaled_copy() {
    const CLIENT: u128 = 1;
    const SESSION: u64 = 1;
    const ACTING_USER: u32 = 7;
    const PRIMARY: u8 = 0;

    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join(crate::impls::METADATA_DIR)).unwrap();
    let journal =
        journal::prepare_journal::PrepareJournal::open(&dir.path().join("journal.wal"), 0)
            .await
            .unwrap();
    // Replica 1 of 3 at view 0: a backup of replica 0.
    let consensus = VsrConsensus::new(
        1,
        1,
        3,
        server_common::sharding::METADATA_GROUP,
        ReplicaSendSpyBus::default(),
        LocalPipeline::new(),
    );
    consensus.init();
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

    let prepare = md
        .prepare_request(create_stream_request(CLIENT, 1, "s1"))
        .expect("CreateStream is client-allowed");
    let op = prepare.header().op;
    let checksum = prepare.header().checksum;
    let conflicting = consensus::seal_prepare_checksum(prepare.clone().transmute_header(
        |old, header: &mut PrepareHeader| {
            *header = old;
            header.timestamp += 1;
        },
    ));
    assert!(
        verify_prepare_integrity(conflicting.header(), conflicting.as_slice()).is_ok(),
        "the conflicting prepare must reach the commit fence, not the integrity check"
    );
    md.on_replicate(prepare.clone()).await;
    consensus.advance_commit_max(op);
    md.commit_journal().await;
    assert_eq!(
        consensus.commit_min(),
        op,
        "the backup must apply the op first"
    );
    consensus.message_bus().replica_sends.borrow_mut().clear();

    md.on_replicate(prepare).await;
    assert_eq!(
        sent_prepare_oks(consensus.message_bus()),
        vec![(PRIMARY, op, checksum)],
        "the retransmit of an applied op went unacked, so a primary that restarted \
         it as uncommitted never reaches quorum while the third replica is down"
    );
    consensus.message_bus().replica_sends.borrow_mut().clear();

    md.on_replicate(conflicting).await;
    assert!(
        sent_prepare_oks(consensus.message_bus()).is_empty(),
        "a prepare that conflicts with the journaled op must never be acked"
    );
    assert_eq!(
        consensus.applied_log_contradictions(),
        1,
        "a prepare naming another prepare at an applied op must be reported, not \
         skipped as an old retransmit"
    );
}

/// A state-transfer receiver admits the first live prepare above the floor it
/// installed, instead of waiting for an op the snapshot already contains.
///
/// `install_state_transfer` moves the snapshot floor, the commit floor, the
/// sequencer and `commit_max`, and leaves the WAL head where it was: the
/// snapshot IS every op below the floor, so there is nothing left to append
/// for them. A gap check reading `last_op` alone therefore has the receiver
/// ask for `last_op + 1`, an op inside the snapshot that no peer will send
/// again, and it drops every live prepare forever.
///
/// Nothing recovers it. An offer built on a quiet cluster carries `commit_op
/// == snapshot_seq`, so the install lands `commit_min == commit_max` and
/// `maybe_request_metadata_repair`, gated on `commit_min < commit_max`, never
/// arms; repair is the only path that could refill the head. With the
/// other backup down the primary needs this replica's ack to commit at all,
/// so a 3-node cluster still inside its quorum stops serving metadata.
#[compio::test]
async fn state_transfer_receiver_admits_the_first_prepare_above_the_installed_floor() {
    const CLIENT: u128 = 1;
    const SESSION: u64 = 1;
    const ACTING_USER: u32 = 7;
    /// The `snapshot_seq` of a transferred offer, far above anything this
    /// replica's own WAL holds.
    const INSTALLED_FLOOR: u64 = 400;

    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join(crate::impls::METADATA_DIR)).unwrap();
    let journal =
        journal::prepare_journal::PrepareJournal::open(&dir.path().join("journal.wal"), 0)
            .await
            .unwrap();
    // Replica 1 of 3 at view 0, so `primary_index(0) == 0` makes this a backup
    // and `on_replicate` takes the gap-check branch.
    let consensus = VsrConsensus::new(
        1,
        1,
        3,
        server_common::sharding::METADATA_GROUP,
        NoopBus,
        LocalPipeline::new(),
    );
    consensus.init();
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
    assert!(
        consensus.is_follower(),
        "replica 1 of 3 at view 0 must be a backup for this to exercise the gap check"
    );
    md.client_table
        .borrow_mut()
        .commit_register(
            CLIENT,
            ACTING_USER,
            [0x5a; 32],
            register_reply(CLIENT, SESSION),
        )
        .unwrap();

    // Give the WAL a head far below the floor about to be installed, which is
    // what a replica that fell behind its peers' retention actually carries.
    let first = md
        .prepare_request(create_stream_request(CLIENT, 1, "s1"))
        .expect("CreateStream is client-allowed");
    md.on_replicate(first).await;
    let journal = md.journal.as_ref().unwrap();
    assert_eq!(
        journal.last_op(),
        Some(1),
        "the WAL head must sit below the installed floor, else the divergence \
         under test is absent"
    );

    // Exactly the frontiers `install_state_transfer` leaves for a quiet-cluster
    // offer, where the manifest's `commit_op` equals its `snapshot_seq`.
    journal.set_snapshot_op(INSTALLED_FLOOR);
    consensus.set_commit_floor(INSTALLED_FLOOR);
    consensus.sequencer().set_sequence(INSTALLED_FLOOR);
    consensus.advance_commit_max(INSTALLED_FLOOR);
    assert_eq!(
        consensus.commit_min(),
        consensus.commit_max(),
        "the wedge needs an install with no repair left to arm; diverged \
         frontiers heal through `maybe_request_metadata_repair`"
    );

    let next = md
        .prepare_request(create_stream_request(CLIENT, 2, "s2"))
        .expect("CreateStream is client-allowed");
    assert_eq!(
        next.header().op,
        INSTALLED_FLOOR + 1,
        "the primary numbers the next op off the installed floor"
    );

    md.on_replicate(next).await;

    assert!(
        journal
            .header(usize::try_from(INSTALLED_FLOOR + 1).unwrap())
            .is_some(),
        "state-transfer receiver dropped the first prepare above its installed \
         floor; the ops the snapshot already holds are never re-sent, and with \
         `commit_min == commit_max` no repair arms, so this op never commits"
    );
}

#[compio::test]
async fn given_committed_receipts_when_checkpoint_stops_at_each_boundary_should_reopen_them() {
    const CLIENT: u128 = 1;
    for boundary in [
        CheckpointBoundary::TableCaptured,
        CheckpointBoundary::TemporaryCreated,
        CheckpointBoundary::Written,
        CheckpointBoundary::FileSynced,
        CheckpointBoundary::Renamed,
        CheckpointBoundary::DirectorySynced,
        CheckpointBoundary::SuperblockWritten,
        CheckpointBoundary::WalDrained,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let metadata_directory = directory.path().join(crate::impls::METADATA_DIR);
        std::fs::create_dir_all(&metadata_directory).unwrap();
        let journal = PrepareJournal::open(&metadata_directory.join("journal.wal"), 0)
            .await
            .unwrap();
        let superblock = journal::superblock::PingPongSuperblock::open(&metadata_directory)
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
        let metadata: IggyMetadata<_, PrepareJournal, (), TestMux> = IggyMetadata::new(
            Some(consensus),
            Some(journal),
            None,
            Some(Rc::new(superblock)),
            TestMux::new((session_users(), (StreamsInner::new().into(), ()))),
            Some(directory.path().to_path_buf()),
        );
        let registration = metadata
            .submit_register_in_process(
                CLIENT,
                0,
                RegisterRequest {
                    user_created_at: 1,
                    bind_verifier: [0x5a; 32],
                },
            )
            .await
            .unwrap();
        let consensus = metadata.consensus.as_ref().unwrap();
        let journal = metadata.journal.as_ref().unwrap();
        metadata.set_checkpoint_margin(usize::MAX);
        metadata.checkpoint_if_needed(consensus, journal).await;
        metadata.set_checkpoint_margin(0);
        for request in 1..=3 {
            let message = create_stream_request(CLIENT, request, &format!("receipt-{request}"))
                .transmute_header(|old, header: &mut RoutedRequestHeader| {
                    *header = old;
                    header.session = registration.epoch;
                });
            let reply = metadata.submit_request_in_process(message).await.unwrap();
            assert_eq!(
                iggy_binary_protocol::result_code(&reply.as_slice()[size_of::<ReplyHeader>()..]),
                Some(0)
            );
        }
        let expected = metadata
            .client_table
            .borrow()
            .get_reply(CLIENT)
            .unwrap()
            .clone()
            .into_wire_bytes()
            .as_slice()
            .to_vec();
        let committed = consensus.commit_min();
        metadata.set_checkpoint_margin(usize::MAX);
        STOP_CHECKPOINT_AFTER.set(Some(boundary));
        metadata.checkpoint_if_needed(consensus, journal).await;
        assert!(
            STOP_CHECKPOINT_AFTER.get().is_none(),
            "{boundary:?} must be reached"
        );
        drop(metadata);
        let recovered = crate::impls::recovery::recover::<TestMux>(
            directory.path(),
            crate::impls::recovery::ReplicaIdentity {
                cluster: 1,
                replica_id: 0,
                replica_count: 1,
            },
            journal::prepare_journal::DEFAULT_SLOT_COUNT,
            CLIENTS_TABLE_MAX,
            |_| {},
            ChangeLogCapacities::default(),
        )
        .await
        .unwrap_or_else(|error| panic!("{boundary:?}: {error}"));
        let consensus::client_table::RequestStatus::Duplicate(reply) = recovered
            .client_table
            .check_request(CLIENT, registration.epoch, 3, Operation::CreateStream)
        else {
            panic!("{boundary:?}: latest unresolved result must remain replayable");
        };
        assert_eq!(
            reply.into_wire_bytes().as_slice(),
            expected,
            "{boundary:?}: latest receipt identity and body must survive reopen"
        );
        assert_eq!(recovered.client_table.get_user_id(CLIENT), Some(0));
        for request in 1..3 {
            assert!(
                matches!(
                    recovered.client_table.check_request(
                        CLIENT,
                        registration.epoch,
                        request,
                        Operation::CreateStream
                    ),
                    consensus::client_table::RequestStatus::Duplicate(_)
                        | consensus::client_table::RequestStatus::AlreadyApplied { .. }
                ),
                "{boundary:?}: earlier acknowledged request {request} must not execute again"
            );
        }
        assert_eq!(recovered.client_table.get_watermark(CLIENT), Some(3));
        assert!(
            recovered
                .last_journaled_op
                .or_else(|| recovered
                    .snapshot
                    .as_ref()
                    .map(IggySnapshot::sequence_number))
                .is_some_and(|op| op >= committed),
            "{boundary:?}: reopened state must cover the acknowledged prefix"
        );
    }
}

/// A checkpoint reclaims the WAL prefix the snapshot supersedes, but must
/// stop one op short of the checkpoint op itself.
///
/// That op is the replica's commit point, and its `DoViewChange` suffix is
/// floored there. The merge scans the commit point and may not discard it,
/// so a sender with no header to put there is deferring to a peer; when
/// every sender has checkpointed at the same op the view change deadlocks
/// (`dvc_merge::merge_dvc_quorum`). Checkpoints fire on local journal
/// occupancy, which is symmetric across replicas seeing the same ops, so
/// "every sender" is the ordinary case, not a coincidence.
#[compio::test]
async fn checkpoint_drain_retains_the_commit_point_header() {
    const CLIENT: u128 = 1;
    const SESSION: u64 = 1;
    const ACTING_USER: u32 = 7;
    const OPS: u64 = 5;
    const CHECKPOINT_OP: u64 = 3;

    let dir = tempfile::tempdir().unwrap();
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

    for op in 1..=OPS {
        let prepare = md
            .prepare_request(create_stream_request(CLIENT, op, &format!("s{op}")))
            .expect("CreateStream is client-allowed");
        consensus.pipeline_message(PlaneKind::Metadata, &prepare);
        md.on_replicate(prepare).await;
    }

    let journal = md.journal.as_ref().unwrap();
    md.coordinator
        .as_ref()
        .expect("data_dir present arms the coordinator")
        .drain(journal, CHECKPOINT_OP)
        .await
        .expect("drain the snapshotted prefix");

    let header_at = |op: u64| journal.header(usize::try_from(op).expect("test ops fit usize"));
    for op in 1..CHECKPOINT_OP {
        assert!(
            header_at(op).is_none(),
            "op {op} is below the checkpoint and must be reclaimed"
        );
    }
    assert!(
        header_at(CHECKPOINT_OP).is_some(),
        "the checkpoint op is the commit point and must stay describable in a DVC"
    );
    for op in CHECKPOINT_OP + 1..=OPS {
        assert!(header_at(op).is_some(), "op {op} was never snapshotted");
    }
}
