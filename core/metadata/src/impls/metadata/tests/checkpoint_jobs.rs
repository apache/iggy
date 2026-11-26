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
async fn given_empty_or_populated_state_when_persisting_a_checkpoint_should_write_the_encoded_snapshot()
 {
    // Above every op the fixtures record, which `IggySnapshot::load` validates.
    const SEQUENCE_NUMBER: u64 = 1_000;
    const CREATED_AT: u64 = 1_700_000_000_000_000;
    let empty = (
        TestMux::default(),
        ClientTable::new(CLIENTS_TABLE_MAX).to_snapshot(),
        ChangeLogSnapshot::default(),
    );
    for (mux, client_table, change_log) in [empty, populated_checkpoint_state()] {
        let directory = tempfile::tempdir().unwrap();
        std::fs::create_dir(directory.path().join(crate::impls::METADATA_DIR)).unwrap();
        let coordinator =
            SnapshotCoordinator::new(directory.path().to_path_buf(), MetadataCapture::create);
        let mut snapshot = IggySnapshot::create(&mux, SEQUENCE_NUMBER, CREATED_AT).unwrap();
        snapshot.snapshot.client_table = Some(client_table.clone());
        snapshot.snapshot.change_log = change_log.clone();
        let expected = snapshot.encode().unwrap();
        let mut one_step_chunks =
            MetadataCapture::create(&mux, SEQUENCE_NUMBER, CREATED_AT).unwrap();
        one_step_chunks.client_table = Some(client_table.clone());
        one_step_chunks.change_log = change_log.clone();

        let capture = coordinator
            .capture(
                &mux,
                SEQUENCE_NUMBER,
                CREATED_AT,
                Some(client_table),
                change_log,
            )
            .unwrap();
        let checksum = coordinator.persist_capture(capture).await.unwrap();

        let path = coordinator.snapshot_path();
        let file = std::fs::read(&path).unwrap();
        let (payload, trailer) = file.split_at(file.len() - SNAPSHOT_TRAILER_LEN);
        assert_eq!(
            payload, expected,
            "the streamed payload must be the encoded snapshot"
        );
        assert_eq!(checksum, checkpoint_checksum(&expected));
        assert_eq!(trailer, snapshot_trailer(checksum));
        assert_eq!(coordinator.last_checkpoint(), (SEQUENCE_NUMBER, checksum));
        let (loaded, loaded_checksum) = IggySnapshot::load(&path).unwrap();
        assert_eq!(loaded_checksum, checksum);
        assert_eq!(loaded.sequence_number(), SEQUENCE_NUMBER);
        assert!(!path.with_extension("bin.tmp").exists());

        // One byte per chunk cuts after every step. Spawned as the server runs
        // it: a yield whose wake got lost would hang here.
        let chunked_path = directory.path().join("one-step-chunks.bin");
        let chunked_checksum = compio::runtime::spawn({
            let chunked_path = chunked_path.clone();
            async move {
                write_snapshot_file(
                    &chunked_path,
                    one_step_chunks.into_encoder(),
                    SnapshotIo::Compio,
                    1,
                )
                .await
            }
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(chunked_checksum, checksum);
        assert_eq!(
            std::fs::read(&chunked_path).unwrap(),
            file,
            "chunk boundaries must not change a byte"
        );
    }
}

/// Synchronous I/O never suspends, so every `Pending` of the write is a yield
/// after a chunk, and each must wake its own task first or no executor polls it
/// again.
#[cfg(feature = "simulator")]
#[test]
fn given_one_byte_chunks_when_writing_a_checkpoint_should_yield_after_every_step() {
    const SEQUENCE_NUMBER: u64 = 1_000;
    const CREATED_AT: u64 = 1_700_000_000_000_000;

    struct CountingWaker(std::sync::atomic::AtomicUsize);

    impl std::task::Wake for CountingWaker {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    let (mux, client_table, change_log) = populated_checkpoint_state();
    let mut snapshot = IggySnapshot::create(&mux, SEQUENCE_NUMBER, CREATED_AT).unwrap();
    snapshot.snapshot.client_table = Some(client_table.clone());
    snapshot.snapshot.change_log = change_log.clone();
    let expected = snapshot.encode().unwrap();
    let capture = || {
        let mut capture = MetadataCapture::create(&mux, SEQUENCE_NUMBER, CREATED_AT).unwrap();
        capture.client_table = Some(client_table.clone());
        capture.change_log = change_log.clone();
        capture
    };
    // A step that writes a byte ends its chunk.
    let mut steps = 0;
    let mut encoder = capture().into_encoder();
    let mut written = Vec::new();
    loop {
        let before = written.len();
        let more = encoder.encode_next(&mut written).unwrap();
        steps += usize::from(written.len() > before);
        if !more {
            break;
        }
    }
    let streams = snapshot.snapshot.streams.as_ref().unwrap().items.len();
    assert!(
        steps > streams,
        "the fixture must hold streams of several steps"
    );
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join(crate::impls::SNAPSHOT_FILE_NAME);

    let wake_count = Arc::new(CountingWaker(std::sync::atomic::AtomicUsize::new(0)));
    let waker = std::task::Waker::from(Arc::clone(&wake_count));
    let mut context = std::task::Context::from_waker(&waker);
    let mut write = std::pin::pin!(write_snapshot_file(
        &path,
        capture().into_encoder(),
        SnapshotIo::Synchronous,
        1
    ));
    let mut pending_polls = 0;
    let checksum = loop {
        match write.as_mut().poll(&mut context) {
            Poll::Ready(result) => break result.unwrap(),
            Poll::Pending => pending_polls += 1,
        }
    };

    assert!(
        pending_polls >= steps,
        "{pending_polls} yields for {steps} steps in chunks of their own"
    );
    assert_eq!(
        wake_count.0.load(std::sync::atomic::Ordering::Relaxed),
        pending_polls
    );
    let file = std::fs::read(&path).unwrap();
    assert_eq!(file[..file.len() - SNAPSHOT_TRAILER_LEN], expected);
    assert_eq!(checksum, checkpoint_checksum(&expected));
}

#[compio::test]
async fn given_half_the_wal_committed_when_the_job_write_is_parked_should_keep_appending_and_trim_once()
 {
    let dir = tempfile::tempdir().unwrap();
    let (md, jobs) = job_metadata(dir.path(), 0, 1, None).await;
    let coordinator = md.coordinator.as_ref().unwrap();
    let held = SNAPSHOT_IO.lock().await;

    commit_on_primary(&md, 1..=JOB_TRIGGER_OP - 1).await;
    assert!(jobs.borrow().is_empty(), "no job below half the WAL");
    commit_on_primary(&md, JOB_TRIGGER_OP..=JOB_MARGIN_OP - 1).await;

    assert_eq!(
        md.consensus.as_ref().unwrap().commit_min(),
        JOB_MARGIN_OP - 1
    );
    assert_eq!(jobs.borrow().len(), 1, "one job, started at half the WAL");
    assert!(
        md.checkpoint_lock.try_acquire().is_none(),
        "the parked job holds its lease"
    );
    assert_eq!(journal_floor(&md), 0);
    drop(held);
    assert_eq!(await_jobs(&jobs).await, 1);

    assert_eq!(journal_floor(&md), JOB_TRIGGER_OP - 1);
    assert_eq!(coordinator.superblock_checkpoint_op.get(), JOB_TRIGGER_OP);
    assert!(!coordinator.checkpoint_pending.get());
    let (snapshot, _) = IggySnapshot::load(&coordinator.snapshot_path()).unwrap();
    assert_eq!(snapshot.sequence_number(), JOB_TRIGGER_OP);
    assert_eq!(COMPLETED_CHECKPOINTS.get(), 1);
    assert_eq!(
        LAST_COMPLETED_CHECKPOINT.get(),
        Some((CheckpointOrigin::Job, CheckpointTrimmer::Job))
    );
}

#[compio::test]
async fn given_a_parked_checkpoint_job_when_offering_should_refuse_until_the_job_ends() {
    let dir = tempfile::tempdir().unwrap();
    let (md, jobs) = job_metadata(dir.path(), 0, 1, None).await;
    persist_initial_snapshot(&md).await;
    let held = SNAPSHOT_IO.lock().await;
    commit_on_primary(&md, 1..=JOB_TRIGGER_OP).await;
    assert_eq!(jobs.borrow().len(), 1, "the job waits in its write");

    assert!(
        matches!(
            md.state_transfer_offer().await,
            Err(StateTransferUnavailable::SnapshotReplaced)
        ),
        "the job trims the WAL past the snapshot that is still on disk"
    );
    drop(held);
    assert_eq!(await_jobs(&jobs).await, 1);

    let offer = md.state_transfer_offer().await.unwrap();
    assert_eq!(offer.snapshot_seq, JOB_TRIGGER_OP);
    assert!(journal_floor(&md) <= offer.snapshot_seq);
}

/// A requester that pulls an offer installs its snapshot and then repairs
/// from the op after it. A job that trims the WAL to its own, newer
/// checkpoint meanwhile leaves the requester nothing to repair from, and it
/// starts over. The shard drops a pulled offer after a short time, which
/// can be before the job ends its writes and before the requester asks.
#[compio::test]
async fn given_an_offer_held_at_the_capture_when_it_is_dropped_during_the_job_should_leave_the_wal_to_the_margin()
 {
    let dir = tempfile::tempdir().unwrap();
    let (md, jobs) = job_metadata(dir.path(), 0, 1, None).await;
    let coordinator = md.coordinator.as_ref().unwrap();
    persist_initial_snapshot(&md).await;
    let served = md.state_transfer_offer().await.unwrap();
    let served_op = served.snapshot_seq;
    let held = SNAPSHOT_IO.lock().await;
    commit_on_primary(&md, 1..=JOB_TRIGGER_OP).await;
    assert_eq!(jobs.borrow().len(), 1, "the job waits in its write");

    drop(served);
    drop(held);
    assert_eq!(await_jobs(&jobs).await, 1);

    assert_eq!(coordinator.superblock_checkpoint_op.get(), JOB_TRIGGER_OP);
    assert_eq!(
        journal_floor(&md),
        served_op,
        "the requester repairs from the op after the snapshot that it pulled"
    );
    assert_eq!(COMPLETED_CHECKPOINTS.get(), 0);

    commit_on_primary(&md, JOB_TRIGGER_OP + 1..=JOB_MARGIN_OP + 1).await;

    assert_eq!(
        journal_floor(&md),
        JOB_TRIGGER_OP - 1,
        "the journal must not run out of slots for a requester"
    );
    assert_eq!(
        LAST_COMPLETED_CHECKPOINT.get(),
        Some((CheckpointOrigin::Job, CheckpointTrimmer::Margin))
    );
    assert!(jobs.borrow().is_empty(), "the margin trims without a job");
}

/// An offer that nobody holds at the capture needs no WAL.
#[compio::test]
async fn given_an_offer_dropped_before_the_capture_when_a_checkpoint_job_ends_should_trim_the_wal()
{
    let dir = tempfile::tempdir().unwrap();
    let (md, jobs) = job_metadata(dir.path(), 0, 1, None).await;
    persist_initial_snapshot(&md).await;
    drop(md.state_transfer_offer().await.unwrap());

    commit_on_primary(&md, 1..=JOB_TRIGGER_OP).await;
    assert_eq!(await_jobs(&jobs).await, 1);

    assert_eq!(journal_floor(&md), JOB_TRIGGER_OP - 1);
    assert_eq!(
        LAST_COMPLETED_CHECKPOINT.get(),
        Some((CheckpointOrigin::Job, CheckpointTrimmer::Job))
    );
}

/// The journal already trimmed past the snapshot of this offer, so its
/// requester cannot repair here any more, and a kept WAL gives it nothing.
#[compio::test]
async fn given_a_held_offer_below_the_journal_floor_when_a_checkpoint_job_ends_should_trim_the_wal()
{
    let dir = tempfile::tempdir().unwrap();
    let (md, jobs) = job_metadata(dir.path(), 0, 1, None).await;
    persist_initial_snapshot(&md).await;
    let served = md.state_transfer_offer().await.unwrap();
    commit_on_primary(&md, 1..=JOB_TRIGGER_OP).await;
    assert_eq!(await_jobs(&jobs).await, 1);
    commit_on_primary(&md, JOB_TRIGGER_OP + 1..=JOB_MARGIN_OP + 1).await;
    assert!(served.snapshot_seq < journal_floor(&md));

    commit_on_primary(&md, JOB_MARGIN_OP + 2..=2 * JOB_TRIGGER_OP).await;
    assert_eq!(await_jobs(&jobs).await, 1);

    assert_eq!(journal_floor(&md), 2 * JOB_TRIGGER_OP - 1);
    assert_eq!(
        LAST_COMPLETED_CHECKPOINT.get(),
        Some((CheckpointOrigin::Job, CheckpointTrimmer::Job))
    );
}

#[compio::test]
async fn given_an_offer_read_in_flight_when_a_checkpoint_job_captures_should_refuse_the_offer() {
    let dir = tempfile::tempdir().unwrap();
    let (md, jobs) = job_metadata(dir.path(), 0, 1, None).await;
    persist_initial_snapshot(&md).await;
    commit_on_primary(&md, 1..=JOB_TRIGGER_OP - 1).await;
    let mut offer = Box::pin(md.state_transfer_offer());
    assert!(
        futures::poll!(&mut offer).is_pending(),
        "the read must suspend"
    );
    let held = SNAPSHOT_IO.lock().await;
    commit_on_primary(&md, JOB_TRIGGER_OP..=JOB_TRIGGER_OP).await;
    assert_eq!(jobs.borrow().len(), 1, "the job captured during the read");

    assert!(matches!(
        offer.await,
        Err(StateTransferUnavailable::SnapshotReplaced)
    ));
    assert!(
        md.transfer_offer_cache.borrow().is_none(),
        "a refused offer must not reach the cache"
    );
    drop(held);
    assert_eq!(await_jobs(&jobs).await, 1);
}

/// A job stopped at any crash boundary releases its lease, and an unstopped one
/// captures exactly the state of the commit that started it.
#[compio::test]
async fn given_a_job_when_it_stops_at_each_boundary_should_release_its_lease() {
    for boundary in [
        None,
        Some(CheckpointBoundary::TableCaptured),
        Some(CheckpointBoundary::TemporaryCreated),
        Some(CheckpointBoundary::Written),
        Some(CheckpointBoundary::FileSynced),
        Some(CheckpointBoundary::Renamed),
        Some(CheckpointBoundary::DirectorySynced),
        Some(CheckpointBoundary::SuperblockWritten),
        Some(CheckpointBoundary::WalDrained),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let superblock = Some(Rc::new(CheckpointSuperblock::default()));
        let (md, jobs) = job_metadata(dir.path(), 0, 1, superblock).await;
        let coordinator = md.coordinator.as_ref().unwrap();
        commit_on_primary(&md, 1..=JOB_TRIGGER_OP - 1).await;
        STOP_CHECKPOINT_AFTER.set(boundary);
        commit_on_primary(&md, JOB_TRIGGER_OP..=JOB_TRIGGER_OP).await;
        let started = await_jobs(&jobs).await;

        assert!(
            STOP_CHECKPOINT_AFTER.get().is_none(),
            "{boundary:?} must be reached"
        );
        assert!(
            md.checkpoint_lock.try_acquire().is_some(),
            "{boundary:?}: the lease must be released"
        );
        let captured = boundary != Some(CheckpointBoundary::TableCaptured);
        assert_eq!(started, usize::from(captured), "{boundary:?}");
        let paired = matches!(
            boundary,
            None | Some(CheckpointBoundary::SuperblockWritten | CheckpointBoundary::WalDrained)
        );
        assert_eq!(
            coordinator.superblock_checkpoint_op.get(),
            if paired { JOB_TRIGGER_OP } else { 0 },
            "{boundary:?}"
        );
        let trimmed = matches!(boundary, None | Some(CheckpointBoundary::WalDrained));
        assert_eq!(
            journal_floor(&md),
            if trimmed { JOB_TRIGGER_OP - 1 } else { 0 },
            "{boundary:?}"
        );
        assert_eq!(
            COMPLETED_CHECKPOINTS.get(),
            u64::from(boundary.is_none()),
            "{boundary:?}"
        );
        if boundary.is_none() {
            assert_snapshot_holds_live_state(&md);
        }
    }
}

#[compio::test]
async fn given_a_parked_job_when_appends_reach_the_margin_should_wait_and_trim_to_the_job_checkpoint_once()
 {
    let dir = tempfile::tempdir().unwrap();
    let (md, jobs) = job_metadata(dir.path(), 0, 1, None).await;
    let held = SNAPSHOT_IO.lock().await;
    commit_on_primary(&md, 1..=JOB_MARGIN_OP).await;

    let waiter = spawn_replicate(&md, JOB_MARGIN_OP + 1);
    yield_now().await;
    assert!(
        !journaled(&md, JOB_MARGIN_OP + 1),
        "the append at the margin waits for the job"
    );
    drop(held);
    waiter.await.unwrap();

    assert!(journaled(&md, JOB_MARGIN_OP + 1));
    assert_eq!(
        journal_floor(&md),
        JOB_TRIGGER_OP - 1,
        "the waiter trims to the job's checkpoint instead of checkpointing inline"
    );
    assert_eq!(COMPLETED_CHECKPOINTS.get(), 1);
    assert_eq!(await_jobs(&jobs).await, 1);
    assert_eq!(journal_floor(&md), JOB_TRIGGER_OP - 1);
    assert_eq!(
        COMPLETED_CHECKPOINTS.get(),
        1,
        "the job must not trim or log a checkpoint the waiter completed"
    );
    assert_eq!(
        LAST_COMPLETED_CHECKPOINT.get(),
        Some((CheckpointOrigin::Job, CheckpointTrimmer::Margin))
    );
    assert!(!md.coordinator.as_ref().unwrap().checkpoint_pending.get());
}

/// A job that fails backs off the next job, and a margin waiter that saw it fail
/// appends into the margin. Nobody trims, and the next prepare at the margin
/// checkpoints inline.
#[compio::test]
async fn given_a_failed_job_when_commits_continue_should_back_off_and_retry_inline_at_the_margin() {
    for superblock_fails in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let superblock = Rc::new(CheckpointSuperblock::default());
        let (md, jobs) = job_metadata(dir.path(), 0, 1, Some(Rc::clone(&superblock))).await;
        let coordinator = md.coordinator.as_ref().unwrap();
        let fail_next_job = || {
            if superblock_fails {
                superblock.fail.set(true);
            } else {
                STOP_CHECKPOINT_AFTER.set(Some(CheckpointBoundary::Written));
            }
        };
        let failed_checkpoint = |op| if superblock_fails { op } else { 0 };

        commit_on_primary(&md, 1..=JOB_TRIGGER_OP - 1).await;
        fail_next_job();
        commit_on_primary(&md, JOB_TRIGGER_OP..=JOB_TRIGGER_OP).await;
        assert_eq!(await_jobs(&jobs).await, 1);
        assert_eq!(
            coordinator.last_checkpoint().0,
            failed_checkpoint(JOB_TRIGGER_OP)
        );
        assert_eq!(coordinator.superblock_checkpoint_op.get(), 0);
        assert_eq!(journal_floor(&md), 0, "a failed job trims nothing");
        assert!(coordinator.checkpoint_pending.get());

        let retry_op = JOB_TRIGGER_OP + JOB_RETRY_OPS;
        commit_on_primary(&md, JOB_TRIGGER_OP + 1..=retry_op - 1).await;
        assert!(jobs.borrow().is_empty(), "no job during the backoff");
        let held = SNAPSHOT_IO.lock().await;
        commit_on_primary(&md, retry_op..=JOB_MARGIN_OP).await;
        assert_eq!(
            jobs.borrow().len(),
            1,
            "the next job starts after the backoff"
        );

        let waiter = spawn_replicate(&md, JOB_MARGIN_OP + 1);
        yield_now().await;
        fail_next_job();
        drop(held);
        waiter.await.unwrap();
        assert_eq!(await_jobs(&jobs).await, 1);
        assert!(
            journaled(&md, JOB_MARGIN_OP + 1),
            "the prepare appends into the margin"
        );
        assert_eq!(
            coordinator.last_checkpoint().0,
            failed_checkpoint(retry_op),
            "the waiter must not checkpoint inline after the job failed during its wait"
        );
        assert_eq!(journal_floor(&md), 0, "nobody trims after a failed job");
        assert_eq!(COMPLETED_CHECKPOINTS.get(), 0);

        superblock.fail.set(false);
        ack_self(&md).await;
        commit_on_primary(&md, JOB_MARGIN_OP + 2..=JOB_MARGIN_OP + 2).await;
        assert_eq!(coordinator.last_checkpoint().0, JOB_MARGIN_OP + 1);
        assert_eq!(journal_floor(&md), JOB_MARGIN_OP, "the inline retry trims");
        assert_eq!(COMPLETED_CHECKPOINTS.get(), 1);
        assert_eq!(
            LAST_COMPLETED_CHECKPOINT.get(),
            Some((CheckpointOrigin::Inline, CheckpointTrimmer::Margin))
        );
        assert!(jobs.borrow().is_empty());
    }
}

/// The backoff of a failed job counts from the commit at its failure, so
/// the commits during a slow write do not use it up.
#[compio::test]
async fn given_a_job_that_fails_after_more_commits_when_committing_should_back_off_from_the_failure()
 {
    let dir = tempfile::tempdir().unwrap();
    let (md, jobs) = job_metadata(dir.path(), 0, 1, None).await;
    let failed_at = JOB_TRIGGER_OP + JOB_RETRY_OPS;
    let retry_op = failed_at + JOB_RETRY_OPS;
    assert!(retry_op < JOB_MARGIN_OP, "the retry must stay a job");

    let held = SNAPSHOT_IO.lock().await;
    commit_on_primary(&md, 1..=failed_at).await;
    assert_eq!(jobs.borrow().len(), 1, "the job waits in its write");
    STOP_CHECKPOINT_AFTER.set(Some(CheckpointBoundary::Written));
    drop(held);
    assert_eq!(await_jobs(&jobs).await, 1);
    assert_eq!(md.coordinator.as_ref().unwrap().failed_jobs.get(), 1);

    commit_on_primary(&md, failed_at + 1..=retry_op - 1).await;
    assert!(jobs.borrow().is_empty(), "no job during the backoff");
    commit_on_primary(&md, retry_op..=retry_op).await;
    assert_eq!(await_jobs(&jobs).await, 1, "a job starts after the backoff");
}

/// A backup whose commits lag its journal starts one job per half WAL of
/// commits, not one per commit while its journal stays over half full.
#[compio::test]
async fn given_commits_lagging_the_journal_when_committing_should_start_one_job_per_half_wal() {
    const HEAD: u64 = JOB_MARGIN_OP - 2;
    let dir = tempfile::tempdir().unwrap();
    let (md, jobs) = job_metadata(dir.path(), 1, 3, None).await;
    let consensus = md.consensus.as_ref().unwrap();
    for op in 1..=HEAD {
        md.on_replicate(job_prepare(&md, op)).await;
    }
    let mut started = 0;
    for op in 1..=HEAD {
        consensus.advance_commit_max(op);
        md.commit_journal().await;
        started += await_jobs(&jobs).await;
        if op < JOB_TRIGGER_OP {
            assert_eq!(started, 0, "op {op}: a full journal alone starts no job");
        }
        if op == JOB_TRIGGER_OP {
            assert_snapshot_holds_live_state(&md);
        }
    }
    assert_eq!(consensus.commit_min(), HEAD);
    assert_eq!(started, 1);
    assert_eq!(journal_floor(&md), JOB_TRIGGER_OP - 1);
}

/// A journal at the margin gets its room from the next append, which trims
/// or checkpoints inline, so a commit there starts no job.
#[compio::test]
async fn given_a_journal_at_the_margin_when_half_the_wal_commits_should_start_no_job() {
    let dir = tempfile::tempdir().unwrap();
    let (md, jobs) = job_metadata(dir.path(), 1, 3, None).await;
    let consensus = md.consensus.as_ref().unwrap();
    for op in 1..=JOB_MARGIN_OP {
        md.on_replicate(job_prepare(&md, op)).await;
    }
    for op in 1..=JOB_TRIGGER_OP {
        consensus.advance_commit_max(op);
        md.commit_journal().await;
    }
    assert_eq!(consensus.commit_min(), JOB_TRIGGER_OP);
    assert!(jobs.borrow().is_empty(), "no job at the margin");
    assert!(!md.checkpoint_running());

    md.on_replicate(job_prepare(&md, JOB_MARGIN_OP + 1)).await;
    assert_eq!(
        journal_floor(&md),
        JOB_TRIGGER_OP - 1,
        "the next append checkpoints inline"
    );
    assert!(jobs.borrow().is_empty());
}

/// The install of a state transfer waits for a running job's write on the
/// shard-0 pump, so no job starts while a transfer is armed.
#[compio::test]
async fn given_an_armed_state_transfer_when_half_the_wal_commits_should_start_no_job() {
    const HEAD: u64 = JOB_TRIGGER_OP + 1;
    let dir = tempfile::tempdir().unwrap();
    let (md, jobs) = job_metadata(dir.path(), 1, 3, None).await;
    let consensus = md.consensus.as_ref().unwrap();
    for op in 1..=HEAD {
        md.on_replicate(job_prepare(&md, op)).await;
    }
    consensus.begin_state_transfer_await();
    for op in 1..=JOB_TRIGGER_OP {
        consensus.advance_commit_max(op);
        md.commit_journal().await;
    }
    assert_eq!(consensus.commit_min(), JOB_TRIGGER_OP);
    assert!(
        jobs.borrow().is_empty(),
        "no job while the transfer is armed"
    );
    assert!(!md.checkpoint_running());

    consensus.set_state_transfer_stage(consensus::StateTransferStage::Idle);
    consensus.advance_commit_max(HEAD);
    md.commit_journal().await;
    assert_eq!(
        await_jobs(&jobs).await,
        1,
        "a job starts once the transfer ended"
    );
}

/// A commit walk that waits for a prepare body keeps no borrow of the
/// journal index, so the trim of a checkpoint job can rebuild the index
/// meanwhile.
#[compio::test]
async fn given_a_commit_walk_reading_a_body_when_the_wal_is_trimmed_should_finish_the_walk() {
    const WALKED: u64 = 2;
    const HEAD: u64 = WALKED + 2;
    let dir = tempfile::tempdir().unwrap();
    let (md, jobs) = job_metadata(dir.path(), 1, 3, None).await;
    let consensus = md.consensus.as_ref().unwrap();
    for op in 1..=HEAD {
        md.on_replicate(job_prepare(&md, op)).await;
    }
    consensus.advance_commit_max(WALKED);
    assert_eq!(md.commit_journal().await, CommitWalkStop::CaughtUp);

    consensus.advance_commit_max(HEAD);
    let mut walk = std::pin::pin!(md.commit_journal());
    assert!(
        futures::poll!(walk.as_mut()).is_pending(),
        "the walk waits for the body of its first op"
    );
    md.journal
        .as_ref()
        .unwrap()
        .handle()
        .drain(1..=WALKED - 1)
        .await
        .unwrap();

    assert_eq!(walk.await, CommitWalkStop::CaughtUp);
    assert_eq!(consensus.commit_min(), HEAD);
    assert_eq!(journal_floor(&md), WALKED - 1);
    assert!(
        jobs.borrow().is_empty(),
        "no job this far below half the WAL"
    );
}

/// A job that finds `checkpoint_lock` taken once it holds `journal_gate`
/// leaves the trim to the holder.
#[compio::test]
async fn given_a_job_waiting_on_the_journal_gate_when_the_checkpoint_lock_is_taken_should_skip_its_trim()
 {
    let dir = tempfile::tempdir().unwrap();
    let (md, jobs) = job_metadata(dir.path(), 1, 3, None).await;
    let consensus = md.consensus.as_ref().unwrap();
    let coordinator = md.coordinator.as_ref().unwrap();
    for op in 1..=JOB_TRIGGER_OP {
        md.on_replicate(job_prepare(&md, op)).await;
    }
    let gate = md.journal_gate.try_acquire().unwrap();
    for op in 1..=JOB_TRIGGER_OP {
        consensus.advance_commit_max(op);
        md.commit_journal().await;
    }
    for _ in 0..JOB_POLL_BUDGET {
        if coordinator.superblock_checkpoint_op.get() == JOB_TRIGGER_OP {
            break;
        }
        compio::time::sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(
        coordinator.superblock_checkpoint_op.get(),
        JOB_TRIGGER_OP,
        "the job's checkpoint must be durable while it waits on the gate"
    );
    let newer_job = md.checkpoint_lock.try_acquire().unwrap();
    drop(gate);

    let mut ended = Box::pin(await_jobs(&jobs));
    let mut started = None;
    for _ in 0..JOB_POLL_BUDGET {
        if let std::task::Poll::Ready(count) = futures::poll!(&mut ended) {
            started = Some(count);
            break;
        }
        compio::time::sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(started, Some(1), "the job must end without the lock");
    assert_eq!(journal_floor(&md), 0, "the trim is the lock holder's");
    assert_eq!(COMPLETED_CHECKPOINTS.get(), 0);
    drop(newer_job);
}

/// The shard 0 pump keeps its appends for later only while a trim is
/// pending. The gate serves its waiters in arrival order, so the job must
/// report its trim before it waits: an append that arrives behind the
/// waiting job waits for the whole rewrite.
#[compio::test]
async fn given_a_job_waiting_on_the_journal_gate_when_asked_should_report_a_pending_trim_until_it_ends()
 {
    let dir = tempfile::tempdir().unwrap();
    let (md, jobs) = job_metadata(dir.path(), 1, 3, None).await;
    let consensus = md.consensus.as_ref().unwrap();
    for op in 1..=JOB_TRIGGER_OP {
        md.on_replicate(job_prepare(&md, op)).await;
    }
    let gate = md.journal_gate.try_acquire().unwrap();
    for op in 1..=JOB_TRIGGER_OP {
        consensus.advance_commit_max(op);
        md.commit_journal().await;
    }
    assert!(
        !md.checkpoint_trim_pending(),
        "a held gate alone is no pending trim"
    );
    for _ in 0..JOB_POLL_BUDGET {
        if md.checkpoint_trim_pending() {
            break;
        }
        compio::time::sleep(Duration::from_millis(1)).await;
    }
    assert!(
        md.checkpoint_trim_pending(),
        "a job that waits for the gate must report its trim"
    );

    drop(gate);
    assert_eq!(await_jobs(&jobs).await, 1);
    assert!(!md.checkpoint_trim_pending());
    assert_eq!(journal_floor(&md), JOB_TRIGGER_OP - 1);
}

/// An install waits for a running job's durable part, then moves the floor past
/// the job's checkpoint, so the job's later trim does nothing.
#[compio::test]
async fn given_a_running_job_when_a_snapshot_is_installed_should_wait_for_its_writes_and_skip_its_trim()
 {
    const HEAD: u64 = JOB_MARGIN_OP - 2;
    const INSTALLED: u64 = JOB_TRIGGER_OP + 10;
    let dir = tempfile::tempdir().unwrap();
    let (md, jobs) = job_metadata(dir.path(), 1, 3, None).await;
    let consensus = md.consensus.as_ref().unwrap();
    let coordinator = md.coordinator.as_ref().unwrap();
    for op in 1..=HEAD {
        md.on_replicate(job_prepare(&md, op)).await;
    }
    let held = SNAPSHOT_IO.lock().await;
    for op in 1..=JOB_TRIGGER_OP {
        consensus.advance_commit_max(op);
        md.commit_journal().await;
    }
    assert_eq!(jobs.borrow().len(), 1);

    let table = ClientTable::new(CLIENTS_TABLE_MAX);
    let mut snapshot = IggySnapshot::create(&TestMux::default(), INSTALLED, 1).unwrap();
    snapshot.snapshot_mut().client_table = Some(table.to_snapshot());
    let bytes = snapshot.encode().unwrap();
    let install = compio::runtime::spawn({
        let md = Rc::clone(&md);
        async move {
            md.install_state_transfer(&bytes, table, INSTALLED, INSTALLED)
                .await
        }
    });
    yield_now().await;
    assert!(
        md.journal_gate.try_acquire().is_none() && md.checkpoint_lock.try_acquire().is_none(),
        "the install holds the gate and waits for the job's lease"
    );
    drop(held);
    install.await.unwrap().unwrap();
    assert_eq!(await_jobs(&jobs).await, 1);

    assert_eq!(consensus.commit_min(), INSTALLED);
    assert_eq!(journal_floor(&md), INSTALLED);
    assert_eq!(coordinator.last_checkpoint().0, INSTALLED);
    assert_eq!(coordinator.superblock_checkpoint_op.get(), INSTALLED);
    let (snapshot, _) = IggySnapshot::load(&coordinator.snapshot_path()).unwrap();
    assert_eq!(
        snapshot.sequence_number(),
        INSTALLED,
        "the job's rename must not land over the installed snapshot"
    );
    assert_eq!(
        COMPLETED_CHECKPOINTS.get(),
        0,
        "the install moved the floor past the job's checkpoint"
    );
}

#[compio::test]
async fn given_state_recovery_would_refuse_when_persisting_a_checkpoint_should_write_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let metadata_dir = directory.path().join(crate::impls::METADATA_DIR);
    std::fs::create_dir_all(&metadata_dir).unwrap();
    let metadata: IggyMetadata<(), (), (), TestMux> = IggyMetadata::new(
        None,
        None,
        None,
        None,
        TestMux::default(),
        Some(directory.path().to_path_buf()),
    );
    // Root carries the revision of op 1, which a checkpoint at op 0 cannot hold.
    metadata.mux_stm.users().ensure_root_user("root", "hash");
    let coordinator = metadata.coordinator.as_ref().unwrap();

    let capture = coordinator
        .capture(
            &*metadata.mux_stm,
            0,
            0,
            None,
            metadata.change_log.snapshot(),
        )
        .unwrap();
    let persisted = coordinator.persist_capture(capture).await;

    assert!(matches!(persisted, Err(SnapshotError::InvalidState(_))));
    assert!(!coordinator.snapshot_path().exists());
    assert_eq!(coordinator.last_checkpoint(), (0, 0));
}

/// A checkpoint job holds `journal_gate` while it rewrites the WAL. A
/// request that the primary prepared meanwhile waited for the rewrite on
/// the shard 0 pump, and so did the promotion of a queued one.
#[compio::test]
async fn given_a_pending_wal_trim_when_a_request_arrives_should_queue_it_until_the_trim_ends() {
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
    let journal = md.journal.as_ref().unwrap().handle();

    // As a checkpoint job: it announces its trim, then takes the gate.
    let trim = md.announce_checkpoint_trim();
    let rewrite = md.hold_journal_writes().await;
    futures::FutureExt::now_or_never(md.on_request(create_stream_request(CLIENT, 1, "s1")))
        .expect("a request must not wait for the WAL trim on the pump");
    assert_eq!(consensus.request_queue_len(), 1);
    futures::FutureExt::now_or_never(md.resume_stranded_commits())
        .expect("the promotion must not wait for the WAL trim on the pump");
    assert_eq!(
        consensus.request_queue_len(),
        1,
        "no request is promoted while the trim is pending"
    );
    assert_eq!(journal.last_op(), None);

    drop(rewrite);
    drop(trim);
    md.resume_stranded_commits().await;

    assert_eq!(consensus.request_queue_len(), 0);
    assert_eq!(
        journal.last_op(),
        Some(consensus.sequencer().current_sequence()),
        "the tick promotes the queued request once the trim ended"
    );
}
