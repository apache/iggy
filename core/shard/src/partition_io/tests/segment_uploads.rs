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

//! Upload passes on a shard that hosts a tiered primary on disk.

use super::*;
use crate::upload_pacing::{TokenBucket, UPLOAD_LAG_ALARM, UPLOAD_RETRY_BASE, UPLOAD_RETRY_CAP};
use crate::{FinishedSegmentUpload, MetadataSubmit};
use futures::future::LocalBoxFuture;
use iggy_common::IggyExpiry;
use iggy_object_store::{
    InlineTierExecutor, KeyLayout, MemoryStore, ObjectStoreError, TierJobCancelled, TierScopeToken,
};
use partitions::{
    ExecutorUploader, LocalEnvelope, SegmentLocation, SegmentUpload, SegmentUploader, UploadError,
    UploadOutcome,
};
use std::time::Instant;

/// Sealed segments the fixture's primary holds, of one message each.
pub(super) const SEALED_SEGMENTS: u64 = 3;

/// The partition every test here uploads from.
pub(super) fn namespace() -> IggyNamespace {
    IggyNamespace::new(0, 0, 0)
}

/// The incarnation of the fixture's partition, which its passes are staged
/// for.
pub(super) fn staged_revision(owner: &IoTestShard) -> u64 {
    owner
        .plane
        .partitions()
        .get_by_ns(&namespace())
        .unwrap()
        .created_revision()
}

/// What the fixture's store does with an upload.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Store {
    Stores,
    Refuses,
    /// Fails in a way that no retry changes.
    Faults,
    /// Never answers, so the upload stays in flight.
    Hangs,
}

/// Uploads into a memory store, and keeps the first offset of every segment
/// it was asked for.
struct RecordingUploader {
    uploader: ExecutorUploader<InlineTierExecutor<MemoryStore>>,
    store: Cell<Store>,
    started: RefCell<Vec<u64>>,
}

impl RecordingUploader {
    fn new(store: Store) -> Rc<Self> {
        Rc::new(Self {
            uploader: ExecutorUploader::new(
                InlineTierExecutor::new(MemoryStore::new()),
                KeyLayout::new("segments").unwrap(),
            ),
            store: Cell::new(store),
            started: RefCell::default(),
        })
    }

    fn started(&self) -> Vec<u64> {
        self.started.borrow().clone()
    }
}

impl SegmentUploader for RecordingUploader {
    fn upload(
        &self,
        upload: SegmentUpload,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, UploadOutcome> {
        self.started.borrow_mut().push(upload.record.start_offset);
        match self.store.get() {
            Store::Stores => self.uploader.upload(upload, scope),
            Store::Refuses => Box::pin(std::future::ready(Ok(Err(UploadError::Store(
                ObjectStoreError::Unavailable("503".to_owned()),
            ))))),
            Store::Faults => Box::pin(std::future::ready(Ok(Err(UploadError::MediaFault {
                start_offset: upload.record.start_offset,
                reason: "its bytes hash to something else",
            })))),
            Store::Hangs => Box::pin(std::future::pending()),
        }
    }
}

/// A shard whose partition [`namespace`] is a tiered single-replica primary
/// on disk, with a segment size that seals every segment at its first batch.
#[allow(clippy::future_not_send)]
pub(super) async fn tiered_owner(directory: &Path) -> IoTestShard {
    let bus = Rc::new(IggyMessageBus::new(0));
    let (owner, _sender) = test_owner(&bus, None);
    let partitions = owner.plane.partitions();
    partitions.set_io_notifier(
        owner.partition_io.notifier(),
        owner.partition_io.limits.bytes_max(),
    );
    let partition = partitions.get_mut_by_ns(&namespace()).unwrap();
    let segment_size = IggyByteSize::from(1);
    partition.set_runtime_options(TopicRuntimeOptions {
        segment_size: Some(segment_size),
        messages_required_to_save: Some(1),
        ..Default::default()
    });
    attach_segment_files(partition, directory, false).await;
    partition.log.active_segment_mut().max_size = segment_size;
    partition.register_builtin_state_machines().unwrap();
    partition
        .open_segment_chain(partitions::object_prefix(namespace(), 0))
        .await
        .unwrap();
    owner
}

/// Run `body` beside the owner's pump, and stop the pump when it ends. A
/// shard runs its pump once, so everything that needs a commit goes in here.
#[allow(clippy::future_not_send)]
pub(super) async fn pumped<T>(owner: &IoTestShard, body: impl Future<Output = T>) -> T {
    let (stop, stopped) = channel(1);
    let pump = owner.run_message_pump(stopped, Arc::new(AtomicBool::new(false)));
    let body = async {
        let output = body.await;
        stop.try_send(()).unwrap();
        output
    };
    futures::join!(pump, body).1
}

/// Wait for the pump to bring the owner to `done`.
#[allow(clippy::future_not_send)]
pub(super) async fn until(owner: &IoTestShard, done: impl Fn(&IoTestShard) -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !done(owner) {
        assert!(
            std::time::Instant::now() < deadline,
            "the pump did not get there"
        );
        compio::time::sleep(TASK_POLL_INTERVAL).await;
    }
}

/// Commit [`SEALED_SEGMENTS`] sends, each of which seals its segment and
/// records it in the chain on the commit path.
#[allow(clippy::future_not_send)]
pub(super) async fn seal_segments(owner: &IoTestShard) {
    for request in 1..=SEALED_SEGMENTS {
        owner.senders[0]
            .try_send(ShardFrame::lifecycle(LifecycleFrame::PartitionSubmit {
                request: send_request(namespace(), request),
                reply: channel(1).0,
                attachment: None,
            }))
            .unwrap();
        until(owner, |owner| {
            let partition = owner.plane.partitions().get_by_ns(&namespace()).unwrap();
            let recorded = partition.log.segments().iter().filter(|segment| {
                segment.sealed
                    && partition
                        .sealed_segment_record(segment.start_offset)
                        .is_some()
            });
            recorded.count() as u64 == request
        })
        .await;
    }
}

/// [`tiered_owner`] with its segments sealed and its pump stopped, for the
/// tests that call the pump's handlers themselves.
#[allow(clippy::future_not_send)]
pub(super) async fn sealed_owner(directory: &Path) -> IoTestShard {
    let owner = tiered_owner(directory).await;
    pumped(&owner, seal_segments(&owner)).await;
    owner
}

pub(super) fn floor(owner: &IoTestShard) -> Option<u64> {
    owner
        .plane
        .partitions()
        .get_by_ns(&namespace())
        .unwrap()
        .tiered_floor()
}

fn head(owner: &IoTestShard) -> u64 {
    owner
        .plane
        .partitions()
        .get_by_ns(&namespace())
        .unwrap()
        .consensus()
        .sequencer()
        .current_sequence()
}

fn locations(owner: &IoTestShard) -> Vec<SegmentLocation> {
    owner
        .plane
        .partitions()
        .get_by_ns(&namespace())
        .unwrap()
        .log
        .segments()
        .iter()
        .filter(|segment| segment.sealed)
        .map(|segment| segment.location)
        .collect()
}

/// Whether the partition counts an upload as in flight.
fn upload_in_flight(owner: &IoTestShard) -> bool {
    let partition = owner
        .plane
        .partitions()
        .get_mut_by_ns(&namespace())
        .unwrap();
    let idle = partition.begin_segment_upload().is_some();
    if idle {
        partition.cancel_segment_upload();
    }
    !idle
}

/// The result frame of the upload a pass started, taken off the inbox the
/// way the pump would.
#[allow(clippy::future_not_send)]
async fn finished_upload(owner: &IoTestShard) -> FinishedSegmentUpload {
    until(owner, |owner| owner.inbox_len() > 0).await;
    match owner.inbox.try_recv() {
        Ok(ShardFrame::Lifecycle(LifecycleFrame::SegmentUploadFinished(finished))) => *finished,
        _ => panic!("only the upload result is on its way"),
    }
}

#[compio::test]
async fn given_sealed_segments_when_one_upload_pass_runs_should_store_them_all_and_raise_the_floor()
{
    let directory = tempfile::tempdir().unwrap();
    let owner = tiered_owner(directory.path()).await;
    let uploader = RecordingUploader::new(Store::Stores);
    owner.set_segment_uploader(Some(uploader.clone()));

    pumped(&owner, async {
        seal_segments(&owner).await;
        owner.request_upload_segments(namespace());
        until(&owner, |owner| floor(owner) == Some(SEALED_SEGMENTS - 1)).await;
    })
    .await;

    assert_eq!(uploader.started(), [0, 1, 2], "oldest first, each once");
    assert_eq!(locations(&owner), [SegmentLocation::Uploaded; 3]);
    // One advance per segment: the pass after an upload does not repeat the
    // advance that the upload just sent.
    assert_eq!(head(&owner), 2 * SEALED_SEGMENTS);
}

#[compio::test]
async fn given_no_uploader_when_an_upload_pass_runs_should_start_no_upload() {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(sealed_owner(directory.path())).await;

    owner
        .on_upload_segments(namespace(), staged_revision(&owner), true, None)
        .await;

    assert!(!upload_in_flight(&owner));
}

#[compio::test]
async fn given_a_store_that_refuses_when_passes_follow_should_retry_only_after_the_backoff() {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(sealed_owner(directory.path())).await;
    let uploader = RecordingUploader::new(Store::Refuses);
    owner.set_segment_uploader(Some(uploader.clone()));
    let failed_at = Instant::now();

    owner.upload_segments_at(namespace(), true, failed_at).await;
    let finished = finished_upload(&owner).await;
    owner.segment_upload_finished_at(finished, failed_at).await;

    assert!(
        owner.inbox.try_recv().is_err(),
        "a failure stages no pass of its own"
    );
    assert_eq!(locations(&owner), [SegmentLocation::Local; 3]);
    let too_early = failed_at + UPLOAD_RETRY_BASE.saturating_sub(Duration::from_millis(1));
    owner.upload_segments_at(namespace(), true, too_early).await;
    assert_eq!(uploader.started(), [0]);
    assert!(!upload_in_flight(&owner));

    owner
        .upload_segments_at(namespace(), true, failed_at + UPLOAD_RETRY_BASE)
        .await;
    until(&owner, |_| uploader.started() == [0, 0]).await;
}

#[compio::test]
async fn given_a_failed_upload_when_a_later_one_is_stored_should_not_hold_back_the_next_pass() {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(sealed_owner(directory.path())).await;
    let uploader = RecordingUploader::new(Store::Refuses);
    owner.set_segment_uploader(Some(uploader.clone()));
    let failed_at = Instant::now();
    owner.upload_segments_at(namespace(), true, failed_at).await;
    let finished = finished_upload(&owner).await;
    owner.segment_upload_finished_at(finished, failed_at).await;

    uploader.store.set(Store::Stores);
    let stored_at = failed_at + UPLOAD_RETRY_BASE;
    owner.upload_segments_at(namespace(), true, stored_at).await;
    let finished = finished_upload(&owner).await;
    owner.segment_upload_finished_at(finished, stored_at).await;
    uploader.store.set(Store::Refuses);
    let _staged = owner.inbox.try_recv();
    owner
        .upload_segments_at(namespace(), false, stored_at)
        .await;
    let finished = finished_upload(&owner).await;
    owner.segment_upload_finished_at(finished, stored_at).await;

    // The wait after the second failure is the base again, not its double.
    owner
        .upload_segments_at(namespace(), true, stored_at + UPLOAD_RETRY_BASE)
        .await;
    until(&owner, |_| uploader.started() == [0, 0, 1, 1]).await;
}

#[compio::test]
async fn given_an_upload_that_cannot_succeed_when_passes_follow_should_retry_only_after_the_longest_wait()
 {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(sealed_owner(directory.path())).await;
    let uploader = RecordingUploader::new(Store::Faults);
    owner.set_segment_uploader(Some(uploader.clone()));
    let failed_at = Instant::now();
    owner.upload_segments_at(namespace(), true, failed_at).await;
    let finished = finished_upload(&owner).await;
    owner.segment_upload_finished_at(finished, failed_at).await;

    let too_early = failed_at + UPLOAD_RETRY_CAP.saturating_sub(Duration::from_millis(1));
    owner.upload_segments_at(namespace(), true, too_early).await;
    assert_eq!(uploader.started(), [0]);
    assert!(!upload_in_flight(&owner));

    owner
        .upload_segments_at(namespace(), true, failed_at + UPLOAD_RETRY_CAP)
        .await;
    until(&owner, |_| uploader.started() == [0, 0]).await;
}

/// A cancelled upload says nothing about the store.
#[compio::test]
async fn given_a_cancelled_upload_when_the_next_pass_runs_should_not_make_it_wait() {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(sealed_owner(directory.path())).await;
    let uploader = RecordingUploader::new(Store::Hangs);
    owner.set_segment_uploader(Some(uploader.clone()));
    let cancelled_at = Instant::now();
    owner
        .upload_segments_at(namespace(), true, cancelled_at)
        .await;
    until(&owner, |_| uploader.started() == [0]).await;
    let partition = owner.plane.partitions().get_by_ns(&namespace()).unwrap();

    owner
        .segment_upload_finished_at(
            FinishedSegmentUpload {
                namespace: namespace(),
                created_revision: partition.created_revision(),
                attempt: partition.segment_upload_attempt().unwrap(),
                record: partition.sealed_segment_record(0).unwrap(),
                outcome: Err(TierJobCancelled),
            },
            cancelled_at,
        )
        .await;

    owner
        .upload_segments_at(namespace(), true, cancelled_at)
        .await;
    until(&owner, |_| uploader.started() == [0, 0]).await;
}

/// The segment of an upload the partition gave up is started again, and the
/// first upload's result can arrive while the second one runs.
#[compio::test]
async fn given_a_replaced_upload_when_the_first_one_fails_late_should_not_touch_the_replacement() {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(sealed_owner(directory.path())).await;
    let uploader = RecordingUploader::new(Store::Hangs);
    owner.set_segment_uploader(Some(uploader.clone()));
    let now = Instant::now();
    owner.upload_segments_at(namespace(), true, now).await;
    until(&owner, |_| uploader.started() == [0]).await;
    let partition = owner
        .plane
        .partitions()
        .get_mut_by_ns(&namespace())
        .unwrap();
    let created_revision = partition.created_revision();
    let given_up = partition.segment_upload_attempt().unwrap();
    let record = partition.sealed_segment_record(0).unwrap();
    partition.cancel_segment_upload();
    owner.upload_segments_at(namespace(), true, now).await;
    until(&owner, |_| uploader.started() == [0, 0]).await;
    let replacement = partition.segment_upload_attempt().unwrap();
    assert_ne!(replacement, given_up);

    owner
        .segment_upload_finished_at(
            FinishedSegmentUpload {
                namespace: namespace(),
                created_revision,
                attempt: given_up,
                record,
                outcome: Ok(Err(UploadError::Io(std::io::Error::other("late")))),
            },
            now,
        )
        .await;

    assert_eq!(partition.segment_upload_attempt(), Some(replacement));
    assert!(
        owner
            .upload_pacing
            .borrow_mut()
            .admit(namespace(), created_revision, 1, now),
        "a result nobody waits for makes no partition wait"
    );
}

/// The wait is kept on the shard, so it has to go when the partition does.
#[compio::test]
async fn given_a_failed_partition_when_a_timed_pass_no_longer_names_it_should_forget_its_wait() {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(sealed_owner(directory.path())).await;
    let uploader = RecordingUploader::new(Store::Refuses);
    owner.set_segment_uploader(Some(uploader.clone()));
    let failed_at = Instant::now();
    owner.upload_segments_at(namespace(), true, failed_at).await;
    let finished = finished_upload(&owner).await;
    owner.segment_upload_finished_at(finished, failed_at).await;

    owner.request_upload_passes(&[], IggyTimestamp::now());

    owner.upload_segments_at(namespace(), true, failed_at).await;
    until(&owner, |_| uploader.started() == [0, 0]).await;
}

#[compio::test]
async fn given_a_segment_that_seals_when_no_timed_pass_runs_should_upload_it() {
    let directory = tempfile::tempdir().unwrap();
    let owner = tiered_owner(directory.path()).await;
    let uploader = RecordingUploader::new(Store::Stores);
    owner.set_segment_uploader(Some(uploader.clone()));

    pumped(&owner, async {
        seal_segments(&owner).await;
        until(&owner, |owner| floor(owner) == Some(SEALED_SEGMENTS - 1)).await;
    })
    .await;

    assert_eq!(uploader.started(), [0, 1, 2], "oldest first, each once");
}

#[compio::test]
async fn given_an_upload_in_flight_when_later_segments_seal_should_not_start_theirs() {
    let directory = tempfile::tempdir().unwrap();
    let owner = tiered_owner(directory.path()).await;
    let uploader = RecordingUploader::new(Store::Hangs);
    owner.set_segment_uploader(Some(uploader.clone()));

    pumped(&owner, async {
        seal_segments(&owner).await;
        until(&owner, |owner| owner.inbox_len() == 0).await;
    })
    .await;

    assert_eq!(uploader.started(), [0]);
    assert!(upload_in_flight(&owner));
}

/// The oldest segment failed, so it is the backlog: the seals behind it ask
/// for passes, and none of them may start a younger segment.
#[compio::test]
async fn given_a_backlog_that_failed_when_later_segments_seal_should_not_overtake_it() {
    let directory = tempfile::tempdir().unwrap();
    let owner = tiered_owner(directory.path()).await;
    let uploader = RecordingUploader::new(Store::Refuses);
    owner.set_segment_uploader(Some(uploader.clone()));

    pumped(&owner, async {
        seal_segments(&owner).await;
        until(&owner, |owner| owner.inbox_len() == 0).await;
    })
    .await;

    let started = uploader.started();
    assert!(!started.is_empty(), "the first seal started its upload");
    assert!(started.iter().all(|start_offset| *start_offset == 0));
    assert_eq!(locations(&owner), [SegmentLocation::Local; 3]);
}

#[compio::test]
async fn given_an_empty_byte_budget_when_a_pass_runs_should_start_nothing_until_it_refills() {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(sealed_owner(directory.path())).await;
    let uploader = RecordingUploader::new(Store::Hangs);
    owner.set_segment_uploader(Some(uploader.clone()));
    let size = owner
        .plane
        .partitions()
        .get_by_ns(&namespace())
        .unwrap()
        .next_segment_upload()
        .unwrap()
        .record
        .size;
    let emptied_at = Instant::now();
    // One byte a second, and room for the segment many times over.
    let mut bucket = TokenBucket::full(1, size * 10, emptied_at);
    assert!(bucket.try_take(size * 10, emptied_at));
    owner.upload_pacing.borrow_mut().bucket = bucket;

    owner
        .upload_segments_at(namespace(), true, emptied_at)
        .await;
    let short = emptied_at + Duration::from_secs(size - 1);
    owner.upload_segments_at(namespace(), true, short).await;
    assert_eq!(uploader.started(), [] as [u64; 0]);
    assert!(!upload_in_flight(&owner));
    assert!(
        owner.inbox.try_recv().is_err(),
        "a refused pass stages no other"
    );

    let refilled = emptied_at + Duration::from_secs(size);
    owner.upload_segments_at(namespace(), true, refilled).await;
    until(&owner, |_| uploader.started() == [0]).await;
    assert_eq!(
        owner.upload_pacing.borrow_mut().bucket.tokens(refilled),
        0,
        "the upload paid for its bytes"
    );
}

/// A time `lag` after the newest message of the fixture's oldest segment.
fn after_oldest_segment(owner: &IoTestShard, lag: Duration) -> IggyTimestamp {
    let partition = owner.plane.partitions().get_by_ns(&namespace()).unwrap();
    let sent = partition.log.segments()[0].max_timestamp;
    IggyTimestamp::from(sent + u64::try_from(lag.as_micros()).unwrap())
}

/// The namespaces of the upload passes waiting in the inbox, in order.
fn staged_passes(owner: &IoTestShard) -> Vec<IggyNamespace> {
    std::iter::from_fn(|| match owner.inbox.try_recv() {
        Ok(ShardFrame::Lifecycle(LifecycleFrame::UploadSegments {
            namespace,
            resend_advance: true,
            committed_up_to: Some(_),
            ..
        })) => Some(namespace),
        Ok(_) => panic!("only timed upload passes are on their way"),
        Err(_) => None,
    })
    .collect()
}

#[compio::test]
async fn given_a_sealed_segment_when_timed_passes_run_should_report_its_age_until_it_is_uploaded() {
    let directory = tempfile::tempdir().unwrap();
    let owner = tiered_owner(directory.path()).await;
    let uploader = RecordingUploader::new(Store::Stores);

    pumped(&owner, async {
        seal_segments(&owner).await;
        let now = after_oldest_segment(&owner, Duration::from_secs(5));
        owner.request_upload_passes(&[(namespace(), staged_revision(&owner), 0)], now);
        assert_eq!(owner.metrics().segment_upload_lag_seconds_value(), 5);

        owner.set_segment_uploader(Some(uploader.clone()));
        owner.request_upload_segments(namespace());
        until(&owner, |owner| floor(owner) == Some(SEALED_SEGMENTS - 1)).await;
        owner.request_upload_passes(&[(namespace(), staged_revision(&owner), 0)], now);
        assert_eq!(owner.metrics().segment_upload_lag_seconds_value(), 0);
    })
    .await;
}

#[compio::test]
async fn given_a_lag_over_the_alarm_line_when_timed_passes_run_should_hold_the_alarm_until_it_recovers()
 {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(sealed_owner(directory.path())).await;
    // The partition without a backlog comes last, and must not hide the lag
    // of the one before it.
    let passes = [
        (namespace(), staged_revision(&owner), 0),
        (IggyNamespace::new(0, 0, 1), 0, 0),
    ];
    let lagging = |owner: &IoTestShard| owner.upload_pacing.borrow().is_lagging(namespace());
    let under = after_oldest_segment(
        &owner,
        UPLOAD_LAG_ALARM.saturating_sub(Duration::from_secs(1)),
    );
    let over = after_oldest_segment(&owner, UPLOAD_LAG_ALARM);

    owner.request_upload_passes(&[(namespace(), staged_revision(&owner), 0)], under);
    assert!(!lagging(&owner));
    owner.request_upload_passes(&passes, over);
    assert!(lagging(&owner));
    let alarm_secs = i64::try_from(UPLOAD_LAG_ALARM.as_secs()).unwrap();
    assert_eq!(
        owner.metrics().segment_upload_lag_seconds_value(),
        alarm_secs
    );

    let partition = owner
        .plane
        .partitions()
        .get_mut_by_ns(&namespace())
        .unwrap();
    while let Some(upload) = partition.next_segment_upload() {
        partition.note_segment_uploaded(&upload.record);
    }
    owner.request_upload_passes(&passes, over);
    assert!(!lagging(&owner));
    assert_eq!(owner.metrics().segment_upload_lag_seconds_value(), 0);
}

#[compio::test]
async fn given_a_partition_whose_eviction_waits_when_timed_passes_are_staged_should_put_it_first() {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(sealed_owner(directory.path())).await;
    let other = IggyNamespace::new(0, 0, 1);
    assert!(owner.plane.partitions().get_by_ns(&other).is_some());
    let now = IggyTimestamp::now();

    owner.request_upload_passes(
        &[(other, 0, 0), (namespace(), staged_revision(&owner), 0)],
        now,
    );
    assert_eq!(
        staged_passes(&owner),
        [other, namespace()],
        "nothing waits, so the order stands"
    );

    owner
        .plane
        .partitions()
        .get_mut_by_ns(&namespace())
        .unwrap()
        .set_local_envelope(Some(LocalEnvelope {
            age: IggyExpiry::NeverExpire,
            size: Some(0),
        }));
    owner.request_upload_passes(
        &[(other, 0, 0), (namespace(), staged_revision(&owner), 0)],
        now,
    );
    assert_eq!(staged_passes(&owner), [namespace(), other]);
}

/// A pass settles the results the inbox refused, whatever partition it is
/// for, so one is staged for a partition that is gone as well.
#[compio::test]
async fn given_a_partition_that_is_gone_when_timed_passes_are_staged_should_still_stage_its_pass() {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(sealed_owner(directory.path())).await;
    let absent = IggyNamespace::new(9, 9, 9);

    owner.request_upload_passes(
        &[(absent, 0, 0), (namespace(), staged_revision(&owner), 0)],
        IggyTimestamp::now(),
    );

    assert_eq!(staged_passes(&owner), [absent, namespace()]);
}

#[compio::test]
async fn given_a_stored_segment_when_its_upload_finishes_should_stage_the_next_pass_without_an_advance()
 {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(sealed_owner(directory.path())).await;
    owner.set_segment_uploader(Some(RecordingUploader::new(Store::Stores)));

    owner
        .on_upload_segments(namespace(), staged_revision(&owner), true, None)
        .await;
    let finished = finished_upload(&owner).await;
    owner.on_segment_upload_finished(finished).await;

    assert!(matches!(
        owner.inbox.try_recv(),
        Ok(ShardFrame::Lifecycle(LifecycleFrame::UploadSegments {
            namespace: staged,
            created_revision,
            resend_advance: false,
            committed_up_to: None,
        })) if staged == namespace() && created_revision == staged_revision(&owner)
    ));
    assert_eq!(locations(&owner)[0], SegmentLocation::Uploaded);
}

/// Slab ids are reused, so a result can arrive at a partition that took the
/// place of the one that started the upload.
#[compio::test]
async fn given_a_result_of_another_incarnation_when_it_arrives_should_not_settle_the_upload() {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(sealed_owner(directory.path())).await;
    owner.set_segment_uploader(Some(RecordingUploader::new(Store::Hangs)));
    owner
        .on_upload_segments(namespace(), staged_revision(&owner), true, None)
        .await;
    let partition = owner.plane.partitions().get_by_ns(&namespace()).unwrap();
    let created_revision = partition.created_revision();
    let attempt = partition.segment_upload_attempt().unwrap();
    let record = partition.sealed_segment_record(0).unwrap();

    owner
        .on_segment_upload_finished(FinishedSegmentUpload {
            namespace: namespace(),
            created_revision: created_revision + 1,
            attempt,
            record,
            outcome: Ok(Ok(())),
        })
        .await;
    assert_eq!(locations(&owner), [SegmentLocation::Local; 3]);
    assert!(upload_in_flight(&owner));

    owner
        .on_segment_upload_finished(FinishedSegmentUpload {
            namespace: namespace(),
            created_revision,
            attempt,
            record,
            outcome: Ok(Ok(())),
        })
        .await;
    assert_eq!(locations(&owner)[0], SegmentLocation::Uploaded);
}

/// The partition starts no upload while it waits for a result, so a result
/// that the inbox refused has to reach it another way.
#[compio::test]
async fn given_a_full_inbox_when_an_upload_finishes_should_settle_it_on_the_next_pass() {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(sealed_owner(directory.path())).await;
    let uploader = RecordingUploader::new(Store::Stores);
    owner.set_segment_uploader(Some(uploader.clone()));
    while owner.senders[0]
        .try_send(ShardFrame::lifecycle(LifecycleFrame::ReconcileApply))
        .is_ok()
    {}

    owner
        .on_upload_segments(namespace(), staged_revision(&owner), true, None)
        .await;
    until(&owner, |owner| {
        !owner.undelivered_segment_uploads.borrow().is_empty()
    })
    .await;
    assert!(upload_in_flight(&owner));
    assert_eq!(locations(&owner), [SegmentLocation::Local; 3]);

    owner
        .on_upload_segments(namespace(), staged_revision(&owner), true, None)
        .await;

    assert!(owner.undelivered_segment_uploads.borrow().is_empty());
    assert_eq!(locations(&owner)[0], SegmentLocation::Uploaded);
    until(&owner, |_| uploader.started() == [0, 1]).await;
}

/// The pass that settles a result has sent that segment's advance, which the
/// segment still owes until it commits.
#[compio::test]
async fn given_an_undelivered_result_when_a_timed_pass_settles_it_should_send_its_advance_once() {
    let directory = tempfile::tempdir().unwrap();
    let owner = tiered_owner(directory.path()).await;
    let uploader = RecordingUploader::new(Store::Hangs);
    owner.set_segment_uploader(Some(uploader.clone()));

    pumped(&owner, async {
        seal_segments(&owner).await;
        owner.request_upload_segments(namespace());
        until(&owner, |_| uploader.started() == [0]).await;
        let partition = owner.plane.partitions().get_by_ns(&namespace()).unwrap();
        owner
            .undelivered_segment_uploads
            .borrow_mut()
            .push(FinishedSegmentUpload {
                namespace: namespace(),
                created_revision: partition.created_revision(),
                attempt: partition.segment_upload_attempt().unwrap(),
                record: partition.sealed_segment_record(0).unwrap(),
                outcome: Ok(Ok(())),
            });

        owner.request_upload_segments(namespace());
        until(&owner, |owner| floor(owner) == Some(0)).await;
    })
    .await;

    assert_eq!(head(&owner), SEALED_SEGMENTS + 1);
    assert_eq!(uploader.started(), [0, 1]);
}

/// The partition skips a stored segment when it picks the next upload, so
/// only the pass can send an advance that was refused or lost.
#[compio::test]
async fn given_a_stored_segment_whose_advance_was_lost_when_a_timed_pass_runs_should_send_it_again()
{
    let directory = tempfile::tempdir().unwrap();
    let owner = tiered_owner(directory.path()).await;
    let uploader = RecordingUploader::new(Store::Hangs);

    pumped(&owner, async {
        seal_segments(&owner).await;
        // Set after the seals, each of which would have started an upload.
        owner.set_segment_uploader(Some(uploader.clone()));
        let partition = owner
            .plane
            .partitions()
            .get_mut_by_ns(&namespace())
            .unwrap();
        let record = partition.sealed_segment_record(0).unwrap();
        partition.note_segment_uploaded(&record);

        // The pass after an upload starts the next one and leaves the
        // advance alone: the upload that staged it sent that advance.
        owner.senders[0]
            .try_send(ShardFrame::lifecycle(LifecycleFrame::UploadSegments {
                namespace: namespace(),
                created_revision: staged_revision(&owner),
                resend_advance: false,
                committed_up_to: None,
            }))
            .unwrap();
        until(&owner, |_| uploader.started() == [1]).await;
        assert_eq!(head(&owner), SEALED_SEGMENTS);

        owner.request_upload_segments(namespace());
        until(&owner, |owner| floor(owner) == Some(record.end_offset)).await;
    })
    .await;

    assert_eq!(head(&owner), SEALED_SEGMENTS + 1);
    assert_eq!(
        uploader.started(),
        [1],
        "the stored segment is not sent again"
    );
}

/// The retention decision a pass handed to shard 0, as `(created revision,
/// last offset)`, taken off the inbox the way the pump would.
fn retention_truncate(owner: &IoTestShard) -> Option<(u64, u64)> {
    match owner.inbox.try_recv() {
        Ok(ShardFrame::Lifecycle(LifecycleFrame::MetadataSubmit(
            MetadataSubmit::RetentionTruncate {
                namespace: decided,
                created_revision,
                up_to_offset,
            },
        ))) if decided == namespace() => Some((created_revision, up_to_offset)),
        Ok(_) => panic!("only a retention decision is on its way"),
        Err(_) => None,
    }
}

#[compio::test]
async fn given_a_tiered_primary_over_budget_when_a_retention_pass_runs_should_hand_shard_zero_the_offset_once()
 {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(sealed_owner(directory.path())).await;
    let now = IggyTimestamp::now();
    let created_revision = owner
        .plane
        .partitions()
        .get_by_ns(&namespace())
        .unwrap()
        .created_revision();
    let last_sealed = SEALED_SEGMENTS - 1;
    let pass = |committed_up_to| {
        owner.on_retain_partition(
            namespace(),
            created_revision,
            now,
            IggyExpiry::NeverExpire,
            Some(0),
            committed_up_to,
        );
        retention_truncate(&owner)
    };

    assert_eq!(pass(0), Some((created_revision, last_sealed)));
    assert_eq!(pass(last_sealed - 1), Some((created_revision, last_sealed)));
    // The decision is committed already, so the pass sends nothing.
    assert_eq!(pass(last_sealed), None);
    assert_eq!(pass(last_sealed + 1), None);
    assert_eq!(
        locations(&owner),
        [SegmentLocation::Local; 3],
        "the pass itself removes nothing"
    );
}

#[compio::test]
async fn given_a_tiered_primary_within_its_policy_when_a_retention_pass_runs_should_send_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(sealed_owner(directory.path())).await;
    let created_revision = owner
        .plane
        .partitions()
        .get_by_ns(&namespace())
        .unwrap()
        .created_revision();

    owner.on_retain_partition(
        namespace(),
        created_revision,
        IggyTimestamp::now(),
        IggyExpiry::NeverExpire,
        None,
        0,
    );

    assert_eq!(retention_truncate(&owner), None);
}

/// The cleaner reads a policy for one incarnation, and the pump can take the
/// frame after the topic was deleted and its slots were given to another.
#[compio::test]
async fn given_a_retention_pass_of_another_incarnation_when_it_runs_should_send_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(sealed_owner(directory.path())).await;
    let now = IggyTimestamp::now();
    let created_revision = owner
        .plane
        .partitions()
        .get_by_ns(&namespace())
        .unwrap()
        .created_revision();
    let pass = |staged_revision| {
        owner.on_retain_partition(
            namespace(),
            staged_revision,
            now,
            IggyExpiry::NeverExpire,
            Some(0),
            0,
        );
        retention_truncate(&owner)
    };

    assert_eq!(pass(created_revision + 1), None);
    assert_eq!(
        pass(created_revision),
        Some((created_revision, SEALED_SEGMENTS - 1))
    );

    // The partition the slots hold now is not the one the pass was staged for.
    owner
        .plane
        .partitions()
        .get_mut_by_ns(&namespace())
        .unwrap()
        .set_created_revision(created_revision + 1);
    assert_eq!(pass(created_revision), None);
}

/// The primary of the fixture after its first two segments were removed and
/// the chain recorded the trim point that left, with no truncation
/// committed: what a replica is left with when its own retention ran before
/// the topic was tiered. Returns the trim point.
#[allow(clippy::future_not_send)]
async fn owner_with_uncommitted_trim_point(directory: &Path) -> (IoTestShard, u64) {
    let owner = tiered_owner(directory).await;
    let trim_point = 1;
    pumped(&owner, async {
        seal_segments(&owner).await;
        let partitions = owner.plane.partitions();
        let partition = partitions.get_mut_by_ns(&namespace()).unwrap();
        Box::pin(partition.remove_sealed_segments_up_to(trim_point)).await;
        until(&owner, |owner| {
            let partition = owner.plane.partitions().get_by_ns(&namespace()).unwrap();
            partition.log.segments()[0].start_offset == trim_point + 1
        })
        .await;
        partitions
            .get_mut_by_ns(&namespace())
            .unwrap()
            .enable_tiering(partitions::object_prefix(namespace(), 0));
        until(&owner, |owner| {
            let partition = owner.plane.partitions().get_by_ns(&namespace()).unwrap();
            partition.chain_trim_point() == Some(trim_point)
        })
        .await;
    })
    .await;
    while owner.inbox.try_recv().is_ok() {}
    (owner, trim_point)
}

#[compio::test]
async fn given_a_trim_point_nothing_committed_when_timed_passes_run_should_propose_it_and_upload_only_once_it_is_committed()
 {
    let directory = tempfile::tempdir().unwrap();
    let (owner, trim_point) = Box::pin(owner_with_uncommitted_trim_point(directory.path())).await;
    let created_revision = owner
        .plane
        .partitions()
        .get_by_ns(&namespace())
        .unwrap()
        .created_revision();

    owner
        .on_upload_segments(namespace(), staged_revision(&owner), true, Some(0))
        .await;
    assert_eq!(
        retention_truncate(&owner),
        None,
        "a shard that uploads nothing has no truncation to ask for"
    );

    let uploader = RecordingUploader::new(Store::Hangs);
    owner.set_segment_uploader(Some(uploader.clone()));
    // The pass a seal or a stored upload stages knows no watermark.
    owner
        .on_upload_segments(namespace(), staged_revision(&owner), false, None)
        .await;
    assert_eq!(retention_truncate(&owner), None);
    assert_eq!(uploader.started(), Vec::<u64>::new());

    owner
        .on_upload_segments(namespace(), staged_revision(&owner), true, Some(0))
        .await;
    assert_eq!(
        retention_truncate(&owner),
        Some((created_revision, trim_point))
    );
    owner
        .on_upload_segments(
            namespace(),
            staged_revision(&owner),
            true,
            Some(trim_point - 1),
        )
        .await;
    assert_eq!(
        retention_truncate(&owner),
        Some((created_revision, trim_point))
    );
    assert_eq!(uploader.started(), Vec::<u64>::new());

    owner
        .on_upload_segments(namespace(), staged_revision(&owner), true, Some(trim_point))
        .await;
    assert_eq!(
        retention_truncate(&owner),
        None,
        "the truncation is committed, so it is not asked for again"
    );
    until(&owner, |_| uploader.started() == [trim_point + 1]).await;
}

/// The timer task reads a watermark for one incarnation, and the pump can take
/// the frame after the topic was deleted and its slots were given to another.
#[compio::test]
async fn given_an_upload_pass_of_another_incarnation_when_it_runs_should_neither_note_its_watermark_nor_commit()
 {
    let directory = tempfile::tempdir().unwrap();
    let (owner, trim_point) = Box::pin(owner_with_uncommitted_trim_point(directory.path())).await;
    let uploader = RecordingUploader::new(Store::Hangs);
    owner.set_segment_uploader(Some(uploader.clone()));
    let stale_revision = staged_revision(&owner);
    // The partition the slots hold now is not the one the passes were staged
    // for, and nothing committed its trim point as deleted.
    let created_revision = stale_revision + 1;
    owner
        .plane
        .partitions()
        .get_mut_by_ns(&namespace())
        .unwrap()
        .set_created_revision(created_revision);

    // A watermark below the trim point would have the recreated topic
    // truncated on the word of the deleted one.
    owner
        .on_upload_segments(namespace(), stale_revision, true, Some(0))
        .await;
    assert_eq!(retention_truncate(&owner), None);
    // One that covers the trim point would let an advance pass over offsets
    // nothing committed as deleted here.
    owner
        .on_upload_segments(namespace(), stale_revision, true, Some(trim_point))
        .await;
    owner
        .on_upload_segments(namespace(), stale_revision, false, None)
        .await;
    let partition = owner.plane.partitions().get_by_ns(&namespace()).unwrap();
    assert!(
        partition.segment_upload_attempt().is_none(),
        "no upload starts on a watermark of another incarnation"
    );
    assert_eq!(partition.trim_point_to_commit(0), Some(trim_point));
    assert!(partition.pending_floor_advance().is_none());

    // A pass staged for the incarnation the slots hold still works.
    owner
        .on_upload_segments(namespace(), created_revision, true, Some(0))
        .await;
    assert_eq!(
        retention_truncate(&owner),
        Some((created_revision, trim_point))
    );
    owner
        .on_upload_segments(namespace(), created_revision, true, Some(trim_point))
        .await;
    until(&owner, |_| uploader.started() == [trim_point + 1]).await;
}

#[compio::test]
async fn given_a_committed_trim_point_when_a_pass_without_the_watermark_follows_should_still_upload()
 {
    let directory = tempfile::tempdir().unwrap();
    let (owner, trim_point) = Box::pin(owner_with_uncommitted_trim_point(directory.path())).await;
    let uploader = RecordingUploader::new(Store::Refuses);
    owner.set_segment_uploader(Some(uploader.clone()));
    let failed_at = Instant::now();

    owner
        .on_upload_segments(namespace(), staged_revision(&owner), true, Some(trim_point))
        .await;
    let finished = finished_upload(&owner).await;
    owner.segment_upload_finished_at(finished, failed_at).await;
    assert_eq!(uploader.started(), [trim_point + 1]);

    owner
        .upload_segments_at(namespace(), false, failed_at + UPLOAD_RETRY_BASE)
        .await;
    until(&owner, |_| {
        uploader.started() == [trim_point + 1, trim_point + 1]
    })
    .await;
}
