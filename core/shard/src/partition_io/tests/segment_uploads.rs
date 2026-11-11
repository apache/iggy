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
use crate::{FinishedSegmentUpload, MetadataSubmit};
use futures::future::LocalBoxFuture;
use iggy_common::IggyExpiry;
use iggy_object_store::{
    InlineTierExecutor, KeyLayout, MemoryStore, ObjectStoreError, TierScopeToken,
};
use partitions::{
    ExecutorUploader, SegmentLocation, SegmentUpload, SegmentUploader, UploadError, UploadOutcome,
};

/// Sealed segments the fixture's primary holds, of one message each.
pub(super) const SEALED_SEGMENTS: u64 = 3;

/// The partition every test here uploads from.
pub(super) fn namespace() -> IggyNamespace {
    IggyNamespace::new(0, 0, 0)
}

/// What the fixture's store does with an upload.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Store {
    Stores,
    Refuses,
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

    owner.on_upload_segments(namespace(), true).await;

    assert!(!upload_in_flight(&owner));
}

#[compio::test]
async fn given_a_store_that_refuses_when_an_upload_finishes_should_leave_the_retry_to_the_next_pass()
 {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(sealed_owner(directory.path())).await;
    let uploader = RecordingUploader::new(Store::Refuses);
    owner.set_segment_uploader(Some(uploader.clone()));

    owner.on_upload_segments(namespace(), true).await;
    let finished = finished_upload(&owner).await;
    owner.on_segment_upload_finished(finished).await;

    assert!(
        owner.inbox.try_recv().is_err(),
        "a failure stages no pass of its own"
    );
    assert_eq!(locations(&owner), [SegmentLocation::Local; 3]);
    owner.on_upload_segments(namespace(), true).await;
    until(&owner, |_| uploader.started() == [0, 0]).await;
}

#[compio::test]
async fn given_a_stored_segment_when_its_upload_finishes_should_stage_the_next_pass_without_an_advance()
 {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(sealed_owner(directory.path())).await;
    owner.set_segment_uploader(Some(RecordingUploader::new(Store::Stores)));

    owner.on_upload_segments(namespace(), true).await;
    let finished = finished_upload(&owner).await;
    owner.on_segment_upload_finished(finished).await;

    assert!(matches!(
        owner.inbox.try_recv(),
        Ok(ShardFrame::Lifecycle(LifecycleFrame::UploadSegments {
            namespace: staged,
            resend_advance: false,
        })) if staged == namespace()
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
    owner.on_upload_segments(namespace(), true).await;
    let partition = owner.plane.partitions().get_by_ns(&namespace()).unwrap();
    let created_revision = partition.created_revision();
    let record = partition.sealed_segment_record(0).unwrap();

    owner
        .on_segment_upload_finished(FinishedSegmentUpload {
            namespace: namespace(),
            created_revision: created_revision + 1,
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

    owner.on_upload_segments(namespace(), true).await;
    until(&owner, |owner| {
        !owner.undelivered_segment_uploads.borrow().is_empty()
    })
    .await;
    assert!(upload_in_flight(&owner));
    assert_eq!(locations(&owner), [SegmentLocation::Local; 3]);

    owner.on_upload_segments(namespace(), true).await;

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
    owner.set_segment_uploader(Some(uploader.clone()));

    pumped(&owner, async {
        seal_segments(&owner).await;
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
                resend_advance: false,
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

    owner.on_retain_partition(
        namespace(),
        IggyTimestamp::now(),
        IggyExpiry::NeverExpire,
        None,
        0,
    );

    assert_eq!(retention_truncate(&owner), None);
}
