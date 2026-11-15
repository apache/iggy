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

//! Delete passes on a shard that hosts a tiered primary on disk.

use super::segment_uploads::{
    SEALED_SEGMENTS, floor, namespace, pumped, staged_revision, tiered_owner, until,
};
use super::*;
use crate::FinishedObjectDelete;
use futures::future::LocalBoxFuture;
use iggy_object_store::{
    InlineTierExecutor, KeyLayout, MemoryStore, SegmentKey, SegmentObject, TierExecutor,
    TierJobHandle, TierJobRequest, TierScopeToken,
};
use partitions::{
    ExecutorDeleter, ExecutorUploader, ObjectDeleteDone, ObjectDeleteJob, ObjectDeleteOutcome,
    ObjectDeleteWork, ObjectDeleter, RETIRED_OBJECT_GRACE, SweptObjects,
};

/// The last offset the fixture's truncation removes. Every segment holds one
/// message, so the segments at 0 and 1 go and the one at 2 stays.
const TRUNCATED_UP_TO: u64 = 1;

/// The objects of the fixture's sealed segments, a `.log` and an `.index`
/// each.
const UPLOADED_OBJECTS: usize = 6;

fn layout() -> KeyLayout {
    KeyLayout::new("segments").unwrap()
}

/// One executor for the uploader and the deleter, so both reach one store.
#[derive(Clone)]
struct Shared(Rc<InlineTierExecutor<MemoryStore>>);

impl TierExecutor for Shared {
    type Store = MemoryStore;

    fn submit<T, F, Fut>(&self, request: TierJobRequest, job: F) -> TierJobHandle<T>
    where
        T: 'static,
        F: FnOnce(Rc<Self::Store>) -> Fut + 'static,
        Fut: Future<Output = T> + 'static,
    {
        self.0.submit(request, job)
    }
}

/// Deletes in a memory store, and keeps what every job was asked to do.
struct RecordingDeleter {
    deleter: ExecutorDeleter<Shared>,
    /// Whether a job never answers, so it stays in flight.
    hangs: Cell<bool>,
    started: RefCell<Vec<ObjectDeleteWork>>,
}

impl RecordingDeleter {
    fn new(executor: Shared) -> Rc<Self> {
        Rc::new(Self {
            deleter: ExecutorDeleter::new(executor, layout()),
            hangs: Cell::new(false),
            started: RefCell::default(),
        })
    }

    fn started(&self) -> Vec<ObjectDeleteWork> {
        self.started.borrow().clone()
    }
}

impl ObjectDeleter for RecordingDeleter {
    fn run(
        &self,
        job: ObjectDeleteJob,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, ObjectDeleteOutcome> {
        self.started.borrow_mut().push(job.work.clone());
        if self.hangs.get() {
            Box::pin(std::future::pending())
        } else {
            self.deleter.run(job, scope)
        }
    }
}

/// A tiered primary whose three segments are uploaded, and whose first two a
/// committed truncation removed. The pump is stopped and the inbox is empty.
#[allow(clippy::future_not_send)]
async fn truncated_owner(directory: &Path) -> (IoTestShard, Shared, Rc<RecordingDeleter>) {
    let owner = tiered_owner(directory).await;
    let executor = Shared(Rc::new(InlineTierExecutor::new(MemoryStore::new())));
    owner.set_segment_uploader(Some(Rc::new(ExecutorUploader::new(
        executor.clone(),
        layout(),
    ))));
    pumped(&owner, async {
        super::segment_uploads::seal_segments(&owner).await;
        owner.request_upload_segments(namespace());
        until(&owner, |owner| floor(owner) == Some(SEALED_SEGMENTS - 1)).await;
        owner.request_truncate_partition(namespace(), TRUNCATED_UP_TO);
        until(&owner, |owner| {
            let partition = owner.plane.partitions().get_by_ns(&namespace()).unwrap();
            partition.log.segments()[0].start_offset == TRUNCATED_UP_TO + 1
                && partition.chain_trim_point() == Some(TRUNCATED_UP_TO)
        })
        .await;
    })
    .await;
    while owner.inbox.try_recv().is_ok() {}
    assert_eq!(executor.0.store().len(), UPLOADED_OBJECTS);
    let deleter = RecordingDeleter::new(executor.clone());
    owner.set_object_deleter(Some(deleter.clone()));
    (owner, executor, deleter)
}

/// The keys the store holds for the segment starting at `start_offset`.
fn keys_of(owner: &IoTestShard, start_offset: u64) -> Vec<String> {
    let prefix = owner
        .plane
        .partitions()
        .get_by_ns(&namespace())
        .unwrap()
        .object_prefix()
        .unwrap();
    [SegmentObject::Index, SegmentObject::Log]
        .map(|object| {
            layout().segment_key(&SegmentKey {
                prefix,
                start_offset,
                object,
            })
        })
        .to_vec()
}

/// The result frame of the job a pass started, taken off the inbox the way
/// the pump would.
#[allow(clippy::future_not_send)]
async fn finished_delete(owner: &IoTestShard) -> FinishedObjectDelete {
    until(owner, |owner| owner.inbox_len() > 0).await;
    match owner.inbox.try_recv() {
        Ok(ShardFrame::Lifecycle(LifecycleFrame::ObjectDeleteFinished(finished))) => *finished,
        _ => panic!("only the result of the job is on its way"),
    }
}

/// Run one pass at `now` and settle the job it started.
#[allow(clippy::future_not_send)]
async fn settled_pass(owner: &IoTestShard, now: IggyTimestamp) {
    owner.on_delete_objects(namespace(), staged_revision(owner), now, TRUNCATED_UP_TO);
    let finished = finished_delete(owner).await;
    owner.on_object_delete_finished(finished);
}

fn after(moment: IggyTimestamp, grace_periods: u32) -> IggyTimestamp {
    let grace = u64::try_from(RETIRED_OBJECT_GRACE.as_micros()).unwrap();
    IggyTimestamp::from(moment.as_micros() + u64::from(grace_periods) * grace)
}

/// Give a task that a pass spawned the time to reach the deleter, and check
/// that the deleter was asked for `jobs` jobs so far.
#[allow(clippy::future_not_send)]
async fn assert_started(owner: &IoTestShard, deleter: &RecordingDeleter, jobs: usize) {
    compio::time::sleep(TASK_POLL_INTERVAL * 10).await;
    assert_eq!(deleter.started().len(), jobs);
    assert_eq!(owner.inbox_len(), 0, "no result is on its way");
}

/// The sweep of the fixture's partition: the last sealed segment and the
/// active one are left, and the bound is the truncation.
fn sweep() -> ObjectDeleteWork {
    ObjectDeleteWork::Sweep {
        segment_starts: vec![TRUNCATED_UP_TO + 1, SEALED_SEGMENTS],
        bound: TRUNCATED_UP_TO,
    }
}

fn delete_of(start_offsets: &[u64]) -> ObjectDeleteWork {
    ObjectDeleteWork::Delete {
        start_offsets: start_offsets.to_vec(),
    }
}

#[compio::test]
async fn given_uploaded_segments_a_truncation_removed_when_delete_passes_run_should_delete_their_objects_after_the_grace_period()
 {
    let directory = tempfile::tempdir().unwrap();
    let before = IggyTimestamp::now();
    let (owner, executor, deleter) = Box::pin(truncated_owner(directory.path())).await;
    let store = executor.0.store();

    // The first pass lists. The removal queued both segments already.
    settled_pass(&owner, IggyTimestamp::now()).await;
    assert_eq!(deleter.started(), [sweep()]);

    // The removal happened after `before`, so its grace period is not over.
    let inside = IggyTimestamp::from(after(before, 1).as_micros() - 1);
    owner.on_delete_objects(
        namespace(),
        staged_revision(&owner),
        inside,
        TRUNCATED_UP_TO,
    );
    assert_started(&owner, &deleter, 1).await;
    assert_eq!(store.len(), UPLOADED_OBJECTS);

    let later = after(IggyTimestamp::now(), 1);
    settled_pass(&owner, later).await;
    assert_eq!(deleter.started()[1], delete_of(&[0, 1]));
    assert_eq!(store.keys(), keys_of(&owner, 2));

    // Nothing is queued any more, so a pass starts no job.
    owner.on_delete_objects(namespace(), staged_revision(&owner), later, TRUNCATED_UP_TO);
    assert_started(&owner, &deleter, 2).await;
    assert_eq!(store.keys(), keys_of(&owner, 2));
}

#[compio::test]
async fn given_no_deleter_when_a_delete_pass_runs_should_start_no_job() {
    let directory = tempfile::tempdir().unwrap();
    let (owner, executor, deleter) = Box::pin(truncated_owner(directory.path())).await;
    owner.set_object_deleter(None);

    owner.on_delete_objects(
        namespace(),
        staged_revision(&owner),
        IggyTimestamp::now(),
        TRUNCATED_UP_TO,
    );

    assert_started(&owner, &deleter, 0).await;
    // No job is in flight, so a pass with a deleter starts the first one.
    owner.set_object_deleter(Some(deleter.clone()));
    settled_pass(&owner, IggyTimestamp::now()).await;
    assert_eq!(deleter.started(), [sweep()]);
    assert_eq!(executor.0.store().len(), UPLOADED_OBJECTS);
}

/// Slab ids are reused, so a pass can reach a partition that took the place
/// of the one its watermark was read for.
#[compio::test]
async fn given_a_delete_pass_of_another_incarnation_when_it_runs_should_start_no_job() {
    let directory = tempfile::tempdir().unwrap();
    let (owner, _executor, deleter) = Box::pin(truncated_owner(directory.path())).await;
    let stale_revision = staged_revision(&owner);
    owner
        .plane
        .partitions()
        .get_mut_by_ns(&namespace())
        .unwrap()
        .set_created_revision(stale_revision + 1);
    let now = IggyTimestamp::now();

    owner.on_delete_objects(namespace(), stale_revision, now, TRUNCATED_UP_TO);
    assert_started(&owner, &deleter, 0).await;

    owner.on_delete_objects(namespace(), stale_revision + 1, now, TRUNCATED_UP_TO);
    until(&owner, |_| deleter.started() == [sweep()]).await;
}

/// Slab ids are reused, so a result can arrive at a partition that took the
/// place of the one that started the job.
#[compio::test]
async fn given_a_result_of_another_incarnation_when_it_arrives_should_not_settle_the_job() {
    let directory = tempfile::tempdir().unwrap();
    let (owner, _executor, deleter) = Box::pin(truncated_owner(directory.path())).await;
    deleter.hangs.set(true);
    let now = IggyTimestamp::now();
    owner.on_delete_objects(namespace(), staged_revision(&owner), now, TRUNCATED_UP_TO);
    until(&owner, |_| deleter.started() == [sweep()]).await;
    let created_revision = owner
        .plane
        .partitions()
        .get_by_ns(&namespace())
        .unwrap()
        .created_revision();
    let listed_nothing = |created_revision| FinishedObjectDelete {
        namespace: namespace(),
        created_revision,
        id: 1,
        outcome: Ok(ObjectDeleteDone::Swept(Ok(SweptObjects::default()))),
    };

    owner.on_object_delete_finished(listed_nothing(created_revision + 1));
    owner.on_delete_objects(
        namespace(),
        staged_revision(&owner),
        after(now, 2),
        TRUNCATED_UP_TO,
    );
    // The job is still in flight.
    assert_started(&owner, &deleter, 1).await;

    owner.on_object_delete_finished(listed_nothing(created_revision));
    owner.on_delete_objects(
        namespace(),
        staged_revision(&owner),
        after(now, 2),
        TRUNCATED_UP_TO,
    );
    until(&owner, |_| deleter.started().len() == 2).await;
    assert_eq!(deleter.started()[1], delete_of(&[0, 1]));
}

/// The partition starts no other job while it waits for a result, so a result
/// that the inbox refused has to reach it another way.
#[compio::test]
async fn given_a_full_inbox_when_a_job_finishes_should_settle_it_on_the_next_pass() {
    let directory = tempfile::tempdir().unwrap();
    let (owner, executor, deleter) = Box::pin(truncated_owner(directory.path())).await;
    while owner.senders[0]
        .try_send(ShardFrame::lifecycle(LifecycleFrame::ReconcileApply))
        .is_ok()
    {}
    let now = IggyTimestamp::now();

    owner.on_delete_objects(namespace(), staged_revision(&owner), now, TRUNCATED_UP_TO);
    until(&owner, |owner| {
        !owner.undelivered_object_deletes.borrow().is_empty()
    })
    .await;

    // The pass settles the listing and starts the delete it allows.
    owner.on_delete_objects(
        namespace(),
        staged_revision(&owner),
        after(now, 2),
        TRUNCATED_UP_TO,
    );
    assert!(owner.undelivered_object_deletes.borrow().is_empty());
    until(&owner, |owner| {
        !owner.undelivered_object_deletes.borrow().is_empty()
    })
    .await;
    assert_eq!(deleter.started(), [sweep(), delete_of(&[0, 1])]);
    assert_eq!(executor.0.store().keys(), keys_of(&owner, 2));

    // The delete's result waits the same way, and the next pass takes it.
    owner.on_delete_objects(
        namespace(),
        staged_revision(&owner),
        after(now, 2),
        TRUNCATED_UP_TO,
    );
    assert!(owner.undelivered_object_deletes.borrow().is_empty());
    compio::time::sleep(TASK_POLL_INTERVAL * 10).await;
    assert_eq!(deleter.started().len(), 2);
}
