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

//! The upload-first produce path on a shard that hosts a primary which
//! places its batches.

use super::segment_uploads::{namespace, pumped, until};
use super::*;
use futures::future::LocalBoxFuture;
use iggy_binary_protocol::placement::{PLACED_BATCH_SIZE, PlacedBatch};
use iggy_object_store::{
    FreshObjectId, InlineTierExecutor, KeyLayout, MemoryStore, ObjectStore, ObjectStoreError,
    TierScopeToken, is_fresh_object,
};
use partitions::{ExecutorFreshUploader, FreshObjectUploader, FreshUploadOutcome};

fn layout() -> KeyLayout {
    KeyLayout::new("fresh-test").unwrap()
}

type MemoryUploader = ExecutorFreshUploader<InlineTierExecutor<MemoryStore>>;

/// Uploads into a memory store, unless told to refuse.
struct TestUploader {
    uploader: MemoryUploader,
    refuses: Cell<bool>,
}

impl TestUploader {
    fn new() -> Rc<Self> {
        Rc::new(Self {
            uploader: ExecutorFreshUploader::new(
                InlineTierExecutor::new(MemoryStore::new()),
                layout(),
            ),
            refuses: Cell::new(false),
        })
    }

    fn store(&self) -> &MemoryStore {
        self.uploader.executor().store()
    }
}

impl FreshObjectUploader for TestUploader {
    fn upload(
        &self,
        object_id: FreshObjectId,
        bytes: bytes::Bytes,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, FreshUploadOutcome> {
        if self.refuses.get() {
            Box::pin(std::future::ready(Ok(Err(ObjectStoreError::Unavailable(
                "503".to_owned(),
            )))))
        } else {
            self.uploader.upload(object_id, bytes, scope)
        }
    }
}

/// A shard whose partition [`namespace`] is a single-replica primary on disk
/// that places its batches.
#[allow(clippy::future_not_send)]
async fn placing_owner(directory: &Path) -> IoTestShard {
    let bus = Rc::new(IggyMessageBus::new(0));
    let (owner, _sender) = test_owner(&bus, None);
    let partitions = owner.plane.partitions();
    partitions.set_io_notifier(
        owner.partition_io.notifier(),
        owner.partition_io.limits.bytes_max(),
    );
    let partition = partitions.get_mut_by_ns(&namespace()).unwrap();
    partition.set_runtime_options(TopicRuntimeOptions {
        messages_required_to_save: Some(1),
        places_batches: true,
        ..Default::default()
    });
    attach_segment_files(partition, directory, false).await;
    partition.register_builtin_state_machines().unwrap();
    owner
}

/// Submit one send of a message of `payload_len` bytes, as a producer does.
/// Returns where its answer arrives.
fn submit_send(
    owner: &IoTestShard,
    request: u64,
    payload_len: usize,
) -> Receiver<Option<Message<GenericHeader>>> {
    let (reply, answer) = channel(1);
    owner.senders[0]
        .try_send(ShardFrame::lifecycle(LifecycleFrame::PartitionSubmit {
            request: sized_send_request(namespace(), request, payload_len),
            reply,
            attachment: None,
        }))
        .unwrap();
    answer
}

/// Ops the partition committed and wrote to its segment. Each send is one.
fn committed_sends(owner: &IoTestShard) -> u64 {
    let partition = owner.plane.partitions().get_by_ns(&namespace()).unwrap();
    let written = partition.log.active_segment().size.as_bytes_u64() / PLACED_BATCH_SIZE as u64;
    partition.consensus().commit_min().min(written)
}

/// The placed batches the partition's segment holds.
fn stored_placed_batches(directory: &Path) -> Vec<PlacedBatch> {
    std::fs::read(directory.join("00000000000000000000.log"))
        .unwrap()
        .chunks(PLACED_BATCH_SIZE)
        .map(|placed| PlacedBatch::decode(placed).unwrap())
        .collect()
}

#[compio::test]
async fn given_sends_to_a_placing_primary_when_the_window_closes_should_store_their_messages_and_commit_where_they_sit()
 {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let uploader = TestUploader::new();
    owner.set_fresh_uploader(Some(uploader.clone()));

    let answers = pumped(&owner, async {
        let answers = [submit_send(&owner, 1, 100), submit_send(&owner, 2, 300)];
        until(&owner, |owner| committed_sends(owner) == 2).await;
        answers
    })
    .await;

    for answer in answers {
        assert!(
            answer.try_recv().unwrap().is_some(),
            "each producer is answered once its request commits"
        );
    }
    let store = uploader.store();
    let keys = store.keys();
    assert_eq!(keys.len(), 1, "both sends came inside one window: {keys:?}");
    let object_id = layout().parse_fresh_key(&keys[0]).unwrap();
    let size = store.head(&keys[0]).await.unwrap().size;
    let object = store.get_range(&keys[0], 0..size).await.unwrap().bytes;
    assert!(is_fresh_object(&object));
    let placed = stored_placed_batches(directory.path());
    assert_eq!(placed.len(), 2);
    for (offset, placed) in (0..).zip(&placed) {
        assert_eq!(placed.header.base_offset, offset);
        assert_eq!(placed.placement.object_id, object_id);
        let start = usize::try_from(placed.placement.position).unwrap();
        let end = start + usize::try_from(placed.placement.length).unwrap();
        assert!(
            placed.placement.matches(&object[start..end]),
            "the placement at offset {offset} names its own messages"
        );
    }
    assert!(placed[0].placement.length < placed[1].placement.length);
    assert_eq!(
        placed[0].placement.position + placed[0].placement.length,
        placed[1].placement.position
    );
}

#[compio::test]
async fn given_a_store_that_refuses_when_the_window_closes_should_answer_its_producers_and_commit_nothing()
 {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let uploader = TestUploader::new();
    uploader.refuses.set(true);
    owner.set_fresh_uploader(Some(uploader.clone()));

    let answer = pumped(&owner, async {
        let answer = submit_send(&owner, 1, 100);
        until(&owner, |_| !answer.is_empty()).await;
        answer
    })
    .await;

    assert!(answer.try_recv().unwrap().is_some());
    assert_eq!(committed_sends(&owner), 0);
    assert_eq!(uploader.store().keys(), Vec::<String>::new());
}

#[compio::test]
async fn given_no_uploader_when_a_send_arrives_should_answer_it_at_once_and_keep_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;

    let answer = pumped(&owner, async {
        let answer = submit_send(&owner, 1, 100);
        until(&owner, |_| !answer.is_empty()).await;
        answer
    })
    .await;

    assert!(answer.try_recv().unwrap().is_some());
    assert_eq!(committed_sends(&owner), 0);
}

#[compio::test]
async fn given_sends_in_two_windows_when_both_close_should_store_two_objects_and_keep_the_order() {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let uploader = TestUploader::new();
    owner.set_fresh_uploader(Some(uploader.clone()));

    pumped(&owner, async {
        let _first = submit_send(&owner, 1, 100);
        until(&owner, |owner| committed_sends(owner) == 1).await;
        let _second = submit_send(&owner, 2, 100);
        until(&owner, |owner| committed_sends(owner) == 2).await;
    })
    .await;

    let mut keys = uploader.store().keys();
    keys.sort();
    assert_eq!(keys.len(), 2);
    let ids: Vec<_> = keys
        .iter()
        .map(|key| layout().parse_fresh_key(key).unwrap())
        .collect();
    let placed = stored_placed_batches(directory.path());
    assert_eq!(
        placed
            .iter()
            .map(|placed| placed.placement.object_id)
            .collect::<Vec<_>>(),
        ids,
        "the later window got the later id, and committed second"
    );
}

/// The shard holds the messages it has just stored, so a consumer at the
/// tail is served without a read of the store. The store the polls would
/// read here is empty, so only those kept bytes can answer.
#[compio::test]
async fn given_sends_this_shard_stored_when_polling_them_should_serve_them_without_reading_the_store()
 {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let uploader = TestUploader::new();
    owner.set_fresh_uploader(Some(uploader.clone()));
    owner
        .plane
        .partitions()
        .set_remote_segments(Rc::new(partitions::RemoteSegments::new(
            Rc::new(MemoryStore::new()),
            layout(),
        )));
    pumped(&owner, async {
        let _answers = [submit_send(&owner, 1, 100), submit_send(&owner, 2, 300)];
        until(&owner, |owner| committed_sends(owner) == 2).await;
    })
    .await;

    let plan = owner
        .plane
        .partitions()
        .build_poll_snapshot(
            &namespace(),
            partitions::PollingConsumer::Consumer(1, 0),
            &partitions::PollingArgs::new(iggy_common::PollingStrategy::offset(0), 10, false),
        )
        .unwrap();
    assert!(plan.needs_off_pump_io());
    let polled = plan.execute().await.checked().unwrap();

    assert_eq!(polled.message_count(), 2);
    assert_eq!(polled.last_matching_offset(), Some(1));
}

/// The whole write path on one shard: a producer's sends are stored and
/// committed as placed batches, the rewrite pass stores them again as a
/// long-term segment, and the partition's long-term log names it.
#[compio::test]
async fn given_committed_sends_when_a_rewrite_pass_runs_should_store_and_publish_a_long_term_segment()
 {
    use super::object_deletes::Shared;
    use iggy_object_store::{SegmentKey, SegmentObject};
    use partitions::{ExecutorLongTermWriter, ExecutorObjects, RemoteSegments};

    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let executor = Shared(Rc::new(InlineTierExecutor::new(MemoryStore::new())));
    owner.set_fresh_uploader(Some(Rc::new(ExecutorFreshUploader::new(
        executor.clone(),
        layout(),
    ))));
    owner.set_long_term_writer(Some(Rc::new(ExecutorLongTermWriter::new(
        executor.clone(),
        layout(),
    ))));
    let partitions = owner.plane.partitions();
    partitions.set_remote_segments(Rc::new(RemoteSegments::new(
        Rc::new(ExecutorObjects::new(executor.clone())),
        layout(),
    )));
    let revision = partitions
        .get_by_ns(&namespace())
        .unwrap()
        .created_revision();
    let floor = |owner: &IoTestShard| {
        owner
            .plane
            .partitions()
            .get_by_ns(&namespace())
            .unwrap()
            .long_term_floor()
    };

    pumped(&owner, async {
        let _answers = [submit_send(&owner, 1, 100), submit_send(&owner, 2, 300)];
        until(&owner, |owner| committed_sends(owner) == 2).await;
        assert_eq!(floor(&owner), None);
        owner.request_rewrite_segments(namespace(), revision);
        until(&owner, |owner| floor(owner) == Some(1)).await;
    })
    .await;

    let partition = owner.plane.partitions().get_by_ns(&namespace()).unwrap();
    let segments = partition.long_term_segments();
    assert_eq!(segments.len(), 1);
    assert_eq!((segments[0].start_offset, segments[0].end_offset), (0, 1));
    assert_eq!(partition.unpublished_segment(), None);
    let store = executor.0.store();
    let log_key = layout().segment_key(&SegmentKey {
        prefix: partitions::object_prefix(namespace(), revision),
        start_offset: 0,
        object: SegmentObject::Log,
    });
    assert_eq!(store.head(&log_key).await.unwrap().size, segments[0].size);
    assert_eq!(
        store.keys().len(),
        3,
        "the fresh object, the log and its index"
    );
}

/// The whole store of one test: the fresh objects, the long-term segments,
/// and the sweeps, all through one executor.
#[allow(clippy::future_not_send)]
async fn swept_owner(directory: &Path) -> (IoTestShard, super::object_deletes::Shared) {
    use super::object_deletes::Shared;
    use partitions::{
        ExecutorFreshSweeper, ExecutorLongTermWriter, ExecutorObjects, RemoteSegments,
    };

    let owner = Box::pin(placing_owner(directory)).await;
    let executor = Shared(Rc::new(InlineTierExecutor::new(MemoryStore::new())));
    owner.set_fresh_uploader(Some(Rc::new(ExecutorFreshUploader::new(
        executor.clone(),
        layout(),
    ))));
    owner.set_long_term_writer(Some(Rc::new(ExecutorLongTermWriter::new(
        executor.clone(),
        layout(),
    ))));
    owner.set_fresh_sweeper(Some(Rc::new(ExecutorFreshSweeper::new(
        executor.clone(),
        layout(),
    ))));
    owner
        .plane
        .partitions()
        .set_remote_segments(Rc::new(RemoteSegments::new(
            Rc::new(ExecutorObjects::new(executor.clone())),
            layout(),
        )));
    (owner, executor)
}

/// The ids of the fresh objects the store holds, ascending.
fn fresh_objects(store: &MemoryStore) -> Vec<u128> {
    let mut objects: Vec<_> = store
        .keys()
        .iter()
        .filter_map(|key| layout().parse_fresh_key(key))
        .collect();
    objects.sort_unstable();
    objects
}

/// Run one sweep at `now` to the answer of every shard, and let the delete
/// it started end.
#[allow(clippy::future_not_send)]
async fn sweep(owner: &IoTestShard, now: IggyTimestamp) {
    let settled = owner.fresh_sweeps_settled();
    owner.request_sweep_fresh_objects(now);
    until(owner, |owner| owner.fresh_sweeps_settled() > settled).await;
    // The delete runs inline in the memory store, and its result is one
    // frame behind.
    compio::time::sleep(Duration::from_millis(50)).await;
}

/// A time one grace period and a second after `now`.
fn past_grace(now: IggyTimestamp) -> IggyTimestamp {
    let grace = u64::try_from(partitions::FRESH_OBJECT_GRACE.as_micros()).unwrap();
    IggyTimestamp::from(now.as_micros() + grace + 1_000_000)
}

/// Rewrite the committed sends of the partition up to `floor`.
#[allow(clippy::future_not_send)]
async fn rewrite_up_to(owner: &IoTestShard, floor: u64) {
    let partitions = owner.plane.partitions();
    let revision = partitions
        .get_by_ns(&namespace())
        .unwrap()
        .created_revision();
    owner.request_rewrite_segments(namespace(), revision);
    until(owner, |owner| {
        owner
            .plane
            .partitions()
            .get_by_ns(&namespace())
            .unwrap()
            .long_term_floor()
            == Some(floor)
    })
    .await;
}

/// Once the rewrite caught up, nothing reads the fresh objects again, and
/// the sweep leaves none behind.
#[compio::test]
async fn given_sends_in_long_term_segments_when_sweeps_run_past_the_grace_should_delete_their_fresh_objects()
 {
    let directory = tempfile::tempdir().unwrap();
    let (owner, executor) = Box::pin(swept_owner(directory.path())).await;
    let store = executor.0.store();
    let now = IggyTimestamp::now();

    pumped(&owner, async {
        let _first = submit_send(&owner, 1, 100);
        until(&owner, |owner| committed_sends(owner) == 1).await;
        let _second = submit_send(&owner, 2, 100);
        until(&owner, |owner| committed_sends(owner) == 2).await;
        assert_eq!(fresh_objects(store).len(), 2);

        // Both are named above the floor.
        sweep(&owner, now).await;
        sweep(&owner, past_grace(now)).await;
        assert_eq!(fresh_objects(store).len(), 2);

        rewrite_up_to(&owner, 1).await;
        let unnamed_since = past_grace(now);
        sweep(&owner, unnamed_since).await;
        assert_eq!(
            fresh_objects(store).len(),
            2,
            "an object waits out its grace period from when a sweep found it unnamed"
        );
        sweep(&owner, past_grace(unnamed_since)).await;
    })
    .await;

    assert_eq!(fresh_objects(store), Vec::<u128>::new());
    assert_eq!(store.keys().len(), 2, "the long-term log and its index");
    assert_eq!(owner.metrics().fresh_objects_deleted_value(), 2);
}

/// One object can hold sends that are rewritten and sends that are not. It
/// stays for as long as one placed batch above the floor names it.
#[compio::test]
async fn given_a_send_above_the_long_term_floor_when_sweeps_run_should_keep_its_fresh_object() {
    let directory = tempfile::tempdir().unwrap();
    let (owner, executor) = Box::pin(swept_owner(directory.path())).await;
    let store = executor.0.store();
    let now = IggyTimestamp::now();

    let kept = pumped(&owner, async {
        let _first = submit_send(&owner, 1, 100);
        until(&owner, |owner| committed_sends(owner) == 1).await;
        rewrite_up_to(&owner, 0).await;
        // The second send stays above the floor: no pass rewrites it.
        owner.set_long_term_writer(None);
        let _second = submit_send(&owner, 2, 100);
        until(&owner, |owner| committed_sends(owner) == 2).await;
        let objects = fresh_objects(store);
        assert_eq!(objects.len(), 2);

        sweep(&owner, now).await;
        sweep(&owner, past_grace(now)).await;
        sweep(&owner, past_grace(past_grace(now))).await;
        objects[1]
    })
    .await;

    assert_eq!(fresh_objects(store), vec![kept]);
}

/// An object whose sends never committed is named by no placed batch. So is
/// one a shard of an earlier life of this replica wrote. The objects of
/// another replica are that replica's to judge.
#[compio::test]
async fn given_objects_no_placed_batch_names_when_sweeps_run_should_delete_those_of_this_replica_after_the_grace()
 {
    use iggy_object_store::{PutBody, PutOptions};

    let directory = tempfile::tempdir().unwrap();
    let (owner, executor) = Box::pin(swept_owner(directory.path())).await;
    let store = executor.0.store();
    let replica = owner.partition_consensus.self_replica_id;
    let orphan = FreshObjectId::new(replica, 5, 1, 0).unwrap().get();
    let foreign = FreshObjectId::new(replica + 1, 0, 1, 0).unwrap().get();
    for object in [orphan, foreign] {
        store
            .put(
                &layout().fresh_key(object),
                PutBody::from_bytes(bytes::Bytes::from_static(b"IGGYFRS1 never committed")),
                &PutOptions::default(),
            )
            .await
            .unwrap();
    }
    let now = IggyTimestamp::now();

    pumped(&owner, async {
        let _send = submit_send(&owner, 1, 100);
        until(&owner, |owner| committed_sends(owner) == 1).await;
        sweep(&owner, now).await;
        assert_eq!(fresh_objects(store).len(), 3);
        sweep(&owner, past_grace(now)).await;
    })
    .await;

    let left = fresh_objects(store);
    assert_eq!(left.len(), 2);
    assert!(!left.contains(&orphan));
    assert!(left.contains(&foreign));
}

/// Stores the object, and then holds the answer back until told.
struct HeldUploader {
    uploader: ExecutorFreshUploader<super::object_deletes::Shared>,
    release: RefCell<Option<futures::channel::oneshot::Receiver<()>>>,
}

impl FreshObjectUploader for HeldUploader {
    fn upload(
        &self,
        object_id: FreshObjectId,
        bytes: bytes::Bytes,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, FreshUploadOutcome> {
        let upload = self.uploader.upload(object_id, bytes, scope);
        let release = self.release.borrow_mut().take();
        Box::pin(async move {
            let outcome = upload.await;
            if let Some(release) = release {
                let _ = release.await;
            }
            outcome
        })
    }
}

/// Between the moment the store holds an object and the moment its sends
/// reach their partitions, no journal names the object. The shard that
/// wrote it does.
#[compio::test]
async fn given_an_object_whose_sends_wait_when_sweeps_run_past_the_grace_should_keep_it() {
    let directory = tempfile::tempdir().unwrap();
    let (owner, executor) = Box::pin(swept_owner(directory.path())).await;
    let store = executor.0.store();
    let (release, held) = futures::channel::oneshot::channel();
    owner.set_fresh_uploader(Some(Rc::new(HeldUploader {
        uploader: ExecutorFreshUploader::new(executor.clone(), layout()),
        release: RefCell::new(Some(held)),
    })));
    let now = IggyTimestamp::now();

    pumped(&owner, async {
        let _send = submit_send(&owner, 1, 100);
        until(&owner, |_| fresh_objects(store).len() == 1).await;
        assert_eq!(committed_sends(&owner), 0);

        sweep(&owner, now).await;
        sweep(&owner, past_grace(now)).await;
        assert_eq!(fresh_objects(store).len(), 1);

        release.send(()).unwrap();
        until(&owner, |owner| committed_sends(owner) == 1).await;
        sweep(&owner, past_grace(past_grace(now))).await;
    })
    .await;

    assert_eq!(fresh_objects(store).len(), 1);
}

/// The segment cleaner stages only the first pass over a partition. The
/// sends that commit after it are rewritten when the partition's wait is
/// over, which the pump's tick finds.
#[compio::test]
async fn given_a_partition_that_had_a_rewrite_pass_when_more_sends_commit_should_rewrite_them_from_the_tick()
 {
    use super::object_deletes::Shared;
    use crate::rewrite_pacing::{REWRITE_INTERVAL_MAX, REWRITE_INTERVAL_START};
    use partitions::{ExecutorLongTermWriter, ExecutorObjects, RemoteSegments};

    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let executor = Shared(Rc::new(InlineTierExecutor::new(MemoryStore::new())));
    owner.set_fresh_uploader(Some(Rc::new(ExecutorFreshUploader::new(
        executor.clone(),
        layout(),
    ))));
    owner.set_long_term_writer(Some(Rc::new(ExecutorLongTermWriter::new(
        executor.clone(),
        layout(),
    ))));
    let partitions = owner.plane.partitions();
    partitions.set_remote_segments(Rc::new(RemoteSegments::new(
        Rc::new(ExecutorObjects::new(executor.clone())),
        layout(),
    )));
    let revision = partitions
        .get_by_ns(&namespace())
        .unwrap()
        .created_revision();
    let key = (namespace(), revision);
    let floor = |owner: &IoTestShard| {
        owner
            .plane
            .partitions()
            .get_by_ns(&namespace())
            .unwrap()
            .long_term_floor()
    };

    pumped(&owner, async {
        let _first = submit_send(&owner, 1, 100);
        until(&owner, |owner| committed_sends(owner) == 1).await;
        owner.request_rewrite_segments(namespace(), revision);
        until(&owner, |owner| floor(owner) == Some(0)).await;
        let wait = owner.rewrite_pacing.borrow().interval(&key).unwrap();
        assert!(
            wait > REWRITE_INTERVAL_START && wait <= REWRITE_INTERVAL_MAX,
            "a small segment lengthens the wait: {wait:?}"
        );

        let _second = submit_send(&owner, 2, 100);
        until(&owner, |owner| committed_sends(owner) == 2).await;
        until(&owner, |owner| floor(owner) == Some(1)).await;
        until(&owner, |owner| {
            owner.metrics().long_term_rewrite_lag_value() == 0
        })
        .await;
    })
    .await;

    let partition = owner.plane.partitions().get_by_ns(&namespace()).unwrap();
    assert_eq!(partition.long_term_segments().len(), 2);
    assert_eq!(partition.long_term_lag(), 0);
}
