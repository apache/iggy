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
use crate::fresh_writes::{FRESH_PARTITION_PENDING_BYTES_MAX, FRESH_UPLOAD_DEADLINE};
use crate::{FRESH_OBJECT_BYTES, FRESH_PENDING_BYTES_MAX};
use futures::future::LocalBoxFuture;
use iggy_binary_protocol::placement::{PLACED_BATCH_SIZE, PlacedBatch};
use iggy_object_store::{
    FreshObjectId, InlineTierExecutor, KeyLayout, MemoryStore, ObjectStore, ObjectStoreError,
    TierScopeToken, is_fresh_object,
};
use partitions::{
    ExecutorFreshUploader, FRESH_DELETES_PER_BATCH, FRESH_OBJECTS_PER_SWEEP, FreshDeleteOutcome,
    FreshListOutcome, FreshListRequest, FreshObjectSweeper, FreshObjectUploader,
    FreshUploadOutcome,
};

fn layout() -> KeyLayout {
    KeyLayout::new("fresh-test").unwrap()
}

type MemoryUploader = ExecutorFreshUploader<InlineTierExecutor<MemoryStore>>;

/// An interval long enough that the sends a test submits together after an
/// upload share one window.
const LONG_INTERVAL: Duration = Duration::from_millis(500);

/// Uploads into a memory store, unless told to refuse.
struct TestUploader {
    uploader: MemoryUploader,
    refuses: Cell<bool>,
    /// The first attempts that fail, in the order they come.
    fails_first: Cell<u32>,
    attempts: Cell<u32>,
}

impl TestUploader {
    fn new() -> Rc<Self> {
        Rc::new(Self {
            uploader: ExecutorFreshUploader::new(
                InlineTierExecutor::new(MemoryStore::new()),
                layout(),
            ),
            refuses: Cell::new(false),
            fails_first: Cell::new(0),
            attempts: Cell::new(0),
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
        let attempt = self.attempts.get();
        self.attempts.set(attempt + 1);
        if self.refuses.get() || attempt < self.fails_first.get() {
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

/// Make the partition beside [`namespace`] place its batches too.
fn place_other_partition(owner: &IoTestShard) -> IggyNamespace {
    place_partition(owner, IggyNamespace::new(0, 0, 1))
}

/// Make `partition` place its batches.
fn place_partition(owner: &IoTestShard, partition: IggyNamespace) -> IggyNamespace {
    let placing = owner.plane.partitions().get_mut_by_ns(&partition).unwrap();
    placing.set_runtime_options(TopicRuntimeOptions {
        places_batches: true,
        ..Default::default()
    });
    placing.register_builtin_state_machines().unwrap();
    partition
}

/// Make the last partition of the shard a primary in normal status that
/// places its batches and commits nothing: the other replica of its group
/// never answers. Once its pipeline is full, it has no room for a request.
fn place_stuck_partition(owner: &IoTestShard) -> IggyNamespace {
    const SILENT_REPLICA: u8 = 1;
    let stuck = IggyNamespace::new(0, 0, 2);
    owner.bus.set_replica_forward_fn(Box::new(|_, _, _| Ok(())));
    connect_replica_on(&owner.bus, SILENT_REPLICA, 1);
    let partitions = owner.plane.partitions();
    partitions.remove(&stuck).unwrap();
    let consensus = VsrConsensus::new(
        1,
        0,
        2,
        stuck.inner(),
        Rc::clone(&owner.bus),
        LocalPipeline::new(),
    );
    consensus.init();
    partitions.insert(
        stuck,
        IggyPartition::with_in_memory_storage(
            Arc::new(PartitionStats::default()),
            consensus,
            IggyByteSize::from(SEGMENT_BYTES),
        ),
    );
    place_partition(owner, stuck)
}

/// Submit one send of a message of `payload_len` bytes, as a producer does.
/// Returns where its answer arrives.
fn submit_send(
    owner: &IoTestShard,
    request: u64,
    payload_len: usize,
) -> Receiver<Option<Message<GenericHeader>>> {
    submit_send_to(owner, namespace(), request, payload_len)
}

/// [`submit_send`] to `partition`.
fn submit_send_to(
    owner: &IoTestShard,
    partition: IggyNamespace,
    request: u64,
    payload_len: usize,
) -> Receiver<Option<Message<GenericHeader>>> {
    let (reply, answer) = channel(1);
    owner.senders[0]
        .try_send(ShardFrame::lifecycle(LifecycleFrame::PartitionSubmit {
            request: sized_send_request(partition, request, payload_len),
            reply,
            attachment: None,
        }))
        .unwrap();
    answer
}

/// [`submit_send`] that waits for room in the shard's inbox, for a test that
/// submits more sends at once than the inbox holds.
#[allow(clippy::future_not_send)]
async fn submit_send_when_room(
    owner: &IoTestShard,
    request: u64,
    payload_len: usize,
) -> Receiver<Option<Message<GenericHeader>>> {
    submit_send_to_when_room(owner, namespace(), request, payload_len).await
}

/// [`submit_send_when_room`] to `partition`.
#[allow(clippy::future_not_send)]
async fn submit_send_to_when_room(
    owner: &IoTestShard,
    partition: IggyNamespace,
    request: u64,
    payload_len: usize,
) -> Receiver<Option<Message<GenericHeader>>> {
    let (reply, answer) = channel(1);
    let mut frame = ShardFrame::lifecycle(LifecycleFrame::PartitionSubmit {
        request: sized_send_request(partition, request, payload_len),
        reply,
        attachment: None,
    });
    loop {
        match owner.senders[0].try_send(frame) {
            Ok(()) => return answer,
            Err(crossfire::TrySendError::Full(refused)) => {
                frame = refused;
                compio::time::sleep(TASK_POLL_INTERVAL).await;
            }
            Err(crossfire::TrySendError::Disconnected(_)) => panic!("the shard's inbox is gone"),
        }
    }
}

/// Ops the partition `partition` committed.
fn committed(owner: &IoTestShard, partition: IggyNamespace) -> u64 {
    owner
        .plane
        .partitions()
        .get_by_ns(&partition)
        .unwrap()
        .consensus()
        .commit_min()
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
    owner.set_fresh_upload_pace(LONG_INTERVAL, 1);

    let answers = pumped(&owner, async {
        // The shard is idle, so this send is stored at once, and the two
        // after it wait out the interval together.
        let _alone = submit_send(&owner, 1, 50);
        until(&owner, |owner| committed_sends(owner) == 1).await;
        let answers = [submit_send(&owner, 2, 100), submit_send(&owner, 3, 300)];
        until(&owner, |owner| committed_sends(owner) == 3).await;
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
    let mut keys = store.keys();
    keys.sort();
    assert_eq!(keys.len(), 2, "both sends came inside one window: {keys:?}");
    let object_id = layout().parse_fresh_key(&keys[1]).unwrap();
    let size = store.head(&keys[1]).await.unwrap().size;
    let object = store.get_range(&keys[1], 0..size).await.unwrap().bytes;
    assert!(is_fresh_object(&object));
    let placed = stored_placed_batches(directory.path());
    assert_eq!(placed.len(), 3);
    let placed = &placed[1..];
    for (offset, placed) in (1..).zip(placed) {
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
    let (stored, refused, _) = owner.metrics().fresh_uploads_value();
    assert_eq!((stored, refused), (0, 1));
    let (retries, denied, in_flight, staged) = owner.metrics().fresh_load_value();
    assert_eq!((retries, denied, in_flight, staged), (2, 1, 0, 0));
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
        4,
        "a fresh object per send, the log and its index"
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
struct HeldSharedUploader {
    uploader: ExecutorFreshUploader<super::object_deletes::Shared>,
    release: RefCell<Option<futures::channel::oneshot::Receiver<()>>>,
}

impl FreshObjectUploader for HeldSharedUploader {
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
    owner.set_fresh_uploader(Some(Rc::new(HeldSharedUploader {
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

/// A reader of the bucket finds a long-term segment through the manifest,
/// so the commit of the segment's command is what asks for the manifest.
/// Nothing else asks for one here.
#[compio::test]
async fn given_a_manifest_publisher_when_a_long_term_segment_commits_should_publish_the_manifest() {
    use super::object_deletes::Shared;
    use iggy_object_store::PartitionManifest;
    use partitions::{
        ExecutorLongTermWriter, ExecutorManifestPublisher, ExecutorObjects, RemoteSegments,
    };

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
    owner.set_manifest_publisher(Some(Rc::new(ExecutorManifestPublisher::new(
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
    let manifest_key = layout().manifest_key(&partitions::object_prefix(namespace(), revision));
    let store = executor.0.store();

    pumped(&owner, async {
        let _answers = [submit_send(&owner, 1, 100), submit_send(&owner, 2, 300)];
        until(&owner, |owner| committed_sends(owner) == 2).await;
        owner.request_rewrite_segments(namespace(), revision);
        until(&owner, |_| store.keys().contains(&manifest_key)).await;
    })
    .await;

    let size = store.head(&manifest_key).await.unwrap().size;
    let object = store.get_range(&manifest_key, 0..size).await.unwrap();
    let manifest = PartitionManifest::decode(&object.bytes).unwrap();
    let partition = owner.plane.partitions().get_by_ns(&namespace()).unwrap();
    assert_eq!(manifest.floor, Some(1));
    assert_eq!(manifest.segments, partition.long_term_segments());
}

/// The segment cleaner stages only the first pass over a partition. The
/// sends that commit after it are rewritten when the partition's wait is
/// over, which the pump's tick finds.
#[compio::test]
async fn given_a_partition_that_had_a_rewrite_pass_when_more_sends_commit_should_rewrite_them_from_the_tick()
 {
    use super::object_deletes::Shared;
    use crate::rewrite_pacing::{REWRITE_INTERVAL_MAX, REWRITE_INTERVAL_START, RewriteResult};
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
        // The wait is longer than this test may take, so the next pass is
        // made due after the shortest one.
        owner.rewrite_pacing.borrow_mut().settle(
            key,
            RewriteResult::Resent,
            owner.bus.monotonic_micros(),
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

/// Stores the object, and then holds the answer back until told.
struct HeldUploader {
    uploader: MemoryUploader,
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

/// A store that does not answer holds back the sends of its window and
/// nothing else: the pump keeps running, and a partition that keeps its
/// batches whole commits a send meanwhile. The producer of the held send is
/// answered only after the store answered and the placement committed.
#[compio::test]
async fn given_a_store_that_does_not_answer_when_sends_wait_for_it_should_keep_serving_other_partitions()
 {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let (release, held) = futures::channel::oneshot::channel();
    let uploader = Rc::new(HeldUploader {
        uploader: ExecutorFreshUploader::new(InlineTierExecutor::new(MemoryStore::new()), layout()),
        release: RefCell::new(Some(held)),
    });
    owner.set_fresh_uploader(Some(uploader.clone()));
    let whole = IggyNamespace::new(0, 0, 1);
    let committed = |owner: &IoTestShard, namespace: IggyNamespace| {
        owner
            .plane
            .partitions()
            .get_by_ns(&namespace)
            .unwrap()
            .consensus()
            .commit_min()
    };

    let answer = pumped(&owner, async {
        let answer = submit_send(&owner, 1, 100);
        until(&owner, |_| {
            uploader.uploader.executor().store().keys().len() == 1
        })
        .await;

        let (reply, other) = channel(1);
        owner.senders[0]
            .try_send(ShardFrame::lifecycle(LifecycleFrame::PartitionSubmit {
                request: send_request(whole, 2),
                reply,
                attachment: None,
            }))
            .unwrap();
        until(&owner, |owner| committed(owner, whole) == 1).await;
        until(&owner, |_| !other.is_empty()).await;
        assert!(
            answer.is_empty(),
            "the object is stored and the store has not answered: no answer yet"
        );
        assert_eq!(committed(&owner, namespace()), 0);

        release.send(()).unwrap();
        until(&owner, |owner| committed_sends(owner) == 1).await;
        until(&owner, |_| !answer.is_empty()).await;
        answer
    })
    .await;

    assert!(answer.try_recv().unwrap().is_some());
    let (stored, refused, bytes) = owner.metrics().fresh_uploads_value();
    assert_eq!((stored, refused), (1, 0));
    assert!(bytes > 100);
}

/// The sends of every placing partition of the shard go into the object of
/// the open window, so the uploads follow the windows and not the
/// partitions.
#[compio::test]
async fn given_sends_to_two_placing_partitions_in_one_window_when_it_closes_should_store_one_object()
 {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let other = place_other_partition(&owner);
    let uploader = TestUploader::new();
    owner.set_fresh_uploader(Some(uploader.clone()));
    owner.set_fresh_upload_pace(LONG_INTERVAL, 1);

    pumped(&owner, async {
        // Stored at once, so the two sends after it share the next window.
        let _alone = submit_send(&owner, 1, 100);
        until(&owner, |owner| committed_sends(owner) == 1).await;
        let _first = submit_send(&owner, 2, 100);
        let _second = submit_send_to(&owner, other, 3, 100);
        until(&owner, |owner| {
            committed_sends(owner) == 2 && committed(owner, other) == 1
        })
        .await;
    })
    .await;

    assert_eq!(uploader.store().keys().len(), 2);
    assert_eq!(owner.metrics().fresh_uploads_value().0, 2);
}

/// Holds every attempt at an upload until the test settles it, so the test
/// picks the order the uploads end in and which of them the store fails.
struct GatedUploader {
    uploader: Rc<MemoryUploader>,
    /// Every attempt in the order it started: its object, and how to end it.
    started: RefCell<
        Vec<(
            FreshObjectId,
            Option<futures::channel::oneshot::Sender<bool>>,
        )>,
    >,
}

impl GatedUploader {
    fn new() -> Rc<Self> {
        Rc::new(Self {
            uploader: Rc::new(ExecutorFreshUploader::new(
                InlineTierExecutor::new(MemoryStore::new()),
                layout(),
            )),
            started: RefCell::default(),
        })
    }

    fn started(&self) -> usize {
        self.started.borrow().len()
    }

    /// The object of the attempt that started as number `attempt`.
    fn object_id(&self, attempt: usize) -> u128 {
        self.started.borrow()[attempt].0.get()
    }

    /// End the attempt that started as number `attempt`: the store takes the
    /// object, or fails in a way a retry can get past.
    fn settle(&self, attempt: usize, stores: bool) {
        let settle = self.started.borrow_mut()[attempt].1.take().unwrap();
        settle.send(stores).unwrap();
    }

    fn keys(&self) -> Vec<String> {
        self.uploader.executor().store().keys()
    }
}

impl FreshObjectUploader for GatedUploader {
    fn upload(
        &self,
        object_id: FreshObjectId,
        bytes: bytes::Bytes,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, FreshUploadOutcome> {
        let (settle, settled) = futures::channel::oneshot::channel();
        self.started.borrow_mut().push((object_id, Some(settle)));
        let uploader = Rc::clone(&self.uploader);
        Box::pin(async move {
            if settled.await == Ok(true) {
                uploader.upload(object_id, bytes, scope).await
            } else {
                Ok(Err(ObjectStoreError::Unavailable("503".to_owned())))
            }
        })
    }
}

/// Objects can finish in any order, and the sends of a partition still
/// commit in the order they came.
#[compio::test]
async fn given_two_uploads_when_the_later_object_is_stored_first_should_commit_the_sends_in_the_order_they_came()
 {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let uploader = GatedUploader::new();
    owner.set_fresh_uploader(Some(uploader.clone()));
    owner.set_fresh_upload_pace(Duration::ZERO, 2);

    pumped(&owner, async {
        let _first = submit_send(&owner, 1, 100);
        until(&owner, |_| uploader.started() == 1).await;
        let second = submit_send(&owner, 2, 100);
        until(&owner, |_| uploader.started() == 2).await;

        uploader.settle(1, true);
        until(&owner, |owner| owner.metrics().fresh_uploads_value().0 == 1).await;
        compio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(committed_sends(&owner), 0);
        assert!(second.is_empty(), "the send before it waits for its object");
        assert_eq!(owner.metrics().fresh_load_value().3, 2);

        uploader.settle(0, true);
        until(&owner, |owner| committed_sends(owner) == 2).await;
    })
    .await;

    let placed = stored_placed_batches(directory.path());
    assert_eq!(
        placed
            .iter()
            .map(|placed| (placed.header.base_offset, placed.placement.object_id))
            .collect::<Vec<_>>(),
        [(0, uploader.object_id(0)), (1, uploader.object_id(1))]
    );
    assert_eq!(owner.metrics().fresh_load_value(), (0, 0, 0, 0));
}

/// A partition holds only so many requests at once. The sends past that
/// wait on the shard for room, and their messages are not stored in vain.
#[compio::test]
async fn given_more_sends_than_a_partition_has_room_for_when_their_object_is_stored_should_commit_them_all()
 {
    const SENDS: u64 =
        2 * (consensus::PIPELINE_PREPARE_QUEUE_MAX + consensus::PIPELINE_REQUEST_QUEUE_MAX) as u64;
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let uploader = GatedUploader::new();
    owner.set_fresh_uploader(Some(uploader.clone()));
    owner.set_fresh_upload_pace(Duration::ZERO, 1);

    let answers = pumped(&owner, async {
        let first = submit_send(&owner, 1, 100);
        until(&owner, |_| uploader.started() == 1).await;
        // The one upload slot is taken, so these share the next object.
        let mut answers = vec![first];
        for request in 2..=SENDS {
            answers.push(submit_send_when_room(&owner, request, 100).await);
        }
        until(&owner, |owner| {
            owner.metrics().fresh_load_value().3 == i64::try_from(SENDS).unwrap()
        })
        .await;
        let (held, pending) = owner.fresh_pending_bytes(namespace());
        assert!(held > 0 && held == pending);
        uploader.settle(0, true);
        until(&owner, |_| uploader.started() == 2).await;
        uploader.settle(1, true);
        until(&owner, |owner| committed_sends(owner) == SENDS).await;
        until(&owner, |_| answers.iter().all(|answer| !answer.is_empty())).await;
        answers
    })
    .await;

    assert_eq!(answers.len() as u64, SENDS);
    assert_eq!(uploader.keys().len(), 2);
    let (_, denied, _, staged) = owner.metrics().fresh_load_value();
    assert_eq!((denied, staged), (0, 0));
    assert_eq!(owner.fresh_pending_bytes(namespace()), (0, 0));
    let placed = stored_placed_batches(directory.path());
    assert_eq!(
        placed
            .iter()
            .map(|placed| placed.header.base_offset)
            .collect::<Vec<_>>(),
        (0..SENDS).collect::<Vec<_>>()
    );
}

#[compio::test]
async fn given_a_store_that_fails_once_when_uploading_should_try_again_under_the_same_id_and_commit()
 {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let uploader = TestUploader::new();
    uploader.fails_first.set(1);
    owner.set_fresh_uploader(Some(uploader.clone()));

    let answer = pumped(&owner, async {
        let answer = submit_send(&owner, 1, 100);
        until(&owner, |owner| committed_sends(owner) == 1).await;
        until(&owner, |_| !answer.is_empty()).await;
        answer
    })
    .await;

    assert!(answer.try_recv().unwrap().is_some());
    assert_eq!(uploader.attempts.get(), 2);
    let keys = uploader.store().keys();
    assert_eq!(keys.len(), 1);
    assert_eq!(
        stored_placed_batches(directory.path())[0]
            .placement
            .object_id,
        layout().parse_fresh_key(&keys[0]).unwrap()
    );
    let (stored, refused, _) = owner.metrics().fresh_uploads_value();
    assert_eq!((stored, refused), (1, 0));
    let (retries, denied, ..) = owner.metrics().fresh_load_value();
    assert_eq!((retries, denied), (1, 0));
}

/// The producer of a send in an object the store failed sends it again. A
/// later send of the same partition must not commit ahead of it, so it is
/// answered the same way. The sends of other partitions are not held back.
#[compio::test]
async fn given_an_object_the_store_fails_for_good_when_later_sends_wait_should_answer_those_of_its_partitions_and_commit_the_rest()
 {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let other = place_other_partition(&owner);
    let uploader = GatedUploader::new();
    owner.set_fresh_uploader(Some(uploader.clone()));
    owner.set_fresh_upload_pace(Duration::ZERO, 2);

    pumped(&owner, async {
        let failed = submit_send(&owner, 1, 100);
        until(&owner, |_| uploader.started() == 1).await;
        let later = submit_send(&owner, 2, 100);
        until(&owner, |_| uploader.started() == 2).await;
        // Both upload slots are taken, so this one waits in the window.
        let unrelated = submit_send_to(&owner, other, 3, 100);

        // The first object fails on every attempt. Each one starts after the
        // one before it failed.
        for (attempt, started) in [(0, 3), (2, 4)] {
            uploader.settle(attempt, false);
            until(&owner, |_| uploader.started() == started).await;
            assert_eq!(uploader.object_id(started - 1), uploader.object_id(0));
            assert!(failed.is_empty() && later.is_empty());
        }
        uploader.settle(3, false);
        until(&owner, |_| !failed.is_empty() && !later.is_empty()).await;
        assert!(unrelated.is_empty());
        let (failed_partition, pending) = owner.fresh_pending_bytes(namespace());
        assert_eq!(failed_partition, 0);
        assert!(pending > 0);
        assert_eq!(owner.fresh_pending_bytes(other), (pending, pending));

        // The slot of the failed object goes to the window.
        until(&owner, |_| uploader.started() == 5).await;
        uploader.settle(1, true);
        uploader.settle(4, true);
        until(&owner, |owner| committed(owner, other) == 1).await;
        until(&owner, |_| !unrelated.is_empty()).await;
        until(&owner, |owner| owner.metrics().fresh_uploads_value().0 == 2).await;
    })
    .await;

    assert_eq!(committed(&owner, namespace()), 0);
    let (stored, refused, _) = owner.metrics().fresh_uploads_value();
    assert_eq!((stored, refused), (2, 1));
    assert_eq!(owner.metrics().fresh_load_value(), (2, 2, 0, 0));
    assert_eq!(owner.fresh_pending_bytes(other), (0, 0));
}

/// The interval counts from the start of the last upload. A send that finds
/// it passed is stored at once, and one that comes sooner waits for the rest
/// of it.
#[compio::test]
async fn given_an_idle_shard_when_sends_arrive_should_upload_the_first_at_once_and_the_next_an_interval_after_it()
 {
    const INTERVAL: Duration = Duration::from_secs(1);
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let uploader = TestUploader::new();
    owner.set_fresh_uploader(Some(uploader.clone()));
    owner.set_fresh_upload_pace(INTERVAL, 4);
    let stored = |count: usize| uploader.store().keys().len() == count;

    pumped(&owner, async {
        assert_eq!(owner.fresh_uploads_deadline(), None);
        let first_sent = std::time::Instant::now();
        let _first = submit_send(&owner, 1, 100);
        until(&owner, |_| stored(1)).await;
        assert!(first_sent.elapsed() < INTERVAL / 2);

        let _second = submit_send(&owner, 2, 100);
        compio::time::sleep(INTERVAL / 4).await;
        assert_eq!(uploader.store().keys().len(), 1);
        let due_in = owner
            .fresh_uploads_deadline()
            .expect("the open window has a deadline of its own")
            .saturating_sub(owner.bus.monotonic_micros());
        assert!(
            Duration::from_micros(due_in) <= INTERVAL * 3 / 4,
            "{due_in}"
        );
        until(&owner, |_| stored(2)).await;
        assert!(first_sent.elapsed() >= INTERVAL);

        compio::time::sleep(INTERVAL + INTERVAL / 10).await;
        let third_sent = std::time::Instant::now();
        let _third = submit_send(&owner, 3, 100);
        until(&owner, |_| stored(3)).await;
        assert!(third_sent.elapsed() < INTERVAL / 2);
        until(&owner, |owner| committed_sends(owner) == 3).await;
    })
    .await;
}

/// A send is never split. One that does not fit beside the sends of the
/// window starts the next object, and a send as large as an object is one.
#[compio::test]
async fn given_a_send_larger_than_an_object_when_the_window_holds_sends_should_store_it_as_its_own_object()
 {
    let large = usize::try_from(FRESH_OBJECT_BYTES).unwrap() + 1024;
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let uploader = GatedUploader::new();
    owner.set_fresh_uploader(Some(uploader.clone()));
    owner.set_fresh_upload_pace(Duration::ZERO, 1);

    pumped(&owner, async {
        let _first = submit_send(&owner, 1, 100);
        until(&owner, |_| uploader.started() == 1).await;
        let _second = submit_send_when_room(&owner, 2, 100).await;
        let _large = submit_send_when_room(&owner, 3, large).await;
        let _fourth = submit_send_when_room(&owner, 4, 100).await;
        until(&owner, |owner| owner.metrics().fresh_load_value().3 == 4).await;
        for attempt in 0..4 {
            until(&owner, |_| uploader.started() == attempt + 1).await;
            uploader.settle(attempt, true);
        }
        until(&owner, |owner| committed_sends(owner) == 4).await;
    })
    .await;

    let store = uploader.uploader.executor().store();
    let mut sizes = Vec::new();
    for attempt in 0..4 {
        let key = layout().fresh_key(uploader.object_id(attempt));
        sizes.push(store.head(&key).await.unwrap().size);
    }
    assert!(sizes[2] > large as u64, "{sizes:?}");
    assert!(
        sizes[0] < 1024 && sizes[1] < 1024 && sizes[3] < 1024,
        "{sizes:?}"
    );
    let placed = stored_placed_batches(directory.path());
    assert_eq!(
        placed
            .iter()
            .map(|placed| placed.placement.object_id)
            .collect::<Vec<_>>(),
        (0..4)
            .map(|attempt| uploader.object_id(attempt))
            .collect::<Vec<_>>()
    );
}

/// One cap holds the bytes of every send that has not reached its
/// partition, whether its object is open, waits for an upload or uploads.
/// It holds for a send that fits in the share of its own partition.
#[compio::test]
async fn given_fresh_bytes_at_the_cap_when_a_send_arrives_should_answer_it_and_keep_nothing_of_it()
{
    // Larger than the share of its partition, the first send gets in
    // because nothing of its partition waits.
    let first_bytes =
        usize::try_from(FRESH_PENDING_BYTES_MAX - 3 * FRESH_PARTITION_PENDING_BYTES_MAX / 2)
            .unwrap();
    let send_bytes = usize::try_from(FRESH_PARTITION_PENDING_BYTES_MAX * 7 / 8).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let second = place_other_partition(&owner);
    let third = place_partition(&owner, IggyNamespace::new(0, 0, 2));
    let uploader = GatedUploader::new();
    owner.set_fresh_uploader(Some(uploader.clone()));
    owner.set_fresh_upload_pace(Duration::ZERO, 1);

    pumped(&owner, async {
        // One object uploads and the other waits for its slot, so the open
        // window is empty when the cap is reached.
        let held = [
            submit_send_when_room(&owner, 1, first_bytes).await,
            submit_send_to_when_room(&owner, second, 2, send_bytes).await,
        ];
        until(&owner, |owner| owner.metrics().fresh_load_value().3 == 2).await;
        let pending = owner.fresh_pending_bytes(third);
        let refused = submit_send_to(&owner, third, 3, send_bytes);
        until(&owner, |_| !refused.is_empty()).await;

        assert!(refused.try_recv().unwrap().is_some());
        assert!(held.iter().all(|answer| answer.is_empty()));
        let (_, denied, in_flight, staged) = owner.metrics().fresh_load_value();
        assert_eq!((denied, in_flight, staged), (1, 1, 2));
        assert_eq!(owner.metrics().fresh_cap_denials_value(), (0, 1));
        assert_eq!(owner.fresh_pending_bytes(third), pending);
        assert_eq!(uploader.started(), 1);
    })
    .await;

    assert_eq!(committed_sends(&owner), 0);
}

/// A primary that commits nothing keeps the sends that wait for its room.
/// They fill its share of the shard's bytes and no more, so the other
/// partitions of the shard keep sending. The share is free again once the
/// partition stops taking requests and its sends are answered.
#[compio::test]
async fn given_a_partition_without_room_when_its_sends_reach_its_share_should_deny_them_and_commit_the_sends_of_another()
 {
    const PIPELINE: u64 =
        (consensus::PIPELINE_PREPARE_QUEUE_MAX + consensus::PIPELINE_REQUEST_QUEUE_MAX) as u64;
    let send_bytes = usize::try_from(FRESH_PARTITION_PENDING_BYTES_MAX / 2).unwrap() - 64 * 1024;
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let other = place_other_partition(&owner);
    let stuck = place_stuck_partition(&owner);
    let uploader = TestUploader::new();
    owner.set_fresh_uploader(Some(uploader.clone()));
    let has_room = |owner: &IoTestShard| {
        owner
            .plane
            .partitions()
            .get_by_ns(&stuck)
            .unwrap()
            .has_request_room()
    };

    let waiting = pumped(&owner, async {
        let mut handed = Vec::new();
        for request in 1..=PIPELINE {
            handed.push(submit_send_to_when_room(&owner, stuck, request, 100).await);
        }
        until(&owner, |owner| {
            !has_room(owner) && owner.metrics().fresh_load_value().3 == 0
        })
        .await;
        assert!(handed.iter().all(|answer| answer.is_empty()));
        assert_eq!(owner.fresh_pending_bytes(stuck), (0, 0));

        let stored = owner.metrics().fresh_uploads_value().0;
        let waiting = [
            submit_send_to_when_room(&owner, stuck, PIPELINE + 1, send_bytes).await,
            submit_send_to_when_room(&owner, stuck, PIPELINE + 2, send_bytes).await,
        ];
        until(&owner, |owner| {
            owner.metrics().fresh_uploads_value().0 == stored + 2
        })
        .await;
        let (held, pending) = owner.fresh_pending_bytes(stuck);
        assert_eq!(held, pending);
        assert!(held <= FRESH_PARTITION_PENDING_BYTES_MAX);

        let refused = submit_send_to(&owner, stuck, PIPELINE + 3, send_bytes);
        until(&owner, |_| !refused.is_empty()).await;
        assert!(refused.try_recv().unwrap().is_some());
        assert_eq!(owner.fresh_pending_bytes(stuck), (held, pending));
        assert_eq!(owner.metrics().fresh_cap_denials_value(), (1, 0));

        let accepted = submit_send_to(&owner, other, PIPELINE + 4, send_bytes);
        until(&owner, |owner| committed(owner, other) == 1).await;
        until(&owner, |_| !accepted.is_empty()).await;
        assert!(accepted.try_recv().unwrap().is_some());
        assert_eq!(owner.fresh_pending_bytes(other), (0, pending));
        assert!(waiting.iter().all(|answer| answer.is_empty()));

        owner
            .plane
            .partitions()
            .get_mut_by_ns(&stuck)
            .unwrap()
            .begin_shutdown_io();
        until(&owner, |owner| owner.fresh_pending_bytes(stuck) == (0, 0)).await;
        until(&owner, |_| waiting.iter().all(|answer| !answer.is_empty())).await;
        waiting
    })
    .await;

    assert!(
        waiting
            .iter()
            .all(|answer| answer.try_recv().unwrap().is_some())
    );
    let (_, denied, _, staged) = owner.metrics().fresh_load_value();
    assert_eq!((denied, staged), (3, 0));
    assert_eq!(owner.metrics().fresh_cap_denials_value(), (1, 0));
}

/// The partition would refuse the request after the upload, so the shard
/// asks first and stores nothing for it.
#[compio::test]
async fn given_a_primary_that_takes_no_requests_when_a_send_arrives_should_answer_it_and_upload_nothing()
 {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let uploader = GatedUploader::new();
    owner.set_fresh_uploader(Some(uploader.clone()));

    pumped(&owner, async {
        owner
            .plane
            .partitions()
            .get_mut_by_ns(&namespace())
            .unwrap()
            .begin_shutdown_io();
        let answer = submit_send(&owner, 1, 100);
        until(&owner, |_| !answer.is_empty()).await;

        assert!(answer.try_recv().unwrap().is_some());
        assert_eq!(uploader.started(), 0);
        assert_eq!(owner.metrics().fresh_load_value(), (0, 1, 0, 0));
    })
    .await;
}

/// The sends of a partition that stops taking requests are answered while
/// their object still uploads, not when the store answers at last.
#[compio::test]
async fn given_a_partition_that_stops_taking_requests_when_its_sends_wait_for_an_upload_should_answer_them()
 {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let uploader = GatedUploader::new();
    owner.set_fresh_uploader(Some(uploader.clone()));

    pumped(&owner, async {
        let answer = submit_send(&owner, 1, 100);
        until(&owner, |_| uploader.started() == 1).await;
        assert!(answer.is_empty());

        owner
            .plane
            .partitions()
            .get_mut_by_ns(&namespace())
            .unwrap()
            .begin_shutdown_io();
        until(&owner, |_| !answer.is_empty()).await;

        assert!(answer.try_recv().unwrap().is_some());
        let (_, denied, in_flight, staged) = owner.metrics().fresh_load_value();
        assert_eq!((denied, in_flight, staged), (1, 1, 0));
        assert_eq!(owner.fresh_pending_bytes(namespace()), (0, 0));
    })
    .await;

    assert_eq!(committed_sends(&owner), 0);
}

/// A store that stopped answering must not hold the producers until they
/// give up, nor the upload slot that the next object needs.
#[compio::test]
async fn given_a_store_that_never_answers_when_the_upload_runs_out_of_time_should_answer_its_sends_and_free_the_slot()
 {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let uploader = GatedUploader::new();
    owner.set_fresh_uploader(Some(uploader.clone()));
    owner.set_fresh_upload_pace(Duration::ZERO, 1);

    pumped(&owner, async {
        let answer = submit_send(&owner, 1, 100);
        until(&owner, |_| uploader.started() == 1).await;
        compio::time::sleep(
            FRESH_UPLOAD_DEADLINE
                .checked_sub(Duration::from_secs(1))
                .unwrap(),
        )
        .await;
        assert!(answer.is_empty());
        compio::time::sleep(Duration::from_secs(1)).await;
        until(&owner, |_| !answer.is_empty()).await;

        assert!(answer.try_recv().unwrap().is_some());
        let (stored, refused, _) = owner.metrics().fresh_uploads_value();
        assert_eq!((stored, refused), (0, 1));
        assert_eq!(owner.metrics().fresh_load_value(), (0, 1, 0, 0));

        let _next = submit_send(&owner, 2, 100);
        until(&owner, |_| uploader.started() == 2).await;
        uploader.settle(1, true);
        until(&owner, |owner| committed_sends(owner) == 1).await;
    })
    .await;
}

/// The fresh objects the shard tells a sweep to keep.
fn awaited(owner: &IoTestShard) -> Vec<u128> {
    owner.fresh_writes.awaited_objects().into_iter().collect()
}

/// An object that has no upload slot yet is in no store and no journal. The
/// sweep must not take it for one that nothing needs, and neither the object
/// that uploads beside it.
#[compio::test]
async fn given_an_object_that_waits_for_an_upload_slot_when_asked_for_the_awaited_objects_should_name_it_until_its_send_is_answered()
 {
    let large = usize::try_from(FRESH_OBJECT_BYTES).unwrap() + 1024;
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let uploader = GatedUploader::new();
    owner.set_fresh_uploader(Some(uploader.clone()));
    owner.set_fresh_upload_pace(LONG_INTERVAL, 1);

    pumped(&owner, async {
        let first = submit_send(&owner, 1, 100);
        until(&owner, |_| uploader.started() == 1).await;
        // As large as an object, so its window closes at once, and the one
        // upload slot is taken.
        let second = submit_send(&owner, 2, large);
        until(&owner, |owner| owner.metrics().fresh_load_value().3 == 2).await;
        let uploading = uploader.object_id(0);
        let named = awaited(&owner);
        assert_eq!(uploader.started(), 1);
        assert_eq!(named.len(), 2, "{named:?}");
        assert!(named.contains(&uploading));

        uploader.settle(0, true);
        until(&owner, |_| uploader.started() == 2).await;
        let waited = uploader.object_id(1);
        assert!(named.contains(&waited));
        until(&owner, |_| !first.is_empty()).await;
        assert_eq!(awaited(&owner), vec![waited]);

        uploader.settle(1, true);
        until(&owner, |owner| {
            let answered = !second.is_empty();
            assert!(answered || awaited(owner) == vec![waited]);
            answered
        })
        .await;
        assert_eq!(awaited(&owner), Vec::<u128>::new());
    })
    .await;
}

/// Between two attempts an object has no upload running, and the attempt
/// that failed may have left it in the store.
#[compio::test]
async fn given_an_object_the_store_failed_once_when_it_waits_for_its_next_attempt_should_name_it_among_the_awaited_objects()
 {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let uploader = GatedUploader::new();
    owner.set_fresh_uploader(Some(uploader.clone()));

    pumped(&owner, async {
        let answer = submit_send(&owner, 1, 100);
        until(&owner, |_| uploader.started() == 1).await;
        let object = uploader.object_id(0);

        uploader.settle(0, false);
        until(&owner, |owner| owner.metrics().fresh_load_value().0 == 1).await;
        assert_eq!(awaited(&owner), vec![object]);
        until(&owner, |owner| {
            assert_eq!(awaited(owner), vec![object]);
            uploader.started() == 2
        })
        .await;
        assert_eq!(uploader.object_id(1), object);

        uploader.settle(1, true);
        until(&owner, |owner| {
            let answered = !answer.is_empty();
            assert!(answered || awaited(owner) == vec![object]);
            answered
        })
        .await;
        assert_eq!(awaited(&owner), Vec::<u128>::new());
    })
    .await;
}

/// A stored object whose sends wait for room in their partition is named by
/// no journal, and by nothing that uploads. The pump does not run until the
/// state is read, so no send commits and the partition stays full.
#[compio::test]
async fn given_a_stored_object_whose_sends_wait_behind_a_full_partition_when_asked_for_the_awaited_objects_should_name_it_until_its_last_send_is_answered()
 {
    const SENDS: u64 =
        2 * (consensus::PIPELINE_PREPARE_QUEUE_MAX + consensus::PIPELINE_REQUEST_QUEUE_MAX) as u64;
    let large = usize::try_from(FRESH_OBJECT_BYTES).unwrap() + 1024;
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let uploader = GatedUploader::new();
    owner.set_fresh_uploader(Some(uploader.clone()));
    owner.set_fresh_upload_pace(LONG_INTERVAL, 2);
    let offer = |request: u64, payload_len: usize| {
        let (reply, answer) = channel(1);
        owner.offer_fresh_send(sized_send_request(namespace(), request, payload_len), reply);
        answer
    };
    let stored = |sequence: u64| crate::fresh_writes::FinishedFreshUpload {
        sequence,
        outcome: Ok(Ok(())),
    };

    // The first send uploads alone. The ones after it share the second
    // object, which the large send closes. The large send is the third.
    let mut answers = vec![offer(1, 100)];
    for request in 2..=SENDS {
        answers.push(offer(request, 100));
    }
    let last = offer(SENDS + 1, large);
    until(&owner, |_| uploader.started() == 2).await;
    Box::pin(owner.on_fresh_upload_finished(stored(0))).await;
    Box::pin(owner.on_fresh_upload_finished(stored(1))).await;
    until(&owner, |_| uploader.started() == 3).await;
    Box::pin(owner.on_fresh_upload_finished(stored(2))).await;

    let objects: Vec<_> = (0..3).map(|attempt| uploader.object_id(attempt)).collect();
    let (_, denied, in_flight, staged) = owner.metrics().fresh_load_value();
    assert_eq!((denied, in_flight), (0, 0), "every object is stored");
    assert!(staged > 1, "sends of the second object wait for room");
    assert!(answers.iter().all(|answer| answer.is_empty()) && last.is_empty());
    assert_eq!(awaited(&owner), objects);

    pumped(&owner, async {
        until(&owner, |owner| {
            let answered = !last.is_empty();
            assert!(answered || awaited(owner).contains(&objects[2]));
            answered
        })
        .await;
    })
    .await;

    assert!(answers.iter().all(|answer| !answer.is_empty()));
    assert_eq!(committed_sends(&owner), SENDS + 1);
    assert_eq!(awaited(&owner), Vec::<u128>::new());
}

/// The sends of an object the store refused are answered, and so is the
/// later send of their partition, whose own object is stored after that.
/// Nothing waits on either object then.
#[compio::test]
async fn given_an_object_the_store_fails_for_good_when_its_sends_and_the_later_ones_are_answered_should_stop_naming_their_objects()
 {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let uploader = GatedUploader::new();
    owner.set_fresh_uploader(Some(uploader.clone()));
    owner.set_fresh_upload_pace(Duration::ZERO, 2);

    pumped(&owner, async {
        let failed = submit_send(&owner, 1, 100);
        until(&owner, |_| uploader.started() == 1).await;
        let later = submit_send(&owner, 2, 100);
        until(&owner, |_| uploader.started() == 2).await;
        let objects = [uploader.object_id(0), uploader.object_id(1)];
        assert_eq!(awaited(&owner), objects);

        for (attempt, started) in [(0, 3), (2, 4)] {
            uploader.settle(attempt, false);
            until(&owner, |_| uploader.started() == started).await;
        }
        uploader.settle(3, false);
        until(&owner, |_| !failed.is_empty() && !later.is_empty()).await;
        assert_eq!(
            awaited(&owner),
            vec![objects[1]],
            "the later object still uploads"
        );

        uploader.settle(1, true);
        until(&owner, |owner| owner.metrics().fresh_uploads_value().0 == 1).await;
        assert_eq!(awaited(&owner), Vec::<u128>::new());
    })
    .await;

    assert_eq!(committed_sends(&owner), 0);
}

/// A partition that stops taking requests drops its waiting sends. Their
/// object is kept while it uploads, and no longer.
#[compio::test]
async fn given_a_partition_that_stops_taking_requests_when_the_object_of_its_sends_is_stored_should_stop_naming_it()
 {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let uploader = GatedUploader::new();
    owner.set_fresh_uploader(Some(uploader.clone()));

    pumped(&owner, async {
        let answer = submit_send(&owner, 1, 100);
        until(&owner, |_| uploader.started() == 1).await;
        let object = uploader.object_id(0);
        owner
            .plane
            .partitions()
            .get_mut_by_ns(&namespace())
            .unwrap()
            .begin_shutdown_io();
        until(&owner, |_| !answer.is_empty()).await;
        assert_eq!(awaited(&owner), vec![object]);

        uploader.settle(0, true);
        until(&owner, |owner| owner.metrics().fresh_uploads_value().0 == 1).await;
        assert_eq!(awaited(&owner), Vec::<u128>::new());
    })
    .await;
}

/// The owner of [`swept_owner`] as the first of two shards. The second one
/// is the test: it reads what the first asks it, and answers by hand.
#[allow(clippy::future_not_send)]
async fn first_of_two_shards(
    directory: &Path,
) -> (
    IoTestShard,
    super::object_deletes::Shared,
    Receiver<ShardFrame>,
) {
    let (mut owner, executor) = Box::pin(swept_owner(directory)).await;
    let (second, asked, _replies) = crate::shard_channel(1, 8, 1);
    owner.senders.push(second);
    (owner, executor, asked)
}

/// The sweep that the first shard asked the second about.
fn asked_sweep(asked: &Receiver<ShardFrame>) -> Option<u64> {
    match asked.try_recv().ok()? {
        ShardFrame::Lifecycle(LifecycleFrame::ReportFreshObjects { sweep }) => Some(sweep),
        _ => None,
    }
}

/// Start a sweep at `now`, and return its number once the second shard was
/// asked.
#[allow(clippy::future_not_send)]
async fn sweep_asking_the_second_shard(
    owner: &IoTestShard,
    asked: &Receiver<ShardFrame>,
    now: IggyTimestamp,
) -> u64 {
    owner.request_sweep_fresh_objects(now);
    until(owner, |_| !asked.is_empty()).await;
    asked_sweep(asked).expect("the first shard asks the second")
}

/// Answer the first shard as its second shard.
fn answer_as_second_shard(owner: &IoTestShard, sweep: u64, in_use: Option<&[u128]>) {
    let report = Box::new(crate::FreshObjectsReport {
        sweep,
        shard: 1,
        in_use: in_use.map(|objects| objects.iter().copied().collect()),
    });
    owner.senders[0]
        .try_send(ShardFrame::lifecycle(LifecycleFrame::FreshObjectsReported(
            report,
        )))
        .unwrap();
}

/// A fresh object of this replica that no partition of the first shard
/// names, as a shard of an earlier life left it.
#[allow(clippy::future_not_send)]
async fn store_unnamed_object(owner: &IoTestShard, store: &MemoryStore) -> u128 {
    use iggy_object_store::{PutBody, PutOptions};

    let replica = owner.partition_consensus.self_replica_id;
    let object = FreshObjectId::new(replica, 1, 1, 0).unwrap().get();
    store
        .put(
            &layout().fresh_key(object),
            PutBody::from_bytes(bytes::Bytes::from_static(b"IGGYFRS1 of another shard")),
            &PutOptions::default(),
        )
        .await
        .unwrap();
    object
}

/// The first shard holds none of the partitions of the other shards. An
/// object stays until every shard answered, and for as long as one of them
/// names it.
#[compio::test]
async fn given_two_shards_when_the_other_one_names_an_object_should_keep_it_until_it_does_not() {
    let directory = tempfile::tempdir().unwrap();
    let (owner, executor, asked) = Box::pin(first_of_two_shards(directory.path())).await;
    let store = executor.0.store();
    let object = Box::pin(store_unnamed_object(&owner, store)).await;
    let now = IggyTimestamp::now();

    pumped(&owner, async {
        // The second shard has not answered: past the grace, nothing goes.
        let unanswered = sweep_asking_the_second_shard(&owner, &asked, now).await;
        compio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(owner.fresh_sweeps_settled(), 0);
        answer_as_second_shard(&owner, unanswered, Some(&[object]));
        until(&owner, |owner| owner.fresh_sweeps_settled() == 1).await;

        // It names the object in every sweep, however late.
        let late = past_grace(past_grace(now));
        let sweep = sweep_asking_the_second_shard(&owner, &asked, late).await;
        answer_as_second_shard(&owner, sweep, Some(&[object]));
        until(&owner, |owner| owner.fresh_sweeps_settled() == 2).await;
        compio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(fresh_objects(store), vec![object]);

        // It stops naming it: the grace starts then.
        let sweep = sweep_asking_the_second_shard(&owner, &asked, late).await;
        answer_as_second_shard(&owner, sweep, Some(&[]));
        until(&owner, |owner| owner.fresh_sweeps_settled() == 3).await;
        compio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(fresh_objects(store), vec![object]);

        let sweep = sweep_asking_the_second_shard(&owner, &asked, past_grace(late)).await;
        answer_as_second_shard(&owner, sweep, Some(&[]));
        until(&owner, |_| fresh_objects(store).is_empty()).await;
    })
    .await;
}

/// A shard with a placing partition outside normal status answers that it
/// cannot tell. The sweep then deletes nothing, whatever the others say,
/// and the object's grace does not run.
#[compio::test]
async fn given_a_shard_that_cannot_tell_when_a_sweep_runs_past_the_grace_should_delete_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let (owner, executor, asked) = Box::pin(first_of_two_shards(directory.path())).await;
    let store = executor.0.store();
    let object = Box::pin(store_unnamed_object(&owner, store)).await;
    let now = IggyTimestamp::now();

    pumped(&owner, async {
        for at in [now, past_grace(now), past_grace(past_grace(now))] {
            let sweep = sweep_asking_the_second_shard(&owner, &asked, at).await;
            answer_as_second_shard(&owner, sweep, None);
            // The answer ends the sweep, so the next one starts at once.
            compio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(owner.fresh_sweeps_settled(), 0);
        assert_eq!(fresh_objects(store), vec![object]);

        // Once it can tell, the grace starts from that sweep.
        let able = past_grace(past_grace(now));
        let sweep = sweep_asking_the_second_shard(&owner, &asked, able).await;
        answer_as_second_shard(&owner, sweep, Some(&[]));
        until(&owner, |owner| owner.fresh_sweeps_settled() == 1).await;
        compio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(fresh_objects(store), vec![object]);
    })
    .await;
}

/// An answer to a sweep that a later one replaced says nothing about the
/// objects the later one listed.
#[compio::test]
async fn given_an_answer_to_an_earlier_sweep_when_it_arrives_should_change_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let (owner, executor, asked) = Box::pin(first_of_two_shards(directory.path())).await;
    let store = executor.0.store();
    let object = Box::pin(store_unnamed_object(&owner, store)).await;
    let now = IggyTimestamp::now();

    pumped(&owner, async {
        let sweep = sweep_asking_the_second_shard(&owner, &asked, now).await;
        answer_as_second_shard(&owner, sweep + 7, Some(&[]));
        answer_as_second_shard(&owner, sweep.wrapping_sub(1), Some(&[]));
        compio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(owner.fresh_sweeps_settled(), 0);
    })
    .await;

    assert_eq!(fresh_objects(store), vec![object]);
}

/// A time one grace period and a second before `now`.
fn before_grace(now: IggyTimestamp) -> IggyTimestamp {
    let grace = u64::try_from(partitions::FRESH_OBJECT_GRACE.as_micros()).unwrap();
    IggyTimestamp::from(now.as_micros() - grace - 1_000_000)
}

/// Store `count` fresh objects of this replica that no partition names, as
/// `shard` wrote them in an earlier life.
#[allow(clippy::future_not_send)]
async fn store_orphans(
    owner: &IoTestShard,
    store: &MemoryStore,
    shard: u16,
    count: u64,
) -> Vec<u128> {
    use iggy_object_store::{PutBody, PutOptions};

    let replica = owner.partition_consensus.self_replica_id;
    let mut objects = Vec::new();
    for sequence in 0..count {
        let object = FreshObjectId::new(replica, shard, 1, sequence)
            .unwrap()
            .get();
        store
            .put(
                &layout().fresh_key(object),
                PutBody::from_bytes(bytes::Bytes::from_static(b"IGGYFRS1 of an earlier life")),
                &PutOptions::default(),
            )
            .await
            .unwrap();
        objects.push(object);
    }
    objects
}

/// Sweeps through the owner's sweeper, except for the deletes the test
/// tells it to leave undone.
struct FlakySweeper {
    sweeper: Rc<dyn FreshObjectSweeper>,
    /// Deletes nothing while set.
    refuses: Cell<bool>,
    /// Objects the next delete that holds them leaves in the store.
    keeps: RefCell<Vec<u128>>,
    deletes: Cell<usize>,
    /// Every listing fails at its first page while set.
    lists_nothing: Cell<bool>,
    lists: Cell<usize>,
    /// The next listing ends only after this fires.
    held_list: RefCell<Option<futures::channel::oneshot::Receiver<()>>>,
    /// The next delete ends only after this fires.
    held_delete: RefCell<Option<futures::channel::oneshot::Receiver<()>>>,
}

impl FlakySweeper {
    /// Put one between `owner` and the sweeper it has.
    fn wrap(owner: &IoTestShard) -> Rc<Self> {
        let flaky = Rc::new(Self {
            sweeper: owner.fresh_sweeper().unwrap(),
            refuses: Cell::new(false),
            keeps: RefCell::default(),
            deletes: Cell::new(0),
            lists_nothing: Cell::new(false),
            lists: Cell::new(0),
            held_list: RefCell::new(None),
            held_delete: RefCell::new(None),
        });
        owner.set_fresh_sweeper(Some(flaky.clone()));
        flaky
    }
}

impl FreshObjectSweeper for FlakySweeper {
    fn list(
        &self,
        replica: u8,
        requests: Vec<FreshListRequest>,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, FreshListOutcome> {
        self.lists.set(self.lists.get() + 1);
        if let Some(held) = self.held_list.borrow_mut().take() {
            let list = self.sweeper.list(replica, requests, scope);
            return Box::pin(async move {
                let _ = held.await;
                list.await
            });
        }
        if self.lists_nothing.get() {
            let scopes = requests
                .into_iter()
                .map(|request| partitions::FreshListedScope {
                    scope: request.scope,
                    from: request.resume.and_then(|cursor| cursor.after),
                    objects: Vec::new(),
                    end: partitions::FreshListEnd::Failed,
                })
                .collect();
            return Box::pin(std::future::ready(Ok(partitions::FreshListing {
                scopes,
                ..Default::default()
            })));
        }
        self.sweeper.list(replica, requests, scope)
    }

    fn delete(
        &self,
        objects: Vec<u128>,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, FreshDeleteOutcome> {
        self.deletes.set(self.deletes.get() + 1);
        if self.refuses.get() {
            return Box::pin(std::future::ready(Ok(objects)));
        }
        let keeps = std::mem::take(&mut *self.keeps.borrow_mut());
        let (kept, sent): (Vec<u128>, Vec<u128>) =
            objects.into_iter().partition(|id| keeps.contains(id));
        let delete = self.sweeper.delete(sent, scope);
        let held = self.held_delete.borrow_mut().take();
        Box::pin(async move {
            if let Some(held) = held {
                let _ = held.await;
            }
            let mut failed = delete.await?;
            failed.extend(kept);
            Ok(failed)
        })
    }
}

/// Ask for a sweep at `now` until one brought the owner to `done`. A sweep
/// asked for while another runs is dropped, and the owner starts sweeps of
/// its own.
#[allow(clippy::future_not_send)]
async fn sweep_until(owner: &IoTestShard, now: IggyTimestamp, done: impl Fn(&IoTestShard) -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !done(owner) {
        assert!(
            std::time::Instant::now() < deadline,
            "the sweeps did not get there"
        );
        owner.request_sweep_fresh_objects(now);
        compio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A delete job takes only so many objects. The sweep runs one job after
/// another until nothing is due, so what it deletes has no ceiling.
#[compio::test]
async fn given_more_objects_past_their_grace_than_one_delete_job_takes_when_a_sweep_runs_should_delete_them_all()
 {
    const OBJECTS: usize = FRESH_DELETES_PER_BATCH + 10;
    let directory = tempfile::tempdir().unwrap();
    let (owner, executor) = Box::pin(swept_owner(directory.path())).await;
    let store = executor.0.store();
    let sweeper = FlakySweeper::wrap(&owner);
    Box::pin(store_orphans(&owner, store, 5, OBJECTS as u64)).await;
    let now = IggyTimestamp::now();

    pumped(&owner, async {
        sweep(&owner, now).await;
        assert_eq!(fresh_objects(store).len(), OBJECTS);
        assert_eq!(
            owner.metrics().fresh_objects_unnamed_value(),
            i64::try_from(OBJECTS).unwrap()
        );
        owner.request_sweep_fresh_objects(past_grace(now));
        until(&owner, |_| fresh_objects(store).is_empty()).await;
        compio::time::sleep(Duration::from_millis(50)).await;
    })
    .await;

    assert_eq!(sweeper.deletes.get(), 2);
    assert_eq!(
        owner.fresh_sweeps_settled(),
        2,
        "one sweep deleted them all"
    );
    assert_eq!(
        owner.metrics().fresh_objects_deleted_value(),
        OBJECTS as u64
    );
    assert_eq!(owner.metrics().fresh_sweep_backlog_value(), (0, 0));
}

/// Nothing but the shard's own tick asks for these sweeps.
#[compio::test]
async fn given_a_sweeper_when_the_pump_ticks_should_sweep_without_being_asked() {
    let directory = tempfile::tempdir().unwrap();
    let (owner, executor) = Box::pin(swept_owner(directory.path())).await;
    let store = executor.0.store();
    Box::pin(store_orphans(&owner, store, 0, 2)).await;
    owner.set_fresh_sweep_pace(Duration::from_millis(50), FRESH_OBJECTS_PER_SWEEP);

    pumped(&owner, async {
        until(&owner, |owner| owner.fresh_sweeps_settled() >= 2).await;
    })
    .await;

    assert_eq!(owner.metrics().fresh_objects_value(), 2);
    assert_eq!(owner.metrics().fresh_objects_unnamed_value(), 2);
    assert_eq!(fresh_objects(store).len(), 2, "their grace has not passed");
}

/// The next sweep on the timer is a minute away here. The object a delete
/// left behind goes long before that.
#[compio::test]
async fn given_a_sweep_that_deleted_some_objects_and_left_one_due_when_it_ends_should_start_the_next_one_at_once()
 {
    let directory = tempfile::tempdir().unwrap();
    let (owner, executor) = Box::pin(swept_owner(directory.path())).await;
    let store = executor.0.store();
    let sweeper = FlakySweeper::wrap(&owner);
    let objects = Box::pin(store_orphans(&owner, store, 0, 3)).await;
    owner.set_fresh_sweep_pace(Duration::from_mins(1), FRESH_OBJECTS_PER_SWEEP);
    let now = IggyTimestamp::now();

    pumped(&owner, async {
        // The sweep the owner starts by itself reads the clock, so the
        // objects are found unnamed in the past.
        sweep(&owner, before_grace(now)).await;
        sweeper.keeps.borrow_mut().push(objects[0]);
        owner.request_sweep_fresh_objects(now);
        until(&owner, |_| fresh_objects(store).is_empty()).await;
        compio::time::sleep(Duration::from_millis(50)).await;
    })
    .await;

    assert_eq!(owner.fresh_sweeps_settled(), 3);
    assert_eq!(sweeper.deletes.get(), 2);
    assert_eq!(owner.metrics().fresh_objects_deleted_value(), 3);
    assert_eq!(owner.metrics().fresh_sweep_backlog_value(), (0, 0));
}

/// A store that deletes nothing is not asked again without a pause, and the
/// objects it keeps past their grace show in a gauge.
#[compio::test]
async fn given_a_store_that_deletes_nothing_when_a_sweep_ends_should_report_the_objects_left_due_and_wait_for_the_timer()
 {
    let directory = tempfile::tempdir().unwrap();
    let (owner, executor) = Box::pin(swept_owner(directory.path())).await;
    let store = executor.0.store();
    let sweeper = FlakySweeper::wrap(&owner);
    Box::pin(store_orphans(&owner, store, 0, 2)).await;
    let now = IggyTimestamp::now();

    pumped(&owner, async {
        sweep(&owner, before_grace(now)).await;
        assert_eq!(owner.metrics().fresh_sweep_backlog_value(), (0, 0));
        sweeper.refuses.set(true);
        sweep(&owner, now).await;
        assert_eq!(owner.metrics().fresh_sweep_backlog_value(), (0, 2));
        compio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(owner.fresh_sweeps_settled(), 2);
        assert_eq!(sweeper.deletes.get(), 1);
        assert_eq!(fresh_objects(store).len(), 2);

        sweeper.refuses.set(false);
        sweep(&owner, now).await;
    })
    .await;

    assert_eq!(fresh_objects(store), Vec::<u128>::new());
    assert_eq!(owner.metrics().fresh_sweep_backlog_value(), (0, 0));
}

/// The gauge of listed objects holds only what one sweep listed. Another
/// one says that the listings stopped short, so the first is not read as
/// the whole store. The sweep found nothing to do, so it waits for the timer.
#[compio::test]
async fn given_listings_that_stop_at_their_share_of_keys_when_a_sweep_ends_should_report_how_many_did()
 {
    let directory = tempfile::tempdir().unwrap();
    let (owner, executor, asked) = Box::pin(first_of_two_shards(directory.path())).await;
    let store = executor.0.store();
    let objects = Box::pin(store_orphans(&owner, store, 1, 3)).await;
    // One key for each shard and one for the replica as a whole.
    owner.set_fresh_sweep_pace(Duration::from_mins(1), 3);

    pumped(&owner, async {
        let sweep = sweep_asking_the_second_shard(&owner, &asked, IggyTimestamp::now()).await;
        answer_as_second_shard(&owner, sweep, Some(&objects));
        until(&owner, |owner| owner.fresh_sweeps_settled() == 1).await;
        compio::time::sleep(Duration::from_millis(100)).await;
        assert!(asked.is_empty(), "no sweep follows at once");
    })
    .await;

    assert_eq!(owner.metrics().fresh_objects_value(), 1);
    assert_eq!(
        owner.metrics().fresh_sweep_backlog_value(),
        (2, 0),
        "the listing of the second shard and the one of the replica"
    );
}

/// The first shard's directory holds more objects than a sweep lists of it.
/// The sweeps follow each other at once while they find objects, each from
/// where the last stopped, and they reach the directory of a shard this node
/// no longer runs. Every object is found and then deleted.
#[compio::test]
async fn given_a_shard_that_holds_more_objects_than_a_sweep_lists_when_sweeps_run_should_find_and_delete_those_of_every_shard()
 {
    let directory = tempfile::tempdir().unwrap();
    let (owner, executor) = Box::pin(swept_owner(directory.path())).await;
    let store = executor.0.store();
    Box::pin(store_orphans(&owner, store, 0, 6)).await;
    Box::pin(store_orphans(&owner, store, 5, 1)).await;
    // Two keys for the shard and two for the replica as a whole.
    owner.set_fresh_sweep_pace(Duration::from_mins(1), 4);
    let now = IggyTimestamp::now();

    pumped(&owner, async {
        owner.request_sweep_fresh_objects(now);
        until(&owner, |owner| {
            owner.metrics().fresh_objects_unnamed_value() == 7
        })
        .await;
        assert!(owner.fresh_sweeps_settled() >= 4);
        assert_eq!(fresh_objects(store).len(), 7);

        // Past the grace of the object that was found last. Each sweep
        // lists two objects of the shard, and deletes all seven.
        sweep_until(&owner, past_grace(IggyTimestamp::now()), |_| {
            fresh_objects(store).is_empty()
        })
        .await;
    })
    .await;

    assert_eq!(owner.metrics().fresh_objects_deleted_value(), 7);
}

/// An object found unnamed stays due whether a later sweep lists it or not.
/// A sweep that listed nothing still asks the shards before it deletes one,
/// and keeps what a shard names.
#[compio::test]
async fn given_a_sweep_that_listed_nothing_when_an_object_found_earlier_is_due_should_ask_the_shards_before_deleting_it()
 {
    let directory = tempfile::tempdir().unwrap();
    let (owner, executor, asked) = Box::pin(first_of_two_shards(directory.path())).await;
    let store = executor.0.store();
    let sweeper = FlakySweeper::wrap(&owner);
    let object = Box::pin(store_unnamed_object(&owner, store)).await;
    let now = IggyTimestamp::now();

    pumped(&owner, async {
        let sweep = sweep_asking_the_second_shard(&owner, &asked, now).await;
        answer_as_second_shard(&owner, sweep, Some(&[]));
        until(&owner, |owner| owner.fresh_sweeps_settled() == 1).await;

        sweeper.lists_nothing.set(true);
        let sweep = sweep_asking_the_second_shard(&owner, &asked, past_grace(now)).await;
        answer_as_second_shard(&owner, sweep, Some(&[object]));
        until(&owner, |owner| owner.fresh_sweeps_settled() == 2).await;
        compio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(fresh_objects(store), vec![object]);
        assert_eq!(sweeper.deletes.get(), 0);

        // Named no more: it is due once the grace passed again, from the
        // sweep that lists it once more.
        sweeper.lists_nothing.set(false);
        let later = past_grace(now);
        let sweep = sweep_asking_the_second_shard(&owner, &asked, later).await;
        answer_as_second_shard(&owner, sweep, Some(&[]));
        until(&owner, |owner| owner.fresh_sweeps_settled() == 3).await;
        let sweep = sweep_asking_the_second_shard(&owner, &asked, past_grace(later)).await;
        answer_as_second_shard(&owner, sweep, Some(&[]));
        until(&owner, |_| fresh_objects(store).is_empty()).await;
    })
    .await;

    assert_eq!(sweeper.deletes.get(), 1);
}

/// A partition in a view change may be missing batches its group holds, so
/// its shard cannot tell what it needs, and the sweep deletes nothing.
#[compio::test]
async fn given_a_placing_partition_that_is_not_normal_when_a_sweep_runs_past_the_grace_should_delete_nothing()
 {
    let directory = tempfile::tempdir().unwrap();
    let (owner, executor) = Box::pin(swept_owner(directory.path())).await;
    let store = executor.0.store();
    let sweeper = FlakySweeper::wrap(&owner);
    Box::pin(store_orphans(&owner, store, 0, 1)).await;
    let now = IggyTimestamp::now();

    pumped(&owner, async {
        sweep(&owner, before_grace(now)).await;
        let partitions = owner.plane.partitions();
        let consensus = partitions.get_by_ns(&namespace()).unwrap().consensus();
        let MessageBag::StartViewChange(change) = view_control_message(
            namespace(),
            Command::StartViewChange,
            consensus.view() + 1,
            consensus.replica(),
        ) else {
            unreachable!()
        };
        consensus.handle_start_view_change(consensus::PlaneKind::Partitions, change.header());
        assert!(!consensus.is_normal());

        owner.request_sweep_fresh_objects(now);
        compio::time::sleep(Duration::from_millis(200)).await;
    })
    .await;

    assert_eq!(owner.fresh_sweeps_settled(), 1);
    assert_eq!(sweeper.deletes.get(), 0);
    assert_eq!(fresh_objects(store).len(), 1);
}

/// One sweep at a time: a sweep asked for while a listing or a delete of
/// another runs starts nothing, however often it is asked for.
#[compio::test]
async fn given_a_sweep_in_flight_when_more_sweeps_are_asked_for_should_start_none() {
    let directory = tempfile::tempdir().unwrap();
    let (owner, executor) = Box::pin(swept_owner(directory.path())).await;
    let store = executor.0.store();
    let sweeper = FlakySweeper::wrap(&owner);
    Box::pin(store_orphans(&owner, store, 0, 1)).await;
    let now = IggyTimestamp::now();
    let ask_often = || async {
        for _ in 0..12 {
            owner.request_sweep_fresh_objects(past_grace(now));
            compio::time::sleep(Duration::from_millis(5)).await;
        }
    };

    pumped(&owner, async {
        let (release, held) = futures::channel::oneshot::channel();
        *sweeper.held_list.borrow_mut() = Some(held);
        owner.request_sweep_fresh_objects(now);
        until(&owner, |_| sweeper.lists.get() == 1).await;
        ask_often().await;
        assert_eq!(sweeper.lists.get(), 1);
        release.send(()).unwrap();
        until(&owner, |owner| owner.fresh_sweeps_settled() == 1).await;

        let (release, held) = futures::channel::oneshot::channel();
        *sweeper.held_delete.borrow_mut() = Some(held);
        owner.request_sweep_fresh_objects(past_grace(now));
        until(&owner, |_| sweeper.deletes.get() == 1).await;
        ask_often().await;
        assert_eq!(sweeper.lists.get(), 2);
        release.send(()).unwrap();
        until(&owner, |_| fresh_objects(store).is_empty()).await;
    })
    .await;

    assert_eq!(owner.fresh_sweeps_settled(), 2);
}
