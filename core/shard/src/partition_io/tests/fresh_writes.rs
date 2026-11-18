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

use super::segment_uploads::{namespace, pumped, staged_revision, until};
use super::*;
use crate::fresh_writes::{FRESH_PARTITION_PENDING_BYTES_MAX, FRESH_UPLOAD_DEADLINE};
use crate::rewrite_pacing::{BACKLOG_DELAY_MAX, LONG_TERM_BACKLOG_LIMIT};
use crate::{FRESH_OBJECT_BYTES, FRESH_PENDING_BYTES_MAX};
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

/// Starts every rewrite and ends none, so the long-term floor stays where
/// it is and the backlog only grows.
struct HeldWriter;

impl partitions::LongTermWriter for HeldWriter {
    fn write(
        &self,
        _rewrite: partitions::SegmentRewrite,
        _scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, partitions::RewriteOutcome> {
        Box::pin(std::future::pending())
    }
}

/// [`placing_owner`] with an uploader and a rewrite that never ends, whose
/// sends wait past a backlog of `limit` bytes.
#[allow(clippy::future_not_send)]
async fn backlog_owner(directory: &Path, limit: u64) -> IoTestShard {
    let owner = Box::pin(placing_owner(directory)).await;
    owner.set_fresh_uploader(Some(TestUploader::new()));
    owner.set_long_term_writer(Some(Rc::new(HeldWriter)));
    owner.set_long_term_backlog_limit(limit);
    owner
}

/// The backlog `partition` counted, if it did.
fn backlog_of(owner: &IoTestShard, partition: IggyNamespace) -> Option<u64> {
    owner
        .plane
        .partitions()
        .get_by_ns(&partition)
        .unwrap()
        .long_term_backlog()
}

fn delayed_sends(owner: &IoTestShard) -> u64 {
    owner.metrics().long_term_backlog_value().1
}

/// Commit a send to [`namespace`], and have a pass count the partition's
/// backlog, which is then the bytes of that send.
#[allow(clippy::future_not_send)]
async fn commit_and_count_a_backlog(owner: &IoTestShard) -> u64 {
    let _first = submit_send(owner, 1, 100);
    until(owner, |owner| committed_sends(owner) == 1).await;
    owner.request_rewrite_segments(namespace(), staged_revision(owner));
    until(owner, |owner| backlog_of(owner, namespace()).is_some()).await;
    backlog_of(owner, namespace()).unwrap()
}

/// Past the limit, each send to the partition waits the longest delay
/// before the shard takes it, and a later send waits behind it even once
/// the limit is off. A partition below the limit is not slowed.
#[compio::test]
async fn given_a_backlog_past_the_limit_when_sends_arrive_should_delay_the_sends_to_that_partition_only()
 {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(backlog_owner(directory.path(), 1)).await;
    let other = place_other_partition(&owner);
    let other_revision = owner
        .plane
        .partitions()
        .get_by_ns(&other)
        .unwrap()
        .created_revision();

    let (backlog, waited) = pumped(&owner, async {
        let backlog = commit_and_count_a_backlog(&owner).await;
        owner.request_rewrite_segments(other, other_revision);
        until(&owner, |owner| backlog_of(owner, other) == Some(0)).await;
        until(&owner, |owner| {
            owner.metrics().long_term_backlog_value().0 == i64::try_from(backlog).unwrap()
        })
        .await;

        let sent = owner.bus.monotonic_micros();
        let _held = submit_send(&owner, 2, 100);
        until(&owner, |owner| delayed_sends(owner) == 1).await;
        assert_eq!(committed_sends(&owner), 1, "held, not taken");
        let due_in = owner
            .fresh_uploads_deadline()
            .unwrap()
            .saturating_sub(owner.bus.monotonic_micros());
        assert!(
            Duration::from_micros(due_in) <= BACKLOG_DELAY_MAX,
            "the delay is capped: {due_in}"
        );
        owner.set_long_term_backlog_limit(0);
        let _behind = submit_send(&owner, 3, 300);
        until(&owner, |owner| {
            delayed_sends(owner) == 2 || committed_sends(owner) > 1
        })
        .await;
        owner.set_long_term_backlog_limit(1);
        let _other = submit_send_to(&owner, other, 4, 100);
        until(&owner, |owner| committed(owner, other) == 1).await;
        until(&owner, |owner| committed_sends(owner) == 3).await;
        (
            backlog,
            Duration::from_micros(owner.bus.monotonic_micros() - sent),
        )
    })
    .await;

    assert!(backlog > 1);
    assert_eq!(
        delayed_sends(&owner),
        2,
        "the held send and the one behind it"
    );
    assert!(waited >= BACKLOG_DELAY_MAX, "{waited:?}");
    let placed = stored_placed_batches(directory.path());
    assert_eq!(placed.len(), 3);
    assert!(
        placed[1].placement.length < placed[2].placement.length,
        "the held send committed before the one behind it"
    );
}

/// Below the limit, or with no limit, a send is taken at once, however
/// large the backlog.
#[compio::test]
async fn given_a_backlog_below_the_limit_or_no_limit_when_a_send_arrives_should_take_it_at_once() {
    for limit in [LONG_TERM_BACKLOG_LIMIT, 0] {
        let directory = tempfile::tempdir().unwrap();
        let owner = Box::pin(backlog_owner(directory.path(), limit)).await;

        pumped(&owner, async {
            let backlog = commit_and_count_a_backlog(&owner).await;
            assert!(backlog > 0);
            let _second = submit_send(&owner, 2, 100);
            until(&owner, |owner| committed_sends(owner) == 2).await;
        })
        .await;

        assert_eq!(delayed_sends(&owner), 0, "limit {limit}");
    }
}

/// A held send counts in neither the share of its partition nor the cap of
/// the shard while it waits, so sends held together are not turned away
/// for the room they would take. The shard checks it against both when it
/// takes it, as it checks a send that has just arrived.
#[compio::test]
async fn given_sends_held_for_the_backlog_when_they_wait_should_count_in_neither_the_share_nor_the_cap()
 {
    let send_bytes = usize::try_from(FRESH_PARTITION_PENDING_BYTES_MAX / 2).unwrap() - 64 * 1024;
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(backlog_owner(directory.path(), 1)).await;

    pumped(&owner, async {
        commit_and_count_a_backlog(&owner).await;
        let mut held = Vec::new();
        for request in 2..=4 {
            held.push(submit_send_when_room(&owner, request, send_bytes).await);
        }
        until(&owner, |owner| delayed_sends(owner) == 3).await;

        assert_eq!(owner.fresh_pending_bytes(namespace()), (0, 0));
        assert!(held.iter().all(|answer| answer.is_empty()));
        assert_eq!(owner.metrics().fresh_load_value().1, 0, "none denied");
        assert_eq!(owner.metrics().fresh_cap_denials_value(), (0, 0));

        until(&owner, |owner| committed_sends(owner) >= 2).await;
    })
    .await;
}

/// A small segment lengthens the wait between passes to its longest. Once
/// the backlog is past the limit, the next pass runs without waiting it out.
#[compio::test]
async fn given_a_backlog_past_the_limit_when_the_partition_waits_out_its_interval_should_rewrite_it_at_once()
 {
    use super::object_deletes::Shared;
    use crate::rewrite_pacing::REWRITE_INTERVAL_MAX;
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
    owner
        .plane
        .partitions()
        .set_remote_segments(Rc::new(RemoteSegments::new(
            Rc::new(ExecutorObjects::new(executor.clone())),
            layout(),
        )));
    owner.set_long_term_backlog_limit(1);
    let revision = staged_revision(&owner);
    let floor = |owner: &IoTestShard| {
        owner
            .plane
            .partitions()
            .get_by_ns(&namespace())
            .unwrap()
            .long_term_floor()
    };

    let rewritten_in = pumped(&owner, async {
        let _first = submit_send(&owner, 1, 100);
        until(&owner, |owner| committed_sends(owner) == 1).await;
        owner.request_rewrite_segments(namespace(), revision);
        until(&owner, |owner| floor(owner) == Some(0)).await;
        assert_eq!(
            owner
                .rewrite_pacing
                .borrow()
                .interval(&(namespace(), revision)),
            Some(REWRITE_INTERVAL_MAX)
        );

        let sent = std::time::Instant::now();
        let _second = submit_send(&owner, 2, 100);
        until(&owner, |owner| floor(owner) == Some(1)).await;
        sent.elapsed()
    })
    .await;

    assert!(rewritten_in < REWRITE_INTERVAL_MAX / 4, "{rewritten_in:?}");
}
