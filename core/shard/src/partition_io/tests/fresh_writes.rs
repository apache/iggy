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

use super::object_deletes::Shared;
use super::segment_uploads::{namespace, pumped, staged_revision, until};
use super::*;
use crate::fresh_writes::{FRESH_PARTITION_PENDING_BYTES_MAX, FRESH_UPLOAD_DEADLINE};
use crate::rewrite_pacing::{BACKLOG_DELAY_MAX, LONG_TERM_BACKLOG_LIMIT};
use crate::{FRESH_OBJECT_BYTES, FRESH_PENDING_BYTES_MAX};
use futures::future::LocalBoxFuture;
use iggy_binary_protocol::placement::{PLACED_BATCH_SIZE, PlacedBatch};
use iggy_object_store::{
    FreshIndex, FreshObjectId, InlineTierExecutor, KeyLayout, MemoryStore, ObjectStore,
    ObjectStoreError, PartitionManifest, RecordingStore, StoreCall, StoreCalls, TierScopeToken,
    is_fresh_object,
};
use partitions::{
    ExecutorFreshUploader, ExecutorLongTermWriter, ExecutorManifestPublisher, ExecutorObjects,
    FRESH_DELETES_PER_BATCH, FRESH_OBJECTS_PER_SWEEP, FreshDeleteOutcome, FreshListOutcome,
    FreshListRequest, FreshObjectSweeper, FreshObjectUploader, FreshUploadOutcome,
    MANIFEST_STORE_INTERVAL, ManifestPublishOutcome, ManifestPublisher, RemoteSegments,
};

pub(super) fn layout() -> KeyLayout {
    KeyLayout::new("fresh-test").unwrap()
}

type MemoryUploader = ExecutorFreshUploader<InlineTierExecutor<MemoryStore>>;

/// An interval long enough that the sends a test submits together after an
/// upload share one window.
pub(super) const LONG_INTERVAL: Duration = Duration::from_millis(500);

/// A wait for proof rows longer than any test, so that only the windows a
/// test sends carry them and the store holds no object of rows alone.
pub(super) const NO_CONFIRMATION: Duration = Duration::from_secs(3600);

/// Uploads into a memory store, unless told to refuse.
pub(super) struct TestUploader {
    uploader: MemoryUploader,
    pub(super) refuses: Cell<bool>,
    /// The first attempts that fail, in the order they come.
    pub(super) fails_first: Cell<u32>,
    attempts: Cell<u32>,
}

impl TestUploader {
    pub(super) fn new() -> Rc<Self> {
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

    pub(super) fn store(&self) -> &MemoryStore {
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
pub(super) async fn placing_owner(directory: &Path) -> IoTestShard {
    Box::pin(placing_owner_of_topic(
        directory,
        TopicRuntimeOptions::default(),
    ))
    .await
}

/// [`placing_owner`] whose committed topic resolves to the options
/// `committed`. Proof rows wait for the windows of the test's sends, so
/// that the store holds only the objects of those sends.
#[allow(clippy::future_not_send)]
async fn placing_owner_of_topic(directory: &Path, committed: TopicRuntimeOptions) -> IoTestShard {
    let bus = Rc::new(IggyMessageBus::new(0));
    let (owner, _sender) = test_owner_of_topic(&bus, None, None, None, committed);
    Box::pin(placing(owner, directory)).await
}

/// [`placing_owner`] whose committed topic is a geo topic that sets
/// `durations` for itself.
#[allow(clippy::future_not_send)]
async fn placing_owner_of_geo_topic(
    directory: &Path,
    durations: iggy_common::GeoTopicDurations,
) -> IoTestShard {
    let bus = Rc::new(IggyMessageBus::new(0));
    let (owner, _sender) = test_owner_of_geo_topic(&bus, durations);
    Box::pin(placing(owner, directory)).await
}

/// `owner` with its partition [`namespace`] a single-replica primary on
/// disk that places its batches.
#[allow(clippy::future_not_send)]
async fn placing(owner: IoTestShard, directory: &Path) -> IoTestShard {
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
    owner.set_fresh_confirm_after(NO_CONFIRMATION);
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
pub(super) fn submit_send(
    owner: &IoTestShard,
    request: u64,
    payload_len: usize,
) -> Receiver<Option<Message<GenericHeader>>> {
    submit_send_to(owner, namespace(), request, payload_len)
}

/// [`submit_send`] to `partition`.
pub(super) fn submit_send_to(
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

/// Objects with data this shard stored: every object it stored but those
/// of proof rows alone.
pub(super) fn data_objects_stored(owner: &IoTestShard) -> u64 {
    let metrics = owner.metrics();
    metrics.fresh_uploads_value().0 - metrics.fresh_confirmations_value()
}

/// Ops the partition `partition` committed.
pub(super) fn committed(owner: &IoTestShard, partition: IggyNamespace) -> u64 {
    owner
        .plane
        .partitions()
        .get_by_ns(&partition)
        .unwrap()
        .consensus()
        .commit_min()
}

/// Ops the partition committed and wrote to its segment. Each send is one.
pub(super) fn committed_sends(owner: &IoTestShard) -> u64 {
    let partition = owner.plane.partitions().get_by_ns(&namespace()).unwrap();
    let written = partition.log.active_segment().size.as_bytes_u64() / PLACED_BATCH_SIZE as u64;
    partition.consensus().commit_min().min(written)
}

/// The placed batches the partition's segment holds.
pub(super) fn stored_placed_batches(directory: &Path) -> Vec<PlacedBatch> {
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

/// Whether the shard paces the rewrites of `partition`.
fn paced(owner: &IoTestShard, partition: IggyNamespace) -> bool {
    let revision = owner
        .plane
        .partitions()
        .get_by_ns(&partition)
        .unwrap()
        .created_revision();
    owner.rewrite_pacing.borrow().paces(&(partition, revision))
}

/// The whole write path on one shard: a producer's sends are stored and
/// committed as placed batches, the rewrite passes store them again as
/// long-term segments, and the partition's long-term log names them. Nothing
/// stages a pass here: the shard starts them for the partition it leads. A
/// primary beside it that keeps its batches whole gets no pass.
#[compio::test]
async fn given_committed_sends_when_nothing_stages_a_pass_should_store_and_publish_them_as_long_term_segments()
 {
    use super::object_deletes::Shared;
    use crate::rewrite_pacing::REWRITE_INTERVAL_MAX;
    use iggy_object_store::{LongTermKey, SegmentObject};
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

    let whole = IggyNamespace::new(0, 0, 1);
    partitions
        .get_mut_by_ns(&whole)
        .unwrap()
        .set_offset_space_used(true);

    // A shard that got its writer looks over all its partitions once. That
    // look must not be what finds the sends below.
    assert!(owner.rewrite_pacing.borrow_mut().take_sweep_wanted());

    let published_in = pumped(&owner, async {
        let _answers = [submit_send(&owner, 1, 100), submit_send(&owner, 2, 300)];
        until(&owner, |owner| committed_sends(owner) == 2).await;
        let committed = std::time::Instant::now();
        until(&owner, |owner| floor(owner) == Some(1)).await;
        committed.elapsed()
    })
    .await;

    assert!(published_in < REWRITE_INTERVAL_MAX, "{published_in:?}");
    assert!(paced(&owner, namespace()));
    assert!(!paced(&owner, whole));
    let partition = owner.plane.partitions().get_by_ns(&namespace()).unwrap();
    let segments = partition.long_term_segments();
    assert_eq!(segments.len(), 1);
    assert_eq!((segments[0].start_offset, segments[0].end_offset), (0, 1));
    assert_eq!(partition.unpublished_segment(), None);
    assert_eq!(
        partition.long_term_log().unwrap().registrations(),
        [],
        "the publish cleared the registration the rewrite made"
    );
    let store = executor.0.store();
    let log_key = layout().long_term_key(&LongTermKey {
        prefix: partitions::object_prefix(namespace(), revision),
        start_offset: 0,
        end_offset: 1,
        object: SegmentObject::Log,
    });
    assert_eq!(store.head(&log_key).await.unwrap().size, segments[0].size);
    let mut fresh_with_data = 0;
    for key in store.keys() {
        if let Some(id) = layout().parse_fresh_key(&key) {
            let size = store.head(&key).await.unwrap().size;
            let bytes = store.get_range(&key, 0..size).await.unwrap().bytes;
            let index = FreshIndex::decode(&bytes, FreshObjectId::from_raw(id)).unwrap();
            fresh_with_data += usize::from(!index.is_confirmation());
        }
    }
    assert_eq!(fresh_with_data, 2, "a fresh object per send");
    assert_eq!(
        store.keys().len()
            - store
                .keys()
                .iter()
                .filter(|key| layout().parse_fresh_key(key).is_some())
                .count(),
        2,
        "the log and its index"
    );
}

/// The whole store of one test: the fresh objects, the long-term segments,
/// and the sweeps, all through one executor.
#[allow(clippy::future_not_send)]
pub(super) async fn swept_owner(directory: &Path) -> (IoTestShard, super::object_deletes::Shared) {
    with_swept_store(Box::pin(placing_owner(directory)).await)
}

/// [`swept_owner`] whose committed topic places its batches, as the topic
/// of a placing partition does outside these fixtures.
#[allow(clippy::future_not_send)]
async fn swept_owner_of_placing_topic(
    directory: &Path,
) -> (IoTestShard, super::object_deletes::Shared) {
    let committed = TopicRuntimeOptions {
        places_batches: true,
        ..Default::default()
    };
    with_swept_store(Box::pin(placing_owner_of_topic(directory, committed)).await)
}

/// Give `owner` the one store of [`swept_owner`].
fn with_swept_store(owner: IoTestShard) -> (IoTestShard, super::object_deletes::Shared) {
    use super::object_deletes::Shared;
    use partitions::{
        ExecutorFreshSweeper, ExecutorLongTermWriter, ExecutorObjects, RemoteSegments,
    };

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
pub(super) fn fresh_objects(store: &MemoryStore) -> Vec<u128> {
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
pub(super) async fn sweep(owner: &IoTestShard, now: IggyTimestamp) {
    let settled = owner.fresh_sweeps_settled();
    owner.request_sweep_fresh_objects(now);
    until(owner, |owner| owner.fresh_sweeps_settled() > settled).await;
    // The delete runs inline in the memory store, and its result is one
    // frame behind.
    compio::time::sleep(Duration::from_millis(50)).await;
}

/// A time one grace period and a second after `now`.
pub(super) fn past_grace(now: IggyTimestamp) -> IggyTimestamp {
    let grace = u64::try_from(partitions::FRESH_OBJECT_GRACE.as_micros()).unwrap();
    IggyTimestamp::from(now.as_micros() + grace + 1_000_000)
}

/// Rewrite the committed sends of the partition up to `floor`.
#[allow(clippy::future_not_send)]
pub(super) async fn rewrite_up_to(owner: &IoTestShard, floor: u64) {
    rewrite_partition_up_to(owner, namespace(), floor).await;
}

/// [`rewrite_up_to`] for `partition`.
#[allow(clippy::future_not_send)]
async fn rewrite_partition_up_to(owner: &IoTestShard, partition: IggyNamespace, floor: u64) {
    let partitions = owner.plane.partitions();
    let revision = partitions.get_by_ns(&partition).unwrap().created_revision();
    owner.request_rewrite_segments(partition, revision);
    until(owner, |owner| {
        long_term_floor(owner, partition) == Some(floor)
    })
    .await;
}

/// The long-term floor of `partition`.
fn long_term_floor(owner: &IoTestShard, partition: IggyNamespace) -> Option<u64> {
    owner
        .plane
        .partitions()
        .get_by_ns(&partition)
        .unwrap()
        .long_term_floor()
}

/// Make the partition beside [`namespace`] place its batches and keep them
/// in a segment file under `directory`, as [`placing_owner`] does for its
/// own.
#[allow(clippy::future_not_send)]
async fn place_other_partition_on_disk(owner: &IoTestShard, directory: &Path) -> IggyNamespace {
    let other = IggyNamespace::new(0, 0, 1);
    let placing = owner.plane.partitions().get_mut_by_ns(&other).unwrap();
    placing.set_runtime_options(TopicRuntimeOptions {
        messages_required_to_save: Some(1),
        places_batches: true,
        ..Default::default()
    });
    attach_segment_files(placing, directory, false).await;
    placing.register_builtin_state_machines().unwrap();
    other
}

/// Take `partition` off the shard the way a delete of it does: the
/// reconciler fences the namespace, and the pump drops the partition once
/// its files are gone.
#[allow(clippy::future_not_send)]
async fn delete_partition(owner: &IoTestShard, partition: IggyNamespace) {
    owner.plane.partitions().tombstone(partition);
    owner.enqueue_reconcile_op(ReconcileOp::ConfirmRemove {
        namespace: partition,
    });
    until(owner, |owner| {
        owner.plane.partitions().get_io_owner(&partition).is_none()
    })
    .await;
    // The marker of the op leaves the inbox a frame later, and a sweep
    // asked for while the inbox is full is dropped.
    compio::time::sleep(TASK_POLL_INTERVAL).await;
}

/// The object each placed batch of the segment under `directory` names, in
/// the order of the offsets.
fn named_objects(directory: &Path) -> Vec<u128> {
    stored_placed_batches(directory)
        .iter()
        .map(|placed| placed.placement.object_id)
        .collect()
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

/// Rewrites through the owner's writer, and ends every segment at `up_to` at
/// the latest, the way the size cap of a segment ends one before the commit
/// point. Past `up_to` it rewrites nothing.
struct CappedWriter {
    writer: Rc<dyn partitions::LongTermWriter>,
    up_to: u64,
}

impl partitions::LongTermWriter for CappedWriter {
    fn write(
        &self,
        mut rewrite: partitions::SegmentRewrite,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, partitions::RewriteOutcome> {
        if rewrite.start_offset > self.up_to {
            return Box::pin(std::future::ready(Err(iggy_object_store::TierJobCancelled)));
        }
        rewrite.up_to = rewrite.up_to.min(self.up_to);
        self.writer.write(rewrite, scope)
    }
}

/// One object can hold sends that are rewritten and sends that are not. It
/// stays for as long as one placed batch above the floor names it.
#[compio::test]
async fn given_a_send_above_the_long_term_floor_when_sweeps_run_should_keep_its_fresh_object() {
    let directory = tempfile::tempdir().unwrap();
    let (owner, executor) = Box::pin(swept_owner(directory.path())).await;
    let store = executor.0.store();
    owner.set_fresh_upload_pace(LONG_INTERVAL, 1);
    owner.set_long_term_writer(Some(Rc::new(CappedWriter {
        writer: owner.long_term_writer().unwrap(),
        up_to: 1,
    })));
    let now = IggyTimestamp::now();

    let shared = pumped(&owner, async {
        // Stored at once, so the two sends after it share the next object.
        let _alone = submit_send(&owner, 1, 100);
        until(&owner, |owner| committed_sends(owner) == 1).await;
        let _shared = [submit_send(&owner, 2, 100), submit_send(&owner, 3, 100)];
        until(&owner, |owner| committed_sends(owner) == 3).await;
        let objects = fresh_objects(store);
        assert_eq!(objects.len(), 2);
        assert_eq!(
            named_objects(directory.path()),
            [objects[0], objects[1], objects[1]]
        );

        // The segment ends between the two sends of the second object.
        rewrite_up_to(&owner, 1).await;
        sweep(&owner, now).await;
        sweep(&owner, past_grace(now)).await;
        sweep(&owner, past_grace(past_grace(now))).await;
        objects[1]
    })
    .await;

    assert_eq!(long_term_floor(&owner, namespace()), Some(1));
    assert_eq!(fresh_objects(store), vec![shared]);
}

/// A delete of a partition takes its placed batches with it, so nothing
/// names the objects only it named. An object it shared with a partition
/// that is still there stays, until the topic goes with every partition.
#[compio::test]
async fn given_a_deleted_partition_when_sweeps_run_past_the_grace_should_delete_the_fresh_objects_only_it_named()
 {
    let directory = tempfile::tempdir().unwrap();
    let other_directory = tempfile::tempdir().unwrap();
    let (owner, executor) = Box::pin(swept_owner(directory.path())).await;
    let other = Box::pin(place_other_partition_on_disk(
        &owner,
        other_directory.path(),
    ))
    .await;
    let store = executor.0.store();
    owner.set_fresh_upload_pace(LONG_INTERVAL, 1);
    let now = IggyTimestamp::now();
    let unnamed_since = past_grace(now);
    let topic_deleted_at = past_grace(unnamed_since);

    let shared = pumped(&owner, async {
        // Stored at once, so the two sends after it share the next object.
        let _alone = submit_send(&owner, 1, 100);
        until(&owner, |owner| committed_sends(owner) == 1).await;
        let _first = submit_send(&owner, 2, 100);
        let _second = submit_send_to(&owner, other, 3, 100);
        until(&owner, |owner| {
            committed_sends(owner) == 2 && committed(owner, other) == 1
        })
        .await;
        let objects = fresh_objects(store);
        assert_eq!(objects.len(), 2);
        assert_eq!(named_objects(directory.path()), objects);
        assert_eq!(named_objects(other_directory.path()), [objects[1]]);
        sweep(&owner, now).await;

        delete_partition(&owner, namespace()).await;
        sweep(&owner, unnamed_since).await;
        assert_eq!(
            fresh_objects(store),
            objects,
            "the objects of a deleted partition wait out the grace period too"
        );
        sweep(&owner, topic_deleted_at).await;
        assert_eq!(
            fresh_objects(store),
            [objects[1]],
            "the other partition still names the object they share"
        );

        for partition in 1..TEST_PARTITIONS {
            delete_partition(&owner, IggyNamespace::new(0, 0, partition)).await;
        }
        sweep(&owner, topic_deleted_at).await;
        assert_eq!(fresh_objects(store), [objects[1]]);
        sweep(&owner, past_grace(topic_deleted_at)).await;
        objects[1]
    })
    .await;

    assert_eq!(fresh_objects(store), Vec::<u128>::new(), "{shared:032x}");
    assert_eq!(owner.metrics().fresh_objects_deleted_value(), 2);
}

/// An object holds the sends of every placing partition of its window, and
/// each partition's rewrite moves its floor alone. The object stays while
/// one of them still names it above its floor.
#[compio::test]
async fn given_an_object_two_partitions_share_when_one_is_rewritten_should_keep_it_until_both_are()
{
    let directory = tempfile::tempdir().unwrap();
    let other_directory = tempfile::tempdir().unwrap();
    let (owner, executor) = Box::pin(swept_owner(directory.path())).await;
    let other = Box::pin(place_other_partition_on_disk(
        &owner,
        other_directory.path(),
    ))
    .await;
    let store = executor.0.store();
    owner.set_fresh_upload_pace(LONG_INTERVAL, 1);
    let now = IggyTimestamp::now();
    let rewritten_at = past_grace(now);

    pumped(&owner, async {
        // Stored at once, so the two sends after it share the next object.
        let _alone = submit_send(&owner, 1, 100);
        until(&owner, |owner| committed_sends(owner) == 1).await;
        let _first = submit_send(&owner, 2, 100);
        let _second = submit_send_to(&owner, other, 3, 100);
        until(&owner, |owner| {
            committed_sends(owner) == 2 && committed(owner, other) == 1
        })
        .await;
        let objects = fresh_objects(store);
        assert_eq!(objects.len(), 2);
        assert_eq!(named_objects(directory.path()), objects);
        assert_eq!(named_objects(other_directory.path()), [objects[1]]);

        rewrite_up_to(&owner, 1).await;
        assert_eq!(long_term_floor(&owner, other), None);
        sweep(&owner, now).await;
        sweep(&owner, rewritten_at).await;
        assert_eq!(
            fresh_objects(store),
            [objects[1]],
            "the partition that is not rewritten still names the object"
        );

        rewrite_partition_up_to(&owner, other, 0).await;
        sweep(&owner, rewritten_at).await;
        assert_eq!(
            fresh_objects(store),
            [objects[1]],
            "the grace period starts when the last partition stops naming the object"
        );
        sweep(&owner, past_grace(rewritten_at)).await;
    })
    .await;

    assert_eq!(fresh_objects(store), Vec::<u128>::new());
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

/// A replica of the headless group that places its batches and holds a
/// committed message above its long-term floor, on a shard whose writer ends
/// no rewrite.
#[allow(clippy::future_not_send)]
async fn placing_headless(replica: u8) -> HeadlessReplica {
    let replica = Box::pin(HeadlessReplica::journaled(replica, namespace(), &[], None)).await;
    place_partition(&replica.owner, namespace());
    replica
        .owner
        .plane
        .partitions()
        .get_mut_by_ns(&namespace())
        .unwrap()
        .set_offset_space_used(true);
    replica
        .owner
        .set_long_term_writer(Some(Rc::new(HeldWriter)));
    replica
}

/// Step `replica` until `done`.
#[allow(clippy::future_not_send)]
async fn stepped(replica: &HeadlessReplica, what: &str, done: impl Fn() -> bool) {
    let mut scratch = Vec::new();
    let stepping = async {
        while !done() {
            replica.step(&mut scratch).await;
        }
    };
    compio::time::timeout(REPLY_DEADLINE, Box::pin(stepping))
        .await
        .unwrap_or_else(|_| panic!("the replica did not get to {what}"));
}

/// The primary of the group is down. The two replicas left elect the
/// candidate, which is the only one whose shard then runs rewrite passes,
/// and only from the moment its view is a normal one. Nothing stages a pass.
#[compio::test]
async fn given_a_placing_backup_when_it_becomes_the_primary_should_start_its_passes_and_no_backup_should()
 {
    use crate::rewrite_pacing::REWRITE_INTERVAL_START;
    const NEXT_VIEW: u32 = REPAIR_VIEW + 1;
    let backup = Box::pin(placing_headless(HEADLESS_BACKUP)).await;
    let candidate = Box::pin(placing_headless(HEADLESS_CANDIDATE)).await;
    let consensus = |replica: &HeadlessReplica| {
        let partition = replica.partition(namespace());
        (
            partition.consensus().is_primary(),
            partition.consensus().is_normal(),
        )
    };
    let mut scratch = Vec::new();

    candidate.step(&mut scratch).await;
    candidate.owner.tick_long_term_rewrites().await;
    assert_eq!(consensus(&candidate), (false, true));
    assert!(!paced(&candidate.owner, namespace()), "a backup");

    // Each one misses the heartbeats of the primary and votes to leave its
    // view.
    for (replica, peer) in [(&candidate, HEADLESS_BACKUP), (&backup, HEADLESS_CANDIDATE)] {
        for _ in 0..2 * TimeoutManager::NORMAL_HEARTBEAT_TICKS {
            if replica.sent_to(peer, Command::StartViewChange, NEXT_VIEW) {
                break;
            }
            assert!(replica.owner.tick_partitions(&mut scratch).await.is_none());
        }
    }
    candidate
        .owner
        .on_message(backup.take(HEADLESS_CANDIDATE, Command::StartViewChange))
        .await;
    candidate.step(&mut scratch).await;
    assert_eq!(consensus(&candidate), (true, false));
    assert!(
        !paced(&candidate.owner, namespace()),
        "its view has not started"
    );

    backup
        .owner
        .on_message(candidate.take(HEADLESS_BACKUP, Command::StartViewChange))
        .await;
    stepped(&backup, "its view change", || {
        backup.sent_to(HEADLESS_CANDIDATE, Command::DoViewChange, NEXT_VIEW)
    })
    .await;
    candidate
        .owner
        .on_message(backup.take(HEADLESS_CANDIDATE, Command::DoViewChange))
        .await;
    stepped(&candidate, "its view", || {
        consensus(&candidate) == (true, true)
    })
    .await;
    candidate.step(&mut scratch).await;

    assert!(paced(&candidate.owner, namespace()));
    compio::time::sleep(REWRITE_INTERVAL_START).await;
    candidate.owner.tick_long_term_rewrites().await;
    let reported = candidate.owner.metrics().long_term_lags();
    assert_eq!(
        reported
            .iter()
            .map(|lag| (lag.namespace, lag.messages))
            .collect::<Vec<_>>(),
        [(namespace(), 1)]
    );
    assert!(
        candidate.partition(namespace()).segment_rewrite_running(),
        "its first pass started a rewrite"
    );

    backup
        .owner
        .on_message(candidate.take(HEADLESS_BACKUP, Command::StartView))
        .await;
    stepped(&backup, "the new view", || {
        consensus(&backup) == (false, true)
    })
    .await;
    backup.step(&mut scratch).await;
    assert!(!paced(&backup.owner, namespace()));
    // The pass the segment cleaner stages gives a backup a wait too. It is
    // not the one that reports the partition.
    backup
        .owner
        .on_rewrite_segments(namespace(), staged_revision(&backup.owner))
        .await;
    assert!(paced(&backup.owner, namespace()));
    compio::time::sleep(REWRITE_INTERVAL_START).await;
    backup.owner.tick_long_term_rewrites().await;
    assert_eq!(backup.owner.metrics().long_term_lags(), []);
    assert!(!backup.partition(namespace()).segment_rewrite_running());
}

/// The store may hold a manifest the primary before wrote, which lags what
/// is committed. The candidate that wins the view stores its own once its
/// view is a normal one, with no segment left to commit, and nothing stages
/// a pass here.
#[compio::test]
async fn given_a_placing_backup_when_it_becomes_the_primary_should_store_the_manifest_of_its_long_term_log()
 {
    use super::object_deletes::Shared;
    use iggy_object_store::PartitionManifest;
    use partitions::ExecutorManifestPublisher;
    const NEXT_VIEW: u32 = REPAIR_VIEW + 1;
    let backup = Box::pin(placing_headless(HEADLESS_BACKUP)).await;
    let candidate = Box::pin(placing_headless(HEADLESS_CANDIDATE)).await;
    let executor = Shared(Rc::new(InlineTierExecutor::new(MemoryStore::new())));
    candidate
        .owner
        .set_manifest_publisher(Some(Rc::new(ExecutorManifestPublisher::new(
            executor.clone(),
            layout(),
        ))));
    let store = executor.0.store();
    let consensus = |replica: &HeadlessReplica| {
        let partition = replica.partition(namespace());
        (
            partition.consensus().is_primary(),
            partition.consensus().is_normal(),
        )
    };
    let awaits_pass =
        |replica: &HeadlessReplica| replica.partition(namespace()).manifest_awaits_first_pass();
    let mut scratch = Vec::new();

    candidate.step(&mut scratch).await;
    assert_eq!(consensus(&candidate), (false, true));
    assert!(!awaits_pass(&candidate), "a backup");

    for (replica, peer) in [(&candidate, HEADLESS_BACKUP), (&backup, HEADLESS_CANDIDATE)] {
        for _ in 0..2 * TimeoutManager::NORMAL_HEARTBEAT_TICKS {
            if replica.sent_to(peer, Command::StartViewChange, NEXT_VIEW) {
                break;
            }
            assert!(replica.owner.tick_partitions(&mut scratch).await.is_none());
        }
    }
    candidate
        .owner
        .on_message(backup.take(HEADLESS_CANDIDATE, Command::StartViewChange))
        .await;
    candidate.step(&mut scratch).await;
    assert_eq!(consensus(&candidate), (true, false));
    assert!(!awaits_pass(&candidate), "its view has not started");
    backup
        .owner
        .on_message(candidate.take(HEADLESS_BACKUP, Command::StartViewChange))
        .await;
    stepped(&backup, "its view change", || {
        backup.sent_to(HEADLESS_CANDIDATE, Command::DoViewChange, NEXT_VIEW)
    })
    .await;
    candidate
        .owner
        .on_message(backup.take(HEADLESS_CANDIDATE, Command::DoViewChange))
        .await;
    stepped(&candidate, "its view", || {
        consensus(&candidate) == (true, true)
    })
    .await;
    candidate.step(&mut scratch).await;

    let revision = candidate.partition(namespace()).created_revision();
    let manifest_key = layout().manifest_key(&partitions::object_prefix(namespace(), revision));
    assert_eq!(store.keys(), Vec::<String>::new());
    pumped(&candidate.owner, async {
        until(&candidate.owner, |_| store.keys().contains(&manifest_key)).await;
    })
    .await;

    let size = store.head(&manifest_key).await.unwrap().size;
    let object = store.get_range(&manifest_key, 0..size).await.unwrap();
    let manifest = PartitionManifest::decode(&object.bytes).unwrap();
    let partition = candidate.partition(namespace());
    assert_eq!(manifest.segments, partition.long_term_segments());
    assert_eq!(manifest.floor, partition.long_term_floor());
    assert!(!partition.manifest_awaits_first_pass());
}

/// A shard gets its writer after its node proved the store is its own. A
/// partition that committed before then sleeps, so no tick visits it: the
/// shard looks over all its partitions once. A tombstoned one is left out.
#[compio::test]
async fn given_a_primary_that_committed_without_a_writer_when_the_shard_gets_one_should_start_its_passes()
 {
    use crate::rewrite_pacing::REWRITE_INTERVAL_START;
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    owner.set_fresh_uploader(Some(TestUploader::new()));
    pumped(&owner, async {
        let _first = submit_send(&owner, 1, 100);
        until(&owner, |owner| committed_sends(owner) == 1).await;
    })
    .await;
    let partitions = owner.plane.partitions();
    let mut scratch = Vec::new();
    assert!(owner.tick_partitions(&mut scratch).await.is_none());
    assert!(!paced(&owner, namespace()), "the shard cannot rewrite");

    partitions.tombstone(namespace());
    owner.set_long_term_writer(Some(Rc::new(HeldWriter)));
    assert!(owner.tick_partitions(&mut scratch).await.is_none());
    owner.tick_long_term_rewrites().await;
    partitions.untombstone(&namespace());
    assert!(!paced(&owner, namespace()), "tombstoned");

    owner.set_long_term_writer(Some(Rc::new(HeldWriter)));
    owner.tick_long_term_rewrites().await;

    assert!(paced(&owner, namespace()));
    compio::time::sleep(REWRITE_INTERVAL_START).await;
    owner.tick_long_term_rewrites().await;
    let partition = partitions.get_by_ns(&namespace()).unwrap();
    assert!(partition.segment_rewrite_running());
}

/// A reader of the bucket finds a long-term segment through the manifest,
/// so the commit of the segment's command is what asks for the manifest.
/// The one this primary stored when its view began lists nothing, and
/// nothing else asks for one here.
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

    let stored = async || {
        let size = store.head(&manifest_key).await.unwrap().size;
        let object = store.get_range(&manifest_key, 0..size).await.unwrap();
        PartitionManifest::decode(&object.bytes).unwrap()
    };

    pumped(&owner, async {
        let _answers = [submit_send(&owner, 1, 100), submit_send(&owner, 2, 300)];
        until(&owner, |owner| committed_sends(owner) == 2).await;
        owner.request_rewrite_segments(namespace(), revision);
        until(&owner, |_| store.keys().contains(&manifest_key)).await;
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while stored().await.floor != Some(1) {
            assert!(
                std::time::Instant::now() < deadline,
                "no manifest lists the segment"
            );
            compio::time::sleep(crate::CONSENSUS_TICK_INTERVAL).await;
        }
    })
    .await;

    let manifest = stored().await;
    let partition = owner.plane.partitions().get_by_ns(&namespace()).unwrap();
    assert_eq!(manifest.floor, Some(1));
    assert_eq!(manifest.segments, partition.long_term_segments());
}

/// Publishes into a memory store, which refuses the first `refused` puts.
struct RefusingPublisher {
    publisher: partitions::ExecutorManifestPublisher<InlineTierExecutor<MemoryStore>>,
    refused: u32,
    attempts: Cell<u32>,
}

impl partitions::ManifestPublisher for RefusingPublisher {
    fn publish(
        &self,
        manifest: iggy_object_store::PartitionManifest,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, partitions::ManifestPublishOutcome> {
        let attempt = self.attempts.get();
        self.attempts.set(attempt + 1);
        if attempt < self.refused {
            Box::pin(std::future::ready(Ok(Err(ObjectStoreError::Unavailable(
                "503".to_owned(),
            )))))
        } else {
            self.publisher.publish(manifest, scope)
        }
    }
}

/// The rewrite of the partition never ends, so no long-term segment commits
/// and nothing else asks for its manifest once the first put failed.
#[compio::test]
async fn given_a_placing_primary_whose_first_manifest_put_fails_when_nothing_commits_should_store_the_manifest_after_the_retry()
 {
    use iggy_object_store::PartitionManifest;
    use partitions::{ExecutorManifestPublisher, MANIFEST_RETRY_AFTER};

    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    owner.set_fresh_uploader(Some(TestUploader::new()));
    owner.set_long_term_writer(Some(Rc::new(HeldWriter)));
    let publisher = Rc::new(RefusingPublisher {
        publisher: ExecutorManifestPublisher::new(
            InlineTierExecutor::new(MemoryStore::new()),
            layout(),
        ),
        refused: 1,
        attempts: Cell::new(0),
    });
    owner.set_manifest_publisher(Some(publisher.clone()));
    let store = publisher.publisher.executor().store();
    let manifest_key = layout().manifest_key(&partitions::object_prefix(
        namespace(),
        staged_revision(&owner),
    ));

    let stored_in = pumped(&owner, async {
        let started = std::time::Instant::now();
        let _answer = submit_send(&owner, 1, 100);
        until(&owner, |owner| {
            committed_sends(owner) == 1 && paced(owner, namespace())
        })
        .await;
        until(&owner, |_| store.keys().contains(&manifest_key)).await;
        started.elapsed()
    })
    .await;

    assert!(stored_in >= MANIFEST_RETRY_AFTER / 2, "{stored_in:?}");
    assert_eq!(publisher.attempts.get(), 2, "the refused put and its retry");
    let partition = owner.plane.partitions().get_by_ns(&namespace()).unwrap();
    assert_eq!(partition.long_term_floor(), None, "nothing was rewritten");
    let size = store.head(&manifest_key).await.unwrap().size;
    let object = store.get_range(&manifest_key, 0..size).await.unwrap();
    let manifest = PartitionManifest::decode(&object.bytes).unwrap();
    assert_eq!(manifest.floor, None);
    assert_eq!(manifest.segments, []);
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

/// Submit a trim of the long-term log up to `up_to_offset`, as the primary
/// sends one.
fn submit_trim(owner: &IoTestShard, up_to_offset: u64, request: u64) {
    let request = owner
        .plane
        .partitions()
        .get_by_ns(&namespace())
        .unwrap()
        .trim_segments_request(up_to_offset, request)
        .unwrap();
    owner.senders[0]
        .try_send(ShardFrame::lifecycle(LifecycleFrame::PartitionSubmit {
            request,
            reply: channel(1).0,
            attachment: None,
        }))
        .unwrap();
}

/// A trim changes what the manifest lists, and a reader must stop looking
/// for the segments it removed before their objects go, so its commit asks
/// for the manifest. A trim that removes nothing leaves the manifest in the
/// store as the current one, and asks for none.
#[compio::test]
async fn given_a_manifest_publisher_when_a_trim_commits_should_publish_the_manifest_only_if_it_removed_a_segment()
 {
    use super::object_deletes::Shared;
    use crate::rewrite_pacing::RewriteResult;
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
    let key = (namespace(), revision);
    let manifest_key = layout().manifest_key(&partitions::object_prefix(namespace(), revision));
    let store = executor.0.store();
    let stored_manifest = async || {
        let size = store.head(&manifest_key).await.ok()?.size;
        let object = store.get_range(&manifest_key, 0..size).await.unwrap();
        Some(PartitionManifest::decode(&object.bytes).unwrap())
    };
    let manifest_with = async |segments: usize| {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            match stored_manifest().await {
                Some(manifest) if manifest.segments.len() == segments => return manifest,
                _ => assert!(
                    std::time::Instant::now() < deadline,
                    "no manifest of {segments} segments was stored"
                ),
            }
            compio::time::sleep(TASK_POLL_INTERVAL).await;
        }
    };
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
        // The wait is longer than this test may take, so the next pass is
        // made due after the shortest one.
        owner.rewrite_pacing.borrow_mut().settle(
            key,
            RewriteResult::Resent,
            owner.bus.monotonic_micros(),
        );
        let _second = submit_send(&owner, 2, 100);
        until(&owner, |owner| floor(owner) == Some(1)).await;
        let listed = manifest_with(2).await;
        assert_eq!((listed.floor, listed.trim_point), (Some(1), None));

        submit_trim(&owner, 0, 1000);
        let trimmed = manifest_with(1).await;
        assert_eq!((trimmed.floor, trimmed.trim_point), (Some(1), Some(0)));
        let partition = owner.plane.partitions().get_by_ns(&namespace()).unwrap();
        assert_eq!(trimmed.segments, partition.long_term_segments());

        let commits = committed(&owner, namespace());
        submit_trim(&owner, 0, 1001);
        until(&owner, |owner| committed(owner, namespace()) > commits).await;
        for _ in 0..20 {
            compio::time::sleep(TASK_POLL_INTERVAL).await;
        }
        assert_eq!(
            stored_manifest().await.unwrap().published_at,
            trimmed.published_at,
            "the manifest in the store is the one of the first trim"
        );
    })
    .await;
}

/// A delete pass over a partition that places its batches lists the
/// objects of its long-term segments. An object that the log does not list
/// and may still come to list is kept, and the shard reports how many of
/// those its partitions found.
#[compio::test]
async fn given_a_placing_partition_when_a_delete_pass_runs_should_list_its_long_term_objects_and_report_the_unannounced()
 {
    use super::object_deletes::Shared;
    use iggy_object_store::{LongTermKey, PutBody, PutOptions, SegmentObject};
    use partitions::{
        ExecutorDeleter, ExecutorLongTermWriter, ExecutorObjects, ObjectDeleteWork, RemoteSegments,
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
    owner.set_object_deleter(Some(Rc::new(ExecutorDeleter::new(
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
    let store = executor.0.store();
    // From where the segment of the log starts to above its floor, as a
    // primary stores that lost to the one whose segment the log lists.
    let unannounced: Vec<String> = SegmentObject::ALL
        .iter()
        .map(|object| {
            layout().long_term_key(&LongTermKey {
                prefix: partitions::object_prefix(namespace(), revision),
                start_offset: 0,
                end_offset: 5,
                object: *object,
            })
        })
        .collect();
    for key in &unannounced {
        store
            .put(key, PutBody::from_bytes(vec![1]), &PutOptions::default())
            .await
            .unwrap();
    }

    pumped(&owner, async {
        let _answers = [submit_send(&owner, 1, 100), submit_send(&owner, 2, 300)];
        until(&owner, |owner| committed_sends(owner) == 2).await;
        owner.request_rewrite_segments(namespace(), revision);
        until(&owner, |owner| floor(owner) == Some(1)).await;
        assert_eq!(owner.metrics().long_term_unannounced_objects_value(), 0);

        owner.request_delete_objects(namespace(), revision, IggyTimestamp::now(), 0);
        until(&owner, |owner| {
            owner.metrics().long_term_unannounced_objects_value() == 1
        })
        .await;
    })
    .await;

    let keys = store.keys();
    assert!(unannounced.iter().all(|key| keys.contains(key)));
    let partition = owner
        .plane
        .partitions()
        .get_mut_by_ns(&namespace())
        .unwrap();
    assert!(
        !matches!(
            partition.begin_object_delete(IggyTimestamp::now(), 0),
            Some((job, _)) if matches!(job.work, ObjectDeleteWork::SweepLongTerm { .. })
        ),
        "the partition listed its objects in this view"
    );
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

/// Make the shard lead `count` partitions that place their batches: the
/// fixture's own, and single-replica primaries in memory beside them.
pub(super) fn place_partitions(owner: &IoTestShard, count: usize) -> Vec<IggyNamespace> {
    let partitions = owner.plane.partitions();
    (0..count)
        .map(|partition_id| {
            let partition = IggyNamespace::new(0, 0, partition_id);
            if partition == namespace() {
                return partition;
            }
            if partitions.get_by_ns(&partition).is_none() {
                let consensus = VsrConsensus::new(
                    1,
                    0,
                    1,
                    partition.inner(),
                    Rc::clone(&owner.bus),
                    LocalPipeline::new(),
                );
                consensus.init();
                partitions.insert(
                    partition,
                    IggyPartition::with_in_memory_storage(
                        Arc::new(PartitionStats::default()),
                        consensus,
                        IggyByteSize::from(SEGMENT_BYTES),
                    ),
                );
            }
            place_partition(owner, partition)
        })
        .collect()
}

/// Every placing partition of the shard writes into the same window, so the
/// store gets one object per upload interval however many partitions send
/// within it.
#[compio::test]
async fn given_sends_to_many_placing_partitions_when_they_arrive_within_an_interval_should_store_objects_by_window_and_not_by_partition()
 {
    const PARTITIONS: usize = 64;
    const ROUNDS: u64 = 3;
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let placing = place_partitions(&owner, PARTITIONS);
    assert_eq!(placing.len(), PARTITIONS);
    let uploader = TestUploader::new();
    owner.set_fresh_uploader(Some(uploader.clone()));
    // Free slots throughout, so only the interval holds an upload back.
    owner.set_fresh_upload_pace(LONG_INTERVAL, 4);

    pumped(&owner, async {
        let mut request = 0;
        for round in 1..=ROUNDS {
            for partition in &placing {
                request += 1;
                // Offered as the pump offers a send it routed. The pump
                // itself refuses a partition that the fixture's metadata
                // does not list.
                owner.offer_fresh_send(
                    sized_send_request(*partition, request, 100),
                    channel(1).0,
                    None,
                );
            }
            until(&owner, |owner| {
                placing
                    .iter()
                    .all(|partition| committed(owner, *partition) == round)
            })
            .await;
        }
    })
    .await;

    let objects = uploader.store().keys().len();
    // The sends of a round arrive together. The first is stored alone when
    // the shard is idle by then, and the window of the others is one object.
    let windows = 2 * usize::try_from(ROUNDS).unwrap();
    assert!(
        (1..=windows).contains(&objects),
        "{objects} objects for {PARTITIONS} partitions in {ROUNDS} rounds"
    );
    let (stored, refused, bytes) = owner.metrics().fresh_uploads_value();
    assert_eq!((stored, refused), (objects as u64, 0));
    let scrape = owner.metrics.scrape();
    for series in [
        format!("fresh_object_size_bytes_count {objects}"),
        format!("fresh_object_size_bytes_sum {bytes}.0"),
    ] {
        assert!(
            scrape.lines().any(|line| line == series),
            "{series} is not in the scrape:\n{scrape}"
        );
    }
}

/// Holds every attempt at an upload until the test settles it, so the test
/// picks the order the uploads end in and which of them the store fails.
pub(super) struct GatedUploader {
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
    pub(super) fn new() -> Rc<Self> {
        Rc::new(Self {
            uploader: Rc::new(ExecutorFreshUploader::new(
                InlineTierExecutor::new(MemoryStore::new()),
                layout(),
            )),
            started: RefCell::default(),
        })
    }

    pub(super) fn started(&self) -> usize {
        self.started.borrow().len()
    }

    /// The object of the attempt that started as number `attempt`.
    fn object_id(&self, attempt: usize) -> u128 {
        self.started.borrow()[attempt].0.get()
    }

    /// End the attempt that started as number `attempt`: the store takes the
    /// object, or fails in a way a retry can get past.
    pub(super) fn settle(&self, attempt: usize, stores: bool) {
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
    // Objects of proof rows alone go out beside the windows.
    let stored = |count: u64| data_objects_stored(&owner) == count;

    pumped(&owner, async {
        assert_eq!(owner.fresh_uploads_deadline(), None);
        let first_sent = std::time::Instant::now();
        let _first = submit_send(&owner, 1, 100);
        until(&owner, |_| stored(1)).await;
        assert!(first_sent.elapsed() < INTERVAL / 2);

        let _second = submit_send(&owner, 2, 100);
        compio::time::sleep(INTERVAL / 4).await;
        assert_eq!(data_objects_stored(&owner), 1);
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

/// A send of a topic that sets a long upload interval waits that long after
/// the last upload started, not the shard's short interval. The pump reads
/// the interval from the topic's committed options.
#[compio::test]
async fn given_a_shard_that_leads_only_a_topic_with_a_long_upload_interval_when_sends_arrive_should_upload_at_that_interval()
 {
    const TOPIC_INTERVAL: Duration = Duration::from_secs(1);
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner_of_geo_topic(
        directory.path(),
        iggy_common::GeoTopicDurations {
            fresh_upload_interval: Some(iggy_common::IggyDuration::new(TOPIC_INTERVAL)),
            ..iggy_common::GeoTopicDurations::default()
        },
    ))
    .await;
    let uploader = TestUploader::new();
    owner.set_fresh_uploader(Some(uploader.clone()));
    owner.set_fresh_upload_pace(Duration::from_millis(10), 4);
    let stored = |count: u64| data_objects_stored(&owner) == count;

    pumped(&owner, async {
        let first_sent = std::time::Instant::now();
        let _first = submit_send(&owner, 1, 100);
        until(&owner, |_| stored(1)).await;
        assert!(first_sent.elapsed() < TOPIC_INTERVAL / 2);

        let _second = submit_send(&owner, 2, 100);
        compio::time::sleep(TOPIC_INTERVAL / 4).await;
        assert_eq!(
            data_objects_stored(&owner),
            1,
            "the shard's 10 ms do not apply"
        );
        until(&owner, |_| stored(2)).await;
        assert!(first_sent.elapsed() >= TOPIC_INTERVAL);
        until(&owner, |owner| committed_sends(owner) == 2).await;
    })
    .await;
}

/// A window holds the sends of every topic the shard leads, and it uploads
/// at the shortest interval among them: a send of a topic with a short
/// interval takes the waiting send of a topic with a long one along.
#[compio::test]
async fn given_a_window_with_a_send_of_a_long_interval_when_a_send_of_a_short_interval_joins_it_should_upload_at_the_short_interval()
 {
    const LONG: Duration = Duration::from_secs(2);
    const SHORT: Duration = Duration::from_millis(20);
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let short_partition = place_other_partition(&owner);
    let uploader = TestUploader::new();
    owner.set_fresh_uploader(Some(uploader.clone()));
    owner.set_fresh_upload_pace(Duration::from_millis(10), 4);
    let stored = |count: u64| data_objects_stored(&owner) == count;
    let offer = |partition: IggyNamespace, request: u64, interval: Duration| {
        let (reply, answer) = channel(1);
        owner.offer_fresh_send(
            sized_send_request(partition, request, 100),
            reply,
            Some(interval),
        );
        answer
    };

    pumped(&owner, async {
        let first_sent = std::time::Instant::now();
        let _first = offer(namespace(), 1, LONG);
        until(&owner, |_| stored(1)).await;

        let _long = offer(namespace(), 2, LONG);
        compio::time::sleep(LONG / 8).await;
        assert_eq!(data_objects_stored(&owner), 1, "the long interval holds it");
        let _short = offer(short_partition, 1, SHORT);
        until(&owner, |_| stored(2)).await;
        assert!(
            first_sent.elapsed() < LONG / 2,
            "the short interval took the long send along: {:?}",
            first_sent.elapsed()
        );
        until(&owner, |owner| {
            committed_sends(owner) == 2 && committed(owner, short_partition) == 1
        })
        .await;
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

/// Two sends of 6 MiB: the first to [`namespace`], and the second, while the
/// upload of the first waits, to the same partition or to another one. The
/// shard keeps `bounds` as its cap and share, or its defaults without them.
/// Returns how many of the two sends committed, and the denials of the share
/// and of the cap.
#[allow(clippy::future_not_send)]
async fn two_large_sends(
    bounds: Option<(u64, u64)>,
    to_another_partition: bool,
) -> (u64, (u64, u64)) {
    let send_bytes = 6 * 1024 * 1024;
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let second_partition = if to_another_partition {
        place_other_partition(&owner)
    } else {
        namespace()
    };
    let uploader = GatedUploader::new();
    owner.set_fresh_uploader(Some(uploader.clone()));
    owner.set_fresh_upload_pace(Duration::ZERO, 1);
    if let Some((shard_bytes, partition_bytes)) = bounds {
        owner.set_fresh_pending_bytes(shard_bytes, partition_bytes);
    }
    let committed_of_both = |owner: &IoTestShard| {
        if to_another_partition {
            committed(owner, namespace()) + committed(owner, second_partition)
        } else {
            committed(owner, namespace())
        }
    };

    pumped(&owner, async {
        let _first = submit_send_when_room(&owner, 1, send_bytes).await;
        until(&owner, |_| uploader.started() == 1).await;
        let _second = submit_send_to_when_room(&owner, second_partition, 2, send_bytes).await;
        until(&owner, |owner| {
            let (_, denied, _, staged) = owner.metrics().fresh_load_value();
            denied + u64::try_from(staged).unwrap() == 2
        })
        .await;
        let taken = u64::try_from(owner.metrics().fresh_load_value().3).unwrap();
        for attempt in 0..usize::try_from(taken).unwrap() {
            until(&owner, |_| uploader.started() == attempt + 1).await;
            uploader.settle(attempt, true);
        }
        until(&owner, |owner| committed_of_both(owner) == taken).await;
        (
            committed_of_both(&owner),
            owner.metrics().fresh_cap_denials_value(),
        )
    })
    .await
}

/// Sends of several MiB to one partition pass its default share of 8 MiB.
/// A share raised above both takes them, and the default turns the second
/// away.
#[compio::test]
async fn given_a_raised_partition_share_when_two_sends_to_one_partition_pass_the_default_should_take_both()
 {
    let raised = (
        4 * FRESH_PENDING_BYTES_MAX,
        2 * FRESH_PARTITION_PENDING_BYTES_MAX,
    );

    assert_eq!(
        Box::pin(two_large_sends(None, false)).await,
        (1, (1, 0)),
        "the default share"
    );
    assert_eq!(
        Box::pin(two_large_sends(Some(raised), false)).await,
        (2, (0, 0)),
        "a share of 16 MiB"
    );
}

/// The cap of the shard holds at the value it was given: a send to a
/// partition that has nothing waiting is turned away once the waiting sends
/// of the shard would pass it, where the default takes it.
#[compio::test]
async fn given_a_lowered_shard_cap_when_sends_to_two_partitions_pass_it_should_answer_the_second() {
    let lowered = (10 * 1024 * 1024, FRESH_PARTITION_PENDING_BYTES_MAX);

    assert_eq!(
        Box::pin(two_large_sends(None, true)).await,
        (2, (0, 0)),
        "the default cap"
    );
    assert_eq!(
        Box::pin(two_large_sends(Some(lowered), true)).await,
        (1, (0, 1)),
        "a cap of 10 MiB"
    );
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
        owner.offer_fresh_send(
            sized_send_request(namespace(), request, payload_len),
            reply,
            None,
        );
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
        floors: partitions::ProofFloors::default(),
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
        until(&owner, |owner| {
            owner.metrics().fresh_sweeps_held_value() == 1
        })
        .await;

        // Once it can tell, the grace starts from that sweep.
        let able = past_grace(past_grace(now));
        let sweep = sweep_asking_the_second_shard(&owner, &asked, able).await;
        answer_as_second_shard(&owner, sweep, Some(&[]));
        until(&owner, |owner| owner.fresh_sweeps_settled() == 1).await;
        compio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(fresh_objects(store), vec![object]);
        assert_eq!(owner.metrics().fresh_sweeps_held_value(), 0);
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
        floors: Rc<partitions::ProofFloors>,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, FreshDeleteOutcome> {
        self.deletes.set(self.deletes.get() + 1);
        if self.refuses.get() {
            return Box::pin(std::future::ready(Ok(partitions::FreshDeleted {
                failed: objects,
                kept: Vec::new(),
            })));
        }
        let keeps = std::mem::take(&mut *self.keeps.borrow_mut());
        let (kept, sent): (Vec<u128>, Vec<u128>) =
            objects.into_iter().partition(|id| keeps.contains(id));
        let delete = self.sweeper.delete(sent, floors, scope);
        let held = self.held_delete.borrow_mut().take();
        Box::pin(async move {
            if let Some(held) = held {
                let _ = held.await;
            }
            let mut deleted = delete.await?;
            deleted.failed.extend(kept);
            Ok(deleted)
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

/// Ask for a sweep at `now` that a shard which cannot tell ends, and give it
/// the time a sweep takes to settle and delete.
#[allow(clippy::future_not_send)]
async fn sweep_that_cannot_settle(owner: &IoTestShard, now: IggyTimestamp) {
    owner.request_sweep_fresh_objects(now);
    compio::time::sleep(Duration::from_millis(200)).await;
}

/// Commit one send to the partition and run the sweep that first finds its
/// object named. Returns the object.
#[allow(clippy::future_not_send)]
async fn commit_a_named_object(
    owner: &IoTestShard,
    store: &MemoryStore,
    now: IggyTimestamp,
) -> u128 {
    let _send = submit_send(owner, 1, 100);
    until(owner, |owner| committed_sends(owner) == 1).await;
    let objects = fresh_objects(store);
    assert_eq!(objects.len(), 1);
    sweep(owner, now).await;
    assert_eq!(owner.fresh_sweeps_settled(), 1);
    objects[0]
}

/// A partition fenced for a rebuild keeps its slot behind the tombstone
/// until its files are quarantined. Its group still holds the placed batches
/// that name this replica's objects, and the copy that comes back reads them
/// again, so the shard cannot tell and the sweep deletes nothing.
#[compio::test]
async fn given_a_placing_partition_fenced_for_a_rebuild_when_sweeps_run_past_the_grace_should_keep_the_objects_it_named()
 {
    let directory = tempfile::tempdir().unwrap();
    let (owner, executor) = Box::pin(swept_owner(directory.path())).await;
    let store = executor.0.store();
    let now = IggyTimestamp::now();

    let object = pumped(&owner, async {
        let object = commit_a_named_object(&owner, store, now).await;

        owner.plane.partitions().tombstone(namespace());
        sweep_that_cannot_settle(&owner, past_grace(now)).await;
        sweep_that_cannot_settle(&owner, past_grace(past_grace(now))).await;
        until(&owner, |owner| {
            owner.metrics().fresh_sweeps_held_value() == 1
        })
        .await;
        let kept = fresh_objects(store);
        // The pump does not stop while it holds a partition tombstoned
        // without a teardown.
        drop_fenced_partition(&owner, namespace()).await;
        assert_eq!(kept, vec![object]);
        object
    })
    .await;

    assert_eq!(fresh_objects(store), vec![object]);
    assert_eq!(owner.metrics().fresh_objects_deleted_value(), 0);
    assert_eq!(owner.fresh_sweeps_settled(), 1);
}

/// The rebuild drops the fenced partition before the reconciler builds the
/// next copy, and a build that fails leaves the namespace without one for
/// as long as it keeps failing. The committed topic still places its
/// batches there, so the shard cannot tell until a copy is back.
#[compio::test]
async fn given_a_committed_placing_partition_this_shard_does_not_hold_when_sweeps_run_past_the_grace_should_keep_the_objects_it_named()
 {
    let directory = tempfile::tempdir().unwrap();
    let (owner, executor) = Box::pin(swept_owner_of_placing_topic(directory.path())).await;
    let store = executor.0.store();
    let now = IggyTimestamp::now();

    let object = pumped(&owner, async {
        let object = commit_a_named_object(&owner, store, now).await;

        drop_for_a_rebuild(&owner).await;
        sweep_that_cannot_settle(&owner, past_grace(now)).await;
        sweep_that_cannot_settle(&owner, past_grace(past_grace(now))).await;
        object
    })
    .await;

    assert_eq!(fresh_objects(store), vec![object]);
    assert_eq!(owner.metrics().fresh_objects_deleted_value(), 0);
    assert_eq!(owner.fresh_sweeps_settled(), 1);
}

/// The copy that a rebuild builds holds no placed batch until a state
/// transfer installs those of its group, and its status is normal while it
/// waits.
#[compio::test]
async fn given_a_rebuilt_placing_partition_awaiting_a_state_transfer_when_sweeps_run_past_the_grace_should_keep_the_objects_its_group_named()
 {
    let directory = tempfile::tempdir().unwrap();
    let (owner, executor) = Box::pin(swept_owner_of_placing_topic(directory.path())).await;
    let store = executor.0.store();
    let now = IggyTimestamp::now();

    let object = pumped(&owner, async {
        let object = commit_a_named_object(&owner, store, now).await;

        drop_for_a_rebuild(&owner).await;
        insert_copy_awaiting_a_state_transfer(&owner).await;
        let partitions = owner.plane.partitions();
        let consensus = partitions.get_by_ns(&namespace()).unwrap().consensus();
        assert!(consensus.is_normal() && consensus.is_transferring());
        sweep_that_cannot_settle(&owner, past_grace(now)).await;
        sweep_that_cannot_settle(&owner, past_grace(past_grace(now))).await;
        object
    })
    .await;

    assert_eq!(fresh_objects(store), vec![object]);
    assert_eq!(owner.metrics().fresh_objects_deleted_value(), 0);
    assert_eq!(owner.fresh_sweeps_settled(), 1);
}

/// The sweeps that a rebuilt copy holds back show in a gauge, which the
/// first sweep that settles once the copy is back in its group clears.
#[compio::test]
async fn given_sweeps_held_by_a_rebuilt_placing_partition_when_its_state_transfer_ends_should_report_them_held_until_one_settles()
 {
    let directory = tempfile::tempdir().unwrap();
    let (owner, executor) = Box::pin(swept_owner_of_placing_topic(directory.path())).await;
    let store = executor.0.store();
    let now = IggyTimestamp::now();
    let held = |owner: &IoTestShard| owner.metrics().fresh_sweeps_held_value();

    pumped(&owner, async {
        commit_a_named_object(&owner, store, now).await;
        assert_eq!(held(&owner), 0);

        drop_for_a_rebuild(&owner).await;
        insert_copy_awaiting_a_state_transfer(&owner).await;
        sweep_that_cannot_settle(&owner, past_grace(now)).await;
        until(&owner, |owner| held(owner) == 1).await;
        sweep_that_cannot_settle(&owner, past_grace(past_grace(now))).await;
        assert_eq!(held(&owner), 1);
        assert_eq!(owner.fresh_sweeps_settled(), 1);

        let partitions = owner.plane.partitions();
        let consensus = partitions.get_by_ns(&namespace()).unwrap().consensus();
        consensus.set_state_transfer_stage(consensus::StateTransferStage::Idle);
        sweep(&owner, past_grace(past_grace(now))).await;
        assert_eq!(owner.fresh_sweeps_settled(), 2);
        assert_eq!(held(&owner), 0);
    })
    .await;
}

/// Fence the partition for a rebuild, as a replica does when its copy
/// diverged from the view, and wait for the pump to drop the fenced copy.
#[allow(clippy::future_not_send)]
async fn drop_for_a_rebuild(owner: &IoTestShard) {
    let partitions = owner.plane.partitions();
    let partition = partitions.get_mut_by_ns(&namespace()).unwrap();
    owner.fence_partition_for_rebuild(namespace(), partition, partition.offset_frontier());
    until(owner, |owner| {
        let partitions = owner.plane.partitions();
        !partitions.contains(&namespace()) && !partitions.is_tombstoned(&namespace())
    })
    .await;
}

/// Hand the pump the next copy of the partition, as the reconciler does
/// after a rebuild: it places its batches, holds none, and waits for a state
/// transfer.
#[allow(clippy::future_not_send)]
async fn insert_copy_awaiting_a_state_transfer(owner: &IoTestShard) {
    use consensus::MetadataHandle;
    use metadata::impls::metadata::StreamsFrontend;

    let consensus = VsrConsensus::new(
        1,
        0,
        1,
        namespace().inner(),
        Rc::clone(&owner.bus),
        LocalPipeline::new(),
    );
    consensus.init();
    let mut copy = IggyPartition::with_in_memory_storage(
        Arc::new(PartitionStats::default()),
        consensus,
        IggyByteSize::from(SEGMENT_BYTES),
    );
    copy.set_runtime_options(TopicRuntimeOptions {
        places_batches: true,
        ..Default::default()
    });
    copy.register_builtin_state_machines().unwrap();
    copy.consensus().begin_state_transfer_await();
    let epoch = owner
        .plane
        .metadata()
        .mux_stm
        .streams()
        .created_revision_for_namespace(namespace())
        .unwrap();
    owner.enqueue_reconcile_op(ReconcileOp::InsertOwned {
        namespace: namespace(),
        partition: Box::new(copy),
        epoch,
    });
    until(owner, |owner| {
        owner.plane.partitions().contains(&namespace())
    })
    .await;
}

/// A delete commits the removal of the partition before the reconciler
/// fences its copy. No committed partition is left to read the placed
/// batches of that copy, so it holds back no sweep, fenced or not.
#[compio::test]
async fn given_a_fenced_placing_partition_that_committed_metadata_dropped_when_sweeps_run_past_the_grace_should_delete_the_objects_it_named()
 {
    let directory = tempfile::tempdir().unwrap();
    let (owner, executor) = Box::pin(swept_owner_of_placing_topic(directory.path())).await;
    let store = executor.0.store();
    let now = IggyTimestamp::now();
    let unnamed_since = past_grace(now);

    pumped(&owner, async {
        commit_a_named_object(&owner, store, now).await;

        delete_committed_topic(&owner);
        owner.plane.partitions().tombstone(namespace());
        sweep(&owner, unnamed_since).await;
        assert_eq!(fresh_objects(store).len(), 1);
        sweep(&owner, past_grace(unnamed_since)).await;
        let left = fresh_objects(store);
        drop_fenced_partition(&owner, namespace()).await;
        assert_eq!(left, Vec::<u128>::new());
    })
    .await;

    assert_eq!(fresh_objects(store), Vec::<u128>::new());
    assert_eq!(owner.metrics().fresh_objects_deleted_value(), 1);
}

/// Drop the fenced `partition` from the shard, as the pump does once the
/// files of the copy are gone or quarantined.
#[allow(clippy::future_not_send)]
async fn drop_fenced_partition(owner: &IoTestShard, partition: IggyNamespace) {
    owner.enqueue_reconcile_op(ReconcileOp::ConfirmRemove {
        namespace: partition,
    });
    until(owner, |owner| {
        !owner.plane.partitions().contains(&partition)
    })
    .await;
}

/// Commit the delete of the fixture's topic in the shard's metadata, with
/// the fence every partition of it installs before the topic goes.
fn delete_committed_topic(owner: &IoTestShard) {
    use consensus::MetadataHandle;
    use iggy_binary_protocol::WireIdentifier;
    use iggy_binary_protocol::requests::topics::DeleteTopicRequest;
    use metadata::impls::metadata::StreamsFrontend;
    use metadata::stm::StateMachine;
    use metadata::stm::lifecycle::{CompleteLifecycleRequest, LifecycleCompletion};

    let body = DeleteTopicRequest {
        stream_id: WireIdentifier::numeric(0),
        topic_id: WireIdentifier::numeric(0),
    }
    .to_bytes();
    let message = metadata_prepare(100, Operation::DeleteTopic, &body);
    let metadata = &owner.plane.metadata().mux_stm;
    assert_eq!(metadata.update(message).unwrap().code, 0);
    for intent in metadata.streams().pending_lifecycles() {
        for target in intent.partitions {
            let completion = CompleteLifecycleRequest::new(
                intent.context.metadata_op,
                intent.stream_id,
                vec![LifecycleCompletion {
                    topic_id: target.topic_id,
                    partition_id: target.partition_id,
                    partition_op: 1,
                }],
            )
            .unwrap();
            metadata
                .update(metadata_prepare(
                    intent.context.metadata_op,
                    Operation::CompleteLifecycle,
                    &completion.to_bytes(),
                ))
                .unwrap();
        }
    }
    assert_eq!(
        owner
            .plane
            .metadata()
            .mux_stm
            .streams()
            .created_revision_for_namespace(namespace()),
        None
    );
}

/// The prepare of the metadata op `op` that carries `body`.
fn metadata_prepare(op: u64, operation: Operation, body: &[u8]) -> Message<PrepareHeader> {
    let size = size_of::<PrepareHeader>() + body.len();
    let mut message = Message::<PrepareHeader>::new(size);
    let header = PrepareHeader {
        command: Command::Prepare,
        op,
        operation,
        size: u32::try_from(size).unwrap(),
        ..Default::default()
    };
    message.as_mut_slice()[size_of::<PrepareHeader>()..].copy_from_slice(body);
    message.transmute_header::<PrepareHeader>(|_, target| *target = header)
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

/// Merges in a memory store once the test lets them, and counts them.
struct GatedMerger {
    merger: partitions::ExecutorLongTermMerger<super::object_deletes::Shared>,
    /// Whether a merge that starts waits for [`Self::open`].
    closed: Cell<bool>,
    gate: RefCell<Vec<futures::channel::oneshot::Sender<()>>>,
    started: Cell<u32>,
    /// The class of every merge that started, in order.
    classes: RefCell<Vec<iggy_object_store::TierJobClass>>,
}

impl GatedMerger {
    fn new(executor: super::object_deletes::Shared) -> Rc<Self> {
        Rc::new(Self {
            merger: partitions::ExecutorLongTermMerger::new(executor, layout()),
            closed: Cell::new(false),
            gate: RefCell::default(),
            started: Cell::new(0),
            classes: RefCell::default(),
        })
    }

    fn open(&self) {
        self.closed.set(false);
        for waiting in self.gate.borrow_mut().drain(..) {
            let _ = waiting.send(());
        }
    }
}

impl partitions::LongTermMerger for GatedMerger {
    fn merge(
        &self,
        merge: partitions::SegmentMerge,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, partitions::MergeOutcome> {
        self.started.set(self.started.get() + 1);
        self.classes.borrow_mut().push(merge.class);
        let merging = self.merger.merge(merge, scope);
        if !self.closed.get() {
            return merging;
        }
        let (open, opened) = futures::channel::oneshot::channel();
        self.gate.borrow_mut().push(open);
        Box::pin(async move {
            let _ = opened.await;
            merging.await
        })
    }
}

/// An owner that stores, rewrites and merges in one memory store, and the
/// merger it merges with.
#[allow(clippy::future_not_send)]
async fn merging_owner(directory: &Path) -> (IoTestShard, Rc<GatedMerger>) {
    use super::object_deletes::Shared;
    use partitions::{ExecutorLongTermWriter, ExecutorObjects, RemoteSegments};

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
    owner
        .plane
        .partitions()
        .set_remote_segments(Rc::new(RemoteSegments::new(
            Rc::new(ExecutorObjects::new(executor.clone())),
            layout(),
        )));
    let merger = GatedMerger::new(executor);
    owner.set_long_term_merger(Some(merger.clone()));
    (owner, merger)
}

/// Commit the send numbered `request` and wait until a long-term segment
/// of its own holds it.
#[allow(clippy::future_not_send)]
async fn send_and_rewrite(owner: &IoTestShard, request: u64) {
    use crate::rewrite_pacing::RewriteResult;
    let revision = super::segment_uploads::staged_revision(owner);
    let _answer = submit_send(owner, request, 100);
    until(owner, |owner| committed_sends(owner) == request).await;
    if owner
        .rewrite_pacing
        .borrow()
        .paces(&(namespace(), revision))
    {
        // The wait is longer than a test may take, so the pass is made due
        // after the shortest one.
        owner.rewrite_pacing.borrow_mut().settle(
            (namespace(), revision),
            RewriteResult::Resent,
            owner.bus.monotonic_micros(),
        );
    } else {
        owner.request_rewrite_segments(namespace(), revision);
    }
    until(owner, |owner| {
        long_term_ends(owner).last() == Some(&(request - 1))
    })
    .await;
}

/// The last offset of every long-term segment of the fixture's partition.
fn long_term_ends(owner: &IoTestShard) -> Vec<u64> {
    owner
        .plane
        .partitions()
        .get_by_ns(&namespace())
        .unwrap()
        .long_term_segments()
        .iter()
        .map(|segment| segment.end_offset)
        .collect()
}

fn scan_for_merges_now(owner: &IoTestShard) {
    owner
        .long_term_merges
        .schedule
        .borrow_mut()
        .scan_at(owner.bus.monotonic_micros());
}

/// The whole merge on one shard: the scan finds the two small segments,
/// the merge stores one in their place, and the replace it commits leaves
/// the long-term log with that one. Nothing is merged before a scan is due.
#[compio::test]
async fn given_small_long_term_segments_when_the_merge_scan_is_due_should_replace_them_with_one() {
    use iggy_object_store::{LongTermKey, SegmentObject};

    let directory = tempfile::tempdir().unwrap();
    let (owner, merger) = Box::pin(merging_owner(directory.path())).await;
    let revision = super::segment_uploads::staged_revision(&owner);

    pumped(&owner, async {
        send_and_rewrite(&owner, 1).await;
        send_and_rewrite(&owner, 2).await;
        assert_eq!(long_term_ends(&owner), [0, 1]);
        compio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(merger.started.get(), 0, "the scan of this interval is over");
        assert_eq!(long_term_ends(&owner), [0, 1]);

        scan_for_merges_now(&owner);

        until(&owner, |owner| long_term_ends(owner) == [1]).await;
        until(&owner, |owner| {
            owner.metrics().long_term_merges_value() == (1, 2)
        })
        .await;
        until(&owner, |owner| {
            owner
                .long_term_merges
                .schedule
                .borrow()
                .due_pass(u64::MAX)
                .is_none()
        })
        .await;
    })
    .await;

    assert_eq!(merger.started.get(), 1);
    let partition = owner.plane.partitions().get_by_ns(&namespace()).unwrap();
    let merged = partition.long_term_segments()[0];
    assert_eq!((merged.start_offset, merged.end_offset), (0, 1));
    assert!(!partition.segment_merge_running());
    let log_key = layout().long_term_key(&LongTermKey {
        prefix: partitions::object_prefix(namespace(), revision),
        start_offset: 0,
        end_offset: 1,
        object: SegmentObject::Log,
    });
    let store = merger.merger.executor().0.store();
    assert_eq!(store.head(&log_key).await.unwrap().size, merged.size);
}

/// A merge that takes long holds back no rewrite: the sends that commit
/// while it runs get their long-term segment. It does hold back the shard's
/// next merge, since a shard runs one at a time.
#[compio::test]
async fn given_a_merge_in_flight_when_sends_commit_should_rewrite_them_and_start_no_other_merge() {
    let directory = tempfile::tempdir().unwrap();
    let (owner, merger) = Box::pin(merging_owner(directory.path())).await;

    pumped(&owner, async {
        send_and_rewrite(&owner, 1).await;
        send_and_rewrite(&owner, 2).await;
        merger.closed.set(true);
        scan_for_merges_now(&owner);
        until(&owner, |_| merger.started.get() == 1).await;

        send_and_rewrite(&owner, 3).await;

        assert_eq!(long_term_ends(&owner), [0, 1, 2]);
        scan_for_merges_now(&owner);
        compio::time::sleep(Duration::from_millis(600)).await;
        assert_eq!(merger.started.get(), 1, "one merge at a time");
        assert_eq!(long_term_ends(&owner), [0, 1, 2]);

        merger.open();
        until(&owner, |owner| long_term_ends(owner) == [1, 2]).await;
        until(&owner, |owner| {
            owner.metrics().long_term_merges_value() == (1, 2)
        })
        .await;
    })
    .await;

    // The merged segment came out small, so only the next scan merges it
    // with the segment that follows.
    assert_eq!(merger.started.get(), 1);
}

/// A shard that lost its merger scans nothing, and a shard that gets one
/// scans at once.
#[compio::test]
async fn given_a_shard_without_a_merger_when_it_gets_one_should_scan_then() {
    let directory = tempfile::tempdir().unwrap();
    let (owner, merger) = Box::pin(merging_owner(directory.path())).await;
    owner.set_long_term_merger(None);

    pumped(&owner, async {
        send_and_rewrite(&owner, 1).await;
        send_and_rewrite(&owner, 2).await;
        compio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(long_term_ends(&owner), [0, 1]);

        owner.set_long_term_merger(Some(merger.clone()));

        until(&owner, |owner| long_term_ends(owner) == [1]).await;
    })
    .await;
}

/// Retention of a topic that places its batches, on the shard that leads
/// its partition: the pass sends the trim its policy asks for, the trim
/// commits, the manifest in the store starts at the trim point, and the
/// next pass removes the segment of placed batches below it. A pass staged
/// for another incarnation does nothing.
#[compio::test]
async fn given_expired_long_term_segments_when_retention_passes_run_should_commit_the_trim_and_remove_the_local_segment_below_it()
 {
    use super::object_deletes::Shared;
    use crate::rewrite_pacing::RewriteResult;
    use iggy_common::{IggyDuration, IggyExpiry};
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
    // Two placed batches fill a segment, so the third send opens the next.
    partitions
        .get_mut_by_ns(&namespace())
        .unwrap()
        .log
        .active_segment_mut()
        .max_size = IggyByteSize::from(2 * PLACED_BATCH_SIZE as u64);
    let revision = super::segment_uploads::staged_revision(&owner);
    let key = (namespace(), revision);
    let manifest_key = layout().manifest_key(&partitions::object_prefix(namespace(), revision));
    let store = executor.0.store();
    let manifest_with = async |segments: usize| {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok(meta) = store.head(&manifest_key).await {
                let object = store.get_range(&manifest_key, 0..meta.size).await.unwrap();
                let manifest = PartitionManifest::decode(&object.bytes).unwrap();
                if manifest.segments.len() == segments {
                    return manifest;
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "no manifest of {segments} segments was stored"
            );
            compio::time::sleep(TASK_POLL_INTERVAL).await;
        }
    };
    let local_segments = |owner: &IoTestShard| {
        let partition = owner.plane.partitions().get_by_ns(&namespace()).unwrap();
        partition
            .log
            .segments()
            .iter()
            .map(|segment| segment.start_offset)
            .collect::<Vec<_>>()
    };
    let trim_point = |owner: &IoTestShard| {
        let partition = owner.plane.partitions().get_by_ns(&namespace()).unwrap();
        partition.long_term_trim_point()
    };
    let first_log = directory.path().join("00000000000000000000.log");
    let much_later = IggyTimestamp::from(
        IggyTimestamp::now().as_micros() + IggyDuration::from(Duration::from_hours(1)).as_micros(),
    );
    let one_second = IggyExpiry::ExpireDuration(IggyDuration::from(Duration::from_secs(1)));

    pumped(&owner, async {
        let _answers = [submit_send(&owner, 1, 100), submit_send(&owner, 2, 100)];
        until(&owner, |owner| local_segments(owner) == [0, 2]).await;
        owner.request_rewrite_segments(namespace(), revision);
        until(&owner, |owner| long_term_ends(owner) == [1]).await;
        // The wait is longer than this test may take, so the next pass is
        // made due after the shortest one.
        owner.rewrite_pacing.borrow_mut().settle(
            key,
            RewriteResult::Resent,
            owner.bus.monotonic_micros(),
        );
        let _third = submit_send(&owner, 3, 100);
        until(&owner, |owner| long_term_ends(owner) == [1, 2]).await;
        manifest_with(2).await;

        owner.request_retain_long_term(namespace(), revision + 1, much_later, one_second, None);
        for _ in 0..20 {
            compio::time::sleep(TASK_POLL_INTERVAL).await;
        }
        assert_eq!(
            trim_point(&owner),
            None,
            "the policy of another incarnation"
        );

        assert_eq!(owner.metrics().long_term_trims_value(), 0);

        owner.request_retain_long_term(namespace(), revision, much_later, one_second, None);
        let trimmed = manifest_with(1).await;
        assert_eq!(owner.metrics().long_term_trims_value(), 1);
        assert_eq!((trimmed.floor, trimmed.trim_point), (Some(2), Some(1)));
        assert_eq!(long_term_ends(&owner), [2]);
        assert_eq!(local_segments(&owner), [0, 2]);
        assert!(first_log.exists());

        owner.request_retain_long_term(namespace(), revision, much_later, one_second, None);
        until(&owner, |owner| local_segments(owner) == [2]).await;
        until(&owner, |_| !first_log.exists()).await;
        assert_eq!(
            owner.metrics().long_term_trims_value(),
            1,
            "a pass that decides on no trim sends none"
        );
    })
    .await;
}

/// Commit segments too large to merge after the ones the long-term log of
/// the fixture's partition holds, until it holds `segments`. The store
/// holds the object of none of them.
#[allow(clippy::future_not_send)]
async fn publish_healthy_segments(owner: &IoTestShard, segments: usize) {
    use iggy_object_store::{ManifestSegment, SEGMENT_HASH_LEN};

    let held = |owner: &IoTestShard| {
        let partition = owner.plane.partitions().get_by_ns(&namespace()).unwrap();
        partition.long_term_segments().len()
    };
    while held(owner) < segments {
        let partition = owner.plane.partitions().get_by_ns(&namespace()).unwrap();
        let next = partition.long_term_floor().map_or(0, |floor| floor + 1);
        let mut sent = 0;
        for offset in (next..).take(segments - held(owner)) {
            let segment = ManifestSegment {
                start_offset: offset,
                end_offset: offset,
                start_timestamp: 0,
                end_timestamp: 0,
                max_timestamp: 0,
                size: partitions::LONG_TERM_MERGE_MIN_BYTES,
                crc32c: 0,
                hash: [0; SEGMENT_HASH_LEN],
            };
            let request = partition
                .publish_segment_request(&segment, 1_000_000 + offset)
                .unwrap();
            let frame = ShardFrame::lifecycle(LifecycleFrame::PartitionSubmit {
                request,
                reply: channel(1).0,
                attachment: None,
            });
            // The inbox is full: the pump takes what it holds first.
            if owner.senders[0].try_send(frame).is_err() {
                break;
            }
            sent += 1;
        }
        let after = held(owner) + sent;
        until(owner, |owner| held(owner) == after).await;
    }
}

/// The scan of the interval is over, and the long-term log grows to the
/// threshold. The commit that takes it there starts the merge, in the class
/// that does not wait for an idle shard, and no commit below it does.
#[compio::test]
async fn given_a_long_term_log_that_reaches_the_pressure_threshold_when_the_segment_commits_should_merge_without_a_scan()
 {
    use iggy_object_store::TierJobClass;
    use partitions::LONG_TERM_SEGMENTS_PRESSURE;

    let directory = tempfile::tempdir().unwrap();
    let (owner, merger) = Box::pin(merging_owner(directory.path())).await;
    let below = i64::try_from(LONG_TERM_SEGMENTS_PRESSURE - 1).unwrap();

    pumped(&owner, async {
        send_and_rewrite(&owner, 1).await;
        send_and_rewrite(&owner, 2).await;
        publish_healthy_segments(&owner, LONG_TERM_SEGMENTS_PRESSURE - 1).await;
        compio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(merger.started.get(), 0, "the log is below the threshold");
        until(&owner, |owner| {
            owner.metrics().long_term_segments_value() == below
        })
        .await;

        publish_healthy_segments(&owner, LONG_TERM_SEGMENTS_PRESSURE).await;

        until(&owner, |owner| {
            owner.metrics().long_term_merges_value() == (1, 2)
        })
        .await;
        until(&owner, |owner| {
            owner.metrics().long_term_segments_value() == below
        })
        .await;
    })
    .await;

    assert_eq!(*merger.classes.borrow(), [TierJobClass::Upload]);
    assert_eq!(long_term_ends(&owner)[..2], [1, 2]);
}

/// A merge leaves the log one segment shorter and two objects dead. The
/// shard reports the length of its longest log, the objects that wait for
/// their delete, and the ones a delete removed.
#[compio::test]
async fn given_a_merge_that_commits_when_its_sources_are_deleted_should_report_the_log_the_dead_objects_and_the_deletes()
 {
    use iggy_common::IggyDuration;
    use iggy_object_store::{PartitionManifest, SegmentObject};
    use partitions::{ExecutorDeleter, ExecutorManifestPublisher};

    let directory = tempfile::tempdir().unwrap();
    let (owner, merger) = Box::pin(merging_owner(directory.path())).await;
    let executor = merger.merger.executor().clone();
    owner.set_manifest_publisher(Some(Rc::new(ExecutorManifestPublisher::new(
        executor.clone(),
        layout(),
    ))));
    owner.set_object_deleter(Some(Rc::new(ExecutorDeleter::new(
        executor.clone(),
        layout(),
    ))));
    let revision = super::segment_uploads::staged_revision(&owner);
    let manifest_key = layout().manifest_key(&partitions::object_prefix(namespace(), revision));
    let store = executor.0.store();
    // The floor and the segments of the stored manifest. The one of the
    // first rewrite lists one segment too, at a lower floor.
    let stored_manifest = async || {
        let meta = store.head(&manifest_key).await.ok()?;
        let object = store.get_range(&manifest_key, 0..meta.size).await.ok()?;
        let manifest = PartitionManifest::decode(&object.bytes).ok()?;
        Some((manifest.floor, manifest.segments.len()))
    };
    let metrics = owner.metrics();

    pumped(&owner, async {
        send_and_rewrite(&owner, 1).await;
        send_and_rewrite(&owner, 2).await;
        until(&owner, |_| metrics.long_term_segments_value() == 2).await;
        assert_eq!(metrics.long_term_dead_objects_value(), 0);

        scan_for_merges_now(&owner);

        until(&owner, |_| metrics.long_term_segments_value() == 1).await;
        until(&owner, |_| metrics.long_term_dead_objects_value() == 2).await;
        assert_eq!(metrics.long_term_deleted_objects_value(), 0);

        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while stored_manifest().await != Some((Some(1), 1)) {
            assert!(
                std::time::Instant::now() < deadline,
                "no manifest of the merged segment was stored"
            );
            compio::time::sleep(TASK_POLL_INTERVAL).await;
        }
        // The objects wait from the moment the partition learns of the
        // manifest, which is after the store holds it.
        compio::time::sleep(Duration::from_millis(100)).await;
        // The delay is over at this time, and the listing the first pass
        // makes is not yet due again.
        let after_the_delay = IggyTimestamp::from(
            IggyTimestamp::now().as_micros()
                + IggyDuration::from(Duration::from_hours(1)).as_micros(),
        );
        while metrics.long_term_deleted_objects_value() < 2 {
            assert!(
                std::time::Instant::now() < deadline,
                "the dead objects were not deleted"
            );
            owner.request_delete_objects(namespace(), revision, after_the_delay, 0);
            compio::time::sleep(TASK_POLL_INTERVAL).await;
        }
        until(&owner, |_| metrics.long_term_dead_objects_value() == 0).await;
    })
    .await;

    assert_eq!(metrics.long_term_deleted_objects_value(), 2);
    assert_eq!(
        store
            .keys()
            .iter()
            .filter(|key| {
                layout()
                    .parse_long_term_key(key)
                    .is_some_and(|key| key.object == SegmentObject::Log)
            })
            .count(),
        1,
        "the merged segment is the one long-term log left"
    );
}

/// What one manifest that a [`TimedPublisher`] stored lists: when it was
/// built, in microseconds since the Unix epoch, its floor, and how many
/// segments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StoredManifest {
    published_at: u64,
    floor: Option<u64>,
    segments: usize,
}

/// Publishes into a recording memory store, and notes every manifest it
/// stores. A manifest carries the time its pass started, so the notes say
/// when each put started on the clock that the interval between them
/// keeps to.
struct TimedPublisher {
    publisher: ExecutorManifestPublisher<InlineTierExecutor<RecordingStore<MemoryStore>>>,
    stored: RefCell<Vec<StoredManifest>>,
}

impl TimedPublisher {
    /// A publisher, and the calls that its store gets.
    fn new() -> (Rc<Self>, StoreCalls) {
        let store = RecordingStore::new(MemoryStore::new());
        let calls = store.calls();
        let publisher = Rc::new(Self {
            publisher: ExecutorManifestPublisher::new(InlineTierExecutor::new(store), layout()),
            stored: RefCell::default(),
        });
        (publisher, calls)
    }

    fn stored(&self) -> Vec<StoredManifest> {
        self.stored.borrow().clone()
    }

    /// The segments of the last manifest it stored.
    fn last_segments(&self) -> Option<usize> {
        self.stored.borrow().last().map(|stored| stored.segments)
    }
}

impl ManifestPublisher for TimedPublisher {
    fn publish(
        &self,
        manifest: PartitionManifest,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, ManifestPublishOutcome> {
        self.stored.borrow_mut().push(StoredManifest {
            published_at: manifest.published_at,
            floor: manifest.floor,
            segments: manifest.segments.len(),
        });
        self.publisher.publish(manifest, scope)
    }
}

/// [`merging_owner`] without a merger, whose manifests go to a
/// [`TimedPublisher`]. Returns the calls of its store and the key of the
/// fixture's manifest too.
#[allow(clippy::future_not_send)]
async fn timed_manifest_owner(
    directory: &Path,
) -> (IoTestShard, Rc<TimedPublisher>, StoreCalls, String) {
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
    owner
        .plane
        .partitions()
        .set_remote_segments(Rc::new(RemoteSegments::new(
            Rc::new(ExecutorObjects::new(executor)),
            layout(),
        )));
    let (publisher, calls) = TimedPublisher::new();
    owner.set_manifest_publisher(Some(publisher.clone()));
    let manifest_key = layout().manifest_key(&partitions::object_prefix(
        namespace(),
        staged_revision(&owner),
    ));
    (owner, publisher, calls, manifest_key)
}

/// The puts of `key` that `calls` holds.
fn puts_of(calls: &StoreCalls, key: &str) -> usize {
    calls
        .all()
        .iter()
        .filter(|call| matches!(call, StoreCall::Put { key: put, .. } if put == key))
        .count()
}

fn micros_of(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap()
}

/// A change of the long-term log right after a store waits out the store
/// interval. Nothing else commits and the shard paces no rewrite of the
/// partition, so the pump's tick is what stores it.
#[compio::test]
async fn given_a_long_term_change_right_after_a_manifest_store_when_nothing_else_happens_should_store_it_once_the_interval_passed()
 {
    let directory = tempfile::tempdir().unwrap();
    let (owner, publisher, calls, manifest_key) =
        Box::pin(timed_manifest_owner(directory.path())).await;
    let revision = staged_revision(&owner);

    pumped(&owner, async {
        send_and_rewrite(&owner, 1).await;
        until(&owner, |_| publisher.last_segments() == Some(1)).await;
        owner
            .rewrite_pacing
            .borrow_mut()
            .forget(&(namespace(), revision));

        publish_healthy_segments(&owner, 2).await;

        until(&owner, |_| publisher.last_segments() == Some(2)).await;
    })
    .await;

    let stored = publisher.stored();
    let [.., before, last] = stored.as_slice() else {
        panic!("a manifest before the change and one after it: {stored:?}");
    };
    let waited = last.published_at - before.published_at;
    assert!(
        waited >= micros_of(MANIFEST_STORE_INTERVAL)
            && waited < micros_of(MANIFEST_STORE_INTERVAL + Duration::from_millis(500)),
        "stored {waited} us after the one before"
    );
    assert_eq!(puts_of(&calls, &manifest_key), stored.len());
}

/// A pass that the store interval held back leaves the partition among the
/// held ones. The tick stages its pass once the interval passed and not
/// before, whether or not the shard paces the partition or wakes it.
#[compio::test]
async fn given_a_pass_held_by_the_store_interval_when_the_tick_runs_should_stage_it_only_once_the_interval_passed()
 {
    let directory = tempfile::tempdir().unwrap();
    let (owner, publisher, _calls, _manifest_key) =
        Box::pin(timed_manifest_owner(directory.path())).await;
    let revision = staged_revision(&owner);
    pumped(&owner, async {
        send_and_rewrite(&owner, 1).await;
        until(&owner, |_| publisher.last_segments() == Some(1)).await;
        publish_healthy_segments(&owner, 2).await;
        compio::time::sleep(TASK_POLL_INTERVAL).await;
    })
    .await;
    // The pump stopped: settle the publish it left, and drop the rest.
    while let Ok(frame) = owner.inbox.try_recv() {
        if let ShardFrame::Lifecycle(LifecycleFrame::ManifestPublishFinished(finished)) = frame {
            owner.on_manifest_publish_finished(&finished);
        }
    }
    assert_eq!(publisher.last_segments(), Some(1), "the change waits");
    let started = publisher.stored().last().unwrap().published_at;
    let at = |micros: u64| IggyTimestamp::from(started + micros);
    owner.manifests_held.borrow_mut().clear();

    owner.on_publish_manifest(namespace(), revision, at(1));

    assert_eq!(publisher.last_segments(), Some(1));
    assert!(
        owner
            .manifests_held
            .borrow()
            .contains(&(namespace(), revision))
    );
    owner.stage_held_manifest_passes(at(micros_of(MANIFEST_STORE_INTERVAL) - 1));
    assert!(
        owner.inbox.try_recv().is_err(),
        "no pass before the interval"
    );
    owner.stage_held_manifest_passes(at(micros_of(MANIFEST_STORE_INTERVAL)));
    assert!(matches!(
        owner.inbox.try_recv(),
        Ok(ShardFrame::Lifecycle(LifecycleFrame::PublishManifest { namespace: staged, .. }))
            if staged == namespace()
    ));
    assert!(owner.manifests_held.borrow().is_empty());
}

/// Long-term segments commit every few milliseconds for seconds. The
/// manifest is put at most once a second, each put lists the log as it was
/// then, and the last one lists every segment once the changes stop.
#[compio::test]
async fn given_long_term_changes_every_few_milliseconds_for_seconds_when_storing_manifests_should_put_each_at_most_once_a_second()
 {
    const BUSY_FOR: Duration = Duration::from_secs(4);
    let directory = tempfile::tempdir().unwrap();
    let (owner, publisher, calls, manifest_key) =
        Box::pin(timed_manifest_owner(directory.path())).await;

    let (changes, busy) = pumped(&owner, async {
        send_and_rewrite(&owner, 1).await;
        let started = std::time::Instant::now();
        let mut segments = 1;
        while started.elapsed() < BUSY_FOR {
            segments += 1;
            publish_healthy_segments(&owner, segments).await;
            compio::time::sleep(Duration::from_millis(10)).await;
        }
        let busy = started.elapsed();
        until(&owner, |_| publisher.last_segments() == Some(segments)).await;
        (segments - 1, busy)
    })
    .await;

    let stored = publisher.stored();
    eprintln!(
        "{changes} long-term changes in {busy:?}: {} manifest puts",
        stored.len()
    );
    assert_eq!(puts_of(&calls, &manifest_key), stored.len());
    for pair in stored.windows(2) {
        assert!(
            pair[1].published_at - pair[0].published_at >= micros_of(MANIFEST_STORE_INTERVAL),
            "two puts less than the interval apart: {pair:?}"
        );
    }
    let mut per_second = BTreeMap::new();
    for manifest in &stored {
        *per_second
            .entry(manifest.published_at / 1_000_000)
            .or_insert(0) += 1;
    }
    assert!(per_second.values().all(|puts| *puts == 1), "{per_second:?}");
    assert!(
        stored.windows(2).all(|pair| pair[0].floor <= pair[1].floor),
        "{stored:?}"
    );
    assert!(changes > 50 && stored.len() < changes / 10, "{stored:?}");
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

/// An operator sees which partition lags: the shard leaves the messages and
/// the bytes each placing partition it leads has yet to rewrite where a
/// scrape reads them. A partition whose rewrites keep up reports zero.
#[compio::test]
async fn given_placing_partitions_this_shard_leads_when_the_tick_runs_should_report_the_lag_of_each()
 {
    use crate::metrics::LongTermLag;

    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(backlog_owner(directory.path(), 0)).await;
    let other = place_other_partition(&owner);
    let other_revision = owner
        .plane
        .partitions()
        .get_by_ns(&other)
        .unwrap()
        .created_revision();
    let lags = |owner: &IoTestShard| {
        let mut lags = owner.metrics().long_term_lags();
        lags.sort_by_key(|lag| lag.namespace.inner());
        lags
    };
    assert_eq!(lags(&owner), []);

    let backlog = pumped(&owner, async {
        let backlog = commit_and_count_a_backlog(&owner).await;
        owner.request_rewrite_segments(other, other_revision);
        until(&owner, |owner| backlog_of(owner, other) == Some(0)).await;
        until(&owner, |owner| lags(owner).len() == 2).await;
        let _second = submit_send(&owner, 2, 300);
        until(&owner, |owner| committed_sends(owner) == 2).await;
        until(&owner, |owner| lags(owner)[0].messages == 2).await;
        backlog
    })
    .await;

    let lags = lags(&owner);
    assert_eq!(
        lags[1],
        LongTermLag {
            namespace: other,
            messages: 0,
            bytes: Some(0),
        }
    );
    assert_eq!((lags[0].namespace, lags[0].messages), (namespace(), 2));
    assert!(lags[0].bytes > Some(backlog), "both sends: {lags:?}");
    assert_eq!(lags[0].bytes, backlog_of(&owner, namespace()));
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

/// The ceiling the shard was given bounds the wait that a small segment
/// lengthens, as `[geo] long_term_rewrite_max_interval` sets it.
#[compio::test]
async fn given_a_rewrite_ceiling_of_two_seconds_when_a_small_segment_settles_should_wait_two_seconds()
 {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    owner.set_long_term_rewrite_max_interval(Duration::from_secs(2));

    let wait = Box::pin(wait_after_a_small_segment(&owner)).await;

    assert_eq!(wait, Some(Duration::from_secs(2)));
}

/// A topic's own ceiling wins over the shard's, for its partitions alone.
#[compio::test]
async fn given_a_topic_with_its_own_rewrite_ceiling_when_a_small_segment_settles_should_wait_that_long()
 {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner_of_geo_topic(
        directory.path(),
        iggy_common::GeoTopicDurations {
            long_term_rewrite_max_interval: Some(iggy_common::IggyDuration::new(
                Duration::from_secs(3),
            )),
            ..iggy_common::GeoTopicDurations::default()
        },
    ))
    .await;
    owner.set_long_term_rewrite_max_interval(Duration::from_secs(2));

    let wait = Box::pin(wait_after_a_small_segment(&owner)).await;

    assert_eq!(wait, Some(Duration::from_secs(3)));
}

/// The wait of the partition [`namespace`] after its first rewrite stored
/// a segment of one small send, which lengthens the wait to its ceiling.
#[allow(clippy::future_not_send)]
async fn wait_after_a_small_segment(owner: &IoTestShard) -> Option<Duration> {
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
    let revision = staged_revision(owner);
    let floor = |owner: &IoTestShard| {
        owner
            .plane
            .partitions()
            .get_by_ns(&namespace())
            .unwrap()
            .long_term_floor()
    };

    pumped(owner, async {
        let _first = submit_send(owner, 1, 100);
        until(owner, |owner| committed_sends(owner) == 1).await;
        owner.request_rewrite_segments(namespace(), revision);
        until(owner, |owner| floor(owner) == Some(0)).await;
        owner
            .rewrite_pacing
            .borrow()
            .interval(&(namespace(), revision))
    })
    .await
}
