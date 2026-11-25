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

//! A partition that follows another cluster's log. The origin is a tiered
//! partition whose uploads stored its segments and its manifest in an `fs`
//! store. The follower lives in another stream and incarnation, holds
//! nothing of its own, and reads the origin's objects under the origin's
//! prefix.

use super::eviction::{KEEP_NOTHING, evict};
use super::floor_eviction::apply;
use super::free_space::send_request;
use super::segment_chain::{PREFIX, tiered_partition};
use super::*;
use crate::journal::MessageLookup;
use crate::poll_plan::{PollPlan, PollReadResult, PollTier};
use crate::segment_chain::PrefixAccess;
use crate::{
    IndexFile, IndexFiles, PeerLogError, PeerManifestContradiction, PeerManifestRefusal,
    RemoteObjects, RemoteSegments, SegmentLocation, upload_segment,
};
use futures::FutureExt;
use futures::channel::oneshot;
use futures::future::LocalBoxFuture;
use iggy_common::{IggyExpiry, IggyTimestamp, PollingStrategy};
use iggy_object_store::{
    FsStore, KeyLayout, ManifestSegment, MemoryStore, ObjectPrefix, ObjectRange, ObjectStore,
    ObjectStoreError, PartitionManifest, PutBody, PutOptions, RecordingStore, SEGMENT_HASH_LEN,
    SegmentKey, SegmentObject, StoreCall,
};
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::future::Future;
use std::io;
use std::ops::Range;
use std::pin::Pin;
use std::time::Duration;

/// The root of the origin cluster's store.
pub(super) const ORIGIN_ROOT: &str = "dc-a";

/// The incarnation of the follower's own partition, unlike the origin's.
const FOLLOWER_REVISION: u64 = 33;

/// The origin's offsets that its manifest lists: two sealed segments of
/// [`BATCHES_PER_SEGMENT`] one-message batches. Its active segment holds
/// offset 4, which no manifest lists yet.
const ORIGIN_FLOOR: u64 = 3;

pub(super) fn origin_layout() -> KeyLayout {
    KeyLayout::new(ORIGIN_ROOT).unwrap()
}

fn object_key(start_offset: u64, object: SegmentObject) -> String {
    origin_layout().segment_key(&SegmentKey {
        prefix: PREFIX,
        start_offset,
        object,
    })
}

/// The origin after every sealed segment went through an upload into
/// `store` and its floor advance committed, with the manifest it then
/// stored beside them.
pub(super) async fn origin_in(
    store: &FsStore,
) -> (
    tempfile::TempDir,
    Box<IggyPartition<IggyMessageBus>>,
    PartitionManifest,
) {
    let (directory, origin) = Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let mut origin = Box::new(origin);
    let mut op = 100;
    while let Some(next) = origin.next_segment_upload() {
        upload_segment(store, &origin_layout(), &next)
            .await
            .unwrap();
        let advance = origin.note_segment_uploaded(&next.record).unwrap();
        Box::pin(apply(&mut origin, op, advance)).await;
        op += 1;
    }
    let manifest = origin.manifest(IggyTimestamp::now()).unwrap();
    assert_eq!(manifest.floor, Some(ORIGIN_FLOOR));
    store
        .put(
            &origin_layout().manifest_key(&PREFIX),
            PutBody::from_bytes(manifest.encode()),
            &PutOptions::default(),
        )
        .await
        .unwrap();
    (directory, origin, manifest)
}

/// What a reader of the store finds under the manifest key.
async fn stored_manifest(store: &impl ObjectStore) -> PartitionManifest {
    let object = store
        .get_range(&origin_layout().manifest_key(&PREFIX), 0..u64::MAX)
        .await
        .unwrap();
    PartitionManifest::decode(&object.bytes).unwrap()
}

/// A partition of stream 5 that follows the origin's partition, reading
/// `objects` under the origin's root.
pub(super) fn follower(
    objects: Rc<dyn RemoteObjects>,
) -> (tempfile::TempDir, Box<IggyPartition<IggyMessageBus>>) {
    let (directory, mut partition) = unattached();
    partition.follow_peer_log(ORIGIN_ROOT, PREFIX).unwrap();
    partition.set_remote_segments(Rc::new(RemoteSegments::new(objects, origin_layout())));
    (directory, partition)
}

/// [`follower`] without a directory, as a simulated node builds it. Its
/// polls keep the indexes that they fetch in `index_files`.
pub(super) fn follower_without_files(
    objects: Rc<dyn RemoteObjects>,
    index_files: &MemoryIndexFiles,
) -> Box<IggyPartition<IggyMessageBus>> {
    let mut partition = without_files();
    partition.follow_peer_log(ORIGIN_ROOT, PREFIX).unwrap();
    partition.set_remote_segments(Rc::new(
        RemoteSegments::new(objects, origin_layout())
            .with_index_files(Rc::new(index_files.clone())),
    ));
    partition
}

/// A partition of stream 5 with a directory and an incarnation of its own,
/// which follows nothing yet.
pub(super) fn unattached() -> (tempfile::TempDir, Box<IggyPartition<IggyMessageBus>>) {
    let directory = tempfile::tempdir().unwrap();
    let mut partition = without_files();
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    (directory, partition)
}

/// A partition of stream 5 with an incarnation of its own and no directory,
/// which follows nothing yet.
fn without_files() -> Box<IggyPartition<IggyMessageBus>> {
    let namespace = IggyNamespace::new(5, 2, 0);
    let consensus = VsrConsensus::new(
        TEST_CLUSTER,
        0,
        1,
        namespace.inner(),
        IggyMessageBus::new(0),
        LocalPipeline::new(),
    );
    consensus.init();
    let mut partition = Box::new(IggyPartition::with_in_memory_storage(
        Arc::new(PartitionStats::default()),
        consensus,
        IggyByteSize::from(1024 * 1024),
    ));
    partition.set_created_revision(FOLLOWER_REVISION);
    partition
}

/// The `.index` files of a node that keeps them in memory, by path.
#[derive(Clone, Default)]
pub(super) struct MemoryIndexFiles(Rc<RefCell<BTreeMap<String, Vec<u8>>>>);

impl MemoryIndexFiles {
    /// Whether it holds a file whose name is `name`.
    pub(super) fn holds(&self, name: &str) -> bool {
        self.0
            .borrow()
            .keys()
            .any(|path| path.rsplit('/').next() == Some(name))
    }
}

/// A file of [`MemoryIndexFiles`] that a fetch writes.
struct MemoryIndexFile {
    files: MemoryIndexFiles,
    path: String,
}

impl IndexFiles for MemoryIndexFiles {
    fn create<'a>(&'a self, path: &'a str) -> LocalBoxFuture<'a, io::Result<Box<dyn IndexFile>>> {
        self.0.borrow_mut().insert(path.to_owned(), Vec::new());
        let file = MemoryIndexFile {
            files: self.clone(),
            path: path.to_owned(),
        };
        async move { Ok(Box::new(file) as Box<dyn IndexFile>) }.boxed_local()
    }

    fn rename<'a>(
        &'a self,
        source: &'a str,
        target: &'a str,
    ) -> LocalBoxFuture<'a, io::Result<()>> {
        let mut files = self.0.borrow_mut();
        let moved = files
            .remove(source)
            .map(|bytes| files.insert(target.to_owned(), bytes));
        drop(files);
        async move {
            moved
                .map(drop)
                .ok_or_else(|| io::ErrorKind::NotFound.into())
        }
        .boxed_local()
    }

    fn sync_directory<'a>(&'a self, _directory: &'a str) -> LocalBoxFuture<'a, io::Result<()>> {
        async { Ok(()) }.boxed_local()
    }

    fn remove<'a>(&'a self, path: &'a str) -> LocalBoxFuture<'a, io::Result<()>> {
        let removed = self.0.borrow_mut().remove(path);
        async move {
            removed
                .map(drop)
                .ok_or_else(|| io::ErrorKind::NotFound.into())
        }
        .boxed_local()
    }

    fn open<'a>(&'a self, path: &'a str) -> LocalBoxFuture<'a, Result<IggyIndexReader, IggyError>> {
        let bytes = self.0.borrow().get(path).cloned();
        async move {
            bytes
                .map(|bytes| IggyIndexReader::from_bytes(path, Bytes::from(bytes)))
                .ok_or(IggyError::CannotReadFile)
        }
        .boxed_local()
    }
}

impl IndexFile for MemoryIndexFile {
    fn write_at(&mut self, bytes: Bytes, position: u64) -> LocalBoxFuture<'_, io::Result<()>> {
        let start = usize::try_from(position).unwrap();
        let end = start + bytes.len();
        let mut files = self.files.0.borrow_mut();
        let file = files.entry(self.path.clone()).or_default();
        if file.len() < end {
            file.resize(end, 0);
        }
        file[start..end].copy_from_slice(&bytes);
        drop(files);
        async { Ok(()) }.boxed_local()
    }

    fn sync(&mut self) -> LocalBoxFuture<'_, io::Result<()>> {
        async { Ok(()) }.boxed_local()
    }
}

pub(super) fn plan<B: MessageBus>(
    partition: &mut IggyPartition<B>,
    strategy: PollingStrategy,
    count: u32,
) -> PollPlan {
    let args = PollingArgs::new(strategy, count, false);
    partition.build_poll_plan(PollingConsumer::Consumer(1, 0), &args, false)
}

/// The offsets of the messages an accepted read returns, and their bytes.
pub(super) type Polled = (Option<u64>, u32, Vec<u8>);

pub(super) fn accepted<B: MessageBus>(
    partition: &mut IggyPartition<B>,
    result: PollReadResult,
) -> Result<Polled, IggyError> {
    let (last, count) = (result.last_matching_offset, result.message_count);
    let completion = partition.complete_poll(result)?;
    let bytes = completion
        .fragments
        .iter()
        .flat_map(|fragment| fragment.as_slice().iter().copied())
        .collect();
    Ok((last, count, bytes))
}

pub(super) async fn polled<B: MessageBus>(
    partition: &mut IggyPartition<B>,
    strategy: PollingStrategy,
    count: u32,
) -> Polled {
    let result = plan(partition, strategy, count).execute().await;
    assert_eq!(result.read_error, None, "{strategy:?}");
    accepted(partition, result).unwrap()
}

pub(super) fn listed(start_offset: u64, end_offset: u64) -> ManifestSegment {
    ManifestSegment {
        start_offset,
        end_offset,
        start_timestamp: 1_000 + start_offset,
        end_timestamp: 1_000 + end_offset,
        max_timestamp: 1_000 + end_offset,
        size: (end_offset - start_offset + 1) * 100,
        crc32c: u32::try_from(start_offset).unwrap(),
        hash: [u8::try_from(start_offset % 251).unwrap(); SEGMENT_HASH_LEN],
    }
}

/// A manifest of the origin's incarnation that lists `segments`, by first
/// and last offset, above `trim_point`.
pub(super) fn manifest_of(trim_point: Option<u64>, segments: &[(u64, u64)]) -> PartitionManifest {
    let segments: Vec<_> = segments
        .iter()
        .map(|(start, end)| listed(*start, *end))
        .collect();
    PartitionManifest {
        prefix: PREFIX,
        published_at: 1,
        floor: segments
            .last()
            .map(|segment| segment.end_offset)
            .or(trim_point),
        trim_point,
        segments_keyed_by_both_offsets: false,
        segments,
    }
}

/// The origin's manifest when its store held only its first segment.
fn first_segment_only(manifest: &PartitionManifest) -> PartitionManifest {
    PartitionManifest {
        floor: Some(manifest.segments[0].end_offset),
        segments: manifest.segments[..1].to_vec(),
        ..manifest.clone()
    }
}

/// The origin's manifest once retention removed its first segment.
fn first_segment_trimmed(manifest: &PartitionManifest) -> PartitionManifest {
    PartitionManifest {
        trim_point: Some(manifest.segments[0].end_offset),
        segments: manifest.segments[1..].to_vec(),
        ..manifest.clone()
    }
}

/// The segments of the log by first and last offset, where they are, and
/// the offsets a poll sees.
pub(super) fn log_shape<B: MessageBus>(
    partition: &IggyPartition<B>,
) -> (Vec<(u64, u64, SegmentLocation)>, u64, bool) {
    (
        partition
            .log
            .segments()
            .iter()
            .map(|segment| (segment.start_offset, segment.end_offset, segment.location))
            .collect(),
        partition.offsets().commit_offset,
        partition.offset_space.committed_seeded,
    )
}

/// A store whose first read waits for `release`.
pub(super) struct Gated {
    pub(super) store: FsStore,
    pub(super) gate: RefCell<Option<oneshot::Receiver<()>>>,
    pub(super) waiting: Cell<bool>,
}

impl RemoteObjects for Gated {
    fn get_range<'a>(
        &'a self,
        key: &'a str,
        range: Range<u64>,
    ) -> LocalBoxFuture<'a, Result<ObjectRange, ObjectStoreError>> {
        async move {
            let gate = self.gate.borrow_mut().take();
            if let Some(gate) = gate {
                self.waiting.set(true);
                gate.await.unwrap();
            }
            ObjectStore::get_range(&self.store, key, range).await
        }
        .boxed_local()
    }
}

pub(super) type InFlight = Pin<Box<dyn Future<Output = PollReadResult>>>;

/// Run `poll` until it waits on the gated store.
pub(super) async fn until_gated(gated: &Gated, poll: &mut InFlight) {
    while !gated.waiting.get() {
        assert!(futures::poll!(poll.as_mut()).is_pending());
        compio::time::sleep(Duration::from_millis(1)).await;
    }
}

#[compio::test]
async fn given_an_origins_manifest_in_an_fs_store_when_a_follower_polls_should_return_the_origins_messages_at_the_same_offsets()
 {
    let bucket = tempfile::tempdir().unwrap();
    let store = FsStore::new(bucket.path());
    let (_origin_directory, mut origin, manifest) = Box::pin(origin_in(&store)).await;
    let (directory, mut follower) = follower(Rc::new(FsStore::new(bucket.path())));

    let applied = follower
        .apply_peer_manifest(&Box::pin(stored_manifest(&store)).await)
        .unwrap();

    assert_eq!(applied.appended, manifest.segments.len());
    assert_eq!(follower.offsets().commit_offset, ORIGIN_FLOOR);
    // (follower's poll, the origin's poll of the same messages)
    let cases = [
        (
            (PollingStrategy::offset(0), 10),
            (PollingStrategy::offset(0), 4),
        ),
        (
            (PollingStrategy::offset(1), 10),
            (PollingStrategy::offset(1), 3),
        ),
        (
            (PollingStrategy::offset(2), 1),
            (PollingStrategy::offset(2), 1),
        ),
        (
            (PollingStrategy::offset(3), 10),
            (PollingStrategy::offset(3), 1),
        ),
        (
            (PollingStrategy::first(), 10),
            (PollingStrategy::offset(0), 4),
        ),
        (
            (PollingStrategy::last(), 2),
            (PollingStrategy::offset(2), 2),
        ),
        (
            (PollingStrategy::timestamp(IggyTimestamp::from(0)), 10),
            (PollingStrategy::offset(0), 4),
        ),
    ];
    for ((strategy, count), (origin_strategy, origin_count)) in cases {
        let followed = Box::pin(polled(&mut follower, strategy, count)).await;
        let original = Box::pin(polled(&mut origin, origin_strategy, origin_count)).await;
        assert!(!followed.2.is_empty(), "{strategy:?}");
        assert_eq!(followed, original, "{strategy:?}");
    }
    // A timestamp between two batches starts at the batch after it, as on
    // the origin.
    let second = origin.log.segments()[1].start_timestamp;
    assert_eq!(
        Box::pin(polled(
            &mut follower,
            PollingStrategy::timestamp(IggyTimestamp::from(second)),
            10
        ))
        .await,
        Box::pin(polled(
            &mut origin,
            PollingStrategy::timestamp(IggyTimestamp::from(second)),
            2
        ))
        .await
    );
    // Nothing above the floor exists here yet.
    let past_floor = plan(&mut follower, PollingStrategy::offset(ORIGIN_FLOOR + 1), 10);
    assert!(matches!(past_floor.tier, PollTier::Empty));

    // The follower read every segment from the store: no `.log` of its own,
    // and the `.index` objects it fetched sit in its own directory.
    for segment in &manifest.segments {
        let stem = format!("{:020}", segment.start_offset);
        assert!(!directory.path().join(format!("{stem}.log")).exists());
        assert!(directory.path().join(format!("{stem}.index")).exists());
    }
}

#[compio::test]
async fn given_a_follower_when_it_polls_takes_manifests_and_runs_every_tiering_pass_should_only_read_under_the_origins_prefix()
 {
    let bucket = tempfile::tempdir().unwrap();
    let store = FsStore::new(bucket.path());
    let (_origin_directory, _origin, manifest) = Box::pin(origin_in(&store)).await;
    let recording = RecordingStore::new(FsStore::new(bucket.path()));
    let calls = recording.calls();
    let (_directory, mut follower) = follower(Rc::new(recording));
    let now = IggyTimestamp::now();

    for next in [
        first_segment_only(&manifest),
        manifest.clone(),
        first_segment_trimmed(&manifest),
    ] {
        let _ = follower.apply_peer_manifest(&next).unwrap();
        for strategy in [
            PollingStrategy::offset(0),
            PollingStrategy::timestamp(IggyTimestamp::from(0)),
        ] {
            Box::pin(polled(&mut follower, strategy, 10)).await;
        }
        // Every pass that stores or deletes objects of a tiered partition.
        assert!(follower.next_segment_upload().is_none());
        assert!(follower.begin_segment_upload().is_none());
        assert!(follower.begin_manifest_publish(now).is_none());
        assert!(follower.begin_object_delete(now, u64::MAX).is_none());
        let shape = log_shape(&follower);
        let evicted = Box::pin(evict(&mut follower, u64::MAX, KEEP_NOTHING)).await;
        assert_eq!(evicted.segments, 0);
        let expiry = IggyExpiry::ExpireDuration(iggy_common::IggyDuration::from(1));
        let removed = Box::pin(follower.clean_expired_segments(now, expiry, Some(0))).await;
        assert_eq!(removed.segments, 0);
        let removed = Box::pin(follower.remove_sealed_segments_up_to(u64::MAX)).await;
        assert_eq!(removed.segments, 0);
        assert_eq!(log_shape(&follower), shape, "only a manifest moves the log");
    }
    // A topic that turns tiering on asks for a chain the follower never
    // keeps, under its own prefix.
    follower.enable_tiering(crate::object_prefix(
        follower.namespace(),
        FOLLOWER_REVISION,
    ));
    Box::pin(settle_partition_io(&mut follower, &repair_config())).await;
    assert!(!follower.is_tiered());
    assert!(!follower.places_batches());

    let incarnation = origin_layout().incarnation_prefix(&PREFIX);
    let all = calls.all();
    assert_ne!(all, Vec::<StoreCall>::new());
    for call in all {
        match call {
            StoreCall::GetRange { key, .. } => assert!(key.starts_with(&incarnation), "{key}"),
            other => panic!("the follower sent {other:?}"),
        }
    }
}

#[compio::test]
async fn given_a_poll_in_flight_when_a_newer_manifest_grows_the_log_should_complete_the_poll_with_what_it_planned()
 {
    let bucket = tempfile::tempdir().unwrap();
    let store = FsStore::new(bucket.path());
    let (_origin_directory, mut origin, manifest) = Box::pin(origin_in(&store)).await;
    let (release, gate) = oneshot::channel();
    let gated = Rc::new(Gated {
        store: FsStore::new(bucket.path()),
        gate: RefCell::new(Some(gate)),
        waiting: Cell::new(false),
    });
    let (_directory, mut follower) = follower(gated.clone());
    let _ = follower
        .apply_peer_manifest(&first_segment_only(&manifest))
        .unwrap();
    let mut poll: InFlight =
        Box::pin(plan(&mut follower, PollingStrategy::offset(0), 10).execute());
    Box::pin(until_gated(&gated, &mut poll)).await;

    let grown = follower.apply_peer_manifest(&manifest).unwrap();
    release.send(()).unwrap();
    let result = poll.await;

    assert_eq!(grown.appended, 1);
    let first = manifest.segments[0].end_offset;
    assert_eq!(
        accepted(&mut follower, result).unwrap(),
        Box::pin(polled(
            &mut origin,
            PollingStrategy::offset(0),
            u32::try_from(first + 1).unwrap()
        ))
        .await
    );
    assert_eq!(
        Box::pin(polled(&mut follower, PollingStrategy::offset(0), 10)).await,
        Box::pin(polled(&mut origin, PollingStrategy::offset(0), 4)).await,
        "a later poll reads the grown log"
    );
}

#[compio::test]
async fn given_a_read_of_a_segment_a_trim_dropped_when_completing_should_refuse_it_and_the_retry_should_start_at_the_new_front()
 {
    let bucket = tempfile::tempdir().unwrap();
    let store = FsStore::new(bucket.path());
    let (_origin_directory, mut origin, manifest) = Box::pin(origin_in(&store)).await;
    let (directory, mut follower) = follower(Rc::new(FsStore::new(bucket.path())));
    let _ = follower.apply_peer_manifest(&manifest).unwrap();
    let from_front = plan(&mut follower, PollingStrategy::offset(0), 10)
        .execute()
        .await;
    let above_trim = plan(&mut follower, PollingStrategy::offset(2), 10)
        .execute()
        .await;
    assert_eq!(from_front.read_error, None);
    assert!(directory.path().join(format!("{:020}.index", 0)).exists());

    let trimmed = follower
        .apply_peer_manifest(&first_segment_trimmed(&manifest))
        .unwrap();

    assert_eq!(trimmed.trimmed, 1);
    assert_eq!(
        accepted(&mut follower, from_front),
        Err(IggyError::TransientNotAccepted),
        "a read that holds a trimmed range is never served"
    );
    let new_front = Box::pin(polled(&mut origin, PollingStrategy::offset(2), 2)).await;
    assert_eq!(
        accepted(&mut follower, above_trim).unwrap(),
        new_front,
        "a read above the trim point is accepted as it is"
    );
    assert_eq!(
        Box::pin(polled(&mut follower, PollingStrategy::offset(0), 10)).await,
        new_front
    );
    Box::pin(trimmed.remove_retired_indexes(&crate::DiskIndexFiles)).await;
    assert!(!directory.path().join(format!("{:020}.index", 0)).exists());
}

#[compio::test]
async fn given_a_follower_without_files_when_a_trim_drops_a_polled_segment_should_remove_the_index_its_poll_fetched()
 {
    let bucket = tempfile::tempdir().unwrap();
    let store = FsStore::new(bucket.path());
    let (_origin_directory, _origin, manifest) = Box::pin(origin_in(&store)).await;
    let index_files = MemoryIndexFiles::default();
    let mut follower = follower_without_files(Rc::new(FsStore::new(bucket.path())), &index_files);
    let _ = follower.apply_peer_manifest(&manifest).unwrap();
    let from_front = plan(&mut follower, PollingStrategy::offset(0), 10)
        .execute()
        .await;
    let first_index = format!("{:020}.index", 0);
    assert_eq!(from_front.read_error, None);
    assert!(index_files.holds(&first_index));

    let trimmed = follower
        .apply_peer_manifest(&first_segment_trimmed(&manifest))
        .unwrap();
    Box::pin(trimmed.remove_retired_indexes(&index_files)).await;

    assert!(
        !index_files.holds(&first_index),
        "the index of the trimmed segment is removed where its poll put it"
    );
}

#[compio::test]
async fn given_a_poll_in_flight_when_a_trim_drops_its_segment_and_the_origin_deletes_it_should_ask_for_a_retry()
 {
    let bucket = tempfile::tempdir().unwrap();
    let store = FsStore::new(bucket.path());
    let (_origin_directory, mut origin, manifest) = Box::pin(origin_in(&store)).await;
    let (release, gate) = oneshot::channel();
    let gated = Rc::new(Gated {
        store: FsStore::new(bucket.path()),
        gate: RefCell::new(Some(gate)),
        waiting: Cell::new(false),
    });
    let (_directory, mut follower) = follower(gated.clone());
    let _ = follower.apply_peer_manifest(&manifest).unwrap();
    let mut poll: InFlight =
        Box::pin(plan(&mut follower, PollingStrategy::offset(0), 10).execute());
    Box::pin(until_gated(&gated, &mut poll)).await;

    let _ = follower
        .apply_peer_manifest(&first_segment_trimmed(&manifest))
        .unwrap();
    let deleted = Box::pin(store.delete(&[
        object_key(0, SegmentObject::Log),
        object_key(0, SegmentObject::Index),
    ]))
    .await
    .unwrap();
    assert!(deleted.is_complete());
    release.send(()).unwrap();
    let result = poll.await;

    assert!(result.read_error.is_some(), "the objects are gone");
    assert_eq!(
        accepted(&mut follower, result),
        Err(IggyError::TransientNotAccepted)
    );
    assert_eq!(
        Box::pin(polled(&mut follower, PollingStrategy::offset(0), 10)).await,
        Box::pin(polled(&mut origin, PollingStrategy::offset(2), 2)).await
    );
}

#[compio::test]
async fn given_manifests_that_break_the_log_when_applying_should_refuse_each_and_change_nothing() {
    let (_directory, mut follower) = follower(Rc::new(MemoryStore::new()));
    let applied = manifest_of(Some(9), &[(10, 19), (20, 29)]);
    let _ = follower.apply_peer_manifest(&applied).unwrap();
    let before = log_shape(&follower);
    let applied_state = follower.peer_log().unwrap().applied();

    let mut changed_hash = manifest_of(Some(9), &[(10, 19), (20, 29), (30, 39)]);
    changed_hash.segments[1].hash[0] ^= 1;
    let mut empty = manifest_of(Some(9), &[(10, 19), (20, 29), (30, 39)]);
    empty.segments[2].size = 0;
    let cases = [
        (
            "another incarnation",
            PartitionManifest {
                prefix: ObjectPrefix {
                    created_revision: PREFIX.created_revision + 1,
                    ..PREFIX
                },
                ..applied.clone()
            },
            PeerManifestRefusal::OtherIncarnation {
                expected: PREFIX,
                found: ObjectPrefix {
                    created_revision: PREFIX.created_revision + 1,
                    ..PREFIX
                },
            },
        ),
        (
            "a lower floor",
            manifest_of(Some(9), &[(10, 19)]),
            PeerManifestRefusal::GoesBack {
                floor: Some(19),
                trim_point: Some(9),
                segments: 1,
                applied_floor: Some(29),
                applied_trim_point: Some(9),
                applied_segments: 2,
            },
        ),
        (
            "no trim point after one",
            manifest_of(None, &[(5, 9), (10, 19), (20, 29)]),
            PeerManifestRefusal::GoesBack {
                floor: Some(29),
                trim_point: None,
                segments: 3,
                applied_floor: Some(29),
                applied_trim_point: Some(9),
                applied_segments: 2,
            },
        ),
        (
            "a gap",
            manifest_of(Some(9), &[(10, 19), (20, 29), (31, 39)]),
            PeerManifestRefusal::Gap { start_offset: 31 },
        ),
        (
            "a segment at the trim point",
            manifest_of(Some(10), &[(10, 19), (20, 29)]),
            PeerManifestRefusal::Gap { start_offset: 10 },
        ),
        (
            "a segment without bytes",
            empty,
            PeerManifestRefusal::EmptySegment { start_offset: 30 },
        ),
        (
            "a floor past the last segment",
            PartitionManifest {
                floor: Some(35),
                ..applied
            },
            PeerManifestRefusal::Floor { floor: Some(35) },
        ),
        (
            "a kept segment that changed",
            changed_hash,
            PeerManifestRefusal::Diverged {
                start_offset: 20,
                contradiction: PeerManifestContradiction::DoesNotAddUp,
            },
        ),
        (
            "a kept segment cut in two",
            manifest_of(Some(14), &[(15, 19), (20, 29)]),
            PeerManifestRefusal::Diverged {
                start_offset: 10,
                contradiction: PeerManifestContradiction::StartsOffBoundary,
            },
        ),
    ];
    for (case, manifest, refusal) in cases {
        assert_eq!(
            follower.apply_peer_manifest(&manifest),
            Err(refusal),
            "{case}"
        );
        assert_eq!(log_shape(&follower), before, "{case}");
        assert_eq!(
            follower.peer_log().unwrap().applied(),
            applied_state,
            "{case}"
        );
    }
}

#[compio::test]
async fn given_manifests_that_grow_and_trim_when_applying_should_keep_the_log_one_run_with_an_empty_tail()
 {
    let (_directory, mut follower) = follower(Rc::new(MemoryStore::new()));
    let tail = |start| (start, start, SegmentLocation::Local);
    let remote = |start, end| (start, end, SegmentLocation::Remote);
    assert_eq!(log_shape(&follower), (vec![tail(0)], 0, false));
    let segments = follower.stats.segments_count_inconsistent();

    // (manifest, appended, trimmed, the log after it, commit offset)
    let steps = [
        (manifest_of(None, &[]), 0, 0, vec![tail(0)], None),
        (
            manifest_of(None, &[(0, 9)]),
            1,
            0,
            vec![remote(0, 9), tail(10)],
            Some(9),
        ),
        (
            manifest_of(None, &[(0, 9), (10, 19), (20, 29)]),
            2,
            0,
            vec![remote(0, 9), remote(10, 19), remote(20, 29), tail(30)],
            Some(29),
        ),
        (
            manifest_of(Some(9), &[(10, 19), (20, 29)]),
            0,
            1,
            vec![remote(10, 19), remote(20, 29), tail(30)],
            Some(29),
        ),
        (
            manifest_of(Some(19), &[(20, 29), (30, 39)]),
            1,
            1,
            vec![remote(20, 29), remote(30, 39), tail(40)],
            Some(39),
        ),
        (
            manifest_of(Some(19), &[(20, 29), (30, 39)]),
            0,
            0,
            vec![remote(20, 29), remote(30, 39), tail(40)],
            Some(39),
        ),
        (manifest_of(Some(49), &[]), 0, 2, vec![tail(50)], Some(49)),
    ];
    for (manifest, appended, trimmed, log, commit_offset) in steps {
        let applied = follower.apply_peer_manifest(&manifest).unwrap();
        assert_eq!((applied.appended, applied.trimmed), (appended, trimmed));
        assert_eq!(
            log_shape(&follower),
            (
                log.clone(),
                commit_offset.unwrap_or_default(),
                commit_offset.is_some()
            )
        );
        assert_eq!(
            follower.stats.segments_count_inconsistent(),
            segments + u32::try_from(log.len()).unwrap() - 1
        );
        let held: u64 = manifest
            .segments
            .iter()
            .map(|segment| segment.end_offset - segment.start_offset + 1)
            .sum();
        assert_eq!(follower.stats.messages_count_inconsistent(), held);
        assert_eq!(
            follower.stats.current_offset(),
            commit_offset.unwrap_or_default()
        );
    }
}

#[compio::test]
async fn given_a_partition_with_a_log_of_its_own_when_following_a_peer_should_refuse() {
    let (_directory, tiered) = Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let mut tiered = Box::new(tiered);
    let (_directory, written) = Box::pin(partition_with_sealed_segments(&segment_records(
        RECEIVER_OPS,
    )))
    .await;
    let mut written = Box::new(written);
    let (_follower_directory, mut follower) = follower(Rc::new(MemoryStore::new()));

    assert_eq!(
        tiered.follow_peer_log(ORIGIN_ROOT, PREFIX),
        Err(PeerLogError::Tiered)
    );
    assert_eq!(
        written.follow_peer_log(ORIGIN_ROOT, PREFIX),
        Err(PeerLogError::HoldsMessages)
    );
    assert_eq!(
        follower.follow_peer_log(ORIGIN_ROOT, PREFIX),
        Err(PeerLogError::Following)
    );
    let mut placing = Box::new(test_partition());
    placing.set_places_batches(true);
    assert_eq!(
        placing.follow_peer_log(ORIGIN_ROOT, PREFIX),
        Err(PeerLogError::PlacesBatches)
    );
    assert!(tiered.peer_log().is_none() && written.peer_log().is_none());
}

#[compio::test]
async fn given_a_follower_when_a_client_sends_messages_should_refuse_them_and_journal_nothing() {
    let (mut partition, _) = recording_partition();
    partition.follow_peer_log(ORIGIN_ROOT, PREFIX).unwrap();
    let _ = partition
        .apply_peer_manifest(&manifest_of(None, &[(0, 3)]))
        .unwrap();

    let (sender, refusal) = consensus::oneshot_channel();
    partition
        .on_request(send_request(&partition), Some(sender))
        .await;

    assert_eq!(
        refusal.await.unwrap().header().status,
        IggyError::FeatureUnavailable.as_code()
    );
    assert_eq!(partition.consensus.sequencer().current_sequence(), 0);
    assert_eq!(partition.offsets().commit_offset, 3);
}

#[compio::test]
async fn given_a_follower_when_a_consumer_stores_its_offset_should_keep_it_as_a_local_offset() {
    let (mut partition, _) = recording_partition();
    let directory = tempfile::tempdir().unwrap();
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
        Some(directory.path().to_string_lossy().into_owned());
    partition.follow_peer_log(ORIGIN_ROOT, PREFIX).unwrap();
    let _ = partition
        .apply_peer_manifest(&manifest_of(None, &[(0, 1), (2, 3)]))
        .unwrap();

    let shape = log_shape(&partition);

    partition
        .on_request(
            store_offset_request(42, 1, ConsumerKind::Consumer, 7, 2, AckLevel::Quorum),
            None,
        )
        .await;
    commit_recorded_loopback(&mut partition).await;
    // The commit flushed the journal through the empty tail, which neither
    // filled nor rolled.
    assert_eq!(log_shape(&partition), shape);
    assert!(partition.log.journal().inner.is_empty());
    let _ = partition
        .apply_peer_manifest(&manifest_of(Some(1), &[(2, 3), (4, 9)]))
        .unwrap();

    let consumer = PollingConsumer::Consumer(7, 0);
    assert_eq!(partition.get_consumer_offset(consumer), Some(2));
    assert_eq!(
        partition.durable_consumer_offset_count(ConsumerKind::Consumer),
        1
    );
    let args = PollingArgs::new(PollingStrategy::next(), 10, false);
    let next = partition.build_poll_plan(consumer, &args, false);
    assert!(matches!(
        next.tier,
        PollTier::Disk {
            query: MessageLookup::Offset { offset: 3, .. },
            ..
        }
    ));
}

#[compio::test]
async fn given_no_trim_when_applying_should_name_no_retired_index() {
    let (_directory, mut follower) = follower(Rc::new(MemoryStore::new()));
    let applied = follower
        .apply_peer_manifest(&manifest_of(None, &[(0, 9)]))
        .unwrap();
    assert_eq!(
        (applied.appended, applied.trimmed, applied.replaced),
        (1, 0, 0)
    );
    assert_eq!(applied.retired_indexes(), [] as [String; 0]);
}

#[compio::test]
async fn given_each_prefix_when_asking_what_a_partition_may_send_should_admit_reads_only_under_the_origins()
 {
    let (_directory, follower) = follower(Rc::new(MemoryStore::new()));
    let own = Box::new(test_partition());
    let follower_own = crate::object_prefix(follower.namespace(), FOLLOWER_REVISION);
    let other_incarnation = ObjectPrefix {
        created_revision: PREFIX.created_revision + 1,
        ..PREFIX
    };
    let own_prefix = crate::object_prefix(own.namespace(), own.created_revision());
    assert_eq!(
        own_prefix, PREFIX,
        "the fixture's own prefix is the origin's"
    );

    // (partition, prefix, read, write)
    let cases = [
        ("the origin's, followed", &follower, PREFIX, true, false),
        ("its own", &follower, follower_own, false, false),
        (
            "another incarnation",
            &follower,
            other_incarnation,
            false,
            false,
        ),
        ("a writer's own", &own, own_prefix, true, true),
        ("a writer's other", &own, other_incarnation, false, false),
    ];
    for (case, partition, prefix, read, write) in cases {
        assert_eq!(
            (
                partition.admits_prefix(&prefix, PrefixAccess::Read),
                partition.admits_prefix(&prefix, PrefixAccess::Write)
            ),
            (read, write),
            "{case}"
        );
    }
    assert_eq!(follower.read_prefix(), Some(PREFIX));
    assert!(!follower.owns_chain_prefix(&PREFIX));
}
