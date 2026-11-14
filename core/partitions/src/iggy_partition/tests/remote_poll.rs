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

use super::eviction::{KEEP_NOTHING, confirm, evict};
use super::segment_chain::{PREFIX, tiered_partition};
use super::*;
use crate::iggy_index::IGGY_INDEX_SIZE;
use crate::poll_plan::{PollPlan, PollReadResult, PollTier};
use crate::{RemoteObjects, RemoteSegments, SegmentLocation};
use futures::FutureExt;
use futures::channel::oneshot;
use futures::future::LocalBoxFuture;
use iggy_binary_protocol::batch::BatchHeader;
use iggy_common::{IggyTimestamp, PollingStrategy};
use iggy_object_store::{
    KeyLayout, MemoryStore, ObjectRange, ObjectStore, ObjectStoreError, PutBody, PutOptions,
    SegmentKey, SegmentObject,
};
use server_common::SegmentStorage;
use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::time::Duration;

/// Sealed segments of a partition with [`RECEIVER_OPS`] one-message ops.
const SEALED: [u64; 2] = [0, 2];

const ROOT: &str = "iggy";

fn layout() -> KeyLayout {
    KeyLayout::new(ROOT).unwrap()
}

fn object_key(start_offset: u64, object: SegmentObject) -> String {
    layout().segment_key(&SegmentKey {
        prefix: PREFIX,
        start_offset,
        object,
    })
}

async fn put(store: &MemoryStore, key: &str, bytes: Vec<u8>) {
    store
        .put(key, PutBody::from_bytes(bytes), &PutOptions::default())
        .await
        .unwrap();
}

/// Store the `.log` and the `.index` of every sealed segment, as an upload
/// leaves them.
async fn upload(store: &MemoryStore, directory: &Path) {
    for start_offset in SEALED {
        for object in SegmentObject::ALL {
            let file = directory.join(format!("{start_offset:020}.{}", object.extension()));
            put(
                store,
                &object_key(start_offset, object),
                std::fs::read(file).unwrap(),
            )
            .await;
        }
    }
}

fn strategies() -> [PollingStrategy; 4] {
    [
        PollingStrategy::offset(0),
        PollingStrategy::offset(1),
        PollingStrategy::offset(3),
        PollingStrategy::timestamp(IggyTimestamp::from(0)),
    ]
}

fn plan(partition: &mut IggyPartition<IggyMessageBus>, strategy: PollingStrategy) -> PollPlan {
    let args = PollingArgs::new(strategy, 10, false);
    partition.build_poll_plan(PollingConsumer::Consumer(1, 0), &args, false)
}

fn accepted(
    partition: &mut IggyPartition<IggyMessageBus>,
    result: PollReadResult,
) -> (Option<u64>, Vec<u8>) {
    assert_eq!(result.read_error, None);
    let last_matching_offset = result.last_matching_offset;
    let completion = partition.complete_poll(result).unwrap();
    let bytes = completion
        .fragments
        .iter()
        .flat_map(|fragment| fragment.as_slice().iter().copied())
        .collect();
    (last_matching_offset, bytes)
}

async fn polled(
    partition: &mut IggyPartition<IggyMessageBus>,
    strategy: PollingStrategy,
) -> (Option<u64>, Vec<u8>) {
    let result = plan(partition, strategy).execute().await;
    accepted(partition, result)
}

async fn polled_all(partition: &mut IggyPartition<IggyMessageBus>) -> Vec<(Option<u64>, Vec<u8>)> {
    let mut polls = Vec::new();
    for strategy in strategies() {
        polls.push(Box::pin(polled(partition, strategy)).await);
    }
    polls
}

async fn failed(
    partition: &mut IggyPartition<IggyMessageBus>,
    strategy: PollingStrategy,
) -> IggyError {
    let result = plan(partition, strategy).execute().await;
    assert_eq!(result.message_count, 0);
    result.read_error.unwrap()
}

/// A tiered partition whose sealed segments are uploaded, with what every
/// poll returned while they were all local.
async fn uploaded_partition() -> (
    tempfile::TempDir,
    IggyPartition<IggyMessageBus>,
    Rc<MemoryStore>,
    Vec<(Option<u64>, Vec<u8>)>,
) {
    let (directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let local = Box::pin(polled_all(&mut partition)).await;
    assert_eq!(local[0].0, Some(RECEIVER_OPS - 1));
    let store = Rc::new(MemoryStore::new());
    upload(&store, directory.path()).await;
    confirm(&mut partition, &SEALED);
    (directory, partition, store, local)
}

fn attach(partition: &mut IggyPartition<IggyMessageBus>, objects: Rc<dyn RemoteObjects>) {
    partition.set_remote_segments(Rc::new(RemoteSegments::new(objects, layout())));
}

async fn evict_sealed(partition: &mut IggyPartition<IggyMessageBus>, directory: &Path) {
    Box::pin(evict(partition, u64::MAX, KEEP_NOTHING)).await;
    for start_offset in SEALED {
        assert!(!directory.join(format!("{start_offset:020}.log")).exists());
    }
    assert_eq!(
        partition
            .log
            .segments()
            .iter()
            .filter(|segment| segment.location == SegmentLocation::Remote)
            .count(),
        SEALED.len()
    );
}

fn index_path(directory: &Path, start_offset: u64) -> std::path::PathBuf {
    directory.join(format!("{start_offset:020}.index"))
}

#[compio::test]
async fn given_evicted_segments_when_polling_across_them_should_return_the_bytes_of_a_local_partition()
 {
    let (directory, mut partition, store, local) = Box::pin(uploaded_partition()).await;
    attach(&mut partition, store);
    Box::pin(evict_sealed(&mut partition, directory.path())).await;

    assert_eq!(Box::pin(polled_all(&mut partition)).await, local);
}

#[compio::test]
async fn given_evicted_segments_without_a_local_index_when_polling_should_fetch_the_index_object() {
    let (directory, mut partition, store, local) = Box::pin(uploaded_partition()).await;
    attach(&mut partition, store.clone());
    Box::pin(evict_sealed(&mut partition, directory.path())).await;
    let uploaded = SEALED.map(|start| std::fs::read(index_path(directory.path(), start)).unwrap());
    for start_offset in SEALED {
        std::fs::remove_file(index_path(directory.path(), start_offset)).unwrap();
    }

    assert_eq!(Box::pin(polled_all(&mut partition)).await, local);
    // Only a poll's starting segment needs its index, and every sealed
    // segment started one of the polls.
    for (start_offset, index) in SEALED.into_iter().zip(uploaded) {
        assert_eq!(
            std::fs::read(index_path(directory.path(), start_offset)).unwrap(),
            index
        );
    }
    let staged = std::fs::read_dir(directory.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(".staging"))
        .count();
    assert_eq!(staged, 0);
}

#[compio::test]
async fn given_a_corrupt_index_object_when_polling_should_refuse_it_and_write_nothing() {
    let (directory, mut partition, store, _local) = Box::pin(uploaded_partition()).await;
    attach(&mut partition, store.clone());
    Box::pin(evict_sealed(&mut partition, directory.path())).await;
    let index = std::fs::read(index_path(directory.path(), 0)).unwrap();
    let log_size = partition.log.segments()[0].size.as_bytes_u64();
    std::fs::remove_file(index_path(directory.path(), 0)).unwrap();

    let mut past_the_segment = index.clone();
    let last_position = past_the_segment.len() - 8;
    past_the_segment[last_position..].copy_from_slice(&log_size.to_le_bytes());
    let mut before_the_segment = index.clone();
    before_the_segment.extend_from_slice(&index[..IGGY_INDEX_SIZE]);
    before_the_segment.extend_from_slice(&[0; IGGY_INDEX_SIZE]);
    let torn = index[..index.len() - 1].to_vec();
    for corrupt in [past_the_segment, before_the_segment, torn] {
        put(&store, &object_key(0, SegmentObject::Index), corrupt).await;
        assert_eq!(
            Box::pin(failed(&mut partition, PollingStrategy::offset(0))).await,
            IggyError::CannotReadMessage
        );
        assert!(!index_path(directory.path(), 0).exists());
    }

    // The segment after it has its own index, so it still reads.
    let (last, _) = Box::pin(polled(&mut partition, PollingStrategy::offset(2))).await;
    assert_eq!(last, Some(RECEIVER_OPS - 1));
}

#[compio::test]
async fn given_a_segment_evicted_after_planning_when_the_walk_reaches_it_should_read_it_remotely() {
    let (directory, mut partition, store, local) = Box::pin(uploaded_partition()).await;
    attach(&mut partition, store);
    let planned = strategies().map(|strategy| plan(&mut partition, strategy));
    // A walk keeps no descriptor for the segments it only passes through, so
    // it opens them by path when it gets there.
    for plan in &planned {
        let PollTier::Disk { disk, .. } = &plan.tier else {
            panic!("the poll reads from disk");
        };
        for segment in &disk.segments {
            segment.read_state.fd.borrow_mut().take();
        }
    }
    Box::pin(evict_sealed(&mut partition, directory.path())).await;

    let mut polls = Vec::new();
    for plan in planned {
        let result = plan.execute().await;
        polls.push(accepted(&mut partition, result));
    }
    assert_eq!(polls, local);
}

#[compio::test]
async fn given_a_damaged_object_when_polling_without_checksum_validation_should_refuse_its_batch() {
    let (directory, mut partition, store, _local) = Box::pin(uploaded_partition()).await;
    attach(&mut partition, store.clone());
    Box::pin(evict_sealed(&mut partition, directory.path())).await;
    let key = object_key(0, SegmentObject::Log);
    let mut log = ObjectStore::get_range(&*store, &key, 0..u64::MAX)
        .await
        .unwrap()
        .bytes
        .to_vec();
    let last = log.len() - 1;
    log[last] ^= 0xff;
    put(&store, &key, log).await;

    // The plan was built with validation off, which a local segment honors.
    let result = plan(&mut partition, PollingStrategy::offset(0))
        .execute()
        .await;
    assert!(result.truncated);
    assert_eq!(result.last_matching_offset, Some(0));
}

#[compio::test]
async fn given_remote_segments_without_a_store_when_polling_should_ask_for_a_retry() {
    let (directory, mut partition, _store, _local) = Box::pin(uploaded_partition()).await;
    Box::pin(evict_sealed(&mut partition, directory.path())).await;

    assert_eq!(
        Box::pin(failed(&mut partition, PollingStrategy::offset(0))).await,
        IggyError::TransientNotAccepted
    );
}

type StoreFailure = fn(&str) -> ObjectStoreError;

/// A store that answers every read with one error.
struct Failing(StoreFailure);

impl RemoteObjects for Failing {
    fn get_range<'a>(
        &'a self,
        key: &'a str,
        _range: Range<u64>,
    ) -> LocalBoxFuture<'a, Result<ObjectRange, ObjectStoreError>> {
        async move { Err((self.0)(key)) }.boxed_local()
    }
}

#[compio::test]
async fn given_a_failing_store_when_polling_should_map_the_error_to_its_class() {
    let cases: [(StoreFailure, IggyError); 3] = [
        (
            |_| ObjectStoreError::Throttled("slow down".to_owned()),
            IggyError::TransientNotAccepted,
        ),
        (
            |key| ObjectStoreError::NotFound {
                key: key.to_owned(),
            },
            IggyError::CannotReadMessage,
        ),
        (
            |_| ObjectStoreError::Auth("denied".to_owned()),
            IggyError::CannotReadMessage,
        ),
    ];
    for (error, expected) in cases {
        let (directory, mut partition, _store, _local) = Box::pin(uploaded_partition()).await;
        attach(&mut partition, Rc::new(Failing(error)));
        Box::pin(evict_sealed(&mut partition, directory.path())).await;

        assert_eq!(
            Box::pin(failed(&mut partition, PollingStrategy::offset(0))).await,
            expected
        );
    }
}

fn planned_segments(plan: &PollPlan) -> (Vec<u64>, bool) {
    let PollTier::Disk { disk, .. } = &plan.tier else {
        panic!("the poll reads from disk");
    };
    (
        disk.segments
            .iter()
            .map(|segment| segment.start_offset)
            .collect(),
        disk.cut,
    )
}

#[compio::test]
async fn given_a_poll_that_later_segments_cover_when_planning_should_stop_the_snapshot_there() {
    let (_directory, mut partition, _store, local) = Box::pin(uploaded_partition()).await;
    let consumer = PollingConsumer::Consumer(1, 0);
    let batch_len = build_segment_record(partition.namespace(), 0).len();
    let mut plan_within = |count, byte_budget| {
        let args = PollingArgs::new(PollingStrategy::offset(0), count, false);
        partition.build_poll_plan_within(consumer, &args, false, byte_budget)
    };

    // The starting segment counts toward nothing, so two messages need the
    // segment after it and no more.
    let by_count = plan_within(2, usize::MAX);
    assert_eq!(planned_segments(&by_count), (vec![0, 2], true));
    let by_bytes = plan_within(10, batch_len);
    assert_eq!(planned_segments(&by_bytes), (vec![0, 2], true));
    let whole = plan_within(10, usize::MAX);
    assert_eq!(planned_segments(&whole), (vec![0, 2, 4], false));

    // A snapshot that stops early still serves a prefix of the same bytes,
    // and never claims the partition ends there.
    let result = by_bytes.execute().await;
    assert!(result.truncated);
    let (last, bytes) = accepted(&mut partition, result);
    assert_eq!(last, Some(3));
    assert!(local[0].1.starts_with(&bytes));
}

/// The header of the batch at `position` of segment 0's object.
async fn stored_batch(store: &MemoryStore, position: u64) -> BatchHeader {
    let log = ObjectStore::get_range(store, &object_key(0, SegmentObject::Log), 0..u64::MAX)
        .await
        .unwrap()
        .bytes;
    BatchHeader::decode(&log[usize::try_from(position).unwrap()..]).unwrap()
}

#[compio::test]
async fn given_an_index_entry_that_names_a_later_batch_when_polling_should_refuse_it() {
    let (directory, mut partition, store, _local) = Box::pin(uploaded_partition()).await;
    attach(&mut partition, store.clone());
    Box::pin(evict_sealed(&mut partition, directory.path())).await;
    let first = stored_batch(&store, 0).await;
    let second_position = first.total_size() as u64;
    let second = stored_batch(&store, second_position).await;
    assert_eq!((first.base_offset, second.base_offset), (0, 1));

    // Each entry stays inside the segment and none steps back, which is all
    // a fetch can check, and each one starts the walk at the second batch for
    // a poll that the first batch answers.
    let wrong_offset = [first.base_offset, first.base_timestamp, second_position];
    let wrong_timestamp = [second.base_offset, 0, second_position];
    let polls = [
        (wrong_offset, PollingStrategy::offset(0)),
        (
            wrong_timestamp,
            PollingStrategy::timestamp(IggyTimestamp::from(0)),
        ),
    ];
    for (entry, strategy) in polls {
        std::fs::remove_file(index_path(directory.path(), 0)).unwrap();
        partition.log.invalidate_sealed_read_state();
        let index = entry.into_iter().flat_map(u64::to_le_bytes).collect();
        put(&store, &object_key(0, SegmentObject::Index), index).await;

        // The second poll resolves the entry from the index the first kept.
        for _ in 0..2 {
            assert_eq!(
                Box::pin(failed(&mut partition, strategy)).await,
                IggyError::CannotReadMessage
            );
        }
    }
}

/// A store whose first read of segment 0's `.index` object waits for
/// `release`.
struct Gated {
    store: Rc<MemoryStore>,
    gate: RefCell<Option<oneshot::Receiver<()>>>,
    waiting: Cell<bool>,
}

impl RemoteObjects for Gated {
    fn get_range<'a>(
        &'a self,
        key: &'a str,
        range: Range<u64>,
    ) -> LocalBoxFuture<'a, Result<ObjectRange, ObjectStoreError>> {
        async move {
            let gate = self
                .gate
                .borrow_mut()
                .take_if(|_| key == object_key(0, SegmentObject::Index));
            if let Some(gate) = gate {
                self.waiting.set(true);
                gate.await.unwrap();
            }
            ObjectStore::get_range(&*self.store, key, range).await
        }
        .boxed_local()
    }
}

type InFlight = Pin<Box<dyn Future<Output = PollReadResult>>>;

/// An evicted partition without the local index of segment 0, and a poll of
/// offset 0 that waits for the store to answer its index fetch.
async fn fetching_partition() -> (
    tempfile::TempDir,
    IggyPartition<IggyMessageBus>,
    InFlight,
    oneshot::Sender<()>,
) {
    let (directory, mut partition, store, _local) = Box::pin(uploaded_partition()).await;
    let (release, gate) = oneshot::channel();
    let gated = Rc::new(Gated {
        store,
        gate: RefCell::new(Some(gate)),
        waiting: Cell::new(false),
    });
    attach(&mut partition, gated.clone());
    Box::pin(evict_sealed(&mut partition, directory.path())).await;
    std::fs::remove_file(index_path(directory.path(), 0)).unwrap();
    let mut poll: InFlight = Box::pin(plan(&mut partition, PollingStrategy::offset(0)).execute());
    while !gated.waiting.get() {
        assert!(futures::poll!(poll.as_mut()).is_pending());
        compio::time::sleep(Duration::from_millis(1)).await;
    }
    (directory, partition, poll, release)
}

/// What a later owner of the path keeps there.
const SUCCESSOR_INDEX: &[u8] = b"the index of whatever owns the path now";

/// Let the fetch finish, and check that it left the successor's file alone.
async fn assert_fetch_does_not_publish(
    directory: &Path,
    poll: InFlight,
    release: oneshot::Sender<()>,
) {
    std::fs::write(index_path(directory, 0), SUCCESSOR_INDEX).unwrap();
    release.send(()).unwrap();
    let result = poll.await;

    assert_eq!(result.message_count, 0);
    assert_eq!(result.read_error, Some(IggyError::TransientNotAccepted));
    assert_eq!(
        std::fs::read(index_path(directory, 0)).unwrap(),
        SUCCESSOR_INDEX
    );
}

#[compio::test]
async fn given_an_index_fetch_in_flight_when_the_partition_is_torn_down_should_wait_and_not_publish()
 {
    let (directory, partition, poll, release) = Box::pin(fetching_partition()).await;

    let mut drain = Box::pin(partition.capture_teardown().drain());
    for _ in 0..3 {
        assert!(futures::poll!(drain.as_mut()).is_pending());
        compio::time::sleep(Duration::from_millis(1)).await;
    }
    Box::pin(assert_fetch_does_not_publish(
        directory.path(),
        poll,
        release,
    ))
    .await;
    drain.await.unwrap();
}

#[compio::test]
async fn given_a_plan_of_a_torn_down_partition_when_executing_should_not_fetch_the_index() {
    let (directory, mut partition, store, _local) = Box::pin(uploaded_partition()).await;
    attach(&mut partition, store);
    Box::pin(evict_sealed(&mut partition, directory.path())).await;
    std::fs::remove_file(index_path(directory.path(), 0)).unwrap();
    let planned = plan(&mut partition, PollingStrategy::offset(0));

    Box::pin(partition.capture_teardown().drain())
        .await
        .unwrap();
    let result = planned.execute().await;

    assert_eq!(result.read_error, Some(IggyError::TransientNotAccepted));
    assert!(!index_path(directory.path(), 0).exists());
}

#[compio::test]
async fn given_an_index_fetch_in_flight_when_the_history_is_replaced_should_not_publish() {
    let (directory, mut partition, poll, release) = Box::pin(fetching_partition()).await;

    partition.invalidate_poll_history();

    Box::pin(assert_fetch_does_not_publish(
        directory.path(),
        poll,
        release,
    ))
    .await;
}

#[compio::test]
async fn given_an_index_fetch_in_flight_when_its_segment_retires_should_not_publish() {
    let (directory, mut partition, poll, release) = Box::pin(fetching_partition()).await;
    let first_end = partition.sealed_segment_record(0).unwrap().end_offset;

    Box::pin(partition.remove_sealed_segments_up_to(first_end)).await;
    Box::pin(settle_partition_io(&mut partition, &repair_config())).await;
    assert_eq!(partition.log.segments()[0].start_offset, first_end + 1);
    release.send(()).unwrap();
    let result = poll.await;

    assert_eq!(result.read_error, Some(IggyError::TransientNotAccepted));
    let left: Vec<_> = std::fs::read_dir(directory.path())
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with(&format!("{:020}.", 0)))
        .collect();
    assert_eq!(left, Vec::<String>::new());
}

#[compio::test]
async fn given_a_fetched_index_when_its_segment_retires_should_remove_it() {
    let (directory, mut partition, store, _local) = Box::pin(uploaded_partition()).await;
    attach(&mut partition, store);
    Box::pin(evict_sealed(&mut partition, directory.path())).await;
    // A segment that arrived remote, as a state transfer plants it: no index
    // on disk and none in its storage.
    std::fs::remove_file(index_path(directory.path(), 0)).unwrap();
    partition.log.storages_mut()[0] = SegmentStorage::without_messages(None);
    let (last, _) = Box::pin(polled(&mut partition, PollingStrategy::offset(0))).await;
    assert_eq!(last, Some(RECEIVER_OPS - 1));
    assert!(index_path(directory.path(), 0).exists());
    let first_end = partition.sealed_segment_record(0).unwrap().end_offset;

    Box::pin(partition.remove_sealed_segments_up_to(first_end)).await;
    Box::pin(settle_partition_io(&mut partition, &repair_config())).await;

    assert_eq!(partition.log.segments()[0].start_offset, first_end + 1);
    assert!(!index_path(directory.path(), 0).exists());
}
