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
use futures::future::LocalBoxFuture;
use iggy_common::{IggyTimestamp, PollingStrategy};
use iggy_object_store::{
    KeyLayout, MemoryStore, ObjectRange, ObjectStore, ObjectStoreError, PutBody, PutOptions,
    SegmentKey, SegmentObject,
};
use std::cell::{Cell, RefCell};
use std::ops::Range;
use std::time::{Duration, Instant};

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
        forget_failures(&partition);
        assert_eq!(
            Box::pin(failed(&mut partition, PollingStrategy::offset(0))).await,
            IggyError::TieredReadFailed
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
    let cases: [(StoreFailure, IggyError); 8] = [
        (
            |_| ObjectStoreError::Throttled("slow down".to_owned()),
            IggyError::TransientNotAccepted,
        ),
        (
            |_| ObjectStoreError::Unavailable("503".to_owned()),
            IggyError::TransientNotAccepted,
        ),
        (
            |_| ObjectStoreError::Transport("reset".to_owned()),
            IggyError::TransientNotAccepted,
        ),
        (
            |key| ObjectStoreError::NotFound {
                key: key.to_owned(),
            },
            IggyError::TieredReadFailed,
        ),
        (
            |_| ObjectStoreError::Auth("denied".to_owned()),
            IggyError::TieredReadFailed,
        ),
        (
            |_| ObjectStoreError::BucketNotFound("iggy".to_owned()),
            IggyError::TieredReadFailed,
        ),
        (
            |key| ObjectStoreError::Archived {
                key: key.to_owned(),
            },
            IggyError::TieredReadFailed,
        ),
        (
            |_| ObjectStoreError::Malformed("xml".to_owned()),
            IggyError::TieredReadFailed,
        ),
    ];
    for (error, expected) in cases {
        let (directory, mut partition, _store, _local) = Box::pin(uploaded_partition()).await;
        attach(&mut partition, Rc::new(Failing(error)));
        Box::pin(evict_sealed(&mut partition, directory.path())).await;

        // Only a failure for good is shown to a poll whose reply has no status.
        let result = plan(&mut partition, PollingStrategy::offset(0))
            .execute()
            .await;
        assert_eq!(
            result.tiered_read_failure(),
            (expected == IggyError::TieredReadFailed).then_some(IggyError::TieredReadFailed)
        );
        forget_failures(&partition);

        assert_eq!(
            Box::pin(failed(&mut partition, PollingStrategy::offset(0))).await,
            expected
        );
    }
}

fn forget_failures(partition: &IggyPartition<IggyMessageBus>) {
    for state in partition.log.sealed_read_state() {
        state.remote_failure.set(None);
    }
}

/// A [`MemoryStore`] that counts its reads, fails them all while `failure` is
/// set, and never answers a read of a key in `stalled`.
#[derive(Default)]
struct Outage {
    store: MemoryStore,
    gets: Cell<usize>,
    failure: Cell<Option<StoreFailure>>,
    stalled: RefCell<Vec<String>>,
}

impl RemoteObjects for Outage {
    fn get_range<'a>(
        &'a self,
        key: &'a str,
        range: Range<u64>,
    ) -> LocalBoxFuture<'a, Result<ObjectRange, ObjectStoreError>> {
        self.gets.set(self.gets.get() + 1);
        async move {
            if self.stalled.borrow().iter().any(|stalled| stalled == key) {
                futures::future::pending::<()>().await;
            }
            match self.failure.get() {
                Some(failure) => Err(failure(key)),
                None => ObjectStore::get_range(&self.store, key, range).await,
            }
        }
        .boxed_local()
    }
}

/// A tiered partition whose sealed segments are evicted to a store that can
/// be told to fail or stall.
async fn partition_on_outage() -> (
    tempfile::TempDir,
    IggyPartition<IggyMessageBus>,
    Rc<Outage>,
    Vec<(Option<u64>, Vec<u8>)>,
) {
    let (directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let local = Box::pin(polled_all(&mut partition)).await;
    let outage = Rc::new(Outage::default());
    upload(&outage.store, directory.path()).await;
    confirm(&mut partition, &SEALED);
    attach(&mut partition, outage.clone());
    Box::pin(evict_sealed(&mut partition, directory.path())).await;
    (directory, partition, outage, local)
}

#[compio::test]
async fn given_a_store_outage_when_clients_retry_should_answer_at_once_without_asking_the_store() {
    let (_directory, mut partition, outage, local) = Box::pin(partition_on_outage()).await;
    outage
        .failure
        .set(Some(|_| ObjectStoreError::Unavailable("503".to_owned())));

    for _retry in 0..5 {
        assert_eq!(
            Box::pin(failed(&mut partition, PollingStrategy::offset(0))).await,
            IggyError::TransientNotAccepted
        );
    }
    assert_eq!(outage.gets.get(), 1, "the retries reached the store");

    // The store is asked again once the failure is forgotten, and a read that
    // works clears it.
    compio::time::sleep(crate::poll_plan::REMOTE_FAILURE_BACKOFF_MIN * 2).await;
    assert_eq!(
        Box::pin(failed(&mut partition, PollingStrategy::offset(0))).await,
        IggyError::TransientNotAccepted
    );
    assert_eq!(outage.gets.get(), 2);
    let remembered = partition.log.sealed_read_state()[0]
        .remote_failure
        .get()
        .unwrap();
    assert_eq!(remembered.failures, 2);
    outage.failure.set(None);
    compio::time::sleep(crate::poll_plan::REMOTE_FAILURE_BACKOFF_MIN * 4).await;
    assert_eq!(
        Box::pin(polled(&mut partition, PollingStrategy::offset(0))).await,
        local[0]
    );
    assert!(
        partition.log.sealed_read_state()[0]
            .remote_failure
            .get()
            .is_none()
    );
}

#[compio::test]
async fn given_a_stalled_store_when_the_deadline_passes_should_return_what_the_poll_has() {
    let (_directory, mut partition, outage, local) = Box::pin(partition_on_outage()).await;
    let deadline = || Instant::now() + Duration::from_millis(50);

    // The second remote segment never answers, so the poll keeps the first.
    outage
        .stalled
        .borrow_mut()
        .push(object_key(2, SegmentObject::Log));
    let started = Instant::now();
    let result = plan(&mut partition, PollingStrategy::offset(0))
        .with_deadline(deadline())
        .execute()
        .await;
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(result.truncated);
    let (last, bytes) = accepted(&mut partition, result);
    assert_eq!(last, Some(1));
    assert!(local[0].1.starts_with(&bytes));

    // With nothing to return, the poll asks for a retry.
    forget_failures(&partition);
    let result = plan(&mut partition, PollingStrategy::offset(2))
        .with_deadline(deadline())
        .execute()
        .await;
    assert_eq!(result.message_count, 0);
    assert_eq!(result.read_error, Some(IggyError::TransientNotAccepted));

    // A deadline that is already past is no failure of the store.
    forget_failures(&partition);
    let gets = outage.gets.get();
    let result = plan(&mut partition, PollingStrategy::offset(2))
        .with_deadline(Instant::now())
        .execute()
        .await;
    assert_eq!(result.read_error, Some(IggyError::TransientNotAccepted));
    assert_eq!(outage.gets.get(), gets);
    assert!(
        partition.log.sealed_read_state()[1]
            .remote_failure
            .get()
            .is_none()
    );
}

#[compio::test]
async fn given_a_plan_when_asked_whether_it_reads_remote_should_follow_its_segments() {
    let (directory, mut partition, store, _local) = Box::pin(uploaded_partition()).await;
    assert!(!plan(&mut partition, PollingStrategy::offset(0)).reads_remote());
    attach(&mut partition, store);
    Box::pin(evict_sealed(&mut partition, directory.path())).await;

    assert!(plan(&mut partition, PollingStrategy::offset(0)).reads_remote());
    assert!(plan(&mut partition, PollingStrategy::offset(3)).reads_remote());
    // The active segment is local, and a poll of it names nothing else.
    assert!(!plan(&mut partition, PollingStrategy::offset(4)).reads_remote());
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
