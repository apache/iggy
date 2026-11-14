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
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::time::{Duration, Instant};

/// Sealed segments of a partition with [`RECEIVER_OPS`] one-message ops.
const SEALED: [u64; 2] = [0, 2];

const ROOT: &str = "iggy";

pub(super) fn layout() -> KeyLayout {
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

pub(super) async fn polled_all(
    partition: &mut IggyPartition<IggyMessageBus>,
) -> Vec<(Option<u64>, Vec<u8>)> {
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

pub(super) fn attach(
    partition: &mut IggyPartition<IggyMessageBus>,
    objects: Rc<dyn RemoteObjects>,
) {
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

/// The descriptors a plan's segments cache. A walk keeps none for the segments
/// it only passes through, so without them it opens every segment by path.
fn forget_descriptors(plan: &PollPlan) {
    let PollTier::Disk { disk, .. } = &plan.tier else {
        panic!("the poll reads from disk");
    };
    for segment in &disk.segments {
        segment.read_state.fd.borrow_mut().take();
    }
}

#[compio::test]
async fn given_a_segment_evicted_after_planning_when_a_cold_walk_reaches_it_should_read_it_remotely()
 {
    let (directory, mut partition, store, local) = Box::pin(uploaded_partition()).await;
    attach(&mut partition, store);
    // Only the first sealed segment is remote when the polls are planned.
    let first_end = partition.sealed_segment_record(0).unwrap().end_offset;
    Box::pin(evict(&mut partition, first_end, KEEP_NOTHING)).await;
    assert!(
        directory
            .path()
            .join(format!("{:020}.log", SEALED[1]))
            .exists()
    );
    let from_the_first = [0, 1, 3].map(|poll| (strategies()[poll], &local[poll]));
    let planned = from_the_first.map(|(strategy, _)| plan(&mut partition, strategy));
    for plan in &planned {
        assert!(plan.reads_remote());
        forget_descriptors(plan);
    }
    Box::pin(evict_sealed(&mut partition, directory.path())).await;

    for (plan, (_, local)) in planned.into_iter().zip(from_the_first) {
        let result = plan.execute().await;
        assert_eq!(&accepted(&mut partition, result), local);
    }
}

#[compio::test]
async fn given_a_segment_evicted_after_planning_when_a_warm_walk_reaches_it_should_ask_for_a_retry()
{
    let (directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let local = Box::pin(polled(&mut partition, PollingStrategy::offset(0))).await;
    let outage = Rc::new(Outage::default());
    upload(&outage.store, directory.path()).await;
    confirm(&mut partition, &SEALED);
    attach(&mut partition, outage.clone());
    let warm = plan(&mut partition, PollingStrategy::offset(0));
    assert!(!warm.reads_remote());
    forget_descriptors(&warm);
    Box::pin(evict_sealed(&mut partition, directory.path())).await;

    // The owner admitted the poll without a place among the cold ones, so it
    // may not wait on the store.
    let result = warm.execute().await;
    assert_eq!(outage.gets.get(), 0, "a warm poll reached the store");
    assert_eq!(result.message_count, 0);
    assert_eq!(result.read_error, Some(IggyError::TransientNotAccepted));

    // Its retry is planned against the remote segments, and admitted as cold.
    let retry = plan(&mut partition, PollingStrategy::offset(0));
    assert!(retry.reads_remote());
    let result = retry.execute().await;
    assert_eq!(accepted(&mut partition, result), local);
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

/// The result of a poll whose read the store failed for good.
async fn read_failed_for_good(partition: &mut IggyPartition<IggyMessageBus>) -> PollReadResult {
    forget_failures(partition);
    let result = plan(partition, PollingStrategy::offset(0)).execute().await;
    assert_eq!(
        result.tiered_read_failure(),
        Some(IggyError::TieredReadFailed)
    );
    result
}

#[compio::test]
async fn given_a_read_the_store_failed_for_good_when_completing_should_check_its_history_first() {
    let (directory, mut partition, _store, _local) = Box::pin(uploaded_partition()).await;
    attach(
        &mut partition,
        Rc::new(Failing(|key| ObjectStoreError::NotFound {
            key: key.to_owned(),
        })),
    );
    Box::pin(evict_sealed(&mut partition, directory.path())).await;
    // The history the read captured is still served, so its failure stands.
    let current = Box::pin(read_failed_for_good(&mut partition)).await;
    assert_eq!(
        partition.complete_poll(current).err(),
        Some(IggyError::TieredReadFailed)
    );

    // A read of a history that was replaced meanwhile says nothing about the
    // one served now, whose objects may all be there.
    let obsolete = Box::pin(read_failed_for_good(&mut partition)).await;
    partition.invalidate_poll_history();
    assert_eq!(
        partition.complete_poll(obsolete).err(),
        Some(IggyError::TransientNotAccepted)
    );
}

fn forget_failures(partition: &IggyPartition<IggyMessageBus>) {
    for state in partition.log.sealed_read_state() {
        state.remote_failure.set(None);
    }
}

/// A [`MemoryStore`] that counts its reads, fails them all while `failure` is
/// set, never answers a read of a key in `stalled`, and answers a read of the
/// key in `slow` only after its delay.
#[derive(Default)]
struct Outage {
    store: MemoryStore,
    gets: Cell<usize>,
    failure: Cell<Option<StoreFailure>>,
    stalled: RefCell<Vec<String>>,
    slow: RefCell<Option<(String, Duration)>>,
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
            let delay = self
                .slow
                .borrow()
                .as_ref()
                .and_then(|(slow, delay)| (slow == key).then_some(*delay));
            if let Some(delay) = delay {
                compio::time::sleep(delay).await;
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

/// How long the store of [`partition_with_a_slow_index`] takes to answer a
/// read of the index, several times what a poll waits there.
const SLOW_INDEX: Duration = Duration::from_millis(600);

/// [`partition_on_outage`] whose first segment has no local index, on a store
/// that takes [`SLOW_INDEX`] to answer a read of its `.index` object.
async fn partition_with_a_slow_index() -> (
    tempfile::TempDir,
    IggyPartition<IggyMessageBus>,
    Rc<Outage>,
    Vec<(Option<u64>, Vec<u8>)>,
) {
    let (directory, partition, outage, local) = Box::pin(partition_on_outage()).await;
    std::fs::remove_file(index_path(directory.path(), 0)).unwrap();
    *outage.slow.borrow_mut() = Some((object_key(0, SegmentObject::Index), SLOW_INDEX));
    (directory, partition, outage, local)
}

/// A poll of the first segment that waits on the store for a fraction of
/// [`SLOW_INDEX`].
async fn polled_briefly(partition: &mut IggyPartition<IggyMessageBus>) -> PollReadResult {
    plan(partition, PollingStrategy::offset(0))
        .with_deadline(Instant::now() + SLOW_INDEX / 12)
        .execute()
        .await
}

#[compio::test]
async fn given_an_index_slower_than_a_poll_when_polls_retry_should_join_one_fetch_until_it_lands() {
    let (directory, mut partition, outage, local) = Box::pin(partition_with_a_slow_index()).await;

    // No single poll waits long enough for the index, and none of them asks
    // the store for it again.
    for _retry in 0..3 {
        let result = polled_briefly(&mut partition).await;
        assert_eq!(result.read_error, Some(IggyError::TransientNotAccepted));
        assert_eq!(outage.gets.get(), 1);
    }
    assert!(
        partition.log.sealed_read_state()[0]
            .remote_failure
            .get()
            .is_none(),
        "a deadline that ends the wait for a running fetch is no failure of the store"
    );

    compio::time::sleep(SLOW_INDEX).await;
    assert!(index_path(directory.path(), 0).exists());
    assert_eq!(outage.gets.get(), 1);
    let result = polled_briefly(&mut partition).await;
    assert_eq!(accepted(&mut partition, result), local[0]);
}

#[compio::test]
async fn given_a_running_index_fetch_when_the_read_state_is_wiped_should_cancel_it() {
    let (directory, mut partition, outage, _local) = Box::pin(partition_with_a_slow_index()).await;
    let result = polled_briefly(&mut partition).await;
    assert_eq!(result.read_error, Some(IggyError::TransientNotAccepted));
    assert_eq!(outage.gets.get(), 1);

    // An install or a converge recreates the segment files.
    partition.invalidate_poll_history();
    partition.log.invalidate_sealed_read_state();

    compio::time::sleep(SLOW_INDEX * 2).await;
    assert!(!index_path(directory.path(), 0).exists());
}

#[compio::test]
async fn given_an_index_fetch_that_fails_when_a_poll_waits_for_it_should_remember_the_failure() {
    let (directory, mut partition, outage, local) = Box::pin(partition_with_a_slow_index()).await;
    outage.slow.borrow_mut().take();
    outage
        .failure
        .set(Some(|_| ObjectStoreError::Unavailable("503".to_owned())));

    for _retry in 0..3 {
        let result = polled_briefly(&mut partition).await;
        assert_eq!(result.read_error, Some(IggyError::TransientNotAccepted));
    }
    assert_eq!(outage.gets.get(), 1, "the retries reached the store");

    // The fetch that failed is over, so the next poll starts another.
    outage.failure.set(None);
    forget_failures(&partition);
    let result = polled_briefly(&mut partition).await;
    assert_eq!(accepted(&mut partition, result), local[0]);
    assert!(index_path(directory.path(), 0).exists());
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

/// The fetch outlives the polls that started it, so the teardown has to wait
/// for the task itself.
#[compio::test]
async fn given_an_index_fetch_no_poll_waits_for_when_the_partition_is_torn_down_should_wait_for_it()
{
    let (directory, mut partition, outage, _local) = Box::pin(partition_with_a_slow_index()).await;
    let result = polled_briefly(&mut partition).await;
    assert_eq!(result.read_error, Some(IggyError::TransientNotAccepted));
    assert_eq!(outage.gets.get(), 1);

    let mut drain = Box::pin(partition.capture_teardown().drain());
    for _ in 0..3 {
        assert!(futures::poll!(drain.as_mut()).is_pending());
        compio::time::sleep(Duration::from_millis(1)).await;
    }
    drain.await.unwrap();

    assert_eq!(outage.gets.get(), 1);
    assert!(!index_path(directory.path(), 0).exists());
}

#[compio::test]
async fn given_a_poll_waiting_for_an_index_fetch_when_the_history_is_replaced_should_end_at_once() {
    let (directory, mut partition, poll, release) = Box::pin(fetching_partition()).await;

    partition.invalidate_poll_history();

    // The store has not answered, and the poll does not wait for it.
    let result = compio::time::timeout(Duration::from_secs(5), poll)
        .await
        .expect("a cancelled fetch releases its waiters");
    assert_eq!(result.read_error, Some(IggyError::TransientNotAccepted));
    assert!(
        partition.log.sealed_read_state()[0]
            .remote_failure
            .get()
            .is_none(),
        "a cancelled fetch is no failure of the store"
    );

    // The task ends on its own once the store answers, and keeps nothing.
    release.send(()).unwrap();
    Box::pin(partition.capture_teardown().drain())
        .await
        .unwrap();
    let index_name = format!("{:020}.index", 0);
    let kept = std::fs::read_dir(directory.path())
        .unwrap()
        .filter_map(Result::ok)
        .any(|entry| entry.file_name().to_string_lossy().starts_with(&index_name));
    assert!(!kept);
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
