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
use super::remote_poll::layout;
use super::segment_chain::{PREFIX, tiered_partition};
use super::segment_upload::Recording;
use super::*;
use crate::{
    DeletedIncarnations, ExecutorDeleter, INCARNATION_DELETE_RETRY, MAX_QUEUED_INCARNATIONS,
    ObjectDeleteDone, ObjectDeleteWork, ObjectDeleter, RETIRED_OBJECT_GRACE, SegmentLocation,
    SweptObjects, delete_incarnation, delete_objects, delete_segments, list_segment_starts,
    upload_segment,
};
use iggy_object_store::{
    DeleteFailure, DeleteOutcome, InlineTierExecutor, ListPage, ListRequest, ListedObject,
    MAX_KEYS_PER_REQUEST, MemoryStore, ObjectMeta, ObjectPrefix, ObjectRange, ObjectStore,
    ObjectStoreError, PutBody, PutOptions, SEGMENT_HASH_LEN, SegmentKey, SegmentMetadata,
    SegmentObject, TierJobClass, UserMetadata,
};
use std::collections::BTreeSet;

/// Sealed segments of a partition with [`RECEIVER_OPS`] one-message ops, as
/// `(start offset, end offset)`. The active segment starts at offset 4.
const SEALED: [(u64, u64); 2] = [(0, 1), (2, 3)];

/// What [`Faulty`] does with a delete.
#[derive(Clone, Default, PartialEq, Eq)]
enum Deletes {
    #[default]
    Work,
    /// Reports this key as not deleted, and deletes the others.
    RefuseKey(String),
    /// Fails every call from this one on, counted from zero.
    FailFromCall(usize),
}

/// A memory store whose deletes and HEADs can fail, that keeps the number of
/// keys of every delete call and the key of every HEAD, and that can list
/// keys it does not hold.
#[derive(Default)]
struct Faulty {
    inner: MemoryStore,
    deletes: RefCell<Deletes>,
    delete_calls: RefCell<Vec<usize>>,
    lists_also: RefCell<Vec<String>>,
    /// Whether every listing fails.
    lists_fail: Cell<bool>,
    /// Whether every HEAD fails.
    heads_fail: Cell<bool>,
    head_calls: RefCell<Vec<String>>,
}

impl ObjectStore for Faulty {
    async fn put(
        &self,
        key: &str,
        body: PutBody,
        options: &PutOptions,
    ) -> Result<(), ObjectStoreError> {
        self.inner.put(key, body, options).await
    }

    async fn get_range(
        &self,
        key: &str,
        range: std::ops::Range<u64>,
    ) -> Result<ObjectRange, ObjectStoreError> {
        self.inner.get_range(key, range).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectStoreError> {
        self.head_calls.borrow_mut().push(key.to_owned());
        if self.heads_fail.get() {
            return Err(ObjectStoreError::Unavailable("503".to_owned()));
        }
        self.inner.head(key).await
    }

    async fn delete(&self, keys: &[String]) -> Result<DeleteOutcome, ObjectStoreError> {
        let call = self.delete_calls.borrow().len();
        self.delete_calls.borrow_mut().push(keys.len());
        let deletes = self.deletes.borrow().clone();
        match deletes {
            Deletes::RefuseKey(refused) => {
                let others: Vec<String> = keys
                    .iter()
                    .filter(|key| **key != refused)
                    .cloned()
                    .collect();
                let mut outcome = self.inner.delete(&others).await?;
                if others.len() < keys.len() {
                    outcome.failed.push(DeleteFailure {
                        key: refused,
                        error: ObjectStoreError::Unavailable("503".to_owned()),
                    });
                }
                Ok(outcome)
            }
            Deletes::FailFromCall(first) if call >= first => {
                Err(ObjectStoreError::Unavailable("503".to_owned()))
            }
            Deletes::Work | Deletes::FailFromCall(_) => self.inner.delete(keys).await,
        }
    }

    async fn list(&self, request: &ListRequest<'_>) -> Result<ListPage, ObjectStoreError> {
        if self.lists_fail.get() {
            return Err(ObjectStoreError::Unavailable("503".to_owned()));
        }
        let mut page = self.inner.list(request).await?;
        if page.next.is_none() {
            page.objects
                .extend(self.lists_also.borrow().iter().map(|key| ListedObject {
                    key: key.clone(),
                    size: 1,
                    etag: String::new(),
                    last_modified: std::time::SystemTime::now(),
                }));
        }
        Ok(page)
    }
}

type Deleter = ExecutorDeleter<InlineTierExecutor<Faulty>>;

fn deleter() -> Deleter {
    ExecutorDeleter::new(InlineTierExecutor::new(Faulty::default()), layout())
}

fn store(deleter: &Deleter) -> &Faulty {
    deleter.executor().store()
}

fn key_of(prefix: ObjectPrefix, start_offset: u64, object: SegmentObject) -> String {
    layout().segment_key(&SegmentKey {
        prefix,
        start_offset,
        object,
    })
}

/// Both keys of a segment, in the order a store lists them.
fn keys_of(start_offset: u64) -> [String; 2] {
    [SegmentObject::Index, SegmentObject::Log].map(|object| key_of(PREFIX, start_offset, object))
}

async fn put(store: &Faulty, key: &str) {
    store
        .put(key, PutBody::from_bytes(vec![1]), &PutOptions::default())
        .await
        .unwrap();
}

/// The user metadata of a `.log` object that records these offsets.
fn recording(start_offset: u64, end_offset: u64) -> UserMetadata {
    UserMetadata::from(&SegmentMetadata {
        start_offset,
        end_offset,
        start_timestamp: 0,
        end_timestamp: 0,
        max_timestamp: 0,
        size: 1,
        hash: [0; SEGMENT_HASH_LEN],
    })
}

/// Store a `.log` object of [`PREFIX`] under the key of `start_offset`, and
/// return the key.
async fn put_log(store: &Faulty, start_offset: u64, metadata: UserMetadata) -> String {
    let key = key_of(PREFIX, start_offset, SegmentObject::Log);
    let options = PutOptions {
        metadata,
        ..PutOptions::default()
    };
    store
        .put(&key, PutBody::from_bytes(vec![1]), &options)
        .await
        .unwrap();
    key
}

/// Store both objects of every sealed segment, the way an upload does.
async fn upload_sealed(partition: &mut IggyPartition<IggyMessageBus>, store: &Faulty) {
    while let Some(next) = partition.next_segment_upload() {
        upload_segment(store, &layout(), &next).await.unwrap();
        partition.note_segment_uploaded(&next.record);
    }
    assert_eq!(store.inner.len(), 2 * SEALED.len());
}

/// Enforce a truncation through `up_to_offset`, as the pump does.
async fn truncate(partition: &mut IggyPartition<IggyMessageBus>, up_to_offset: u64) {
    Box::pin(partition.remove_sealed_segments_up_to(up_to_offset)).await;
    Box::pin(settle_partition_io(partition, &repair_config())).await;
    assert_eq!(partition.chain_trim_point(), Some(up_to_offset));
}

fn after(moment: IggyTimestamp, micros: u64) -> IggyTimestamp {
    IggyTimestamp::from(moment.as_micros() + micros)
}

fn grace() -> u64 {
    u64::try_from(RETIRED_OBJECT_GRACE.as_micros()).unwrap()
}

/// A moment by which everything retired so far has waited its grace period.
fn past_the_grace_period() -> IggyTimestamp {
    after(IggyTimestamp::now(), grace())
}

/// One pass: start the next job, run it and hand its result back. Returns
/// what the job did, `None` when the pass started none.
async fn pass(
    partition: &mut IggyPartition<IggyMessageBus>,
    deleter: &dyn ObjectDeleter,
    now: IggyTimestamp,
    committed_up_to: u64,
) -> Option<ObjectDeleteWork> {
    let (job, scope) = partition.begin_object_delete(now, committed_up_to)?;
    let (id, work) = (job.id, job.work.clone());
    let outcome = deleter.run(job, scope).await;
    partition.finish_object_delete(id, outcome, now);
    Some(work)
}

fn delete_of(start_offsets: &[u64]) -> ObjectDeleteWork {
    ObjectDeleteWork::Delete {
        start_offsets: start_offsets.to_vec(),
    }
}

/// The sweep a pass with this watermark starts for the partition as it is.
fn sweep_of(partition: &IggyPartition<IggyMessageBus>, committed_up_to: u64) -> ObjectDeleteWork {
    ObjectDeleteWork::Sweep {
        segment_starts: partition.segment_start_offsets(),
        bound: partition.object_delete_bound(committed_up_to).unwrap(),
    }
}

/// What a sweep reports for objects that record these `(start, end)`
/// offsets.
fn swept_of(segments: &[(u64, u64)]) -> SweptObjects {
    SweptObjects {
        listed: segments
            .iter()
            .map(|(start_offset, _)| *start_offset)
            .collect(),
        end_offsets: segments.iter().copied().collect(),
    }
}

fn queued(partition: &IggyPartition<IggyMessageBus>) -> Vec<u64> {
    partition.object_deletes.retired.start_offsets()
}

/// Make the replica the primary of a later view, as after primacy moved away
/// and came back.
fn regain_primacy(partition: &mut IggyPartition<IggyMessageBus>) {
    loop {
        let view = partition.consensus.view() + 1;
        partition.consensus.set_view(view);
        if partition.consensus.is_primary() {
            break;
        }
    }
    assert!(partition.deletes_objects());
}

#[compio::test]
async fn given_a_removed_segment_when_passes_run_should_delete_its_objects_only_after_the_grace_period()
 {
    let (_directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let deleter = deleter();
    upload_sealed(&mut partition, store(&deleter)).await;
    let before = IggyTimestamp::now();
    truncate(&mut partition, SEALED[0].1).await;
    let committed = SEALED[0].1;

    // The first job of a primary lists, and deletes nothing.
    assert_eq!(
        pass(&mut partition, &deleter, before, committed).await,
        Some(sweep_of(&partition, committed))
    );
    let just_inside = after(before, grace() - 1);
    assert_eq!(
        pass(&mut partition, &deleter, just_inside, committed).await,
        None
    );
    assert_eq!(store(&deleter).inner.len(), 4);

    let later = past_the_grace_period();
    assert_eq!(
        pass(&mut partition, &deleter, later, committed).await,
        Some(delete_of(&[SEALED[0].0]))
    );
    assert_eq!(store(&deleter).inner.keys(), keys_of(SEALED[1].0));
    assert_eq!(queued(&partition), [0_u64; 0]);

    // Nothing is left to do, and a delete that runs again finds nothing.
    assert_eq!(pass(&mut partition, &deleter, later, committed).await, None);
    assert_eq!(
        delete_objects(store(&deleter), &keys_of(SEALED[0].0)).await,
        Vec::<String>::new()
    );
    assert_eq!(store(&deleter).inner.keys(), keys_of(SEALED[1].0));
}

/// The listing finds nothing here, so only the removal itself can have
/// queued the segment.
#[compio::test]
async fn given_a_primary_when_it_removes_a_segment_should_queue_its_objects_without_a_listing() {
    let (_directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let deleter = deleter();
    truncate(&mut partition, SEALED[0].1).await;
    assert_eq!(queued(&partition), [SEALED[0].0]);

    assert_eq!(
        pass(&mut partition, &deleter, IggyTimestamp::now(), SEALED[0].1).await,
        Some(sweep_of(&partition, SEALED[0].1))
    );
    assert_eq!(
        pass(
            &mut partition,
            &deleter,
            past_the_grace_period(),
            SEALED[0].1
        )
        .await,
        Some(delete_of(&[SEALED[0].0]))
    );
}

#[compio::test]
async fn given_a_backup_when_it_removes_a_segment_should_queue_and_delete_nothing() {
    let (_directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let deleter = deleter();
    upload_sealed(&mut partition, store(&deleter)).await;
    let view = partition.consensus.view() + 1;
    partition.consensus.set_view(view);
    assert!(!partition.consensus.is_primary());

    truncate(&mut partition, SEALED[1].1).await;

    assert_eq!(queued(&partition), [0_u64; 0]);
    assert_eq!(
        pass(
            &mut partition,
            &deleter,
            past_the_grace_period(),
            SEALED[1].1
        )
        .await,
        None
    );
    assert_eq!(store(&deleter).inner.len(), 4);
    assert!(store(&deleter).delete_calls.borrow().is_empty());
}

#[compio::test]
async fn given_a_delete_that_fails_when_it_finishes_should_keep_its_segments_queued() {
    let (_directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let deleter = deleter();
    upload_sealed(&mut partition, store(&deleter)).await;
    truncate(&mut partition, SEALED[1].1).await;
    let committed = SEALED[1].1;
    pass(&mut partition, &deleter, IggyTimestamp::now(), committed).await;
    let later = past_the_grace_period();
    let both = [SEALED[0].0, SEALED[1].0];

    // The store takes no call at all.
    store(&deleter).deletes.replace(Deletes::FailFromCall(0));
    assert_eq!(
        pass(&mut partition, &deleter, later, committed).await,
        Some(delete_of(&both))
    );
    assert_eq!(queued(&partition), both);
    assert_eq!(store(&deleter).inner.len(), 4);

    // The store refuses the `.log` of the second segment.
    let refused = key_of(PREFIX, SEALED[1].0, SegmentObject::Log);
    store(&deleter)
        .deletes
        .replace(Deletes::RefuseKey(refused.clone()));
    assert_eq!(
        pass(&mut partition, &deleter, later, committed).await,
        Some(delete_of(&both))
    );
    assert_eq!(queued(&partition), [SEALED[1].0]);
    assert_eq!(store(&deleter).inner.keys(), [refused]);

    store(&deleter).deletes.replace(Deletes::Work);
    assert_eq!(
        pass(&mut partition, &deleter, later, committed).await,
        Some(delete_of(&[SEALED[1].0]))
    );
    assert_eq!(queued(&partition), [0_u64; 0]);
    assert!(store(&deleter).inner.is_empty());
}

/// The next primary starts without the queue and finds a segment only by its
/// `.log` object, so a delete that got through half of a segment has to leave
/// that one.
#[compio::test]
async fn given_a_refused_index_delete_when_primacy_moves_should_leave_the_log_for_the_next_primary_to_finish()
 {
    let (_directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let deleter = deleter();
    upload_sealed(&mut partition, store(&deleter)).await;
    truncate(&mut partition, SEALED[1].1).await;
    let committed = SEALED[1].1;
    pass(&mut partition, &deleter, IggyTimestamp::now(), committed).await;

    let refused = key_of(PREFIX, SEALED[1].0, SegmentObject::Index);
    store(&deleter).deletes.replace(Deletes::RefuseKey(refused));
    assert_eq!(
        pass(&mut partition, &deleter, past_the_grace_period(), committed).await,
        Some(delete_of(&[SEALED[0].0, SEALED[1].0]))
    );
    assert_eq!(store(&deleter).inner.keys(), keys_of(SEALED[1].0));

    regain_primacy(&mut partition);
    store(&deleter).deletes.replace(Deletes::Work);
    assert_eq!(
        pass(&mut partition, &deleter, IggyTimestamp::now(), committed).await,
        Some(sweep_of(&partition, committed))
    );
    assert_eq!(
        pass(&mut partition, &deleter, past_the_grace_period(), committed).await,
        Some(delete_of(&[SEALED[1].0]))
    );
    assert!(store(&deleter).inner.is_empty());
}

/// Every `.index` goes before any `.log`, and a segment whose `.index` the
/// store kept is not asked for its `.log`.
#[compio::test]
async fn given_segments_to_delete_when_deleting_should_send_every_index_before_the_logs_of_the_deleted_ones()
 {
    let store = Faulty::default();
    for start_offset in [0, 10, 20] {
        for key in keys_of(start_offset) {
            put(&store, &key).await;
        }
    }
    store
        .deletes
        .replace(Deletes::RefuseKey(key_of(PREFIX, 10, SegmentObject::Index)));

    assert_eq!(
        delete_segments(&store, &layout(), &PREFIX, &[0, 10, 20]).await,
        [10]
    );

    assert_eq!(store.inner.keys(), keys_of(10));
    assert_eq!(store.delete_calls.take(), [3, 2]);

    // A store that takes no call is not asked for a single `.log`.
    store.deletes.replace(Deletes::FailFromCall(0));
    assert_eq!(
        delete_segments(&store, &layout(), &PREFIX, &[10, 20]).await,
        [10, 20]
    );
    assert_eq!(store.delete_calls.take(), [2]);
}

#[compio::test]
async fn given_more_keys_than_one_call_takes_when_deleting_should_split_them_and_report_the_unsent()
{
    let keys: Vec<String> = (0..2 * MAX_KEYS_PER_REQUEST as u64 + 500)
        .map(|start_offset| key_of(PREFIX, start_offset, SegmentObject::Log))
        .collect();
    let store = Faulty::default();
    for key in &keys[..3] {
        put(&store, key).await;
    }

    assert_eq!(delete_objects(&store, &keys).await, Vec::<String>::new());
    assert_eq!(
        store.delete_calls.take(),
        [MAX_KEYS_PER_REQUEST, MAX_KEYS_PER_REQUEST, 500]
    );
    assert!(store.inner.is_empty());

    // The second call fails, so the third is not sent, and the keys of both
    // are the ones to send again.
    store.deletes.replace(Deletes::FailFromCall(1));
    assert_eq!(
        delete_objects(&store, &keys).await,
        keys[MAX_KEYS_PER_REQUEST..]
    );
    assert_eq!(
        store.delete_calls.take(),
        [MAX_KEYS_PER_REQUEST, MAX_KEYS_PER_REQUEST]
    );
}

#[compio::test]
async fn given_more_objects_than_one_page_when_listing_should_follow_every_page() {
    let store = Faulty::default();
    let segments = MAX_KEYS_PER_REQUEST as u64 / 2 + 100;
    for start_offset in 0..segments {
        for key in keys_of(start_offset * 10) {
            put(&store, &key).await;
        }
    }

    let listed = list_segment_starts(&store, &layout(), &PREFIX)
        .await
        .unwrap();

    assert_eq!(
        listed,
        (0..segments)
            .map(|segment| segment * 10)
            .collect::<BTreeSet<_>>()
    );
}

#[compio::test]
async fn given_foreign_and_unparsable_keys_when_listing_should_leave_them_out() {
    let store = Faulty::default();
    let other_incarnation = ObjectPrefix {
        created_revision: PREFIX.created_revision + 1,
        ..PREFIX
    };
    let incarnation = layout().incarnation_prefix(&PREFIX);
    let strangers = [
        format!("{incarnation}00000000000000000007.tmp"),
        format!("{incarnation}8.log"),
        format!("{incarnation}nested/00000000000000000009.log"),
        format!("{incarnation}manifest.bin"),
    ];
    for key in keys_of(2).iter().chain(&strangers) {
        put(&store, key).await;
    }
    // A store that answers with a key of another incarnation.
    store
        .lists_also
        .borrow_mut()
        .push(key_of(other_incarnation, 5, SegmentObject::Log));

    let listed = list_segment_starts(&store, &layout(), &PREFIX)
        .await
        .unwrap();

    assert_eq!(listed, BTreeSet::from([2]));
}

/// A primary that starts without a queue: both sealed segments were removed
/// and uploaded before it took over.
async fn new_primary_after_removals(
    committed_up_to: u64,
) -> (
    tempfile::TempDir,
    IggyPartition<IggyMessageBus>,
    Deleter,
    Option<ObjectDeleteWork>,
) {
    let (directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let deleter = deleter();
    upload_sealed(&mut partition, store(&deleter)).await;
    truncate(&mut partition, SEALED[1].1).await;
    regain_primacy(&mut partition);
    let swept = pass(
        &mut partition,
        &deleter,
        IggyTimestamp::now(),
        committed_up_to,
    )
    .await;
    (directory, partition, deleter, swept)
}

#[compio::test]
async fn given_a_new_primary_when_it_sweeps_should_delete_the_objects_below_both_bounds_after_the_grace_period()
 {
    let (_directory, mut partition, deleter, swept) =
        Box::pin(new_primary_after_removals(SEALED[1].1)).await;
    assert_eq!(swept, Some(sweep_of(&partition, SEALED[1].1)));
    assert_eq!(queued(&partition), [SEALED[0].0, SEALED[1].0]);
    let swept_at = IggyTimestamp::now();

    assert_eq!(
        pass(
            &mut partition,
            &deleter,
            after(swept_at, grace() / 2),
            SEALED[1].1
        )
        .await,
        None,
        "the grace period counts from the sweep"
    );
    assert_eq!(store(&deleter).inner.len(), 4);
    assert_eq!(
        pass(
            &mut partition,
            &deleter,
            past_the_grace_period(),
            SEALED[1].1
        )
        .await,
        Some(delete_of(&[SEALED[0].0, SEALED[1].0]))
    );
    assert!(store(&deleter).inner.is_empty());
}

/// This replica trimmed both segments, but metadata committed the removal of
/// the first one only.
#[compio::test]
async fn given_a_committed_watermark_below_the_trim_point_when_sweeping_should_keep_what_lies_above_it()
 {
    let (_directory, mut partition, deleter, _swept) =
        Box::pin(new_primary_after_removals(SEALED[0].1)).await;
    assert_eq!(queued(&partition), [SEALED[0].0]);

    // A watermark that moved on does not bring the listing back.
    let later = past_the_grace_period();
    assert_eq!(
        pass(&mut partition, &deleter, later, SEALED[1].1).await,
        Some(delete_of(&[SEALED[0].0]))
    );
    assert_eq!(
        pass(&mut partition, &deleter, later, SEALED[1].1).await,
        None
    );
    assert_eq!(store(&deleter).inner.keys(), keys_of(SEALED[1].0));
}

/// The second segment left this replica's list without a trim, as when its
/// front moved for another reason than its own retention.
#[compio::test]
async fn given_an_object_below_the_front_and_above_the_trim_point_when_sweeping_should_keep_it() {
    let (_directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let deleter = deleter();
    upload_sealed(&mut partition, store(&deleter)).await;
    truncate(&mut partition, SEALED[0].1).await;
    partition.log.retire_front().unwrap();
    assert_eq!(partition.log.segments()[0].start_offset, SEALED[1].1 + 1);
    regain_primacy(&mut partition);

    let committed = SEALED[1].1;
    assert_eq!(
        pass(&mut partition, &deleter, IggyTimestamp::now(), committed).await,
        Some(sweep_of(&partition, committed))
    );
    assert_eq!(queued(&partition), [SEALED[0].0]);
    let later = past_the_grace_period();
    pass(&mut partition, &deleter, later, committed).await;
    assert_eq!(store(&deleter).inner.keys(), keys_of(SEALED[1].0));
}

#[compio::test]
async fn given_no_committed_watermark_or_no_trim_point_when_a_pass_runs_should_start_no_job() {
    let (_directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let deleter = deleter();
    upload_sealed(&mut partition, store(&deleter)).await;
    let later = past_the_grace_period();

    assert_eq!(partition.chain_trim_point(), None);
    assert_eq!(pass(&mut partition, &deleter, later, u64::MAX).await, None);

    truncate(&mut partition, SEALED[0].1).await;
    assert_eq!(pass(&mut partition, &deleter, later, 0).await, None);
    assert_eq!(store(&deleter).inner.len(), 4);
}

#[compio::test]
async fn given_stray_objects_when_a_primary_sweeps_should_delete_only_the_proven_ones() {
    let (_directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let deleter = deleter();
    upload_sealed(&mut partition, store(&deleter)).await;
    truncate(&mut partition, SEALED[1].1).await;
    regain_primacy(&mut partition);
    let active = partition.log.active_segment().start_offset;
    let incarnation = layout().incarnation_prefix(&PREFIX);
    let other_incarnation = ObjectPrefix {
        created_revision: PREFIX.created_revision + 1,
        ..PREFIX
    };
    let kept = [
        // Nothing known starts after it, so its end is unknown.
        key_of(PREFIX, active + 6, SegmentObject::Log),
        // The partition still lists a segment at its first offset.
        key_of(PREFIX, active, SegmentObject::Log),
        format!("{incarnation}00000000000000000001.tmp"),
        key_of(other_incarnation, 0, SegmentObject::Log),
    ];
    for key in &kept {
        put(store(&deleter), key).await;
    }

    let committed = u64::MAX;
    pass(&mut partition, &deleter, IggyTimestamp::now(), committed).await;
    assert_eq!(queued(&partition), [SEALED[0].0, SEALED[1].0]);
    pass(&mut partition, &deleter, past_the_grace_period(), committed).await;

    let mut left = kept.to_vec();
    left.sort();
    assert_eq!(store(&deleter).inner.keys(), left);
}

/// A primary that starts without a queue and with an empty store: both sealed
/// segments were removed before it took over, and nothing was uploaded.
async fn new_primary_over_an_empty_store()
-> (tempfile::TempDir, IggyPartition<IggyMessageBus>, Deleter) {
    let (directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let deleter = deleter();
    truncate(&mut partition, SEALED[1].1).await;
    regain_primacy(&mut partition);
    (directory, partition, deleter)
}

/// Sweep with a watermark at the end of the first sealed segment, then run a
/// pass after the grace period, and return what that pass did.
async fn sweep_then_delete(
    partition: &mut IggyPartition<IggyMessageBus>,
    deleter: &Deleter,
) -> Option<ObjectDeleteWork> {
    let committed = SEALED[0].1;
    assert_eq!(
        pass(partition, deleter, IggyTimestamp::now(), committed).await,
        Some(sweep_of(partition, committed))
    );
    pass(partition, deleter, past_the_grace_period(), committed).await
}

#[compio::test]
async fn given_an_object_that_records_an_end_at_the_bound_when_a_primary_sweeps_should_queue_and_delete_it()
 {
    let (_directory, mut partition, deleter) = Box::pin(new_primary_over_an_empty_store()).await;
    let first = put_log(
        store(&deleter),
        SEALED[0].0,
        recording(SEALED[0].0, SEALED[0].1),
    )
    .await;
    let second = put_log(
        store(&deleter),
        SEALED[1].0,
        recording(SEALED[1].0, SEALED[1].1),
    )
    .await;

    assert_eq!(
        sweep_then_delete(&mut partition, &deleter).await,
        Some(delete_of(&[SEALED[0].0]))
    );

    assert_eq!(store(&deleter).inner.keys(), [second]);
    // The second segment can end above the watermark, so nothing asked it.
    assert_eq!(store(&deleter).head_calls.take(), [first]);
}

/// Two primaries that sealed at different offsets: the object at the first
/// start holds the offsets of both sealed segments, and the next start says
/// it ends below the watermark.
#[compio::test]
async fn given_an_object_that_records_an_end_above_the_bound_when_a_primary_sweeps_should_keep_it_despite_the_next_start()
 {
    let (_directory, mut partition, deleter) = Box::pin(new_primary_over_an_empty_store()).await;
    let wide = put_log(
        store(&deleter),
        SEALED[0].0,
        recording(SEALED[0].0, SEALED[1].1),
    )
    .await;
    let next = put_log(
        store(&deleter),
        SEALED[1].0,
        recording(SEALED[1].0, SEALED[1].1),
    )
    .await;

    assert_eq!(sweep_then_delete(&mut partition, &deleter).await, None);

    assert_eq!(queued(&partition), [0_u64; 0]);
    assert_eq!(store(&deleter).inner.keys(), [wide.clone(), next]);
    assert_eq!(store(&deleter).head_calls.take(), [wide]);
    assert!(store(&deleter).delete_calls.borrow().is_empty());
}

#[compio::test]
async fn given_an_object_that_records_another_start_than_its_key_when_a_primary_sweeps_should_keep_it()
 {
    let (_directory, mut partition, deleter) = Box::pin(new_primary_over_an_empty_store()).await;
    let stray = put_log(store(&deleter), SEALED[0].0, recording(1, SEALED[0].1)).await;
    let next = put_log(
        store(&deleter),
        SEALED[1].0,
        recording(SEALED[1].0, SEALED[1].1),
    )
    .await;

    assert_eq!(sweep_then_delete(&mut partition, &deleter).await, None);

    assert_eq!(queued(&partition), [0_u64; 0]);
    assert_eq!(store(&deleter).inner.keys(), [stray.clone(), next]);
    assert_eq!(store(&deleter).head_calls.take(), [stray]);
}

#[compio::test]
async fn given_a_log_object_without_decodable_metadata_when_a_primary_sweeps_should_keep_it() {
    let (_directory, mut partition, deleter) = Box::pin(new_primary_over_an_empty_store()).await;
    let mut unversioned = recording(SEALED[0].0, SEALED[0].1);
    unversioned.remove("metadata-version").unwrap();
    let bare = put_log(store(&deleter), SEALED[0].0, unversioned).await;
    let next = put_log(
        store(&deleter),
        SEALED[1].0,
        recording(SEALED[1].0, SEALED[1].1),
    )
    .await;

    assert_eq!(sweep_then_delete(&mut partition, &deleter).await, None);

    assert_eq!(queued(&partition), [0_u64; 0]);
    assert_eq!(store(&deleter).inner.keys(), [bare.clone(), next]);
    assert_eq!(store(&deleter).head_calls.take(), [bare]);
}

#[compio::test]
async fn given_an_index_without_its_log_when_a_primary_sweeps_should_keep_it_and_finish_the_sweep()
{
    let (_directory, mut partition, deleter) = Box::pin(new_primary_over_an_empty_store()).await;
    let index = key_of(PREFIX, SEALED[0].0, SegmentObject::Index);
    put(store(&deleter), &index).await;
    let next = put_log(
        store(&deleter),
        SEALED[1].0,
        recording(SEALED[1].0, SEALED[1].1),
    )
    .await;

    assert_eq!(sweep_then_delete(&mut partition, &deleter).await, None);

    assert!(
        partition.object_deletes.swept,
        "a missing object is an answer"
    );
    assert_eq!(queued(&partition), [0_u64; 0]);
    assert_eq!(store(&deleter).inner.keys(), [index, next]);
    assert_eq!(
        store(&deleter).head_calls.take(),
        [key_of(PREFIX, SEALED[0].0, SegmentObject::Log)]
    );
}

#[compio::test]
async fn given_a_head_that_fails_when_a_primary_sweeps_should_delete_nothing_and_sweep_again_on_the_next_pass()
 {
    let (_directory, mut partition, deleter) = Box::pin(new_primary_over_an_empty_store()).await;
    let first = put_log(
        store(&deleter),
        SEALED[0].0,
        recording(SEALED[0].0, SEALED[0].1),
    )
    .await;
    let second = put_log(
        store(&deleter),
        SEALED[1].0,
        recording(SEALED[1].0, SEALED[1].1),
    )
    .await;
    let committed = SEALED[0].1;
    let now = IggyTimestamp::now();

    store(&deleter).heads_fail.set(true);
    assert_eq!(
        pass(&mut partition, &deleter, now, committed).await,
        Some(sweep_of(&partition, committed))
    );
    assert!(!partition.object_deletes.swept);
    assert_eq!(queued(&partition), [0_u64; 0]);
    assert_eq!(
        store(&deleter).inner.keys(),
        [first.clone(), second.clone()]
    );
    assert!(store(&deleter).delete_calls.borrow().is_empty());

    store(&deleter).heads_fail.set(false);
    assert_eq!(
        pass(&mut partition, &deleter, now, committed).await,
        Some(sweep_of(&partition, committed))
    );
    assert_eq!(queued(&partition), [SEALED[0].0]);
    assert_eq!(
        pass(&mut partition, &deleter, past_the_grace_period(), committed).await,
        Some(delete_of(&[SEALED[0].0]))
    );
    assert_eq!(store(&deleter).inner.keys(), [second]);
}

#[compio::test]
async fn given_a_primary_that_swept_when_passes_repeat_should_list_once_until_primacy_moves() {
    let (_directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let deleter = deleter();
    truncate(&mut partition, SEALED[0].1).await;
    let committed = SEALED[0].1;
    let now = IggyTimestamp::now();

    assert_eq!(
        pass(&mut partition, &deleter, now, committed).await,
        Some(sweep_of(&partition, committed))
    );
    assert_eq!(pass(&mut partition, &deleter, now, committed).await, None);

    regain_primacy(&mut partition);
    assert_eq!(
        pass(&mut partition, &deleter, now, committed).await,
        Some(sweep_of(&partition, committed))
    );
    assert_eq!(pass(&mut partition, &deleter, now, committed).await, None);
}

#[compio::test]
async fn given_a_listing_that_fails_when_it_finishes_should_list_again_on_the_next_pass() {
    let (_directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    truncate(&mut partition, SEALED[0].1).await;
    let committed = SEALED[0].1;
    let now = IggyTimestamp::now();

    let (job, _scope) = partition.begin_object_delete(now, committed).unwrap();
    assert_eq!(job.work, sweep_of(&partition, committed));
    partition.finish_object_delete(
        job.id,
        Ok(ObjectDeleteDone::Swept(Err(ObjectStoreError::Unavailable(
            "503".to_owned(),
        )))),
        now,
    );

    let (job, _scope) = partition.begin_object_delete(now, committed).unwrap();
    assert_eq!(job.work, sweep_of(&partition, committed));
}

#[compio::test]
async fn given_an_upload_that_lands_after_its_segment_was_removed_when_it_finishes_should_not_advance_and_retire_its_objects()
 {
    let (_directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let deleter = deleter();
    let (upload, _scope, attempt) = partition.begin_segment_upload().unwrap();
    assert_eq!(upload.record.start_offset, SEALED[0].0);
    upload_segment(store(&deleter), &layout(), &upload)
        .await
        .unwrap();
    truncate(&mut partition, SEALED[0].1).await;
    let committed = SEALED[0].1;
    assert_eq!(
        pass(&mut partition, &deleter, IggyTimestamp::now(), committed).await,
        Some(sweep_of(&partition, committed))
    );
    compio::time::sleep(std::time::Duration::from_millis(5)).await;
    let landed = IggyTimestamp::now();

    assert_eq!(
        partition.finish_segment_upload(attempt, &upload.record, &Ok(Ok(()))),
        None,
        "no advance over a segment that is gone"
    );

    assert_eq!(partition.tiered_floor(), None);
    assert_eq!(queued(&partition), [SEALED[0].0]);
    // The removal is older than this, and the landing is not.
    assert_eq!(
        pass(
            &mut partition,
            &deleter,
            after(landed, grace() - 1),
            committed
        )
        .await,
        None
    );
    assert_eq!(store(&deleter).inner.keys(), keys_of(SEALED[0].0));
    assert_eq!(
        pass(&mut partition, &deleter, past_the_grace_period(), committed).await,
        Some(delete_of(&[SEALED[0].0]))
    );
    assert!(store(&deleter).inner.is_empty());
    // The next upload is the next segment, as if the first never ran. Its
    // advance reaches over the removed one, so it waits for a pass that
    // brings the watermark of that truncation.
    assert!(partition.begin_segment_upload().is_none());
    partition.note_committed_truncation(committed);
    let (next, _scope, _attempt) = partition.begin_segment_upload().unwrap();
    assert_eq!(next.record.start_offset, SEALED[1].0);
}

#[compio::test]
async fn given_segments_still_in_the_list_when_passes_run_should_never_delete_their_objects() {
    // Sealed segments at 0, 2 and 4, and the active one at 6.
    let (_directory, mut partition) = Box::pin(tiered_partition(&segment_records(7), 0)).await;
    let deleter = deleter();
    while let Some(next) = partition.next_segment_upload() {
        upload_segment(store(&deleter), &layout(), &next)
            .await
            .unwrap();
        partition.note_segment_uploaded(&next.record);
    }
    // The `.log` of the first two leaves the disk, and the third stays.
    confirm(&mut partition, &[0, 2]);
    Box::pin(evict(&mut partition, 3, KEEP_NOTHING)).await;
    truncate(&mut partition, 1).await;
    assert_eq!(
        partition
            .log
            .segments()
            .iter()
            .map(|segment| (segment.start_offset, segment.location))
            .collect::<Vec<_>>(),
        [
            (2, SegmentLocation::Remote),
            (4, SegmentLocation::Uploaded),
            (6, SegmentLocation::Local)
        ]
    );

    let committed = u64::MAX;
    assert_eq!(
        pass(&mut partition, &deleter, IggyTimestamp::now(), committed).await,
        Some(sweep_of(&partition, committed))
    );
    assert_eq!(queued(&partition), [0]);
    // The partition never queues a segment it lists. Even if it had, the
    // delete leaves the segment out.
    let long_ago = IggyTimestamp::from(1);
    for (start_offset, end_offset) in [(2, 3), (4, 5)] {
        partition
            .object_deletes
            .retired
            .retire(start_offset, end_offset, long_ago);
    }
    assert_eq!(
        pass(&mut partition, &deleter, past_the_grace_period(), committed).await,
        Some(delete_of(&[0]))
    );

    let mut left = [keys_of(2), keys_of(4)].concat();
    left.sort();
    assert_eq!(store(&deleter).inner.keys(), left);
}

#[compio::test]
async fn given_a_job_in_flight_when_passes_run_should_start_no_other_and_cancel_it_on_demotion() {
    let (_directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    truncate(&mut partition, SEALED[0].1).await;
    let committed = SEALED[0].1;
    let now = IggyTimestamp::now();

    let (job, scope) = partition.begin_object_delete(now, committed).unwrap();
    assert!(
        partition.begin_object_delete(now, committed).is_none(),
        "one job at a time"
    );
    // A result of another job is not the one this partition waits for.
    partition.finish_object_delete(
        job.id + 1,
        Ok(ObjectDeleteDone::Swept(Ok(SweptObjects::default()))),
        now,
    );
    assert!(partition.begin_object_delete(now, committed).is_none());
    assert!(!partition.object_deletes.swept);

    // The next view's primary is another replica.
    let view = partition.consensus.view() + 1;
    partition.consensus.set_view(view);
    assert!(partition.begin_object_delete(now, committed).is_none());
    assert!(
        scope.is_cancelled(),
        "the pass stops the demoted replica's job"
    );
    assert_eq!(queued(&partition), [0_u64; 0]);

    // The job's result arrives after the replica is the primary again.
    regain_primacy(&mut partition);
    partition.finish_object_delete(
        job.id,
        Ok(ObjectDeleteDone::Swept(Ok(SweptObjects::default()))),
        now,
    );
    let (again, _scope) = partition.begin_object_delete(now, committed).unwrap();
    assert_eq!(
        again.work,
        sweep_of(&partition, committed),
        "the stale listing did not count as this view's"
    );
}

/// A result that arrives after primacy moved, with no pass in between.
#[compio::test]
async fn given_a_listing_that_ends_after_primacy_moved_when_it_finishes_should_queue_nothing() {
    let (_directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    truncate(&mut partition, SEALED[1].1).await;
    regain_primacy(&mut partition);
    let now = IggyTimestamp::now();
    let (job, _scope) = partition.begin_object_delete(now, SEALED[1].1).unwrap();
    let listed = Ok(ObjectDeleteDone::Swept(Ok(swept_of(&SEALED))));

    let view = partition.consensus.view() + 1;
    partition.consensus.set_view(view);
    partition.finish_object_delete(job.id, listed, now);

    assert_eq!(queued(&partition), [0_u64; 0]);
    assert!(!partition.object_deletes.swept);
}

/// Primacy moved away and came back with no pass in between, so nothing
/// dropped the job of the earlier view.
#[compio::test]
async fn given_a_listing_of_an_earlier_view_when_it_finishes_should_queue_nothing() {
    let (_directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    truncate(&mut partition, SEALED[1].1).await;
    regain_primacy(&mut partition);
    let now = IggyTimestamp::now();
    let (job, _scope) = partition.begin_object_delete(now, SEALED[1].1).unwrap();

    regain_primacy(&mut partition);
    partition.finish_object_delete(
        job.id,
        Ok(ObjectDeleteDone::Swept(Ok(swept_of(&SEALED)))),
        now,
    );

    assert_eq!(queued(&partition), [0_u64; 0]);
    assert!(!partition.object_deletes.swept);
}

#[compio::test]
async fn given_a_primary_in_a_state_transfer_when_passes_run_should_queue_and_start_nothing() {
    let (_directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    truncate(&mut partition, SEALED[0].1).await;
    let now = IggyTimestamp::now();
    let (job, scope) = partition.begin_object_delete(now, SEALED[1].1).unwrap();

    partition.consensus.begin_state_transfer_await();
    assert!(partition.consensus.is_primary());

    // The result of the job it started before the transfer settles nothing.
    partition.finish_object_delete(
        job.id,
        Ok(ObjectDeleteDone::Swept(Ok(swept_of(&SEALED[..1])))),
        now,
    );
    assert_eq!(queued(&partition), [0_u64; 0]);
    assert!(scope.is_cancelled());

    truncate(&mut partition, SEALED[1].1).await;
    assert_eq!(queued(&partition), [0_u64; 0]);
    assert!(
        partition
            .begin_object_delete(past_the_grace_period(), SEALED[1].1)
            .is_none()
    );
}

#[compio::test]
async fn given_a_partition_without_a_chain_when_it_removes_a_segment_should_queue_nothing() {
    let (_directory, mut partition) = Box::pin(partition_with_sealed_segments(&segment_records(
        RECEIVER_OPS,
    )))
    .await;
    assert!(partition.consensus.is_primary());

    Box::pin(partition.remove_sealed_segments_up_to(SEALED[0].1)).await;
    Box::pin(settle_partition_io(&mut partition, &repair_config())).await;

    assert_eq!(partition.log.segments()[0].start_offset, SEALED[1].0);
    assert_eq!(queued(&partition), [0_u64; 0]);
    assert!(
        partition
            .begin_object_delete(past_the_grace_period(), u64::MAX)
            .is_none()
    );
}

/// The executor caps deletes apart from uploads and reads, so each job has
/// to name its class.
#[compio::test]
async fn given_delete_jobs_when_submitted_should_name_the_housekeeping_and_the_delete_class() {
    let (_directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    truncate(&mut partition, SEALED[0].1).await;
    let deleter = ExecutorDeleter::new(
        Recording {
            store: Rc::new(MemoryStore::new()),
            requests: RefCell::default(),
        },
        layout(),
    );

    let committed = SEALED[0].1;
    pass(&mut partition, &deleter, IggyTimestamp::now(), committed).await;
    pass(&mut partition, &deleter, past_the_grace_period(), committed).await;

    assert_eq!(
        deleter.executor().requests.take(),
        [(TierJobClass::Housekeeping, 0), (TierJobClass::Delete, 0)]
    );
}

/// Another incarnation of the ids of [`PREFIX`], as a topic recreated into
/// the same ids has.
const RECREATED: ObjectPrefix = ObjectPrefix {
    created_revision: PREFIX.created_revision + 1,
    ..PREFIX
};

fn retry() -> u64 {
    u64::try_from(INCARNATION_DELETE_RETRY.as_micros()).unwrap()
}

/// Store both objects of the segments at `start_offsets` and the manifest of
/// `prefix`, and return their keys in the order a store lists them.
async fn store_incarnation(
    store: &Faulty,
    prefix: ObjectPrefix,
    start_offsets: &[u64],
) -> Vec<String> {
    let mut keys = vec![layout().manifest_key(&prefix)];
    for start_offset in start_offsets {
        for object in SegmentObject::ALL {
            keys.push(key_of(prefix, *start_offset, object));
        }
    }
    for key in &keys {
        put(store, key).await;
    }
    keys.sort();
    keys
}

/// One pass over the deleted incarnations: start the next job, run it and
/// settle it. Returns the incarnation the job was for, `None` when the pass
/// started none.
async fn incarnation_pass(
    queue: &mut DeletedIncarnations,
    deleter: &dyn ObjectDeleter,
    now: IggyTimestamp,
) -> Option<ObjectPrefix> {
    let (job, scope) = queue.begin(now, |_| false)?;
    assert_eq!(job.work, ObjectDeleteWork::DeleteIncarnation);
    let (id, prefix) = (job.id, job.prefix);
    let outcome = deleter.run(job, scope).await;
    queue.finish(id, outcome, now);
    Some(prefix)
}

#[compio::test]
async fn given_a_deleted_incarnation_when_passes_run_should_delete_its_objects_and_manifest_only_after_the_grace_period()
 {
    let deleter = deleter();
    let stored = store_incarnation(store(&deleter), PREFIX, &[0, 10, 20]).await;
    let mut queue = DeletedIncarnations::default();
    let observed = IggyTimestamp::from(1_000);
    assert!(queue.queue(PREFIX, observed));

    let just_inside = after(observed, grace() - 1);
    assert_eq!(
        incarnation_pass(&mut queue, &deleter, just_inside).await,
        None
    );
    assert_eq!(store(&deleter).inner.keys(), stored);
    assert!(store(&deleter).delete_calls.borrow().is_empty());

    let due = after(observed, grace());
    assert_eq!(
        incarnation_pass(&mut queue, &deleter, due).await,
        Some(PREFIX)
    );
    assert!(store(&deleter).inner.is_empty());
    assert!(queue.is_empty());

    // Nothing is left to do, and a delete that runs again finds nothing.
    assert_eq!(incarnation_pass(&mut queue, &deleter, due).await, None);
    assert_eq!(
        delete_incarnation(store(&deleter), &layout(), &PREFIX)
            .await
            .unwrap(),
        0
    );
}

#[compio::test]
async fn given_an_incarnation_queued_twice_when_asking_what_is_due_should_wait_from_the_first_moment()
 {
    let deleter = deleter();
    let mut queue = DeletedIncarnations::default();
    queue.queue(PREFIX, IggyTimestamp::from(1_000));
    queue.queue(PREFIX, IggyTimestamp::from(9_000));

    assert_eq!(queue.len(), 1);
    assert_eq!(
        incarnation_pass(&mut queue, &deleter, IggyTimestamp::from(1_000 + grace())).await,
        Some(PREFIX)
    );
}

#[compio::test]
async fn given_keys_of_other_incarnations_and_unparsable_keys_when_deleting_an_incarnation_should_keep_them()
 {
    let store = Faulty::default();
    store_incarnation(&store, PREFIX, &[0, 10]).await;
    let other_partition = ObjectPrefix {
        partition_id: PREFIX.partition_id + 1,
        ..PREFIX
    };
    let incarnation = layout().incarnation_prefix(&PREFIX);
    let mut kept = store_incarnation(&store, RECREATED, &[0, 10]).await;
    kept.extend(store_incarnation(&store, other_partition, &[0]).await);
    let strangers = [
        format!("{incarnation}00000000000000000007.tmp"),
        format!("{incarnation}8.log"),
        format!("{incarnation}nested/00000000000000000009.log"),
        format!("{incarnation}manifest.bin"),
    ];
    for key in &strangers {
        put(&store, key).await;
    }
    kept.extend(strangers);
    kept.sort();
    // A store that answers the listing with a key of the recreated
    // incarnation.
    store
        .lists_also
        .borrow_mut()
        .push(key_of(RECREATED, 10, SegmentObject::Log));

    assert_eq!(
        delete_incarnation(&store, &layout(), &PREFIX)
            .await
            .unwrap(),
        0
    );

    assert_eq!(store.inner.keys(), kept);
}

#[compio::test]
async fn given_a_delete_that_fails_when_an_incarnation_is_deleted_should_keep_it_queued_and_try_again_later()
 {
    let deleter = deleter();
    store_incarnation(store(&deleter), PREFIX, &[0, 10]).await;
    let refused = key_of(PREFIX, 10, SegmentObject::Log);
    let mut queue = DeletedIncarnations::default();
    let observed = IggyTimestamp::from(1_000);
    queue.queue(PREFIX, observed);
    let due = after(observed, grace());

    store(&deleter)
        .deletes
        .replace(Deletes::RefuseKey(refused.clone()));
    assert_eq!(
        incarnation_pass(&mut queue, &deleter, due).await,
        Some(PREFIX)
    );
    // The manifest outlives the segment that is still there.
    assert_eq!(
        store(&deleter).inner.keys(),
        [refused, layout().manifest_key(&PREFIX)]
    );
    assert!(queue.contains(&PREFIX));

    // The store refuses every call, and nothing is asked before the retry
    // delay ends.
    store(&deleter).deletes.replace(Deletes::FailFromCall(0));
    assert_eq!(
        incarnation_pass(&mut queue, &deleter, after(due, retry() - 1)).await,
        None
    );
    let second = after(due, retry());
    assert_eq!(
        incarnation_pass(&mut queue, &deleter, second).await,
        Some(PREFIX)
    );
    assert_eq!(store(&deleter).inner.len(), 2);
    assert!(queue.contains(&PREFIX));

    store(&deleter).deletes.replace(Deletes::Work);
    assert_eq!(
        incarnation_pass(&mut queue, &deleter, after(second, retry())).await,
        Some(PREFIX)
    );
    assert!(store(&deleter).inner.is_empty());
    assert!(queue.is_empty());
}

#[compio::test]
async fn given_a_listing_that_fails_when_an_incarnation_is_deleted_should_delete_nothing_and_try_again_later()
 {
    let deleter = deleter();
    let stored = store_incarnation(store(&deleter), PREFIX, &[0]).await;
    let mut queue = DeletedIncarnations::default();
    let observed = IggyTimestamp::from(1_000);
    queue.queue(PREFIX, observed);
    let due = after(observed, grace());

    store(&deleter).lists_fail.set(true);
    assert_eq!(
        incarnation_pass(&mut queue, &deleter, due).await,
        Some(PREFIX)
    );
    assert_eq!(store(&deleter).inner.keys(), stored);
    assert!(store(&deleter).delete_calls.borrow().is_empty());
    assert!(queue.contains(&PREFIX));

    store(&deleter).lists_fail.set(false);
    assert_eq!(
        incarnation_pass(&mut queue, &deleter, after(due, retry())).await,
        Some(PREFIX)
    );
    assert!(store(&deleter).inner.is_empty());
    assert!(queue.is_empty());
}

#[compio::test]
async fn given_more_objects_than_one_page_when_deleting_an_incarnation_should_follow_every_page_in_bounded_calls()
 {
    let store = Faulty::default();
    let segments = MAX_KEYS_PER_REQUEST as u64 + 100;
    let start_offsets: Vec<u64> = (0..segments).map(|segment| segment * 10).collect();
    store_incarnation(&store, PREFIX, &start_offsets).await;

    assert_eq!(
        delete_incarnation(&store, &layout(), &PREFIX)
            .await
            .unwrap(),
        0
    );

    assert!(store.inner.is_empty());
    // The `.index` and then the `.log` objects of a full call of segments,
    // the same for the rest, then the manifest.
    assert_eq!(
        store.delete_calls.take(),
        [MAX_KEYS_PER_REQUEST, MAX_KEYS_PER_REQUEST, 100, 100, 1]
    );

    // The first call fails, so no other is sent and the manifest stays.
    store_incarnation(&store, PREFIX, &start_offsets).await;
    store.deletes.replace(Deletes::FailFromCall(0));
    assert_eq!(
        delete_incarnation(&store, &layout(), &PREFIX)
            .await
            .unwrap(),
        start_offsets.len() + 1
    );
    assert_eq!(store.delete_calls.take(), [MAX_KEYS_PER_REQUEST]);
    assert_eq!(store.inner.len(), 2 * start_offsets.len() + 1);
}

#[compio::test]
async fn given_a_queued_incarnation_that_is_live_when_a_pass_runs_should_drop_it_and_delete_nothing()
 {
    let deleter = deleter();
    let stored = store_incarnation(store(&deleter), PREFIX, &[0]).await;
    let recreated = store_incarnation(store(&deleter), RECREATED, &[0]).await;
    let mut queue = DeletedIncarnations::default();
    let observed = IggyTimestamp::from(1_000);
    queue.queue(PREFIX, observed);
    queue.queue(RECREATED, observed);
    let due = after(observed, grace());

    // Committed metadata holds the recreated incarnation.
    let (job, scope) = queue.begin(due, |prefix| *prefix == RECREATED).unwrap();
    assert_eq!(job.prefix, PREFIX);
    let outcome = deleter.run(job.clone(), scope).await;
    queue.finish(job.id, outcome, due);
    assert!(queue.begin(due, |prefix| *prefix == RECREATED).is_none());

    assert!(queue.is_empty());
    assert_eq!(store(&deleter).inner.keys(), recreated);
    assert_ne!(stored, recreated);
}

#[compio::test]
async fn given_a_job_in_flight_when_incarnation_passes_run_should_start_no_other_and_ignore_it_once_cleared()
 {
    let mut queue = DeletedIncarnations::default();
    let observed = IggyTimestamp::from(1_000);
    queue.queue(PREFIX, observed);
    queue.queue(RECREATED, observed);
    let due = after(observed, grace());

    let (first, scope) = queue.begin(due, |_| false).unwrap();
    assert!(queue.begin(due, |_| false).is_none());

    queue.clear();
    assert!(scope.is_cancelled());
    assert!(queue.is_empty());

    // A queue that filled again must not take the old job's result for its
    // own job.
    queue.queue(RECREATED, observed);
    let (second, _scope) = queue.begin(due, |_| false).unwrap();
    assert_ne!(first.id, second.id);
    queue.finish(
        first.id,
        Ok(ObjectDeleteDone::IncarnationDeleted(Ok(0))),
        due,
    );
    assert!(queue.contains(&RECREATED));
    assert!(queue.begin(due, |_| false).is_none());
}

#[compio::test]
async fn given_a_cancelled_or_foreign_result_when_an_incarnation_job_finishes_should_keep_it_queued()
 {
    let mut queue = DeletedIncarnations::default();
    let observed = IggyTimestamp::from(1_000);
    queue.queue(PREFIX, observed);
    let mut now = after(observed, grace());

    for outcome in [
        Err(iggy_object_store::TierJobCancelled),
        Ok(ObjectDeleteDone::Deleted { failed: Vec::new() }),
        Ok(ObjectDeleteDone::Swept(Ok(SweptObjects::default()))),
    ] {
        let (job, _scope) = queue.begin(now, |_| false).unwrap();
        queue.finish(job.id, outcome, now);
        assert!(queue.contains(&PREFIX));
        now = after(now, retry());
    }
}

#[test]
fn given_a_full_queue_when_queueing_an_incarnation_should_leave_it_out() {
    let mut queue = DeletedIncarnations::default();
    let now = IggyTimestamp::from(1_000);
    for created_revision in 0..MAX_QUEUED_INCARNATIONS as u64 {
        assert!(queue.queue(
            ObjectPrefix {
                created_revision,
                ..PREFIX
            },
            now
        ));
    }
    let one_more = ObjectPrefix {
        created_revision: u64::MAX,
        ..PREFIX
    };

    assert!(!queue.queue(one_more, now));
    assert!(!queue.contains(&one_more));
    assert_eq!(queue.len(), MAX_QUEUED_INCARNATIONS);
    // One that waits already is not turned away.
    assert!(queue.queue(
        ObjectPrefix {
            created_revision: 0,
            ..PREFIX
        },
        now
    ));
}

#[compio::test]
async fn given_an_incarnation_delete_when_submitted_should_name_the_delete_class() {
    let deleter = ExecutorDeleter::new(
        Recording {
            store: Rc::new(MemoryStore::new()),
            requests: RefCell::default(),
        },
        layout(),
    );
    let mut queue = DeletedIncarnations::default();
    queue.queue(PREFIX, IggyTimestamp::from(1_000));

    incarnation_pass(&mut queue, &deleter, IggyTimestamp::from(1_000 + grace())).await;

    assert_eq!(
        deleter.executor().requests.take(),
        [(TierJobClass::Delete, 0)]
    );
}
