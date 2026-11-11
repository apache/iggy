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
    ExecutorDeleter, ObjectDeleteDone, ObjectDeleteWork, ObjectDeleter, RETIRED_OBJECT_GRACE,
    SegmentLocation, delete_objects, list_segment_starts, upload_segment,
};
use iggy_object_store::{
    DeleteFailure, DeleteOutcome, InlineTierExecutor, ListPage, ListRequest, ListedObject,
    MAX_KEYS_PER_REQUEST, MemoryStore, ObjectMeta, ObjectPrefix, ObjectRange, ObjectStore,
    ObjectStoreError, PutBody, PutOptions, SegmentKey, SegmentObject, TierJobClass,
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

/// A memory store whose deletes can fail, that keeps the number of keys of
/// every delete call, and that can list keys it does not hold.
#[derive(Default)]
struct Faulty {
    inner: MemoryStore,
    deletes: RefCell<Deletes>,
    delete_calls: RefCell<Vec<usize>>,
    lists_also: RefCell<Vec<String>>,
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
        Some(ObjectDeleteWork::Sweep)
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
        Some(ObjectDeleteWork::Sweep)
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

    // The store refuses one object of the second segment.
    let refused = key_of(PREFIX, SEALED[1].0, SegmentObject::Index);
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
    assert_eq!(swept, Some(ObjectDeleteWork::Sweep));
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
        Some(ObjectDeleteWork::Sweep)
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
        Some(ObjectDeleteWork::Sweep)
    );
    assert_eq!(pass(&mut partition, &deleter, now, committed).await, None);

    regain_primacy(&mut partition);
    assert_eq!(
        pass(&mut partition, &deleter, now, committed).await,
        Some(ObjectDeleteWork::Sweep)
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
    assert_eq!(job.work, ObjectDeleteWork::Sweep);
    partition.finish_object_delete(
        job.id,
        Ok(ObjectDeleteDone::Swept(Err(ObjectStoreError::Unavailable(
            "503".to_owned(),
        )))),
        now,
    );

    let (job, _scope) = partition.begin_object_delete(now, committed).unwrap();
    assert_eq!(job.work, ObjectDeleteWork::Sweep);
}

#[compio::test]
async fn given_an_upload_that_lands_after_its_segment_was_removed_when_it_finishes_should_not_advance_and_retire_its_objects()
 {
    let (_directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let deleter = deleter();
    let (upload, _scope) = partition.begin_segment_upload().unwrap();
    assert_eq!(upload.record.start_offset, SEALED[0].0);
    upload_segment(store(&deleter), &layout(), &upload)
        .await
        .unwrap();
    truncate(&mut partition, SEALED[0].1).await;
    let committed = SEALED[0].1;
    assert_eq!(
        pass(&mut partition, &deleter, IggyTimestamp::now(), committed).await,
        Some(ObjectDeleteWork::Sweep)
    );
    compio::time::sleep(std::time::Duration::from_millis(5)).await;
    let landed = IggyTimestamp::now();

    assert_eq!(
        partition.finish_segment_upload(&upload.record, &Ok(Ok(()))),
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
    // The next upload is the next segment, as if the first never ran.
    let (next, _scope) = partition.begin_segment_upload().unwrap();
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
        Some(ObjectDeleteWork::Sweep)
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
        Ok(ObjectDeleteDone::Swept(Ok(BTreeSet::new()))),
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
        Ok(ObjectDeleteDone::Swept(Ok(BTreeSet::new()))),
        now,
    );
    let (again, _scope) = partition.begin_object_delete(now, committed).unwrap();
    assert_eq!(
        again.work,
        ObjectDeleteWork::Sweep,
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
    let listed = Ok(ObjectDeleteDone::Swept(Ok(BTreeSet::from([
        SEALED[0].0,
        SEALED[1].0,
    ]))));

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
        Ok(ObjectDeleteDone::Swept(Ok(BTreeSet::from([
            SEALED[0].0,
            SEALED[1].0,
        ])))),
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
        Ok(ObjectDeleteDone::Swept(Ok(BTreeSet::from([SEALED[0].0])))),
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
