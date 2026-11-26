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

//! The store calls of the orphan sweep, see [`crate::orphan_sweep`]: the
//! read of the claim of the root, the listing of the root, and the delete of
//! an orphan.

use std::time::SystemTime;

use iggy_object_store::{
    IdentityRead, KeyLayout, ListRequest, MAX_IDENTITY_LEN, ObjectPrefix, ObjectStore,
    ObjectStoreError, SegmentObject, TopicKey,
};

use crate::object_delete::delete_objects;
use crate::orphan_sweep::{
    ClaimStamp, ORPHAN_LIST_PAGES_PER_JOB, ORPHAN_PAGES_PER_DELETE_TURN, OrphanCheck, OrphanDelete,
    OrphanGuard, OrphanProgress, OrphanRefusal, RootListing, RootPages,
};

/// Read the store identity object of the root: who claimed it, and when.
/// `None` when the store holds no identity object that a cluster wrote, or
/// when it was replaced between the two calls.
///
/// # Errors
/// The store's error for either call, unless the object is missing.
pub async fn read_claim<S: ObjectStore>(
    store: &S,
    layout: &KeyLayout,
) -> Result<Option<ClaimStamp>, ObjectStoreError> {
    let key = layout.identity_key();
    let held = match store.head(&key).await {
        Ok(held) => held,
        Err(ObjectStoreError::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(error),
    };
    let answer = store.get_range(&key, 0..MAX_IDENTITY_LEN).await;
    let etag = answer.as_ref().ok().map(|range| range.etag.clone());
    match IdentityRead::of(answer)? {
        IdentityRead::Found(identity) if etag.as_deref() == Some(held.etag.as_str()) => {
            Ok(Some(ClaimStamp {
                cluster_uid: identity.cluster_uid,
                etag: held.etag,
                claimed_at: held.last_modified,
            }))
        }
        IdentityRead::Found(_) | IdentityRead::Missing | IdentityRead::Invalid => Ok(None),
    }
}

/// List up to [`ORPHAN_LIST_PAGES_PER_JOB`] pages of the root, from the one
/// after `after` or from the first key, under a claim of the root the store
/// committed at `claimed_at`.
///
/// # Errors
/// The store's error for the page that failed. Nothing is returned for the
/// pages before it, and the sweep that asked is not a full one.
pub async fn list_root<S: ObjectStore>(
    store: &S,
    layout: &KeyLayout,
    after: Option<String>,
    claimed_at: SystemTime,
) -> Result<RootPages, ObjectStoreError> {
    let prefix = format!("{}/", layout.root());
    let mut listing = RootListing::default();
    let mut continuation = after;
    for _ in 0..ORPHAN_LIST_PAGES_PER_JOB {
        let request = ListRequest::new(&prefix);
        let request = continuation
            .as_deref()
            .map_or(request, |continuation| request.after(continuation));
        let page = store.list(&request).await?;
        for object in &page.objects {
            listing.take(layout, object, claimed_at);
        }
        continuation = page.next;
        if continuation.is_none() {
            break;
        }
    }
    Ok(RootPages {
        listing,
        next: continuation,
    })
}

/// Run one turn of the delete of the orphan `prefix` from `progress`, once
/// the store still holds what the sweep judged as `guard` records it.
///
/// Every turn reads the identity object first, and a turn under another
/// claim of the root deletes nothing. Then it lists up to
/// [`ORPHAN_PAGES_PER_DELETE_TURN`] pages of the incarnation and checks each
/// object as its page arrives. The turns of the check delete nothing. At
/// the end of their listing they ask for the manifest and for the object of
/// the topic incarnation, and a long-term segment, a topic object, or an
/// object outside the moments the guard allows ends the delete. The turns
/// after them check the same way before they delete what they listed: every
/// `.index` before any `.log`, and the manifest last, once the listing ended
/// and nothing was left.
///
/// # Errors
/// The store's error for a read or a listing that failed. The turn deletes
/// nothing then.
pub async fn delete_orphan<S: ObjectStore>(
    store: &S,
    layout: &KeyLayout,
    prefix: &ObjectPrefix,
    guard: &OrphanGuard,
    progress: OrphanProgress,
) -> Result<OrphanDelete, ObjectStoreError> {
    let answer = store
        .get_range(&layout.identity_key(), 0..MAX_IDENTITY_LEN)
        .await;
    let etag = answer.as_ref().ok().map(|range| range.etag.clone());
    if !guard.same_claim(&IdentityRead::of(answer)?, etag.as_deref()) {
        return Ok(OrphanDelete::Refused(OrphanRefusal::IdentityChanged));
    }
    let OrphanProgress {
        deleting,
        after,
        mut check,
    } = progress;
    let listed = list_orphan(store, layout, prefix, guard, after, &mut check).await?;
    let manifest_key = layout.manifest_key(prefix);
    let manifest = match listed.next {
        Some(_) => None,
        None => modified_at(store, &manifest_key).await?,
    };
    if let Some(modified) = manifest {
        check.take(guard, modified, false);
    }
    if !deleting {
        if check.long_term {
            return Ok(OrphanDelete::Refused(OrphanRefusal::LongTerm));
        }
        if let Some(after) = listed.next {
            return Ok(OrphanDelete::Continues(OrphanProgress {
                deleting,
                after: Some(after),
                check,
            }));
        }
    }
    if let Some(refusal) = check.refusal(topic_object(store, layout, prefix).await?) {
        return Ok(OrphanDelete::Refused(refusal));
    }
    if !deleting {
        return Ok(OrphanDelete::Continues(OrphanProgress {
            deleting: true,
            ..OrphanProgress::default()
        }));
    }
    if !delete_objects(store, &listed.indexes).await.is_empty()
        || !delete_objects(store, &listed.logs).await.is_empty()
    {
        return Ok(OrphanDelete::Unfinished);
    }
    if let Some(after) = listed.next {
        return Ok(OrphanDelete::Continues(OrphanProgress {
            deleting,
            after: Some(after),
            ..OrphanProgress::default()
        }));
    }
    if manifest.is_some() && !delete_objects(store, &[manifest_key]).await.is_empty() {
        return Ok(OrphanDelete::Unfinished);
    }
    Ok(OrphanDelete::Deleted)
}

/// What one turn lists of the objects of an orphan.
struct ListedTurn {
    /// The keys of the `.index` and of the `.log` objects of its segments.
    indexes: Vec<String>,
    logs: Vec<String>,
    /// Where the listing goes on, `None` once it reached the last key.
    next: Option<String>,
}

/// List up to [`ORPHAN_PAGES_PER_DELETE_TURN`] pages of the segment objects
/// of `prefix`, of both kinds of key, after `after`, and take each object
/// into `check` as its page arrives. A key the layout did not build for this
/// incarnation is left out, so nothing deletes it, and so is its moment.
async fn list_orphan<S: ObjectStore>(
    store: &S,
    layout: &KeyLayout,
    prefix: &ObjectPrefix,
    guard: &OrphanGuard,
    after: Option<String>,
    check: &mut OrphanCheck,
) -> Result<ListedTurn, ObjectStoreError> {
    let listing_prefix = layout.incarnation_prefix(prefix);
    let mut listed = ListedTurn {
        indexes: Vec::new(),
        logs: Vec::new(),
        next: after,
    };
    for _ in 0..ORPHAN_PAGES_PER_DELETE_TURN {
        let request = ListRequest::new(&listing_prefix);
        let request = listed
            .next
            .as_deref()
            .map_or(request, |continuation| request.after(continuation));
        let page = store.list(&request).await?;
        for object in page.objects {
            if let Some(key) = layout.parse_segment_key(&object.key)
                && key.prefix == *prefix
            {
                check.take(guard, object.last_modified, false);
                match key.object {
                    SegmentObject::Index => listed.indexes.push(object.key),
                    SegmentObject::Log => listed.logs.push(object.key),
                }
            } else if let Some(key) = layout.parse_long_term_key(&object.key)
                && key.prefix == *prefix
            {
                check.take(guard, object.last_modified, true);
            }
        }
        listed.next = page.next;
        if listed.next.is_none() {
            break;
        }
    }
    Ok(listed)
}

/// Whether a geo topic object names the topic incarnation of `prefix`.
async fn topic_object<S: ObjectStore>(
    store: &S,
    layout: &KeyLayout,
    prefix: &ObjectPrefix,
) -> Result<bool, ObjectStoreError> {
    let key = layout.topic_key(&TopicKey {
        stream_id: prefix.stream_id,
        topic_id: prefix.topic_id,
        created_revision: prefix.created_revision,
    });
    Ok(modified_at(store, &key).await?.is_some())
}

/// When the store committed the object under `key`, `None` without one.
async fn modified_at<S: ObjectStore>(
    store: &S,
    key: &str,
) -> Result<Option<SystemTime>, ObjectStoreError> {
    match store.head(key).await {
        Ok(held) => Ok(Some(held.last_modified)),
        Err(ObjectStoreError::NotFound { .. }) => Ok(None),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::collections::BTreeSet;
    use std::ops::Range;
    use std::time::{Duration, UNIX_EPOCH};

    use futures::executor::block_on;
    use iggy_object_store::{
        DeleteFailure, DeleteOutcome, ListPage, LongTermKey, MAX_KEYS_PER_REQUEST, MemoryStore,
        ObjectMeta, ObjectRange, PutBody, PutOptions, SegmentKey, StoreIdentity,
    };

    use super::*;

    const ORPHAN: ObjectPrefix = ObjectPrefix {
        stream_id: 1,
        topic_id: 2,
        partition_id: 0,
        created_revision: 7,
    };
    const OURS: u128 = 0x0123_4567_89ab_cdef_0011_2233_4455_6677;

    fn layout() -> KeyLayout {
        KeyLayout::new("iggy").unwrap()
    }

    /// A memory store that keeps the keys of every delete call, counts the
    /// reads of the identity object and the listing pages, and can refuse
    /// one key.
    #[derive(Default)]
    struct Deletes {
        inner: MemoryStore,
        refused: Option<String>,
        calls: RefCell<Vec<Vec<String>>>,
        identity_reads: Cell<usize>,
        pages: Cell<usize>,
    }

    impl ObjectStore for Deletes {
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
            range: Range<u64>,
        ) -> Result<ObjectRange, ObjectStoreError> {
            if key == layout().identity_key() {
                self.identity_reads.set(self.identity_reads.get() + 1);
            }
            self.inner.get_range(key, range).await
        }

        async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectStoreError> {
            self.inner.head(key).await
        }

        async fn delete(&self, keys: &[String]) -> Result<DeleteOutcome, ObjectStoreError> {
            self.calls.borrow_mut().push(keys.to_vec());
            let (refused, deleted): (Vec<String>, Vec<String>) = keys
                .iter()
                .cloned()
                .partition(|key| self.refused.as_ref() == Some(key));
            let mut outcome = self.inner.delete(&deleted).await?;
            outcome
                .failed
                .extend(refused.into_iter().map(|key| DeleteFailure {
                    error: ObjectStoreError::Transport("refused".to_owned()),
                    key,
                }));
            Ok(outcome)
        }

        async fn list(&self, request: &ListRequest<'_>) -> Result<ListPage, ObjectStoreError> {
            self.pages.set(self.pages.get() + 1);
            self.inner.list(request).await
        }
    }

    fn put(store: &impl ObjectStore, key: &str) {
        block_on(store.put(key, PutBody::from_bytes(vec![1]), &PutOptions::default())).unwrap();
    }

    fn segment(prefix: ObjectPrefix, start_offset: u64, object: SegmentObject) -> String {
        layout().segment_key(&SegmentKey {
            prefix,
            start_offset,
            object,
        })
    }

    /// Two segments and the manifest of `prefix`, with their keys.
    fn store_incarnation(store: &impl ObjectStore, prefix: ObjectPrefix) -> Vec<String> {
        let mut keys: Vec<String> = [0, 10]
            .into_iter()
            .flat_map(|start| SegmentObject::ALL.map(|object| segment(prefix, start, object)))
            .collect();
        keys.push(layout().manifest_key(&prefix));
        for key in &keys {
            put(store, key);
        }
        keys
    }

    /// Claim the root for `cluster_uid`, and return what a sweep reads of
    /// the claim.
    fn claimed(store: &impl ObjectStore, cluster_uid: u128) -> ClaimStamp {
        block_on(StoreIdentity { cluster_uid }.replace(store, &layout())).unwrap();
        block_on(read_claim(store, &layout())).unwrap().unwrap()
    }

    /// A guard that lets every object stored after the claim and up to an
    /// hour from now go.
    fn open_guard(claim: ClaimStamp) -> OrphanGuard {
        OrphanGuard {
            claim,
            newest: SystemTime::now() + Duration::from_hours(1),
        }
    }

    fn keys(store: &Deletes) -> BTreeSet<String> {
        store.inner.keys().into_iter().collect()
    }

    /// One turn of the delete of [`ORPHAN`] from `progress`.
    fn turn(store: &Deletes, guard: &OrphanGuard, progress: OrphanProgress) -> OrphanDelete {
        block_on(delete_orphan(store, &layout(), &ORPHAN, guard, progress)).unwrap()
    }

    /// Run the turns of the delete of [`ORPHAN`] until one ends it.
    fn delete_in_turns(store: &Deletes, guard: &OrphanGuard) -> OrphanDelete {
        let mut progress = OrphanProgress::default();
        for _ in 0..64 {
            match turn(store, guard, progress) {
                OrphanDelete::Continues(next) => progress = next,
                ended => return ended,
            }
        }
        panic!("the delete of an orphan did not end");
    }

    #[test]
    fn given_a_claimed_root_when_reading_the_claim_should_name_the_cluster_its_object_and_its_moment()
     {
        let store = MemoryStore::new();

        assert_eq!(block_on(read_claim(&store, &layout())).unwrap(), None);
        let claim = claimed(&store, OURS);
        let held = block_on(store.head(&layout().identity_key())).unwrap();

        assert_eq!(claim.cluster_uid, OURS);
        assert_eq!(claim.etag, held.etag);
        assert_eq!(claim.claimed_at, held.last_modified);
        assert!(claimed(&store, OURS).etag != claim.etag, "a new put");
    }

    #[test]
    fn given_bytes_no_cluster_wrote_when_reading_the_claim_should_find_none() {
        let store = MemoryStore::new();
        put(&store, &layout().identity_key());

        assert_eq!(block_on(read_claim(&store, &layout())).unwrap(), None);
    }

    #[test]
    fn given_more_keys_than_one_job_lists_when_listing_the_root_should_go_on_where_the_job_stopped()
    {
        let store = MemoryStore::new();
        let objects = ORPHAN_LIST_PAGES_PER_JOB * MAX_KEYS_PER_REQUEST + 3;
        for start in 0..objects {
            put(&store, &segment(ORPHAN, start as u64, SegmentObject::Log));
        }

        let first = block_on(list_root(&store, &layout(), None, UNIX_EPOCH)).unwrap();
        let second =
            block_on(list_root(&store, &layout(), first.next.clone(), UNIX_EPOCH)).unwrap();

        assert!(first.next.is_some());
        assert_eq!(
            first.listing.incarnations[&ORPHAN].objects,
            ORPHAN_LIST_PAGES_PER_JOB * MAX_KEYS_PER_REQUEST
        );
        assert_eq!(second.next, None);
        assert_eq!(second.listing.incarnations[&ORPHAN].objects, 3);
    }

    #[test]
    fn given_an_orphan_the_store_holds_as_judged_when_deleting_should_delete_its_segments_and_then_its_manifest()
     {
        let store = Deletes::default();
        let claim = claimed(&store, OURS);
        let orphan = store_incarnation(&store, ORPHAN);
        let other = ObjectPrefix {
            partition_id: 1,
            ..ORPHAN
        };
        let mut kept = store_incarnation(&store, other);
        let stray = format!("{}notes.txt", layout().incarnation_prefix(&ORPHAN));
        put(&store, &stray);
        kept.extend([stray, layout().identity_key()]);
        let guard = open_guard(claim);

        let checked = turn(&store, &guard, OrphanProgress::default());
        assert!(store.calls.borrow().is_empty(), "the check deletes nothing");
        let OrphanDelete::Continues(progress) = checked else {
            panic!("the check ends in a turn that deletes: {checked:?}");
        };
        assert_eq!(turn(&store, &guard, progress), OrphanDelete::Deleted);

        assert_eq!(keys(&store), kept.into_iter().collect());
        assert_eq!(
            *store.calls.borrow(),
            [
                vec![
                    segment(ORPHAN, 0, SegmentObject::Index),
                    segment(ORPHAN, 10, SegmentObject::Index),
                ],
                vec![
                    segment(ORPHAN, 0, SegmentObject::Log),
                    segment(ORPHAN, 10, SegmentObject::Log),
                ],
                vec![layout().manifest_key(&ORPHAN)],
            ]
        );
        assert_eq!(
            store
                .calls
                .borrow()
                .iter()
                .flatten()
                .cloned()
                .collect::<BTreeSet<_>>(),
            orphan.into_iter().collect()
        );
    }

    #[test]
    fn given_a_segment_object_the_store_refuses_to_delete_when_deleting_an_orphan_should_report_it_unfinished_and_keep_every_log_and_the_manifest()
     {
        let refused = segment(ORPHAN, 10, SegmentObject::Index);
        let store = Deletes {
            refused: Some(refused.clone()),
            ..Deletes::default()
        };
        let claim = claimed(&store, OURS);
        store_incarnation(&store, ORPHAN);

        let deleted = delete_in_turns(&store, &open_guard(claim));

        assert_eq!(deleted, OrphanDelete::Unfinished);
        assert_eq!(
            keys(&store),
            BTreeSet::from([
                layout().identity_key(),
                refused,
                segment(ORPHAN, 0, SegmentObject::Log),
                segment(ORPHAN, 10, SegmentObject::Log),
                layout().manifest_key(&ORPHAN),
            ])
        );
    }

    #[test]
    fn given_an_orphan_of_more_pages_than_a_turn_lists_when_deleting_should_read_the_identity_and_list_at_most_its_pages_in_each_turn()
     {
        let store = Deletes::default();
        let claim = claimed(&store, OURS);
        let objects = ORPHAN_PAGES_PER_DELETE_TURN * MAX_KEYS_PER_REQUEST + 1;
        for start in 0..objects {
            put(&store, &segment(ORPHAN, start as u64, SegmentObject::Log));
        }
        put(&store, &layout().manifest_key(&ORPHAN));
        let guard = open_guard(claim);

        let mut progress = OrphanProgress::default();
        let mut turns = Vec::new();
        let ended = loop {
            let (reads, pages, calls) = (
                store.identity_reads.get(),
                store.pages.get(),
                store.calls.borrow().len(),
            );
            let done = turn(&store, &guard, progress);
            turns.push((
                store.identity_reads.get() - reads,
                store.pages.get() - pages,
                store.calls.borrow().len() > calls,
            ));
            match done {
                OrphanDelete::Continues(next) => progress = next,
                ended => break ended,
            }
        };

        assert_eq!(ended, OrphanDelete::Deleted);
        assert_eq!(
            turns,
            [
                (1, ORPHAN_PAGES_PER_DELETE_TURN, false),
                (1, 1, false),
                (1, ORPHAN_PAGES_PER_DELETE_TURN, true),
                (1, 1, true),
            ],
            "identity reads, listing pages, and whether the turn deleted"
        );
        assert_eq!(keys(&store), BTreeSet::from([layout().identity_key()]));
    }

    #[test]
    fn given_an_identity_change_after_the_check_when_a_turn_would_delete_should_delete_nothing() {
        let store = Deletes::default();
        let claim = claimed(&store, OURS);
        store_incarnation(&store, ORPHAN);
        let guard = open_guard(claim);
        let OrphanDelete::Continues(progress) = turn(&store, &guard, OrphanProgress::default())
        else {
            panic!("the check refused the orphan");
        };
        assert!(progress.deleting);
        let stored = keys(&store);

        claimed(&store, OURS + 1);
        let deleted = turn(&store, &guard, progress);

        assert_eq!(
            deleted,
            OrphanDelete::Refused(OrphanRefusal::IdentityChanged)
        );
        assert_eq!(keys(&store), stored);
        assert!(store.calls.borrow().is_empty());
    }

    #[test]
    fn given_an_object_stored_after_the_check_when_a_turn_would_delete_should_refuse_and_delete_nothing()
     {
        let store = Deletes::default();
        let claim = claimed(&store, OURS);
        store_incarnation(&store, ORPHAN);
        std::thread::sleep(Duration::from_millis(2));
        let guard = OrphanGuard {
            claim,
            newest: SystemTime::now(),
        };
        let OrphanDelete::Continues(progress) = turn(&store, &guard, OrphanProgress::default())
        else {
            panic!("the check refused the orphan");
        };

        std::thread::sleep(Duration::from_millis(2));
        put(&store, &segment(ORPHAN, 20, SegmentObject::Log));
        let stored = keys(&store);

        assert_eq!(
            turn(&store, &guard, progress),
            OrphanDelete::Refused(OrphanRefusal::Recent)
        );
        assert_eq!(keys(&store), stored);
        assert!(store.calls.borrow().is_empty());
    }

    #[test]
    fn given_a_recent_object_on_the_first_page_and_a_long_term_segment_in_a_later_turn_when_checking_should_refuse_for_the_long_term_segment()
     {
        let store = Deletes::default();
        let claim = claimed(&store, OURS);
        let objects = ORPHAN_PAGES_PER_DELETE_TURN * MAX_KEYS_PER_REQUEST;
        for start in 1..=objects {
            put(&store, &segment(ORPHAN, start as u64, SegmentObject::Log));
        }
        put(
            &store,
            &layout().long_term_key(&LongTermKey {
                prefix: ORPHAN,
                start_offset: 9_000_000,
                end_offset: 9_000_009,
                object: SegmentObject::Log,
            }),
        );
        std::thread::sleep(Duration::from_millis(2));
        let guard = OrphanGuard {
            claim,
            newest: SystemTime::now(),
        };
        std::thread::sleep(Duration::from_millis(2));
        put(&store, &segment(ORPHAN, 0, SegmentObject::Log));

        let OrphanDelete::Continues(progress) = turn(&store, &guard, OrphanProgress::default())
        else {
            panic!("the first turn of the check ended the delete");
        };
        assert_eq!(progress.check.age, Some(OrphanRefusal::Recent));
        assert!(!progress.check.long_term);

        assert_eq!(
            turn(&store, &guard, progress),
            OrphanDelete::Refused(OrphanRefusal::LongTerm)
        );
        assert!(store.calls.borrow().is_empty());
    }

    #[test]
    fn given_an_identity_object_other_than_the_claim_when_deleting_an_orphan_should_delete_nothing()
    {
        for change in ["another cluster", "a new put of ours", "no object"] {
            let store = Deletes::default();
            let claim = claimed(&store, OURS);
            let stored: BTreeSet<String> = store_incarnation(&store, ORPHAN).into_iter().collect();
            match change {
                "another cluster" => {
                    claimed(&store, OURS + 1);
                }
                "a new put of ours" => {
                    claimed(&store, OURS);
                }
                _ => {
                    let outcome = block_on(store.inner.delete(&[layout().identity_key()])).unwrap();
                    assert!(outcome.is_complete());
                }
            }

            let deleted = turn(&store, &open_guard(claim), OrphanProgress::default());

            assert_eq!(
                deleted,
                OrphanDelete::Refused(OrphanRefusal::IdentityChanged),
                "{change}"
            );
            let mut left = keys(&store);
            left.remove(&layout().identity_key());
            assert_eq!(left, stored, "{change}");
            assert!(store.calls.borrow().is_empty(), "{change}");
        }
    }

    #[test]
    fn given_an_orphan_the_store_no_longer_holds_as_judged_when_deleting_should_delete_nothing() {
        let long_term = layout().long_term_key(&LongTermKey {
            prefix: ORPHAN,
            start_offset: 0,
            end_offset: 9,
            object: SegmentObject::Log,
        });
        let topic = layout().topic_key(&TopicKey {
            stream_id: ORPHAN.stream_id,
            topic_id: ORPHAN.topic_id,
            created_revision: ORPHAN.created_revision,
        });
        for (case, extra, refusal) in [
            ("long-term", Some(long_term), OrphanRefusal::LongTerm),
            ("topic object", Some(topic), OrphanRefusal::TopicObject),
            ("recent", None, OrphanRefusal::Recent),
            ("before the claim", None, OrphanRefusal::BeforeClaim),
        ] {
            let store = Deletes::default();
            let claim = claimed(&store, OURS);
            store_incarnation(&store, ORPHAN);
            if let Some(key) = &extra {
                put(&store, key);
            }
            let stored = keys(&store);
            let guard = match refusal {
                OrphanRefusal::Recent => OrphanGuard {
                    newest: claim.claimed_at,
                    claim,
                },
                OrphanRefusal::BeforeClaim => OrphanGuard {
                    claim: ClaimStamp {
                        claimed_at: SystemTime::now() + Duration::from_hours(1),
                        ..claim
                    },
                    newest: SystemTime::now() + Duration::from_hours(2),
                },
                _ => open_guard(claim),
            };

            let deleted = delete_in_turns(&store, &guard);

            assert_eq!(deleted, OrphanDelete::Refused(refusal), "{case}");
            assert_eq!(keys(&store), stored, "{case}");
            assert!(store.calls.borrow().is_empty(), "{case}");
        }
    }

    #[test]
    fn given_a_manifest_older_than_the_claim_when_deleting_an_orphan_should_delete_nothing() {
        let store = Deletes::default();
        put(&store, &layout().manifest_key(&ORPHAN));
        std::thread::sleep(Duration::from_millis(2));
        let claim = claimed(&store, OURS);
        std::thread::sleep(Duration::from_millis(2));
        put(&store, &segment(ORPHAN, 0, SegmentObject::Log));

        let deleted = delete_in_turns(&store, &open_guard(claim));

        assert_eq!(deleted, OrphanDelete::Refused(OrphanRefusal::BeforeClaim));
        assert!(store.calls.borrow().is_empty());
    }
}
