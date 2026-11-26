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

//! The sweep of the store root for the objects of partition incarnations
//! that committed metadata no longer holds.
//!
//! A committed delete queues the incarnations it removed in memory only, see
//! [`crate::DeletedIncarnations`]. A restart, a change of the metadata
//! primary, a change feed that overflowed and a full queue each leave their
//! objects in the store, and the sweep finds them. The caught-up metadata
//! primary reads the revision committed metadata reached, then lists the
//! whole root, and sorts what it lists by incarnation.
//!
//! An incarnation is an orphan when committed metadata holds no partition of
//! it and its revision is not above the one read before the listing.
//! Revisions only rise, and a partition stores objects only under the
//! revision its creation committed at, so an incarnation created while the
//! listing ran is never one. An orphan is deleted once full sweeps in a row,
//! under the same claim of the root and in the same view of the same
//! process, listed the same objects of it from the start of the second of
//! them for at least the grace period of this node's monotonic clock. The
//! first sweep listed them before the second started, so the grace never
//! counts on a moment the store gave, and a step of the wall clock never
//! shortens it.
//!
//! Whatever metadata holds, an incarnation stays when it has a long-term
//! segment or when a geo topic object names its topic incarnation, as the
//! sweep of geo topics owns those. It stays as well when one of its objects
//! is no newer than this cluster's claim of the root, compared in whole
//! seconds: a retired cluster left it, and the operator decides.
//!
//! Nothing here does any I/O.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use iggy_object_store::{KeyLayout, ListedObject, ObjectPrefix, TopicKey};

/// The longest time between the starts of two sweeps of the root.
///
/// A grace period that is shorter takes its place, so a short grace does not
/// wait an hour between the sweeps a delete needs.
pub const ORPHAN_SWEEP_INTERVAL: Duration = Duration::from_hours(1);

/// The most pages of the root one listing job reads.
pub const ORPHAN_LIST_PAGES_PER_JOB: usize = 16;

/// The most pages of an orphan's objects one turn of its delete lists.
pub const ORPHAN_PAGES_PER_DELETE_TURN: usize = 4;

/// The time between the starts of two sweeps with `grace`.
#[must_use]
pub fn sweep_interval(grace: Duration) -> Duration {
    grace.min(ORPHAN_SWEEP_INTERVAL)
}

/// Whether the store committed an object at `modified` no later than the
/// claim of the root it committed at `claimed_at`. A head can give the claim
/// to the second while a listing gives the object finer, as MinIO does, so
/// both count in whole seconds, and an object from the second of the claim
/// counts as before it.
#[must_use]
pub fn no_newer_than_claim(modified: SystemTime, claimed_at: SystemTime) -> bool {
    whole_seconds(modified) <= whole_seconds(claimed_at)
}

/// Seconds since the Unix epoch, zero for a moment before it.
fn whole_seconds(moment: SystemTime) -> u64 {
    moment
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs())
}

/// What a listed key under the root is to the sweep.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RootKey {
    /// A `.log` or a `.index` of a segment whose key names only its start.
    Segment(ObjectPrefix),
    /// An object of a long-term segment.
    LongTerm(ObjectPrefix),
    Manifest(ObjectPrefix),
    /// The object of a geo topic incarnation, live or a tombstone.
    Topic(TopicKey),
    /// A key under `meta/` of no incarnation, such as the identity object, a
    /// fresh object, a positions object or a recovery record.
    Meta,
    /// A key the layout did not build.
    Unrecognised,
}

impl RootKey {
    #[must_use]
    pub fn of(layout: &KeyLayout, key: &str) -> Self {
        if let Some(segment) = layout.parse_segment_key(key) {
            return Self::Segment(segment.prefix);
        }
        if let Some(segment) = layout.parse_long_term_key(key) {
            return Self::LongTerm(segment.prefix);
        }
        if let Some(prefix) = layout.parse_manifest_key(key) {
            return Self::Manifest(prefix);
        }
        if let Some(topic) = layout.parse_topic_key(key) {
            return Self::Topic(topic);
        }
        if layout.is_meta_key(key) {
            Self::Meta
        } else {
            Self::Unrecognised
        }
    }
}

/// What a listing found of one incarnation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListedIncarnation {
    /// Its segment objects, long-term objects and manifest.
    pub objects: usize,
    /// How many of them the store committed no later than this cluster's
    /// claim of the root, see [`no_newer_than_claim`].
    pub before_claim: usize,
    pub long_term: bool,
    /// When the store committed the newest of them.
    pub newest: SystemTime,
}

impl ListedIncarnation {
    fn merge(&mut self, other: &Self) {
        self.objects += other.objects;
        self.before_claim += other.before_claim;
        self.long_term |= other.long_term;
        self.newest = self.newest.max(other.newest);
    }
}

/// What a listing of the root found, by incarnation.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RootListing {
    pub incarnations: BTreeMap<ObjectPrefix, ListedIncarnation>,
    /// The geo topic incarnations that have an object.
    pub topics: BTreeSet<TopicKey>,
    /// Keys outside `meta/` that the layout did not build.
    pub unrecognised: usize,
}

impl RootListing {
    /// Take one listed object into the listing, under a claim of the root
    /// the store committed at `claimed_at`.
    pub fn take(&mut self, layout: &KeyLayout, object: &ListedObject, claimed_at: SystemTime) {
        let (prefix, long_term) = match RootKey::of(layout, &object.key) {
            RootKey::Segment(prefix) | RootKey::Manifest(prefix) => (prefix, false),
            RootKey::LongTerm(prefix) => (prefix, true),
            RootKey::Topic(topic) => {
                self.topics.insert(topic);
                return;
            }
            RootKey::Meta => return,
            RootKey::Unrecognised => {
                self.unrecognised += 1;
                return;
            }
        };
        let listed = ListedIncarnation {
            objects: 1,
            before_claim: usize::from(no_newer_than_claim(object.last_modified, claimed_at)),
            long_term,
            newest: object.last_modified,
        };
        self.incarnations
            .entry(prefix)
            .and_modify(|incarnation| incarnation.merge(&listed))
            .or_insert(listed);
    }

    /// Take what another part of the same listing found.
    pub fn merge(&mut self, other: Self) {
        for (prefix, listed) in other.incarnations {
            self.incarnations
                .entry(prefix)
                .and_modify(|incarnation| incarnation.merge(&listed))
                .or_insert(listed);
        }
        self.topics.extend(other.topics);
        self.unrecognised += other.unrecognised;
    }
}

/// What one listing job read: its part of the root, and where the next job
/// goes on, `None` once the listing reached the last key.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RootPages {
    pub listing: RootListing,
    pub next: Option<String>,
}

/// The store identity object as a sweep read it: the cluster it names, the
/// etag of the object, and when the store committed it, which is when this
/// cluster claimed the root, or took it over in a recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimStamp {
    pub cluster_uid: u128,
    pub etag: String,
    pub claimed_at: SystemTime,
}

/// What a sweep makes of one listed incarnation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Committed metadata holds a partition of the incarnation.
    Live,
    /// Its revision is above the one committed metadata had reached before
    /// the listing.
    AfterSnapshot,
    /// It has a long-term segment, or a geo topic object names it.
    Geo,
    /// One of its objects is no newer than this cluster's claim of the root.
    BeforeClaim,
    Orphan,
}

/// The verdict on the incarnation `prefix`.
///
/// The listing found it as `listed`, next to the geo topic objects of
/// `topics`. `revision` is the one committed metadata had reached before the
/// listing, and `holds` says whether committed metadata holds a partition of
/// exactly that incarnation.
#[must_use]
pub fn verdict(
    prefix: &ObjectPrefix,
    listed: &ListedIncarnation,
    topics: &BTreeSet<TopicKey>,
    revision: u64,
    holds: impl Fn(&ObjectPrefix) -> bool,
) -> Verdict {
    let topic = TopicKey {
        stream_id: prefix.stream_id,
        topic_id: prefix.topic_id,
        created_revision: prefix.created_revision,
    };
    if holds(prefix) {
        Verdict::Live
    } else if prefix.created_revision > revision {
        Verdict::AfterSnapshot
    } else if listed.long_term || topics.contains(&topic) {
        Verdict::Geo
    } else if listed.before_claim > 0 {
        Verdict::BeforeClaim
    } else {
        Verdict::Orphan
    }
}

/// The verdicts of one full sweep that matter.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepFindings {
    /// The orphans, with what the listing found of each.
    pub orphans: BTreeMap<ObjectPrefix, ListedIncarnation>,
    /// The incarnations left from before the claim, with how many of their
    /// objects are no newer than it.
    pub before_claim: BTreeMap<ObjectPrefix, usize>,
}

impl SweepFindings {
    /// The findings of `listing`, judged as [`verdict`] judges them.
    #[must_use]
    pub fn of(listing: &RootListing, revision: u64, holds: impl Fn(&ObjectPrefix) -> bool) -> Self {
        let mut findings = Self::default();
        for (prefix, listed) in &listing.incarnations {
            match verdict(prefix, listed, &listing.topics, revision, &holds) {
                Verdict::Orphan => {
                    findings.orphans.insert(*prefix, *listed);
                }
                Verdict::BeforeClaim => {
                    findings.before_claim.insert(*prefix, listed.before_claim);
                }
                Verdict::Live | Verdict::AfterSnapshot | Verdict::Geo => {}
            }
        }
        findings
    }

    /// The objects no newer than the claim, of the incarnations left from
    /// before it.
    #[must_use]
    pub fn objects_before_claim(&self) -> usize {
        self.before_claim.values().sum()
    }
}

/// What two full sweeps must share for their findings to count together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SweepStamp {
    /// The view of the metadata primary that ran the sweep.
    pub view: u32,
    pub claim: ClaimStamp,
}

/// The orphans of the last full sweep, kept in memory by the primary that
/// found them.
#[derive(Debug, Default)]
pub struct OrphanCandidates {
    stamp: Option<SweepStamp>,
    found: BTreeMap<ObjectPrefix, Candidate>,
}

/// What the sweeps in a row listed of one orphan.
#[derive(Debug, Clone, Copy)]
struct Candidate {
    newest: SystemTime,
    objects: usize,
    /// When the second sweep that listed the same objects started, by this
    /// node's monotonic clock. `None` after the first.
    since: Option<Instant>,
}

impl OrphanCandidates {
    /// Take the orphans that a full sweep stamped `stamp`, started at
    /// `started`, found, and return the ones to delete, each with the newest
    /// moment the store gave one of its objects.
    ///
    /// An orphan is due once every sweep in a row under the same stamp
    /// listed the same newest moment and number of objects for it, and
    /// `grace` passed from the start of the second of them to `started`. A
    /// sweep that lists it otherwise starts its count again.
    pub fn settle(
        &mut self,
        stamp: SweepStamp,
        orphans: &BTreeMap<ObjectPrefix, ListedIncarnation>,
        started: Instant,
        grace: Duration,
    ) -> Vec<(ObjectPrefix, SystemTime)> {
        let same_stamp = self.stamp.as_ref() == Some(&stamp);
        let mut due = Vec::new();
        let found = orphans
            .iter()
            .map(|(prefix, listed)| {
                let since = self
                    .found
                    .get(prefix)
                    .filter(|before| {
                        same_stamp
                            && before.newest == listed.newest
                            && before.objects == listed.objects
                    })
                    .map(|before| before.since.unwrap_or(started));
                if since.is_some_and(|since| started.saturating_duration_since(since) >= grace) {
                    due.push((*prefix, listed.newest));
                }
                let candidate = Candidate {
                    newest: listed.newest,
                    objects: listed.objects,
                    since,
                };
                (*prefix, candidate)
            })
            .collect();
        self.stamp = Some(stamp);
        self.found = found;
        due
    }

    /// Forget every orphan, so the next full sweep starts a new count.
    pub fn clear(&mut self) {
        *self = Self::default();
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.found.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.found.is_empty()
    }
}

/// What the delete of an orphan checks the store against before it deletes
/// anything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrphanGuard {
    /// The view of the metadata primary whose sweeps found the orphan. A
    /// turn of its delete starts only in that view.
    pub view: u32,
    /// The claim of the root that the sweeps which found the orphan read.
    pub claim: ClaimStamp,
    /// The newest moment the store gave an object of the orphan in those
    /// sweeps. No sweep judged an object newer than it.
    pub newest: SystemTime,
}

/// Why the delete of an orphan deleted nothing, because the store no longer
/// holds what the sweep judged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrphanRefusal {
    /// The store identity object is not the claim the sweep read.
    IdentityChanged,
    /// The incarnation has a long-term segment.
    LongTerm,
    /// A geo topic object names the incarnation.
    TopicObject,
    /// An object is newer than the newest one the sweeps judged.
    Recent,
    /// An object is no newer than the claim of the root.
    BeforeClaim,
}

impl OrphanGuard {
    /// Whether `read` is the claim the sweep read: the same cluster in the
    /// same object, committed at the same moment. A put of the same bytes
    /// can keep the etag and still move the claim.
    #[must_use]
    pub fn same_claim(&self, read: Option<&ClaimStamp>) -> bool {
        read == Some(&self.claim)
    }
}

/// What the listing of an orphan's objects found so far that keeps them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OrphanCheck {
    /// Whether an object of a long-term segment was listed.
    pub long_term: bool,
    /// Why the first object listed outside the moments of the guard stays.
    pub age: Option<OrphanRefusal>,
}

impl OrphanCheck {
    /// Take one listed object into the check: one the store committed at
    /// `modified`, of a long-term segment when `long_term`.
    pub fn take(&mut self, guard: &OrphanGuard, modified: SystemTime, long_term: bool) {
        self.long_term |= long_term;
        if self.age.is_none() {
            self.age = if no_newer_than_claim(modified, guard.claim.claimed_at) {
                Some(OrphanRefusal::BeforeClaim)
            } else if modified > guard.newest {
                Some(OrphanRefusal::Recent)
            } else {
                None
            };
        }
    }

    /// Why the objects taken must stay, `None` when they may go.
    /// `topic_object` says whether a geo topic object names the incarnation.
    /// A long-term segment comes first, then a topic object, then the first
    /// object outside the moments of the guard.
    #[must_use]
    pub const fn refusal(&self, topic_object: bool) -> Option<OrphanRefusal> {
        if self.long_term {
            Some(OrphanRefusal::LongTerm)
        } else if topic_object {
            Some(OrphanRefusal::TopicObject)
        } else {
            self.age
        }
    }
}

/// Where the delete of an orphan stands between two of its turns.
///
/// The turns of the check list every object of the orphan and delete
/// nothing. The turns after them list it again and delete what they list.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OrphanProgress {
    /// Whether the check found nothing that keeps the orphan.
    pub deleting: bool,
    /// Where the listing goes on, `None` at its start.
    pub after: Option<String>,
    /// What the turns of the check found so far.
    pub check: OrphanCheck,
}

/// What one turn of the delete of an orphan did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrphanDelete {
    /// The turn deleted nothing, for this reason, and the delete ends.
    Refused(OrphanRefusal),
    /// The next turn goes on from here.
    Continues(OrphanProgress),
    /// The store kept an object the turn asked it to delete, so the delete
    /// starts over later.
    Unfinished,
    /// No object of the incarnation is left.
    Deleted,
}

#[cfg(test)]
mod tests;
