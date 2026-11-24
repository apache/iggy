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
//! listing ran is never one. An orphan is deleted once two full sweeps in a
//! row, under the same claim of the root and in the same view of the same
//! process, found it, and its newest object is older than the grace period
//! by the moment the store gives it.
//!
//! Whatever metadata holds, an incarnation stays when it has a long-term
//! segment or when a geo topic object names its topic incarnation, as the
//! sweep of geo topics owns those. It stays as well when one of its objects
//! is no newer than this cluster's claim of the root: a retired cluster left
//! it, and the operator decides.
//!
//! Nothing here does any I/O.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, SystemTime};

use iggy_object_store::{IdentityRead, KeyLayout, ListedObject, ObjectPrefix, TopicKey};

/// The longest time between the starts of two sweeps of the root.
///
/// A grace period that is shorter takes its place, so a short grace does not
/// wait an hour between the two sweeps a delete needs.
pub const ORPHAN_SWEEP_INTERVAL: Duration = Duration::from_hours(1);

/// The most pages of the root one listing job reads.
pub const ORPHAN_LIST_PAGES_PER_JOB: usize = 16;

/// The time between the starts of two sweeps with `grace`.
#[must_use]
pub fn sweep_interval(grace: Duration) -> Duration {
    grace.min(ORPHAN_SWEEP_INTERVAL)
}

/// The latest moment an object may carry at `now` and still be older than
/// `grace`. `None` when no moment is that old.
#[must_use]
pub fn quiet_since(now: SystemTime, grace: Duration) -> Option<SystemTime> {
    now.checked_sub(grace)
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
        if key.starts_with(&layout.meta_prefix()) {
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
    pub manifest: bool,
    pub long_term: bool,
    /// When the store committed the oldest and the newest of them.
    pub oldest: SystemTime,
    pub newest: SystemTime,
}

impl ListedIncarnation {
    const fn of(modified: SystemTime) -> Self {
        Self {
            objects: 0,
            manifest: false,
            long_term: false,
            oldest: modified,
            newest: modified,
        }
    }

    fn merge(&mut self, other: &Self) {
        self.objects += other.objects;
        self.manifest |= other.manifest;
        self.long_term |= other.long_term;
        self.oldest = self.oldest.min(other.oldest);
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
    /// Take one listed object into the listing.
    pub fn take(&mut self, layout: &KeyLayout, object: &ListedObject) {
        let (prefix, long_term, manifest) = match RootKey::of(layout, &object.key) {
            RootKey::Segment(prefix) => (prefix, false, false),
            RootKey::LongTerm(prefix) => (prefix, true, false),
            RootKey::Manifest(prefix) => (prefix, false, true),
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
            manifest,
            long_term,
            ..ListedIncarnation::of(object.last_modified)
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
/// listing, `claimed_at` the moment of this cluster's claim of the root, and
/// `holds` says whether committed metadata holds a partition of exactly that
/// incarnation.
#[must_use]
pub fn verdict(
    prefix: &ObjectPrefix,
    listed: &ListedIncarnation,
    topics: &BTreeSet<TopicKey>,
    revision: u64,
    claimed_at: SystemTime,
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
    } else if listed.oldest <= claimed_at {
        Verdict::BeforeClaim
    } else {
        Verdict::Orphan
    }
}

/// The verdicts of one full sweep that matter.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepFindings {
    /// The orphans, with the moment of the newest object of each.
    pub orphans: BTreeMap<ObjectPrefix, SystemTime>,
    /// The incarnations left from before the claim, with their objects.
    pub before_claim: BTreeMap<ObjectPrefix, usize>,
}

impl SweepFindings {
    /// The findings of `listing`, judged as [`verdict`] judges them.
    #[must_use]
    pub fn of(
        listing: &RootListing,
        revision: u64,
        claimed_at: SystemTime,
        holds: impl Fn(&ObjectPrefix) -> bool,
    ) -> Self {
        let mut findings = Self::default();
        for (prefix, listed) in &listing.incarnations {
            match verdict(
                prefix,
                listed,
                &listing.topics,
                revision,
                claimed_at,
                &holds,
            ) {
                Verdict::Orphan => {
                    findings.orphans.insert(*prefix, listed.newest);
                }
                Verdict::BeforeClaim => {
                    findings.before_claim.insert(*prefix, listed.objects);
                }
                Verdict::Live | Verdict::AfterSnapshot | Verdict::Geo => {}
            }
        }
        findings
    }

    /// The objects of the incarnations left from before the claim.
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
    found: BTreeSet<ObjectPrefix>,
}

impl OrphanCandidates {
    /// Take the orphans that a full sweep stamped `stamp` found, and return
    /// the ones to delete: those the full sweep before it found under the
    /// same stamp, whose newest object is no newer than `quiet_since`.
    pub fn settle(
        &mut self,
        stamp: SweepStamp,
        orphans: &BTreeMap<ObjectPrefix, SystemTime>,
        quiet_since: Option<SystemTime>,
    ) -> Vec<ObjectPrefix> {
        let due = match (&self.stamp, quiet_since) {
            (Some(last), Some(quiet_since)) if *last == stamp => orphans
                .iter()
                .filter(|(prefix, newest)| self.found.contains(prefix) && **newest <= quiet_since)
                .map(|(prefix, _)| *prefix)
                .collect(),
            _ => Vec::new(),
        };
        self.stamp = Some(stamp);
        self.found = orphans.keys().copied().collect();
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
    /// The claim of the root that the sweeps which found the orphan read.
    pub claim: ClaimStamp,
    /// The latest moment an object of the orphan may carry.
    pub quiet_since: SystemTime,
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
    /// An object is newer than the grace period allows.
    Recent,
    /// An object is no newer than the claim of the root.
    BeforeClaim,
}

impl OrphanGuard {
    /// Whether `read`, from an object with `etag`, is the claim the sweep
    /// read: an identity of the same cluster in the same object.
    #[must_use]
    pub fn same_claim(&self, read: &IdentityRead, etag: Option<&str>) -> bool {
        matches!(read, IdentityRead::Found(identity) if identity.cluster_uid == self.claim.cluster_uid)
            && etag == Some(self.claim.etag.as_str())
    }

    /// Why the objects the delete found must stay, `None` when they may go.
    /// `modified` holds the moment of each of them, `long_term` whether one
    /// is of a long-term segment, and `topic_object` whether a geo topic
    /// object names the incarnation.
    #[must_use]
    pub fn refusal(
        &self,
        modified: impl IntoIterator<Item = SystemTime>,
        long_term: bool,
        topic_object: bool,
    ) -> Option<OrphanRefusal> {
        if long_term {
            return Some(OrphanRefusal::LongTerm);
        }
        if topic_object {
            return Some(OrphanRefusal::TopicObject);
        }
        for moment in modified {
            if moment <= self.claim.claimed_at {
                return Some(OrphanRefusal::BeforeClaim);
            }
            if moment > self.quiet_since {
                return Some(OrphanRefusal::Recent);
            }
        }
        None
    }
}

/// What the delete of an orphan did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrphanDelete {
    /// It deleted nothing, for this reason.
    Refused(OrphanRefusal),
    /// How many objects of the incarnation are still to delete.
    Deleted { left: usize },
}

#[cfg(test)]
mod tests;
