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

//! The log of a partition whose messages live in another cluster's store.
//!
//! A geo topic of another datacenter appears in this cluster as partitions
//! that hold none of its messages. Each one serves the segments that the
//! origin's manifest lists, from the origin's store, under the origin's
//! prefix and at the origin's offsets. Such a partition takes no send and
//! keeps no chain. It uploads, evicts, retains and deletes nothing: only a
//! newer manifest adds segments at the end of its log, drops them from the
//! front, or lists one segment in the place of several.
//!
//! An origin that places its batches merges small segments into one, which
//! holds the bytes of its sources back to back under a key of its own. The
//! origin keeps the objects of the sources for a while after the manifest
//! that leaves them out, and a poll that walked one of them is refused when
//! it completes.
//!
//! The origin overwrites its manifest on every change, so a partition sees
//! only some of them. The manifest alone says which segments exist, and the
//! log moves to any newer one that agrees with it: every offset where a
//! listed segment starts or ends, among the offsets the log holds, is one
//! where a segment of the log starts or ends. A listed segment over segments
//! of the log has to be what they add up to. One that also holds offsets
//! past the log's end merges segments the partition never saw, and is held
//! to what the segments of the log under it can show.
//!
//! The log ends with an empty tail that has no files. A commit of a consumer
//! offset flushes the journal through the last segment of the log, so the
//! tail takes the place of the active segment of a partition that writes. It
//! never holds a byte.
//!
//! Applying a manifest changes memory only, and the same manifests applied in
//! the same order give the same log on every replica.

use std::collections::VecDeque;
use std::sync::atomic::Ordering;

use iggy_common::IggyByteSize;
use iggy_object_store::{ManifestSegment, ObjectPrefix, PartitionManifest};
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use server_common::SegmentStorage;

use crate::iggy_partition::settle_cleaned_segment;
use crate::long_term::concatenated;
use crate::poll_plan::PeerSegmentRun;
use crate::remote_segments::index_file_name;
use crate::segment::{Segment, SegmentLocation};
use crate::{IggyPartition, PollReadResult};

/// The partition incarnation of another cluster that a partition follows,
/// and what the manifests it applied listed.
#[derive(Debug, Clone)]
pub struct PeerLog {
    origin: String,
    prefix: ObjectPrefix,
    applied: Option<AppliedManifest>,
    /// Whether the keys of the segments' objects name both their offsets,
    /// as the manifests that listed them said.
    keyed_by_both_offsets: bool,
    /// The remote segments of the log, oldest first, as the manifests listed
    /// them. The log holds a [`Segment`] for each one, then its tail.
    segments: VecDeque<ManifestSegment>,
}

/// What the last manifest that a partition applied said of the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppliedManifest {
    /// When the origin's primary built it, in microseconds since the Unix
    /// epoch.
    pub published_at: u64,
    /// The last offset the store holds, `None` while it held no segment.
    pub floor: Option<u64>,
    /// Every offset at or below it is gone for good.
    pub trim_point: Option<u64>,
}

/// What applying a manifest changed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[must_use = "the fetched indexes of the segments that left the log stay on disk until `remove_retired_indexes` runs"]
pub struct PeerManifestApplied {
    /// Segments added at the end of the log.
    pub appended: usize,
    /// Segments dropped from the front of the log.
    pub trimmed: usize,
    /// Segments that left the log because a listed one holds their offsets.
    /// The listed one may hold offsets past theirs too.
    pub replaced: usize,
    retired_indexes: Vec<String>,
}

impl PeerManifestApplied {
    /// Where the polls of the dropped and the replaced segments may have
    /// put a fetched `.index`. Nothing reads these files again.
    #[must_use]
    pub fn retired_indexes(&self) -> &[String] {
        &self.retired_indexes
    }

    /// Remove the `.index` files that the polls of dropped and replaced
    /// segments fetched.
    /// Best effort: a file that stays is never read again, and the next
    /// start of the partition sweeps it.
    pub async fn remove_retired_indexes(self) {
        for path in &self.retired_indexes {
            let _ = compio::fs::remove_file(path).await;
        }
    }
}

/// Why a partition did not apply a manifest. A refused manifest changes
/// nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PeerManifestRefusal {
    #[error("the partition follows no other cluster's log")]
    NotFollowing,
    #[error("the manifest describes {found:?}, and the partition follows {expected:?}")]
    OtherIncarnation {
        expected: ObjectPrefix,
        found: ObjectPrefix,
    },
    #[error(
        "the manifest is older than the applied one: floor {floor:?}, trim point {trim_point:?} and {segments} segments, after floor {applied_floor:?}, trim point {applied_trim_point:?} and {applied_segments} segments"
    )]
    GoesBack {
        floor: Option<u64>,
        trim_point: Option<u64>,
        segments: usize,
        applied_floor: Option<u64>,
        applied_trim_point: Option<u64>,
        applied_segments: usize,
    },
    #[error(
        "the manifest keys its segments by both offsets: {found}, and the segments of the log are keyed so: {expected}"
    )]
    KeyScheme { expected: bool, found: bool },
    #[error(
        "the manifest's segment at offset {start_offset} is out of order: it does not start above the trim point or right after the segment before it"
    )]
    Gap { start_offset: u64 },
    #[error("the manifest's segment at offset {start_offset} holds no bytes")]
    EmptySegment { start_offset: u64 },
    #[error("the manifest's floor {floor:?} is not where its segments end")]
    Floor { floor: Option<u64> },
    #[error(
        "the manifest contradicts the manifests before it at the log's segment at offset {start_offset}: {contradiction}"
    )]
    Diverged {
        start_offset: u64,
        contradiction: PeerManifestContradiction,
    },
}

/// How a manifest that is not older than the applied one disagrees with the
/// segments of the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PeerManifestContradiction {
    #[error("the listed segments do not start where it starts")]
    StartsOffBoundary,
    #[error("a listed segment ends inside a segment of the log")]
    EndsOffBoundary,
    #[error("the listed segment over it is not what the segments it covers add up to")]
    DoesNotAddUp,
    #[error("a segment keyed by its first offset alone cannot take the place of others")]
    MergesUnderOneKey,
}

/// Why a partition cannot follow another cluster's log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PeerLogError {
    #[error("the partition follows another cluster's log already")]
    Following,
    #[error("the partition holds messages of its own")]
    HoldsMessages,
    #[error("the partition keeps a chain or is turning tiering on")]
    Tiered,
    #[error("the partition places its batches")]
    PlacesBatches,
}

/// Which applied segments a manifest drops and replaces, and which of its
/// own it adds.
struct ManifestChange {
    trimmed: usize,
    /// Oldest first.
    covers: Vec<Cover>,
    /// The index in the manifest's segments of the first one that holds no
    /// offset of the log.
    first_new: usize,
}

/// One listed segment that holds the offsets of applied ones, which are not
/// that segment alone.
struct Cover {
    /// Where the first of the applied segments is in the log, once the
    /// trimmed ones left it.
    at: usize,
    sources: usize,
    listed: ManifestSegment,
    /// What the listed segment holds past the log's end. Nothing when it
    /// merges applied segments alone.
    unseen: Unseen,
}

/// The part of a listed segment that no applied segment holds.
#[derive(Clone, Copy, Default)]
struct Unseen {
    bytes: u64,
    messages: u64,
}

impl PeerLog {
    const fn new(origin: String, prefix: ObjectPrefix) -> Self {
        Self {
            origin,
            prefix,
            applied: None,
            keyed_by_both_offsets: false,
            segments: VecDeque::new(),
        }
    }

    /// The log that `manifest` lists, for a check of what a later one
    /// changes. Its origin is unnamed, which no check reads.
    pub(crate) fn of_manifest(prefix: ObjectPrefix, manifest: Option<&PartitionManifest>) -> Self {
        let Some(manifest) = manifest else {
            return Self::new(String::new(), prefix);
        };
        Self {
            origin: String::new(),
            prefix,
            applied: Some(AppliedManifest {
                published_at: manifest.published_at,
                floor: manifest.floor,
                trim_point: manifest.trim_point,
            }),
            keyed_by_both_offsets: manifest.segments_keyed_by_both_offsets,
            segments: manifest.segments.iter().copied().collect(),
        }
    }

    /// Whether the log takes `manifest`, see
    /// [`IggyPartition::apply_peer_manifest`].
    ///
    /// # Errors
    /// The refusal the apply would return.
    pub(crate) fn check(&self, manifest: &PartitionManifest) -> Result<(), PeerManifestRefusal> {
        self.change(manifest).map(|_| ())
    }

    /// The name of the peer whose store holds the log.
    #[must_use]
    pub fn origin(&self) -> &str {
        &self.origin
    }

    /// Where the origin keeps the objects of the incarnation.
    #[must_use]
    pub const fn prefix(&self) -> ObjectPrefix {
        self.prefix
    }

    /// What the last applied manifest said, `None` before the first.
    #[must_use]
    pub const fn applied(&self) -> Option<AppliedManifest> {
        self.applied
    }

    /// Whether the keys of the segments' objects name both their offsets.
    /// `false` while the log holds no segment.
    #[must_use]
    pub const fn keyed_by_both_offsets(&self) -> bool {
        self.keyed_by_both_offsets
    }

    fn trim_point(&self) -> Option<u64> {
        self.applied.and_then(|applied| applied.trim_point)
    }

    /// Whether the log still holds every segment of `run` as a plan saw it.
    /// Offsets where segments start and end only leave the log, so the same
    /// number of segments between the same two ends are the same segments.
    fn holds(&self, run: &PeerSegmentRun) -> bool {
        self.segments
            .binary_search_by_key(&run.start_offset, |segment| segment.start_offset)
            .ok()
            .and_then(|first| self.segments.get(first + run.segments.checked_sub(1)?))
            .is_some_and(|last| last.end_offset == run.end_offset)
    }

    /// The applied segments from `first` on that the listed segment `entry`
    /// holds, where `entry` starts with the one at `first` and is not that
    /// one, or how `entry` contradicts them.
    ///
    /// An entry that ends where an applied segment ends merges applied
    /// segments alone, and its fields have to be what they add up to. The
    /// hash of the merged bytes is taken as listed, which the hashes of the
    /// sources cannot give.
    ///
    /// An entry that ends past the log's end also merges segments that
    /// earlier manifests listed and this partition never saw. Their sizes and
    /// checksums are unknown, so the entry is held to what the applied ones
    /// decide: its first timestamp, and bounds on its size and its newest
    /// timestamp.
    fn cover(
        &self,
        first: usize,
        entry: &ManifestSegment,
    ) -> Result<Cover, PeerManifestContradiction> {
        let covered: Vec<ManifestSegment> = self
            .segments
            .range(first..)
            .take_while(|segment| segment.end_offset <= entry.end_offset)
            .copied()
            .collect();
        let floor = self.segments.back().map(|segment| segment.end_offset);
        let Some(last) = covered.last() else {
            return Err(PeerManifestContradiction::EndsOffBoundary);
        };
        let unseen = if last.end_offset == entry.end_offset {
            // One segment in the place of another alone is other bytes
            // under the key of the first.
            if covered.len() == 1 {
                return Err(PeerManifestContradiction::DoesNotAddUp);
            }
            if !self.keyed_by_both_offsets {
                return Err(PeerManifestContradiction::MergesUnderOneKey);
            }
            if concatenated(&covered, entry.hash) != Some(*entry) {
                return Err(PeerManifestContradiction::DoesNotAddUp);
            }
            Unseen::default()
        } else if Some(last.end_offset) == floor {
            if !self.keyed_by_both_offsets {
                return Err(PeerManifestContradiction::MergesUnderOneKey);
            }
            let seen = concatenated(&covered, entry.hash)
                .ok_or(PeerManifestContradiction::DoesNotAddUp)?;
            if entry.start_timestamp != seen.start_timestamp
                || entry.size <= seen.size
                || entry.max_timestamp < seen.max_timestamp
            {
                return Err(PeerManifestContradiction::DoesNotAddUp);
            }
            Unseen {
                bytes: entry.size - seen.size,
                messages: entry.end_offset - seen.end_offset,
            }
        } else {
            return Err(PeerManifestContradiction::EndsOffBoundary);
        };
        Ok(Cover {
            at: first,
            sources: covered.len(),
            listed: *entry,
            unseen,
        })
    }

    /// What `manifest` changes, or why it changes nothing.
    fn change(&self, manifest: &PartitionManifest) -> Result<ManifestChange, PeerManifestRefusal> {
        if manifest.prefix != self.prefix {
            return Err(PeerManifestRefusal::OtherIncarnation {
                expected: self.prefix,
                found: manifest.prefix,
            });
        }
        check_shape(manifest)?;
        // The key of a segment the log holds already cannot change.
        if !self.segments.is_empty()
            && manifest.segments_keyed_by_both_offsets != self.keyed_by_both_offsets
        {
            return Err(PeerManifestRefusal::KeyScheme {
                expected: self.keyed_by_both_offsets,
                found: manifest.segments_keyed_by_both_offsets,
            });
        }
        // The floor and the trim point only rise. While neither moves, the
        // origin only merges, so of two manifests the one that lists more
        // segments is the older.
        if let Some(applied) = self.applied
            && (manifest.floor < applied.floor
                || manifest.trim_point < applied.trim_point
                || (manifest.floor == applied.floor
                    && manifest.trim_point == applied.trim_point
                    && manifest.segments.len() > self.segments.len()))
        {
            return Err(PeerManifestRefusal::GoesBack {
                floor: manifest.floor,
                trim_point: manifest.trim_point,
                segments: manifest.segments.len(),
                applied_floor: applied.floor,
                applied_trim_point: applied.trim_point,
                applied_segments: self.segments.len(),
            });
        }
        let trimmed = self
            .segments
            .iter()
            .take_while(|segment| Some(segment.end_offset) <= manifest.trim_point)
            .count();
        // The origin trims whole segments and merges whole segments, so the
        // first segment that stays starts the listed ones. One that ends
        // past the trim point but starts at or below it was cut in two.
        if let Some(kept) = self.segments.get(trimmed)
            && manifest.segments.first().map(|entry| entry.start_offset) != Some(kept.start_offset)
        {
            return Err(PeerManifestRefusal::Diverged {
                start_offset: kept.start_offset,
                contradiction: PeerManifestContradiction::StartsOffBoundary,
            });
        }
        // A segment that stays has to come back as it was, or inside a
        // listed segment that merges it with its neighbours.
        let mut covers = Vec::new();
        let mut listed = 0;
        let mut at = trimmed;
        while let Some(kept) = self.segments.get(at) {
            let diverged = |contradiction| PeerManifestRefusal::Diverged {
                start_offset: kept.start_offset,
                contradiction,
            };
            let entry = manifest
                .segments
                .get(listed)
                .ok_or_else(|| diverged(PeerManifestContradiction::EndsOffBoundary))?;
            let sources = if entry == kept {
                1
            } else {
                let cover = self.cover(at, entry).map_err(diverged)?;
                let sources = cover.sources;
                covers.push(Cover {
                    at: at - trimmed,
                    ..cover
                });
                sources
            };
            at += sources;
            listed += 1;
        }
        Ok(ManifestChange {
            trimmed,
            covers,
            first_new: listed,
        })
    }
}

/// What a manifest has to hold for its segments to be one run of offsets
/// above its trim point: each segment holds bytes and starts right after the
/// one before it, and the floor is where the last one ends, or the trim
/// point when it lists none. A decoded manifest already starts above its
/// trim point, and one built in memory is checked the same way.
fn check_shape(manifest: &PartitionManifest) -> Result<(), PeerManifestRefusal> {
    let mut previous_end = manifest.trim_point;
    for (at, segment) in manifest.segments.iter().enumerate() {
        let follows = match previous_end {
            Some(end) if at > 0 => end.checked_add(1) == Some(segment.start_offset),
            Some(trim_point) => segment.start_offset > trim_point,
            None => true,
        };
        if !follows || segment.end_offset < segment.start_offset {
            return Err(PeerManifestRefusal::Gap {
                start_offset: segment.start_offset,
            });
        }
        // A poll walks past a segment without bytes, so its offsets would
        // vanish from the log.
        if segment.size == 0 {
            return Err(PeerManifestRefusal::EmptySegment {
                start_offset: segment.start_offset,
            });
        }
        previous_end = Some(segment.end_offset);
    }
    let ends_at = manifest
        .segments
        .last()
        .map(|segment| segment.end_offset)
        .or(manifest.trim_point);
    if manifest.floor != ends_at {
        return Err(PeerManifestRefusal::Floor {
            floor: manifest.floor,
        });
    }
    Ok(())
}

/// The segment of the log that stands for a listed one.
fn remote_segment(listed: &ManifestSegment, max_size: IggyByteSize) -> Segment {
    let mut segment = Segment::new(listed.start_offset, max_size);
    segment.sealed = true;
    segment.location = SegmentLocation::Remote;
    segment.start_timestamp = listed.start_timestamp;
    segment.end_timestamp = listed.end_timestamp;
    segment.max_timestamp = listed.max_timestamp;
    segment.end_offset = listed.end_offset;
    segment.size = IggyByteSize::from(listed.size);
    segment.current_position = listed.size;
    segment
}

impl PeerSegmentRun {
    /// The remote segments among `walked`, which a plan of a peer log reads
    /// from the store. `None` when it reads none.
    pub(crate) fn of(walked: &[Segment]) -> Option<Self> {
        let mut remote = walked
            .iter()
            .filter(|segment| segment.location == SegmentLocation::Remote);
        let first = remote.next()?;
        let (last, more) = remote.fold((first, 0), |(_, more), segment| (segment, more + 1));
        Some(Self {
            start_offset: first.start_offset,
            end_offset: last.end_offset,
            segments: 1 + more,
        })
    }
}

impl<B, SB> IggyPartition<B, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    /// Make this partition follow the incarnation `prefix` of the peer
    /// `origin`: from here on its log is what that incarnation's manifests
    /// list, read from the peer's store, and it takes no send.
    ///
    /// Call it before the partition takes its first request. It drops the
    /// log's empty segments, which hold no byte, and starts the log with an
    /// empty tail at offset 0. It also drops the store the partition reads
    /// remote segments from, so the shard hands it the peer's.
    ///
    /// # Errors
    /// See [`PeerLogError`].
    pub fn follow_peer_log(
        &mut self,
        origin: impl Into<String>,
        prefix: ObjectPrefix,
    ) -> Result<(), PeerLogError> {
        if self.peer_log.is_some() {
            return Err(PeerLogError::Following);
        }
        if self.places_batches() {
            return Err(PeerLogError::PlacesBatches);
        }
        if self.is_tiered() || self.pending_tiering().is_some() {
            return Err(PeerLogError::Tiered);
        }
        // A partition that committed peer manifests before a restart counts
        // its offsets from them, and still holds no message of its own.
        let counts_peer_offsets = self.committed_peer_manifest().is_some();
        if (!counts_peer_offsets
            && (self.offset_space.committed_seeded || self.offset_space.append_live))
            || self
                .log
                .segments()
                .iter()
                .any(|segment| segment.size.as_bytes_u64() > 0)
        {
            return Err(PeerLogError::HoldsMessages);
        }
        let mut retired = 0;
        while self.log.retire_back().is_some() {
            retired += 1;
        }
        let _ = self.stats.decrement_segments_count(retired);
        self.plant_peer_tail(0);
        self.stats.increment_segments_count(1);
        self.clear_remote_segments();
        self.peer_log = Some(Box::new(PeerLog::new(origin.into(), prefix)));
        self.follow_committed_peer_manifest();
        Ok(())
    }

    /// Take the `.index` files that the polls of segments the committed peer
    /// manifests took out of the log may have fetched. Nothing reads them
    /// again, so the owner removes them when it can.
    pub fn take_retired_peer_indexes(&mut self) -> Vec<String> {
        std::mem::take(&mut self.retired_peer_indexes)
    }

    /// The other cluster's log this partition follows, `None` for a partition
    /// that holds its own messages.
    #[must_use]
    pub fn peer_log(&self) -> Option<&PeerLog> {
        self.peer_log.as_deref()
    }

    /// The trim point of the last applied manifest, `None` for a partition
    /// that follows no other cluster's log or whose origin trimmed nothing.
    pub(crate) fn peer_trim_point(&self) -> Option<u64> {
        self.peer_log.as_deref().and_then(PeerLog::trim_point)
    }

    /// Whether the segments of the remote objects that a poll plan names
    /// are keyed by both their offsets: those of a peer log whose manifests
    /// say so. The remote segments of a partition of this cluster are keyed
    /// by their first offset alone.
    pub(crate) fn reads_segments_keyed_by_both_offsets(&self) -> bool {
        self.peer_log
            .as_deref()
            .is_some_and(PeerLog::keyed_by_both_offsets)
    }

    /// Whether a manifest took a segment that `result` walked out of the
    /// peer log after its plan was built: a trim dropped the first one, or
    /// one listed segment took the place of any of them. The origin deletes
    /// the objects of such a segment once a manifest leaves them out, so the
    /// read may have failed on a missing object, or it holds a range that is
    /// gone now. The owner refuses such a read as one that no replica
    /// accepted, and the retry plans from the log as it is.
    #[must_use]
    pub fn peer_log_moved_under_read(&self, result: &PollReadResult) -> bool {
        let trimmed = result.context.peer_first_segment.is_some_and(|first| {
            self.peer_trim_point()
                .is_some_and(|trim_point| first <= trim_point)
        });
        let replaced = result.context.peer_segments.is_some_and(|run| {
            self.peer_log
                .as_deref()
                .is_some_and(|peer| !peer.holds(&run))
        });
        trimmed || replaced
    }

    /// Bring the log to what `manifest` lists: drop the segments it trimmed
    /// from the front, put each segment that merges several in their place,
    /// and add the ones it lists past the log's end. The manifest may be any
    /// number of the origin's changes ahead of the applied one.
    ///
    /// A poll already planned keeps the segments it planned, so a grown log
    /// does not disturb it. A poll whose first segment a trim dropped, or
    /// one of whose segments a merge replaced, is refused when it completes,
    /// and its retry plans from the log as it is.
    ///
    /// The counts of bytes and messages grow by what the manifest lists past
    /// the log's end and by nothing else: a listed segment holds what the
    /// segments it replaces held.
    ///
    /// The polls of the segments that left the log may have fetched their
    /// `.index`, so remove those files with
    /// [`PeerManifestApplied::remove_retired_indexes`] after every manifest.
    ///
    /// # Errors
    /// See [`PeerManifestRefusal`]. A manifest changes nothing when it is
    /// older than the applied one ([`PeerManifestRefusal::GoesBack`]), when
    /// it contradicts the segments of the log
    /// ([`PeerManifestRefusal::Diverged`]), when it names another
    /// incarnation or keys its segments unlike the ones the log holds, and
    /// when its own segments are no run of offsets.
    pub fn apply_peer_manifest(
        &mut self,
        manifest: &PartitionManifest,
    ) -> Result<PeerManifestApplied, PeerManifestRefusal> {
        let mut peer = self
            .peer_log
            .take()
            .ok_or(PeerManifestRefusal::NotFollowing)?;
        let change = match peer.change(manifest) {
            Ok(change) => change,
            Err(refusal) => {
                self.peer_log = Some(peer);
                return Err(refusal);
            }
        };
        let mut applied = PeerManifestApplied::default();
        for _ in 0..change.trimmed {
            peer.segments.pop_front();
            let Some((segment, _)) = self.log.retire_front() else {
                break;
            };
            debug_assert_eq!(segment.location, SegmentLocation::Remote);
            self.retire_peer_segment(&segment, peer.keyed_by_both_offsets, &mut applied);
        }
        // Newest first, so the places of the older ones stay as counted.
        for cover in change.covers.iter().rev() {
            self.cover_peer_segments(&mut peer, cover, &mut applied);
        }
        if peer.segments.is_empty() {
            peer.keyed_by_both_offsets = manifest.segments_keyed_by_both_offsets;
        }
        let added = &manifest.segments[change.first_new..];
        let next_offset = manifest.floor.map_or(0, |floor| floor.saturating_add(1));
        let tail_moves = self
            .log
            .segments()
            .last()
            .is_none_or(|tail| tail.start_offset != next_offset);
        if !added.is_empty() || tail_moves {
            let tail = self.log.retire_back();
            debug_assert!(
                tail.as_ref()
                    .is_none_or(|(tail, _)| !tail.sealed && tail.size.as_bytes_u64() == 0),
                "the last segment of a peer log is its empty tail"
            );
            let max_size = self.effective_segment_size();
            for listed in added {
                self.log.add_persisted_segment(
                    remote_segment(listed, max_size),
                    SegmentStorage::without_messages(None),
                    None,
                    None,
                );
                self.stats.increment_size_bytes(listed.size);
                self.stats
                    .increment_messages_count(listed.end_offset - listed.start_offset + 1);
                self.stats.increment_segments_count(1);
                self.committed_bytes = self.committed_bytes.saturating_add(listed.size);
                peer.segments.push_back(*listed);
                applied.appended += 1;
            }
            self.plant_peer_tail(next_offset);
        }
        // An older publish can land after a newer one, as the put of a
        // deposed primary does, so the latest time seen stays.
        let published_at = peer.applied.map_or(manifest.published_at, |applied| {
            applied.published_at.max(manifest.published_at)
        });
        peer.applied = Some(AppliedManifest {
            published_at,
            floor: manifest.floor,
            trim_point: manifest.trim_point,
        });
        self.peer_log = Some(peer);
        if let Some(floor) = manifest.floor {
            self.offset.store(floor, Ordering::Release);
            self.dirty_offset.store(floor, Ordering::Relaxed);
            self.offset_space.append_live = true;
            self.offset_space.committed_seeded = true;
        }
        self.publish_current_offset();
        self.publish_poll_visibility();
        Ok(applied)
    }

    /// Forget a segment a trim dropped from the front of the log.
    fn retire_peer_segment(
        &self,
        segment: &Segment,
        keyed_by_both_offsets: bool,
        applied: &mut PeerManifestApplied,
    ) {
        // A fetch of its index that is still running publishes nothing now,
        // and one whose file lands after this removes it again.
        self.index_fetches.retire_through(segment.end_offset);
        let (_, shortfall) = settle_cleaned_segment(&self.stats, segment);
        shortfall.report(
            self.namespace(),
            Some((segment.start_offset, segment.end_offset)),
        );
        self.retire_peer_index(segment, keyed_by_both_offsets, applied);
        applied.trimmed += 1;
    }

    /// Put the segment of `cover` in the place of the segments it holds.
    fn cover_peer_segments(
        &mut self,
        peer: &mut PeerLog,
        cover: &Cover,
        applied: &mut PeerManifestApplied,
    ) {
        let slots = cover.at..cover.at + cover.sources;
        let max_size = self.effective_segment_size();
        let replaced = self.log.replace_segments(
            slots.clone(),
            remote_segment(&cover.listed, max_size),
            SegmentStorage::without_messages(None),
        );
        debug_assert!(
            replaced.len() == cover.sources
                && replaced
                    .iter()
                    .all(|segment| segment.location == SegmentLocation::Remote),
            "a listed segment replaces remote segments of the peer log"
        );
        peer.segments.drain(slots);
        peer.segments.insert(cover.at, cover.listed);
        // The listed segment starts where the first of them did, so a fetch
        // of that one's index must not pass for a fetch of its own.
        self.index_fetches
            .replace_segments(cover.listed.start_offset..=cover.listed.end_offset);
        self.stats.increment_size_bytes(cover.unseen.bytes);
        self.stats.increment_messages_count(cover.unseen.messages);
        self.committed_bytes = self.committed_bytes.saturating_add(cover.unseen.bytes);
        for segment in &replaced {
            self.retire_peer_index(segment, peer.keyed_by_both_offsets, applied);
        }
        let _ = self
            .stats
            .decrement_segments_count(u32::try_from(replaced.len() - 1).unwrap_or(u32::MAX));
        applied.replaced += replaced.len();
    }

    /// Name the fetched `.index` of a segment that left the log, for
    /// [`PeerManifestApplied::remove_retired_indexes`].
    fn retire_peer_index(
        &self,
        segment: &Segment,
        keyed_by_both_offsets: bool,
        applied: &mut PeerManifestApplied,
    ) {
        if let Some(directory) = self.partition_dir.as_deref() {
            let name = index_file_name(
                segment.start_offset,
                keyed_by_both_offsets.then_some(segment.end_offset),
            );
            applied.retired_indexes.push(format!("{directory}/{name}"));
        }
    }

    /// Put the empty tail of a peer log at `start_offset`, after its remote
    /// segments. It has no files and takes no writes.
    fn plant_peer_tail(&mut self, start_offset: u64) {
        let max_size = self.effective_segment_size();
        self.log.add_persisted_segment(
            Segment::new(start_offset, max_size),
            SegmentStorage::default(),
            None,
            None,
        );
    }
}
