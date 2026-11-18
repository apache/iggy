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
//! newer manifest adds segments at the end of its log or drops them from the
//! front.
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
use crate::segment::{Segment, SegmentLocation};
use crate::{IggyPartition, PollReadResult};

/// The partition incarnation of another cluster that a partition follows,
/// and what the manifests it applied listed.
#[derive(Debug, Clone)]
pub struct PeerLog {
    origin: String,
    prefix: ObjectPrefix,
    applied: Option<AppliedManifest>,
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
pub struct PeerManifestApplied {
    /// Segments added at the end of the log.
    pub appended: usize,
    /// Segments dropped from the front of the log.
    pub trimmed: usize,
    /// Where the polls of the dropped segments may have put a fetched
    /// `.index`. Nothing reads these files again, and
    /// [`remove_retired_indexes`] removes them.
    pub retired_indexes: Vec<String>,
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
        "the manifest goes back: floor {floor:?} and trim point {trim_point:?}, after floor {applied_floor:?} and trim point {applied_trim_point:?}"
    )]
    GoesBack {
        floor: Option<u64>,
        trim_point: Option<u64>,
        applied_floor: Option<u64>,
        applied_trim_point: Option<u64>,
    },
    #[error(
        "the manifest's segment at offset {start_offset} is out of order: it does not start above the trim point or right after the segment before it"
    )]
    Gap { start_offset: u64 },
    #[error("the manifest's segment at offset {start_offset} holds no bytes")]
    EmptySegment { start_offset: u64 },
    #[error("the manifest's floor {floor:?} is not where its segments end")]
    Floor { floor: Option<u64> },
    #[error(
        "the manifest lists the segment at offset {start_offset} unlike the manifests before it"
    )]
    Diverged { start_offset: u64 },
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

/// Which applied segments a manifest drops and which of its own it adds.
struct ManifestChange {
    trimmed: usize,
    /// The index in the manifest's segments of the first one the log does
    /// not hold yet.
    first_new: usize,
}

impl PeerLog {
    const fn new(origin: String, prefix: ObjectPrefix) -> Self {
        Self {
            origin,
            prefix,
            applied: None,
            segments: VecDeque::new(),
        }
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

    fn trim_point(&self) -> Option<u64> {
        self.applied.and_then(|applied| applied.trim_point)
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
        if let Some(applied) = self.applied
            && (manifest.floor < applied.floor || manifest.trim_point < applied.trim_point)
        {
            return Err(PeerManifestRefusal::GoesBack {
                floor: manifest.floor,
                trim_point: manifest.trim_point,
                applied_floor: applied.floor,
                applied_trim_point: applied.trim_point,
            });
        }
        let trimmed = self
            .segments
            .iter()
            .take_while(|segment| Some(segment.end_offset) <= manifest.trim_point)
            .count();
        // A segment that stays has to come back as it was. One that ends past
        // the trim point but starts at or below it was cut in two.
        for (at, kept) in self.segments.iter().skip(trimmed).enumerate() {
            if manifest.segments.get(at) != Some(kept) {
                return Err(PeerManifestRefusal::Diverged {
                    start_offset: kept.start_offset,
                });
            }
        }
        Ok(ManifestChange {
            trimmed,
            first_new: self.segments.len() - trimmed,
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

/// Remove the `.index` files that the polls of dropped segments fetched.
/// Best effort: a file that stays is never read again.
pub async fn remove_retired_indexes(applied: &PeerManifestApplied) {
    for path in &applied.retired_indexes {
        let _ = compio::fs::remove_file(path).await;
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
        if self.offset_space.committed_seeded
            || self.offset_space.append_live
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
        Ok(())
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

    /// Whether a trim dropped the first segment that `result` walked after
    /// its plan was built. The origin deletes the objects of a trimmed
    /// segment once a manifest leaves them out, so the read may have failed
    /// on a missing object, or it holds a range that is gone now. The owner
    /// refuses such a read as one that no replica accepted.
    #[must_use]
    pub fn trimmed_under_peer_read(&self, result: &PollReadResult) -> bool {
        result.context.peer_first_segment.is_some_and(|first| {
            self.peer_trim_point()
                .is_some_and(|trim_point| first <= trim_point)
        })
    }

    /// Bring the log to what `manifest` lists: drop the segments it trimmed
    /// from the front, and add the ones it lists past the log's end.
    ///
    /// A poll already planned keeps the segments it planned, so a grown log
    /// does not disturb it. A poll whose first segment a trim dropped is
    /// refused when it completes, and its retry plans from the new front.
    ///
    /// # Errors
    /// See [`PeerManifestRefusal`]. A manifest that goes back, names another
    /// incarnation, lists a segment unlike an earlier manifest, or has a gap
    /// changes nothing.
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
            self.retire_peer_segment(&segment, &mut applied);
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
    fn retire_peer_segment(&self, segment: &Segment, applied: &mut PeerManifestApplied) {
        // A fetch of its index that is still running publishes nothing now,
        // and one whose file lands after this removes it again.
        self.index_fetches.retire_through(segment.end_offset);
        let (_, shortfall) = settle_cleaned_segment(&self.stats, segment);
        shortfall.report(
            self.namespace(),
            Some((segment.start_offset, segment.end_offset)),
        );
        if let Some(directory) = self.partition_dir.as_deref() {
            applied
                .retired_indexes
                .push(format!("{directory}/{:0>20}.index", segment.start_offset));
        }
        applied.trimmed += 1;
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
