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

//! The peer manifests that a partition of a shadow topic committed: the one
//! record of its log that every replica agrees on.
//!
//! A partition that follows a peer's log, see [`crate::PeerLog`], learns of
//! the peer's segments from the manifests that the peer publishes. Its
//! primary reads a changed manifest and commits it through the partition's
//! consensus as one or more [`PeerManifestDelta`] commands. Every replica
//! applies them in op order, so every replica serves the same range, and a
//! restart replays them or restores them from a checkpoint.
//!
//! A command lists only the segments after the last one that stayed as the
//! applied manifest listed it. A replica rebuilds the whole manifest from
//! what it applied and checks it as the peer log does. A large catch-up
//! takes several commands: each lists the next run of new segments and ends
//! the manifest there, which is a manifest the peer published earlier.
//!
//! The machine holds the last applied manifest whole. The partition's peer
//! log follows it after every change, and again whenever the partition
//! starts to follow its peer.

use iggy_binary_protocol::RoutedRequestHeader;
use iggy_common::{IggyExpiry, IggyTimestamp};
use iggy_object_store::{ManifestSegment, ObjectPrefix, PartitionManifest};
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use server_common::Message;

use crate::IggyPartition;
use crate::iggy_partition::retention_end;
use crate::long_term::{SEGMENT_PAYLOAD_SIZE, decode_segment, encode_segment};
use crate::peer_log::{PeerLog, PeerManifestRefusal};
use crate::state_machine::{
    Applied, COMMAND_BYTES_MAX, CommandEncodeError, CommandKind, MachineCommand, MachineId,
    MachineRestoreError, PartitionStateMachine,
};

/// The machine's id in snapshots, stable within a storage format.
pub const PEER_MANIFEST_MACHINE: MachineId = MachineId(3);

/// The command kind of [`PeerManifestDelta`].
pub const FOLLOW_PEER_MANIFEST: CommandKind = CommandKind(7);

/// `stream_id | topic_id | partition_id | created_revision | published_at |
/// flags | floor | trim_point`, all `u64`, then `kept u32 | count u32`,
/// little-endian.
const DELTA_HEADER_LEN: usize = size_of::<[u64; 8]>() + 2 * size_of::<u32>();

/// The most segments one command lists, so it stays within the size of a
/// command.
pub const DELTA_SEGMENTS_MAX: usize =
    (COMMAND_BYTES_MAX - crate::state_machine::MACHINE_COMMAND_HEADER_SIZE - DELTA_HEADER_LEN)
        / SEGMENT_PAYLOAD_SIZE;

/// The most segments a committed manifest lists, which bounds the machine's
/// snapshot well below what a partition's machines may write together.
pub const PEER_MANIFEST_SEGMENTS_MAX: usize = 65_536;

const HAS_FLOOR: u64 = 1;
const HAS_TRIM_POINT: u64 = 1 << 1;
const KEYED_BY_BOTH_OFFSETS: u64 = 1 << 2;

/// One step from the applied manifest to a newer one of the same partition
/// incarnation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerManifestDelta {
    pub prefix: ObjectPrefix,
    pub published_at: u64,
    pub floor: Option<u64>,
    pub trim_point: Option<u64>,
    pub segments_keyed_by_both_offsets: bool,
    /// How many of the applied segments above `trim_point` the manifest
    /// keeps as they were, from the first one on.
    pub kept: u32,
    /// The segments that the manifest lists after the kept ones.
    pub segments: Vec<ManifestSegment>,
}

/// Why a fetched manifest cannot be committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PeerManifestCommitError {
    #[error(transparent)]
    Refused(#[from] PeerManifestRefusal),
    #[error(
        "the manifest changes {changed} segments at or below the applied floor, more than the {DELTA_SEGMENTS_MAX} one command lists"
    )]
    TooManyChanged { changed: usize },
    #[error("the manifest lists {segments} segments, more than {PEER_MANIFEST_SEGMENTS_MAX}")]
    TooManySegments { segments: usize },
    #[error(transparent)]
    Encode(#[from] CommandEncodeError),
}

impl PeerManifestDelta {
    /// The command payload.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut payload = vec![0; DELTA_HEADER_LEN + self.segments.len() * SEGMENT_PAYLOAD_SIZE];
        let flags = (if self.floor.is_some() { HAS_FLOOR } else { 0 })
            | (if self.trim_point.is_some() {
                HAS_TRIM_POINT
            } else {
                0
            })
            | (if self.segments_keyed_by_both_offsets {
                KEYED_BY_BOTH_OFFSETS
            } else {
                0
            });
        let integers = [
            self.prefix.stream_id,
            self.prefix.topic_id,
            self.prefix.partition_id,
            self.prefix.created_revision,
            self.published_at,
            flags,
            self.floor.unwrap_or(0),
            self.trim_point.unwrap_or(0),
        ];
        let (header, segments) = payload.split_at_mut(DELTA_HEADER_LEN);
        let (words, counts) = header.split_at_mut(size_of::<[u64; 8]>());
        for (field, value) in words.chunks_mut(size_of::<u64>()).zip(integers) {
            field.copy_from_slice(&value.to_le_bytes());
        }
        let count = u32::try_from(self.segments.len()).unwrap_or(u32::MAX);
        counts[..size_of::<u32>()].copy_from_slice(&self.kept.to_le_bytes());
        counts[size_of::<u32>()..].copy_from_slice(&count.to_le_bytes());
        for (segment, out) in self
            .segments
            .iter()
            .zip(segments.chunks_mut(SEGMENT_PAYLOAD_SIZE))
        {
            encode_segment(segment, out);
        }
        payload
    }

    /// The delta that `payload` holds. `None` for bytes no delta encodes to.
    #[must_use]
    pub fn decode(payload: &[u8]) -> Option<Self> {
        let (header, segments) = payload.split_at_checked(DELTA_HEADER_LEN)?;
        let (words, counts) = header.split_at(size_of::<[u64; 8]>());
        let mut values = words
            .chunks(size_of::<u64>())
            .map(|field| field.try_into().map(u64::from_le_bytes));
        let mut next = || values.next()?.ok();
        let prefix = ObjectPrefix {
            stream_id: next()?,
            topic_id: next()?,
            partition_id: next()?,
            created_revision: next()?,
        };
        let published_at = next()?;
        let flags = next()?;
        let floor = next()?;
        let trim_point = next()?;
        let kept = u32::from_le_bytes(counts[..size_of::<u32>()].try_into().ok()?);
        let count = u32::from_le_bytes(counts[size_of::<u32>()..].try_into().ok()?) as usize;
        if flags & !(HAS_FLOOR | HAS_TRIM_POINT | KEYED_BY_BOTH_OFFSETS) != 0
            || count > DELTA_SEGMENTS_MAX
            || segments.len() != count * SEGMENT_PAYLOAD_SIZE
        {
            return None;
        }
        Some(Self {
            prefix,
            published_at,
            floor: (flags & HAS_FLOOR != 0).then_some(floor),
            trim_point: (flags & HAS_TRIM_POINT != 0).then_some(trim_point),
            segments_keyed_by_both_offsets: flags & KEYED_BY_BOTH_OFFSETS != 0,
            kept,
            segments: segments
                .chunks(SEGMENT_PAYLOAD_SIZE)
                .map(decode_segment)
                .collect::<Option<_>>()?,
        })
    }

    /// The whole manifest this delta names over `applied`, `None` when it
    /// keeps more segments than `applied` holds above its trim point.
    #[must_use]
    pub fn rebuild(&self, applied: Option<&PartitionManifest>) -> Option<PartitionManifest> {
        let above_trim = applied.map_or(&[][..], |applied| {
            let trimmed = applied
                .segments
                .iter()
                .take_while(|segment| Some(segment.end_offset) <= self.trim_point)
                .count();
            &applied.segments[trimmed..]
        });
        let kept = above_trim.get(..self.kept as usize)?;
        Some(PartitionManifest {
            prefix: self.prefix,
            published_at: self.published_at,
            floor: self.floor,
            trim_point: self.trim_point,
            segments_keyed_by_both_offsets: self.segments_keyed_by_both_offsets,
            segments: kept.iter().chain(&self.segments).copied().collect(),
        })
    }
}

/// The deltas that take `applied` to `fetched`, which the caller checked
/// against the log. Each one ends the manifest after its last segment.
///
/// # Errors
/// [`PeerManifestCommitError::TooManyChanged`] when the segments that
/// `fetched` changes at or below the applied floor take more than one
/// command, and [`PeerManifestCommitError::TooManySegments`] for a manifest
/// longer than the machine holds.
pub fn deltas(
    applied: Option<&PartitionManifest>,
    fetched: &PartitionManifest,
) -> Result<Vec<PeerManifestDelta>, PeerManifestCommitError> {
    if fetched.segments.len() > PEER_MANIFEST_SEGMENTS_MAX {
        return Err(PeerManifestCommitError::TooManySegments {
            segments: fetched.segments.len(),
        });
    }
    let above_trim = applied.map_or(&[][..], |applied| {
        let trimmed = applied
            .segments
            .iter()
            .take_while(|segment| Some(segment.end_offset) <= fetched.trim_point)
            .count();
        &applied.segments[trimmed..]
    });
    let kept = above_trim
        .iter()
        .zip(&fetched.segments)
        .take_while(|(applied, listed)| applied == listed)
        .count();
    let rest = &fetched.segments[kept..];
    // The segments up to the applied floor change what the log holds, so the
    // first command carries all of them. A command that ended inside them
    // would name a manifest with a lower floor than the applied one.
    let applied_floor = applied.and_then(|applied| applied.floor);
    let changed = rest
        .iter()
        .take_while(|segment| applied_floor.is_some_and(|floor| segment.start_offset <= floor))
        .count();
    if changed > DELTA_SEGMENTS_MAX {
        return Err(PeerManifestCommitError::TooManyChanged { changed });
    }
    let delta = |kept: usize, listed: &[ManifestSegment], last: bool| PeerManifestDelta {
        prefix: fetched.prefix,
        published_at: fetched.published_at,
        floor: if last {
            fetched.floor
        } else {
            listed.last().map(|segment| segment.end_offset)
        },
        trim_point: fetched.trim_point,
        segments_keyed_by_both_offsets: fetched.segments_keyed_by_both_offsets,
        kept: u32::try_from(kept).unwrap_or(u32::MAX),
        segments: listed.to_vec(),
    };
    let first_len = changed.max(rest.len().min(DELTA_SEGMENTS_MAX));
    let mut out = vec![delta(kept, &rest[..first_len], first_len == rest.len())];
    let mut done = kept + first_len;
    for chunk in rest[first_len..].chunks(DELTA_SEGMENTS_MAX) {
        let last = done + chunk.len() == fetched.segments.len();
        out.push(delta(done, chunk, last));
        done += chunk.len();
    }
    Ok(out)
}

/// The last peer manifest that the partition committed.
#[derive(Debug, Default)]
pub struct PeerManifests {
    applied: Option<PartitionManifest>,
}

impl PeerManifests {
    /// The last committed manifest, `None` before the first.
    #[must_use]
    pub const fn applied(&self) -> Option<&PartitionManifest> {
        self.applied.as_ref()
    }

    /// The manifest of `delta` over the applied one, when the peer log takes
    /// it: of the incarnation the applied one names, not older, and in line
    /// with its segments.
    fn accepts(&self, delta: &PeerManifestDelta) -> Option<PartitionManifest> {
        if self
            .applied
            .as_ref()
            .is_some_and(|applied| applied.prefix != delta.prefix)
        {
            return None;
        }
        let manifest = delta.rebuild(self.applied.as_ref())?;
        if manifest.segments.len() > PEER_MANIFEST_SEGMENTS_MAX {
            return None;
        }
        PeerLog::of_manifest(delta.prefix, self.applied.as_ref())
            .check(&manifest)
            .ok()?;
        Some(manifest)
    }
}

impl PartitionStateMachine for PeerManifests {
    fn id(&self) -> MachineId {
        PEER_MANIFEST_MACHINE
    }

    fn command_kinds(&self) -> &[CommandKind] {
        &[FOLLOW_PEER_MANIFEST]
    }

    /// A delta committed twice rebuilds the same manifest the second time,
    /// since it keeps the same segments of the manifest it made.
    fn apply(&mut self, _: u64, command: MachineCommand<'_>) -> Applied {
        let Some(manifest) =
            PeerManifestDelta::decode(command.payload).and_then(|delta| self.accepts(&delta))
        else {
            return Applied::Refused;
        };
        self.applied = Some(manifest);
        Applied::Changed
    }

    fn snapshot_into(&self, out: &mut Vec<u8>) {
        if let Some(applied) = &self.applied {
            out.extend_from_slice(&applied.encode());
        }
    }

    fn holds_state(&self) -> bool {
        self.applied.is_some()
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<(), MachineRestoreError> {
        let manifest = PartitionManifest::decode(snapshot).map_err(|_| MachineRestoreError {
            reason: "a peer manifest snapshot is one manifest as a peer publishes it",
        })?;
        if manifest.segments.len() > PEER_MANIFEST_SEGMENTS_MAX {
            return Err(MachineRestoreError {
                reason: "a peer manifest snapshot lists more segments than the machine holds",
            });
        }
        self.applied = Some(manifest);
        Ok(())
    }

    fn reset(&mut self) {
        self.applied = None;
    }
}

impl<B, SB> IggyPartition<B, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    /// The last peer manifest this partition committed, `None` before the
    /// first one and on a partition of this cluster's own topics.
    #[must_use]
    pub fn committed_peer_manifest(&self) -> Option<&PartitionManifest> {
        self.state_machine::<PeerManifests>(PEER_MANIFEST_MACHINE)
            .and_then(PeerManifests::applied)
    }

    /// The requests that commit `fetched`, for the primary's `on_request`,
    /// numbered from `first_request`. Empty when the committed manifest
    /// lists the same as `fetched`, whatever it says of the time it was
    /// published.
    ///
    /// # Errors
    /// See [`PeerManifestCommitError`]. The peer log refuses `fetched`, or it
    /// cannot be split into commands.
    pub fn peer_manifest_requests(
        &self,
        fetched: &PartitionManifest,
        first_request: u64,
    ) -> Result<Vec<Message<RoutedRequestHeader>>, PeerManifestCommitError> {
        let peer = self.peer_log().ok_or(PeerManifestRefusal::NotFollowing)?;
        let committed = self.committed_peer_manifest();
        if committed.is_some_and(|committed| lists_the_same(committed, fetched)) {
            return Ok(Vec::new());
        }
        peer.check(fetched)?;
        deltas(committed, fetched)?
            .iter()
            .zip(first_request..)
            .map(|(delta, request)| {
                self.command_request(FOLLOW_PEER_MANIFEST, &delta.encode(), request)
                    .map_err(PeerManifestCommitError::from)
            })
            .collect()
    }

    /// What the retention of its topic leaves of the manifest that this
    /// partition committed, once the origin it follows retired and no newer
    /// manifest comes: the committed manifest without its leading segments
    /// that expired, or that push the bytes of those before the last one
    /// past `max_bytes`, held at the consumer barrier. The last segment
    /// stays. The primary commits it as one more manifest, so every replica
    /// drops the same segments, and nothing reaches the origin's store.
    ///
    /// `None` when retention removes nothing, on a partition that follows no
    /// peer, and on every replica but a primary that applied the view it
    /// started.
    #[must_use]
    pub fn retired_peer_retention(
        &self,
        now: IggyTimestamp,
        message_expiry: IggyExpiry,
        max_bytes: Option<u64>,
    ) -> Option<PartitionManifest> {
        if self.peer_log().is_none() || !self.state_machines_current() {
            return None;
        }
        let committed = self.committed_peer_manifest()?;
        let trim_point = retention_end(
            &committed.segments,
            now,
            message_expiry,
            max_bytes,
            self.min_committed_offset().map(|(barrier, _, _)| barrier),
        )?;
        if committed
            .trim_point
            .is_some_and(|applied| applied >= trim_point)
        {
            return None;
        }
        let mut trimmed = committed.clone();
        trimmed
            .segments
            .retain(|segment| segment.end_offset > trim_point);
        trimmed.trim_point = Some(trim_point);
        Some(trimmed)
    }

    /// Bring the peer log to the committed manifest. Every replica runs it
    /// after the manifest changes, and a partition that starts to follow its
    /// peer runs it once. The peer log checked the manifest as the machine
    /// did, so it takes it.
    pub(crate) fn follow_committed_peer_manifest(&mut self) {
        let Some(manifest) = self.committed_peer_manifest().cloned() else {
            return;
        };
        if self.peer_log().is_none() {
            return;
        }
        match self.apply_peer_manifest(&manifest) {
            Ok(applied) => self
                .retired_peer_indexes
                .extend_from_slice(applied.retired_indexes()),
            Err(refusal) => tracing::error!(
                namespace_raw = self.namespace().inner(),
                %refusal,
                "the peer log refused a committed peer manifest"
            ),
        }
    }
}

/// Whether two manifests list the same, apart from when they were published.
fn lists_the_same(left: &PartitionManifest, right: &PartitionManifest) -> bool {
    left.prefix == right.prefix
        && left.floor == right.floor
        && left.trim_point == right.trim_point
        && left.segments_keyed_by_both_offsets == right.segments_keyed_by_both_offsets
        && left.segments == right.segments
}
