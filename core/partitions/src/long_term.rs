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

//! The long-term log of a partition that places its batches: the segments
//! its primary wrote into the object store from the messages of its placed
//! batches.
//!
//! A placed batch names messages in a fresh object, which holds batches of
//! many partitions and lives only until they are rewritten. The primary
//! rewrites its committed batches into segment objects of the partition's
//! own, in the layout of tiered storage, and commits each one here once it
//! is stored. So every replica agrees on which offsets a long-term segment
//! holds, where it ends, and what its bytes hash to.
//!
//! The primary picks where a segment ends when it writes it. Nothing ties it
//! to the partition's local segments, which hold placed batches and roll by
//! their own size.
//!
//! Two more commands change the log at or below its floor. A replace puts
//! one stored segment in the place of the run of segments that hold the same
//! offsets, and a trim removes segments from the front. Neither moves the
//! floor, so both commute with the publish of the next segment.
//!
//! Every partition registers the machine. Only the primary of a partition
//! that places its batches proposes a command, so the log of any other
//! partition stays empty and adds nothing to its checkpoint or transfer.

use std::fmt;

use iggy_binary_protocol::RoutedRequestHeader;
use iggy_object_store::{ManifestSegment, SEGMENT_HASH_LEN};
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use server_common::Message;

use crate::IggyPartition;
use crate::manifest_publish::ManifestProgress;
use crate::state_machine::{
    Applied, CommandEncodeError, CommandKind, MachineCommand, MachineId, MachineRestoreError,
    PartitionStateMachine,
};

#[cfg(test)]
mod tests;

/// The long-term log's id in snapshots, stable within a storage format.
pub const LONG_TERM_LOG_MACHINE: MachineId = MachineId(2);

/// The command kind of [`PublishSegment`].
pub const PUBLISH_SEGMENT: CommandKind = CommandKind(2);

/// The command kind of [`ReplaceSegments`].
pub const REPLACE_SEGMENTS: CommandKind = CommandKind(3);

/// The command kind of [`TrimSegments`].
pub const TRIM_SEGMENTS: CommandKind = CommandKind(4);

/// `start_offset | end_offset | start_timestamp | end_timestamp |
/// max_timestamp | size`, all `u64`, then `crc32c u32 | 4 zero bytes | hash`,
/// little-endian.
pub const SEGMENT_PAYLOAD_SIZE: usize =
    6 * size_of::<u64>() + 2 * size_of::<u32>() + SEGMENT_HASH_LEN;

/// Segments the log holds at most.
///
/// Its snapshot stays far below what a partition's machines may write
/// together, and a partition that reaches it publishes nothing until a
/// replace or a trim makes it shorter.
pub const LONG_TERM_SEGMENTS_MAX: usize = 65_536;

/// Segments one [`ReplaceSegments`] replaces at most, which bounds the time
/// the command takes to apply.
pub const REPLACE_SOURCES_MAX: usize = 1024;

/// The offset of a [`TrimSegments`], a little-endian `u64`.
pub const TRIM_PAYLOAD_SIZE: usize = size_of::<u64>();

/// Append one stored segment to the long-term log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishSegment(pub ManifestSegment);

impl PublishSegment {
    /// The command payload.
    #[must_use]
    pub fn encode(&self) -> [u8; SEGMENT_PAYLOAD_SIZE] {
        let mut payload = [0; SEGMENT_PAYLOAD_SIZE];
        encode_segment(&self.0, &mut payload);
        payload
    }
}

impl TryFrom<&[u8]> for PublishSegment {
    type Error = SegmentDecodeError;

    fn try_from(payload: &[u8]) -> Result<Self, Self::Error> {
        decode_segment(payload).map(Self).ok_or(SegmentDecodeError {
            length: payload.len(),
        })
    }
}

/// Put one stored segment in the place of the segments that hold its
/// offsets.
///
/// The segments it replaces are the ones from its first offset to its last,
/// so the command names none of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplaceSegments(pub ManifestSegment);

impl ReplaceSegments {
    /// The command payload.
    #[must_use]
    pub fn encode(&self) -> [u8; SEGMENT_PAYLOAD_SIZE] {
        let mut payload = [0; SEGMENT_PAYLOAD_SIZE];
        encode_segment(&self.0, &mut payload);
        payload
    }
}

impl TryFrom<&[u8]> for ReplaceSegments {
    type Error = SegmentDecodeError;

    fn try_from(payload: &[u8]) -> Result<Self, Self::Error> {
        decode_segment(payload).map(Self).ok_or(SegmentDecodeError {
            length: payload.len(),
        })
    }
}

/// Remove the segments at the front of the log that end at or below
/// `up_to_offset`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TrimSegments {
    pub up_to_offset: u64,
}

impl TrimSegments {
    /// The command payload.
    #[must_use]
    pub const fn encode(&self) -> [u8; TRIM_PAYLOAD_SIZE] {
        self.up_to_offset.to_le_bytes()
    }
}

impl TryFrom<&[u8]> for TrimSegments {
    type Error = TrimDecodeError;

    fn try_from(payload: &[u8]) -> Result<Self, Self::Error> {
        payload
            .try_into()
            .map(|up_to_offset| Self {
                up_to_offset: u64::from_le_bytes(up_to_offset),
            })
            .map_err(|_| TrimDecodeError {
                length: payload.len(),
            })
    }
}

/// A command payload that is not one long-term segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentDecodeError {
    pub length: usize,
}

impl fmt::Display for SegmentDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "long-term segment of {} bytes, expected {SEGMENT_PAYLOAD_SIZE} with zero padding",
            self.length
        )
    }
}

impl std::error::Error for SegmentDecodeError {}

/// A command payload that is not a [`TrimSegments`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TrimDecodeError {
    pub length: usize,
}

impl fmt::Display for TrimDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "long-term trim of {} bytes, expected {TRIM_PAYLOAD_SIZE}",
            self.length
        )
    }
}

impl std::error::Error for TrimDecodeError {}

fn encode_segment(segment: &ManifestSegment, out: &mut [u8]) {
    let (integers, rest) = out.split_at_mut(6 * size_of::<u64>());
    for (field, value) in integers
        .as_chunks_mut::<{ size_of::<u64>() }>()
        .0
        .iter_mut()
        .zip([
            segment.start_offset,
            segment.end_offset,
            segment.start_timestamp,
            segment.end_timestamp,
            segment.max_timestamp,
            segment.size,
        ])
    {
        *field = value.to_le_bytes();
    }
    let (crc32c, rest) = rest.split_at_mut(size_of::<u32>());
    crc32c.copy_from_slice(&segment.crc32c.to_le_bytes());
    let (padding, hash) = rest.split_at_mut(size_of::<u32>());
    padding.fill(0);
    hash.copy_from_slice(&segment.hash);
}

fn decode_segment(bytes: &[u8]) -> Option<ManifestSegment> {
    if bytes.len() != SEGMENT_PAYLOAD_SIZE {
        return None;
    }
    let (integers, rest) = bytes.split_at(6 * size_of::<u64>());
    let mut values = integers
        .as_chunks::<{ size_of::<u64>() }>()
        .0
        .iter()
        .map(|field| u64::from_le_bytes(*field));
    let (crc32c, rest) = rest.split_first_chunk()?;
    let (padding, hash) = rest.split_first_chunk::<{ size_of::<u32>() }>()?;
    if padding != &[0; size_of::<u32>()] {
        return None;
    }
    Some(ManifestSegment {
        start_offset: values.next()?,
        end_offset: values.next()?,
        start_timestamp: values.next()?,
        end_timestamp: values.next()?,
        max_timestamp: values.next()?,
        size: values.next()?,
        crc32c: u32::from_le_bytes(*crc32c),
        hash: hash.try_into().ok()?,
    })
}

/// The segments of the partition that the object store holds, in offset
/// order and without a gap from the first one on.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LongTermLog {
    segments: Vec<ManifestSegment>,
}

impl LongTermLog {
    #[must_use]
    pub fn segments(&self) -> &[ManifestSegment] {
        &self.segments
    }

    /// The last offset a long-term segment holds, `None` before the first
    /// one.
    #[must_use]
    pub fn floor(&self) -> Option<u64> {
        self.segments.last().map(|segment| segment.end_offset)
    }

    /// The last offset a trim removed from the front of the log, `None`
    /// while the log starts at the first offset.
    #[must_use]
    pub fn trim_point(&self) -> Option<u64> {
        self.segments
            .first()
            .and_then(|first| first.start_offset.checked_sub(1))
    }

    /// The first offset the next segment must start at.
    fn next_start(&self) -> u64 {
        self.floor().map_or(0, |floor| floor.saturating_add(1))
    }

    fn publish(&mut self, segment: ManifestSegment) -> Applied {
        // A primary that never learned its command committed sends it again,
        // so the segment the log ends with is accepted and changes nothing.
        if self.segments.last() == Some(&segment) {
            return Applied::Changed;
        }
        // A segment anywhere else would leave a gap, or name offsets that
        // another segment holds already.
        if segment.start_offset != self.next_start()
            || segment.end_offset < segment.start_offset
            || self.segments.len() >= LONG_TERM_SEGMENTS_MAX
        {
            return Applied::Refused;
        }
        self.segments.push(segment);
        Applied::Changed
    }

    /// Nothing here asks how long the log is: a replace makes it shorter, so
    /// a full log takes one.
    fn replace(&mut self, merged: ManifestSegment) -> Applied {
        let first = self
            .segments
            .binary_search_by_key(&merged.start_offset, |segment| segment.start_offset);
        // A primary that never learned its command committed sends it again,
        // and the log holds the merged segment by then.
        if first.is_ok_and(|first| self.segments[first] == merged) {
            return Applied::Changed;
        }
        let last = self
            .segments
            .binary_search_by_key(&merged.end_offset, |segment| segment.end_offset);
        let (Ok(first), Ok(last)) = (first, last) else {
            return Applied::Refused;
        };
        // One segment in the place of another alone would be other bytes
        // under the key of the first.
        if last <= first || last - first >= REPLACE_SOURCES_MAX {
            return Applied::Refused;
        }
        if concatenated(&self.segments[first..=last], merged.hash) != Some(merged) {
            return Applied::Refused;
        }
        self.segments.splice(first..=last, [merged]);
        Applied::Changed
    }

    fn trim(&mut self, up_to_offset: u64) -> Applied {
        // The floor and the trim point are read from the segments, so the
        // last one stays whatever the offset.
        let removable = self.segments.len().saturating_sub(1);
        let ended = self.segments[..removable]
            .partition_point(|segment| segment.end_offset <= up_to_offset);
        self.segments.drain(..ended);
        Applied::Changed
    }
}

/// What a segment that holds the bytes of `sources` back to back records,
/// with `hash` for its bytes, which the sources cannot give. `None` for no
/// source, and for sizes no segment has.
fn concatenated(
    sources: &[ManifestSegment],
    hash: [u8; SEGMENT_HASH_LEN],
) -> Option<ManifestSegment> {
    let (first, rest) = sources.split_first()?;
    rest.iter()
        .try_fold(ManifestSegment { hash, ..*first }, |merged, next| {
            Some(ManifestSegment {
                end_offset: next.end_offset,
                end_timestamp: next.end_timestamp,
                max_timestamp: merged.max_timestamp.max(next.max_timestamp),
                size: merged.size.checked_add(next.size)?,
                crc32c: crc32c::crc32c_combine(
                    merged.crc32c,
                    next.crc32c,
                    usize::try_from(next.size).ok()?,
                ),
                ..merged
            })
        })
}

impl PartitionStateMachine for LongTermLog {
    fn id(&self) -> MachineId {
        LONG_TERM_LOG_MACHINE
    }

    fn command_kinds(&self) -> &[CommandKind] {
        &[PUBLISH_SEGMENT, REPLACE_SEGMENTS, TRIM_SEGMENTS]
    }

    fn apply(&mut self, _: u64, command: MachineCommand<'_>) -> Applied {
        match command.kind {
            PUBLISH_SEGMENT => PublishSegment::try_from(command.payload)
                .map_or(Applied::Refused, |PublishSegment(segment)| {
                    self.publish(segment)
                }),
            REPLACE_SEGMENTS => ReplaceSegments::try_from(command.payload)
                .map_or(Applied::Refused, |ReplaceSegments(merged)| {
                    self.replace(merged)
                }),
            TRIM_SEGMENTS => TrimSegments::try_from(command.payload)
                .map_or(Applied::Refused, |trim| self.trim(trim.up_to_offset)),
            _ => Applied::Refused,
        }
    }

    fn snapshot_into(&self, out: &mut Vec<u8>) {
        let start = out.len();
        out.resize(start + self.segments.len() * SEGMENT_PAYLOAD_SIZE, 0);
        for (segment, bytes) in self
            .segments
            .iter()
            .zip(out[start..].chunks_mut(SEGMENT_PAYLOAD_SIZE))
        {
            encode_segment(segment, bytes);
        }
    }

    fn holds_state(&self) -> bool {
        !self.segments.is_empty()
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<(), MachineRestoreError> {
        let malformed = MachineRestoreError {
            reason: "a long-term log snapshot is whole segments in offset order without a gap",
        };
        if !snapshot.len().is_multiple_of(SEGMENT_PAYLOAD_SIZE)
            || snapshot.len() / SEGMENT_PAYLOAD_SIZE > LONG_TERM_SEGMENTS_MAX
        {
            return Err(malformed);
        }
        let mut restored = Self::default();
        for bytes in snapshot.chunks(SEGMENT_PAYLOAD_SIZE) {
            let segment = decode_segment(bytes).ok_or(malformed)?;
            // The first segment starts wherever a trim left the front.
            let continues = restored
                .floor()
                .is_none_or(|floor| floor.checked_add(1) == Some(segment.start_offset));
            if !continues || segment.end_offset < segment.start_offset {
                return Err(malformed);
            }
            restored.segments.push(segment);
        }
        *self = restored;
        Ok(())
    }

    fn reset(&mut self) {
        self.segments.clear();
    }
}

impl<B, SB> IggyPartition<B, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    /// The long-term segments of this partition, in offset order. Empty on a
    /// partition that does not register the machine or published none.
    #[must_use]
    pub fn long_term_segments(&self) -> &[ManifestSegment] {
        self.state_machine::<LongTermLog>(LONG_TERM_LOG_MACHINE)
            .map_or(&[], LongTermLog::segments)
    }

    /// The last offset a long-term segment holds, `None` before the first
    /// one.
    #[must_use]
    pub fn long_term_floor(&self) -> Option<u64> {
        self.long_term_segments()
            .last()
            .map(|segment| segment.end_offset)
    }

    /// The last offset a trim removed from the front of the long-term log,
    /// `None` while the log starts at the first offset.
    #[must_use]
    pub fn long_term_trim_point(&self) -> Option<u64> {
        self.state_machine::<LongTermLog>(LONG_TERM_LOG_MACHINE)
            .and_then(LongTermLog::trim_point)
    }

    /// Whether a command changed the long-term log on this primary since
    /// the last call.
    pub fn take_long_term_changed(&self) -> bool {
        self.long_term_changed.take()
    }

    /// A command applied to the log that was at `before`. Only a primary
    /// notes a change, since only a primary publishes the manifest and
    /// rewrites. A command the log accepts without changing, as a trim below
    /// its first segment or one that was sent twice, is not a change: the
    /// manifest it would ask for is the one in the store.
    pub(crate) fn note_long_term_command(&self, before: ManifestProgress) {
        if self.long_term_progress() != before && self.consensus().is_primary() {
            self.long_term_changed.set(true);
        }
    }

    /// A request committing `segment` to the long-term log, for the
    /// primary's `on_request`. `request` is the caller's own number for it,
    /// as for [`Self::command_request`].
    ///
    /// # Errors
    /// Returns an error when the command exceeds the command size limit,
    /// which a segment never does.
    pub fn publish_segment_request(
        &self,
        segment: &ManifestSegment,
        request: u64,
    ) -> Result<Message<RoutedRequestHeader>, CommandEncodeError> {
        self.command_request(PUBLISH_SEGMENT, &PublishSegment(*segment).encode(), request)
    }

    /// A request putting `merged` in the place of the long-term segments
    /// that hold its offsets, for the primary's `on_request`.
    ///
    /// # Errors
    /// Returns an error when the command exceeds the command size limit,
    /// which a segment never does.
    pub fn replace_segments_request(
        &self,
        merged: &ManifestSegment,
        request: u64,
    ) -> Result<Message<RoutedRequestHeader>, CommandEncodeError> {
        self.command_request(
            REPLACE_SEGMENTS,
            &ReplaceSegments(*merged).encode(),
            request,
        )
    }

    /// A request removing the long-term segments at the front of the log
    /// that end at or below `up_to_offset`, for the primary's `on_request`.
    ///
    /// # Errors
    /// Returns an error when the command exceeds the command size limit,
    /// which an offset never does.
    pub fn trim_segments_request(
        &self,
        up_to_offset: u64,
        request: u64,
    ) -> Result<Message<RoutedRequestHeader>, CommandEncodeError> {
        self.command_request(
            TRIM_SEGMENTS,
            &TrimSegments { up_to_offset }.encode(),
            request,
        )
    }
}
