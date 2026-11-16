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

/// `start_offset | end_offset | start_timestamp | end_timestamp |
/// max_timestamp | size`, all `u64`, then `crc32c u32 | 4 zero bytes | hash`,
/// little-endian.
pub const SEGMENT_PAYLOAD_SIZE: usize =
    6 * size_of::<u64>() + 2 * size_of::<u32>() + SEGMENT_HASH_LEN;

/// Segments the log holds at most.
///
/// Its snapshot stays far below what a partition's machines may write
/// together, and a partition that reaches it publishes nothing until
/// retention removes segments from its front.
pub const LONG_TERM_SEGMENTS_MAX: usize = 65_536;

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

/// A command payload that is not a [`PublishSegment`].
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

    /// The first offset the next segment must start at.
    fn next_start(&self) -> u64 {
        self.floor().map_or(0, |floor| floor.saturating_add(1))
    }
}

impl PartitionStateMachine for LongTermLog {
    fn id(&self) -> MachineId {
        LONG_TERM_LOG_MACHINE
    }

    fn command_kinds(&self) -> &[CommandKind] {
        &[PUBLISH_SEGMENT]
    }

    fn apply(&mut self, _: u64, command: MachineCommand<'_>) -> Applied {
        let Ok(PublishSegment(segment)) = PublishSegment::try_from(command.payload) else {
            return Applied::Refused;
        };
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
            // The first segment starts wherever retention left the front.
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
}
