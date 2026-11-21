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
//! A primary registers every object it stores before it stores it: the
//! segment the next publish lists, or the merged one a replace lists. The
//! command that lists the segment clears the registration. A registration
//! that nothing cleared within [`LONG_TERM_REGISTRATION_TTL`] expires once
//! the floor is past its first offset, and from then on no command
//! registers or lists those offsets again, so its object can be deleted by
//! any primary, however stale its view. An object stored under a primary
//! that lost primacy before its command committed is deleted that way when
//! the next primary lists other offsets.
//!
//! Every partition registers the machine. Only the primary of a partition
//! that places its batches proposes a command, so the log of any other
//! partition stays empty and adds nothing to its checkpoint or transfer.

use std::fmt;
use std::slice::ChunksExact;
use std::time::Duration;

use iggy_binary_protocol::RoutedRequestHeader;
use iggy_object_store::{ManifestSegment, SEGMENT_HASH_LEN};
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use server_common::Message;

use crate::IggyPartition;
use crate::long_term_delete::{LongTermNames, LongTermObject, SegmentSpan};
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

/// The command kind of [`RegisterObject`].
pub const REGISTER_OBJECT: CommandKind = CommandKind(5);

/// The command kind of [`ExpireRegistrations`].
pub const EXPIRE_REGISTRATIONS: CommandKind = CommandKind(6);

/// `start_offset | end_offset | registered_at`, all `u64`, little-endian.
pub const REGISTRATION_PAYLOAD_SIZE: usize = 3 * size_of::<u64>();

/// The time of an [`ExpireRegistrations`], a little-endian `u64`.
pub const EXPIRE_PAYLOAD_SIZE: usize = size_of::<u64>();

/// Registered and expired objects the log holds at most, together.
///
/// Each one past the few of the jobs in flight is left by a job whose
/// primary lost primacy before its command committed, so the limit bounds
/// the snapshot and is never reached by a partition that works.
pub const LONG_TERM_REGISTRATIONS_MAX: usize = 4096;

/// How long after its registration the object of a rewrite or a merge may
/// wait for the command that lists it, by the clock of the primary that
/// registered it.
pub const LONG_TERM_REGISTRATION_TTL: Duration = Duration::from_hours(1);

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

/// An object a primary is about to store, named by the offsets its key
/// carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectRegistration {
    pub start_offset: u64,
    pub end_offset: u64,
    /// When the primary registered it, in microseconds of its own clock.
    pub registered_at: u64,
}

impl ObjectRegistration {
    /// The first and the last offset, which name the object.
    #[must_use]
    pub const fn key(&self) -> (u64, u64) {
        (self.start_offset, self.end_offset)
    }
}

impl SegmentSpan for ObjectRegistration {
    fn start_offset(&self) -> u64 {
        self.start_offset
    }

    fn end_offset(&self) -> u64 {
        self.end_offset
    }
}

/// Register the object of the segment the next publish or a replace will
/// list, before it is stored. Registering it again keeps the later time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegisterObject(pub ObjectRegistration);

impl RegisterObject {
    /// The command payload.
    #[must_use]
    pub fn encode(&self) -> [u8; REGISTRATION_PAYLOAD_SIZE] {
        let mut payload = [0; REGISTRATION_PAYLOAD_SIZE];
        encode_u64s(
            [self.0.start_offset, self.0.end_offset, self.0.registered_at],
            &mut payload,
        );
        payload
    }
}

impl TryFrom<&[u8]> for RegisterObject {
    type Error = PayloadSizeError;

    fn try_from(payload: &[u8]) -> Result<Self, Self::Error> {
        let [start_offset, end_offset, registered_at] =
            decode_u64s(payload).ok_or(PayloadSizeError {
                length: payload.len(),
                expected: REGISTRATION_PAYLOAD_SIZE,
            })?;
        Ok(Self(ObjectRegistration {
            start_offset,
            end_offset,
            registered_at,
        }))
    }
}

/// Expire every registration made before `before`, but that of the next
/// segment.
///
/// The time is in microseconds of the clocks that made the registrations.
/// The primary picks it with its own clock, so every replica expires the
/// same ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExpireRegistrations {
    pub before: u64,
}

impl ExpireRegistrations {
    /// The command payload.
    #[must_use]
    pub const fn encode(&self) -> [u8; EXPIRE_PAYLOAD_SIZE] {
        self.before.to_le_bytes()
    }
}

impl TryFrom<&[u8]> for ExpireRegistrations {
    type Error = PayloadSizeError;

    fn try_from(payload: &[u8]) -> Result<Self, Self::Error> {
        let [before] = decode_u64s(payload).ok_or(PayloadSizeError {
            length: payload.len(),
            expected: EXPIRE_PAYLOAD_SIZE,
        })?;
        Ok(Self { before })
    }
}

/// A command payload of another size than its kind has.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PayloadSizeError {
    pub length: usize,
    pub expected: usize,
}

impl fmt::Display for PayloadSizeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "long-term command of {} bytes, expected {}",
            self.length, self.expected
        )
    }
}

impl std::error::Error for PayloadSizeError {}

fn encode_u64s<const N: usize>(values: [u64; N], out: &mut [u8]) {
    for (field, value) in out
        .as_chunks_mut::<{ size_of::<u64>() }>()
        .0
        .iter_mut()
        .zip(values)
    {
        *field = value.to_le_bytes();
    }
}

/// `N` little-endian `u64`s, when `bytes` is exactly that long.
fn decode_u64s<const N: usize>(bytes: &[u8]) -> Option<[u64; N]> {
    let (fields, []) = bytes.as_chunks::<{ size_of::<u64>() }>() else {
        return None;
    };
    let fields: &[[u8; size_of::<u64>()]; N] = fields.try_into().ok()?;
    Some(fields.map(u64::from_le_bytes))
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
/// order and without a gap from the first one on, and the objects stored
/// for segments that a later command may come to list.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LongTermLog {
    segments: Vec<ManifestSegment>,
    /// Ascending by offsets, none listed and none that the segments alone
    /// make dead. Each one starts where the next segment starts or where a
    /// segment starts.
    registered: Vec<ObjectRegistration>,
    /// The first and the last offset of each object whose registration
    /// expired, ascending, none that the segments alone make dead. Each one
    /// starts where a segment starts. None is registered, and no command
    /// lists one.
    expired: Vec<(u64, u64)>,
}

impl LongTermLog {
    #[must_use]
    pub fn segments(&self) -> &[ManifestSegment] {
        &self.segments
    }

    /// The objects registered for a replace that has not listed them, in
    /// offset order.
    #[must_use]
    pub fn registrations(&self) -> &[ObjectRegistration] {
        &self.registered
    }

    /// The registration of the object from `start_offset` to `end_offset`.
    #[must_use]
    pub fn registration(&self, start_offset: u64, end_offset: u64) -> Option<&ObjectRegistration> {
        self.registered
            .binary_search_by_key(&(start_offset, end_offset), ObjectRegistration::key)
            .ok()
            .map(|index| &self.registered[index])
    }

    /// The first and the last offset of each object whose registration
    /// expired, ascending.
    #[must_use]
    pub fn expired(&self) -> &[(u64, u64)] {
        &self.expired
    }

    /// Whether the log takes no registration of an object it does not hold
    /// one for already.
    #[must_use]
    pub const fn registrations_full(&self) -> bool {
        self.registered.len() + self.expired.len() >= LONG_TERM_REGISTRATIONS_MAX
    }

    /// Whether an [`ExpireRegistrations`] at `before` would expire a
    /// registration.
    #[must_use]
    pub fn expires_any(&self, before: u64) -> bool {
        let next_start = self.next_start();
        self.registered
            .iter()
            .any(|registration| expires(registration, before, next_start))
    }

    /// What the log says of an object keyed by two offsets.
    #[must_use]
    pub fn classify(&self, start_offset: u64, end_offset: u64) -> LongTermObject {
        LongTermObject::named(
            &self.segments,
            &self.registered,
            &self.expired,
            start_offset,
            end_offset,
        )
    }

    /// The offsets of everything the log names, for a classification away
    /// from the partition.
    #[must_use]
    pub fn names(&self) -> LongTermNames {
        LongTermNames {
            segments: spans_of(&self.segments),
            registered: spans_of(&self.registered),
            expired: self.expired.clone(),
        }
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
    #[must_use]
    pub fn next_start(&self) -> u64 {
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
        self.forget_settled();
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
        let key = (merged.start_offset, merged.end_offset);
        // Its object may be deleted already, by a primary that saw the
        // registration expire.
        if self.expired.binary_search(&key).is_ok() {
            return Applied::Refused;
        }
        if concatenated(&self.segments[first..=last], merged.hash) != Some(merged) {
            return Applied::Refused;
        }
        self.segments.splice(first..=last, [merged]);
        self.forget_settled();
        Applied::Changed
    }

    fn trim(&mut self, up_to_offset: u64) -> Applied {
        // The floor and the trim point are read from the segments, so the
        // last one stays whatever the offset.
        let removable = self.segments.len().saturating_sub(1);
        let ended = self.segments[..removable]
            .partition_point(|segment| segment.end_offset <= up_to_offset);
        self.segments.drain(..ended);
        self.forget_settled();
        Applied::Changed
    }

    /// A rewrite stores the next segment and a merge the run it replaces.
    /// Nothing else is ever listed, so nothing else is registered.
    fn register(&mut self, registration: ObjectRegistration) -> Applied {
        let key = registration.key();
        if let Ok(index) = self
            .registered
            .binary_search_by_key(&key, ObjectRegistration::key)
        {
            let registered = &mut self.registered[index];
            registered.registered_at = registered.registered_at.max(registration.registered_at);
            return Applied::Changed;
        }
        let (start_offset, end_offset) = key;
        let next = start_offset == self.next_start() && end_offset >= start_offset;
        if !(next || replaceable(&self.segments, key))
            || self.expired.binary_search(&key).is_ok()
            || self.registrations_full()
        {
            return Applied::Refused;
        }
        let index = self
            .registered
            .partition_point(|registered| registered.key() < key);
        self.registered.insert(index, registration);
        Applied::Changed
    }

    /// An expiry moves registrations to the expired ones and nothing else,
    /// so it never needs room of its own.
    fn expire(&mut self, before: u64) -> Applied {
        let next_start = self.next_start();
        let (expired, open): (Vec<_>, Vec<_>) = self
            .registered
            .iter()
            .partition(|registration| expires(registration, before, next_start));
        if !expired.is_empty() {
            self.registered = open;
            self.expired
                .extend(expired.iter().map(ObjectRegistration::key));
            self.expired.sort_unstable();
        }
        Applied::Changed
    }

    /// Drop the registrations the segments list, which are fulfilled, and
    /// the registrations and the expired objects the segments alone make
    /// dead: no later state lists them, and every primary deletes them by
    /// the segments.
    fn forget_settled(&mut self) {
        let segments = &self.segments;
        self.registered
            .retain(|registration| may_be_listed(segments, registration.key()));
        self.expired.retain(|&key| may_be_listed(segments, key));
    }
}

/// Whether an expiry at `before` expires `registration`. The registration of
/// the next segment waits for the publish that moves the floor: a rewrite
/// that picks the same offsets again, as one of an idle partition does,
/// lists them, and any other publish leaves the object dead or expirable.
const fn expires(registration: &ObjectRegistration, before: u64, next_start: u64) -> bool {
    registration.registered_at < before && registration.start_offset < next_start
}

/// Whether `segments` hold a run of two segments or more, and at most
/// [`REPLACE_SOURCES_MAX`], from where `key` starts to where it ends.
fn replaceable(segments: &[ManifestSegment], (start, end): (u64, u64)) -> bool {
    let first = segments.binary_search_by_key(&start, |segment| segment.start_offset);
    let last = segments.binary_search_by_key(&end, |segment| segment.end_offset);
    matches!((first, last), (Ok(first), Ok(last)) if last > first && last - first < REPLACE_SOURCES_MAX)
}

/// Whether a later state of `segments` can still list the object `key`
/// that they do not list now.
fn may_be_listed(segments: &[ManifestSegment], (start, end): (u64, u64)) -> bool {
    end >= start && LongTermObject::classify(segments, start, end) == LongTermObject::Unannounced
}

fn spans_of<S: SegmentSpan>(spans: &[S]) -> Vec<(u64, u64)> {
    spans
        .iter()
        .map(|span| (span.start_offset(), span.end_offset()))
        .collect()
}

/// What a segment that holds the bytes of `sources` back to back records,
/// with `hash` for its bytes, which the sources cannot give. `None` for no
/// source, and for sizes no segment has.
pub(crate) fn concatenated(
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
        &[
            PUBLISH_SEGMENT,
            REPLACE_SEGMENTS,
            TRIM_SEGMENTS,
            REGISTER_OBJECT,
            EXPIRE_REGISTRATIONS,
        ]
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
            REGISTER_OBJECT => RegisterObject::try_from(command.payload)
                .map_or(Applied::Refused, |RegisterObject(registration)| {
                    self.register(registration)
                }),
            EXPIRE_REGISTRATIONS => ExpireRegistrations::try_from(command.payload)
                .map_or(Applied::Refused, |expiry| self.expire(expiry.before)),
            _ => Applied::Refused,
        }
    }

    /// Three parts, each a little-endian `u32` count and then that many
    /// entries: the segments as their commands carry them, the
    /// registrations as theirs do, and the first and the last offset of
    /// each expired object. A snapshot of the segments alone, without the
    /// counts, is another format, and [`Self::restore`] refuses it.
    fn snapshot_into(&self, out: &mut Vec<u8>) {
        out.reserve(
            3 * SNAPSHOT_COUNT_SIZE
                + self.segments.len() * SEGMENT_PAYLOAD_SIZE
                + self.registered.len() * REGISTRATION_PAYLOAD_SIZE
                + self.expired.len() * EXPIRED_SNAPSHOT_SIZE,
        );
        put_count(out, self.segments.len());
        for segment in &self.segments {
            let start = out.len();
            out.resize(start + SEGMENT_PAYLOAD_SIZE, 0);
            encode_segment(segment, &mut out[start..]);
        }
        put_count(out, self.registered.len());
        for registration in &self.registered {
            out.extend_from_slice(&RegisterObject(*registration).encode());
        }
        put_count(out, self.expired.len());
        for &(start_offset, end_offset) in &self.expired {
            out.extend_from_slice(&start_offset.to_le_bytes());
            out.extend_from_slice(&end_offset.to_le_bytes());
        }
    }

    fn holds_state(&self) -> bool {
        !self.segments.is_empty() || !self.registered.is_empty() || !self.expired.is_empty()
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<(), MachineRestoreError> {
        let restored = Self::decode(snapshot).ok_or(MachineRestoreError {
            reason: "a long-term log snapshot is its segments in offset order without a gap, \
                     then the registered and the expired objects the segments may still \
                     list, each part counted and ascending",
        })?;
        *self = restored;
        Ok(())
    }

    fn reset(&mut self) {
        *self = Self::default();
    }
}

/// Bytes of the count in front of each part of a snapshot.
const SNAPSHOT_COUNT_SIZE: usize = size_of::<u32>();

/// Bytes of an expired object in a snapshot: its first and its last offset.
const EXPIRED_SNAPSHOT_SIZE: usize = 2 * size_of::<u64>();

fn put_count(out: &mut Vec<u8>, count: usize) {
    // Every part is bounded far below `u32::MAX` by what apply accepts.
    let count = u32::try_from(count).unwrap_or(u32::MAX);
    out.extend_from_slice(&count.to_le_bytes());
}

/// The counted entries of `size` bytes each at the front of `bytes`, and
/// the bytes after them.
fn take_part(bytes: &[u8], size: usize, count_max: usize) -> Option<(ChunksExact<'_, u8>, &[u8])> {
    let (count, rest) = bytes.split_first_chunk::<SNAPSHOT_COUNT_SIZE>()?;
    let count = usize::try_from(u32::from_le_bytes(*count)).ok()?;
    if count > count_max {
        return None;
    }
    let (entries, rest) = rest.split_at_checked(count.checked_mul(size)?)?;
    Some((entries.chunks_exact(size), rest))
}

impl LongTermLog {
    fn decode(snapshot: &[u8]) -> Option<Self> {
        let (segments, rest) = take_part(snapshot, SEGMENT_PAYLOAD_SIZE, LONG_TERM_SEGMENTS_MAX)?;
        let (registered, rest) =
            take_part(rest, REGISTRATION_PAYLOAD_SIZE, LONG_TERM_REGISTRATIONS_MAX)?;
        let (expired, rest) = take_part(rest, EXPIRED_SNAPSHOT_SIZE, LONG_TERM_REGISTRATIONS_MAX)?;
        if !rest.is_empty() || registered.len() + expired.len() > LONG_TERM_REGISTRATIONS_MAX {
            return None;
        }
        let mut log = Self::default();
        for bytes in segments {
            let segment = decode_segment(bytes)?;
            // The first segment starts wherever a trim left the front.
            let continues = log
                .floor()
                .is_none_or(|floor| floor.checked_add(1) == Some(segment.start_offset));
            if !continues || segment.end_offset < segment.start_offset {
                return None;
            }
            log.segments.push(segment);
        }
        let next_start = log.next_start();
        for bytes in registered {
            let RegisterObject(registration) = RegisterObject::try_from(bytes).ok()?;
            let key = registration.key();
            let ascending = log.registered.last().is_none_or(|last| last.key() < key);
            if !ascending || key.0 > next_start || !may_be_listed(&log.segments, key) {
                return None;
            }
            log.registered.push(registration);
        }
        for bytes in expired {
            let [start_offset, end_offset] = decode_u64s(bytes)?;
            let key = (start_offset, end_offset);
            let ascending = log.expired.last().is_none_or(|last| *last < key);
            if !ascending
                || start_offset >= next_start
                || !may_be_listed(&log.segments, key)
                || log.registration(start_offset, end_offset).is_some()
            {
                return None;
            }
            log.expired.push(key);
        }
        Some(log)
    }
}

impl<B, SB> IggyPartition<B, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    /// The long-term log of this partition, `None` on a partition that does
    /// not register the machine.
    #[must_use]
    pub fn long_term_log(&self) -> Option<&LongTermLog> {
        self.state_machine::<LongTermLog>(LONG_TERM_LOG_MACHINE)
    }

    /// The long-term segments of this partition, in offset order. Empty on a
    /// partition that does not register the machine or published none.
    #[must_use]
    pub fn long_term_segments(&self) -> &[ManifestSegment] {
        self.long_term_log().map_or(&[], LongTermLog::segments)
    }

    /// What the long-term log says of the object from `start_offset` to
    /// `end_offset`.
    #[must_use]
    pub fn long_term_object(&self, start_offset: u64, end_offset: u64) -> LongTermObject {
        self.long_term_log().map_or_else(
            || LongTermObject::classify::<ManifestSegment>(&[], start_offset, end_offset),
            |log| log.classify(start_offset, end_offset),
        )
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

    /// A request registering the object of a segment the next publish or a
    /// replace will list, for the primary's `on_request`.
    ///
    /// # Errors
    /// Returns an error when the command exceeds the command size limit,
    /// which a registration never does.
    pub fn register_object_request(
        &self,
        registration: &ObjectRegistration,
        request: u64,
    ) -> Result<Message<RoutedRequestHeader>, CommandEncodeError> {
        self.command_request(
            REGISTER_OBJECT,
            &RegisterObject(*registration).encode(),
            request,
        )
    }

    /// A request expiring every registration of the long-term log made
    /// before `before`, but that of the next segment, for the primary's
    /// `on_request`.
    ///
    /// # Errors
    /// Returns an error when the command exceeds the command size limit,
    /// which a time never does.
    pub fn expire_registrations_request(
        &self,
        before: u64,
        request: u64,
    ) -> Result<Message<RoutedRequestHeader>, CommandEncodeError> {
        self.command_request(
            EXPIRE_REGISTRATIONS,
            &ExpireRegistrations { before }.encode(),
            request,
        )
    }
}
