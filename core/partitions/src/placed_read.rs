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

//! Polls of a partition that places its batches.
//!
//! Such a partition holds placed batches: in its segments once they commit,
//! and in its journal before that. A poll finds the placed batches of the
//! offsets it asks for, fetches the messages of each from the fresh object
//! its placement names, and puts the batch back together. The selection of
//! messages then runs over those batches as it does over the journal of any
//! other partition.
//!
//! A segment of placed batches needs no index to seek in: its batches have
//! one size and ascend by offset and by timestamp, so the walk finds its
//! start by halving.
//!
//! The primary rewrites committed batches into long-term segments, which
//! hold them whole. A poll reads the offsets at or below the long-term floor
//! from those, and goes on into the placed batches above it. A batch that
//! both hold is taken once, from the long-term segment.

use std::collections::BTreeSet;
use std::rc::Rc;
use std::task::Poll;
use std::time::Instant;

use compio::fs::File;
use compio::io::AsyncReadAtExt;
use iggy_binary_protocol::batch::{BATCH_HEADER_SIZE, BatchHeader};
use iggy_binary_protocol::placement::{PLACED_BATCH_SIZE, PlacedBatch};
use iggy_binary_protocol::{Command, Operation, PrepareHeader};
use iggy_object_store::{LongTermKey, ManifestSegment, ObjectPrefix, SegmentObject};
use server_common::iobuf::{Frozen, Owned};
use server_common::send_messages::{decode_batch_slice, decode_prepare_batch_trusted};

use crate::RemoteSegments;
use crate::iggy_index::IGGY_INDEX_SIZE;
use crate::journal::MessageLookup;
use crate::poll_plan::{DiskFault, PartitionDirResolution};

const PREPARE_HEADER_SIZE: usize = size_of::<PrepareHeader>();

/// Placed batches one read of a segment asks the disk for.
const PLACED_PER_READ: u64 = 256;

/// Long-term segments one plan carries, from the one that holds the poll's
/// start on. A poll that runs past them returns what it has.
pub const LONG_TERM_SEGMENTS_PER_POLL: usize = 4;

/// Bytes of a long-term segment one read asks the store for. A batch larger
/// than this is read on its own.
const LONG_TERM_READ_BYTES: u64 = 1024 * 1024;

/// The most a read of a long-term segment's index asks for. The index of a
/// segment is a small fraction of this.
pub const LONG_TERM_INDEX_BYTES: u64 = 4 * 1024 * 1024;

/// A local segment of placed batches as a poll plan snapshots it.
#[derive(Debug, Clone, Copy)]
pub struct PlacedSegment {
    pub start_offset: u64,
    pub end_offset: u64,
    pub max_timestamp: u64,
    /// Bytes the partition counts as stored, all of them committed.
    pub size: u64,
}

/// What a poll of a placing partition reads, captured under the partition
/// borrow so the read runs without it.
pub struct PlacedReadPlan {
    pub(crate) partition_dir: PartitionDirResolution,
    /// Segments that hold a batch, in offset order.
    pub(crate) segments: Vec<PlacedSegment>,
    /// The journal's message entries, op-ascending: the placed batches that
    /// no segment holds yet.
    pub(crate) resident: Vec<Frozen<4096>>,
    pub(crate) remote: Option<Rc<RemoteSegments>>,
    pub(crate) deadline: Option<Instant>,
    /// The partition's prefix in the object store, where its long-term
    /// segments are.
    pub(crate) prefix: ObjectPrefix,
    /// The long-term segments the poll can start in, in offset order. Empty
    /// for a read that starts above the long-term floor.
    pub(crate) long_term: Vec<ManifestSegment>,
    /// The last offset a long-term segment holds. The placed batches at or
    /// below it name fresh objects that may be gone.
    pub(crate) long_term_floor: Option<u64>,
}

impl PlacedReadPlan {
    /// The batches that answer `query`, each put back together as the
    /// prepare a partition of whole batches would hold, in offset order.
    ///
    /// Reads stop once the batches hold the messages the query counts, or
    /// `max_bytes` of them, whichever comes first. The batch that holds the
    /// first match is read whatever its size.
    pub(crate) async fn read(
        &self,
        query: MessageLookup,
        max_bytes: usize,
    ) -> Result<Vec<Frozen<4096>>, DiskFault> {
        let mut wanted = Wanted::new(query, max_bytes);
        let mut batches = Vec::new();
        self.collect_from_long_term(&mut wanted, &mut batches)
            .await?;
        if let Some(floor) = self.long_term_floor {
            // The plan carries only so many long-term segments. A read that
            // ran out of them below the floor ends there: going on above the
            // floor would leave a gap.
            if !self.long_term.is_empty() && !wanted.is_past(floor) {
                return Ok(batches);
            }
            wanted.skip_through(floor);
        }
        let mut placed = Vec::new();
        self.collect_from_segments(&mut wanted, &mut placed).await?;
        for entry in &self.resident {
            if wanted.is_full() {
                break;
            }
            let Ok(batch) = decode_prepare_batch_trusted(entry.as_slice()) else {
                continue;
            };
            if let Some(placement) = batch.placement {
                wanted.take(PlacedBatch::new(batch.header, placement), &mut placed);
            }
        }
        self.fetch_blobs(&placed).await?;
        batches.reserve(placed.len());
        for placed in &placed {
            batches.push(self.assemble(placed).await?);
        }
        Ok(batches)
    }

    /// The batches the poll wants from the long-term segments, whole as the
    /// segments hold them.
    async fn collect_from_long_term(
        &self,
        wanted: &mut Wanted,
        batches: &mut Vec<Frozen<4096>>,
    ) -> Result<(), DiskFault> {
        for segment in &self.long_term {
            if wanted.is_full() {
                break;
            }
            if !wanted.may_start_in(
                segment.start_offset,
                segment.end_offset,
                segment.max_timestamp,
            ) {
                continue;
            }
            // A shard that has not connected its store yet cannot read the
            // segment now, and can once it has.
            let remote = self.remote.as_deref().ok_or(DiskFault::Retryable)?;
            let key = |object| LongTermKey {
                prefix: self.prefix,
                start_offset: segment.start_offset,
                end_offset: segment.end_offset,
                object,
            };
            let index_key = key(SegmentObject::Index);
            let index = self
                .wait(remote.read_long_term_object(&index_key, 0..LONG_TERM_INDEX_BYTES))
                .await?;
            let log = key(SegmentObject::Log);
            let mut position = wanted.first_position(&index, segment.size);
            let mut len = LONG_TERM_READ_BYTES;
            while position < segment.size && !wanted.is_full() {
                let end = segment.size.min(position.saturating_add(len));
                let chunk = self
                    .wait(remote.read_long_term_object(&log, position..end))
                    .await?;
                if chunk.len() as u64 != end - position {
                    return Err(DiskFault::Tiered);
                }
                match take_whole_batches(&chunk, wanted, batches)? {
                    Read::Batches(bytes) => {
                        position += bytes;
                        len = LONG_TERM_READ_BYTES;
                    }
                    // The batch at `position` is larger than the chunk.
                    Read::Needs(bytes) if bytes > len && position + bytes <= segment.size => {
                        len = bytes;
                    }
                    Read::Needs(_) => return Err(DiskFault::Tiered),
                }
            }
        }
        Ok(())
    }

    async fn collect_from_segments(
        &self,
        wanted: &mut Wanted,
        placed: &mut Vec<PlacedBatch>,
    ) -> Result<(), DiskFault> {
        let directory = match &self.partition_dir {
            PartitionDirResolution::Resolved(directory) => directory,
            PartitionDirResolution::NoFiles => return Ok(()),
            PartitionDirResolution::Unresolvable => return Err(DiskFault::Retryable),
        };
        for segment in &self.segments {
            if wanted.is_full() {
                break;
            }
            if !wanted.may_start_in(
                segment.start_offset,
                segment.end_offset,
                segment.max_timestamp,
            ) {
                continue;
            }
            let path = format!("{directory}/{:0>20}.log", segment.start_offset);
            let file = File::open(&path).await.map_err(|_| DiskFault::Retryable)?;
            let count = segment.size / PLACED_BATCH_SIZE as u64;
            let mut index = wanted.first_index(&file, count).await?;
            while index < count && !wanted.is_full() {
                let batches = PLACED_PER_READ.min(count - index);
                for batch in read_placed(&file, index, batches).await? {
                    wanted.take(batch, placed);
                }
                index += batches;
            }
        }
        Ok(())
    }

    /// The fresh objects that the placed batches above `floor` name, in the
    /// partition's segments and in its journal. With no floor, those of
    /// every placed batch the partition holds.
    ///
    /// The journal's batches count whether they are committed or not: one
    /// that is not yet committed may still commit.
    pub(crate) async fn fresh_objects_above(
        &self,
        floor: Option<u64>,
    ) -> Result<BTreeSet<u128>, DiskFault> {
        let first = floor.map_or(0, |floor| floor.saturating_add(1));
        let reaches =
            |header: &BatchHeader| header.base_offset + u64::from(header.message_count) > first;
        let mut objects = BTreeSet::new();
        let directory = match &self.partition_dir {
            PartitionDirResolution::Resolved(directory) => Some(directory),
            PartitionDirResolution::NoFiles => None,
            PartitionDirResolution::Unresolvable => return Err(DiskFault::Retryable),
        };
        for segment in &self.segments {
            if segment.end_offset < first {
                continue;
            }
            let Some(directory) = directory else {
                break;
            };
            let path = format!("{directory}/{:0>20}.log", segment.start_offset);
            let file = File::open(&path).await.map_err(|_| DiskFault::Retryable)?;
            let count = segment.size / PLACED_BATCH_SIZE as u64;
            let mut index = first_reaching(&file, count, first).await?;
            while index < count {
                let batches = PLACED_PER_READ.min(count - index);
                for batch in read_placed(&file, index, batches).await? {
                    if reaches(&batch.header) {
                        objects.insert(batch.placement.object_id);
                    }
                }
                index += batches;
            }
        }
        for entry in &self.resident {
            // An entry that does not decode is no placed batch a poll could
            // read, and the journal refuses it long before this walk.
            let Ok(batch) = decode_prepare_batch_trusted(entry.as_slice()) else {
                return Err(DiskFault::Unreadable);
            };
            if let Some(placement) = batch.placement
                && reaches(&batch.header)
            {
                objects.insert(placement.object_id);
            }
        }
        Ok(objects)
    }

    /// Fetch the blobs of `placed` object by object, so the batches that
    /// follow each other in one fresh object cost one read of the store and
    /// not one each. A batch alone in its object is left to
    /// [`Self::assemble`], which reads it the same way.
    async fn fetch_blobs(&self, placed: &[PlacedBatch]) -> Result<(), DiskFault> {
        let Some(remote) = self.remote.as_deref() else {
            return Ok(());
        };
        let same_object = |one: &PlacedBatch, other: &PlacedBatch| {
            one.placement.object_id == other.placement.object_id
        };
        for run in placed.chunk_by(same_object).filter(|run| run.len() > 1) {
            let blobs: Vec<_> = run
                .iter()
                .filter_map(|placed| {
                    let placement = placed.placement;
                    let end = placement.position.checked_add(placement.length)?;
                    Some(placement.position..end)
                })
                .collect();
            self.wait(remote.fetch_fresh(run[0].placement.object_id, &blobs))
                .await?;
        }
        Ok(())
    }

    /// The whole batch of `placed`: its header with the batch checksum it
    /// has over its messages, and the messages its placement names.
    async fn assemble(&self, placed: &PlacedBatch) -> Result<Frozen<4096>, DiskFault> {
        // A shard that has not connected its store yet cannot read the
        // messages now, and can once it has.
        let remote = self.remote.as_deref().ok_or(DiskFault::Retryable)?;
        let placement = placed.placement;
        let end = placement
            .position
            .checked_add(placement.length)
            .ok_or(DiskFault::Unreadable)?;
        let blob = self
            .wait(remote.read_fresh(placement.object_id, placement.position..end))
            .await?;
        if !placement.matches(&blob) {
            return Err(DiskFault::Tiered);
        }
        let mut header = placed.header;
        header.batch_checksum = header.checksum_for_blob(&blob);
        whole_prepare(&header, &blob)
    }

    /// One wait on the object store, which ends at the poll's deadline.
    ///
    /// A read that this shard answers from memory is ready at once, and most
    /// reads at the tail are. It takes no timer.
    async fn wait<T>(
        &self,
        read: impl Future<Output = Result<T, DiskFault>>,
    ) -> Result<T, DiskFault> {
        let mut read = std::pin::pin!(read);
        if let Poll::Ready(read) = futures::poll!(read.as_mut()) {
            return read;
        }
        let now = Instant::now();
        match self.deadline {
            None => read.await,
            Some(deadline) if deadline <= now => Err(DiskFault::Retryable),
            Some(deadline) => compio::time::timeout(deadline - now, read)
                .await
                .unwrap_or(Err(DiskFault::Retryable)),
        }
    }
}

/// The prepare a partition of whole batches would hold for the batch of
/// `header` and `blob`, which is what the selection of messages reads.
fn whole_prepare(header: &BatchHeader, blob: &[u8]) -> Result<Frozen<4096>, DiskFault> {
    let total = PREPARE_HEADER_SIZE + BATCH_HEADER_SIZE + blob.len();
    let mut prepare = Owned::<4096>::zeroed(total);
    let bytes = prepare.as_mut_slice();
    header.encode_into(&mut bytes[PREPARE_HEADER_SIZE..PREPARE_HEADER_SIZE + BATCH_HEADER_SIZE]);
    bytes[PREPARE_HEADER_SIZE + BATCH_HEADER_SIZE..].copy_from_slice(blob);
    let prepare_header =
        bytemuck::checked::from_bytes_mut::<PrepareHeader>(&mut bytes[..PREPARE_HEADER_SIZE]);
    prepare_header.command = Command::Prepare;
    prepare_header.operation = Operation::SendMessages;
    prepare_header.size = u32::try_from(total).map_err(|_| DiskFault::Unreadable)?;
    Ok(prepare.into())
}

/// What one chunk of a long-term segment gave.
enum Read {
    /// Bytes of whole batches walked from the chunk's start.
    Batches(u64),
    /// Not one whole batch: the first one is this long.
    Needs(u64),
}

/// Walk the whole batches at the start of `chunk`, which starts at a batch,
/// and take the ones the poll wants. Each one passes its checksums.
fn take_whole_batches(
    chunk: &[u8],
    wanted: &mut Wanted,
    batches: &mut Vec<Frozen<4096>>,
) -> Result<Read, DiskFault> {
    let mut at = 0;
    while !wanted.is_full() {
        let rest = &chunk[at..];
        if rest.len() < BATCH_HEADER_SIZE {
            break;
        }
        let header = BatchHeader::decode(rest).map_err(|_| DiskFault::Tiered)?;
        let total = header.total_size();
        if total > rest.len() {
            if at == 0 {
                return Ok(Read::Needs(total as u64));
            }
            break;
        }
        let batch = decode_batch_slice(&rest[..total]).map_err(|_| DiskFault::Tiered)?;
        if wanted.admit(&batch.header) {
            batches.push(whole_prepare(&batch.header, batch.blob())?);
        }
        at += total;
    }
    if at == 0 && !wanted.is_full() {
        // A tail shorter than a batch header is no batch.
        return Err(DiskFault::Tiered);
    }
    Ok(Read::Batches(at as u64))
}

/// What a poll still needs: where its first message is, and how many
/// messages and bytes it may take.
struct Wanted {
    query: MessageLookup,
    /// The offset after the last batch taken, so a batch that two sources
    /// hold is taken once.
    next_offset: u64,
    messages: u64,
    bytes: usize,
    max_bytes: usize,
}

impl Wanted {
    const fn new(query: MessageLookup, max_bytes: usize) -> Self {
        Self {
            query,
            next_offset: 0,
            messages: 0,
            bytes: 0,
            max_bytes,
        }
    }

    fn is_full(&self) -> bool {
        self.messages >= u64::from(self.query.count())
            || (self.messages > 0 && self.bytes >= self.max_bytes)
    }

    /// Whether the batches taken so far reach past `offset`.
    const fn is_past(&self, offset: u64) -> bool {
        self.next_offset > offset
    }

    /// Take no batch that starts at or below `offset` from here on.
    fn skip_through(&mut self, offset: u64) {
        self.next_offset = self.next_offset.max(offset.saturating_add(1));
    }

    /// Whether the first message of the poll can be in a segment of these
    /// bounds or before it, so the walk has to look into it.
    const fn may_start_in(&self, start_offset: u64, end_offset: u64, max_timestamp: u64) -> bool {
        match self.query {
            MessageLookup::Offset {
                offset, ceiling, ..
            } => end_offset >= offset && start_offset <= ceiling,
            MessageLookup::Timestamp {
                timestamp, ceiling, ..
            } => max_timestamp >= timestamp && start_offset <= ceiling,
        }
    }

    /// Whether the batch of `header` holds a message at or after the poll's
    /// start.
    const fn reaches(&self, header: &BatchHeader) -> bool {
        match self.query {
            MessageLookup::Offset { offset, .. } => {
                header.base_offset + header.message_count as u64 > offset
            }
            // A batch's messages are stamped from its base on, so one stamped
            // before the poll's start can still hold a message after it.
            MessageLookup::Timestamp { .. } => true,
        }
    }

    /// Take `batch` when the poll has not passed it and may still use it.
    fn take(&mut self, batch: PlacedBatch, placed: &mut Vec<PlacedBatch>) {
        if self.admit(&batch.header) {
            placed.push(batch);
        }
    }

    /// Count the batch of `header` towards what the poll takes, when the
    /// poll has not passed it and may still use it. Returns whether it did.
    fn admit(&mut self, header: &BatchHeader) -> bool {
        if self.is_full()
            || header.base_offset < self.next_offset
            || header.base_offset > self.query.ceiling()
            || !self.reaches(header)
        {
            return false;
        }
        self.next_offset = header.base_offset + u64::from(header.message_count);
        self.messages += self.counted_messages(header);
        self.bytes = self
            .bytes
            .saturating_add(usize::try_from(header.batch_length).unwrap_or(usize::MAX));
        true
    }

    /// Messages of the batch of `header` that an offset poll counts: those
    /// from its start on. A timestamp poll cannot tell before it reads them,
    /// so it counts none of a batch stamped before its start.
    fn counted_messages(&self, header: &BatchHeader) -> u64 {
        match self.query {
            MessageLookup::Offset { offset, .. } => {
                let end = header.base_offset + u64::from(header.message_count);
                end - offset.max(header.base_offset)
            }
            MessageLookup::Timestamp { timestamp, .. } if header.base_timestamp >= timestamp => {
                u64::from(header.message_count)
            }
            MessageLookup::Timestamp { .. } => 0,
        }
    }

    /// The index of the first of `count` placed batches in `file` that the
    /// walk reads: the last one that starts at or before the poll's start.
    async fn first_index(&self, file: &File, count: u64) -> Result<u64, DiskFault> {
        let (mut low, mut high) = (0, count);
        while low < high {
            let middle = low + (high - low) / 2;
            let Some(batch) = read_placed(file, middle, 1).await?.pop() else {
                return Err(DiskFault::Unreadable);
            };
            let before = match self.query {
                MessageLookup::Offset { offset, .. } => batch.header.base_offset <= offset,
                MessageLookup::Timestamp { timestamp, .. } => {
                    batch.header.base_timestamp < timestamp
                }
            };
            if before {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        Ok(low.saturating_sub(1))
    }

    /// Where in a long-term segment of `size` bytes the walk starts: the
    /// position its index gives for the last entry at or before the poll's
    /// start. The start of the segment when the index says nothing.
    fn first_position(&self, index: &[u8], size: u64) -> u64 {
        let entries = index.as_chunks::<IGGY_INDEX_SIZE>().0;
        let field = |entry: &[u8; IGGY_INDEX_SIZE], slot: usize| {
            let mut bytes = [0; size_of::<u64>()];
            let at = slot * size_of::<u64>();
            bytes.copy_from_slice(&entry[at..at + size_of::<u64>()]);
            u64::from_le_bytes(bytes)
        };
        let before = entries.partition_point(|entry| match self.query {
            MessageLookup::Offset { offset, .. } => field(entry, 0) <= offset,
            MessageLookup::Timestamp { timestamp, .. } => field(entry, 1) < timestamp,
        });
        before
            .checked_sub(1)
            .map(|last| field(&entries[last], 2))
            .filter(|position| *position < size)
            .unwrap_or(0)
    }
}

/// The index of the first of `count` placed batches in `file` that holds an
/// offset at or after `first`, or `count` when none does.
async fn first_reaching(file: &File, count: u64, first: u64) -> Result<u64, DiskFault> {
    let (mut low, mut high) = (0, count);
    while low < high {
        let middle = low + (high - low) / 2;
        let Some(batch) = read_placed(file, middle, 1).await?.pop() else {
            return Err(DiskFault::Unreadable);
        };
        if batch.header.base_offset + u64::from(batch.header.message_count) > first {
            high = middle;
        } else {
            low = middle + 1;
        }
    }
    Ok(low)
}

/// `count` placed batches of `file`, from the `index`-th one on. Each one
/// passes its own checksum, or the read fails.
async fn read_placed(file: &File, index: u64, count: u64) -> Result<Vec<PlacedBatch>, DiskFault> {
    let len = usize::try_from(count)
        .ok()
        .and_then(|count| count.checked_mul(PLACED_BATCH_SIZE))
        .ok_or(DiskFault::Unreadable)?;
    let position = index
        .checked_mul(PLACED_BATCH_SIZE as u64)
        .ok_or(DiskFault::Unreadable)?;
    let compio::BufResult(read, bytes) = file.read_exact_at(vec![0; len], position).await;
    read.map_err(|_| DiskFault::Retryable)?;
    bytes
        .as_chunks::<PLACED_BATCH_SIZE>()
        .0
        .iter()
        .map(|placed| PlacedBatch::decode(placed).map_err(|_| DiskFault::Unreadable))
        .collect()
}
