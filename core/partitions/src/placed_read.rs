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
use std::ops::Range;
use std::rc::Rc;
use std::task::Poll;
use std::time::Instant;

use bytes::Bytes;
use compio::fs::File;
use compio::io::AsyncReadAtExt;
use futures::{StreamExt, stream};
use iggy_binary_protocol::batch::{BATCH_HEADER_SIZE, BatchHeader};
use iggy_binary_protocol::placement::{self, PLACED_BATCH_SIZE, PlacedBatch};
use iggy_binary_protocol::{Command, Operation, PrepareHeader};
use iggy_object_store::{
    FreshObjectId, FreshProofRow, LongTermKey, ManifestSegment, ObjectPrefix, SegmentObject,
};
use server_common::iobuf::{Frozen, Owned};
use server_common::send_messages::{decode_batch_slice, decode_prepare_batch_trusted};
use server_common::yield_to_reactor;

use crate::RemoteSegments;
use crate::iggy_index::IGGY_INDEX_SIZE;
use crate::journal::MessageLookup;
use crate::poll_plan::{DiskFault, PartitionDirResolution};
use crate::remote_segments::{FRESH_READ_BYTES_MAX, FRESH_READ_GAP_BYTES, FreshReads};

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

/// Reads of fresh objects one rewrite has in flight at once.
pub const REWRITE_FETCHES: usize = 4;

/// Bytes of batches a rewrite takes between two yields to the shard's other
/// tasks. Copying and summing this many takes a fraction of a millisecond,
/// against about 12 us for a yield.
pub const REWRITE_YIELD_BYTES: usize = 1024 * 1024;

/// A piece of a batch as [`PlacedReadPlan::read_blobs`] hands it over: the
/// header first, then the messages in order, in as many pieces as it takes.
#[derive(Clone, Copy)]
pub enum BatchPiece<'a> {
    /// The header of the whole batch, with the batch checksum over its
    /// messages.
    Header(&'a BatchHeader),
    Messages(&'a [u8]),
}

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

/// What [`PlacedReadPlan::read`] returns.
pub struct PlacedRead {
    /// The batches that answer the query, each put back together as the
    /// prepare a partition of whole batches would hold, in offset order.
    pub(crate) batches: Vec<Frozen<4096>>,
    /// A byte limit, or the end of the long-term segments the plan carries,
    /// stopped the read ahead of a batch the query asks for.
    pub(crate) truncated: bool,
}

impl PlacedReadPlan {
    /// The batches that answer `query`.
    ///
    /// The read takes whole batches, and stops once they hold the messages
    /// the query counts. It also stops ahead of a batch that would take the
    /// batches it holds past `max_bytes`, or that follows a selection of
    /// `max_selected_bytes`, and is then [`PlacedRead::truncated`]. Until a
    /// batch holds a message of the query, no limit applies: that batch is
    /// read whatever its size, so a poll always makes progress.
    pub(crate) async fn read(
        &self,
        query: MessageLookup,
        max_bytes: usize,
        max_selected_bytes: usize,
    ) -> Result<PlacedRead, DiskFault> {
        let mut wanted = Wanted::new(query, max_bytes, max_selected_bytes);
        let mut batches = Vec::new();
        self.collect_from_long_term(&mut wanted, &mut batches)
            .await?;
        if let Some(floor) = self.long_term_floor {
            // The plan carries only so many long-term segments. A read that
            // ran out of them below the floor ends there: going on above the
            // floor would leave a gap.
            if !self.long_term.is_empty() && !wanted.is_past(floor) {
                return Ok(PlacedRead {
                    batches,
                    truncated: wanted.limited || !wanted.is_full(),
                });
            }
            wanted.skip_through(floor);
        }
        let placed = self.collect_placed(&mut wanted).await?;
        let same_object = |one: &PlacedBatch, other: &PlacedBatch| {
            one.placement.object_id == other.placement.object_id
        };
        batches.reserve(placed.len());
        for run in placed.chunk_by(same_object) {
            let fetched = self.fetch_blobs(run).await?;
            for placed in run {
                batches.push(self.assemble(placed, &fetched).await?);
            }
        }
        Ok(PlacedRead {
            batches,
            truncated: wanted.limited,
        })
    }

    /// The placed batches that answer `query`, in offset order and without
    /// their messages: what the segments hold, then what the journal does.
    /// They stop as [`Self::read`] does. The long-term segments are not
    /// read, so this is for a read that starts above the long-term floor.
    pub(crate) async fn placed_batches(
        &self,
        query: MessageLookup,
        max_bytes: usize,
    ) -> Result<Vec<PlacedBatch>, DiskFault> {
        self.collect_placed(&mut Wanted::new(query, usize::MAX, max_bytes))
            .await
    }

    /// Hand `take` each batch of `placed` in order, as [`BatchPiece`]s: its
    /// header with the batch checksum it has over its messages, and the
    /// messages, checked against the placement that names them.
    ///
    /// The messages this shard holds in memory come from there. The others
    /// come from the store: the batches that follow each other closely in
    /// one fresh object in one read, and up to [`REWRITE_FETCHES`] reads at
    /// a time. Nothing read here is kept for the polls. No deadline ends
    /// the reads: the caller is a background job that its scope cancels.
    ///
    /// The loop gives way to the other tasks of the shard every
    /// [`REWRITE_YIELD_BYTES`], inside a batch too, since a segment's worth
    /// of messages takes tens of milliseconds to copy and sum.
    pub(crate) async fn read_blobs(
        &self,
        placed: &[PlacedBatch],
        mut take: impl FnMut(BatchPiece<'_>),
    ) -> Result<(), DiskFault> {
        // A shard that has not connected its store yet cannot read the
        // messages now, and can once it has.
        let remote = self.remote.as_deref().ok_or(DiskFault::Retryable)?;
        let reads = BlobReads {
            remote,
            placed,
            next: 0,
        };
        let mut fetched = stream::iter(reads)
            .map(|read| read.fetch(remote))
            .buffered(REWRITE_FETCHES);
        let mut since_yield = 0;
        while let Some(read) = fetched.next().await {
            let (batches, start, bytes) = read?;
            for placed in &placed[batches] {
                let blob = placed
                    .placement
                    .position
                    .checked_sub(start)
                    .and_then(|at| usize::try_from(at).ok())
                    .zip(usize::try_from(placed.placement.length).ok())
                    .and_then(|(at, len)| bytes.get(at..at.checked_add(len)?))
                    .ok_or(DiskFault::Tiered)?;
                take(BatchPiece::Header(&whole_header(placed, blob)?));
                since_yield += BATCH_HEADER_SIZE;
                let mut messages = blob;
                loop {
                    if since_yield >= REWRITE_YIELD_BYTES {
                        since_yield = 0;
                        yield_to_reactor().await;
                    }
                    if messages.is_empty() {
                        break;
                    }
                    let (piece, rest) =
                        messages.split_at(messages.len().min(REWRITE_YIELD_BYTES - since_yield));
                    take(BatchPiece::Messages(piece));
                    since_yield += piece.len();
                    messages = rest;
                }
            }
        }
        Ok(())
    }

    /// Bytes of the whole batches whose first offset is from `from` to
    /// `up_to`, as their headers state them, read from the segments and the
    /// journal without their messages. A batch that both hold counts once.
    pub(crate) async fn batch_bytes(&self, from: u64, up_to: u64) -> Result<u64, DiskFault> {
        let mut counted = CountedBatches {
            next_offset: from,
            up_to,
            bytes: 0,
        };
        match &self.partition_dir {
            PartitionDirResolution::Resolved(directory) => {
                self.count_in_segments(directory, &mut counted).await?;
            }
            PartitionDirResolution::NoFiles => {}
            PartitionDirResolution::Unresolvable => return Err(DiskFault::Retryable),
        }
        for entry in &self.resident {
            if let Ok(batch) = decode_prepare_batch_trusted(entry.as_slice())
                && batch.placement.is_some()
            {
                counted.take(&batch.header);
            }
        }
        Ok(counted.bytes)
    }

    /// The placed batches `wanted` takes from the segments and the journal.
    async fn collect_placed(&self, wanted: &mut Wanted) -> Result<Vec<PlacedBatch>, DiskFault> {
        let mut placed = Vec::new();
        self.collect_from_segments(wanted, &mut placed).await?;
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
        Ok(placed)
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
                .wait(
                    remote,
                    remote.read_long_term_object(&index_key, 0..LONG_TERM_INDEX_BYTES),
                )
                .await?;
            let log = key(SegmentObject::Log);
            let mut position = wanted.first_position(&index, segment.size);
            let mut len = LONG_TERM_READ_BYTES;
            while position < segment.size && !wanted.is_full() {
                let end = segment.size.min(position.saturating_add(len));
                let chunk = self
                    .wait(remote, remote.read_long_term_object(&log, position..end))
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

    /// The proof rows of the committed placed batches from `from` to
    /// `up_to`, the last committed offset, from the segments and the
    /// journal, without their messages.
    ///
    /// The newest committed placement claims every batch below its
    /// `proven_below` proven, so the read starts there when that is past
    /// `from`, and reads the placed records of the tail that may lack a
    /// stored proof rather than every one above the floor.
    pub(crate) async fn proof_rows(
        &self,
        from: u64,
        up_to: u64,
    ) -> Result<WalkedProofs, DiskFault> {
        let claim = self.newest_claim(up_to).await?;
        let start = from.max(claim);
        let mut walked = WalkedProofs::starting_at(start, up_to, claim);
        let directory = match &self.partition_dir {
            PartitionDirResolution::Resolved(directory) => Some(directory),
            PartitionDirResolution::NoFiles => None,
            PartitionDirResolution::Unresolvable => return Err(DiskFault::Retryable),
        };
        let segments = self
            .segments
            .iter()
            .filter(|segment| segment.end_offset >= start && segment.start_offset <= up_to);
        for segment in segments {
            let Some(directory) = directory else {
                break;
            };
            let path = format!("{directory}/{:0>20}.log", segment.start_offset);
            let file = File::open(&path).await.map_err(|_| DiskFault::Retryable)?;
            let count = segment.size / PLACED_BATCH_SIZE as u64;
            let mut index = first_reaching(&file, count, start).await?;
            while index < count {
                let batches = PLACED_PER_READ.min(count - index);
                for (batch, checksum) in read_placed_records(&file, index, batches).await? {
                    walked.take(&batch, checksum);
                }
                index += batches;
            }
        }
        for entry in &self.resident {
            let Ok(batch) = decode_prepare_batch_trusted(entry.as_slice()) else {
                return Err(DiskFault::Unreadable);
            };
            if let Some(placement) = batch.placement {
                walked.take(
                    &PlacedBatch::new(batch.header, placement),
                    batch.body_checksum,
                );
            }
        }
        if walked.broken {
            return Err(DiskFault::Unreadable);
        }
        Ok(walked)
    }

    /// The `proven_below` of the newest placed batch at or below `up_to`,
    /// zero when there is none.
    async fn newest_claim(&self, up_to: u64) -> Result<u64, DiskFault> {
        for entry in self.resident.iter().rev() {
            if let Ok(batch) = decode_prepare_batch_trusted(entry.as_slice())
                && let Some(placement) = batch.placement
                && batch.header.base_offset <= up_to
            {
                return Ok(placement.proven_below);
            }
        }
        let PartitionDirResolution::Resolved(directory) = &self.partition_dir else {
            return Ok(0);
        };
        let newest = self
            .segments
            .iter()
            .rev()
            .find(|segment| segment.start_offset <= up_to && segment.size > 0);
        let Some(segment) = newest else {
            return Ok(0);
        };
        let path = format!("{directory}/{:0>20}.log", segment.start_offset);
        let file = File::open(&path).await.map_err(|_| DiskFault::Retryable)?;
        let count = segment.size / PLACED_BATCH_SIZE as u64;
        let last = read_placed(&file, count.saturating_sub(1), 1).await?;
        Ok(last
            .first()
            .filter(|batch| batch.header.base_offset <= up_to)
            .map_or(0, |batch| batch.placement.proven_below))
    }

    /// Add to `counted` the batches of the segments in `directory`.
    async fn count_in_segments(
        &self,
        directory: &str,
        counted: &mut CountedBatches,
    ) -> Result<(), DiskFault> {
        let (from, up_to) = (counted.next_offset, counted.up_to);
        let start = Wanted::new(
            MessageLookup::Offset {
                offset: from,
                count: u32::MAX,
                ceiling: up_to,
            },
            usize::MAX,
            usize::MAX,
        );
        let segments = self
            .segments
            .iter()
            .filter(|segment| segment.end_offset >= from && segment.start_offset <= up_to);
        for segment in segments {
            let path = format!("{directory}/{:0>20}.log", segment.start_offset);
            let file = File::open(&path).await.map_err(|_| DiskFault::Retryable)?;
            let count = segment.size / PLACED_BATCH_SIZE as u64;
            let mut index = start.first_index(&file, count).await?;
            while index < count {
                let batches = PLACED_PER_READ.min(count - index);
                for batch in read_placed(&file, index, batches).await? {
                    counted.take(&batch.header);
                }
                index += batches;
            }
        }
        Ok(())
    }

    /// Fetch the blobs of `run`, batches of one fresh object, so the batches
    /// that follow each other in it cost one read of the store and not one
    /// each. A batch alone in its object is left to [`Self::assemble`], which
    /// reads it the same way.
    async fn fetch_blobs(&self, run: &[PlacedBatch]) -> Result<FreshReads, DiskFault> {
        let (Some(remote), [first, _, ..]) = (self.remote.as_deref(), run) else {
            return Ok(FreshReads::default());
        };
        let blobs: Vec<_> = run
            .iter()
            .filter_map(|placed| {
                let placement = placed.placement;
                let end = placement.position.checked_add(placement.length)?;
                Some(placement.position..end)
            })
            .collect();
        self.wait(
            remote,
            remote.fetch_fresh(first.placement.object_id, &blobs),
        )
        .await
    }

    /// The whole batch of `placed`: its header with the batch checksum it
    /// has over its messages, and the messages its placement names, out of
    /// `fetched` when they are there.
    async fn assemble(
        &self,
        placed: &PlacedBatch,
        fetched: &FreshReads,
    ) -> Result<Frozen<4096>, DiskFault> {
        // A shard that has not connected its store yet cannot read the
        // messages now, and can once it has.
        let remote = self.remote.as_deref().ok_or(DiskFault::Retryable)?;
        let placement = placed.placement;
        let end = placement
            .position
            .checked_add(placement.length)
            .ok_or(DiskFault::Unreadable)?;
        let range = placement.position..end;
        let blob = if let Some(blob) = fetched.get(placement.object_id, &range) {
            blob
        } else {
            self.wait(remote, remote.read_fresh(placement.object_id, range))
                .await?
        };
        whole_prepare(&whole_header(placed, &blob)?, &blob)
    }

    /// One wait on the object store `remote`, which ends at the poll's
    /// deadline.
    ///
    /// A read that this shard answers from memory is ready at once, and most
    /// reads at the tail are. It takes no timer.
    async fn wait<T>(
        &self,
        remote: &RemoteSegments,
        read: impl Future<Output = Result<T, DiskFault>>,
    ) -> Result<T, DiskFault> {
        let mut read = std::pin::pin!(read);
        if let Poll::Ready(read) = futures::poll!(read.as_mut()) {
            return read;
        }
        let runtime = remote.runtime();
        let now = runtime.now();
        match self.deadline {
            None => read.await,
            Some(deadline) if deadline <= now => Err(DiskFault::Retryable),
            Some(deadline) => runtime
                .timeout(deadline - now, read)
                .await
                .unwrap_or(Err(DiskFault::Retryable)),
        }
    }
}

/// One read of a rewrite: the batches of `placed` it covers, and where
/// their messages are.
struct BlobRead {
    batches: Range<usize>,
    object_id: u128,
    range: Range<u64>,
    /// The bytes at `range`, when this shard held them as the read was
    /// planned.
    cached: Option<Bytes>,
}

impl BlobRead {
    /// The batches the read covers, where its bytes start in the object,
    /// and the bytes.
    async fn fetch(self, remote: &RemoteSegments) -> Result<(Range<usize>, u64, Bytes), DiskFault> {
        let bytes = match self.cached {
            Some(bytes) => bytes,
            None => {
                remote
                    .read_fresh_from_store(self.object_id, self.range.clone())
                    .await?
            }
        };
        Ok((self.batches, self.range.start, bytes))
    }
}

/// The reads that cover `placed` in order, planned as they are asked for.
struct BlobReads<'a> {
    remote: &'a RemoteSegments,
    placed: &'a [PlacedBatch],
    next: usize,
}

impl BlobReads<'_> {
    /// Where the messages of the batch at `index` are, and the messages
    /// when this shard holds them. `None` past the last batch, and for a
    /// placement that names no range.
    fn blob(&self, index: usize) -> Option<(u128, Range<u64>, Option<Bytes>)> {
        let placement = self.placed.get(index)?.placement;
        let range = placement.position..placement.position.checked_add(placement.length)?;
        let cached = if range.is_empty() {
            Some(Bytes::new())
        } else {
            self.remote.cached_fresh(placement.object_id, &range)
        };
        Some((placement.object_id, range, cached))
    }
}

impl Iterator for BlobReads<'_> {
    type Item = BlobRead;

    fn next(&mut self) -> Option<BlobRead> {
        let first = self.next;
        let placement = self.placed.get(first)?.placement;
        // A placement that names no range reads as an empty one, which the
        // batch then fails to match.
        let (object_id, mut range, cached) =
            self.blob(first)
                .unwrap_or((placement.object_id, 0..0, Some(Bytes::new())));
        self.next += 1;
        if cached.is_none() {
            while let Some((next_object, next, next_cached)) = self.blob(self.next) {
                let close = next
                    .start
                    .checked_sub(range.end)
                    .is_some_and(|gap| gap <= FRESH_READ_GAP_BYTES);
                let follows = next_object == object_id
                    && close
                    && next.end - range.start <= FRESH_READ_BYTES_MAX;
                if !follows || next_cached.is_some() {
                    break;
                }
                range.end = next.end;
                self.next += 1;
            }
        }
        Some(BlobRead {
            batches: first..self.next,
            object_id,
            range,
            cached,
        })
    }
}

/// The header of the whole batch of `placed`, whose messages are `blob`:
/// the one it has, with the batch checksum over the messages.
///
/// This is the one place that checks the messages of a placed batch
/// against what its producer sent, so every read of one goes through it.
fn whole_header(placed: &PlacedBatch, blob: &[u8]) -> Result<BatchHeader, DiskFault> {
    if !placed.placement.matches(blob) {
        return Err(DiskFault::Tiered);
    }
    let mut header = placed.header;
    header.batch_checksum = header.checksum_for_blob(blob);
    Ok(header)
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
        if wanted.admit(&batch.header, PREPARE_HEADER_SIZE + total) {
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
    /// Bytes of the batches taken, as a reply counts them.
    selected_bytes: usize,
    max_selected_bytes: usize,
    /// Bytes of the batches taken, as the read holds them once they are put
    /// back together.
    held_bytes: usize,
    max_bytes: usize,
    /// A byte limit turned away a batch the poll would have taken. Nothing
    /// is taken after it, or the poll would skip its messages.
    limited: bool,
}

impl Wanted {
    const fn new(query: MessageLookup, max_bytes: usize, max_selected_bytes: usize) -> Self {
        Self {
            query,
            next_offset: 0,
            messages: 0,
            selected_bytes: 0,
            max_selected_bytes,
            held_bytes: 0,
            max_bytes,
            limited: false,
        }
    }

    fn is_full(&self) -> bool {
        self.limited || self.messages >= u64::from(self.query.count())
    }

    /// Whether taking a batch that the read holds as `held` bytes would pass
    /// a byte limit. Never before a batch counted a message of the poll.
    const fn over_limit(&self, held: usize) -> bool {
        self.messages > 0
            && (self.selected_bytes >= self.max_selected_bytes
                || self.held_bytes.saturating_add(held) > self.max_bytes)
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
        if self.admit(&batch.header, held_bytes(&batch)) {
            placed.push(batch);
        }
    }

    /// Count the batch of `header`, which the read holds as `held` bytes,
    /// towards what the poll takes, when the poll has not passed it and may
    /// still use it. Returns whether it did.
    fn admit(&mut self, header: &BatchHeader, held: usize) -> bool {
        if self.is_full()
            || header.base_offset < self.next_offset
            || header.base_offset > self.query.ceiling()
            || !self.reaches(header)
        {
            return false;
        }
        if self.over_limit(held) {
            self.limited = true;
            return false;
        }
        self.next_offset = header.base_offset + u64::from(header.message_count);
        self.messages += self.counted_messages(header);
        self.selected_bytes = self
            .selected_bytes
            .saturating_add(usize::try_from(header.batch_length).unwrap_or(usize::MAX));
        self.held_bytes = self.held_bytes.saturating_add(held);
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

/// The bytes of the batches a count of [`PlacedReadPlan::batch_bytes`] took
/// so far.
struct CountedBatches {
    /// The offset after the last batch counted, so a batch that two sources
    /// hold counts once.
    next_offset: u64,
    up_to: u64,
    bytes: u64,
}

impl CountedBatches {
    fn take(&mut self, header: &BatchHeader) {
        if header.base_offset < self.next_offset || header.base_offset > self.up_to {
            return;
        }
        self.bytes = self.bytes.saturating_add(header.batch_length);
        self.next_offset = header
            .base_offset
            .saturating_add(u64::from(header.message_count.max(1)));
    }
}

/// The proof rows a walk of [`PlacedReadPlan::proof_rows`] found: one per
/// committed placed batch from [`Self::from`] to the last committed offset,
/// contiguous.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalkedProofs {
    /// Where the rows start.
    pub(crate) from: u64,
    up_to: u64,
    /// One past the last offset of the last row, or `from` without a row.
    pub(crate) end: u64,
    pub(crate) rows: Vec<FreshProofRow>,
    /// The highest `proven_below` among the placements read.
    pub(crate) highest_claim: u64,
    /// A batch did not follow the one before it.
    broken: bool,
}

impl WalkedProofs {
    /// What a walk that found `rows`, contiguous, would hand back.
    #[cfg(test)]
    pub(crate) fn of(rows: Vec<FreshProofRow>, highest_claim: u64) -> Self {
        let mut walked = Self::starting_at(0, u64::MAX, highest_claim);
        for row in rows {
            walked.from = if walked.rows.is_empty() {
                row.base_offset
            } else {
                walked.from
            };
            walked.end = row.base_offset + u64::from(row.message_count);
            walked.rows.push(row);
        }
        walked
    }

    const fn starting_at(from: u64, up_to: u64, claim: u64) -> Self {
        Self {
            from,
            up_to,
            end: from,
            rows: Vec::new(),
            highest_claim: claim,
            broken: false,
        }
    }

    /// Take `batch`, whose placed record carries `checksum`, when it is in
    /// the walk's range and not taken already from another source.
    fn take(&mut self, batch: &PlacedBatch, checksum: u64) {
        let header = &batch.header;
        if header.base_offset < self.end || header.base_offset > self.up_to {
            return;
        }
        if self.rows.is_empty() {
            self.from = header.base_offset;
        } else if header.base_offset != self.end {
            self.broken = true;
            return;
        }
        self.end = header
            .base_offset
            .saturating_add(u64::from(header.message_count));
        self.highest_claim = self.highest_claim.max(batch.placement.proven_below);
        self.rows.push(FreshProofRow {
            object_id: FreshObjectId::from_raw(batch.placement.object_id),
            position: batch.placement.position,
            base_offset: header.base_offset,
            base_timestamp: header.base_timestamp,
            message_count: header.message_count,
            placed_checksum: checksum,
        });
    }
}

/// Bytes of the buffer that holds `batch` once [`PlacedReadPlan::assemble`]
/// put it back together.
fn held_bytes(batch: &PlacedBatch) -> usize {
    usize::try_from(batch.placement.length)
        .unwrap_or(usize::MAX)
        .saturating_add(PREPARE_HEADER_SIZE + BATCH_HEADER_SIZE)
}

/// `count` placed batches of `file`, from the `index`-th one on. Each one
/// passes its own checksum, or the read fails.
async fn read_placed(file: &File, index: u64, count: u64) -> Result<Vec<PlacedBatch>, DiskFault> {
    Ok(read_placed_records(file, index, count)
        .await?
        .into_iter()
        .map(|(batch, _)| batch)
        .collect())
}

/// [`read_placed`], each batch with the checksum its record carries.
async fn read_placed_records(
    file: &File,
    index: u64,
    count: u64,
) -> Result<Vec<(PlacedBatch, u64)>, DiskFault> {
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
        .map(|placed| {
            PlacedBatch::decode(placed)
                .map(|batch| (batch, placement::checksum_of(placed)))
                .map_err(|_| DiskFault::Unreadable)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RemoteObjects;
    use futures::FutureExt;
    use futures::future::LocalBoxFuture;
    use iggy_binary_protocol::placement::Placement;
    use iggy_object_store::{KeyLayout, ObjectRange, ObjectStoreError};
    use std::cell::{Cell, RefCell};
    use std::collections::HashMap;
    use std::time::Duration;

    /// Fresh objects by key. Every read waits one turn before it answers,
    /// so reads that run at the same time are seen to.
    #[derive(Default)]
    struct Objects {
        held: RefCell<HashMap<String, Bytes>>,
        asked: RefCell<Vec<(String, Range<u64>)>>,
        in_flight: Cell<usize>,
        most_in_flight: Cell<usize>,
    }

    impl RemoteObjects for Objects {
        fn get_range<'a>(
            &'a self,
            key: &'a str,
            range: Range<u64>,
        ) -> LocalBoxFuture<'a, Result<ObjectRange, ObjectStoreError>> {
            self.asked
                .borrow_mut()
                .push((key.to_owned(), range.clone()));
            self.in_flight.set(self.in_flight.get() + 1);
            self.most_in_flight
                .set(self.most_in_flight.get().max(self.in_flight.get()));
            async move {
                yield_to_reactor().await;
                self.in_flight.set(self.in_flight.get() - 1);
                let object = self.held.borrow().get(key).cloned().ok_or_else(|| {
                    ObjectStoreError::NotFound {
                        key: key.to_owned(),
                    }
                })?;
                let end = usize::try_from(range.end).unwrap().min(object.len());
                Ok(ObjectRange {
                    bytes: object.slice(usize::try_from(range.start).unwrap()..end),
                    etag: String::new(),
                    size: object.len() as u64,
                })
            }
            .boxed_local()
        }
    }

    fn layout() -> KeyLayout {
        KeyLayout::new("iggy").unwrap()
    }

    /// A plan that reads nothing from the disk and its fresh objects from
    /// `objects`.
    fn plan(objects: &Rc<Objects>) -> PlacedReadPlan {
        PlacedReadPlan {
            partition_dir: PartitionDirResolution::NoFiles,
            segments: Vec::new(),
            resident: Vec::new(),
            remote: Some(Rc::new(RemoteSegments::new(
                Rc::clone(objects) as Rc<dyn RemoteObjects>,
                layout(),
            ))),
            deadline: None,
            prefix: ObjectPrefix {
                stream_id: 1,
                topic_id: 1,
                partition_id: 0,
                created_revision: 0,
            },
            long_term: Vec::new(),
            long_term_floor: None,
        }
    }

    /// The fresh object `object_id` with one blob of `blob_len` bytes for
    /// each of `count` batches from `base_offset` on, `gap` bytes apart,
    /// and the placed batches that name them.
    fn object_of(
        object_id: u128,
        base_offset: u64,
        count: u64,
        blob_len: usize,
        gap: usize,
    ) -> (Bytes, Vec<PlacedBatch>) {
        let mut object = Vec::new();
        let placed = (0..count)
            .map(|batch| {
                object.resize(object.len() + gap, 0xEE);
                let position = object.len() as u64;
                let fill = u8::try_from((base_offset + batch) % 251).unwrap();
                object.resize(object.len() + blob_len, fill);
                let blob = &object[object.len() - blob_len..];
                let mut header = BatchHeader::new(0, 0, (BATCH_HEADER_SIZE + blob_len) as u64, 1);
                header.base_offset = base_offset + batch;
                PlacedBatch::new(header, Placement::of(object_id, position, blob, 0))
            })
            .collect();
        (Bytes::from(object), placed)
    }

    /// The offsets and the messages [`PlacedReadPlan::read_blobs`] hands
    /// over for `placed`.
    async fn blobs_of(
        plan: &PlacedReadPlan,
        placed: &[PlacedBatch],
    ) -> Result<Vec<(u64, Vec<u8>)>, DiskFault> {
        let mut taken: Vec<(BatchHeader, Vec<u8>)> = Vec::new();
        plan.read_blobs(placed, |piece| match piece {
            BatchPiece::Header(header) => taken.push((*header, Vec::new())),
            BatchPiece::Messages(messages) => taken.last_mut().unwrap().1.extend(messages),
        })
        .await?;
        Ok(taken
            .into_iter()
            .map(|(header, blob)| {
                assert_eq!(header.batch_checksum, header.checksum_for_blob(&blob));
                (header.base_offset, blob)
            })
            .collect())
    }

    /// The bytes [`PlacedReadPlan::read_blobs`] hands over between two of
    /// its yields while it reads `placed`, and after the last one.
    async fn stretches_of(plan: &PlacedReadPlan, placed: &[PlacedBatch]) -> Vec<usize> {
        let handed = Cell::new(0);
        let mut reading = std::pin::pin!(plan.read_blobs(placed, |piece| {
            let len = match piece {
                BatchPiece::Header(_) => BATCH_HEADER_SIZE,
                BatchPiece::Messages(messages) => messages.len(),
            };
            handed.set(handed.get() + len);
        }));
        let mut stretches = Vec::new();
        loop {
            let polled = futures::poll!(reading.as_mut());
            stretches.push(handed.replace(0));
            if let Poll::Ready(read) = polled {
                read.unwrap();
                return stretches;
            }
            // Long enough for the timer of the yield to have fired.
            compio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    fn expected(objects: &[(Bytes, Vec<PlacedBatch>)]) -> Vec<(u64, Vec<u8>)> {
        objects
            .iter()
            .flat_map(|(object, placed)| {
                placed.iter().map(|placed| {
                    let start = usize::try_from(placed.placement.position).unwrap();
                    let len = usize::try_from(placed.placement.length).unwrap();
                    (
                        placed.header.base_offset,
                        object[start..start + len].to_vec(),
                    )
                })
            })
            .collect()
    }

    /// Six objects: three batches in each of the first five, and one alone
    /// in the last.
    fn cold_objects(store: &Objects) -> Vec<(Bytes, Vec<PlacedBatch>)> {
        let objects: Vec<_> = (0..6_u64)
            .map(|object| {
                let count = if object == 5 { 1 } else { 3 };
                object_of(u128::from(object) + 1, object * 3, count, 100, 10)
            })
            .collect();
        for (id, (bytes, _)) in (1_u128..).zip(&objects) {
            store
                .held
                .borrow_mut()
                .insert(layout().fresh_key(id), bytes.clone());
        }
        objects
    }

    #[compio::test]
    async fn given_batches_the_shard_does_not_hold_when_reading_their_blobs_should_ask_once_an_object_and_several_at_a_time()
     {
        let store = Rc::new(Objects::default());
        let objects = cold_objects(&store);
        let placed: Vec<_> = objects
            .iter()
            .flat_map(|(_, placed)| placed.clone())
            .collect();
        let plan = plan(&store);

        let taken = blobs_of(&plan, &placed).await.unwrap();

        assert_eq!(taken, expected(&objects));
        let asked = store.asked.borrow();
        assert_eq!(asked.len(), objects.len(), "one read an object");
        assert_eq!(asked[0], (layout().fresh_key(1), 10..330));
        assert_eq!(asked[5], (layout().fresh_key(6), 10..110));
        assert_eq!(store.most_in_flight.get(), REWRITE_FETCHES);
    }

    #[compio::test]
    async fn given_blobs_far_apart_in_one_object_when_reading_them_should_ask_for_each_run() {
        let store = Rc::new(Objects::default());
        let gap = usize::try_from(FRESH_READ_GAP_BYTES).unwrap() + 1;
        let (object, placed) = object_of(1, 0, 2, 100, gap);
        store
            .held
            .borrow_mut()
            .insert(layout().fresh_key(1), object.clone());
        let plan = plan(&store);

        let taken = blobs_of(&plan, &placed).await.unwrap();

        assert_eq!(taken, expected(&[(object, placed)]));
        assert_eq!(store.asked.borrow().len(), 2);
    }

    #[compio::test]
    async fn given_batches_the_shard_does_not_hold_when_reading_their_blobs_should_keep_none_for_the_polls()
     {
        let store = Rc::new(Objects::default());
        let objects = cold_objects(&store);
        let placed: Vec<_> = objects
            .iter()
            .flat_map(|(_, placed)| placed.clone())
            .collect();
        let plan = plan(&store);
        let remote = plan.remote.clone().unwrap();

        blobs_of(&plan, &placed).await.unwrap();

        for placed in &placed {
            let placement = placed.placement;
            let range = placement.position..placement.position + placement.length;
            assert_eq!(remote.cached_fresh(placement.object_id, &range), None);
        }
    }

    #[compio::test]
    async fn given_blobs_the_shard_holds_when_reading_them_should_not_ask_the_store() {
        let store = Rc::new(Objects::default());
        let (object, placed) = object_of(1, 0, 3, 100, 10);
        let plan = plan(&store);
        plan.remote
            .as_ref()
            .unwrap()
            .cache_fresh_object(1, object.clone());

        let taken = blobs_of(&plan, &placed).await.unwrap();

        assert_eq!(taken, expected(&[(object, placed)]));
        assert!(store.asked.borrow().is_empty());
    }

    #[compio::test]
    async fn given_a_blob_other_than_the_one_its_placement_names_when_reading_should_fail() {
        let store = Rc::new(Objects::default());
        let (object, placed) = object_of(1, 0, 3, 100, 10);
        let mut changed = object.to_vec();
        let last = changed.len() - 1;
        changed[last] ^= 1;
        let plan = plan(&store);
        plan.remote
            .as_ref()
            .unwrap()
            .cache_fresh_object(1, Bytes::from(changed));

        assert_eq!(blobs_of(&plan, &placed).await, Err(DiskFault::Tiered));
    }

    /// Every batch is in memory, so nothing but the yields returns control
    /// while a segment is put together.
    #[compio::test]
    async fn given_more_bytes_than_a_stride_in_memory_when_reading_the_blobs_should_yield_once_a_stride()
     {
        let store = Rc::new(Objects::default());
        let blob_len = REWRITE_YIELD_BYTES / 4 - BATCH_HEADER_SIZE;
        let (object, placed) = object_of(1, 0, 8, blob_len, 0);
        let plan = plan(&store);
        plan.remote.as_ref().unwrap().cache_fresh_object(1, object);

        let stretches = stretches_of(&plan, &placed).await;

        assert_eq!(stretches, [REWRITE_YIELD_BYTES, REWRITE_YIELD_BYTES, 0]);
    }

    #[compio::test]
    async fn given_a_batch_larger_than_a_stride_when_reading_its_blob_should_yield_inside_it() {
        let store = Rc::new(Objects::default());
        let blob_len = 2 * REWRITE_YIELD_BYTES + REWRITE_YIELD_BYTES / 2 - BATCH_HEADER_SIZE;
        let (object, placed) = object_of(1, 0, 1, blob_len, 0);
        let plan = plan(&store);
        plan.remote
            .as_ref()
            .unwrap()
            .cache_fresh_object(1, object.clone());

        let stretches = stretches_of(&plan, &placed).await;

        assert_eq!(
            stretches,
            [
                REWRITE_YIELD_BYTES,
                REWRITE_YIELD_BYTES,
                REWRITE_YIELD_BYTES / 2
            ]
        );
        assert_eq!(
            blobs_of(&plan, &placed).await.unwrap(),
            expected(&[(object, placed)])
        );
    }

    /// The journal entry of `placed`.
    fn journal_entry(placed: &PlacedBatch) -> Frozen<4096> {
        let total = PREPARE_HEADER_SIZE + PLACED_BATCH_SIZE;
        let mut prepare = Owned::<4096>::zeroed(total);
        let bytes = prepare.as_mut_slice();
        placed.encode_into(&mut bytes[PREPARE_HEADER_SIZE..]);
        let header =
            bytemuck::checked::from_bytes_mut::<PrepareHeader>(&mut bytes[..PREPARE_HEADER_SIZE]);
        header.command = Command::Prepare;
        header.operation = Operation::SendMessages;
        header.size = u32::try_from(total).unwrap();
        prepare.into()
    }

    /// Six batches of one message each, of different lengths: offsets 0 to
    /// 3 in a segment file, and 3 to 5 in the journal, which the segment
    /// has yet to drop. Only the lengths of 1 to 4 count.
    #[compio::test]
    async fn given_batches_in_a_segment_and_in_the_journal_when_counting_their_bytes_should_count_each_one_once()
     {
        let directory = tempfile::tempdir().unwrap();
        let placed: Vec<_> = (0..6_u64)
            .map(|offset| {
                let length = 10 * (offset + 1);
                let mut header = BatchHeader::new(0, 0, BATCH_HEADER_SIZE as u64 + length, 1);
                header.base_offset = offset;
                let blob = vec![0; usize::try_from(length).unwrap()];
                PlacedBatch::new(header, Placement::of(1, 0, &blob, 0))
            })
            .collect();
        let mut log = vec![0; 4 * PLACED_BATCH_SIZE];
        for (record, placed) in log.chunks_mut(PLACED_BATCH_SIZE).zip(&placed) {
            placed.encode_into(record);
        }
        std::fs::write(directory.path().join(format!("{:020}.log", 0)), &log).unwrap();
        let store = Rc::new(Objects::default());
        let plan = PlacedReadPlan {
            partition_dir: PartitionDirResolution::Resolved(
                directory.path().to_string_lossy().into_owned(),
            ),
            segments: vec![PlacedSegment {
                start_offset: 0,
                end_offset: 3,
                max_timestamp: 0,
                size: log.len() as u64,
            }],
            resident: placed[3..].iter().map(journal_entry).collect(),
            ..plan(&store)
        };
        let lengths: u64 = placed[1..5]
            .iter()
            .map(|placed| placed.header.batch_length)
            .sum();

        assert_eq!(plan.batch_bytes(1, 4).await, Ok(lengths));
    }
}
