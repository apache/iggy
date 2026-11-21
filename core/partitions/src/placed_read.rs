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

use std::rc::Rc;
use std::time::Instant;

use compio::fs::File;
use compio::io::AsyncReadAtExt;
use iggy_binary_protocol::batch::BATCH_HEADER_SIZE;
use iggy_binary_protocol::placement::{PLACED_BATCH_SIZE, PlacedBatch};
use iggy_binary_protocol::{Command, Operation, PrepareHeader};
use server_common::iobuf::{Frozen, Owned};
use server_common::send_messages::decode_prepare_batch_trusted;

use crate::RemoteSegments;
use crate::journal::MessageLookup;
use crate::poll_plan::{DiskFault, PartitionDirResolution};
use crate::remote_segments::FreshReads;

const PREPARE_HEADER_SIZE: usize = size_of::<PrepareHeader>();

/// Placed batches one read of a segment asks the disk for.
const PLACED_PER_READ: u64 = 256;

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
}

/// What [`PlacedReadPlan::read`] returns.
pub struct PlacedRead {
    /// The batches that answer the query, each put back together as the
    /// prepare a partition of whole batches would hold, in offset order.
    pub(crate) batches: Vec<Frozen<4096>>,
    /// A byte limit stopped the read ahead of a batch the query asks for.
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
        let same_object = |one: &PlacedBatch, other: &PlacedBatch| {
            one.placement.object_id == other.placement.object_id
        };
        let mut batches = Vec::with_capacity(placed.len());
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
        let fetch = remote.fetch_fresh(first.placement.object_id, &blobs);
        let now = Instant::now();
        match self.deadline {
            None => fetch.await,
            Some(deadline) if deadline <= now => Err(DiskFault::Retryable),
            Some(deadline) => compio::time::timeout(deadline - now, fetch)
                .await
                .unwrap_or(Err(DiskFault::Retryable)),
        }
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
            if !wanted.may_start_in(segment) {
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
            let fetch = remote.read_fresh(placement.object_id, range);
            let now = Instant::now();
            match self.deadline {
                None => fetch.await,
                Some(deadline) if deadline <= now => return Err(DiskFault::Retryable),
                Some(deadline) => compio::time::timeout(deadline - now, fetch)
                    .await
                    .unwrap_or(Err(DiskFault::Retryable)),
            }?
        };
        if !placement.matches(&blob) {
            return Err(DiskFault::Tiered);
        }
        let total = held_bytes(placed);
        let mut prepare = Owned::<4096>::zeroed(total);
        let bytes = prepare.as_mut_slice();
        let mut header = placed.header;
        header.batch_checksum = header.checksum_for_blob(&blob);
        header
            .encode_into(&mut bytes[PREPARE_HEADER_SIZE..PREPARE_HEADER_SIZE + BATCH_HEADER_SIZE]);
        bytes[PREPARE_HEADER_SIZE + BATCH_HEADER_SIZE..].copy_from_slice(&blob);
        let prepare_header =
            bytemuck::checked::from_bytes_mut::<PrepareHeader>(&mut bytes[..PREPARE_HEADER_SIZE]);
        prepare_header.command = Command::Prepare;
        prepare_header.operation = Operation::SendMessages;
        prepare_header.size = u32::try_from(total).map_err(|_| DiskFault::Unreadable)?;
        Ok(prepare.into())
    }
}

/// What a poll still needs: where its first message is, and how many
/// messages and bytes it may take.
struct Wanted {
    query: MessageLookup,
    /// The offset after the last batch taken, so a batch that both a segment
    /// and the journal hold is taken once.
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

    /// Whether the first message of the poll can be in `segment` or before
    /// it, so the walk has to look into it.
    const fn may_start_in(&self, segment: &PlacedSegment) -> bool {
        match self.query {
            MessageLookup::Offset {
                offset, ceiling, ..
            } => segment.end_offset >= offset && segment.start_offset <= ceiling,
            MessageLookup::Timestamp {
                timestamp, ceiling, ..
            } => segment.max_timestamp >= timestamp && segment.start_offset <= ceiling,
        }
    }

    /// Whether `batch` holds a message at or after the poll's start.
    const fn reaches(&self, batch: &PlacedBatch) -> bool {
        let header = &batch.header;
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
        let header = batch.header;
        if self.is_full()
            || header.base_offset < self.next_offset
            || header.base_offset > self.query.ceiling()
            || !self.reaches(&batch)
        {
            return;
        }
        let held = held_bytes(&batch);
        if self.over_limit(held) {
            self.limited = true;
            return;
        }
        self.next_offset = header.base_offset + u64::from(header.message_count);
        self.messages += self.counted_messages(&batch);
        self.selected_bytes = self
            .selected_bytes
            .saturating_add(usize::try_from(header.batch_length).unwrap_or(usize::MAX));
        self.held_bytes = self.held_bytes.saturating_add(held);
        placed.push(batch);
    }

    /// Messages of `batch` that an offset poll counts: those from its start
    /// on. A timestamp poll cannot tell before it reads them, so it counts
    /// none of a batch stamped before its start.
    fn counted_messages(&self, batch: &PlacedBatch) -> u64 {
        let header = &batch.header;
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
