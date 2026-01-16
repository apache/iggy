/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::binary::handlers::messages::poll_messages_handler::IggyPollMetadata;
use crate::streaming::segments::IggyIndexesMut;
use bytes::Bytes;
use iggy_common::{
    IggyByteSize, IggyMessage, IggyMessageView, IggyMessagesBatch, PolledMessages, PooledBuffer,
    Sizeable,
};
use std::ops::Index;
use tracing::trace;

use super::IggyMessagesBatchMut;

// ============================================================================
// IggyMessagesBatchSet - Immutable/frozen batches (Arc-backed Bytes, zero-copy)
// ============================================================================

/// A container for immutable message batches (frozen, Arc-backed).
///
/// Used for reads from in-flight buffer during async disk I/O.
/// All slicing operations use `Bytes::slice()` for zero-copy.
#[derive(Debug, Default, Clone)]
pub struct IggyMessagesBatchSet {
    batches: Vec<IggyMessagesBatch>,
    count: u32,
    size: u32,
}

impl IggyMessagesBatchSet {
    pub fn empty() -> Self {
        Self {
            batches: Vec::new(),
            count: 0,
            size: 0,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            batches: Vec::with_capacity(capacity),
            count: 0,
            size: 0,
        }
    }

    pub fn from_vec(batches: Vec<IggyMessagesBatch>) -> Self {
        let count = batches.iter().map(|b| b.count()).sum();
        let size = batches.iter().map(|b| b.size()).sum();
        Self {
            batches,
            count,
            size,
        }
    }

    pub fn add_batch(&mut self, batch: IggyMessagesBatch) {
        self.count += batch.count();
        self.size += batch.size();
        self.batches.push(batch);
    }

    pub fn count(&self) -> u32 {
        self.count
    }

    pub fn size(&self) -> u32 {
        self.size
    }

    pub fn containers_count(&self) -> usize {
        self.batches.len()
    }

    pub fn is_empty(&self) -> bool {
        self.batches.is_empty() || self.count == 0
    }

    pub fn first_offset(&self) -> Option<u64> {
        self.batches.first().and_then(|b| b.first_offset())
    }

    pub fn last_offset(&self) -> Option<u64> {
        self.batches.last().and_then(|b| b.last_offset())
    }

    pub fn first_timestamp(&self) -> Option<u64> {
        self.batches.first().and_then(|b| b.first_timestamp())
    }

    pub fn last_timestamp(&self) -> Option<u64> {
        self.batches.last().and_then(|b| b.last_timestamp())
    }

    pub fn iter(&self) -> impl Iterator<Item = &IggyMessagesBatch> {
        self.batches.iter()
    }

    pub fn into_inner(self) -> Vec<IggyMessagesBatch> {
        self.batches
    }

    /// Zero-copy filtering by offset using `Bytes::slice()`.
    pub fn get_by_offset(&self, offset: u64, count: u32) -> Self {
        if self.is_empty() || count == 0 {
            return Self::empty();
        }

        let end_offset = offset + count as u64 - 1;
        let mut result = Self::with_capacity(self.batches.len());
        let mut remaining = count;

        for batch in &self.batches {
            if remaining == 0 {
                break;
            }

            let batch_first = match batch.first_offset() {
                Some(o) => o,
                None => continue,
            };
            let batch_last = match batch.last_offset() {
                Some(o) => o,
                None => continue,
            };

            // Skip batches entirely before requested range
            if batch_last < offset {
                continue;
            }

            // Stop if batch is entirely after requested range
            if batch_first > end_offset {
                break;
            }

            // Batch overlaps - slice it (zero-copy via Bytes::slice)
            if let Some(sliced) = batch.slice_by_offset(offset, remaining) {
                remaining = remaining.saturating_sub(sliced.count());
                result.add_batch(sliced);
            }
        }

        result
    }

    /// Convert to mutable batch set (copies data).
    pub fn into_mutable(self) -> IggyMessagesBatchSetMut {
        let mut result = IggyMessagesBatchSetMut::with_capacity(self.batches.len());
        for batch in self.batches {
            let mutable = frozen_to_mutable(&batch);
            result.add_batch(mutable);
        }
        result
    }

    pub fn into_polled_messages(&self, poll_metadata: IggyPollMetadata) -> PolledMessages {
        if self.is_empty() {
            return PolledMessages::empty();
        }

        let mut messages = Vec::with_capacity(self.count as usize);

        for batch in &self.batches {
            for message in batch.iter() {
                let header = message.header().to_header();
                let payload = Bytes::copy_from_slice(message.payload());
                let user_headers = message.user_headers().map(Bytes::copy_from_slice);
                let msg = IggyMessage {
                    header,
                    payload,
                    user_headers,
                };
                messages.push(msg);
            }
        }

        trace!(
            "Converted frozen batch of {} messages from partition {} with current offset {}",
            messages.len(),
            poll_metadata.partition_id,
            poll_metadata.current_offset
        );

        PolledMessages {
            partition_id: poll_metadata.partition_id,
            current_offset: poll_metadata.current_offset,
            count: messages.len() as u32,
            messages,
        }
    }
}

impl From<Vec<IggyMessagesBatch>> for IggyMessagesBatchSet {
    fn from(batches: Vec<IggyMessagesBatch>) -> Self {
        Self::from_vec(batches)
    }
}

impl Sizeable for IggyMessagesBatchSet {
    fn get_size_bytes(&self) -> IggyByteSize {
        IggyByteSize::from(self.size as u64)
    }
}

// ============================================================================
// IggyMessagesBatchSetMut - Mutable batches (PooledBuffer)
// ============================================================================

/// A container for mutable message batches (using PooledBuffer).
///
/// Used for reads from journal and disk.
#[derive(Debug, Default)]
pub struct IggyMessagesBatchSetMut {
    batches: Vec<IggyMessagesBatchMut>,
    count: u32,
    size: u32,
}

impl IggyMessagesBatchSetMut {
    pub fn empty() -> Self {
        Self {
            batches: Vec::new(),
            count: 0,
            size: 0,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            batches: Vec::with_capacity(capacity),
            count: 0,
            size: 0,
        }
    }

    pub fn from_vec(messages: Vec<IggyMessagesBatchMut>) -> Self {
        let mut batch = Self::with_capacity(messages.len());
        for msg in messages {
            batch.add_batch(msg);
        }
        batch
    }

    pub fn add_batch(&mut self, batch: IggyMessagesBatchMut) {
        self.count += batch.count();
        self.size += batch.size();
        self.batches.push(batch);
    }

    pub fn add_batch_set(&mut self, mut other: IggyMessagesBatchSetMut) {
        self.count += other.count();
        self.size += other.size();
        let other_batches = std::mem::take(&mut other.batches);
        self.batches.extend(other_batches);
    }

    pub fn append_indexes_to(&self, target: &mut IggyIndexesMut) {
        for batch in self.iter() {
            let indexes = batch.indexes();
            target.append_slice(indexes);
        }
    }

    pub fn count(&self) -> u32 {
        self.count
    }

    pub fn size(&self) -> u32 {
        self.size
    }

    pub fn containers_count(&self) -> usize {
        self.batches.len()
    }

    pub fn is_empty(&self) -> bool {
        self.batches.is_empty() || self.count == 0
    }

    pub fn first_timestamp(&self) -> Option<u64> {
        if self.is_empty() {
            return None;
        }
        self.batches
            .first()
            .and_then(|batch| batch.first_timestamp())
    }

    pub fn first_offset(&self) -> Option<u64> {
        if self.is_empty() {
            return None;
        }
        self.batches.first().and_then(|batch| batch.first_offset())
    }

    pub fn last_timestamp(&self) -> Option<u64> {
        if self.is_empty() {
            return None;
        }
        self.batches.last().and_then(|batch| batch.last_timestamp())
    }

    pub fn last_offset(&self) -> Option<u64> {
        self.batches.last().and_then(|batch| batch.last_offset())
    }

    pub fn inner(&self) -> &Vec<IggyMessagesBatchMut> {
        &self.batches
    }

    pub fn into_inner(mut self) -> Vec<IggyMessagesBatchMut> {
        std::mem::take(&mut self.batches)
    }

    pub fn iter(&self) -> impl Iterator<Item = &IggyMessagesBatchMut> {
        self.batches.iter()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut IggyMessagesBatchMut> {
        self.batches.iter_mut()
    }

    pub fn into_polled_messages(&self, poll_metadata: IggyPollMetadata) -> PolledMessages {
        if self.is_empty() {
            return PolledMessages::empty();
        }

        let mut messages = Vec::with_capacity(self.count() as usize);

        for batch in self.iter() {
            for message in batch.iter() {
                let header = message.header().to_header();
                let payload = Bytes::copy_from_slice(message.payload());
                let user_headers = message.user_headers().map(Bytes::copy_from_slice);
                let message = IggyMessage {
                    header,
                    payload,
                    user_headers,
                };
                messages.push(message);
            }
        }

        trace!(
            "Converted batch of {} messages from partition {} with current offset {}",
            messages.len(),
            poll_metadata.partition_id,
            poll_metadata.current_offset
        );

        PolledMessages {
            partition_id: poll_metadata.partition_id,
            current_offset: poll_metadata.current_offset,
            count: messages.len() as u32,
            messages,
        }
    }

    pub fn get_by_offset(&self, start_offset: u64, count: u32) -> Self {
        if self.is_empty() || count == 0 {
            return Self::empty();
        }

        let mut result = Self::with_capacity(self.containers_count());
        let mut remaining_count = count;

        for container in self.iter() {
            if remaining_count == 0 {
                break;
            }

            let first_offset = container.first_offset();
            if first_offset.is_none()
                || first_offset.unwrap() + container.count() as u64 <= start_offset
            {
                continue;
            }

            if let Some(sliced) = container.slice_by_offset(start_offset, remaining_count)
                && sliced.count() > 0
            {
                remaining_count -= sliced.count();
                result.add_batch(sliced);
            }
        }

        result
    }

    pub fn get_by_timestamp(&self, timestamp: u64, count: u32) -> Self {
        if self.is_empty() || count == 0 {
            return Self::empty();
        }

        let mut result = Self::with_capacity(self.containers_count());
        let mut remaining_count = count;

        for container in self.iter() {
            if remaining_count == 0 {
                break;
            }

            let first_timestamp = container.first_timestamp();
            if first_timestamp.is_none() || first_timestamp.unwrap() < timestamp {
                continue;
            }

            if let Some(sliced) = container.slice_by_timestamp(timestamp, remaining_count)
                && sliced.count() > 0
            {
                remaining_count -= sliced.count();
                result.add_batch(sliced);
            }
        }

        result
    }

    pub fn get(&self, index: usize) -> Option<IggyMessageView<'_>> {
        if index >= self.count as usize {
            return None;
        }

        let mut seen_messages = 0;

        for batch in &self.batches {
            let batch_count = batch.count() as usize;

            if index < seen_messages + batch_count {
                let local_index = index - seen_messages;
                return batch.get(local_index);
            }

            seen_messages += batch_count;
        }

        None
    }
}

impl Index<usize> for IggyMessagesBatchSetMut {
    type Output = [u8];

    fn index(&self, index: usize) -> &Self::Output {
        if index >= self.count as usize {
            panic!(
                "Index out of bounds: the len is {} but the index is {}",
                self.count, index
            );
        }

        let mut seen_messages = 0;

        for batch in &self.batches {
            let batch_count = batch.count() as usize;

            if index < seen_messages + batch_count {
                let local_index = index - seen_messages;
                return &batch[local_index];
            }

            seen_messages += batch_count;
        }

        unreachable!("Failed to find message at index {}", index);
    }
}

impl Sizeable for IggyMessagesBatchSetMut {
    fn get_size_bytes(&self) -> IggyByteSize {
        IggyByteSize::from(self.size as u64)
    }
}

impl From<Vec<IggyMessagesBatchMut>> for IggyMessagesBatchSetMut {
    fn from(messages: Vec<IggyMessagesBatchMut>) -> Self {
        Self::from_vec(messages)
    }
}

impl From<IggyMessagesBatchMut> for IggyMessagesBatchSetMut {
    fn from(messages: IggyMessagesBatchMut) -> Self {
        Self::from_vec(vec![messages])
    }
}

// ============================================================================
// PolledBatches - Result of polling (either mutable or frozen)
// ============================================================================

/// Result of polling messages - either mutable or frozen batches.
///
/// Using an enum avoids copying when reading from the in-flight buffer,
/// as the frozen `Bytes` are Arc-backed and use zero-copy slicing.
#[derive(Debug)]
pub enum PolledBatches {
    /// Mutable batches from journal or disk
    Mutable(IggyMessagesBatchSetMut),
    /// Frozen batches from in-flight buffer (Arc-backed, zero-copy)
    Frozen(IggyMessagesBatchSet),
}

impl PolledBatches {
    pub fn empty() -> Self {
        Self::Mutable(IggyMessagesBatchSetMut::empty())
    }

    pub fn count(&self) -> u32 {
        match self {
            Self::Mutable(set) => set.count(),
            Self::Frozen(set) => set.count(),
        }
    }

    pub fn size(&self) -> u32 {
        match self {
            Self::Mutable(set) => set.size(),
            Self::Frozen(set) => set.size(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::Mutable(set) => set.is_empty(),
            Self::Frozen(set) => set.is_empty(),
        }
    }

    pub fn containers_count(&self) -> usize {
        match self {
            Self::Mutable(set) => set.containers_count(),
            Self::Frozen(set) => set.containers_count(),
        }
    }

    pub fn last_offset(&self) -> Option<u64> {
        match self {
            Self::Mutable(set) => set.last_offset(),
            Self::Frozen(set) => set.last_offset(),
        }
    }

    pub fn first_offset(&self) -> Option<u64> {
        match self {
            Self::Mutable(set) => set.first_offset(),
            Self::Frozen(set) => set.first_offset(),
        }
    }

    /// Filter batches by offset. Zero-copy for frozen batches.
    pub fn filter_by_offset(self, offset: u64, count: u32) -> Self {
        match self {
            Self::Mutable(set) => Self::Mutable(set.get_by_offset(offset, count)),
            Self::Frozen(set) => Self::Frozen(set.get_by_offset(offset, count)),
        }
    }

    /// Convert to mutable batch set (copies frozen data).
    pub fn into_mutable(self) -> IggyMessagesBatchSetMut {
        match self {
            Self::Mutable(set) => set,
            Self::Frozen(set) => set.into_mutable(),
        }
    }

    pub fn into_polled_messages(&self, poll_metadata: IggyPollMetadata) -> PolledMessages {
        match self {
            Self::Mutable(set) => set.into_polled_messages(poll_metadata),
            Self::Frozen(set) => set.into_polled_messages(poll_metadata),
        }
    }
}

impl Default for PolledBatches {
    fn default() -> Self {
        Self::empty()
    }
}

// ============================================================================
// Helper functions
// ============================================================================

fn frozen_to_mutable(batch: &IggyMessagesBatch) -> IggyMessagesBatchMut {
    let count = batch.count();
    let base_position = batch.indexes().base_position();
    let indexes_buffer = PooledBuffer::from(batch.indexes_slice());
    let indexes = IggyIndexesMut::from_bytes(indexes_buffer, base_position);
    let messages = PooledBuffer::from(batch.buffer());
    IggyMessagesBatchMut::from_indexes_and_messages(count, indexes, messages)
}
