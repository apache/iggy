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
use iggy_common::{IggyByteSize, IggyMessage, IggyMessageView, PolledMessages, Sizeable};
use std::ops::Index;
use tracing::trace;

use super::IggyMessagesBatchMut;

/// A container for multiple IggyMessagesBatch objects
#[derive(Debug, Default)]
pub struct IggyMessagesBatchSet {
    /// The collection of message containers
    batches: Vec<IggyMessagesBatchMut>,
    /// Total number of messages across all containers
    count: u32,
    /// Total size in bytes across all containers
    size: u32,
}

impl IggyMessagesBatchSet {
    /// Create a new empty batch
    pub fn empty() -> Self {
        Self {
            batches: Vec::new(),
            count: 0,
            size: 0,
        }
    }

    /// Create a new empty batch set with a specified initial capacity of message containers
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            batches: Vec::with_capacity(capacity),
            count: 0,
            size: 0,
        }
    }

    /// Create a batch set from an existing vector of IggyMessages
    pub fn from_vec(messages: Vec<IggyMessagesBatchMut>) -> Self {
        let mut batch = Self::with_capacity(messages.len());
        for msg in messages {
            batch.add_batch(msg);
        }
        batch
    }

    /// Add another batch of messages to the batch set
    pub fn add_batch(&mut self, batch: IggyMessagesBatchMut) {
        self.count += batch.count();
        self.size += batch.size();
        self.batches.push(batch);
    }

    /// Add another batch set of messages to the batch set
    pub fn add_batch_set(&mut self, mut other_batch_set: IggyMessagesBatchSet) {
        self.count += other_batch_set.count();
        self.size += other_batch_set.size();
        let other_batches = std::mem::take(&mut other_batch_set.batches);
        self.batches.extend(other_batches);
    }

    /// Extract indexes from all batches in the set
    pub fn append_indexes_to(&self, target: &mut IggyIndexesMut) {
        for batch in self.iter() {
            let indexes = batch.indexes();
            target.append_slice(indexes);
        }
    }

    /// Get the total number of messages in the batch
    pub fn count(&self) -> u32 {
        self.count
    }

    /// Get the total size of all messages in bytes
    pub fn size(&self) -> u32 {
        self.size
    }

    /// Get the number of message containers in the batch
    pub fn containers_count(&self) -> usize {
        self.batches.len()
    }

    /// Check if the batch is empty
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty() || self.count == 0
    }

    /// Get timestamp of first message in first batch
    pub fn first_timestamp(&self) -> Option<u64> {
        if self.is_empty() {
            return None;
        }
        self.batches.first().map(|batch| batch.first_timestamp())?
    }

    /// Get offset of first message in first batch
    pub fn first_offset(&self) -> Option<u64> {
        if self.is_empty() {
            return None;
        }
        self.batches.first().map(|batch| batch.first_offset())?
    }

    /// Get timestamp of last message in last batch
    pub fn last_timestamp(&self) -> Option<u64> {
        if self.is_empty() {
            return None;
        }
        self.batches.last().map(|batch| batch.last_timestamp())?
    }

    /// Get offset of last message in last batch
    pub fn last_offset(&self) -> Option<u64> {
        self.batches.last().map(|batch| batch.last_offset())?
    }

    /// Get a reference to the underlying vector of message containers
    pub fn inner(&self) -> &Vec<IggyMessagesBatchMut> {
        &self.batches
    }

    /// Consume the batch, returning the underlying vector of message containers
    pub fn into_inner(mut self) -> Vec<IggyMessagesBatchMut> {
        std::mem::take(&mut self.batches)
    }

    /// Iterate over all message containers in the batch
    pub fn iter(&self) -> impl Iterator<Item = &IggyMessagesBatchMut> {
        self.batches.iter()
    }

    /// Iterate over all mutable message containers in the batch
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut IggyMessagesBatchMut> {
        self.batches.iter_mut()
    }

    /// Convert this batch and poll metadata into a vector of fully-formed IggyMessage objects
    ///
    /// This method transforms the internal message views into complete IggyMessage objects
    /// that can be returned to clients. It should only be used by server http implementation.
    ///
    /// # Arguments
    ///
    /// * `poll_metadata` - Metadata about the partition and current offset
    ///
    /// # Returns
    ///
    /// A vector of IggyMessage objects with proper metadata
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

    /// Returns a new IggyMessagesBatch containing only messages with offsets greater than or equal to the specified offset,
    /// up to the specified count.
    ///
    /// If no messages match the criteria, returns an empty batch.
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

    /// Returns a new IggyMessagesBatch containing only messages with timestamps greater than or equal
    /// to the specified timestamp, up to the specified count.
    ///
    /// If no messages match the criteria, returns an empty batch.
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

    /// Get the message at the specified index.
    /// Returns None if the index is out of bounds.
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

impl Index<usize> for IggyMessagesBatchSet {
    type Output = [u8];

    /// Get the message bytes at the specified index across all batches
    ///
    /// # Panics
    ///
    /// Panics if the index is out of bounds (>= total number of messages)
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

impl Sizeable for IggyMessagesBatchSet {
    fn get_size_bytes(&self) -> IggyByteSize {
        IggyByteSize::from(self.size as u64)
    }
}

impl From<Vec<IggyMessagesBatchMut>> for IggyMessagesBatchSet {
    fn from(messages: Vec<IggyMessagesBatchMut>) -> Self {
        Self::from_vec(messages)
    }
}

impl From<IggyMessagesBatchMut> for IggyMessagesBatchSet {
    fn from(messages: IggyMessagesBatchMut) -> Self {
        Self::from_vec(vec![messages])
    }
}
