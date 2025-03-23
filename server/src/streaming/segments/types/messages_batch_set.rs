use crate::binary::handlers::messages::poll_messages_handler::IggyPollMetadata;
use bytes::Bytes;
use iggy::models::messaging::IggyMessageView;
use iggy::models::messaging::IggyMessagesBatch;
use iggy::prelude::*;
use std::ops::Index;
use tracing::trace;

/// A container for multiple IggyMessagesBatch objects
#[derive(Debug, Clone, Default)]
pub struct IggyMessagesBatchSet {
    /// The collection of message containers
    batches: Vec<IggyMessagesBatch>,
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

    /// Create a new empty batch with a specified initial capacity of message containers
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            batches: Vec::with_capacity(capacity),
            count: 0,
            size: 0,
        }
    }

    /// Create a batch from an existing vector of IggyMessages
    pub fn from_vec(messages: Vec<IggyMessagesBatch>) -> Self {
        let mut batch = Self::with_capacity(messages.len());
        for msg in messages {
            batch.add_batch(msg);
        }
        batch
    }

    /// Add a message container to the batch
    pub fn add_batch(&mut self, messages: IggyMessagesBatch) {
        self.count += messages.count();
        self.size += messages.size();
        self.batches.push(messages);
    }

    /// Add another batch of messages to the batch
    pub fn add_batch_set(&mut self, other: IggyMessagesBatchSet) {
        self.count += other.count();
        self.size += other.size();
        self.batches.extend(other.batches);
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

    /// Get first timestamp of first batch
    pub fn first_timestamp(&self) -> Option<u64> {
        self.batches.first().map(|batch| batch.first_timestamp())
    }

    /// Get first offset of first batch
    pub fn first_offset(&self) -> Option<u64> {
        self.batches.first().map(|batch| batch.first_offset())
    }

    /// Get offset of last message of last batch
    pub fn last_offset(&self) -> Option<u64> {
        self.batches.last().map(|batch| batch.last_offset())
    }

    /// Get timestamp of last message of last batch
    pub fn last_timestamp(&self) -> Option<u64> {
        self.batches.last().map(|batch| batch.last_timestamp())
    }

    /// Get a reference to the underlying vector of message containers
    pub fn inner(&self) -> &Vec<IggyMessagesBatch> {
        &self.batches
    }

    /// Consume the batch, returning the underlying vector of message containers
    pub fn into_inner(self) -> Vec<IggyMessagesBatch> {
        self.batches
    }

    /// Iterate over all message containers in the batch
    pub fn iter(&self) -> impl Iterator<Item = &IggyMessagesBatch> {
        self.batches.iter()
    }

    /// Convert this batch and poll metadata into a vector of fully-formed IggyMessage objects
    ///
    /// This method transforms the internal message views into complete IggyMessage objects
    /// that can be returned to clients.
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
            return PolledMessages {
                messages: vec![],
                partition_id: 0,
                current_offset: 0,
                count: 0,
            };
        }

        let mut messages = Vec::with_capacity(self.count() as usize);

        // TODO(hubcio): this can be also optimized for zero copy, but it's http...
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
        tracing::trace!(
            "Getting {} messages from batch set, start offset {}, end offset calculated {}, end offset real {}, messages count {}...",
            count,
            start_offset,
            start_offset + count as u64 - 1,
            self.last_offset().unwrap_or(0),
            self.count()
        );

        let mut result = Self::with_capacity(self.containers_count());
        let mut remaining_count = count;

        for container in self.iter() {
            if remaining_count == 0 {
                break;
            }

            let first_offset = container.first_offset();
            if first_offset + container.count() as u64 <= start_offset {
                continue;
            }

            if let Some(sliced) = container.slice_by_offset(start_offset, remaining_count) {
                if sliced.count() > 0 {
                    remaining_count -= sliced.count();
                    result.add_batch(sliced);
                }
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
            if first_timestamp < timestamp {
                continue;
            }

            if let Some(sliced) = container.slice_by_timestamp(timestamp, remaining_count) {
                if sliced.count() > 0 {
                    remaining_count -= sliced.count();
                    result.add_batch(sliced);
                }
            }
        }

        result
    }

    /// Get the message at the specified index.
    /// Returns None if the index is out of bounds.
    pub fn get(&self, index: usize) -> Option<IggyMessageView> {
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

impl From<Vec<IggyMessagesBatch>> for IggyMessagesBatchSet {
    fn from(messages: Vec<IggyMessagesBatch>) -> Self {
        Self::from_vec(messages)
    }
}

impl From<IggyMessagesBatch> for IggyMessagesBatchSet {
    fn from(messages: IggyMessagesBatch) -> Self {
        Self::from_vec(vec![messages])
    }
}

/// Iterator that consumes an IggyMessagesBatchSet and yields batches
pub struct IggyMessagesBatchSetIntoIter {
    batches: Vec<IggyMessagesBatch>,
    position: usize,
}

impl Iterator for IggyMessagesBatchSetIntoIter {
    type Item = IggyMessagesBatch;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position < self.batches.len() {
            let batch =
                std::mem::replace(&mut self.batches[self.position], IggyMessagesBatch::empty());
            self.position += 1;
            Some(batch)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.batches.len() - self.position;
        (remaining, Some(remaining))
    }
}

impl IntoIterator for IggyMessagesBatchSet {
    type Item = IggyMessagesBatch;
    type IntoIter = IggyMessagesBatchSetIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        IggyMessagesBatchSetIntoIter {
            batches: self.batches,
            position: 0,
        }
    }
}
