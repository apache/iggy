use super::IggyMessages;
use iggy::prelude::*;

/// A batch container for multiple IggyMessages objects
#[derive(Debug, Clone, Default)]
pub struct IggyBatch {
    /// The collection of message containers
    messages: Vec<IggyMessages>,
    /// Total number of messages across all containers
    count: u32,
    /// Total size in bytes across all containers
    size: u32,
}

impl IggyBatch {
    /// Create a new empty batch
    pub fn empty() -> Self {
        Self {
            messages: Vec::new(),
            count: 0,
            size: 0,
        }
    }

    /// Create a new empty batch with a specified initial capacity of message containers
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            messages: Vec::with_capacity(capacity),
            count: 0,
            size: 0,
        }
    }

    /// Create a batch from an existing vector of IggyMessages
    pub fn from_vec(messages: Vec<IggyMessages>) -> Self {
        let mut batch = Self::with_capacity(messages.len());
        for msg in messages {
            batch.add(msg);
        }
        batch
    }

    /// Add a message container to the batch
    pub fn add(&mut self, messages: IggyMessages) {
        self.count += messages.count();
        self.size += messages.size();
        self.messages.push(messages);
    }

    /// Add another batch of messages to the batch
    pub fn add_batch(&mut self, other: IggyBatch) {
        self.count += other.count();
        self.size += other.size();
        self.messages.extend(other.messages);
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
        self.messages.len()
    }

    /// Check if the batch is empty
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty() || self.count == 0
    }

    /// Get a reference to the underlying vector of message containers
    pub fn inner(&self) -> &Vec<IggyMessages> {
        &self.messages
    }

    /// Consume the batch, returning the underlying vector of message containers
    pub fn into_inner(self) -> Vec<IggyMessages> {
        self.messages
    }

    /// Iterate over all message containers in the batch
    pub fn iter(&self) -> impl Iterator<Item = &IggyMessages> {
        self.messages.iter()
    }

    /// Returns a new IggyBatch containing only messages with offsets greater than or equal to the specified offset,
    /// up to the specified count.
    ///
    /// If no messages match the criteria, returns an empty batch.
    pub fn get_by_offset(&self, start_offset: u64, count: u32) -> Self {
        if self.is_empty() || count == 0 {
            return Self::empty();
        }

        let mut result = Self::with_capacity(self.containers_count());
        let mut remaining_count = count;

        // Process each container, extracting messages that match the offset criteria
        for container in self.iter() {
            // If we've collected enough messages, stop processing containers
            if remaining_count == 0 {
                break;
            }

            // Skip this container if all its messages have offsets less than what we're looking for
            // We can do this check because we know the last message's offset in this container
            // must be greater than or equal to the first message's offset
            let first_offset = container.first_index();
            if first_offset + container.count() as u64 <= start_offset {
                continue;
            }

            // Try to extract messages from this container
            if let Some(sliced) = container.slice_by_offset(start_offset, remaining_count) {
                // If we got some messages, add them to result and update remaining count
                if sliced.count() > 0 {
                    remaining_count -= sliced.count();
                    result.add(sliced);
                }
            }
        }

        result
    }

    /// Returns a new IggyBatch containing only messages with timestamps greater than or equal
    /// to the specified timestamp, up to the specified count.
    ///
    /// If no messages match the criteria, returns an empty batch.
    pub fn get_by_timestamp(&self, timestamp: u64, count: u32) -> Self {
        if self.is_empty() || count == 0 {
            return Self::empty();
        }

        let mut result = Self::with_capacity(self.containers_count());
        let mut remaining_count = count;

        // Process each container, extracting messages that match the timestamp criteria
        for container in self.iter() {
            // If we've collected enough messages, stop processing containers
            if remaining_count == 0 {
                break;
            }

            // Since multiple messages can have the same timestamp,
            // we can't safely skip containers based on first_timestamp alone.
            // Instead, we'll let slice_by_timestamp handle the filtering.
            if let Some(sliced) = container.slice_by_timestamp(timestamp, remaining_count) {
                // If we got some messages, add them to result and update remaining count
                if sliced.count() > 0 {
                    remaining_count -= sliced.count();
                    result.add(sliced);
                }
            }
        }

        result
    }

    /// Convert IggyMessagesBatch to flat Vec<IggyMessage>
    /// This should be used only for testing purposes.
    pub fn into_messages_vec(self) -> Vec<IggyMessage> {
        let mut messages = Vec::with_capacity(self.count() as usize);

        for batch in self.iter() {
            for view in batch.iter() {
                let header = view.msg_header().to_header();
                let payload = bytes::Bytes::copy_from_slice(view.payload());

                let headers = if header.headers_length > 0 {
                    let headers_bytes = bytes::Bytes::copy_from_slice(
                        &view.headers()[..header.headers_length as usize],
                    );

                    match std::collections::HashMap::<HeaderKey, HeaderValue>::from_bytes(
                        headers_bytes,
                    ) {
                        Ok(h) => Some(h),
                        Err(e) => {
                            tracing::error!(
                                "Error parsing headers: {}, header_length={}",
                                e,
                                header.headers_length
                            );
                            None
                        }
                    }
                } else {
                    None
                };

                messages.push(IggyMessage {
                    header,
                    payload,
                    headers,
                });
            }
        }
        messages
    }
}

impl Sizeable for IggyBatch {
    fn get_size_bytes(&self) -> IggyByteSize {
        IggyByteSize::from(self.size as u64)
    }
}

impl From<Vec<IggyMessages>> for IggyBatch {
    fn from(messages: Vec<IggyMessages>) -> Self {
        Self::from_vec(messages)
    }
}

impl From<IggyMessages> for IggyBatch {
    fn from(messages: IggyMessages) -> Self {
        Self::from_vec(vec![messages])
    }
}
