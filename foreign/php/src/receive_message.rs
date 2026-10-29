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

use ext_php_rs::{binary::Binary, exception::PhpResult, php_class, php_impl};
use iggy::prelude::{
    Checkpoint as RustCheckpoint, IggyMessage as RustReceiveMessage, IggyMessageHeader,
    PolledMessages as RustPolledMessages, PollingStrategy as RustPollingStrategy, ReceivedMessage,
};

use crate::error::to_php_exception;

/// A PHP class representing a received message.
///
/// This class wraps a Rust message, allowing PHP code to access its payload and metadata.
#[php_class]
#[php(name = "Iggy\\ReceiveMessage")]
pub struct ReceiveMessage {
    pub(crate) inner: RustReceiveMessage,
    pub(crate) checkpoint: RustCheckpoint,
}

impl From<ReceivedMessage> for ReceiveMessage {
    fn from(message: ReceivedMessage) -> Self {
        Self {
            checkpoint: message.checkpoint(),
            inner: message.message,
        }
    }
}

impl Clone for ReceiveMessage {
    fn clone(&self) -> Self {
        Self {
            inner: copy_message(&self.inner),
            checkpoint: self.checkpoint,
        }
    }
}

#[php_impl]
impl ReceiveMessage {
    /// Retrieves the payload of the received message.
    ///
    /// The payload is returned as a PHP string, which can represent both text and binary data.
    /// The bytes are copied into a PHP string on each getter call; cache the result in PHP if
    /// the payload will be read repeatedly.
    pub fn payload(&self) -> Binary<u8> {
        Binary::new(self.inner.payload.to_vec())
    }

    /// Retrieves the offset of the received message.
    ///
    /// The offset represents the position of the message within its topic.
    pub fn offset(&self) -> u64 {
        self.inner.header.offset
    }

    /// Retrieves the timestamp of the received message.
    ///
    /// The timestamp represents the time of the message within its topic.
    pub fn timestamp(&self) -> u64 {
        self.inner.header.timestamp
    }

    /// Retrieves the id of the received message.
    ///
    /// The id represents unique identifier of the message within its topic.
    pub fn id(&self) -> String {
        self.inner.header.id.to_string()
    }

    /// Retrieves the checksum of the received message.
    ///
    /// The checksum represents the integrity of the message within its topic.
    pub fn checksum(&self) -> String {
        self.inner.header.checksum.to_string()
    }

    /// Retrieves the length of the received message.
    ///
    /// The length represents the length of the payload.
    pub fn length(&self) -> u32 {
        self.inner.header.payload_length
    }

    /// Retrieves the partition this message belongs to.
    pub fn partition_id(&self) -> u32 {
        self.checkpoint.partition_id()
    }

    /// Retrieves the position after this message, which commits it and every earlier message
    /// of its partition.
    ///
    /// It keeps the partition incarnation and owner that delivered the message, so a commit
    /// after the partition was recreated or changed owner is refused instead of landing on
    /// the newer partition. Only the reader or the consumer that delivered the message accepts
    /// it in commit().
    pub fn checkpoint(&self) -> Checkpoint {
        self.checkpoint.into()
    }
}

/// A PHP class representing the position after one delivered message.
///
/// A commit stores its offset, so it covers every earlier message of the partition.
#[php_class]
#[php(name = "Iggy\\Checkpoint")]
#[derive(Clone, Copy)]
pub struct Checkpoint {
    pub(crate) inner: RustCheckpoint,
}

impl From<RustCheckpoint> for Checkpoint {
    fn from(inner: RustCheckpoint) -> Self {
        Self { inner }
    }
}

#[php_impl]
impl Checkpoint {
    #[php(getter, change_case = "snake_case")]
    pub fn partition_id(&self) -> u32 {
        self.inner.partition_id()
    }

    /// The offset of the delivered message, which a commit stores.
    #[php(getter)]
    pub fn offset(&self) -> u64 {
        self.inner.offset()
    }
}

/// A PHP class representing one batch of one partition.
#[php_class]
#[php(name = "Iggy\\PolledMessages")]
pub struct PolledMessages {
    inner: RustPolledMessages,
}

impl From<RustPolledMessages> for PolledMessages {
    fn from(inner: RustPolledMessages) -> Self {
        Self { inner }
    }
}

#[php_impl]
impl PolledMessages {
    #[php(getter, change_case = "snake_case")]
    pub fn partition_id(&self) -> u32 {
        self.inner.partition_id
    }

    /// The offset of the newest message in the partition when the batch was read.
    #[php(getter, change_case = "snake_case")]
    pub fn current_offset(&self) -> u64 {
        self.inner.current_offset
    }

    #[php(getter)]
    pub fn count(&self) -> u32 {
        self.inner.count
    }

    /// The messages of the batch, in offset order.
    ///
    /// Each read of this property copies the messages, so keep the result in a variable to
    /// read them more than once.
    #[php(getter)]
    pub fn messages(&self) -> Vec<ReceiveMessage> {
        let checkpoints = (0..).map_while(|index| self.inner.checkpoint_at(index));
        self.inner
            .messages
            .iter()
            .zip(checkpoints)
            .map(|(message, checkpoint)| ReceiveMessage {
                inner: copy_message(message),
                checkpoint,
            })
            .collect()
    }

    /// The position after the last message, to commit the whole batch. Null for an empty
    /// batch.
    pub fn checkpoint(&self) -> Option<Checkpoint> {
        self.inner.checkpoint().map(Checkpoint::from)
    }

    /// The position after the message at `index`, to commit a processed part of the batch.
    /// Null for an index out of range.
    pub fn checkpoint_at(&self, index: usize) -> Option<Checkpoint> {
        self.inner.checkpoint_at(index).map(Checkpoint::from)
    }

    /// The position after the batch, for the next read of its partition.
    ///
    /// After the last message it is the next offset. An empty batch keeps the start of its
    /// request, except that first() becomes offset 0, so a start can take more than one read.
    /// The continuation belongs to the partition of the batch, and a read of another partition
    /// refuses it with InvalidCommand (3). It keeps the partition history of the batch, so a
    /// read after the partition was recreated fails with HistoryUnavailable (87).
    pub fn continuation(&self) -> PollingStrategy {
        self.inner.continuation().into()
    }
}

#[php_class]
#[php(name = "Iggy\\PollingStrategy")]
#[derive(Clone)]
pub struct PollingStrategy {
    pub(crate) inner: RustPollingStrategy,
}

impl From<&PollingStrategy> for RustPollingStrategy {
    fn from(value: &PollingStrategy) -> Self {
        value.inner
    }
}

impl From<RustPollingStrategy> for PollingStrategy {
    fn from(inner: RustPollingStrategy) -> Self {
        Self { inner }
    }
}

#[php_impl]
impl PollingStrategy {
    pub fn offset(value: u64) -> Self {
        Self {
            inner: RustPollingStrategy::offset(value),
        }
    }

    /// Poll messages at or after a UNIX timestamp expressed in microseconds.
    pub fn timestamp_micros(value: u64) -> Self {
        Self {
            inner: RustPollingStrategy::timestamp(value.into()),
        }
    }

    /// Poll messages at or after a UNIX timestamp expressed in seconds.
    pub fn timestamp_seconds(value: u64) -> PhpResult<Self> {
        let Some(micros) = value.checked_mul(1_000_000) else {
            return Err(to_php_exception("timestamp seconds value is too large"));
        };

        Ok(Self::timestamp_micros(micros))
    }

    /// Poll messages at or after a UNIX timestamp expressed in microseconds.
    pub fn timestamp(value: u64) -> Self {
        Self::timestamp_micros(value)
    }

    pub fn first() -> Self {
        Self {
            inner: RustPollingStrategy::first(),
        }
    }

    pub fn last() -> Self {
        Self {
            inner: RustPollingStrategy::last(),
        }
    }

    pub fn next() -> Self {
        Self {
            inner: RustPollingStrategy::next(),
        }
    }
}

// The SDK message has no Clone; its payload and headers are reference counted.
fn copy_message(message: &RustReceiveMessage) -> RustReceiveMessage {
    RustReceiveMessage {
        header: IggyMessageHeader {
            checksum: message.header.checksum,
            id: message.header.id,
            offset: message.header.offset,
            timestamp: message.header.timestamp,
            origin_timestamp: message.header.origin_timestamp,
            user_headers_length: message.header.user_headers_length,
            payload_length: message.header.payload_length,
            reserved: message.header.reserved,
        },
        payload: message.payload.clone(),
        user_headers: message.user_headers.clone(),
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use iggy::prelude::{PartitionContext, PollingKind};

    use super::*;

    const PARTITION_ID: u32 = 2;
    const CONTEXT: PartitionContext = PartitionContext {
        incarnation: 7,
        owner_generation: 3,
        metadata_op: 11,
    };

    fn message(offset: u64) -> RustReceiveMessage {
        let mut message = RustReceiveMessage::default();
        message.header.offset = offset;
        message.payload = Bytes::from(offset.to_string());
        message
    }

    fn batch(offsets: &[u64]) -> PolledMessages {
        let messages: Vec<_> = offsets.iter().copied().map(message).collect();
        let count = u32::try_from(messages.len()).unwrap();
        RustPolledMessages::new(CONTEXT, PARTITION_ID, 50, count, messages).into()
    }

    #[test]
    fn given_consumed_message_when_converted_should_keep_its_checkpoint() {
        let received = ReceivedMessage::new(message(42), 50, PARTITION_ID, CONTEXT);
        let expected = received.checkpoint();

        let message = ReceiveMessage::from(received);

        assert_eq!(message.checkpoint().inner, expected);
        assert_eq!(message.partition_id(), PARTITION_ID);
        assert_eq!(message.offset(), 42);
    }

    #[test]
    fn given_batch_when_listing_messages_should_give_each_message_its_own_checkpoint() {
        let polled = batch(&[4, 5, 6]);

        let messages = polled.messages();

        let offsets: Vec<_> = messages.iter().map(ReceiveMessage::offset).collect();
        assert_eq!(offsets, [4, 5, 6]);
        for (index, message) in messages.iter().enumerate() {
            assert_eq!(
                Some(message.checkpoint().inner),
                polled.inner.checkpoint_at(index)
            );
        }
        assert_eq!(messages[1].payload().as_slice(), b"5");
        assert_eq!(
            polled.checkpoint().map(|checkpoint| checkpoint.inner),
            polled.inner.checkpoint()
        );
        assert!(polled.checkpoint_at(3).is_none());
    }

    #[test]
    fn given_batch_when_continuing_should_read_after_its_last_message_of_the_same_partition() {
        let continuation = batch(&[4, 5]).continuation().inner;

        assert_eq!(continuation.kind, PollingKind::Offset);
        assert_eq!(continuation.value, 6);
        assert_eq!(continuation.partition_id(), Some(PARTITION_ID));
        assert!(continuation.validate_partition(PARTITION_ID + 1).is_err());
    }

    #[test]
    fn given_empty_batch_when_inspected_should_have_no_checkpoint() {
        let polled = batch(&[]);

        assert!(polled.messages().is_empty());
        assert!(polled.checkpoint().is_none());
        assert!(polled.checkpoint_at(0).is_none());
    }
}
