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
    ConsumerPosition as RustConsumerPosition, IggyMessage as RustReceiveMessage, IggyMessageHeader,
    PartitionContext as RustPartitionContext, PollingStrategy as RustPollingStrategy,
    ReceivedMessage,
};

use crate::error::to_php_exception;

/// A PHP class representing a received message.
///
/// This class wraps a Rust message, allowing PHP code to access its payload and metadata.
#[php_class]
#[php(name = "Iggy\\ReceiveMessage")]
pub struct ReceiveMessage {
    pub(crate) inner: RustReceiveMessage,
    pub(crate) partition_id: u32,
    pub(crate) context: RustPartitionContext,
}

impl From<ReceivedMessage> for ReceiveMessage {
    fn from(message: ReceivedMessage) -> Self {
        Self {
            inner: message.message,
            partition_id: message.partition_id,
            context: message.context,
        }
    }
}

impl Clone for ReceiveMessage {
    fn clone(&self) -> Self {
        Self {
            inner: RustReceiveMessage {
                header: IggyMessageHeader {
                    checksum: self.inner.header.checksum,
                    id: self.inner.header.id,
                    offset: self.inner.header.offset,
                    timestamp: self.inner.header.timestamp,
                    origin_timestamp: self.inner.header.origin_timestamp,
                    user_headers_length: self.inner.header.user_headers_length,
                    payload_length: self.inner.header.payload_length,
                    reserved: self.inner.header.reserved,
                },
                payload: self.inner.payload.clone(),
                user_headers: self.inner.user_headers.clone(),
            },
            partition_id: self.partition_id,
            context: self.context,
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
        self.partition_id
    }

    /// Retrieves the partition incarnation and owner captured by the poll that delivered
    /// this message.
    pub fn context(&self) -> PartitionContext {
        self.context.into()
    }

    /// Retrieves the position to commit for this message with Consumer::storePosition().
    ///
    /// It keeps the partition incarnation and owner that delivered the message, so a commit
    /// after the partition was recreated or changed owner is refused instead of landing on
    /// the newer partition.
    pub fn position(&self) -> ConsumerPosition {
        RustConsumerPosition {
            partition_id: self.partition_id,
            offset: self.inner.header.offset,
            context: self.context,
        }
        .into()
    }
}

/// A PHP class representing the partition incarnation and owner that a poll captured.
///
/// Keep it with an offset that is continued or committed later. The server then refuses the
/// request after the partition was deleted and recreated, or after its consumer group owner
/// changed.
#[php_class]
#[php(name = "Iggy\\PartitionContext")]
#[derive(Clone, Copy)]
pub struct PartitionContext {
    pub(crate) inner: RustPartitionContext,
}

impl From<RustPartitionContext> for PartitionContext {
    fn from(inner: RustPartitionContext) -> Self {
        Self { inner }
    }
}

#[php_impl]
impl PartitionContext {
    /// Rebuilds a context from values saved with an offset, for example by another process.
    #[php(constructor)]
    pub fn __construct(incarnation: u64, owner_generation: u64, metadata_op: u64) -> Self {
        Self {
            inner: RustPartitionContext {
                incarnation,
                owner_generation,
                metadata_op,
            },
        }
    }

    /// The partition's creation revision. It changes when the partition is deleted and
    /// recreated.
    #[php(getter)]
    pub fn incarnation(&self) -> u64 {
        self.inner.incarnation
    }

    /// The consumer group owner generation, or 0 for a poll without a consumer group.
    #[php(getter)]
    pub fn owner_generation(&self) -> u64 {
        self.inner.owner_generation
    }

    /// The metadata operation the server must have applied before it serves the request.
    #[php(getter)]
    pub fn metadata_op(&self) -> u64 {
        self.inner.metadata_op
    }
}

/// A PHP class representing a consumed position to commit with Consumer::storePosition().
#[php_class]
#[php(name = "Iggy\\ConsumerPosition")]
#[derive(Clone, Copy)]
pub struct ConsumerPosition {
    pub(crate) inner: RustConsumerPosition,
}

impl From<RustConsumerPosition> for ConsumerPosition {
    fn from(inner: RustConsumerPosition) -> Self {
        Self { inner }
    }
}

#[php_impl]
impl ConsumerPosition {
    /// Rebuilds a position from values saved with a processed message.
    #[php(constructor)]
    pub fn __construct(partition_id: u32, offset: u64, context: &PartitionContext) -> Self {
        Self {
            inner: RustConsumerPosition {
                partition_id,
                offset,
                context: context.inner,
            },
        }
    }

    #[php(getter)]
    pub fn partition_id(&self) -> u32 {
        self.inner.partition_id
    }

    /// The inclusive message offset to commit.
    #[php(getter)]
    pub fn offset(&self) -> u64 {
        self.inner.offset
    }

    /// The partition incarnation and owner captured by the poll that delivered the message.
    #[php(getter)]
    pub fn context(&self) -> PartitionContext {
        self.inner.context.into()
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

    /// Continues at an offset under the partition incarnation and owner that produced it.
    ///
    /// A stale context fails the poll instead of reading that offset from a recreated
    /// partition. Without a context, the poll uses the context of its route.
    pub fn with_context(&self, context: &PartitionContext) -> Self {
        Self {
            inner: self.inner.with_context(context.inner),
        }
    }
}

#[cfg(test)]
mod tests {
    use iggy::prelude::PollingKind;

    use super::*;

    const CONTEXT: RustPartitionContext = RustPartitionContext {
        incarnation: 7,
        owner_generation: 3,
        metadata_op: 11,
    };

    #[test]
    fn given_consumed_message_when_converted_should_keep_its_context_and_position() {
        let mut message = RustReceiveMessage::default();
        message.header.offset = 42;
        let received = ReceivedMessage::new(message, 50, 2, CONTEXT);
        let expected = received.position();

        let message = ReceiveMessage::from(received);

        assert_eq!(message.context().inner, CONTEXT);
        assert_eq!(message.position().inner, expected);
    }

    #[test]
    fn given_offset_strategy_when_given_context_should_stamp_it_into_the_poll() {
        let strategy = PollingStrategy::offset(42).with_context(&PartitionContext::from(CONTEXT));

        assert_eq!(
            strategy.inner,
            RustPollingStrategy {
                kind: PollingKind::Offset,
                value: 42,
                context: Some(CONTEXT),
            }
        );
    }

    #[test]
    fn given_saved_checkpoint_when_rebuilt_should_match_the_rust_position() {
        let context = PartitionContext::__construct(7, 3, 11);

        let position = ConsumerPosition::__construct(2, 42, &context);

        assert_eq!(
            position.inner,
            RustConsumerPosition {
                partition_id: 2,
                offset: 42,
                context: CONTEXT,
            }
        );
    }
}
