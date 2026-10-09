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

use iggy::prelude::{
    ConsumerPosition as RustConsumerPosition, IggyMessage as RustReceiveMessage,
    PartitionContext as RustPartitionContext, PollingStrategy as RustPollingStrategy,
    ReceivedMessage,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyclass_complex_enum, gen_stub_pymethods};

use crate::user_headers::{UserHeaders, rust_user_headers_to_py};

/// The partition incarnation and consumer group owner a poll was served under.
///
/// Keep it with an offset to continue or commit under the same partition later: the
/// server refuses a context whose partition was deleted and created again, or whose
/// consumer group owner changed, instead of applying the offset to the new partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[gen_stub_pyclass]
#[pyclass(eq, frozen, hash, skip_from_py_object)]
pub struct PartitionContext {
    /// Identifies one creation of the partition. It changes when a deleted partition id is
    /// created again.
    #[pyo3(get)]
    pub incarnation: u64,
    /// The consumer group owner generation, or `0` outside a consumer group.
    #[pyo3(get)]
    pub owner_generation: u64,
    /// The metadata operation the server must have applied before it serves a request with
    /// this context.
    #[pyo3(get)]
    pub metadata_op: u64,
}

impl From<RustPartitionContext> for PartitionContext {
    fn from(context: RustPartitionContext) -> Self {
        Self {
            incarnation: context.incarnation,
            owner_generation: context.owner_generation,
            metadata_op: context.metadata_op,
        }
    }
}

impl From<PartitionContext> for RustPartitionContext {
    fn from(context: PartitionContext) -> Self {
        Self {
            incarnation: context.incarnation,
            owner_generation: context.owner_generation,
            metadata_op: context.metadata_op,
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PartitionContext {
    #[new]
    fn new(incarnation: u64, owner_generation: u64, metadata_op: u64) -> Self {
        Self {
            incarnation,
            owner_generation,
            metadata_op,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "PartitionContext(incarnation={}, owner_generation={}, metadata_op={})",
            self.incarnation, self.owner_generation, self.metadata_op
        )
    }
}

/// The position of a received message: its partition, its offset and the context of
/// the poll that delivered it. Commit it with `IggyConsumer.store_position()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[gen_stub_pyclass]
#[pyclass(eq, frozen, hash, skip_from_py_object)]
pub struct ConsumerPosition {
    /// The partition the message was read from.
    #[pyo3(get)]
    pub partition_id: u32,
    /// The offset of the message. Committing it marks the message as consumed.
    #[pyo3(get)]
    pub offset: u64,
    pub context: PartitionContext,
}

impl From<ConsumerPosition> for RustConsumerPosition {
    fn from(position: ConsumerPosition) -> Self {
        Self {
            partition_id: position.partition_id,
            offset: position.offset,
            context: position.context.into(),
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl ConsumerPosition {
    #[new]
    fn new(partition_id: u32, offset: u64, context: &PartitionContext) -> Self {
        Self {
            partition_id,
            offset,
            context: *context,
        }
    }

    // A getter, not `#[pyo3(get)]`: pyo3-stub-gen writes an undefined type name for a
    // pyclass-typed field.
    /// The partition incarnation and owner the poll was served under.
    #[getter]
    fn context(&self) -> PartitionContext {
        self.context
    }

    fn __repr__(&self) -> String {
        format!(
            "ConsumerPosition(partition_id={}, offset={}, context={})",
            self.partition_id,
            self.offset,
            self.context.__repr__()
        )
    }
}

/// A Python class representing a received message.
/// It provides access to the message payload and offset.
#[pyclass]
#[gen_stub_pyclass]
pub struct ReceiveMessage {
    pub(crate) inner: RustReceiveMessage,
    pub(crate) partition_id: u32,
    pub(crate) context: PartitionContext,
}

impl From<ReceivedMessage> for ReceiveMessage {
    fn from(received: ReceivedMessage) -> Self {
        Self {
            inner: received.message,
            partition_id: received.partition_id,
            context: received.context.into(),
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl ReceiveMessage {
    /// Retrieves the payload of the received message.
    /// The payload is returned as a Python bytes object.
    pub fn payload<'a>(&self, py: Python<'a>) -> Bound<'a, PyBytes> {
        PyBytes::new(py, &self.inner.payload)
    }

    /// Retrieves the offset of the received message.
    /// The offset represents the position of the message within its topic.
    pub fn offset(&self) -> u64 {
        self.inner.header.offset
    }

    /// Retrieves the timestamp of the received message.
    /// The timestamp represents the time of the message within its topic.
    pub fn timestamp(&self) -> u64 {
        self.inner.header.timestamp
    }

    /// Retrieves the origin timestamp of the received message.
    /// The origin timestamp represents when the message was originally created.
    pub fn origin_timestamp(&self) -> u64 {
        self.inner.header.origin_timestamp
    }

    /// Retrieves the id of the received message.
    /// The id represents unique identifier of the message within its topic.
    pub fn id(&self) -> u128 {
        self.inner.header.id
    }

    /// Retrieves the checksum of the received message.
    /// The checksum represents the integrity of the message within its topic.
    pub fn checksum(&self) -> u64 {
        self.inner.header.checksum
    }

    /// Retrieves the length of the received message.
    /// The length represents the length of the payload.
    pub fn length(&self) -> u32 {
        self.inner.header.payload_length
    }

    /// Retrieves the partition this message belongs to.
    pub fn partition_id(&self) -> u32 {
        self.partition_id
    }

    /// Retrieves the partition incarnation and owner the poll that delivered this message
    /// was served under.
    pub fn context(&self) -> PartitionContext {
        self.context
    }

    /// The position to commit for this message with `IggyConsumer.store_position()`.
    /// It keeps the context that delivered the message, so a commit after the partition
    /// was deleted and created again, or after its consumer group owner changed, is
    /// refused instead of landing on the new partition.
    pub fn position(&self) -> ConsumerPosition {
        ConsumerPosition {
            partition_id: self.partition_id,
            offset: self.inner.header.offset,
            context: self.context,
        }
    }

    /// Retrieves user headers attached to the received message.
    ///
    /// Returns `None` when no headers are present or when the headers
    /// on the wire are structurally malformed (those errors are logged
    /// internally). Only known semantic decode errors raise `ValueError`.
    #[gen_stub(override_return_type(type_repr = "UserHeaders | None"))]
    pub fn user_headers<'a>(&self, py: Python<'a>) -> PyResult<Option<Bound<'a, UserHeaders>>> {
        let Some(headers) = self
            .inner
            .user_headers_map()
            .map_err(|e| PyValueError::new_err(e.to_string()))?
        else {
            return Ok(None);
        };
        rust_user_headers_to_py(py, headers).map(Some)
    }
}

#[derive(Clone, Copy)]
#[gen_stub_pyclass_complex_enum]
#[pyclass(from_py_object)]
pub enum PollingStrategy {
    Offset { value: u64 },
    Timestamp { value: u64 },
    First {},
    Last {},
    Next {},
}

impl From<&PollingStrategy> for RustPollingStrategy {
    fn from(value: &PollingStrategy) -> Self {
        match value {
            PollingStrategy::Offset { value } => RustPollingStrategy::offset(value.to_owned()),
            PollingStrategy::Timestamp { value } => {
                RustPollingStrategy::timestamp(value.to_owned().into())
            }
            PollingStrategy::First {} => RustPollingStrategy::first(),
            PollingStrategy::Last {} => RustPollingStrategy::last(),
            PollingStrategy::Next {} => RustPollingStrategy::next(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    const PARTITION_ID: u32 = 3;
    const OFFSET: u64 = 42;
    const CONTEXT: RustPartitionContext = RustPartitionContext {
        incarnation: 7,
        owner_generation: 11,
        metadata_op: 13,
    };

    fn received_message() -> ReceivedMessage {
        let mut message = RustReceiveMessage::from_str("payload").expect("build message");
        message.header.offset = OFFSET;
        ReceivedMessage::new(message, OFFSET, PARTITION_ID, CONTEXT)
    }

    #[test]
    fn given_received_message_when_converted_should_keep_its_context() {
        let message = ReceiveMessage::from(received_message());

        assert_eq!(RustPartitionContext::from(message.context()), CONTEXT);
    }

    #[test]
    fn given_received_message_when_reading_position_should_match_the_rust_position() {
        let received = received_message();
        let expected = received.position();

        let message = ReceiveMessage::from(received);

        assert_eq!(RustConsumerPosition::from(message.position()), expected);
    }
}
