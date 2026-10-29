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
    Checkpoint as RustCheckpoint, IggyMessage as RustReceiveMessage,
    PartitionContext as RustPartitionContext, PolledMessages as RustPolledMessages, PollingKind,
    PollingStrategy as RustPollingStrategy, ReceivedMessage,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple};
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

use crate::user_headers::{UserHeaders, rust_user_headers_to_py};

/// The partition incarnation and consumer group owner a request was served under.
///
/// The server refuses a context whose partition was deleted and created again, or whose
/// consumer group owner changed, instead of applying an offset to the new partition.
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

/// The position after one received message: its partition, its offset and the partition
/// history that delivered it. A commit stores its offset, which marks every earlier message
/// of the partition as consumed.
///
/// Get it from `ReceiveMessage.checkpoint()` or `PolledMessages.checkpoint()`, and commit it
/// with `PartitionReader.commit()` or `IggyConsumer.commit()`. A commit after the partition was
/// deleted and created again, or after its consumer group owner changed, is refused instead of
/// landing on the new partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[gen_stub_pyclass]
#[pyclass(eq, frozen, skip_from_py_object)]
pub struct Checkpoint {
    pub(crate) inner: RustCheckpoint,
}

#[gen_stub_pymethods]
#[pymethods]
impl Checkpoint {
    /// The partition the message was read from.
    #[getter]
    fn partition_id(&self) -> u32 {
        self.inner.partition_id()
    }

    /// The offset of the message. Committing it marks the message as consumed.
    #[getter]
    fn offset(&self) -> u64 {
        self.inner.offset()
    }

    fn __repr__(&self) -> String {
        format!(
            "Checkpoint(partition_id={}, offset={})",
            self.inner.partition_id(),
            self.inner.offset()
        )
    }
}

/// A Python class representing a received message.
/// It provides access to the message payload and offset.
#[pyclass(frozen)]
#[gen_stub_pyclass]
pub struct ReceiveMessage {
    pub(crate) inner: RustReceiveMessage,
    pub(crate) checkpoint: RustCheckpoint,
}

impl From<ReceivedMessage> for ReceiveMessage {
    fn from(received: ReceivedMessage) -> Self {
        Self {
            checkpoint: received.checkpoint(),
            inner: received.message,
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
        self.checkpoint.partition_id()
    }

    /// The position after this message, to commit it and every earlier message of its
    /// partition with `PartitionReader.commit()` or `IggyConsumer.commit()`. The reader or
    /// consumer that delivered the message must commit it.
    pub fn checkpoint(&self) -> Checkpoint {
        Checkpoint {
            inner: self.checkpoint,
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

/// One batch of messages of one partition, from `IggyClient.poll_messages()` or
/// `PartitionReader.next()`.
#[gen_stub_pyclass]
#[pyclass(frozen)]
pub struct PolledMessages {
    partition_id: u32,
    current_offset: u64,
    messages: Py<PyTuple>,
    checkpoints: Vec<RustCheckpoint>,
    continuation: RustPollingStrategy,
}

impl PolledMessages {
    fn new(py: Python<'_>, polled: RustPolledMessages) -> PyResult<Self> {
        let continuation = polled.continuation();
        let checkpoints: Vec<RustCheckpoint> = (0..polled.messages.len())
            .map_while(|index| polled.checkpoint_at(index))
            .collect();
        let messages = polled
            .messages
            .into_iter()
            .zip(checkpoints.iter().copied())
            .map(|(inner, checkpoint)| ReceiveMessage { inner, checkpoint });
        let messages = PyTuple::new(py, messages)?.unbind();
        Ok(Self {
            partition_id: polled.partition_id,
            current_offset: polled.current_offset,
            messages,
            checkpoints,
            continuation,
        })
    }
}

/// A batch that becomes a `PolledMessages` where pyo3 hands the result to Python, so the
/// conversion holds the GIL on no Tokio worker.
pub(crate) struct PolledBatch(pub(crate) RustPolledMessages);

impl<'py> IntoPyObject<'py> for PolledBatch {
    type Target = PolledMessages;
    type Output = Bound<'py, PolledMessages>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        Bound::new(py, PolledMessages::new(py, self.0)?)
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PolledMessages {
    /// The partition the batch was read from.
    #[getter]
    fn partition_id(&self) -> u32 {
        self.partition_id
    }

    /// The offset of the newest message in the partition when the batch was read.
    #[getter]
    fn current_offset(&self) -> u64 {
        self.current_offset
    }

    /// The messages of the batch, in offset order. Each read returns the same tuple.
    #[gen_stub(override_return_type(type_repr = "builtins.tuple[ReceiveMessage, ...]"))]
    #[getter]
    fn messages(&self, py: Python<'_>) -> Py<PyTuple> {
        self.messages.clone_ref(py)
    }

    /// The position after the last message, to commit the whole batch. `None` for an empty
    /// batch.
    #[gen_stub(override_return_type(type_repr = "Checkpoint | None"))]
    fn checkpoint(&self) -> Option<Checkpoint> {
        self.checkpoints.last().map(|&inner| Checkpoint { inner })
    }

    /// The position after `messages[index]`, to commit a processed part of the batch. `None`
    /// for an index out of range.
    #[gen_stub(override_return_type(type_repr = "Checkpoint | None"))]
    fn checkpoint_at(&self, index: usize) -> Option<Checkpoint> {
        self.checkpoints
            .get(index)
            .map(|&inner| Checkpoint { inner })
    }

    /// The position after the batch, for the next read of its partition.
    ///
    /// After the last message, it reads from the next offset. An empty batch keeps the start of
    /// its request: `Offset` and `Timestamp` keep their value, `First` reads from offset 0,
    /// `Last` stays `Last`, and `Next` reads after the stored offset again.
    ///
    /// The continuation belongs to the partition of the batch, so a read of another partition
    /// refuses it with `InvalidCommand` (3). It keeps the partition history of the batch, so a
    /// read after the partition was deleted and created again, or after its consumer group
    /// owner changed, raises `HistoryUnavailable` (87) instead of reading the new partition.
    fn continuation(&self) -> PollingStrategy {
        PollingStrategy {
            inner: self.continuation,
        }
    }
}

/// Where a read starts. A strategy from a constructor reads from its own start in any
/// partition. A continuation from `PolledMessages.continuation()` belongs to the partition of
/// its batch and keeps its partition history.
#[derive(Clone, Copy)]
#[gen_stub_pyclass]
#[pyclass(frozen, from_py_object)]
pub struct PollingStrategy {
    pub(crate) inner: RustPollingStrategy,
}

impl From<RustPollingStrategy> for PollingStrategy {
    fn from(inner: RustPollingStrategy) -> Self {
        Self { inner }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PollingStrategy {
    /// Reads from the offset `value`.
    #[staticmethod]
    #[pyo3(name = "Offset")]
    fn offset(value: u64) -> Self {
        RustPollingStrategy::offset(value).into()
    }

    /// Reads from the first message at or after the timestamp `value`, in microseconds.
    #[staticmethod]
    #[pyo3(name = "Timestamp")]
    fn timestamp(value: u64) -> Self {
        RustPollingStrategy::timestamp(value.into()).into()
    }

    /// Reads from the first message of the partition.
    #[staticmethod]
    #[pyo3(name = "First")]
    fn first() -> Self {
        RustPollingStrategy::first().into()
    }

    /// Reads the last messages of the partition.
    #[staticmethod]
    #[pyo3(name = "Last")]
    fn last() -> Self {
        RustPollingStrategy::last().into()
    }

    /// Reads after the offset stored for the consumer, or from offset 0 without one.
    #[staticmethod]
    #[pyo3(name = "Next")]
    fn next() -> Self {
        RustPollingStrategy::next().into()
    }

    fn __repr__(&self) -> String {
        let value = self.inner.value;
        match self.inner.kind {
            PollingKind::Offset => format!("PollingStrategy.Offset(value={value})"),
            PollingKind::Timestamp => format!("PollingStrategy.Timestamp(value={value})"),
            PollingKind::First => "PollingStrategy.First()".to_owned(),
            PollingKind::Last => "PollingStrategy.Last()".to_owned(),
            PollingKind::Next => "PollingStrategy.Next()".to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    const PARTITION_ID: u32 = 3;
    const FIRST_OFFSET: u64 = 42;
    const MESSAGES_COUNT: u64 = 3;
    const CURRENT_OFFSET: u64 = 100;
    const CONTEXT: RustPartitionContext = RustPartitionContext {
        incarnation: 7,
        owner_generation: 11,
        metadata_op: 13,
    };

    fn message(offset: u64) -> RustReceiveMessage {
        let mut message = RustReceiveMessage::from_str("payload").expect("build message");
        message.header.offset = offset;
        message
    }

    fn polled(
        messages: Vec<RustReceiveMessage>,
        start: &RustPollingStrategy,
    ) -> RustPolledMessages {
        let count = u32::try_from(messages.len()).expect("message count fits u32");
        let mut polled =
            RustPolledMessages::new(CONTEXT, PARTITION_ID, CURRENT_OFFSET, count, messages);
        polled.set_request_start(start);
        polled
    }

    #[test]
    fn given_received_message_when_converted_should_keep_its_checkpoint() {
        let received =
            ReceivedMessage::new(message(FIRST_OFFSET), CURRENT_OFFSET, PARTITION_ID, CONTEXT);
        let expected = received.checkpoint();

        let message = ReceiveMessage::from(received);

        assert_eq!(message.checkpoint().inner, expected);
        assert_eq!(message.partition_id(), PARTITION_ID);
        assert_eq!(message.offset(), FIRST_OFFSET);
    }

    #[test]
    fn given_polled_batch_when_converted_should_match_the_rust_checkpoints_and_continuation() {
        Python::initialize();
        Python::attach(|py| {
            let offsets = FIRST_OFFSET..FIRST_OFFSET + MESSAGES_COUNT;
            let rust = polled(
                offsets.clone().map(message).collect(),
                &RustPollingStrategy::offset(FIRST_OFFSET),
            );
            let expected_checkpoints: Vec<_> = (0..offsets.clone().count())
                .map(|index| rust.checkpoint_at(index))
                .collect();
            let expected_last = rust.checkpoint();
            let expected_continuation = rust.continuation();

            let batch = PolledMessages::new(py, rust).expect("convert batch");

            assert_eq!(batch.partition_id(), PARTITION_ID);
            assert_eq!(batch.current_offset(), CURRENT_OFFSET);
            assert!(batch.messages(py).is(batch.messages(py)));
            let messages: Vec<Py<ReceiveMessage>> =
                batch.messages(py).extract(py).expect("a tuple of messages");
            assert_eq!(
                messages
                    .iter()
                    .map(|message| message.get().offset())
                    .collect::<Vec<_>>(),
                offsets.collect::<Vec<_>>()
            );
            for (index, expected) in expected_checkpoints.into_iter().enumerate() {
                assert_eq!(
                    batch
                        .checkpoint_at(index)
                        .map(|checkpoint| checkpoint.inner),
                    expected
                );
                assert_eq!(Some(messages[index].get().checkpoint().inner), expected);
            }
            assert_eq!(
                batch.checkpoint().map(|checkpoint| checkpoint.inner),
                expected_last
            );
            assert!(batch.checkpoint_at(messages.len()).is_none());
            assert_eq!(batch.continuation().inner, expected_continuation);
        });
    }

    #[test]
    fn given_empty_batch_when_converted_should_keep_the_start_of_its_request() {
        Python::initialize();
        Python::attach(|py| {
            let start = RustPollingStrategy::timestamp(CURRENT_OFFSET.into());
            let rust = polled(Vec::new(), &start);
            let expected_continuation = rust.continuation();

            let batch = PolledMessages::new(py, rust).expect("convert batch");

            assert!(batch.messages(py).bind(py).is_empty());
            assert!(batch.checkpoint().is_none());
            assert_eq!(batch.continuation().inner, expected_continuation);
            assert_eq!(batch.continuation().inner.value, start.value);
        });
    }
}
