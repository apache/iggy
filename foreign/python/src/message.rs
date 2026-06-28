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

use bytes::Bytes;
use iggy::prelude::{
    IggyMessage as RustIggyMessage, IggyMessage as RustReceiveMessage, IggyMessageHeader,
    PollingStrategy as RustPollingStrategy,
};
use pyo3::{prelude::*, types::PyBytes};
use pyo3_stub_gen::{
    derive::{gen_stub_pyclass, gen_stub_pyclass_complex_enum, gen_stub_pymethods},
    impl_stub_type,
};
use std::str::FromStr;

/// A message to publish with `IggyClient.send_messages()`.
#[pyclass(from_py_object)]
#[gen_stub_pyclass]
pub struct SendMessage {
    pub(crate) inner: RustIggyMessage,
}

impl Clone for SendMessage {
    fn clone(&self) -> Self {
        Self {
            inner: RustIggyMessage {
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
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl SendMessage {
    /// Create a `SendMessage`.
    ///
    /// Args:
    ///     data: Message payload as `str | bytes`.
    ///
    /// Returns:
    ///     A `SendMessage`.
    ///
    /// Raises:
    ///     PyValueError: If the payload cannot be converted into a message.
    #[new]
    pub fn new(py: Python, data: PyMessagePayload) -> PyResult<Self> {
        let inner = match data {
            PyMessagePayload::String(data) => RustIggyMessage::from_str(&data)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?,
            PyMessagePayload::Bytes(data) => {
                let bytes = Bytes::from(data.extract::<Vec<u8>>(py)?);
                RustIggyMessage::builder()
                    .payload(bytes)
                    .build()
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?
            }
        };
        Ok(Self { inner })
    }
}

#[derive(FromPyObject, IntoPyObject)]
pub enum PyMessagePayload {
    #[pyo3(transparent, annotation = "str")]
    String(String),
    #[pyo3(transparent, annotation = "bytes")]
    Bytes(Py<PyBytes>),
}
impl_stub_type!(PyMessagePayload = String | PyBytes);

/// A message returned by `IggyClient.poll_messages()` or `IggyConsumer`.
///
/// This object exposes the payload and metadata for a single received message.
#[pyclass]
#[gen_stub_pyclass]
pub struct ReceiveMessage {
    pub(crate) inner: RustReceiveMessage,
    pub(crate) partition_id: u32,
}

#[gen_stub_pymethods]
#[pymethods]
impl ReceiveMessage {
    /// Get the message payload.
    ///
    /// Returns:
    ///     The payload as `bytes`.
    #[getter]
    pub fn payload<'a>(&self, py: Python<'a>) -> Bound<'a, PyBytes> {
        PyBytes::new(py, &self.inner.payload)
    }

    /// Get the message offset.
    ///
    /// Returns:
    ///     The message offset as `int`.
    #[getter]
    pub fn offset(&self) -> u64 {
        self.inner.header.offset
    }

    /// Get the message timestamp.
    ///
    /// Returns:
    ///     The message timestamp as `int`.
    #[getter]
    pub fn timestamp(&self) -> u64 {
        self.inner.header.timestamp
    }

    /// Get the message id.
    ///
    /// Returns:
    ///     The message id as `int`.
    #[getter]
    pub fn id(&self) -> u128 {
        self.inner.header.id
    }

    /// Get the message checksum.
    ///
    /// Returns:
    ///     The checksum as `int`.
    #[getter]
    pub fn checksum(&self) -> u64 {
        self.inner.header.checksum
    }

    /// Get the payload length.
    ///
    /// Returns:
    ///     The payload length in bytes as `int`.
    #[getter]
    pub fn length(&self) -> u32 {
        self.inner.header.payload_length
    }

    /// Get the partition id for this message.
    ///
    /// Returns:
    ///     The partition id as `int`.
    #[getter]
    pub fn partition_id(&self) -> u32 {
        self.partition_id
    }
}

#[derive(Clone, Copy)]
/// The starting point used when polling messages.
///
/// Use this type with `IggyClient.poll_messages(..., polling_strategy=...)` and
/// `IggyClient.consumer_group(..., polling_strategy=...)`.
#[gen_stub_pyclass_complex_enum]
#[pyclass(from_py_object)]
pub enum PollingStrategy {
    /// Start reading from an absolute offset.
    ///
    /// Args:
    ///     value: Absolute message offset as `int`.
    Offset { value: u64 },

    /// Start reading from the first message at or after a timestamp.
    ///
    /// Args:
    ///     value: Unix timestamp in microseconds as `int`.
    Timestamp { value: u64 },

    /// Start reading from the first available message.
    First {},

    /// Start reading from the most recent available message.
    Last {},

    /// Start reading from the next message after the stored consumer offset.
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
