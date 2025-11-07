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

use iggy::prelude::{IggyMessage as RustReceiveMessage, PollingStrategy as RustPollingStrategy};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyclass_complex_enum, gen_stub_pymethods};

/// A Python class representing a received message.
///
/// This class wraps a Rust message, allowing for access to its payload and offset from Python.
#[pyclass]
#[gen_stub_pyclass]
pub struct ReceiveMessage {
    pub(crate) inner: RustReceiveMessage,
}

impl ReceiveMessage {
    /// Converts a Rust message into its corresponding Python representation.
    ///
    /// This is an internal utility function, not exposed to Python.
    pub(crate) fn from_rust_message(message: RustReceiveMessage) -> Self {
        Self { inner: message }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl ReceiveMessage {
    /// Retrieves the payload of the received message.
    ///
    /// The payload is returned as a Python bytes object.
    pub fn payload<'a>(&self, py: Python<'a>) -> Bound<'a, PyBytes> {
        PyBytes::new(py, &self.inner.payload)
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
    pub fn id(&self) -> u128 {
        self.inner.header.id
    }

    /// Retrieves the checksum of the received message.
    ///
    /// The checksum represents the integrity of the message within its topic.
    pub fn checksum(&self) -> u64 {
        self.inner.header.checksum
    }

    /// Retrieves the length of the received message.
    ///
    /// The length represents the length of the payload.
    pub fn length(&self) -> u32 {
        self.inner.header.payload_length
    }
}

#[derive(Clone, Copy)]
#[gen_stub_pyclass_complex_enum]
#[pyclass]
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
