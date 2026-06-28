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

use iggy::prelude::StreamDetails as RustStreamDetails;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

/// Metadata returned for a stream.
///
/// This object is returned by `IggyClient.create_stream()` and
/// `IggyClient.get_stream()`.
#[pyclass]
#[gen_stub_pyclass]
pub struct StreamDetails {
    pub(crate) inner: RustStreamDetails,
}

impl From<RustStreamDetails> for StreamDetails {
    fn from(stream_details: RustStreamDetails) -> Self {
        Self {
            inner: stream_details,
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl StreamDetails {
    /// Get the stream id.
    ///
    /// Returns:
    ///     The stream id as `int`.
    #[getter]
    pub fn id(&self) -> u32 {
        self.inner.id
    }

    /// Get the stream name.
    ///
    /// Returns:
    ///     The stream name as `str`.
    #[getter]
    pub fn name(&self) -> String {
        self.inner.name.to_string()
    }

    /// Get the stream creation timestamp.
    ///
    /// Returns:
    ///     The creation timestamp in microseconds since the Unix epoch as `int`.
    #[getter]
    pub fn created_at(&self) -> u64 {
        self.inner.created_at.into()
    }

    /// Get the stream size in bytes.
    ///
    /// Returns:
    ///     The total stream size in bytes as `int`.
    #[getter]
    pub fn size(&self) -> u64 {
        self.inner.size.as_bytes_u64()
    }

    /// Get the number of messages in the stream.
    ///
    /// Returns:
    ///     The message count as `int`.
    #[getter]
    pub fn messages_count(&self) -> u64 {
        self.inner.messages_count
    }

    /// Get the number of topics in the stream.
    ///
    /// Returns:
    ///     The topic count as `int`.
    #[getter]
    pub fn topics_count(&self) -> u32 {
        self.inner.topics_count
    }
}

// TODO(slbotbm): after Topic and Partitions structs are added, implement topics() getter method in StreamDetails.
