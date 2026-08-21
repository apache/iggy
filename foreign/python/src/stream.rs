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

use iggy::prelude::{Stream as RustStream, StreamDetails as RustStreamDetails};
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

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
    #[getter]
    pub fn id(&self) -> u32 {
        self.inner.id
    }

    #[getter]
    pub fn name(&self) -> String {
        self.inner.name.to_string()
    }

    #[getter]
    pub fn messages_count(&self) -> u64 {
        self.inner.messages_count
    }

    #[getter]
    pub fn topics_count(&self) -> u32 {
        self.inner.topics_count
    }
}

/// Summary information returned by `IggyClient.get_streams()`.
///
/// `created_at` is Unix time in microseconds. `size_bytes` is the stream's
/// current stored size in bytes.
#[gen_stub_pyclass]
#[pyclass]
pub struct Stream {
    pub(crate) inner: RustStream,
}

impl From<RustStream> for Stream {
    fn from(stream: RustStream) -> Self {
        Self { inner: stream }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl Stream {
    /// Numeric stream identifier.
    #[getter]
    pub fn id(&self) -> u32 {
        self.inner.id
    }

    /// Stream creation time as Unix time in microseconds.
    #[getter]
    pub fn created_at(&self) -> u64 {
        self.inner.created_at.as_micros()
    }

    /// Unique stream name.
    #[getter]
    pub fn name(&self) -> String {
        self.inner.name.to_string()
    }

    /// Current stored stream size in bytes.
    #[getter]
    pub fn size_bytes(&self) -> u64 {
        self.inner.size.as_bytes_u64()
    }

    /// Total messages across all topics in the stream.
    #[getter]
    pub fn messages_count(&self) -> u64 {
        self.inner.messages_count
    }

    /// Number of topics in the stream.
    #[getter]
    pub fn topics_count(&self) -> u32 {
        self.inner.topics_count
    }
}
