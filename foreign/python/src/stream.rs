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

use crate::options::options_by_provenance;
use crate::topic::Topic;
use crate::user_headers::UserHeaders;

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
    /// Stream creation time as Unix time in microseconds.
    #[getter]
    pub fn created_at(&self) -> u64 {
        self.inner.created_at.as_micros()
    }

    #[getter]
    pub fn id(&self) -> u32 {
        self.inner.id
    }

    #[getter]
    pub fn name(&self) -> String {
        self.inner.name.to_string()
    }

    /// Current stored stream size in bytes.
    #[getter]
    pub fn size(&self) -> u64 {
        self.inner.size.as_bytes_u64()
    }

    #[getter]
    pub fn messages_count(&self) -> u64 {
        self.inner.messages_count
    }

    #[getter]
    pub fn topics_count(&self) -> u32 {
        self.inner.topics_count
    }

    /// Returns the topics in the stream.
    #[getter]
    pub fn topics(&self) -> Vec<Topic> {
        self.inner.topics.iter().map(Topic::from).collect()
    }

    /// Options the creating client set explicitly. The stream's encryption
    /// key is write-only and never appears here.
    ///
    /// The same `dict[HeaderKey, HeaderValue]` that `ReceiveMessage.user_headers`
    /// returns, since options ride that codec. `to_scalar_dict()` gives the
    /// plain-scalar form.
    #[getter]
    pub fn options<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, UserHeaders>> {
        options_by_provenance(py, &self.inner.options, true)
    }

    /// Options the server resolved or computed, including the encryption key
    /// projections. Same shape as `options`.
    #[getter]
    pub fn derived_options<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, UserHeaders>> {
        options_by_provenance(py, &self.inner.options, false)
    }

    /// Who chose the key the stream's data is stored under: `"stream"` for a
    /// key the client sent, `"server"` for a key the server generated, or
    /// `"none"` for a plaintext stream. `None` when the server reported no
    /// source.
    #[getter]
    pub fn encryption_key_source(&self) -> Option<String> {
        self.inner
            .encryption_key_source()
            .map(|source| source.to_string())
    }

    /// Fingerprint of the stream's key, 16 lowercase hex characters. `None`
    /// for a plaintext stream. Compare it with `encryption_key_fingerprint()`
    /// of the key the stream was created with.
    #[getter]
    pub fn encryption_key_fingerprint(&self) -> Option<String> {
        self.inner.encryption_key_fingerprint()
    }

    /// Fingerprints of the server keys that wrap the stream's key: one outside
    /// a key rotation, empty for a plaintext stream.
    #[getter]
    pub fn encryption_kek_fingerprints(&self) -> Vec<String> {
        self.inner.encryption_kek_fingerprints()
    }
}

/// Summary information returned by `IggyClient.get_streams()`.
///
/// `created_at` is Unix time in microseconds. `size` is the stream's current
/// stored size in bytes.
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
    pub fn size(&self) -> u64 {
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

    /// Options the creating client set explicitly. The stream's encryption
    /// key is write-only and never appears here.
    ///
    /// The same `dict[HeaderKey, HeaderValue]` that `ReceiveMessage.user_headers`
    /// returns, since options ride that codec. `to_scalar_dict()` gives the
    /// plain-scalar form.
    #[getter]
    pub fn options<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, UserHeaders>> {
        options_by_provenance(py, &self.inner.options, true)
    }

    /// Options the server resolved or computed, including the encryption key
    /// projections. Same shape as `options`.
    #[getter]
    pub fn derived_options<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, UserHeaders>> {
        options_by_provenance(py, &self.inner.options, false)
    }

    /// Who chose the key the stream's data is stored under: `"stream"` for a
    /// key the client sent, `"server"` for a key the server generated, or
    /// `"none"` for a plaintext stream. `None` when the server reported no
    /// source.
    #[getter]
    pub fn encryption_key_source(&self) -> Option<String> {
        self.inner
            .encryption_key_source()
            .map(|source| source.to_string())
    }

    /// Fingerprint of the stream's key, 16 lowercase hex characters. `None`
    /// for a plaintext stream. Compare it with `encryption_key_fingerprint()`
    /// of the key the stream was created with.
    #[getter]
    pub fn encryption_key_fingerprint(&self) -> Option<String> {
        self.inner.encryption_key_fingerprint()
    }

    /// Fingerprints of the server keys that wrap the stream's key: one outside
    /// a key rotation, empty for a plaintext stream.
    #[getter]
    pub fn encryption_kek_fingerprints(&self) -> Vec<String> {
        self.inner.encryption_kek_fingerprints()
    }
}
