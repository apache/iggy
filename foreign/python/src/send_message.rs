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
    IggyMessage as RustIggyMessage, IggyMessageHeader,
    SendMessagesConfirmationResponse as RustSendMessagesConfirmationResponse,
    SendMessagesResponse as RustSendMessagesResponse,
};
use pyo3::{exceptions::PyValueError, prelude::*, types::PyBytes};
use pyo3_stub_gen::{
    derive::{gen_stub_pyclass, gen_stub_pymethods},
    impl_stub_type,
};

use crate::user_headers::py_user_headers_to_rust;

/// A Python class representing a message to be sent.
/// This class wraps a Rust message meant for sending, facilitating
/// the creation of such messages from Python and their subsequent use in Rust.
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
    /// Constructs a new `SendMessage` instance from a string or bytes.
    /// This method allows for the creation of a `SendMessage` instance
    /// directly from Python using the provided string or bytes data.
    #[new]
    #[pyo3(signature = (data, user_headers=None, id=None))]
    pub fn new(
        py: Python,
        data: PyMessagePayload,
        #[gen_stub(override_type(type_repr = "dict | None"))] user_headers: Option<
            &Bound<'_, PyAny>,
        >,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))] id: Option<u128>,
    ) -> PyResult<Self> {
        let payload = match data {
            PyMessagePayload::String(data) => Bytes::from(data),
            PyMessagePayload::Bytes(data) => Bytes::from(data.extract::<Vec<u8>>(py)?),
        };
        let user_headers = user_headers
            .map(|headers| py_user_headers_to_rust(py, headers))
            .transpose()?;
        let inner = RustIggyMessage::builder()
            .maybe_id(id)
            .payload(payload)
            .maybe_user_headers(user_headers)
            .build()
            .map_err(to_value_error)?;
        Ok(Self { inner })
    }
}

fn to_value_error(error: impl ToString) -> PyErr {
    PyValueError::new_err(error.to_string())
}

#[derive(FromPyObject, IntoPyObject)]
pub enum PyMessagePayload {
    #[pyo3(transparent, annotation = "str")]
    String(String),
    #[pyo3(transparent, annotation = "bytes")]
    Bytes(Py<PyBytes>),
}
impl_stub_type!(PyMessagePayload = String | PyBytes);

/// A Python class representing the commit confirmation for one partition
/// written by a send.
#[pyclass]
#[gen_stub_pyclass]
pub struct SendMessagesConfirmationResponse {
    pub(crate) inner: RustSendMessagesConfirmationResponse,
}

impl From<&RustSendMessagesConfirmationResponse> for SendMessagesConfirmationResponse {
    fn from(confirmation: &RustSendMessagesConfirmationResponse) -> Self {
        Self {
            inner: confirmation.clone(),
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl SendMessagesConfirmationResponse {
    /// Gets the unique identifier (numeric) of the stream the batch was written to.
    #[getter]
    pub fn stream_id(&self) -> u32 {
        self.inner.stream_id
    }

    /// Gets the unique identifier (numeric) of the topic the batch was written to.
    #[getter]
    pub fn topic_id(&self) -> u32 {
        self.inner.topic_id
    }

    /// Gets the identifier of the partition the batch was written to.
    #[getter]
    pub fn partition_id(&self) -> u32 {
        self.inner.partition_id
    }

    /// Gets the offset assigned to the first message of the batch in this partition.
    #[getter]
    pub fn base_offset(&self) -> u64 {
        self.inner.base_offset
    }
}

/// A Python class representing the outcome of a successful send.
#[pyclass]
#[gen_stub_pyclass]
pub struct SendMessagesResponse {
    pub(crate) inner: RustSendMessagesResponse,
}

impl From<RustSendMessagesResponse> for SendMessagesResponse {
    fn from(response: RustSendMessagesResponse) -> Self {
        Self { inner: response }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl SendMessagesResponse {
    /// Gets the commit confirmations, one per partition the batch was written to.
    /// A server that commits without reporting offsets yields either an empty
    /// list or a single all-zero entry, so neither the length nor a zero
    /// `base_offset` can be read as a real offset.
    // TODO(hubcio): the all-zero entry is the SDK's stand-in for the empty body legacy
    // core/server sends; remove that shape once core/server is retired in favor of
    // core/server-ng, which always reports confirmations. An empty list stays valid.
    #[getter]
    pub fn confirmations(&self) -> Vec<SendMessagesConfirmationResponse> {
        self.inner
            .confirmations
            .iter()
            .map(SendMessagesConfirmationResponse::from)
            .collect()
    }
}
