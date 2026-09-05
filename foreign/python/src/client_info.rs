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
    ClientInfo as RustClientInfo, ClientInfoDetails as RustClientInfoDetails,
    ConsumerGroupInfo as RustConsumerGroupInfo,
};
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

#[gen_stub_pyclass]
#[pyclass]
pub struct ClientInfo {
    pub(crate) inner: RustClientInfo,
}

impl From<RustClientInfo> for ClientInfo {
    fn from(client: RustClientInfo) -> Self {
        Self { inner: client }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl ClientInfo {
    /// The unique identifier of the client.
    #[getter]
    pub fn client_id(&self) -> u32 {
        self.inner.client_id
    }

    /// The unique identifier of the user, or `None` while the client is
    /// connected but not yet authenticated.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "builtins.int | None"))]
    pub fn user_id(&self) -> Option<u32> {
        self.inner.user_id
    }

    /// The remote address of the client.
    #[getter]
    pub fn address(&self) -> &str {
        &self.inner.address
    }

    /// The transport protocol used by the client, one of `"TCP"`, `"QUIC"`,
    /// `"HTTP"`, `"WebSocket"`, or `"Unknown"` for a transport this server
    /// does not recognise.
    #[getter]
    pub fn transport(&self) -> &str {
        &self.inner.transport
    }

    /// The number of consumer groups the client is part of.
    #[getter]
    pub fn consumer_groups_count(&self) -> u32 {
        self.inner.consumer_groups_count
    }
}

#[gen_stub_pyclass]
#[pyclass]
pub struct ClientInfoDetails {
    pub(crate) inner: RustClientInfoDetails,
}

impl From<RustClientInfoDetails> for ClientInfoDetails {
    fn from(client: RustClientInfoDetails) -> Self {
        Self { inner: client }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl ClientInfoDetails {
    /// The unique identifier of the client.
    #[getter]
    pub fn client_id(&self) -> u32 {
        self.inner.client_id
    }

    /// The unique identifier of the user, or `None` while the client is
    /// connected but not yet authenticated.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "builtins.int | None"))]
    pub fn user_id(&self) -> Option<u32> {
        self.inner.user_id
    }

    /// The remote address of the client.
    #[getter]
    pub fn address(&self) -> &str {
        &self.inner.address
    }

    /// The transport protocol used by the client, one of `"TCP"`, `"QUIC"`,
    /// `"HTTP"`, `"WebSocket"`, or `"Unknown"` for a transport this server
    /// does not recognise.
    #[getter]
    pub fn transport(&self) -> &str {
        &self.inner.transport
    }

    /// The number of consumer groups the client is part of.
    #[getter]
    pub fn consumer_groups_count(&self) -> u32 {
        self.inner.consumer_groups_count
    }

    /// The collection of consumer groups the client is part of.
    ///
    /// Each read rebuilds the list and every `ConsumerGroupInfo` in it.
    /// `ConsumerGroupInfo` has no `__eq__`, so two `ConsumerGroupInfo`
    /// instances built from separate reads compare unequal even with
    /// identical field values; bind the list once
    /// (`groups = details.consumer_groups`) and compare fields, not objects.
    #[getter]
    pub fn consumer_groups(&self) -> Vec<ConsumerGroupInfo> {
        self.inner
            .consumer_groups
            .iter()
            .map(ConsumerGroupInfo::from)
            .collect()
    }
}

#[gen_stub_pyclass]
#[pyclass]
pub struct ConsumerGroupInfo {
    pub(crate) inner: RustConsumerGroupInfo,
}

impl From<&RustConsumerGroupInfo> for ConsumerGroupInfo {
    fn from(group: &RustConsumerGroupInfo) -> Self {
        Self { inner: *group }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl ConsumerGroupInfo {
    /// The unique identifier (numeric) of the stream.
    #[getter]
    pub fn stream_id(&self) -> u32 {
        self.inner.stream_id
    }

    /// The unique identifier (numeric) of the topic.
    #[getter]
    pub fn topic_id(&self) -> u32 {
        self.inner.topic_id
    }

    /// The unique identifier (numeric) of the consumer group.
    #[getter]
    pub fn group_id(&self) -> u32 {
        self.inner.group_id
    }
}
