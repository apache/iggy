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

use iggy::prelude::{PollOptions as RustPollOptions, ServerCommitPolicy as RustServerCommitPolicy};
use pyo3::prelude::*;
use pyo3::types::PyDelta;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyclass_enum, gen_stub_pymethods};

use crate::duration::{duration_repr, iggy_duration_to_py_delta, py_delta_to_iggy_duration};

/// Who stores the offset of a read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[gen_stub_pyclass_enum]
#[pyclass(eq, from_py_object)]
pub enum ServerCommitPolicy {
    /// The application, a `PartitionReader` or an `IggyConsumer` commits.
    Disabled,
    /// The server stores the offset of the last message of a reply as part of the reply.
    OnRead,
}

impl From<ServerCommitPolicy> for RustServerCommitPolicy {
    fn from(policy: ServerCommitPolicy) -> Self {
        match policy {
            ServerCommitPolicy::Disabled => Self::Disabled,
            ServerCommitPolicy::OnRead => Self::OnRead,
        }
    }
}

impl From<RustServerCommitPolicy> for ServerCommitPolicy {
    fn from(policy: RustServerCommitPolicy) -> Self {
        match policy {
            RustServerCommitPolicy::Disabled => Self::Disabled,
            RustServerCommitPolicy::OnRead => Self::OnRead,
        }
    }
}

/// The limits of each read and who stores its offset.
#[derive(Clone, Copy)]
#[gen_stub_pyclass]
#[pyclass(frozen, from_py_object)]
pub struct PollOptions {
    pub(crate) inner: RustPollOptions,
}

#[gen_stub_pymethods]
#[pymethods]
impl PollOptions {
    /// Constructs the options of a read.
    ///
    /// Args:
    ///     count: The most messages in one batch.
    ///     max_wait: How long the server waits for `min_count` messages, at most 600 seconds.
    ///         `None` answers at once. A reader or a consumer with no wait on an idle topic
    ///         polls in a busy loop.
    ///     min_count: The messages to wait for, from 1 to `count`.
    ///     max_bytes: The most bytes in one batch. A message larger than that comes alone.
    ///     server_commit: `ServerCommitPolicy.OnRead` makes the server store the offset of
    ///         each reply. `None` stores no offset, as `ServerCommitPolicy.Disabled`.
    ///
    /// The limits are checked when the options are used, and a fault raises `IggyError`.
    #[new]
    #[pyo3(signature = (
        *,
        count=RustPollOptions::default().count,
        max_wait=None,
        min_count=RustPollOptions::default().min_count,
        max_bytes=RustPollOptions::default().max_bytes,
        server_commit=None,
    ))]
    fn new(
        count: u32,
        #[gen_stub(override_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
        max_wait: Option<Py<PyDelta>>,
        min_count: u32,
        max_bytes: u32,
        // A `ServerCommitPolicy` default makes the stub refer to the class before its
        // definition, which the stub linters refuse.
        #[gen_stub(override_type(type_repr = "ServerCommitPolicy | None"))] server_commit: Option<
            ServerCommitPolicy,
        >,
    ) -> PyResult<Self> {
        let defaults = RustPollOptions::default();
        let max_wait = max_wait
            .as_ref()
            .map(py_delta_to_iggy_duration)
            .transpose()?
            .unwrap_or(defaults.max_wait);
        Ok(Self {
            inner: RustPollOptions {
                count,
                max_wait,
                min_count,
                max_bytes,
                server_commit: server_commit.map_or(defaults.server_commit, Into::into),
            },
        })
    }

    /// The most messages in one batch.
    #[getter]
    fn count(&self) -> u32 {
        self.inner.count
    }

    /// How long the server waits for `min_count` messages. Zero answers at once.
    #[gen_stub(override_return_type(type_repr = "datetime.timedelta", imports=("datetime")))]
    #[getter]
    fn max_wait<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDelta>> {
        iggy_duration_to_py_delta(py, self.inner.max_wait)
    }

    /// The messages to wait for.
    #[getter]
    fn min_count(&self) -> u32 {
        self.inner.min_count
    }

    /// The most bytes in one batch.
    #[getter]
    fn max_bytes(&self) -> u32 {
        self.inner.max_bytes
    }

    /// Who stores the offset of a read.
    #[getter]
    fn server_commit(&self) -> ServerCommitPolicy {
        self.inner.server_commit.into()
    }

    fn __repr__(&self) -> String {
        format!(
            "PollOptions(count={}, max_wait={}, min_count={}, max_bytes={}, server_commit=ServerCommitPolicy.{:?})",
            self.inner.count,
            duration_repr(self.inner.max_wait),
            self.inner.min_count,
            self.inner.max_bytes,
            ServerCommitPolicy::from(self.inner.server_commit)
        )
    }
}
