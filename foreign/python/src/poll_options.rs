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

use std::num::NonZeroU32;

use iggy::prelude::{
    AutoCommit as RustAutoCommit, CommitAfter as RustCommitAfter, CommitEvery as RustCommitEvery,
    CommitPolicy as RustCommitPolicy, PollOptions as RustPollOptions,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDelta;
use pyo3_stub_gen::derive::{
    gen_stub_pyclass, gen_stub_pyclass_complex_enum, gen_stub_pyclass_enum, gen_stub_pymethods,
};

use crate::duration::{
    duration_repr, iggy_duration_to_py_delta, py_delta_to_iggy_duration, reject_zero,
};

/// When the offsets of the messages that a read returns are stored.
#[derive(Clone, Copy, PartialEq)]
#[gen_stub_pyclass_complex_enum]
#[pyclass(eq, from_py_object)]
pub enum CommitPolicy {
    /// Nothing is stored. The application commits checkpoints. The default.
    Manual(),
    /// The server stores the offset of the last message of each read as part of the read: one
    /// round trip, at most once.
    OnRead(),
    /// A `PartitionReader` or an `IggyConsumer` stores the latest counted position, at least
    /// once with `CommitAfter.Processing`. `IggyClient.poll_messages()` refuses it.
    Auto(AutoCommit),
}

/// When `CommitPolicy.Auto` stores. It also stores on close, on shutdown and before a rebalance
/// takes a partition away. A store only moves forward.
#[derive(Clone, Copy, PartialEq)]
#[gen_stub_pyclass]
#[pyclass(eq, frozen, from_py_object)]
pub struct AutoCommit {
    pub(crate) inner: RustAutoCommit,
}

/// The event on which `AutoCommit` also stores.
#[derive(Clone, Copy, PartialEq)]
#[gen_stub_pyclass_complex_enum]
#[pyclass(eq, from_py_object)]
pub enum CommitEvery {
    /// Each counted message, or each counted batch of a `PartitionReader`.
    Message(),
    /// This many counted messages of a partition since its last store. Not zero.
    Messages(u32),
    /// The last message of each batch.
    Batch(),
}

/// When a message counts for `CommitPolicy.Auto`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[gen_stub_pyclass_enum]
#[pyclass(eq, from_py_object)]
pub enum CommitAfter {
    /// When the application asks for the next message or batch, returns from its callback, or
    /// closes the reader. A callback that raises leaves its message uncounted.
    Processing,
    /// When the message or the batch is handed over.
    Delivery,
    /// Only when the application calls `mark()` with its checkpoint.
    Marked,
}

#[gen_stub_pymethods]
#[pymethods]
impl AutoCommit {
    /// Constructs when `CommitPolicy.Auto` stores. `AutoCommit.default()` stores every 10,000
    /// messages or every 5 seconds, whichever comes first, after processing.
    ///
    /// Args:
    ///     interval: Store the latest counted position at this interval, when it moved.
    ///     every: Also store it on this event.
    ///     after: When a message counts. `None` counts after processing, as
    ///         `CommitAfter.Processing`.
    ///
    /// Raises `ValueError` for a negative or zero `interval`, for `CommitEvery.Messages(0)`, and
    /// without `interval` and `every`, which would store only on close.
    #[new]
    #[pyo3(signature = (*, interval=None, every=None, after=None))]
    fn new(
        #[gen_stub(override_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
        interval: Option<Py<PyDelta>>,
        #[gen_stub(override_type(type_repr = "CommitEvery | None"))] every: Option<CommitEvery>,
        // A `CommitAfter` default makes the stub refer to the class before its definition.
        #[gen_stub(override_type(type_repr = "CommitAfter | None"))] after: Option<CommitAfter>,
    ) -> PyResult<Self> {
        let interval = interval
            .as_ref()
            .map(|interval| reject_zero(py_delta_to_iggy_duration(interval)?, "interval"))
            .transpose()?;
        let every = every.map(RustCommitEvery::try_from).transpose()?;
        if interval.is_none() && every.is_none() {
            return Err(PyValueError::new_err(
                "an AutoCommit needs an 'interval' or an 'every' event",
            ));
        }
        Ok(Self {
            inner: RustAutoCommit {
                interval,
                every,
                after: after.map_or_else(RustCommitAfter::default, Into::into),
            },
        })
    }

    /// Every 10,000 messages or every 5 seconds, whichever comes first, after processing.
    #[staticmethod]
    fn default() -> Self {
        Self {
            inner: RustAutoCommit::default(),
        }
    }

    /// The interval of the stores, or `None`.
    #[gen_stub(override_return_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
    #[getter]
    fn interval<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyDelta>>> {
        self.inner
            .interval
            .map(|interval| iggy_duration_to_py_delta(py, interval.get()))
            .transpose()
    }

    /// The event of the stores, or `None`.
    #[gen_stub(override_return_type(type_repr = "CommitEvery | None"))]
    #[getter]
    fn every(&self) -> Option<CommitEvery> {
        self.inner.every.map(CommitEvery::from)
    }

    /// When a message counts.
    #[getter]
    fn after(&self) -> CommitAfter {
        self.inner.after.into()
    }

    fn __repr__(&self) -> String {
        auto_commit_repr(&self.inner)
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl CommitPolicy {
    fn __repr__(&self) -> String {
        commit_policy_repr((*self).into())
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl CommitEvery {
    fn __repr__(&self) -> String {
        commit_every_repr(match self {
            Self::Message() => Some(RustCommitEvery::Message),
            Self::Messages(count) => NonZeroU32::new(*count).map(RustCommitEvery::Messages),
            Self::Batch() => Some(RustCommitEvery::Batch),
        })
    }
}

impl From<CommitPolicy> for RustCommitPolicy {
    fn from(policy: CommitPolicy) -> Self {
        match policy {
            CommitPolicy::Manual() => Self::Manual,
            CommitPolicy::OnRead() => Self::OnRead,
            CommitPolicy::Auto(auto) => Self::Auto(auto.inner),
        }
    }
}

impl From<RustCommitPolicy> for CommitPolicy {
    fn from(policy: RustCommitPolicy) -> Self {
        match policy {
            RustCommitPolicy::Manual => Self::Manual(),
            RustCommitPolicy::OnRead => Self::OnRead(),
            RustCommitPolicy::Auto(inner) => Self::Auto(AutoCommit { inner }),
        }
    }
}

impl TryFrom<CommitEvery> for RustCommitEvery {
    type Error = PyErr;

    fn try_from(every: CommitEvery) -> PyResult<Self> {
        Ok(match every {
            CommitEvery::Message() => Self::Message,
            CommitEvery::Messages(count) => {
                Self::Messages(NonZeroU32::new(count).ok_or_else(|| {
                    PyValueError::new_err("'CommitEvery.Messages' needs a count above zero")
                })?)
            }
            CommitEvery::Batch() => Self::Batch,
        })
    }
}

impl From<RustCommitEvery> for CommitEvery {
    fn from(every: RustCommitEvery) -> Self {
        match every {
            RustCommitEvery::Message => Self::Message(),
            RustCommitEvery::Messages(count) => Self::Messages(count.get()),
            RustCommitEvery::Batch => Self::Batch(),
        }
    }
}

impl From<CommitAfter> for RustCommitAfter {
    fn from(after: CommitAfter) -> Self {
        match after {
            CommitAfter::Processing => Self::Processing,
            CommitAfter::Delivery => Self::Delivery,
            CommitAfter::Marked => Self::Marked,
        }
    }
}

impl From<RustCommitAfter> for CommitAfter {
    fn from(after: RustCommitAfter) -> Self {
        match after {
            RustCommitAfter::Processing => Self::Processing,
            RustCommitAfter::Delivery => Self::Delivery,
            RustCommitAfter::Marked => Self::Marked,
        }
    }
}

/// The policy as Python writes it, so a `__repr__` built from it can be pasted back.
fn commit_policy_repr(policy: RustCommitPolicy) -> String {
    match policy {
        RustCommitPolicy::Manual => "CommitPolicy.Manual()".to_owned(),
        RustCommitPolicy::OnRead => "CommitPolicy.OnRead()".to_owned(),
        RustCommitPolicy::Auto(auto) => format!("CommitPolicy.Auto({})", auto_commit_repr(&auto)),
    }
}

fn auto_commit_repr(auto: &RustAutoCommit) -> String {
    let interval = auto.interval.map_or_else(
        || "None".to_owned(),
        |interval| duration_repr(interval.get()),
    );
    format!(
        "AutoCommit(interval={interval}, every={}, after=CommitAfter.{:?})",
        commit_every_repr(auto.every),
        CommitAfter::from(auto.after)
    )
}

fn commit_every_repr(every: Option<RustCommitEvery>) -> String {
    match every {
        None => "None".to_owned(),
        Some(RustCommitEvery::Message) => "CommitEvery.Message()".to_owned(),
        Some(RustCommitEvery::Messages(count)) => format!("CommitEvery.Messages({count})"),
        Some(RustCommitEvery::Batch) => "CommitEvery.Batch()".to_owned(),
    }
}

/// The limits of each read and when its offsets are stored.
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
    ///     commit: When offsets are stored. `None` stores none, as `CommitPolicy.Manual()`.
    ///
    /// The limits are checked when the options are used, and a fault raises `IggyError`.
    #[new]
    #[pyo3(signature = (
        *,
        count=RustPollOptions::default().count,
        max_wait=None,
        min_count=RustPollOptions::default().min_count,
        max_bytes=RustPollOptions::default().max_bytes,
        commit=None,
    ))]
    fn new(
        count: u32,
        #[gen_stub(override_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
        max_wait: Option<Py<PyDelta>>,
        min_count: u32,
        max_bytes: u32,
        // A `CommitPolicy` default makes the stub refer to the class before its definition,
        // which the stub linters refuse.
        #[gen_stub(override_type(type_repr = "CommitPolicy | None"))] commit: Option<CommitPolicy>,
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
                commit: commit.map_or(defaults.commit, Into::into),
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

    /// When the offsets of a read are stored.
    #[getter]
    fn commit(&self) -> CommitPolicy {
        self.inner.commit.into()
    }

    fn __repr__(&self) -> String {
        format!(
            "PollOptions(count={}, max_wait={}, min_count={}, max_bytes={}, commit={})",
            self.inner.count,
            duration_repr(self.inner.max_wait),
            self.inner.min_count,
            self.inner.max_bytes,
            commit_policy_repr(self.inner.commit)
        )
    }
}
