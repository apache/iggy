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

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use iggy::consumer_ext::{IggyConsumerMessageExt, MessageConsumer};
use iggy::prelude::{
    AutoCommit as RustAutoCommit, AutoCommitAfter as RustAutoCommitAfter,
    AutoCommitWhen as RustAutoCommitWhen, ConsumerGroup as RustConsumerGroup,
    ConsumerGroupDetails as RustConsumerGroupDetails,
    ConsumerGroupMember as RustConsumerGroupMember, IggyConsumer as RustIggyConsumer, IggyDuration,
    IggyError, ReceivedMessage,
};
use pyo3::exceptions::PyStopAsyncIteration;
use pyo3::types::{PyDelta, PyDeltaAccess};

use pyo3::prelude::*;
use pyo3_async_runtimes::TaskLocals;
use pyo3_async_runtimes::tokio::{future_into_py, get_runtime, into_future, scope};
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyclass_complex_enum, gen_stub_pymethods};
use pyo3_stub_gen::{PyStubType, TypeInfo};
use tokio::sync::Mutex;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;

use crate::identity::PyIdentifier;
use crate::message::ReceiveMessage;

/// A Python class representing the Iggy consumer.
/// It wraps the RustIggyConsumer and provides asynchronous functionality
/// through the contained runtime.
#[gen_stub_pyclass]
#[pyclass]
pub struct IggyConsumer {
    pub(crate) inner: Arc<Mutex<RustIggyConsumer>>,
}

#[gen_stub_pymethods]
#[pymethods]
impl IggyConsumer {
    /// Get the last consumed offset for a partition.
    ///
    /// Args:
    ///     partition_id: Partition id as `int`.
    ///
    /// Returns:
    ///     The last consumed offset as `int`, or `None` if no offset has been consumed yet.
    #[gen_stub(override_return_type(type_repr = "builtins.int | None"))]
    fn get_last_consumed_offset(&self, partition_id: u32) -> Option<u64> {
        self.inner
            .blocking_lock()
            .get_last_consumed_offset(partition_id)
    }

    /// Get the last stored offset for a partition.
    ///
    /// Args:
    ///     partition_id: Partition id as `int`.
    ///
    /// Returns:
    ///     The last stored offset as `int`, or `None` if no offset has been stored yet.
    #[gen_stub(override_return_type(type_repr = "builtins.int | None"))]
    fn get_last_stored_offset(&self, partition_id: u32) -> Option<u64> {
        self.inner
            .blocking_lock()
            .get_last_stored_offset(partition_id)
    }

    /// Get the consumer group name.
    ///
    /// Returns:
    ///     The consumer group name as `str`.
    fn name(&self) -> String {
        self.inner.blocking_lock().name().to_string()
    }

    /// Get the current partition id.
    ///
    /// Returns:
    ///     The current partition id as `int`. Returns `0` if no messages have been
    ///     polled yet.
    fn partition_id(&self) -> u32 {
        self.inner.blocking_lock().partition_id()
    }

    /// Get the configured stream identifier.
    ///
    /// Returns:
    ///     The stream identifier as `str | int`, depending on how the consumer was configured.
    ///
    /// Raises:
    ///     PyRuntimeError: If the identifier cannot be converted for Python.
    fn stream(&self) -> PyResult<PyIdentifier> {
        let guard = self.inner.blocking_lock();
        PyIdentifier::try_from(guard.stream())
    }

    /// Get the configured topic identifier.
    ///
    /// Returns:
    ///     The topic identifier as `str | int`, depending on how the consumer was configured.
    ///
    /// Raises:
    ///     PyRuntimeError: If the identifier cannot be converted for Python.
    fn topic(&self) -> PyResult<PyIdentifier> {
        let guard = self.inner.blocking_lock();
        PyIdentifier::try_from(guard.topic())
    }

    /// Store a consumer offset on the server.
    ///
    /// Args:
    ///     offset: Offset to store as `int`.
    ///     partition_id: Partition id as `int | None`. If `None`, the current
    ///         consumer partition is used.
    ///
    /// Returns:
    ///     An awaitable that resolves to `None` when the offset is stored.
    ///
    /// Raises:
    ///     PyRuntimeError: If storing the offset fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn store_offset<'a>(
        &self,
        py: Python<'a>,
        offset: u64,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))] partition_id: Option<u32>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner
                .lock()
                .await
                .store_offset(offset, partition_id)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
        })
    }

    /// Delete a stored consumer offset from the server.
    ///
    /// Args:
    ///     partition_id: Partition id as `int | None`. If `None`, the current
    ///         consumer partition is used.
    ///
    /// Returns:
    ///     An awaitable that resolves to `None` when the offset is deleted.
    ///
    /// Raises:
    ///     PyRuntimeError: If deleting the offset fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn delete_offset<'a>(
        &self,
        py: Python<'a>,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))] partition_id: Option<u32>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner
                .lock()
                .await
                .delete_offset(partition_id)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
        })
    }

    /// Create an async iterator over `ReceiveMessage`.
    ///
    /// Returns:
    ///     A `collections.abc.AsyncIterator[ReceiveMessage]`.
    ///
    /// Raises:
    ///     PyRuntimeError: If polling messages fails while iterating.
    ///
    /// Notes:
    ///     This method does not currently support `AutoCommit.After`.
    ///     For `AutoCommit.IntervalOrAfter(datetime.timedelta, AutoCommitAfter)`,
    ///     only the interval part is applied; the `after` mode is ignored.
    ///     Use `consume_messages()` if you need commit-after-processing semantics.
    #[gen_stub(override_return_type(type_repr="collections.abc.AsyncIterator[ReceiveMessage]", imports=("collections.abc")))]
    fn iter_messages(&self) -> ReceiveMessageIterator {
        let inner = self.inner.clone();
        ReceiveMessageIterator { inner }
    }

    /// Consume messages continuously with an async callback.
    ///
    /// Args:
    ///     callback: Async callback as
    ///         `collections.abc.Callable[[ReceiveMessage], collections.abc.Awaitable[None]]`.
    ///         It is called once for each `ReceiveMessage`.
    ///     shutdown_event: Optional `asyncio.Event`. If provided, consumption
    ///         stops when the event is set.
    ///
    /// Returns:
    ///     An awaitable that resolves to `None` when consumption stops.
    ///
    /// Raises:
    ///     PyRuntimeError: If message consumption fails or shutdown signaling fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn consume_messages<'a>(
        &self,
        py: Python<'a>,
        #[gen_stub(override_type(type_repr="collections.abc.Callable[[ReceiveMessage], collections.abc.Awaitable[None]]", imports=("collections.abc")))]
        callback: Bound<'a, PyAny>,
        #[gen_stub(override_type(type_repr="asyncio.Event | None", imports=("asyncio")))]
        shutdown_event: Option<Bound<'a, PyAny>>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let inner = self.inner.clone();
        let callback: Py<PyAny> = callback.unbind();
        let shutdown_event: Option<Py<PyAny>> = shutdown_event.map(|e| e.unbind());

        future_into_py(py, async {
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

            let task_locals = Python::attach(pyo3_async_runtimes::tokio::get_current_locals)?;
            let handle_consume: JoinHandle<PyResult<Result<(), IggyError>>> =
                get_runtime().spawn(scope(task_locals, async move {
                    let task_locals =
                        Python::attach(pyo3_async_runtimes::tokio::get_current_locals)?;
                    let consumer = PyCallbackConsumer {
                        callback: Arc::new(callback),
                        task_locals: Arc::new(Mutex::new(task_locals)),
                    };
                    let mut inner = inner.lock().await;
                    Ok(inner.consume_messages(&consumer, shutdown_rx).await)
                }));
            let consume_result;

            if let Some(shutdown_event) = shutdown_event {
                let task_locals = Python::attach(pyo3_async_runtimes::tokio::get_current_locals)?;
                async fn shutdown_impl(
                    shutdown_event: Py<PyAny>,
                    shutdown_tx: Sender<()>,
                ) -> PyResult<()> {
                    Python::attach(|py| {
                        into_future(shutdown_event.bind(py).as_any().call_method0("wait")?)
                    })?
                    .await?;
                    shutdown_tx.send(()).map_err(|_| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                            "Failed to signal shutdown",
                        )
                    })?;
                    Ok(())
                }
                let handle_shutdown: JoinHandle<Result<(), PyErr>> = get_runtime().spawn(scope(
                    task_locals,
                    shutdown_impl(shutdown_event, shutdown_tx),
                ));
                let shutdown_result;
                (consume_result, shutdown_result) = tokio::join!(handle_consume, handle_shutdown);
                shutdown_result.map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                })??;
            } else {
                consume_result = handle_consume.await;
            }

            consume_result
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))??
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(())
        })
    }
}

#[gen_stub_pyclass]
#[pyclass]
pub struct ConsumerGroup {
    pub(crate) inner: RustConsumerGroup,
}

impl From<RustConsumerGroup> for ConsumerGroup {
    fn from(group: RustConsumerGroup) -> Self {
        Self { inner: group }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl ConsumerGroup {
    /// Gets the unique identifier (numeric) of the consumer group.
    #[getter]
    pub fn id(&self) -> u32 {
        self.inner.id
    }

    /// Gets the name of the consumer group.
    #[getter]
    pub fn name(&self) -> String {
        self.inner.name.to_string()
    }

    /// Gets the number of partitions the consumer group is consuming.
    #[getter]
    pub fn partitions_count(&self) -> u32 {
        self.inner.partitions_count
    }

    /// Gets the number of members in the consumer group.
    #[getter]
    pub fn members_count(&self) -> u32 {
        self.inner.members_count
    }
}

#[gen_stub_pyclass]
#[pyclass]
pub struct ConsumerGroupDetails {
    pub(crate) inner: RustConsumerGroupDetails,
}

impl From<RustConsumerGroupDetails> for ConsumerGroupDetails {
    fn from(group: RustConsumerGroupDetails) -> Self {
        Self { inner: group }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl ConsumerGroupDetails {
    /// Gets the unique identifier (numeric) of the consumer group.
    #[getter]
    pub fn id(&self) -> u32 {
        self.inner.id
    }

    /// Gets the name of the consumer group.
    #[getter]
    pub fn name(&self) -> String {
        self.inner.name.to_string()
    }

    /// Gets the number of partitions the consumer group is consuming.
    #[getter]
    pub fn partitions_count(&self) -> u32 {
        self.inner.partitions_count
    }

    /// Gets the number of members in the consumer group.
    #[getter]
    pub fn members_count(&self) -> u32 {
        self.inner.members_count
    }

    /// Gets the collection of members in the consumer group.
    #[getter]
    pub fn members(&self) -> Vec<ConsumerGroupMember> {
        self.inner
            .members
            .iter()
            .map(ConsumerGroupMember::from)
            .collect()
    }
}

#[gen_stub_pyclass]
#[pyclass]
pub struct ConsumerGroupMember {
    pub(crate) inner: RustConsumerGroupMember,
}

impl From<&RustConsumerGroupMember> for ConsumerGroupMember {
    fn from(member: &RustConsumerGroupMember) -> Self {
        Self {
            inner: RustConsumerGroupMember {
                id: member.id,
                partitions_count: member.partitions_count,
                partitions: member.partitions.clone(),
            },
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl ConsumerGroupMember {
    /// Gets the unique identifier (numeric) of the consumer group member.
    #[getter]
    pub fn id(&self) -> u32 {
        self.inner.id
    }

    /// Gets the number of partitions the consumer group member is consuming.
    #[getter]
    pub fn partitions_count(&self) -> u32 {
        self.inner.partitions_count
    }

    /// Gets the collection of partitions the consumer group member is consuming.
    #[getter]
    pub fn partitions(&self) -> Vec<u32> {
        self.inner.partitions.clone()
    }
}

#[pyclass]
pub struct ReceiveMessageIterator {
    pub(crate) inner: Arc<Mutex<RustIggyConsumer>>,
}

#[pymethods]
impl ReceiveMessageIterator {
    /// Return the next message from the iterator.
    ///
    /// Returns:
    ///     An awaitable that resolves to `ReceiveMessage`.
    ///
    /// Raises:
    ///     StopAsyncIteration: If there are no more messages.
    ///     PyRuntimeError: If polling the next message fails.
    pub fn __anext__<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let mut inner = inner.lock().await;
            if let Some(message) = inner.next().await {
                Ok(message
                    .map(|m| ReceiveMessage {
                        inner: m.message,
                        partition_id: m.partition_id,
                    })
                    .map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                    })?)
            } else {
                Err(PyStopAsyncIteration::new_err("No more messages"))
            }
        })
    }

    /// Return this async iterator.
    pub fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
}

struct PyCallbackConsumer {
    callback: Arc<Py<PyAny>>,
    task_locals: Arc<Mutex<TaskLocals>>,
}

impl MessageConsumer for PyCallbackConsumer {
    async fn consume(&self, received: ReceivedMessage) -> Result<(), IggyError> {
        let callback = self.callback.clone();
        let task_locals = self.task_locals.clone().lock_owned().await;
        let task_locals = task_locals.clone();
        let message = ReceiveMessage {
            inner: received.message,
            partition_id: received.partition_id,
        };
        get_runtime()
            .spawn(scope(task_locals, async move {
                Python::attach(|py| {
                    let callback = callback.bind(py);
                    let result = callback.as_any().call1((message,))?;
                    into_future(result)
                })
            }))
            .await
            .map_err(|_| IggyError::CannotReadMessage)?
            .map_err(|_| IggyError::CannotReadMessage)?
            .await
            .map_err(|_| IggyError::CannotReadMessage)?;
        Ok(())
    }
}

/// The auto-commit configuration for storing the offset on the server.
///
/// Use this type with `IggyClient.consumer_group(..., auto_commit=...)`.
// #[derive(Debug, PartialEq, Copy, Clone)]
#[gen_stub_pyclass_complex_enum]
#[pyclass]
pub enum AutoCommit {
    /// Disable automatic offset commits. Offsets must be stored manually.
    Disabled(),

    /// Commit offsets on a fixed interval.
    ///
    /// Args:
    ///     interval: Interval between automatic offset commits as
    ///         `datetime.timedelta`.
    Interval { interval: Py<PyDelta> },

    /// Commit offsets on a fixed interval or according to an `AutoCommitWhen` mode.
    ///
    /// Args:
    ///     interval: Interval between automatic offset commits as
    ///         `datetime.timedelta`.
    ///     when: Additional event that can trigger an offset commit as
    ///         `AutoCommitWhen`.
    IntervalOrWhen {
        interval: Py<PyDelta>,
        when: AutoCommitWhen,
    },

    /// Commit offsets on a fixed interval or according to an `AutoCommitAfter` mode.
    ///
    /// Args:
    ///     interval: Interval between automatic offset commits as
    ///         `datetime.timedelta`.
    ///     after: Post-consumption condition that can also trigger an offset
    ///         commit as `AutoCommitAfter`.
    IntervalOrAfter {
        interval: Py<PyDelta>,
        after: AutoCommitAfter,
    },

    /// Commit offsets according to an `AutoCommitWhen` mode.
    ///
    /// Args:
    ///     when: Event that triggers an offset commit as `AutoCommitWhen`.
    When { when: AutoCommitWhen },

    /// Commit offsets according to an `AutoCommitAfter` mode.
    ///
    /// Args:
    ///     after: Post-consumption condition that triggers an offset commit as
    ///         `AutoCommitAfter`.
    After { after: AutoCommitAfter },
}

impl From<&AutoCommit> for RustAutoCommit {
    fn from(val: &AutoCommit) -> RustAutoCommit {
        match val {
            AutoCommit::Disabled() => RustAutoCommit::Disabled,
            AutoCommit::Interval { interval } => {
                let duration = py_delta_to_iggy_duration(interval);
                RustAutoCommit::Interval(duration)
            }
            AutoCommit::IntervalOrWhen { interval, when } => {
                let duration = py_delta_to_iggy_duration(interval);
                RustAutoCommit::IntervalOrWhen(duration, when.into())
            }
            AutoCommit::IntervalOrAfter { interval, after } => {
                let duration = py_delta_to_iggy_duration(interval);
                RustAutoCommit::IntervalOrAfter(duration, after.into())
            }
            AutoCommit::When { when } => RustAutoCommit::When(when.into()),
            AutoCommit::After { after } => RustAutoCommit::After(after.into()),
        }
    }
}

/// The auto-commit mode for storing the offset on the server.
///
/// Use this type inside `AutoCommit.When(...)` or `AutoCommit.IntervalOrWhen(...)`.
#[derive(Debug, PartialEq, Copy, Clone)]
#[gen_stub_pyclass_complex_enum(skip_stub_type)]
#[pyclass(from_py_object)]
pub enum AutoCommitWhen {
    /// Store the offset when messages are polled from the server.
    PollingMessages(),

    /// Store the offset after all messages from a poll have been consumed.
    ConsumingAllMessages(),

    /// Store the offset after each consumed message.
    ConsumingEachMessage(),

    /// Store the offset after every Nth consumed message.
    ///
    /// Args:
    ///     count: Number of consumed messages between offset commits as `int`.
    ConsumingEveryNthMessage { count: u32 },
}

impl From<&AutoCommitWhen> for RustAutoCommitWhen {
    fn from(val: &AutoCommitWhen) -> RustAutoCommitWhen {
        match val {
            AutoCommitWhen::PollingMessages() => RustAutoCommitWhen::PollingMessages,
            AutoCommitWhen::ConsumingAllMessages() => RustAutoCommitWhen::ConsumingAllMessages,
            AutoCommitWhen::ConsumingEachMessage() => RustAutoCommitWhen::ConsumingEachMessage,
            AutoCommitWhen::ConsumingEveryNthMessage { count } => {
                RustAutoCommitWhen::ConsumingEveryNthMessage(count.to_owned())
            }
        }
    }
}

impl PyStubType for AutoCommitWhen {
    fn type_output() -> TypeInfo {
        TypeInfo::unqualified("AutoCommitWhen")
    }
}

/// The auto-commit mode for storing the offset on the server after receiving
/// the messages.
///
/// Use this type inside `AutoCommit.After(...)` or `AutoCommit.IntervalOrAfter(...)`.
#[derive(Debug, PartialEq, Copy, Clone)]
#[gen_stub_pyclass_complex_enum(skip_stub_type)]
#[pyclass(from_py_object)]
#[allow(clippy::enum_variant_names)]
pub enum AutoCommitAfter {
    /// Store the offset after all messages from a poll have been consumed.
    ConsumingAllMessages(),

    /// Store the offset after each consumed message.
    ConsumingEachMessage(),

    /// Store the offset after every Nth consumed message.
    ///
    /// Args:
    ///     count: Number of consumed messages between offset commits as `int`.
    ConsumingEveryNthMessage { count: u32 },
}

impl From<&AutoCommitAfter> for RustAutoCommitAfter {
    fn from(val: &AutoCommitAfter) -> RustAutoCommitAfter {
        match val {
            AutoCommitAfter::ConsumingAllMessages() => RustAutoCommitAfter::ConsumingAllMessages,
            AutoCommitAfter::ConsumingEachMessage() => RustAutoCommitAfter::ConsumingEachMessage,
            AutoCommitAfter::ConsumingEveryNthMessage { count } => {
                RustAutoCommitAfter::ConsumingEveryNthMessage(count.to_owned())
            }
        }
    }
}

impl PyStubType for AutoCommitAfter {
    fn type_output() -> TypeInfo {
        TypeInfo::unqualified("AutoCommitAfter")
    }
}

pub fn py_delta_to_iggy_duration(delta1: &Py<PyDelta>) -> IggyDuration {
    Python::attach(|py| {
        let delta = delta1.bind(py);
        let seconds = (delta.get_days() * 60 * 60 * 24 + delta.get_seconds()) as u64;
        let nanos = (delta.get_microseconds() * 1_000) as u32;
        IggyDuration::new(Duration::new(seconds, nanos))
    })
}
