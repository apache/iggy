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

use futures::StreamExt;
use iggy::consumer_ext::{IggyConsumerMessageExt, MessageConsumer};
use iggy::prelude::{
    AutoCommit as RustAutoCommit, AutoCommitAfter as RustAutoCommitAfter,
    AutoCommitWhen as RustAutoCommitWhen, Consumer as RustConsumer,
    ConsumerGroup as RustConsumerGroup, ConsumerGroupDetails as RustConsumerGroupDetails,
    ConsumerGroupMember as RustConsumerGroupMember, Identifier, IggyConsumer as RustIggyConsumer,
    IggyConsumerState as RustIggyConsumerState, IggyDuration, IggyError, ReceivedMessage,
};
use pyo3::exceptions::PyStopAsyncIteration;
use pyo3::types::PyDelta;

use pyo3::prelude::*;
use pyo3_async_runtimes::TaskLocals;
use pyo3_async_runtimes::tokio::{future_into_py, get_runtime, into_future, scope};
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyclass_complex_enum, gen_stub_pymethods};
use pyo3_stub_gen::{PyStubType, TypeInfo};
use tokio::sync::Mutex;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;

use crate::duration::{py_delta_to_iggy_duration, reject_zero};
use crate::identifier::PyIdentifier;
use crate::receive_message::ReceiveMessage;

/// A Python class representing the Iggy consumer.
/// It provides asynchronous functionality through the contained runtime.
// `inner` stays locked for the whole duration of a consumption run, so everything that can
// be served from `state` or from a snapshot must not touch it.
#[gen_stub_pyclass]
#[pyclass]
pub struct IggyConsumer {
    pub(crate) inner: Arc<Mutex<RustIggyConsumer>>,
    pub(crate) state: RustIggyConsumerState,
    pub(crate) name: String,
    pub(crate) stream: PyIdentifier,
    pub(crate) topic: PyIdentifier,
}

#[gen_stub_pymethods]
#[pymethods]
impl IggyConsumer {
    /// Get the last consumed offset for the given partition, or `None` while that partition
    /// is untracked. Polling starts tracking a partition at `0`, so `0` also means
    /// "seen, nothing consumed yet".
    #[gen_stub(override_return_type(type_repr = "builtins.int | None"))]
    fn get_last_consumed_offset(&self, partition_id: u32) -> Option<u64> {
        self.state.get_last_consumed_offset(partition_id)
    }

    /// Get the last stored offset for the given partition, or `None` while that partition is
    /// untracked. Polling starts tracking a partition at `0`, so `0` also means
    /// "seen, nothing stored yet", including under `AutoCommit.Disabled()`.
    #[gen_stub(override_return_type(type_repr = "builtins.int | None"))]
    fn get_last_stored_offset(&self, partition_id: u32) -> Option<u64> {
        self.state.get_last_stored_offset(partition_id)
    }

    /// Gets the name of the consumer group.
    fn name(&self) -> &str {
        &self.name
    }

    /// Gets the current partition id or `0` if no messages have been polled yet.
    fn partition_id(&self) -> u32 {
        self.state.partition_id()
    }

    /// Gets the identifier of the stream this consumer group is configured for.
    fn stream(&self) -> PyIdentifier {
        self.stream.clone()
    }

    /// Gets the identifier of the topic this consumer group is configured for.
    fn topic(&self) -> PyIdentifier {
        self.topic.clone()
    }

    /// Stores the provided offset for the provided partition id or if none is specified
    /// uses the current partition id for the consumer group.
    /// Raises `RuntimeError` if the operation fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn store_offset<'a>(
        &self,
        py: Python<'a>,
        offset: u64,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))] partition_id: Option<u32>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let state = self.state.clone();
        future_into_py(py, async move {
            state
                .store_offset(offset, partition_id)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
        })
    }

    /// Deletes the offset for the provided partition id or if none is specified
    /// uses the current partition id for the consumer group.
    /// Raises `RuntimeError` if the operation fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn delete_offset<'a>(
        &self,
        py: Python<'a>,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))] partition_id: Option<u32>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let state = self.state.clone();
        future_into_py(py, async move {
            state
                .delete_offset(partition_id)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
        })
    }

    /// Asynchronously iterate over `ReceiveMessage`s.
    /// Returns an async iterator that raises `StopAsyncIteration` when no more messages are available
    /// or a `RuntimeError` on failure.
    /// Note: This method does not currently support `AutoCommit.After`.
    /// For `AutoCommit.IntervalOrAfter(datetime.timedelta, AutoCommitAfter)`,
    /// only the interval part is applied; the `after` mode is ignored.
    /// Use `consume_messages()` if you need commit-after-processing semantics.
    #[gen_stub(override_return_type(type_repr="collections.abc.AsyncIterator[ReceiveMessage]", imports=("collections.abc")))]
    fn iter_messages(&self) -> ReceiveMessageIterator {
        let inner = self.inner.clone();
        ReceiveMessageIterator { inner }
    }

    /// Consumes messages continuously using a callback function and an optional `asyncio.Event` for signaling shutdown.
    /// Returns an awaitable that completes when shutdown is signaled or a RuntimeError on failure.
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
            let handle_shutdown = shutdown_event
                .map(|shutdown_event| -> PyResult<JoinHandle<PyResult<()>>> {
                    let task_locals =
                        Python::attach(pyo3_async_runtimes::tokio::get_current_locals)?;
                    Ok(get_runtime().spawn(scope(
                        task_locals,
                        wait_for_shutdown(shutdown_event, shutdown_tx),
                    )))
                })
                .transpose()?;

            let consume_result = handle_consume.await;

            if let Some(handle_shutdown) = handle_shutdown {
                // Consuming can also end on its own, and the shutdown task would then park on
                // `Event.wait()` forever.
                handle_shutdown.abort();
                match handle_shutdown.await {
                    Ok(shutdown_result) => shutdown_result?,
                    Err(error) if error.is_cancelled() => {}
                    Err(error) => {
                        return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                            error.to_string(),
                        ));
                    }
                }
            }

            consume_result
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))??
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(())
        })
    }
}

async fn wait_for_shutdown(shutdown_event: Py<PyAny>, shutdown_tx: Sender<()>) -> PyResult<()> {
    Python::attach(|py| into_future(shutdown_event.bind(py).as_any().call_method0("wait")?))?
        .await?;
    // A closed receiver only means consuming has already stopped, so there is nothing left
    // to signal and the result of the run is the one worth reporting.
    let _ = shutdown_tx.send(());
    Ok(())
}

/// The consumer polling the messages. It selects both the consumer kind and the
/// identifier the server keys the stored offset on.
#[derive(Clone)]
#[gen_stub_pyclass_complex_enum]
#[pyclass(from_py_object)]
pub enum Consumer {
    /// A regular consumer, owning its offset on the polled partition.
    Single { id: PyIdentifier },
    /// A member of the consumer group, sharing the group's offset.
    Group { id: PyIdentifier },
}

impl TryFrom<&Consumer> for RustConsumer {
    type Error = PyErr;

    fn try_from(value: &Consumer) -> PyResult<RustConsumer> {
        Ok(match value {
            Consumer::Single { id } => RustConsumer::new(Identifier::try_from(id)?),
            Consumer::Group { id } => RustConsumer::group(Identifier::try_from(id)?),
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
        let task_locals = self.task_locals.lock().await.clone();
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
// #[derive(Debug, PartialEq, Copy, Clone)]
#[gen_stub_pyclass_complex_enum]
#[pyclass]
pub enum AutoCommit {
    /// The auto-commit is disabled and the offset must be stored manually by the consumer.
    Disabled(),
    /// The auto-commit is enabled and the offset is stored on the server after a certain interval.
    Interval(Py<PyDelta>),
    /// The auto-commit is enabled and the offset is stored on the server after a certain interval or depending on the mode when consuming the messages.
    IntervalOrWhen(Py<PyDelta>, AutoCommitWhen),
    /// The auto-commit is enabled and the offset is stored on the server after a certain interval or depending on the mode after consuming the messages.
    IntervalOrAfter(Py<PyDelta>, AutoCommitAfter),
    /// The auto-commit is enabled and the offset is stored on the server depending on the mode when consuming the messages.
    When(AutoCommitWhen),
    /// The auto-commit is enabled and the offset is stored on the server depending on the mode after consuming the messages.
    After(AutoCommitAfter),
}

impl TryFrom<&AutoCommit> for RustAutoCommit {
    type Error = PyErr;

    fn try_from(val: &AutoCommit) -> PyResult<RustAutoCommit> {
        Ok(match val {
            AutoCommit::Disabled() => RustAutoCommit::Disabled,
            AutoCommit::Interval(delta) => RustAutoCommit::Interval(auto_commit_interval(delta)?),
            AutoCommit::IntervalOrWhen(delta, when) => {
                RustAutoCommit::IntervalOrWhen(auto_commit_interval(delta)?, when.into())
            }
            AutoCommit::IntervalOrAfter(delta, after) => {
                RustAutoCommit::IntervalOrAfter(auto_commit_interval(delta)?, after.into())
            }
            AutoCommit::When(when) => RustAutoCommit::When(when.into()),
            AutoCommit::After(after) => RustAutoCommit::After(after.into()),
        })
    }
}

fn auto_commit_interval(delta: &Py<PyDelta>) -> PyResult<IggyDuration> {
    reject_zero(py_delta_to_iggy_duration(delta)?, "AutoCommit interval")
}

/// The auto-commit mode for storing the offset on the server.
#[derive(Debug, PartialEq, Copy, Clone)]
#[gen_stub_pyclass_complex_enum(skip_stub_type)]
#[pyclass(from_py_object)]
pub enum AutoCommitWhen {
    /// The offset is stored on the server when the messages are received.
    PollingMessages(),
    /// The offset is stored on the server when all the messages are consumed.
    ConsumingAllMessages(),
    /// The offset is stored on the server when consuming each message.
    ConsumingEachMessage(),
    /// The offset is stored on the server when consuming every Nth message.
    ConsumingEveryNthMessage(u32),
}

impl From<&AutoCommitWhen> for RustAutoCommitWhen {
    fn from(val: &AutoCommitWhen) -> RustAutoCommitWhen {
        match val {
            AutoCommitWhen::PollingMessages() => RustAutoCommitWhen::PollingMessages,
            AutoCommitWhen::ConsumingAllMessages() => RustAutoCommitWhen::ConsumingAllMessages,
            AutoCommitWhen::ConsumingEachMessage() => RustAutoCommitWhen::ConsumingEachMessage,
            AutoCommitWhen::ConsumingEveryNthMessage(n) => {
                RustAutoCommitWhen::ConsumingEveryNthMessage(n.to_owned())
            }
        }
    }
}

impl PyStubType for AutoCommitWhen {
    fn type_output() -> TypeInfo {
        TypeInfo::unqualified("AutoCommitWhen")
    }
}

/// The auto-commit mode for storing the offset on the server **after** receiving the messages.
#[derive(Debug, PartialEq, Copy, Clone)]
#[gen_stub_pyclass_complex_enum(skip_stub_type)]
#[pyclass(from_py_object)]
#[allow(clippy::enum_variant_names)]
pub enum AutoCommitAfter {
    /// The offset is stored on the server after all the messages are consumed.
    ConsumingAllMessages(),
    /// The offset is stored on the server after consuming each message.
    ConsumingEachMessage(),
    /// The offset is stored on the server after consuming every Nth message.
    ConsumingEveryNthMessage(u32),
}

impl From<&AutoCommitAfter> for RustAutoCommitAfter {
    fn from(val: &AutoCommitAfter) -> RustAutoCommitAfter {
        match val {
            AutoCommitAfter::ConsumingAllMessages() => RustAutoCommitAfter::ConsumingAllMessages,
            AutoCommitAfter::ConsumingEachMessage() => RustAutoCommitAfter::ConsumingEachMessage,
            AutoCommitAfter::ConsumingEveryNthMessage(n) => {
                RustAutoCommitAfter::ConsumingEveryNthMessage(n.to_owned())
            }
        }
    }
}

impl PyStubType for AutoCommitAfter {
    fn type_output() -> TypeInfo {
        TypeInfo::unqualified("AutoCommitAfter")
    }
}
