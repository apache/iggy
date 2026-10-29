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
use std::sync::atomic::{AtomicBool, Ordering};

use futures::StreamExt;
use iggy::consumer_ext::{IggyConsumerMessageExt, MessageConsumer};
use iggy::prelude::{
    ClientCommitPolicy as RustClientCommitPolicy, CommitEvent as RustCommitEvent,
    Consumer as RustConsumer, ConsumerGroup as RustConsumerGroup,
    ConsumerGroupDetails as RustConsumerGroupDetails,
    ConsumerGroupMember as RustConsumerGroupMember, Identifier, IggyConsumer as RustIggyConsumer,
    IggyConsumerState as RustIggyConsumerState, IggyError, ReadError as RustReadError,
    ReceivedMessage,
};
use pyo3::exceptions::{PyRuntimeError, PyStopAsyncIteration};
use pyo3::intern;
use pyo3::types::PyDelta;

use pyo3::prelude::*;
use pyo3_async_runtimes::TaskLocals;
use pyo3_async_runtimes::tokio::{future_into_py, get_runtime, into_future, scope};
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyclass_complex_enum, gen_stub_pymethods};
use tokio::sync::Mutex;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;

use crate::client::{to_read_error, to_runtime_error};
use crate::duration::{iggy_duration_to_py_delta, py_delta_to_iggy_duration, reject_zero};
use crate::event_loop::on_event_loop;
use crate::identifier::PyIdentifier;
use crate::receive_message::{Checkpoint, PollingStrategy, ReceiveMessage};

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
    /// Set while `consume_messages()` holds `inner`, so a `seek()` from its callback fails
    /// instead of waiting for the run that waits for the callback.
    pub(crate) consuming: Arc<AtomicBool>,
}

#[gen_stub_pymethods]
#[pymethods]
impl IggyConsumer {
    /// Get the last offset handed over for this partition, or `None` until a message
    /// from the partition has been consumed.
    #[gen_stub(override_return_type(type_repr = "builtins.int | None"))]
    fn get_last_consumed_offset(&self, partition_id: u32) -> Option<u64> {
        self.state.get_last_consumed_offset(partition_id)
    }

    /// Get the last offset successfully stored for this partition, or `None` until
    /// this consumer has stored one. The default `ClientCommitPolicy()` leaves it unset
    /// until a commit. Under `ServerCommitPolicy.OnRead` it is the end of the last batch
    /// that the consumer took from its reader.
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
    /// It always sends, also an offset below the stored one. The offset is stored under the
    /// context of the latest message consumed from the partition, or without a context when
    /// nothing was consumed from it. To commit a message held across a newer read or an
    /// ownership change, pass `message.checkpoint()` to `commit()` instead.
    /// Raises `IggyError` if the operation fails, for example `HistoryUnavailable` (87)
    /// after the partition was deleted and created again.
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
                .map_err(to_runtime_error)
        })
    }

    /// Stores the offset of a checkpoint that this consumer yielded, taken from
    /// `ReceiveMessage.checkpoint()`, which covers every earlier message of its partition.
    /// The write keeps the partition history that delivered the message, so it fails with
    /// `HistoryUnavailable` (87) after the partition was deleted and created again, and
    /// with `ConsumerGroupPartitionNotOwned` (5009) after its consumer group owner
    /// changed, instead of committing the offset to the new partition.
    /// A checkpoint of an older generation of its partition, after `seek()` or a new
    /// assignment, or at or below the last commit of its generation, stores nothing.
    /// Raises `IggyError` with `InvalidCommand` (3) under `ServerCommitPolicy.OnRead` and for
    /// a checkpoint that this consumer did not yield, or if the operation fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn commit<'a>(&self, py: Python<'a>, checkpoint: &Checkpoint) -> PyResult<Bound<'a, PyAny>> {
        let state = self.state.clone();
        let checkpoint = checkpoint.inner;
        future_into_py(py, async move {
            state.commit(checkpoint).await.map_err(to_runtime_error)
        })
    }

    /// Sets the next read position of a partition and drops the rest of its batch that the
    /// consumer yields from. It stores no offset, and it resumes a stopped partition.
    /// `start` is a new strategy or a continuation of the same partition, such as
    /// `ReadError.skip`. After `HistoryUnavailable` (87), the partition reads nothing until
    /// this call picks a start in its new history.
    /// Raises `IggyError` with `InvalidCommand` (3) for a partition that the consumer does not
    /// read and for a continuation of another partition, `PendingOperationError` under
    /// `ServerCommitPolicy.OnRead` while a read of the partition has an unknown outcome
    /// (abandon its operation first), and `RuntimeError` while `consume_messages()` runs, also
    /// from its callback: seek after the run ends.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn seek<'a>(
        &self,
        py: Python<'a>,
        partition_id: u32,
        start: &PollingStrategy,
    ) -> PyResult<Bound<'a, PyAny>> {
        if self.consuming.load(Ordering::SeqCst) {
            return Err(PyRuntimeError::new_err(
                "seek() cannot run while consume_messages() runs",
            ));
        }
        let inner = self.inner.clone();
        let start = start.inner;
        future_into_py(py, async move {
            inner
                .lock()
                .await
                .seek(partition_id, start)
                .await
                .map_err(to_runtime_error)
        })
    }

    /// Deletes the offset for the provided partition id or if none is specified
    /// uses the current partition id for the consumer group.
    /// Raises `IggyError` if the operation fails, for example `ConsumerOffsetNotFound`
    /// when nothing is stored for the partition.
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
                .map_err(to_runtime_error)
        })
    }

    /// Shuts the consumer down. It stores the last yielded position of each partition under a
    /// `ClientCommitPolicy` other than the default, closes the reader and leaves the consumer
    /// group. With `ServerCommitPolicy.OnRead`, it moves the offset back to the last yielded
    /// message, which a dropped consumer does not do.
    /// It waits for a running `consume_messages()` to end, and iterating ends after it.
    /// Repeated calls are safe.
    /// Raises `IggyError` for the first failed store of the `OnRead` close, or else
    /// `PendingOperationError` for a partition with an unknown outcome: abandon its operation
    /// before another consumer reads the partition.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn shutdown<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner
                .lock()
                .await
                .shutdown()
                .await
                .map_err(to_runtime_error)
        })
    }

    /// Asynchronously iterate over `ReceiveMessage`s.
    /// Returns an async iterator that raises `StopAsyncIteration` when no more messages are
    /// available or a `ReadError` on a failed read. The consumer stays usable after it and
    /// retries by itself.
    /// The `on` event of a `ClientCommitPolicy` counts a message when it is yielded. Use
    /// `consume_messages()` to count it after its processing.
    /// After another client deleted and created a partition again, one iteration raises
    /// `ReadError` with `HistoryUnavailable` (87), and the partition reads nothing until
    /// `seek()` picks a start in its new history.
    /// A cancelled iteration leaves its message for the next one.
    #[gen_stub(override_return_type(type_repr="collections.abc.AsyncIterator[ReceiveMessage]", imports=("collections.abc")))]
    fn iter_messages(&self) -> ReceiveMessageIterator {
        let inner = self.inner.clone();
        ReceiveMessageIterator { inner }
    }

    /// Consumes messages continuously using a callback function and an optional `asyncio.Event` for signaling shutdown.
    /// Returns an awaitable that completes when shutdown is signaled. Without an event, a cancel of
    /// the awaitable ends the run once the running callback returns.
    /// The `on` event of a `ClientCommitPolicy` counts a message after its callback returns,
    /// also after a failure. Its interval can store a message whose callback still runs.
    /// A failed read is logged, and the consumer retries by itself. The run raises `ReadError`
    /// if the client cannot go on without the caller, or if a read stops its partition until
    /// `seek()`: `HistoryUnavailable` (87), or an unknown outcome under
    /// `ServerCommitPolicy.OnRead`. For a stopped partition, `ReadError.partition_id` names it.
    /// To resume it, abandon the operation of an unknown outcome, seek the partition and consume
    /// again.
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
        let consuming = self.consuming.clone();
        let callback: Py<PyAny> = callback.unbind();
        let shutdown_event: Option<Py<PyAny>> = shutdown_event.map(|e| e.unbind());

        future_into_py(py, async {
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

            let task_locals = Python::attach(pyo3_async_runtimes::tokio::get_current_locals)?;
            let handle_consume: JoinHandle<PyResult<Result<(), RustReadError>>> = get_runtime()
                .spawn(scope(task_locals, async move {
                    let task_locals =
                        Python::attach(pyo3_async_runtimes::tokio::get_current_locals)?;
                    let consumer = PyCallbackConsumer {
                        callback: Arc::new(callback),
                        task_locals: Arc::new(Mutex::new(task_locals)),
                    };
                    let mut inner = inner.lock().await;
                    let _consuming = Consuming::start(consuming);
                    Ok(inner.consume_messages(&consumer, shutdown_rx).await)
                }));
            // A dropped sender ends the run, so without an event this future keeps the sender
            // until the run ends or the awaitable is cancelled.
            let handle_shutdown = match shutdown_event {
                Some(shutdown_event) => {
                    let task_locals =
                        Python::attach(pyo3_async_runtimes::tokio::get_current_locals)?;
                    Some(get_runtime().spawn(scope(
                        task_locals,
                        wait_for_shutdown(shutdown_event, shutdown_tx),
                    )))
                }
                None => None,
            };

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
                .map_err(to_read_error)?;
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

/// Marks a `consume_messages()` run while it holds the consumer. The drop also clears the mark
/// of a run that panics.
struct Consuming(Arc<AtomicBool>);

impl Consuming {
    fn start(consuming: Arc<AtomicBool>) -> Self {
        consuming.store(true, Ordering::SeqCst);
        Self(consuming)
    }
}

impl Drop for Consuming {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
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
    // pyo3 builds no slot from an `async fn`, so the slot returns the coroutine of `_next`.
    pub fn __anext__<'a>(slf: &Bound<'a, Self>) -> PyResult<Bound<'a, PyAny>> {
        slf.call_method0(intern!(slf.py(), "_next"))
    }

    #[pyo3(name = "_next")]
    async fn next_message(&self) -> PyResult<ReceiveMessage> {
        on_event_loop(async {
            let mut inner = self.inner.lock().await;
            match inner.next().await {
                Some(message) => message.map(ReceiveMessage::from).map_err(to_read_error),
                None => Err(PyStopAsyncIteration::new_err("No more messages")),
            }
        })
        .await
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
        let message = ReceiveMessage::from(received);
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

/// When an `IggyConsumer` stores the position of the last message that it yielded. It never
/// stores the end of a prefetched batch. The default stores nothing: commit checkpoints with
/// `IggyConsumer.commit()`.
#[gen_stub_pyclass]
#[pyclass(frozen)]
pub struct ClientCommitPolicy {
    pub(crate) inner: RustClientCommitPolicy,
}

#[gen_stub_pymethods]
#[pymethods]
impl ClientCommitPolicy {
    /// Constructs a client commit policy.
    ///
    /// Args:
    ///     interval: Store the latest position at this interval. Raises `ValueError` if it is
    ///         negative or zero.
    ///     on: Also store it on this event.
    #[new]
    #[pyo3(signature = (*, interval=None, on=None))]
    fn new(
        #[gen_stub(override_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
        interval: Option<Py<PyDelta>>,
        #[gen_stub(override_type(type_repr = "CommitEvent | None"))] on: Option<&CommitEvent>,
    ) -> PyResult<Self> {
        let interval = interval
            .as_ref()
            .map(|interval| reject_zero(py_delta_to_iggy_duration(interval)?, "interval"))
            .transpose()?;
        Ok(Self {
            inner: RustClientCommitPolicy {
                interval,
                on: on.map(RustCommitEvent::from),
            },
        })
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
    #[gen_stub(override_return_type(type_repr = "CommitEvent | None"))]
    #[getter]
    fn on(&self) -> Option<CommitEvent> {
        self.inner.on.map(CommitEvent::from)
    }
}

/// The event of a `ClientCommitPolicy`.
#[derive(Clone, Copy)]
#[gen_stub_pyclass_complex_enum]
#[pyclass(from_py_object)]
pub enum CommitEvent {
    /// Each message.
    EachMessage(),
    /// A message whose offset is a multiple of `n`. The consumer refuses zero.
    EveryNthMessage(u32),
    /// The last message of a batch.
    EachBatch(),
}

impl From<&CommitEvent> for RustCommitEvent {
    fn from(event: &CommitEvent) -> Self {
        match event {
            CommitEvent::EachMessage() => Self::EachMessage,
            CommitEvent::EveryNthMessage(n) => Self::EveryNthMessage(*n),
            CommitEvent::EachBatch() => Self::EachBatch,
        }
    }
}

impl From<RustCommitEvent> for CommitEvent {
    fn from(event: RustCommitEvent) -> Self {
        match event {
            RustCommitEvent::EachMessage => Self::EachMessage(),
            RustCommitEvent::EveryNthMessage(n) => Self::EveryNthMessage(n),
            RustCommitEvent::EachBatch => Self::EachBatch(),
        }
    }
}
