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

use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex, PoisonError};

use futures::StreamExt;
use futures::future::{Either, select};
use iggy::consumer_ext::{IggyConsumerMessageExt, MessageConsumer};
use iggy::prelude::{
    Consumer as RustConsumer, ConsumerGroup as RustConsumerGroup,
    ConsumerGroupDetails as RustConsumerGroupDetails,
    ConsumerGroupMember as RustConsumerGroupMember, Identifier, IggyConsumer as RustIggyConsumer,
    IggyConsumerState as RustIggyConsumerState, IggyError, ReceivedMessage,
};
use pyo3::exceptions::{PyRuntimeError, PyStopAsyncIteration};
use pyo3::intern;

use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3_async_runtimes::tokio::{
    future_into_py, get_current_locals, get_runtime, into_future, scope,
};
use pyo3_async_runtimes::{TaskLocals, into_future_with_locals};
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyclass_complex_enum, gen_stub_pymethods};
use tokio::sync::{Mutex, oneshot, watch};

use crate::client::{to_read_error, to_runtime_error};
use crate::event_loop::on_event_loop;
use crate::identifier::PyIdentifier;
use crate::poll_options::CommitPolicy;
use crate::receive_message::{Checkpoint, PollingStrategy, ReceiveMessage};

/// Names the consumer whose `consume_messages()` callback runs in the current context, so a call
/// that would wait for the run that waits for the callback raises instead.
static CONSUMING_CALLBACK: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

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
    /// Set by `shutdown()` before it waits for `inner`, which a parked iteration holds.
    pub(crate) stopping: Arc<watch::Sender<bool>>,
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
    /// this consumer has stored one. `CommitPolicy.Manual()`, the default, leaves it unset
    /// until a commit. Under `CommitPolicy.OnRead()` it is the end of the last batch that the
    /// consumer took from its reader.
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
    /// Raises `IggyError` with `InvalidCommand` (3) under `CommitPolicy.OnRead()` and for a
    /// checkpoint that this consumer did not yield, or if the operation fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn commit<'a>(&self, py: Python<'a>, checkpoint: &Checkpoint) -> PyResult<Bound<'a, PyAny>> {
        let state = self.state.clone();
        let checkpoint = checkpoint.inner;
        future_into_py(py, async move {
            state.commit(checkpoint).await.map_err(to_runtime_error)
        })
    }

    /// Counts a checkpoint that this consumer yielded under `CommitPolicy.Auto` and stores
    /// nothing now: the policy stores the latest counted position of its partition. With
    /// `CommitAfter.Marked` only marks count, so messages processed concurrently count once
    /// they are done. It works also in a `consume_messages()` callback.
    /// Raises `IggyError` with `InvalidCommand` (3) without `CommitPolicy.Auto` and for a
    /// checkpoint that this consumer did not yield.
    fn mark(&self, checkpoint: &Checkpoint) -> PyResult<()> {
        self.state.mark(checkpoint.inner).map_err(to_runtime_error)
    }

    /// When the offsets of this consumer are stored, from its `PollOptions`.
    #[getter]
    fn commit_policy(&self) -> CommitPolicy {
        self.state.commit_policy().into()
    }

    /// Sets the next read position of a partition and drops the rest of its batch that the
    /// consumer yields from. With `CommitPolicy.OnRead()`, a seek to an offset above 0 first
    /// stores the offset before it, so a restart resumes at the new position. Every other seek
    /// stores nothing. It resumes a stopped partition.
    /// `start` is a new strategy or a continuation of the same partition, such as
    /// `ReadError.skip`. After `HistoryUnavailable` (87), the partition reads nothing until
    /// this call picks a start in its new history.
    /// Raises `IggyError` with `InvalidCommand` (3) for a partition that the consumer does not
    /// read and for a continuation of another partition, `PendingOperationError` under
    /// `CommitPolicy.OnRead()` while a read of the partition has an unknown outcome
    /// (abandon its operation first), the errors of the offset store with `OnRead`, and
    /// `RuntimeError` while `consume_messages()` runs, also from its callback: seek after the
    /// run ends.
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

    /// Shuts the consumer down. Under `CommitPolicy.Auto` it stores the latest counted position
    /// of each partition first, then it closes the reader and leaves the consumer group. With
    /// `CommitPolicy.OnRead()`, it moves the offset back to the last yielded message, which a
    /// dropped consumer does not do.
    /// It ends a parked iteration at once, which then raises `StopAsyncIteration`, and it waits
    /// for a running `consume_messages()` to end. Repeated calls are safe.
    /// Raises `IggyError` for the first failed store of the `OnRead` close, or else
    /// `PendingOperationError` for a partition with an unknown outcome: abandon its operation
    /// before another consumer reads the partition. Raises `RuntimeError` in a
    /// `consume_messages()` callback of this consumer, because the run waits for the callback:
    /// set the shutdown event or cancel the run instead.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn shutdown<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        future_into_py(py, self.shutting_down(py)?)
    }

    #[gen_stub(override_return_type(type_repr = "collections.abc.Awaitable[IggyConsumer]", imports=("collections.abc")))]
    fn __aenter__<'py>(this: Py<Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move { Ok(this) })
    }

    /// Shuts the consumer down as `shutdown()` does, also when the block raised.
    #[gen_stub(override_return_type(type_repr = "collections.abc.Awaitable[builtins.bool]", imports=("collections.abc")))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: &Bound<'_, PyAny>,
        _exc_value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let shutting_down = self.shutting_down(py)?;
        future_into_py(py, async move {
            shutting_down.await?;
            Ok(false)
        })
    }

    /// Asynchronously iterate over `ReceiveMessage`s.
    /// Returns an async iterator that raises `StopAsyncIteration` when no more messages are
    /// available or a `ReadError` on a failed read. The consumer stays usable after it and
    /// retries by itself.
    /// Under `CommitPolicy.Auto`, a message counts when it is yielded with
    /// `CommitAfter.Delivery`, and when the next iteration starts with `CommitAfter.Processing`.
    /// After another client deleted and created a partition again, one iteration raises
    /// `ReadError` with `HistoryUnavailable` (87), and the partition reads nothing until
    /// `seek()` picks a start in its new history.
    /// A cancelled iteration leaves its message for the next one. Iterating in a
    /// `consume_messages()` callback of this consumer raises `RuntimeError`.
    #[gen_stub(override_return_type(type_repr="collections.abc.AsyncIterator[ReceiveMessage]", imports=("collections.abc")))]
    fn iter_messages(&self) -> ReceiveMessageIterator {
        ReceiveMessageIterator {
            inner: self.inner.clone(),
            stopping: self.stopping.clone(),
            owner: self.owner(),
        }
    }

    /// Consumes messages continuously using a callback function and an optional `asyncio.Event` for signaling shutdown.
    /// Returns an awaitable that completes when shutdown is signaled. A cancel of the awaitable
    /// ends the run once the running callback returns, with or without an event.
    /// Under `CommitPolicy.Auto`, a message counts before its callback with
    /// `CommitAfter.Delivery`, and after its callback returns with `CommitAfter.Processing`.
    /// With `CommitAfter.Processing`, a callback that raises ends the run with its exception,
    /// and its message stays uncounted, so the next run reads it again. Otherwise a callback
    /// that raises is logged and the run goes on.
    /// A failed read is logged, and the consumer retries by itself. The run raises `ReadError`
    /// if the client cannot go on without the caller, or if a read stops its partition until
    /// `seek()`: `HistoryUnavailable` (87), or an unknown outcome under
    /// `CommitPolicy.OnRead()`. For a stopped partition, `ReadError.partition_id` names it.
    /// To resume it, abandon the operation of an unknown outcome, seek the partition and consume
    /// again.
    /// Raises `RuntimeError` in a callback of a run of this consumer, because the run waits for
    /// the callback.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn consume_messages<'a>(
        &self,
        py: Python<'a>,
        #[gen_stub(override_type(type_repr="collections.abc.Callable[[ReceiveMessage], collections.abc.Awaitable[None]]", imports=("collections.abc")))]
        callback: Bound<'a, PyAny>,
        #[gen_stub(override_type(type_repr="asyncio.Event | None", imports=("asyncio")))]
        shutdown_event: Option<Bound<'a, PyAny>>,
    ) -> PyResult<Bound<'a, PyAny>> {
        refuse_in_callback(py, self.owner(), "consume_messages()")?;
        let inner = self.inner.clone();
        let consuming = self.consuming.clone();
        let locals = get_current_locals(py)?;
        let callback_context = locals.context(py).call_method0("copy")?;
        callback_context.call_method1(
            "run",
            (consuming_callback(py)?.getattr("set")?, self.owner()),
        )?;
        let failure = Arc::new(StdMutex::new(None));
        let consumer = PyCallbackConsumer {
            callback: Arc::new(callback.unbind()),
            task_locals: Arc::new(Mutex::new(locals.clone().with_context(callback_context))),
            failure: failure.clone(),
        };
        let shutdown_requested = shutdown_event
            .map(|event| into_future_with_locals(&locals, event.call_method0("wait")?))
            .transpose()?;

        future_into_py(py, async move {
            // A dropped sender ends the run, so dropping this future, as a cancel does, ends it
            // too.
            let (stop, stopped) = oneshot::channel::<()>();
            let run = get_runtime().spawn(async move {
                let mut inner = inner.lock().await;
                let _consuming = Consuming::start(consuming);
                inner.consume_messages(&consumer, stopped).await
            });
            let outcome = match shutdown_requested {
                None => run.await,
                Some(shutdown_requested) => match select(run, pin!(shutdown_requested)).await {
                    Either::Left((outcome, _)) => outcome,
                    Either::Right((requested, run)) => {
                        // A closed receiver only means consuming has already stopped, so the
                        // result of the run is the one worth reporting.
                        let _ = stop.send(());
                        let outcome = run.await;
                        requested?;
                        outcome
                    }
                },
            };
            match outcome.map_err(|error| PyRuntimeError::new_err(error.to_string()))? {
                Ok(()) => Ok(()),
                // A run that a callback ended raises the exception of that callback.
                Err(error) if error.error == IggyError::CannotReadMessage => {
                    match failure
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .take()
                    {
                        Some(callback_error) => Err(callback_error),
                        None => Err(to_read_error(error)),
                    }
                }
                Err(error) => Err(to_read_error(error)),
            }
        })
    }
}

impl IggyConsumer {
    /// Identifies this consumer to the guard of its callbacks.
    fn owner(&self) -> usize {
        Arc::as_ptr(&self.consuming) as usize
    }

    /// Ends a parked iteration and returns the shutdown, which waits for the consumer.
    fn shutting_down(
        &self,
        py: Python<'_>,
    ) -> PyResult<impl Future<Output = PyResult<()>> + use<>> {
        refuse_in_callback(py, self.owner(), "shutdown()")?;
        self.stopping.send_replace(true);
        let inner = self.inner.clone();
        Ok(async move {
            inner
                .lock()
                .await
                .shutdown()
                .await
                .map_err(to_runtime_error)
        })
    }
}

/// The context variable that names the consumer whose callback runs.
fn consuming_callback(py: Python<'_>) -> PyResult<&Bound<'_, PyAny>> {
    CONSUMING_CALLBACK
        .get_or_try_init(py, || {
            Ok::<_, PyErr>(
                py.import("contextvars")?
                    .getattr("ContextVar")?
                    .call1(("apache_iggy_consuming_callback",))?
                    .unbind(),
            )
        })
        .map(|variable| variable.bind(py))
}

/// Refuses `call` in a `consume_messages()` callback of the consumer `owner`, which would wait
/// for the run that waits for the callback.
fn refuse_in_callback(py: Python<'_>, owner: usize, call: &str) -> PyResult<()> {
    let running = consuming_callback(py)?
        .call_method1("get", (py.None(),))?
        .extract::<Option<usize>>()?;
    if running == Some(owner) {
        return Err(PyRuntimeError::new_err(format!(
            "{call} cannot run in a consume_messages() callback"
        )));
    }
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

/// The offset that a consumer stored on a partition.
#[gen_stub_pyclass]
#[pyclass(frozen)]
pub struct ConsumerOffsetInfo {
    pub(crate) partition_id: u32,
    pub(crate) current_offset: u64,
    pub(crate) stored_offset: u64,
}

#[gen_stub_pymethods]
#[pymethods]
impl ConsumerOffsetInfo {
    /// The partition of the offset.
    #[getter]
    fn partition_id(&self) -> u32 {
        self.partition_id
    }

    /// The offset of the last message of the partition when the offset was read.
    #[getter]
    fn current_offset(&self) -> u64 {
        self.current_offset
    }

    /// The stored offset.
    #[getter]
    fn stored_offset(&self) -> u64 {
        self.stored_offset
    }

    fn __repr__(&self) -> String {
        format!(
            "ConsumerOffsetInfo(partition_id={}, current_offset={}, stored_offset={})",
            self.partition_id, self.current_offset, self.stored_offset
        )
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
    pub(crate) stopping: Arc<watch::Sender<bool>>,
    pub(crate) owner: usize,
}

#[pymethods]
impl ReceiveMessageIterator {
    // pyo3 builds no slot from an `async fn`, so the slot returns the coroutine of `_next`.
    pub fn __anext__<'a>(slf: &Bound<'a, Self>) -> PyResult<Bound<'a, PyAny>> {
        refuse_in_callback(slf.py(), slf.borrow().owner, "Iterating the consumer")?;
        slf.call_method0(intern!(slf.py(), "_next"))
    }

    #[pyo3(name = "_next")]
    async fn next_message(&self) -> PyResult<ReceiveMessage> {
        let mut stopping = self.stopping.subscribe();
        on_event_loop(async {
            let next = async {
                let mut inner = self.inner.lock().await;
                inner.next().await
            };
            // The iteration holds the consumer while it waits, so a shutdown ends it first.
            let stopped = stopping.wait_for(|stopping| *stopping);
            match select(pin!(next), pin!(stopped)).await {
                Either::Left((Some(message), _)) => {
                    message.map(ReceiveMessage::from).map_err(to_read_error)
                }
                Either::Left((None, _)) | Either::Right(_) => {
                    Err(PyStopAsyncIteration::new_err("No more messages"))
                }
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
    /// The exception of the last callback, so a run that it ends raises it.
    failure: Arc<StdMutex<Option<PyErr>>>,
}

impl MessageConsumer for PyCallbackConsumer {
    async fn consume(&self, received: ReceivedMessage) -> Result<(), IggyError> {
        let callback = self.callback.clone();
        let task_locals = self.task_locals.lock().await.clone();
        let message = ReceiveMessage::from(received);
        let called = get_runtime()
            .spawn(scope(task_locals, async move {
                Python::attach(|py| {
                    let callback = callback.bind(py);
                    let result = callback.as_any().call1((message,))?;
                    into_future(result)
                })?
                .await
            }))
            .await
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))
            .and_then(|called| called);
        let mut failure = self.failure.lock().unwrap_or_else(PoisonError::into_inner);
        match called {
            Ok(_) => {
                *failure = None;
                Ok(())
            }
            Err(error) => {
                *failure = Some(error);
                Err(IggyError::CannotReadMessage)
            }
        }
    }
}
