/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use std::sync::Arc;
use std::time::Duration;

use iggy::consumer_ext::{IggyConsumerMessageExt, MessageConsumer};
use iggy::prelude::{
    AutoCommit as RustAutoCommit, AutoCommitAfter as RustAutoCommitAfter,
    AutoCommitWhen as RustAutoCommitWhen, *,
};
use iggy::prelude::{IggyConsumer as RustIggyConsumer, IggyError, ReceivedMessage};
use pyo3::types::{PyDelta, PyDeltaAccess};

use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::{future_into_py, get_runtime, into_future, scope};
use pyo3_async_runtimes::TaskLocals;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyclass_complex_enum, gen_stub_pymethods};
use tokio::sync::oneshot::Sender;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::identifier::PyIdentifier;
use crate::receive_message::ReceiveMessage;

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
    /// Get the last consumed offset or `None` if no offset has been consumed yet.
    fn get_last_consumed_offset(&self, partition_id: u32) -> Option<u64> {
        self.inner
            .blocking_lock()
            .get_last_consumed_offset(partition_id)
    }

    /// Get the last stored offset or `None` if no offset has been stored yet.
    fn get_last_stored_offset(&self, partition_id: u32) -> Option<u64> {
        self.inner
            .blocking_lock()
            .get_last_stored_offset(partition_id)
    }

    /// Gets the name of the consumer group.
    fn name(&self) -> String {
        self.inner.blocking_lock().name().to_string()
    }

    /// Gets the current partition id or `0` if no messages have been polled yet.
    fn partition_id(&self) -> u32 {
        self.inner.blocking_lock().partition_id()
    }

    /// Gets the name of the stream this consumer group is configured for.
    fn stream(&self) -> PyIdentifier {
        self.inner.blocking_lock().stream().into()
    }

    /// Gets the name of the topic this consumer group is configured for.
    fn topic(&self) -> PyIdentifier {
        self.inner.blocking_lock().topic().into()
    }

    /// Stores the provided offset for the provided partition id or if none is specified
    /// uses the current partition id for the consumer group.
    ///
    /// Returns `Ok(())` if the server responds successfully, or a `PyRuntimeError`
    /// if the operation fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn store_offset<'a>(
        &self,
        py: Python<'a>,
        offset: u64,
        partition_id: Option<u32>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner
                .lock()
                .await
                .store_offset(offset, partition_id)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e:?}")))
        })
    }

    /// Deletes the offset for the provided partition id or if none is specified
    /// uses the current partition id for the consumer group.
    ///
    /// Returns `Ok(())` if the server responds successfully, or a `PyRuntimeError`
    /// if the operation fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn delete_offset<'a>(
        &self,
        py: Python<'a>,
        partition_id: Option<u32>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner
                .lock()
                .await
                .delete_offset(partition_id)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e:?}")))
        })
    }

    /// Consumes messages continuously using a callback function and an optional `asyncio.Event` for signaling shutdown.
    ///
    /// Returns an awaitable that completes when shutdown is signaled or a PyRuntimeError on failure.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn consume_messages<'a>(
        &self,
        py: Python<'a>,
        #[gen_stub(override_type(type_repr="collections.abc.Callable[[str]]", imports=("collections.abc")))]
        callback: Bound<'a, PyAny>,
        #[gen_stub(override_type(type_repr="typing.Optional[asyncio.Event]", imports=("asyncio")))]
        shutdown_event: Option<Bound<'a, PyAny>>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let inner = self.inner.clone();
        let callback: Py<PyAny> = callback.unbind();
        let shutdown_event: Option<Py<PyAny>> = shutdown_event.map(|e| e.unbind());

        future_into_py(py, async {
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

            let inner_init = inner.clone();
            let mut inner_init = inner_init.lock().await;
            inner_init
                .init()
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e:?}")))?;
            drop(inner_init);

            let task_locals = Python::with_gil(pyo3_async_runtimes::tokio::get_current_locals)?;
            let handle_consume = get_runtime().spawn(scope(task_locals, async move {
                let task_locals =
                    Python::with_gil(pyo3_async_runtimes::tokio::get_current_locals).unwrap();
                let consumer = PyCallbackConsumer {
                    callback: Arc::new(callback),
                    task_locals: Arc::new(Mutex::new(task_locals)),
                };
                let mut inner = inner.lock().await;
                inner.consume_messages(&consumer, shutdown_rx).await
            }));
            let consume_result;

            if let Some(shutdown_event) = shutdown_event {
                let task_locals = Python::with_gil(pyo3_async_runtimes::tokio::get_current_locals)?;
                async fn shutdown_impl(
                    shutdown_event: Py<PyAny>,
                    shutdown_tx: Sender<()>,
                ) -> PyResult<()> {
                    Python::with_gil(|py| {
                        into_future(
                            shutdown_event
                                .bind(py)
                                .as_any()
                                .call_method0("wait")
                                .unwrap(),
                        )
                    })?
                    .await?;
                    shutdown_tx.send(()).map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e:?}"))
                    })?;
                    Ok(())
                }
                let handle_shutdown: JoinHandle<Result<(), PyErr>> = get_runtime().spawn(scope(
                    task_locals,
                    shutdown_impl(shutdown_event, shutdown_tx),
                ));
                let shutdown_result;
                (consume_result, shutdown_result) = tokio::join!(handle_consume, handle_shutdown);
                shutdown_result.unwrap()?;
            } else {
                consume_result = handle_consume.await;
            }

            consume_result
                .unwrap()
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e:?}")))?;
            Ok(())
        })
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
        let task_locals = Python::with_gil(|py| task_locals.clone_ref(py));
        let message = ReceiveMessage::from_rust_message(received.message);
        get_runtime()
            .spawn(scope(task_locals, async move {
                Python::with_gil(|py| {
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

impl From<&AutoCommit> for RustAutoCommit {
    fn from(val: &AutoCommit) -> RustAutoCommit {
        match val {
            AutoCommit::Disabled() => RustAutoCommit::Disabled,
            AutoCommit::Interval(delta) => {
                let duration = py_delta_to_iggy_duration(delta);
                RustAutoCommit::Interval(duration)
            }
            AutoCommit::IntervalOrWhen(delta, when) => {
                let duration = py_delta_to_iggy_duration(delta);
                RustAutoCommit::IntervalOrWhen(duration, when.into())
            }
            AutoCommit::IntervalOrAfter(delta, after) => {
                let duration = py_delta_to_iggy_duration(delta);
                RustAutoCommit::IntervalOrAfter(duration, after.into())
            }
            AutoCommit::When(when) => RustAutoCommit::When(when.into()),
            AutoCommit::After(after) => RustAutoCommit::After(after.into()),
        }
    }
}

/// The auto-commit mode for storing the offset on the server.
#[derive(Debug, PartialEq, Copy, Clone)]
#[gen_stub_pyclass_complex_enum]
#[pyclass]
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

/// The auto-commit mode for storing the offset on the server **after** receiving the messages.
#[derive(Debug, PartialEq, Copy, Clone)]
#[gen_stub_pyclass_complex_enum]
#[pyclass]
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

pub fn py_delta_to_iggy_duration(delta1: &Py<PyDelta>) -> IggyDuration {
    Python::with_gil(|py| {
        let delta = delta1.bind(py);
        let seconds = (delta.get_days() * 60 * 60 * 24 + delta.get_seconds()) as u64;
        let nanos = (delta.get_microseconds() * 1_000) as u32;
        IggyDuration::new(Duration::new(seconds, nanos))
    })
}
