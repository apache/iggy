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

use std::{str::FromStr, sync::Arc};

#[cfg(test)]
use iggy::clients::producer_config::BackpressureMode as RustBackpressureMode;
#[cfg(test)]
use iggy::prelude::BackgroundConfig as RustBackgroundConfig;
use iggy::prelude::{
    DirectConfig as RustDirectConfig, Identifier, IggyByteSize, IggyDuration, IggyError,
    IggyMessage as RustIggyMessage, IggyProducer as RustIggyProducer,
    SendMessagesConfirmationResponse as RustSendMessagesConfirmationResponse,
};
use pyo3::IntoPyObjectExt;
use pyo3::conversion::FromPyObject;
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDelta, PyInt, PyList, PyString};
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyclass_enum, gen_stub_pymethods};
use pyo3_stub_gen::{PyStubType, TypeInfo};
use tokio::sync::RwLock;

use crate::duration::{duration_repr, iggy_duration_to_py_delta, py_delta_to_iggy_duration};
use crate::partitioning::Partitioning;
use crate::send_message::{SendMessage, SendMessagesConfirmation, SendMessagesResponse};

const DEFAULT_BACKGROUND_NUM_SHARDS: usize = 1;
const DEFAULT_BACKGROUND_BATCH_SIZE: usize = 1024 * 1024;
const DEFAULT_BACKGROUND_BATCH_LENGTH: usize = 1_000;
const DEFAULT_BACKGROUND_MAX_BUFFER_SIZE: u64 = 32 * 1024 * 1024;
const DEFAULT_BACKGROUND_MAX_IN_FLIGHT: usize = 1;

/// Configuration for a producer that sends from the calling task.
#[derive(Clone)]
#[gen_stub_pyclass]
#[pyclass(frozen, from_py_object)]
pub struct DirectProducerConfig {
    pub(crate) inner: RustDirectConfig,
}

impl Default for DirectProducerConfig {
    fn default() -> Self {
        Self {
            inner: RustDirectConfig::builder().build(),
        }
    }
}

impl From<&DirectProducerConfig> for RustDirectConfig {
    fn from(config: &DirectProducerConfig) -> Self {
        config.inner.clone()
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl DirectProducerConfig {
    /// Constructs direct-producer batching and pacing configuration.
    #[new]
    #[pyo3(signature = (*, batch_length=1000, linger_time=DefaultDuration::default()))]
    fn new(batch_length: i64, linger_time: DefaultDuration) -> PyResult<Self> {
        let batch_length = u32_param(batch_length, "batch_length")?;
        let linger_time = linger_time.resolve(IggyDuration::from(0))?;
        if linger_time.get_duration().as_micros() > u128::from(u64::MAX) {
            return Err(PyValueError::new_err(format!(
                "'linger_time' must not exceed {} microseconds",
                u64::MAX
            )));
        }
        Ok(Self {
            inner: RustDirectConfig::builder()
                .batch_length(batch_length)
                .linger_time(linger_time)
                .build(),
        })
    }

    /// Maximum number of messages sent in one request.
    /// A value of zero uses the internal limit of 1,000,000 messages.
    #[getter]
    fn batch_length(&self) -> u32 {
        self.inner.batch_length
    }

    /// Minimum gap requested between sequential direct sends.
    #[gen_stub(override_return_type(type_repr = "datetime.timedelta", imports=("datetime")))]
    #[getter]
    fn linger_time<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDelta>> {
        iggy_duration_to_py_delta(py, self.inner.linger_time)
    }

    fn __repr__(&self) -> String {
        format!(
            "DirectProducerConfig(batch_length={}, linger_time={})",
            self.inner.batch_length,
            duration_repr(self.inner.linger_time)
        )
    }
}

/// How a background producer distributes sends among its workers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[gen_stub_pyclass_enum]
#[pyclass(eq, from_py_object, rename_all = "UPPERCASE")]
pub enum ProducerSharding {
    Ordered,
    Balanced,
}

/// What a background send does when the producer buffer is full.
#[derive(Debug, Clone, PartialEq, Eq)]
#[gen_stub_pyclass]
#[pyclass(eq, frozen, from_py_object)]
pub struct BackpressureMode {
    kind: BackpressureKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BackpressureKind {
    Block,
    BlockWithTimeout(IggyDuration),
    FailImmediately,
}

#[gen_stub_pymethods]
#[pymethods]
impl BackpressureMode {
    /// Wait indefinitely for buffer capacity.
    #[staticmethod]
    fn block() -> Self {
        Self {
            kind: BackpressureKind::Block,
        }
    }

    /// Wait up to `timeout` for buffer capacity.
    #[staticmethod]
    fn block_with_timeout(
        #[gen_stub(override_type(type_repr = "datetime.timedelta", imports=("datetime")))]
        timeout: Py<PyDelta>,
    ) -> PyResult<Self> {
        Ok(Self {
            kind: BackpressureKind::BlockWithTimeout(py_delta_to_iggy_duration(&timeout)?),
        })
    }

    /// Fail immediately when the producer buffer is full.
    #[staticmethod]
    fn fail_immediately() -> Self {
        Self {
            kind: BackpressureKind::FailImmediately,
        }
    }

    /// The configured timeout, or `None` for modes without one.
    #[gen_stub(override_return_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
    #[getter]
    fn timeout<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyDelta>>> {
        match self.kind {
            BackpressureKind::BlockWithTimeout(timeout) => {
                iggy_duration_to_py_delta(py, timeout).map(Some)
            }
            BackpressureKind::Block | BackpressureKind::FailImmediately => Ok(None),
        }
    }

    fn __repr__(&self) -> String {
        match self.kind {
            BackpressureKind::Block => "BackpressureMode.block()".to_owned(),
            BackpressureKind::BlockWithTimeout(timeout) => format!(
                "BackpressureMode.block_with_timeout({})",
                duration_repr(timeout)
            ),
            BackpressureKind::FailImmediately => "BackpressureMode.fail_immediately()".to_owned(),
        }
    }
}

/// Immutable configuration for the future background producer mode.
///
/// For detailed background-producer semantics, see
/// https://iggy.apache.org/docs/sdk/rust/high-level-sdk/.
#[derive(Clone)]
#[gen_stub_pyclass]
#[pyclass(frozen, from_py_object)]
pub struct BackgroundProducerConfig {
    num_shards: usize,
    linger_time: IggyDuration,
    batch_size: usize,
    batch_length: usize,
    max_buffer_size: IggyByteSize,
    failure_mode: BackpressureMode,
    max_in_flight: usize,
    sharding: ProducerSharding,
}

impl Default for BackgroundProducerConfig {
    fn default() -> Self {
        Self {
            num_shards: DEFAULT_BACKGROUND_NUM_SHARDS,
            linger_time: IggyDuration::from(1_000),
            batch_size: DEFAULT_BACKGROUND_BATCH_SIZE,
            batch_length: DEFAULT_BACKGROUND_BATCH_LENGTH,
            max_buffer_size: IggyByteSize::from(DEFAULT_BACKGROUND_MAX_BUFFER_SIZE),
            failure_mode: BackpressureMode::block(),
            max_in_flight: DEFAULT_BACKGROUND_MAX_IN_FLIGHT,
            sharding: ProducerSharding::Ordered,
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl BackgroundProducerConfig {
    /// Constructs the stable configuration surface for background mode.
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        *,
        num_shards=1,
        linger_time=DefaultDuration::one_millisecond(),
        batch_size=1048576,
        batch_length=1000,
        max_buffer_size=33554432,
        failure_mode=DefaultBackpressureMode::default(),
        max_in_flight=1,
        sharding=DefaultProducerSharding::default(),
    ))]
    fn new(
        num_shards: i128,
        linger_time: DefaultDuration,
        batch_size: i128,
        batch_length: i128,
        max_buffer_size: i128,
        failure_mode: DefaultBackpressureMode,
        max_in_flight: i128,
        sharding: DefaultProducerSharding,
    ) -> PyResult<Self> {
        Ok(Self {
            num_shards: usize_param(num_shards, "num_shards")?,
            linger_time: linger_time.resolve(IggyDuration::from(1_000))?,
            batch_size: usize_param(batch_size, "batch_size")?,
            batch_length: usize_param(batch_length, "batch_length")?,
            max_buffer_size: IggyByteSize::from(u64_param(max_buffer_size, "max_buffer_size")?),
            failure_mode: failure_mode.resolve(),
            max_in_flight: usize_param(max_in_flight, "max_in_flight")?,
            sharding: sharding.resolve(),
        })
    }

    /// Number of background worker shards, each with its own queue.
    /// A value of zero is treated as one shard.
    #[getter]
    fn num_shards(&self) -> usize {
        self.num_shards
    }

    /// Maximum time a worker holds a non-empty buffer before flushing it.
    /// A zero duration flushes as soon as the worker receives a send.
    #[gen_stub(override_return_type(type_repr = "datetime.timedelta", imports=("datetime")))]
    #[getter]
    fn linger_time<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDelta>> {
        iggy_duration_to_py_delta(py, self.linger_time)
    }

    /// Per-worker flush threshold in buffered bytes.
    /// A value of zero disables this threshold.
    #[getter]
    fn batch_size(&self) -> usize {
        self.batch_size
    }

    /// Per-worker flush threshold in queued sends, not individual messages.
    /// A value of zero disables this threshold.
    #[getter]
    fn batch_length(&self) -> usize {
        self.batch_length
    }

    /// Maximum bytes buffered or in flight across all worker shards.
    /// A value of zero makes the byte budget unlimited.
    #[getter]
    fn max_buffer_size(&self) -> u64 {
        self.max_buffer_size.as_bytes_u64()
    }

    /// Behavior when `max_buffer_size` is exhausted.
    #[getter]
    fn failure_mode(&self) -> BackpressureMode {
        self.failure_mode.clone()
    }

    /// Maximum number of requests written concurrently across all workers.
    /// A value of zero uses the runtime's maximum semaphore permit count.
    #[getter]
    fn max_in_flight(&self) -> usize {
        self.max_in_flight
    }

    /// Strategy used to assign each send to a worker shard.
    /// Ordered sharding preserves per-destination dispatch order, while balanced
    /// sharding distributes sends round-robin and may reorder them.
    #[getter]
    fn sharding(&self) -> ProducerSharding {
        self.sharding
    }

    fn __repr__(&self) -> String {
        let sharding = match self.sharding {
            ProducerSharding::Ordered => "ProducerSharding.ORDERED",
            ProducerSharding::Balanced => "ProducerSharding.BALANCED",
        };
        format!(
            "BackgroundProducerConfig(num_shards={}, linger_time={}, batch_size={}, batch_length={}, max_buffer_size={}, failure_mode={}, max_in_flight={}, sharding={sharding})",
            self.num_shards,
            duration_repr(self.linger_time),
            self.batch_size,
            self.batch_length,
            self.max_buffer_size.as_bytes_u64(),
            self.failure_mode.__repr__(),
            self.max_in_flight,
        )
    }
}

/// A direct producer error that preserves partial-send recovery state.
#[gen_stub_pyclass]
#[pyclass(frozen, extends=PyRuntimeError)]
pub struct ProducerSendError {
    cause: String,
    failed: Arc<Vec<RustIggyMessage>>,
    committed: Arc<Vec<RustSendMessagesConfirmationResponse>>,
}

#[gen_stub_pymethods]
#[pymethods]
impl ProducerSendError {
    /// The underlying Iggy error message.
    #[getter]
    fn cause(&self) -> &str {
        &self.cause
    }

    /// Messages without a usable confirmation after the failure.
    /// An encryptor can leave these messages encrypted, so do not submit them
    /// to the same producer without restoring their original payloads.
    #[getter]
    fn failed(&self) -> Vec<SendMessage> {
        self.failed
            .iter()
            .map(SendMessage::clone_from_rust)
            .collect()
    }

    /// Confirmations returned for chunks committed before the failure.
    #[getter]
    fn committed(&self) -> Vec<SendMessagesConfirmation> {
        self.committed
            .iter()
            .map(SendMessagesConfirmation::from)
            .collect()
    }
}

impl ProducerSendError {
    fn new_err(
        cause: IggyError,
        failed: Arc<Vec<RustIggyMessage>>,
        committed: Arc<Vec<RustSendMessagesConfirmationResponse>>,
    ) -> PyErr {
        Python::attach(|py| {
            let cause = cause.to_string();
            let message = format!("Producer send failed: {cause}");
            let cause_error = PyRuntimeError::new_err(cause.clone());
            let instance = match Bound::new(
                py,
                Self {
                    cause,
                    failed,
                    committed,
                },
            ) {
                Ok(instance) => instance,
                Err(error) => return error,
            };
            if let Err(error) = instance.setattr("args", (message,)) {
                return error;
            }
            let error = PyErr::from_value(instance.into_any());
            error.set_cause(py, Some(cause_error));
            error
        })
    }
}

/// Python port of the Rust high-level producer API, bound to one stream and topic.
///
/// For detailed producer semantics, see
/// https://iggy.apache.org/docs/sdk/rust/high-level-sdk/.
#[derive(Clone)]
#[gen_stub_pyclass]
#[pyclass(from_py_object)]
pub struct IggyProducer {
    inner: Arc<RwLock<Option<RustIggyProducer>>>,
}

impl IggyProducer {
    pub(crate) fn new(producer: RustIggyProducer) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Some(producer))),
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl IggyProducer {
    /// Sends a batch to the producer's bound stream and topic.
    #[gen_stub(override_return_type(type_repr = "collections.abc.Awaitable[SendMessagesResponse]", imports=("collections.abc")))]
    fn send<'py>(
        &self,
        py: Python<'py>,
        #[gen_stub(override_type(type_repr = "list[SendMessage]"))] messages: &Bound<'_, PyList>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let messages = extract_messages(messages)?;
        let inner = self.inner.clone();
        future_into_py(py, async move {
            // Every send keeps a read guard for its full future. Concurrent sends
            // remain possible while shutdown cannot consume an active producer.
            let producer = inner.read().await;
            let producer = producer
                .as_ref()
                .ok_or_else(|| PyRuntimeError::new_err("producer has been shut down"))?;
            producer
                .send(messages)
                .await
                .map(SendMessagesResponse::from)
                .map_err(to_send_error)
        })
    }

    /// Sends one message to the producer's bound stream and topic.
    #[gen_stub(override_return_type(type_repr = "collections.abc.Awaitable[SendMessagesResponse]", imports=("collections.abc")))]
    fn send_one<'py>(
        &self,
        py: Python<'py>,
        message: PyRef<'_, SendMessage>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let message = clone_rust_message(&message);
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let producer = inner.read().await;
            let producer = producer
                .as_ref()
                .ok_or_else(|| PyRuntimeError::new_err("producer has been shut down"))?;
            producer
                .send_one(message)
                .await
                .map(SendMessagesResponse::from)
                .map_err(to_send_error)
        })
    }

    /// Sends a batch with an optional per-call partitioning override.
    #[pyo3(signature = (messages, partitioning=None))]
    #[gen_stub(override_return_type(type_repr = "collections.abc.Awaitable[SendMessagesResponse]", imports=("collections.abc")))]
    fn send_with_partitioning<'py>(
        &self,
        py: Python<'py>,
        #[gen_stub(override_type(type_repr = "list[SendMessage]"))] messages: &Bound<'_, PyList>,
        #[gen_stub(override_type(type_repr = "Partitioning | None"))] partitioning: Option<
            &Partitioning,
        >,
    ) -> PyResult<Bound<'py, PyAny>> {
        let messages = extract_messages(messages)?;
        let partitioning = partitioning.map(|value| value.inner.clone());
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let producer = inner.read().await;
            let producer = producer
                .as_ref()
                .ok_or_else(|| PyRuntimeError::new_err("producer has been shut down"))?;
            producer
                .send_with_partitioning(messages, partitioning)
                .await
                .map(SendMessagesResponse::from)
                .map_err(to_send_error)
        })
    }

    /// Sends a batch to another existing stream and topic.
    #[pyo3(signature = (stream, topic, messages, partitioning=None))]
    #[gen_stub(override_return_type(type_repr = "collections.abc.Awaitable[SendMessagesResponse]", imports=("collections.abc")))]
    fn send_to<'py>(
        &self,
        py: Python<'py>,
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.int"))] stream: &Bound<
            '_,
            PyAny,
        >,
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.int"))] topic: &Bound<
            '_,
            PyAny,
        >,
        #[gen_stub(override_type(type_repr = "list[SendMessage]"))] messages: &Bound<'_, PyList>,
        #[gen_stub(override_type(type_repr = "Partitioning | None"))] partitioning: Option<
            &Partitioning,
        >,
    ) -> PyResult<Bound<'py, PyAny>> {
        let stream = Arc::new(extract_send_to_identifier(stream, "stream")?);
        let topic = Arc::new(extract_send_to_identifier(topic, "topic")?);
        let messages = extract_messages(messages)?;
        let partitioning = partitioning.map(|value| value.inner.clone());
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let producer = inner.read().await;
            let producer = producer
                .as_ref()
                .ok_or_else(|| PyRuntimeError::new_err("producer has been shut down"))?;
            producer
                .send_to(stream, topic, messages, partitioning)
                .await
                .map(SendMessagesResponse::from)
                .map_err(to_send_error)
        })
    }

    #[gen_stub(skip)]
    fn _is_send_active(&self) -> bool {
        self.inner.try_write().is_err()
    }

    /// Waits for active sends and closes the producer. Repeated calls are safe.
    #[gen_stub(override_return_type(type_repr = "collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn shutdown<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            shutdown(inner).await?;
            Ok(Python::attach(|py| py.None()))
        })
    }

    #[gen_stub(override_return_type(type_repr = "collections.abc.Awaitable[IggyProducer]", imports=("collections.abc")))]
    fn __aenter__<'py>(this: Py<Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move { Ok(this) })
    }

    #[gen_stub(override_return_type(type_repr = "collections.abc.Awaitable[builtins.bool]", imports=("collections.abc")))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: &Bound<'_, PyAny>,
        _exc_value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            shutdown(inner).await?;
            Ok(false)
        })
    }
}

#[derive(Clone, FromPyObject)]
pub(crate) enum ProducerMode {
    #[pyo3(transparent)]
    Direct(DirectProducerConfig),
    #[pyo3(transparent)]
    Background(BackgroundProducerConfig),
}

impl Default for ProducerMode {
    fn default() -> Self {
        Self::Direct(DirectProducerConfig::default())
    }
}

#[derive(FromPyObject)]
#[pyo3(transparent)]
struct DefaultBackpressureMode(BackpressureMode);

impl Default for DefaultBackpressureMode {
    fn default() -> Self {
        Self(BackpressureMode::block())
    }
}

impl DefaultBackpressureMode {
    fn resolve(self) -> BackpressureMode {
        self.0
    }
}

impl PyStubType for DefaultBackpressureMode {
    fn type_output() -> TypeInfo {
        BackpressureMode::type_output()
    }

    fn type_input() -> TypeInfo {
        BackpressureMode::type_input()
    }
}

impl<'py> IntoPyObject<'py> for DefaultBackpressureMode {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(opaque_stub_default(py))
    }
}

#[derive(FromPyObject)]
#[pyo3(transparent)]
struct DefaultProducerSharding(ProducerSharding);

impl Default for DefaultProducerSharding {
    fn default() -> Self {
        Self(ProducerSharding::Ordered)
    }
}

impl DefaultProducerSharding {
    fn resolve(self) -> ProducerSharding {
        self.0
    }
}

impl PyStubType for DefaultProducerSharding {
    fn type_output() -> TypeInfo {
        ProducerSharding::type_output()
    }

    fn type_input() -> TypeInfo {
        ProducerSharding::type_input()
    }
}

impl<'py> IntoPyObject<'py> for DefaultProducerSharding {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(opaque_stub_default(py))
    }
}

#[derive(Default)]
pub(crate) enum RetryInterval {
    #[default]
    Omitted,
    Disabled,
    Duration(Py<PyDelta>),
}

impl<'a, 'py> FromPyObject<'a, 'py> for RetryInterval {
    type Error = PyErr;

    fn extract(object: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if object.is_none() {
            Ok(Self::Disabled)
        } else {
            Ok(Self::Duration(object.extract::<Py<PyDelta>>()?))
        }
    }
}

impl PyStubType for RetryInterval {
    fn type_output() -> TypeInfo {
        <std::time::Duration>::type_output() | TypeInfo::none()
    }

    fn type_input() -> TypeInfo {
        <std::time::Duration>::type_input() | TypeInfo::none()
    }
}

impl<'py> IntoPyObject<'py> for RetryInterval {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            Self::Omitted => std::time::Duration::from_secs(1).into_bound_py_any(py),
            Self::Disabled => Ok(py.None().into_bound(py)),
            Self::Duration(duration) => Ok(duration.into_bound(py).into_any()),
        }
    }
}

impl RetryInterval {
    pub(crate) fn resolve(self) -> PyResult<Option<iggy::prelude::NonZeroIggyDuration>> {
        match self {
            Self::Omitted => Ok(Some(iggy::prelude::NonZeroIggyDuration::ONE_SECOND)),
            Self::Disabled => Ok(None),
            Self::Duration(duration) => py_delta_to_iggy_duration(&duration).and_then(|duration| {
                iggy::prelude::NonZeroIggyDuration::try_from(duration)
                    .map(Some)
                    .map_err(|_| PyValueError::new_err("'send_retry_interval' must not be zero"))
            }),
        }
    }
}

#[derive(Default)]
enum DefaultDuration {
    #[default]
    Zero,
    OneMillisecond,
    Value(Py<PyDelta>),
}

impl DefaultDuration {
    fn one_millisecond() -> Self {
        Self::OneMillisecond
    }
}

impl PyStubType for DefaultDuration {
    fn type_output() -> TypeInfo {
        timedelta_type_info()
    }

    fn type_input() -> TypeInfo {
        timedelta_type_info()
    }
}

impl<'py> IntoPyObject<'py> for DefaultDuration {
    type Target = PyDelta;
    type Output = Bound<'py, PyDelta>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            Self::Zero => std::time::Duration::ZERO.into_pyobject(py),
            Self::OneMillisecond => std::time::Duration::from_millis(1).into_pyobject(py),
            Self::Value(duration) => Ok(duration.into_bound(py)),
        }
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for DefaultDuration {
    type Error = PyErr;

    fn extract(object: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        Ok(Self::Value(object.extract::<Py<PyDelta>>()?))
    }
}

impl DefaultDuration {
    fn resolve(self, default: IggyDuration) -> PyResult<IggyDuration> {
        match self {
            Self::Value(duration) => py_delta_to_iggy_duration(&duration),
            Self::Zero | Self::OneMillisecond => Ok(default),
        }
    }
}

fn timedelta_type_info() -> TypeInfo {
    let mut type_info = <std::time::Duration>::type_input();
    type_info.source_module = None;
    type_info
}

fn opaque_stub_default(py: Python<'_>) -> Bound<'_, PyAny> {
    // The stub generator renders objects without a stable Python expression as
    // `...`. An actual Ellipsis is rendered as `Ellipsis`, which type checkers
    // reject as a default for these public configuration types.
    py.None().into_bound(py).get_type().into_any()
}

async fn shutdown(inner: Arc<RwLock<Option<RustIggyProducer>>>) -> PyResult<()> {
    // Exclusive access must span consuming shutdown so another close waits for
    // completion and no send can observe a producer being closed underneath it.
    let mut producer = inner.write().await;
    if let Some(producer) = producer.take() {
        producer.shutdown().await;
    }
    Ok(())
}

fn extract_messages(messages: &Bound<'_, PyList>) -> PyResult<Vec<RustIggyMessage>> {
    messages
        .iter()
        .map(|item| {
            let message = item.extract::<PyRef<'_, SendMessage>>()?;
            Ok(clone_rust_message(&message))
        })
        .collect()
}

fn extract_send_to_identifier(value: &Bound<'_, PyAny>, parameter: &str) -> PyResult<Identifier> {
    if let Ok(value) = value.cast::<PyString>() {
        return Identifier::from_str(value.to_str()?)
            .map_err(|error| PyValueError::new_err(error.to_string()));
    }
    if value.is_instance_of::<PyInt>() {
        let value = value.extract::<u32>()?;
        return Identifier::numeric(value)
            .map_err(|error| PyValueError::new_err(error.to_string()));
    }
    Err(PyTypeError::new_err(format!(
        "'{parameter}' must be a string or an integer"
    )))
}

fn clone_rust_message(message: &SendMessage) -> RustIggyMessage {
    message.clone().inner
}

fn to_send_error(error: IggyError) -> PyErr {
    match error {
        IggyError::ProducerSendFailed {
            cause,
            failed,
            committed,
            ..
        } => ProducerSendError::new_err(*cause, failed, committed),
        error => PyRuntimeError::new_err(error.to_string()),
    }
}

pub(crate) fn u32_param(value: i64, parameter: &str) -> PyResult<u32> {
    u32::try_from(value).map_err(|_| {
        PyValueError::new_err(format!("'{parameter}' must be between 0 and {}", u32::MAX))
    })
}

fn usize_param(value: i128, parameter: &str) -> PyResult<usize> {
    usize::try_from(value).map_err(|_| {
        PyValueError::new_err(format!(
            "'{parameter}' must be between 0 and {}",
            usize::MAX
        ))
    })
}

fn u64_param(value: i128, parameter: &str) -> PyResult<u64> {
    u64::try_from(value).map_err(|_| {
        PyValueError::new_err(format!("'{parameter}' must be between 0 and {}", u64::MAX))
    })
}

#[cfg(test)]
mod tests {
    use pyo3::exceptions::{PyOverflowError, PyRuntimeError, PyTypeError, PyValueError};

    use super::*;

    #[test]
    fn direct_defaults_match_rust() {
        let python = DirectProducerConfig::default();
        let rust = RustDirectConfig::builder().build();

        assert_eq!(python.inner.batch_length, rust.batch_length);
        assert_eq!(python.inner.linger_time, rust.linger_time);
    }

    #[test]
    fn direct_linger_rejects_values_above_u64_microseconds() {
        Python::initialize();
        Python::attach(|py| {
            let duration = PyDelta::new(py, 999_999_999, 0, 0, false).unwrap().unbind();
            let error = DirectProducerConfig::new(1_000, DefaultDuration::Value(duration))
                .err()
                .unwrap();

            assert!(error.is_instance_of::<PyValueError>(py));
            assert!(error.to_string().contains("linger_time"));
        });
    }

    #[test]
    fn producer_send_error_preserves_recovery_state() {
        Python::initialize();
        Python::attach(|py| {
            let cause = IggyError::CannotSendMessagesDueToClientDisconnection;
            let cause_message = cause.to_string();
            let error = to_send_error(IggyError::ProducerSendFailed {
                cause: Box::new(cause),
                failed: Arc::new(vec![RustIggyMessage::from_str("failed").unwrap()]),
                committed: Arc::new(vec![RustSendMessagesConfirmationResponse {
                    stream_id: 1,
                    topic_id: 2,
                    partition_id: 3,
                    base_offset: 4,
                }]),
                stream_name: "stream".to_owned(),
                topic_name: "topic".to_owned(),
            });

            assert!(error.is_instance_of::<ProducerSendError>(py));
            assert!(error.is_instance_of::<PyRuntimeError>(py));
            assert_eq!(
                error
                    .value(py)
                    .getattr("cause")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                cause_message
            );

            let failed = error
                .value(py)
                .getattr("failed")
                .unwrap()
                .extract::<Vec<Py<SendMessage>>>()
                .unwrap();
            assert_eq!(failed.len(), 1);
            assert_eq!(failed[0].borrow(py).inner.payload.as_ref(), b"failed");

            let committed = error
                .value(py)
                .getattr("committed")
                .unwrap()
                .extract::<Vec<Py<SendMessagesConfirmation>>>()
                .unwrap();
            assert_eq!(committed.len(), 1);
            assert_eq!(committed[0].borrow(py).inner.base_offset, 4);

            let source = error.cause(py).unwrap();
            assert!(source.is_instance_of::<PyRuntimeError>(py));
            assert_eq!(
                source.value(py).str().unwrap().to_str().unwrap(),
                cause_message
            );
        });
    }

    #[test]
    fn background_defaults_match_rust() {
        let python = BackgroundProducerConfig::default();
        let rust = RustBackgroundConfig::builder().build();

        assert_eq!(python.num_shards, rust.num_shards);
        assert_eq!(python.linger_time, rust.linger_time);
        assert_eq!(python.batch_size, rust.batch_size);
        assert_eq!(python.batch_length, rust.batch_length);
        assert_eq!(python.max_buffer_size, rust.max_buffer_size);
        assert!(matches!(rust.failure_mode, RustBackpressureMode::Block));
        assert_eq!(python.max_in_flight, rust.max_in_flight);
        assert!(matches!(python.sharding, ProducerSharding::Ordered));
        assert_eq!(format!("{:?}", rust.sharding), "OrderedSharding");
    }

    #[test]
    fn direct_batch_length_rejects_values_outside_u32() {
        assert!(u32_param(-1, "batch_length").is_err());
        assert!(u32_param(i64::from(u32::MAX) + 1, "batch_length").is_err());
        assert_eq!(
            u32_param(i64::from(u32::MAX), "batch_length").unwrap(),
            u32::MAX
        );
    }

    #[test]
    fn send_to_identifier_preserves_python_error_categories() {
        Python::initialize();
        Python::attach(|py| {
            let string = PyString::new(py, "stream");
            let numeric = 7_u32.into_pyobject(py).unwrap();
            let negative = (-1_i64).into_pyobject(py).unwrap();
            let too_large = (u64::from(u32::MAX) + 1).into_pyobject(py).unwrap();
            let wrong_type = py.None().into_bound(py);

            assert_eq!(
                extract_send_to_identifier(string.as_any(), "stream")
                    .unwrap()
                    .get_string_value()
                    .unwrap(),
                "stream"
            );
            assert_eq!(
                extract_send_to_identifier(numeric.as_any(), "stream")
                    .unwrap()
                    .get_u32_value()
                    .unwrap(),
                7
            );

            let negative = extract_send_to_identifier(negative.as_any(), "stream").unwrap_err();
            let too_large = extract_send_to_identifier(too_large.as_any(), "stream").unwrap_err();
            let wrong_type = extract_send_to_identifier(&wrong_type, "stream").unwrap_err();

            assert!(negative.is_instance_of::<PyOverflowError>(py));
            assert!(too_large.is_instance_of::<PyOverflowError>(py));
            assert!(wrong_type.is_instance_of::<PyTypeError>(py));
        });
    }

    #[test]
    fn representations_name_public_python_constructors() {
        let direct = DirectProducerConfig::default();
        let background = BackgroundProducerConfig::default();

        assert_eq!(
            direct.__repr__(),
            "DirectProducerConfig(batch_length=1000, linger_time=datetime.timedelta(seconds=0))"
        );
        assert_eq!(
            BackpressureMode::block().__repr__(),
            "BackpressureMode.block()"
        );
        assert_eq!(
            BackpressureMode::fail_immediately().__repr__(),
            "BackpressureMode.fail_immediately()"
        );
        assert!(
            background
                .__repr__()
                .ends_with("sharding=ProducerSharding.ORDERED)")
        );
    }

    #[test]
    fn numeric_getters_return_configured_values() {
        let background = BackgroundProducerConfig::default();

        assert_eq!(background.num_shards(), 1);
        assert_eq!(background.batch_size(), 1_048_576);
        assert_eq!(background.batch_length(), 1_000);
        assert_eq!(background.max_buffer_size(), 33_554_432);
        assert_eq!(background.max_in_flight(), 1);
        assert_eq!(background.sharding(), ProducerSharding::Ordered);
        assert_eq!(background.failure_mode(), BackpressureMode::block());
    }

    #[test]
    fn numeric_validation_uses_python_semantic_ranges() {
        assert!(usize_param(-1, "num_shards").is_err());
        assert!(u64_param(-1, "max_buffer_size").is_err());
        assert_eq!(
            usize_param(usize::MAX as i128, "num_shards").unwrap(),
            usize::MAX
        );
        assert_eq!(
            u64_param(u64::MAX as i128, "max_buffer_size").unwrap(),
            u64::MAX
        );
    }

    #[test]
    fn direct_default_constants_match_builder() {
        let rust = RustDirectConfig::builder().build();
        assert_eq!(1_000, rust.batch_length);
        assert_eq!(IggyDuration::from(0), rust.linger_time);
    }

    #[test]
    fn background_default_constants_match_builder() {
        let rust = RustBackgroundConfig::builder().build();
        assert_eq!(DEFAULT_BACKGROUND_NUM_SHARDS, rust.num_shards);
        assert_eq!(IggyDuration::from(1_000), rust.linger_time);
        assert_eq!(DEFAULT_BACKGROUND_BATCH_SIZE, rust.batch_size);
        assert_eq!(DEFAULT_BACKGROUND_BATCH_LENGTH, rust.batch_length);
        assert_eq!(
            DEFAULT_BACKGROUND_MAX_BUFFER_SIZE,
            rust.max_buffer_size.as_bytes_u64()
        );
        assert_eq!(DEFAULT_BACKGROUND_MAX_IN_FLIGHT, rust.max_in_flight);
    }
}
