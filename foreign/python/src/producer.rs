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

use iggy::clients::producer_config::BackpressureMode as RustBackpressureMode;
use iggy::prelude::{
    BackgroundConfig as RustBackgroundConfig, BalancedSharding, DirectConfig as RustDirectConfig,
    Identifier, IggyByteSize, IggyDuration, IggyMessage as RustIggyMessage,
    IggyProducer as RustIggyProducer, OrderedSharding, Sharding,
};
use pyo3::IntoPyObjectExt;
use pyo3::conversion::FromPyObject;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDelta, PyList};
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyclass_enum, gen_stub_pymethods};
use pyo3_stub_gen::{PyStubType, TypeInfo};
use tokio::sync::RwLock;

use crate::duration::{duration_repr, iggy_duration_to_py_delta, py_delta_to_iggy_duration};
use crate::identifier::PyIdentifier;
use crate::partitioning::Partitioning;
use crate::send_message::{SendMessage, SendMessagesResponse};

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
        Ok(Self {
            inner: RustDirectConfig::builder()
                .batch_length(batch_length)
                .linger_time(linger_time)
                .build(),
        })
    }

    /// Maximum number of messages sent in one request.
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

impl ProducerSharding {
    fn into_rust(self) -> Box<dyn Sharding + Send + Sync> {
        match self {
            Self::Ordered => Box::new(OrderedSharding),
            Self::Balanced => Box::new(BalancedSharding::default()),
        }
    }
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

impl From<&BackpressureMode> for RustBackpressureMode {
    fn from(mode: &BackpressureMode) -> Self {
        match mode.kind {
            BackpressureKind::Block => Self::Block,
            BackpressureKind::BlockWithTimeout(timeout) => Self::BlockWithTimeout(timeout),
            BackpressureKind::FailImmediately => Self::FailImmediately,
        }
    }
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

impl From<&BackgroundProducerConfig> for RustBackgroundConfig {
    fn from(config: &BackgroundProducerConfig) -> Self {
        RustBackgroundConfig::builder()
            .num_shards(config.num_shards)
            .linger_time(config.linger_time)
            .batch_size(config.batch_size)
            .batch_length(config.batch_length)
            .max_buffer_size(config.max_buffer_size)
            .failure_mode((&config.failure_mode).into())
            .max_in_flight(config.max_in_flight)
            .sharding(config.sharding.into_rust())
            .build()
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

    #[getter]
    fn num_shards(&self) -> usize {
        self.num_shards
    }

    #[gen_stub(override_return_type(type_repr = "datetime.timedelta", imports=("datetime")))]
    #[getter]
    fn linger_time<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDelta>> {
        iggy_duration_to_py_delta(py, self.linger_time)
    }

    #[getter]
    fn batch_size(&self) -> usize {
        self.batch_size
    }

    #[getter]
    fn batch_length(&self) -> usize {
        self.batch_length
    }

    #[getter]
    fn max_buffer_size(&self) -> u64 {
        self.max_buffer_size.as_bytes_u64()
    }

    #[getter]
    fn failure_mode(&self) -> BackpressureMode {
        self.failure_mode.clone()
    }

    #[getter]
    fn max_in_flight(&self) -> usize {
        self.max_in_flight
    }

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

/// A producer bound to one stream and topic and ready to send messages.
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
            let producer = inner.read().await;
            let producer = producer
                .as_ref()
                .ok_or_else(|| PyRuntimeError::new_err("producer has been shut down"))?;
            producer
                .send(messages)
                .await
                .map(SendMessagesResponse::from)
                .map_err(to_runtime_error)
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
                .map_err(to_runtime_error)
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
        let partitioning = partitioning.map(|value| Arc::new(value.inner.clone()));
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
                .map_err(to_runtime_error)
        })
    }

    /// Sends a batch to another existing stream and topic.
    #[pyo3(signature = (stream, topic, messages, partitioning=None))]
    #[gen_stub(override_return_type(type_repr = "collections.abc.Awaitable[SendMessagesResponse]", imports=("collections.abc")))]
    fn send_to<'py>(
        &self,
        py: Python<'py>,
        stream: PyIdentifier,
        topic: PyIdentifier,
        #[gen_stub(override_type(type_repr = "list[SendMessage]"))] messages: &Bound<'_, PyList>,
        #[gen_stub(override_type(type_repr = "Partitioning | None"))] partitioning: Option<
            &Partitioning,
        >,
    ) -> PyResult<Bound<'py, PyAny>> {
        let stream = Arc::new(Identifier::try_from(stream)?);
        let topic = Arc::new(Identifier::try_from(topic)?);
        let messages = extract_messages(messages)?;
        let partitioning = partitioning.map(|value| Arc::new(value.inner.clone()));
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
                .map_err(to_runtime_error)
        })
    }

    /// Waits for active sends and closes the producer. Repeated calls are safe.
    #[gen_stub(override_return_type(type_repr = "collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn shutdown<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move { shutdown(inner).await })
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

impl PyStubType for ProducerMode {
    fn type_output() -> TypeInfo {
        DirectProducerConfig::type_output() | BackgroundProducerConfig::type_output()
    }

    fn type_input() -> TypeInfo {
        DirectProducerConfig::type_input() | BackgroundProducerConfig::type_input()
    }
}

impl<'py> IntoPyObject<'py> for ProducerMode {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            Self::Direct(config) => Ok(config.into_pyobject(py)?.into_any()),
            Self::Background(config) => Ok(config.into_pyobject(py)?.into_any()),
        }
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

fn clone_rust_message(message: &SendMessage) -> RustIggyMessage {
    message.clone().inner
}

fn to_runtime_error(error: impl ToString) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
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
    use super::*;

    #[test]
    fn direct_defaults_match_rust() {
        let python = DirectProducerConfig::default();
        let rust = RustDirectConfig::builder().build();

        assert_eq!(python.inner.batch_length, rust.batch_length);
        assert_eq!(python.inner.linger_time, rust.linger_time);
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
    fn background_configuration_converts_to_rust() {
        let python = BackgroundProducerConfig {
            num_shards: 4,
            linger_time: IggyDuration::from(2_000),
            batch_size: 2_048,
            batch_length: 32,
            max_buffer_size: IggyByteSize::from(8_192),
            failure_mode: BackpressureMode::fail_immediately(),
            max_in_flight: 3,
            sharding: ProducerSharding::Balanced,
        };
        let rust = RustBackgroundConfig::from(&python);

        assert_eq!(rust.num_shards, 4);
        assert_eq!(rust.linger_time, IggyDuration::from(2_000));
        assert_eq!(rust.batch_size, 2_048);
        assert_eq!(rust.batch_length, 32);
        assert_eq!(rust.max_buffer_size.as_bytes_u64(), 8_192);
        assert!(matches!(
            rust.failure_mode,
            RustBackpressureMode::FailImmediately
        ));
        assert_eq!(rust.max_in_flight, 3);
        assert_eq!(
            format!("{:?}", rust.sharding),
            "BalancedSharding { counter: 0 }"
        );
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
