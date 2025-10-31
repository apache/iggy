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

use std::str::FromStr;
use std::sync::Arc;

use iggy::prelude::{
    Consumer as RustConsumer, IggyClient as RustIggyClient, IggyMessage as RustMessage,
    PollingStrategy as RustPollingStrategy, *,
};
use pyo3::prelude::*;
use pyo3::types::{PyDelta, PyList, PyType};
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_stub_gen::define_stub_info_gatherer;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

use crate::consumer::{py_delta_to_iggy_duration, AutoCommit, IggyConsumer};
use crate::identifier::PyIdentifier;
use crate::receive_message::{PollingStrategy, ReceiveMessage};
use crate::send_message::SendMessage;
use crate::stream::StreamDetails;
use crate::topic::TopicDetails;
use tokio::sync::Mutex;

/// A Python class representing the Iggy client.
/// It wraps the RustIggyClient and provides asynchronous functionality
/// through the contained runtime.
#[gen_stub_pyclass]
#[pyclass]
pub struct IggyClient {
    inner: Arc<RustIggyClient>,
}

#[gen_stub_pymethods]
#[pymethods]
impl IggyClient {
    /// Constructs a new IggyClient from a TCP server address.
    ///
    /// This initializes a new runtime for asynchronous operations.
    /// Future versions might utilize asyncio for more Pythonic async.
    #[new]
    #[pyo3(signature = (conn=None))]
    fn new(conn: Option<String>) -> Self {
        let client = IggyClientBuilder::new()
            .with_tcp()
            .with_server_address(conn.unwrap_or("127.0.0.1:8090".to_string()))
            .build()
            .unwrap();
        IggyClient {
            inner: Arc::new(client),
        }
    }

    /// Constructs a new IggyClient from a connection string.
    ///
    /// Returns an error if the connection string provided is invalid.
    // TODO: add examples for connection strings or at least a link to the doc page where
    // connection strings are explained.
    #[classmethod]
    #[pyo3(signature = (connection_string))]
    fn from_connection_string(
        _cls: &Bound<'_, PyType>,
        connection_string: String,
    ) -> PyResult<Self> {
        let client = RustIggyClient::from_connection_string(&connection_string)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e:?}")))?;
        Ok(Self {
            inner: Arc::new(client),
        })
    }

    /// Sends a ping request to the server to check connectivity.
    ///
    /// Returns `Ok(())` if the server responds successfully, or a `PyRuntimeError`
    /// if the connection fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn ping<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner
                .ping()
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e:?}")))
        })
    }

    /// Logs in the user with the given credentials.
    ///
    /// Returns `Ok(())` on success, or a PyRuntimeError on failure.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn login_user<'a>(
        &self,
        py: Python<'a>,
        username: String,
        password: String,
    ) -> PyResult<Bound<'a, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner
                .login_user(&username, &password)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e:?}")))?;
            Ok(())
        })
    }

    /// Connects the IggyClient to its service.
    ///
    /// Returns Ok(()) on successful connection or a PyRuntimeError on failure.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn connect<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner
                .connect()
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e:?}")))?;
            Ok(())
        })
    }

    /// Creates a new stream with the provided ID and name.
    ///
    /// Returns Ok(()) on successful stream creation or a PyRuntimeError on failure.
    #[pyo3(signature = (name))]
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn create_stream<'a>(
        &self,
        py: Python<'a>,
        name: String,
    ) -> PyResult<Bound<'a, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner
                .create_stream(&name)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e:?}")))?;
            Ok(())
        })
    }

    /// Gets stream by id.
    ///
    /// Returns Option of stream details or a PyRuntimeError on failure.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[typing.Optional[StreamDetails]]", imports=("collections.abc")))]
    fn get_stream<'a>(
        &self,
        py: Python<'a>,
        stream_id: PyIdentifier,
    ) -> PyResult<Bound<'a, PyAny>> {
        let stream_id = Identifier::from(stream_id);
        let inner = self.inner.clone();

        future_into_py(py, async move {
            let stream = inner
                .get_stream(&stream_id)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e:?}")))?;
            Ok(stream.map(StreamDetails::from))
        })
    }

    /// Creates a new topic with the given parameters.
    ///
    /// Returns Ok(()) on successful topic creation or a PyRuntimeError on failure.
    #[pyo3(
        signature = (stream, name, partitions_count, compression_algorithm = None, replication_factor = None)
    )]
    #[allow(clippy::too_many_arguments)]
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn create_topic<'a>(
        &self,
        py: Python<'a>,
        stream: PyIdentifier,
        name: String,
        partitions_count: u32,
        compression_algorithm: Option<String>,
        replication_factor: Option<u8>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let compression_algorithm = match compression_algorithm {
            Some(algo) => CompressionAlgorithm::from_str(&algo)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e:?}")))?,
            None => CompressionAlgorithm::default(),
        };

        let stream = Identifier::from(stream);
        let inner = self.inner.clone();

        future_into_py(py, async move {
            inner
                .create_topic(
                    &stream,
                    &name,
                    partitions_count,
                    compression_algorithm,
                    replication_factor,
                    IggyExpiry::NeverExpire,
                    MaxTopicSize::ServerDefault,
                )
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e:?}")))?;
            Ok(())
        })
    }

    /// Gets topic by stream and id.
    ///
    /// Returns Option of topic details or a PyRuntimeError on failure.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[typing.Optional[TopicDetails]]", imports=("collections.abc")))]
    fn get_topic<'a>(
        &self,
        py: Python<'a>,
        stream_id: PyIdentifier,
        topic_id: PyIdentifier,
    ) -> PyResult<Bound<'a, PyAny>> {
        let stream_id = Identifier::from(stream_id);
        let topic_id = Identifier::from(topic_id);
        let inner = self.inner.clone();

        future_into_py(py, async move {
            let topic = inner
                .get_topic(&stream_id, &topic_id)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e:?}")))?;
            Ok(topic.map(TopicDetails::from))
        })
    }

    /// Sends a list of messages to the specified topic.
    ///
    /// Returns Ok(()) on successful sending or a PyRuntimeError on failure.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn send_messages<'a>(
        &self,
        py: Python<'a>,
        stream: PyIdentifier,
        topic: PyIdentifier,
        partitioning: u32,
        #[gen_stub(override_type(type_repr = "list[SendMessage]"))] messages: &Bound<'_, PyList>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let messages: Vec<SendMessage> = messages
            .iter()
            .map(|item| item.extract::<SendMessage>())
            .collect::<Result<Vec<_>, _>>()?;
        let mut messages: Vec<RustMessage> = messages
            .into_iter()
            .map(|message| message.inner)
            .collect::<Vec<_>>();

        let stream = Identifier::from(stream);
        let topic = Identifier::from(topic);
        let partitioning = Partitioning::partition_id(partitioning);
        let inner = self.inner.clone();

        future_into_py(py, async move {
            inner
                .send_messages(&stream, &topic, &partitioning, messages.as_mut())
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e:?}")))?;
            Ok(())
        })
    }

    /// Polls for messages from the specified topic and partition.
    ///
    /// Returns a list of received messages or a PyRuntimeError on failure.
    #[allow(clippy::too_many_arguments)]
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[list[ReceiveMessage]]", imports=("collections.abc")))]
    fn poll_messages<'a>(
        &self,
        py: Python<'a>,
        stream: PyIdentifier,
        topic: PyIdentifier,
        partition_id: u32,
        polling_strategy: &PollingStrategy,
        count: u32,
        auto_commit: bool,
    ) -> PyResult<Bound<'a, PyAny>> {
        let consumer = RustConsumer::default();
        let stream = Identifier::from(stream);
        let topic = Identifier::from(topic);
        let strategy: RustPollingStrategy = polling_strategy.into();

        let inner = self.inner.clone();

        future_into_py(py, async move {
            let polled_messages = inner
                .poll_messages(
                    &stream,
                    &topic,
                    Some(partition_id),
                    &consumer,
                    &strategy,
                    count,
                    auto_commit,
                )
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e:?}")))?;
            let messages = polled_messages
                .messages
                .into_iter()
                .map(ReceiveMessage::from_rust_message)
                .collect::<Vec<_>>();
            Ok(messages)
        })
    }

    /// Creates a new consumer group consumer.
    ///
    /// Returns the consumer or a PyRuntimeError on failure.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        name,
        stream,
        topic,
        partition_id=None,
        polling_strategy=None,
        batch_length=None,
        auto_commit=None,
        create_consumer_group_if_not_exists=true,
        auto_join_consumer_group=true,
        poll_interval=None,
        polling_retry_interval=None,
        init_retries=None,
        init_retry_interval=None,
        allow_replay=false,
    ))]
    fn consumer_group(
        &self,
        name: &str,
        stream: &str,
        topic: &str,
        partition_id: Option<u32>,
        polling_strategy: Option<&PollingStrategy>,
        batch_length: Option<u32>,
        auto_commit: Option<&AutoCommit>,
        create_consumer_group_if_not_exists: bool,
        auto_join_consumer_group: bool,
        poll_interval: Option<Py<PyDelta>>,
        polling_retry_interval: Option<Py<PyDelta>>,
        init_retries: Option<u32>,
        init_retry_interval: Option<Py<PyDelta>>,
        allow_replay: bool,
    ) -> PyResult<IggyConsumer> {
        let mut builder = self
            .inner
            .consumer_group(name, stream, topic)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e:?}")))?
            .without_encryptor()
            .partition(partition_id);

        if create_consumer_group_if_not_exists {
            builder = builder.create_consumer_group_if_not_exists()
        } else {
            builder = builder.do_not_create_consumer_group_if_not_exists()
        };
        if auto_join_consumer_group {
            builder = builder.auto_join_consumer_group()
        } else {
            builder = builder.do_not_auto_join_consumer_group()
        };
        if let Some(polling_strategy) = polling_strategy {
            builder = builder.polling_strategy(polling_strategy.into())
        };
        if let Some(batch_length) = batch_length {
            builder = builder.batch_length(batch_length)
        };
        if let Some(auto_commit) = auto_commit {
            builder = builder.auto_commit(auto_commit.into())
        };
        if let Some(poll_interval) = poll_interval {
            builder = builder.poll_interval(py_delta_to_iggy_duration(&poll_interval))
        } else {
            builder = builder.without_poll_interval()
        };
        if let Some(polling_retry_interval) = polling_retry_interval {
            builder =
                builder.polling_retry_interval(py_delta_to_iggy_duration(&polling_retry_interval))
        }
        if init_retries.is_some() && init_retry_interval.is_none() {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "'init_retry_interval' is required if 'init_retries' is set",
            ));
        }
        if init_retries.is_none() && init_retry_interval.is_some() {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "'init_retries' is required if 'init_retry_interval' is set",
            ));
        }
        if let (Some(init_retries), Some(init_retry_interval)) = (init_retries, init_retry_interval)
        {
            builder = builder.init_retries(
                init_retries,
                py_delta_to_iggy_duration(&init_retry_interval),
            );
        }
        if allow_replay {
            builder = builder.allow_replay()
        }
        let consumer = builder.build();

        Ok(IggyConsumer {
            inner: Arc::new(Mutex::new(consumer)),
        })
    }
}

define_stub_info_gatherer!(stub_info);
