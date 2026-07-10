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

use iggy::prelude::{
    Consumer as RustConsumer, IggyClient as RustIggyClient, IggyExpiry as RustIggyExpiry,
    IggyMessage as RustMessage, MaxTopicSize as RustMaxTopicSize,
    PollingStrategy as RustPollingStrategy, *,
};
use pyo3::PyRef;
use pyo3::prelude::*;
use pyo3::types::{PyDelta, PyList, PyType};
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_stub_gen::define_stub_info_gatherer;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::str::FromStr;
use std::sync::Arc;

use crate::consumer::{
    AutoCommit, ConsumerGroup as PyConsumerGroup, ConsumerGroupDetails as PyConsumerGroupDetails,
    IggyConsumer, py_delta_to_iggy_duration,
};
use crate::identifier::PyIdentifier;
use crate::receive_message::{PollingStrategy, ReceiveMessage};
use crate::send_message::SendMessage;
use crate::stream::StreamDetails;
use crate::topic::{IggyExpiry, MaxTopicSize, Topic, TopicDetails};
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
    /// This initializes a new runtime for asynchronous operations.
    /// Future versions might utilize asyncio for more Pythonic async.
    #[new]
    #[pyo3(signature = (conn=None))]
    fn new(
        #[gen_stub(override_type(type_repr = "builtins.str | None"))] conn: Option<String>,
    ) -> PyResult<Self> {
        let client = IggyClientBuilder::new()
            .with_tcp()
            .with_server_address(conn.unwrap_or("127.0.0.1:8090".to_string()))
            .build()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(IggyClient {
            inner: Arc::new(client),
        })
    }

    /// Constructs a new IggyClient from a connection string.
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
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(Self {
            inner: Arc::new(client),
        })
    }

    /// Sends a ping request to the server to check connectivity.
    /// Returns `Ok(())` if the server responds successfully, or a `PyRuntimeError`
    /// if the connection fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn ping<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner
                .ping()
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
        })
    }

    /// Logs in the user with the given credentials.
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
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(())
        })
    }

    /// Connects the IggyClient to its service.
    /// Returns Ok(()) on successful connection or a PyRuntimeError on failure.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn connect<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner
                .connect()
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(())
        })
    }

    /// Creates a new stream with the provided ID and name.
    /// Returns Ok(()) on successful stream creation or a PyRuntimeError on failure.
    #[pyo3(signature = (name))]
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn create_stream<'a>(&self, py: Python<'a>, name: String) -> PyResult<Bound<'a, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner
                .create_stream(&name)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(())
        })
    }

    /// Gets stream by id.
    /// Returns Option of stream details or a PyRuntimeError on failure.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[StreamDetails | None]", imports=("collections.abc")))]
    fn get_stream<'a>(
        &self,
        py: Python<'a>,
        stream_id: PyIdentifier,
    ) -> PyResult<Bound<'a, PyAny>> {
        let stream_id = Identifier::try_from(stream_id)?;
        let inner = self.inner.clone();

        future_into_py(py, async move {
            let stream = inner
                .get_stream(&stream_id)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(stream.map(StreamDetails::from))
        })
    }

    /// Creates a new topic with the given parameters.
    /// Returns Ok(()) on successful topic creation or a PyRuntimeError on failure.
    #[pyo3(
        signature = (stream, name, partitions_count, compression_algorithm = None, replication_factor = None, message_expiry = None, max_topic_size = None)
    )]
    #[allow(clippy::too_many_arguments)]
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn create_topic<'a>(
        &self,
        py: Python<'a>,
        stream: PyIdentifier,
        name: String,
        partitions_count: u32,
        #[gen_stub(override_type(type_repr = "builtins.str | None"))] compression_algorithm: Option<
            String,
        >,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))] replication_factor: Option<
            u8,
        >,
        #[gen_stub(override_type(type_repr = "IggyExpiry | None"))] message_expiry: Option<
            &IggyExpiry,
        >,
        #[gen_stub(override_type(type_repr = "MaxTopicSize | None"))] max_topic_size: Option<
            &MaxTopicSize,
        >,
    ) -> PyResult<Bound<'a, PyAny>> {
        let compression_algorithm = match compression_algorithm {
            Some(algo) => CompressionAlgorithm::from_str(&algo)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?,
            None => CompressionAlgorithm::default(),
        };

        let expiry = message_expiry.map_or(RustIggyExpiry::ServerDefault, RustIggyExpiry::from);

        let max_size = max_topic_size.map_or(RustMaxTopicSize::ServerDefault, RustMaxTopicSize::from);

        let stream = Identifier::try_from(stream)?;
        let inner = self.inner.clone();

        future_into_py(py, async move {
            inner
                .create_topic(
                    &stream,
                    &name,
                    partitions_count,
                    compression_algorithm,
                    replication_factor,
                    expiry,
                    max_size,
                )
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(())
        })
    }

    /// Gets topic by stream and id.
    /// Returns Option of topic details or a PyRuntimeError on failure.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[TopicDetails | None]", imports=("collections.abc")))]
    fn get_topic<'a>(
        &self,
        py: Python<'a>,
        stream_id: PyIdentifier,
        topic_id: PyIdentifier,
    ) -> PyResult<Bound<'a, PyAny>> {
        let stream_id = Identifier::try_from(stream_id)?;
        let topic_id = Identifier::try_from(topic_id)?;
        let inner = self.inner.clone();

        future_into_py(py, async move {
            let topic = inner
                .get_topic(&stream_id, &topic_id)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(topic.map(TopicDetails::from))
        })
    }

    /// Get all topics in a stream.
    ///
    /// Args:
    ///     stream_id: Stream identifier as `str | int`.
    ///
    /// Returns:
    ///     An awaitable that resolves to `list[Topic]`.
    ///
    /// Raises:
    ///     PyRuntimeError: If the identifier is invalid or the request fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[list[Topic]]", imports=("collections.abc")))]
    fn get_topics<'a>(
        &self,
        py: Python<'a>,
        stream_id: PyIdentifier,
    ) -> PyResult<Bound<'a, PyAny>> {
        let stream_id = Identifier::try_from(stream_id)?;
        let inner = self.inner.clone();

        future_into_py(py, async move {
            let topics = inner
                .get_topics(&stream_id)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(topics.into_iter().map(Topic::from).collect::<Vec<_>>())
        })
    }

    /// Update an existing topic.
    ///
    /// This is a full replacement: any optional parameter left unset is reset to
    /// its server default rather than preserved.
    ///
    /// Args:
    ///     stream_id: Stream identifier as `str | int`.
    ///     topic_id: Topic identifier as `str | int`.
    ///     name: New topic name as `str`.
    ///     compression_algorithm: Compression algorithm as `str | None`.
    ///     replication_factor: Replication factor as `int | None`.
    ///     message_expiry: Message expiry as `IggyExpiry | None`.
    ///     max_topic_size: Maximum topic size as `MaxTopicSize | None`.
    ///
    /// Returns:
    ///     An awaitable that resolves to `None` when the topic is updated.
    ///
    /// Raises:
    ///     PyRuntimeError: If an argument is invalid or the request fails.
    #[pyo3(
        signature = (stream_id, topic_id, name, compression_algorithm = None, replication_factor = None, message_expiry = None, max_topic_size = None)
    )]
    #[allow(clippy::too_many_arguments)]
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn update_topic<'a>(
        &self,
        py: Python<'a>,
        stream_id: PyIdentifier,
        topic_id: PyIdentifier,
        name: String,
        #[gen_stub(override_type(type_repr = "builtins.str | None"))] compression_algorithm: Option<
            String,
        >,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))] replication_factor: Option<
            u8,
        >,
        #[gen_stub(override_type(type_repr = "IggyExpiry | None"))] message_expiry: Option<
            &IggyExpiry,
        >,
        #[gen_stub(override_type(type_repr = "MaxTopicSize | None"))] max_topic_size: Option<
            &MaxTopicSize,
        >,
    ) -> PyResult<Bound<'a, PyAny>> {
        let compression_algorithm = match compression_algorithm {
            Some(algo) => CompressionAlgorithm::from_str(&algo)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?,
            None => CompressionAlgorithm::default(),
        };

        let expiry = message_expiry.map_or(RustIggyExpiry::ServerDefault, RustIggyExpiry::from);

        let max_size = max_topic_size.map_or(RustMaxTopicSize::ServerDefault, RustMaxTopicSize::from);

        let stream_id = Identifier::try_from(stream_id)?;
        let topic_id = Identifier::try_from(topic_id)?;
        let inner = self.inner.clone();

        future_into_py(py, async move {
            inner
                .update_topic(
                    &stream_id,
                    &topic_id,
                    &name,
                    compression_algorithm,
                    replication_factor,
                    expiry,
                    max_size,
                )
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(())
        })
    }

    /// Delete a topic from a stream.
    ///
    /// Args:
    ///     stream_id: Stream identifier as `str | int`.
    ///     topic_id: Topic identifier as `str | int`.
    ///
    /// Returns:
    ///     An awaitable that resolves to `None` when the topic is deleted.
    ///
    /// Raises:
    ///     PyRuntimeError: If an identifier is invalid or the request fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn delete_topic<'a>(
        &self,
        py: Python<'a>,
        stream_id: PyIdentifier,
        topic_id: PyIdentifier,
    ) -> PyResult<Bound<'a, PyAny>> {
        let stream_id = Identifier::try_from(stream_id)?;
        let topic_id = Identifier::try_from(topic_id)?;
        let inner = self.inner.clone();

        future_into_py(py, async move {
            inner
                .delete_topic(&stream_id, &topic_id)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(())
        })
    }

    /// Purge all messages from a topic.
    ///
    /// Args:
    ///     stream_id: Stream identifier as `str | int`.
    ///     topic_id: Topic identifier as `str | int`.
    ///
    /// Returns:
    ///     An awaitable that resolves to `None` when the topic is purged.
    ///
    /// Raises:
    ///     PyRuntimeError: If an identifier is invalid or the request fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn purge_topic<'a>(
        &self,
        py: Python<'a>,
        stream_id: PyIdentifier,
        topic_id: PyIdentifier,
    ) -> PyResult<Bound<'a, PyAny>> {
        let stream_id = Identifier::try_from(stream_id)?;
        let topic_id = Identifier::try_from(topic_id)?;
        let inner = self.inner.clone();

        future_into_py(py, async move {
            inner
                .purge_topic(&stream_id, &topic_id)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(())
        })
    }

    /// Create a consumer group for a stream and topic.
    ///
    /// Args:
    ///     stream_id: Stream identifier as `str | int`.
    ///     topic_id: Topic identifier as `str | int`.
    ///     name: Consumer group name as `str`.
    ///
    /// Returns:
    ///     An awaitable that resolves to `None` when the consumer group is created.
    ///
    /// Raises:
    ///     PyValueError: If an identifier is invalid.
    ///     PyRuntimeError: If the request fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn create_consumer_group<'a>(
        &self,
        py: Python<'a>,
        stream_id: PyIdentifier,
        topic_id: PyIdentifier,
        name: String,
    ) -> PyResult<Bound<'a, PyAny>> {
        let stream_id = Identifier::try_from(stream_id)?;
        let topic_id = Identifier::try_from(topic_id)?;
        let inner = self.inner.clone();

        future_into_py(py, async move {
            inner
                .create_consumer_group(&stream_id, &topic_id, &name)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(())
        })
    }

    /// Retrieve details for a consumer group from the specified stream and topic.
    ///
    /// Args:
    ///     stream_id: Stream identifier as `str | int`.
    ///     topic_id: Topic identifier as `str | int`.
    ///     group_id: Consumer group identifier as `str | int`.
    ///
    /// Returns:
    ///     An awaitable that resolves to `ConsumerGroupDetails` if the consumer group exists,
    ///     or `None` otherwise.
    ///
    /// Raises:
    ///     PyValueError: If an identifier is invalid.
    ///     PyRuntimeError: If the request fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[ConsumerGroupDetails | None]", imports=("collections.abc")))]
    fn get_consumer_group<'a>(
        &self,
        py: Python<'a>,
        stream_id: PyIdentifier,
        topic_id: PyIdentifier,
        group_id: PyIdentifier,
    ) -> PyResult<Bound<'a, PyAny>> {
        let stream_id = Identifier::try_from(stream_id)?;
        let topic_id = Identifier::try_from(topic_id)?;
        let group_id = Identifier::try_from(group_id)?;
        let inner = self.inner.clone();

        future_into_py(py, async move {
            let group = inner
                .get_consumer_group(&stream_id, &topic_id, &group_id)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(group.map(PyConsumerGroupDetails::from))
        })
    }

    /// Get all consumer groups for the specified stream and topic.
    ///
    /// Args:
    ///     stream_id: Stream identifier as `str | int`.
    ///     topic_id: Topic identifier as `str | int`.
    ///
    /// Returns:
    ///     An awaitable that resolves to `list[ConsumerGroup]`.
    ///
    /// Raises:
    ///     PyValueError: If an identifier is invalid.
    ///     PyRuntimeError: If the request fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[list[ConsumerGroup]]", imports=("collections.abc")))]
    fn get_consumer_groups<'a>(
        &self,
        py: Python<'a>,
        stream_id: PyIdentifier,
        topic_id: PyIdentifier,
    ) -> PyResult<Bound<'a, PyAny>> {
        let stream_id = Identifier::try_from(stream_id)?;
        let topic_id = Identifier::try_from(topic_id)?;
        let inner = self.inner.clone();

        future_into_py(py, async move {
            let groups = inner
                .get_consumer_groups(&stream_id, &topic_id)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(groups
                .into_iter()
                .map(PyConsumerGroup::from)
                .collect::<Vec<_>>())
        })
    }

    /// Sends a list of messages to the specified topic.
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
            .map(|item| {
                let msg: PyRef<'_, SendMessage> = item.extract()?;
                Ok::<_, PyErr>(msg.clone())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut messages: Vec<RustMessage> = messages
            .into_iter()
            .map(|message| message.inner)
            .collect::<Vec<_>>();

        let stream = Identifier::try_from(stream)?;
        let topic = Identifier::try_from(topic)?;
        let partitioning = Partitioning::partition_id(partitioning);
        let inner = self.inner.clone();

        future_into_py(py, async move {
            inner
                .send_messages(&stream, &topic, &partitioning, messages.as_mut())
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(())
        })
    }

    /// Polls for messages from the specified topic and partition.
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
        let stream = Identifier::try_from(stream)?;
        let topic = Identifier::try_from(topic)?;
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
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            let messages = polled_messages
                .messages
                .into_iter()
                .map(|m| ReceiveMessage {
                    inner: m,
                    partition_id,
                })
                .collect::<Vec<_>>();
            Ok(messages)
        })
    }

    /// Creates a new consumer group consumer.
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
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[IggyConsumer]", imports=("collections.abc")))]
    fn consumer_group<'a>(
        &self,
        py: Python<'a>,
        name: &str,
        stream: &str,
        topic: &str,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))] partition_id: Option<u32>,
        #[gen_stub(override_type(type_repr = "PollingStrategy | None"))] polling_strategy: Option<
            &PollingStrategy,
        >,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))] batch_length: Option<u32>,
        #[gen_stub(override_type(type_repr = "AutoCommit | None"))] auto_commit: Option<
            &AutoCommit,
        >,
        create_consumer_group_if_not_exists: bool,
        auto_join_consumer_group: bool,
        #[gen_stub(override_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
        poll_interval: Option<Py<PyDelta>>,
        #[gen_stub(override_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
        polling_retry_interval: Option<Py<PyDelta>>,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))] init_retries: Option<u32>,
        #[gen_stub(override_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
        init_retry_interval: Option<Py<PyDelta>>,
        allow_replay: bool,
    ) -> PyResult<Bound<'a, PyAny>> {
        let mut builder = self
            .inner
            .consumer_group(name, stream, topic)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
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
        let mut consumer = builder.build();

        future_into_py(py, async move {
            consumer
                .init()
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(IggyConsumer {
                inner: Arc::new(Mutex::new(consumer)),
            })
        })
    }
}

define_stub_info_gatherer!(stub_info);
