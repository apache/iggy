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
    Consumer as RustConsumer, IggyClient as RustIggyClient, IggyMessage as RustMessage,
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
    AutoCommit, ConsumerGroup, ConsumerGroupDetails, IggyConsumer, py_delta_to_iggy_duration,
};
use crate::identity::{IdentityInfo, PyIdentifier};
use crate::message::{PollingStrategy, ReceiveMessage, SendMessage};
use crate::stream::StreamDetails;
use crate::topic::{Topic, TopicDetails};
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
    /// Create an `IggyClient`.
    ///
    /// Use this constructor when you want to configure the connection with
    /// explicit keyword arguments instead of a connection string.
    ///
    /// Args:
    ///     server_address: Server address as `str` in `host:port` form. Pass `None`
    ///         to use the TCP transport default, currently `127.0.0.1:8090`.
    ///     reconnection_max_retries: Maximum number of reconnect attempts as `int`
    ///         after a disconnect. Pass `None` to use the TCP transport default,
    ///         currently unlimited retries.
    ///     reconnection_interval: Delay between reconnect attempts as
    ///         `datetime.timedelta`. Pass `None` to use the TCP transport default,
    ///         currently 1 second.
    ///     reestablish_after: Delay before attempting to reestablish the
    ///         connection as `datetime.timedelta`. Pass `None` to use the TCP
    ///         transport default, currently 5 seconds.
    ///     tls_enabled: Whether to use TLS for the connection as `bool`. Pass
    ///         `None` to use the TCP transport default, currently `False`.
    ///     tls_domain: Server name as `str` to validate against the TLS certificate.
    ///         Pass `None` to use the TCP transport default, currently an empty
    ///         string that triggers auto-detection from `server_address`. This
    ///         option is ignored when `tls_enabled` is `False`.
    ///     tls_ca_file: Path as `str` to a CA certificate file used to validate
    ///         the server certificate. Pass `None` to use the TCP transport
    ///         default, currently no CA file. This option is ignored when
    ///         `tls_enabled` is `False`.
    ///     tls_validate_certificate: Whether to validate the server certificate.
    ///         Accepts `bool | None`. Pass `None` to use the TCP transport
    ///         default, currently `True`. This option is ignored when
    ///         `tls_enabled` is `False`.
    ///     no_delay: Whether to send packets immediately instead of allowing the
    ///         socket to coalesce small writes. Accepts `bool`. Defaults to
    ///         `False`.
    ///
    /// Returns:
    ///     A configured `IggyClient`.
    ///
    /// Raises:
    ///     PyRuntimeError: If the connection settings are invalid.
    #[allow(clippy::too_many_arguments)]
    #[new]
    #[pyo3(signature = (
        *,
        server_address = None,
        reconnection_max_retries = None,
        reconnection_interval = None,
        reestablish_after = None,
        tls_enabled = None,
        tls_domain = None,
        tls_ca_file = None,
        tls_validate_certificate = None,
        no_delay = false
    ))]
    fn new(
        #[gen_stub(override_type(type_repr = "builtins.str | None"))] server_address: Option<
            String,
        >,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))]
        reconnection_max_retries: Option<u32>,
        #[gen_stub(override_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
        reconnection_interval: Option<Py<PyDelta>>,
        #[gen_stub(override_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
        reestablish_after: Option<Py<PyDelta>>,
        #[gen_stub(override_type(type_repr = "builtins.bool | None"))] tls_enabled: Option<bool>,
        #[gen_stub(override_type(type_repr = "builtins.str | None"))] tls_domain: Option<String>,
        #[gen_stub(override_type(type_repr = "builtins.str | None"))] tls_ca_file: Option<String>,
        #[gen_stub(override_type(type_repr = "builtins.bool | None"))]
        tls_validate_certificate: Option<bool>,
        no_delay: bool,
    ) -> PyResult<Self> {
        let mut builder = IggyClientBuilder::new()
            .with_tcp()
            .with_server_address(server_address.unwrap_or_else(|| "127.0.0.1:8090".to_string()));
        let tls_enabled = tls_enabled.unwrap_or(false);

        if let Some(retries) = reconnection_max_retries {
            builder = builder.with_reconnection_max_retries(Some(retries));
        }
        if let Some(interval) = reconnection_interval {
            builder = builder.with_reconnection_interval(py_delta_to_iggy_duration(&interval));
        }
        if let Some(duration) = reestablish_after {
            builder = builder.with_reestablish_after(py_delta_to_iggy_duration(&duration));
        }
        if tls_enabled {
            builder = builder.with_tls_enabled(true);
            if let Some(domain) = tls_domain {
                builder = builder.with_tls_domain(domain);
            }
            if let Some(ca_file) = tls_ca_file {
                builder = builder.with_tls_ca_file(ca_file);
            }
            if let Some(validate) = tls_validate_certificate {
                builder = builder.with_tls_validate_certificate(validate);
            }
        }
        if no_delay {
            builder = builder.with_no_delay();
        }

        let client = builder
            .build()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(IggyClient {
            inner: Arc::new(client),
        })
    }

    /// Create an `IggyClient` from a connection string.
    ///
    /// Use this when the connection configuration is already available as a single string.
    ///
    /// Args:
    ///     connection_string: Connection string as `str` describing how to connect
    ///         to the server.
    ///
    /// Returns:
    ///     A configured `IggyClient`.
    ///
    /// Raises:
    ///     PyRuntimeError: If the connection string is invalid.
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

    /// Check whether the server is reachable.
    ///
    /// Sends a ping request and waits for the server to respond.
    ///
    /// Returns:
    ///     An awaitable that resolves to `None` when the server responds.
    ///
    /// Raises:
    ///     PyRuntimeError: If the request fails or the server cannot be reached.
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

    /// Authenticate with a username and password.
    ///
    /// Args:
    ///     username: Username as `str`.
    ///     password: Password as `str`.
    ///
    /// Returns:
    ///     An awaitable that resolves to `IdentityInfo` for the authenticated user.
    ///
    /// Raises:
    ///     PyRuntimeError: If authentication fails or the request cannot be completed.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[IdentityInfo]", imports=("collections.abc")))]
    fn login_user<'a>(
        &self,
        py: Python<'a>,
        username: String,
        password: String,
    ) -> PyResult<Bound<'a, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let identity_info = inner
                .login_user(&username, &password)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Python::attach(|py| Py::new(py, IdentityInfo::from(identity_info)))
        })
    }

    /// Open the connection to the server.
    ///
    /// Returns:
    ///     An awaitable that resolves to `None` when the connection is established.
    ///
    /// Raises:
    ///     PyRuntimeError: If the connection attempt fails.
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

    /// Create a stream.
    ///
    /// Args:
    ///     name: Stream name as `str`.
    ///
    /// Returns:
    ///     An awaitable that resolves to `StreamDetails` for the created stream.
    ///
    /// Raises:
    ///     PyRuntimeError: If the stream cannot be created.
    #[pyo3(signature = (name))]
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[StreamDetails]", imports=("collections.abc")))]
    fn create_stream<'a>(&self, py: Python<'a>, name: String) -> PyResult<Bound<'a, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let stream = inner
                .create_stream(&name)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Python::attach(|py| Py::new(py, StreamDetails::from(stream)))
        })
    }

    /// Get a stream by identifier.
    ///
    /// Args:
    ///     stream_id: Stream identifier as `str | int`.
    ///
    /// Returns:
    ///     An awaitable that resolves to `StreamDetails` if the stream exists,
    ///     or `None` if it does not.
    ///
    /// Raises:
    ///     PyRuntimeError: If the identifier is invalid or the request fails.
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

    /// Create a topic in a stream.
    ///
    /// Args:
    ///     stream: Stream identifier as `str | int`.
    ///     name: Topic name as `str`.
    ///     partitions_count: Number of partitions to create as `int`.
    ///     compression_algorithm: Compression algorithm as `str | None`. Supported
    ///         values are `\"none\"` and `\"gzip\"`, case-insensitive. If `None`,
    ///         the default compression setting is used.
    ///     replication_factor: Replication factor as `int | None` from `0` to `255`.
    ///         Passing `0` or `None` uses the server default. The current server
    ///         default is `1`.
    ///     message_expiry: Message retention period as `datetime.timedelta | None`.
    ///         Use `datetime.timedelta(0)` to request the server default, which
    ///         currently means messages do not expire. If `None`, the server default
    ///         is also used.
    ///     max_topic_size: Maximum topic size in bytes as `int | None`. Use `0` to request the
    ///         server default, which is currently unlimited. If `None`, the server
    ///         default is also used.
    ///
    /// Returns:
    ///     An awaitable that resolves to `TopicDetails` for the created topic.
    ///
    /// Raises:
    ///     PyRuntimeError: If an argument is invalid or the topic cannot be created.
    #[pyo3(
        signature = (stream, name, partitions_count, compression_algorithm = None, replication_factor = None, message_expiry = None, max_topic_size = None)
    )]
    #[allow(clippy::too_many_arguments)]
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[TopicDetails]", imports=("collections.abc")))]
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
        #[gen_stub(override_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
        message_expiry: Option<Py<PyDelta>>,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))] max_topic_size: Option<u64>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let compression_algorithm = match compression_algorithm {
            Some(algo) => CompressionAlgorithm::from_str(&algo)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?,
            None => CompressionAlgorithm::default(),
        };

        let expiry = match message_expiry {
            Some(delta) => IggyExpiry::ExpireDuration(py_delta_to_iggy_duration(&delta)),
            None => IggyExpiry::ServerDefault,
        };

        let max_size = max_topic_size.map_or(MaxTopicSize::ServerDefault, MaxTopicSize::from);

        let stream = Identifier::try_from(stream)?;
        let inner = self.inner.clone();

        future_into_py(py, async move {
            let topic = inner
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
            Python::attach(|py| Py::new(py, TopicDetails::from(topic)))
        })
    }

    /// Get a topic by stream and topic identifier.
    ///
    /// Args:
    ///     stream_id: Stream identifier as `str | int`.
    ///     topic_id: Topic identifier as `str | int`.
    ///
    /// Returns:
    ///     An awaitable that resolves to `TopicDetails` if the topic exists,
    ///     or `None` if it does not.
    ///
    /// Raises:
    ///     PyRuntimeError: If an identifier is invalid or the request fails.
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
    ///     message_expiry: Message expiry as `datetime.timedelta | None`.
    ///     max_topic_size: Maximum topic size in bytes as `int | None`.
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
        #[gen_stub(override_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
        message_expiry: Option<Py<PyDelta>>,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))] max_topic_size: Option<u64>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let compression_algorithm = match compression_algorithm {
            Some(algo) => CompressionAlgorithm::from_str(&algo)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?,
            None => CompressionAlgorithm::default(),
        };

        let expiry = match message_expiry {
            Some(delta) => IggyExpiry::ExpireDuration(py_delta_to_iggy_duration(&delta)),
            None => IggyExpiry::ServerDefault,
        };

        let max_size = max_topic_size.map_or(MaxTopicSize::ServerDefault, MaxTopicSize::from);

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
            Ok(group.map(ConsumerGroupDetails::from))
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
                .map(ConsumerGroup::from)
                .collect::<Vec<_>>())
        })
    }

    /// Send messages to a topic partition.
    ///
    /// Args:
    ///     stream: Stream identifier as `str | int`.
    ///     topic: Topic identifier as `str | int`.
    ///     partitioning: Partition id as `int`.
    ///     messages: Messages to publish as `list[SendMessage]`.
    ///
    /// Returns:
    ///     An awaitable that resolves to `None` after the messages are sent.
    ///
    /// Raises:
    ///     PyRuntimeError: If an identifier is invalid, the messages cannot be
    ///         converted, or the send request fails.
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

    /// Poll messages from a topic partition.
    ///
    /// Args:
    ///     stream: Stream identifier as `str | int`.
    ///     topic: Topic identifier as `str | int`.
    ///     partition_id: Partition to read from as `int`.
    ///     polling_strategy: Polling strategy as `PollingStrategy`. See
    ///         `PollingStrategy` for the available variants and their payloads.
    ///     count: Maximum number of messages to return as `int`.
    ///     auto_commit: Whether to store the consumer offset automatically after
    ///         polling as `bool`.
    ///
    /// Returns:
    ///     An awaitable that resolves to a `list[ReceiveMessage]`.
    ///
    /// Raises:
    ///     PyRuntimeError: If an identifier is invalid or the poll request fails.
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

    /// Create a consumer group consumer.
    ///
    /// Args:
    ///     name: Consumer group name as `str`.
    ///     stream: Stream identifier as `str`.
    ///     topic: Topic identifier as `str`.
    ///     partition_id: Partition id as `int | None`. If `None`, partition
    ///         assignment is left to the consumer group.
    ///     polling_strategy: Polling strategy as `PollingStrategy | None`. See
    ///         `PollingStrategy` for the available variants and their payloads.
    ///         If `None`, reading starts from the next message after the stored offset.
    ///     batch_length: Maximum number of messages to fetch per poll as `int | None`.
    ///         If `None`, the default is `1000`.
    ///     auto_commit: Offset commit policy as `AutoCommit | None`. See
    ///         `AutoCommit` for the available modes. If `None`, offsets are committed
    ///         every second and when messages are polled.
    ///     create_consumer_group_if_not_exists: Whether to create the consumer
    ///         group automatically if it does not exist as `bool`. Defaults to `True`.
    ///     auto_join_consumer_group: Whether to join the consumer group automatically
    ///         during initialization as `bool`. Defaults to `True`.
    ///     poll_interval: Delay between polling attempts as `datetime.timedelta | None`.
    ///         If `None`, no poll interval is configured.
    ///     polling_retry_interval: Delay before retrying polling after a disconnect
    ///         as `datetime.timedelta | None`. If `None`, the default is 1 second.
    ///     init_retries: Number of retries to use during initialization when the
    ///         stream or topic is not available as `int | None`. If `None`,
    ///         initialization is not retried.
    ///     init_retry_interval: Delay between initialization retries as
    ///         `datetime.timedelta | None`. Must be set together with `init_retries`.
    ///     allow_replay: Whether replaying previously available messages is allowed.
    ///         Accepts `bool`. Defaults to `False`.
    ///
    /// Returns:
    ///     An awaitable that resolves to `IggyConsumer`.
    ///
    /// Raises:
    ///     PyRuntimeError: If the configuration is invalid or the consumer cannot
    ///         be initialized.
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
