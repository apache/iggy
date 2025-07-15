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
use pyo3::types::PyList;
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use pyo3_stub_gen::{define_stub_info_gatherer, impl_stub_type};

use crate::receive_message::{PollingStrategy, ReceiveMessage};
use crate::send_message::SendMessage;
use crate::stream::StreamDetails;
use crate::topic::TopicDetails;

/// A Python class representing the Iggy client.
/// It wraps the RustIggyClient and provides asynchronous functionality
/// through the contained runtime.
#[gen_stub_pyclass]
#[pyclass]
pub struct IggyClient {
    inner: Arc<RustIggyClient>,
}

#[derive(FromPyObject)]
enum PyIdentifier {
    #[pyo3(transparent, annotation = "str")]
    String(String),
    #[pyo3(transparent, annotation = "int")]
    Int(u32),
}
impl_stub_type!(PyIdentifier = String | isize);

impl From<PyIdentifier> for Identifier {
    fn from(py_identifier: PyIdentifier) -> Self {
        match py_identifier {
            PyIdentifier::String(s) => Identifier::from_str(&s).unwrap(),
            PyIdentifier::Int(i) => Identifier::numeric(i).unwrap(),
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl IggyClient {
    /// Constructs a new IggyClient.
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

    /// Sends a ping request to the server to check connectivity.
    ///
    /// Returns `Ok(())` if the server responds successfully, or a `PyRuntimeError`
    /// if the connection fails.
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
    fn login_user<'a>(
        &self,
        py: Python<'a>,
        username: String,
        password: String,
    ) -> PyResult<Bound<'a, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner.login_user(&username, &password).await.map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e:?}"))
            })?;
            Ok(())
        })
    }

    /// Connects the IggyClient to its service.
    ///
    /// Returns Ok(()) on successful connection or a PyRuntimeError on failure.
    fn connect<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner.connect().await.map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e:?}"))
            })?;
            Ok(())
        })
    }

    /// Creates a new stream with the provided ID and name.
    ///
    /// Returns Ok(()) on successful stream creation or a PyRuntimeError on failure.
    #[pyo3(signature = (name, stream_id = None))]
    fn create_stream<'a>(
        &self,
        py: Python<'a>,
        name: String,
        stream_id: Option<u32>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner.create_stream(&name, stream_id).await.map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e:?}"))
            })?;
            Ok(())
        })
    }

    /// Gets stream by id.
    ///
    /// Returns Option of stream details or a PyRuntimeError on failure.
    fn get_stream<'a>(
        &self,
        py: Python<'a>,
        stream_id: PyIdentifier,
    ) -> PyResult<Bound<'a, PyAny>> {
        let stream_id = Identifier::from(stream_id);
        let inner = self.inner.clone();

        future_into_py(py, async move {
            let stream = inner.get_stream(&stream_id).await.map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e:?}"))
            })?;
            Ok(stream.map(StreamDetails::from))
        })
    }

    /// Creates a new topic with the given parameters.
    ///
    /// Returns Ok(()) on successful topic creation or a PyRuntimeError on failure.
    #[pyo3(
        signature = (stream, name, partitions_count, compression_algorithm = None, topic_id = None, replication_factor = None)
    )]
    #[allow(clippy::too_many_arguments)]
    fn create_topic<'a>(
        &self,
        py: Python<'a>,
        stream: PyIdentifier,
        name: String,
        partitions_count: u32,
        compression_algorithm: Option<String>,
        topic_id: Option<u32>,
        replication_factor: Option<u8>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let compression_algorithm = match compression_algorithm {
            Some(algo) => CompressionAlgorithm::from_str(&algo).map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e:?}"))
            })?,
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
                    topic_id,
                    IggyExpiry::NeverExpire,
                    MaxTopicSize::ServerDefault,
                )
                .await
                .map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e:?}"))
                })?;
            Ok(())
        })
    }

    /// Gets topic by stream and id.
    ///
    /// Returns Option of topic details or a PyRuntimeError on failure.
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
            let topic = inner.get_topic(&stream_id, &topic_id).await.map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e:?}"))
            })?;
            Ok(topic.map(TopicDetails::from))
        })
    }

    /// Sends a list of messages to the specified topic.
    ///
    /// Returns Ok(()) on successful sending or a PyRuntimeError on failure.
    fn send_messages<'a>(
        &self,
        py: Python<'a>,
        stream: PyIdentifier,
        topic: PyIdentifier,
        partitioning: u32,
        messages: &Bound<'_, PyList>,
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
                .map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e:?}"))
                })?;
            Ok(())
        })
    }

    /// Polls for messages from the specified topic and partition.
    ///
    /// Returns a list of received messages or a PyRuntimeError on failure.
    #[allow(clippy::too_many_arguments)]
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
        let strategy: RustPollingStrategy = (*polling_strategy).into();

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
                .map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e:?}"))
                })?;
            let messages = polled_messages
                .messages
                .into_iter()
                .map(ReceiveMessage::from_rust_message)
                .collect::<Vec<_>>();
            Ok(messages)
        })
    }
}

define_stub_info_gatherer!(stub_info);
