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

use std::{str::FromStr, sync::Arc, time::Duration};

use ext_php_rs::{
    exception::{PhpException, PhpResult},
    php_class, php_impl,
};
use iggy::prelude::{
    CompressionAlgorithm, Consumer as RustConsumer, Identifier, IggyClient as RustIggyClient,
    IggyClientBuilder, IggyDuration, IggyExpiry, IggyMessage as RustMessage, MaxTopicSize,
    Partitioning, PollingStrategy as RustPollingStrategy, *,
};
use php_tokio::EventLoop;
use tokio::sync::Mutex;

use crate::async_consumer::IggyAsyncConsumer;
use crate::consumer::AutoCommit;
use crate::identifier::PhpIdentifier;
use crate::receive_message::{PollingStrategy, ReceiveMessage};
use crate::send_message::SendMessage;
use crate::stream::StreamDetails;
use crate::topic::TopicDetails;

type AsyncPhpResult<T = ()> = Result<T, String>;

/// A Fiber/Revolt-aware PHP client for Iggy.
///
/// Methods suspend the current PHP Fiber while the Rust Tokio future is pending.
/// Use `IggyAsyncClient::init()` and `IggyAsyncClient::wakeup()` to bridge Tokio
/// readiness notifications into the PHP event loop.
#[php_class]
pub struct IggyAsyncClient {
    inner: Arc<RustIggyClient>,
}

#[php_impl]
impl IggyAsyncClient {
    /// Constructs a new async client from a TCP server address.
    #[php(constructor)]
    pub fn __construct(conn: Option<String>) -> PhpResult<Self> {
        let client = IggyClientBuilder::new()
            .with_tcp()
            .with_server_address(conn.unwrap_or_else(|| "127.0.0.1:8090".to_string()))
            .build()
            .map_err(to_php_exception)?;

        Ok(Self {
            inner: Arc::new(client),
        })
    }

    /// Initializes the php-tokio bridge and returns a readable file descriptor.
    ///
    /// Register this descriptor with Revolt and call `IggyAsyncClient::wakeup()`
    /// whenever it becomes readable.
    pub fn init() -> PhpResult<u64> {
        EventLoop::init()
    }

    /// Resumes PHP Fibers whose Tokio futures have completed.
    pub fn wakeup() -> PhpResult {
        EventLoop::wakeup()
    }

    /// Clears the thread-local php-tokio bridge state.
    pub fn shutdown() {
        EventLoop::shutdown();
    }

    /// Constructs a new async client from a connection string.
    pub fn from_connection_string(connection_string: String) -> PhpResult<Self> {
        let client =
            RustIggyClient::from_connection_string(&connection_string).map_err(to_php_exception)?;

        Ok(Self {
            inner: Arc::new(client),
        })
    }

    /// Sends a ping request to the server.
    pub fn ping(&self) -> AsyncPhpResult {
        let inner = self.inner.clone();

        EventLoop::suspend_on(async move { inner.ping().await.map_err(to_async_exception) })
    }

    /// Logs in the user with the given credentials.
    pub fn login_user(&self, username: String, password: String) -> AsyncPhpResult {
        let inner = self.inner.clone();

        EventLoop::suspend_on(async move {
            inner
                .login_user(&username, &password)
                .await
                .map(|_| ())
                .map_err(to_async_exception)
        })
    }

    /// Connects the client to its service.
    pub fn connect(&self) -> AsyncPhpResult {
        let inner = self.inner.clone();

        EventLoop::suspend_on(async move { inner.connect().await.map_err(to_async_exception) })
    }

    /// Creates a new stream.
    pub fn create_stream(&self, name: String) -> AsyncPhpResult {
        let inner = self.inner.clone();

        EventLoop::suspend_on(async move {
            inner
                .create_stream(&name)
                .await
                .map(|_| ())
                .map_err(to_async_exception)
        })
    }

    /// Gets a stream by id or name.
    pub fn get_stream(&self, stream_id: PhpIdentifier) -> AsyncPhpResult<Option<StreamDetails>> {
        let stream_id = Identifier::from(stream_id);
        let inner = self.inner.clone();

        EventLoop::suspend_on(async move {
            inner
                .get_stream(&stream_id)
                .await
                .map(|stream| stream.map(StreamDetails::from))
                .map_err(to_async_exception)
        })
    }

    /// Creates a topic.
    ///
    /// message_expiry_micros is null for server default.
    #[allow(clippy::too_many_arguments)]
    pub fn create_topic(
        &self,
        stream: PhpIdentifier,
        name: String,
        partitions_count: u32,
        compression_algorithm: Option<String>,
        replication_factor: Option<u8>,
        message_expiry_micros: Option<u64>,
        max_topic_size: Option<u64>,
    ) -> AsyncPhpResult {
        let compression_algorithm = match compression_algorithm {
            Some(value) => CompressionAlgorithm::from_str(&value).map_err(to_async_exception)?,
            None => CompressionAlgorithm::default(),
        };
        let expiry = message_expiry_micros.map_or(IggyExpiry::ServerDefault, |micros| {
            IggyExpiry::ExpireDuration(iggy_duration_from_micros(micros))
        });
        let max_size = max_topic_size.map_or(MaxTopicSize::ServerDefault, MaxTopicSize::from);
        let stream = Identifier::from(stream);
        let inner = self.inner.clone();

        EventLoop::suspend_on(async move {
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
                .map(|_| ())
                .map_err(to_async_exception)
        })
    }

    /// Gets a topic by stream and topic id/name.
    pub fn get_topic(
        &self,
        stream_id: PhpIdentifier,
        topic_id: PhpIdentifier,
    ) -> AsyncPhpResult<Option<TopicDetails>> {
        let stream_id = Identifier::from(stream_id);
        let topic_id = Identifier::from(topic_id);
        let inner = self.inner.clone();

        EventLoop::suspend_on(async move {
            inner
                .get_topic(&stream_id, &topic_id)
                .await
                .map(|topic| topic.map(TopicDetails::from))
                .map_err(to_async_exception)
        })
    }

    /// Sends messages to a topic.
    pub fn send_messages(
        &self,
        stream: PhpIdentifier,
        topic: PhpIdentifier,
        partition_id: u32,
        messages: Vec<&SendMessage>,
    ) -> AsyncPhpResult {
        let stream = Identifier::from(stream);
        let topic = Identifier::from(topic);
        let partitioning = Partitioning::partition_id(partition_id);
        let mut messages: Vec<RustMessage> = messages
            .into_iter()
            .map(|message| (*message).clone().inner)
            .collect();
        let inner = self.inner.clone();

        EventLoop::suspend_on(async move {
            inner
                .send_messages(&stream, &topic, &partitioning, messages.as_mut())
                .await
                .map_err(to_async_exception)
        })
    }

    /// Polls messages from the specified topic and partition.
    pub fn poll_messages(
        &self,
        stream: PhpIdentifier,
        topic: PhpIdentifier,
        partition_id: u32,
        polling_strategy: &PollingStrategy,
        count: u32,
        auto_commit: bool,
    ) -> AsyncPhpResult<Vec<ReceiveMessage>> {
        let consumer = RustConsumer::default();
        let stream = Identifier::from(stream);
        let topic = Identifier::from(topic);
        let strategy: RustPollingStrategy = polling_strategy.into();
        let inner = self.inner.clone();

        EventLoop::suspend_on(async move {
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
                .map_err(to_async_exception)?;

            Ok(polled_messages
                .messages
                .into_iter()
                .map(|message| ReceiveMessage {
                    inner: message,
                    partition_id,
                })
                .collect())
        })
    }

    /// Creates and initializes a consumer group consumer.
    #[allow(clippy::too_many_arguments)]
    pub fn consumer_group(
        &self,
        name: String,
        stream: String,
        topic: String,
        partition_id: Option<u32>,
        polling_strategy: Option<&PollingStrategy>,
        batch_length: Option<u32>,
        auto_commit: Option<&AutoCommit>,
        create_consumer_group_if_not_exists: bool,
        auto_join_consumer_group: bool,
        poll_interval_micros: Option<u64>,
        polling_retry_interval_micros: Option<u64>,
        init_retries: Option<u32>,
        init_retry_interval_micros: Option<u64>,
        allow_replay: bool,
    ) -> AsyncPhpResult<IggyAsyncConsumer> {
        let mut builder = self
            .inner
            .consumer_group(&name, &stream, &topic)
            .map_err(to_async_exception)?
            .without_encryptor()
            .partition(partition_id);

        builder = if create_consumer_group_if_not_exists {
            builder.create_consumer_group_if_not_exists()
        } else {
            builder.do_not_create_consumer_group_if_not_exists()
        };
        builder = if auto_join_consumer_group {
            builder.auto_join_consumer_group()
        } else {
            builder.do_not_auto_join_consumer_group()
        };
        if let Some(polling_strategy) = polling_strategy {
            builder = builder.polling_strategy(polling_strategy.into());
        }
        if let Some(batch_length) = batch_length {
            builder = builder.batch_length(batch_length);
        }
        if let Some(auto_commit) = auto_commit {
            builder = builder.auto_commit(auto_commit.into());
        }
        builder = match poll_interval_micros {
            Some(micros) => builder.poll_interval(iggy_duration_from_micros(micros)),
            None => builder.without_poll_interval(),
        };
        if let Some(micros) = polling_retry_interval_micros {
            builder = builder.polling_retry_interval(iggy_duration_from_micros(micros));
        }

        match (init_retries, init_retry_interval_micros) {
            (Some(retries), Some(micros)) => {
                builder = builder.init_retries(retries, iggy_duration_from_micros(micros));
            }
            (Some(_), None) => {
                return Err(
                    "'init_retry_interval_micros' is required if 'init_retries' is set".to_string(),
                );
            }
            (None, Some(_)) => {
                return Err(
                    "'init_retries' is required if 'init_retry_interval_micros' is set".to_string(),
                );
            }
            (None, None) => {}
        }
        if allow_replay {
            builder = builder.allow_replay();
        }

        let mut consumer = builder.build();
        EventLoop::suspend_on(async move {
            consumer.init().await.map_err(to_async_exception)?;
            Ok(IggyAsyncConsumer {
                inner: Arc::new(Mutex::new(consumer)),
            })
        })
    }
}

pub unsafe extern "C" fn request_shutdown(_type: i32, _module_number: i32) -> i32 {
    EventLoop::shutdown();
    0
}

fn to_php_exception(error: impl std::fmt::Display) -> PhpException {
    PhpException::default(error.to_string())
}

fn to_async_exception(error: impl std::fmt::Display) -> String {
    error.to_string()
}

fn iggy_duration_from_micros(micros: u64) -> IggyDuration {
    IggyDuration::new(Duration::from_micros(micros))
}
