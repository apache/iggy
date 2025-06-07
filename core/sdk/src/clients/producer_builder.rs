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

use super::MAX_BATCH_LENGTH;
use crate::clients::producer_config::{BackgroundConfig, BackpressureMode, SyncConfig};
use crate::clients::producer_error_callback::ErrorCallback;
use crate::clients::producer_error_callback::LogErrorCallback;
use crate::clients::producer_sharding::{BalancedSharding, Sharding};
use crate::prelude::IggyProducer;
use iggy_binary_protocol::Client;
use iggy_common::locking::IggySharedMut;
use iggy_common::{
    EncryptorKind, Identifier, IggyByteSize, IggyDuration, IggyExpiry, MaxTopicSize, Partitioner,
    Partitioning,
};
use std::sync::Arc;

pub struct BackgroundBuilder {
    num_shards: Option<usize>,
    batch_size: Option<usize>,
    batch_length: Option<usize>,
    failure_mode: Option<BackpressureMode>,
    max_buffer_size: Option<IggyByteSize>,
    linger_time: Option<IggyDuration>,
    max_in_flight: Option<usize>,

    error_callback: Box<dyn ErrorCallback>,
    sharding: Box<dyn Sharding>,
}

impl Default for BackgroundBuilder {
    fn default() -> Self {
        let num_shards = default_shard_count();
        BackgroundBuilder {
            num_shards: Some(num_shards),
            sharding: Box::new(BalancedSharding::default()),
            error_callback: Box::new(LogErrorCallback),
            batch_size: Some(1_048_576),
            batch_length: Some(1000),
            failure_mode: Some(BackpressureMode::Block),
            max_buffer_size: Some(IggyByteSize::from(32 * 1_048_576)),
            linger_time: Some(IggyDuration::from(1000)),
            max_in_flight: Some(num_shards * num_shards),
        }
    }
}

impl BackgroundBuilder {
    /// Sets the number of messages to batch before sending them, can be combined with `interval`.
    pub fn batch_length(self, batch_length: u32) -> Self {
        Self {
            batch_length: if batch_length == 0 {
                None
            } else {
                Some(batch_length.min(MAX_BATCH_LENGTH as u32) as usize)
            },
            ..self
        }
    }

    /// Clears the batch size.
    pub fn without_batch_length(self) -> Self {
        Self {
            batch_length: None,
            ..self
        }
    }

    /// Sets the interval between sending the messages, can be combined with `batch_length`.
    pub fn linger_time(self, interval: IggyDuration) -> Self {
        Self {
            linger_time: Some(interval),
            ..self
        }
    }

    /// Clears the interval.
    pub fn without_linger_time(self) -> Self {
        Self {
            linger_time: None,
            ..self
        }
    }

    /// Sets the number of shards (background workers).
    pub fn num_shards(self, value: usize) -> Self {
        Self {
            num_shards: Some(value),
            ..self
        }
    }

    /// Sets the maximum size of a batch in bytes.
    pub fn batch_size(self, value: usize) -> Self {
        Self {
            batch_size: Some(value),
            ..self
        }
    }

    /// Sets the sharding strategy.
    /// You can pass a custom implementation that implements the `Sharding` trait.
    pub fn sharding(self, sharding: Box<dyn Sharding>) -> Self {
        Self { sharding, ..self }
    }

    /// Sets the maximum buffer size for all in-flight messages (in bytes).
    pub fn max_buffer_size(self, value: IggyByteSize) -> Self {
        Self {
            max_buffer_size: Some(value),
            ..self
        }
    }

    /// Sets the failure mode behavior (e.g., block, fail immediately, timeout).
    pub fn failure_mode(self, mode: BackpressureMode) -> Self {
        Self {
            failure_mode: Some(mode),
            ..self
        }
    }

    /// Sets the error callback for handling background sending errors.
    pub fn error_callback(self, callback: Box<dyn ErrorCallback>) -> Self {
        Self {
            error_callback: callback,
            ..self
        }
    }

    /// Sets the maximum number of in-flight batches/messages.
    pub fn max_in_flight(self, value: usize) -> Self {
        Self {
            max_in_flight: Some(value),
            ..self
        }
    }

    pub fn build(self) -> BackgroundConfig {
        BackgroundConfig {
            num_shards: self.num_shards.unwrap_or(8),
            batch_size: self.batch_size,
            batch_length: self.batch_length,
            failure_mode: self.failure_mode.unwrap_or(BackpressureMode::Block),
            max_buffer_size: self.max_buffer_size,
            linger_time: self.linger_time.unwrap_or(IggyDuration::from(1000)),
            error_callback: Arc::new(self.error_callback),
            sharding: self.sharding,
            max_in_flight: self.max_in_flight,
        }
    }
}

pub struct SyncBuilder {
    batch_length: Option<usize>,
    linger_time: Option<IggyDuration>,
}

impl Default for SyncBuilder {
    fn default() -> Self {
        Self {
            batch_length: Some(1000),
            linger_time: Some(IggyDuration::from(1000)),
        }
    }
}

impl SyncBuilder {
    /// Sets the number of messages to batch before sending them, can be combined with `interval`.
    pub fn batch_length(self, batch_length: u32) -> Self {
        Self {
            batch_length: if batch_length == 0 {
                None
            } else {
                Some(batch_length.min(MAX_BATCH_LENGTH as u32) as usize)
            },
            ..self
        }
    }

    /// Clears the batch size.
    pub fn without_batch_length(self) -> Self {
        Self {
            batch_length: None,
            ..self
        }
    }

    /// Sets the interval between sending the messages, can be combined with `batch_length`.
    pub fn linger_time(self, interval: IggyDuration) -> Self {
        Self {
            linger_time: Some(interval),
            ..self
        }
    }

    /// Clears the interval.
    pub fn without_linger_time(self) -> Self {
        Self {
            linger_time: None,
            ..self
        }
    }

    pub fn build(self) -> SyncConfig {
        SyncConfig {
            batch_length: self.batch_length.unwrap_or(MAX_BATCH_LENGTH),
            linger_time: self.linger_time,
        }
    }
}

pub enum SendMode {
    Sync(SyncConfig),
    Background(BackgroundConfig),
}

impl Default for SendMode {
    fn default() -> Self {
        SendMode::Sync(SyncBuilder::default().build())
    }
}

pub struct IggyProducerBuilder {
    client: IggySharedMut<Box<dyn Client>>,
    stream: Identifier,
    stream_name: String,
    topic: Identifier,
    topic_name: String,
    encryptor: Option<Arc<EncryptorKind>>,
    partitioner: Option<Arc<dyn Partitioner>>,
    create_stream_if_not_exists: bool,
    create_topic_if_not_exists: bool,
    topic_partitions_count: u32,
    topic_replication_factor: Option<u8>,
    send_retries_count: Option<u32>,
    send_retries_interval: Option<IggyDuration>,
    topic_message_expiry: IggyExpiry,
    topic_max_size: MaxTopicSize,
    partitioning: Option<Partitioning>,
    mode: SendMode,
}

impl IggyProducerBuilder {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        client: IggySharedMut<Box<dyn Client>>,
        stream: Identifier,
        stream_name: String,
        topic: Identifier,
        topic_name: String,
        encryptor: Option<Arc<EncryptorKind>>,
        partitioner: Option<Arc<dyn Partitioner>>,
    ) -> Self {
        Self {
            client,
            stream,
            stream_name,
            topic,
            topic_name,
            partitioning: None,
            encryptor,
            partitioner,
            create_stream_if_not_exists: true,
            create_topic_if_not_exists: true,
            topic_partitions_count: 1,
            topic_replication_factor: None,
            topic_message_expiry: IggyExpiry::ServerDefault,
            topic_max_size: MaxTopicSize::ServerDefault,
            send_retries_count: Some(3),
            send_retries_interval: Some(IggyDuration::ONE_SECOND),
            mode: SendMode::default(),
        }
    }

    /// Sets the stream identifier.
    pub fn stream(self, stream: Identifier) -> Self {
        Self { stream, ..self }
    }

    /// Sets the stream name.
    pub fn topic(self, topic: Identifier) -> Self {
        Self { topic, ..self }
    }

    /// Sets the encryptor for encrypting the messages' payloads.
    pub fn encryptor(self, encryptor: Arc<EncryptorKind>) -> Self {
        Self {
            encryptor: Some(encryptor),
            ..self
        }
    }

    /// Clears the encryptor for encrypting the messages' payloads.
    pub fn without_encryptor(self) -> Self {
        Self {
            encryptor: None,
            ..self
        }
    }

    /// Sets the partitioning strategy for messages.
    pub fn partitioning(self, partitioning: Partitioning) -> Self {
        Self {
            partitioning: Some(partitioning),
            ..self
        }
    }

    /// Clears the partitioning strategy.
    pub fn without_partitioning(self) -> Self {
        Self {
            partitioning: None,
            ..self
        }
    }

    /// Sets the partitioner for messages.
    pub fn partitioner(self, partitioner: Arc<dyn Partitioner>) -> Self {
        Self {
            partitioner: Some(partitioner),
            ..self
        }
    }

    /// Clears the partitioner.
    pub fn without_partitioner(self) -> Self {
        Self {
            partitioner: None,
            ..self
        }
    }

    /// Creates the stream if it does not exist - requires user to have the necessary permissions.
    pub fn create_stream_if_not_exists(self) -> Self {
        Self {
            create_stream_if_not_exists: true,
            ..self
        }
    }

    /// Does not create the stream if it does not exist.
    pub fn do_not_create_stream_if_not_exists(self) -> Self {
        Self {
            create_stream_if_not_exists: false,
            ..self
        }
    }

    /// Creates the topic if it does not exist - requires user to have the necessary permissions.
    pub fn create_topic_if_not_exists(
        self,
        partitions_count: u32,
        replication_factor: Option<u8>,
        message_expiry: IggyExpiry,
        max_size: MaxTopicSize,
    ) -> Self {
        Self {
            create_topic_if_not_exists: true,
            topic_partitions_count: partitions_count,
            topic_replication_factor: replication_factor,
            topic_message_expiry: message_expiry,
            topic_max_size: max_size,
            ..self
        }
    }

    /// Does not create the topic if it does not exist.
    pub fn do_not_create_topic_if_not_exists(self) -> Self {
        Self {
            create_topic_if_not_exists: false,
            ..self
        }
    }

    /// Sets the retry policy (maximum number of retries and interval between them) in case of messages sending failure.
    /// The error can be related either to disconnecting from the server or to the server rejecting the messages.
    /// Default is 3 retries with 1 second interval between them.
    pub fn send_retries(self, retries: Option<u32>, interval: Option<IggyDuration>) -> Self {
        Self {
            send_retries_count: retries,
            send_retries_interval: interval,
            ..self
        }
    }

    /// Configures the producer to use synchronous (immediate) sending mode.
    ///
    /// In sync mode, messages are sent immediately on `.send()` without background buffering.
    ///
    /// # Arguments
    /// * `f` - A closure that modifies the `SyncBuilder` configuration.
    pub fn sync<F>(mut self, f: F) -> Self
    where
        F: FnOnce(SyncBuilder) -> SyncBuilder,
    {
        let cfg = f(SyncBuilder::default()).build();
        self.mode = SendMode::Sync(cfg);
        self
    }

    /// Configures the producer to use background (asynchronous) sending mode.
    ///
    /// In background mode, messages are buffered and sent in batches via background tasks.
    ///
    /// # Arguments
    /// * `f` - A closure that modifies the `BackgroundBuilder` configuration.
    pub fn background<F>(mut self, f: F) -> Self
    where
        F: FnOnce(BackgroundBuilder) -> BackgroundBuilder,
    {
        let cfg = f(BackgroundBuilder::default()).build();
        self.mode = SendMode::Background(cfg);
        self
    }

    pub fn build(self) -> IggyProducer {
        IggyProducer::new(
            self.client,
            self.stream,
            self.stream_name,
            self.topic,
            self.topic_name,
            self.partitioning,
            self.encryptor,
            self.partitioner,
            self.create_stream_if_not_exists,
            self.create_topic_if_not_exists,
            self.topic_partitions_count,
            self.topic_replication_factor,
            self.topic_message_expiry,
            self.topic_max_size,
            self.send_retries_count,
            self.send_retries_interval,
            self.mode,
        )
    }
}

fn default_shard_count() -> usize {
    let cpus = num_cpus::get();
    cpus.clamp(2, 16)
}
