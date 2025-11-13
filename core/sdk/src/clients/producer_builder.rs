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

use crate::client_wrappers::client_wrapper::ClientWrapper;
use crate::clients::producer_config::{BackgroundConfig, DirectConfig};
use crate::prelude::IggyProducer;
use iggy_common::locking::IggyRwLock;
use iggy_common::{
    EncryptorKind, Identifier, IggyDuration, IggyExpiry, MaxTopicSize, Partitioner, Partitioning,
};
use std::sync::Arc;

pub enum SendMode {
    Direct(DirectConfig),
    Background(BackgroundConfig),
}

impl Default for SendMode {
    fn default() -> Self {
        SendMode::Direct(DirectConfig::builder().build())
    }
}

pub struct IggyProducerBuilder {
    client: IggyRwLock<ClientWrapper>,
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
        client: IggyRwLock<ClientWrapper>,
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

    /// Sets the producer to use direct message sending.
    /// This mode ensures that messages are sent immediately to the server
    /// without being buffered or delayed.
    pub fn direct(mut self, config: DirectConfig) -> Self {
        self.mode = SendMode::Direct(config);
        self
    }

    /// Sets the producer to use background message sending.
    /// This mode buffers messages and sends them in the background.
    pub fn background(mut self, config: BackgroundConfig) -> Self {
        self.mode = SendMode::Background(config);
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
