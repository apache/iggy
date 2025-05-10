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

use super::MAX_BATCH_SIZE;
use crate::prelude::IggyProducer;
use iggy_binary_protocol::Client;
use iggy_common::locking::IggySharedMut;
use iggy_common::{
    EncryptorKind, Identifier, IggyDuration, IggyExpiry, MaxTopicSize, Partitioner, Partitioning,
};
use std::sync::Arc;

#[derive(Debug)]
pub struct IggyProducerBuilder {
    client: IggySharedMut<Box<dyn Client>>,
    stream: Identifier,
    stream_name: String,
    topic: Identifier,
    topic_name: String,
    batch_size: Option<usize>,
    partitioning: Option<Partitioning>,
    encryptor: Option<Arc<EncryptorKind>>,
    partitioner: Option<Arc<dyn Partitioner>>,
    send_interval: Option<IggyDuration>,
    create_stream_if_not_exists: bool,
    create_topic_if_not_exists: bool,
    topic_partitions_count: u32,
    topic_replication_factor: Option<u8>,
    send_retries_count: Option<u32>,
    send_retries_interval: Option<IggyDuration>,
    topic_message_expiry: IggyExpiry,
    topic_max_size: MaxTopicSize,
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
            batch_size: Some(1000),
            partitioning: None,
            encryptor,
            partitioner,
            send_interval: Some(IggyDuration::from(1000)),
            create_stream_if_not_exists: true,
            create_topic_if_not_exists: true,
            topic_partitions_count: 1,
            topic_replication_factor: None,
            topic_message_expiry: IggyExpiry::ServerDefault,
            topic_max_size: MaxTopicSize::ServerDefault,
            send_retries_count: Some(3),
            send_retries_interval: Some(IggyDuration::ONE_SECOND),
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

    /// Sets the number of messages to batch before sending them, can be combined with `interval`.
    pub fn batch_size(self, batch_size: u32) -> Self {
        Self {
            batch_size: if batch_size == 0 {
                None
            } else {
                Some(batch_size.min(MAX_BATCH_SIZE as u32) as usize)
            },
            ..self
        }
    }

    /// Clears the batch size.
    pub fn without_batch_size(self) -> Self {
        Self {
            batch_size: None,
            ..self
        }
    }

    /// Sets the interval between sending the messages, can be combined with `batch_size`.
    pub fn send_interval(self, interval: IggyDuration) -> Self {
        Self {
            send_interval: Some(interval),
            ..self
        }
    }

    /// Clears the interval.
    pub fn without_send_interval(self) -> Self {
        Self {
            send_interval: None,
            ..self
        }
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

    /// Builds the producer.
    ///
    /// Note: After building the producer, `init()` must be invoked before producing messages.
    pub fn build(self) -> IggyProducer {
        IggyProducer::new(
            self.client,
            self.stream,
            self.stream_name,
            self.topic,
            self.topic_name,
            self.batch_size,
            self.partitioning,
            self.encryptor,
            self.partitioner,
            self.send_interval,
            self.create_stream_if_not_exists,
            self.create_topic_if_not_exists,
            self.topic_partitions_count,
            self.topic_replication_factor,
            self.topic_message_expiry,
            self.topic_max_size,
            self.send_retries_count,
            self.send_retries_interval,
        )
    }
}
