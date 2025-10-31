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

use crate::client_wrappers::client_wrapper::ClientWrapper;
use crate::prelude::{AutoCommit, AutoCommitWhen, IggyConsumer};
use iggy_common::locking::IggyRwLock;
use iggy_common::{Consumer, EncryptorKind, Identifier, IggyDuration, PollingStrategy};
use std::sync::Arc;

#[derive(Debug)]
pub struct IggyConsumerBuilder {
    client: IggyRwLock<ClientWrapper>,
    consumer_name: String,
    consumer: Consumer,
    stream: Identifier,
    topic: Identifier,
    partition: Option<u32>,
    polling_strategy: PollingStrategy,
    polling_interval: Option<IggyDuration>,
    batch_length: u32,
    auto_commit: AutoCommit,
    auto_join_consumer_group: bool,
    create_consumer_group_if_not_exists: bool,
    encryptor: Option<Arc<EncryptorKind>>,
    polling_retry_interval: IggyDuration,
    init_retries: Option<u32>,
    init_retry_interval: IggyDuration,
    allow_replay: bool,
}

impl IggyConsumerBuilder {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        client: IggyRwLock<ClientWrapper>,
        consumer_name: String,
        consumer: Consumer,
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: Option<u32>,
        encryptor: Option<Arc<EncryptorKind>>,
        polling_interval: Option<IggyDuration>,
    ) -> Self {
        Self {
            client,
            consumer_name,
            consumer,
            stream: stream_id,
            topic: topic_id,
            partition: partition_id,
            polling_strategy: PollingStrategy::next(),
            batch_length: 1000,
            auto_commit: AutoCommit::IntervalOrWhen(
                IggyDuration::ONE_SECOND,
                AutoCommitWhen::PollingMessages,
            ),
            auto_join_consumer_group: true,
            create_consumer_group_if_not_exists: true,
            encryptor,
            polling_interval,
            polling_retry_interval: IggyDuration::ONE_SECOND,
            init_retries: None,
            init_retry_interval: IggyDuration::ONE_SECOND,
            allow_replay: false,
        }
    }

    /// Sets the stream identifier.
    pub fn stream(self, stream: Identifier) -> Self {
        Self { stream, ..self }
    }

    /// Sets the topic identifier.
    pub fn topic(self, topic: Identifier) -> Self {
        Self { topic, ..self }
    }

    /// Sets the partition identifier.
    pub fn partition(self, partition: Option<u32>) -> Self {
        Self { partition, ..self }
    }

    /// Sets the polling strategy.
    pub fn polling_strategy(self, polling_strategy: PollingStrategy) -> Self {
        Self {
            polling_strategy,
            ..self
        }
    }

    /// Sets the batch size for polling messages.
    pub fn batch_length(self, batch_length: u32) -> Self {
        Self {
            batch_length,
            ..self
        }
    }

    /// Sets the auto-commit configuration for storing the offset on the server.
    pub fn auto_commit(self, auto_commit: AutoCommit) -> Self {
        Self {
            auto_commit,
            ..self
        }
    }

    pub fn commit_failed_messages(self) -> Self {
        Self {
            auto_commit: AutoCommit::Disabled,
            ..self
        }
    }

    /// Automatically joins the consumer group if the consumer is a part of a consumer group.
    pub fn auto_join_consumer_group(self) -> Self {
        Self {
            auto_join_consumer_group: true,
            ..self
        }
    }

    /// Does not automatically join the consumer group if the consumer is a part of a consumer group.
    pub fn do_not_auto_join_consumer_group(self) -> Self {
        Self {
            auto_join_consumer_group: false,
            ..self
        }
    }

    /// Automatically creates the consumer group if it does not exist.
    pub fn create_consumer_group_if_not_exists(self) -> Self {
        Self {
            create_consumer_group_if_not_exists: true,
            ..self
        }
    }

    /// Does not automatically create the consumer group if it does not exist.
    pub fn do_not_create_consumer_group_if_not_exists(self) -> Self {
        Self {
            create_consumer_group_if_not_exists: false,
            ..self
        }
    }

    /// Sets the polling interval for messages.
    pub fn poll_interval(self, interval: IggyDuration) -> Self {
        Self {
            polling_interval: Some(interval),
            ..self
        }
    }

    /// Clears the polling interval for messages.
    pub fn without_poll_interval(self) -> Self {
        Self {
            polling_interval: None,
            ..self
        }
    }

    /// Sets the encryptor for decrypting the messages' payloads.
    pub fn encryptor(self, encryptor: Arc<EncryptorKind>) -> Self {
        Self {
            encryptor: Some(encryptor),
            ..self
        }
    }

    /// Clears the encryptor for decrypting the messages' payloads.
    pub fn without_encryptor(self) -> Self {
        Self {
            encryptor: None,
            ..self
        }
    }

    /// Sets the polling retry interval in case of server disconnection.
    pub fn polling_retry_interval(self, interval: IggyDuration) -> Self {
        Self {
            polling_retry_interval: interval,
            ..self
        }
    }

    /// Sets the number of retries and the interval when initializing the consumer if the stream or topic is not found.
    /// Might be useful when the stream or topic is created dynamically by the producer.
    /// By default, the consumer will not retry.
    pub fn init_retries(self, retries: u32, interval: IggyDuration) -> Self {
        Self {
            init_retries: Some(retries),
            init_retry_interval: interval,
            ..self
        }
    }

    /// Allows replaying the messages, `false` by default.
    pub fn allow_replay(self) -> Self {
        Self {
            allow_replay: true,
            ..self
        }
    }

    /// Builds the consumer.
    ///
    /// Note: After building the consumer, `init()` must be invoked before producing messages.
    pub fn build(self) -> IggyConsumer {
        IggyConsumer::new(
            self.client,
            self.consumer_name,
            self.consumer,
            self.stream,
            self.topic,
            self.partition,
            self.polling_interval,
            self.polling_strategy,
            self.batch_length,
            self.auto_commit,
            self.auto_join_consumer_group,
            self.create_consumer_group_if_not_exists,
            self.encryptor,
            self.polling_retry_interval,
            self.init_retries,
            self.init_retry_interval,
            self.allow_replay,
        )
    }
}
