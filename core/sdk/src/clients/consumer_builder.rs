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
use crate::clients::reader_builder::{DEFAULT_MAX_WAIT, DEFAULT_PREFETCH_BYTES};
use crate::prelude::{ClientCommitPolicy, IggyConsumer};
use iggy_common::locking::IggyRwLock;
use iggy_common::{
    Consumer, EncryptorKind, Identifier, IggyDuration, NonZeroIggyDuration, PollOptions,
    PollingStrategy,
};
use std::sync::Arc;

const DEFAULT_OFFSET_DRAIN_TIMEOUT_SECS: u64 = 5;

#[derive(Debug)]
pub struct IggyConsumerBuilder {
    client: IggyRwLock<ClientWrapper>,
    consumer_name: String,
    consumer: Consumer,
    stream: Identifier,
    topic: Identifier,
    partitions: Option<Vec<u32>>,
    polling_strategy: PollingStrategy,
    poll_options: PollOptions,
    prefetch_bytes: u32,
    prefetch_messages: Option<u32>,
    client_commit: ClientCommitPolicy,
    create_consumer_group_if_not_exists: bool,
    encryptor: Option<Arc<EncryptorKind>>,
    polling_retry_interval: NonZeroIggyDuration,
    init_retries: Option<u32>,
    init_retry_interval: NonZeroIggyDuration,
    allow_replay: bool,
    offset_drain_timeout: IggyDuration,
}

impl IggyConsumerBuilder {
    pub(crate) fn new(
        client: IggyRwLock<ClientWrapper>,
        consumer_name: String,
        consumer: Consumer,
        stream_id: Identifier,
        topic_id: Identifier,
        encryptor: Option<Arc<EncryptorKind>>,
    ) -> Self {
        Self {
            client,
            consumer_name,
            consumer,
            stream: stream_id,
            topic: topic_id,
            partitions: None,
            polling_strategy: PollingStrategy::next(),
            poll_options: PollOptions::wait(DEFAULT_MAX_WAIT.into()),
            prefetch_bytes: DEFAULT_PREFETCH_BYTES,
            prefetch_messages: None,
            client_commit: ClientCommitPolicy::default(),
            create_consumer_group_if_not_exists: true,
            encryptor,
            polling_retry_interval: NonZeroIggyDuration::ONE_SECOND,
            init_retries: None,
            init_retry_interval: NonZeroIggyDuration::ONE_SECOND,
            allow_replay: false,
            offset_drain_timeout: IggyDuration::new_from_secs(DEFAULT_OFFSET_DRAIN_TIMEOUT_SECS),
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

    /// Sets the partitions of a standalone consumer, which requires them. A consumer group reads
    /// the assignment of its member, so `init()` refuses them for a group.
    pub fn partitions(self, partitions: impl IntoIterator<Item = u32>) -> Self {
        Self {
            partitions: Some(partitions.into_iter().collect()),
            ..self
        }
    }

    /// Sets the start of a partition without a position. Defaults to
    /// [`PollingStrategy::next()`]. `init()` refuses a continuation, because it belongs to one
    /// partition.
    pub fn polling_strategy(self, polling_strategy: PollingStrategy) -> Self {
        Self {
            polling_strategy,
            ..self
        }
    }

    /// Sets the limits of each request and who stores the offset of a read. Defaults to
    /// [`PollOptions::wait`] of one second: at most 1000 messages and a 1 MiB reply, and the
    /// application or the client commit policy stores offsets. A message larger than the reply
    /// limit is fetched alone, without that limit.
    pub fn poll_options(self, poll_options: PollOptions) -> Self {
        Self {
            poll_options,
            ..self
        }
    }

    /// Caps encoded bytes reserved for in-flight and buffered replies. Default: 16 MiB.
    pub fn prefetch_bytes(self, prefetch_bytes: u32) -> Self {
        Self {
            prefetch_bytes,
            ..self
        }
    }

    /// Caps messages reserved for in-flight and buffered replies. Default: 16,000, or the
    /// `count` of the poll options when that is larger.
    pub fn prefetch_messages(self, prefetch_messages: u32) -> Self {
        Self {
            prefetch_messages: Some(prefetch_messages),
            ..self
        }
    }

    /// Sets when the consumer stores the position of the last message that it yielded.
    /// Defaults to [`ClientCommitPolicy::default()`]: the consumer stores no offset, so a
    /// restart with `Next` reads every message after the last checkpoint committed with
    /// `commit` again. `init()` refuses a policy other than the default with
    /// [`ServerCommitPolicy::OnRead`](iggy_common::ServerCommitPolicy::OnRead).
    pub fn client_commit(self, client_commit: ClientCommitPolicy) -> Self {
        Self {
            client_commit,
            ..self
        }
    }

    /// Automatically creates the consumer group if it does not exist, also when the member
    /// joins again after a session loss. On by default.
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

    /// Sets how long a refused partition waits before the next read: after a transient
    /// refusal, or a message that does not decode. One second by default.
    pub fn polling_retry_interval(self, interval: NonZeroIggyDuration) -> Self {
        Self {
            polling_retry_interval: interval,
            ..self
        }
    }

    /// Sets the number of retries and the interval when initializing the consumer if the stream or topic is not found.
    /// Might be useful when the stream or topic is created dynamically by the producer.
    /// By default, the consumer will not retry.
    pub fn init_retries(self, retries: u32, interval: NonZeroIggyDuration) -> Self {
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

    /// Sets how long `shutdown()` waits for each background commit task to drain before the
    /// reader closes. 5 seconds by default.
    pub fn offset_drain_timeout(self, timeout: IggyDuration) -> Self {
        Self {
            offset_drain_timeout: timeout,
            ..self
        }
    }

    /// Builds the consumer.
    ///
    /// Note: After building the consumer, `init()` must be invoked before consuming messages.
    pub fn build(self) -> IggyConsumer {
        IggyConsumer::new(
            self.client,
            self.consumer_name,
            self.consumer,
            self.stream,
            self.topic,
            self.partitions,
            self.polling_strategy,
            self.poll_options,
            self.prefetch_bytes,
            self.prefetch_messages,
            self.client_commit,
            self.create_consumer_group_if_not_exists,
            self.encryptor,
            self.polling_retry_interval,
            self.init_retries,
            self.init_retry_interval,
            self.allow_replay,
            self.offset_drain_timeout,
        )
    }
}
