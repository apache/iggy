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

use std::fmt::{self, Debug, Formatter};
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use iggy_common::locking::{IggyRwLock, IggyRwLockFn};
use iggy_common::{
    Client, CommitPolicy, Consumer, ConsumerKind, EncryptorKind, Identifier, IggyDuration,
    IggyError, PollOptions, PollingStrategy, group_cache_key,
};
use tokio::sync::Notify;
use tracing::error;

use super::reader::membership::Membership;
use super::reader::progress::Progress;
use super::reader::{Config, PartitionReader, Shared, first_as_offset};
use crate::client_wrappers::client_wrapper::ClientWrapper;
use crate::driver::MAX_DEFERRED_REQUESTS_PER_CONSUMER;

pub(crate) const DEFAULT_PREFETCH_BYTES: u32 = 16 * 1024 * 1024;
pub(crate) const DEFAULT_PREFETCH_MESSAGES: u32 = 16_000;
pub(crate) const DEFAULT_MAX_WAIT: Duration = Duration::from_secs(5);
const DEFAULT_RETRY_INTERVAL: Duration = Duration::from_secs(1);

/// Builds a [`PartitionReader`]. Get one from [`IggyClient::reader`](crate::prelude::IggyClient::reader).
pub struct ReaderBuilder {
    client: IggyRwLock<ClientWrapper>,
    stream: Identifier,
    topic: Identifier,
    consumer: Consumer,
    partitions: Option<Vec<u32>>,
    start: PollingStrategy,
    options: PollOptions,
    prefetch_bytes: u32,
    prefetch_messages: Option<u32>,
    encryptor: Option<Arc<EncryptorKind>>,
    retry_interval: Duration,
    /// Set by `IggyConsumer`, which reports its deliveries to the same record.
    progress: Option<Arc<Progress>>,
    /// Set by `IggyConsumer`, which keeps an uncertain create or join for its next `init()`.
    membership: Option<Arc<Membership>>,
}

impl Debug for ReaderBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReaderBuilder")
            .field("stream", &self.stream)
            .field("topic", &self.topic)
            .field("consumer", &self.consumer)
            .field("partitions", &self.partitions)
            .field("start", &self.start)
            .field("options", &self.options)
            .field("prefetch_bytes", &self.prefetch_bytes)
            .field("prefetch_messages", &self.prefetch_messages)
            .finish_non_exhaustive()
    }
}

impl ReaderBuilder {
    pub(crate) fn new(
        client: IggyRwLock<ClientWrapper>,
        stream: Identifier,
        topic: Identifier,
        consumer: Consumer,
        encryptor: Option<Arc<EncryptorKind>>,
    ) -> Self {
        Self {
            client,
            stream,
            topic,
            consumer,
            partitions: None,
            start: PollingStrategy::next(),
            options: PollOptions::wait(IggyDuration::from(DEFAULT_MAX_WAIT)),
            prefetch_bytes: DEFAULT_PREFETCH_BYTES,
            prefetch_messages: None,
            encryptor,
            retry_interval: DEFAULT_RETRY_INTERVAL,
            progress: None,
            membership: None,
        }
    }

    /// The partitions of a standalone consumer, which requires them. A consumer group reads the
    /// assignment of its member, so `build()` refuses them for a group.
    pub fn partitions(self, partitions: impl IntoIterator<Item = u32>) -> Self {
        Self {
            partitions: Some(partitions.into_iter().collect()),
            ..self
        }
    }

    /// The start of a partition without a position. Default `PollingStrategy::next()`.
    /// `build()` refuses a continuation, because it belongs to one partition.
    pub fn start(self, start: PollingStrategy) -> Self {
        Self { start, ..self }
    }

    /// The options of each request and the [`CommitPolicy`] of the reader. Default
    /// `PollOptions::wait(5 s)`, which commits nothing.
    pub fn options(self, options: PollOptions) -> Self {
        Self { options, ..self }
    }

    /// The encoded bytes of the requests in flight and the queued batches. Default 16 MiB.
    pub fn prefetch_bytes(self, bytes: u32) -> Self {
        Self {
            prefetch_bytes: bytes,
            ..self
        }
    }

    /// The messages of the requests in flight and the queued batches. Default: the larger of
    /// 16,000 and `count`.
    pub fn prefetch_messages(self, messages: u32) -> Self {
        Self {
            prefetch_messages: Some(messages),
            ..self
        }
    }

    /// The record of deliveries and commits, which `IggyConsumer` shares with its reader.
    pub(crate) fn progress(self, progress: Arc<Progress>) -> Self {
        Self {
            progress: Some(progress),
            ..self
        }
    }

    /// The membership of a group reader, which `IggyConsumer` keeps across builds.
    pub(crate) fn membership(self, membership: Arc<Membership>) -> Self {
        Self {
            membership: Some(membership),
            ..self
        }
    }

    /// A membership that creates a missing group, as the one of `IggyConsumer` does.
    #[cfg(test)]
    pub(crate) fn create_group(self, create_group: bool) -> Self {
        let membership = Membership::new(
            self.client.clone(),
            Arc::new(self.stream.clone()),
            Arc::new(self.topic.clone()),
            self.consumer.id.clone(),
            create_group,
        );
        self.membership(Arc::new(membership))
    }

    /// The wait before a refused partition reads again.
    pub(crate) fn retry_interval(self, retry_interval: Duration) -> Self {
        Self {
            retry_interval,
            ..self
        }
    }

    /// Checks the configuration, joins the group of a group reader and starts reading.
    ///
    /// # Errors
    ///
    /// - [`IggyError::FeatureUnavailable`] for an external group, and for a consumer group on
    ///   HTTP, which cannot join a group.
    /// - [`IggyError::InvalidCommand`] for partitions of a consumer group, a standalone consumer
    ///   without partitions, or a continuation as the start.
    /// - [`IggyError::InvalidConfiguration`] for `CommitPolicy::OnRead` with an encryptor:
    ///   the server would store the offset of a batch before it decrypts.
    /// - The errors of [`PollOptions::validate`], and the error of the first join.
    /// - [`IggyError::OperationUncertain`] for a create or a join of the group with an unknown
    ///   outcome. The client keeps that operation, so a new `build()` gets
    ///   [`IggyError::LaneBlocked`] until [`Client::resolve_operation`] or
    ///   [`Client::abandon_operation`] settles it.
    pub async fn build(self) -> Result<PartitionReader, IggyError> {
        let (config, mut shared) = self.into_parts()?;
        let events = {
            let client = config.client.read().await;
            // A transport without group state must fail before the join, or the member
            // would hold partitions that it never reads.
            if config.is_group() {
                let state = client.deferred_group_state().await?;
                let key = group_cache_key(&config.stream, &config.topic, &config.consumer.id);
                shared.assignment_state = Some((state, key));
            }
            // A clone of the client's receiver holds every event published since the client
            // started, and the worker would act on that history as if it were live.
            client.subscribe_events().await.new_receiver()
        };
        if let Some(membership) = &shared.membership {
            membership.join().await?;
        }
        Ok(PartitionReader::start(
            Arc::new(config),
            Arc::new(shared),
            Some(events),
        ))
    }

    /// Checks the configuration and builds the parts of a reader, without I/O.
    pub(crate) fn into_parts(self) -> Result<(Config, Shared), IggyError> {
        let is_group = match self.consumer.kind {
            ConsumerKind::Consumer => false,
            ConsumerKind::ConsumerGroup => true,
            ConsumerKind::ExternalGroup => {
                error!(
                    "Reader of consumer: {} cannot read: an external group only holds offsets.",
                    self.consumer
                );
                return Err(IggyError::FeatureUnavailable);
            }
        };
        let partitions = match (is_group, self.partitions) {
            (false, Some(mut partitions)) if !partitions.is_empty() => {
                partitions.sort_unstable();
                partitions.dedup();
                partitions
            }
            (true, None) => Vec::new(),
            (false, partitions) => {
                error!(
                    "Reader of consumer: {} has the partitions: {partitions:?}. A standalone consumer needs at least one partition.",
                    self.consumer
                );
                return Err(IggyError::InvalidCommand);
            }
            (true, partitions) => {
                error!(
                    "Reader of consumer: {} has the partitions: {partitions:?}. A consumer group reads the partitions of its assignment, so it takes none.",
                    self.consumer
                );
                return Err(IggyError::InvalidCommand);
            }
        };
        if let Err(refusal) = self.options.validate() {
            error!(
                "Reader of consumer: {} has invalid poll options: {:?}: {refusal}.",
                self.consumer, self.options
            );
            return Err(refusal);
        }
        if let Some(partition_id) = self.start.partition_id() {
            error!(
                "Reader of consumer: {} has the start: {}, a continuation of partition: {partition_id}. A continuation belongs to one partition, so it cannot start every partition.",
                self.consumer, self.start
            );
            return Err(IggyError::InvalidCommand);
        }
        if self.options.commit == CommitPolicy::OnRead && self.encryptor.is_some() {
            error!(
                "Reader of consumer: {} has an encryptor and CommitPolicy::OnRead. The server would store the offset of a batch before it decrypts, so a batch that fails to decrypt would be lost.",
                self.consumer
            );
            return Err(IggyError::InvalidConfiguration);
        }
        let prefetch_messages = self
            .prefetch_messages
            .unwrap_or(DEFAULT_PREFETCH_MESSAGES.max(self.options.count));
        let slots = budget_slots(prefetch_messages, self.prefetch_bytes, self.options);
        let start = first_as_offset(self.start);
        let stream = Arc::new(self.stream);
        let topic = Arc::new(self.topic);
        let consumer = Arc::new(self.consumer);
        let progress = self.progress.unwrap_or_else(|| {
            Arc::new(Progress::new(
                self.client.clone(),
                Arc::clone(&consumer),
                Arc::clone(&stream),
                Arc::clone(&topic),
                self.options.commit,
            ))
        });
        // Only `IggyConsumer` creates a missing group, through the membership that it passes.
        let membership = is_group.then(|| {
            self.membership.unwrap_or_else(|| {
                Arc::new(Membership::new(
                    self.client.clone(),
                    Arc::clone(&stream),
                    Arc::clone(&topic),
                    consumer.id.clone(),
                    false,
                ))
            })
        });
        let shared = Shared {
            progress,
            epoch: AtomicU64::new(0),
            wake: Arc::new(Notify::new()),
            demand: AtomicBool::new(false),
            closing: AtomicBool::new(false),
            can_poll: AtomicBool::new(true),
            partitions: Mutex::new(partitions.clone()),
            membership,
            assignment_state: None,
        };
        let config = Config {
            client: self.client,
            stream,
            topic,
            consumer,
            partitions,
            start,
            options: self.options,
            slots,
            encryptor: self.encryptor,
            retry_interval: self.retry_interval,
        };
        Ok((config, shared))
    }
}

/// The requests in flight and the queued replies, each of which reserves `count` messages and
/// `max_bytes` bytes. One fits even when the prefetch limits are lower, and the driver admits at
/// most 16 waiting requests of one consumer.
fn budget_slots(prefetch_messages: u32, prefetch_bytes: u32, options: PollOptions) -> usize {
    let messages = prefetch_messages
        .checked_div(options.count)
        .unwrap_or_default();
    let bytes = prefetch_bytes
        .checked_div(options.max_bytes)
        .unwrap_or_default();
    usize::try_from(messages.min(bytes))
        .unwrap_or(usize::MAX)
        .clamp(1, MAX_DEFERRED_REQUESTS_PER_CONSUMER)
}
