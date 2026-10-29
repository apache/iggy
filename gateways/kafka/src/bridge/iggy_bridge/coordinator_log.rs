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

//! The Iggy side of the coordinator log: its topics, its lease group and its clients.
//!
//! The log is the topic [`COORDINATOR_LOG_TOPIC`] in the stream of the cluster id, with one
//! partition per coordinator partition. Every gateway joins the Iggy consumer group
//! [`COORDINATOR_LEASE_GROUP`] on it, and Iggy spreads the partitions over the gateways. The
//! gateways never poll through that group: it only tells each gateway which partitions to
//! claim, and a partition that no member polled moves at once when Iggy hands it over.
//!
//! Who owns which partition is announced on the one-partition topic
//! [`COORDINATOR_OWNER_TOPIC`], which every gateway reads.

use std::fmt;
use std::time::Duration;

use bytes::Bytes;
use futures::future::join_all;
use iggy::prelude::{
    Consumer, ConsumerGroupClient, DeferredPollOptions, Durability, Identifier, IggyByteSize,
    IggyClient, IggyDuration, IggyError, IggyExpiry, IggyMessage, MaxTopicSize, MessageClient,
    Partitioning, PollingStrategy, TopicClient, TopicCreateOptions, TopicDetails,
};
use iggy_common::MIN_TOPIC_SEGMENT_SIZE;
use tokio::time::Instant;
use tracing::{debug, info};

use super::{
    IggyBridge, REQUEST_TIMEOUT, cluster_id_stream, connect_client, metadata_write_on,
    resolve_blocker, sign_out_and_shut_down, with_request_timeout,
};
use crate::bridge::error::BridgeError;

/// The topic of the coordinator log, in the stream of the cluster id.
pub const COORDINATOR_LOG_TOPIC: &str = "coordinators";

/// The topic where each gateway announces the coordinator partitions it owns.
pub const COORDINATOR_OWNER_TOPIC: &str = "coordinator_owners";

/// The Iggy consumer group on [`COORDINATOR_LOG_TOPIC`] that spreads its partitions over the
/// gateways.
pub const COORDINATOR_LEASE_GROUP: &str = "kafka-gateways";

/// How long Iggy keeps a log record. A coordinator writes a checkpoint well inside it, so a load
/// never starts at an expired record.
const LOG_EXPIRY: Duration = Duration::from_hours(24 * 7);

/// How long Iggy keeps an announcement. Owners announce again well inside it.
const OWNER_EXPIRY: Duration = Duration::from_hours(24);

/// The segment size of the owner topic. Iggy expires only sealed segments, and announcements
/// fill the smallest one in days, so a start reads only the last few days of them.
const OWNER_SEGMENT_BYTES: u64 = MIN_TOPIC_SEGMENT_SIZE;

/// Records a read returns at most.
const READ_COUNT: u32 = 1_000;

/// The bytes a waiting read returns at most. Above the largest group state a record can hold.
const READ_BYTES: u32 = 64 * 1024 * 1024;

/// What a waiting read adds to its wait for the reply to arrive.
const READ_REPLY_MARGIN: Duration = Duration::from_secs(10);

/// One record as the log returns it.
#[derive(Debug, Clone)]
pub struct LoggedRecord {
    pub offset: u64,
    pub id: u128,
    /// When Iggy stored the record, in microseconds since the Unix epoch.
    pub timestamp: u64,
    pub payload: Bytes,
}

/// The call to Iggy that failed while preparing the coordinator log, named in
/// [`BridgeError::CoordinatorLogCall`] with the permission it takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoordinatorLogCall {
    ReadTopic,
    CreateTopic,
    ReadGroup,
    CreateGroup,
}

impl CoordinatorLogCall {
    /// What to grant when Iggy refused the call, or nothing for any other failure.
    pub(crate) fn permission_hint(self, source: &BridgeError, stream: &str) -> String {
        if !matches!(source, BridgeError::Iggy(IggyError::Unauthorized)) {
            return String::new();
        }
        let needed = match self {
            Self::CreateTopic => "manage_topics",
            Self::ReadTopic | Self::ReadGroup | Self::CreateGroup => "read_topics",
        };
        format!(
            " (the bridge's Iggy user needs {needed} on stream '{stream}', or an operator \
             creates topics '{COORDINATOR_LOG_TOPIC}' and '{COORDINATOR_OWNER_TOPIC}' there \
             and grants the user read_topics, poll_messages and send_messages on it)"
        )
    }
}

impl fmt::Display for CoordinatorLogCall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ReadTopic => "read Iggy topic",
            Self::CreateTopic => "create Iggy topic",
            Self::ReadGroup => "read the Iggy consumer group of",
            Self::CreateGroup => "create the Iggy consumer group of",
        })
    }
}

/// A topic of the coordinator log, as a gateway creates it.
struct LogTopic {
    name: &'static str,
    partitions: u32,
    expiry: Duration,
    durability: Durability,
    segment_bytes: Option<u64>,
}

/// The Iggy calls of the coordinator log, on clients of their own.
///
/// The lease client is the gateway's member in the lease group, so it must outlive every other
/// call: Iggy hands its partitions to other gateways when its session ends. Reads share one
/// client, since a poll takes no lane of it. Appends spread over `writers`, since the appends of
/// one client go one at a time.
pub struct CoordinatorLink {
    stream_id: Identifier,
    log_topic: Identifier,
    owner_topic: Identifier,
    group_id: Identifier,
    lease: IggyClient,
    reader: IggyClient,
    writers: Vec<IggyClient>,
    max_send_bytes: u64,
}

impl IggyBridge {
    /// The topics and the lease group of the coordinator log, created when missing, and clients
    /// for them.
    ///
    /// # Errors
    ///
    /// [`BridgeError::InvalidConfig`] when the log topic exists with another partition count than
    /// `partitions`, since every gateway must map each group to the same partition.
    /// [`BridgeError::CoordinatorLogCall`] when a call to Iggy fails, with the permission it
    /// takes. [`BridgeError::Iggy`] or [`BridgeError::Timeout`] when a client does not connect.
    pub async fn coordinator_link(
        &self,
        partitions: u32,
        writers: usize,
    ) -> Result<CoordinatorLink, BridgeError> {
        let stream_name = cluster_id_stream(self.config.topic_mapping.default_stream());
        let stream_id = Identifier::named(&stream_name).map_err(BridgeError::Iggy)?;
        let log_topic = Identifier::named(COORDINATOR_LOG_TOPIC).map_err(BridgeError::Iggy)?;
        let owner_topic = Identifier::named(COORDINATOR_OWNER_TOPIC).map_err(BridgeError::Iggy)?;
        let group_id = Identifier::named(COORDINATOR_LEASE_GROUP).map_err(BridgeError::Iggy)?;

        let found = self
            .coordinator_topic(
                &stream_name,
                &stream_id,
                &LogTopic {
                    name: COORDINATOR_LOG_TOPIC,
                    partitions,
                    expiry: LOG_EXPIRY,
                    // A claim that a crash of a quorum loses would let two gateways own one
                    // partition. So each append waits for stable storage on a quorum.
                    durability: Durability::Persisted,
                    segment_bytes: None,
                },
            )
            .await?;
        if found.partitions_count != partitions {
            return Err(BridgeError::InvalidConfig(format!(
                "Iggy topic '{stream_name}/{COORDINATOR_LOG_TOPIC}' has {} partitions, but the \
                 coordinator log uses {partitions}; every gateway maps each group to one of them, \
                 so the count never changes",
                found.partitions_count
            )));
        }
        self.coordinator_topic(
            &stream_name,
            &stream_id,
            &LogTopic {
                name: COORDINATOR_OWNER_TOPIC,
                partitions: 1,
                expiry: OWNER_EXPIRY,
                durability: Durability::Replicated,
                segment_bytes: Some(OWNER_SEGMENT_BYTES),
            },
        )
        .await?;
        self.lease_group(&stream_name, &stream_id, &log_topic, &group_id)
            .await?;

        let lease = connect_client(&self.config).await?;
        let reader = connect_client(&self.config).await?;
        let mut clients = Vec::with_capacity(writers.max(1));
        for _ in 0..writers.max(1) {
            clients.push(connect_client(&self.config).await?);
        }
        info!(
            "Coordinator log ready on Iggy topic '{stream_name}/{COORDINATOR_LOG_TOPIC}' with \
             {partitions} partitions"
        );
        Ok(CoordinatorLink {
            stream_id,
            log_topic,
            owner_topic,
            group_id,
            lease,
            reader,
            writers: clients,
            max_send_bytes: self.max_send_bytes(),
        })
    }

    /// The topic `topic`, created when the read finds none: Iggy checks the permission to create
    /// before it checks whether the name exists, so a topic an operator created needs no
    /// `manage_topics`.
    async fn coordinator_topic(
        &self,
        stream: &str,
        stream_id: &Identifier,
        topic: &LogTopic,
    ) -> Result<TopicDetails, BridgeError> {
        let topic_id = Identifier::named(topic.name).map_err(BridgeError::Iggy)?;
        let failed = |call, source| coordinator_call_failed(call, stream, topic.name, source);
        let read = || async {
            with_request_timeout(self.client.get_topic(stream_id, &topic_id))
                .await
                .map_err(|error| failed(CoordinatorLogCall::ReadTopic, error))
        };
        if let Some(found) = read().await? {
            return Ok(found);
        }
        let options = TopicCreateOptions {
            partitions_count: Some(topic.partitions),
            message_expiry: Some(IggyExpiry::ExpireDuration(IggyDuration::from(topic.expiry))),
            max_topic_size: Some(MaxTopicSize::Unlimited),
            segment_size: topic.segment_bytes.map(IggyByteSize::from),
            durability: topic.durability,
            ..TopicCreateOptions::default()
        };
        match self
            .metadata_write(|| self.client.create_topic(stream_id, topic.name, &options))
            .await
        {
            Ok(created) => return Ok(created),
            // Another gateway created it first.
            Err(BridgeError::Iggy(IggyError::TopicNameAlreadyExists(_, _))) => {}
            Err(error) => return Err(failed(CoordinatorLogCall::CreateTopic, error)),
        }
        read().await?.ok_or_else(|| {
            failed(
                CoordinatorLogCall::ReadTopic,
                BridgeError::Iggy(IggyError::TopicNameNotFound(
                    topic.name.to_string(),
                    stream.to_string(),
                )),
            )
        })
    }

    /// The lease group, created when the read finds none.
    async fn lease_group(
        &self,
        stream: &str,
        stream_id: &Identifier,
        log_topic: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), BridgeError> {
        let failed =
            |call, source| coordinator_call_failed(call, stream, COORDINATOR_LOG_TOPIC, source);
        let found = with_request_timeout(
            self.client
                .get_consumer_group(stream_id, log_topic, group_id),
        )
        .await
        .map_err(|error| failed(CoordinatorLogCall::ReadGroup, error))?;
        if found.is_some() {
            return Ok(());
        }
        match self
            .metadata_write(|| {
                self.client
                    .create_consumer_group(stream_id, log_topic, COORDINATOR_LEASE_GROUP)
            })
            .await
        {
            Ok(_) | Err(BridgeError::Iggy(IggyError::ConsumerGroupNameAlreadyExists(_, _))) => {
                Ok(())
            }
            Err(error) => Err(failed(CoordinatorLogCall::CreateGroup, error)),
        }
    }
}

fn coordinator_call_failed(
    call: CoordinatorLogCall,
    stream: &str,
    topic: &'static str,
    source: BridgeError,
) -> BridgeError {
    BridgeError::CoordinatorLogCall {
        call,
        stream: stream.to_string(),
        topic,
        source: Box::new(source),
    }
}

impl CoordinatorLink {
    /// The message bytes one append sends at most: Iggy's message size, less its frame.
    #[must_use]
    pub const fn max_send_bytes(&self) -> u64 {
        self.max_send_bytes
    }

    /// Joins the lease group. Iggy then gives this gateway a share of the partitions.
    ///
    /// # Errors
    ///
    /// [`BridgeError`] when the call to Iggy fails.
    pub async fn join_lease(&self) -> Result<(), BridgeError> {
        metadata_write_on(&self.lease, || {
            self.lease
                .join_consumer_group(&self.stream_id, &self.log_topic, &self.group_id)
        })
        .await
    }

    /// Leaves the lease group, so Iggy hands this gateway's partitions to the others at once.
    ///
    /// # Errors
    ///
    /// [`BridgeError`] when the call to Iggy fails.
    pub async fn leave_lease(&self) -> Result<(), BridgeError> {
        metadata_write_on(&self.lease, || {
            self.lease
                .leave_consumer_group(&self.stream_id, &self.log_topic, &self.group_id)
        })
        .await
    }

    /// The partitions Iggy gives this gateway now, or `None` when it is no member. A partition
    /// that Iggy hands to another gateway leaves the list when the handoff starts.
    ///
    /// # Errors
    ///
    /// [`BridgeError`] when the call to Iggy fails.
    pub async fn assignment(&self) -> Result<Option<Vec<u32>>, BridgeError> {
        let assignment = with_request_timeout(self.lease.get_consumer_group_assignment(
            &self.stream_id,
            &self.log_topic,
            &self.group_id,
        ))
        .await?;
        Ok(assignment.map(|assignment| assignment.partitions))
    }

    /// Appends `records`, each with its id, to `partition` as one batch, on the writer client
    /// that serves the partition.
    ///
    /// # Errors
    ///
    /// [`BridgeError`] when the send fails or times out. The records can still land.
    pub async fn append(
        &self,
        partition: u32,
        records: Vec<(u128, Bytes)>,
    ) -> Result<(), BridgeError> {
        let mut messages = records
            .into_iter()
            .map(|(id, payload)| IggyMessage::builder().id(id).payload(payload).build())
            .collect::<Result<Vec<_>, IggyError>>()
            .map_err(BridgeError::Iggy)?;
        let writer = &self.writers[partition as usize % self.writers.len()];
        send_settled(
            writer,
            &self.stream_id,
            &self.log_topic,
            partition,
            &mut messages,
        )
        .await
    }

    /// The records of `partition` from offset `from`. With a `wait`, Iggy holds the read until a
    /// record is there or the wait ends, so an empty answer means none arrived.
    ///
    /// # Errors
    ///
    /// [`BridgeError`] when the poll fails.
    pub async fn read(
        &self,
        partition: u32,
        from: u64,
        wait: Duration,
    ) -> Result<Vec<LoggedRecord>, BridgeError> {
        read_topic(
            &self.reader,
            &self.stream_id,
            &self.log_topic,
            partition,
            from,
            wait,
        )
        .await
    }

    /// The last record of `partition`, if it holds any.
    ///
    /// # Errors
    ///
    /// [`BridgeError`] when the poll fails.
    pub async fn last(&self, partition: u32) -> Result<Option<LoggedRecord>, BridgeError> {
        let polled = with_request_timeout(self.reader.poll_messages(
            &self.stream_id,
            &self.log_topic,
            Some(partition),
            &Consumer::default(),
            &PollingStrategy::last(),
            1,
            false,
        ))
        .await?;
        Ok(polled.messages.into_iter().last().map(LoggedRecord::from))
    }

    /// Announces that this gateway owns a partition.
    ///
    /// # Errors
    ///
    /// [`BridgeError`] when the send fails.
    pub async fn announce(&self, payload: Bytes) -> Result<(), BridgeError> {
        let mut messages = vec![
            IggyMessage::builder()
                .payload(payload)
                .build()
                .map_err(BridgeError::Iggy)?,
        ];
        send_settled(
            &self.writers[0],
            &self.stream_id,
            &self.owner_topic,
            0,
            &mut messages,
        )
        .await
    }

    /// The announcements from offset `from`, waiting as [`Self::read`] does.
    ///
    /// # Errors
    ///
    /// [`BridgeError`] when the poll fails.
    pub async fn read_announcements(
        &self,
        from: u64,
        wait: Duration,
    ) -> Result<Vec<LoggedRecord>, BridgeError> {
        read_topic(
            &self.reader,
            &self.stream_id,
            &self.owner_topic,
            0,
            from,
            wait,
        )
        .await
    }

    /// Signs every client out, the writers and the reader at once. The lease client goes last,
    /// so the other gateways take the partitions only once this one writes nothing more.
    pub async fn close(&self) {
        let clients = self.writers.iter().chain([&self.reader]);
        for closed in join_all(clients.map(sign_out_and_shut_down)).await {
            if let Err(error) = closed {
                debug!(%error, "coordinator log client did not close cleanly");
            }
        }
        if let Err(error) = sign_out_and_shut_down(&self.lease).await {
            debug!(%error, "coordinator lease client did not close cleanly");
        }
    }
}

/// Sends `messages` to `partition` on `client`. A send of the client that timed out or ended in
/// doubt keeps its lane, and fails each later send there at once. So that send is settled first:
/// it lands or proves refused before these messages go, and the log keeps their order.
async fn send_settled(
    client: &IggyClient,
    stream_id: &Identifier,
    topic_id: &Identifier,
    partition: u32,
    messages: &mut [IggyMessage],
) -> Result<(), BridgeError> {
    let deadline = (Instant::now() + REQUEST_TIMEOUT).into_std();
    let partitioning = Partitioning::partition_id(partition);
    with_request_timeout(async {
        loop {
            match client
                .send_messages(stream_id, topic_id, &partitioning, messages)
                .await
            {
                Err(IggyError::LaneBlocked(operation)) => {
                    resolve_blocker(client, operation, deadline).await?;
                }
                sent => return sent,
            }
        }
    })
    .await
    .map(drop)
}

async fn read_topic(
    client: &IggyClient,
    stream_id: &Identifier,
    topic_id: &Identifier,
    partition: u32,
    from: u64,
    wait: Duration,
) -> Result<Vec<LoggedRecord>, BridgeError> {
    let consumer = Consumer::default();
    let strategy = PollingStrategy::offset(from);
    let polled = if wait.is_zero() {
        with_request_timeout(client.poll_messages(
            stream_id,
            topic_id,
            Some(partition),
            &consumer,
            &strategy,
            READ_COUNT,
            false,
        ))
        .await?
    } else {
        let options = DeferredPollOptions {
            max_wait: IggyDuration::from(wait),
            min_count: 1,
            max_bytes: READ_BYTES,
            request_timeout: IggyDuration::from(wait + READ_REPLY_MARGIN),
        };
        tokio::time::timeout(
            wait + READ_REPLY_MARGIN,
            client.poll_messages_deferred(
                stream_id,
                topic_id,
                Some(partition),
                &consumer,
                &strategy,
                READ_COUNT,
                false,
                options,
            ),
        )
        .await
        .map_err(|_elapsed| BridgeError::Timeout)?
        .map_err(BridgeError::Iggy)?
    };
    Ok(polled
        .messages
        .into_iter()
        .map(LoggedRecord::from)
        .collect())
}

impl From<IggyMessage> for LoggedRecord {
    fn from(message: IggyMessage) -> Self {
        Self {
            offset: message.header.offset,
            id: message.header.id,
            timestamp: message.header.timestamp,
            payload: message.payload,
        }
    }
}

impl std::fmt::Debug for CoordinatorLink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoordinatorLink").finish_non_exhaustive()
    }
}
