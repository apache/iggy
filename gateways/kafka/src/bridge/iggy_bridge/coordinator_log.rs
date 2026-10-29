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

use std::time::Duration;

use bytes::Bytes;
use iggy::prelude::{
    Consumer, ConsumerGroupClient, DeferredPollOptions, Durability, Identifier, IggyClient,
    IggyDuration, IggyError, IggyExpiry, IggyMessage, MaxTopicSize, MessageClient, Partitioning,
    PollingStrategy, TopicClient, TopicCreateOptions,
};
use tracing::{debug, info};

use super::{
    IggyBridge, cluster_id_stream, connect_client, sign_out_and_shut_down, with_request_timeout,
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
    pub payload: Bytes,
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
}

impl IggyBridge {
    /// The topics and the lease group of the coordinator log, created when missing, and clients
    /// for them.
    ///
    /// # Errors
    ///
    /// [`BridgeError::InvalidConfig`] when the log topic exists with another partition count than
    /// `partitions`, since every gateway must map each group to the same partition.
    /// [`BridgeError::Iggy`] or [`BridgeError::Timeout`] when a call to Iggy fails.
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

        self.create_coordinator_topic(
            &stream_id,
            COORDINATOR_LOG_TOPIC,
            partitions,
            LOG_EXPIRY,
            Durability::Persisted,
        )
        .await?;
        self.create_coordinator_topic(
            &stream_id,
            COORDINATOR_OWNER_TOPIC,
            1,
            OWNER_EXPIRY,
            Durability::Replicated,
        )
        .await?;
        let found = with_request_timeout(self.client.get_topic(&stream_id, &log_topic))
            .await?
            .ok_or(BridgeError::Iggy(IggyError::TopicNameNotFound(
                COORDINATOR_LOG_TOPIC.to_string(),
                stream_name.clone(),
            )))?;
        if found.partitions_count != partitions {
            return Err(BridgeError::InvalidConfig(format!(
                "Iggy topic '{stream_name}/{COORDINATOR_LOG_TOPIC}' has {} partitions, but the \
                 coordinator log uses {partitions}; every gateway maps each group to one of them, \
                 so the count never changes",
                found.partitions_count
            )));
        }
        match self
            .metadata_write(|| {
                self.client
                    .create_consumer_group(&stream_id, &log_topic, COORDINATOR_LEASE_GROUP)
            })
            .await
        {
            Ok(_) | Err(BridgeError::Iggy(IggyError::ConsumerGroupNameAlreadyExists(_, _))) => {}
            Err(error) => return Err(error),
        }

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
        })
    }

    async fn create_coordinator_topic(
        &self,
        stream_id: &Identifier,
        name: &str,
        partitions: u32,
        expiry: Duration,
        durability: Durability,
    ) -> Result<(), BridgeError> {
        let options = TopicCreateOptions {
            partitions_count: Some(partitions),
            message_expiry: Some(IggyExpiry::ExpireDuration(IggyDuration::from(expiry))),
            max_topic_size: Some(MaxTopicSize::Unlimited),
            durability,
            ..TopicCreateOptions::default()
        };
        match self
            .metadata_write(|| self.client.create_topic(stream_id, name, &options))
            .await
        {
            Ok(_) | Err(BridgeError::Iggy(IggyError::TopicNameAlreadyExists(_, _))) => Ok(()),
            Err(error) => Err(error),
        }
    }
}

impl CoordinatorLink {
    /// Joins the lease group. Iggy then gives this gateway a share of the partitions.
    ///
    /// # Errors
    ///
    /// [`BridgeError`] when the call to Iggy fails.
    pub async fn join_lease(&self) -> Result<(), BridgeError> {
        with_request_timeout(self.lease.join_consumer_group(
            &self.stream_id,
            &self.log_topic,
            &self.group_id,
        ))
        .await
    }

    /// Leaves the lease group, so Iggy hands this gateway's partitions to the others at once.
    ///
    /// # Errors
    ///
    /// [`BridgeError`] when the call to Iggy fails.
    pub async fn leave_lease(&self) -> Result<(), BridgeError> {
        with_request_timeout(self.lease.leave_consumer_group(
            &self.stream_id,
            &self.log_topic,
            &self.group_id,
        ))
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
        with_request_timeout(writer.send_messages(
            &self.stream_id,
            &self.log_topic,
            &Partitioning::partition_id(partition),
            &mut messages,
        ))
        .await
        .map(drop)
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
        Ok(polled
            .messages
            .into_iter()
            .last()
            .map(|message| LoggedRecord {
                offset: message.header.offset,
                id: message.header.id,
                payload: message.payload,
            }))
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
        with_request_timeout(self.writers[0].send_messages(
            &self.stream_id,
            &self.owner_topic,
            &Partitioning::partition_id(0),
            &mut messages,
        ))
        .await
        .map(drop)
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

    /// Signs every client out. The lease client goes last, so the other gateways take the
    /// partitions only once this one writes nothing more.
    pub async fn close(&self) {
        let mut clients: Vec<&IggyClient> = self.writers.iter().collect();
        clients.push(&self.reader);
        for client in clients {
            if let Err(error) = sign_out_and_shut_down(client).await {
                debug!(%error, "coordinator log client did not close cleanly");
            }
        }
        if let Err(error) = sign_out_and_shut_down(&self.lease).await {
            debug!(%error, "coordinator lease client did not close cleanly");
        }
    }
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
        .map(|message| LoggedRecord {
            offset: message.header.offset,
            id: message.header.id,
            payload: message.payload,
        })
        .collect())
}

impl std::fmt::Debug for CoordinatorLink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoordinatorLink").finish_non_exhaustive()
    }
}
