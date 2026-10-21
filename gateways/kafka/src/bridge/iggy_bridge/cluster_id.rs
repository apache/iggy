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

//! The cluster id, kept in Iggy so every gateway on one Iggy reports the same one.
//!
//! The id is the first message of the one-partition topic [`CLUSTER_ID_TOPIC`] in the stream
//! named by [`cluster_id_stream`]. Whoever writes first sets it: a gateway that finds the
//! partition empty writes an id of its own and then reads the partition back, so two gateways
//! starting at once both end up with the message that landed first. Nothing ever rewrites it.

use std::fmt;

use iggy::prelude::{
    Consumer, Durability, Identifier, IggyError, IggyExpiry, IggyMessage, MaxTopicSize,
    MessageClient, Partitioning, PollingStrategy, StreamClient, StreamCreateOptions, TopicClient,
    TopicCreateOptions, TopicDetails,
};
use tracing::{info, warn};

use super::{IggyBridge, with_request_timeout};
use crate::bridge::error::BridgeError;
use crate::cluster_id::ClusterId;

/// The topic that holds the id, in the stream [`cluster_id_stream`] names.
pub const CLUSTER_ID_TOPIC: &str = "cluster_id";

/// The stream that holds gateway state, beside the default stream that holds the Kafka topics.
///
/// Named after the default stream, so two gateway deployments that map to different default
/// streams on one Iggy are two Kafka clusters with two ids.
#[must_use]
pub fn cluster_id_stream(default_stream: &str) -> String {
    format!("{default_stream}.meta")
}

/// The call to Iggy that failed while loading the cluster id, named in
/// [`BridgeError::ClusterIdCall`] with the permission it takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterIdCall {
    CreateStream,
    CreateTopic,
    Poll,
    Send,
}

impl ClusterIdCall {
    /// What the call addresses, in the form [`Display`](fmt::Display) names its kind.
    pub(crate) fn target(self, stream: &str, topic: &str) -> String {
        match self {
            Self::CreateStream => stream.to_string(),
            Self::CreateTopic | Self::Poll | Self::Send => format!("{stream}/{topic}"),
        }
    }

    /// What to grant when Iggy refused the call, or nothing for any other failure.
    pub(crate) fn permission_hint(self, source: &BridgeError, stream: &str) -> String {
        if !matches!(source, BridgeError::Iggy(IggyError::Unauthorized)) {
            return String::new();
        }
        let needed = match self {
            Self::CreateStream => "the global manage_streams permission".to_string(),
            Self::CreateTopic => format!("manage_topics on stream '{stream}'"),
            Self::Poll | Self::Send => {
                format!("poll_messages and send_messages on stream '{stream}'")
            }
        };
        format!(
            " (the bridge's Iggy user needs {needed}, or an operator creates stream '{stream}' \
             and topic '{CLUSTER_ID_TOPIC}' and grants the user poll_messages and send_messages \
             there)"
        )
    }
}

impl fmt::Display for ClusterIdCall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::CreateStream => "create Iggy stream",
            Self::CreateTopic => "create Iggy topic",
            Self::Poll => "poll Iggy topic",
            Self::Send => "send to Iggy topic",
        })
    }
}

impl IggyBridge {
    /// The cluster id stored in Iggy, written first if no gateway has stored one yet.
    ///
    /// # Errors
    ///
    /// [`BridgeError::InvalidClusterId`] if the stored message is not a Kafka cluster id.
    /// [`BridgeError::ClusterIdCall`] if a call to Iggy fails or times out.
    pub async fn load_cluster_id(&self) -> Result<ClusterId, BridgeError> {
        let store = IdStore::new(self.config.topic_mapping.default_stream())?;
        info!(
            "Loading the Kafka cluster id from Iggy topic '{}/{CLUSTER_ID_TOPIC}'",
            store.stream_name
        );
        // Each create runs only for what the poll found missing: Iggy checks the permission to
        // create before it checks whether the name exists, so a user an operator granted only
        // poll and send on a pre-created topic is refused even a create that would be a no-op.
        let id = match self.read_cluster_id(&store).await? {
            StoredId::Stored(id) => {
                info!("Kafka cluster id {id} read from Iggy");
                id
            }
            StoredId::StreamMissing => {
                self.create_cluster_id_stream(&store).await?;
                self.create_cluster_id_topic(&store).await?;
                self.store_cluster_id(&store).await?
            }
            StoredId::TopicMissing => {
                self.create_cluster_id_topic(&store).await?;
                self.store_cluster_id(&store).await?
            }
            StoredId::Empty => self.store_cluster_id(&store).await?,
        };
        self.check_cluster_id_topic(&store).await;
        Ok(id)
    }

    /// Proposes a new id, then returns whichever one landed first.
    async fn store_cluster_id(&self, store: &IdStore) -> Result<ClusterId, BridgeError> {
        let mut proposal = vec![IggyMessage::from(ClusterId::random().as_str())];
        with_request_timeout(self.client.send_messages(
            &store.stream_id,
            &store.topic_id,
            &Partitioning::partition_id(0),
            &mut proposal,
        ))
        .await
        .map_err(|error| store.call_failed(ClusterIdCall::Send, error))?;
        match self.read_cluster_id(store).await? {
            StoredId::Stored(id) => {
                info!("Kafka cluster id {id} stored in Iggy");
                Ok(id)
            }
            StoredId::StreamMissing | StoredId::TopicMissing | StoredId::Empty => Err(store
                .call_failed(
                    ClusterIdCall::Poll,
                    BridgeError::Iggy(IggyError::EmptyResponse),
                )),
        }
    }

    /// What the partition holds at its earliest offset, or which level of it is missing.
    async fn read_cluster_id(&self, store: &IdStore) -> Result<StoredId, BridgeError> {
        let polled = with_request_timeout(self.client.poll_messages(
            &store.stream_id,
            &store.topic_id,
            Some(0),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            1,
            false,
        ))
        .await;
        let polled = match polled {
            Ok(polled) => polled,
            Err(BridgeError::Iggy(
                IggyError::StreamIdNotFound(_) | IggyError::StreamNameNotFound(_),
            )) => return Ok(StoredId::StreamMissing),
            Err(BridgeError::Iggy(
                IggyError::TopicIdNotFound(_, _) | IggyError::TopicNameNotFound(_, _),
            )) => return Ok(StoredId::TopicMissing),
            Err(error) => return Err(store.call_failed(ClusterIdCall::Poll, error)),
        };
        let Some(first) = polled.messages.first() else {
            return Ok(StoredId::Empty);
        };
        // Offset 0 is gone only if its segment was deleted or lost. Refusing to start would
        // strand every gateway on this Iggy, and the earliest survivor is still one id they all
        // agree on.
        if first.header.offset != 0 {
            warn!(
                "Iggy topic '{}/{CLUSTER_ID_TOPIC}' no longer holds offset 0; using the message \
                 at offset {} as the Kafka cluster id",
                store.stream_name, first.header.offset
            );
        }
        String::from_utf8_lossy(&first.payload)
            .parse()
            .map(StoredId::Stored)
            .map_err(|source| BridgeError::InvalidClusterId {
                stream: store.stream_name.clone(),
                topic: CLUSTER_ID_TOPIC,
                source,
            })
    }

    async fn create_cluster_id_stream(&self, store: &IdStore) -> Result<(), BridgeError> {
        let options = StreamCreateOptions::default();
        match self
            .metadata_write(|| self.client.create_stream(&store.stream_name, &options))
            .await
        {
            Ok(_) | Err(BridgeError::Iggy(IggyError::StreamNameAlreadyExists(_))) => Ok(()),
            Err(error) => Err(store.call_failed(ClusterIdCall::CreateStream, error)),
        }
    }

    /// Creates the topic, unless another gateway just did.
    ///
    /// Expiry, size cap and durability are set explicitly because the id must never change.
    /// The default durability, `Replicated`, acknowledges before a stable-storage barrier, and
    /// a single-node Iggy that crashes then discards the unflushed id on restart.
    async fn create_cluster_id_topic(&self, store: &IdStore) -> Result<(), BridgeError> {
        let created = IdTopicOptions::CREATED;
        let options = TopicCreateOptions {
            partitions_count: Some(created.partitions_count),
            message_expiry: Some(created.message_expiry),
            max_topic_size: Some(created.max_topic_size),
            durability: created.durability,
            ..TopicCreateOptions::default()
        };
        match self
            .metadata_write(|| {
                self.client
                    .create_topic(&store.stream_id, CLUSTER_ID_TOPIC, &options)
            })
            .await
        {
            Ok(_) | Err(BridgeError::Iggy(IggyError::TopicNameAlreadyExists(_, _))) => Ok(()),
            Err(error) => Err(store.call_failed(ClusterIdCall::CreateTopic, error)),
        }
    }

    /// Warns when the topic could lose the id: one an operator or an earlier build created,
    /// which this gateway adopted as it was.
    ///
    /// Runs on every start, not only after a create, because a topic that already holds an id
    /// never reaches the create path. One call per process start costs nothing. It needs
    /// `read_topics`, which the id itself does not, so a refusal is logged and skipped.
    async fn check_cluster_id_topic(&self, store: &IdStore) {
        let topic =
            match with_request_timeout(self.client.get_topic(&store.stream_id, &store.topic_id))
                .await
            {
                Ok(Some(topic)) => topic,
                Ok(None) => return,
                Err(error) => {
                    info!(
                        %error,
                        "Not checking the options of Iggy topic '{}/{CLUSTER_ID_TOPIC}': reading \
                         them needs read_topics on stream '{}'",
                        store.stream_name, store.stream_name
                    );
                    return;
                }
            };
        let options = IdTopicOptions::from(&topic);
        if options != IdTopicOptions::CREATED {
            warn!(
                "Iggy topic '{}/{CLUSTER_ID_TOPIC}' holds the Kafka cluster id with {} \
                 partitions, durability {}, expiry {} and size cap {}, so the id can be lost \
                 and the next start would report a new one. Update it to no expiry and no size \
                 cap in place. Other durability or partitions need a recreate: stop every \
                 gateway first, or one that starts meanwhile stores a new id, then recreate it \
                 with one partition and durability persisted and send this id back as its first \
                 message",
                store.stream_name,
                options.partitions_count,
                options.durability,
                options.message_expiry,
                options.max_topic_size
            );
        }
    }
}

/// What one poll of the id topic's earliest offset found.
enum StoredId {
    StreamMissing,
    TopicMissing,
    Empty,
    Stored(ClusterId),
}

/// The options of the id topic that decide whether it keeps the id.
///
/// Expiry, a size cap or a durability that acks before stable storage can drop it. Any other
/// partition count means some other tool made the topic, and may store an id where the gateway
/// does not read.
#[derive(Debug, Clone, Copy, PartialEq)]
struct IdTopicOptions {
    partitions_count: u32,
    durability: Durability,
    message_expiry: IggyExpiry,
    max_topic_size: MaxTopicSize,
}

impl IdTopicOptions {
    /// What this gateway creates the topic with.
    const CREATED: Self = Self {
        partitions_count: 1,
        durability: Durability::Persisted,
        message_expiry: IggyExpiry::NeverExpire,
        max_topic_size: MaxTopicSize::Unlimited,
    };
}

impl From<&TopicDetails> for IdTopicOptions {
    fn from(topic: &TopicDetails) -> Self {
        let resolved = topic.resolved_options();
        Self {
            partitions_count: topic.partitions_count,
            durability: TopicCreateOptions::from_resource_options(&topic.options).durability,
            message_expiry: resolved.message_expiry,
            max_topic_size: resolved.max_topic_size,
        }
    }
}

/// Where the id lives, resolved once per load.
struct IdStore {
    stream_name: String,
    stream_id: Identifier,
    topic_id: Identifier,
}

impl IdStore {
    fn new(default_stream: &str) -> Result<Self, BridgeError> {
        let stream_name = cluster_id_stream(default_stream);
        Ok(Self {
            stream_id: Identifier::named(&stream_name).map_err(BridgeError::Iggy)?,
            topic_id: Identifier::named(CLUSTER_ID_TOPIC).map_err(BridgeError::Iggy)?,
            stream_name,
        })
    }

    fn call_failed(&self, call: ClusterIdCall, source: BridgeError) -> BridgeError {
        BridgeError::ClusterIdCall {
            call,
            stream: self.stream_name.clone(),
            topic: CLUSTER_ID_TOPIC,
            source: Box::new(source),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call_failed(call: ClusterIdCall, source: IggyError) -> String {
        IdStore::new("kafka")
            .expect("valid names")
            .call_failed(call, BridgeError::Iggy(source))
            .to_string()
    }

    #[test]
    fn given_a_refused_call_when_reported_should_name_the_topic_and_the_permission() {
        let message = call_failed(ClusterIdCall::CreateStream, IggyError::Unauthorized);
        assert!(
            message.starts_with("cannot create Iggy stream 'kafka.meta' for the Kafka cluster id"),
            "{message}"
        );
        assert!(message.contains("global manage_streams"), "{message}");

        let message = call_failed(ClusterIdCall::Poll, IggyError::Unauthorized);
        assert!(
            message.contains("needs poll_messages and send_messages on stream 'kafka.meta'"),
            "{message}"
        );
    }

    #[test]
    fn given_the_created_options_when_one_differs_should_not_match() {
        let created = IdTopicOptions::CREATED;
        for changed in [
            IdTopicOptions {
                partitions_count: 2,
                ..created
            },
            IdTopicOptions {
                durability: Durability::Replicated,
                ..created
            },
            IdTopicOptions {
                message_expiry: IggyExpiry::ServerDefault,
                ..created
            },
            IdTopicOptions {
                max_topic_size: MaxTopicSize::ServerDefault,
                ..created
            },
        ] {
            assert_ne!(changed, created);
        }
    }

    #[test]
    fn given_a_call_failing_for_another_reason_when_reported_should_not_name_a_permission() {
        let message = call_failed(ClusterIdCall::Send, IggyError::Disconnected);
        assert!(
            message.starts_with("cannot send to Iggy topic 'kafka.meta/cluster_id'"),
            "{message}"
        );
        assert!(!message.contains("needs"), "{message}");
    }
}
