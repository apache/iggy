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

use iggy::prelude::{
    AutoLogin, Client, Credentials, Identifier, IggyClient, IggyClientBuilder, IggyError,
    StreamClient, TopicClient, TopicCreateOptions,
};
use tracing::{debug, info};

use crate::bridge::config::IggyBridgeConfig;
use crate::bridge::error::BridgeError;

/// Passes attempted, after the first, before [`IggyBridge::connect`] gives up and returns `Err`.
///
/// Not the SDK's own default (`TcpClientReconnectionConfig::default()` is `max_retries: None` -
/// unlimited, one dial per second, forever). A Kafka client already retries at the wire-protocol
/// level once a handler maps a bridge failure to a retriable error code; the bridge blocking a
/// request task inside an unbounded internal reconnect loop would just add a second, invisible
/// retry layer underneath that one instead of surfacing the failure so the mapped code can be
/// sent. At the default `reconnection_interval` (1s), a fully unreachable address fails in a few
/// seconds rather than hanging.
const RECONNECTION_RETRIES: u32 = 3;

/// Owns one connected `IggyClient` and resolves Kafka topics against it.
///
/// Produce/Fetch handler wiring is a separate, later change (`#3535`/`#3536`) - this type is the
/// shared plumbing those handlers will call into, exercised standalone here via its own tests and
/// an integration test against a real `iggy-server`.
pub struct IggyBridge {
    client: IggyClient,
    config: IggyBridgeConfig,
}

impl IggyBridge {
    /// Connects to Iggy using `config` and authenticates.
    ///
    /// Builds the client through the SDK's fluent TCP builder rather than hand-assembling an
    /// `iggy://user:pass@host` connection string: that string format splits on `@` then `:`, so a
    /// password containing either character (`p@ss:word`) would be misparsed into a garbled
    /// address instead of failing with a diagnosable config error. The fluent builder passes
    /// `username`/`password` as already-separated fields, sidestepping the ambiguity entirely.
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError::InvalidConfig`] if `config.address` is empty. Returns
    /// [`BridgeError::Iggy`] if the address is malformed, the TCP connection fails, or
    /// authentication is rejected - this is the boundary [`BridgeError::to_kafka_error_code`]
    /// exists for: a handler calling this must map the error to a wire response, never panic or
    /// unwrap, since an unreachable Iggy backend is an expected runtime condition, not a bug.
    pub async fn connect(config: IggyBridgeConfig) -> Result<Self, BridgeError> {
        if config.address.trim().is_empty() {
            return Err(BridgeError::InvalidConfig(
                "Iggy address must not be empty".to_string(),
            ));
        }

        let credentials =
            Credentials::UsernamePassword(config.username.clone(), config.password.clone());
        let client = IggyClientBuilder::new()
            .with_tcp()
            .with_server_address(config.address.clone())
            .with_auto_sign_in(AutoLogin::Enabled(credentials))
            .with_reconnection_max_retries(Some(RECONNECTION_RETRIES))
            .build()
            .map_err(BridgeError::Iggy)?;
        client.connect().await.map_err(BridgeError::Iggy)?;
        info!("Iggy bridge connected to {}", config.address);

        Ok(Self { client, config })
    }

    /// Disconnects the underlying Iggy client.
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError::Iggy`] if the client reports a disconnect failure.
    pub async fn close(&self) -> Result<(), BridgeError> {
        self.client.disconnect().await.map_err(BridgeError::Iggy)
    }

    /// Ensures the Iggy stream and topic backing `kafka_topic` exist, creating either or both if
    /// missing. Resolves `kafka_topic` through the configured [`TopicMapping`](crate::bridge::topic_map::TopicMapping).
    ///
    /// Idempotent when repeated with the *same* `partition_count`: a `get` before each `create`
    /// means calling this twice for the same topic is a no-op the second time, and a
    /// `NameAlreadyExists` race from a concurrent caller creating the same stream/topic between
    /// this call's `get` and `create` is treated as success, not an error - the desired end state
    /// (it exists) is what idempotency actually promises, not that this call was the one that
    /// created it. A *different* `partition_count` against an already-existing topic is not
    /// idempotent - see [`BridgeError::PartitionCountMismatch`].
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError::Iggy`] for connectivity/auth/invalid-name failures. Returns
    /// [`BridgeError::PartitionCountMismatch`] if the topic already exists with a different
    /// partition count than `partition_count`.
    pub async fn ensure_stream_and_topic(
        &self,
        kafka_topic: &str,
        partition_count: u32,
    ) -> Result<(), BridgeError> {
        let (stream_name, topic_name) = self.config.topic_mapping.resolve(kafka_topic);
        let stream_id = self.ensure_stream(&stream_name).await?;
        self.ensure_topic(&stream_id, &topic_name, partition_count)
            .await?;
        Ok(())
    }

    /// Looks up (or creates) the stream named `stream_name`.
    ///
    /// `Identifier::named` - never `Identifier::try_from`/`FromStr` - because the latter parses
    /// an all-digit string as a numeric Iggy ID rather than a name. A stream or topic named e.g.
    /// `"42"` would otherwise resolve against the wrong resource on every call after the first:
    /// the first `ensure_stream_and_topic("42", ...)` creates a stream *named* `"42"`, but a
    /// second call would look it up *by ID* `42` instead, almost certainly finding nothing and
    /// breaking the "idempotent on repeated calls" guarantee.
    async fn ensure_stream(&self, stream_name: &str) -> Result<Identifier, BridgeError> {
        let identifier = Identifier::named(stream_name).map_err(BridgeError::Iggy)?;
        if let Some(existing) = self
            .client
            .get_stream(&identifier)
            .await
            .map_err(BridgeError::Iggy)?
        {
            debug!("Iggy stream '{stream_name}' already exists");
            return Identifier::numeric(existing.id).map_err(BridgeError::Iggy);
        }

        match self.client.create_stream(stream_name).await {
            Ok(created) => {
                info!("created Iggy stream '{stream_name}'");
                Identifier::numeric(created.id).map_err(BridgeError::Iggy)
            }
            Err(IggyError::StreamNameAlreadyExists(_)) => {
                // Lost a create race - re-verify by name (the identifier this arm already has,
                // fixed above) rather than trusting the race outcome alone.
                let existing = self
                    .client
                    .get_stream(&identifier)
                    .await
                    .map_err(BridgeError::Iggy)?
                    .ok_or_else(|| IggyError::StreamNameNotFound(stream_name.to_string()))?;
                Identifier::numeric(existing.id).map_err(BridgeError::Iggy)
            }
            Err(err) => Err(BridgeError::Iggy(err)),
        }
    }

    /// Looks up (or creates) the topic named `topic_name` under `stream_id`.
    ///
    /// `Identifier::named`, not `Identifier::try_from` - see [`Self::ensure_stream`]'s doc
    /// comment; the same numeric-name ambiguity applies to topic names.
    async fn ensure_topic(
        &self,
        stream_id: &Identifier,
        topic_name: &str,
        partition_count: u32,
    ) -> Result<(), BridgeError> {
        let identifier = Identifier::named(topic_name).map_err(BridgeError::Iggy)?;
        if let Some(existing) = self
            .client
            .get_topic(stream_id, &identifier)
            .await
            .map_err(BridgeError::Iggy)?
        {
            debug!("Iggy topic '{topic_name}' already exists");
            // ensure_topic's contract is "the topic has partition_count partitions afterward" -
            // a mismatch here means that's false. Returning Ok(()) anyway (even with a warn!)
            // would let two concurrent callers requesting different counts for the same topic
            // both believe they succeeded; growing partitions on the caller's behalf is also a
            // bigger decision (CreatePartitions has its own semantics) than this method should
            // make silently. Erring is the only response that keeps the postcondition honest.
            if existing.partitions_count != partition_count {
                return Err(BridgeError::PartitionCountMismatch {
                    topic: topic_name.to_string(),
                    existing: existing.partitions_count,
                    requested: partition_count,
                });
            }
            return Ok(());
        }

        let options = TopicCreateOptions {
            partitions_count: Some(partition_count),
            ..TopicCreateOptions::default()
        };
        match self
            .client
            .create_topic(stream_id, topic_name, &options)
            .await
        {
            Ok(_) => {
                info!("created Iggy topic '{topic_name}' with {partition_count} partitions");
                Ok(())
            }
            Err(IggyError::TopicNameAlreadyExists(_, _)) => {
                // Lost a create race - re-verify by name rather than trusting the race outcome
                // alone, the same way ensure_stream's arm does. The winner of the race may have
                // created it with a different partition count than this call requested, so this
                // needs the same mismatch check the existing-topic branch above makes - skipping
                // it here would let two concurrent ensure_topic(N) / ensure_topic(M) calls for
                // the same topic both return Ok(()).
                let existing = self
                    .client
                    .get_topic(stream_id, &identifier)
                    .await
                    .map_err(BridgeError::Iggy)?
                    .ok_or_else(|| {
                        IggyError::TopicNameNotFound(topic_name.to_string(), stream_id.to_string())
                    })?;
                if existing.partitions_count != partition_count {
                    return Err(BridgeError::PartitionCountMismatch {
                        topic: topic_name.to_string(),
                        existing: existing.partitions_count,
                        requested: partition_count,
                    });
                }
                Ok(())
            }
            Err(err) => Err(BridgeError::Iggy(err)),
        }
    }

    /// Returns the high watermark (offset the next produced message would receive) for one
    /// partition of the Iggy topic `kafka_topic` maps to.
    ///
    /// Takes `kafka_topic`, not raw Iggy stream/topic names, and resolves it through the same
    /// [`TopicMapping`](crate::bridge::topic_map::TopicMapping) `ensure_stream_and_topic`
    /// uses - a caller (a future `ListOffsets` handler ) only ever has the Kafka-side
    /// name, and a topic with a mapping override would silently query the wrong Iggy resource
    /// if this took Iggy-space names directly instead.
    ///
    /// `Partition::current_offset` is the offset of the *last written* message, not "next offset
    /// to produce" - confirmed against a live server (3 produced messages read back
    /// `current_offset == 2`). An empty partition has no last-written offset at all, so this
    /// needs a dedicated empty case rather than inferring it from `current_offset == 0` (also a
    /// fresh partition's default value, indistinguishable from "one message at offset 0").
    ///
    /// That empty case is `messages_count == 0 && current_offset == 0`, not `messages_count == 0`
    /// alone: retention cleanup decrements `messages_count` as segments are dropped
    /// (`core/partitions/src/iggy_partition.rs::decrement_messages_count`) but never rewinds
    /// `current_offset` - a fully-retention-purged partition that has produced messages reports
    /// `messages_count == 0` with `current_offset` still at its high value. Treating that as
    /// "empty" would report high watermark `0` for a partition that has, in fact, produced past
    /// offset `0`; a future `ListOffsets` (`#3537`) LATEST built on this would rewind instead of
    /// pointing at the true next-write position.
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError::Iggy`] if the mapped stream/topic doesn't exist. Returns
    /// [`BridgeError::PartitionOutOfRange`] if `partition` is beyond the topic's partition count.
    pub async fn high_watermark(
        &self,
        kafka_topic: &str,
        partition: u32,
    ) -> Result<u64, BridgeError> {
        let (stream_name, topic_name) = self.config.topic_mapping.resolve(kafka_topic);
        let stream_id = Identifier::named(&stream_name).map_err(BridgeError::Iggy)?;
        let topic_id = Identifier::named(&topic_name).map_err(BridgeError::Iggy)?;
        let details = self
            .client
            .get_topic(&stream_id, &topic_id)
            .await
            .map_err(BridgeError::Iggy)?
            .ok_or_else(|| {
                BridgeError::Iggy(IggyError::TopicNameNotFound(
                    topic_name.clone(),
                    stream_name.clone(),
                ))
            })?;

        let partition_details = details
            .partitions
            .iter()
            .find(|p| p.id == partition)
            .ok_or_else(|| BridgeError::PartitionOutOfRange {
                // The Kafka-side name a caller (a future ListOffsets handler) actually asked
                // about, not `topic_name` - a mapping override would otherwise quote the wrong
                // (Iggy-side) name back at a Kafka client that never heard of it.
                topic: kafka_topic.to_string(),
                partition,
                partitions_count: details.partitions_count,
            })?;

        Ok(
            if partition_details.messages_count == 0 && partition_details.current_offset == 0 {
                0
            } else {
                partition_details.current_offset + 1
            },
        )
    }
}
