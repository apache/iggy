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

//! Stream and topic provisioning.

use iggy::prelude::{Identifier, IggyError, StreamClient, TopicClient, TopicCreateOptions};
use tracing::{debug, info};

use super::{IggyBridge, with_request_timeout};
use crate::bridge::error::BridgeError;
use crate::bridge::topic_map::validate_kafka_topic_name;

impl IggyBridge {
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
    /// Ensures the stream before the topic, so a topic-creation failure (a different
    /// `partition_count`, a transient error) can leave a stream that now exists with no topic in
    /// it yet - a retry heals this (idempotent on the stream half too), and no rollback is
    /// attempted: `TopicMapping::resolve` sends every *unmapped* Kafka topic to the same
    /// `default_stream`, so deleting a stream on a topic-creation failure risks deleting another
    /// topic's data that happens to share it, and this call has no way to tell whether it was the
    /// one that created the stream in the first place.
    ///
    /// `partition_count` is `u32`, so it cannot carry Kafka's own `CreateTopics` sentinel
    /// (`num_partitions == -1`, KIP-464 "use the broker default" - `protocol/responses.rs`
    /// already accepts that sentinel at the wire-validation layer). The broker default is for
    /// the first caller with a real Kafka request (`Metadata` or `CreateTopics`) to decide.
    ///
    /// No caching: every call pays a `get_stream` and a `get_topic` (two round trips once both
    /// already exist), even for a topic this same bridge already confirmed a moment ago. A cache
    /// keyed on `kafka_topic` would remove that cost, but would also have to answer "how does a
    /// cache entry ever get invalidated" - the topic being deleted and recreated with a different
    /// partition count out from under a stale cache entry is exactly
    /// `ensure_topic_targets_the_streams_live_incarnation_after_a_delete_and_recreate`'s own
    /// scenario, and a naive cache breaks that guarantee to save two round trips. A caller must
    /// call it once per topic and remember that it did, not once per request. Produce does not
    /// call it: it creates nothing.
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError::InvalidKafkaTopicName`] if `kafka_topic` fails Kafka's own
    /// topic-naming rules. Returns [`BridgeError::Timeout`] if a call takes longer than
    /// `REQUEST_TIMEOUT`. Returns [`BridgeError::Iggy`] for connectivity/auth failures. Returns
    /// [`BridgeError::PartitionCountMismatch`] if the topic already exists with a different
    /// partition count than `partition_count`.
    pub async fn ensure_stream_and_topic(
        &self,
        kafka_topic: &str,
        partition_count: u32,
    ) -> Result<(), BridgeError> {
        validate_kafka_topic_name("kafka_topic", kafka_topic)?;
        let (stream_name, topic_name) = self.config.topic_mapping.resolve(kafka_topic);
        let stream_id = self.ensure_stream(stream_name).await?;
        self.ensure_topic(&stream_id, topic_name, kafka_topic, partition_count)
            .await?;
        Ok(())
    }

    /// Ensures the stream named `stream_name` exists, creating it if missing.
    ///
    /// `Identifier::named` - never `Identifier::try_from`/`FromStr` - because the latter parses
    /// an all-digit string as a numeric Iggy ID rather than a name. A stream or topic named e.g.
    /// `"42"` would otherwise resolve against the wrong resource on every call after the first:
    /// the first `ensure_stream_and_topic("42", ...)` creates a stream *named* `"42"`, but a
    /// second call would look it up *by ID* `42` instead, almost certainly finding nothing and
    /// breaking the "idempotent on repeated calls" guarantee.
    ///
    /// Returns the same *named* `Identifier` it was given, not the numeric id the SDK hands back
    /// from `get`/`create` - streams are backed by a recycled slab (`core/metadata`'s
    /// `stm/stream.rs`: freed keys are reused by the next created stream), so a numeric id
    /// captured here could point at a *different* stream by the time `ensure_topic` uses it, if
    /// this stream is deleted and recreated in between. The name has no such window.
    async fn ensure_stream(&self, stream_name: &str) -> Result<Identifier, BridgeError> {
        let identifier = Identifier::named(stream_name).map_err(BridgeError::Iggy)?;
        if with_request_timeout(self.client.get_stream(&identifier))
            .await?
            .is_some()
        {
            debug!("Iggy stream '{stream_name}' already exists");
            return Ok(identifier);
        }

        match with_request_timeout(self.client.create_stream(stream_name)).await {
            Ok(_created) => {
                info!("created Iggy stream '{stream_name}'");
                Ok(identifier)
            }
            Err(BridgeError::Iggy(IggyError::StreamNameAlreadyExists(_))) => {
                // Lost a create race - the name now exists regardless of who won it.
                Ok(identifier)
            }
            Err(err) => Err(err),
        }
    }

    /// Looks up (or creates) the topic named `topic_name` under `stream_id`.
    ///
    /// `Identifier::named`, not `Identifier::try_from` - the same numeric-name ambiguity
    /// [`Self::ensure_stream`]'s doc comment describes for stream names applies to topic names.
    ///
    /// `partition_count == 0` is accepted here as defense in depth, not the primary guard: the
    /// server allows it by design (`rewrite.rs`), and the `CreateTopics` stub already rejects it
    /// at the wire level (`protocol/responses.rs`) before any bridge call would be reachable.
    async fn ensure_topic(
        &self,
        stream_id: &Identifier,
        topic_name: &str,
        kafka_topic: &str,
        partition_count: u32,
    ) -> Result<(), BridgeError> {
        let identifier = Identifier::named(topic_name).map_err(BridgeError::Iggy)?;
        if let Some(existing) =
            with_request_timeout(self.client.get_topic(stream_id, &identifier)).await?
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
                    // The Kafka-side name a caller actually asked about, not `topic_name` - see
                    // the identical note on `PartitionOutOfRange` in `high_watermark`.
                    topic: kafka_topic.to_string(),
                    existing: existing.partitions_count,
                    requested: partition_count,
                });
            }
            return Ok(());
        }

        // message_expiry left at TopicCreateOptions::default() (None -> ServerDefault) means
        // never-expire (segment_cleaner.rs treats ServerDefault the same as NeverExpire), not
        // Kafka's own 7-day default - deliberate for now (imposing a retention policy is a product
        // decision this bridge shouldn't make unasked), but a real surprise for anyone repointing
        // a Kafka app that assumes bounded retention. Flagged in the README; revisit once there's
        // a way to configure it (env var, topic-mapping field) rather than hardcoding a number.
        let options = TopicCreateOptions {
            partitions_count: Some(partition_count),
            ..TopicCreateOptions::default()
        };
        match with_request_timeout(self.client.create_topic(stream_id, topic_name, &options)).await
        {
            Ok(created) => {
                info!("created Iggy topic '{topic_name}' with {partition_count} partitions");
                // Cheap: TopicDetails is already in hand, no extra round trip. `partitions_count`
                // is a hard argument to create_topic (Some(partition_count), never None), so the
                // server has no "resolve at admission" substitution to fall back on here - but
                // checking anyway, the same way the other two branches check their own
                // postcondition, means a future server-side clamp/cap fails loudly here instead
                // of this method silently reporting success under a broken contract.
                if created.partitions_count != partition_count {
                    return Err(BridgeError::PartitionCountMismatch {
                        topic: kafka_topic.to_string(),
                        existing: created.partitions_count,
                        requested: partition_count,
                    });
                }
                Ok(())
            }
            // Lost a create race - re-verify by name rather than trusting the race outcome alone.
            // The winner may have created it with a different partition count than this call
            // requested, so this needs the same mismatch check the existing-topic branch above
            // makes - skipping it here would let two concurrent ensure_topic(N) / ensure_topic(M)
            // calls for the same topic both return Ok(()).
            //
            // Untested: reaching this arm needs a real concurrent second caller mid-race, and
            // `IggyBridge` holds a concrete `IggyClient` with no seam for a fake that returns
            // `TopicNameAlreadyExists` on demand. A test that spins up two real concurrent callers
            // would hit it only sometimes - flaky, and passing wouldn't prove this arm ran. Left
            // as a known gap until something introduces a client seam.
            Err(BridgeError::Iggy(IggyError::TopicNameAlreadyExists(_, _))) => {
                let existing = with_request_timeout(self.client.get_topic(stream_id, &identifier))
                    .await?
                    .ok_or_else(|| {
                        BridgeError::Iggy(IggyError::TopicNameNotFound(
                            topic_name.to_string(),
                            stream_id.to_string(),
                        ))
                    })?;
                if existing.partitions_count != partition_count {
                    return Err(BridgeError::PartitionCountMismatch {
                        topic: kafka_topic.to_string(),
                        existing: existing.partitions_count,
                        requested: partition_count,
                    });
                }
                Ok(())
            }
            Err(err) => Err(err),
        }
    }
}
