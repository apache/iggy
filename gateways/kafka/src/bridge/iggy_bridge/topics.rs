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

use std::collections::{HashMap, HashSet};

use iggy::prelude::{
    Identifier, IggyError, StreamClient, TopicClient, TopicCreateOptions, TopicDetails,
};
use kafka_protocol::protocol::StrBytes;
use tracing::{debug, info};

use super::{IggyBridge, with_request_timeout};
use crate::bridge::error::BridgeError;
use crate::bridge::topic_map::validate_kafka_topic_name;

/// Outcome of [`IggyBridge::create_kafka_topic`].
///
/// Distinguishes "this call is the one that created it" from "it already existed", so
/// `CreateTopics` can answer `TOPIC_ALREADY_EXISTS` correctly even when two requests for the same
/// new topic race each other. Unlike [`IggyBridge::ensure_stream_and_topic`]'s idempotent-success
/// contract, `CreateTopics` itself is not an upsert: real Kafka guarantees exactly one caller sees
/// a create succeed, every other concurrent caller for the same new name sees
/// `TOPIC_ALREADY_EXISTS`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicCreationOutcome {
    Created,
    AlreadyExists,
}

/// One Kafka-visible topic, as reported by [`IggyBridge::list_kafka_topics`].
///
/// Deliberately narrower than the SDK's own `TopicDetails` - `Metadata`'s "all topics" listing
/// needs only the Kafka-side name and a partition count, not every Iggy-internal field.
/// [`IggyBridge::get_kafka_topic`] (a single named lookup) returns the full `TopicDetails`
/// instead; this type exists only because `list_kafka_topics` must carry a *resolved* Kafka-side
/// name for each entry, which `TopicDetails` alone (just the raw Iggy-side name) cannot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaTopicMetadata {
    pub kafka_topic: String,
    pub partitions_count: u32,
}

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
    /// already accepts that sentinel at the wire-validation layer). Resolving *what* the broker
    /// default should be for this bridge is a decision for whichever of `#3535`/`#3536` first
    /// calls this with a real Kafka request in hand, not one to invent here ahead of that need.
    ///
    /// No caching: every call pays a stream-create attempt ([`Self::ensure_stream`]) and a
    /// `get_topic` (two round trips once both already exist), even for a topic this same bridge
    /// already confirmed a moment ago. A cache keyed on `kafka_topic` would remove that cost, but
    /// would also have to answer "how does a cache entry ever get invalidated" - the topic being
    /// deleted and recreated with a different partition count out from under a stale cache entry
    /// is exactly
    /// `ensure_topic_targets_the_streams_live_incarnation_after_a_delete_and_recreate`'s own
    /// scenario, and a naive cache breaks that guarantee to save two round trips. Whatever wires
    /// this into `#3535`/`#3536` should call it once per topic and remember that it did, rather
    /// than once per Produce/Fetch - the cost belongs at the call site's discretion, not hidden
    /// (and potentially made wrong) inside this method.
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError::InvalidKafkaTopicName`] if `kafka_topic` fails Kafka's own
    /// topic-naming rules. Returns [`BridgeError::Timeout`] if a call takes longer than
    /// `REQUEST_TIMEOUT`. Returns [`BridgeError::Iggy`] for connectivity/auth failures. Returns
    /// [`BridgeError::PartitionCountMismatch`] if the topic already exists with a different
    /// partition count than `partition_count`. Returns [`BridgeError::InvalidPartitionCount`] if
    /// `partition_count` is 0.
    pub async fn ensure_stream_and_topic(
        &self,
        kafka_topic: &str,
        partition_count: u32,
    ) -> Result<(), BridgeError> {
        validate_kafka_topic_name("kafka_topic", kafka_topic)?;
        if partition_count == 0 {
            return Err(BridgeError::InvalidPartitionCount {
                kafka_topic: kafka_topic.to_string(),
            });
        }
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
    ///
    /// Attempts the create directly rather than probing existence first: `get_stream` answers
    /// with `StreamDetails`, which embeds every topic header in the stream (`iggy_common`'s
    /// `StreamDetails.topics: Vec<Topic>`), not just the stream's own metadata - a probe paid on
    /// every call, including the steady-state case where the stream (almost always) already
    /// exists, could cost one full per-stream topic listing per topic in a `CreateTopics` batch
    /// (up to 100 today). `StreamNameAlreadyExists` on the create attempt is exactly as
    /// informative as a prior `get_stream` would have been - the stream exists either way - so
    /// this pays one round trip in every case instead of up to two in the common one.
    async fn ensure_stream(&self, stream_name: &str) -> Result<Identifier, BridgeError> {
        let identifier = Identifier::named(stream_name).map_err(BridgeError::Iggy)?;
        match with_request_timeout(self.client.create_stream(stream_name)).await {
            Ok(_created) => {
                info!("created Iggy stream '{stream_name}'");
                Ok(identifier)
            }
            Err(BridgeError::Iggy(IggyError::StreamNameAlreadyExists(_))) => {
                debug!("Iggy stream '{stream_name}' already exists");
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
    /// `partition_count == 0` is unreachable here: [`IggyBridge::ensure_stream_and_topic`] rejects
    /// it with [`BridgeError::InvalidPartitionCount`] before calling this method. The server
    /// itself allows 0 by design (`rewrite.rs`), so this is defense in depth against a future
    /// caller of this crate-private method skipping that check, not the primary guard.
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

    /// Looks up `kafka_topic`, resolved through the configured
    /// [`TopicMapping`](crate::bridge::topic_map::TopicMapping), without creating it.
    ///
    /// Returns `Ok(None)` when either the mapped stream or the mapped topic doesn't exist -
    /// callers (`CreateTopics`' existence check, `Metadata`'s lookup) treat both the same way:
    /// nothing answers to this Kafka-side name yet.
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError::InvalidKafkaTopicName`] if `kafka_topic` fails Kafka's own
    /// topic-naming rules. Returns [`BridgeError::Timeout`] if a call takes longer than
    /// `REQUEST_TIMEOUT`. Returns [`BridgeError::Iggy`] for connectivity/auth failures.
    pub async fn get_kafka_topic(
        &self,
        kafka_topic: &str,
    ) -> Result<Option<TopicDetails>, BridgeError> {
        validate_kafka_topic_name("kafka_topic", kafka_topic)?;
        let (stream_name, topic_name) = self.config.topic_mapping.resolve(kafka_topic);
        let stream_id = Identifier::named(stream_name).map_err(BridgeError::Iggy)?;
        let topic_id = Identifier::named(topic_name).map_err(BridgeError::Iggy)?;
        // No separate get_stream probe: get_topic already answers Ok(None) when the stream
        // itself is missing (see high_watermarks' own doc on this same fact), so a probe first
        // would just pay a second round trip to learn something this one call already tells us.
        with_request_timeout(self.client.get_topic(&stream_id, &topic_id)).await
    }

    /// Resolves many Kafka-visible names at once, one entry per input in the same order.
    ///
    /// Batches by the Iggy stream each name resolves to, rather than paying one round trip per
    /// name: most requested names share the default stream, and override targets are a handful
    /// at most, so the real round-trip cost is the number of *distinct streams* involved, not the
    /// number of names. A caller with many requested names but few distinct target streams pays
    /// one `get_topics` call per stream - this is what makes it safe to resolve an unbounded
    /// number of names in one call, unlike looping [`Self::get_kafka_topic`] per name, which pays
    /// one round trip per name regardless of how many share a stream and needs its own
    /// caller-side cap to keep that cost bounded.
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError::Timeout`] if a call takes longer than `REQUEST_TIMEOUT`. Returns
    /// [`BridgeError::Iggy`] for connectivity/auth failures.
    pub async fn get_kafka_topics(
        &self,
        kafka_topics: &[StrBytes],
    ) -> Result<Vec<(StrBytes, Option<KafkaTopicMetadata>)>, BridgeError> {
        let resolved: Vec<(StrBytes, String, String)> = kafka_topics
            .iter()
            .map(|kafka_topic| {
                let (stream_name, topic_name) =
                    self.config.topic_mapping.resolve(kafka_topic.as_str());
                (
                    kafka_topic.clone(),
                    stream_name.to_string(),
                    topic_name.to_string(),
                )
            })
            .collect();

        let distinct_streams: HashSet<String> = resolved
            .iter()
            .map(|(_, stream_name, _)| stream_name.clone())
            .collect();

        let mut topics_by_stream: HashMap<String, HashMap<String, u32>> =
            HashMap::with_capacity(distinct_streams.len());
        for stream_name in distinct_streams {
            let stream_id = Identifier::named(&stream_name).map_err(BridgeError::Iggy)?;
            let topics = with_request_timeout(self.client.get_topics(&stream_id)).await?;
            let by_name = topics
                .into_iter()
                .map(|topic| (topic.name, topic.partitions_count))
                .collect();
            topics_by_stream.insert(stream_name, by_name);
        }

        Ok(resolved
            .into_iter()
            .map(|(kafka_topic, stream_name, topic_name)| {
                let metadata = topics_by_stream
                    .get(&stream_name)
                    .and_then(|topics| topics.get(&topic_name))
                    .map(|&partitions_count| KafkaTopicMetadata {
                        kafka_topic: kafka_topic.as_str().to_string(),
                        partitions_count,
                    });
                (kafka_topic, metadata)
            })
            .collect())
    }

    /// Creates the Iggy stream/topic backing `kafka_topic`, or reports that it already exists.
    ///
    /// Atomic from this call's perspective, unlike a separate existence check
    /// ([`Self::get_kafka_topic`]) followed by [`Self::ensure_stream_and_topic`]: that sequence
    /// has a TOCTOU window between the two calls, and `ensure_stream_and_topic`'s own idempotent
    /// contract would then absorb a second concurrent caller's create into a silent `Ok`, so both
    /// callers see success for a `CreateTopics` request Kafka promises exactly one `NONE` for.
    /// Here, the create attempt itself is the existence check: no separate read precedes it, and
    /// [`TopicCreationOutcome::AlreadyExists`] comes from the server's own rejection of the write,
    /// not from an earlier read that could already be stale by the time this call's write lands.
    ///
    /// # Errors
    ///
    /// Same as [`Self::ensure_stream_and_topic`], except an already-existing topic is reported as
    /// [`TopicCreationOutcome::AlreadyExists`] rather than [`BridgeError::PartitionCountMismatch`].
    /// `CreateTopics` is not an upsert, so a pre-existing topic is never itself an error here,
    /// regardless of whether its partition count matches `partition_count`.
    pub async fn create_kafka_topic(
        &self,
        kafka_topic: &str,
        partition_count: u32,
    ) -> Result<TopicCreationOutcome, BridgeError> {
        validate_kafka_topic_name("kafka_topic", kafka_topic)?;
        if partition_count == 0 {
            return Err(BridgeError::InvalidPartitionCount {
                kafka_topic: kafka_topic.to_string(),
            });
        }
        let (stream_name, topic_name) = self.config.topic_mapping.resolve(kafka_topic);
        let stream_id = self.ensure_stream(stream_name).await?;

        let options = TopicCreateOptions {
            partitions_count: Some(partition_count),
            ..TopicCreateOptions::default()
        };
        match with_request_timeout(self.client.create_topic(&stream_id, topic_name, &options)).await
        {
            Ok(created) => {
                info!("created Iggy topic '{topic_name}' with {partition_count} partitions");
                // Same postcondition check ensure_topic's own create branch makes: partitions_count
                // is a hard argument (Some(partition_count), never None), so a mismatch here means
                // a future server-side clamp/cap, not a client input problem - fails loudly instead
                // of silently reporting Created under a broken contract.
                if created.partitions_count != partition_count {
                    return Err(BridgeError::PartitionCountMismatch {
                        topic: kafka_topic.to_string(),
                        existing: created.partitions_count,
                        requested: partition_count,
                    });
                }
                Ok(TopicCreationOutcome::Created)
            }
            Err(BridgeError::Iggy(IggyError::TopicNameAlreadyExists(_, _))) => {
                Ok(TopicCreationOutcome::AlreadyExists)
            }
            Err(err) => Err(err),
        }
    }

    /// Every Kafka-visible topic: the target of every configured
    /// [`TopicMapping`](crate::bridge::topic_map::TopicMapping) override that actually exists in
    /// Iggy, plus every topic in the default stream that isn't itself one of those override
    /// targets and isn't itself named the same as an override key - checked so an overridden
    /// topic is never listed twice, once under its Kafka-side name and once under its raw Iggy
    /// name, and so a raw default-stream topic never masquerades under a Kafka-side name an
    /// override has already claimed for different data. The second check matters for a chained
    /// override (`foo -> (kafka, bar)`, `bar -> (other, x)`): without it, a raw Iggy topic
    /// literally named `foo` sitting in the default stream would be reported a second time under
    /// the same `foo` name the override loop already emitted (backed by `kafka/bar`'s data), and
    /// that second `foo` would be unreachable by name anyway, since `get_kafka_topic("foo")`
    /// always resolves through the override to `kafka/bar`, never to the raw `kafka/foo`.
    ///
    /// An Iggy stream this bridge has no mapping rule pointing at (neither the default stream nor
    /// any override's target) holds data no Kafka client ever named - deliberately excluded, the
    /// same way a real Kafka broker never reports storage it doesn't own.
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError::Timeout`] if a call takes longer than `REQUEST_TIMEOUT`. Returns
    /// [`BridgeError::Iggy`] for connectivity/auth failures.
    pub async fn list_kafka_topics(&self) -> Result<Vec<KafkaTopicMetadata>, BridgeError> {
        let default_stream = self.config.topic_mapping.default_stream();
        let mut default_stream_override_targets: HashSet<&str> = HashSet::new();
        let mut override_keys: HashSet<&str> = HashSet::new();
        let mut results = Vec::new();

        for (kafka_topic, over) in self.config.topic_mapping.overrides() {
            override_keys.insert(kafka_topic);
            if over.stream == default_stream {
                default_stream_override_targets.insert(over.topic.as_str());
            }
            match self.get_kafka_topic(kafka_topic).await {
                Ok(Some(details)) => results.push(KafkaTopicMetadata {
                    kafka_topic: kafka_topic.to_string(),
                    partitions_count: details.partitions_count,
                }),
                Ok(None) => {}
                // One override naming a topic this caller (the bridge user) can't read must not
                // abort every other topic's listing - real Kafka's own DescribeTopics skips a
                // topic the caller lacks ACLs for rather than failing the whole response. Any
                // other error kind (timeout, connectivity) still propagates: those aren't
                // per-topic facts, they mean nothing in this response can be trusted.
                Err(BridgeError::Iggy(IggyError::Unauthorized)) => {
                    debug!(
                        "skipping override '{kafka_topic}' -> '{}/{}' in Metadata: caller is \
                         unauthorized to read it",
                        over.stream, over.topic
                    );
                }
                Err(err) => return Err(err),
            }
        }

        let default_stream_id = Identifier::named(default_stream).map_err(BridgeError::Iggy)?;
        // No separate get_stream probe: get_topics already answers an empty list when the stream
        // itself is missing (server-side "legacy parity: a missing stream lists as empty, not
        // StreamNotFound"), so a probe first would pay a second round trip to learn something
        // this one call already tells us - and it costs an extra ACL check this caller might not
        // even have, when a user with only read_topics on the default stream (no read_streams)
        // should still see its topics listed.
        let topics = with_request_timeout(self.client.get_topics(&default_stream_id)).await?;
        for topic in topics {
            if default_stream_override_targets.contains(topic.name.as_str())
                || override_keys.contains(topic.name.as_str())
            {
                continue;
            }
            if let Err(reason) = validate_kafka_topic_name("kafka_topic", &topic.name) {
                // A raw Iggy topic name that isn't itself a legal Kafka topic name (e.g. contains
                // a space) would list under a name no named Metadata/CreateTopics lookup can ever
                // resolve back - `validate_kafka_topic_name` is the same gate every named path
                // already enforces, so this keeps "listed" and "reachable by name" the same set.
                debug!(
                    "skipping raw Iggy topic '{}' in Metadata: {reason}",
                    topic.name
                );
                continue;
            }
            results.push(KafkaTopicMetadata {
                kafka_topic: topic.name,
                partitions_count: topic.partitions_count,
            });
        }

        Ok(results)
    }
}
