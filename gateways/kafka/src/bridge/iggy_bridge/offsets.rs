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

//! Offset and high-watermark lookups.

use iggy::prelude::{Identifier, IggyError, TopicClient};

use super::{IggyBridge, with_request_timeout};
use crate::bridge::error::BridgeError;
use crate::bridge::topic_map::validate_kafka_topic_name;

impl IggyBridge {
    /// Returns the high watermark (one past the highest *committed* offset - see
    /// `Partition::offset_frontier`'s own definition) for every partition in `partitions` of the
    /// Iggy topic `kafka_topic` maps to, in one round trip.
    ///
    /// Takes `kafka_topic`, not raw Iggy stream/topic names, and resolves it through the same
    /// [`TopicMapping`](crate::bridge::topic_map::TopicMapping) `ensure_stream_and_topic`
    /// uses - a caller (a future `ListOffsets` handler) only ever has the Kafka-side name, and a
    /// topic with a mapping override would silently query the wrong Iggy resource if this took
    /// Iggy-space names directly instead.
    ///
    /// One call to `get_topic`, not one per partition: a single Kafka `ListOffsets` request asks
    /// about many partitions of one topic at once (`ListOffsetsRequest.json`'s
    /// `Topics[] -> Partitions[]`), and `get_topic`'s decode already rebuilds and sorts the whole
    /// partition vector regardless of how many of them the caller wants
    /// (`wire_conversions.rs`) - a naive one-call-per-partition wrapper around a
    /// single-partition method would turn one Kafka request into N round trips and N times the
    /// decode work. Returns results in the same order as `partitions`.
    ///
    /// `i64`, not `u64`: the `ListOffsets` response field this exists to fill is `int64`
    /// (`ListOffsetsResponse.json`), so a future handler needs no cast at the wire boundary - the
    /// lossy conversion happens once, here (`Partition::current_offset` is `u64`; converted via
    /// `i64::try_from`, saturating to `i64::MAX` on the practically-unreachable overflow case
    /// rather than panicking or wrapping).
    ///
    /// `Partition::current_offset` is the offset of the *last written* message, not "next offset
    /// to produce" - confirmed against a live server (3 produced messages read back
    /// `current_offset == 2`). An empty partition has no last-written offset at all, so this
    /// needs a dedicated empty case rather than inferring it from `current_offset == 0` (also a
    /// fresh partition's default value, indistinguishable from "one message at offset 0").
    ///
    /// That empty case is `messages_count == 0 && current_offset == 0`, not `messages_count == 0`
    /// alone: retention cleanup decrements `messages_count`
    /// (`iggy_partition.rs::decrement_messages_count`) without rewinding `current_offset`, so a
    /// fully-purged but previously-produced-to partition would otherwise read as empty. This is
    /// still not exact: a partition whose *only* message, at offset 0, gets trimmed by the same
    /// retention pass reports `(messages_count, current_offset) == (0, 0)` too, so its watermark
    /// would read `1` then drop back to `0` - a real Kafka watermark never moves backward. Narrow
    /// (needs retention to fully empty a partition that only ever held one message) and not
    /// fixable client-side (`PartitionResponse` carries no frontier field to disambiguate it), but
    /// worth having on the record rather than only implicitly true of the check below.
    ///
    /// Not the same value as `Partition::mint_frontier` (the offset the *next* mint will take):
    /// after a crash-recovery reservation, the append point can sit above the committed frontier
    /// by the reservation's lease block, and neither value is on the wire to tell them apart. This
    /// is still correct for `ListOffsets` LATEST (defined in terms of the committed offset), but a
    /// future Produce handler must not use it to predict a base offset for a write in flight.
    ///
    /// Known remaining gap, not fixable client-side: `(0, 0)` is also what a stats-registry MISS
    /// reports (`responses.rs`'s `PartitionResponse` builder), indistinguishable on the wire from
    /// a genuinely empty partition. Narrow and self-healing - `partition_reconciler.rs`'s
    /// `settle_partition_stats` opens this only on the teardown-for-rebuild path, closing once the
    /// rebuild completes; deletes never open it. The real answer (`Partition::offset_frontier`)
    /// stays server-side and isn't in this response. The check matches the server's own
    /// `PartitionState::store_offset_range_error` condition, so it's not an invented heuristic - a
    /// future `ListOffsets` (`#3537`) built on this inherits the same blind spot.
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError::InvalidKafkaTopicName`] if `kafka_topic` fails Kafka's own
    /// topic-naming rules. Returns [`BridgeError::Timeout`] if the call takes longer than
    /// `REQUEST_TIMEOUT`. Returns [`BridgeError::Iggy`] if the mapped stream doesn't exist (see
    /// the note above on which resource is actually missing). These are call-level failures -
    /// nothing about any individual partition could be resolved. A single out-of-range
    /// `partition` does not fail the whole call: it is reported per-partition (see the return
    /// type), so the partitions that did resolve are never discarded to report one that didn't.
    pub async fn high_watermarks(
        &self,
        kafka_topic: &str,
        partitions: &[u32],
    ) -> Result<Vec<(u32, Result<i64, BridgeError>)>, BridgeError> {
        validate_kafka_topic_name("kafka_topic", kafka_topic)?;
        let (stream_name, topic_name) = self.config.topic_mapping.resolve(kafka_topic);
        let stream_id = Identifier::named(stream_name).map_err(BridgeError::Iggy)?;
        let topic_id = Identifier::named(topic_name).map_err(BridgeError::Iggy)?;
        let details = with_request_timeout(self.client.get_topic(&stream_id, &topic_id))
            .await?
            .ok_or_else(|| {
                // A missing *stream* also makes get_topic return Ok(None) (responses.rs), so this
                // reports TopicNameNotFound even when the stream is what's actually gone - both
                // map to the same Kafka wire code either way, so only the log text is affected.
                BridgeError::Iggy(IggyError::TopicNameNotFound(
                    topic_name.to_string(),
                    stream_name.to_string(),
                ))
            })?;

        Ok(partitions
            .iter()
            .map(|&partition| {
                // `TryFrom<GetTopicResponse> for TopicDetails` (wire_conversions.rs) sorts
                // `partitions` by `id` on every decode, so a binary search is correct here, not
                // just faster than a linear scan - for a 1000-partition topic, the difference is
                // O(log n) vs O(n) probes per requested partition.
                let watermark = details
                    .partitions
                    .binary_search_by_key(&partition, |p| p.id)
                    .map(|index| &details.partitions[index])
                    .map_err(|_| BridgeError::PartitionOutOfRange {
                        // The Kafka-side name a caller (a future ListOffsets handler) actually
                        // asked about, not `topic_name` - a mapping override would otherwise
                        // quote the wrong (Iggy-side) name back at a Kafka client that never
                        // heard of it.
                        topic: kafka_topic.to_string(),
                        partition,
                        partitions_count: details.partitions_count,
                    })
                    .map(|partition_details| {
                        if partition_details.messages_count == 0
                            && partition_details.current_offset == 0
                        {
                            0
                        } else {
                            partition_details.current_offset.saturating_add(1)
                        }
                    })
                    .map(|watermark| i64::try_from(watermark).unwrap_or(i64::MAX));
                (partition, watermark)
            })
            .collect())
    }

    /// Convenience wrapper around [`Self::high_watermarks`] for a single partition. See that
    /// method's doc for the full semantics - callers wanting more than one partition of the same
    /// topic should call it directly instead of this in a loop, to get its one-round-trip batching.
    ///
    /// # Errors
    ///
    /// See [`Self::high_watermarks`].
    ///
    /// # Panics
    ///
    /// Never in practice: [`Self::high_watermarks`] returns exactly one result per requested
    /// partition on `Ok`, and this always requests exactly one.
    pub async fn high_watermark(
        &self,
        kafka_topic: &str,
        partition: u32,
    ) -> Result<i64, BridgeError> {
        let (_, watermark) = self
            .high_watermarks(kafka_topic, &[partition])
            .await?
            .into_iter()
            .next()
            .expect(
                "high_watermarks returns exactly one result per requested partition, \
                 and exactly one was requested",
            );
        watermark
    }
}
