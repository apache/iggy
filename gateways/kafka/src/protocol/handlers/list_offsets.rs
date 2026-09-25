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

//! `ListOffsets` (API key 2).

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::{ListOffsetsRequest, ListOffsetsResponse};
use tokio::time::Instant;

use crate::bridge::{BridgeError, IggyBridge};
use crate::error::Result;
use crate::protocol::api::{
    API_KEY_LIST_OFFSETS, ApiVersionRange, ERROR_NOT_LEADER_OR_FOLLOWER, ERROR_REQUEST_TIMED_OUT,
    ERROR_UNKNOWN_TOPIC_OR_PARTITION, ERROR_UNSUPPORTED_FOR_MESSAGE_FORMAT, GatewayState,
    HandleOutcome,
};
use crate::protocol::bounds_guard::validate_list_offsets_shape;
use crate::protocol::handlers::{decode_guarded, decode_request, encode_message, respond_or_close};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_LIST_OFFSETS,
    min_version: 1,
    max_version: 6,
};

/// Cap on distinct topics one `ListOffsets` request resolves through the bridge in one pass.
///
/// `bounds_guard`'s `MAX_REQUEST_ELEMENTS` (4,096) is a pre-decode `DoS` ceiling, not a usability
/// recommendation: each distinct topic here costs one `high_watermarks` round trip against the
/// single lockstep `IggyClient` every Kafka connection on this gateway shares (`README.md`'s
/// "Concurrency ceiling"). Each call takes its own turn on that shared client and releases it
/// before the next, so a large batch does not hold other connections off for its whole duration -
/// only for whichever single call is in flight at a time. 100 keeps a worst-case batch's aggregate
/// bridge cost small relative to that shared resource while remaining generous for any real
/// consumer's offset lookup. A request naming more than this many distinct topics gets the first
/// 100 resolved and the rest answered [`ERROR_REQUEST_TIMED_OUT`] with no bridge call at all - a
/// client that retries only its still-erroring topics (the common case) narrows below the cap on
/// its own within a couple of retries, rather than resending the same oversized request forever.
const MAX_BRIDGE_BACKED_TOPICS: usize = 100;

/// Wall-clock ceiling for one request's aggregate bridge work.
///
/// `ListOffsets` carries no `timeout_ms` field in any version this gateway supports (that field
/// is v10+; [`RANGE`] tops out at v6) - unlike `CreateTopics`, there is no client-supplied value
/// to honor here, so this is a fixed ceiling instead. Sized well above one `high_watermarks`
/// call's own `REQUEST_TIMEOUT` (15s, bridge-internal) so a single slow-but-alive call is not the
/// common trigger, while still bounding the sum across up to [`MAX_BRIDGE_BACKED_TOPICS`] calls -
/// without this, a large batch against a struggling bridge could hold the shared client for
/// `MAX_BRIDGE_BACKED_TOPICS * 15s`, not just one call's worth.
///
/// Applied per call, not once around the whole batch: [`resolve_all_topics`] checks it before
/// starting each topic's `high_watermarks` call and wraps the call itself in
/// [`tokio::time::timeout_at`] against the same instant, so a topic already resolved when the
/// deadline arrives keeps its real answer and only the not-yet-started ones fall back to
/// [`ERROR_REQUEST_TIMED_OUT`].
const REQUEST_DEADLINE: Duration = Duration::from_secs(20);

/// KIP-79 sentinel: the offset of the next message that would be produced.
const LATEST_TIMESTAMP: i64 = -1;
/// KIP-79 sentinel: the offset of the first message still retained.
const EARLIEST_TIMESTAMP: i64 = -2;
/// Placeholder offset/timestamp for a partition result that carries an error - matches real
/// Kafka's own convention on the error path.
const NO_OFFSET: i64 = -1;

/// [`IggyBridge::high_watermarks`]'s return type, spelled once for [`resolve_one_partition`].
type HighWatermarksResult =
    core::result::Result<Vec<(u32, core::result::Result<i64, BridgeError>)>, BridgeError>;

pub async fn handle(state: &GatewayState, api_version: i16, body: Bytes) -> HandleOutcome {
    let decoded = decode_request(
        API_KEY_LIST_OFFSETS,
        api_version,
        body,
        |v, b| decode(state, v, b),
        encode_error_response,
        "ListOffsets",
    );
    let req = match decoded {
        Ok(req) => req,
        Err(outcome) => return outcome,
    };
    let Some(bridge) = &state.bridge else {
        return respond_or_close(encode_response(api_version, &req), "ListOffsets");
    };

    let deadline = Instant::now() + REQUEST_DEADLINE;
    let topics = resolve_all_topics(bridge, &req.topics, deadline).await;
    let resp = ListOffsetsResponse::default().with_topics(topics);
    respond_or_close(encode_message(&resp, api_version, 256), "ListOffsets")
}

fn decode(state: &GatewayState, version: i16, body: Bytes) -> Result<ListOffsetsRequest> {
    decode_guarded::<ListOffsetsRequest>(version, body, |v, b| {
        validate_list_offsets_shape(v, b, state.max_frame_size)
    })
}

/// One topic's bridge-lookup outcome, decided once per distinct name in [`resolve_all_topics`]
/// and reused for every partition of that topic in [`resolve_one_partition`].
enum TopicLookup {
    /// A real `high_watermarks` call was made and returned - or every partition requested was an
    /// invalid index and there was nothing to call `high_watermarks` for, which is `Ok(vec![])`
    /// as far as [`resolve_one_partition`] is concerned (every partition in it fails its own
    /// `u32::try_from` before ever consulting this).
    Watermarks(HighWatermarksResult),
    /// Beyond [`MAX_BRIDGE_BACKED_TOPICS`], or the deadline elapsed before this topic's turn - no
    /// call was made. Answered [`ERROR_REQUEST_TIMED_OUT`] (retriable) rather than
    /// [`ERROR_INVALID_REQUEST`](crate::protocol::api::ERROR_INVALID_REQUEST) so a client's own
    /// per-topic retry narrows the batch on its own.
    NotAttempted,
}

/// Dedupes `requested` by topic name, merging every entry's partitions.
///
/// `order` preserves first-seen order so the topic cap in [`resolve_topic_lookups`] keeps a
/// deterministic prefix of the request rather than an arbitrary hash-order subset.
///
/// Deliberately does *not* skip a topic based on what timestamp its partitions ask for: the only
/// place this bridge checks whether a topic exists at all is the `get_topic` call
/// `high_watermarks` makes internally, so a topic whose partitions all ask an unsupported
/// timestamp still needs that same call to tell a nonexistent topic
/// (`ERROR_UNKNOWN_TOPIC_OR_PARTITION`) apart from an existing one with an unsupported timestamp
/// (`ERROR_UNSUPPORTED_FOR_MESSAGE_FORMAT`) - skipping it would report the latter for both.
fn group_requested_topics(requested: &[ListOffsetsTopic]) -> (Vec<&str>, HashMap<&str, Vec<u32>>) {
    let mut order: Vec<&str> = Vec::new();
    let mut partitions_by_name: HashMap<&str, Vec<u32>> = HashMap::new();
    for topic in requested {
        let name = topic.name.as_str();
        if !partitions_by_name.contains_key(name) {
            order.push(name);
        }
        let entry = partitions_by_name.entry(name).or_default();
        let valid = topic
            .partitions
            .iter()
            .filter_map(|p| u32::try_from(p.partition_index).ok());
        entry.extend(valid);
    }
    for partitions in partitions_by_name.values_mut() {
        partitions.sort_unstable();
        partitions.dedup();
    }
    (order, partitions_by_name)
}

/// Resolves one [`TopicLookup`] per name in `order`: [`TopicLookup::NotAttempted`] beyond
/// [`MAX_BRIDGE_BACKED_TOPICS`] or once `deadline` has passed, otherwise a real `high_watermarks`
/// call wrapped in [`tokio::time::timeout_at`] against `deadline` so a topic already resolved
/// when time runs out keeps its real answer. A topic with no *valid* partition index at all
/// skips the call - there is nothing `high_watermarks` could tell us that would change any
/// partition's answer, since every one of them already fails its own index check.
async fn resolve_topic_lookups<'a>(
    bridge: &IggyBridge,
    order: &[&'a str],
    partitions_by_name: &HashMap<&'a str, Vec<u32>>,
    deadline: Instant,
) -> HashMap<&'a str, TopicLookup> {
    let accepted: HashSet<&str> = order
        .iter()
        .take(MAX_BRIDGE_BACKED_TOPICS)
        .copied()
        .collect();
    if order.len() > MAX_BRIDGE_BACKED_TOPICS {
        // debug!, not warn!: the client controls how many topics it batches into one request and
        // the connection stays open, so a consumer stuck above the cap logs this every retry.
        tracing::debug!(
            distinct_topics = order.len(),
            max = MAX_BRIDGE_BACKED_TOPICS,
            "ListOffsets request exceeds the per-request topic cap; resolving the first {} and \
             answering the rest retriable",
            MAX_BRIDGE_BACKED_TOPICS
        );
    }

    let mut lookups: HashMap<&str, TopicLookup> = HashMap::new();
    let mut deadline_exceeded = false;
    for &name in order {
        if !accepted.contains(name) {
            lookups.insert(name, TopicLookup::NotAttempted);
            continue;
        }
        let partitions = partitions_by_name[name].as_slice();
        if partitions.is_empty() {
            lookups.insert(name, TopicLookup::Watermarks(Ok(Vec::new())));
            continue;
        }
        if deadline_exceeded || Instant::now() >= deadline {
            if !deadline_exceeded {
                deadline_exceeded = true;
                tracing::warn!(
                    deadline_secs = REQUEST_DEADLINE.as_secs(),
                    "ListOffsets request's aggregate bridge work exceeded its deadline; \
                     answering remaining topics retriable instead of starting new bridge calls"
                );
            }
            lookups.insert(name, TopicLookup::NotAttempted);
            continue;
        }

        let result =
            match tokio::time::timeout_at(deadline, bridge.high_watermarks(name, partitions)).await
            {
                Ok(result) => result,
                Err(_elapsed) => {
                    deadline_exceeded = true;
                    tracing::warn!(
                        topic = name,
                        deadline_secs = REQUEST_DEADLINE.as_secs(),
                        "ListOffsets bridge call for this topic exceeded the request's aggregate \
                     deadline; answering retriable instead of blocking further"
                    );
                    lookups.insert(name, TopicLookup::NotAttempted);
                    continue;
                }
            };

        if let Err(call_err) = &result {
            let kafka_code = call_err.to_kafka_error_code();
            if kafka_code == ERROR_UNKNOWN_TOPIC_OR_PARTITION {
                // Client-caused (topic doesn't exist / isn't mapped): expected traffic, not
                // operator-actionable.
                tracing::debug!(topic = name, %call_err, "ListOffsets bridge lookup: topic not found");
            } else {
                tracing::error!(topic = name, %call_err, "ListOffsets bridge lookup failed");
            }
        }
        lookups.insert(name, TopicLookup::Watermarks(result));
    }
    lookups
}

/// Resolves every requested topic entry via [`group_requested_topics`] +
/// [`resolve_topic_lookups`], then stamps each requested partition with its topic's
/// [`TopicLookup`] outcome. `EARLIEST`/`LATEST` are the only timestamps this bridge resolves -
/// Iggy exposes no per-message timestamp index - so every other requested timestamp gets
/// [`ERROR_UNSUPPORTED_FOR_MESSAGE_FORMAT`] rather than a fabricated offset.
async fn resolve_all_topics(
    bridge: &IggyBridge,
    requested: &[ListOffsetsTopic],
    deadline: Instant,
) -> Vec<ListOffsetsTopicResponse> {
    let (order, partitions_by_name) = group_requested_topics(requested);
    let lookups = resolve_topic_lookups(bridge, &order, &partitions_by_name, deadline).await;

    requested
        .iter()
        .map(|topic| {
            // Always present: `order` (and so `lookups`) was built from exactly these same
            // requested topic names, just above.
            let lookup = lookups
                .get(topic.name.as_str())
                .expect("every requested topic name was resolved above");
            let partitions = topic
                .partitions
                .iter()
                .map(|requested| resolve_one_partition(requested, lookup))
                .collect();
            ListOffsetsTopicResponse::default()
                .with_name(topic.name.clone())
                .with_partitions(partitions)
        })
        .collect()
}

/// `lookup` is the whole topic's resolution outcome: an errored [`TopicLookup::Watermarks`] is a
/// call-level failure (e.g. the mapped stream doesn't exist) applying to every partition alike;
/// the inner per-partition `Result` inside its `Ok` is [`BridgeError::PartitionOutOfRange`] for one
/// bad index among otherwise resolvable ones.
fn resolve_one_partition(
    requested: &ListOffsetsPartition,
    lookup: &TopicLookup,
) -> ListOffsetsPartitionResponse {
    let Ok(partition_index) = u32::try_from(requested.partition_index) else {
        return error_response(requested.partition_index, ERROR_UNKNOWN_TOPIC_OR_PARTITION);
    };

    let results = match lookup {
        TopicLookup::NotAttempted => {
            return error_response(requested.partition_index, ERROR_REQUEST_TIMED_OUT);
        }
        TopicLookup::Watermarks(Err(call_err)) => {
            return error_response(requested.partition_index, call_err.to_kafka_error_code());
        }
        TopicLookup::Watermarks(Ok(results)) => results,
    };

    // `results` preserves the order of the sorted, deduped partition list `resolve_all_topics`
    // passed to `high_watermarks` (`IggyBridge::high_watermarks` maps over its input in place),
    // so a binary search is correct here, not just faster than the linear scan this replaced.
    let Ok(found) = results.binary_search_by_key(&partition_index, |(index, _)| *index) else {
        return error_response(requested.partition_index, ERROR_UNKNOWN_TOPIC_OR_PARTITION);
    };
    let (_, watermark) = &results[found];

    let watermark = match watermark {
        Err(err) => return error_response(requested.partition_index, err.to_kafka_error_code()),
        Ok(watermark) => *watermark,
    };

    match requested.timestamp {
        LATEST_TIMESTAMP => offset_response(requested.partition_index, watermark),
        // Real only for a partition retention has never trimmed: Iggy tracks no rolling
        // low-watermark distinct from partition creation, so a `0` here for an older,
        // already-trimmed partition names a log-start offset that no longer exists - a real
        // consumer with `auto.offset.reset=earliest` would seek into a hole. Fetch there reads
        // from the oldest kept message, so the consumer skips the gap. Not fixable client-side;
        // needs the bridge to expose a real start offset.
        EARLIEST_TIMESTAMP => offset_response(requested.partition_index, 0),
        // Non-retriable, unlike ERROR_UNKNOWN_SERVER_ERROR: a Java client resolves this
        // immediately instead of retrying the request until its own default.api.timeout.ms.
        _ => error_response(
            requested.partition_index,
            ERROR_UNSUPPORTED_FOR_MESSAGE_FORMAT,
        ),
    }
}

fn offset_response(partition: i32, offset: i64) -> ListOffsetsPartitionResponse {
    ListOffsetsPartitionResponse::default()
        .with_partition_index(partition)
        .with_timestamp(LATEST_TIMESTAMP)
        .with_offset(offset)
}

fn error_response(partition: i32, error_code: i16) -> ListOffsetsPartitionResponse {
    ListOffsetsPartitionResponse::default()
        .with_partition_index(partition)
        .with_error_code(error_code)
        .with_timestamp(NO_OFFSET)
        .with_offset(NO_OFFSET)
}

/// Well-formed `ListOffsets` response with a single placeholder topic/partition.
///
/// `kafka_protocol` has no encodable representation for `ListOffsets` v0 (the legacy
/// `old_style_offsets` shape predates the schema this crate generates from); a v0 request now
/// falls through `super::unsupported_version_response`'s encode-failure path to `Close`
/// instead of the pre-migration downgraded response.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version` (always the
/// case for `version == 0`).
pub fn encode_error_response(version: i16, error_code: i16) -> Result<Bytes> {
    let topics = vec![
        ListOffsetsTopicResponse::default()
            .with_partitions(vec![stub_partition_response(0, error_code)]),
    ];
    encode_inner(version, topics)
}

/// Stub: discard the payload and return a retriable error, matching Produce/Fetch - a genuine
/// offset lookup requires the same partition-leadership the stub doesn't have yet.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_response(version: i16, req: &ListOffsetsRequest) -> Result<Bytes> {
    let topics = req
        .topics
        .iter()
        .map(|topic| {
            ListOffsetsTopicResponse::default()
                .with_name(topic.name.clone())
                .with_partitions(
                    topic
                        .partitions
                        .iter()
                        .map(|p| {
                            stub_partition_response(p.partition_index, ERROR_NOT_LEADER_OR_FOLLOWER)
                        })
                        .collect(),
                )
        })
        .collect();
    encode_inner(version, topics)
}

fn encode_inner(version: i16, topics: Vec<ListOffsetsTopicResponse>) -> Result<Bytes> {
    let resp = ListOffsetsResponse::default().with_topics(topics);
    encode_message(&resp, version, 256)
}

fn stub_partition_response(partition: i32, error_code: i16) -> ListOffsetsPartitionResponse {
    ListOffsetsPartitionResponse::default()
        .with_partition_index(partition)
        .with_error_code(error_code)
}

#[cfg(test)]
mod tests {
    use crate::protocol::api::ERROR_NONE;

    use super::*;

    fn partition(index: i32, timestamp: i64) -> ListOffsetsPartition {
        ListOffsetsPartition::default()
            .with_partition_index(index)
            .with_timestamp(timestamp)
    }

    fn ok_watermarks(entries: &[(u32, i64)]) -> Vec<(u32, core::result::Result<i64, BridgeError>)> {
        entries.iter().map(|&(p, w)| (p, Ok(w))).collect()
    }

    #[test]
    fn latest_resolves_to_the_watermark() {
        let lookup = TopicLookup::Watermarks(Ok(ok_watermarks(&[(0, 42)])));
        let resp = resolve_one_partition(&partition(0, LATEST_TIMESTAMP), &lookup);
        assert_eq!(resp.error_code, ERROR_NONE);
        assert_eq!(resp.offset, 42);
    }

    #[test]
    fn earliest_resolves_to_zero_regardless_of_the_watermark() {
        let lookup = TopicLookup::Watermarks(Ok(ok_watermarks(&[(0, 42)])));
        let resp = resolve_one_partition(&partition(0, EARLIEST_TIMESTAMP), &lookup);
        assert_eq!(resp.error_code, ERROR_NONE);
        assert_eq!(resp.offset, 0);
    }

    #[test]
    fn an_arbitrary_timestamp_is_unsupported() {
        let lookup = TopicLookup::Watermarks(Ok(ok_watermarks(&[(0, 42)])));
        let resp = resolve_one_partition(&partition(0, 1_700_000_000_000), &lookup);
        assert_eq!(resp.error_code, ERROR_UNSUPPORTED_FOR_MESSAGE_FORMAT);
        assert_eq!(resp.offset, NO_OFFSET);
    }

    #[test]
    fn a_nonexistent_topic_is_unknown_regardless_of_the_requested_timestamp() {
        // Regression for the NoLookupNeeded design this replaced: existence is only ever
        // established by the high_watermarks call itself, so a call-level error must win over
        // the timestamp branch even when the requested timestamp is unsupported - the call is
        // never skipped just because nothing would use its watermark.
        let lookup = TopicLookup::Watermarks(Err(BridgeError::Timeout));
        let resp = resolve_one_partition(&partition(0, 1_700_000_000_000), &lookup);
        assert_eq!(resp.error_code, BridgeError::Timeout.to_kafka_error_code());
    }

    #[test]
    fn a_topic_beyond_the_cap_or_past_the_deadline_answers_retriable() {
        let resp =
            resolve_one_partition(&partition(0, LATEST_TIMESTAMP), &TopicLookup::NotAttempted);
        assert_eq!(resp.error_code, ERROR_REQUEST_TIMED_OUT);
        assert_eq!(resp.offset, NO_OFFSET);
    }

    #[test]
    fn a_negative_partition_index_is_rejected_without_consulting_the_bridge_result() {
        let lookup = TopicLookup::Watermarks(Ok(vec![]));
        let resp = resolve_one_partition(&partition(-1, LATEST_TIMESTAMP), &lookup);
        assert_eq!(resp.error_code, ERROR_UNKNOWN_TOPIC_OR_PARTITION);
    }

    #[test]
    fn a_partition_specific_error_only_affects_that_partition() {
        let lookup = TopicLookup::Watermarks(Ok(vec![(
            0,
            Err(BridgeError::PartitionOutOfRange {
                topic: "orders".to_string(),
                partition: 0,
                partitions_count: 0,
            }),
        )]));
        let resp = resolve_one_partition(&partition(0, LATEST_TIMESTAMP), &lookup);
        assert_eq!(resp.error_code, ERROR_UNKNOWN_TOPIC_OR_PARTITION);
    }

    #[test]
    fn a_call_level_error_applies_regardless_of_the_requested_timestamp() {
        let lookup = TopicLookup::Watermarks(Err(BridgeError::Timeout));
        let resp = resolve_one_partition(&partition(0, EARLIEST_TIMESTAMP), &lookup);
        assert_eq!(resp.error_code, BridgeError::Timeout.to_kafka_error_code());
    }

    #[test]
    fn a_partition_index_absent_from_a_sorted_result_is_rejected() {
        // Exercises the binary search on a multi-entry, sorted-by-index result - a single-entry
        // vec would pass a linear scan and a broken binary search alike.
        let lookup = TopicLookup::Watermarks(Ok(ok_watermarks(&[(0, 10), (2, 30), (5, 60)])));
        let resp = resolve_one_partition(&partition(3, LATEST_TIMESTAMP), &lookup);
        assert_eq!(resp.error_code, ERROR_UNKNOWN_TOPIC_OR_PARTITION);
    }
}
