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

use crate::bridge::{BridgeError, IggyBridge};
use crate::error::Result;
use crate::protocol::api::{
    API_KEY_LIST_OFFSETS, ApiVersionRange, ERROR_INVALID_REQUEST, ERROR_NOT_LEADER_OR_FOLLOWER,
    ERROR_REQUEST_TIMED_OUT, ERROR_UNKNOWN_SERVER_ERROR, ERROR_UNKNOWN_TOPIC_OR_PARTITION,
    GatewayState, HandleOutcome,
};
use crate::protocol::bounds_guard::validate_list_offsets_shape;
use crate::protocol::handlers::{
    decode_guarded, encode_message, handle_versioned_request, is_supported_version,
    respond_or_close, unsupported_version_response,
};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_LIST_OFFSETS,
    min_version: 1,
    max_version: 6,
};

/// Cap on distinct topics one `ListOffsets` request may address through the bridge.
///
/// `bounds_guard`'s `MAX_REQUEST_ELEMENTS` (4,096) is a pre-decode `DoS` ceiling, not a usability
/// recommendation: each distinct topic here costs one `high_watermarks` round trip against the
/// single lockstep `IggyClient` every Kafka connection on this gateway shares
/// (`bridge/iggy_bridge/mod.rs`'s "Concurrency ceiling"), so a large batch head-of-line-blocks
/// every other client's control plane for its duration. 100 keeps a worst-case batch's aggregate
/// bridge cost small relative to that shared resource while remaining generous for any real
/// consumer's offset lookup.
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
    let Some(bridge) = &state.bridge else {
        return handle_versioned_request(
            API_KEY_LIST_OFFSETS,
            api_version,
            body,
            |v, b| {
                decode_guarded::<ListOffsetsRequest>(v, b, |v, b| {
                    validate_list_offsets_shape(v, b, state.max_frame_size)
                })
            },
            encode_response,
            encode_error_response,
            "ListOffsets",
        );
    };

    if !is_supported_version(API_KEY_LIST_OFFSETS, api_version) {
        return unsupported_version_response(API_KEY_LIST_OFFSETS, api_version, |version| {
            encode_error_response(version, ERROR_INVALID_REQUEST)
        });
    }

    let req = match decode_guarded::<ListOffsetsRequest>(api_version, body, |v, b| {
        validate_list_offsets_shape(v, b, state.max_frame_size)
    }) {
        Ok(req) => req,
        Err(error) => {
            // debug!, not warn!: attacker-controlled, not operator-actionable.
            tracing::debug!(%error, "Failed to decode ListOffsets request");
            return respond_or_close(
                encode_error_response(api_version, ERROR_INVALID_REQUEST),
                "ListOffsets",
            );
        }
    };

    let distinct_names: HashSet<&str> = req.topics.iter().map(|t| t.name.as_str()).collect();
    if distinct_names.len() > MAX_BRIDGE_BACKED_TOPICS {
        tracing::warn!(
            distinct_topics = distinct_names.len(),
            max = MAX_BRIDGE_BACKED_TOPICS,
            "ListOffsets request addresses too many distinct topics; rejecting"
        );
        let topics = req
            .topics
            .iter()
            .map(|topic| error_topic_response(topic, ERROR_INVALID_REQUEST))
            .collect();
        let resp = ListOffsetsResponse::default().with_topics(topics);
        return respond_or_close(encode_message(&resp, api_version, 256), "ListOffsets");
    }

    let topics =
        match tokio::time::timeout(REQUEST_DEADLINE, resolve_all_topics(bridge, &req.topics)).await
        {
            Ok(topics) => topics,
            Err(_elapsed) => {
                tracing::warn!(
                    distinct_topics = distinct_names.len(),
                    deadline_secs = REQUEST_DEADLINE.as_secs(),
                    "ListOffsets request's aggregate bridge work exceeded its deadline; \
                 answering retriable instead of blocking further"
                );
                req.topics
                    .iter()
                    .map(|topic| error_topic_response(topic, ERROR_REQUEST_TIMED_OUT))
                    .collect()
            }
        };
    let resp = ListOffsetsResponse::default().with_topics(topics);
    respond_or_close(encode_message(&resp, api_version, 256), "ListOffsets")
}

/// Resolves every requested topic entry, deduping by name first so a topic named in more than
/// one request entry (or a name repeated verbatim) costs one `high_watermarks` round trip, not
/// one per entry - the earlier per-entry loop paid a separate round trip for each, even for the
/// same name.
///
/// `EARLIEST`/`LATEST` are the only timestamps this bridge resolves - Iggy exposes no
/// per-message timestamp index, so "offset of the first message at or after timestamp T" for any
/// other `T` cannot be answered without one. Those partitions get [`ERROR_UNKNOWN_SERVER_ERROR`],
/// not a fabricated offset.
async fn resolve_all_topics(
    bridge: &IggyBridge,
    requested: &[ListOffsetsTopic],
) -> Vec<ListOffsetsTopicResponse> {
    let mut partitions_by_name: HashMap<&str, Vec<u32>> = HashMap::new();
    for topic in requested {
        let valid = topic
            .partitions
            .iter()
            .filter_map(|p| u32::try_from(p.partition_index).ok());
        partitions_by_name
            .entry(topic.name.as_str())
            .or_default()
            .extend(valid);
    }
    for partitions in partitions_by_name.values_mut() {
        partitions.sort_unstable();
        partitions.dedup();
    }

    let mut watermarks_by_name: HashMap<&str, HighWatermarksResult> = HashMap::new();
    for (&name, partitions) in &partitions_by_name {
        let result = if partitions.is_empty() {
            Ok(Vec::new())
        } else {
            bridge.high_watermarks(name, partitions).await
        };
        watermarks_by_name.insert(name, result);
    }

    requested
        .iter()
        .map(|topic| {
            // Always present: partitions_by_name (and so watermarks_by_name) was built from
            // exactly these same requested topic names, just above.
            let watermarks = watermarks_by_name
                .get(topic.name.as_str())
                .expect("every requested topic name was resolved above");
            let partitions = topic
                .partitions
                .iter()
                .map(|requested| resolve_one_partition(requested, watermarks))
                .collect();
            ListOffsetsTopicResponse::default()
                .with_name(topic.name.clone())
                .with_partitions(partitions)
        })
        .collect()
}

/// One response entry for `topic`, every requested partition carrying `error_code` - used for a
/// whole-request failure (over the topic cap, or the aggregate deadline) where no bridge call
/// was made for this topic at all.
fn error_topic_response(topic: &ListOffsetsTopic, error_code: i16) -> ListOffsetsTopicResponse {
    let partitions = topic
        .partitions
        .iter()
        .map(|p| error_response(p.partition_index, error_code))
        .collect();
    ListOffsetsTopicResponse::default()
        .with_name(topic.name.clone())
        .with_partitions(partitions)
}

/// `watermarks` is the whole topic's batched result: `Err` is a call-level failure (e.g. the
/// mapped stream doesn't exist) applying to every partition alike; the inner per-partition
/// `Result` inside `Ok` is [`BridgeError::PartitionOutOfRange`] for one bad index among otherwise
/// resolvable ones.
fn resolve_one_partition(
    requested: &ListOffsetsPartition,
    watermarks: &HighWatermarksResult,
) -> ListOffsetsPartitionResponse {
    let Ok(partition_index) = u32::try_from(requested.partition_index) else {
        return error_response(requested.partition_index, ERROR_UNKNOWN_TOPIC_OR_PARTITION);
    };

    let results = match watermarks {
        Err(call_err) => {
            return error_response(requested.partition_index, call_err.to_kafka_error_code());
        }
        Ok(results) => results,
    };

    // Always present: `valid_partitions` (built from the same `topic.partitions`) is exactly
    // what was passed to `high_watermarks`, which returns one result per requested partition.
    let Some((_, watermark)) = results.iter().find(|(index, _)| *index == partition_index) else {
        return error_response(requested.partition_index, ERROR_UNKNOWN_TOPIC_OR_PARTITION);
    };

    let watermark = match watermark {
        Err(err) => return error_response(requested.partition_index, err.to_kafka_error_code()),
        Ok(watermark) => *watermark,
    };

    match requested.timestamp {
        LATEST_TIMESTAMP => offset_response(requested.partition_index, watermark),
        // Real only for a partition retention has never trimmed: Iggy tracks no rolling
        // low-watermark distinct from partition creation, so a `0` here for an older,
        // already-trimmed partition names a log-start offset that no longer exists - a real
        // consumer with `auto.offset.reset=earliest` would seek into a hole. Harmless *today*
        // only because Fetch (`#3536`) is still a stub - nothing yet reads at the offset this
        // returns. Not fixable client-side; needs the bridge to expose a real start offset.
        EARLIEST_TIMESTAMP => offset_response(requested.partition_index, 0),
        _ => error_response(requested.partition_index, ERROR_UNKNOWN_SERVER_ERROR),
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
        let watermarks: HighWatermarksResult = Ok(ok_watermarks(&[(0, 42)]));
        let resp = resolve_one_partition(&partition(0, LATEST_TIMESTAMP), &watermarks);
        assert_eq!(resp.error_code, ERROR_NONE);
        assert_eq!(resp.offset, 42);
    }

    #[test]
    fn earliest_resolves_to_zero_regardless_of_the_watermark() {
        let watermarks: HighWatermarksResult = Ok(ok_watermarks(&[(0, 42)]));
        let resp = resolve_one_partition(&partition(0, EARLIEST_TIMESTAMP), &watermarks);
        assert_eq!(resp.error_code, ERROR_NONE);
        assert_eq!(resp.offset, 0);
    }

    #[test]
    fn an_arbitrary_timestamp_is_unsupported() {
        let watermarks: HighWatermarksResult = Ok(ok_watermarks(&[(0, 42)]));
        let resp = resolve_one_partition(&partition(0, 1_700_000_000_000), &watermarks);
        assert_eq!(resp.error_code, ERROR_UNKNOWN_SERVER_ERROR);
        assert_eq!(resp.offset, NO_OFFSET);
    }

    #[test]
    fn a_negative_partition_index_is_rejected_without_consulting_the_bridge_result() {
        let watermarks: HighWatermarksResult = Ok(vec![]);
        let resp = resolve_one_partition(&partition(-1, LATEST_TIMESTAMP), &watermarks);
        assert_eq!(resp.error_code, ERROR_UNKNOWN_TOPIC_OR_PARTITION);
    }

    #[test]
    fn a_partition_specific_error_only_affects_that_partition() {
        let watermarks: HighWatermarksResult = Ok(vec![(
            0,
            Err(BridgeError::PartitionOutOfRange {
                topic: "orders".to_string(),
                partition: 0,
                partitions_count: 0,
            }),
        )]);
        let resp = resolve_one_partition(&partition(0, LATEST_TIMESTAMP), &watermarks);
        assert_eq!(resp.error_code, ERROR_UNKNOWN_TOPIC_OR_PARTITION);
    }

    #[test]
    fn a_call_level_error_applies_regardless_of_the_requested_timestamp() {
        let watermarks: HighWatermarksResult = Err(BridgeError::Timeout);
        let resp = resolve_one_partition(&partition(0, EARLIEST_TIMESTAMP), &watermarks);
        assert_eq!(resp.error_code, BridgeError::Timeout.to_kafka_error_code());
    }

    #[test]
    fn error_topic_response_stamps_every_requested_partition_with_the_same_code() {
        let topic = ListOffsetsTopic::default()
            .with_name(kafka_protocol::messages::TopicName(
                kafka_protocol::protocol::StrBytes::from_static_str("orders"),
            ))
            .with_partitions(vec![
                partition(0, LATEST_TIMESTAMP),
                partition(1, LATEST_TIMESTAMP),
            ]);
        let resp = error_topic_response(&topic, ERROR_INVALID_REQUEST);
        assert_eq!(resp.partitions.len(), 2);
        for p in &resp.partitions {
            assert_eq!(p.error_code, ERROR_INVALID_REQUEST);
            assert_eq!(p.offset, NO_OFFSET);
        }
    }
}
