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

//! Metadata (API key 3).

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::{BrokerId, MetadataRequest, MetadataResponse, TopicName};
use kafka_protocol::protocol::StrBytes;

use crate::bridge::IggyBridge;
use crate::error::{KafkaProtocolError, Result};
use crate::protocol::api::{
    API_KEY_METADATA, ApiVersionRange, BrokerAdvertise, ERROR_INVALID_REQUEST, ERROR_NONE,
    ERROR_REQUEST_TIMED_OUT, ERROR_UNKNOWN_TOPIC_OR_PARTITION, GatewayState, HandleOutcome,
    is_supported_version, supported_max_version,
};
use crate::protocol::bounds_guard::validate_metadata_shape;
use crate::protocol::handlers::{decode_guarded, encode_message, respond_or_close};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_METADATA,
    min_version: 0,
    max_version: 9,
};

/// Cap on distinct topic names one named-lookup `Metadata` request may address through the
/// bridge. Does not apply to a null topics array ("all topics") - that path is server-driven
/// (bounded by [`response_would_exceed_frame_size`] instead), not client-count-driven.
///
/// `bounds_guard`'s `MAX_REQUEST_ELEMENTS` (4,096) is a pre-decode `DoS` ceiling, not a usability
/// recommendation: each distinct name costs one `get_kafka_topic` round trip against the single
/// lockstep `IggyClient` every Kafka connection on this gateway shares
/// (`bridge/iggy_bridge/mod.rs`'s "Concurrency ceiling").
const MAX_BRIDGE_BACKED_TOPICS: usize = 100;

/// Wall-clock ceiling for a named-lookup request's aggregate bridge work.
///
/// `Metadata` carries no `timeout_ms` field in any version (unlike `CreateTopics`), so this is a
/// fixed ceiling, not a client-honored one - sized well above one `get_kafka_topic` call's own
/// `REQUEST_TIMEOUT` (15s, bridge-internal) so a single slow-but-alive call is not the common
/// trigger, while still bounding the sum across up to [`MAX_BRIDGE_BACKED_TOPICS`] calls.
const REQUEST_DEADLINE: Duration = Duration::from_secs(20);

pub async fn handle(state: &GatewayState, api_version: i16, body: Bytes) -> HandleOutcome {
    if !is_supported_version(API_KEY_METADATA, api_version) {
        // Clamping the response to the supported max leaves a body the client parses at its own
        // (unsupported) version, so UNSUPPORTED_VERSION never survives. Clients that skip
        // ApiVersions get a naked close instead.
        tracing::warn!(
            api_version,
            max_supported = supported_max_version(API_KEY_METADATA),
            "Metadata version unsupported; closing connection"
        );
        return HandleOutcome::Close;
    }

    let Some(bridge) = &state.bridge else {
        return match decode_topics(api_version, body, state.max_frame_size) {
            Ok(topics) => respond_or_close(
                encode_response(api_version, &topics, &state.broker, ERROR_NONE),
                "Metadata",
            ),
            Err(error) => {
                // Metadata has no top-level error field; a malformed body cannot carry
                // INVALID_REQUEST in a version-correct way for every client. Close.
                // debug!, not warn!: attacker-controlled, not operator-actionable.
                tracing::debug!(
                    %error,
                    api_version,
                    "Failed to decode Metadata request; closing connection"
                );
                HandleOutcome::Close
            }
        };
    };

    let requested = match decode_requested_topics(api_version, body, state.max_frame_size) {
        Ok(requested) => requested,
        Err(error) => {
            tracing::debug!(
                %error,
                api_version,
                "Failed to decode Metadata request; closing connection"
            );
            return HandleOutcome::Close;
        }
    };

    let results = match requested {
        None => match bridge.list_kafka_topics().await {
            Ok(topics) => topics.into_iter().map(found_result).collect(),
            Err(error) => {
                // Same "no top-level error field" constraint as a decode failure: there is no
                // way to answer "the bridge itself is unreachable" for an all-topics request
                // that doesn't also falsely claim zero topics exist.
                tracing::warn!(%error, "Failed to list Kafka topics from the Iggy bridge; closing connection");
                return HandleOutcome::Close;
            }
        },
        Some(names) => {
            let distinct_names: HashSet<&str> = names.iter().map(StrBytes::as_str).collect();
            if distinct_names.len() > MAX_BRIDGE_BACKED_TOPICS {
                tracing::warn!(
                    distinct_topics = distinct_names.len(),
                    max = MAX_BRIDGE_BACKED_TOPICS,
                    "Metadata request addresses too many distinct topics; rejecting"
                );
                names
                    .iter()
                    .map(|name| error_result(name.clone(), ERROR_INVALID_REQUEST))
                    .collect()
            } else {
                match tokio::time::timeout(REQUEST_DEADLINE, resolve_named_topics(bridge, &names))
                    .await
                {
                    Ok(results) => results,
                    Err(_elapsed) => {
                        tracing::warn!(
                            distinct_topics = distinct_names.len(),
                            deadline_secs = REQUEST_DEADLINE.as_secs(),
                            "Metadata request's aggregate bridge work exceeded its deadline; \
                             answering retriable instead of blocking further"
                        );
                        names
                            .iter()
                            .map(|name| error_result(name.clone(), ERROR_REQUEST_TIMED_OUT))
                            .collect()
                    }
                }
            }
        }
    };

    // `bounds_guard` cannot see this: it charges the projected response by *requested* element
    // (one topic name), but one real topic can carry up to Iggy's own per-topic partition cap
    // (1000) - a handful of names, or one `list_kafka_topics()` call, can still expand into a
    // response `bounds_guard` never had the information to price in before this bridge round
    // trip returned. Checked here, before `encode_real_response` builds one
    // `MetadataResponsePartition` per partition, not after - the expensive part is building that
    // `Vec`, not encoding the bytes that follow it.
    let total_partitions: usize = results
        .iter()
        .filter(|result| result.error_code == ERROR_NONE)
        .map(|result| result.partitions_count as usize)
        .sum();
    if response_would_exceed_frame_size(total_partitions, state.max_frame_size) {
        tracing::warn!(
            total_partitions,
            max_frame_size = state.max_frame_size,
            "Metadata response would exceed max_frame_size; closing connection"
        );
        return HandleOutcome::Close;
    }

    respond_or_close(
        encode_real_response(api_version, &results, &state.broker),
        "Metadata",
    )
}

/// Conservative per-partition byte cost of one encoded `MetadataResponsePartition` at v9 (the
/// densest wire shape this handler emits): measured ~26 bytes (`error_code` + `partition_index` +
/// `leader_id` + `leader_epoch` + a 1-entry `replica_nodes` + a 1-entry `isr_nodes` + an empty
/// `offline_replicas` + tagged fields). 64 matches the margin `bounds_guard`'s own
/// `RESPONSE_BYTES_PER_ELEMENT` uses for the same kind of estimate, rather than shaving this to
/// the measured minimum.
const RESPONSE_BYTES_PER_PARTITION: usize = 64;

const fn response_would_exceed_frame_size(total_partitions: usize, max_frame_size: usize) -> bool {
    total_partitions.saturating_mul(RESPONSE_BYTES_PER_PARTITION) > max_frame_size
}

/// One resolved topic result for the real (bridge-backed) path - `partitions_count` is
/// meaningless when `error_code != ERROR_NONE`.
struct TopicResult {
    name: StrBytes,
    error_code: i16,
    partitions_count: u32,
}

fn found_result(metadata: crate::bridge::KafkaTopicMetadata) -> TopicResult {
    TopicResult {
        name: StrBytes::from_string(metadata.kafka_topic),
        error_code: ERROR_NONE,
        partitions_count: metadata.partitions_count,
    }
}

async fn lookup_one_topic(bridge: &IggyBridge, name: StrBytes) -> TopicResult {
    match bridge.get_kafka_topic(&name).await {
        // `get_kafka_topic` (unlike `list_kafka_topics`) returns the SDK's own `TopicDetails`,
        // which carries the topic's raw Iggy-side name, not the Kafka-side one under an override
        // - so this echoes the caller's own `name`, not a field off the result.
        Ok(Some(details)) => TopicResult {
            name,
            error_code: ERROR_NONE,
            partitions_count: details.partitions_count,
        },
        Ok(None) => TopicResult {
            name,
            error_code: ERROR_UNKNOWN_TOPIC_OR_PARTITION,
            partitions_count: 0,
        },
        Err(err) => TopicResult {
            name,
            error_code: err.to_kafka_error_code(),
            partitions_count: 0,
        },
    }
}

const fn error_result(name: StrBytes, error_code: i16) -> TopicResult {
    TopicResult {
        name,
        error_code,
        partitions_count: 0,
    }
}

/// Resolves every requested name, deduping first so a name repeated in the request (or asked
/// about more than once, which the wire technically allows) costs one `get_kafka_topic` round
/// trip, not one per occurrence.
async fn resolve_named_topics(bridge: &IggyBridge, names: &[StrBytes]) -> Vec<TopicResult> {
    let mut seen = HashSet::with_capacity(names.len());
    let mut distinct = Vec::new();
    for name in names {
        if seen.insert(name.as_str()) {
            distinct.push(name.clone());
        }
    }

    let mut results_by_name: HashMap<&str, TopicResult> = HashMap::with_capacity(distinct.len());
    for name in &distinct {
        let result = lookup_one_topic(bridge, name.clone()).await;
        results_by_name.insert(name.as_str(), result);
    }

    names
        .iter()
        .map(|name| {
            // Always present: `distinct` (and so results_by_name) was built from exactly these
            // same requested names, just above.
            let cached = results_by_name
                .get(name.as_str())
                .expect("every requested name was resolved above");
            TopicResult {
                name: name.clone(),
                error_code: cached.error_code,
                partitions_count: cached.partitions_count,
            }
        })
        .collect()
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `response_version`.
pub fn encode_response(
    response_version: i16,
    topics: &[StrBytes],
    broker: &BrokerAdvertise,
    topic_error_override: i16,
) -> Result<Bytes> {
    // Stub has no topic catalog: echo requested names with UNKNOWN_TOPIC_OR_PARTITION,
    // or a forced override (unused today; kept for symmetry with other encoders).
    let topic_error = if topic_error_override == ERROR_NONE {
        ERROR_UNKNOWN_TOPIC_OR_PARTITION
    } else {
        topic_error_override
    };

    let response_topics = topics
        .iter()
        .map(|name| {
            MetadataResponseTopic::default()
                .with_error_code(topic_error)
                .with_name(Some(TopicName(name.clone())))
        })
        .collect();

    let broker_entry = MetadataResponseBroker::default()
        .with_node_id(BrokerId(1))
        .with_host(StrBytes::from_string(broker.host.clone()))
        .with_port(broker.port);

    let resp = MetadataResponse::default()
        .with_brokers(vec![broker_entry])
        .with_controller_id(BrokerId(1))
        .with_topics(response_topics);

    encode_message(&resp, response_version, 256)
}

/// Real (bridge-backed) response: unlike [`encode_response`], each topic carries its own
/// resolved error code and, on success, one [`MetadataResponsePartition`] per partition with
/// this gateway's single broker (node id 1) as leader/replica/ISR - there is only ever one
/// broker behind this gateway, so that triple is never actually in question.
fn encode_real_response(
    response_version: i16,
    results: &[TopicResult],
    broker: &BrokerAdvertise,
) -> Result<Bytes> {
    let response_topics = results
        .iter()
        .map(|result| {
            let partitions = if result.error_code == ERROR_NONE {
                (0..result.partitions_count)
                    .map(|index| {
                        MetadataResponsePartition::default()
                            .with_partition_index(i32::try_from(index).unwrap_or(i32::MAX))
                            .with_leader_id(BrokerId(1))
                            .with_replica_nodes(vec![BrokerId(1)])
                            .with_isr_nodes(vec![BrokerId(1)])
                    })
                    .collect()
            } else {
                Vec::new()
            };
            MetadataResponseTopic::default()
                .with_error_code(result.error_code)
                .with_name(Some(TopicName(result.name.clone())))
                .with_partitions(partitions)
        })
        .collect();

    let broker_entry = MetadataResponseBroker::default()
        .with_node_id(BrokerId(1))
        .with_host(StrBytes::from_string(broker.host.clone()))
        .with_port(broker.port);

    let resp = MetadataResponse::default()
        .with_brokers(vec![broker_entry])
        .with_controller_id(BrokerId(1))
        .with_topics(response_topics);

    encode_message(&resp, response_version, 256)
}

/// Decodes a Metadata request body so the response can echo topic names.
///
/// A null topics array (`-1` legacy / `varint=0` compact) means "all topics" and decodes to an
/// empty list for this stub. A null per-topic `name` (v10+ allows topic-id-only lookups) has no
/// name to echo, so it errors rather than silently dropping the topic from the response.
fn decode_topics(api_version: i16, body: Bytes, max_frame_size: usize) -> Result<Vec<StrBytes>> {
    let req = decode_guarded::<MetadataRequest>(api_version, body, |v, b| {
        validate_metadata_shape(v, b, max_frame_size)
    })?;
    req.topics
        .unwrap_or_default()
        .into_iter()
        .map(|topic| {
            topic
                .name
                .map(|name| name.0)
                .ok_or(KafkaProtocolError::NullTopicName)
        })
        .collect()
}

/// Like [`decode_topics`], but for the real (bridge-backed) path, which must tell apart what
/// `decode_topics`' `unwrap_or_default()` deliberately collapses: a null topics array (`None`,
/// "all topics") from an explicit, merely empty one (`Some(vec![])`, "these zero topics") - the
/// stub has no topic catalog to answer either request differently, but the real path does.
///
/// At `api_version == 0` an explicit empty array is folded into `None` too: Kafka's own rule
/// (`MetadataRequest.isAllTopics()`) is `topics == null || (topics.isEmpty() && version == 0)` -
/// there is no v0 wire shape for "cluster info only, zero topics" (that distinct shape, KIP-4's
/// `describeCluster()`, starts at v1), so an empty array at v0 can only mean "all topics."
fn decode_requested_topics(
    api_version: i16,
    body: Bytes,
    max_frame_size: usize,
) -> Result<Option<Vec<StrBytes>>> {
    let req = decode_guarded::<MetadataRequest>(api_version, body, |v, b| {
        validate_metadata_shape(v, b, max_frame_size)
    })?;
    let requested = req
        .topics
        .map(|topics| {
            topics
                .into_iter()
                .map(|topic| {
                    topic
                        .name
                        .map(|name| name.0)
                        .ok_or(KafkaProtocolError::NullTopicName)
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?;
    Ok(match requested {
        Some(topics) if topics.is_empty() && api_version == 0 => None,
        other => other,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_MAX_FRAME_SIZE: usize = 8 * 1024 * 1024;

    #[test]
    fn decode_topics_legacy_null_topic_name_fails() {
        let body = Bytes::from_static(&[
            0x00, 0x00, 0x00, 0x01, // one topic
            0xff, 0xff, // null topic name
        ]);
        let err = decode_topics(0, body, TEST_MAX_FRAME_SIZE).unwrap_err();
        assert!(matches!(err, KafkaProtocolError::NullTopicName));
    }

    #[test]
    fn decode_topics_legacy_null_array_means_all_topics() {
        // -1 is the spec-defined "all topics" sentinel for the legacy i32 array count, not a
        // malformed request - must decode to an empty list.
        let body = Bytes::from_static(&[0xff, 0xff, 0xff, 0xff]); // -1
        let topics = decode_topics(0, body, TEST_MAX_FRAME_SIZE).unwrap();
        assert!(topics.is_empty());
    }

    #[test]
    fn decode_topics_empty_body_is_malformed() {
        assert!(decode_topics(0, Bytes::new(), TEST_MAX_FRAME_SIZE).is_err());
    }

    #[test]
    fn decode_topics_flexible_truncated_after_topics_fails() {
        // topics = null (all topics) but missing allow_auto / auth flags / tagged fields.
        let body = Bytes::from_static(&[0x00]);
        assert!(decode_topics(9, body, TEST_MAX_FRAME_SIZE).is_err());
    }

    #[test]
    fn decode_requested_topics_legacy_null_array_is_none_not_some_empty() {
        // The exact distinction decode_topics' unwrap_or_default() collapses: -1 must decode to
        // None ("all topics"), not Some(vec![]) ("these zero topics").
        let body = Bytes::from_static(&[0xff, 0xff, 0xff, 0xff]); // -1
        let requested = decode_requested_topics(0, body, TEST_MAX_FRAME_SIZE).unwrap();
        assert_eq!(requested, None);
    }

    #[test]
    fn decode_requested_topics_v1_explicit_empty_array_is_some_empty_not_none() {
        // v1+, unlike v0 (see the next test): an explicit empty array really does mean "these
        // zero topics" (KIP-4's describeCluster() shape).
        let body = Bytes::from_static(&[0x00, 0x00, 0x00, 0x00]); // 0 topics, not -1
        let requested = decode_requested_topics(1, body, TEST_MAX_FRAME_SIZE).unwrap();
        assert_eq!(requested, Some(Vec::new()));
    }

    #[test]
    fn decode_requested_topics_v0_explicit_empty_array_means_all_topics_too() {
        // Kafka's own isAllTopics(): topics == null || (topics.isEmpty() && version == 0). No v0
        // client can express "cluster info only, zero topics" - that shape starts at v1.
        let body = Bytes::from_static(&[0x00, 0x00, 0x00, 0x00]); // 0 topics, not -1
        let requested = decode_requested_topics(0, body, TEST_MAX_FRAME_SIZE).unwrap();
        assert_eq!(requested, None);
    }

    #[test]
    fn decode_requested_topics_legacy_named_topic_is_some_with_that_name() {
        let body = Bytes::from_static(&[
            0x00, 0x00, 0x00, 0x01, // one topic
            0x00, 0x06, b'o', b'r', b'd', b'e', b'r', b's', // "orders"
        ]);
        let requested = decode_requested_topics(0, body, TEST_MAX_FRAME_SIZE).unwrap();
        assert_eq!(requested, Some(vec![StrBytes::from_static_str("orders")]));
    }

    #[test]
    fn response_would_exceed_frame_size_rejects_a_projection_over_the_limit() {
        // bounds_guard cannot see this cost: one requested topic name can expand into up to
        // Iggy's own per-topic partition cap (1000) worth of MetadataResponsePartition entries,
        // information only known after the bridge round trip this check runs after.
        let max_frame_size = 1024;
        let total_partitions = (max_frame_size / RESPONSE_BYTES_PER_PARTITION) + 1;
        assert!(response_would_exceed_frame_size(
            total_partitions,
            max_frame_size
        ));
    }

    #[test]
    fn response_would_exceed_frame_size_accepts_a_projection_at_or_under_the_limit() {
        let max_frame_size = 1024;
        let total_partitions = max_frame_size / RESPONSE_BYTES_PER_PARTITION;
        assert!(!response_would_exceed_frame_size(
            total_partitions,
            max_frame_size
        ));
    }

    #[test]
    fn response_would_exceed_frame_size_does_not_overflow_on_a_pathological_partition_count() {
        assert!(response_would_exceed_frame_size(usize::MAX, 1024));
    }
}
