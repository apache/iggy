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

use std::collections::HashSet;
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::{BrokerId, MetadataRequest, MetadataResponse, TopicName};
use kafka_protocol::protocol::{Encodable, StrBytes};

use crate::bridge::IggyBridge;
use crate::error::{KafkaProtocolError, Result};
use crate::protocol::api::{
    API_KEY_METADATA, ApiVersionRange, BrokerAdvertise, ERROR_NONE, ERROR_REQUEST_TIMED_OUT,
    ERROR_UNKNOWN_TOPIC_OR_PARTITION, GatewayState, HandleOutcome, is_supported_version,
    supported_max_version,
};
use crate::protocol::bounds_guard::validate_metadata_shape;
use crate::protocol::handlers::{decode_guarded, encode_message, respond_or_close};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_METADATA,
    min_version: 0,
    max_version: 9,
};

/// Wall-clock ceiling for a named-lookup request's aggregate bridge work.
///
/// `Metadata` carries no `timeout_ms` field in any version (unlike `CreateTopics`), so this is a
/// fixed ceiling, not a client-honored one - sized well above one `get_topics` call's own
/// `REQUEST_TIMEOUT` (15s, bridge-internal) so a single slow-but-alive stream lookup is not the
/// common trigger, while still bounding the sum across every distinct stream
/// [`IggyBridge::get_kafka_topics`] ends up calling for this request.
const REQUEST_DEADLINE: Duration = Duration::from_secs(20);

/// Above this many total partitions in an all-topics response, `encode_real_response` runs on a
/// blocking-pool thread instead of inline (see the call site). Conservative relative to the one
/// measured data point available (20-54ms at 131,000 partitions) rather than a profiled
/// threshold - tune with a real benchmark if it proves too high or too low in practice.
const BLOCKING_ENCODE_PARTITION_THRESHOLD: u32 = 20_000;

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

    let per_partition_bytes = template_partition_bytes(api_version);

    let results = match requested {
        None => match tokio::time::timeout(REQUEST_DEADLINE, bridge.list_kafka_topics()).await {
            Ok(Ok(topics)) => {
                let results: Vec<TopicResult> = topics.into_iter().map(found_result).collect();
                truncate_all_topics_to_frame_budget(
                    results,
                    state.max_frame_size,
                    per_partition_bytes,
                )
            }
            Ok(Err(error)) => {
                // Same "no top-level error field" constraint as a decode failure: there is no
                // way to answer "the bridge itself is unreachable" for an all-topics request
                // that doesn't also falsely claim zero topics exist.
                tracing::warn!(%error, "Failed to list Kafka topics from the Iggy bridge; closing connection");
                return HandleOutcome::Close;
            }
            Err(_elapsed) => {
                // No per-name cap to answer against here (this is the server-driven all-topics
                // path), so there is no honest per-topic RETRIABLE list to send back either -
                // same close as an unreachable bridge, not a different case.
                tracing::warn!(
                    deadline_secs = REQUEST_DEADLINE.as_secs(),
                    "Listing Kafka topics from the Iggy bridge exceeded its deadline; closing \
                     connection"
                );
                return HandleOutcome::Close;
            }
        },
        Some(names) => resolve_requested_named_topics(bridge, &names).await,
    };

    // `bounds_guard` cannot see this: it charges the projected response by *requested* element
    // (one topic name), but one real topic can carry up to Iggy's own per-topic partition cap
    // (1000) - a handful of names can still expand into a response `bounds_guard` never had the
    // information to price in before this bridge round trip returned. The all-topics arm is
    // pre-truncated to this same budget above (`truncate_all_topics_to_frame_budget`) since its
    // size is server-side, not client-controllable - closing over it would take down every
    // client's bootstrap Metadata call, permanently, the moment the catalog grows past the
    // trip point. This check is what still enforces the budget for the named-lookup arm, where
    // the cap (100 distinct topics) bounds the request but not what each one costs to answer.
    let total_bytes: usize = results
        .iter()
        .map(|result| estimated_response_bytes(result, per_partition_bytes))
        .sum();
    if response_would_exceed_frame_size(total_bytes, state.max_frame_size) {
        tracing::warn!(
            total_bytes,
            max_frame_size = state.max_frame_size,
            "Metadata response would exceed max_frame_size; closing connection"
        );
        return HandleOutcome::Close;
    }

    let total_partitions: u64 = results
        .iter()
        .filter(|result| result.error_code == ERROR_NONE)
        .map(|result| u64::from(result.partitions_count))
        .sum();

    let encoded = if total_partitions > u64::from(BLOCKING_ENCODE_PARTITION_THRESHOLD) {
        // Building `MetadataResponsePartition` once per partition and serializing the whole
        // response is measured at 20-54ms at 131,000 partitions - long enough, on a
        // shared-nothing shard's single executor, to stall every other connection's request on
        // this shard for the duration. `spawn_blocking` moves that cost off the shard's own task
        // so ordinary requests keep their latency regardless of how large one all-topics response
        // gets.
        let broker = state.broker.clone();
        tokio::task::spawn_blocking(move || encode_real_response(api_version, &results, &broker))
            .await
            .unwrap_or_else(|join_error| {
                tracing::error!(%join_error, "Metadata all-topics encode task panicked");
                Err(KafkaProtocolError::Malformed(join_error.to_string()))
            })
    } else {
        encode_real_response(api_version, &results, &state.broker)
    };
    respond_or_close(encoded, "Metadata")
}

/// Resolves a named-lookup Metadata request's topics: dedupes first so the response is built from
/// the distinct set rather than one entry per request occurrence - real Kafka answers a topic
/// named more than once with one response entry, not one per repeat, and re-expanding to match
/// the request would let a handful of repeats of one large topic name amplify a response sized
/// off the repeat count instead of the distinct count.
///
/// No cap on how many distinct names one request may carry (`bounds_guard`'s
/// `MAX_REQUEST_ELEMENTS`, 4,096, is still the decode-time ceiling): the bridge round trip cost
/// this used to bound was one call per name, but [`IggyBridge::get_kafka_topics`] batches by the
/// distinct *streams* those names resolve to instead, which is a config-time-bounded quantity a
/// large name count doesn't grow. A Java producer's `ProducerMetadata` resends every topic it has
/// ever produced to on each refresh, not just the ones in the current batch - once that set
/// crossed a per-request name cap, every subsequent Metadata call answered every topic
/// `INVALID_REQUEST` and the producer could never recover, since it has no way to shrink its own
/// tracked set. A single timeout still bounds the one batched call.
async fn resolve_requested_named_topics(
    bridge: &IggyBridge,
    names: &[StrBytes],
) -> Vec<TopicResult> {
    let distinct = dedup_topic_names(names);
    match tokio::time::timeout(REQUEST_DEADLINE, bridge.get_kafka_topics(&distinct)).await {
        Ok(Ok(resolved)) => resolved
            .into_iter()
            .map(|(name, metadata)| match metadata {
                Some(metadata) => TopicResult {
                    name,
                    error_code: ERROR_NONE,
                    partitions_count: metadata.partitions_count,
                },
                None => error_result(name, ERROR_UNKNOWN_TOPIC_OR_PARTITION),
            })
            .collect(),
        Ok(Err(error)) => {
            tracing::error!(%error, "Metadata: resolving requested topics failed against the Iggy bridge");
            let error_code = error.to_kafka_error_code();
            distinct
                .into_iter()
                .map(|name| error_result(name, error_code))
                .collect()
        }
        Err(_elapsed) => {
            tracing::warn!(
                distinct_topics = distinct.len(),
                deadline_secs = REQUEST_DEADLINE.as_secs(),
                "Metadata request's aggregate bridge work exceeded its deadline; answering \
                 retriable instead of blocking further"
            );
            distinct
                .into_iter()
                .map(|name| error_result(name, ERROR_REQUEST_TIMED_OUT))
                .collect()
        }
    }
}

/// Fallback per-partition byte cost, used only if [`template_partition_bytes`]'s own
/// `compute_size` call somehow fails for the fixed, always-well-formed shape it builds. Chosen
/// well above every version's real cost (measured 26-34 bytes) so a fallback can only truncate
/// more eagerly than the real encoder would, never less.
const RESPONSE_BYTES_PER_PARTITION_FALLBACK: usize = 64;

/// Exact per-partition byte cost of one encoded `MetadataResponsePartition` at `version`, shaped
/// identically to what [`encode_real_response`] actually emits for a successful topic: one
/// `replica_nodes` entry, one `isr_nodes` entry, an empty `offline_replicas`.
///
/// A flat constant here previously overcharged every version below v9 (the densest wire shape,
/// which a flat 64-byte charge was measured against) - v0-v8's smaller encodings (fewer or
/// narrower fixed fields, no tagged-field byte) cost as little as 26 bytes, so the flat charge
/// started truncating an all-topics response at 41-53% of `max_frame_size` instead of near its
/// actual limit. `compute_size` asks the encoder itself, so this tracks every version exactly
/// instead of re-deriving and re-verifying a magic number per version by hand.
fn template_partition_bytes(version: i16) -> usize {
    MetadataResponsePartition::default()
        .with_partition_index(0)
        .with_leader_id(BrokerId(1))
        .with_replica_nodes(vec![BrokerId(1)])
        .with_isr_nodes(vec![BrokerId(1)])
        .compute_size(version)
        .unwrap_or(RESPONSE_BYTES_PER_PARTITION_FALLBACK)
}

/// Fixed per-`MetadataResponseTopic` cost, independent of name length or partition count:
/// `error_code`(2) + `is_internal`(1) + the `partitions` array's own compact-length varint
/// (charged at its 5-byte worst case) + `topic_authorized_operations`(4) + empty tagged fields(1).
/// Every result pays this once - `encode_real_response` emits a full wrapper even for an error
/// result, just with an empty `partitions` array, so this cost isn't conditional on `ERROR_NONE`.
const RESPONSE_BYTES_TOPIC_OVERHEAD: usize = 13;

/// Estimated encoded byte cost of one [`TopicResult`]: the fixed per-topic overhead, the name's
/// own bytes (compact string: length plus a short varint prefix, charged at a flat +2), and -
/// only for a successful result, since `encode_real_response` sends an empty array otherwise -
/// [`RESPONSE_BYTES_PER_PARTITION`] per partition.
///
/// `bounds_guard` cannot see any of this: it charges the request by name count alone, with no way
/// to know a name's length, a topic's per-entry overhead, or its partition count before this
/// bridge round trip returns real data. Charging partitions alone (this function's earlier form)
/// undercounted every result: a name costs bytes whether or not the lookup succeeded, and a
/// zero-partition or errored result - free under a partitions-only charge - still costs a full
/// wrapper to encode. 500 topics with 255-byte names and one partition each encode to ~146 KB
/// against the ~32 KB a partitions-only charge would have priced in.
fn estimated_response_bytes(result: &TopicResult, per_partition_bytes: usize) -> usize {
    let name_bytes = result.name.as_str().len() + 2;
    let partition_bytes = if result.error_code == ERROR_NONE {
        (result.partitions_count as usize).saturating_mul(per_partition_bytes)
    } else {
        0
    };
    RESPONSE_BYTES_TOPIC_OVERHEAD + name_bytes + partition_bytes
}

const fn response_would_exceed_frame_size(total_bytes: usize, max_frame_size: usize) -> bool {
    total_bytes > max_frame_size
}

/// Trims an all-topics [`IggyBridge::list_kafka_topics`] result to fit `max_frame_size`, keeping
/// as many whole topics (in listing order) as the budget allows.
///
/// Unlike the named-lookup arm, the all-topics response's size is a server-side property (the
/// cluster's total partition count) that the requesting client never chose and cannot shrink -
/// closing the connection over it, as the shared frame-size guard below does for the
/// client-controllable named-lookup case, would make every all-topics Metadata call fail
/// identically and permanently once the catalog crosses the trip point. That call is the
/// bootstrap and refresh shape both librdkafka and the Java client use, so a hard close reads as
/// "broker down" and reconnect-loops rather than surfacing a usable, if partial, result. This
/// wire protocol has no pagination cursor to ask for the rest with, so a truncated list - honest
/// about being incomplete via the dropped entries, not via an error code - is what's available.
fn truncate_all_topics_to_frame_budget(
    mut results: Vec<TopicResult>,
    max_frame_size: usize,
    per_partition_bytes: usize,
) -> Vec<TopicResult> {
    let mut cumulative_bytes = 0usize;
    let mut keep = results.len();
    for (index, result) in results.iter().enumerate() {
        let next = cumulative_bytes + estimated_response_bytes(result, per_partition_bytes);
        if response_would_exceed_frame_size(next, max_frame_size) {
            keep = index;
            break;
        }
        cumulative_bytes = next;
    }
    if keep < results.len() {
        tracing::warn!(
            total_topics = results.len(),
            kept_topics = keep,
            max_frame_size,
            "All-topics Metadata response would exceed max_frame_size; truncating rather than \
             closing the connection"
        );
        results.truncate(keep);
    }
    results
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

const fn error_result(name: StrBytes, error_code: i16) -> TopicResult {
    TopicResult {
        name,
        error_code,
        partitions_count: 0,
    }
}

/// Drops repeats, keeping first-seen order so a capped or timed-out response still answers a
/// deterministic prefix of the request rather than an arbitrary hash-order subset.
fn dedup_topic_names(names: &[StrBytes]) -> Vec<StrBytes> {
    let mut seen = HashSet::with_capacity(names.len());
    let mut distinct = Vec::with_capacity(names.len());
    for name in names {
        if seen.insert(name.as_str()) {
            distinct.push(name.clone());
        }
    }
    distinct
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
    fn response_would_exceed_frame_size_rejects_bytes_over_the_limit() {
        assert!(response_would_exceed_frame_size(1025, 1024));
    }

    #[test]
    fn response_would_exceed_frame_size_accepts_bytes_at_the_limit() {
        assert!(!response_would_exceed_frame_size(1024, 1024));
    }

    fn topic_result(name: &'static str, error_code: i16, partitions_count: u32) -> TopicResult {
        TopicResult {
            name: StrBytes::from_static_str(name),
            error_code,
            partitions_count,
        }
    }

    const TEST_PER_PARTITION_BYTES: usize = RESPONSE_BYTES_PER_PARTITION_FALLBACK;

    #[test]
    fn estimated_response_bytes_charges_name_and_overhead_even_at_zero_partitions() {
        // Regression: a partitions-only charge (the earlier form of this function) priced a
        // zero-partition result at 0, when `encode_real_response` still emits a full
        // `MetadataResponseTopic` wrapper with the name in it.
        let result = topic_result("orders", ERROR_NONE, 0);
        assert_eq!(
            estimated_response_bytes(&result, TEST_PER_PARTITION_BYTES),
            RESPONSE_BYTES_TOPIC_OVERHEAD + "orders".len() + 2
        );
    }

    #[test]
    fn estimated_response_bytes_charges_overhead_and_name_on_an_error_result_too() {
        // Regression: the earlier per-topic sum filtered to `error_code == ERROR_NONE` before
        // charging anything, so an all-erroring batch (e.g. every requested name unknown) was
        // priced at 0 bytes total despite `encode_real_response` emitting one wrapper per result
        // regardless of its error code.
        let result = topic_result("orders", ERROR_UNKNOWN_TOPIC_OR_PARTITION, 0);
        assert_eq!(
            estimated_response_bytes(&result, TEST_PER_PARTITION_BYTES),
            RESPONSE_BYTES_TOPIC_OVERHEAD + "orders".len() + 2
        );
    }

    #[test]
    fn estimated_response_bytes_ignores_partitions_count_on_an_error_result() {
        // `encode_real_response` sends an empty `partitions` array whenever `error_code !=
        // ERROR_NONE`, so a stale non-zero `partitions_count` on an error result (shouldn't occur,
        // but nothing enforces it structurally) must not inflate the charge.
        let ok = topic_result("orders", ERROR_NONE, 10);
        let err = topic_result("orders", ERROR_UNKNOWN_TOPIC_OR_PARTITION, 10);
        assert_eq!(
            estimated_response_bytes(&err, TEST_PER_PARTITION_BYTES),
            RESPONSE_BYTES_TOPIC_OVERHEAD + "orders".len() + 2
        );
        assert!(
            estimated_response_bytes(&ok, TEST_PER_PARTITION_BYTES)
                > estimated_response_bytes(&err, TEST_PER_PARTITION_BYTES)
        );
    }

    #[test]
    fn estimated_response_bytes_charges_longer_names_more() {
        let short = topic_result("a", ERROR_NONE, 0);
        let long = topic_result("a-much-longer-topic-name", ERROR_NONE, 0);
        assert!(
            estimated_response_bytes(&long, TEST_PER_PARTITION_BYTES)
                > estimated_response_bytes(&short, TEST_PER_PARTITION_BYTES)
        );
    }

    #[test]
    fn template_partition_bytes_is_smaller_at_v0_than_the_conservative_fallback() {
        // v0 has no leader_epoch, no offline_replicas tagged-field byte, and a legacy (non-compact)
        // array encoding - real cost is well under the flat 64-byte charge this replaced.
        assert!(template_partition_bytes(0) < RESPONSE_BYTES_PER_PARTITION_FALLBACK);
    }

    #[test]
    fn template_partition_bytes_is_never_larger_than_the_fallback() {
        for version in RANGE.min_version..=RANGE.max_version {
            assert!(
                template_partition_bytes(version) <= RESPONSE_BYTES_PER_PARTITION_FALLBACK,
                "v{version} exceeded the conservative fallback"
            );
        }
    }
}
