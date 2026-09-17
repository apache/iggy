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

//! Kafka response encoders.
//!
//! Wire encoding (field order, version gating, compact vs. legacy shapes) is
//! `kafka_protocol`'s responsibility. Produce/Fetch stay stub *policy* - which placeholder error
//! code a request gets back before `#3535`/`#3536` land - while Metadata (`#3534`, in
//! `protocol::api`), `CreateTopics` and `ListOffsets` below are backed by real `IggyBridge` results.

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::create_topics_response::CreatableTopicResult;
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use kafka_protocol::messages::{
    CreateTopicsResponse, FetchRequest, FetchResponse, ListOffsetsRequest, ListOffsetsResponse,
    ProduceRequest, ProduceResponse,
};
use kafka_protocol::protocol::Encodable;

use crate::error::{KafkaProtocolError, Result};
use crate::protocol::api::{
    ERROR_INVALID_PARTITIONS, ERROR_INVALID_REPLICATION_FACTOR, ERROR_NONE,
    ERROR_NOT_LEADER_OR_FOLLOWER,
};

/// Encode a `kafka_protocol` message, mapping its `anyhow::Error` (the crate has no stable
/// decode/encode error taxonomy) to a variant callers can log or fold into [`HandleOutcome::Close`].
///
/// [`HandleOutcome::Close`]: crate::protocol::api::HandleOutcome::Close
pub(crate) fn encode_message<T: Encodable>(
    msg: &T,
    version: i16,
    capacity: usize,
) -> Result<Bytes> {
    let mut buf = BytesMut::with_capacity(capacity);
    msg.encode(&mut buf, version)
        .map_err(|e| KafkaProtocolError::Malformed(e.to_string()))?;
    Ok(buf.freeze())
}

// ── Produce ──────────────────────────────────────────────────────────────────

/// Well-formed Produce response with a single placeholder topic/partition.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_produce_error_response(version: i16, error_code: i16) -> Result<Bytes> {
    let resp = ProduceResponse::default().with_responses(vec![
        TopicProduceResponse::default()
            .with_partition_responses(vec![produce_partition_response(0, error_code)]),
    ]);
    encode_message(&resp, version, 512)
}

/// Stub: discard the payload and return a retriable error so clients keep data locally until
/// the Iggy bridge lands (do not advertise silent success).
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_produce_response(version: i16, req: &ProduceRequest) -> Result<Bytes> {
    let responses = req
        .topic_data
        .iter()
        .map(|topic| {
            TopicProduceResponse::default()
                .with_name(topic.name.clone())
                .with_partition_responses(
                    topic
                        .partition_data
                        .iter()
                        .map(|p| produce_partition_response(p.index, ERROR_NOT_LEADER_OR_FOLLOWER))
                        .collect(),
                )
        })
        .collect();
    let resp = ProduceResponse::default().with_responses(responses);
    encode_message(&resp, version, 512)
}

fn produce_partition_response(index: i32, error_code: i16) -> PartitionProduceResponse {
    PartitionProduceResponse::default()
        .with_index(index)
        .with_error_code(error_code)
        .with_log_start_offset(0)
}

// ── Fetch ────────────────────────────────────────────────────────────────────

/// Well-formed Fetch response. Uses top-level `error_code` at v7+, or a single
/// placeholder topic/partition with per-partition `error_code` below v7.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_fetch_error_response(version: i16, error_code: i16) -> Result<Bytes> {
    if version >= 7 {
        return encode_fetch_response_inner(version, Vec::new(), error_code);
    }
    // No top-level error field below v7; the error surfaces on the placeholder partition instead.
    let topics = vec![
        FetchableTopicResponse::default()
            .with_partitions(vec![fetch_partition_response(0, error_code)]),
    ];
    encode_fetch_response_inner(version, topics, ERROR_NONE)
}

/// Stub: discard the payload and return a retriable error so clients don't mistake "no real
/// data yet" for a genuinely empty partition (same philosophy as Produce).
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_fetch_response(version: i16, req: &FetchRequest) -> Result<Bytes> {
    let topics = req
        .topics
        .iter()
        .map(|topic| {
            FetchableTopicResponse::default()
                .with_topic(topic.topic.clone())
                .with_partitions(
                    topic
                        .partitions
                        .iter()
                        .map(|p| {
                            fetch_partition_response(p.partition, ERROR_NOT_LEADER_OR_FOLLOWER)
                        })
                        .collect(),
                )
        })
        .collect();
    encode_fetch_response_inner(version, topics, ERROR_NONE)
}

fn encode_fetch_response_inner(
    version: i16,
    topics: Vec<FetchableTopicResponse>,
    top_level_error: i16,
) -> Result<Bytes> {
    let resp = FetchResponse::default()
        .with_error_code(top_level_error)
        .with_responses(topics);
    encode_message(&resp, version, 512)
}

fn fetch_partition_response(partition: i32, error_code: i16) -> PartitionData {
    PartitionData::default()
        .with_partition_index(partition)
        .with_error_code(error_code)
        .with_last_stable_offset(0)
        .with_log_start_offset(0)
        .with_records(None)
}

// ── ListOffsets ──────────────────────────────────────────────────────────────

/// Well-formed `ListOffsets` response with a single placeholder topic/partition.
///
/// `kafka_protocol` has no encodable representation for `ListOffsets` v0 (the legacy
/// `old_style_offsets` shape predates the schema this crate generates from); a v0 request now
/// falls through `super::api::unsupported_version_response`'s encode-failure path to `Close`
/// instead of the pre-migration downgraded response.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version` (always the
/// case for `version == 0`).
pub fn encode_list_offsets_error_response(version: i16, error_code: i16) -> Result<Bytes> {
    let topics = vec![
        ListOffsetsTopicResponse::default()
            .with_partitions(vec![list_offsets_partition_response(0, error_code)]),
    ];
    encode_list_offsets_response_inner(version, topics)
}

/// One resolved `ListOffsets` answer for a single requested partition.
///
/// The wire `(timestamp, offset)` pair on success, or the error code to report for that
/// partition alone - a topic-level failure (bad topic name, bridge timeout, topic doesn't exist)
/// still lets other topics in the same request succeed, so this stays per-partition rather than
/// failing the whole response.
pub type ListOffsetsPartitionResult = std::result::Result<(i64, i64), i16>;

/// Bridge-backed `ListOffsets` response.
///
/// One [`ListOffsetsPartitionResult`] per partition in `req`, in the same topic/partition order
/// as the request (`handle_list_offsets` builds this by resolving each topic against
/// [`crate::bridge::TopicCatalog::high_watermarks`]).
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_list_offsets_response(
    version: i16,
    req: &ListOffsetsRequest,
    results: &[Vec<ListOffsetsPartitionResult>],
) -> Result<Bytes> {
    let topics = req
        .topics
        .iter()
        .zip(results)
        .map(|(topic, partition_results)| {
            ListOffsetsTopicResponse::default()
                .with_name(topic.name.clone())
                .with_partitions(
                    topic
                        .partitions
                        .iter()
                        .zip(partition_results)
                        .map(|(p, result)| match result {
                            Ok((timestamp, offset)) => ListOffsetsPartitionResponse::default()
                                .with_partition_index(p.partition_index)
                                .with_error_code(ERROR_NONE)
                                .with_timestamp(*timestamp)
                                .with_offset(*offset),
                            Err(error_code) => {
                                list_offsets_partition_response(p.partition_index, *error_code)
                            }
                        })
                        .collect(),
                )
        })
        .collect();
    encode_list_offsets_response_inner(version, topics)
}

fn encode_list_offsets_response_inner(
    version: i16,
    topics: Vec<ListOffsetsTopicResponse>,
) -> Result<Bytes> {
    let resp = ListOffsetsResponse::default().with_topics(topics);
    encode_message(&resp, version, 256)
}

/// `timestamp`/`offset` default to `-1`, matching a real broker's own error-path convention
/// (`kafka_protocol`'s `#[derive(Default)]` would otherwise zero them, which reads as "offset 0",
/// a genuinely valid answer, rather than "no answer").
fn list_offsets_partition_response(
    partition: i32,
    error_code: i16,
) -> ListOffsetsPartitionResponse {
    ListOffsetsPartitionResponse::default()
        .with_partition_index(partition)
        .with_error_code(error_code)
        .with_timestamp(-1)
        .with_offset(-1)
}

// ── CreateTopics ─────────────────────────────────────────────────────────────

/// Whole-request `CreateTopics` failure (decode error, unsupported version).
///
/// One placeholder topic result carrying `error_code`, since no real per-topic breakdown is
/// possible when the request itself couldn't be read.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_create_topics_error_response(version: i16, error_code: i16) -> Result<Bytes> {
    let topics = vec![
        CreatableTopicResult::default()
            .with_error_code(error_code)
            .with_error_message(None)
            .with_num_partitions(-1)
            .with_replication_factor(-1),
    ];
    let resp = CreateTopicsResponse::default().with_topics(topics);
    encode_message(&resp, version, 256)
}

/// One resolved `CreateTopics` outcome.
///
/// `Ok(partitions_created)` on success, `Err(error_code)` otherwise - a per-topic failure (bad
/// config, already exists with a different count) must not fail sibling topics in the same
/// request.
pub type CreateTopicResult = std::result::Result<u32, i16>;

/// Bridge-backed `CreateTopics` response.
///
/// One [`CreateTopicResult`] per topic in `topics`, in the same order (`handle_create_topics`
/// builds this by resolving each topic against
/// [`crate::bridge::TopicCatalog::ensure_stream_and_topic`]).
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_create_topics_response(
    topics: &[CreatableTopic],
    results: &[CreateTopicResult],
    version: i16,
) -> Result<Bytes> {
    let response_results = topics
        .iter()
        .zip(results)
        .map(|(topic, result)| match result {
            Ok(partitions_count) => CreatableTopicResult::default()
                .with_name(topic.name.clone())
                .with_error_code(ERROR_NONE)
                .with_error_message(None)
                .with_num_partitions(i32::try_from(*partitions_count).unwrap_or(i32::MAX))
                // Iggy's replication is cluster-wide (Raft over the whole stream), not a
                // per-topic knob this bridge can set - echoed back as 1 regardless of what was
                // requested, matching this bridge's documented "RF is accepted, not applied"
                // policy (see README's CreateTopics section).
                .with_replication_factor(1),
            Err(error_code) => CreatableTopicResult::default()
                .with_name(topic.name.clone())
                .with_error_code(*error_code)
                .with_error_message(None)
                .with_num_partitions(-1)
                .with_replication_factor(-1),
        })
        .collect();
    let resp = CreateTopicsResponse::default().with_topics(response_results);
    encode_message(&resp, version, 256)
}

/// Broker default when `num_partitions == -1` (KIP-464). Real Kafka's own broker default
/// (`num.partitions`) is 1 out of the box; this bridge has no equivalent per-deployment config
/// yet, so 1 is hardcoded rather than invented.
const DEFAULT_PARTITION_COUNT: u32 = 1;

/// Validates one requested topic's `num_partitions`/`replication_factor` shape.
///
/// Not whether the bridge call itself would succeed - existence conflicts surface later, from
/// `ensure_stream_and_topic`'s own result - and resolves the KIP-464 broker-default sentinel
/// (`-1`) to a concrete partition count.
///
/// KIP-464: `num_partitions = -1` / `replication_factor = -1` mean broker default when either
/// (a) the version is v4+, or (b) the topic carries a manual partition assignment (valid on
/// v2/v3 as well).
///
/// # Errors
///
/// Returns [`ERROR_INVALID_PARTITIONS`] or [`ERROR_INVALID_REPLICATION_FACTOR`] if the shape is
/// invalid for `version`.
pub fn validate_create_topic_shape(
    topic: &CreatableTopic,
    version: i16,
) -> std::result::Result<u32, i16> {
    let broker_default_ok = version >= 4 || !topic.assignments.is_empty();

    let partition_count = if topic.num_partitions == -1 && broker_default_ok {
        DEFAULT_PARTITION_COUNT
    } else if topic.num_partitions > 0 {
        u32::try_from(topic.num_partitions).map_err(|_| ERROR_INVALID_PARTITIONS)?
    } else {
        return Err(ERROR_INVALID_PARTITIONS);
    };

    let replication_ok = if broker_default_ok {
        topic.replication_factor == -1 || topic.replication_factor > 0
    } else {
        topic.replication_factor > 0
    };
    if !replication_ok {
        return Err(ERROR_INVALID_REPLICATION_FACTOR);
    }

    Ok(partition_count)
}
