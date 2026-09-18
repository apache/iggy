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

//! Kafka response encoders (stub implementations).
//!
//! Wire encoding (field order, version gating, compact vs. legacy shapes) is
//! `kafka_protocol`'s responsibility; everything here is stub *policy* - which placeholder
//! values and error codes a request gets back before the Iggy bridge lands.

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::create_topics_response::CreatableTopicResult;
use kafka_protocol::messages::describe_acls_response::{AclDescription, DescribeAclsResource};
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use kafka_protocol::messages::{
    CreateTopicsRequest, CreateTopicsResponse, DescribeAclsResponse, FetchRequest, FetchResponse,
    ListOffsetsRequest, ListOffsetsResponse, ProduceRequest, ProduceResponse,
    SaslAuthenticateResponse, SaslHandshakeResponse,
};
use kafka_protocol::protocol::{Encodable, StrBytes};

use crate::error::{KafkaProtocolError, Result};
use crate::protocol::acl::{self, AclBinding};
use crate::protocol::api::{
    ERROR_INVALID_PARTITIONS, ERROR_INVALID_REPLICATION_FACTOR, ERROR_NONE, ERROR_NOT_CONTROLLER,
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

/// Stub: discard the payload and return a retriable error, matching Produce/Fetch - a genuine
/// offset lookup requires the same partition-leadership the stub doesn't have yet.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_list_offsets_response(version: i16, req: &ListOffsetsRequest) -> Result<Bytes> {
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
                            list_offsets_partition_response(
                                p.partition_index,
                                ERROR_NOT_LEADER_OR_FOLLOWER,
                            )
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

fn list_offsets_partition_response(
    partition: i32,
    error_code: i16,
) -> ListOffsetsPartitionResponse {
    ListOffsetsPartitionResponse::default()
        .with_partition_index(partition)
        .with_error_code(error_code)
}

// ── SASL ─────────────────────────────────────────────────────────────────────

/// `SaslHandshake` response: the outcome plus the mechanisms this gateway enables.
///
/// The mechanism list is sent on every outcome, not just success. A client that asked for an
/// unsupported mechanism prints the list to tell its operator what to configure instead, so
/// omitting it on the error path turns a fixable misconfiguration into an opaque failure.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_sasl_handshake_response(
    version: i16,
    error_code: i16,
    mechanisms: &[&str],
) -> Result<Bytes> {
    let resp = SaslHandshakeResponse::default()
        .with_error_code(error_code)
        .with_mechanisms(
            mechanisms
                .iter()
                .map(|name| StrBytes::from_string((*name).to_string()))
                .collect(),
        );
    encode_message(&resp, version, 64)
}

/// `SaslAuthenticate` response.
///
/// `session_lifetime_ms` is always 0, meaning the session never needs re-authenticating (KIP-368).
/// That is not only a simplification: Iggy's sole correct re-authentication is logout followed by
/// login, which drops and re-mints the session, and re-login on a still-bound connection takes a
/// replay branch that reports the new user while leaving the server bound to the old one. Until
/// that is resolved, promising a finite lifetime would promise something unsafe to deliver.
///
/// `auth_bytes` is empty on success, which is what PLAIN's server completion looks like.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_sasl_authenticate_response(
    version: i16,
    error_code: i16,
    error_message: Option<&str>,
) -> Result<Bytes> {
    let resp = SaslAuthenticateResponse::default()
        .with_error_code(error_code)
        .with_error_message(error_message.map(|msg| StrBytes::from_string(msg.to_string())))
        .with_auth_bytes(Bytes::new())
        .with_session_lifetime_ms(0);
    encode_message(&resp, version, 64)
}

// ── DescribeAcls ─────────────────────────────────────────────────────────────

/// `DescribeAcls` response, grouping the selected bindings by resource.
///
/// Kafka nests ACLs under the resource they apply to, so bindings that share a resource type and
/// name become one entry with several operations. An empty result is `error_code` 0 with no
/// resources, never an error: a real broker distinguishes "nothing matched" from "the request
/// failed", and an admin tool prints them very differently.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_describe_acls_response(
    version: i16,
    principal: &str,
    bindings: &[AclBinding],
) -> Result<Bytes> {
    let mut grouped: Vec<DescribeAclsResource> = Vec::new();
    for binding in bindings {
        let description = AclDescription::default()
            .with_principal(StrBytes::from_string(format!("User:{principal}")))
            .with_host(StrBytes::from_static_str(acl::ANY_HOST))
            .with_operation(binding.operation)
            .with_permission_type(acl::permission_type::ALLOW);

        if let Some(resource) = grouped.iter_mut().find(|resource| {
            resource.resource_type == binding.resource_type
                && resource.resource_name.as_str() == binding.resource_name
        }) {
            resource.acls.push(description);
        } else {
            grouped.push(
                DescribeAclsResource::default()
                    .with_resource_type(binding.resource_type)
                    .with_resource_name(StrBytes::from_static_str(binding.resource_name))
                    .with_pattern_type(acl::pattern_type::LITERAL)
                    .with_acls(vec![description]),
            );
        }
    }

    // `error_message` is explicitly null, not left at the type's default: that default is
    // `Some("")`, and Java substitutes its own text for an error only when the field is null, so a
    // defaulted empty string reaches an operator as a failure with no stated cause.
    let resp = DescribeAclsResponse::default()
        .with_error_code(ERROR_NONE)
        .with_error_message(None)
        .with_resources(grouped);
    encode_message(&resp, version, 256)
}

/// Well-formed `DescribeAcls` response carrying only an error.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_describe_acls_error_response(version: i16, error_code: i16) -> Result<Bytes> {
    // Null rather than the type's default `Some("")`, so the client substitutes the standard text
    // for the code instead of printing an empty reason.
    let resp = DescribeAclsResponse::default()
        .with_error_code(error_code)
        .with_error_message(None)
        .with_resources(Vec::new());
    encode_message(&resp, version, 64)
}

// ── CreateTopics ─────────────────────────────────────────────────────────────

/// Well-formed `CreateTopics` response with a single placeholder topic.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_create_topics_error_response(version: i16, error_code: i16) -> Result<Bytes> {
    let topics = vec![
        CreatableTopic::default()
            .with_num_partitions(1)
            .with_replication_factor(1),
    ];
    encode_create_topics_response_inner(version, &topics, error_code)
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_create_topics_response(version: i16, req: &CreateTopicsRequest) -> Result<Bytes> {
    encode_create_topics_response_inner(version, &req.topics, ERROR_NONE)
}

/// Resolve per-topic `CreateTopics` error.
///
/// KIP-464: `num_partitions = -1` / `replication_factor = -1` mean broker default when either
/// (a) the version is v4+, or (b) the topic carries a manual partition assignment (valid on
/// v2/v3 as well). Otherwise non-positive values are [`ERROR_INVALID_PARTITIONS`] /
/// [`ERROR_INVALID_REPLICATION_FACTOR`]. When validation passes, the stub returns
/// [`ERROR_NOT_CONTROLLER`] so clients do not believe the topic was created.
const fn create_topics_topic_error(version: i16, topic: &CreatableTopic, forced_error: i16) -> i16 {
    if forced_error != ERROR_NONE {
        return forced_error;
    }

    let broker_default_ok = version >= 4 || !topic.assignments.is_empty();

    let partitions_ok = if broker_default_ok {
        topic.num_partitions == -1 || topic.num_partitions > 0
    } else {
        topic.num_partitions > 0
    };
    if !partitions_ok {
        return ERROR_INVALID_PARTITIONS;
    }

    let replication_ok = if broker_default_ok {
        topic.replication_factor == -1 || topic.replication_factor > 0
    } else {
        topic.replication_factor > 0
    };
    if !replication_ok {
        return ERROR_INVALID_REPLICATION_FACTOR;
    }

    ERROR_NOT_CONTROLLER
}

fn encode_create_topics_response_inner(
    version: i16,
    topics: &[CreatableTopic],
    topic_error: i16,
) -> Result<Bytes> {
    let results = topics
        .iter()
        .map(|topic| {
            CreatableTopicResult::default()
                .with_name(topic.name.clone())
                .with_error_code(create_topics_topic_error(version, topic, topic_error))
                .with_error_message(None)
                .with_num_partitions(topic.num_partitions)
                .with_replication_factor(topic.replication_factor)
        })
        .collect();
    let resp = CreateTopicsResponse::default().with_topics(results);
    encode_message(&resp, version, 256)
}
