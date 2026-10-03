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

//! `DeleteTopics` (API key 20).
//!
//! Deletes the Iggy topic a Kafka topic name resolves to. Never deletes the backing Iggy
//! stream - see [`IggyBridge::delete_kafka_topic`]'s own doc comment for why: the shared
//! `default_stream` can hold other Kafka topics' data, and this bridge has no way to tell
//! whether the stream it just emptied is actually abandoned or just temporarily topic-less.

use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::messages::delete_topics_response::DeletableTopicResult;
use kafka_protocol::messages::{DeleteTopicsRequest, DeleteTopicsResponse, TopicName};

use tokio::time::Instant;

use crate::bridge::{BridgeError, IggyBridge};
use crate::error::Result;
use crate::protocol::api::{
    API_KEY_DELETE_TOPICS, ApiVersionRange, ERROR_INVALID_REQUEST, ERROR_NONE,
    ERROR_NOT_CONTROLLER, ERROR_POLICY_VIOLATION, ERROR_REQUEST_TIMED_OUT, GatewayState,
    HandleOutcome,
};
use crate::protocol::bounds_guard::validate_delete_topics_shape;
use crate::protocol::handlers::{
    decode_guarded, encode_message, handle_versioned_request, is_supported_version,
    respond_or_close, unsupported_version_response,
};

/// This bridge never advertises v6 (the `topics: Vec<DeleteTopicState>`, topic-id-based shape).
///
/// It has no concept of a Kafka topic id, only the Kafka-side name `TopicMapping` resolves. v1
/// is the crate's own floor (`kafka_protocol` does not implement v0 for this message).
pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_DELETE_TOPICS,
    min_version: 1,
    max_version: 5,
};

/// Cap on distinct topic names one `DeleteTopics` request may address through the bridge.
///
/// Same rationale as `create_topics::MAX_BRIDGE_BACKED_TOPICS`: each name costs a bridge round
/// trip against the single lockstep `IggyClient` every Kafka connection on this gateway shares.
const MAX_BRIDGE_BACKED_TOPICS: usize = 100;

/// Same bounds as `create_topics::clamp_request_timeout` - this request carries the identical
/// KIP-4 `timeout_ms` field for the identical reason (a client-supplied deadline that becomes
/// the aggregate bridge-work budget, otherwise unchecked).
const MIN_REQUEST_TIMEOUT: Duration = Duration::from_millis(1_000);
const MAX_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

fn clamp_request_timeout(timeout_ms: i32) -> Duration {
    let requested = Duration::from_millis(u64::try_from(timeout_ms).unwrap_or(0));
    requested.clamp(MIN_REQUEST_TIMEOUT, MAX_REQUEST_TIMEOUT)
}

pub async fn handle(state: &GatewayState, api_version: i16, body: Bytes) -> HandleOutcome {
    let Some(bridge) = &state.bridge else {
        return handle_versioned_request(
            API_KEY_DELETE_TOPICS,
            api_version,
            body,
            |v, b| {
                decode_guarded::<DeleteTopicsRequest>(v, b, |v, b| {
                    validate_delete_topics_shape(v, b, state.max_frame_size)
                })
            },
            encode_response,
            encode_error_response,
            "DeleteTopics",
        );
    };

    if !is_supported_version(API_KEY_DELETE_TOPICS, api_version) {
        return unsupported_version_response(API_KEY_DELETE_TOPICS, api_version, |version| {
            encode_error_response(version, ERROR_INVALID_REQUEST)
        });
    }

    let req = match decode_guarded::<DeleteTopicsRequest>(api_version, body, |v, b| {
        validate_delete_topics_shape(v, b, state.max_frame_size)
    }) {
        Ok(req) => req,
        Err(error) => {
            // debug!, not warn!: attacker-controlled, not operator-actionable.
            tracing::debug!(%error, "Failed to decode DeleteTopics request");
            return respond_or_close(
                encode_error_response(api_version, ERROR_INVALID_REQUEST),
                "DeleteTopics",
            );
        }
    };

    // RANGE caps at v5, so req.topics (the v6 topic-id shape) is always empty - topic_names is
    // the only populated field at any version this bridge advertises.
    if req.topic_names.len() > MAX_BRIDGE_BACKED_TOPICS {
        tracing::warn!(
            distinct_topics = req.topic_names.len(),
            max = MAX_BRIDGE_BACKED_TOPICS,
            "DeleteTopics request addresses too many topics; rejecting"
        );
        let message = kafka_protocol::protocol::StrBytes::from(format!(
            "this gateway addresses at most {MAX_BRIDGE_BACKED_TOPICS} distinct topics per DeleteTopics request"
        ));
        let results = req
            .topic_names
            .iter()
            .map(|name| {
                DeletableTopicResult::default()
                    .with_name(Some(name.clone()))
                    .with_error_code(ERROR_POLICY_VIOLATION)
                    .with_error_message(Some(message.clone()))
            })
            .collect();
        let resp = DeleteTopicsResponse::default().with_responses(results);
        return respond_or_close(encode_message(&resp, api_version, 256), "DeleteTopics");
    }

    let deadline = Instant::now() + clamp_request_timeout(req.timeout_ms);
    let results = delete_all_topics(bridge, &req.topic_names, deadline).await;
    let resp = DeleteTopicsResponse::default().with_responses(results);
    respond_or_close(encode_message(&resp, api_version, 256), "DeleteTopics")
}

/// Deletes every requested topic, independently of the others.
///
/// `deadline` bounds each topic's own bridge work individually (`timeout_at`), not the whole
/// loop - same reasoning as `create_topics::create_all_topics`: a single `timeout` around the
/// entire call would discard every already-resolved result the moment one topic's call ran
/// long, answering a retriable code even for topics that had already deleted cleanly.
///
/// A name repeated within the same request is not specially rejected the way `CreateTopics`
/// rejects a duplicate: deleting the same topic twice has no race to protect against the way
/// creating it twice does - the first occurrence deletes it, the second then answers
/// `UNKNOWN_TOPIC_OR_PARTITION` because by the time it runs, the topic genuinely doesn't exist
/// any more. That's the correct answer for a duplicate, not a special case.
async fn delete_all_topics(
    bridge: &IggyBridge,
    topic_names: &[TopicName],
    deadline: Instant,
) -> Vec<DeletableTopicResult> {
    let mut results = Vec::with_capacity(topic_names.len());
    for name in topic_names {
        let result = match tokio::time::timeout_at(deadline, delete_one_topic(bridge, name)).await {
            Ok(result) => result,
            Err(_elapsed) => {
                tracing::warn!(
                    kafka_topic = name.as_str(),
                    "DeleteTopics: this topic's bridge work exceeded the request deadline; \
                     answering retriable instead of blocking further"
                );
                DeletableTopicResult::default()
                    .with_name(Some(name.clone()))
                    .with_error_code(ERROR_REQUEST_TIMED_OUT)
                    .with_error_message(None)
            }
        };
        results.push(result);
    }
    results
}

async fn delete_one_topic(bridge: &IggyBridge, name: &TopicName) -> DeletableTopicResult {
    let result = DeletableTopicResult::default()
        .with_name(Some(name.clone()))
        .with_error_message(None);
    match bridge.delete_kafka_topic(name.as_str()).await {
        Ok(()) => result.with_error_code(ERROR_NONE),
        Err(err) => bridge_error_result(result, name.as_str(), &err),
    }
}

/// Maps a bridge failure to a Kafka result, logging the real cause server-side.
///
/// Same caution as `create_topics::bridge_error_result` on forwarding `err.to_string()`: a
/// server-side rejection can reconstruct with default-valued fields, so only the client-caused
/// variant (an invalid name) forwards its own text; everything else gets a fixed message.
fn bridge_error_result(
    result: DeletableTopicResult,
    kafka_topic: &str,
    err: &BridgeError,
) -> DeletableTopicResult {
    let error_code = err.to_kafka_error_code();
    match err {
        BridgeError::InvalidKafkaTopicName { reason, .. } => {
            tracing::debug!(
                kafka_topic,
                reason,
                "DeleteTopics rejected an invalid topic name"
            );
            result.with_error_code(error_code).with_error_message(Some(
                kafka_protocol::protocol::StrBytes::from(reason.clone()),
            ))
        }
        other => {
            tracing::error!(kafka_topic, %other, "DeleteTopics failed against the Iggy bridge");
            result.with_error_code(error_code).with_error_message(Some(
                kafka_protocol::protocol::StrBytes::from(
                    "internal error deleting this topic".to_string(),
                ),
            ))
        }
    }
}

/// Well-formed `DeleteTopics` response naming every requested topic.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_error_response(version: i16, error_code: i16) -> Result<Bytes> {
    encode_inner(version, &[], error_code)
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_response(version: i16, req: &DeleteTopicsRequest) -> Result<Bytes> {
    encode_inner(version, &req.topic_names, ERROR_NOT_CONTROLLER)
}

fn encode_inner(version: i16, topic_names: &[TopicName], forced_error: i16) -> Result<Bytes> {
    let results = topic_names
        .iter()
        .map(|name| {
            DeletableTopicResult::default()
                .with_name(Some(name.clone()))
                .with_error_code(forced_error)
                .with_error_message(None)
        })
        .collect();
    let resp = DeleteTopicsResponse::default().with_responses(results);
    encode_message(&resp, version, 256)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamp_request_timeout_rejects_a_zero_or_negative_value_up_to_the_floor() {
        assert_eq!(clamp_request_timeout(0), MIN_REQUEST_TIMEOUT);
        assert_eq!(clamp_request_timeout(-1), MIN_REQUEST_TIMEOUT);
        assert_eq!(clamp_request_timeout(i32::MIN), MIN_REQUEST_TIMEOUT);
    }

    #[test]
    fn clamp_request_timeout_caps_an_oversized_value_at_the_ceiling() {
        assert_eq!(clamp_request_timeout(i32::MAX), MAX_REQUEST_TIMEOUT);
    }

    #[test]
    fn clamp_request_timeout_passes_through_a_reasonable_value_unchanged() {
        assert_eq!(clamp_request_timeout(5_000), Duration::from_secs(5));
    }
}
