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

use bytes::Bytes;
use kafka_protocol::messages::metadata_response::{MetadataResponseBroker, MetadataResponseTopic};
use kafka_protocol::messages::{BrokerId, MetadataRequest, MetadataResponse, TopicName};
use kafka_protocol::protocol::StrBytes;

use crate::error::{KafkaProtocolError, Result};
use crate::protocol::api::{
    API_KEY_METADATA, ApiVersionRange, BrokerAdvertise, ERROR_NONE,
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

#[expect(
    clippy::unused_async,
    reason = "the shared handler signature, kept until a handler awaits the bridge"
)]
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
    match decode_topics(api_version, body, state.max_frame_size) {
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
    }
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
}
