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

use bytes::Bytes;

use crate::error::{KafkaProtocolError, Result};
use crate::protocol::codec::{Decoder, Encoder};
use crate::protocol::requests::{
    ProduceDecodeResult, decode_create_topics_request, decode_fetch_request,
    decode_list_offsets_request, decode_produce_request,
};
use crate::protocol::responses::{
    encode_create_topics_error_response, encode_create_topics_response,
    encode_fetch_error_response, encode_fetch_response, encode_list_offsets_error_response,
    encode_list_offsets_response, encode_produce_error_response, encode_produce_response,
};

pub const API_KEY_PRODUCE: i16 = 0;
pub const API_KEY_FETCH: i16 = 1;
pub const API_KEY_LIST_OFFSETS: i16 = 2;
pub const API_KEY_METADATA: i16 = 3;
pub const API_KEY_API_VERSIONS: i16 = 18;
pub const API_KEY_CREATE_TOPICS: i16 = 19;

pub const DEFAULT_KAFKA_PORT: u16 = 9093;

pub const ERROR_NONE: i16 = 0;
pub const ERROR_UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
pub const ERROR_UNSUPPORTED_VERSION: i16 = 35;
pub const ERROR_INVALID_PARTITIONS: i16 = 37;
pub const ERROR_INVALID_REQUEST: i16 = 42;

const MAX_SUPPORTED_METADATA_VERSION: i16 = 9;

/// Sentinel for `topic_authorized_operations` / `cluster_authorized_operations` when ACLs are not supported.
const AUTHORIZED_OPS_UNKNOWN: i32 = i32::MIN;

/// Result of handling one Kafka request body.
#[derive(Debug)]
pub enum HandleOutcome {
    /// Write this response body (with a response header).
    Respond(Bytes),
    /// Produce with `acks=0`: write nothing, keep the connection open.
    NoResponse,
    /// Client cannot parse an error at this request wire version; close the TCP connection.
    Close,
}

impl HandleOutcome {
    /// Collapse to `Some(body)` for a normal response, or `None` for [`HandleOutcome::NoResponse`].
    ///
    /// # Panics
    ///
    /// Panics on [`HandleOutcome::Close`] — match on `Close` explicitly, or use
    /// [`Self::expect_response`] in tests that require a body.
    #[must_use]
    pub fn into_optional_response(self) -> Option<Bytes> {
        match self {
            Self::Respond(body) => Some(body),
            Self::NoResponse => None,
            Self::Close => panic!("HandleOutcome::Close has no response body"),
        }
    }

    /// Return the response body, or panic with `msg` if the outcome is not [`Self::Respond`].
    ///
    /// # Panics
    ///
    /// Panics when the outcome is [`Self::NoResponse`] or [`Self::Close`].
    #[must_use]
    pub fn expect_response(self, msg: &str) -> Bytes {
        match self {
            Self::Respond(body) => body,
            Self::NoResponse => panic!("{msg}: got NoResponse"),
            Self::Close => panic!("{msg}: got Close"),
        }
    }

    #[must_use]
    pub const fn is_no_response(&self) -> bool {
        matches!(self, Self::NoResponse)
    }

    #[must_use]
    pub const fn is_close(&self) -> bool {
        matches!(self, Self::Close)
    }
}

#[derive(Debug, Clone)]
pub struct BrokerAdvertise {
    pub host: String,
    pub port: i32,
}

impl Default for BrokerAdvertise {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: i32::from(DEFAULT_KAFKA_PORT),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ApiVersionRange {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

static SUPPORTED_RANGES: &[ApiVersionRange] = &[
    ApiVersionRange {
        api_key: API_KEY_PRODUCE,
        min_version: 3,
        max_version: 9,
    },
    ApiVersionRange {
        api_key: API_KEY_FETCH,
        min_version: 4,
        max_version: 12,
    },
    ApiVersionRange {
        api_key: API_KEY_LIST_OFFSETS,
        min_version: 1,
        max_version: 6,
    },
    ApiVersionRange {
        api_key: API_KEY_METADATA,
        min_version: 0,
        max_version: 9,
    },
    ApiVersionRange {
        api_key: API_KEY_API_VERSIONS,
        min_version: 0,
        max_version: 3,
    },
    ApiVersionRange {
        api_key: API_KEY_CREATE_TOPICS,
        min_version: 2,
        max_version: 5,
    },
];

#[must_use]
pub fn supported_api_ranges() -> &'static [ApiVersionRange] {
    SUPPORTED_RANGES
}

/// Handles one decoded request frame and returns how the connection should proceed.
pub fn handle_request(
    api_key: i16,
    api_version: i16,
    body: Bytes,
    broker: &BrokerAdvertise,
) -> HandleOutcome {
    if api_key == API_KEY_PRODUCE {
        return handle_produce_request(api_version, body);
    }
    handle_other_request(api_key, api_version, body, broker)
}

/// Produce is the only request the wire protocol allows to go unanswered
/// (`acks=0`), so it gets its own path that may return [`HandleOutcome::NoResponse`].
fn handle_produce_request(api_version: i16, body: Bytes) -> HandleOutcome {
    if !is_supported_version(API_KEY_PRODUCE, api_version) {
        return HandleOutcome::Respond(encode_produce_error_response(
            api_version,
            ERROR_UNSUPPORTED_VERSION,
        ));
    }
    match decode_produce_request(api_version, body) {
        // acks=0 is fire-and-forget: the client isn't reading a response, so
        // sending one desyncs the next correlation id it expects.
        ProduceDecodeResult::Ok(req) if req.acks == 0 => HandleOutcome::NoResponse,
        ProduceDecodeResult::Ok(req) => {
            HandleOutcome::Respond(encode_produce_response(api_version, &req))
        }
        ProduceDecodeResult::Err {
            acks: Some(0),
            error,
        } => {
            tracing::warn!(
                "Failed to decode Produce request with acks=0 (no response): {:?}",
                error
            );
            HandleOutcome::NoResponse
        }
        ProduceDecodeResult::Err { error, .. } => {
            tracing::warn!("Failed to decode Produce request: {:?}", error);
            HandleOutcome::Respond(encode_produce_error_response(
                api_version,
                ERROR_INVALID_REQUEST,
            ))
        }
    }
}

fn handle_other_request(
    api_key: i16,
    api_version: i16,
    body: Bytes,
    broker: &BrokerAdvertise,
) -> HandleOutcome {
    match api_key {
        API_KEY_API_VERSIONS => {
            if is_supported_version(api_key, api_version) {
                HandleOutcome::Respond(encode_api_versions_response(api_version, ERROR_NONE))
            } else {
                // KIP-511: reply with v0 when the requested version is not understood.
                HandleOutcome::Respond(encode_api_versions_response(0, ERROR_UNSUPPORTED_VERSION))
            }
        }
        API_KEY_METADATA => {
            if is_supported_version(api_key, api_version) {
                HandleOutcome::Respond(encode_metadata_response(
                    api_version,
                    api_version,
                    body,
                    broker,
                    ERROR_NONE,
                ))
            } else {
                // Clamping the response to MAX_SUPPORTED_METADATA_VERSION leaves a body the
                // client parses at its own (unsupported) version, so UNSUPPORTED_VERSION never
                // survives. Clients that skip ApiVersions get a naked close instead.
                tracing::warn!(
                    api_version,
                    max_supported = MAX_SUPPORTED_METADATA_VERSION,
                    "Metadata version unsupported; closing connection"
                );
                HandleOutcome::Close
            }
        }
        API_KEY_FETCH => {
            if is_supported_version(api_key, api_version) {
                match decode_fetch_request(api_version, body) {
                    Ok(req) => HandleOutcome::Respond(encode_fetch_response(api_version, &req)),
                    Err(e) => {
                        tracing::warn!("Failed to decode Fetch request: {:?}", e);
                        HandleOutcome::Respond(encode_fetch_error_response(
                            api_version,
                            ERROR_INVALID_REQUEST,
                        ))
                    }
                }
            } else {
                HandleOutcome::Respond(encode_fetch_error_response(
                    api_version,
                    ERROR_UNSUPPORTED_VERSION,
                ))
            }
        }
        API_KEY_LIST_OFFSETS => {
            if is_supported_version(api_key, api_version) {
                match decode_list_offsets_request(api_version, body) {
                    Ok(req) => {
                        HandleOutcome::Respond(encode_list_offsets_response(api_version, &req))
                    }
                    Err(e) => {
                        tracing::warn!("Failed to decode ListOffsets request: {:?}", e);
                        HandleOutcome::Respond(encode_list_offsets_error_response(
                            api_version,
                            ERROR_INVALID_REQUEST,
                        ))
                    }
                }
            } else {
                HandleOutcome::Respond(encode_list_offsets_error_response(
                    api_version,
                    ERROR_UNSUPPORTED_VERSION,
                ))
            }
        }
        API_KEY_CREATE_TOPICS => {
            if is_supported_version(api_key, api_version) {
                match decode_create_topics_request(api_version, body) {
                    Ok(req) => {
                        HandleOutcome::Respond(encode_create_topics_response(api_version, &req))
                    }
                    Err(e) => {
                        tracing::warn!("Failed to decode CreateTopics request: {:?}", e);
                        HandleOutcome::Respond(encode_create_topics_error_response(
                            api_version,
                            ERROR_INVALID_REQUEST,
                        ))
                    }
                }
            } else {
                HandleOutcome::Respond(encode_create_topics_error_response(
                    api_version,
                    ERROR_UNSUPPORTED_VERSION,
                ))
            }
        }
        _ => HandleOutcome::Respond(encode_error_only_response(ERROR_UNSUPPORTED_VERSION)),
    }
}

#[must_use]
pub fn is_supported_version(api_key: i16, api_version: i16) -> bool {
    SUPPORTED_RANGES
        .iter()
        .find(|r| r.api_key == api_key)
        .is_some_and(|r| api_version >= r.min_version && api_version <= r.max_version)
}

/// Min version advertised in `ApiVersions` (may differ from the firewall min).
///
/// Produce must advertise min=0 per KAFKA-18659 / `PRODUCE_API_VERSIONS_RESPONSE_MIN_VERSION`
/// even though this gateway only accepts Produce v3+.
#[must_use]
pub const fn advertised_min_version(api_key: i16, firewall_min: i16) -> i16 {
    if api_key == API_KEY_PRODUCE {
        0
    } else {
        firewall_min
    }
}

fn encode_api_versions_response(api_version: i16, error_code: i16) -> Bytes {
    let flexible = api_version >= 3;
    let ranges = SUPPORTED_RANGES;
    let mut e = Encoder::with_capacity(128);

    e.write_i16(error_code);

    if flexible {
        e.write_varint((ranges.len() + 1) as u64);
        for r in ranges {
            e.write_i16(r.api_key);
            e.write_i16(advertised_min_version(r.api_key, r.min_version));
            e.write_i16(r.max_version);
            e.write_empty_tagged_fields();
        }
    } else {
        e.write_i32(i32::try_from(ranges.len()).expect("supported range table is small"));
        for r in ranges {
            e.write_i16(r.api_key);
            e.write_i16(advertised_min_version(r.api_key, r.min_version));
            e.write_i16(r.max_version);
        }
    }

    if api_version >= 1 {
        e.write_i32(0);
    }

    if flexible {
        e.write_empty_tagged_fields();
    }

    e.freeze()
}

fn encode_metadata_response(
    response_version: i16,
    decode_version: i16,
    body: Bytes,
    broker: &BrokerAdvertise,
    top_level_error_code: i16,
) -> Bytes {
    let flexible = response_version >= 9;
    // Empty body = all-topics request; 0 topics is correct for this stub.
    // Non-empty body that fails to decode = malformed request; return 0 topics.
    // Kafka Metadata response has no top-level error code field: errors are per-topic only.
    // 0 topics is spec-correct and unambiguous for a decode failure.
    let (topics, effective_error) = if body.is_empty() {
        (Vec::new(), top_level_error_code)
    } else {
        decode_metadata_request_topics(body, decode_version)
            .map_or((Vec::new(), ERROR_INVALID_REQUEST), |names| {
                (names, top_level_error_code)
            })
    };
    let topics_count = topics.len();
    let topic_error = if effective_error == ERROR_NONE {
        ERROR_UNKNOWN_TOPIC_OR_PARTITION
    } else {
        effective_error
    };

    let mut e = Encoder::with_capacity(256);

    if response_version >= 3 {
        e.write_i32(0); // throttle_time_ms (Metadata v3+)
    }

    if flexible {
        e.write_varint(2); // one broker (N+1)
        e.write_i32(1);
        e.write_compact_nullable_string(Some(&broker.host));
        e.write_i32(broker.port);
        e.write_compact_nullable_string(None); // rack
        e.write_empty_tagged_fields();

        e.write_compact_nullable_string(None); // cluster_id (v2+)
        e.write_i32(1); // controller_id (v1+)

        e.write_varint((topics_count + 1) as u64);
        for name in &topics {
            e.write_i16(topic_error);
            e.write_compact_nullable_string(Some(name));
            e.write_bool(false); // is_internal (v1+)
            e.write_varint(1); // empty partitions array
            e.write_i32(AUTHORIZED_OPS_UNKNOWN); // topic_authorized_operations (v8+)
            e.write_empty_tagged_fields();
        }
        e.write_i32(AUTHORIZED_OPS_UNKNOWN); // cluster_authorized_operations (v8+)
        e.write_empty_tagged_fields();
    } else {
        e.write_i32(1); // brokers array length
        e.write_i32(1); // node_id
        // broker.host is config-derived (KAFKA_ADVERTISED_HOST), not request-decoded — use
        // the checked variant so an overly long hostname returns an error instead of panicking.
        if e.write_nullable_string(Some(&broker.host)).is_err() {
            return encode_error_only_response(ERROR_INVALID_REQUEST);
        }
        e.write_i32(broker.port);
        if response_version >= 1 {
            e.write_nullable_string_unchecked(None); // rack
        }

        if response_version >= 2 {
            e.write_nullable_string_unchecked(None); // cluster_id
        }
        if response_version >= 1 {
            e.write_i32(1); // controller_id — must come before topics array
        }

        e.write_i32(i32::try_from(topics_count).expect("topic count bounded"));
        for name in &topics {
            e.write_i16(topic_error);
            e.write_nullable_string_unchecked(Some(name));
            if response_version >= 1 {
                e.write_bool(false); // is_internal
            }
            e.write_i32(0); // partitions array (empty)
            if response_version >= 8 {
                e.write_i32(AUTHORIZED_OPS_UNKNOWN); // topic_authorized_operations
            }
        }
        if response_version >= 8 {
            e.write_i32(AUTHORIZED_OPS_UNKNOWN); // cluster_authorized_operations
        }
    }

    e.freeze()
}

#[must_use]
pub fn encode_error_only_response(error_code: i16) -> Bytes {
    let mut e = Encoder::with_capacity(2);
    e.write_i16(error_code);
    e.freeze()
}

/// Decodes the requested topic names from a Metadata request body so the
/// response can echo them back; clients match metadata by name, not position.
pub(crate) fn decode_metadata_request_topics(body: Bytes, api_version: i16) -> Result<Vec<String>> {
    let mut d = Decoder::new(body);
    let flexible = api_version >= 9;
    let topics_count = if flexible {
        d.read_compact_array_count()?
    } else {
        d.read_i32_array_count()?
    };

    let mut topics = Vec::with_capacity(topics_count);
    for _ in 0..topics_count {
        if flexible && api_version >= 10 {
            // MetadataRequestTopic.topic_id: 16-byte UUID before name (v10+).
            let _topic_id = d.read_bytes(16)?;
        }
        let name = if flexible {
            d.read_compact_nullable_string()?
                .ok_or(KafkaProtocolError::NullTopicName)?
        } else {
            d.read_nullable_string()?
                .ok_or(KafkaProtocolError::NullTopicName)?
        };
        topics.push(name);
        if flexible {
            d.read_tagged_fields()?;
        }
    }

    Ok(topics)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::codec::Encoder;

    #[test]
    fn decode_metadata_request_topics_legacy_null_topic_name_fails() {
        let body = Bytes::from_static(&[
            0x00, 0x00, 0x00, 0x01, // one topic
            0xff, 0xff, // null topic name
        ]);
        let err = decode_metadata_request_topics(body, 0).unwrap_err();
        assert!(matches!(err, KafkaProtocolError::NullTopicName));
    }

    #[test]
    fn decode_metadata_request_topics_flexible_v10_truncated_topic_id_fails() {
        let mut enc = Encoder::with_capacity(8);
        enc.write_varint(2); // one topic
        enc.write_bytes(&[0u8; 8]); // truncated topic_id, should be 16 bytes
        let err = decode_metadata_request_topics(enc.freeze(), 10).unwrap_err();
        assert!(matches!(err, KafkaProtocolError::BufferUnderflow { .. }));
    }

    #[test]
    fn decode_metadata_request_topics_flexible_invalid_utf8_fails() {
        let body = Bytes::from_static(&[
            0x02, // one topic
            0x02, // string len = 1
            0xff, // invalid utf-8
            0x00, // tagged fields
        ]);
        let err = decode_metadata_request_topics(body, 9).unwrap_err();
        assert!(matches!(err, KafkaProtocolError::InvalidUtf8));
    }
}
