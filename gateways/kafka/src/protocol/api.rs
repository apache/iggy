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
use crate::protocol::codec::{Decoder, Encoder, PREALLOC_HINT};
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
/// Retriable; Produce stub uses this until the Iggy bridge persists records.
pub const ERROR_NOT_LEADER_OR_FOLLOWER: i16 = 6;
pub const ERROR_UNSUPPORTED_VERSION: i16 = 35;
pub const ERROR_INVALID_PARTITIONS: i16 = 37;
pub const ERROR_INVALID_REPLICATION_FACTOR: i16 = 38;
/// `CreateTopics` stub: do not claim topics were created (no controller / no Iggy bridge).
pub const ERROR_NOT_CONTROLLER: i16 = 41;
pub const ERROR_INVALID_REQUEST: i16 = 42;

/// Sentinel for `topic_authorized_operations` / `cluster_authorized_operations` when ACLs are not supported.
const AUTHORIZED_OPS_UNKNOWN: i32 = i32::MIN;

/// Result of handling one Kafka request body.
#[derive(Debug)]
pub enum HandleOutcome {
    /// Write this response body (with a response header).
    Respond(Bytes),
    /// Produce with `acks=0`: write nothing, keep the connection open.
    NoResponse,
    /// No parseable response exists for this request; close the TCP connection.
    Close,
}

impl HandleOutcome {
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
///
/// The firewall check runs AFTER decoding `acks`, not before: `ApiVersions` advertises
/// Produce min=0 (see [`advertised_min_version`]) while the firewall's real floor is 3, so a
/// spec-compliant client can legitimately send Produce v0-2 with `acks=0`. Rejecting those
/// versions before reading `acks` would send an error response the client never expects,
/// desyncing the next correlation id it reads.
fn handle_produce_request(api_version: i16, body: Bytes) -> HandleOutcome {
    match decode_produce_request(api_version, body) {
        // acks=0 is fire-and-forget: the client isn't reading a response, so
        // sending one desyncs the next correlation id it expects.
        ProduceDecodeResult::Ok(req) if req.acks == 0 => HandleOutcome::NoResponse,
        ProduceDecodeResult::Ok(req) => {
            if !is_supported_version(API_KEY_PRODUCE, api_version) {
                return unsupported_version_response(API_KEY_PRODUCE, api_version, |v| {
                    encode_produce_error_response(v, ERROR_UNSUPPORTED_VERSION)
                });
            }
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
        ProduceDecodeResult::Err { acks: None, error } => {
            // Decode failed before `acks` was read (malformed transactional_id or a truncated
            // frame). Whether the client wants a response is unknowable, and an error response
            // would desync an acks=0 fire-and-forget client's correlation stream. Frames are
            // length-delimited, so the malformed frame does not affect the next frame's boundary;
            // stay silent and keep the connection usable rather than risk that desync.
            tracing::warn!(
                "Failed to decode Produce request before acks was read (no response): {:?}",
                error
            );
            HandleOutcome::NoResponse
        }
        ProduceDecodeResult::Err {
            acks: Some(_),
            error,
        } => {
            tracing::warn!("Failed to decode Produce request: {:?}", error);
            if is_supported_version(API_KEY_PRODUCE, api_version) {
                HandleOutcome::Respond(encode_produce_error_response(
                    api_version,
                    ERROR_INVALID_REQUEST,
                ))
            } else {
                unsupported_version_response(API_KEY_PRODUCE, api_version, |v| {
                    encode_produce_error_response(v, ERROR_UNSUPPORTED_VERSION)
                })
            }
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
        API_KEY_API_VERSIONS => handle_api_versions(api_version, &body),
        API_KEY_METADATA => handle_metadata(api_version, body, broker),
        API_KEY_FETCH => handle_versioned_request(
            API_KEY_FETCH,
            api_version,
            body,
            decode_fetch_request,
            encode_fetch_response,
            encode_fetch_error_response,
            "Fetch",
        ),
        API_KEY_LIST_OFFSETS => handle_versioned_request(
            API_KEY_LIST_OFFSETS,
            api_version,
            body,
            decode_list_offsets_request,
            encode_list_offsets_response,
            encode_list_offsets_error_response,
            "ListOffsets",
        ),
        API_KEY_CREATE_TOPICS => handle_versioned_request(
            API_KEY_CREATE_TOPICS,
            api_version,
            body,
            decode_create_topics_request,
            encode_create_topics_response,
            encode_create_topics_error_response,
            "CreateTopics",
        ),
        _ => HandleOutcome::RespondAndClose(encode_error_only_response(ERROR_UNSUPPORTED_VERSION)),
    }
}

fn handle_api_versions(api_version: i16, body: &Bytes) -> HandleOutcome {
    if is_supported_version(API_KEY_API_VERSIONS, api_version) {
        match decode_api_versions_request(api_version, body) {
            Ok(()) => HandleOutcome::Respond(encode_api_versions_response(api_version, ERROR_NONE)),
            Err(e) => {
                tracing::warn!("Failed to decode ApiVersions request: {:?}", e);
                HandleOutcome::Respond(encode_api_versions_response(
                    api_version,
                    ERROR_INVALID_REQUEST,
                ))
            }
        }
    } else {
        // KIP-511: reply with v0 when the requested version is not understood.
        HandleOutcome::Respond(encode_api_versions_response(0, ERROR_UNSUPPORTED_VERSION))
    }
}

fn handle_metadata(api_version: i16, body: Bytes, broker: &BrokerAdvertise) -> HandleOutcome {
    if !is_supported_version(API_KEY_METADATA, api_version) {
        // Clamping the response to MAX_SUPPORTED_METADATA_VERSION leaves a body the
        // client parses at its own (unsupported) version, so UNSUPPORTED_VERSION never
        // survives. Clients that skip ApiVersions get a naked close instead.
        tracing::warn!(
            api_version,
            max_supported = MAX_SUPPORTED_METADATA_VERSION,
            "Metadata version unsupported; closing connection"
        );
        return HandleOutcome::Close;
    }
    match decode_metadata_request(api_version, body) {
        Ok(topics) => HandleOutcome::Respond(encode_metadata_response(
            api_version,
            &topics,
            broker,
            ERROR_NONE,
        )),
        Err(e) => {
            // Metadata has no top-level error field; a malformed body cannot carry
            // INVALID_REQUEST in a version-correct way for every client. Close.
            tracing::warn!(
                ?e,
                api_version,
                "Failed to decode Metadata request; closing connection"
            );
            HandleOutcome::Close
        }
    }
}

fn handle_versioned_request<T>(
    api_key: i16,
    api_version: i16,
    body: Bytes,
    decode: impl FnOnce(i16, Bytes) -> Result<T>,
    encode_ok: impl FnOnce(i16, &T) -> Bytes,
    encode_err: impl Fn(i16, i16) -> Bytes,
    api_name: &str,
) -> HandleOutcome {
    if is_supported_version(api_key, api_version) {
        match decode(api_version, body) {
            Ok(req) => HandleOutcome::Respond(encode_ok(api_version, &req)),
            Err(e) => {
                tracing::warn!("Failed to decode {api_name} request: {:?}", e);
                HandleOutcome::Respond(encode_err(api_version, ERROR_INVALID_REQUEST))
            }
        }
    } else {
        unsupported_version_response(api_key, api_version, |version| {
            encode_err(version, ERROR_UNSUPPORTED_VERSION)
        })
    }
}

/// Unsupported-version policy for APIs whose encoders only implement up to
/// [`ApiVersionRange::max_version`].
///
/// - `api_version > max`: Close. Encoding at the client's raw version would omit later-version
///   fields (`CreateTopics` v7 `TopicId`, Produce v13 UUID topic, …) and the client could not
///   parse the intended `UNSUPPORTED_VERSION` body.
/// - `api_version < min` but still within encoder capability: Respond with an error shaped for
///   that version (e.g. `ListOffsets` v0 `old_style_offsets`, Produce v0–2).
fn unsupported_version_response(
    api_key: i16,
    api_version: i16,
    encode: impl FnOnce(i16) -> Bytes,
) -> HandleOutcome {
    let max_version = SUPPORTED_RANGES
        .iter()
        .find(|r| r.api_key == api_key)
        .map_or(0, |r| r.max_version);
    if api_version > max_version {
        tracing::warn!(
            api_key,
            api_version,
            max_version,
            "request version above encoder max; closing connection"
        );
        return HandleOutcome::Close;
    }
    HandleOutcome::Respond(encode(api_version))
}

#[must_use]
pub fn is_supported_version(api_key: i16, api_version: i16) -> bool {
    SUPPORTED_RANGES
        .iter()
        .find(|r| r.api_key == api_key)
        .is_some_and(|r| api_version >= r.min_version && api_version <= r.max_version)
}

/// Highest version this gateway accepts for `api_key`, from the single firewall table.
#[must_use]
pub fn supported_max_version(api_key: i16) -> Option<i16> {
    SUPPORTED_RANGES
        .iter()
        .find(|r| r.api_key == api_key)
        .map(|r| r.max_version)
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
    topics: &[String],
    broker: &BrokerAdvertise,
    topic_error_override: i16,
) -> Bytes {
    let flexible = response_version >= 9;
    let topics_count = topics.len();
    // Stub has no topic catalog: echo requested names with UNKNOWN_TOPIC_OR_PARTITION,
    // or a forced override (unused today; kept for symmetry with other encoders).
    let topic_error = if topic_error_override == ERROR_NONE {
        ERROR_UNKNOWN_TOPIC_OR_PARTITION
    } else {
        topic_error_override
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
        for name in topics {
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
        // broker.host is bounded to i16::MAX at BrokerAdvertise::from_server_config, so the
        // unchecked writer cannot exceed the length prefix - same guarantee the flexible path
        // relies on for its compact string.
        e.write_nullable_string_unchecked(Some(&broker.host));
        e.write_i32(broker.port);
        if response_version >= 1 {
            e.write_nullable_string_unchecked(None); // rack
        }

        if response_version >= 2 {
            e.write_nullable_string_unchecked(None); // cluster_id
        }
        if response_version >= 1 {
            e.write_i32(1); // controller_id - must come before topics array
        }

        e.write_i32(i32::try_from(topics_count).expect("topic count bounded"));
        for name in topics {
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

/// Decodes an `ApiVersions` request body.
///
/// v0–v2 have an empty body. v3+ requires `ClientSoftwareName`, `ClientSoftwareVersion`,
/// and tagged fields (KIP-511 flexible encoding).
fn decode_api_versions_request(api_version: i16, body: &Bytes) -> Result<()> {
    if api_version < 3 {
        if !body.is_empty() {
            return Err(KafkaProtocolError::UnexpectedTrailingBytes);
        }
        return Ok(());
    }
    let mut d = Decoder::new(body.clone());
    let _client_software_name = d.read_compact_string()?;
    let _client_software_version = d.read_compact_string()?;
    d.read_tagged_fields()?;
    if d.remaining() != 0 {
        return Err(KafkaProtocolError::UnexpectedTrailingBytes);
    }
    Ok(())
}

/// Decodes a Metadata request body so the response can echo topic names.
///
/// Every Metadata version requires at least the topics array count on the wire — an empty
/// body is malformed, not "all topics". A null topics array (`-1` legacy / `varint=0`
/// compact) means "all topics" and decodes to an empty list for this stub. Remaining
/// version-gated fields (`AllowAutoTopicCreation`, authorized-ops flags, tagged fields)
/// are consumed so truncated flexible bodies are rejected.
pub(crate) fn decode_metadata_request(api_version: i16, body: Bytes) -> Result<Vec<String>> {
    if body.is_empty() {
        return Err(KafkaProtocolError::BufferUnderflow {
            needed: 1,
            remaining: 0,
        });
    }
    let mut d = Decoder::new(body);
    let flexible = api_version >= 9;
    let topics_count = if flexible {
        // Metadata topics is a nullable compact array ("all topics" when null).
        d.read_compact_array_count_nullable()?
    } else {
        d.read_i32_array_count_nullable()?
    };

    let mut topics = Vec::with_capacity(topics_count.min(PREALLOC_HINT));
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

    // allow_auto_topic_creation (v4+)
    if api_version >= 4 {
        let _allow_auto_topic_creation = d.read_bool()?;
    }
    // include_cluster_authorized_operations (v8–v10; removed in v11)
    if (8..=10).contains(&api_version) {
        let _include_cluster_authorized_operations = d.read_bool()?;
    }
    // include_topic_authorized_operations (v8+)
    if api_version >= 8 {
        let _include_topic_authorized_operations = d.read_bool()?;
    }
    if flexible {
        d.read_tagged_fields()?;
    }
    if d.remaining() != 0 {
        return Err(KafkaProtocolError::UnexpectedTrailingBytes);
    }

    Ok(topics)
}

/// Compatibility alias used by existing unit tests.
#[cfg(test)]
pub(crate) fn decode_metadata_request_topics(body: Bytes, api_version: i16) -> Result<Vec<String>> {
    decode_metadata_request(api_version, body)
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
    fn decode_metadata_request_topics_legacy_null_array_means_all_topics() {
        // -1 is the spec-defined "all topics" sentinel for the legacy i32 array count, not a
        // malformed request - must decode to an empty list, not InvalidArrayLength.
        let body = Bytes::from_static(&[0xff, 0xff, 0xff, 0xff]); // -1
        let topics = decode_metadata_request_topics(body, 0).unwrap();
        assert!(topics.is_empty());
    }

    #[test]
    fn decode_metadata_request_topics_legacy_other_negative_counts_still_fail() {
        // Only -1 is the null sentinel; any other negative count is genuinely malformed.
        let body = Bytes::from_static(&[0xff, 0xff, 0xff, 0xfe]); // -2
        let err = decode_metadata_request_topics(body, 0).unwrap_err();
        assert!(matches!(err, KafkaProtocolError::InvalidArrayLength(-2)));
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
        let mut enc = Encoder::with_capacity(16);
        enc.write_varint(2); // one topic
        enc.write_varint(2); // string len = 1
        enc.write_u8(0xff); // invalid utf-8
        enc.write_empty_tagged_fields(); // per-topic tagged
        enc.write_bool(true); // allow_auto_topic_creation
        enc.write_bool(false); // include_cluster_authorized_operations
        enc.write_bool(false); // include_topic_authorized_operations
        enc.write_empty_tagged_fields(); // top-level tagged
        let err = decode_metadata_request_topics(enc.freeze(), 9).unwrap_err();
        assert!(matches!(err, KafkaProtocolError::InvalidUtf8));
    }

    #[test]
    fn decode_metadata_request_empty_body_is_malformed() {
        let err = decode_metadata_request(0, Bytes::new()).unwrap_err();
        assert!(matches!(err, KafkaProtocolError::BufferUnderflow { .. }));
    }

    #[test]
    fn decode_metadata_request_flexible_truncated_after_topics_fails() {
        // topics = null (all topics) but missing allow_auto / auth flags / tagged fields.
        let body = Bytes::from_static(&[0x00]);
        let err = decode_metadata_request(9, body).unwrap_err();
        assert!(matches!(err, KafkaProtocolError::BufferUnderflow { .. }));
    }

    #[test]
    fn decode_api_versions_v3_requires_software_fields() {
        let err = decode_api_versions_request(3, &Bytes::new()).unwrap_err();
        assert!(matches!(
            err,
            KafkaProtocolError::BufferUnderflow { .. } | KafkaProtocolError::NullCompactString
        ));
    }

    #[test]
    fn decode_api_versions_v3_accepts_valid_body() {
        let mut enc = Encoder::with_capacity(32);
        enc.write_compact_nullable_string(Some("iggy-test"));
        enc.write_compact_nullable_string(Some("0.1.0"));
        enc.write_empty_tagged_fields();
        decode_api_versions_request(3, &enc.freeze()).unwrap();
    }
}
