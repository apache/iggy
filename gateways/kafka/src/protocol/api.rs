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

use bytes::{Buf, Bytes};
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::metadata_response::{MetadataResponseBroker, MetadataResponseTopic};
use kafka_protocol::messages::{
    ApiVersionsRequest, ApiVersionsResponse, BrokerId, CreateTopicsRequest, FetchRequest,
    ListOffsetsRequest, MetadataRequest, MetadataResponse, ProduceRequest, TopicName,
};
use kafka_protocol::protocol::{Decodable, StrBytes};

use crate::error::{KafkaProtocolError, Result};
use crate::protocol::responses::{
    encode_create_topics_error_response, encode_create_topics_response,
    encode_fetch_error_response, encode_fetch_response, encode_list_offsets_error_response,
    encode_list_offsets_response, encode_message, encode_produce_error_response,
    encode_produce_response,
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

/// Decode `T` from the whole request body and reject unconsumed trailing bytes.
///
/// `kafka_protocol`'s `Decodable` stops once it has read the fields its schema defines; it does
/// not know (or care) whether the caller handed it an exact-length body, so the trailing-bytes
/// check has to live here.
fn decode_exhaustive<T: Decodable>(version: i16, mut body: Bytes) -> Result<T> {
    let value =
        T::decode(&mut body, version).map_err(|e| KafkaProtocolError::Malformed(e.to_string()))?;
    if body.has_remaining() {
        return Err(KafkaProtocolError::Malformed(
            "unexpected trailing bytes in request body".to_string(),
        ));
    }
    Ok(value)
}

/// Turn an encode [`Result`] into a [`HandleOutcome`], closing the connection when encoding
/// fails rather than propagating - there is no parseable response to send in that case.
fn respond_or_close(result: Result<Bytes>, api_name: &str) -> HandleOutcome {
    match result {
        Ok(body) => HandleOutcome::Respond(body),
        Err(error) => {
            tracing::warn!(%error, "failed to encode {api_name} response; closing connection");
            HandleOutcome::Close
        }
    }
}

/// Produce is the only request the wire protocol allows to go unanswered
/// (`acks=0`), so it gets its own path that may return [`HandleOutcome::NoResponse`].
///
/// The firewall check runs AFTER decoding the request, not before: `ApiVersions` advertises
/// Produce min=0 (see [`advertised_min_version`]) while the firewall's real floor is 3, so a
/// spec-compliant client can legitimately send Produce v0-2 with `acks=0`. Rejecting those
/// versions before reading `acks` would send an error response the client never expects,
/// desyncing the next correlation id it reads.
fn handle_produce_request(api_version: i16, body: Bytes) -> HandleOutcome {
    // Above the encoder max there is no response parseable at the client's version, so close
    // rather than decode (same policy the other APIs apply).
    if api_version > supported_max_version(API_KEY_PRODUCE).unwrap_or(i16::MAX) {
        return HandleOutcome::Close;
    }
    // `kafka_protocol`'s ProduceRequest/ProduceResponse schemas only go back to v3, so v0-2
    // (still advertised as the min in ApiVersions per KAFKA-18659) can be neither decoded nor
    // encoded by the crate - there is no parseable response at these versions regardless of
    // body content. `acks` is always the first i16 on the wire there (`transactional_id` was
    // added in v3), so it's peeked by hand: acks=0 must keep the connection open per the wire
    // protocol's fire-and-forget rule even though no response can ever be encoded for it.
    if api_version < 3 {
        let acks = match body.get(0..2) {
            Some(&[hi, lo]) => Some(i16::from_be_bytes([hi, lo])),
            _ => None,
        };
        return match acks {
            Some(0) | None => HandleOutcome::NoResponse,
            Some(_) => unsupported_version_response(API_KEY_PRODUCE, api_version, |v| {
                encode_produce_error_response(v, ERROR_UNSUPPORTED_VERSION)
            }),
        };
    }
    match decode_exhaustive::<ProduceRequest>(api_version, body) {
        // acks=0 is fire-and-forget: the client isn't reading a response, so
        // sending one desyncs the next correlation id it expects.
        Ok(req) if req.acks == 0 => HandleOutcome::NoResponse,
        Ok(req) => {
            if !is_supported_version(API_KEY_PRODUCE, api_version) {
                return unsupported_version_response(API_KEY_PRODUCE, api_version, |v| {
                    encode_produce_error_response(v, ERROR_UNSUPPORTED_VERSION)
                });
            }
            respond_or_close(encode_produce_response(api_version, &req), "Produce")
        }
        Err(error) => {
            // `kafka_protocol` decodes the whole request in one shot; a failure anywhere gives
            // no partial-field access, so `acks` is unknowable here (unlike the pre-migration
            // field-by-field decoder, which could still know `acks` on a later-field failure).
            // Responding risks desyncing an acks=0 fire-and-forget client's correlation stream,
            // so every Produce decode failure now stays silent - a behavior change from the
            // hand-rolled decoder, which answered with INVALID_REQUEST when `acks` was known and
            // nonzero.
            tracing::warn!(%error, "failed to decode Produce request (no response)");
            HandleOutcome::NoResponse
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
        API_KEY_API_VERSIONS => handle_api_versions(api_version, body),
        API_KEY_METADATA => handle_metadata(api_version, body, broker),
        API_KEY_FETCH => handle_versioned_request(
            API_KEY_FETCH,
            api_version,
            body,
            decode_exhaustive::<FetchRequest>,
            encode_fetch_response,
            encode_fetch_error_response,
            "Fetch",
        ),
        API_KEY_LIST_OFFSETS => handle_versioned_request(
            API_KEY_LIST_OFFSETS,
            api_version,
            body,
            decode_exhaustive::<ListOffsetsRequest>,
            encode_list_offsets_response,
            encode_list_offsets_error_response,
            "ListOffsets",
        ),
        API_KEY_CREATE_TOPICS => handle_versioned_request(
            API_KEY_CREATE_TOPICS,
            api_version,
            body,
            decode_exhaustive::<CreateTopicsRequest>,
            encode_create_topics_response,
            encode_create_topics_error_response,
            "CreateTopics",
        ),
        // Unknown API key: no api-specific response schema exists, so any body we send is
        // misparsed by the client against the schema it expected. Close is unambiguous.
        _ => HandleOutcome::Close,
    }
}

fn handle_api_versions(api_version: i16, body: Bytes) -> HandleOutcome {
    if !is_supported_version(API_KEY_API_VERSIONS, api_version) {
        // KIP-511: reply with v0 when the requested version is not understood.
        return respond_or_close(
            encode_api_versions_response(0, ERROR_UNSUPPORTED_VERSION),
            "ApiVersions",
        );
    }
    match decode_exhaustive::<ApiVersionsRequest>(api_version, body) {
        Ok(_) => respond_or_close(
            encode_api_versions_response(api_version, ERROR_NONE),
            "ApiVersions",
        ),
        Err(error) => {
            tracing::warn!(%error, "failed to decode ApiVersions request");
            respond_or_close(
                encode_api_versions_response(api_version, ERROR_INVALID_REQUEST),
                "ApiVersions",
            )
        }
    }
}

fn handle_metadata(api_version: i16, body: Bytes, broker: &BrokerAdvertise) -> HandleOutcome {
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
    match decode_metadata_topics(api_version, body) {
        Ok(topics) => respond_or_close(
            encode_metadata_response(api_version, &topics, broker, ERROR_NONE),
            "Metadata",
        ),
        Err(error) => {
            // Metadata has no top-level error field; a malformed body cannot carry
            // INVALID_REQUEST in a version-correct way for every client. Close.
            tracing::warn!(
                %error,
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
    encode_ok: impl FnOnce(i16, &T) -> Result<Bytes>,
    encode_err: impl Fn(i16, i16) -> Result<Bytes>,
    api_name: &str,
) -> HandleOutcome {
    if is_supported_version(api_key, api_version) {
        match decode(api_version, body) {
            Ok(req) => respond_or_close(encode_ok(api_version, &req), api_name),
            Err(error) => {
                tracing::warn!(%error, "Failed to decode {api_name} request");
                respond_or_close(encode_err(api_version, ERROR_INVALID_REQUEST), api_name)
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
/// - `api_version > max`: Close. `SUPPORTED_RANGES` is the governance boundary, not just an
///   encoding-capability limit - `kafka_protocol` can often encode versions above our firewall
///   max just fine, but responding there would silently widen what this gateway accepts.
/// - `api_version < min`: Respond with an error shaped for that version when `kafka_protocol`
///   can encode it, otherwise `encode` fails and [`respond_or_close`] closes instead. In
///   practice every `SUPPORTED_RANGES` min was chosen at or above the oldest version
///   `kafka_protocol` implements for that message, so this always closes today (e.g.
///   `ListOffsets` v0's legacy `old_style_offsets` shape predates the crate's schema) - kept
///   generic rather than hard-coded so a future `kafka_protocol` upgrade that widens a schema's
///   floor is picked up automatically instead of silently staying on `Close`.
fn unsupported_version_response(
    api_key: i16,
    api_version: i16,
    encode: impl FnOnce(i16) -> Result<Bytes>,
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
    respond_or_close(encode(api_version), "unsupported-version")
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

fn encode_api_versions_response(api_version: i16, error_code: i16) -> Result<Bytes> {
    let api_keys = SUPPORTED_RANGES
        .iter()
        .map(|r| {
            ApiVersion::default()
                .with_api_key(r.api_key)
                .with_min_version(advertised_min_version(r.api_key, r.min_version))
                .with_max_version(r.max_version)
        })
        .collect();
    let resp = ApiVersionsResponse::default()
        .with_error_code(error_code)
        .with_api_keys(api_keys);
    encode_message(&resp, api_version, 128)
}

fn encode_metadata_response(
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
fn decode_metadata_topics(api_version: i16, body: Bytes) -> Result<Vec<StrBytes>> {
    let req = decode_exhaustive::<MetadataRequest>(api_version, body)?;
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

    #[test]
    fn decode_metadata_topics_legacy_null_topic_name_fails() {
        let body = Bytes::from_static(&[
            0x00, 0x00, 0x00, 0x01, // one topic
            0xff, 0xff, // null topic name
        ]);
        let err = decode_metadata_topics(0, body).unwrap_err();
        assert!(matches!(err, KafkaProtocolError::NullTopicName));
    }

    #[test]
    fn decode_metadata_topics_legacy_null_array_means_all_topics() {
        // -1 is the spec-defined "all topics" sentinel for the legacy i32 array count, not a
        // malformed request - must decode to an empty list.
        let body = Bytes::from_static(&[0xff, 0xff, 0xff, 0xff]); // -1
        let topics = decode_metadata_topics(0, body).unwrap();
        assert!(topics.is_empty());
    }

    #[test]
    fn decode_metadata_topics_empty_body_is_malformed() {
        assert!(decode_metadata_topics(0, Bytes::new()).is_err());
    }

    #[test]
    fn decode_metadata_topics_flexible_truncated_after_topics_fails() {
        // topics = null (all topics) but missing allow_auto / auth flags / tagged fields.
        let body = Bytes::from_static(&[0x00]);
        assert!(decode_metadata_topics(9, body).is_err());
    }

    #[test]
    fn decode_api_versions_v3_requires_software_fields() {
        assert!(decode_exhaustive::<ApiVersionsRequest>(3, Bytes::new()).is_err());
    }

    #[test]
    fn decode_api_versions_v3_accepts_valid_body() {
        // Hand-encoded rather than round-tripped through `ApiVersionsRequest::encode`: encoding
        // is gated behind the crate's "client" feature, which this broker-only binary doesn't
        // enable.
        let body = Bytes::from_static(&[
            0x0a, b'i', b'g', b'g', b'y', b'-', b't', b'e', b's',
            b't', // compact string (len 9)
            0x06, b'0', b'.', b'1', b'.', b'0', // compact string (len 5)
            0x00, // empty tagged fields
        ]);
        decode_exhaustive::<ApiVersionsRequest>(3, body).unwrap();
    }
}
