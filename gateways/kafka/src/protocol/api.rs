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
    decode_create_topics_request, decode_fetch_request, decode_list_offsets_request,
    decode_produce_request,
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

/// Sentinel for `topic_authorized_operations` / `cluster_authorized_operations` when ACLs are not supported.
const AUTHORIZED_OPS_UNKNOWN: i32 = i32::MIN;

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

pub fn handle_request(
    api_key: i16,
    api_version: i16,
    body: Bytes,
    broker: &BrokerAdvertise,
) -> Bytes {
    match api_key {
        API_KEY_API_VERSIONS => {
            if is_supported_version(api_key, api_version) {
                encode_api_versions_response(api_version, ERROR_NONE)
            } else {
                // KIP-511: reply with v0 when the requested version is not understood.
                encode_api_versions_response(0, ERROR_UNSUPPORTED_VERSION)
            }
        }
        API_KEY_METADATA => {
            if is_supported_version(api_key, api_version) {
                encode_metadata_response(api_version, body, broker, ERROR_NONE)
            } else {
                encode_metadata_response(api_version, body, broker, ERROR_UNSUPPORTED_VERSION)
            }
        }
        API_KEY_PRODUCE => {
            if is_supported_version(api_key, api_version) {
                match decode_produce_request(api_version, body) {
                    Ok(req) => encode_produce_response(api_version, &req),
                    Err(e) => {
                        tracing::warn!("Failed to decode Produce request: {:?}", e);
                        encode_produce_error_response(api_version, ERROR_INVALID_REQUEST)
                    }
                }
            } else {
                encode_produce_error_response(api_version, ERROR_UNSUPPORTED_VERSION)
            }
        }
        API_KEY_FETCH => {
            if is_supported_version(api_key, api_version) {
                match decode_fetch_request(api_version, body) {
                    Ok(req) => encode_fetch_response(api_version, &req),
                    Err(e) => {
                        tracing::warn!("Failed to decode Fetch request: {:?}", e);
                        encode_fetch_error_response(api_version, ERROR_INVALID_REQUEST)
                    }
                }
            } else {
                encode_fetch_error_response(api_version, ERROR_UNSUPPORTED_VERSION)
            }
        }
        API_KEY_LIST_OFFSETS => {
            if is_supported_version(api_key, api_version) {
                match decode_list_offsets_request(api_version, body) {
                    Ok(req) => encode_list_offsets_response(api_version, &req),
                    Err(e) => {
                        tracing::warn!("Failed to decode ListOffsets request: {:?}", e);
                        encode_list_offsets_error_response(api_version, ERROR_INVALID_REQUEST)
                    }
                }
            } else {
                encode_list_offsets_error_response(api_version, ERROR_UNSUPPORTED_VERSION)
            }
        }
        API_KEY_CREATE_TOPICS => {
            if is_supported_version(api_key, api_version) {
                match decode_create_topics_request(api_version, body) {
                    Ok(req) => encode_create_topics_response(api_version, &req),
                    Err(e) => {
                        tracing::warn!("Failed to decode CreateTopics request: {:?}", e);
                        encode_create_topics_error_response(api_version, ERROR_INVALID_REQUEST)
                    }
                }
            } else {
                encode_create_topics_error_response(api_version, ERROR_UNSUPPORTED_VERSION)
            }
        }
        _ => encode_error_only_response(ERROR_UNSUPPORTED_VERSION),
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
    api_version: i16,
    body: Bytes,
    broker: &BrokerAdvertise,
    top_level_error_code: i16,
) -> Bytes {
    let flexible = api_version >= 9;
    // BufferUnderflow (empty body) → treat as 0 topics; other decode errors are truly invalid.
    let topics_count = match split_metadata_request_topics(body, api_version) {
        Ok(n) => n,
        Err(KafkaProtocolError::BufferUnderflow { .. }) => 0,
        Err(_) => return encode_error_only_response(ERROR_INVALID_REQUEST),
    };
    let topic_error = if top_level_error_code == ERROR_NONE {
        ERROR_UNKNOWN_TOPIC_OR_PARTITION
    } else {
        top_level_error_code
    };

    let mut e = Encoder::with_capacity(256);

    if api_version >= 3 {
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
        for _ in 0..topics_count {
            e.write_i16(topic_error);
            e.write_compact_nullable_string(Some("unknown-topic"));
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
        if api_version >= 1 {
            e.write_nullable_string_unchecked(None); // rack
        }

        if api_version >= 2 {
            e.write_nullable_string_unchecked(None); // cluster_id
        }
        if api_version >= 1 {
            e.write_i32(1); // controller_id — must come before topics array
        }

        e.write_i32(i32::try_from(topics_count).expect("topic count bounded"));
        for _ in 0..topics_count {
            e.write_i16(topic_error);
            e.write_nullable_string_unchecked(Some("unknown-topic"));
            if api_version >= 1 {
                e.write_bool(false); // is_internal
            }
            e.write_i32(0); // partitions array (empty)
            if api_version >= 8 {
                e.write_i32(AUTHORIZED_OPS_UNKNOWN); // topic_authorized_operations
            }
        }
        if api_version >= 8 {
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

pub(crate) fn split_metadata_request_topics(body: Bytes, api_version: i16) -> Result<usize> {
    let mut d = Decoder::new(body);
    if api_version >= 9 {
        d.read_compact_array_count()
    } else {
        d.read_i32_array_count()
    }
}
