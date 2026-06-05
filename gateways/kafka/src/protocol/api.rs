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

use crate::protocol::codec::{Decoder, Encoder};
use crate::protocol::requests::{
    decode_create_topics_request, decode_fetch_request, decode_list_offsets_request,
    decode_produce_request,
};
use crate::protocol::responses::{
    encode_create_topics_response, encode_fetch_response, encode_list_offsets_response,
    encode_produce_response,
};

pub const API_KEY_PRODUCE: i16 = 0;
pub const API_KEY_FETCH: i16 = 1;
pub const API_KEY_LIST_OFFSETS: i16 = 2;
pub const API_KEY_METADATA: i16 = 3;
pub const API_KEY_OFFSET_COMMIT: i16 = 8;
pub const API_KEY_OFFSET_FETCH: i16 = 9;
pub const API_KEY_FIND_COORDINATOR: i16 = 10;
pub const API_KEY_JOIN_GROUP: i16 = 11;
pub const API_KEY_HEARTBEAT: i16 = 12;
pub const API_KEY_LEAVE_GROUP: i16 = 13;
pub const API_KEY_SYNC_GROUP: i16 = 14;
pub const API_KEY_DESCRIBE_GROUPS: i16 = 15;
pub const API_KEY_LIST_GROUPS: i16 = 16;
pub const API_KEY_SASL_HANDSHAKE: i16 = 17;
pub const API_KEY_API_VERSIONS: i16 = 18;
pub const API_KEY_CREATE_TOPICS: i16 = 19;
pub const API_KEY_DELETE_TOPICS: i16 = 20;

pub const ERROR_NONE: i16 = 0;
pub const ERROR_OFFSET_OUT_OF_RANGE: i16 = 1;
pub const ERROR_CORRUPT_MESSAGE: i16 = 2;
pub const ERROR_UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
pub const ERROR_INVALID_FETCH_SIZE: i16 = 4;
pub const ERROR_LEADER_NOT_AVAILABLE: i16 = 5;
pub const ERROR_NOT_LEADER_OR_FOLLOWER: i16 = 6;
pub const ERROR_REQUEST_TIMED_OUT: i16 = 7;
pub const ERROR_UNKNOWN_SERVER_ERROR: i16 = -1;
pub const ERROR_UNSUPPORTED_VERSION: i16 = 35;
pub const ERROR_TOPIC_ALREADY_EXISTS: i16 = 36;
pub const ERROR_INVALID_PARTITIONS: i16 = 37;
pub const ERROR_INVALID_REPLICATION_FACTOR: i16 = 38;
pub const ERROR_INVALID_REQUEST: i16 = 42;
pub const ERROR_UNSUPPORTED_FOR_MESSAGE_FORMAT: i16 = 43;

#[derive(Debug, Clone, Copy)]
pub struct ApiVersionRange {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

pub fn supported_api_ranges() -> Vec<ApiVersionRange> {
    vec![
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
    ]
}

pub fn handle_request(api_key: i16, api_version: i16, body: Bytes) -> Bytes {
    match api_key {
        API_KEY_API_VERSIONS => {
            if is_supported_version(api_key, api_version) {
                encode_api_versions_response(api_version, ERROR_NONE)
            } else {
                encode_api_versions_response(1, ERROR_UNSUPPORTED_VERSION)
            }
        }
        API_KEY_METADATA => {
            if is_supported_version(api_key, api_version) {
                encode_metadata_response(api_version, body, ERROR_NONE)
            } else {
                encode_metadata_response(0, body, ERROR_UNSUPPORTED_VERSION)
            }
        }
        API_KEY_PRODUCE => {
            if is_supported_version(api_key, api_version) {
                match decode_produce_request(api_version, body) {
                    Ok(req) => encode_produce_response(api_version, req),
                    Err(e) => {
                        tracing::error!("Failed to decode Produce request: {:?}", e);
                        encode_error_only_response(ERROR_CORRUPT_MESSAGE)
                    }
                }
            } else {
                encode_error_only_response(ERROR_UNSUPPORTED_VERSION)
            }
        }
        API_KEY_FETCH => {
            if is_supported_version(api_key, api_version) {
                match decode_fetch_request(api_version, body) {
                    Ok(req) => encode_fetch_response(api_version, req),
                    Err(e) => {
                        tracing::error!("Failed to decode Fetch request: {:?}", e);
                        encode_error_only_response(ERROR_CORRUPT_MESSAGE)
                    }
                }
            } else {
                encode_error_only_response(ERROR_UNSUPPORTED_VERSION)
            }
        }
        API_KEY_LIST_OFFSETS => {
            if is_supported_version(api_key, api_version) {
                match decode_list_offsets_request(api_version, body) {
                    Ok(req) => encode_list_offsets_response(api_version, req),
                    Err(e) => {
                        tracing::error!("Failed to decode ListOffsets request: {:?}", e);
                        encode_error_only_response(ERROR_CORRUPT_MESSAGE)
                    }
                }
            } else {
                encode_error_only_response(ERROR_UNSUPPORTED_VERSION)
            }
        }
        API_KEY_CREATE_TOPICS => {
            if is_supported_version(api_key, api_version) {
                match decode_create_topics_request(api_version, body) {
                    Ok(req) => encode_create_topics_response(api_version, req),
                    Err(e) => {
                        tracing::error!("Failed to decode CreateTopics request: {:?}", e);
                        encode_error_only_response(ERROR_CORRUPT_MESSAGE)
                    }
                }
            } else {
                encode_error_only_response(ERROR_UNSUPPORTED_VERSION)
            }
        }
        _ => encode_error_only_response(ERROR_UNSUPPORTED_VERSION),
    }
}

pub fn is_supported_version(api_key: i16, api_version: i16) -> bool {
    supported_api_ranges()
        .into_iter()
        .find(|r| r.api_key == api_key)
        .is_some_and(|r| api_version >= r.min_version && api_version <= r.max_version)
}

fn encode_api_versions_response(api_version: i16, error_code: i16) -> Bytes {
    let flexible = api_version >= 3;
    let ranges = supported_api_ranges();
    let mut e = Encoder::with_capacity(128);

    e.write_i16(error_code);

    if flexible {
        e.write_varint((ranges.len() + 1) as u64);
        for r in &ranges {
            e.write_i16(r.api_key);
            e.write_i16(r.min_version);
            e.write_i16(r.max_version);
            e.write_empty_tagged_fields();
        }
    } else {
        e.write_i32(ranges.len() as i32);
        for r in &ranges {
            e.write_i16(r.api_key);
            e.write_i16(r.min_version);
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

fn encode_metadata_response(_api_version: i16, body: Bytes, top_level_error_code: i16) -> Bytes {
    let mut e = Encoder::with_capacity(256);

    e.write_i32(1);
    e.write_i32(1);
    e.write_nullable_string(Some("127.0.0.1"));
    e.write_i32(9093);

    let topics_count = split_metadata_request_topics(body);
    e.write_i32(topics_count as i32);
    for _ in 0..topics_count {
        e.write_i16(if top_level_error_code == ERROR_NONE {
            ERROR_UNKNOWN_TOPIC_OR_PARTITION
        } else {
            top_level_error_code
        });
        e.write_nullable_string(Some("unknown-topic"));
        e.write_i32(0);
    }

    e.write_i32(1);
    e.freeze()
}

fn encode_error_only_response(error_code: i16) -> Bytes {
    let mut e = Encoder::with_capacity(2);
    e.write_i16(error_code);
    e.freeze()
}

pub fn split_metadata_request_topics(body: Bytes) -> usize {
    let mut d = Decoder::new(body);
    d.read_i32().unwrap_or_default().max(0) as usize
}
