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

#[path = "common/wire.rs"]
mod wire;

use bytes::Bytes;

use iggy_gateway_kafka::protocol::api::{
    API_KEY_API_VERSIONS, API_KEY_CREATE_TOPICS, API_KEY_FETCH, API_KEY_LIST_OFFSETS,
    API_KEY_METADATA, API_KEY_PRODUCE, BrokerAdvertise, ERROR_INVALID_REQUEST,
    ERROR_UNSUPPORTED_VERSION, handle_request, is_supported_version, supported_api_ranges,
};

fn test_broker() -> BrokerAdvertise {
    BrokerAdvertise::default()
}
use iggy_gateway_kafka::protocol::codec::Decoder;
use wire::build_metadata_flexible_request_v10;

// ── ApiVersions ─────────────────────────────────────────────────────────────

#[test]
fn api_versions_v1_response_non_flexible_format() {
    let body = handle_request(API_KEY_API_VERSIONS, 1, Bytes::new(), &test_broker())
        .expect("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(body);

    assert_eq!(d.read_i16().unwrap(), 0); // error_code

    // Non-flexible: i32 array count
    let count = d.read_i32().unwrap();
    assert!(count >= 2);
    let mut keys = Vec::new();
    for _ in 0..count {
        keys.push(d.read_i16().unwrap());
        d.read_i16().unwrap(); // min
        d.read_i16().unwrap(); // max
    }
    assert_eq!(d.read_i32().unwrap(), 0); // throttle_time_ms

    let expected_keys: Vec<i16> = supported_api_ranges().iter().map(|r| r.api_key).collect();
    for k in expected_keys {
        assert!(keys.contains(&k));
    }
}

#[test]
fn api_versions_v3_response_flexible_format() {
    let body = handle_request(API_KEY_API_VERSIONS, 3, Bytes::new(), &test_broker())
        .expect("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(body);

    assert_eq!(d.read_i16().unwrap(), 0); // error_code

    // Flexible: varint(len+1) compact array
    let count_plus_one = d.read_varint().unwrap();
    assert!(count_plus_one >= 3); // at least 2 entries → varint = 3+
    let count = i32::try_from(count_plus_one - 1).expect("api count fits i32");

    let mut keys = Vec::new();
    for _ in 0..count {
        keys.push(d.read_i16().unwrap());
        d.read_i16().unwrap(); // min
        d.read_i16().unwrap(); // max
        d.read_tagged_fields().unwrap(); // per-entry tagged fields
    }
    assert_eq!(d.read_i32().unwrap(), 0); // throttle_time_ms
    d.read_tagged_fields().unwrap(); // top-level tagged fields

    let expected_keys: Vec<i16> = supported_api_ranges().iter().map(|r| r.api_key).collect();
    for k in expected_keys {
        assert!(keys.contains(&k));
    }
}

// ── Metadata ─────────────────────────────────────────────────────────────────

#[test]
fn metadata_response_has_broker_array_and_topic_array() {
    let body = handle_request(API_KEY_METADATA, 0, Bytes::new(), &test_broker())
        .expect("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(body);

    let broker_count = d.read_i32().unwrap();
    assert_eq!(broker_count, 1);
    let node_id = d.read_i32().unwrap();
    assert_eq!(node_id, 1);
    let host = d.read_nullable_string().unwrap().unwrap();
    assert_eq!(host, "127.0.0.1");
    let port = d.read_i32().unwrap();
    assert_eq!(port, 9093);

    let topic_count = d.read_i32().unwrap();
    assert_eq!(topic_count, 0);
}

#[test]
fn unsupported_version_returns_protocol_error() {
    // v99 client sends a well-formed v10+ flexible body (topic_id + name) requesting "orders".
    // Response encodes at v9 (highest supported); request decodes at client version 99.
    let body = handle_request(
        API_KEY_METADATA,
        99,
        build_metadata_flexible_request_v10(&["orders"]),
        &test_broker(),
    )
    .expect("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(body);
    // v9 flexible response layout:
    d.read_i32().unwrap(); // throttle_time_ms (v3+)
    let broker_count = usize::try_from(d.read_varint().unwrap())
        .unwrap()
        .saturating_sub(1);
    for _ in 0..broker_count {
        d.read_i32().unwrap(); // node_id
        d.read_compact_nullable_string().unwrap(); // host
        d.read_i32().unwrap(); // port
        d.read_compact_nullable_string().unwrap(); // rack
        d.read_tagged_fields().unwrap();
    }
    d.read_compact_nullable_string().unwrap(); // cluster_id (v2+)
    d.read_i32().unwrap(); // controller_id (v1+)
    let topic_count = usize::try_from(d.read_varint().unwrap())
        .unwrap()
        .saturating_sub(1);
    assert_eq!(topic_count, 1);
    let topic_error = d.read_i16().unwrap();
    assert_eq!(topic_error, ERROR_UNSUPPORTED_VERSION);
    let topic_name = d.read_compact_nullable_string().unwrap();
    assert_eq!(
        topic_name,
        Some("orders".to_string()),
        "metadata must echo the requested topic name even on an unsupported-version reply"
    );
}

// ── Misc ────────────────────────────────────────────────────────────────────

#[test]
fn unknown_api_key_returns_error_only_payload() {
    let body = handle_request(999, 0, Bytes::new(), &test_broker())
        .expect("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(body);
    assert_eq!(d.read_i16().unwrap(), ERROR_UNSUPPORTED_VERSION);
}

#[test]
fn version_support_table_is_applied() {
    assert!(is_supported_version(API_KEY_API_VERSIONS, 3));
    assert!(!is_supported_version(API_KEY_API_VERSIONS, 10));
    assert!(is_supported_version(API_KEY_METADATA, 1));
    assert!(!is_supported_version(API_KEY_METADATA, -1));
}

#[test]
fn apiversions_unsupported_version_uses_v0_encoding_without_throttle() {
    let body = handle_request(API_KEY_API_VERSIONS, 99, Bytes::new(), &test_broker())
        .expect("test request has acks != 0 and expects a response");
    // v0: error_code(2) + api_keys i32 count(4) + 6 entries × 6 bytes = 42 — no throttle_time_ms.
    assert_eq!(body.len(), 42);
    let mut d = Decoder::new(body);
    assert_eq!(d.read_i16().unwrap(), ERROR_UNSUPPORTED_VERSION);
    assert_eq!(d.read_i32().unwrap(), 6);
    assert_eq!(d.remaining(), 36);
}

#[test]
fn produce_malformed_body_with_acks_one_returns_invalid_request() {
    let body = Bytes::from_static(&[
        0xff, 0xff, // null transactional_id
        0x00, 0x01, // acks = 1
        0x00, 0x00, 0x03, 0xe8, // timeout_ms
        0x00, 0x00, 0x00, 0x01, // one topic
    ]);
    let response = handle_request(API_KEY_PRODUCE, 3, body, &test_broker())
        .expect("acks=1 malformed produce should get error response");
    let mut d = Decoder::new(response);
    assert_eq!(d.read_i32().unwrap(), 1);
    assert_eq!(d.read_nullable_string().unwrap(), Some(String::new()));
    assert_eq!(d.read_i32().unwrap(), 1);
    assert_eq!(d.read_i32().unwrap(), 0);
    assert_eq!(d.read_i16().unwrap(), ERROR_INVALID_REQUEST);
}

#[test]
fn fetch_malformed_body_returns_invalid_request() {
    let response = handle_request(
        API_KEY_FETCH,
        12,
        Bytes::from_static(&[
            0xff, 0xff, 0xff, 0xff, // replica_id
            0x00, 0x00, 0x00, 0x64, // max_wait_ms
            0x00, 0x00, 0x00, 0x01, // min_bytes
        ]),
        &test_broker(),
    )
    .expect("fetch must return error response");
    let mut d = Decoder::new(response);
    assert_eq!(d.read_i32().unwrap(), 0); // throttle
    assert_eq!(d.read_i16().unwrap(), ERROR_INVALID_REQUEST);
}

#[test]
fn list_offsets_malformed_body_returns_invalid_request() {
    let response = handle_request(
        API_KEY_LIST_OFFSETS,
        6,
        Bytes::from_static(&[
            0xff, 0xff, 0xff, 0xff, // replica_id
            0x01, // isolation_level
            0x02, // compact topics count = one
            0x00, // null topic name
        ]),
        &test_broker(),
    )
    .expect("list offsets must return error response");
    let mut d = Decoder::new(response);
    assert_eq!(d.read_i32().unwrap(), 0); // throttle
    assert_eq!(d.read_varint().unwrap(), 2);
    assert_eq!(
        d.read_compact_nullable_string().unwrap(),
        Some(String::new())
    );
    assert_eq!(d.read_varint().unwrap(), 2);
    assert_eq!(d.read_i32().unwrap(), 0);
    assert_eq!(d.read_i16().unwrap(), ERROR_INVALID_REQUEST);
}

#[test]
fn create_topics_malformed_body_returns_invalid_request() {
    let response = handle_request(
        API_KEY_CREATE_TOPICS,
        5,
        Bytes::from_static(&[
            0x02, // compact topics count = one
            0x00, // null compact topic name
        ]),
        &test_broker(),
    )
    .expect("create topics must return error response");
    let mut d = Decoder::new(response);
    assert_eq!(d.read_i32().unwrap(), 0);
    assert_eq!(d.read_varint().unwrap(), 2);
    assert_eq!(
        d.read_compact_nullable_string().unwrap(),
        Some(String::new())
    );
    assert_eq!(d.read_i16().unwrap(), ERROR_INVALID_REQUEST);
}

#[test]
fn metadata_null_topic_name_yields_zero_topics() {
    let body = handle_request(
        API_KEY_METADATA,
        0,
        Bytes::from_static(&[
            0x00, 0x00, 0x00, 0x01, // one topic
            0xff, 0xff, // null topic name
        ]),
        &test_broker(),
    )
    .expect("metadata request should still return response");
    let mut d = Decoder::new(body);
    assert_eq!(d.read_i32().unwrap(), 1);
    d.read_i32().unwrap();
    d.read_nullable_string().unwrap();
    d.read_i32().unwrap();
    assert_eq!(d.read_i32().unwrap(), 0);
    assert_eq!(d.remaining(), 0);
}
