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

use iggy_gateway_kafka::protocol::api::{
    API_KEY_API_VERSIONS, API_KEY_METADATA, BrokerAdvertise, ERROR_UNSUPPORTED_VERSION,
    handle_request, is_supported_version, split_metadata_request_topics, supported_api_ranges,
};

fn test_broker() -> BrokerAdvertise {
    BrokerAdvertise::default()
}
use iggy_gateway_kafka::protocol::codec::Decoder;

// ── ApiVersions ─────────────────────────────────────────────────────────────

#[test]
fn api_versions_v1_response_non_flexible_format() {
    let body = handle_request(API_KEY_API_VERSIONS, 1, Bytes::new(), &test_broker());
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
    let body = handle_request(API_KEY_API_VERSIONS, 3, Bytes::new(), &test_broker());
    let mut d = Decoder::new(body);

    assert_eq!(d.read_i16().unwrap(), 0); // error_code

    // Flexible: varint(len+1) compact array
    let count_plus_one = d.read_varint().unwrap();
    assert!(count_plus_one >= 3); // at least 2 entries → varint = 3+
    let count = (count_plus_one - 1) as i32;

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
    let body = handle_request(API_KEY_METADATA, 0, Bytes::new(), &test_broker());
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
    let mut req = Vec::new();
    req.extend_from_slice(&1_i32.to_be_bytes());
    let body = handle_request(API_KEY_METADATA, 99, Bytes::from(req), &test_broker());
    let mut d = Decoder::new(body);
    // Metadata v0: brokers[], topics[] — no controller_id (added in v1)
    let _broker_count = d.read_i32().unwrap();
    let _ = d.read_i32().unwrap(); // node_id
    let _ = d.read_nullable_string().unwrap(); // host
    let _ = d.read_i32().unwrap(); // port
    let topic_count = d.read_i32().unwrap();
    assert_eq!(topic_count, 1);
    let topic_error = d.read_i16().unwrap();
    assert_eq!(topic_error, ERROR_UNSUPPORTED_VERSION);
    let topic_name = d.read_nullable_string().unwrap().unwrap();
    assert_eq!(topic_name, "unknown-topic");
    let partitions_count = d.read_i32().unwrap();
    assert_eq!(partitions_count, 0);
}

// ── Misc ────────────────────────────────────────────────────────────────────

#[test]
fn unknown_api_key_returns_error_only_payload() {
    let body = handle_request(999, 0, Bytes::new(), &test_broker());
    let mut d = Decoder::new(body);
    assert_eq!(d.read_i16().unwrap(), ERROR_UNSUPPORTED_VERSION);
}

#[test]
fn metadata_topic_split_reads_array_count() {
    let mut raw = Vec::new();
    raw.extend_from_slice(&2_i32.to_be_bytes());
    assert_eq!(split_metadata_request_topics(Bytes::from(raw), 0), 2);
}

#[test]
fn version_support_table_is_applied() {
    assert!(is_supported_version(API_KEY_API_VERSIONS, 3));
    assert!(!is_supported_version(API_KEY_API_VERSIONS, 10));
    assert!(is_supported_version(API_KEY_METADATA, 1));
    assert!(!is_supported_version(API_KEY_METADATA, -1));
}
