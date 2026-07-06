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

//! Version negotiation firewall — boundary tests for every scoped API key.

#[path = "common/fixtures.rs"]
mod fixtures;
#[path = "common/scope.rs"]
mod scope;
#[path = "common/tcp.rs"]
mod tcp;
#[path = "common/wire.rs"]
mod wire;

use bytes::Bytes;

use iggy_gateway_kafka::protocol::api::{
    API_KEY_API_VERSIONS, API_KEY_CREATE_TOPICS, API_KEY_FETCH, API_KEY_LIST_OFFSETS,
    API_KEY_METADATA, API_KEY_PRODUCE, ERROR_INVALID_REQUEST, ERROR_UNSUPPORTED_VERSION,
    advertised_min_version, handle_request, is_supported_version, supported_api_ranges,
};
use iggy_gateway_kafka::protocol::codec::Decoder;

use fixtures::load_fixture_body;
use scope::{SCOPED_API_KEYS, default_broker};
use tcp::build_metadata_legacy_request;
use wire::build_metadata_flexible_request;

#[test]
fn supported_ranges_table_has_six_entries() {
    assert_eq!(supported_api_ranges().len(), 6);
}

#[test]
fn is_supported_version_matches_scope_table() {
    for &(api_key, _, min_ver, max_ver) in SCOPED_API_KEYS {
        assert!(
            !is_supported_version(api_key, min_ver - 1),
            "key {api_key} must reject v{}",
            min_ver - 1
        );
        assert!(
            is_supported_version(api_key, min_ver),
            "key {api_key} must accept min v{min_ver}"
        );
        assert!(
            is_supported_version(api_key, max_ver),
            "key {api_key} must accept max v{max_ver}"
        );
        assert!(
            !is_supported_version(api_key, max_ver + 1),
            "key {api_key} must reject v{}",
            max_ver + 1
        );
    }
}

#[test]
fn apiversions_advertises_exact_supported_ranges_v1() {
    let body = handle_request(API_KEY_API_VERSIONS, 1, Bytes::new(), &default_broker())
        .expect("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(body);
    assert_eq!(d.read_i16().unwrap(), 0);
    let count = usize::try_from(d.read_i32().unwrap()).expect("api count fits usize");
    assert_eq!(count, supported_api_ranges().len());

    for expected in supported_api_ranges() {
        let key = d.read_i16().unwrap();
        let min = d.read_i16().unwrap();
        let max = d.read_i16().unwrap();
        assert_eq!(key, expected.api_key);
        assert_eq!(
            min,
            advertised_min_version(expected.api_key, expected.min_version)
        );
        assert_eq!(max, expected.max_version);
    }
    assert_eq!(d.read_i32().unwrap(), 0); // throttle
    assert_eq!(d.remaining(), 0);
}

#[test]
fn apiversions_advertises_exact_supported_ranges_v3_flexible() {
    let body = handle_request(API_KEY_API_VERSIONS, 3, Bytes::new(), &default_broker())
        .expect("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(body);
    assert_eq!(d.read_i16().unwrap(), 0);
    let count = usize::try_from(d.read_varint().unwrap() - 1).expect("api count fits usize");
    assert_eq!(count, supported_api_ranges().len());

    for expected in supported_api_ranges() {
        let key = d.read_i16().unwrap();
        let min = d.read_i16().unwrap();
        let max = d.read_i16().unwrap();
        d.read_tagged_fields().unwrap();
        assert_eq!(key, expected.api_key);
        assert_eq!(
            min,
            advertised_min_version(expected.api_key, expected.min_version)
        );
        assert_eq!(max, expected.max_version);
    }
    assert_eq!(d.read_i32().unwrap(), 0);
    d.read_tagged_fields().unwrap();
    assert_eq!(d.remaining(), 0);
}

#[test]
fn apiversions_advertises_produce_min_zero_while_firewall_stays_three() {
    let range = supported_api_ranges()
        .iter()
        .find(|r| r.api_key == API_KEY_PRODUCE)
        .expect("produce range");
    assert_eq!(range.min_version, 3);
    assert_eq!(
        advertised_min_version(API_KEY_PRODUCE, range.min_version),
        0
    );
    assert!(!is_supported_version(API_KEY_PRODUCE, 0));
}

#[test]
fn apiversions_all_versions_return_success() {
    for version in 0i16..=3 {
        let body = handle_request(
            API_KEY_API_VERSIONS,
            version,
            Bytes::new(),
            &default_broker(),
        )
        .expect("test request has acks != 0 and expects a response");
        let mut d = Decoder::new(body);
        assert_eq!(d.read_i16().unwrap(), 0, "ApiVersions v{version}");
    }
}

#[test]
fn apiversions_out_of_range_returns_unsupported_in_body() {
    let body = handle_request(API_KEY_API_VERSIONS, 99, Bytes::new(), &default_broker())
        .expect("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(body);
    assert_eq!(d.read_i16().unwrap(), ERROR_UNSUPPORTED_VERSION);
}

fn metadata_request_one_topic() -> Bytes {
    build_metadata_legacy_request(&["test-topic"])
}

#[test]
fn metadata_below_min_version_returns_topic_error() {
    let body = handle_request(
        API_KEY_METADATA,
        -1,
        metadata_request_one_topic(),
        &default_broker(),
    )
    .expect("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(body);
    let _brokers = d.read_i32().unwrap();
    let _ = d.read_i32().unwrap();
    let _ = d.read_nullable_string().unwrap();
    let _ = d.read_i32().unwrap();
    assert_eq!(d.read_i32().unwrap(), 1); // mirrors request topic count
    assert_eq!(d.read_i16().unwrap(), ERROR_UNSUPPORTED_VERSION);
}

#[test]
fn metadata_above_max_version_returns_topic_error() {
    // v10 request uses flexible encoding; response is clamped to v9.
    let body = handle_request(
        API_KEY_METADATA,
        10,
        build_metadata_flexible_request(&["test-topic"]),
        &default_broker(),
    )
    .expect("test request has acks != 0 and expects a response");
    // Response is in v9 flexible format (highest supported).
    let mut d = Decoder::new(body);
    d.read_i32().unwrap(); // throttle_time_ms (v3+)
    let broker_count = usize::try_from(d.read_varint().unwrap())
        .unwrap()
        .saturating_sub(1);
    for _ in 0..broker_count {
        d.read_i32().unwrap();
        d.read_compact_nullable_string().unwrap();
        d.read_i32().unwrap();
        d.read_compact_nullable_string().unwrap();
        d.read_tagged_fields().unwrap();
    }
    d.read_compact_nullable_string().unwrap(); // cluster_id
    d.read_i32().unwrap(); // controller_id
    let topic_count = usize::try_from(d.read_varint().unwrap())
        .unwrap()
        .saturating_sub(1);
    assert_eq!(topic_count, 1);
    assert_eq!(d.read_i16().unwrap(), ERROR_UNSUPPORTED_VERSION);
}

#[test]
fn produce_unsupported_version_returns_well_formed_error_response() {
    let body = handle_request(API_KEY_PRODUCE, 2, Bytes::new(), &default_broker())
        .expect("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(body);
    assert_eq!(d.read_i32().unwrap(), 1);
    assert_eq!(d.read_nullable_string().unwrap(), Some(String::new()));
    assert_eq!(d.read_i32().unwrap(), 1);
    assert_eq!(d.read_i32().unwrap(), 0);
    assert_eq!(d.read_i16().unwrap(), ERROR_UNSUPPORTED_VERSION);
    let _ = d.read_i64().unwrap();
    let _ = d.read_i64().unwrap();
    assert_eq!(d.read_i32().unwrap(), 0);
    assert_eq!(d.remaining(), 0);
}

#[test]
fn fetch_unsupported_version_returns_well_formed_error_response() {
    let body = handle_request(API_KEY_FETCH, 3, Bytes::new(), &default_broker())
        .expect("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(body);
    assert_eq!(d.read_i32().unwrap(), 0);
    assert_eq!(d.read_i32().unwrap(), 1);
    assert_eq!(d.read_nullable_string().unwrap(), Some(String::new()));
    assert_eq!(d.read_i32().unwrap(), 1);
    assert_eq!(d.read_i32().unwrap(), 0);
    assert_eq!(d.read_i16().unwrap(), ERROR_UNSUPPORTED_VERSION);
    assert_eq!(d.read_i64().unwrap(), 0);
    assert_eq!(d.read_nullable_bytes().unwrap(), None);
    assert_eq!(d.remaining(), 0);
}

#[test]
fn fetch_unsupported_version_above_max_uses_top_level_error() {
    let body = handle_request(API_KEY_FETCH, 13, Bytes::new(), &default_broker())
        .expect("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(body);
    assert_eq!(d.read_i32().unwrap(), 0);
    assert_eq!(d.read_i16().unwrap(), ERROR_UNSUPPORTED_VERSION);
    assert_eq!(d.read_i32().unwrap(), 0);
    assert_eq!(d.read_varint().unwrap(), 1);
    d.read_tagged_fields().unwrap();
    assert_eq!(d.remaining(), 0);
}

#[test]
fn list_offsets_unsupported_version_returns_well_formed_error_response() {
    let body = handle_request(API_KEY_LIST_OFFSETS, 0, Bytes::new(), &default_broker())
        .expect("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(body);
    assert_eq!(d.read_i32().unwrap(), 1);
    assert_eq!(d.read_nullable_string().unwrap(), Some(String::new()));
    assert_eq!(d.read_i32().unwrap(), 1);
    assert_eq!(d.read_i32().unwrap(), 0); // partition index
    assert_eq!(d.read_i16().unwrap(), ERROR_UNSUPPORTED_VERSION);
    assert_eq!(d.read_i32().unwrap(), 0); // old_style_offsets empty array (v0 wire)
    assert_eq!(d.remaining(), 0);
}

#[test]
fn create_topics_unsupported_version_returns_well_formed_error_response() {
    let body = handle_request(API_KEY_CREATE_TOPICS, 1, Bytes::new(), &default_broker())
        .expect("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(body);
    assert_eq!(d.read_i32().unwrap(), 1);
    assert_eq!(d.read_nullable_string().unwrap(), Some(String::new()));
    assert_eq!(d.read_i16().unwrap(), ERROR_UNSUPPORTED_VERSION);
    assert_eq!(d.read_nullable_string().unwrap(), None);
    assert_eq!(d.remaining(), 0);
}

#[test]
fn unsupported_api_keys_return_error_only() {
    for key in [8, 9, 10, 11, 17, 20, 42, 999] {
        let body = handle_request(key, 0, Bytes::new(), &default_broker())
            .expect("test request has acks != 0 and expects a response");
        let mut d = Decoder::new(body);
        assert_eq!(
            d.read_i16().unwrap(),
            ERROR_UNSUPPORTED_VERSION,
            "api_key {key}"
        );
    }
}

#[test]
fn supported_produce_versions_accept_valid_fixture() {
    for version in 3i16..=9 {
        let body = load_fixture_body(0, "Produce", version);
        let resp = handle_request(API_KEY_PRODUCE, version, body, &default_broker())
            .expect("test request has acks != 0 and expects a response");
        assert!(!resp.is_empty(), "Produce v{version} response empty");
    }
}

#[test]
fn supported_fetch_versions_accept_valid_fixture() {
    for version in 4i16..=12 {
        let body = load_fixture_body(1, "Fetch", version);
        let resp = handle_request(API_KEY_FETCH, version, body, &default_broker())
            .expect("test request has acks != 0 and expects a response");
        assert!(!resp.is_empty(), "Fetch v{version} response empty");
    }
}

#[test]
fn corrupt_produce_body_returns_invalid_request_error() {
    let body = Bytes::from_static(&[0xFF, 0xFF, 0xFF]);
    let resp = handle_request(API_KEY_PRODUCE, 3, body, &default_broker())
        .expect("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(resp);
    assert_eq!(d.read_i32().unwrap(), 1);
    assert_eq!(d.read_nullable_string().unwrap(), Some(String::new()));
    assert_eq!(d.read_i32().unwrap(), 1);
    assert_eq!(d.read_i32().unwrap(), 0);
    assert_eq!(d.read_i16().unwrap(), ERROR_INVALID_REQUEST);
}

#[test]
fn corrupt_fetch_body_returns_invalid_request_error() {
    let body = Bytes::from_static(&[0xFF, 0xFF, 0xFF]);
    let resp = handle_request(API_KEY_FETCH, 4, body, &default_broker())
        .expect("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(resp);
    assert_eq!(d.read_i32().unwrap(), 0);
    assert_eq!(d.read_i32().unwrap(), 1);
    assert_eq!(d.read_nullable_string().unwrap(), Some(String::new()));
    assert_eq!(d.read_i32().unwrap(), 1);
    assert_eq!(d.read_i32().unwrap(), 0);
    assert_eq!(d.read_i16().unwrap(), ERROR_INVALID_REQUEST);
}
