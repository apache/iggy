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

//! Version negotiation firewall - boundary tests for every scoped API key.

#[path = "common/fixtures.rs"]
mod fixtures;
#[path = "common/scope.rs"]
mod scope;
#[path = "common/server.rs"]
mod server;
#[path = "common/tcp.rs"]
mod tcp;
#[path = "common/wire.rs"]
mod wire;

use std::time::Duration;

use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use iggy_gateway_kafka::protocol::api::{
    API_KEY_API_VERSIONS, API_KEY_CREATE_TOPICS, API_KEY_FETCH, API_KEY_LIST_OFFSETS,
    API_KEY_METADATA, API_KEY_PRODUCE, ERROR_INVALID_REQUEST, ERROR_NONE,
    ERROR_UNSUPPORTED_VERSION, advertised_min_version, handle_request, is_supported_version,
    supported_api_ranges,
};
use iggy_gateway_kafka::protocol::codec::Decoder;

use fixtures::{fixture_exists, load_fixture_body, load_fixture_body_or_skip};
use scope::{SCOPED_API_KEYS, default_broker};
use server::spawn_test_server;
use tcp::{
    ByteRead, build_list_offsets_v0_request_with_topic_t, build_metadata_legacy_request,
    build_produce_v3_body, build_request_frame, parse_response_payload, read_byte_with_timeout,
    round_trip, scan_for_error_code,
};
use wire::{
    OUT_OF_SCOPE_API_KEYS, build_api_versions_flexible_request, build_create_topics_empty_request,
    build_fetch_empty_topics_request, build_list_offsets_request,
    build_metadata_all_topics_flexible, build_metadata_all_topics_legacy,
    build_metadata_flexible_request_v10,
};

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
        .expect_response("test request has acks != 0 and expects a response");
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
    let body = handle_request(
        API_KEY_API_VERSIONS,
        3,
        build_api_versions_flexible_request("iggy-test", "0.1.0"),
        &default_broker(),
    )
    .expect_response("test request has acks != 0 and expects a response");
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
        let request = if version >= 3 {
            build_api_versions_flexible_request("iggy-test", "0.1.0")
        } else {
            Bytes::new()
        };
        let body = handle_request(API_KEY_API_VERSIONS, version, request, &default_broker())
            .expect_response("test request has acks != 0 and expects a response");
        let mut d = Decoder::new(body);
        assert_eq!(d.read_i16().unwrap(), 0, "ApiVersions v{version}");
    }
}

#[test]
fn apiversions_out_of_range_returns_unsupported_in_body() {
    let body = handle_request(API_KEY_API_VERSIONS, 99, Bytes::new(), &default_broker())
        .expect_response("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(body);
    assert_eq!(d.read_i16().unwrap(), ERROR_UNSUPPORTED_VERSION);
}

fn metadata_request_one_topic() -> Bytes {
    build_metadata_legacy_request(&["test-topic"])
}

#[test]
fn metadata_below_min_version_closes_connection() {
    assert!(
        handle_request(
            API_KEY_METADATA,
            -1,
            metadata_request_one_topic(),
            &default_broker(),
        )
        .is_close(),
        "Metadata below supported min must close rather than return a clamped body"
    );
}

#[test]
fn metadata_above_max_version_closes_connection() {
    // v10 request uses flexible encoding; a clamped v9 reply would not survive client parsing.
    assert!(
        handle_request(
            API_KEY_METADATA,
            10,
            build_metadata_flexible_request_v10(&["test-topic"]),
            &default_broker(),
        )
        .is_close(),
        "Metadata above supported max must close rather than return a clamped body"
    );
}

#[tokio::test]
async fn e2e_metadata_above_max_version_closes_tcp_connection() {
    let (addr, _shutdown) = spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    let body = build_metadata_flexible_request_v10(&["orders"]);
    let frame = build_request_frame(API_KEY_METADATA, 10, 44, Some("n9-test"), &body);
    stream.write_all(&frame).await.expect("write metadata v10");

    assert_eq!(
        read_byte_with_timeout(&mut stream, Duration::from_secs(2)).await,
        ByteRead::Closed,
        "unsupported Metadata version must close the connection"
    );
}

#[test]
fn produce_unsupported_version_returns_well_formed_error_response() {
    let body = handle_request(API_KEY_PRODUCE, 2, Bytes::new(), &default_broker())
        .expect_response("test request has acks != 0 and expects a response");
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
        .expect_response("test request has acks != 0 and expects a response");
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
fn fetch_unsupported_version_above_max_closes_connection() {
    // Fetch v13+ response shape differs from the v12 encoder; a clamped body is unparsable.
    assert!(
        handle_request(API_KEY_FETCH, 13, Bytes::new(), &default_broker()).is_close(),
        "Fetch above encoder max must close rather than return a clamped body"
    );
}

#[test]
fn produce_unsupported_version_above_max_closes_connection() {
    assert!(
        handle_request(API_KEY_PRODUCE, 13, Bytes::new(), &default_broker()).is_close(),
        "Produce above encoder max must close rather than return a clamped body"
    );
}

#[test]
fn create_topics_unsupported_version_above_max_closes_connection() {
    assert!(
        handle_request(API_KEY_CREATE_TOPICS, 7, Bytes::new(), &default_broker()).is_close(),
        "CreateTopics above encoder max must close rather than return a clamped body"
    );
}

#[test]
fn list_offsets_unsupported_version_above_max_closes_connection() {
    assert!(
        handle_request(API_KEY_LIST_OFFSETS, 7, Bytes::new(), &default_broker()).is_close(),
        "ListOffsets above encoder max must close rather than return a clamped body"
    );
}

#[test]
fn list_offsets_unsupported_version_returns_well_formed_error_response() {
    let body = handle_request(API_KEY_LIST_OFFSETS, 0, Bytes::new(), &default_broker())
        .expect_response("test request has acks != 0 and expects a response");
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
        .expect_response("test request has acks != 0 and expects a response");
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
            .expect_response("test request has acks != 0 and expects a response");
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
        let Some(body) = load_fixture_body_or_skip(0, "Produce", version) else {
            continue;
        };
        let resp = handle_request(API_KEY_PRODUCE, version, body, &default_broker())
            .expect_response("test request has acks != 0 and expects a response");
        assert!(!resp.is_empty(), "Produce v{version} response empty");
    }
}

#[test]
fn supported_fetch_versions_accept_valid_fixture() {
    for version in 4i16..=12 {
        let Some(body) = load_fixture_body_or_skip(1, "Fetch", version) else {
            continue;
        };
        let resp = handle_request(API_KEY_FETCH, version, body, &default_broker())
            .expect_response("test request has acks != 0 and expects a response");
        assert!(!resp.is_empty(), "Fetch v{version} response empty");
    }
}

#[test]
fn corrupt_produce_body_returns_invalid_request_error() {
    let body = Bytes::from_static(&[0xFF, 0xFF, 0xFF]);
    let resp = handle_request(API_KEY_PRODUCE, 3, body, &default_broker())
        .expect_response("test request has acks != 0 and expects a response");
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
        .expect_response("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(resp);
    assert_eq!(d.read_i32().unwrap(), 0);
    assert_eq!(d.read_i32().unwrap(), 1);
    assert_eq!(d.read_nullable_string().unwrap(), Some(String::new()));
    assert_eq!(d.read_i32().unwrap(), 1);
    assert_eq!(d.read_i32().unwrap(), 0);
    assert_eq!(d.read_i16().unwrap(), ERROR_INVALID_REQUEST);
}

// ── ListOffsets v0 wire shape (old_style_offsets array, not bare i64) ───────

/// Parse one `ListOffsets` v0 partition entry the way a v0 Kafka client would.
fn parse_list_offsets_v0_partition(d: &mut Decoder) {
    let _partition_index = d.read_i32().expect("partition_index");
    let _error_code = d.read_i16().expect("error_code");
    let offset_count = d.read_i32().expect("old_style_offsets array length");
    assert!(
        offset_count >= 0,
        "old_style_offsets count must be non-negative, got {offset_count}"
    );
    for _ in 0..offset_count {
        d.read_i64().expect("old_style_offsets entry");
    }
}

#[test]
fn list_offsets_v0_unsupported_version_is_parseable_by_v0_clients() {
    let body = handle_request(API_KEY_LIST_OFFSETS, 0, Bytes::new(), &default_broker())
        .expect_response("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(body);

    assert_eq!(d.read_i32().unwrap(), 1, "topics array length");
    assert_eq!(
        d.read_nullable_string().unwrap(),
        Some(String::new()),
        "placeholder topic name"
    );
    assert_eq!(d.read_i32().unwrap(), 1, "partitions array length");

    parse_list_offsets_v0_partition(&mut d);

    assert_eq!(
        d.remaining(),
        0,
        "v0 client must consume the full error response without trailing bytes"
    );
}

#[test]
fn list_offsets_v0_unsupported_version_carries_error_code_in_partition() {
    let request_body = build_list_offsets_v0_request_with_topic_t();
    let body = handle_request(API_KEY_LIST_OFFSETS, 0, request_body, &default_broker())
        .expect_response("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(body);

    assert_eq!(d.read_i32().unwrap(), 1);
    d.read_nullable_string().unwrap();
    assert_eq!(d.read_i32().unwrap(), 1);
    assert_eq!(d.read_i32().unwrap(), 0, "partition index");
    assert_eq!(
        d.read_i16().unwrap(),
        ERROR_UNSUPPORTED_VERSION,
        "partition error code"
    );

    // partition_index and error_code were already asserted above; only the
    // trailing old_style_offsets array remains for this single partition.
    let offset_count = d.read_i32().expect("old_style_offsets array length");
    assert!(
        offset_count >= 0,
        "old_style_offsets count must be non-negative, got {offset_count}"
    );
    for _ in 0..offset_count {
        d.read_i64().expect("old_style_offsets entry");
    }
    assert_eq!(d.remaining(), 0);
}

// ── Comprehensive scoped-API coverage (correlation id, boundary versions) ──

fn request_body_for_scoped_api(api_key: i16, name: &str, version: i16) -> Bytes {
    match api_key {
        API_KEY_METADATA => {
            if version >= 9 {
                build_metadata_all_topics_flexible(version)
            } else {
                build_metadata_all_topics_legacy(version)
            }
        }
        API_KEY_API_VERSIONS => {
            if version >= 3 {
                build_api_versions_flexible_request("iggy-test", "0.1.0")
            } else {
                Bytes::new()
            }
        }
        API_KEY_PRODUCE => {
            if fixture_exists(api_key, name, version) {
                load_fixture_body(api_key, name, version)
            } else {
                build_produce_v3_body(1, 0)
            }
        }
        API_KEY_FETCH => {
            if fixture_exists(api_key, name, version) {
                load_fixture_body(api_key, name, version)
            } else {
                build_fetch_empty_topics_request(version)
            }
        }
        API_KEY_LIST_OFFSETS => {
            if fixture_exists(api_key, name, version) {
                load_fixture_body(api_key, name, version)
            } else {
                build_list_offsets_request(version, "scope-topic", 0)
            }
        }
        API_KEY_CREATE_TOPICS => build_create_topics_empty_request(version),
        _ => Bytes::new(),
    }
}

#[tokio::test]
async fn each_scoped_api_min_version_preserves_correlation_id_e2e() {
    let (addr, _shutdown) = spawn_test_server().await;

    for &(api_key, name, min_ver, _max_ver) in SCOPED_API_KEYS {
        let correlation_id = 10_000 + i32::from(api_key);
        let body = request_body_for_scoped_api(api_key, name, min_ver);
        let (corr, resp_body) = round_trip(addr, api_key, min_ver, correlation_id, &body).await;
        assert_eq!(
            corr, correlation_id,
            "{name} v{min_ver} correlation id must round-trip"
        );
        assert!(
            !resp_body.is_empty(),
            "{name} v{min_ver} must return non-empty body"
        );
    }
}

#[tokio::test]
async fn each_scoped_api_max_version_preserves_correlation_id_e2e() {
    let (addr, _shutdown) = spawn_test_server().await;

    for &(api_key, name, _min_ver, max_ver) in SCOPED_API_KEYS {
        let correlation_id = 20_000 + i32::from(api_key);
        let body = request_body_for_scoped_api(api_key, name, max_ver);
        let (corr, resp_body) = round_trip(addr, api_key, max_ver, correlation_id, &body).await;
        assert_eq!(
            corr, correlation_id,
            "{name} v{max_ver} correlation id must round-trip"
        );
        assert!(
            !resp_body.is_empty(),
            "{name} v{max_ver} must return non-empty body"
        );
    }
}

#[tokio::test]
async fn apiversions_v0_and_v2_e2e_return_success() {
    let (addr, _shutdown) = spawn_test_server().await;

    for version in [0i16, 2] {
        let correlation_id = 300 + i32::from(version);
        let (corr, body) =
            round_trip(addr, API_KEY_API_VERSIONS, version, correlation_id, &[]).await;
        assert_eq!(corr, correlation_id);
        let mut d = Decoder::new(body);
        assert_eq!(
            d.read_i16().unwrap(),
            ERROR_NONE,
            "ApiVersions v{version} must succeed"
        );
    }
}

#[tokio::test]
async fn apiversions_v4_out_of_range_e2e_returns_unsupported() {
    let (addr, _shutdown) = spawn_test_server().await;
    let (corr, body) = round_trip(addr, API_KEY_API_VERSIONS, 4, 350, &[]).await;
    assert_eq!(corr, 350);
    let mut d = Decoder::new(body);
    assert_eq!(
        d.read_i16().unwrap(),
        ERROR_UNSUPPORTED_VERSION,
        "ApiVersions v4 must return UNSUPPORTED_VERSION per KIP-511"
    );
}

// ── Out-of-scope API keys (SCOPE.md unsupported list) ───────────────────────

#[test]
fn out_of_scope_api_keys_return_unsupported_version_without_panic() {
    for &(api_key, name) in OUT_OF_SCOPE_API_KEYS {
        let body = handle_request(api_key, 0, Bytes::new(), &default_broker())
            .expect_response("test request has acks != 0 and expects a response");
        let mut d = Decoder::new(body);
        assert_eq!(
            d.read_i16().unwrap(),
            ERROR_UNSUPPORTED_VERSION,
            "{name} (key {api_key})"
        );
    }
}

// ── Boundary versions keep the TCP session (except Metadata) ───────────────

#[tokio::test]
async fn each_scoped_api_above_max_version_e2e_closes_or_kip511() {
    let (addr, _shutdown) = spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    for &(api_key, name, _min_ver, max_ver) in SCOPED_API_KEYS {
        let above = max_ver + 1;
        let frame = build_request_frame(
            api_key,
            above,
            50_000 + i32::from(api_key),
            Some("scope-test"),
            &[],
        );
        stream
            .write_all(&frame)
            .await
            .unwrap_or_else(|_| panic!("write {name} v{above}"));
        if api_key == API_KEY_API_VERSIONS {
            // KIP-511: ApiVersions above max still answers with a v0 UNSUPPORTED_VERSION body.
            let payload = tcp::read_response_frame(&mut stream, 8 * 1024 * 1024).await;
            assert!(
                !payload.is_empty(),
                "{name} v{above} must still respond on wire"
            );
            continue;
        }
        // Other APIs: encoding at the client's raw version above our encoder max would omit
        // later-version fields, so Close is the honest contract (same as Metadata).
        assert_eq!(
            read_byte_with_timeout(&mut stream, Duration::from_secs(2)).await,
            ByteRead::Closed,
            "{name} v{above} must close the connection"
        );
        stream = TcpStream::connect(addr)
            .await
            .unwrap_or_else(|_| panic!("reconnect after {name} close"));
    }

    let ok = build_request_frame(API_KEY_API_VERSIONS, 1, 89_999, Some("scope-test"), &[]);
    stream.write_all(&ok).await.expect("recovery request");
    let payload = tcp::read_response_frame(&mut stream, 8 * 1024 * 1024).await;
    assert_eq!(
        parse_response_payload(API_KEY_API_VERSIONS, 1, payload).0,
        89_999
    );
}

#[tokio::test]
async fn each_scoped_api_below_min_version_e2e_keeps_connection() {
    let (addr, _shutdown) = spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    for &(api_key, name, min_ver, _max_ver) in SCOPED_API_KEYS {
        let below = min_ver - 1;
        let frame = build_request_frame(
            api_key,
            below,
            40_000 + i32::from(api_key),
            Some("scope-test"),
            &[],
        );
        stream
            .write_all(&frame)
            .await
            .unwrap_or_else(|_| panic!("write {name} v{below}"));
        if api_key == API_KEY_METADATA {
            assert_eq!(
                read_byte_with_timeout(&mut stream, Duration::from_secs(2)).await,
                ByteRead::Closed,
                "Metadata v{below} must close the connection"
            );
            stream = TcpStream::connect(addr)
                .await
                .expect("reconnect after Metadata close");
            continue;
        }
        let payload = tcp::read_response_frame(&mut stream, 8 * 1024 * 1024).await;
        assert!(
            !payload.is_empty(),
            "{name} v{below} must still respond on wire"
        );
    }

    let ok = build_request_frame(API_KEY_API_VERSIONS, 1, 88_888, Some("scope-test"), &[]);
    stream.write_all(&ok).await.expect("recovery request");
    let payload = tcp::read_response_frame(&mut stream, 8 * 1024 * 1024).await;
    assert_eq!(
        parse_response_payload(API_KEY_API_VERSIONS, 1, payload).0,
        88_888
    );
}

#[test]
fn produce_advertises_min_zero_but_firewall_rejects_below_v3() {
    let range = SCOPED_API_KEYS
        .iter()
        .find(|(k, _, _, _)| *k == API_KEY_PRODUCE)
        .expect("produce in scope");
    let (_, _, firewall_min, _) = *range;
    assert_eq!(firewall_min, 3);
    assert_eq!(advertised_min_version(API_KEY_PRODUCE, firewall_min), 0);
    assert!(!is_supported_version(API_KEY_PRODUCE, 0));
    assert!(!is_supported_version(API_KEY_PRODUCE, 2));

    let body = handle_request(API_KEY_PRODUCE, 2, Bytes::new(), &default_broker())
        .expect_response("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(body);
    let _topics = d.read_i32().unwrap();
    let _name = d.read_nullable_string().unwrap();
    let _parts = d.read_i32().unwrap();
    assert_eq!(d.read_i32().unwrap(), 0, "partition index");
    assert_eq!(d.read_i16().unwrap(), ERROR_UNSUPPORTED_VERSION);
}

// ── Unsupported-version e2e paths for remaining scoped APIs ─────────────────

#[tokio::test]
async fn list_offsets_v7_unsupported_e2e_closes_connection() {
    let (addr, _shutdown) = spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let frame = build_request_frame(
        API_KEY_LIST_OFFSETS,
        7,
        370,
        Some("scope-test"),
        &[0x00, 0x00, 0x00, 0x00],
    );
    stream.write_all(&frame).await.expect("write");
    assert_eq!(
        read_byte_with_timeout(&mut stream, Duration::from_secs(2)).await,
        ByteRead::Closed,
        "ListOffsets v7 is above encoder max and must close"
    );
}

#[tokio::test]
async fn create_topics_v1_unsupported_e2e_returns_error() {
    let (addr, _shutdown) = spawn_test_server().await;
    let (corr, body) = round_trip(addr, API_KEY_CREATE_TOPICS, 1, 380, &[]).await;
    assert_eq!(corr, 380);
    assert!(
        scan_for_error_code(&body, ERROR_UNSUPPORTED_VERSION),
        "CreateTopics v1 must be rejected"
    );
}

// ── Corrupt decode paths for remaining scoped APIs ──────────────────────────

#[test]
fn corrupt_list_offsets_body_returns_invalid_request_error() {
    let body = Bytes::from_static(&[0xFF, 0xFF, 0xFF]);
    let resp = handle_request(API_KEY_LIST_OFFSETS, 1, body, &default_broker())
        .expect_response("test request has acks != 0 and expects a response");
    assert!(!resp.is_empty());
    assert!(
        scan_for_error_code(&resp, ERROR_INVALID_REQUEST)
            || scan_for_error_code(&resp, ERROR_UNSUPPORTED_VERSION),
        "corrupt ListOffsets must surface protocol error"
    );
}

#[test]
fn corrupt_create_topics_body_returns_invalid_request_error() {
    let body = Bytes::from_static(&[0xFF, 0xFF, 0xFF]);
    let resp = handle_request(API_KEY_CREATE_TOPICS, 2, body, &default_broker())
        .expect_response("test request has acks != 0 and expects a response");
    assert!(!resp.is_empty());
    assert!(
        scan_for_error_code(&resp, ERROR_INVALID_REQUEST)
            || scan_for_error_code(&resp, ERROR_UNSUPPORTED_VERSION)
    );
}
