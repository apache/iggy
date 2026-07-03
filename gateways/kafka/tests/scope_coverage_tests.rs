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

//! Comprehensive scoped-API coverage per SCOPE.md and Kafka protocol spec.
//!
//! Fills gaps not covered by existing regression suites. Some tests encode
//! client-correct behavior and fail until implementation catches up.

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

use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use iggy_gateway_kafka::protocol::api::{
    API_KEY_API_VERSIONS, API_KEY_CREATE_TOPICS, API_KEY_FETCH, API_KEY_LIST_OFFSETS,
    API_KEY_METADATA, API_KEY_PRODUCE, ERROR_INVALID_REQUEST, ERROR_NONE,
    ERROR_UNKNOWN_TOPIC_OR_PARTITION, ERROR_UNSUPPORTED_VERSION, advertised_min_version,
    handle_request, is_supported_version,
};
use iggy_gateway_kafka::protocol::codec::Decoder;

use fixtures::{fixture_exists, load_fixture_body};
use scope::{SCOPED_API_KEYS, default_broker};
use server::spawn_test_server;
use tcp::{
    build_metadata_legacy_request, build_produce_v3_body, build_request_frame,
    parse_response_payload, round_trip,
};
use wire::{
    OUT_OF_SCOPE_API_KEYS, build_create_topics_empty_request, build_fetch_empty_topics_request,
    build_list_offsets_request, build_metadata_flexible_request,
    build_produce_flexible_empty_request,
};

fn metadata_empty_legacy_body() -> Bytes {
    let mut body = BytesMut::new();
    body.put_i32(0);
    body.freeze()
}

fn request_body_for_scoped_api(api_key: i16, name: &str, version: i16) -> Bytes {
    match api_key {
        API_KEY_METADATA => metadata_empty_legacy_body(),
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

// ── Correlation ID preservation (all scoped keys × min/max/flexible) ────────

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

// ── Out-of-scope API keys (SCOPE.md unsupported list) ───────────────────────

#[test]
fn out_of_scope_api_keys_return_unsupported_version_without_panic() {
    for &(api_key, name) in OUT_OF_SCOPE_API_KEYS {
        let body = handle_request(api_key, 0, Bytes::new(), &default_broker());
        let mut d = Decoder::new(body);
        assert_eq!(
            d.read_i16().unwrap(),
            ERROR_UNSUPPORTED_VERSION,
            "{name} (key {api_key})"
        );
    }
}

#[tokio::test]
async fn out_of_scope_api_keys_e2e_keep_connection_for_follow_up() {
    let (addr, _shutdown) = spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    for &(api_key, _name) in &OUT_OF_SCOPE_API_KEYS[..4] {
        let frame = build_request_frame(api_key, 0, i32::from(api_key), Some("scope-test"), &[]);
        stream.write_all(&frame).await.expect("write oos key");
        let payload = tcp::read_response_frame(&mut stream, 8 * 1024 * 1024).await;
        let mut d = Decoder::new(parse_response_payload(api_key, 0, payload).1);
        assert_eq!(d.read_i16().unwrap(), ERROR_UNSUPPORTED_VERSION);
    }

    let follow_up = build_request_frame(API_KEY_API_VERSIONS, 1, 99_999, Some("scope-test"), &[]);
    stream.write_all(&follow_up).await.expect("follow-up write");
    let payload = tcp::read_response_frame(&mut stream, 8 * 1024 * 1024).await;
    let (corr, body) = parse_response_payload(API_KEY_API_VERSIONS, 1, payload);
    assert_eq!(corr, 99_999);
    assert_eq!(Decoder::new(body).read_i16().unwrap(), ERROR_NONE);
}

// ── Version firewall: unsupported version keeps TCP session ─────────────────

#[tokio::test]
async fn each_scoped_api_above_max_version_e2e_keeps_connection() {
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
        let payload = tcp::read_response_frame(&mut stream, 8 * 1024 * 1024).await;
        assert!(
            !payload.is_empty(),
            "{name} v{above} must still respond on wire"
        );
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
    let range = scope::SCOPED_API_KEYS
        .iter()
        .find(|(k, _, _, _)| *k == API_KEY_PRODUCE)
        .expect("produce in scope");
    let (_, _, firewall_min, _) = *range;
    assert_eq!(firewall_min, 3);
    assert_eq!(advertised_min_version(API_KEY_PRODUCE, firewall_min), 0);
    assert!(!is_supported_version(API_KEY_PRODUCE, 0));
    assert!(!is_supported_version(API_KEY_PRODUCE, 2));

    let body = handle_request(API_KEY_PRODUCE, 2, Bytes::new(), &default_broker());
    let mut d = Decoder::new(body);
    let _topics = d.read_i32().unwrap();
    let _name = d.read_nullable_string().unwrap();
    let _parts = d.read_i32().unwrap();
    assert_eq!(d.read_i32().unwrap(), 0, "partition index");
    assert_eq!(d.read_i16().unwrap(), ERROR_UNSUPPORTED_VERSION);
}

// ── Metadata (spec + SCOPE) ─────────────────────────────────────────────────

#[test]
fn metadata_v0_empty_topics_returns_zero_length_topic_array() {
    let body = handle_request(
        API_KEY_METADATA,
        0,
        metadata_empty_legacy_body(),
        &default_broker(),
    );
    let mut d = Decoder::new(body);
    let _brokers = d.read_i32().unwrap();
    d.read_i32().unwrap();
    d.read_nullable_string().unwrap();
    d.read_i32().unwrap();
    assert_eq!(d.read_i32().unwrap(), 0, "empty request → zero topics");
    assert_eq!(d.remaining(), 0);
}

#[test]
fn metadata_v3_includes_throttle_time_ms_before_brokers() {
    let body = handle_request(
        API_KEY_METADATA,
        3,
        metadata_empty_legacy_body(),
        &default_broker(),
    );
    let mut d = Decoder::new(body);
    assert_eq!(d.read_i32().unwrap(), 0, "throttle_time_ms");
}

#[test]
fn metadata_v9_flexible_empty_topics_returns_zero_topics() {
    let body = handle_request(
        API_KEY_METADATA,
        9,
        build_metadata_flexible_request(&[]),
        &default_broker(),
    );
    let mut d = Decoder::new(body);
    d.read_i32().unwrap(); // throttle
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
    assert_eq!(topic_count, 0);
}

#[test]
fn metadata_v9_flexible_echoes_each_requested_topic_name() {
    let topics = ["orders", "payments", "inventory"];
    let body = handle_request(
        API_KEY_METADATA,
        9,
        build_metadata_flexible_request(&topics),
        &default_broker(),
    );

    let mut d = Decoder::new(body);
    d.read_i32().unwrap();
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
    d.read_compact_nullable_string().unwrap();
    d.read_i32().unwrap();

    let topic_count = usize::try_from(d.read_varint().unwrap())
        .unwrap()
        .saturating_sub(1);
    assert_eq!(topic_count, topics.len());

    let mut names = Vec::new();
    for _ in 0..topic_count {
        assert_eq!(
            d.read_i16().unwrap(),
            ERROR_UNKNOWN_TOPIC_OR_PARTITION,
            "stub gateway returns unknown topic error per topic"
        );
        names.push(
            d.read_compact_nullable_string()
                .unwrap()
                .expect("topic name"),
        );
        d.read_bool().unwrap();
        assert_eq!(
            usize::try_from(d.read_varint().unwrap())
                .unwrap()
                .saturating_sub(1),
            0,
            "empty partitions array"
        );
        d.read_tagged_fields().unwrap();
    }

    assert_eq!(
        names,
        topics
            .iter()
            .map(|topic| (*topic).to_string())
            .collect::<Vec<_>>(),
        "metadata must echo requested topic names for client matching"
    );
}

#[test]
fn metadata_v1_legacy_multiple_topics_echo_names() {
    let topics = ["alpha", "beta"];
    let body = handle_request(
        API_KEY_METADATA,
        1,
        build_metadata_legacy_request(&topics),
        &default_broker(),
    );

    let mut d = Decoder::new(body);
    d.read_i32().unwrap();
    d.read_i32().unwrap();
    d.read_nullable_string().unwrap();
    d.read_i32().unwrap();
    d.read_nullable_string().unwrap();
    d.read_i32().unwrap();
    assert_eq!(d.read_i32().unwrap(), 2);

    for expected in topics {
        assert_eq!(d.read_i16().unwrap(), ERROR_UNKNOWN_TOPIC_OR_PARTITION);
        assert_eq!(
            d.read_nullable_string().unwrap().as_deref(),
            Some(expected),
            "metadata v1 must echo {expected}"
        );
        d.read_bool().unwrap();
        assert_eq!(d.read_i32().unwrap(), 0);
    }
}

// ── Produce (Kafka spec acks semantics) ─────────────────────────────────────

#[tokio::test]
async fn e2e_produce_v3_acks_all_minus_one_returns_response() {
    let (addr, _shutdown) = spawn_test_server().await;
    let body = build_produce_v3_body(-1, 0);
    let (corr, resp) = round_trip(addr, API_KEY_PRODUCE, 3, 501, &body).await;
    assert_eq!(corr, 501);
    assert!(!resp.is_empty());
}

#[tokio::test]
async fn e2e_produce_v9_flexible_header_with_empty_topics() {
    let (addr, _shutdown) = spawn_test_server().await;
    let body = build_produce_flexible_empty_request(1);
    let (corr, resp) = round_trip(addr, API_KEY_PRODUCE, 9, 502, &body).await;
    assert_eq!(corr, 502);
    let mut d = Decoder::new(resp);
    assert!(
        d.read_varint().unwrap() >= 1,
        "flexible topics array header"
    );
}

#[tokio::test]
async fn produce_v3_through_v9_e2e_preserve_correlation_id() {
    let (addr, _shutdown) = spawn_test_server().await;

    for version in 3i16..=9 {
        let body = if version >= 9 {
            build_produce_flexible_empty_request(1)
        } else {
            build_produce_v3_body(1, 0)
        };
        let correlation_id = 510 + i32::from(version);
        let (corr, resp) = round_trip(addr, API_KEY_PRODUCE, version, correlation_id, &body).await;
        assert_eq!(corr, correlation_id, "Produce v{version} correlation");
        assert!(!resp.is_empty(), "Produce v{version} response");
    }
}

// ── ListOffsets supported versions ──────────────────────────────────────────

#[tokio::test]
async fn list_offsets_v1_through_v6_e2e_return_partition_error_zero() {
    let (addr, _shutdown) = spawn_test_server().await;

    for version in 1i16..=6 {
        let body = build_list_offsets_request(version, "offsets-topic", 0);
        let correlation_id = 600 + i32::from(version);
        let (corr, resp) =
            round_trip(addr, API_KEY_LIST_OFFSETS, version, correlation_id, &body).await;
        assert_eq!(corr, correlation_id);

        let flexible = version >= 6;
        let mut d = Decoder::new(resp);
        if version >= 2 {
            d.read_i32().unwrap();
        }
        if flexible {
            d.read_varint().unwrap();
            d.read_compact_nullable_string().unwrap();
            d.read_varint().unwrap();
        } else {
            d.read_i32().unwrap();
            d.read_nullable_string().unwrap();
            d.read_i32().unwrap();
        }
        d.read_i32().unwrap();
        assert_eq!(
            d.read_i16().unwrap(),
            ERROR_NONE,
            "ListOffsets v{version} stub partition error"
        );
    }
}

// ── CreateTopics empty create ───────────────────────────────────────────────

#[tokio::test]
async fn create_topics_v2_through_v5_empty_request_e2e_succeeds() {
    let (addr, _shutdown) = spawn_test_server().await;

    for version in 2i16..=5 {
        let body = build_create_topics_empty_request(version);
        let correlation_id = 700 + i32::from(version);
        let (corr, resp) =
            round_trip(addr, API_KEY_CREATE_TOPICS, version, correlation_id, &body).await;
        assert_eq!(corr, correlation_id);
        let mut d = Decoder::new(resp);
        if version >= 2 {
            d.read_i32().unwrap();
        }
        if version >= 5 {
            assert!(d.read_varint().unwrap() >= 1);
        } else {
            assert_eq!(d.read_i32().unwrap(), 0);
        }
    }
}

// ── Fetch flexible boundary ─────────────────────────────────────────────────

#[tokio::test]
async fn fetch_v4_through_v12_e2e_preserve_correlation_id() {
    let (addr, _shutdown) = spawn_test_server().await;

    for version in 4i16..=12 {
        let body = request_body_for_scoped_api(API_KEY_FETCH, "Fetch", version);
        let correlation_id = 800 + i32::from(version);
        let (corr, resp) = round_trip(addr, API_KEY_FETCH, version, correlation_id, &body).await;
        assert_eq!(corr, correlation_id, "Fetch v{version} correlation");
        assert!(!resp.is_empty(), "Fetch v{version} response");
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

#[tokio::test]
async fn metadata_empty_body_e2e_all_topics_request_returns_broker() {
    let (addr, _shutdown) = spawn_test_server().await;
    let (corr, body) = round_trip(addr, API_KEY_METADATA, 0, 360, &[]).await;
    assert_eq!(corr, 360);
    let mut d = Decoder::new(body);
    assert_eq!(d.read_i32().unwrap(), 1, "one stub broker");
    d.read_i32().unwrap(); // node_id
    let host = d.read_nullable_string().unwrap().expect("broker host");
    assert!(!host.is_empty());
    let port = d.read_i32().unwrap();
    assert!(port > 0);
}

#[tokio::test]
async fn list_offsets_v7_unsupported_e2e_returns_error() {
    let (addr, _shutdown) = spawn_test_server().await;
    let (corr, body) = round_trip(
        addr,
        API_KEY_LIST_OFFSETS,
        7,
        370,
        &[0x00, 0x00, 0x00, 0x00],
    )
    .await;
    assert_eq!(corr, 370);
    assert!(
        scan_for_error_code(&body, ERROR_UNSUPPORTED_VERSION),
        "ListOffsets v7 must be rejected"
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

#[tokio::test]
async fn corrupt_produce_body_e2e_returns_error_without_disconnect() {
    let (addr, _shutdown) = spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    let bad = build_request_frame(
        API_KEY_PRODUCE,
        3,
        391,
        Some("scope-test"),
        &[0xFF, 0xFF, 0xFF],
    );
    stream.write_all(&bad).await.expect("corrupt produce");
    let payload = tcp::read_response_frame(&mut stream, 8 * 1024 * 1024).await;
    assert!(
        scan_for_error_code(
            &parse_response_payload(API_KEY_PRODUCE, 3, payload).1,
            ERROR_INVALID_REQUEST
        ),
        "corrupt Produce must surface INVALID_REQUEST"
    );

    let ok = build_request_frame(API_KEY_API_VERSIONS, 1, 392, Some("scope-test"), &[]);
    stream.write_all(&ok).await.expect("follow-up");
    let payload = tcp::read_response_frame(&mut stream, 8 * 1024 * 1024).await;
    assert_eq!(
        parse_response_payload(API_KEY_API_VERSIONS, 1, payload).0,
        392
    );
}

#[tokio::test]
async fn corrupt_fetch_body_e2e_returns_error_without_disconnect() {
    let (addr, _shutdown) = spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    let bad = build_request_frame(
        API_KEY_FETCH,
        4,
        393,
        Some("scope-test"),
        &[0xFF, 0xFF, 0xFF],
    );
    stream.write_all(&bad).await.expect("corrupt fetch");
    let payload = tcp::read_response_frame(&mut stream, 8 * 1024 * 1024).await;
    assert!(
        scan_for_error_code(
            &parse_response_payload(API_KEY_FETCH, 4, payload).1,
            ERROR_INVALID_REQUEST
        ),
        "corrupt Fetch must surface INVALID_REQUEST"
    );

    let ok = build_request_frame(API_KEY_API_VERSIONS, 1, 394, Some("scope-test"), &[]);
    stream.write_all(&ok).await.expect("follow-up");
    let payload = tcp::read_response_frame(&mut stream, 8 * 1024 * 1024).await;
    assert_eq!(
        parse_response_payload(API_KEY_API_VERSIONS, 1, payload).0,
        394
    );
}

// ── Corrupt decode paths for remaining scoped APIs ──────────────────────────

#[test]
fn corrupt_list_offsets_body_returns_invalid_request_error() {
    let body = Bytes::from_static(&[0xFF, 0xFF, 0xFF]);
    let resp = handle_request(API_KEY_LIST_OFFSETS, 1, body, &default_broker());
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
    let resp = handle_request(API_KEY_CREATE_TOPICS, 2, body, &default_broker());
    assert!(!resp.is_empty());
    assert!(
        scan_for_error_code(&resp, ERROR_INVALID_REQUEST)
            || scan_for_error_code(&resp, ERROR_UNSUPPORTED_VERSION)
    );
}

#[test]
fn corrupt_metadata_body_returns_zero_topics_not_panic() {
    let body = Bytes::from_static(&[0x00, 0x00]);
    let resp = handle_request(API_KEY_METADATA, 0, body, &default_broker());
    assert!(!resp.is_empty());
    let mut d = Decoder::new(resp);
    d.read_i32().unwrap();
    d.read_i32().unwrap();
    d.read_nullable_string().unwrap();
    d.read_i32().unwrap();
    assert_eq!(
        d.read_i32().unwrap(),
        0,
        "malformed topic list → zero topics"
    );
}

fn scan_for_error_code(body: &Bytes, code: i16) -> bool {
    body.windows(2)
        .any(|w| i16::from_be_bytes([w[0], w[1]]) == code)
}

// ── Flexible encoding boundaries (SCOPE.md) ───────────────────────────────────

#[test]
fn request_header_version_switches_at_scope_flexible_boundaries() {
    use iggy_gateway_kafka::protocol::header::request_header_version;

    assert_eq!(request_header_version(API_KEY_PRODUCE, 8), 1);
    assert_eq!(request_header_version(API_KEY_PRODUCE, 9), 2);
    assert_eq!(request_header_version(API_KEY_FETCH, 11), 1);
    assert_eq!(request_header_version(API_KEY_FETCH, 12), 2);
    assert_eq!(request_header_version(API_KEY_LIST_OFFSETS, 5), 1);
    assert_eq!(request_header_version(API_KEY_LIST_OFFSETS, 6), 2);
    assert_eq!(request_header_version(API_KEY_METADATA, 8), 1);
    assert_eq!(request_header_version(API_KEY_METADATA, 9), 2);
    assert_eq!(request_header_version(API_KEY_API_VERSIONS, 2), 1);
    assert_eq!(request_header_version(API_KEY_API_VERSIONS, 3), 2);
    assert_eq!(request_header_version(API_KEY_CREATE_TOPICS, 4), 1);
    assert_eq!(request_header_version(API_KEY_CREATE_TOPICS, 5), 2);
}

#[test]
fn metadata_v9_request_with_three_topics_yields_three_response_slots() {
    let topics = ["a", "b", "c"];
    let body = handle_request(
        API_KEY_METADATA,
        9,
        build_metadata_flexible_request(&topics),
        &default_broker(),
    );
    let mut d = Decoder::new(body);
    d.read_i32().unwrap();
    skip_metadata_v9_prefix(&mut d);
    let topic_count = usize::try_from(d.read_varint().unwrap())
        .unwrap()
        .saturating_sub(1);
    assert_eq!(
        topic_count,
        topics.len(),
        "response topic count must mirror request topic count"
    );
}

fn skip_metadata_v9_prefix(d: &mut Decoder) {
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
    d.read_compact_nullable_string().unwrap();
    d.read_i32().unwrap();
}

// ── Handler-level: every in-range version returns bytes ─────────────────────

#[test]
fn every_in_range_version_returns_non_empty_handler_response() {
    for &(api_key, name, min_ver, max_ver) in SCOPED_API_KEYS {
        for version in min_ver..=max_ver {
            let body = request_body_for_scoped_api(api_key, name, version);
            let resp = handle_request(api_key, version, body, &default_broker());
            assert!(!resp.is_empty(), "{name} v{version} handler returned empty");
        }
    }
}
