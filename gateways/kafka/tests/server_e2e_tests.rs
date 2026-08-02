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

//! End-to-end TCP tests through `KafkaServer` (full request/response cycle).

#[path = "common/fixtures.rs"]
mod fixtures;
#[path = "common/server.rs"]
mod server;
#[path = "common/tcp.rs"]
mod tcp;
#[path = "common/wire.rs"]
mod wire;

use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use iggy_gateway_kafka::protocol::api::{
    API_KEY_API_VERSIONS, API_KEY_CREATE_TOPICS, API_KEY_FETCH, API_KEY_LIST_OFFSETS,
    API_KEY_METADATA, API_KEY_PRODUCE, ERROR_NOT_LEADER_OR_FOLLOWER, ERROR_UNSUPPORTED_VERSION,
};
use iggy_gateway_kafka::protocol::codec::Decoder;

use fixtures::load_fixture_body_or_skip;
use server::spawn_test_server;
use std::time::Duration;
use tcp::{
    ByteRead, build_list_offsets_v0_request_with_topic_t, build_metadata_legacy_request,
    build_produce_v3_body, build_request_frame, parse_response_payload, read_byte_with_timeout,
    read_response_frame, read_response_frame_with_timeout, round_trip,
};
use wire::{
    OUT_OF_SCOPE_API_KEYS, build_create_topics_empty_request, build_fetch_empty_topics_request,
    build_list_offsets_request, build_produce_flexible_empty_request,
};

#[tokio::test]
async fn e2e_apiversions_v1_preserves_correlation_id() {
    let (addr, _shutdown) = spawn_test_server().await;
    let (corr, body) = round_trip(addr, API_KEY_API_VERSIONS, 1, 42_001, &[]).await;
    assert_eq!(corr, 42_001);
    let mut d = Decoder::new(body);
    assert_eq!(d.read_i16().unwrap(), 0);
}

#[tokio::test]
async fn e2e_apiversions_v3_flexible_preserves_correlation_id() {
    let (addr, _shutdown) = spawn_test_server().await;
    let (corr, body) = round_trip(addr, API_KEY_API_VERSIONS, 3, 42_002, &[]).await;
    assert_eq!(corr, 42_002);
    let mut d = Decoder::new(body);
    assert_eq!(d.read_i16().unwrap(), 0);
    let count = usize::try_from(d.read_varint().unwrap() - 1).expect("api count fits usize");
    assert_eq!(count, 6);
}

#[tokio::test]
async fn e2e_metadata_v0_returns_stub_broker() {
    let (addr, _shutdown) = spawn_test_server().await;
    let mut req = BytesMut::new();
    req.put_i32(0); // empty topics
    let (corr, body) = round_trip(addr, API_KEY_METADATA, 0, 77, &req).await;
    assert_eq!(corr, 77);
    let mut d = Decoder::new(body);
    assert_eq!(d.read_i32().unwrap(), 1);
    d.read_i32().unwrap();
    let host = d.read_nullable_string().unwrap().unwrap();
    assert_eq!(host, "127.0.0.1");
}

#[tokio::test]
async fn e2e_produce_v3_round_trip_with_fixture() {
    let (addr, _shutdown) = spawn_test_server().await;
    let Some(body) = load_fixture_body_or_skip(0, "Produce", 3) else {
        return;
    };
    let (corr, resp_body) = round_trip(addr, API_KEY_PRODUCE, 3, 88, &body).await;
    assert_eq!(corr, 88);
    assert!(!resp_body.is_empty());
}

#[tokio::test]
async fn e2e_unsupported_api_key_returns_error_then_closes() {
    let (addr, _shutdown) = spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let frame1 = build_request_frame(8, 2, 99, Some("e2e-test"), &[]);
    stream.write_all(&frame1).await.unwrap();
    let payload1 = read_response_frame(&mut stream, 8 * 1024 * 1024).await;
    let (corr, body) = parse_response_payload(8, 2, payload1);
    assert_eq!(corr, 99);
    let mut d = Decoder::new(body);
    assert_eq!(d.read_i16().unwrap(), ERROR_UNSUPPORTED_VERSION);

    // The unsupported-version error is terminal: the server closes the connection.
    assert_eq!(
        read_byte_with_timeout(&mut stream, Duration::from_secs(2)).await,
        ByteRead::Closed,
        "connection must close after the unsupported-version error response"
    );
}

#[tokio::test]
async fn e2e_sequential_requests_on_one_connection() {
    let (addr, _shutdown) = spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let requests = [(API_KEY_API_VERSIONS, 1i16), (API_KEY_METADATA, 0i16)];
    for (i, (key, ver)) in requests.iter().enumerate() {
        let meta_body = {
            let mut b = BytesMut::new();
            b.put_i32(0);
            b
        };
        let body: &[u8] = if *key == API_KEY_METADATA {
            &meta_body
        } else {
            &[]
        };
        let correlation_id = 1000 + i32::try_from(i).expect("test index fits i32");
        let frame = build_request_frame(*key, *ver, correlation_id, Some("seq-test"), body);
        stream.write_all(&frame).await.unwrap();
        let payload = read_response_frame(&mut stream, 8 * 1024 * 1024).await;
        let (corr, _) = parse_response_payload(*key, *ver, payload);
        assert_eq!(corr, correlation_id);
    }
}

// Negative-frame-length-closes-connection coverage lives in listener_robustness_tests.rs
// (uses a timeout-guarded read helper, so a regression fails fast instead of hanging).

#[tokio::test]
async fn e2e_oversized_frame_is_rejected() {
    let (addr, _shutdown) = spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let mut frame = BytesMut::new();
    frame.put_i32(10_000_000); // exceeds default 8 MiB cap
    frame.resize(4 + 100, 0);
    stream.write_all(&frame).await.unwrap();

    let mut buf = [0u8; 1];
    let n = stream.read(&mut buf).await.unwrap_or(0);
    assert_eq!(n, 0, "server should close after oversized frame");
}

// ── Produce acks=0 (broker must stay silent) ────────────────────────────────

#[tokio::test]
async fn e2e_produce_v3_acks_zero_sends_no_response() {
    let (addr, _shutdown) = spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    let body = build_produce_v3_body(0, 0);
    let frame = build_request_frame(API_KEY_PRODUCE, 3, 42, Some("review-test"), &body);
    stream
        .write_all(&frame)
        .await
        .expect("write produce acks=0");

    let response =
        read_response_frame_with_timeout(&mut stream, 8 * 1024 * 1024, Duration::from_millis(500))
            .await;

    assert!(
        response.is_none(),
        "Produce with acks=0 must not receive a response frame (Kafka spec); got {} bytes",
        response.as_ref().map_or(0, Bytes::len)
    );
}

#[tokio::test]
async fn e2e_produce_v3_acks_zero_malformed_topics_sends_no_response() {
    let (addr, _shutdown) = spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    // acks=0, claims one topic, no topic bytes - decode fails after acks is read.
    let body = build_produce_v3_body(0, 1);
    let frame = build_request_frame(API_KEY_PRODUCE, 3, 99, Some("review-test"), &body);
    stream
        .write_all(&frame)
        .await
        .expect("write produce acks=0 malformed");

    let response =
        read_response_frame_with_timeout(&mut stream, 8 * 1024 * 1024, Duration::from_millis(500))
            .await;

    assert!(
        response.is_none(),
        "Produce with acks=0 must stay silent even when the body is malformed; got {} bytes",
        response.as_ref().map_or(0, Bytes::len)
    );
}

#[tokio::test]
async fn e2e_produce_v3_acks_one_still_returns_response() {
    let (addr, _shutdown) = spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    let body = build_produce_v3_body(1, 0);
    let frame = build_request_frame(API_KEY_PRODUCE, 3, 43, Some("review-test"), &body);
    stream
        .write_all(&frame)
        .await
        .expect("write produce acks=1");

    let response =
        read_response_frame_with_timeout(&mut stream, 8 * 1024 * 1024, Duration::from_secs(2))
            .await
            .expect("Produce with acks=1 should receive a response");

    let (corr, resp_body) = parse_response_payload(API_KEY_PRODUCE, 3, response);
    assert_eq!(corr, 43);
    assert!(!resp_body.is_empty());
}

// ── ListOffsets v0 wire shape (old_style_offsets array, not bare i64) ───────

#[tokio::test]
async fn e2e_list_offsets_v0_unsupported_version_no_trailing_bytes() {
    let (addr, _shutdown) = spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    let request_body = build_list_offsets_v0_request_with_topic_t();
    let frame = build_request_frame(
        API_KEY_LIST_OFFSETS,
        0,
        7,
        Some("review-test"),
        &request_body,
    );
    stream
        .write_all(&frame)
        .await
        .expect("write list offsets v0");

    let payload =
        read_response_frame_with_timeout(&mut stream, 8 * 1024 * 1024, Duration::from_secs(2))
            .await
            .expect("ListOffsets v0 should still get an error response");

    let (_corr, body) = parse_response_payload(API_KEY_LIST_OFFSETS, 0, payload);
    let mut d = Decoder::new(body);
    assert_eq!(d.read_i32().unwrap(), 1);
    d.read_nullable_string().unwrap();
    assert_eq!(d.read_i32().unwrap(), 1);
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
    assert_eq!(d.remaining(), 0);
}

// ── Metadata topic name echo (must not hardcode a placeholder topic name) ──

#[tokio::test]
async fn e2e_metadata_v1_response_contains_requested_topic_name() {
    let (addr, _shutdown) = spawn_test_server().await;
    let topic = "orders";
    let request_body = build_metadata_legacy_request(&[topic]);
    let frame = build_request_frame(API_KEY_METADATA, 1, 9, Some("review-test"), &request_body);

    let mut stream = TcpStream::connect(addr).await.expect("connect");
    stream.write_all(&frame).await.expect("write metadata v1");

    let payload =
        read_response_frame_with_timeout(&mut stream, 8 * 1024 * 1024, Duration::from_secs(2))
            .await
            .expect("metadata response");

    let full_response = {
        let mut framed = BytesMut::with_capacity(4 + payload.len());
        framed.put_i32(i32::try_from(payload.len()).expect("metadata response fits i32"));
        framed.extend_from_slice(&payload);
        framed.freeze()
    };

    assert!(
        full_response
            .windows(topic.len())
            .any(|window| window == topic.as_bytes()),
        "metadata response must contain requested topic name {topic:?}; \
         placeholder-only responses break client topic matching"
    );
    assert!(
        !full_response
            .windows(b"unknown-topic".len())
            .any(|window| window == b"unknown-topic"),
        "metadata response must not substitute unknown-topic for requested names"
    );
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
async fn list_offsets_v1_through_v6_e2e_return_retriable_not_leader() {
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
            ERROR_NOT_LEADER_OR_FOLLOWER,
            "ListOffsets v{version} stub partition error"
        );
    }
}

// ── CreateTopics empty create ────────────────────────────────────────────────

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

// ── Fetch flexible boundary ──────────────────────────────────────────────────

#[tokio::test]
async fn fetch_v4_through_v12_e2e_preserve_correlation_id() {
    let (addr, _shutdown) = spawn_test_server().await;

    for version in 4i16..=12 {
        let body = load_fixture_body_or_skip(1, "Fetch", version)
            .unwrap_or_else(|| build_fetch_empty_topics_request(version));

        let correlation_id = 800 + i32::from(version);
        let (corr, resp) = round_trip(addr, API_KEY_FETCH, version, correlation_id, &body).await;
        assert_eq!(corr, correlation_id, "Fetch v{version} correlation");
        assert!(!resp.is_empty(), "Fetch v{version} response");
    }
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

// ── Out-of-scope API keys (SCOPE.md unsupported list) ───────────────────────

#[tokio::test]
async fn out_of_scope_api_keys_e2e_respond_then_close() {
    let (addr, _shutdown) = spawn_test_server().await;

    for &(api_key, name) in &OUT_OF_SCOPE_API_KEYS[..4] {
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        let frame = build_request_frame(api_key, 0, i32::from(api_key), Some("scope-test"), &[]);
        stream.write_all(&frame).await.expect("write oos key");

        let payload = read_response_frame(&mut stream, 8 * 1024 * 1024).await;
        let mut d = Decoder::new(parse_response_payload(api_key, 0, payload).1);
        assert_eq!(
            d.read_i16().unwrap(),
            ERROR_UNSUPPORTED_VERSION,
            "{name} (key {api_key})"
        );

        assert_eq!(
            read_byte_with_timeout(&mut stream, Duration::from_secs(2)).await,
            ByteRead::Closed,
            "{name} (key {api_key}) must close the connection after the error response"
        );
    }
}
