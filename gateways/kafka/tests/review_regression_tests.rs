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

//! Regression tests for PR #3519 review findings (atharvalade, Jul 2026).
//!
//! Each test encodes Kafka-client-correct behavior for a specific review finding and
//! guards against regressions now that the corresponding protocol/server fixes have landed.

#[path = "common/scope.rs"]
mod scope;
#[path = "common/server.rs"]
mod server;
#[path = "common/tcp.rs"]
mod tcp;
#[path = "common/wire.rs"]
mod wire;

use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time;

use iggy_gateway_kafka::ServerConfig;
use iggy_gateway_kafka::protocol::api::{
    API_KEY_API_VERSIONS, API_KEY_LIST_OFFSETS, API_KEY_METADATA, API_KEY_PRODUCE,
    ERROR_UNKNOWN_TOPIC_OR_PARTITION, ERROR_UNSUPPORTED_VERSION, handle_request,
};
use iggy_gateway_kafka::protocol::codec::Decoder;

use scope::default_broker;
use server::{spawn_test_server, spawn_test_server_with_config};
use tcp::{
    build_metadata_legacy_request, build_produce_v3_body, build_request_frame,
    parse_response_payload, read_response_frame_with_timeout,
};
use wire::build_metadata_flexible_request_v10;

// ── Produce acks=0 (review: broker must stay silent) ─────────────────────────

#[test]
fn produce_acks_zero_malformed_body_decode_carries_acks() {
    use iggy_gateway_kafka::protocol::requests::{ProduceDecodeResult, decode_produce_request};

    let body = build_produce_v3_body(0, 1);
    match decode_produce_request(3, body.clone()) {
        ProduceDecodeResult::Err { acks: Some(0), .. } => {}
        other => panic!("expected decode error with acks=0, got {other:?}"),
    }
    assert!(
        handle_request(API_KEY_PRODUCE, 3, body, &default_broker()).is_none(),
        "handler must not respond when acks=0 even if decode fails after acks"
    );
}

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

    // acks=0, claims one topic, no topic bytes — decode fails after acks is read.
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

// ── ListOffsets v0 wire shape (review: old_style_offsets array, not bare i64) ─

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
        .expect("test request has acks != 0 and expects a response");
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
        .expect("test request has acks != 0 and expects a response");
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

/// `ListOffsets` v0 request below firewall min (mirrors atharvalade repro script body).
fn build_list_offsets_v0_request_with_topic_t() -> Bytes {
    let mut body = BytesMut::new();
    body.put_i32(-1); // replica_id
    body.put_i32(1); // topics array length
    body.put_i16(1); // topic name length
    body.put_u8(b't');
    body.put_i32(1); // partitions array length
    body.put_i32(0); // partition index
    body.put_i64(-1); // timestamp
    body.put_i32(1); // max_num_offsets
    body.freeze()
}

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
    parse_list_offsets_v0_partition(&mut d);
    assert_eq!(d.remaining(), 0);
}

// ── Metadata topic name echo (review: must not hardcode "unknown-topic") ────

#[test]
fn metadata_v10_unsupported_decodes_client_body_not_clamped_version() {
    let body = handle_request(
        API_KEY_METADATA,
        10,
        build_metadata_flexible_request_v10(&["payments"]),
        &default_broker(),
    )
    .expect("test request has acks != 0 and expects a response");

    let mut d = Decoder::new(body);
    d.read_i32().unwrap(); // throttle_time_ms
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
    assert_eq!(
        usize::try_from(d.read_varint().unwrap())
            .unwrap()
            .saturating_sub(1),
        1
    );
    assert_eq!(d.read_i16().unwrap(), ERROR_UNSUPPORTED_VERSION);
    assert_eq!(
        d.read_compact_nullable_string()
            .unwrap()
            .expect("topic name"),
        "payments"
    );
}

fn read_metadata_v1_topics(d: &mut Decoder, expected_count: i32) -> Vec<String> {
    let _brokers_count = d.read_i32().unwrap();
    d.read_i32().unwrap(); // node_id
    d.read_nullable_string().unwrap(); // host
    d.read_i32().unwrap(); // port
    d.read_nullable_string().unwrap(); // rack (v1+)
    d.read_i32().unwrap(); // controller_id (v1+)

    assert_eq!(d.read_i32().unwrap(), expected_count);
    let mut names = Vec::with_capacity(usize::try_from(expected_count).unwrap_or(0));
    for _ in 0..expected_count {
        d.read_i16().unwrap(); // topic_error
        names.push(d.read_nullable_string().unwrap().expect("topic name"));
        d.read_bool().unwrap(); // is_internal (v1+)
        assert_eq!(d.read_i32().unwrap(), 0, "empty partitions array");
    }
    names
}

#[test]
fn metadata_v1_echoes_requested_topic_name_in_response() {
    let topic = "orders";
    let request = build_metadata_legacy_request(&[topic]);
    let body = handle_request(API_KEY_METADATA, 1, request, &default_broker())
        .expect("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(body);

    let names = read_metadata_v1_topics(&mut d, 1);
    assert_eq!(names, vec![topic.to_string()]);
    assert_eq!(d.remaining(), 0);
}

#[test]
fn metadata_v1_unknown_topic_returns_error_with_requested_name() {
    let topic = "orders";
    let request = build_metadata_legacy_request(&[topic]);
    let body = handle_request(API_KEY_METADATA, 1, request, &default_broker())
        .expect("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(body);

    let _brokers_count = d.read_i32().unwrap();
    d.read_i32().unwrap();
    d.read_nullable_string().unwrap();
    d.read_i32().unwrap();
    d.read_nullable_string().unwrap();
    d.read_i32().unwrap();

    assert_eq!(d.read_i32().unwrap(), 1);
    assert_eq!(
        d.read_i16().unwrap(),
        ERROR_UNKNOWN_TOPIC_OR_PARTITION,
        "unknown topic should surface error 3"
    );
    assert_eq!(
        d.read_nullable_string().unwrap().as_deref(),
        Some(topic),
        "response must echo requested topic name, not a placeholder"
    );
}

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

// ── Idle connection handling (review: read_timeout must not act as idle cap) ─

#[tokio::test]
async fn e2e_quiet_connection_accepts_request_after_short_idle() {
    let (addr, _shutdown) = spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    time::sleep(Duration::from_secs(2)).await;

    let frame = build_request_frame(API_KEY_API_VERSIONS, 1, 501, Some("idle-test"), &[]);
    stream
        .write_all(&frame)
        .await
        .expect("connection should stay open after short idle");

    let payload =
        read_response_frame_with_timeout(&mut stream, 8 * 1024 * 1024, Duration::from_secs(2))
            .await
            .expect("request after short idle should succeed");

    let (corr, _) = parse_response_payload(API_KEY_API_VERSIONS, 1, payload);
    assert_eq!(corr, 501);
}

#[tokio::test]
async fn e2e_quiet_connection_survives_beyond_read_timeout_idle_cap() {
    let (addr, _shutdown) = spawn_test_server_with_config(ServerConfig {
        bind_addr: String::new(),
        advertised_host: None,
        advertised_port: None,
        max_frame_size: 8 * 1024 * 1024,
        read_timeout: Duration::from_secs(3),
        write_timeout: Duration::from_secs(5),
    })
    .await;

    let mut stream = TcpStream::connect(addr).await.expect("connect");

    // Idle longer than read_timeout: prefix wait has no timer; connection stays open.
    time::sleep(Duration::from_secs(4)).await;

    let frame = build_request_frame(API_KEY_API_VERSIONS, 1, 502, Some("idle-test"), &[]);
    stream
        .write_all(&frame)
        .await
        .expect("idle connection should remain writable after read_timeout elapses");

    let payload =
        read_response_frame_with_timeout(&mut stream, 8 * 1024 * 1024, Duration::from_secs(2))
            .await
            .expect(
                "request after idle period longer than read_timeout should succeed \
                 (read_timeout applies to in-flight frame body only)",
            );

    let (corr, _) = parse_response_payload(API_KEY_API_VERSIONS, 1, payload);
    assert_eq!(corr, 502);
}
