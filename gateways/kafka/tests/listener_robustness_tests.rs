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

//! TCP listener robustness — framing, pipelining, concurrency, edge cases.

#[path = "common/server.rs"]
mod server;
#[path = "common/tcp.rs"]
mod tcp;
#[path = "common/wire.rs"]
mod wire;

use std::time::Duration;

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time;

use iggy_gateway_kafka::ServerConfig;
use iggy_gateway_kafka::protocol::api::API_KEY_API_VERSIONS;
use iggy_gateway_kafka::protocol::codec::Decoder;

use server::{spawn_test_server, spawn_test_server_with_config};
use tcp::{
    build_request_frame, concat_frames, parse_response_payload, read_byte_with_timeout,
    read_response_frame, read_response_frame_with_timeout,
};

#[tokio::test]
async fn e2e_pipelined_requests_receive_responses_in_order() {
    let (addr, _shutdown) = spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    let frame1 = build_request_frame(API_KEY_API_VERSIONS, 1, 1, Some("pipe-test"), &[]);
    let frame2 = build_request_frame(API_KEY_API_VERSIONS, 1, 2, Some("pipe-test"), &[]);
    let frame3 = build_request_frame(API_KEY_API_VERSIONS, 1, 3, Some("pipe-test"), &[]);
    stream
        .write_all(&concat_frames(&[frame1, frame2, frame3]))
        .await
        .expect("pipelined write");

    for expected_corr in 1..=3 {
        let payload = read_response_frame(&mut stream, 8 * 1024 * 1024).await;
        let (corr, body) = parse_response_payload(API_KEY_API_VERSIONS, 1, payload);
        assert_eq!(corr, expected_corr);
        assert_eq!(Decoder::new(body).read_i16().unwrap(), 0);
    }
}

#[tokio::test]
async fn e2e_partial_length_prefix_then_remainder_accepted() {
    let (addr, _shutdown) = spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    let frame = build_request_frame(API_KEY_API_VERSIONS, 1, 42, Some("partial-test"), &[]);
    assert!(frame.len() > 6, "test frame long enough to split");

    stream.write_all(&frame[..2]).await.expect("partial prefix");
    time::sleep(Duration::from_millis(50)).await;
    stream.write_all(&frame[2..]).await.expect("remainder");

    let payload = read_response_frame(&mut stream, 8 * 1024 * 1024).await;
    let (corr, _) = parse_response_payload(API_KEY_API_VERSIONS, 1, payload);
    assert_eq!(corr, 42);
}

#[tokio::test]
async fn e2e_frame_within_custom_max_frame_size_accepted() {
    let max_frame = 512;
    let (addr, _shutdown) = spawn_test_server_with_config(ServerConfig {
        bind_addr: String::new(),
        advertised_host: None,
        advertised_port: None,
        max_frame_size: max_frame,
        read_timeout: Duration::from_secs(5),
        write_timeout: Duration::from_secs(5),
    })
    .await;

    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let frame = build_request_frame(API_KEY_API_VERSIONS, 1, 55, Some("max-frame-test"), &[]);
    assert!(
        frame.len() <= max_frame,
        "ApiVersions frame must fit test max ({max_frame})"
    );

    stream.write_all(&frame).await.expect("write");
    let payload = read_response_frame(&mut stream, max_frame).await;
    assert_eq!(
        parse_response_payload(API_KEY_API_VERSIONS, 1, payload).0,
        55
    );
}

#[tokio::test]
async fn e2e_frame_exceeding_max_frame_size_closes_connection() {
    let max_frame = 64;
    let (addr, _shutdown) = spawn_test_server_with_config(ServerConfig {
        bind_addr: String::new(),
        advertised_host: None,
        advertised_port: None,
        max_frame_size: max_frame,
        read_timeout: Duration::from_secs(5),
        write_timeout: Duration::from_secs(5),
    })
    .await;

    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let mut frame = BytesMut::new();
    frame.put_i32(200);
    frame.resize(4 + 200, 0);
    stream.write_all(&frame).await.expect("oversized frame");

    let byte = read_byte_with_timeout(&mut stream, Duration::from_secs(2)).await;
    assert!(
        byte.is_none(),
        "oversized frame should close connection (EOF)"
    );
}

#[tokio::test]
async fn e2e_truncated_frame_body_closes_connection() {
    let (addr, _shutdown) = spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    let full = build_request_frame(API_KEY_API_VERSIONS, 1, 66, Some("trunc-test"), &[]);
    let payload_len = i32::from_be_bytes([full[0], full[1], full[2], full[3]]) as usize;
    assert!(full.len() >= 4 + payload_len);

    stream
        .write_all(&full[..4 + payload_len / 2])
        .await
        .expect("half body");

    let byte = read_byte_with_timeout(&mut stream, Duration::from_secs(3)).await;
    assert!(byte.is_none(), "truncated body should close connection");
}

#[tokio::test]
async fn e2e_multiple_concurrent_connections_are_independent() {
    let (addr, _shutdown) = spawn_test_server().await;

    let (r1, r2, r3) = tokio::join!(
        tcp::round_trip(addr, API_KEY_API_VERSIONS, 1, 101, &[]),
        tcp::round_trip(addr, API_KEY_API_VERSIONS, 1, 102, &[]),
        tcp::round_trip(addr, API_KEY_API_VERSIONS, 1, 103, &[]),
    );

    assert_eq!(r1.0, 101);
    assert_eq!(r2.0, 102);
    assert_eq!(r3.0, 103);
}

#[tokio::test]
async fn e2e_client_disconnect_mid_frame_allows_new_connection() {
    let (addr, _shutdown) = spawn_test_server().await;

    {
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        let full = build_request_frame(API_KEY_API_VERSIONS, 1, 77, Some("abort-test"), &[]);
        stream.write_all(&full[..8]).await.expect("partial write");
        drop(stream);
    }

    time::sleep(Duration::from_millis(100)).await;

    let (corr, body) = tcp::round_trip(addr, API_KEY_API_VERSIONS, 1, 78, &[]).await;
    assert_eq!(corr, 78);
    assert_eq!(Decoder::new(body).read_i16().unwrap(), 0);
}

#[tokio::test]
async fn e2e_response_frames_have_positive_big_endian_length_prefix() {
    let (addr, _shutdown) = spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    let frame = build_request_frame(API_KEY_API_VERSIONS, 3, 200, Some("len-test"), &[]);
    stream.write_all(&frame).await.expect("write");

    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .expect("length prefix");
    let len = i32::from_be_bytes(len_buf);
    assert!(len > 0, "response length prefix must be positive");
    let body_len = usize::try_from(len).expect("positive length");
    let mut body = vec![0u8; body_len];
    stream.read_exact(&mut body).await.expect("response body");
    assert!(!body.is_empty());
}

#[tokio::test]
async fn e2e_zero_frame_length_closes_connection() {
    let (addr, _shutdown) = spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    stream
        .write_all(&0i32.to_be_bytes())
        .await
        .expect("zero len");
    let byte = read_byte_with_timeout(&mut stream, Duration::from_secs(2)).await;
    assert!(byte.is_none(), "zero frame length must close connection");
}

#[tokio::test]
async fn e2e_negative_frame_length_closes_connection() {
    let (addr, _shutdown) = spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    stream
        .write_all(&(-5_i32).to_be_bytes())
        .await
        .expect("negative len");
    let byte = read_byte_with_timeout(&mut stream, Duration::from_secs(2)).await;
    assert!(
        byte.is_none(),
        "negative frame length must close connection"
    );
}

#[tokio::test]
async fn e2e_slow_client_can_complete_request_within_read_timeout() {
    let (addr, _shutdown) = spawn_test_server_with_config(ServerConfig {
        bind_addr: String::new(),
        advertised_host: None,
        advertised_port: None,
        max_frame_size: 8 * 1024 * 1024,
        read_timeout: Duration::from_secs(5),
        write_timeout: Duration::from_secs(5),
    })
    .await;

    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let frame = build_request_frame(API_KEY_API_VERSIONS, 1, 301, Some("slow-test"), &[]);

    for chunk in frame.chunks(1) {
        stream.write_all(chunk).await.expect("byte drip");
        time::sleep(Duration::from_millis(5)).await;
    }

    let payload =
        read_response_frame_with_timeout(&mut stream, 8 * 1024 * 1024, Duration::from_secs(3))
            .await
            .expect("slow send should complete within read timeout");
    assert_eq!(
        parse_response_payload(API_KEY_API_VERSIONS, 1, payload).0,
        301
    );
}

#[tokio::test]
async fn e2e_many_sequential_requests_on_one_connection() {
    let (addr, _shutdown) = spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    for index in 0..20_i32 {
        let corr = 1_000 + index;
        let frame = build_request_frame(API_KEY_API_VERSIONS, 1, corr, Some("seq-stress"), &[]);
        stream.write_all(&frame).await.expect("write");
        let payload = read_response_frame(&mut stream, 8 * 1024 * 1024).await;
        assert_eq!(
            parse_response_payload(API_KEY_API_VERSIONS, 1, payload).0,
            corr
        );
    }
}

#[tokio::test]
async fn e2e_empty_client_id_request_succeeds() {
    let (addr, _shutdown) = spawn_test_server().await;
    let frame = build_request_frame(API_KEY_API_VERSIONS, 1, 400, None, &[]);
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    stream.write_all(&frame).await.expect("write");
    let payload = read_response_frame(&mut stream, 8 * 1024 * 1024).await;
    assert_eq!(
        parse_response_payload(API_KEY_API_VERSIONS, 1, payload).0,
        400
    );
}

#[tokio::test]
async fn e2e_flexible_apiversions_v3_request_succeeds() {
    let (addr, _shutdown) = spawn_test_server().await;
    let (corr, body) = tcp::round_trip(addr, API_KEY_API_VERSIONS, 3, 401, &[]).await;
    assert_eq!(corr, 401);
    let mut d = Decoder::new(body);
    assert_eq!(d.read_i16().unwrap(), 0);
    let count = usize::try_from(d.read_varint().unwrap() - 1).unwrap();
    assert_eq!(count, 6, "must advertise all six scoped API keys");
}

#[tokio::test]
async fn e2e_frame_payload_shorter_than_kafka_header_closes_connection() {
    let (addr, _shutdown) = spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    let mut frame = BytesMut::new();
    frame.put_i32(4);
    frame.extend_from_slice(&[0x00, 0x12, 0x00, 0x01]);
    stream.write_all(&frame).await.expect("short payload");

    let byte = read_byte_with_timeout(&mut stream, Duration::from_secs(2)).await;
    assert!(
        byte.is_none(),
        "payload shorter than 8-byte Kafka header must close connection"
    );
}

#[tokio::test]
async fn e2e_mixed_api_key_pipeline_returns_responses_in_order() {
    let (addr, _shutdown) = spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    use iggy_gateway_kafka::protocol::api::{API_KEY_METADATA, API_KEY_PRODUCE};

    let frames = [
        build_request_frame(API_KEY_API_VERSIONS, 1, 501, Some("mix-test"), &[]),
        build_request_frame(
            API_KEY_METADATA,
            0,
            502,
            Some("mix-test"),
            &tcp::build_metadata_legacy_request(&["pipe-topic"]),
        ),
        build_request_frame(
            API_KEY_PRODUCE,
            3,
            503,
            Some("mix-test"),
            &tcp::build_produce_v3_body(1, 0),
        ),
    ];
    stream
        .write_all(&concat_frames(&frames))
        .await
        .expect("mixed pipeline write");

    for (api_key, api_version, expected_corr) in [
        (API_KEY_API_VERSIONS, 1i16, 501),
        (API_KEY_METADATA, 0, 502),
        (API_KEY_PRODUCE, 3, 503),
    ] {
        let payload = read_response_frame(&mut stream, 8 * 1024 * 1024).await;
        assert_eq!(
            parse_response_payload(api_key, api_version, payload).0,
            expected_corr
        );
    }
}

#[tokio::test]
async fn e2e_connection_idle_after_response_accepts_next_request() {
    let (addr, _shutdown) = spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    let first = build_request_frame(API_KEY_API_VERSIONS, 1, 601, Some("idle-test"), &[]);
    stream.write_all(&first).await.expect("first write");
    let payload = read_response_frame(&mut stream, 8 * 1024 * 1024).await;
    assert_eq!(
        parse_response_payload(API_KEY_API_VERSIONS, 1, payload).0,
        601
    );

    time::sleep(Duration::from_secs(2)).await;

    let second = build_request_frame(API_KEY_API_VERSIONS, 1, 602, Some("idle-test"), &[]);
    stream.write_all(&second).await.expect("second write");
    let payload = read_response_frame(&mut stream, 8 * 1024 * 1024).await;
    assert_eq!(
        parse_response_payload(API_KEY_API_VERSIONS, 1, payload).0,
        602,
        "idle gap under read_timeout must not drop connection"
    );
}

#[tokio::test]
async fn e2e_flexible_metadata_v9_empty_topics_round_trip() {
    let (addr, _shutdown) = spawn_test_server().await;
    use iggy_gateway_kafka::protocol::api::API_KEY_METADATA;

    let body = wire::build_metadata_flexible_request(&[]);
    let (corr, resp) = tcp::round_trip(addr, API_KEY_METADATA, 9, 701, &body).await;
    assert_eq!(corr, 701);
    let mut d = Decoder::new(resp);
    d.read_i32().unwrap();
    let broker_count = usize::try_from(d.read_varint().unwrap())
        .unwrap()
        .saturating_sub(1);
    assert_eq!(broker_count, 1);
}
