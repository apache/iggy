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

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use iggy_gateway_kafka::protocol::api::{
    API_KEY_API_VERSIONS, API_KEY_METADATA, API_KEY_PRODUCE, ERROR_UNSUPPORTED_VERSION,
};
use iggy_gateway_kafka::protocol::codec::Decoder;

use fixtures::load_fixture_body;
use server::spawn_test_server;
use tcp::{build_request_frame, parse_response_payload, read_response_frame, round_trip};

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
    let body = load_fixture_body(0, "Produce", 3);
    let (corr, resp_body) = round_trip(addr, API_KEY_PRODUCE, 3, 88, &body).await;
    assert_eq!(corr, 88);
    assert!(!resp_body.is_empty());
}

#[tokio::test]
async fn e2e_unsupported_api_key_returns_error_without_disconnect() {
    let (addr, _shutdown) = spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let frame1 = build_request_frame(8, 2, 99, Some("e2e-test"), &[]);
    stream.write_all(&frame1).await.unwrap();
    let payload1 = read_response_frame(&mut stream, 8 * 1024 * 1024).await;
    let (corr, body) = parse_response_payload(8, 2, payload1);
    assert_eq!(corr, 99);
    let mut d = Decoder::new(body);
    assert_eq!(d.read_i16().unwrap(), ERROR_UNSUPPORTED_VERSION);

    // Second request on same connection must still work.
    let frame2 = build_request_frame(API_KEY_API_VERSIONS, 1, 100, Some("e2e-test"), &[]);
    stream.write_all(&frame2).await.unwrap();
    let payload2 = read_response_frame(&mut stream, 8 * 1024 * 1024).await;
    let (corr2, body2) = parse_response_payload(API_KEY_API_VERSIONS, 1, payload2);
    assert_eq!(corr2, 100);
    let mut d2 = Decoder::new(body2);
    assert_eq!(d2.read_i16().unwrap(), 0);
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

#[tokio::test]
async fn e2e_negative_frame_length_closes_connection() {
    let (addr, _shutdown) = spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(&(-1i32).to_be_bytes()).await.unwrap();

    let mut buf = [0u8; 1];
    let n = stream.read(&mut buf).await.unwrap_or(0);
    assert_eq!(n, 0, "server should close after invalid frame length");
}

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
