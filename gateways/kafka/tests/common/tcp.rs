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

//! TCP round-trip helpers — compiled into each integration test binary via `#[path]`.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time;

use iggy_gateway_kafka::protocol::codec::Decoder;
use iggy_gateway_kafka::protocol::header::{request_header_version, response_header_version};

/// Build a complete length-prefixed Kafka request frame (header + body).
pub fn build_request_frame(
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: Option<&str>,
    body: &[u8],
) -> Bytes {
    let hdr_ver = request_header_version(api_key, api_version);
    let mut enc = iggy_gateway_kafka::protocol::codec::Encoder::with_capacity(64 + body.len());
    enc.write_i16(api_key);
    enc.write_i16(api_version);
    enc.write_i32(correlation_id);
    if hdr_ver >= 2 {
        enc.write_compact_nullable_string(client_id);
        enc.write_empty_tagged_fields();
    } else {
        enc.write_nullable_string(client_id)
            .expect("test client_id fits i16");
    }
    enc.write_bytes(body);

    let payload = enc.freeze();
    let payload_len = i32::try_from(payload.len()).expect("test payload fits i32");
    let mut frame = BytesMut::with_capacity(4 + payload.len());
    frame.put_i32(payload_len);
    frame.extend_from_slice(&payload);
    frame.freeze()
}

/// Parse correlation id and response body from a raw response payload (no length prefix).
pub fn parse_response_payload(api_key: i16, api_version: i16, payload: Bytes) -> (i32, Bytes) {
    let resp_hdr_ver = response_header_version(api_key, api_version);
    let mut d = Decoder::new(payload);
    let correlation_id = d.read_i32().expect("correlation_id");
    if resp_hdr_ver >= 1 {
        d.read_tagged_fields().expect("response tagged fields");
    }
    let body = d.read_bytes(d.remaining()).expect("response body");
    (correlation_id, body)
}

/// Generous ceiling for the "default" response read. A server regression that drops a
/// response then becomes a bounded test failure instead of an indefinite hang.
const DEFAULT_RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);

/// Read one length-prefixed response frame from the stream.
///
/// Bounded by [`DEFAULT_RESPONSE_TIMEOUT`] so a dropped response fails fast instead of hanging.
pub async fn read_response_frame(stream: &mut TcpStream, max_size: usize) -> Bytes {
    time::timeout(
        DEFAULT_RESPONSE_TIMEOUT,
        read_response_frame_raw(stream, max_size),
    )
    .await
    .unwrap_or_else(|_| {
        panic!(
            "no response frame within {DEFAULT_RESPONSE_TIMEOUT:?} (server dropped the response?)"
        )
    })
}

async fn read_response_frame_raw(stream: &mut TcpStream, max_size: usize) -> Bytes {
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .expect("response length prefix");
    let frame_len_i32 = i32::from_be_bytes(len_buf);
    assert!(frame_len_i32 > 0, "response frame length must be positive");
    let frame_len = usize::try_from(frame_len_i32).expect("positive i32 frame length fits usize");
    assert!(
        frame_len <= max_size,
        "response frame too large: {frame_len}"
    );
    let mut buf = vec![0u8; frame_len];
    stream.read_exact(&mut buf).await.expect("response body");
    Bytes::from(buf)
}

/// Minimal Produce v3 body: nullable `transactional_id`, acks, timeout, empty topics array.
pub fn build_produce_v3_body(acks: i16, topics_count: i32) -> Bytes {
    let mut body = BytesMut::new();
    body.put_i16(-1); // null transactional_id
    body.put_i16(acks);
    body.put_i32(1_000); // timeout_ms
    body.put_i32(topics_count);
    body.freeze()
}

/// Legacy Metadata request body listing topic names (non-flexible, v0–v8).
pub fn build_metadata_legacy_request(topic_names: &[&str]) -> Bytes {
    let mut body = BytesMut::new();
    body.put_i32(i32::try_from(topic_names.len()).expect("topic name count fits i32"));
    for name in topic_names {
        let name_bytes = name.as_bytes();
        let len = i16::try_from(name_bytes.len()).expect("topic name fits i16");
        body.put_i16(len);
        body.extend_from_slice(name_bytes);
    }
    body.freeze()
}

/// Read one length-prefixed response frame, returning `None` on timeout.
pub async fn read_response_frame_with_timeout(
    stream: &mut TcpStream,
    max_size: usize,
    timeout: Duration,
) -> Option<Bytes> {
    time::timeout(timeout, read_response_frame_raw(stream, max_size))
        .await
        .ok()
}

/// Concatenate multiple length-prefixed frames (for pipelining tests).
pub fn concat_frames(frames: &[Bytes]) -> Bytes {
    let total: usize = frames.iter().map(Bytes::len).sum();
    let mut out = BytesMut::with_capacity(total);
    for frame in frames {
        out.extend_from_slice(frame);
    }
    out.freeze()
}

/// Outcome of a single-byte read, used by connection-close assertions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteRead {
    /// A byte was read from the stream.
    Byte(u8),
    /// The server closed the connection (clean EOF or reset).
    Closed,
    /// No byte arrived within the timeout; the connection is still open.
    Timeout,
}

/// Read one byte, distinguishing a server-side close from an idle timeout so that
/// "server must close the connection" tests can assert [`ByteRead::Closed`] explicitly
/// instead of passing on a mere stall.
pub async fn read_byte_with_timeout(stream: &mut TcpStream, timeout: Duration) -> ByteRead {
    let mut buf = [0u8; 1];
    match time::timeout(timeout, stream.read(&mut buf)).await {
        // 0 bytes = clean EOF; a reset / broken pipe is still a closed connection here.
        Ok(Ok(0) | Err(_)) => ByteRead::Closed,
        Ok(Ok(_)) => ByteRead::Byte(buf[0]),
        Err(_) => ByteRead::Timeout,
    }
}

/// Send one request frame and return parsed `(correlation_id, response_body)`.
pub async fn round_trip(
    addr: SocketAddr,
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    body: &[u8],
) -> (i32, Bytes) {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let frame = build_request_frame(
        api_key,
        api_version,
        correlation_id,
        Some("regression-test"),
        body,
    );
    stream.write_all(&frame).await.expect("write request");
    let payload = read_response_frame(&mut stream, 8 * 1024 * 1024).await;
    parse_response_payload(api_key, api_version, payload)
}
