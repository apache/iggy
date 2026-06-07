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

use std::time::Duration;

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use iggy_gateway_kafka::protocol::codec::Encoder;
use iggy_gateway_kafka::server::{read_frame, write_frame};

async fn tcp_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let client = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });
    let (server, _) = listener.accept().await.unwrap();
    let client = client.await.unwrap();
    (client, server)
}

#[tokio::test]
async fn read_frame_reads_valid_payload() {
    let (mut client, mut server) = tcp_pair().await;

    let mut enc = Encoder::with_capacity(64);
    enc.write_i16(18);
    enc.write_i16(3);
    enc.write_i32(123);
    enc.write_nullable_string(Some("test-client")).unwrap();
    let payload = enc.freeze();

    let mut frame = BytesMut::with_capacity(4 + payload.len());
    frame.extend_from_slice(&(payload.len() as i32).to_be_bytes());
    frame.extend_from_slice(&payload);
    client.write_all(&frame).await.unwrap();

    let parsed = read_frame(&mut server, 4096, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(parsed, payload);
}

#[tokio::test]
async fn write_frame_writes_length_prefixed_payload() {
    let (mut client, mut server) = tcp_pair().await;
    let payload = b"abc123";
    write_frame(&mut server, payload, Duration::from_secs(1))
        .await
        .unwrap();

    let mut len = [0u8; 4];
    client.read_exact(&mut len).await.unwrap();
    let len = i32::from_be_bytes(len) as usize;
    assert_eq!(len, payload.len());

    let mut body = vec![0u8; len];
    client.read_exact(&mut body).await.unwrap();
    assert_eq!(body, payload);
}

#[tokio::test]
async fn read_frame_rejects_invalid_lengths() {
    let (mut client, mut server) = tcp_pair().await;

    client.write_all(&0i32.to_be_bytes()).await.unwrap();
    let err = read_frame(&mut server, 128, Duration::from_secs(1))
        .await
        .expect_err("zero frame must fail");
    assert!(err.to_string().contains("invalid frame length"));

    // Ensure connection can still be reused for a second scenario by writing a valid new prefix+payload.
    let mut frame = BytesMut::new();
    frame.extend_from_slice(&(200i32).to_be_bytes());
    frame.resize(4 + 200, 0);
    client.write_all(&frame).await.unwrap();
    let err = read_frame(&mut server, 64, Duration::from_secs(1))
        .await
        .expect_err("large frame must fail");
    assert!(err.to_string().contains("exceeds max frame size"));
}

#[tokio::test]
async fn write_frame_length_prefix_is_big_endian() {
    let (mut client, mut server) = tcp_pair().await;
    write_frame(&mut server, &[1, 2, 3, 4], Duration::from_secs(1))
        .await
        .unwrap();

    let mut len_and_data = [0u8; 8];
    client.read_exact(&mut len_and_data).await.unwrap();
    let mut buf = &len_and_data[..];
    let len = buf.get_i32();
    assert_eq!(len, 4);
    assert_eq!(&len_and_data[4..], &[1, 2, 3, 4]);
}
