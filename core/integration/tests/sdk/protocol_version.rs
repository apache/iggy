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

//! Negative tests for the login protocol-version gate. The Rust SDK always
//! sends a well-formed prefix, so the rejections are hand-crafted on a raw
//! TCP socket: `[256-byte RequestHeader][LoginRegisterRequest body]`. An
//! out-of-window version must be answered with a header-only `Eviction`
//! frame carrying `IncompatibleProtocol` plus the accepted window; a body
//! without a decodable prefix with `MalformedLogin` and a zero window.

#![cfg(feature = "vsr")]

use iggy::prelude::*;
use iggy_binary_protocol::codec::WireEncode;
use iggy_binary_protocol::consensus::{Command2, Operation, RequestHeader};
use iggy_binary_protocol::namespace::METADATA_CONSENSUS_NAMESPACE;
use iggy_binary_protocol::requests::users::LoginRegisterRequest;
use iggy_binary_protocol::{
    ClientVersionInfo, HEADER_SIZE, IGGY_PROTOCOL_VERSION, IGGY_PROTOCOL_VERSION_MIN, WireName,
};
use integration::harness::TestHarness;
use integration::iggy_harness;
use secrecy::SecretString;
use std::mem::offset_of;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// Wire bytes pinned to `EvictionReason` discriminants in `consensus::header`.
const EVICTION_REASON_INCOMPATIBLE_PROTOCOL: u8 = 14;
const EVICTION_REASON_MALFORMED_LOGIN: u8 = 15;

#[iggy_harness]
async fn given_incompatible_protocol_version_when_logging_in_should_receive_eviction(
    harness: &TestHarness,
) {
    let body = LoginRegisterRequest {
        version_info: ClientVersionInfo {
            protocol_version: 0,
            sdk_name: WireName::new("rust-sdk").unwrap(),
            sdk_version: WireName::new("0.0.1").unwrap(),
        },
        username: WireName::new(DEFAULT_ROOT_USERNAME).unwrap(),
        password: SecretString::from(DEFAULT_ROOT_PASSWORD),
        client_context: None,
    }
    .to_bytes();

    assert_login_evicted(
        harness,
        &body,
        EVICTION_REASON_INCOMPATIBLE_PROTOCOL,
        (IGGY_PROTOCOL_VERSION, IGGY_PROTOCOL_VERSION_MIN),
    )
    .await;
}

#[iggy_harness]
async fn given_no_version_prefix_when_logging_in_should_receive_eviction(harness: &TestHarness) {
    // Empty body cannot hold a ClientVersionInfo prefix; the gate rejects it
    // as malformed, window bytes zero.
    assert_login_evicted(harness, &[], EVICTION_REASON_MALFORMED_LOGIN, (0, 0)).await;
}

async fn assert_login_evicted(
    harness: &TestHarness,
    body: &[u8],
    expected_reason: u8,
    expected_window: (u32, u32),
) {
    let header = RequestHeader {
        command: Command2::Request,
        operation: Operation::Register,
        size: u32::try_from(HEADER_SIZE + body.len()).unwrap(),
        client: 0xC0FFEE,
        session: 0,
        request: 0,
        namespace: METADATA_CONSENSUS_NAMESPACE,
        ..Default::default()
    };

    let addr = harness
        .server()
        .tcp_addr()
        .expect("server must expose a TCP address");
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(bytemuck::bytes_of(&header)).await.unwrap();
    stream.write_all(body).await.unwrap();

    // Eviction is header-only: exactly 256 bytes.
    let mut reply = [0u8; HEADER_SIZE];
    stream.read_exact(&mut reply).await.unwrap();

    let command_offset = offset_of!(RequestHeader, command);
    assert_eq!(
        reply[command_offset],
        Command2::Eviction as u8,
        "expected an Eviction frame"
    );
    assert_eq!(
        reply[HEADER_SIZE - 1],
        expected_reason,
        "unexpected eviction reason"
    );
    // Window carved at fixed offsets 144 / 148; zero outside
    // IncompatibleProtocol.
    let server_version = u32::from_le_bytes(reply[144..148].try_into().unwrap());
    let server_version_min = u32::from_le_bytes(reply[148..152].try_into().unwrap());
    assert_eq!((server_version, server_version_min), expected_window);
}
