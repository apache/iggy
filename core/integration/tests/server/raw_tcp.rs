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

//! Raw TCP framing for the server suites that hand-craft client frames the
//! SDK cannot emit: connect to the harness server, write one request frame,
//! read the header the server answers with, and register root so a frame
//! can ride a bound session.

use std::mem::offset_of;
use std::time::Duration;

use iggy::prelude::*;
use iggy_binary_protocol::codec::{WireDecode, WireEncode};
use iggy_binary_protocol::consensus::{
    Command, Operation, ReplyHeader, RequestHeader, read_size_field, result_code,
    result_section_len,
};
use iggy_binary_protocol::requests::users::LoginRegisterRequest;
use iggy_binary_protocol::responses::users::LoginRegisterResponse;
use iggy_binary_protocol::{
    ClientVersionInfo, EvictionHeader, HEADER_SIZE, IGGY_PROTOCOL_VERSION, WireName,
};
use integration::harness::TestHarness;
use secrecy::SecretString;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Instant, sleep, timeout};

/// Per-frame reply wait. A server that drops the frame answers nothing at
/// all, so an unanswered read is a verdict, not a reason to wait longer.
const REPLY_WAIT: Duration = Duration::from_secs(5);

/// Budget for the register to commit: right after boot the single node may
/// still be electing itself and answers transient rejections meanwhile.
const COMMIT_BUDGET: Duration = Duration::from_secs(15);

const RETRY_PAUSE: Duration = Duration::from_millis(100);

pub(crate) async fn connect(harness: &TestHarness) -> TcpStream {
    let addr = harness
        .server()
        .tcp_addr()
        .expect("server must expose a TCP address");
    TcpStream::connect(addr).await.unwrap()
}

pub(crate) fn request_header(
    operation: Operation,
    client: u128,
    session: u64,
    request: u64,
    body_len: usize,
) -> RequestHeader {
    RequestHeader {
        command: Command::Request,
        operation,
        size: u32::try_from(HEADER_SIZE + body_len).unwrap(),
        client,
        session,
        request,
        ..Default::default()
    }
}

/// A header-only `NonReplicated` frame; the command code travels in the
/// first 4 reserved bytes.
pub(crate) fn non_replicated_header(
    client: u128,
    session: u64,
    request: u64,
    code: u32,
) -> RequestHeader {
    let mut header = request_header(Operation::NonReplicated, client, session, request, 0);
    header.reserved[..4].copy_from_slice(&code.to_le_bytes());
    header
}

pub(crate) async fn write_frame(stream: &mut TcpStream, header: &RequestHeader, body: &[u8]) {
    stream.write_all(bytemuck::bytes_of(header)).await.unwrap();
    if !body.is_empty() {
        stream.write_all(body).await.unwrap();
    }
}

/// Read the header of the next server frame, within [`REPLY_WAIT`].
pub(crate) async fn read_frame_header(stream: &mut TcpStream) -> [u8; HEADER_SIZE] {
    let mut header = [0u8; HEADER_SIZE];
    timeout(REPLY_WAIT, stream.read_exact(&mut header))
        .await
        .expect("server must answer within the reply wait, not stall")
        .expect("reply header read failed");
    header
}

/// Write one frame and read one Reply off the lockstep connection, the body
/// sized by the reply's size field.
pub(crate) async fn exchange(
    stream: &mut TcpStream,
    header: &RequestHeader,
    body: &[u8],
) -> ([u8; HEADER_SIZE], Vec<u8>) {
    write_frame(stream, header, body).await;
    let reply_header = read_frame_header(stream).await;
    let command = frame_command(&reply_header);
    assert_eq!(
        command,
        Command::Reply as u8,
        "expected a Reply frame, got command byte {command} (an Eviction carries reason {})",
        eviction_reason(&reply_header)
    );

    let total_size = read_size_field(&reply_header).expect("reply size field") as usize;
    // `read_size_field` is a bare 4-byte LE read with no floor (the SDK adds
    // its own guard, which this helper cannot inherit), so an under-sized
    // field would either panic on subtract overflow or wrap into a ~1.8e19
    // allocation and abort - either way hiding the regression under test.
    assert!(
        total_size >= HEADER_SIZE,
        "reply size field {total_size} is below the {HEADER_SIZE}-byte header"
    );
    let mut reply_body = vec![0u8; total_size - HEADER_SIZE];
    timeout(REPLY_WAIT, stream.read_exact(&mut reply_body))
        .await
        .expect("reply body timed out")
        .expect("reply body read failed");
    (reply_header, reply_body)
}

pub(crate) fn frame_command(header: &[u8; HEADER_SIZE]) -> u8 {
    header[offset_of!(RequestHeader, command)]
}

/// The typed reason byte of an `Eviction` frame, read by field offset rather
/// than by a hardcoded index: `EvictionReason` is renumberable, so a literal
/// would keep passing while checking a different reason.
pub(crate) fn eviction_reason(header: &[u8; HEADER_SIZE]) -> u8 {
    header[offset_of!(EvictionHeader, reason)]
}

pub(crate) fn reply_status(reply_header: &[u8; HEADER_SIZE]) -> u32 {
    let offset = offset_of!(ReplyHeader, status);
    u32::from_le_bytes(reply_header[offset..offset + 4].try_into().unwrap())
}

/// Root login/register on the socket as `client`, replayed on transient
/// rejections until it commits; returns the bound session id. The session
/// binds to THIS transport connection server-side, so a frame that must ride
/// it has to reuse the stream.
pub(crate) async fn register_root(stream: &mut TcpStream, client: u128) -> u64 {
    let body = LoginRegisterRequest {
        version_info: ClientVersionInfo {
            protocol_version: IGGY_PROTOCOL_VERSION,
            sdk_name: WireName::new("raw-tcp").unwrap(),
            sdk_version: WireName::new("0.0.1").unwrap(),
        },
        username: WireName::new(DEFAULT_ROOT_USERNAME).unwrap(),
        password: SecretString::from(DEFAULT_ROOT_PASSWORD),
        client_context: None,
    }
    .to_bytes();
    let header = request_header(Operation::Register, client, 0, 0, body.len());
    let deadline = Instant::now() + COMMIT_BUDGET;
    loop {
        let (reply_header, reply_body) = exchange(stream, &header, &body).await;
        // A pre-commit deny rides the status word; a committed verdict rides
        // the result section that leads a Register reply body.
        let code = match reply_status(&reply_header) {
            0 => result_code(&reply_body).expect("register reply must carry a result section"),
            status => status,
        };
        if code == 0 {
            let payload_start = result_section_len(&reply_body).unwrap();
            let response = LoginRegisterResponse::decode_from(&reply_body[payload_start..])
                .expect("register payload must decode");
            assert_ne!(response.session, 0, "server must bind a nonzero session");
            return response.session;
        }
        assert!(
            is_transient(code),
            "register rejected with a terminal code {code}"
        );
        assert!(
            Instant::now() < deadline,
            "register did not commit within {COMMIT_BUDGET:?}"
        );
        sleep(RETRY_PAUSE).await;
    }
}

fn is_transient(code: u32) -> bool {
    code == IggyError::TransientNotCommitted.as_code()
        || code == IggyError::TransientNotAccepted.as_code()
}
