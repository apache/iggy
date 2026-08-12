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

//! Legacy login codes against server-ng (vsr). server-ng authenticates only
//! through the Register handshake, so the pre-register `LOGIN_USER` (38) and
//! `LOGIN_WITH_PERSONAL_ACCESS_TOKEN` (44) codes -- which the vsr SDK never
//! emits (its typed login methods send the register codes, its raw path
//! rejects session-control codes) -- must be rejected with a typed
//! `MalformedLogin` eviction, instead of the misleading `NoSession` eviction
//! the pre-auth guard would send unbound, or the silent empty-ok reply the
//! bound non-replicated path would send.
//! Since the SDK cannot send these codes, the frames are hand-crafted on a raw
//! TCP socket: a header-only non-replicated frame carrying the code in the
//! reserved command slot.

#![cfg(feature = "vsr")]

use iggy_binary_protocol::HEADER_SIZE;
use iggy_binary_protocol::codes::{LOGIN_USER_CODE, LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE};
use iggy_binary_protocol::consensus::{Command2, Operation, RequestHeader};
use iggy_binary_protocol::namespace::METADATA_CONSENSUS_NAMESPACE;
use integration::harness::TestHarness;
use integration::iggy_harness;
use std::mem::offset_of;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

// Wire byte pinned to `EvictionReason::MalformedLogin` in consensus::header.
const EVICTION_REASON_MALFORMED_LOGIN: u8 = 15;

#[iggy_harness]
async fn given_legacy_login_user_code_when_sent_raw_should_evict_malformed_login(
    harness: &TestHarness,
) {
    assert_legacy_login_code_evicted(harness, LOGIN_USER_CODE).await;
}

#[iggy_harness]
async fn given_legacy_pat_login_code_when_sent_raw_should_evict_malformed_login(
    harness: &TestHarness,
) {
    assert_legacy_login_code_evicted(harness, LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE).await;
}

/// Send a header-only non-replicated frame carrying `code` in the reserved
/// command slot and assert the server answers with a `MalformedLogin`
/// eviction. The reject runs before the session gate, so this unbound socket
/// exercises the same path a bound connection would.
async fn assert_legacy_login_code_evicted(harness: &TestHarness, code: u32) {
    let mut header = RequestHeader {
        command: Command2::Request,
        operation: Operation::NonReplicated,
        size: u32::try_from(HEADER_SIZE).unwrap(),
        // NonReplicated leaves session / request unchecked, but the header
        // validator still requires a nonzero client id.
        client: 0xC0FFEE,
        session: 0,
        request: 0,
        namespace: METADATA_CONSENSUS_NAMESPACE,
        ..Default::default()
    };
    // A non-replicated command code travels in the first 4 reserved bytes.
    header.reserved[..4].copy_from_slice(&code.to_le_bytes());

    let addr = harness
        .server()
        .tcp_addr()
        .expect("server must expose a TCP address");
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(bytemuck::bytes_of(&header)).await.unwrap();

    // Eviction is header-only: exactly 256 bytes. The timeout makes the
    // fail-fast contract explicit -- a regression that silently drops the
    // frame trips this instead of hanging until the test wall clock.
    let mut reply = [0u8; HEADER_SIZE];
    timeout(Duration::from_secs(5), stream.read_exact(&mut reply))
        .await
        .expect("server must answer a legacy login code within 5s, not stall")
        .expect("reading the eviction frame must succeed");

    let command_offset = offset_of!(RequestHeader, command);
    assert_eq!(
        reply[command_offset],
        Command2::Eviction as u8,
        "expected an Eviction frame for legacy login code {code}, not a Reply"
    );
    assert_eq!(
        reply[HEADER_SIZE - 1],
        EVICTION_REASON_MALFORMED_LOGIN,
        "legacy login code {code} must evict with MalformedLogin"
    );
}
