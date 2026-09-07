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

//! Legacy login codes against the server (vsr). The server authenticates only
//! through the Register handshake, so the pre-register `LOGIN_USER` (38) and
//! `LOGIN_WITH_PERSONAL_ACCESS_TOKEN` (44) codes -- which the vsr SDK never
//! emits (its typed login methods send the register codes, its raw path
//! rejects session-control codes) -- must be rejected with a typed
//! `MalformedLogin` eviction, instead of the generic `Unauthenticated` deny
//! reply the pre-auth guard would send unbound, or the silent empty-ok reply
//! the bound non-replicated path would send.
//! Since the SDK cannot send these codes, the frames are hand-crafted on a raw
//! TCP socket: a header-only non-replicated frame carrying the code in the
//! reserved command slot.

use iggy_binary_protocol::EvictionReason;
use iggy_binary_protocol::codes::{LOGIN_USER_CODE, LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE};
use iggy_binary_protocol::consensus::Command;
use integration::harness::TestHarness;
use integration::iggy_harness;

use crate::server::raw_tcp::{
    connect, eviction_reason, frame_command, non_replicated_header, read_frame_header, write_frame,
};

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
    // NonReplicated leaves session / request unchecked, but the header
    // validator still requires a nonzero client id.
    let header = non_replicated_header(0xC0FFEE, 0, 0, code);

    let mut stream = connect(harness).await;
    write_frame(&mut stream, &header, &[]).await;

    // Eviction is header-only: exactly 256 bytes. The bounded read makes the
    // fail-fast contract explicit -- a regression that silently drops the
    // frame trips this instead of hanging until the test wall clock.
    let reply = read_frame_header(&mut stream).await;

    assert_eq!(
        frame_command(&reply),
        Command::Eviction as u8,
        "expected an Eviction frame for legacy login code {code}, not a Reply"
    );
    assert_eq!(
        eviction_reason(&reply),
        EvictionReason::MalformedLogin as u8,
        "legacy login code {code} must evict with MalformedLogin"
    );
}
