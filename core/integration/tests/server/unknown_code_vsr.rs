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

//! Requests the server has no handler for, against the server (vsr). Each
//! must get a typed `InvalidCommand` deny Reply, because the client decodes
//! replies in lockstep and silence would stall its connection.
//!
//! Armless non-replicated command codes: the read gate is total over the
//! protocol command table, so a bound session sending a `NonReplicated`
//! header whose reserved command slot carries a code the reads have no arm
//! for must be denied. Two codes pin the two halves. One no table entry
//! claims (the SDK forwards unknown codes untouched, `COMMAND_TABLE` being a
//! registry rather than a capability list): the shared response builder's
//! catch-all already denied it `InvalidCommand`, so that test pins the
//! pre-existing deny now that the gate owns it. Retired codes are unknown in
//! the same way. One the table lists but no read serves (`LOGOUT_USER`, which
//! the SDK only ever sends as `Operation::Logout`): the old gate fell open
//! and let the builder acknowledge it empty-ok, as if a logout had happened,
//! so that test pins the closed fail-open.
//!
//! An operation byte `Operation` does not declare fails the typed header
//! decode, so the funnel answers it before any gate. The deny must echo the
//! byte and the request id, which clients match a reply by, and the
//! connection must keep serving the session.
//!
//! The frames are hand-crafted on a raw TCP socket to pin the status word the
//! SDK maps through `IggyError::from_code`.

use std::mem::offset_of;

use iggy::prelude::*;
use iggy_binary_protocol::codes::{GET_ME_CODE, LOGOUT_USER_CODE};
use iggy_binary_protocol::consensus::{Operation, ReplyHeader, RequestHeader};
use iggy_binary_protocol::{HEADER_SIZE, lookup_command};
use integration::harness::TestHarness;
use integration::iggy_harness;

use crate::server::raw_tcp::{
    connect, exchange, exchange_raw, non_replicated_header, register_root, reply_status,
    request_header,
};

/// A code no `COMMAND_TABLE` entry claims.
const UNKNOWN_CODE: u32 = 9999;

/// Retired command codes, as `codes.rs` records them: 205 was `PURGE_STREAM`
/// and 305 was `PURGE_TOPIC`. A client whose table no longer lists a code
/// sends it as `NonReplicated`, like any code it does not know.
const RETIRED_STREAM_CODE: u32 = 205;
const RETIRED_TOPIC_CODE: u32 = 305;

/// An operation byte `Operation` does not declare.
/// `reserved_codes_remain_unknown` pins 131 as never reused, so a new
/// operation cannot claim it and turn this frame into a valid request.
const UNDECLARED_OPERATION: u8 = 131;

/// Opaque body: nothing decodes it, but the server must consume the whole
/// frame for the next request to stay aligned.
const UNDECLARED_BODY: [u8; 16] = [0xA5; 16];

/// Nonzero, so an echoed stamp is distinguishable from a zeroed field. The
/// server cannot verify it without a typed header, so any value is valid.
const UNDECLARED_REQUEST_CHECKSUM: u128 = 0x0123_4567_89AB_CDEF;

const UNDECLARED_REQUEST: u64 = 1;
const NEXT_REQUEST: u64 = 2;

/// The header validator requires a nonzero client id; the value is otherwise
/// free since nothing here reconnects.
const CLIENT_ID: u128 = 0xBAD_C0DE;

#[iggy_harness]
async fn given_bound_session_when_unknown_non_replicated_code_sent_should_deny_invalid_command(
    harness: &TestHarness,
) {
    assert!(
        lookup_command(UNKNOWN_CODE).is_none(),
        "test needs a code absent from COMMAND_TABLE"
    );
    assert_non_replicated_code_denied_invalid_command(harness, UNKNOWN_CODE).await;
}

#[iggy_harness]
async fn given_bound_session_when_table_listed_code_without_read_arm_sent_should_deny_invalid_command(
    harness: &TestHarness,
) {
    assert!(
        lookup_command(LOGOUT_USER_CODE).is_some_and(|meta| !meta.is_replicated()),
        "test needs a non-replicated COMMAND_TABLE entry"
    );
    assert_non_replicated_code_denied_invalid_command(harness, LOGOUT_USER_CODE).await;
}

#[iggy_harness]
async fn given_bound_session_when_retired_stream_code_sent_should_deny_invalid_command(
    harness: &TestHarness,
) {
    assert!(
        lookup_command(RETIRED_STREAM_CODE).is_none(),
        "test needs a retired code absent from COMMAND_TABLE"
    );
    assert_non_replicated_code_denied_invalid_command(harness, RETIRED_STREAM_CODE).await;
}

#[iggy_harness]
async fn given_bound_session_when_retired_topic_code_sent_should_deny_invalid_command(
    harness: &TestHarness,
) {
    assert!(
        lookup_command(RETIRED_TOPIC_CODE).is_none(),
        "test needs a retired code absent from COMMAND_TABLE"
    );
    assert_non_replicated_code_denied_invalid_command(harness, RETIRED_TOPIC_CODE).await;
}

#[iggy_harness]
async fn given_bound_session_when_undeclared_operation_sent_should_deny_invalid_command_and_keep_the_connection(
    harness: &TestHarness,
) {
    assert!(
        !Operation::is_known_code(UNDECLARED_OPERATION),
        "test needs an operation byte this build does not declare"
    );
    let mut stream = connect(harness).await;
    let session = register_root(&mut stream, CLIENT_ID).await;

    // `Reserved` only holds the place of the undeclared byte: a frame that
    // kept it would be dropped, not denied.
    let mut header = request_header(
        Operation::Reserved,
        CLIENT_ID,
        session,
        UNDECLARED_REQUEST,
        UNDECLARED_BODY.len(),
    );
    header.request_checksum = UNDECLARED_REQUEST_CHECKSUM;
    let mut frame_header: [u8; HEADER_SIZE] = bytemuck::cast(header);
    frame_header[offset_of!(RequestHeader, operation)] = UNDECLARED_OPERATION;
    let (mut reply_header, reply_body) =
        exchange_raw(&mut stream, &frame_header, &UNDECLARED_BODY).await;

    let operation_offset = offset_of!(ReplyHeader, operation);
    assert_eq!(
        reply_header[operation_offset], UNDECLARED_OPERATION,
        "the deny must echo the request's own operation byte"
    );
    // A declared byte in its place lets the typed view check the other fields.
    reply_header[operation_offset] = Operation::Reserved as u8;
    let reply = bytemuck::checked::try_pod_read_unaligned::<ReplyHeader>(&reply_header)
        .expect("the deny decodes once its operation byte is declared");
    assert_eq!(
        reply.status,
        IggyError::InvalidCommand.as_code(),
        "an undeclared operation must be denied InvalidCommand"
    );
    assert!(
        reply_body.is_empty(),
        "a deny Reply carries an empty body, got {} bytes",
        reply_body.len()
    );
    assert_eq!(
        reply.request, UNDECLARED_REQUEST,
        "the deny must echo the request id"
    );
    assert_eq!(
        reply.request_checksum, UNDECLARED_REQUEST_CHECKSUM,
        "the deny must echo the request checksum"
    );
    assert_eq!(
        reply.commit, 0,
        "the deny must not disclose the commit frontier"
    );

    let next = non_replicated_header(CLIENT_ID, session, NEXT_REQUEST, GET_ME_CODE);
    let (next_header, _) = exchange(&mut stream, &next, &[]).await;
    let next_reply = bytemuck::checked::try_pod_read_unaligned::<ReplyHeader>(&next_header)
        .expect("the next reply decodes");
    assert_eq!(
        next_reply.status, 0,
        "the connection must keep serving its bound session after the deny"
    );
    assert_eq!(
        next_reply.request, NEXT_REQUEST,
        "the next reply must answer the next request"
    );
}

/// Register root on a raw socket, send a header-only `NonReplicated` frame
/// carrying `code` in the reserved command slot on that bound connection, and
/// assert the server answers with an `InvalidCommand` deny Reply.
async fn assert_non_replicated_code_denied_invalid_command(harness: &TestHarness, code: u32) {
    let mut stream = connect(harness).await;
    let session = register_root(&mut stream, CLIENT_ID).await;

    let header = non_replicated_header(CLIENT_ID, session, 1, code);
    let (reply_header, reply_body) = exchange(&mut stream, &header, &[]).await;

    assert_eq!(
        reply_status(&reply_header),
        IggyError::InvalidCommand.as_code(),
        "a bound session sending non-replicated code {code} must be denied InvalidCommand"
    );
    assert!(
        reply_body.is_empty(),
        "a deny Reply carries an empty body, got {} bytes",
        reply_body.len()
    );
}
