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

//! Armless non-replicated command codes against the server (vsr). The read
//! gate is total over the protocol command table, so a bound session sending
//! a `NonReplicated` header whose reserved command slot carries a code the
//! reads have no arm for must get a typed `InvalidCommand` deny Reply. Two
//! codes pin the two halves. One no table entry claims (the SDK forwards
//! unknown codes untouched, `COMMAND_TABLE` being a registry rather than a
//! capability list): the shared response builder's catch-all already denied
//! it `InvalidCommand`, so that test pins the pre-existing deny now that the
//! gate owns it. One the table lists but no read serves (`LOGOUT_USER`, which
//! the SDK only ever sends as `Operation::Logout`): the old gate fell open
//! and let the builder acknowledge it empty-ok, as if a logout had happened,
//! so that test pins the closed fail-open. The frames are hand-crafted on a
//! raw TCP socket to pin the status word the SDK maps through
//! `IggyError::from_code`.

use iggy::prelude::*;
use iggy_binary_protocol::codes::LOGOUT_USER_CODE;
use iggy_binary_protocol::lookup_command;
use integration::harness::TestHarness;
use integration::iggy_harness;

use crate::server::raw_tcp::{
    connect, exchange, non_replicated_header, register_root, reply_status,
};

/// A code no `COMMAND_TABLE` entry claims.
const UNKNOWN_CODE: u32 = 9999;

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
