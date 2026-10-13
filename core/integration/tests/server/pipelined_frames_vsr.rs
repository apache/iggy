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

//! Frames a client pipelines on one connection, against the server (vsr). A
//! read held for the metadata frontier runs in its own task, so a frame sent
//! behind it on the same connection gets its reply first.
//!
//! The frames are hand-crafted on a raw TCP socket, because the SDK waits for
//! each reply before it sends the next frame.

use iggy::prelude::*;
use iggy_binary_protocol::codes::{GET_ME_CODE, PING_CODE};
use iggy_binary_protocol::consensus::ReplyHeader;
use integration::iggy_harness;

use crate::server::raw_tcp::{
    connect, non_replicated_header, read_reply, register_root, write_frame,
};

/// The header validator requires a nonzero client id; the value is otherwise
/// free since nothing here reconnects.
const CLIENT_ID: u128 = 0x9_1BE;

/// No commit reaches this op, so a read that waits for it is held for its
/// whole budget and then fails retryable.
const UNREACHED_OP: u64 = u64::MAX;

const READ_REQUEST: u64 = 1;
const PING_REQUEST: u64 = 2;

#[iggy_harness]
async fn given_a_held_read_when_a_ping_follows_on_one_tcp_connection_should_answer_the_ping_first(
    harness: &TestHarness,
) {
    let mut stream = connect(harness).await;
    let session = register_root(&mut stream, CLIENT_ID).await;

    let mut read = non_replicated_header(CLIENT_ID, session, READ_REQUEST, GET_ME_CODE);
    read.minimum_metadata_op = UNREACHED_OP;
    write_frame(&mut stream, &read, &[]).await;
    let ping = non_replicated_header(CLIENT_ID, session, PING_REQUEST, PING_CODE);
    write_frame(&mut stream, &ping, &[]).await;

    let first = reply_header(&read_reply(&mut stream).await.0);
    assert_eq!(
        first.request, PING_REQUEST,
        "the ping must not wait for the held read"
    );
    assert_eq!(first.status, 0, "a served ping succeeds");
    let second = reply_header(&read_reply(&mut stream).await.0);
    assert_eq!(second.request, READ_REQUEST);
    assert_eq!(
        second.status,
        IggyError::TransientNotAccepted.as_code(),
        "a read held past its budget must fail retryable"
    );
}

fn reply_header(header: &[u8]) -> ReplyHeader {
    bytemuck::checked::try_pod_read_unaligned::<ReplyHeader>(header)
        .expect("a Reply header decodes")
}
