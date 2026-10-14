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
use iggy_binary_protocol::codes::{GET_ME_CODE, GET_STREAM_CODE, PING_CODE, POLL_MESSAGES_CODE};
use iggy_binary_protocol::consensus::{Operation, ReplyHeader, result_code};
use iggy_binary_protocol::requests::consumer_offsets::StoreConsumerOffsetRequest;
use iggy_binary_protocol::requests::messages::PollMessagesRequest;
use iggy_binary_protocol::requests::streams::{CreateStreamRequest, GetStreamRequest};
use iggy_binary_protocol::responses::streams::GetStreamResponse;
use iggy_binary_protocol::{
    AckLevel, HEADER_SIZE, WireConsumer, WireDecode, WireEncode, WireIdentifier, WireName,
    WireOptions, WirePollingStrategy,
};
use integration::harness::{TestBinary, TestHarness};
use integration::iggy_harness;

use crate::server::raw_tcp::{
    connect, non_replicated_header, partition_context, read_reply, register_root, request_header,
    write_frame,
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

#[iggy_harness(cluster_nodes = 3)]
async fn given_a_pending_metadata_write_when_a_read_and_ping_follow_should_order_the_read_and_serve_ping(
    harness: &TestHarness,
) {
    const NAME: &str = "pipelined-created-stream";
    const CREATE: u64 = 1;
    const READ: u64 = 2;
    const PING: u64 = 3;
    let mut socket = connect(harness).await;
    let session = register_root(&mut socket, CLIENT_ID).await;
    for node in 1..harness.cluster_size() {
        signal_node(harness, node, libc::SIGSTOP);
    }
    let create = CreateStreamRequest {
        name: WireName::new(NAME).unwrap(),
        options: WireOptions::empty(),
    }
    .to_bytes();
    let header = request_header(
        Operation::CreateStream,
        CLIENT_ID,
        session,
        CREATE,
        create.len(),
    );
    write_frame(&mut socket, &header, &create).await;
    let read = GetStreamRequest {
        stream_id: WireIdentifier::named(NAME).unwrap(),
    }
    .to_bytes();
    let mut header = non_replicated_header(CLIENT_ID, session, READ, GET_STREAM_CODE);
    header.size = u32::try_from(HEADER_SIZE + read.len()).unwrap();
    write_frame(&mut socket, &header, &read).await;
    write_frame(
        &mut socket,
        &non_replicated_header(CLIENT_ID, session, PING, PING_CODE),
        &[],
    )
    .await;
    let first = reply_header(&read_reply(&mut socket).await.0);
    for node in 1..harness.cluster_size() {
        signal_node(harness, node, libc::SIGCONT);
    }
    assert_eq!(
        first.request, PING,
        "PING must bypass both the blocked write and its dependent read"
    );
    assert_eq!(first.status, 0);
    for _ in 0..2 {
        let (header, body) = read_reply(&mut socket).await;
        let reply = reply_header(&header);
        assert_eq!(reply.status, 0);
        match reply.request {
            CREATE => assert_eq!(result_code(&body), Some(0)),
            READ => assert_eq!(
                GetStreamResponse::decode_from(&body)
                    .unwrap()
                    .stream
                    .name
                    .as_str(),
                NAME
            ),
            request => panic!("unexpected reply {request}"),
        }
    }
}

#[iggy_harness(cluster_nodes = 1)]
async fn given_two_pipelined_next_polls_when_auto_committing_should_return_distinct_messages(
    harness: &mut TestHarness,
) {
    let context = seed_poll_messages(harness).await;
    let mut socket = connect(harness).await;
    let session = register_root(&mut socket, CLIENT_ID).await;
    let body = next_poll().to_bytes();
    for request in 1..=2 {
        let mut header = non_replicated_header(CLIENT_ID, session, request, POLL_MESSAGES_CODE);
        header.size = u32::try_from(HEADER_SIZE + body.len()).unwrap();
        header.partition_incarnation = context.incarnation;
        header.minimum_metadata_op = context.metadata_op;
        write_frame(&mut socket, &header, &body).await;
    }
    for offset in 0..2 {
        let (header, body) = read_reply(&mut socket).await;
        let reply = reply_header(&header);
        assert_eq!(reply.status, 0);
        assert_eq!(reply.request, offset + 1);
        let polled = PolledMessages::from_bytes(body.into()).unwrap();
        assert_eq!(polled.messages.len(), 1);
        assert_eq!(polled.messages[0].header.offset, offset);
    }
}

#[iggy_harness(cluster_nodes = 1)]
async fn given_a_pipelined_poll_then_store_when_both_complete_should_preserve_consumer_progress(
    harness: &mut TestHarness,
) {
    let context = seed_poll_messages(harness).await;
    let mut socket = connect(harness).await;
    let session = register_root(&mut socket, CLIENT_ID).await;
    let poll = next_poll();
    let body = poll.to_bytes();
    let mut header = non_replicated_header(CLIENT_ID, session, 1, POLL_MESSAGES_CODE);
    header.size = u32::try_from(HEADER_SIZE + body.len()).unwrap();
    header.partition_incarnation = context.incarnation;
    header.minimum_metadata_op = context.metadata_op;
    write_frame(&mut socket, &header, &body).await;
    let store = StoreConsumerOffsetRequest {
        consumer: poll.consumer,
        stream_id: poll.stream_id,
        topic_id: poll.topic_id,
        partition_id: Some(0),
        offset: 1,
        ack: AckLevel::Quorum,
    }
    .to_bytes();
    let mut header = request_header(
        Operation::StoreConsumerOffset,
        CLIENT_ID,
        session,
        2,
        store.len(),
    );
    header.partition_incarnation = context.incarnation;
    header.minimum_metadata_op = context.metadata_op;
    write_frame(&mut socket, &header, &store).await;
    let (header, body) = read_reply(&mut socket).await;
    let reply = reply_header(&header);
    assert_eq!(
        reply.request, 1,
        "the poll must finish before its following store"
    );
    assert_eq!(reply.status, 0);
    let polled = PolledMessages::from_bytes(body.into()).unwrap();
    assert_eq!(polled.messages.len(), 1);
    assert_eq!(polled.messages[0].header.offset, 0);
    let (header, _) = read_reply(&mut socket).await;
    let reply = reply_header(&header);
    assert_eq!(reply.request, 2);
    assert_eq!(reply.status, 0);
    let client = harness.tcp_root_client().await.unwrap();
    let stored = client
        .get_consumer_offset(
            &Consumer::new(Identifier::numeric(1).unwrap()),
            &Identifier::named(STREAM).unwrap(),
            &Identifier::named(TOPIC).unwrap(),
            Some(0),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.stored_offset, 1);
}

const STREAM: &str = "pipelined-stream";
const TOPIC: &str = "pipelined-topic";

async fn seed_poll_messages(harness: &mut TestHarness) -> PartitionContext {
    let client = harness.tcp_root_client().await.unwrap();
    let stream = Identifier::named(STREAM).unwrap();
    let topic = Identifier::named(TOPIC).unwrap();
    client.create_stream(STREAM).await.unwrap();
    client
        .create_topic(
            &stream,
            TOPIC,
            &TopicCreateOptions {
                partitions_count: Some(1),
                messages_required_to_save: Some(1),
                durability: Durability::Persisted,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let mut messages = (0..3)
        .map(|value| {
            IggyMessage::builder()
                .payload(format!("message-{value}").into())
                .build()
                .unwrap()
        })
        .collect::<Vec<_>>();
    client
        .send_messages(
            &stream,
            &topic,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .unwrap();
    drop(client);
    // Recovery leaves the messages on disk, so polling must yield for I/O.
    harness.restart_server().await.unwrap();
    let client = harness.tcp_root_client().await.unwrap();
    partition_context(&client, &stream, &topic, 0).await
}

fn next_poll() -> PollMessagesRequest {
    PollMessagesRequest {
        consumer: WireConsumer::consumer(WireIdentifier::numeric(1)),
        stream_id: WireIdentifier::named(STREAM).unwrap(),
        topic_id: WireIdentifier::named(TOPIC).unwrap(),
        partition_id: Some(0),
        strategy: WirePollingStrategy::next(),
        count: 1,
        auto_commit: true,
    }
}

fn signal_node(harness: &TestHarness, node: usize, signal: libc::c_int) {
    let pid = harness.node(node).pid().expect("the node is running");
    // SAFETY: the signal targets a child owned by the harness and not yet reaped.
    assert_eq!(unsafe { libc::kill(pid as libc::pid_t, signal) }, 0);
}
