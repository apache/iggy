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

//! RED SPEC, expected to FAIL: client continuity across a primary SIGKILL.
//!
//! A producing SDK client pinned to the primary must, after the primary dies,
//! complete its next operation against the surviving quorum within a small
//! budget and without an authentication error. The SDK cannot: it has no
//! multi-endpoint failover. A client is built around a single
//! `server_address`, so it knows no other endpoint to dial; the transport's
//! fail-fast gate (auto-login disabled, the shape this harness client runs
//! with) returns errors without attempting a reconnect; and the
//! leader-redirect machinery that could reroute it needs a live connection to
//! read the cluster roster. Every retry therefore redials the dead endpoint
//! and fails with a connection error until the caller gives up. Surviving a
//! primary crash needs a seed roster of endpoints, not just a redirect.

use std::time::Duration;

use iggy::prelude::*;
use integration::harness::disk;
use integration::iggy_harness;
use tokio::time::sleep;

const STREAM_NAME: &str = "failover-stream";
const TOPIC_NAME: &str = "failover-topic";
const PARTITION_ID: u32 = 0;

/// Acks the pinned producer must capture before the kill, so the session is
/// warm and mid-stream rather than freshly connected.
const PRE_KILL_ACKS: usize = 20;

/// Budget for reaching the pre-kill ack count.
const WARMUP_TIMEOUT: Duration = Duration::from_secs(30);

/// The continuity contract under test: the client's next operation after the
/// primary dies must complete within this budget. Far above a survivor
/// election (a few seconds) and far below the transport timeout a hang burns.
const RESUME_BUDGET: Duration = Duration::from_secs(10);

const RETRY_PAUSE: Duration = Duration::from_millis(250);

fn build_message(payload: &str) -> IggyMessage {
    IggyMessage::builder()
        .payload(payload.to_owned().into())
        .build()
        .expect("build message")
}

/// A producing client pinned to the primary; SIGKILL the primary mid-stream;
/// the same client's next send must succeed against the surviving quorum
/// within `RESUME_BUDGET` and must never surface Unauthenticated.
// TODO(hubcio): fix this test
#[ignore = "SDK has no multi-endpoint failover; client redials the dead primary forever"]
#[iggy_harness(cluster_nodes = 3)]
async fn given_a_client_producing_when_its_primary_is_killed_should_resume_without_hang_or_unauthenticated(
    harness: &mut TestHarness,
) {
    let setup_client = harness.tcp_root_client().await.unwrap();
    setup_client
        .create_stream(STREAM_NAME)
        .await
        .expect("create stream");
    // Eagerly flushed so every pre-kill ack is durable on the survivors and
    // the post-kill send is a pure continuity question, not a durability one.
    let options = TopicCreateOptions {
        partitions_count: Some(1),
        message_expiry: Some(IggyExpiry::NeverExpire),
        messages_required_to_save: Some(1),
        enforce_fsync: Some(true),
        ..TopicCreateOptions::default()
    };
    setup_client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            &options,
        )
        .await
        .expect("create topic");
    drop(setup_client);

    // Pin the producing client to the primary's own endpoint, the way a
    // leader-aware SDK ends up connected to whichever node answers as leader.
    let leader = disk::leader_node_index(harness).await;
    let producer = harness
        .node(leader)
        .tcp_client()
        .expect("leader exposes a TCP endpoint")
        .with_root_login()
        .connect()
        .await
        .expect("connect the producer to the primary");

    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let partitioning = Partitioning::partition_id(PARTITION_ID);

    let warmup_deadline = tokio::time::Instant::now() + WARMUP_TIMEOUT;
    let mut acked = 0usize;
    while acked < PRE_KILL_ACKS {
        assert!(
            tokio::time::Instant::now() < warmup_deadline,
            "the pinned producer must reach {PRE_KILL_ACKS} acked sends before the kill"
        );
        let mut messages = vec![build_message(&format!("pre-kill-{acked:05}"))];
        let response = producer
            .send_messages(&stream, &topic, &partitioning, &mut messages)
            .await
            .expect("pre-kill sends against the live primary must succeed");
        assert!(
            !response.confirmations.is_empty(),
            "the VSR server confirms every send"
        );
        acked += 1;
    }

    harness.kill_node(leader).expect("SIGKILL the primary");

    // The contract: the SAME client completes a send within RESUME_BUDGET.
    // The first attempt is allowed to fail (its connection just died); what
    // is not allowed is burning the whole budget without one success.
    let resume_deadline = tokio::time::Instant::now() + RESUME_BUDGET;
    let mut attempt = 0usize;
    let mut last_error: Option<IggyError> = None;
    let resumed = loop {
        let now = tokio::time::Instant::now();
        if now >= resume_deadline {
            break false;
        }
        let mut messages = vec![build_message(&format!("post-kill-{attempt:05}"))];
        attempt += 1;
        let send = producer.send_messages(&stream, &topic, &partitioning, &mut messages);
        match tokio::time::timeout(resume_deadline - now, send).await {
            Ok(Ok(_)) => break true,
            Ok(Err(error)) => {
                last_error = Some(error);
                sleep(RETRY_PAUSE).await;
            }
            // Self-bound: an attempt that outlives the remaining budget ends
            // the loop instead of wedging the suite.
            Err(_elapsed) => break false,
        }
    };

    assert!(
        resumed,
        "a client pinned to a killed primary must complete its next operation against \
         the surviving quorum within {RESUME_BUDGET:?}, but the SDK has no \
         multi-endpoint failover: it holds only the dead node's server_address, its \
         fail-fast gate (auto-login disabled) surfaces errors without reconnecting, \
         and the leader redirect that could reroute it needs a live connection to \
         read the roster, so every retry redialed the dead endpoint \
         ({attempt} attempts, last error: {last_error:?})"
    );
}
