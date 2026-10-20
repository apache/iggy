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

//! Client continuity across a primary SIGKILL.
//!
//! A producing SDK client must reach the surviving partition primary after
//! SIGKILL without replacing its session. Partition and metadata primaries
//! move independently, so verify each route through an operation on its plane.

use std::time::Duration;

use iggy::prelude::*;
use integration::harness::disk;
use integration::iggy_harness;
use tokio::time::sleep;

const STREAM_NAME: &str = "failover-stream";
const TOPIC_NAME: &str = "failover-topic";
const GROUP_NAME: &str = "failover-session";
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
        durability: iggy_common::Durability::Persisted,
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
    let primary_endpoint = harness
        .node(leader)
        .tcp_addr()
        .expect("leader exposes a TCP endpoint")
        .to_string();
    let producer = harness
        .node(leader)
        .tcp_client()
        .expect("leader exposes a TCP endpoint")
        .with_root_login()
        .connect()
        .await
        .expect("connect the producer to the primary");

    assert_eq!(
        producer.get_connection_info().await.server_address,
        primary_endpoint,
        "the producer must be pinned to the node this test kills, or it proves nothing"
    );

    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let partitioning = Partitioning::partition_id(PARTITION_ID);
    let group = producer
        .create_consumer_group(&stream, &topic, GROUP_NAME)
        .await
        .expect("create the session continuity group");
    producer
        .join_consumer_group(&stream, &topic, &Identifier::numeric(group.id).unwrap())
        .await
        .expect("join the session continuity group");
    let memberships_before = producer
        .get_me()
        .await
        .expect("read the producer's pre-kill session membership")
        .consumer_groups;
    assert_eq!(
        memberships_before.len(),
        1,
        "the producer must have a membership identifying its VSR session"
    );

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
         the surviving quorum within {RESUME_BUDGET:?}: the roster learned while the \
         connection was healthy names the survivors, and the same session remains bound \
         on the surviving links \
         ({attempt} attempts, last error: {last_error:?})"
    );
    tokio::time::timeout_at(resume_deadline, producer.get_stream(&stream))
        .await
        .expect("metadata recovery must share the continuity budget")
        .expect("the same session must reach the surviving metadata primary")
        .expect("the stream must survive primary loss");
    let memberships_after = tokio::time::timeout_at(resume_deadline, producer.get_me())
        .await
        .expect("session verification must share the continuity budget")
        .expect("read the producer's recovered session membership")
        .consumer_groups;
    assert_eq!(
        memberships_after.len(),
        memberships_before.len(),
        "the recovered session must retain its pre-kill group membership"
    );
    let before = &memberships_before[0];
    let after = &memberships_after[0];
    assert_eq!(
        (after.stream_id, after.topic_id, after.group_id),
        (before.stream_id, before.topic_id, before.group_id),
        "recovery must preserve the same VSR session's group membership"
    );
    assert_ne!(
        producer.get_connection_info().await.server_address,
        primary_endpoint,
        "connection info must name the recovered metadata primary"
    );
}
