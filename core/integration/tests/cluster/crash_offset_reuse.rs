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

//! RED SPEC, expected to FAIL: offset identity across a crash.
//!
//! `SendMessagesResponse::confirmations` hands clients concrete base offsets,
//! which makes offset reuse client-visible: a client that recorded offset N
//! for its message must never see the server confirm a DIFFERENT message at
//! N later. A solo node acks below the flush thresholds from RAM only and
//! persists no offset watermark, so after a SIGKILL it restarts the partition
//! log at the last flushed position and re-mints offsets it already
//! confirmed. Passes only once a durable watermark (or durable journal tail)
//! keeps post-restart offsets above everything ever acked.

use std::time::Duration;

use iggy::prelude::*;
use integration::harness::TestHarness;
use integration::iggy_harness;
use tokio::time::sleep;

const STREAM_NAME: &str = "offset-reuse-stream";
const TOPIC_NAME: &str = "offset-reuse-topic";
const PARTITION_ID: u32 = 0;
/// Confirmed sends before the crash; small enough to stay far below the
/// 1024-message / 1 MiB flush thresholds, so nothing reaches the segments.
const PRE_CRASH_SENDS: u32 = 5;

/// Bounds the restarted node's boot and its consensus groups settling.
const SERVE_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Send `count` single-message batches, returning each confirmed base offset.
async fn produce_acked(client: &IggyClient, payload_prefix: &str, count: u32) -> Vec<u64> {
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let mut acked = Vec::with_capacity(count as usize);
    for index in 0..count {
        let payload = format!("{payload_prefix}-{index:03}");
        let mut messages = vec![
            IggyMessage::builder()
                .payload(payload.clone().into())
                .build()
                .expect("build message"),
        ];
        let response = client
            .send_messages(
                &stream,
                &topic,
                &Partitioning::partition_id(PARTITION_ID),
                &mut messages,
            )
            .await
            .unwrap_or_else(|error| panic!("send {payload}: {error}"));
        acked.push(
            response
                .confirmations
                .first()
                .unwrap_or_else(|| panic!("the VSR server confirms every send, none for {payload}"))
                .base_offset,
        );
    }
    acked
}

/// Poll until the restarted node serves the pre-crash stream again, returning
/// a connected root client. Panics at the deadline.
async fn wait_until_serving(harness: &TestHarness, budget: Duration) -> IggyClient {
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        if let Ok(builder) = harness.node(0).tcp_client()
            && let Ok(client) = builder.with_root_login().connect().await
            && matches!(client.get_stream(&stream).await, Ok(Some(_)))
        {
            return client;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the restarted node did not serve the stream within {budget:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

// TODO(hubcio): fix this test
#[ignore = "confirmed offsets re-minted after a crash; no durable offset watermark"]
#[iggy_harness(cluster_nodes = 1)]
async fn given_confirmed_sends_below_flush_threshold_when_a_solo_node_is_killed_should_not_remint_offsets(
    harness: &mut TestHarness,
) {
    let client = harness.tcp_root_client().await.unwrap();
    client
        .create_stream(STREAM_NAME)
        .await
        .expect("create stream");
    client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::NeverExpire),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .expect("create topic");

    let acked = produce_acked(&client, "pre-crash", PRE_CRASH_SENDS).await;
    let highest_confirmed = *acked.last().expect("confirmed sends");
    drop(client);

    harness.kill_node(0).expect("SIGKILL the only node");
    harness.restart_node(0).expect("restart it");

    let client = wait_until_serving(harness, SERVE_TIMEOUT).await;
    let post_crash_offset = produce_acked(&client, "post-crash", 1).await[0];

    assert!(
        post_crash_offset > highest_confirmed,
        "a crash-restarted node re-minted offsets it already confirmed: offset \
         {highest_confirmed} was handed to a client before the SIGKILL, yet the first \
         post-restart send was confirmed at offset {post_crash_offset}; without a durable \
         offset watermark the node restarts the partition log below what it acknowledged, \
         so two different messages now share an offset and consumers reading by offset get \
         silently different data"
    );
}
