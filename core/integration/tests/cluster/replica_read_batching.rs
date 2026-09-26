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

//! The plaintext replica plane counts what it reads.
//!
//! `replica_socket_reads_total` and `replica_inbound_frames_total` are the
//! direct evidence for `message_bus.replica_read_buffer_size`: their ratio is
//! the batching factor the read-ahead buffer exists to raise. Only a shard
//! that owns a replica socket bumps them, so a nonzero pair also names the
//! link shard.
//!
//! One shard per node, so shard 0 is that link shard and the assertion needs
//! no search. What is pinned here is that both counters reach the scrape at
//! all: the `Rc` reaches the reader task, the take in `tick_partitions` runs,
//! and the two `ShardMetrics` counters are registered. The ratio itself is
//! only logged. Frame arrival timing on a live link is not controlled, so a
//! threshold on it would be a flaky test;
//! `framing::tests::buffered_read_batches_many_frames_per_socket_read` asserts
//! the factor where the traffic shape is fixed.
//!
//! `frames >= reads` is deliberately not asserted: a body split across two
//! segments, or one straddling the end of the buffer, costs two reads for one
//! frame.

use iggy::prelude::*;
use integration::iggy_harness;
use std::time::Duration;
use tokio::time::{Instant, sleep};

use crate::server::http_client::HttpClient;

const STREAM: &str = "replica-read-stream";
const TOPIC: &str = "replica-read-topic";
const PARTITION_ID: u32 = 0;
const MESSAGES: u32 = 64;

/// The counters land on the scrape through the shard's partition tick, so the
/// first scrape after a produce can still read zero.
const COUNTER_BUDGET: Duration = Duration::from_secs(10);
const COUNTER_POLL: Duration = Duration::from_millis(250);

/// One shard per node, so this is the only shard and it owns both replica
/// links.
const LINK_SHARD: u16 = 0;

/// Read one `shard`-labelled counter out of the Prometheus text exposition.
///
/// The sub-registry label comes first in the label set, and these two counters
/// carry no others, so the series name plus the label is an exact line prefix.
fn shard_counter(metrics: &str, name: &str, shard: u16) -> Option<u64> {
    let prefix = format!("{name}{{shard=\"{shard}\"}} ");
    metrics
        .lines()
        .find_map(|line| line.strip_prefix(&prefix)?.trim().parse().ok())
}

async fn scrape(http: &HttpClient) -> String {
    http.client
        .get(http.url("/metrics"))
        .bearer_auth(&http.token)
        .send()
        .await
        .expect("metrics response")
        .text()
        .await
        .expect("metrics text")
}

#[iggy_harness(cluster_nodes = 3, server(sharding.cpu_allocation = "0..1"))]
async fn given_a_replicating_cluster_when_scraping_should_report_replica_reads_and_frames(
    harness: &TestHarness,
) {
    let client = harness.new_client().await.unwrap();
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    let stream = Identifier::named(STREAM).unwrap();
    let topic = Identifier::named(TOPIC).unwrap();
    client.create_stream(STREAM).await.unwrap();
    client
        .create_topic(
            &stream,
            TOPIC,
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::NeverExpire),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .expect("create topic");

    for index in 0..MESSAGES {
        let mut messages = vec![
            IggyMessage::builder()
                .payload(format!("payload-{index}").into())
                .build()
                .expect("message build"),
        ];
        client
            .send_messages(
                &stream,
                &topic,
                &Partitioning::partition_id(PARTITION_ID),
                &mut messages,
            )
            .await
            .unwrap_or_else(|error| panic!("send_messages {index}: {error}"));
    }

    let http = HttpClient::login_root(harness).await;
    let deadline = Instant::now() + COUNTER_BUDGET;
    loop {
        let metrics = scrape(&http).await;
        let reads = shard_counter(&metrics, "replica_socket_reads_total", LINK_SHARD);
        let frames = shard_counter(&metrics, "replica_inbound_frames_total", LINK_SHARD);
        if let (Some(reads), Some(frames)) = (reads, frames)
            && reads > 0
            && frames > 0
        {
            println!(
                "link shard {LINK_SHARD}: {frames} frames over {reads} socket reads, \
                 {:.2} frames per read",
                frames as f64 / reads as f64
            );
            return;
        }
        assert!(
            Instant::now() < deadline,
            "link shard {LINK_SHARD} reported reads={reads:?} frames={frames:?} within \
             {COUNTER_BUDGET:?}; both must be nonzero once replica traffic has flowed"
        );
        sleep(COUNTER_POLL).await;
    }
}
