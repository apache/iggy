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

//! Peer traffic of a cluster whose partitions take no writes.
//!
//! Each node batches the Commits and replies of all its groups, so the peer
//! traffic stays small with many idle partitions. The measurement starts after
//! a warmup, so the setup of the partitions is not in it. Set `PARTITIONS` to
//! the number of partitions to measure.
//!
//! Each link carries at most one Commit batch and one reply batch per sending
//! shard and shard tick, on average over each report. The relay sees frames
//! when they arrive, not when a shard sends them, so it checks the average.
//! A debug `iggy-server` itself stops when a shard sends two Commit batches in
//! one tick.
//!
//! Build `iggy-server`, then run `cargo test -p integration -- --ignored
//! cluster::idle_partition_traffic --nocapture`.

use configs::cluster::CONSENSUS_TICK;
use iggy::prelude::*;
use iggy_common::MAX_PARTITIONS_COUNT;
use integration::harness::{PeerTraffic, TestHarness, TestServerConfig, resolve_config_paths};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;

const NODES: usize = 3;
const SHARDS: u64 = 4;
/// The partitions the test creates in the cluster.
const PARTITIONS: u32 = 1_000;
/// One topic holds at most [`MAX_PARTITIONS_COUNT`] partitions, so the
/// partitions are spread over this many topics.
const TOPICS: u32 = PARTITIONS.div_ceil(MAX_PARTITIONS_COUNT);
/// The partitions finish their setup in this time after their topics exist.
/// The measurement starts after it.
const WARMUP: Duration = Duration::from_secs(35);
const REPORT_INTERVAL: Duration = Duration::from_secs(5);
const REPORTS: u32 = 12;
/// The default `cluster.commit_busy_heartbeat_interval`. The report averages
/// per this period, so its numbers compare with runs before batching.
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(500);
const STREAM: &str = "idle-traffic-stream";
const TOPIC: &str = "idle-traffic-topic";

#[tokio::test(flavor = "multi_thread")]
#[ignore = "measures peer traffic for about two minutes, run it by hand"]
async fn given_idle_partitions_when_reported_every_interval_should_send_one_batch_per_shard_tick() {
    let overrides = HashMap::from([("sharding.cpu_allocation".to_owned(), SHARDS.to_string())]);
    let extra_envs = resolve_config_paths(&overrides).expect("valid server config paths");
    let mut harness = TestHarness::builder()
        .server(TestServerConfig::builder().extra_envs(extra_envs).build())
        .cluster_nodes(NODES)
        .peer_relays()
        .build()
        .expect("build the harness");
    harness.start().await.expect("start the cluster");
    let client = harness.tcp_root_client().await.expect("root client");
    client
        .create_stream(STREAM, &StreamCreateOptions::default())
        .await
        .expect("create stream");
    let stream = Identifier::named(STREAM).expect("stream name");
    for topic in 0..TOPICS {
        let partitions = (PARTITIONS - topic * MAX_PARTITIONS_COUNT).min(MAX_PARTITIONS_COUNT);
        client
            .create_topic(
                &stream,
                &format!("{TOPIC}-{topic}"),
                &TopicCreateOptions {
                    partitions_count: Some(partitions),
                    message_expiry: Some(IggyExpiry::NeverExpire),
                    ..TopicCreateOptions::default()
                },
            )
            .await
            .expect("create topic");
    }
    sleep(WARMUP).await;

    let start = traffic(&harness);
    let mut previous = start.clone();
    let mut elapsed = Duration::ZERO;
    for _ in 0..REPORTS {
        sleep(REPORT_INTERVAL).await;
        elapsed += REPORT_INTERVAL;
        let now = traffic(&harness);
        println!(
            "[{elapsed:?}] {PARTITIONS} partitions\n  last {REPORT_INTERVAL:?}: {}\n  since start: {}",
            report(total(&now) - total(&previous), REPORT_INTERVAL),
            report(total(&now) - total(&start), elapsed)
        );
        let frames_max = SHARDS * (REPORT_INTERVAL.as_millis() / CONSENSUS_TICK.as_millis()) as u64;
        for ((from, to, link), (_, _, before)) in now.iter().zip(&previous) {
            let link = *link - *before;
            assert!(
                link.commit_frames <= frames_max,
                "node {from} -> node {to}: more than one Commit batch per shard and tick: {link:?}"
            );
            assert!(
                link.reply_frames <= frames_max,
                "node {from} -> node {to}: more than one reply batch per shard and tick: {link:?}"
            );
        }
        previous = now;
    }
}

/// The raw counters of `traffic`, the frames that are neither Commit nor
/// `CommitReply`, and each kind per [`HEARTBEAT_INTERVAL`], averaged over
/// `window`.
fn report(traffic: PeerTraffic, window: Duration) -> String {
    let per_interval =
        |count: u64| count as f64 * HEARTBEAT_INTERVAL.as_secs_f64() / window.as_secs_f64();
    let other_frames = traffic.frames - traffic.commit_frames - traffic.reply_frames;
    let other_bytes = traffic.bytes - traffic.commit_bytes - traffic.reply_bytes;
    format!(
        "{traffic:?}\n  other frames: {other_frames} frames, {other_bytes} bytes\n  \
         per {HEARTBEAT_INTERVAL:?}: {:.1} commit batches, {:.0} commit bytes, \
         {:.1} reply batches, {:.0} reply bytes, {:.1} other frames, {:.0} other bytes, \
         {:.0} total bytes",
        per_interval(traffic.commit_frames),
        per_interval(traffic.commit_bytes),
        per_interval(traffic.reply_frames),
        per_interval(traffic.reply_bytes),
        per_interval(other_frames),
        per_interval(other_bytes),
        per_interval(traffic.bytes),
    )
}

/// The traffic of every link of the cluster.
fn total(links: &[(usize, usize, PeerTraffic)]) -> PeerTraffic {
    links
        .iter()
        .fold(PeerTraffic::default(), |sum, (_, _, link)| sum + *link)
}

fn traffic(harness: &TestHarness) -> Vec<(usize, usize, PeerTraffic)> {
    (0..NODES)
        .flat_map(|from| {
            (0..NODES)
                .filter(move |to| *to != from)
                .map(move |to| (from, to))
        })
        .map(|(from, to)| {
            let traffic = harness.peer_traffic(from, to).expect("peer relays");
            (from, to, traffic)
        })
        .collect()
}
