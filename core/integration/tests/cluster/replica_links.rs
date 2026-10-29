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

//! Two and four replica links per peer on four shards.
//!
//! With two links, link 0 carries the frames of shards 0 and 1, link 1 those
//! of shards 2 and 3. The skip flag keeps links off shard 0, so on every node
//! link 0 of both peers lives on shard 1, and link 1 on shard 2 for one peer
//! and shard 3 for the other. One link per peer would sit on shards 1 and 2
//! only. Only a shard that owns a replica link reads replica sockets, so the
//! per-shard read counters show where the links live. Shard 0 alone tracks
//! bundles, so the bundle metrics carry its label.

use iggy::prelude::*;
use integration::harness::{TestHarness, TestServerConfig, resolve_config_paths};
use integration::iggy_harness;
use serial_test::parallel;
use std::collections::HashMap;
use std::iter;
use std::time::Duration;
use tokio::time::{Instant, sleep};

use crate::server::http_client::{HttpClient, node_url};

const NODES: usize = 3;
const SHARDS: &str = "4";
const LINKS: &str = "2";
const PEERS: u64 = 2;
const STREAM: &str = "replica-links-stream";
const TOPIC: &str = "replica-links-topic";
/// Enough partitions that the hash assignment puts some on both shard ranges.
const PARTITIONS: u32 = 8;
const MESSAGES_PER_PARTITION: u32 = 4;
/// The counters reach the scrape through the shard tick, and a re-formed
/// bundle waits for the next reconnect sweep.
const METRICS_BUDGET: Duration = Duration::from_secs(20);
const METRICS_POLL: Duration = Duration::from_millis(250);
const SHARD_ZERO: u16 = 0;
const LINK_SHARDS: [u16; 3] = [1, 2, 3];

#[iggy_harness(
    cluster_nodes = 3,
    server(
        sharding.cpu_allocation = "4",
        cluster.coordinator.replica_connections_per_peer = "2"
    )
)]
async fn given_two_links_per_peer_when_replicating_should_read_on_both_link_ranges(
    harness: &TestHarness,
) {
    let client = harness.tcp_root_client().await.expect("root client");
    replicate_every_partition(&client, TOPIC).await;

    for node in 0..NODES {
        // Every ordered pair of nodes exchanges frames for every partition,
        // so each link reads once a partition of its range takes messages.
        let links = wait_for_links(harness, node, |links| {
            links.bundles_up == PEERS && LINK_SHARDS.iter().all(|&shard| links.reads(shard) > 0)
        })
        .await;
        assert_eq!(
            links.reads(SHARD_ZERO),
            0,
            "node {node}: shard 0 must own no replica link"
        );
    }
}

/// With a link per shard, range 0 is shard 0 alone, so link 0 lives there
/// despite the skip flag.
#[iggy_harness(
    cluster_nodes = 3,
    server(
        sharding.cpu_allocation = "4",
        cluster.coordinator.replica_connections_per_peer = "4"
    )
)]
async fn given_a_link_per_shard_when_replicating_should_read_on_every_shard(harness: &TestHarness) {
    let client = harness.tcp_root_client().await.expect("root client");
    replicate_every_partition(&client, TOPIC).await;

    for node in 0..NODES {
        wait_for_links(harness, node, |links| {
            links.bundles_up == PEERS
                && iter::once(SHARD_ZERO)
                    .chain(LINK_SHARDS)
                    .all(|shard| links.reads(shard) > 0)
        })
        .await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[parallel]
async fn given_two_links_per_peer_when_one_connection_drops_should_reform_only_that_bundle() {
    let overrides = HashMap::from([
        ("sharding.cpu_allocation".to_owned(), SHARDS.to_owned()),
        (
            "cluster.coordinator.replica_connections_per_peer".to_owned(),
            LINKS.to_owned(),
        ),
    ]);
    let extra_envs = resolve_config_paths(&overrides).expect("valid server config paths");
    let mut harness = TestHarness::builder()
        .server(TestServerConfig::builder().extra_envs(extra_envs).build())
        .cluster_nodes(NODES)
        .peer_relays()
        .build()
        .expect("build the harness");
    harness.start().await.expect("start the cluster");
    let mut before = Vec::with_capacity(NODES);
    for node in 0..NODES {
        before.push(wait_for_links(&harness, node, |links| links.bundles_up == PEERS).await);
    }

    // Node 0 closes the other link of the bundle, then node 1 sees both
    // close. The bundles of node 2 never change.
    harness
        .drop_peer_connection(0, 1)
        .expect("drop one link from node 0 to node 1");
    for node in [0, 1] {
        wait_for_links(&harness, node, |links| {
            links.teardowns > before[node].teardowns && links.bundles_up == PEERS
        })
        .await;
    }
    let client = harness.tcp_root_client().await.expect("root client");
    replicate_every_partition(&client, TOPIC).await;
    let untouched = links_of(&harness, 2).await;
    assert_eq!(
        untouched.teardowns, before[2].teardowns,
        "node 2 must keep its bundles"
    );
    assert_eq!(untouched.bundles_up, PEERS);
}

#[iggy_harness(
    cluster_nodes = 3,
    server(
        sharding.cpu_allocation = "4",
        cluster.coordinator.replica_connections_per_peer = "2"
    )
)]
async fn given_two_links_per_peer_when_a_node_restarts_should_reform_every_bundle(
    harness: &mut TestHarness,
) {
    const RESTARTED: usize = 2;
    let client = harness.tcp_root_client().await.expect("root client");
    replicate_every_partition(&client, TOPIC).await;
    drop(client);

    harness
        .restart_node(RESTARTED)
        .expect("restart the node with its data intact");
    for node in 0..NODES {
        wait_for_links(harness, node, |links| links.bundles_up == PEERS).await;
    }
    let client = harness.tcp_root_client().await.expect("root client");
    replicate_every_partition(&client, "replica-links-after-restart").await;
}

/// Shard metrics of one node for the replica links.
#[derive(Debug, Default)]
struct Links {
    bundles_up: u64,
    teardowns: u64,
    reads: HashMap<u16, u64>,
}

impl Links {
    fn parse(metrics: &str) -> Self {
        let mut links = Self::default();
        for line in metrics.lines() {
            let Some((series, value)) = line.rsplit_once(' ') else {
                continue;
            };
            let Ok(value) = value.parse::<u64>() else {
                continue;
            };
            let Some((name, shard)) = series
                .strip_suffix("\"}")
                .and_then(|series| series.split_once("{shard=\""))
            else {
                continue;
            };
            let Ok(shard) = shard.parse::<u16>() else {
                continue;
            };
            match name {
                "replica_bundles_up" if shard == SHARD_ZERO => links.bundles_up = value,
                "replica_bundle_teardowns_total" if shard == SHARD_ZERO => {
                    links.teardowns = value;
                }
                "replica_socket_reads_total" => {
                    links.reads.insert(shard, value);
                }
                _ => {}
            }
        }
        links
    }

    fn reads(&self, shard: u16) -> u64 {
        self.reads.get(&shard).copied().unwrap_or_default()
    }
}

async fn links_of(harness: &TestHarness, node: usize) -> Links {
    let http = HttpClient::login_root_no_redirect(node_url(harness, node)).await;
    let response = http.get("/metrics").await;
    assert!(
        response.status().is_success(),
        "metrics scrape of node {node}"
    );
    Links::parse(&response.text().await.expect("metrics text"))
}

async fn wait_for_links(
    harness: &TestHarness,
    node: usize,
    done: impl Fn(&Links) -> bool,
) -> Links {
    let deadline = Instant::now() + METRICS_BUDGET;
    loop {
        let links = links_of(harness, node).await;
        if done(&links) {
            return links;
        }
        assert!(
            Instant::now() < deadline,
            "node {node} replica links did not settle within {METRICS_BUDGET:?}: {links:?}"
        );
        sleep(METRICS_POLL).await;
    }
}

/// Create `topic`, send to every partition and read every message back.
async fn replicate_every_partition(client: &IggyClient, topic: &str) {
    let stream = Identifier::named(STREAM).expect("stream name");
    if client
        .get_stream(&stream)
        .await
        .expect("get stream")
        .is_none()
    {
        client
            .create_stream(STREAM, &StreamCreateOptions::default())
            .await
            .expect("create stream");
    }
    client
        .create_topic(
            &stream,
            topic,
            &TopicCreateOptions {
                partitions_count: Some(PARTITIONS),
                message_expiry: Some(IggyExpiry::NeverExpire),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .expect("create topic");
    let topic = Identifier::named(topic).expect("topic name");
    for partition in 0..PARTITIONS {
        let mut messages = (0..MESSAGES_PER_PARTITION)
            .map(|index| {
                IggyMessage::builder()
                    .payload(format!("{partition}-{index}").into())
                    .build()
                    .expect("message build")
            })
            .collect::<Vec<_>>();
        client
            .send_messages(
                &stream,
                &topic,
                &Partitioning::partition_id(partition),
                &mut messages,
            )
            .await
            .unwrap_or_else(|error| panic!("send to partition {partition}: {error}"));
    }
    for partition in 0..PARTITIONS {
        let polled = client
            .poll_messages(
                &stream,
                &topic,
                Some(partition),
                &Consumer::default(),
                &PollingStrategy::offset(0),
                MESSAGES_PER_PARTITION,
                false,
            )
            .await
            .unwrap_or_else(|error| panic!("poll partition {partition}: {error}"));
        assert_eq!(
            polled.messages.len(),
            MESSAGES_PER_PARTITION as usize,
            "partition {partition}"
        );
    }
}
