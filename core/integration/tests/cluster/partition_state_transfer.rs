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

//! Partition-plane state transfer: a rejoining replica whose journal repair
//! cannot close the gap (the peer's evicted ring moved past it) pulls the
//! partition's retained segments + consumer offsets from the caught-up
//! primary, installs them, and hands the live tail to ordinary repair.
//!
//! Forcing function: `messages_required_to_save = 1` flushes (and ring-
//! evicts) every committed batch, and `evicted_ring_capacity = 64` keeps the
//! repair window shallow, so ~200 produced batches push `repair_retained_from`
//! far past a rejoiner's durable end. Its gap-fill repair then gets
//! `RangeEvicted`, the repaired window cannot connect to recovered state,
//! and `complete_repair` returns the `FloorRefused` conversion trigger.

#![cfg(feature = "vsr")]

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, Instant};

use iggy::prelude::*;
use integration::harness::TestHarness;
use integration::iggy_harness;
use tokio::time::sleep;

const STREAM_NAME: &str = "partition-transfer-stream";
const TOPIC_NAME: &str = "partition-transfer-topic";
/// server-ng partition ids are 0-based (CreateTopic assigns them from 0).
const PARTITION_ID: u32 = 0;
/// Enough batches to push the evicted ring (capacity 64) well past the
/// window a rejoiner could repair from op 1.
const MESSAGES_COUNT: u32 = 200;
const STORED_CONSUMER_OFFSET: u64 = 17;

const CONVERSION_MARKER: &str = "partition repair floor unreachable; converting to state transfer";
const INSTALL_MARKER: &str = "partition state transfer installed";
const SERVING_MARKER: &str = "serving partition state transfer";
const ABANDON_MARKER: &str =
    "partition state transfer stalled past its retry budget; abandoning and falling back";

/// Transfer end-to-end: adoption, repair round-trip, conversion, chunk pull,
/// install, tail repair. CI runners are slow; bound without hanging the suite.
const TRANSFER_BUDGET: Duration = Duration::from_secs(60);
const MARKER_POLL: Duration = Duration::from_millis(200);

#[iggy_harness(
    cluster_nodes = 3,
    server(
        system.sharding.cpu_allocation = "0..1",
        partition.evicted_ring_capacity = "64",
        system.partition.messages_required_to_save = "1"
    )
)]
async fn given_evicted_ring_when_fresh_node_joins_late_should_state_transfer_partition(
    harness: &mut TestHarness,
) {
    let client = connect(harness, 0).await;
    seed_partition(&client).await;
    client
        .store_consumer_offset(
            &Consumer::default(),
            &Identifier::named(STREAM_NAME).expect("stream identifier"),
            &Identifier::named(TOPIC_NAME).expect("topic identifier"),
            Some(PARTITION_ID),
            STORED_CONSUMER_OFFSET,
        )
        .await
        .expect("store a consumer offset before the wipe");
    sleep(Duration::from_secs(1)).await;
    // Deliberately NOT dropped before the wipe: a disconnect commits a
    // Logout, and a fresh (never-checkpointed) metadata rejoin currently
    // wedges repairing session-scoped ops. Keeping the seed session alive
    // caps the metadata window at ops a fresh joiner provably replays, so
    // this spec isolates the PARTITION plane.
    let _seed_client = client;

    // Wipe node 2 and rejoin: no local history at all, so repair cannot
    // connect any floor and the refusal converts to a transfer.
    harness
        .restart_node_from_clean_slate(2)
        .expect("clean-slate restart of node 2");

    await_marker(harness, 2, CONVERSION_MARKER).await;
    await_marker(harness, 2, INSTALL_MARKER).await;

    // Disk proof on the rejoined node: transferred segment bytes and a
    // persisted consumer-offset file (a single LE u64).
    let data_path = harness.node(2).data_path();
    // Each transferred batch is at least its 256-byte header; anything below
    // this floor is a truncated install, not the seeded 200 batches.
    let transferred_floor = u64::from(MESSAGES_COUNT) * 256;
    let deadline = Instant::now() + TRANSFER_BUDGET;
    loop {
        if total_partition_log_bytes(&data_path) >= transferred_floor {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "node 2 never materialized the transferred segment bytes \
             (expected at least {transferred_floor})"
        );
        sleep(MARKER_POLL).await;
    }
    let offsets_file = find_consumer_offset_file(&data_path)
        .expect("transferred consumer offset file exists on node 2");
    let bytes = std::fs::read(&offsets_file).expect("read transferred consumer offset");
    assert_eq!(
        u64::from_le_bytes(bytes.as_slice().try_into().expect("offset file is one u64")),
        STORED_CONSUMER_OFFSET,
        "the stored consumer offset must survive the transfer"
    );

    // Functional capstone: with node 1 down, quorum is node 0 + the
    // transferred node 2, so one more produce+poll round-trip cannot commit
    // unless node 2 PrepareOks from its transferred state.
    harness.stop_node(1).expect("stop node 1");
    let deadline = Instant::now() + TRANSFER_BUDGET;
    loop {
        if let Some(client) = connect_any(harness, &[2, 0]).await {
            let mut extra = vec![IggyMessage::from_str("post-transfer").expect("message")];
            if client
                .send_messages(
                    &Identifier::named(STREAM_NAME).expect("stream identifier"),
                    &Identifier::named(TOPIC_NAME).expect("topic identifier"),
                    &Partitioning::partition_id(PARTITION_ID),
                    &mut extra,
                )
                .await
                .is_ok()
                && poll_count(&client, MESSAGES_COUNT + 1).await == Ok(MESSAGES_COUNT + 1)
            {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "the pre-wipe batch plus one post-transfer message never became pollable \
             with only node 0 and the transferred node 2 alive"
        );
        sleep(MARKER_POLL).await;
    }
}

#[iggy_harness(
    cluster_nodes = 3,
    server(
        system.sharding.cpu_allocation = "0..1",
        partition.evicted_ring_capacity = "64",
        system.partition.messages_required_to_save = "1"
    )
)]
async fn given_evicted_ring_when_node_restarts_with_data_should_state_transfer_partition(
    harness: &mut TestHarness,
) {
    // Node 2 holds a durable prefix, then misses enough traffic that the
    // survivors' ring moves past its durable end: its repaired window cannot
    // connect, which is exactly the refusal-site trigger.
    let client = connect(harness, 0).await;
    seed_topic(&client).await;
    produce(&client, 40).await;
    sleep(Duration::from_secs(1)).await;
    harness.stop_node(2).expect("stop node 2");

    produce(&client, MESSAGES_COUNT).await;
    let _seed_client = client;

    harness.restart_node(2).expect("restart node 2 with data");

    await_marker(harness, 2, CONVERSION_MARKER).await;
    await_marker(harness, 2, INSTALL_MARKER).await;
    // The serving side proves the pull actually ran (not a vacuous install).
    let served_somewhere = [0, 1]
        .iter()
        .any(|&node| harness.node(node).stdout_contains(SERVING_MARKER));
    assert!(
        served_somewhere,
        "some survivor must have served the partition transfer"
    );
}

#[iggy_harness(
    cluster_nodes = 3,
    server(
        system.sharding.cpu_allocation = "0..1",
        partition.evicted_ring_capacity = "64",
        system.partition.messages_required_to_save = "1"
    )
)]
async fn given_transfer_peer_dies_when_stalled_should_abandon_and_recover_partition(
    harness: &mut TestHarness,
) {
    let client = connect(harness, 0).await;
    seed_partition(&client).await;
    sleep(Duration::from_secs(1)).await;
    let _seed_client = client;

    // Wipe node 2, wait until its rejoin CONVERTED to a transfer (armed at
    // the view-0 primary, node 0), then kill that serving peer. Whatever
    // in-flight phase the kill lands in, node 2 must not retry into the
    // corpse forever: the stall budget abandons, the survivors elect past
    // node 0, and repair -> refusal -> transfer re-runs against the new
    // primary (node 1).
    harness
        .restart_node_from_clean_slate(2)
        .expect("clean-slate restart of node 2");
    await_marker(harness, 2, CONVERSION_MARKER).await;
    harness
        .stop_node(0)
        .expect("stop the serving peer (node 0)");

    // Convergence-only: the install marker within budget. No follow-up
    // commit is asserted -- the cluster is quorum-marginal with one node
    // down, and an unanswered read mid-election is not a verdict. The
    // abandon marker is timing-dependent (the kill can land before the
    // first chunk or after install) and is logged for diagnosis only.
    let deadline = Instant::now() + TRANSFER_BUDGET;
    loop {
        if harness.node(2).stdout_contains(INSTALL_MARKER) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "node 2 never installed a partition transfer with node 0 dead \
             (abandon marker seen: {})",
            harness.node(2).stdout_contains(ABANDON_MARKER)
        );
        sleep(MARKER_POLL).await;
    }
}

/// Connect a root-authenticated TCP client to a specific node.
async fn connect(harness: &TestHarness, node: usize) -> IggyClient {
    harness
        .node(node)
        .tcp_client()
        .expect("tcp client builder")
        .with_root_login()
        .connect()
        .await
        .unwrap_or_else(|e| panic!("connect to node {node}: {e}"))
}

async fn connect_any(harness: &TestHarness, nodes: &[usize]) -> Option<IggyClient> {
    for &node in nodes {
        if let Ok(builder) = harness.node(node).tcp_client()
            && let Ok(client) = builder.with_root_login().connect().await
        {
            return Some(client);
        }
    }
    None
}

async fn seed_topic(client: &IggyClient) {
    client
        .create_stream(STREAM_NAME)
        .await
        .expect("create stream");
    client
        .create_topic(
            &Identifier::named(STREAM_NAME).expect("stream identifier"),
            TOPIC_NAME,
            1,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .expect("create topic with one partition");
}

async fn produce(client: &IggyClient, count: u32) {
    // One message per send: each commit flushes (messages_required_to_save=1)
    // and ring-evicts, which is what marches `repair_retained_from` forward.
    for sequence in 0..count {
        let mut messages =
            vec![IggyMessage::from_str(&format!("message-{sequence}")).expect("message")];
        client
            .send_messages(
                &Identifier::named(STREAM_NAME).expect("stream identifier"),
                &Identifier::named(TOPIC_NAME).expect("topic identifier"),
                &Partitioning::partition_id(PARTITION_ID),
                &mut messages,
            )
            .await
            .expect("send message");
    }
}

async fn seed_partition(client: &IggyClient) {
    seed_topic(client).await;
    produce(client, MESSAGES_COUNT).await;
    assert_eq!(
        poll_count(client, MESSAGES_COUNT).await,
        Ok(MESSAGES_COUNT),
        "the seed batch must commit before the fault is injected"
    );
}

async fn poll_count(client: &IggyClient, count: u32) -> Result<u32, IggyError> {
    let polled = client
        .poll_messages(
            &Identifier::named(STREAM_NAME).expect("stream identifier"),
            &Identifier::named(TOPIC_NAME).expect("topic identifier"),
            Some(PARTITION_ID),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            count,
            false,
        )
        .await?;
    #[allow(clippy::cast_possible_truncation)]
    Ok(polled.messages.len() as u32)
}

async fn await_marker(harness: &TestHarness, node: usize, marker: &str) {
    let deadline = Instant::now() + TRANSFER_BUDGET;
    while !harness.node(node).stdout_contains(marker) {
        assert!(
            Instant::now() < deadline,
            "node {node} never logged {marker:?} within {TRANSFER_BUDGET:?}"
        );
        sleep(MARKER_POLL).await;
    }
}

/// Total `.log` bytes outside the metadata directory: transferred segment
/// payload. Walked rather than path-derived so the test does not hard-code
/// the `streams/<s>/topics/<t>/partitions/<p>` layout.
fn total_partition_log_bytes(root: &Path) -> u64 {
    let mut total = 0;
    let _ = walk(root, &mut |path| {
        if path.extension().is_some_and(|extension| extension == "log")
            && let Ok(metadata) = std::fs::metadata(path)
        {
            total += metadata.len();
        }
        false
    });
    total
}

fn find_consumer_offset_file(root: &Path) -> Option<PathBuf> {
    walk(root, &mut |path| {
        path.parent()
            .and_then(Path::file_name)
            .is_some_and(|name| name == "consumers")
            && std::fs::metadata(path).is_ok_and(|metadata| metadata.len() == 8)
    })
}

fn walk(root: &Path, matches: &mut dyn FnMut(&Path) -> bool) -> Option<PathBuf> {
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        if dir.file_name().is_some_and(|name| name == "metadata") {
            continue;
        }
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if matches(&path) {
                return Some(path);
            }
        }
    }
    None
}
