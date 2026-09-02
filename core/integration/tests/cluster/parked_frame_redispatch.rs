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

//! A BACKUP parking replicated partition prepares, and the frames it parked
//! reaching the plane in op order once its partition materialises.
//!
//! `multi_shard_partition_convergence` covers the same fence on one node and
//! says outright that it cannot tell a request served straight through from one
//! that parked. This test pins the park path positively, and on the replica
//! where getting it wrong is unrecoverable: a client request that never reaches
//! the plane is answered with a retriable status and the SDK replays it, while a
//! replicated PREPARE has no client behind it: nothing re-sends it once its op
//! has quorum, so the backup gap-stops and waits out `tick_partitions`' repair
//! debounce before anything refetches it.
//!
//! What makes the window wide on a backup is the commit broadcast. A backup
//! learns a metadata commit from the `commit` field of the next prepare on that
//! plane or from the primary's `CommitMessage` heartbeat, whose interval is
//! `cluster.commit_broadcast_interval` (500ms by default). Nothing in
//! `create_topic`'s reply path waits for that, so a produce issued the instant
//! `create_topic` returns reaches the backups as a partition prepare for a
//! namespace they have not yet heard of, let alone built. Four producers on
//! their own connections keep a burst in flight across that gap, so several ops
//! of one partition park together and the order they leave in is observable.
//!
//! Three things are asserted, and they fail separately:
//!
//! - The path was entered on a backup. `redispatch_parked_frames` logs at
//!   `debug`, hence the `system.logging.level` override; the marker on a node
//!   that is not the leader is proof, because a fresh partition group seeds its
//!   view from the metadata plane, so every partition primary here is the
//!   metadata leader and no client request lands anywhere else.
//! - No park path degraded into a shed, an aged-out answer, or an incarnation
//!   rejection, and no replica dropped a prepare for arriving out of order.
//!   That last marker is the direct symptom of re-dispatch losing a frame's
//!   arrival position.
//! - Every acked message is readable in dense offset order, each producer's own
//!   sends stay in the order it made them, and all three replicas hold
//!   byte-identical segments. A prepare lost to the gap check leaves a backup
//!   permanently short, since the gap never closes on its own.
//!
//! `RUST_LOG` in the test process environment overrides the config level and
//! would take the positive marker with it; the assertion says so when it fires.

use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use futures::future::join_all;
use iggy::prelude::*;
use integration::harness::{TestHarness, disk};
use integration::iggy_harness;
use tokio::time::{Instant, sleep};

const STREAM: &str = "parked-redispatch-stream";
/// Each topic is one shot at the race, and each costs about one commit
/// broadcast interval.
const TOPICS: u32 = 6;
const PARTITIONS: u32 = 4;
/// The reconciler builds a topic's namespaces in id order, so the last one has
/// the longest wait for its `InsertOwned`.
const TARGET_PARTITION: u32 = PARTITIONS - 1;
/// Separate connections, because one `IggyClient` serialises its requests and a
/// single in-flight prepare would never expose park ordering.
const PRODUCERS: usize = 4;
const PER_PRODUCER: usize = 8;
const TOTAL_MESSAGES: usize = PRODUCERS * PER_PRODUCER;

/// Budget for eagerly flushed batches to reach every node's segment files.
const FLUSH_INSTALL_TIMEOUT: Duration = Duration::from_secs(20);
const POLL_INTERVAL: Duration = Duration::from_millis(250);

/// `IggyShard::redispatch_parked_frames`, at `debug`.
const REDISPATCH_MARKER: &str = "re-dispatching parked partition frames after materialisation";

/// Modes in which the park path gives up instead of converging. All three cost
/// a frame: the first two shed or answer, the third refuses a namespace whose
/// incarnation moved under it.
const DEGRADED_MARKERS: [&str; 3] = [
    "park buffer at capacity",
    "outlived their admission window",
    "rejecting parked partition frame",
];

/// `IggyPartition::on_replicate`'s backup gap check. A re-dispatch that appends
/// behind an op already queued on the inbox surfaces here. The dropped op is
/// refetched by `tick_partitions`' level-triggered repair driver, but only after
/// its debounce interval, so a re-dispatch that trips this has already stalled
/// the replica for ~1s and the marker still means the ordering broke.
///
/// Names its plane: `stdout_occurrences` counts SUBSTRINGS, and the metadata
/// plane logs its own gap drop at `warn`, which passes this test's `info`
/// filter. A shared wording would fail a partition-plane assertion on a
/// metadata-plane event.
///
/// Deliberately not scoped to one namespace or node. Chain replication is
/// ordered per connection, so on a healthy three-node cluster with no injected
/// loss no partition-plane gap drop is expected anywhere.
const GAP_MARKER: &str = "dropping out-of-order partition prepare (gap)";

fn topic_name(index: u32) -> String {
    format!("parked-redispatch-topic-{index}")
}

#[iggy_harness(cluster_nodes = 3, server(system.logging.level = "info,shard=debug"))]
async fn given_a_produce_burst_right_after_create_topic_when_backups_park_the_prepares_should_re_dispatch_them_in_order(
    harness: &mut TestHarness,
) {
    // Read once, before any topic exists: the leader is the primary of every
    // partition group created below, so it is also the only node a produce can
    // be admitted on.
    let leader = disk::leader_node_index_via(harness, 0).await;
    let setup = harness
        .root_client_for_node(leader)
        .await
        .expect("root client on the metadata leader");
    setup.create_stream(STREAM).await.expect("create stream");
    let stream = Identifier::named(STREAM).expect("stream identifier");

    // Connected and logged in before the first `create_topic`, so the burst
    // costs one round trip rather than a handshake.
    let mut producers = Vec::with_capacity(PRODUCERS);
    for _ in 0..PRODUCERS {
        producers.push(
            harness
                .root_client_for_node(leader)
                .await
                .expect("root client for a producer"),
        );
    }

    let mut all_payloads = Vec::with_capacity(TOPICS as usize * TOTAL_MESSAGES);
    for topic_index in 0..TOPICS {
        let name = topic_name(topic_index);
        create_topic(&setup, &stream, &name).await;
        let topic = Identifier::named(&name).expect("topic identifier");

        let sent = produce_burst(&producers, &stream, &topic, topic_index).await;
        let polled = poll_payloads(&setup, &stream, &topic).await;
        assert_eq!(
            polled.len(),
            TOTAL_MESSAGES,
            "topic {topic_index} must serve every acked message, got {polled:?}"
        );
        assert_producer_order(&polled, &sent, topic_index);
        all_payloads.extend(polled);
    }

    let data_paths: Vec<PathBuf> = harness
        .all_servers()
        .iter()
        .map(|server| server.data_path())
        .collect();
    wait_until_payloads_installed(harness, &all_payloads).await;
    disk::wait_for_log_convergence(&data_paths).await;
    // Also flushes each node's non-blocking log appender, so the markers below
    // are read off a complete file.
    harness
        .stop()
        .await
        .expect("stop the cluster for the at-rest comparison");

    assert_backup_re_dispatched(harness, leader);
    assert_no_degraded_park_paths(harness);
    disk::assert_replica_data_identical(&data_paths, false);
}

/// `messages_required_to_save` + `enforce_fsync` persist every committed batch
/// on every replica, which is what makes the on-disk assertions mean anything
/// on a run this small; the default thresholds would ack from RAM alone.
async fn create_topic(client: &IggyClient, stream: &Identifier, name: &str) {
    client
        .create_topic(
            stream,
            name,
            &TopicCreateOptions {
                partitions_count: Some(PARTITIONS),
                message_expiry: Some(IggyExpiry::NeverExpire),
                messages_required_to_save: Some(1),
                enforce_fsync: Some(true),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap_or_else(|error| panic!("create_topic {name}: {error}"));
}

/// Fire every producer at once, returning each one's payloads in the order it
/// sent them.
async fn produce_burst(
    producers: &[IggyClient],
    stream: &Identifier,
    topic: &Identifier,
    topic_index: u32,
) -> Vec<Vec<String>> {
    let partitioning = Partitioning::partition_id(TARGET_PARTITION);
    let sends = producers.iter().enumerate().map(|(producer, client)| {
        let partitioning = &partitioning;
        async move {
            let mut sent = Vec::with_capacity(PER_PRODUCER);
            for sequence in 0..PER_PRODUCER {
                let payload = format!("t{topic_index}-p{producer}-{sequence}");
                let mut messages = vec![IggyMessage::from_str(&payload).expect("build message")];
                client
                    .send_messages(stream, topic, partitioning, &mut messages)
                    .await
                    .unwrap_or_else(|error| panic!("send_messages {payload}: {error}"));
                sent.push(payload);
            }
            sent
        }
    });
    join_all(sends).await
}

/// Payloads of the target partition in offset order, asserting the offsets are
/// dense on the way out: a hole would mean an acked op the leader itself cannot
/// serve.
async fn poll_payloads(
    client: &IggyClient,
    stream: &Identifier,
    topic: &Identifier,
) -> Vec<String> {
    let polled = client
        .poll_messages(
            stream,
            topic,
            Some(TARGET_PARTITION),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            TOTAL_MESSAGES as u32,
            false,
        )
        .await
        .unwrap_or_else(|error| panic!("poll_messages: {error}"));
    for (expected, message) in polled.messages.iter().enumerate() {
        assert_eq!(
            message.header.offset, expected as u64,
            "offsets must be dense from 0, got {} at position {expected}",
            message.header.offset
        );
    }
    polled
        .messages
        .iter()
        .map(|message| String::from_utf8_lossy(&message.payload).into_owned())
        .collect()
}

/// Each producer's sends must appear in the order it made them. Nothing pins
/// the interleaving of four connections, but a partition that reordered one
/// producer's own ops reordered the log.
fn assert_producer_order(polled: &[String], sent: &[Vec<String>], topic_index: u32) {
    let positions: HashMap<&str, usize> = polled
        .iter()
        .enumerate()
        .map(|(position, payload)| (payload.as_str(), position))
        .collect();
    for (producer, payloads) in sent.iter().enumerate() {
        let mut previous: Option<(&str, usize)> = None;
        for payload in payloads {
            let position = *positions.get(payload.as_str()).unwrap_or_else(|| {
                panic!("topic {topic_index}: {payload} was acked but never polled back")
            });
            if let Some((earlier, earlier_position)) = previous {
                assert!(
                    earlier_position < position,
                    "topic {topic_index}: producer {producer} sent {earlier} before {payload}, \
                     but they polled back at {earlier_position} and {position}"
                );
            }
            previous = Some((payload.as_str(), position));
        }
    }
}

/// Poll until every node's segments hold every payload at a non-decreasing
/// position. A backup that lost a prepare to the gap check never gets it back,
/// so this is where the loss surfaces first, naming the node.
async fn wait_until_payloads_installed(harness: &TestHarness, payloads: &[String]) {
    let deadline = Instant::now() + FLUSH_INSTALL_TIMEOUT;
    loop {
        let pending: Vec<String> = (0..harness.cluster_size())
            .filter_map(|node| {
                disk::installed_payloads_complete(&harness.node(node).data_path(), payloads)
                    .err()
                    .map(|error| format!("node {node}: {error}"))
            })
            .collect();
        if pending.is_empty() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "every acked payload must reach every replica's segments within \
             {FLUSH_INSTALL_TIMEOUT:?}: {pending:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// The point of the test. A non-leader node logging the re-dispatch is a backup
/// that parked REPLICATED prepares: client requests only ever reach the leader,
/// which is the primary of every partition group created here.
fn assert_backup_re_dispatched(harness: &TestHarness, leader: usize) {
    let counts: Vec<(usize, usize)> = (0..harness.cluster_size())
        .map(|node| {
            (
                node,
                harness.node(node).stdout_occurrences(REDISPATCH_MARKER),
            )
        })
        .collect();
    let on_backups: usize = counts
        .iter()
        .filter(|(node, _)| *node != leader)
        .map(|(_, count)| *count)
        .sum();
    assert!(
        on_backups > 0,
        "no backup logged {REDISPATCH_MARKER:?} (leader is node {leader}, per-node counts \
         {counts:?}); either the produce never raced materialisation, in which case this test \
         proves nothing, or `RUST_LOG` in the environment overrode the debug level the marker \
         needs"
    );
}

fn assert_no_degraded_park_paths(harness: &TestHarness) {
    for node in 0..harness.cluster_size() {
        let server = harness.node(node);
        for marker in DEGRADED_MARKERS {
            assert_eq!(
                server.stdout_occurrences(marker),
                0,
                "node {node} logged {marker:?}: the park buffer degraded instead of converging"
            );
        }
        assert_eq!(
            server.stdout_occurrences(GAP_MARKER),
            0,
            "node {node} logged {GAP_MARKER:?}: a re-dispatched prepare lost its arrival \
             position, and the op it displaced is recoverable only by waiting out the \
             repair driver's debounce"
        );
    }
}
