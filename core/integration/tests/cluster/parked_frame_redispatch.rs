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

//! Produce bursts after topic creation must converge on all replicas.
//!
//! The SDK looks up the partition route before sending. Replicas may all
//! materialise before the first prepare arrives, so this workload does not
//! guarantee that any frame parks. Partition primaries can also differ from
//! the metadata leader.
//!
//! Assert dense offsets, per-producer order, byte-identical replicas, and no
//! degraded park paths or partition gaps. Deterministic coverage of parked
//! prepare ordering lives in the simulator's
//! `parked_prepare_redispatch_is_seed_replayable`, which holds one backup
//! unmaterialised and checks the parked prefix precedes a later inbox op.
//!
//! The harness removes an ambient `RUST_LOG` when this test supplies its explicit
//! logging level, and the log oracle falls back from captured stdout to the
//! server's own log file so `IGGY_TEST_VERBOSE` cannot disable it.

use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use futures::future::join_all;
use iggy::prelude::*;
use integration::harness::{ServerHandle, TestHarness, disk};
use integration::iggy_harness;
use tokio::time::{Instant, sleep};

const STREAM: &str = "parked-redispatch-stream";
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
/// The metadata plane logs the same string, so the counting below pairs it with
/// `PARTITION_PLANE_FIELD`.
const GAP_MARKER: &str = "dropping out-of-order prepare (gap)";
const PARTITION_PLANE_FIELD: &str = "plane=\"partitions\"";

fn topic_name(index: u32) -> String {
    format!("parked-redispatch-topic-{index}")
}

#[iggy_harness(cluster_nodes = 3, server(logging.level = "info,shard=debug"))]
async fn given_a_new_topic_when_producers_send_a_burst_should_preserve_order_on_all_replicas(
    harness: &mut TestHarness,
) {
    // Bootstrap through the metadata leader; the SDK routes each send to its
    // partition's primary.
    let leader = disk::leader_node_index_via(harness, 0).await;
    let setup = harness
        .root_client_for_node(leader)
        .await
        .expect("root client on the metadata leader");
    setup
        .create_stream(STREAM, &StreamCreateOptions::default())
        .await
        .expect("create stream");
    let stream = Identifier::named(STREAM).expect("stream identifier");

    // Connect and log in before topic creation, leaving only routing and sends
    // on the burst path.
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

    assert_no_degraded_park_paths(harness);
    disk::assert_replica_data_identical(&data_paths, false);
}

/// `messages_required_to_save` + `durability=persisted` persist every committed batch
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
                durability: iggy_common::Durability::Persisted,
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
/// dense on the way out: a hole would mean an acked op the partition primary
/// cannot serve.
async fn poll_payloads(
    client: &IggyClient,
    stream: &Identifier,
    topic: &Identifier,
) -> Vec<String> {
    let polled = client
        .poll_messages(
            stream,
            topic,
            TARGET_PARTITION,
            &Consumer::default(),
            &PollingStrategy::offset(0),
            PollOptions {
                count: TOTAL_MESSAGES as u32,
                ..PollOptions::default()
            },
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

/// Check payload completeness before comparing segments, so identical but
/// incomplete replicas cannot pass.
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

fn assert_no_degraded_park_paths(harness: &TestHarness) {
    for node in 0..harness.cluster_size() {
        let server = harness.node(node);
        let log = server_log_plain(server);
        for marker in DEGRADED_MARKERS {
            assert_eq!(
                log.matches(marker).count(),
                0,
                "node {node} logged {marker:?}: the park buffer degraded instead of converging"
            );
        }
        let partition_gaps = log
            .lines()
            .filter(|line| line.contains(GAP_MARKER) && line.contains(PARTITION_PLANE_FIELD))
            .count();
        assert_eq!(
            partition_gaps, 0,
            "node {node} logged {GAP_MARKER:?}: a re-dispatched prepare lost its arrival \
             position, and the op it displaced is recoverable only by waiting out the \
             repair driver's debounce"
        );
    }
}

/// The shared handle falls back to the server's own appender when
/// `IGGY_TEST_VERBOSE` makes the child inherit stdout.
fn server_log_plain(server: &ServerHandle) -> String {
    server.stdout_plain()
}
