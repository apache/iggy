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

//! Acked-write durability across real SIGKILL crashes.
//!
//! Every other harness fault is a graceful stop whose shutdown hook flushes
//! buffers before the process exits. SIGKILL runs no hook, so these tests pin
//! what the durability contract actually promises at the moment of the ack:
//! a confirmation (`SendMessagesResponse::confirmations`) is granted on
//! quorum COMMIT, which for a topic below its flush thresholds means the
//! batch lives only in the in-memory partition journal
//! (`PartitionJournalMemStorage`) on each node. Process death never loses a
//! completed `write()` (the page cache survives the process), so what SIGKILL
//! probes is precisely that written-before-acked gap, not fsync ordering.
//!
//! Single-node-crash tests must hold every ack via the surviving quorum;
//! the whole-cluster-crash test must hold exactly what reached the segments.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use iggy::prelude::*;
use integration::harness::{TestHarness, disk};
use integration::iggy_harness;
use tokio::time::sleep;

const STREAM_NAME: &str = "crash-stream";
const TOPIC_NAME: &str = "crash-topic";
const PARTITION_ID: u32 = 0;
const CONSUMER_ID: u32 = 1;

/// Bounds a full election plus a rejoining node's recovery on slow CI runners.
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(60);
/// Budget for the survivors to elect a new primary after the old one is killed.
const ELECTION_TIMEOUT: Duration = Duration::from_secs(30);
/// Budget for a committed consumer offset to reach a replica's disk.
const OFFSET_REPLICATION_TIMEOUT: Duration = Duration::from_secs(15);
/// Budget for eagerly flushed batches to land in every node's segment files.
const FLUSH_INSTALL_TIMEOUT: Duration = Duration::from_secs(20);
const POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Acks the producer loop in the primary-kill test must capture before the
/// primary dies, so the surviving quorum has a meaningful prefix to preserve.
const PRE_KILL_ACKS: usize = 50;

async fn create_stream_and_topic(client: &IggyClient, eager_flush: bool) {
    client
        .create_stream(STREAM_NAME)
        .await
        .expect("create stream");
    // `eager_flush` persists and fsyncs every committed batch on every
    // replica before anything else happens on the partition; it is a topic
    // creation option, so it travels with the topic to all nodes. The default
    // shape leaves the 1024-message / 1 MiB thresholds in place, so small
    // runs are acked from RAM only.
    let options = if eager_flush {
        TopicCreateOptions {
            partitions_count: Some(1),
            message_expiry: Some(IggyExpiry::NeverExpire),
            messages_required_to_save: Some(1),
            durability: iggy_common::Durability::Persisted,
            ..TopicCreateOptions::default()
        }
    } else {
        TopicCreateOptions {
            partitions_count: Some(1),
            message_expiry: Some(IggyExpiry::NeverExpire),
            ..TopicCreateOptions::default()
        }
    };
    client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            &options,
        )
        .await
        .expect("create topic");
}

/// Send `count` single-message batches, returning each confirmed
/// `(base_offset, payload)`. One message per send, so every confirmation pins
/// exactly one offset.
async fn produce_acked(
    client: &IggyClient,
    payload_prefix: &str,
    count: u32,
) -> Vec<(u64, String)> {
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let mut acked = Vec::with_capacity(count as usize);
    for index in 0..count {
        let payload = format!("{payload_prefix}-{index:05}");
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
        let confirmation = response
            .confirmations
            .first()
            .unwrap_or_else(|| panic!("the VSR server confirms every send, none for {payload}"));
        acked.push((confirmation.base_offset, payload));
    }
    acked
}

/// Poll from offset 0 until every acked `(offset, payload)` reads back at its
/// confirmed offset with offsets strictly increasing, or `budget` runs out.
/// `Err` carries the last observed shortfall so the caller's verdict can show
/// what was actually readable.
async fn wait_for_acked_readable(
    client: &IggyClient,
    acked: &[(u64, String)],
    budget: Duration,
) -> Result<(), String> {
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let deadline = tokio::time::Instant::now() + budget;
    let want = acked.len() as u32 + 16;
    let mut state;
    loop {
        match client
            .poll_messages(
                &stream,
                &topic,
                Some(PARTITION_ID),
                &Consumer::default(),
                &PollingStrategy::offset(0),
                want,
                false,
            )
            .await
        {
            Ok(polled) => {
                let offsets: Vec<u64> = polled
                    .messages
                    .iter()
                    .map(|message| message.header.offset)
                    .collect();
                let in_order = offsets.windows(2).all(|pair| pair[0] < pair[1]);
                let by_offset: HashMap<u64, &[u8]> = polled
                    .messages
                    .iter()
                    .map(|message| (message.header.offset, message.payload.as_ref()))
                    .collect();
                let missing: Vec<u64> = acked
                    .iter()
                    .filter(|(offset, payload)| {
                        by_offset
                            .get(offset)
                            .is_none_or(|bytes| *bytes != payload.as_bytes())
                    })
                    .map(|(offset, _)| *offset)
                    .collect();
                if in_order && missing.is_empty() {
                    return Ok(());
                }
                state = format!(
                    "{} of {} acked offsets readable ({} polled, in order: {in_order}), \
                     first missing or payload-mismatched offset: {:?}",
                    acked.len() - missing.len(),
                    acked.len(),
                    polled.messages.len(),
                    missing.first(),
                );
            }
            Err(error) => state = format!("poll failed: {error}"),
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(state);
        }
        sleep(POLL_INTERVAL).await;
    }
}

/// Connect a root client to the first node in `nodes` that accepts one,
/// `None` when none do (mid-election, or a node still restarting).
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

/// Number of nodes the metadata roster marks as leader, `None` if the query
/// fails (mid-election, connection dropped).
async fn leader_count(client: &IggyClient) -> Option<usize> {
    let metadata = client.get_cluster_metadata().await.ok()?;
    Some(
        metadata
            .nodes
            .iter()
            .filter(|node| node.role == ClusterNodeRole::Leader)
            .count(),
    )
}

/// Poll until one of `nodes` serves the test stream under exactly one leader,
/// returning the connected client. Panics at the deadline.
async fn wait_until_cluster_serves(
    harness: &TestHarness,
    nodes: &[usize],
    budget: Duration,
) -> IggyClient {
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        if let Some(client) = connect_any(harness, nodes).await
            && matches!(client.get_stream(&stream).await, Ok(Some(_)))
            && leader_count(&client).await == Some(1)
        {
            return client;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "cluster did not serve the stream under a single leader within {budget:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// Poll until every node's segment files hold all of `payloads`, so a
/// follow-up SIGKILL provably erases nothing that was flushed. Replaces a
/// settle sleep: the ack proves quorum COMMIT, but each replica flushes on
/// its own commit walk.
async fn wait_until_payloads_installed(
    harness: &TestHarness,
    payloads: &[String],
    budget: Duration,
) {
    let deadline = tokio::time::Instant::now() + budget;
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
            tokio::time::Instant::now() < deadline,
            "eagerly flushed batches did not reach every node's segments within {budget:?}: \
             {pending:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// Poll a node's on-disk consumer-offset record until it equals `expected`.
/// Panics at the deadline.
async fn wait_for_disk_consumer_offset(
    harness: &TestHarness,
    node: usize,
    expected: u64,
    budget: Duration,
) {
    let data_path = harness.node(node).data_path();
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        if disk::read_replicated_consumer_offset(&data_path) == Some(expected) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "node {node} did not persist the replicated consumer offset {expected} \
             within {budget:?} (found {:?})",
            disk::read_replicated_consumer_offset(&data_path),
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// A backup dies by SIGKILL and rejoins from its intact data dir. Every ack
/// granted before, during, and after the outage must poll back in order, and
/// once converged all three replicas must hold byte-identical partition
/// segments (the hash-chain identity recovery depends on).
#[iggy_harness(cluster_nodes = 3)]
async fn given_committed_sends_when_a_backup_is_killed_and_restarts_should_preserve_all_acked_offsets_byte_identical(
    harness: &mut TestHarness,
) {
    let client = harness.tcp_root_client().await.unwrap();
    create_stream_and_topic(&client, true).await;
    let mut acked = produce_acked(&client, "pre-kill", 200).await;

    let leader = disk::leader_node_index(harness).await;
    let backup = (0..harness.cluster_size())
        .find(|index| *index != leader)
        .expect("a 3-node cluster has a backup");
    harness.kill_node(backup).expect("SIGKILL a backup");

    acked.extend(produce_acked(&client, "during-outage", 50).await);

    harness
        .restart_node(backup)
        .expect("restart the killed backup");

    wait_for_acked_readable(&client, &acked, CONVERGE_TIMEOUT)
        .await
        .unwrap_or_else(|state| {
            panic!("every acked offset must poll back after the backup rejoins: {state}")
        });

    let data_paths: Vec<PathBuf> = harness
        .all_servers()
        .iter()
        .map(|server| server.data_path())
        .collect();
    disk::wait_for_log_convergence(&data_paths).await;
    // Stop the cluster so every segment is at rest before the byte compare.
    harness
        .stop()
        .await
        .expect("stop the cluster for the at-rest comparison");
    disk::assert_replica_data_identical(&data_paths, false);
}

/// The primary dies by SIGKILL while a producer is mid-flight. On a DEFAULT
/// topic an ack proves only a RAM quorum, so this pins the core VSR promise:
/// the two survivors hold every acked batch in their journals and the new
/// primary must serve all of them.
#[iggy_harness(cluster_nodes = 3)]
async fn given_mid_flight_produce_when_the_primary_is_killed_should_elect_and_preserve_acked_offsets(
    harness: &mut TestHarness,
) {
    let setup_client = harness.tcp_root_client().await.unwrap();
    create_stream_and_topic(&setup_client, false).await;

    let producer_client = harness.tcp_root_client().await.unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let acked_count = Arc::new(AtomicUsize::new(0));
    let producer = tokio::spawn({
        let stop = stop.clone();
        let acked_count = acked_count.clone();
        async move {
            let stream = Identifier::named(STREAM_NAME).unwrap();
            let topic = Identifier::named(TOPIC_NAME).unwrap();
            let partitioning = Partitioning::partition_id(PARTITION_ID);
            let mut acked: Vec<(u64, String)> = Vec::new();
            let mut index = 0u32;
            loop {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                let payload = format!("mid-flight-{index:05}");
                let mut messages = vec![
                    IggyMessage::builder()
                        .payload(payload.clone().into())
                        .build()
                        .expect("build message"),
                ];
                match producer_client
                    .send_messages(&stream, &topic, &partitioning, &mut messages)
                    .await
                {
                    Ok(response) => {
                        if let Some(confirmation) = response.confirmations.first() {
                            acked.push((confirmation.base_offset, payload));
                            acked_count.fetch_add(1, Ordering::Relaxed);
                        }
                        index += 1;
                    }
                    // The kill lands mid-send; whatever error surfaces ends
                    // the run. Only Ok confirmations count as acked.
                    Err(_) => break,
                }
            }
            acked
        }
    });

    let deadline = tokio::time::Instant::now() + ELECTION_TIMEOUT;
    while acked_count.load(Ordering::Relaxed) < PRE_KILL_ACKS {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the producer must reach {PRE_KILL_ACKS} acked sends before the kill"
        );
        sleep(Duration::from_millis(50)).await;
    }

    let leader = disk::leader_node_index(harness).await;
    harness.kill_node(leader).expect("SIGKILL the primary");
    stop.store(true, Ordering::Relaxed);

    let survivors: Vec<usize> = (0..harness.cluster_size())
        .filter(|index| *index != leader)
        .collect();
    let survivor_client = wait_until_cluster_serves(harness, &survivors, ELECTION_TIMEOUT).await;

    // The producer's in-flight send may burn its full transport timeout
    // against the dead primary before erroring out; bound the join so a
    // wedged client fails loudly instead of hanging the test.
    let acked = tokio::time::timeout(Duration::from_secs(120), producer)
        .await
        .expect("the producer client wedged after the primary kill instead of failing its send")
        .expect("join the producer task");
    assert!(
        acked.len() >= PRE_KILL_ACKS,
        "at least the pre-kill acks must have been captured: got {}, expected at least \
         {PRE_KILL_ACKS}",
        acked.len()
    );

    wait_for_acked_readable(&survivor_client, &acked, CONVERGE_TIMEOUT)
        .await
        .unwrap_or_else(|state| {
            panic!(
                "every offset acked before the primary died must be readable from the \
                 surviving quorum: {state}"
            )
        });
}

/// A committed consumer offset must survive the crash of a backup. The offset
/// is stored while the backup is already dead, so only the surviving quorum
/// holds it (and persists it) and the rejoining node can converge to it only
/// through replication, never from its own pre-crash disk.
#[iggy_harness(cluster_nodes = 3)]
async fn given_a_committed_consumer_offset_when_a_backup_is_killed_should_survive_on_the_survivors(
    harness: &mut TestHarness,
) {
    let client = harness.tcp_root_client().await.unwrap();
    create_stream_and_topic(&client, false).await;
    let acked = produce_acked(&client, "offset-fodder", 10).await;
    let stored_offset = acked.last().expect("ten acked sends").0;

    let leader = disk::leader_node_index(harness).await;
    let backup = (0..harness.cluster_size())
        .find(|index| *index != leader)
        .expect("a 3-node cluster has a backup");
    harness.kill_node(backup).expect("SIGKILL a backup");

    let consumer = Consumer::new(Identifier::numeric(CONSUMER_ID).unwrap());
    client
        .store_consumer_offset(
            &consumer,
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            Some(PARTITION_ID),
            stored_offset,
        )
        .await
        .expect("store the consumer offset (commits on the surviving quorum)");

    for node in (0..harness.cluster_size()).filter(|index| *index != backup) {
        wait_for_disk_consumer_offset(harness, node, stored_offset, OFFSET_REPLICATION_TIMEOUT)
            .await;
    }

    harness
        .restart_node(backup)
        .expect("restart the killed backup");
    wait_for_disk_consumer_offset(harness, backup, stored_offset, CONVERGE_TIMEOUT).await;
}

/// Whole-cluster SIGKILL with an eagerly flushed topic: what reached the
/// segments before the crash must be served by the reformed cluster. The kill
/// is gated on every node's segments holding every payload, so the recovery
/// obligation is exact. No settled primary survives, so the replicas must
/// give up probing and elect among their recovered logs.
#[iggy_harness(cluster_nodes = 3)]
async fn given_eager_flush_topic_when_the_whole_cluster_is_killed_should_recover_flushed_data(
    harness: &mut TestHarness,
) {
    let client = harness.tcp_root_client().await.unwrap();
    create_stream_and_topic(&client, true).await;
    let acked = produce_acked(&client, "flushed", 30).await;
    let payloads: Vec<String> = acked.iter().map(|(_, payload)| payload.clone()).collect();
    wait_until_payloads_installed(harness, &payloads, FLUSH_INSTALL_TIMEOUT).await;

    // The producer's connection stays open across the kill on purpose.
    // Closing it first replicates a Logout, and journaling that op is what
    // tells a backup the topic create before it committed: a backup recovers
    // its commit point from the watermark stamped on the NEXT entry. With the
    // create as the last journaled op, the backups boot without the partition
    // and must recover it from disk once the metadata election re-commits it.
    harness.kill_cluster().expect("SIGKILL every node");
    harness
        .restart_cluster()
        .await
        .expect("restart the whole cluster");

    let nodes: Vec<usize> = (0..harness.cluster_size()).collect();
    let client = wait_until_cluster_serves(harness, &nodes, CONVERGE_TIMEOUT).await;
    wait_for_acked_readable(&client, &acked, CONVERGE_TIMEOUT)
        .await
        .unwrap_or_else(|state| {
            panic!("eagerly flushed acked data must survive a whole-cluster SIGKILL: {state}")
        });
}

/// The kill follows the confirmation without waiting for segment installation.
/// This exercises the prepare WAL rather than graceful shutdown or page-cache flushes.
#[iggy_harness(cluster_nodes = 3)]
async fn given_persisted_topic_when_killed_below_flush_threshold_should_recover_acked_messages(
    harness: &mut TestHarness,
) {
    verify_persisted_restart(harness, Durability::Persisted).await;
}

#[iggy_harness(cluster_nodes = 1)]
async fn given_persisted_singleton_when_killed_below_flush_threshold_should_recover_acked_messages(
    harness: &mut TestHarness,
) {
    verify_persisted_restart(harness, Durability::Persisted).await;
}

#[iggy_harness(cluster_nodes = 3)]
async fn given_mixed_durability_when_killed_below_flush_threshold_should_recover_offset_predecessors(
    harness: &mut TestHarness,
) {
    verify_persisted_restart(harness, Durability::Replicated).await;
}

async fn verify_persisted_restart(harness: &mut TestHarness, durability: Durability) {
    let client = harness.tcp_root_client().await.unwrap();
    client.create_stream(STREAM_NAME).await.unwrap();
    let stream = Identifier::named(STREAM_NAME).unwrap();
    client
        .create_topic(
            &stream,
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                durability,
                consumer_offset_durability: Durability::Persisted,
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let consumer = Consumer::new(Identifier::numeric(CONSUMER_ID).unwrap());
    let mut acked = Vec::new();
    for group in 0..3 {
        acked.extend(produce_acked(&client, &format!("durable-prepare-{group}"), 4).await);
        client
            .store_consumer_offset(
                &consumer,
                &stream,
                &topic,
                Some(PARTITION_ID),
                acked.last().unwrap().0,
            )
            .await
            .unwrap();
    }
    let stored_offset = acked.last().unwrap().0;
    harness.kill_cluster().unwrap();
    harness.restart_cluster().await.unwrap();
    let nodes: Vec<usize> = (0..harness.cluster_size()).collect();
    let client = wait_until_cluster_serves(harness, &nodes, CONVERGE_TIMEOUT).await;
    wait_for_acked_readable(&client, &acked, CONVERGE_TIMEOUT)
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + CONVERGE_TIMEOUT;
    loop {
        let stored = client
            .get_consumer_offset(&consumer, &stream, &topic, Some(PARTITION_ID))
            .await
            .unwrap();
        if stored.is_some_and(|stored| stored.stored_offset == stored_offset) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "durable offset was not recovered"
        );
        sleep(POLL_INTERVAL).await;
    }
}

#[iggy_harness(cluster_nodes = 3)]
async fn given_persisted_topic_when_backup_misses_writes_should_repair_before_durable_ack(
    harness: &mut TestHarness,
) {
    let client = harness.tcp_root_client().await.unwrap();
    client.create_stream(STREAM_NAME).await.unwrap();
    client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                durability: Durability::Persisted,
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();
    let mut acked = produce_acked(&client, "before-repair", 4).await;
    let leader = disk::leader_node_index(harness).await;
    let backup = (0..3).find(|node| *node != leader).unwrap();
    harness.kill_node(backup).unwrap();
    acked.extend(produce_acked(&client, "missed", 8).await);
    harness.restart_node(backup).unwrap();
    sleep(Duration::from_secs(6)).await;
    acked.extend(produce_acked(&client, "after-repair", 4).await);
    harness.kill_cluster().unwrap();
    harness.restart_cluster().await.unwrap();
    let client = wait_until_cluster_serves(harness, &[0, 1, 2], CONVERGE_TIMEOUT).await;
    wait_for_acked_readable(&client, &acked, CONVERGE_TIMEOUT)
        .await
        .unwrap();
}

#[iggy_harness(cluster_nodes = 3, server(partition.wal_bytes_max = "134225920 B"))]
async fn given_all_replicas_checkpointed_when_restarted_should_elect_and_extend_the_log(
    harness: &mut TestHarness,
) {
    let client = harness.tcp_root_client().await.unwrap();
    let stream_details = client.create_stream(STREAM_NAME).await.unwrap();
    let stream = Identifier::numeric(stream_details.id).unwrap();
    let topic_details = client
        .create_topic(
            &stream,
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                durability: Durability::Persisted,
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();
    let topic = Identifier::numeric(topic_details.id).unwrap();
    for batch in 1..=2u8 {
        let payload = bytes::Bytes::from(vec![batch; 1024 * 1024]);
        let mut messages = (0..33)
            .map(|_| {
                IggyMessage::builder()
                    .payload(payload.clone())
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
    }
    let deadline = tokio::time::Instant::now() + CONVERGE_TIMEOUT;
    loop {
        let checkpointed = (0..3).all(|node| {
            let directory = harness.node(node).data_path().join(format!(
                "streams/{}/topics/{}/partitions/0",
                stream_details.id, topic_details.id
            ));
            std::fs::read_dir(directory).ok().is_some_and(|entries| {
                entries.flatten().any(|entry| {
                    entry.file_name().to_string_lossy().starts_with("prepares-")
                        && std::fs::read(entry.path().join("frontier"))
                            .ok()
                            .is_some_and(|bytes| {
                                bytes.len() == 4096
                                    && u64::from_le_bytes(bytes[48..56].try_into().unwrap()) == 2
                                    && u64::from_le_bytes(bytes[72..80].try_into().unwrap()) == 2
                            })
                })
            })
        });
        if checkpointed {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "all replicas must checkpoint the same head before restart"
        );
        sleep(POLL_INTERVAL).await;
    }
    harness.kill_cluster().unwrap();
    harness.restart_cluster().await.unwrap();
    let client = wait_until_cluster_serves(harness, &[0, 1, 2], CONVERGE_TIMEOUT).await;
    // The transferred checkpoint body exceeds the former offsets-only
    // artifact cap, so this also verifies admission of a large prepare.
    let payload = bytes::Bytes::from(vec![3; 1024 * 1024]);
    let mut next = (0..33)
        .map(|_| {
            IggyMessage::builder()
                .payload(payload.clone())
                .build()
                .unwrap()
        })
        .collect::<Vec<_>>();
    let clients = [
        client,
        harness.root_client_for_node(1).await.unwrap(),
        harness.root_client_for_node(2).await.unwrap(),
    ];
    let deadline = tokio::time::Instant::now() + CONVERGE_TIMEOUT;
    let writer = 'admission: loop {
        for (index, client) in clients.iter().enumerate() {
            match client
                .send_messages(&stream, &topic, &Partitioning::partition_id(0), &mut next)
                .await
            {
                Ok(_) => break 'admission index,
                Err(IggyError::TransientNotAccepted) => {}
                Err(error) => panic!("checkpointed cluster did not resume writes: {error}"),
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "checkpointed partition must elect a primary"
        );
        sleep(POLL_INTERVAL).await;
    };
    let client = &clients[writer];
    for (offset, value) in [(0, 1u8), (65, 2u8)] {
        let polled = client
            .poll_messages(
                &stream,
                &topic,
                Some(0),
                &Consumer::default(),
                &PollingStrategy::offset(offset),
                1,
                false,
            )
            .await
            .unwrap();
        assert_eq!(polled.messages.len(), 1);
        assert_eq!(
            polled.messages[0].payload.as_ref(),
            vec![value; 1024 * 1024]
        );
    }
    verify_checkpoint_quarantine(harness, stream_details.id, topic_details.id, 2).await;
    verify_checkpoint_quarantine(harness, stream_details.id, topic_details.id, 1).await;
    verify_transferred_quorum(harness, &stream, &topic).await;
}

async fn verify_checkpoint_quarantine(
    harness: &mut TestHarness,
    stream_id: u32,
    topic_id: u32,
    node: usize,
) {
    let directory = harness.node(node).data_path().join(format!(
        "streams/{stream_id}/topics/{topic_id}/partitions/0"
    ));
    harness.kill_node(node).unwrap();
    // An empty segment starting inside the existing segment makes the chain
    // structurally invalid even when recovery can use a sparse index.
    std::fs::write(directory.join("00000000000000000001.log"), []).unwrap();
    std::fs::write(directory.join("00000000000000000001.index"), []).unwrap();
    harness.restart_node(node).unwrap();
    let client = harness.root_client_for_node(node).await.unwrap();
    let stream = Identifier::numeric(stream_id).unwrap();
    let topic = Identifier::numeric(topic_id).unwrap();
    let deadline = tokio::time::Instant::now() + CONVERGE_TIMEOUT;
    loop {
        let repaired = client
            .poll_messages(
                &stream,
                &topic,
                Some(0),
                &Consumer::default(),
                &PollingStrategy::offset(0),
                1,
                false,
            )
            .await
            .is_ok_and(|polled| {
                polled
                    .messages
                    .first()
                    .is_some_and(|message| message.payload.as_ref() == vec![1; 1024 * 1024])
            });
        let quarantined = std::fs::read_dir(directory.parent().unwrap())
            .unwrap()
            .flatten()
            .any(|entry| entry.file_name().to_string_lossy().starts_with("0.fenced."));
        if repaired && quarantined && !directory.join("materialization.missing").exists() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "quarantined checkpoint must be restored by full state transfer"
        );
        sleep(POLL_INTERVAL).await;
    }
}

async fn verify_transferred_quorum(
    harness: &mut TestHarness,
    stream: &Identifier,
    topic: &Identifier,
) {
    harness.kill_cluster().unwrap();
    harness.restart_node(1).unwrap();
    harness.restart_node(2).unwrap();
    let _ = wait_until_cluster_serves(harness, &[1, 2], CONVERGE_TIMEOUT).await;
    let clients = [
        harness.root_client_for_node(1).await.unwrap(),
        harness.root_client_for_node(2).await.unwrap(),
    ];
    let mut messages = vec![
        IggyMessage::builder()
            .payload(bytes::Bytes::from_static(b"after-donor-loss"))
            .build()
            .unwrap(),
    ];
    let deadline = tokio::time::Instant::now() + CONVERGE_TIMEOUT;
    loop {
        for client in &clients {
            match client
                .send_messages(stream, topic, &Partitioning::partition_id(0), &mut messages)
                .await
            {
                Ok(_) => return,
                Err(IggyError::TransientNotAccepted) => {}
                Err(error) => panic!("transferred quorum rejected the next operation: {error}"),
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "transferred replicas must elect without their donor"
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// Every persisted case in this file kills with SIGKILL, and the module doc
/// above states why that cannot reach fsync ordering. This pins the premise:
/// a completed `write()` lives in the page cache, which the kernel owns, so
/// process death cannot lose it. Consequently none of the barriers this PR
/// adds -- `file.sync()`, `sync_data()`, `fsync_dir()` -- changes the outcome
/// of a single test here, and `persisted` differs from `replicated` only in
/// surviving writes that were never synced.
///
/// Covering the real contract needs a fault that discards unsynced pages:
/// either the deterministic simulator (drop the persisted-topic assert at
/// `core/shard/src/lib.rs:4193-4198` and route partition storage through
/// `DurableStorage`) or `dm-log-writes` / `dm-flakey --drop_writes` under the
/// data directory.
#[test]
#[ignore = "PR #4092 review: SIGKILL cannot lose a completed write, so no test in this file distinguishes a correct barrier order from a deleted one"]
fn given_a_completed_write_when_the_process_is_sigkilled_then_the_bytes_should_not_survive() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("unsynced");
    let status = std::process::Command::new("sh")
        .arg("-c")
        .arg(r#"printf 'acknowledged' > "$0"; kill -9 $$"#)
        .arg(&path)
        .status()
        .expect("spawn a writer that dies before any barrier");
    assert!(
        !status.success(),
        "the writer exited normally, so it is not modelling a crash"
    );

    let survived = std::fs::read(&path).unwrap_or_default();
    assert!(
        survived.is_empty(),
        "an unsynced write survived SIGKILL, so no test in this file can distinguish a correct barrier order from a deleted one: {}",
        String::from_utf8_lossy(&survived)
    );
}
