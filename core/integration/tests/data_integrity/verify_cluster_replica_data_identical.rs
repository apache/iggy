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

//! Cross-replica on-disk data identity.
//!
//! VSR replicates each plane's log as a hash chain: every prepare header carries
//! `parent == previous entry checksum`, and recovery / view-change locate entries
//! by `(op, checksum)`. That makes one property load-bearing for safety: the
//! committed on-disk bytes for a given op must be identical on every replica. If
//! they differ, per-replica checksums differ, the chain identity breaks across
//! nodes, and recovery's checksum-keyed lookups miss.
//!
//! This test produces a single batch, then a small concurrent burst, then a long
//! sequential run to a 3-node cluster, and compares the partition segment `.log`
//! and the metadata-plane files byte-for-byte across every node. The burst
//! pipelines ops so backups journal ahead of their commit frontier (the window in
//! which a backup commit must persist only the committed prefix and keep the
//! uncommitted tail resident); the sequential run that follows drives many backup
//! commit cycles and would expose a backup left permanently behind. The segment
//! `.index` is excluded - see `integration::harness::disk`.
//!
//! The run stays far below the checkpoint margin, so the metadata WAL is still
//! byte-identical across replicas and is included in the comparison.

use iggy::prelude::*;
use integration::harness::disk;
use integration::iggy_harness;
use std::path::PathBuf;

const STREAM_NAME: &str = "di-stream";
const TOPIC_NAME: &str = "di-topic";
const CLUSTER_NODES: usize = 3;
const MESSAGES: u32 = 64;

// Sequential multi-batch phase: drive the backup commit path through many
// commit cycles (each batch is its own op), so `collect_committable_from_journal`
// runs repeatedly and `commit_messages` keeps proving the retain-the-tail /
// persist-only-committed contract op after op. Deterministic, unlike the burst.
const SEQUENTIAL_BATCHES: u32 = 24;
const SEQUENTIAL_BATCH_MESSAGES: u32 = 3;

// Concurrent producers that pipeline several ops at once so backups journal ops
// ahead of their commit frontier (op > commit_max) - the window in which a
// backup commit must persist ONLY the committed prefix and keep the uncommitted
// tail resident. Kept small and the connections are held open until after the
// cluster is stopped: a large fan-out plus a simultaneous disconnect storm trips
// unrelated metadata/consensus concurrency asserts (mass in-process Logout, a
// view-change with an over-deep in-flight range) and destabilises the cluster
// before this test can observe its own property.
const BURST_CLIENTS: usize = 8;
const BURST_SENDS_PER_CLIENT: u32 = 3;
const BURST_BATCH_MESSAGES: u32 = 2;

// `messages_required_to_save = 1` forces every committed batch to persist to its
// segment immediately on every node, so each replica materialises the segment
// files while running (the VSR server serves no flush_unsaved_buffer, and
// shutdown-flush would couple the test to drain behaviour). It is a topic
// creation option, so it travels with the topic to every replica.
#[iggy_harness(cluster_nodes = 3)]
async fn should_persist_byte_identical_data_across_cluster_replicas(harness: &mut TestHarness) {
    let client = harness.tcp_root_client().await.unwrap();
    client.create_stream(STREAM_NAME).await.unwrap();
    client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::NeverExpire),
                messages_required_to_save: Some(1),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();

    // Phase 1: a single batch. Exercises the simple one-op commit on every
    // replica and keeps the original byte-identity baseline.
    let mut messages: Vec<IggyMessage> = (0..MESSAGES)
        .map(|i| {
            IggyMessage::builder()
                .id(u128::from(i + 1))
                .payload(format!("replica-identity-payload-{i:04}").into())
                .build()
                .unwrap()
        })
        .collect();
    client
        .send_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .unwrap();

    // Phase 2: a concurrent burst that pipelines ops through the single
    // partition's consensus group, so backups journal ops above their
    // commit_max. On the buggy path a backup over-drains its journal on the first
    // committed op, persisting uncommitted bytes and dropping the headers of
    // still-uncommitted ops; that wedges its commit_min permanently (the dropped
    // headers never reappear), so it stops persisting from then on. The
    // connections are returned and held open until after the cluster stops: a
    // mid-test disconnect storm would trip an unrelated metadata Logout race.
    let _burst_clients =
        send_concurrent_burst(harness.tcp_root_clients(BURST_CLIENTS).await.unwrap()).await;

    // Phase 3: a long sequential run AFTER the burst. Each batch is its own
    // committed op, exercising the backup journal-fallback commit and the
    // persist-only-committed / retain-the-tail path once per op. It also exposes
    // a PERMANENT wedge from phase 2: a backup whose commit_min stalled cannot
    // advance past the gap, so none of these later batches reach its segment and
    // its `.log` falls behind the primary's at rest. A healthy backup persists
    // every batch and stays byte-identical.
    for batch_index in 0..SEQUENTIAL_BATCHES {
        let mut messages: Vec<IggyMessage> = (0..SEQUENTIAL_BATCH_MESSAGES)
            .map(|message_index| {
                let unique = batch_index * SEQUENTIAL_BATCH_MESSAGES + message_index;
                IggyMessage::builder()
                    .id(u128::from(MESSAGES + unique + 1) << 64)
                    .payload(format!("sequential-{batch_index:04}-{message_index:02}").into())
                    .build()
                    .unwrap()
            })
            .collect();
        client
            .send_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                &Partitioning::partition_id(0),
                &mut messages,
            )
            .await
            .unwrap();
    }

    let data_paths: Vec<PathBuf> = harness
        .all_servers()
        .iter()
        .map(|server| server.data_path())
        .collect();
    assert_eq!(
        data_paths.len(),
        CLUSTER_NODES,
        "expected {CLUSTER_NODES} node data dirs"
    );

    // Wait for replication + per-batch persistence to converge across nodes
    // before reading files at rest (replaces a fixed settle sleep).
    // `messages_required_to_save = 1` flushes every committed batch, so the
    // at-rest `.log` total tracks committed persistence while running.
    disk::wait_for_log_convergence(&data_paths).await;

    // Stop the whole cluster so every segment / metadata file is at rest. The
    // burst connections are still held; dropping them only after stop avoids the
    // concurrent in-process Logout race on the metadata plane.
    harness.stop().await.unwrap();
    drop(_burst_clients);

    disk::assert_replica_data_identical(&data_paths, true);
}

/// Drive a concurrent send burst across independent connections so the primary
/// pipelines multiple ops at once and backups journal ops ahead of their commit
/// frontier. Each client sends its own batches in a loop; the sends race so that
/// while one op is committing on a backup, later ops are already resident in its
/// journal. Message ids are disjoint per client to avoid any cross-batch id
/// collision. Every send must succeed: a wedged backup still acks (the primary
/// commits on quorum), so the divergence shows up on disk, not as a send error.
///
/// The clients are returned so the caller can hold the connections open until
/// after the cluster is stopped (a simultaneous disconnect storm trips an
/// unrelated metadata-plane in-process Logout race).
async fn send_concurrent_burst(clients: Vec<IggyClient>) -> Vec<IggyClient> {
    let mut handles = Vec::with_capacity(clients.len());
    for (client_index, client) in clients.into_iter().enumerate() {
        // Each task owns its own connection so the sends run concurrently and
        // the primary's pipeline fills with multiple in-flight ops; the client
        // is handed back so the connection stays open past the burst.
        let handle = tokio::spawn(async move {
            let stream = Identifier::named(STREAM_NAME).unwrap();
            let topic = Identifier::named(TOPIC_NAME).unwrap();
            let partitioning = Partitioning::partition_id(0);
            for send_index in 0..BURST_SENDS_PER_CLIENT {
                let base = ((client_index as u128) << 32) | (u128::from(send_index) << 8);
                let mut messages: Vec<IggyMessage> = (0..BURST_BATCH_MESSAGES)
                    .map(|message_index| {
                        IggyMessage::builder()
                            .id(base + u128::from(message_index) + 1)
                            .payload(
                                format!(
                                    "burst-{client_index:02}-{send_index:02}-{message_index:02}"
                                )
                                .into(),
                            )
                            .build()
                            .unwrap()
                    })
                    .collect();
                client
                    .send_messages(&stream, &topic, &partitioning, &mut messages)
                    .await
                    .unwrap();
            }
            client
        });
        handles.push(handle);
    }

    let mut clients = Vec::with_capacity(handles.len());
    for handle in handles {
        clients.push(handle.await.unwrap());
    }
    clients
}
