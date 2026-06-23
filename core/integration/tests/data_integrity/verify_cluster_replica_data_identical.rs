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
//! `.index` is excluded - see `is_comparable`.

use iggy::prelude::*;
use integration::iggy_harness;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::time::sleep;

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

// Poll the per-node segment `.log` sizes until they agree and hold steady,
// instead of a fixed sleep: a fixed wait either flakes under CI load or hides a
// real replication lag. `messages_required_to_save = 1` flushes every committed
// batch, so the at-rest `.log` total tracks committed persistence while running.
const CONVERGENCE_POLL_INTERVAL: Duration = Duration::from_millis(200);
const CONVERGENCE_DEADLINE: Duration = Duration::from_secs(20);
const CONVERGENCE_STABLE_POLLS: u32 = 3;

// `messages_required_to_save = 1` forces every committed batch to persist to its
// segment immediately on every node, so each replica materialises the segment
// files while running (the VSR server serves no flush_unsaved_buffer, and
// shutdown-flush would couple the test to drain behaviour). The harness applies
// the `server(...)` config to every cluster node.
#[iggy_harness(
    cluster_nodes = 3,
    server(partition.messages_required_to_save = 1)
)]
async fn should_persist_byte_identical_data_across_cluster_replicas(harness: &mut TestHarness) {
    let client = harness.tcp_root_client().await.unwrap();
    client.create_stream(STREAM_NAME).await.unwrap();
    client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            1,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
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
    wait_for_log_convergence(&data_paths).await;

    // Stop the whole cluster so every segment / metadata file is at rest. The
    // burst connections are still held; dropping them only after stop avoids the
    // concurrent in-process Logout race on the metadata plane.
    harness.stop().await.unwrap();
    drop(_burst_clients);

    let per_node: Vec<BTreeMap<String, Vec<u8>>> = data_paths
        .iter()
        .map(|root| collect_comparable_files(root))
        .collect();

    for (idx, node) in per_node.iter().enumerate() {
        eprintln!(
            "node {idx}: {} comparable file(s): {:?}",
            node.len(),
            node.iter()
                .map(|(rel, bytes)| format!("{rel} ({} B)", bytes.len()))
                .collect::<Vec<_>>()
        );
    }

    assert_replica_data_identical(&per_node);
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

/// Poll each node's total segment `.log` bytes until all nodes agree and the
/// figure holds steady for `CONVERGENCE_STABLE_POLLS` consecutive polls, or the
/// deadline elapses. On timeout, return anyway: the byte-for-byte compare after
/// stop then fails with a precise diff instead of this masking a real lag.
async fn wait_for_log_convergence(data_paths: &[PathBuf]) {
    let deadline = tokio::time::Instant::now() + CONVERGENCE_DEADLINE;
    let mut previous: Option<Vec<u64>> = None;
    let mut stable_polls = 0u32;
    loop {
        let sizes: Vec<u64> = data_paths
            .iter()
            .map(|root| total_log_bytes(root))
            .collect();
        let all_equal = sizes.iter().all(|size| *size == sizes[0]);
        if all_equal && previous.as_ref() == Some(&sizes) {
            stable_polls += 1;
            if stable_polls >= CONVERGENCE_STABLE_POLLS {
                return;
            }
        } else {
            stable_polls = 0;
        }
        if tokio::time::Instant::now() >= deadline {
            return;
        }
        previous = Some(sizes);
        sleep(CONVERGENCE_POLL_INTERVAL).await;
    }
}

/// Total bytes of every partition segment `.log` under a node's data dir.
/// Mirrors the `.log` selection in `is_comparable`; sizes only, no contents.
fn total_log_bytes(root: &Path) -> u64 {
    let mut total = 0;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_dir() {
                stack.push(path);
            } else if file_type.is_file()
                && let Ok(rel) = path.strip_prefix(root)
            {
                let rel = rel.to_string_lossy().replace('\\', "/");
                if rel.starts_with("streams/") && rel.ends_with(".log") {
                    total += fs::metadata(&path).map(|meta| meta.len()).unwrap_or(0);
                }
            }
        }
    }
    total
}

/// A file (relative to a node's data dir) whose bytes must match across replicas:
/// the partition segment `.log` and the replicated metadata WAL. Per-node files
/// (logs, runtime, config, stdout) are excluded by construction.
///
/// Only the replicated, byte-identical artifacts are compared. Two metadata-plane
/// files are deliberately excluded as local, per-replica artifacts:
///
/// - `metadata/snapshot.bin`: a local compaction artifact stamped with
///   `created_at = now()` and a per-replica `sequence_number` at each node's own
///   checkpoint, plus unsorted hashmap iteration order. It can never match across
///   replicas even when the data is identical, so comparing it would ship a
///   nondeterministic oracle (it is only written once a node crosses the
///   checkpoint margin, so the flake is latent, not constant).
/// - `state/`: not populated by the VSR plane (0 bytes today); excluded for the
///   same local-artifact reason so it cannot start flaking if that ever changes.
///
/// The segment `.index` is excluded for the same class of reason: a local sparse
/// index (one entry per persist flush), not replicated and not part of the VSR
/// hash chain; recovery rebuilds it from the `.log`. Its byte length tracks the
/// number of `commit_messages` flushes, which differs by commit cadence - the
/// primary commits one op per quorum ack (one flush per op), a backup commits a
/// whole heartbeat's committed range in a single `commit_journal` (one flush for
/// many ops). So the `.index` legitimately differs across replicas for a
/// multi-batch stream even though every committed `.log` byte is identical.
fn is_comparable(rel: &str) -> bool {
    let is_segment = rel.starts_with("streams/") && rel.ends_with(".log");
    let is_metadata_wal = rel == "metadata/journal.wal";
    is_segment || is_metadata_wal
}

fn collect_comparable_files(root: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut files = BTreeMap::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_dir() {
                stack.push(path);
            } else if file_type.is_file()
                && let Ok(rel) = path.strip_prefix(root)
            {
                let rel = rel.to_string_lossy().replace('\\', "/");
                if is_comparable(&rel) {
                    let bytes = fs::read(&path)
                        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
                    files.insert(rel, bytes);
                }
            }
        }
    }
    files
}

fn assert_replica_data_identical(per_node: &[BTreeMap<String, Vec<u8>>]) {
    // Guard against a vacuous pass: node 0 must actually hold a produced segment,
    // otherwise nothing was persisted and the comparison proves nothing.
    let node0 = &per_node[0];
    assert!(
        node0
            .keys()
            .any(|k| k.starts_with("streams/") && k.ends_with(".log")),
        "node 0 holds no segment .log under streams/ - no partition data was persisted, \
         so the cross-replica comparison would be vacuous. Comparable files: {:?}",
        node0.keys().collect::<Vec<_>>()
    );

    let all_keys: BTreeSet<&str> = per_node
        .iter()
        .flat_map(|node| node.keys().map(String::as_str))
        .collect();

    let mut problems = Vec::new();
    for key in all_keys {
        let mut reference: Option<(usize, &[u8])> = None;
        for (idx, node) in per_node.iter().enumerate() {
            let Some(bytes) = node.get(key) else {
                problems.push(format!(
                    "`{key}` present on some replicas but MISSING on node {idx}"
                ));
                continue;
            };
            let bytes: &[u8] = bytes;
            match reference {
                None => reference = Some((idx, bytes)),
                Some((ref_idx, ref_bytes)) => {
                    if bytes != ref_bytes {
                        problems.push(describe_mismatch(key, ref_idx, ref_bytes, idx, bytes));
                    }
                }
            }
        }
    }

    assert!(
        problems.is_empty(),
        "cross-replica data divergence ({} issue(s)):\n{}",
        problems.len(),
        problems.join("\n")
    );
}

fn describe_mismatch(key: &str, a_idx: usize, a: &[u8], b_idx: usize, b: &[u8]) -> String {
    let window = |buf: &[u8], at: usize| {
        let start = at.saturating_sub(8);
        let end = (at + 8).min(buf.len());
        buf[start..end]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<Vec<_>>()
            .join(" ")
    };
    match a.iter().zip(b.iter()).position(|(x, y)| x != y) {
        Some(at) => format!(
            "`{key}`: bytes differ between node {a_idx} ({} B) and node {b_idx} ({} B) at offset {at}. \
             node{a_idx}=[{}] node{b_idx}=[{}]",
            a.len(),
            b.len(),
            window(a, at),
            window(b, at),
        ),
        None => format!(
            "`{key}`: length differs between node {a_idx} ({} B) and node {b_idx} ({} B)",
            a.len(),
            b.len(),
        ),
    }
}
