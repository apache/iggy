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

//! On-disk oracles over a node's data directory: segment walkers, cross-replica
//! byte comparison, consumer-offset and superblock readers, and cluster-role
//! lookups. Several older test files still hold local copies of these helpers;
//! consolidating them onto this module is a follow-up.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use consensus::VsrState;
use iggy::prelude::{ClusterClient, ClusterNodeRole};
use journal::superblock::{SLOT_FILE_NAMES, SuperblockContents, decode_slots};
use tokio::time::sleep;

use super::TestHarness;

// Poll the per-node segment `.log` sizes until they agree and hold steady,
// instead of a fixed sleep: a fixed wait either flakes under CI load or hides a
// real replication lag.
const CONVERGENCE_POLL_INTERVAL: Duration = Duration::from_millis(200);
const CONVERGENCE_DEADLINE: Duration = Duration::from_secs(20);
const CONVERGENCE_STABLE_POLLS: u32 = 3;

/// A partition segment `.log`, named for its 20-digit zero-padded base offset
/// (see `partitions::state_transfer`'s path builders).
///
/// Matches the segment file NAME shape, not the `.log` extension alone and not
/// a `streams/` path prefix: the server's own text log sits under the same data
/// root, so an extension-only match would count tracing output as segment data.
pub fn is_segment_log(path: &Path) -> bool {
    path.extension().is_some_and(|extension| extension == "log")
        && path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .is_some_and(|stem| stem.len() == 20 && stem.bytes().all(|byte| byte.is_ascii_digit()))
}

/// Depth-first walk of `root`, skipping the `metadata` plane, returning the
/// first path for which `matches` is true. Callers wanting a full sweep return
/// `false` from `matches` and accumulate via its side effects.
pub fn walk(root: &Path, matches: &mut dyn FnMut(&Path) -> bool) -> Option<PathBuf> {
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        if dir.file_name().is_some_and(|name| name == "metadata") {
            continue;
        }
        let Ok(entries) = fs::read_dir(&dir) else {
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

/// `Ok(())` when every payload in `expected` appears in node-local segment
/// bytes at a non-decreasing position, otherwise the first discrepancy.
pub fn installed_payloads_complete(data_path: &Path, expected: &[String]) -> Result<(), String> {
    let mut chain = Vec::new();
    let mut paths = Vec::new();
    let _ = walk(data_path, &mut |path| {
        if is_segment_log(path) {
            paths.push(path.to_path_buf());
        }
        false
    });
    // Segment files are named for their zero-padded base offset, so lexical
    // order is offset order.
    paths.sort();
    for path in paths {
        let Ok(bytes) = fs::read(&path) else {
            return Err(format!("{} could not be read", path.display()));
        };
        chain.extend_from_slice(&bytes);
    }
    let mut searched_from = 0;
    for payload in expected {
        let found = chain[searched_from..]
            .windows(payload.len())
            .enumerate()
            .filter(|(_, window)| *window == payload.as_bytes())
            .map(|(offset, _)| searched_from + offset)
            // A bare find would match `message-1` inside `message-10`; skip
            // digit-extended matches and take the next occurrence instead of
            // rejecting the payload outright (the follow byte of a genuine
            // match can itself be a digit, e.g. inside a following header).
            .find(|start| {
                chain
                    .get(start + payload.len())
                    .is_none_or(|byte| !byte.is_ascii_digit())
            });
        let Some(start) = found else {
            return Err(format!(
                "{payload:?} is absent from the {} installed bytes after position {searched_from}",
                chain.len()
            ));
        };
        searched_from = start;
    }
    Ok(())
}

/// A file (relative to a node's data dir) whose bytes must match across
/// replicas: the partition segment `.log`, plus the replicated metadata WAL
/// when `include_wal` is set. Per-node files (logs, runtime, config, stdout)
/// are excluded by construction.
///
/// Callers default `include_wal` to false: WAL byte content legitimately
/// diverges across replicas once a node crosses its checkpoint margin, so only
/// runs kept well below that margin may compare it. Two metadata-plane files
/// are always excluded as local, per-replica artifacts:
///
/// - `metadata/snapshot.bin`: a local compaction artifact stamped with
///   `created_at = now()` and a per-replica `sequence_number`, plus unsorted
///   hashmap iteration order; it can never match across replicas.
/// - `state/`: not populated by the VSR plane; excluded for the same
///   local-artifact reason so it cannot start flaking if that changes.
///
/// The segment `.index` is excluded for the same class of reason: a local
/// sparse index (one entry per persist flush), not replicated and not part of
/// the VSR hash chain; recovery rebuilds it from the `.log`. Its length tracks
/// commit cadence, which differs between primary (one flush per op) and backup
/// (one flush per committed heartbeat range).
fn is_comparable(rel: &str, include_wal: bool) -> bool {
    let is_segment = rel.starts_with("streams/") && rel.ends_with(".log");
    let is_metadata_wal = rel == "metadata/journal.wal";
    is_segment || (include_wal && is_metadata_wal)
}

/// Every comparable file under `root`, keyed by its `/`-separated relative path.
pub fn collect_comparable_files(root: &Path, include_wal: bool) -> BTreeMap<String, Vec<u8>> {
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
                if is_comparable(&rel, include_wal) {
                    let bytes = fs::read(&path)
                        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
                    files.insert(rel, bytes);
                }
            }
        }
    }
    files
}

/// Byte-compare every comparable file across the given node data dirs,
/// panicking with a per-file diff on any divergence.
///
/// Guards against a vacuous pass: node 0 must hold at least one produced
/// segment, otherwise nothing was persisted and the comparison proves nothing.
pub fn assert_replica_data_identical(data_paths: &[PathBuf], include_wal: bool) {
    let per_node: Vec<BTreeMap<String, Vec<u8>>> = data_paths
        .iter()
        .map(|root| collect_comparable_files(root, include_wal))
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

/// Human-readable first-difference report for one relative path across two nodes.
pub fn describe_mismatch(key: &str, a_idx: usize, a: &[u8], b_idx: usize, b: &[u8]) -> String {
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

/// Poll each node's total segment `.log` bytes until all nodes agree and the
/// figure holds steady for a few consecutive polls, or the deadline (20s)
/// elapses. On timeout, return anyway: the byte-for-byte compare that follows
/// then fails with a precise diff instead of this masking a real lag.
pub async fn wait_for_log_convergence(data_paths: &[PathBuf]) {
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

/// The u64 offset persisted under any `offsets/consumers/<id>` file in a node's
/// data dir, or `None` when no such file has been written yet. Walks the tree
/// so it is robust to the stream/topic/partition id layout. Reads the leading
/// u64 of the record: the file is offset + trailing checksum (see
/// `partitions::offset_storage::encode_offset_record`), and a shorter read
/// (persist truncates before writing) is treated as not-yet-written.
pub fn read_replicated_consumer_offset(data_dir: &Path) -> Option<u64> {
    let mut stack: Vec<PathBuf> = vec![data_dir.to_path_buf()];
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
                continue;
            }
            let is_consumer_offset = path
                .parent()
                .and_then(Path::file_name)
                .is_some_and(|name| name == "consumers")
                && path
                    .parent()
                    .and_then(Path::parent)
                    .and_then(Path::file_name)
                    .is_some_and(|name| name == "offsets");
            if is_consumer_offset
                && let Ok(bytes) = fs::read(&path)
                && let Some(offset_bytes) = bytes.first_chunk::<8>()
            {
                return Some(u64::from_le_bytes(*offset_bytes));
            }
        }
    }
    None
}

/// Decode a node's durable metadata `VsrState` from its on-disk superblock,
/// `None` if no record exists yet. Reads the two slot files with blocking I/O
/// (callers are tokio tests off any compio runtime) and decodes them through
/// the journal's own newest-verifying-wins selection, so the caller sees
/// exactly what `PingPongSuperblock::read_latest` would.
///
/// # Panics
/// If a slot holds bytes that do not verify. For callers that do not corrupt
/// slots that is a real durability bug; returning `None` would let pollers
/// read it as "not written yet" and time out on a misleading message.
pub fn read_metadata_superblock_state(data_path: &Path) -> Option<VsrState> {
    // `<data_dir>/metadata/` is where shard 0 opens its `PingPongSuperblock`.
    let dir = data_path.join("metadata");
    read_superblock_state_in(&dir, "metadata")
}

/// The single partition directory's superblock record on a node, `None` while
/// no record exists yet (a partition group that never left view 0 has an empty
/// superblock, and its slot files may not exist at all). Intended for layouts
/// with exactly one partition group.
///
/// # Panics
/// Same contract as [`read_metadata_superblock_state`].
pub fn read_partition_superblock_state(data_path: &Path) -> Option<VsrState> {
    let dir = find_partition_superblock_dir(data_path)?;
    read_superblock_state_in(&dir, "partition")
}

fn read_superblock_state_in(dir: &Path, plane: &str) -> Option<VsrState> {
    let slot_a = fs::read(dir.join(SLOT_FILE_NAMES[0])).ok();
    let slot_b = fs::read(dir.join(SLOT_FILE_NAMES[1])).ok();
    match decode_slots(slot_a.as_deref(), slot_b.as_deref()) {
        SuperblockContents::Present(payload) => VsrState::try_from(payload.as_slice()).ok(),
        SuperblockContents::Empty => None,
        SuperblockContents::Unreadable { version } => panic!(
            "{plane} superblock at {} is unreadable (version {version:?})",
            dir.display()
        ),
    }
}

/// First directory under `root` (metadata plane excluded) holding a superblock
/// slot file; walked so callers do not hard-code the
/// `streams/<s>/topics/<t>/partitions/<p>` layout.
pub fn find_partition_superblock_dir(root: &Path) -> Option<PathBuf> {
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        // The metadata plane keeps its own superblock under `<data>/metadata`;
        // only partition records are of interest here.
        if dir.file_name().is_some_and(|name| name == "metadata") {
            continue;
        }
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if path
                .file_name()
                .is_some_and(|name| name == SLOT_FILE_NAMES[0])
            {
                return Some(dir);
            }
        }
    }
    None
}

/// Index of the node the metadata roster marks as leader, resolved by matching
/// the roster's TCP port against each node's bound address.
///
/// Reads through node 0. Use [`leader_node_index_via`] where node 0 may be
/// down or where the roster must be read from a named node.
///
/// # Panics
/// If no root client connects, the roster query fails, no node is marked
/// leader, or the leader's port matches no harness node.
pub async fn leader_node_index(harness: &TestHarness) -> usize {
    leader_node_index_via(harness, 0).await
}

/// [`leader_node_index`] reading the roster through node `via`.
///
/// Named separately because which node answers matters once node 0 may be
/// down: the roster read is auth-gated, so it needs a node that can complete a
/// login, and a client built against a dead node cannot.
///
/// # Panics
/// As [`leader_node_index`].
pub async fn leader_node_index_via(harness: &TestHarness, via: usize) -> usize {
    let client = harness
        .root_client_for_node(via)
        .await
        .expect("a root client (redirecting to the leader if the dialed node is not it)");
    let metadata = client
        .get_cluster_metadata()
        .await
        .expect("get cluster metadata");
    let leader_port = metadata
        .nodes
        .iter()
        .find(|node| node.role == ClusterNodeRole::Leader)
        .unwrap_or_else(|| panic!("the cluster must have elected a leader, got {metadata}"))
        .endpoints
        .tcp;
    (0..harness.cluster_size())
        .find(|index| {
            harness
                .node(*index)
                .tcp_addr()
                .is_some_and(|address| address.port() == leader_port)
        })
        .expect("the leader must be one of the roster nodes")
}
