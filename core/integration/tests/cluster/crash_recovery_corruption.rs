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

//! Recovery from on-disk corruption, staged as offline byte surgery: a node is
//! stopped gracefully, one file under its data dir is mutated, and the node is
//! started again. The four scenarios split along the durability contract:
//!
//! - A torn tail of a partition segment `.log` or `.index` is the shape a
//!   crash legitimately leaves behind; recovery must absorb it without the
//!   replica silently diverging from its peers.
//! - Interior damage to the metadata WAL or a superblock slot can only be
//!   bit-rot or operator error, never a torn append, so boot must refuse
//!   loudly and the node heals by rejoining from a clean slate.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use iggy::prelude::*;
use integration::harness::{TestHarness, disk};
use integration::iggy_harness;
use tokio::time::sleep;

const STREAM_NAME: &str = "corruption-stream";
const TOPIC_NAME: &str = "corruption-topic";
const PARTITION_ID: u32 = 0;

/// Bounds an election, a rejoin, or a state-transfer heal on slow CI runners.
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(60);
/// Budget for eagerly flushed batches to land in a node's segment files.
const FLUSH_INSTALL_TIMEOUT: Duration = Duration::from_secs(20);
const POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Garbage appended to a segment `.log`: shorter than a batch command header,
/// so recovery must classify it as a torn tail, not a decodable batch.
const TORN_LOG_GARBAGE: usize = 40;
/// Garbage appended to a segment `.index`: deliberately NOT a multiple of the
/// 24-byte sparse entry stride, the torn shape a mid-entry crash leaves.
const TORN_INDEX_GARBAGE: usize = 10;
/// Filler byte for surgery. 0xA5 decodes as neither a valid batch command
/// header nor a plausible index position.
const GARBAGE_BYTE: u8 = 0xA5;
/// Metadata ops produced before the WAL surgery, so a flip at one quarter of
/// the file provably lands ahead of many complete committed entries.
const WAL_FODDER_STREAMS: usize = 20;

async fn create_stream_and_topic(client: &IggyClient) {
    client
        .create_stream(STREAM_NAME)
        .await
        .expect("create stream");
    // Eager flush persists and fsyncs every committed batch on every replica,
    // so the on-disk oracles below observe exactly what was acked.
    let options = TopicCreateOptions {
        partitions_count: Some(1),
        message_expiry: Some(IggyExpiry::NeverExpire),
        messages_required_to_save: Some(1),
        enforce_fsync: Some(true),
        ..TopicCreateOptions::default()
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
/// `(base_offset, payload)`.
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
/// confirmed offset, or `budget` runs out; `Err` carries the last shortfall.
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
                let missing = acked.iter().find(|(offset, payload)| {
                    !polled.messages.iter().any(|message| {
                        message.header.offset == *offset
                            && message.payload.as_ref() == payload.as_bytes()
                    })
                });
                match missing {
                    None => return Ok(()),
                    Some((offset, _)) => {
                        state = format!(
                            "{} messages polled, offset {offset} missing or mismatched",
                            polled.messages.len()
                        );
                    }
                }
            }
            Err(error) => state = format!("poll failed: {error}"),
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(state);
        }
        sleep(POLL_INTERVAL).await;
    }
}

/// Connect a root client to the first node in `nodes` that accepts one.
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

/// Number of nodes the metadata roster marks as leader.
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

/// Poll until one of `nodes` serves the test stream under exactly one leader.
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

/// Poll until `node`'s segment files hold all of `payloads`; `context` names
/// the phase for the deadline message.
async fn wait_until_node_holds_payloads(
    harness: &TestHarness,
    node: usize,
    payloads: &[String],
    budget: Duration,
    context: &str,
) {
    let data_path = harness.node(node).data_path();
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        match disk::installed_payloads_complete(&data_path, payloads) {
            Ok(()) => return,
            Err(error) => assert!(
                tokio::time::Instant::now() < deadline,
                "node {node} did not hold every payload within {budget:?} ({context}): {error}"
            ),
        }
        sleep(POLL_INTERVAL).await;
    }
}

/// Under `IGGY_TEST_VERBOSE` the server's output is inherited rather than
/// captured, so a boot-refusal error cannot carry the child's diagnostic
/// text; the `Err` itself stays the load-bearing assertion.
fn stderr_is_captured() -> bool {
    std::env::var("IGGY_TEST_VERBOSE").is_err()
}

/// The leader index and the first backup index.
async fn pick_backup(harness: &TestHarness) -> (usize, usize) {
    let leader = disk::leader_node_index(harness).await;
    let backup = (0..harness.cluster_size())
        .find(|index| *index != leader)
        .expect("a 3-node cluster has a backup");
    (leader, backup)
}

/// The ACTIVE (highest base offset) partition segment file with `extension`
/// under a node's data dir.
fn find_active_segment_file(data_path: &Path, extension: &str) -> PathBuf {
    let mut matches = Vec::new();
    let _ = disk::walk(data_path, &mut |path| {
        let named_like_segment = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .is_some_and(|stem| stem.len() == 20 && stem.bytes().all(|byte| byte.is_ascii_digit()));
        if named_like_segment && path.extension().is_some_and(|found| found == extension) {
            matches.push(path.to_path_buf());
        }
        false
    });
    matches.sort();
    matches.pop().unwrap_or_else(|| {
        panic!(
            "no segment .{extension} found under {}",
            data_path.display()
        )
    })
}

fn append_garbage(path: &Path, count: usize) {
    let mut bytes =
        fs::read(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    bytes.extend(std::iter::repeat_n(GARBAGE_BYTE, count));
    fs::write(path, bytes).unwrap_or_else(|error| panic!("write {}: {error}", path.display()));
}

/// Flip one interior byte at `numerator/denominator` of the file's length.
fn flip_interior_byte(path: &Path, numerator: u64, denominator: u64) {
    let mut bytes =
        fs::read(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    assert!(
        bytes.len() >= 1024,
        "{} holds only {} bytes; the surgery needs a file large enough that an \
         interior flip provably precedes complete entries",
        path.display(),
        bytes.len()
    );
    let at = (bytes.len() as u64 * numerator / denominator) as usize;
    bytes[at] ^= 0xFF;
    fs::write(path, bytes).unwrap_or_else(|error| panic!("write {}: {error}", path.display()));
}

/// Byte-compare the partition segment `.log` files across all nodes, panicking
/// with the violated `invariant` plus a per-file diff on divergence.
fn assert_segment_logs_identical(data_paths: &[PathBuf], invariant: &str) {
    let per_node: Vec<_> = data_paths
        .iter()
        .map(|root| disk::collect_comparable_files(root, false))
        .collect();
    assert!(
        per_node[0].keys().any(|key| key.ends_with(".log")),
        "node 0 holds no segment .log; the comparison would be vacuous"
    );
    let mut problems = Vec::new();
    for (key, reference) in &per_node[0] {
        for (node, files) in per_node.iter().enumerate().skip(1) {
            match files.get(key) {
                None => problems.push(format!("`{key}` missing on node {node}")),
                Some(bytes) if bytes != reference => {
                    problems.push(disk::describe_mismatch(key, 0, reference, node, bytes));
                }
                Some(_) => {}
            }
        }
    }
    assert!(problems.is_empty(), "{invariant}:\n{}", problems.join("\n"));
}

/// A torn tail of garbage on the active segment `.log` must be truncated for
/// good by recovery: the walked bounds govern both the on-disk length and the
/// reopened write cursor, so post-recovery appends land exactly where their
/// index entries point. The spec asserts the outcomes that used to break: the
/// node survives its NEXT restart (a stale cursor bricked it as message/index
/// divergence), every acked offset still reads back, and the at-rest `.log`
/// bytes stay identical across replicas (resurrected garbage diverged them).
// TODO(hubcio): both torn-tail specs tear a BACKUP; add a primary-side
// variant (tear the leader's segment, restart it) since the leader path
// exercises different reopen and catch-up code.
#[iggy_harness(cluster_nodes = 3)]
async fn given_a_torn_segment_tail_when_a_node_recovers_should_keep_size_counter_consistent(
    harness: &mut TestHarness,
) {
    let client = harness.tcp_root_client().await.unwrap();
    create_stream_and_topic(&client).await;
    let mut acked = produce_acked(&client, "pre-torn", 30).await;
    let pre_payloads: Vec<String> = acked.iter().map(|(_, payload)| payload.clone()).collect();
    for node in 0..harness.cluster_size() {
        wait_until_node_holds_payloads(
            harness,
            node,
            &pre_payloads,
            FLUSH_INSTALL_TIMEOUT,
            "eager flush before the surgery",
        )
        .await;
    }

    // The setup client is pinned to node 0, which is the backup whenever the
    // leader sits elsewhere; harness clients never reconnect, so swap to a
    // producer on the leader, which stays up across both backup restarts.
    drop(client);
    let (leader, backup) = pick_backup(harness).await;
    let client = harness
        .root_client_for_node(leader)
        .await
        .expect("connect a producer to the leader");
    harness.stop_node(backup).expect("stop the backup");
    let segment_log = find_active_segment_file(&harness.node(backup).data_path(), "log");
    append_garbage(&segment_log, TORN_LOG_GARBAGE);

    harness
        .restart_node(backup)
        .expect("recovery must absorb a torn segment tail and boot");

    acked.extend(produce_acked(&client, "post-torn", 30).await);
    let all_payloads: Vec<String> = acked.iter().map(|(_, payload)| payload.clone()).collect();
    for node in 0..harness.cluster_size() {
        wait_until_node_holds_payloads(
            harness,
            node,
            &all_payloads,
            CONVERGE_TIMEOUT,
            "replication after the torn-tail recovery",
        )
        .await;
    }

    harness.restart_node(backup).unwrap_or_else(|error| {
        panic!(
            "a node must restart cleanly over a segment it repaired and then \
             appended to; boot error: {error}"
        )
    });
    let nodes: Vec<usize> = (0..harness.cluster_size()).collect();
    let client = wait_until_cluster_serves(harness, &nodes, CONVERGE_TIMEOUT).await;
    wait_for_acked_readable(&client, &acked, CONVERGE_TIMEOUT)
        .await
        .unwrap_or_else(|state| {
            panic!("every acked offset must poll back after the torn-tail recovery: {state}")
        });

    let data_paths: Vec<PathBuf> = harness
        .all_servers()
        .iter()
        .map(|server| server.data_path())
        .collect();
    harness
        .stop()
        .await
        .expect("stop the cluster for the at-rest comparison");
    assert_segment_logs_identical(
        &data_paths,
        "segment .log files must stay byte-identical across replicas after a \
         torn-tail recovery",
    );
}

/// A torn tail on the segment `.index` (10 bytes, not a multiple of the
/// 24-byte entry stride) must not poison later entries: recovery floors the
/// index to whole entries and truncates the partial tail off the file, so
/// every subsequent entry keeps the stride-from-0 addressing readers assume.
/// The spec asserts the node survives its NEXT restart (an unaligned write
/// cursor used to make the following recovery decode a garbage-straddling
/// entry and refuse the partition) and every acked offset still reads back.
#[iggy_harness(cluster_nodes = 3)]
async fn given_a_torn_index_tail_when_a_node_recovers_should_not_misalign_subsequent_entries(
    harness: &mut TestHarness,
) {
    let client = harness.tcp_root_client().await.unwrap();
    create_stream_and_topic(&client).await;
    let mut acked = produce_acked(&client, "pre-torn-index", 30).await;
    let pre_payloads: Vec<String> = acked.iter().map(|(_, payload)| payload.clone()).collect();
    for node in 0..harness.cluster_size() {
        wait_until_node_holds_payloads(
            harness,
            node,
            &pre_payloads,
            FLUSH_INSTALL_TIMEOUT,
            "eager flush before the surgery",
        )
        .await;
    }

    // The setup client is pinned to node 0, which is the backup whenever the
    // leader sits elsewhere; harness clients never reconnect, so swap to a
    // producer on the leader, which stays up across both backup restarts.
    drop(client);
    let (leader, backup) = pick_backup(harness).await;
    let client = harness
        .root_client_for_node(leader)
        .await
        .expect("connect a producer to the leader");
    harness.stop_node(backup).expect("stop the backup");
    let segment_index = find_active_segment_file(&harness.node(backup).data_path(), "index");
    append_garbage(&segment_index, TORN_INDEX_GARBAGE);

    harness
        .restart_node(backup)
        .expect("a torn index tail is a whole-entry floor away from clean; boot must succeed");

    acked.extend(produce_acked(&client, "post-torn-index", 30).await);
    let all_payloads: Vec<String> = acked.iter().map(|(_, payload)| payload.clone()).collect();
    wait_until_node_holds_payloads(
        harness,
        backup,
        &all_payloads,
        CONVERGE_TIMEOUT,
        "replication after the torn-index recovery",
    )
    .await;

    harness.restart_node(backup).unwrap_or_else(|error| {
        panic!(
            "a node must restart cleanly over an index it repaired and then \
             appended to; boot error: {error}"
        )
    });

    let nodes: Vec<usize> = (0..harness.cluster_size()).collect();
    let client = wait_until_cluster_serves(harness, &nodes, CONVERGE_TIMEOUT).await;
    wait_for_acked_readable(&client, &acked, CONVERGE_TIMEOUT)
        .await
        .unwrap_or_else(|state| {
            panic!("every acked offset must poll back after the torn-index recovery: {state}")
        });
}

/// An interior flip in the metadata WAL is bit-rot, not a torn append: a
/// complete committed entry follows the damage, so truncating would discard
/// acked history. Boot must refuse with the interior-corruption diagnostic,
/// the surviving quorum must keep serving, and the node heals by rejoining
/// from a clean slate (state transfer).
#[iggy_harness(cluster_nodes = 3)]
async fn given_interior_wal_corruption_when_a_node_boots_should_refuse_and_heal_via_clean_slate(
    harness: &mut TestHarness,
) {
    let client = harness.tcp_root_client().await.unwrap();
    create_stream_and_topic(&client).await;
    let mut acked = produce_acked(&client, "pre-fault", 20).await;
    // Extra committed metadata ops so the flip at one quarter of the WAL
    // provably precedes many complete entries.
    for index in 0..WAL_FODDER_STREAMS {
        client
            .create_stream(&format!("wal-fodder-{index:02}"))
            .await
            .expect("create fodder stream");
    }
    let pre_payloads: Vec<String> = acked.iter().map(|(_, payload)| payload.clone()).collect();
    for node in 0..harness.cluster_size() {
        wait_until_node_holds_payloads(
            harness,
            node,
            &pre_payloads,
            FLUSH_INSTALL_TIMEOUT,
            "eager flush before the surgery",
        )
        .await;
    }
    drop(client);

    let (_, backup) = pick_backup(harness).await;
    harness.stop_node(backup).expect("stop the backup");
    let wal_path = harness
        .node(backup)
        .data_path()
        .join("metadata/journal.wal");
    flip_interior_byte(&wal_path, 1, 4);

    let error = harness
        .restart_node(backup)
        .expect_err("boot over an interior-corrupt WAL must refuse, not truncate");
    if stderr_is_captured() {
        let diagnostics = error.to_string();
        assert!(
            diagnostics.contains("refusing to truncate"),
            "the boot refusal must name the interior WAL corruption (\"refusing to \
             truncate\"), got: {diagnostics}"
        );
    }

    let survivors: Vec<usize> = (0..harness.cluster_size())
        .filter(|index| *index != backup)
        .collect();
    let survivor_client = wait_until_cluster_serves(harness, &survivors, CONVERGE_TIMEOUT).await;
    acked.extend(produce_acked(&survivor_client, "post-fault", 10).await);
    wait_for_acked_readable(&survivor_client, &acked, CONVERGE_TIMEOUT)
        .await
        .unwrap_or_else(|state| {
            panic!("the surviving quorum must keep serving all acked offsets: {state}")
        });

    harness
        .restart_node_from_clean_slate(backup)
        .expect("a clean-slate rejoin heals the corrupt-WAL node");
    let all_payloads: Vec<String> = acked.iter().map(|(_, payload)| payload.clone()).collect();
    wait_until_node_holds_payloads(
        harness,
        backup,
        &all_payloads,
        CONVERGE_TIMEOUT,
        "state-transfer heal after the clean-slate rejoin",
    )
    .await;
}

/// A superblock slot whose bytes no longer verify, beside a valid sibling, is
/// bit-rot whose lost `sequence` may have been the newer generation: falling
/// back could walk consensus state backwards, so boot must refuse. The
/// surviving quorum keeps serving and a clean-slate rejoin heals the node.
#[iggy_harness(cluster_nodes = 3)]
async fn given_a_corrupt_superblock_slot_when_a_node_boots_should_refuse_boot(
    harness: &mut TestHarness,
) {
    let client = harness.tcp_root_client().await.unwrap();
    create_stream_and_topic(&client).await;
    let mut acked = produce_acked(&client, "pre-fault", 20).await;
    let pre_payloads: Vec<String> = acked.iter().map(|(_, payload)| payload.clone()).collect();
    for node in 0..harness.cluster_size() {
        wait_until_node_holds_payloads(
            harness,
            node,
            &pre_payloads,
            FLUSH_INSTALL_TIMEOUT,
            "eager flush before the surgery",
        )
        .await;
    }
    drop(client);

    // A quiet cluster at view 0 has an EMPTY metadata superblock: the record
    // is first persisted when a replica adopts an advanced view. Force a view
    // change by stopping the view-0 metadata primary (replica 0), wait for a
    // survivor to persist `view >= 1`, then bring node 0 back so a full
    // quorum survives the later surgery victim.
    harness
        .stop_node(0)
        .expect("stop the view-0 metadata primary");
    let view_change_deadline = tokio::time::Instant::now() + CONVERGE_TIMEOUT;
    while (1..harness.cluster_size())
        .all(|node| disk::read_metadata_superblock_state(&harness.node(node).data_path()).is_none())
    {
        assert!(
            tokio::time::Instant::now() < view_change_deadline,
            "no survivor persisted a metadata superblock record after the primary stop"
        );
        sleep(POLL_INTERVAL).await;
    }
    harness
        .restart_node(0)
        .expect("rejoin the old primary so the cluster is whole before the surgery");
    let nodes: Vec<usize> = (0..harness.cluster_size()).collect();
    drop(wait_until_cluster_serves(harness, &nodes, CONVERGE_TIMEOUT).await);

    // The surgery victim: a node holding a durable record that is not the
    // current leader, so stopping it leaves a serving quorum.
    let leader = disk::leader_node_index(harness).await;
    let backup = (1..harness.cluster_size())
        .find(|node| {
            *node != leader
                && disk::read_metadata_superblock_state(&harness.node(*node).data_path()).is_some()
        })
        .expect("a non-leader survivor holds a durable superblock record");
    let backup_data = harness.node(backup).data_path();

    harness.stop_node(backup).expect("stop the backup");
    let slot_path = ["superblock.a", "superblock.b"]
        .iter()
        .map(|name| backup_data.join("metadata").join(name))
        .find(|path| fs::metadata(path).is_ok_and(|meta| meta.len() > 0))
        .expect("at least one non-empty superblock slot exists");
    // Flip one byte halfway into the record: past the header, inside the
    // checksummed payload, so the slot classifies as corrupt (not absent).
    let mut bytes = fs::read(&slot_path)
        .unwrap_or_else(|error| panic!("read {}: {error}", slot_path.display()));
    let at = bytes.len() / 2;
    bytes[at] ^= 0xFF;
    fs::write(&slot_path, bytes)
        .unwrap_or_else(|error| panic!("write {}: {error}", slot_path.display()));

    let error = harness
        .restart_node(backup)
        .expect_err("boot over a corrupt superblock slot must refuse even beside a valid sibling");
    if stderr_is_captured() {
        // The refusal reaches stderr as the Debug form of the recovery error,
        // so the variant name is the decisive marker.
        let diagnostics = error.to_string();
        assert!(
            diagnostics.contains("SuperblockUnreadable"),
            "the boot refusal must name the unreadable superblock, got: {diagnostics}"
        );
    }

    let survivors: Vec<usize> = (0..harness.cluster_size())
        .filter(|index| *index != backup)
        .collect();
    let survivor_client = wait_until_cluster_serves(harness, &survivors, CONVERGE_TIMEOUT).await;
    acked.extend(produce_acked(&survivor_client, "post-fault", 10).await);
    wait_for_acked_readable(&survivor_client, &acked, CONVERGE_TIMEOUT)
        .await
        .unwrap_or_else(|state| {
            panic!("the surviving quorum must keep serving all acked offsets: {state}")
        });

    harness
        .restart_node_from_clean_slate(backup)
        .expect("a clean-slate rejoin heals the corrupt-superblock node");
    let all_payloads: Vec<String> = acked.iter().map(|(_, payload)| payload.clone()).collect();
    wait_until_node_holds_payloads(
        harness,
        backup,
        &all_payloads,
        CONVERGE_TIMEOUT,
        "state-transfer heal after the clean-slate rejoin",
    )
    .await;
}
