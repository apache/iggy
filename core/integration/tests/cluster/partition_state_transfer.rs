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

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, Instant};

use iggy::prelude::*;
use integration::harness::TestHarness;
use integration::iggy_harness;
use tokio::time::sleep;

const STREAM_NAME: &str = "partition-transfer-stream";
const TOPIC_NAME: &str = "partition-transfer-topic";
/// Partition ids are 0-based (CreateTopic assigns them from 0).
const PARTITION_ID: u32 = 0;
/// Enough batches to push the evicted ring (capacity 64) well past the
/// window a rejoiner could repair from op 1.
const MESSAGES_COUNT: u32 = 200;
const STORED_CONSUMER_OFFSET: u64 = 17;

/// Dead-peer spec sizing: enough 256 KiB payloads (64 MiB total) that the
/// transfer spans hundreds of 256 KiB chunk round-trips, keeping the pull
/// in flight long enough for a marker-gated kill to land mid-transfer (a
/// 16 MiB pull completed in ~120ms on a release build, inside one marker
/// poll). Also enough commits (> ring capacity 64) that the fresh
/// rejoiner's floor refuses.
const BULKY_MESSAGES_COUNT: u32 = 256;
const BULKY_PAYLOAD_LEN: usize = 256 * 1024;
/// Poll for the kill gate: the serving marker appears at descriptor-serve
/// time and the kill must land within the pull, so the ordinary 200ms
/// cadence is too coarse here.
const KILL_GATE_POLL: Duration = Duration::from_millis(10);

const CONVERSION_MARKER: &str = "partition repair floor unreachable; converting to state transfer";
const INSTALL_MARKER: &str = "partition state transfer installed";
/// Logged once per transfer, at the last byte of the last artifact. The
/// descriptor-time "serving" line only proves a request ARRIVED; this proves the
/// pull ran.
const FULLY_SERVED_MARKER: &str = "partition state transfer fully served";
const ABANDON_MARKER: &str =
    "partition state transfer stalled past its retry budget; abandoning with a backed-off re-arm";
/// The second route off a dead transfer peer. A replica parked in
/// `AwaitingTarget` re-arms the moment it adopts a StartView, and with the
/// stock config that beats the stall budget every time: the budget needs six
/// rounds of `repair_retry_interval` (~6s) while `heartbeat_timeout` elects
/// the new primary in 5s. Both routes prove the same thing - the replica did
/// not retry into the corpse - so a spec that pins one is asserting on
/// scheduler luck.
const REARM_ON_VIEW_MARKER: &str =
    "adopted a live view while awaiting transfer; requesting partition state transfer";

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
    let client = harness
        .root_client_for_node(0)
        .await
        .expect("connect a root client to the node");
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
    let client = harness
        .root_client_for_node(0)
        .await
        .expect("connect a root client to the node");
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
    // Retried like every other marker check here: stdout goes through the
    // buffered non-blocking appender, so a survivor's line can trail node 2's
    // install by more than one poll.
    await_marker_any(harness, &[0, 1], FULLY_SERVED_MARKER).await;

    // The markers only say the machinery ran. Read node 2's OWN segment bytes
    // and check every produced payload is there, in ascending file order: a
    // truncated, short or reordered install fails here and passes above.
    //
    // Read off disk rather than polled: the SDK is leader-aware and redirects
    // a poll to the primary, so no client-side read can be pinned to the
    // rejoined node.
    // Two produce runs, each numbering its payloads from 0, so the expected
    // chain is 0..40 followed by 0..MESSAGES_COUNT.
    let expected: Vec<String> = (0..40)
        .chain(0..MESSAGES_COUNT)
        .map(|sequence| format!("message-{sequence}"))
        .collect();
    await_installed_payloads(harness, 2, &expected).await;
}

#[iggy_harness(
    cluster_nodes = 3,
    server(
        system.sharding.cpu_allocation = "0..1",
        partition.evicted_ring_capacity = "64",
        system.partition.messages_required_to_save = "1",
        system.segment.size = "8MiB"
    )
)]
// The 8 MiB segment cap makes the 64 MiB bulky seed span several sealed
// segments, so unlike the other specs (single-segment under the default 1 GiB
// cap) this one installs from a MULTI-ARTIFACT manifest, one spill per artifact.
// The installed segment count at the end is what asserts that. The re-armed
// pull also runs the staged-segment reuse scan, but how much it can adopt
// depends on how many artifacts completed before the kill, so nothing here
// asserts on reuse.
async fn given_transfer_peer_dies_when_stalled_should_leave_dead_peer_and_recover_partition(
    harness: &mut TestHarness,
) {
    let client = harness
        .root_client_for_node(0)
        .await
        .expect("connect a root client to the node");
    seed_topic(&client).await;
    // Bulky payloads so the pull spans many 256 KiB chunks: the kill below
    // must land while the transfer is provably in flight, and a small
    // partition finishes inside the marker-poll latency, leaving the
    // dead-peer path untested.
    produce_bulky(&client, BULKY_MESSAGES_COUNT, BULKY_PAYLOAD_LEN).await;
    sleep(Duration::from_secs(1)).await;
    let _seed_client = client;

    // Wipe node 2, wait until its rejoin CONVERTED to a transfer, then kill
    // the serving peer (the view-0 primary, node 0) mid-pull. Node 2 must not
    // retry into the corpse forever: it re-arms against the next peer -- via
    // the stall budget, or via the StartView it adopts once the survivors
    // elect past node 0 -- and the transfer re-runs against the new primary
    // (node 1).
    harness
        .restart_node_from_clean_slate(2)
        .expect("clean-slate restart of node 2");
    // Kill node 0 the moment node 2 CONVERTED: the transfer is then armed
    // at node 0 but the 64 MiB pull cannot possibly finish inside the kill
    // latency, so node 2 deterministically ends up parked against a dead
    // peer, whether the descriptor made it out or not. (Gating on the serving
    // marker instead raced the pull itself: a release-build pull finishes in
    // a few hundred ms.)
    let deadline = Instant::now() + TRANSFER_BUDGET;
    while !harness.node(2).stdout_contains(CONVERSION_MARKER) {
        assert!(
            Instant::now() < deadline,
            "node 2 never converted its refused repair floor to a transfer"
        );
        sleep(KILL_GATE_POLL).await;
    }
    // Baselines BEFORE the kill. Every marker check below counts occurrences
    // against these instead of scanning the whole accumulated log: node 2 can
    // have installed and node 1 can have fully served an earlier attempt while
    // node 0 was still up, and a `contains` would call those the recovery.
    let installs_before_kill = harness.node(2).stdout_occurrences(INSTALL_MARKER);
    let served_before_kill = harness.node(1).stdout_occurrences(FULLY_SERVED_MARKER);
    let abandons_before_kill = harness.node(2).stdout_occurrences(ABANDON_MARKER);
    let rearms_before_kill = harness.node(2).stdout_occurrences(REARM_ON_VIEW_MARKER);
    harness
        .stop_node(0)
        .expect("stop the serving peer (node 0)");

    // The pull was in flight against a peer that is gone, so node 2 must leave
    // the dead session by one of the two routes off it, whichever fires first.
    await_new_marker_any(
        harness,
        2,
        &[
            (ABANDON_MARKER, abandons_before_kill),
            (REARM_ON_VIEW_MARKER, rearms_before_kill),
        ],
    )
    .await;

    // Recovery: the scheduled re-arm targets the surviving primary. No
    // follow-up commit is asserted -- the cluster is quorum-marginal with
    // one node down, and an unanswered read mid-election is not a verdict.
    //
    // Node 1 is named explicitly, not "any survivor": with node 0 dead it is
    // the only replica left that can serve, so a marker from it is proof the
    // re-arm found a new peer rather than proof of the pre-kill attempt.
    await_new_marker(harness, 2, INSTALL_MARKER, installs_before_kill).await;
    await_new_marker(harness, 1, FULLY_SERVED_MARKER, served_before_kill).await;

    // The manifest the install consumed carried one artifact per sealed
    // segment, and each was spilled and renamed separately: the seeded 64 MiB
    // over an 8 MiB segment cap cannot land as one file.
    let data_path = harness.node(2).data_path();
    let deadline = Instant::now() + TRANSFER_BUDGET;
    loop {
        let installed = segment_log_count(&data_path);
        if installed > 1 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "node 2 installed {installed} segment file(s); a 64 MiB transfer under an \
             8 MiB segment cap must arrive as a multi-artifact manifest"
        );
        sleep(MARKER_POLL).await;
    }
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

/// Like [`produce`], one commit per send, but with a payload of
/// `payload_len` filler bytes so the on-disk segment grows fast.
async fn produce_bulky(client: &IggyClient, count: u32, payload_len: usize) {
    for sequence in 0..count {
        let payload = format!("bulky-{sequence}-{}", "x".repeat(payload_len));
        let mut messages = vec![IggyMessage::from_str(&payload).expect("message")];
        client
            .send_messages(
                &Identifier::named(STREAM_NAME).expect("stream identifier"),
                &Identifier::named(TOPIC_NAME).expect("topic identifier"),
                &Partitioning::partition_id(PARTITION_ID),
                &mut messages,
            )
            .await
            .expect("send bulky message");
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

/// Polls node `node`'s installed segment files until every payload in
/// `expected` is present, in the order given.
///
/// The payloads are the oracle the markers are not: a short install is missing
/// the tail, a torn one is missing a middle, and a reordered one fails the
/// ascending-position check.
async fn await_installed_payloads(harness: &TestHarness, node: usize, expected: &[String]) {
    let data_path = harness.node(node).data_path();
    let deadline = Instant::now() + TRANSFER_BUDGET;
    loop {
        if let Err(missing) = installed_payloads_complete(&data_path, expected) {
            assert!(
                Instant::now() < deadline,
                "node {node}'s installed segments never held the whole produced batch: {missing}"
            );
            sleep(MARKER_POLL).await;
            continue;
        }
        return;
    }
}

/// `Ok(())` when every payload in `expected` appears in node-local segment
/// bytes at a non-decreasing position, otherwise the first discrepancy.
fn installed_payloads_complete(data_path: &Path, expected: &[String]) -> Result<(), String> {
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
        let Ok(bytes) = std::fs::read(&path) else {
            return Err(format!("{} could not be read", path.display()));
        };
        chain.extend_from_slice(&bytes);
    }
    let mut searched_from = 0;
    for payload in expected {
        let found = chain[searched_from..]
            .windows(payload.len())
            .position(|window| window == payload.as_bytes())
            // A bare find would match `message-1` inside `message-10`.
            .map(|offset| searched_from + offset)
            .filter(|start| {
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

/// [`await_marker`], but satisfied only by an occurrence beyond `baseline` -
/// the whole-log scan cannot distinguish a line from before the fault.
async fn await_new_marker(harness: &TestHarness, node: usize, marker: &str, baseline: usize) {
    let deadline = Instant::now() + TRANSFER_BUDGET;
    while harness.node(node).stdout_occurrences(marker) <= baseline {
        assert!(
            Instant::now() < deadline,
            "node {node} never logged {marker:?} again after the fault \
             (still at the pre-fault count of {baseline}) within {TRANSFER_BUDGET:?}"
        );
        sleep(MARKER_POLL).await;
    }
}

/// [`await_new_marker`] over alternative markers on one node, each carried
/// with its own pre-fault baseline: satisfied by the first to advance.
async fn await_new_marker_any(harness: &TestHarness, node: usize, markers: &[(&str, usize)]) {
    let deadline = Instant::now() + TRANSFER_BUDGET;
    loop {
        if markers
            .iter()
            .any(|(marker, baseline)| harness.node(node).stdout_occurrences(marker) > *baseline)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "node {node} logged none of {markers:?} again after the fault within {TRANSFER_BUDGET:?}"
        );
        sleep(MARKER_POLL).await;
    }
}

/// [`await_marker`] over a set of nodes: satisfied by the first one to log it.
async fn await_marker_any(harness: &TestHarness, nodes: &[usize], marker: &str) {
    let deadline = Instant::now() + TRANSFER_BUDGET;
    while !nodes
        .iter()
        .any(|&node| harness.node(node).stdout_contains(marker))
    {
        assert!(
            Instant::now() < deadline,
            "none of {nodes:?} logged {marker:?} within {TRANSFER_BUDGET:?}"
        );
        sleep(MARKER_POLL).await;
    }
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

/// Total transferred segment payload under `root`. Walked rather than
/// path-derived so the test does not hard-code the
/// `streams/<s>/topics/<t>/partitions/<p>` layout.
///
/// Matches the segment file NAME shape, not the `.log` extension alone: the
/// server's own text log sits under the same data root (~15 KiB on a local run,
/// about a third of the floor this feeds), and the child inherits `RUST_LOG`, so
/// an extension-only sum let a debug run satisfy the caller with zero
/// transferred segment bytes.
fn total_partition_log_bytes(root: &Path) -> u64 {
    let mut total = 0;
    let _ = walk(root, &mut |path| {
        if is_segment_log(path)
            && let Ok(metadata) = std::fs::metadata(path)
        {
            total += metadata.len();
        }
        false
    });
    total
}

/// Number of segment files under `root`; more than one proves the install came
/// from a multi-artifact manifest, each artifact spilled and renamed in turn.
fn segment_log_count(root: &Path) -> usize {
    let mut count = 0;
    let _ = walk(root, &mut |path| {
        if is_segment_log(path) {
            count += 1;
        }
        false
    });
    count
}

/// A segment `.log`, named for its 20-digit zero-padded base offset (see
/// `partitions::state_transfer`'s path builders).
fn is_segment_log(path: &Path) -> bool {
    path.extension().is_some_and(|extension| extension == "log")
        && path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .is_some_and(|stem| stem.len() == 20 && stem.bytes().all(|byte| byte.is_ascii_digit()))
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
