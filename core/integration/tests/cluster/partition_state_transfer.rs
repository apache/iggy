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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use iggy::prelude::*;
use integration::harness::{TestHarness, TestServerConfig, resolve_config_paths};
use integration::iggy_harness;
use serial_test::parallel;
use tokio::task::JoinHandle;
use tokio::time::sleep;

const STREAM_NAME: &str = "partition-transfer-stream";
const TOPIC_NAME: &str = "partition-transfer-topic";
/// Partition ids are 0-based (CreateTopic assigns them from 0).
const PARTITION_ID: u32 = 0;
/// Enough batches to push the evicted ring (capacity 64) well past the
/// window a rejoiner could repair from op 1.
const MESSAGES_COUNT: u32 = 200;
const STORED_CONSUMER_OFFSET: u64 = 17;
/// A Kafka-style group name, which the gateway maps onto an Iggy consumer group.
const EXTERNAL_GROUP: &str = "kafka.cg.transfer";
/// Past the partition end: an external group offset must cross the transfer unclamped.
const STORED_EXTERNAL_OFFSET: u64 = 1 << 40;

/// Dead-peer spec sizing: enough 256 KiB payloads (64 MiB total) that the
/// transfer spans hundreds of 256 KiB chunk round-trips, keeping the pull
/// in flight long enough for a marker-gated kill to land mid-transfer (a
/// 16 MiB pull completed in ~120ms on a release build, inside one marker
/// poll). Also enough commits (> ring capacity 64) that the fresh
/// rejoiner's floor refuses.
/// Segment cap for the multi-artifact spec: small enough that the 64 MiB bulky
/// seed spans several sealed segments, so the install runs from a manifest with
/// one spill per artifact rather than a single segment.
const SEGMENT_SIZE_MULTI_ARTIFACT: u64 = 8 * 1024 * 1024;
const BULKY_MESSAGES_COUNT: u32 = 256;
const BULKY_PAYLOAD_LEN: usize = 256 * 1024;
/// Poll for the kill gate: the serving marker appears at descriptor-serve
/// time and the kill must land within the pull, so the ordinary 200ms
/// cadence is too coarse here.
const KILL_GATE_POLL: Duration = Duration::from_millis(10);

const CONVERSION_MARKER: &str = "partition repair floor unreachable; converting to state transfer";
const INSTALL_MARKER: &str = "partition state transfer installed";
const INSTALL_FAILED_MARKER: &str = "partition state transfer install failed";
/// The `OfferErasesHeldOps` reason, logged inside an [`INSTALL_FAILED_MARKER`]
/// line.
const HELD_OPS_INSTALL_MARKER: &str = "would erase held ops";
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

/// Sustained-write spec: the node cut off and then healed.
const LAGGING_NODE: usize = 2;
/// Sends committed while node 2 is cut off: more than the evicted ring (capacity
/// 64) holds, so its repair after the heal cannot reach its own commit point.
const ISOLATED_SENDS: u32 = 100;
/// Pace of the writer that keeps running after the heal. One message a second
/// already moves the commit point past every offer before its pull lands.
const WRITE_INTERVAL: Duration = Duration::from_secs(1);
/// How long node 2 must go without another conversion once it installed.
const CAUGHT_UP_WINDOW: Duration = Duration::from_secs(10);
/// Logged by a receiver that links segments it already holds into a
/// transfer. Its `reused_len` field counts the bytes that it did not pull.
const REUSE_MARKER: &str = "reused segments that this node already holds";
/// Repeated-install specs must exhaust snapshot-tail retention as well as the
/// ring; otherwise the next gap can correctly heal without another snapshot.
const SNAPSHOT_ONLY_REPAIR_MEMORY_BYTES_MAX: &str = "1 B";
/// Logged by a receiver that adopts the staged copies of an earlier attempt.
/// Its `adopted_len` field counts the bytes that it did not pull.
const ADOPTED_MARKER: &str = "adopted staged segments from an earlier transfer attempt";
/// Logged by a receiver that fills an artifact with the bytes of its own
/// segment. Its `seeded_len` field counts them.
const SEED_MARKER: &str = "seeded a partition transfer artifact from the local segment";
/// Logged by a receiver whose seeded artifact failed the manifest checksum.
const PREFIX_MISMATCH_MARKER: &str = "the local segment prefix does not match the offered artifact";
/// Logged once per session before the install. Its `pulled_len` field counts
/// the chunk bytes that crossed the network.
const PULLED_MARKER: &str = "partition transfer pulled every artifact";
/// Logged by a requester that schedules another transfer attempt, for
/// example after a refusal.
const REARM_MARKER: &str = "partition transfer did not land; scheduling a re-arm";
/// Bulky sends per round of the reuse spec: 10 MiB, more than one
/// [`SEGMENT_SIZE_MULTI_ARTIFACT`], so each round seals at least one segment.
const BULKY_SENDS_PER_ROUND: u32 = 40;
/// Transfers in the reuse spec. The second one must reuse a sealed segment
/// that the first one pulled.
const REUSE_ROUNDS: usize = 2;

/// Transfer end-to-end: adoption, repair round-trip, conversion, chunk pull,
/// install, tail repair. CI runners are slow; bound without hanging the suite.
const TRANSFER_BUDGET: Duration = Duration::from_secs(60);
const MARKER_POLL: Duration = Duration::from_millis(200);

#[iggy_harness(
    cluster_nodes = 3,
    server(
        sharding.cpu_allocation = "0..1",
        partition.evicted_ring_capacity = "64"
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
    let stream = Identifier::named(STREAM_NAME).expect("stream identifier");
    let topic = Identifier::named(TOPIC_NAME).expect("topic identifier");
    client
        .create_consumer_group(&stream, &topic, EXTERNAL_GROUP)
        .await
        .expect("create the consumer group that names the external group");
    client
        .store_consumer_offset(
            &Consumer::external_group(Identifier::named(EXTERNAL_GROUP).expect("group identifier")),
            &stream,
            &topic,
            Some(PARTITION_ID),
            STORED_EXTERNAL_OFFSET,
        )
        .await
        .expect("store an external group offset before the wipe");
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
    // persisted consumer-offset file (leading LE u64 offset, trailing
    // checksum; see `partitions::offset_storage::encode_offset_record`).
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
    for (kind_dir, expected) in [
        ("consumers", STORED_CONSUMER_OFFSET),
        ("external_groups", STORED_EXTERNAL_OFFSET),
    ] {
        let offsets_file = find_offset_file(&data_path, kind_dir)
            .unwrap_or_else(|| panic!("transferred {kind_dir} offset file exists on node 2"));
        let bytes = std::fs::read(&offsets_file).expect("read transferred offset");
        let offset_bytes = bytes
            .first_chunk::<8>()
            .expect("offset file starts with a u64 offset");
        assert_eq!(
            u64::from_le_bytes(*offset_bytes),
            expected,
            "the stored {kind_dir} offset must survive the transfer"
        );
    }

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
        sharding.cpu_allocation = "0..1",
        partition.evicted_ring_capacity = "64"
    )
)]
async fn given_evicted_ring_when_node_restarts_with_data_should_state_transfer_partition(
    harness: &mut TestHarness,
) {
    restart_lagging_node_and_await_install(harness).await;
}

/// The survivors' ring holds more than one repair chunk (`repair_chunk_max`,
/// 128 ops) above node 2's floor, yet fewer than the 200 ops node 2 misses, so
/// the floor is still evicted. Nothing is written after the restart, so the
/// ring never slides the window down to one chunk. The chunk is pinned: one of
/// 150 ops or more covers the window in one round, and a refusal that waits
/// for the whole window would then pass too.
#[iggy_harness(
    cluster_nodes = 3,
    server(
        sharding.cpu_allocation = "0..1",
        partition.evicted_ring_capacity = "150",
        cluster.repair_chunk_max = "128"
    )
)]
async fn given_evicted_ring_wider_than_repair_chunk_when_node_restarts_with_data_should_state_transfer_partition(
    harness: &mut TestHarness,
) {
    restart_lagging_node_and_await_install(harness).await;
}

#[iggy_harness(
    cluster_nodes = 3,
    server(
        sharding.cpu_allocation = "0..1",
        partition.evicted_ring_capacity = "64"
    )
)]
// The topic's 8 MiB segment cap makes the 64 MiB bulky seed span several sealed
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
    seed_topic(&client, Some(SEGMENT_SIZE_MULTI_ARTIFACT)).await;
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

/// A backup that falls past the evicted ring must rejoin through one state
/// transfer while clients keep writing, and then stay caught up.
///
/// Node 2 is cut off while the partition commits more ops than the ring holds,
/// so its repair after the heal hits `RangeEvicted` and converts to a transfer.
/// One message per second keeps arriving for the whole run. That is enough to
/// keep the cluster's commit point moving during every pull. No bulky seed is
/// needed: the pull itself is a few KiB.
#[tokio::test(flavor = "multi_thread")]
#[parallel]
async fn given_sustained_writes_when_a_node_falls_past_the_evicted_ring_should_rejoin_through_one_transfer()
 {
    let mut harness = relayed_cluster();
    harness.start().await.expect("start the cluster");
    let client = harness
        .root_client_for_node(0)
        .await
        .expect("connect a root client to the node");
    seed_topic(&client, None).await;

    harness
        .isolate_node(LAGGING_NODE)
        .expect("cut node 2 off from its peers");
    produce(&client, ISOLATED_SENDS).await;
    let writer = SteadyWriter::start(
        harness
            .root_client_for_node(0)
            .await
            .expect("connect the steady writer"),
    );
    harness
        .heal_node(LAGGING_NODE)
        .expect("restore node 2's peer links");
    await_marker(&harness, LAGGING_NODE, CONVERSION_MARKER).await;

    // The first request only pins the offer at the primary's commit point (the
    // build answers "flush is pending"), and the pull runs on the next request,
    // one `repair_retry_interval` later. Node 2 journals the ops committed in
    // between, so it holds ops above the offer it installs.
    let deadline = Instant::now() + TRANSFER_BUDGET;
    while harness
        .node(LAGGING_NODE)
        .stdout_occurrences(INSTALL_MARKER)
        == 0
    {
        assert!(
            Instant::now() < deadline,
            "node 2 fell past the evicted ring and never installed a state transfer within \
             {TRANSFER_BUDGET:?} while one message per second was written: the peers fully \
             served {} pull(s) and node 2 failed {} install(s), {} of them for held ops, so \
             a backup cannot rejoin under sustained writes",
            fully_served_count(&harness),
            harness
                .node(LAGGING_NODE)
                .stdout_occurrences(INSTALL_FAILED_MARKER),
            harness
                .node(LAGGING_NODE)
                .stdout_occurrences(HELD_OPS_INSTALL_MARKER),
        );
        sleep(MARKER_POLL).await;
    }

    // Installed: the tail above the offer is now ordinary journal repair. A
    // backup that keeps up never converts to a transfer again.
    let conversions = harness
        .node(LAGGING_NODE)
        .stdout_occurrences(CONVERSION_MARKER);
    let acked_at_install = writer.acked();
    let window_end = Instant::now() + CAUGHT_UP_WINDOW;
    while Instant::now() < window_end {
        let converted = harness
            .node(LAGGING_NODE)
            .stdout_occurrences(CONVERSION_MARKER)
            - conversions;
        assert_eq!(
            converted, 0,
            "node 2 installed a state transfer, then fell past the evicted ring again and \
             converted to another transfer while writes continued"
        );
        sleep(MARKER_POLL).await;
    }
    // The markers only say the machinery ran. Every send committed while node
    // 2 was cut off must be in its own segment bytes, in order.
    let expected: Vec<String> = (0..ISOLATED_SENDS)
        .map(|sequence| format!("message-{sequence}"))
        .collect();
    await_installed_payloads(&harness, LAGGING_NODE, &expected).await;
    let acked = writer.stop().await;
    assert!(
        acked > acked_at_install,
        "the steady writer must keep committing through the caught-up window"
    );
}

/// A node that falls past the evicted ring must link the sealed segments it
/// already holds into each transfer and pull only the rest.
///
/// Node 2 holds sealed segments that it wrote itself, then falls behind twice.
/// Each segment that node 2 held sealed at the cut must keep its inode through
/// the install: the install renamed a link to it, and none of its bytes
/// crossed the network. A pulled segment always lands in a new file, because
/// the old one still exists when the pull writes it. The second round must
/// also keep a segment that the first round pulled, which proves that the
/// install stamps the segments it installs. The reuse line must count exactly
/// the bytes of the kept segments.
#[tokio::test(flavor = "multi_thread")]
#[parallel]
async fn given_sealed_segments_on_a_lagging_node_when_it_transfers_twice_should_pull_only_the_rest()
{
    let mut harness =
        relayed_cluster_with_repair_budget(Some(SNAPSHOT_ONLY_REPAIR_MEMORY_BYTES_MAX));
    harness.start().await.expect("start the cluster");
    let client = harness
        .root_client_for_node(0)
        .await
        .expect("connect a root client to the node");
    seed_topic(&client, Some(SEGMENT_SIZE_MULTI_ARTIFACT)).await;
    produce_bulky(&client, 2 * BULKY_SENDS_PER_ROUND, BULKY_PAYLOAD_LEN).await;
    let mut expected: Vec<String> = (0..2 * BULKY_SENDS_PER_ROUND)
        .map(bulky_payload_prefix)
        .collect();
    let data_path = harness.node(LAGGING_NODE).data_path();
    let mut kept_before = BTreeSet::new();

    for round in 1..=REUSE_ROUNDS {
        // Node 2 must hold every committed payload, so the segments that it
        // holds sealed are final before the cut.
        await_installed_payloads(&harness, LAGGING_NODE, &expected).await;
        let held = segment_files(&data_path);
        let sealed: Vec<u64> = held.keys().rev().skip(1).copied().collect();
        assert!(
            !sealed.is_empty(),
            "round {round}: node 2 must hold a sealed segment before the cut"
        );
        if round > 1 {
            assert!(
                sealed.iter().any(|start| !kept_before.contains(start)),
                "round {round}: node 2 must hold a sealed segment that the last transfer \
                 pulled, or this round cannot prove that the install stamps it"
            );
        }
        let installs = harness
            .node(LAGGING_NODE)
            .stdout_occurrences(INSTALL_MARKER);
        let reuses = harness.node(LAGGING_NODE).stdout_occurrences(REUSE_MARKER);

        harness
            .isolate_node(LAGGING_NODE)
            .expect("cut node 2 off from its peers");
        produce_bulky(&client, BULKY_SENDS_PER_ROUND, BULKY_PAYLOAD_LEN).await;
        produce(&client, ISOLATED_SENDS).await;
        harness
            .heal_node(LAGGING_NODE)
            .expect("restore node 2's peer links");
        await_new_marker(&harness, LAGGING_NODE, INSTALL_MARKER, installs).await;
        expected.extend((0..BULKY_SENDS_PER_ROUND).map(bulky_payload_prefix));
        expected.extend((0..ISOLATED_SENDS).map(|sequence| format!("message-{sequence}")));
        await_installed_payloads(&harness, LAGGING_NODE, &expected).await;

        let installed = segment_files(&data_path);
        let kept: BTreeMap<u64, u64> = held
            .iter()
            .filter(|(start, file)| installed.get(start) == Some(file))
            .map(|(start, (_, len))| (*start, *len))
            .collect();
        for start in &sealed {
            assert!(
                kept.contains_key(start),
                "round {round}: node 2 held segment {start} sealed, but the install replaced \
                 its file, so the transfer copied bytes that node 2 already had"
            );
        }
        assert_eq!(
            reused_len_after(&harness, LAGGING_NODE, reuses),
            kept.values().sum::<u64>(),
            "round {round}: the reuse line must count exactly the bytes of the kept segments"
        );
        kept_before = kept.into_keys().collect();
    }
}

/// A node that falls past the evicted ring while the primary keeps writing
/// the segment that the node holds a prefix of must pull only the rest.
///
/// Each round, node 2 holds the leading bytes of the segment that the
/// primary writes on during the cut. The transfer must seed that artifact
/// with them, so the pulled bytes are the manifest minus the reused and the
/// held bytes. Between the rounds, node 2 fills and seals the segment that the
/// transfer installed as active. The next transfer must keep that segment like
/// any other sealed one, which proves that its checksum memo grew with it.
#[tokio::test(flavor = "multi_thread")]
#[parallel]
async fn given_a_segment_prefix_on_a_lagging_node_when_it_transfers_should_pull_only_the_suffix() {
    let mut harness =
        relayed_cluster_with_repair_budget(Some(SNAPSHOT_ONLY_REPAIR_MEMORY_BYTES_MAX));
    harness.start().await.expect("start the cluster");
    let client = harness
        .root_client_for_node(0)
        .await
        .expect("connect a root client to the node");
    seed_topic(&client, Some(SEGMENT_SIZE_MULTI_ARTIFACT)).await;
    produce_bulky(&client, 2 * BULKY_SENDS_PER_ROUND, BULKY_PAYLOAD_LEN).await;
    let mut expected: Vec<String> = (0..2 * BULKY_SENDS_PER_ROUND)
        .map(bulky_payload_prefix)
        .collect();
    let data_path = harness.node(LAGGING_NODE).data_path();
    let mut installed_active: Option<u64> = None;

    for round in 1..=REUSE_ROUNDS {
        await_installed_payloads(&harness, LAGGING_NODE, &expected).await;
        let held = segment_files(&data_path);
        let (active_start, active_len) = held_prefix(&held, round);
        if let Some(installed) = installed_active {
            assert!(
                installed < active_start,
                "round {round}: the segment that the last transfer installed as active must \
                 seal on node 2 before the cut"
            );
        }
        let installs = harness
            .node(LAGGING_NODE)
            .stdout_occurrences(INSTALL_MARKER);

        harness
            .isolate_node(LAGGING_NODE)
            .expect("cut node 2 off from its peers");
        produce_bulky(&client, BULKY_SENDS_PER_ROUND, BULKY_PAYLOAD_LEN).await;
        produce(&client, ISOLATED_SENDS).await;
        harness
            .heal_node(LAGGING_NODE)
            .expect("restore node 2's peer links");
        await_new_marker(&harness, LAGGING_NODE, INSTALL_MARKER, installs).await;
        expected.extend((0..BULKY_SENDS_PER_ROUND).map(bulky_payload_prefix));
        expected.extend((0..ISOLATED_SENDS).map(|sequence| format!("message-{sequence}")));
        await_installed_payloads(&harness, LAGGING_NODE, &expected).await;

        let session = installed_session(&harness, LAGGING_NODE);
        // Node 2 can persist bytes of its active segment after the files were
        // listed, so the seed holds at least what the listing saw.
        let (seeded_len, _) = session.seed.unwrap_or_else(|| {
            panic!(
                "round {round}: node 2 held {active_len} bytes of segment {active_start}, \
                 but it did not seed them"
            )
        });
        assert!(
            seeded_len >= active_len,
            "round {round}: the seed must hold the {active_len} bytes that node 2 held, \
             got {seeded_len}"
        );
        assert_eq!(
            session.pulled_len,
            session.total_len - session.reused_len - seeded_len,
            "round {round}: node 2 seeded {seeded_len} bytes of segment {active_start}, so the \
             transfer must pull the manifest minus those and the reused bytes"
        );
        let installed = segment_files(&data_path);
        if let Some(sealed) = installed_active {
            assert_eq!(
                installed.get(&sealed).map(|(inode, _)| inode),
                held.get(&sealed).map(|(inode, _)| inode),
                "round {round}: node 2 sealed segment {sealed} after the last transfer \
                 installed it as active, but this install replaced its file, so the transfer \
                 pulled bytes that node 2 already had"
            );
        }
        installed_active = installed.keys().next_back().copied();
        // With node 2 connected: fill and seal the segment that this transfer
        // installed as active. The small sends leave bytes in the segment
        // after it, which the next round seeds.
        produce_bulky(&client, BULKY_SENDS_PER_ROUND, BULKY_PAYLOAD_LEN).await;
        produce(&client, ISOLATED_SENDS).await;
        expected.extend((0..BULKY_SENDS_PER_ROUND).map(bulky_payload_prefix));
        expected.extend((0..ISOLATED_SENDS).map(|sequence| format!("message-{sequence}")));
    }
}

/// A held prefix with other bytes than the offered segment must not reach the
/// install. The checksum of the seeded artifact fails, and the transfer pulls
/// that artifact whole in the same session.
#[tokio::test(flavor = "multi_thread")]
#[parallel]
async fn given_a_damaged_segment_prefix_when_it_transfers_should_pull_the_whole_artifact() {
    let mut harness = relayed_cluster();
    harness.start().await.expect("start the cluster");
    let client = harness
        .root_client_for_node(0)
        .await
        .expect("connect a root client to the node");
    seed_topic(&client, Some(SEGMENT_SIZE_MULTI_ARTIFACT)).await;
    produce_bulky(&client, 2 * BULKY_SENDS_PER_ROUND, BULKY_PAYLOAD_LEN).await;
    let mut expected: Vec<String> = (0..2 * BULKY_SENDS_PER_ROUND)
        .map(bulky_payload_prefix)
        .collect();
    let data_path = harness.node(LAGGING_NODE).data_path();
    await_installed_payloads(&harness, LAGGING_NODE, &expected).await;
    let held = segment_files(&data_path);
    let (active_start, active_len) = held_prefix(&held, 1);
    let installs = harness
        .node(LAGGING_NODE)
        .stdout_occurrences(INSTALL_MARKER);
    let mismatches = harness
        .node(LAGGING_NODE)
        .stdout_occurrences(PREFIX_MISMATCH_MARKER);

    harness
        .isolate_node(LAGGING_NODE)
        .expect("cut node 2 off from its peers");
    flip_segment_byte(&data_path, active_start, active_len / 2);
    produce_bulky(&client, BULKY_SENDS_PER_ROUND, BULKY_PAYLOAD_LEN).await;
    produce(&client, ISOLATED_SENDS).await;
    harness
        .heal_node(LAGGING_NODE)
        .expect("restore node 2's peer links");
    await_new_marker(&harness, LAGGING_NODE, INSTALL_MARKER, installs).await;
    expected.extend((0..BULKY_SENDS_PER_ROUND).map(bulky_payload_prefix));
    expected.extend((0..ISOLATED_SENDS).map(|sequence| format!("message-{sequence}")));
    await_installed_payloads(&harness, LAGGING_NODE, &expected).await;

    assert_eq!(
        harness
            .node(LAGGING_NODE)
            .stdout_occurrences(PREFIX_MISMATCH_MARKER),
        mismatches + 1,
        "the seeded artifact must fail its checksum once, and the whole pull must pass"
    );
    let session = installed_session(&harness, LAGGING_NODE);
    let (seeded_len, artifact_len) = session
        .seed
        .expect("node 2 must seed the artifact before its checksum fails");
    assert!(
        seeded_len >= active_len,
        "the seed must hold the {active_len} bytes that node 2 held, got {seeded_len}"
    );
    assert_eq!(
        session.pulled_len,
        session.total_len - session.reused_len + artifact_len - seeded_len,
        "the transfer must pull the suffix, then the whole artifact once the checksum failed"
    );
}

/// The first request of a transfer finds the plan of the primary unpinned,
/// because the pin waits for the next I/O turn of the partition. The primary
/// must keep that request and answer it once the plan is pinned. A refusal
/// makes the requester wait out a whole re-arm interval for nothing.
#[tokio::test(flavor = "multi_thread")]
#[parallel]
async fn given_an_unpinned_plan_when_a_transfer_is_requested_should_answer_without_a_re_arm() {
    let mut harness = relayed_cluster();
    harness.start().await.expect("start the cluster");
    let client = harness
        .root_client_for_node(0)
        .await
        .expect("connect a root client to the node");
    seed_topic(&client, Some(SEGMENT_SIZE_MULTI_ARTIFACT)).await;
    produce_bulky(&client, BULKY_SENDS_PER_ROUND, BULKY_PAYLOAD_LEN).await;
    let mut expected: Vec<String> = (0..BULKY_SENDS_PER_ROUND)
        .map(bulky_payload_prefix)
        .collect();
    await_installed_payloads(&harness, LAGGING_NODE, &expected).await;
    let installs = harness
        .node(LAGGING_NODE)
        .stdout_occurrences(INSTALL_MARKER);
    let rearms = harness.node(LAGGING_NODE).stdout_occurrences(REARM_MARKER);

    harness
        .isolate_node(LAGGING_NODE)
        .expect("cut node 2 off from its peers");
    produce_bulky(&client, BULKY_SENDS_PER_ROUND, BULKY_PAYLOAD_LEN).await;
    produce(&client, ISOLATED_SENDS).await;
    harness
        .heal_node(LAGGING_NODE)
        .expect("restore node 2's peer links");
    await_new_marker(&harness, LAGGING_NODE, INSTALL_MARKER, installs).await;
    expected.extend((0..BULKY_SENDS_PER_ROUND).map(bulky_payload_prefix));
    expected.extend((0..ISOLATED_SENDS).map(|sequence| format!("message-{sequence}")));
    await_installed_payloads(&harness, LAGGING_NODE, &expected).await;

    assert_eq!(
        harness.node(LAGGING_NODE).stdout_occurrences(REARM_MARKER),
        rearms,
        "the primary must answer the first transfer request of node 2 with an offer, \
         so node 2 must not schedule a re-arm before the install"
    );
}

async fn restart_lagging_node_and_await_install(harness: &mut TestHarness) {
    // Node 2 holds a durable prefix, then misses enough traffic that the
    // survivors' ring moves past its durable end: its repaired window cannot
    // connect, which is exactly the refusal-site trigger.
    let client = harness
        .root_client_for_node(0)
        .await
        .expect("connect a root client to the node");
    seed_topic(&client, None).await;
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

/// The specs above go through `iggy_harness`, which cannot relay the replica
/// links, so the one that cuts a node off builds its cluster here with the same
/// server settings.
fn relayed_cluster() -> TestHarness {
    relayed_cluster_with_repair_budget(None)
}

fn relayed_cluster_with_repair_budget(repair_memory_bytes_max: Option<&str>) -> TestHarness {
    let mut overrides = HashMap::from([
        ("sharding.cpu_allocation".to_owned(), "0..1".to_owned()),
        (
            "partition.evicted_ring_capacity".to_owned(),
            "64".to_owned(),
        ),
    ]);
    if let Some(bytes_max) = repair_memory_bytes_max {
        overrides.insert(
            "partition.repair_memory_bytes_max".to_owned(),
            bytes_max.to_owned(),
        );
    }
    let extra_envs = resolve_config_paths(&overrides).expect("valid server config paths");
    TestHarness::builder()
        .server(TestServerConfig::builder().extra_envs(extra_envs).build())
        .cluster_nodes(3)
        .peer_relays()
        .build()
        .expect("build the harness")
}

/// Sends one small message per [`WRITE_INTERVAL`] from its own task until
/// stopped. A failed send is not retried: the next tick sends a new message,
/// and only acknowledged sends are counted.
struct SteadyWriter {
    stop: Arc<AtomicBool>,
    acked: Arc<AtomicU64>,
    task: JoinHandle<()>,
}

impl SteadyWriter {
    fn start(client: IggyClient) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let acked = Arc::new(AtomicU64::new(0));
        let task = tokio::spawn({
            let stop = Arc::clone(&stop);
            let acked = Arc::clone(&acked);
            async move {
                let stream = Identifier::named(STREAM_NAME).expect("stream identifier");
                let topic = Identifier::named(TOPIC_NAME).expect("topic identifier");
                let mut sequence = 0_u64;
                while !stop.load(Ordering::Relaxed) {
                    let mut messages = vec![
                        IggyMessage::from_str(&format!("steady-{sequence}")).expect("message"),
                    ];
                    sequence += 1;
                    if client
                        .send_messages(
                            &stream,
                            &topic,
                            &Partitioning::partition_id(PARTITION_ID),
                            &mut messages,
                        )
                        .await
                        .is_ok()
                    {
                        acked.fetch_add(1, Ordering::Relaxed);
                    }
                    sleep(WRITE_INTERVAL).await;
                }
            }
        });
        Self { stop, acked, task }
    }

    fn acked(&self) -> u64 {
        self.acked.load(Ordering::Relaxed)
    }

    async fn stop(self) -> u64 {
        self.stop.store(true, Ordering::Relaxed);
        self.task.await.expect("the steady writer task ends");
        self.acked.load(Ordering::Relaxed)
    }
}

/// Pulls the survivors served to their last byte, whoever served them.
fn fully_served_count(harness: &TestHarness) -> usize {
    (0..harness.cluster_size())
        .filter(|&node| node != LAGGING_NODE)
        .map(|node| harness.node(node).stdout_occurrences(FULLY_SERVED_MARKER))
        .sum()
}

/// `reused_len` of the first reuse line that `node` logged after `baseline`
/// earlier ones.
fn reused_len_after(harness: &TestHarness, node: usize, baseline: usize) -> u64 {
    let log = harness.node(node).stdout_plain();
    let line = log
        .lines()
        .filter(|line| line.contains(REUSE_MARKER))
        .nth(baseline)
        .unwrap_or_else(|| panic!("node {node} logged no {REUSE_MARKER:?} line for the transfer"));
    logged_number(line, "reused_len")
}

/// What the last session that `node` pulled to the end logged.
struct PulledSession {
    total_len: u64,
    /// The bytes that it staged without a pull: `reused_len` of its reuse
    /// line plus `adopted_len` of the adopted line before it.
    reused_len: u64,
    /// `seeded_len` and `artifact_len` of its seed line.
    seed: Option<(u64, u64)>,
    pulled_len: u64,
}

/// The lines of one session come in order: adopted and reuse when its reuse
/// scan lands, the seed when the pull reaches that artifact, the pulled line
/// before the install.
fn installed_session(harness: &TestHarness, node: usize) -> PulledSession {
    let log = harness.node(node).stdout_plain();
    let mut adopted = None;
    let mut reuse = None;
    let mut seed = None;
    let mut session = None;
    for line in log.lines() {
        if line.contains(ADOPTED_MARKER) {
            adopted = Some(line);
        } else if line.contains(REUSE_MARKER) {
            reuse = Some((line, adopted.take()));
            seed = None;
        } else if line.contains(SEED_MARKER) {
            seed = Some(line);
        } else if line.contains(PULLED_MARKER) {
            session = Some((reuse, seed, line));
        }
    }
    let (reuse, seed, pulled) =
        session.unwrap_or_else(|| panic!("node {node} logged no {PULLED_MARKER:?} line"));
    let (reuse, adopted) =
        reuse.unwrap_or_else(|| panic!("node {node} logged no {REUSE_MARKER:?} line"));
    PulledSession {
        total_len: logged_number(reuse, "total_len"),
        reused_len: logged_number(reuse, "reused_len")
            + adopted.map_or(0, |line| logged_number(line, "adopted_len")),
        seed: seed.map(|line| {
            (
                logged_number(line, "seeded_len"),
                logged_number(line, "artifact_len"),
            )
        }),
        pulled_len: logged_number(pulled, "pulled_len"),
    }
}

fn logged_number(line: &str, field: &str) -> u64 {
    let label = format!(" {field}=");
    let start = line
        .find(&label)
        .unwrap_or_else(|| panic!("the line has no {label:?} field: {line}"))
        + label.len();
    line[start..]
        .split(|character: char| !character.is_ascii_digit())
        .next()
        .and_then(|digits| digits.parse().ok())
        .unwrap_or_else(|| panic!("the line has no number in {label:?}: {line}"))
}

/// The active segment of `held` and its length. Sealed segments must exist,
/// so the transfer logs a reuse line, and the active one must hold bytes, so
/// it can seed an artifact.
fn held_prefix(held: &BTreeMap<u64, (u64, u64)>, round: usize) -> (u64, u64) {
    assert!(
        held.len() > 1,
        "round {round}: node 2 must hold a sealed segment before the cut"
    );
    let (&start, &(_, len)) = held
        .last_key_value()
        .expect("node 2 holds a segment before the cut");
    assert!(
        len > 0,
        "round {round}: node 2's active segment must hold bytes before the cut"
    );
    (start, len)
}

/// Invert one byte of the segment log at `start_offset`.
fn flip_segment_byte(root: &Path, start_offset: u64, position: u64) {
    let path = walk(root, &mut |path| {
        is_segment_log(path)
            && path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .and_then(|stem| stem.parse::<u64>().ok())
                == Some(start_offset)
    })
    .unwrap_or_else(|| panic!("no segment log starts at offset {start_offset}"));
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .expect("open the segment log");
    let mut byte = [0_u8; 1];
    file.read_exact_at(&mut byte, position)
        .expect("read the segment byte");
    byte[0] = !byte[0];
    file.write_all_at(&byte, position)
        .expect("write the segment byte");
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

/// `segment_size` is `None` for the specs that only need the default cap; the
/// multi-artifact spec passes one small enough that its bulky seed spans
/// several sealed segments. `messages_required_to_save = 1` is the forcing
/// function every spec here shares: each commit flushes and ring-evicts, which
/// is what marches `repair_retained_from` forward.
async fn seed_topic(client: &IggyClient, segment_size: Option<u64>) {
    client
        .create_stream(STREAM_NAME, &StreamCreateOptions::default())
        .await
        .expect("create stream");
    client
        .create_topic(
            &Identifier::named(STREAM_NAME).expect("stream identifier"),
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::NeverExpire),
                messages_required_to_save: Some(1),
                segment_size: segment_size.map(IggyByteSize::from),
                durability: Durability::Persisted,
                consumer_offset_durability: Durability::Persisted,
                ..TopicCreateOptions::default()
            },
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
        let payload = format!(
            "{}{}",
            bulky_payload_prefix(sequence),
            "x".repeat(payload_len)
        );
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

/// The leading bytes of the payload that [`produce_bulky`] sends as its
/// `sequence`-th message.
fn bulky_payload_prefix(sequence: u32) -> String {
    format!("bulky-{sequence}-")
}

async fn seed_partition(client: &IggyClient) {
    seed_topic(client, None).await;
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
            PARTITION_ID,
            &Consumer::default(),
            &PollingStrategy::offset(0),
            PollOptions {
                count,
                ..PollOptions::default()
            },
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

/// Inode and length of each segment log under `root`, by base offset. A
/// segment that keeps its inode through an install was linked, not copied.
fn segment_files(root: &Path) -> BTreeMap<u64, (u64, u64)> {
    let mut files = BTreeMap::new();
    let _ = walk(root, &mut |path| {
        if is_segment_log(path)
            && let Some(start_offset) = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .and_then(|stem| stem.parse().ok())
            && let Ok(metadata) = std::fs::metadata(path)
        {
            files.insert(start_offset, (metadata.ino(), metadata.len()));
        }
        false
    });
    files
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

/// The first offset file in a `kind_dir` directory, such as `consumers`.
fn find_offset_file(root: &Path, kind_dir: &str) -> Option<PathBuf> {
    walk(root, &mut |path| {
        path.parent()
            .and_then(Path::file_name)
            .is_some_and(|name| name == kind_dir)
            && std::fs::metadata(path).is_ok_and(|metadata| metadata.len() >= 8)
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
