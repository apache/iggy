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

//! Spec test for metadata state transfer: a node that restarts into a live
//! cluster replaces its snapshot-shaped state (metadata snapshot + client
//! table) from the current primary instead of relying on its own WAL.
//!
//! The scenario forces the interesting case: enough committed metadata ops
//! to trip a checkpoint on every node (`journal_slots` shrunk below), which
//! drains the early ops -- including the client's register -- out of every
//! WAL. A restarted node's local recovery can then neither replay those ops
//! nor journal-repair them (the serving peers evicted them too); only the
//! transferred snapshot + table carry that history.
//!
//! The transferred node is a follower afterwards, so its installed state has
//! no client-visible surface to assert against (followers neither commit nor
//! serve resume lookups). The install is pinned via its log marker; the
//! functional assert (post-restart continuation commits cluster-wide) rides
//! on top.

#![cfg(feature = "vsr")]

use super::client_table_restart::{
    commit_request, create_stream_payload, register, resume_request, tcp_addr, tcp_addrs,
};
use integration::harness::TestHarness;
use integration::iggy_harness;
use std::time::Duration;
use tokio::time::{Instant, sleep};

/// Committed ops before the restart. `journal_slots = 256` with the built-in
/// checkpoint margin (64) forces a checkpoint at ~192 committed ops, so this
/// guarantees at least one checkpoint+drain on every node, pushing the
/// register (op 1) below every snapshot floor.
const OPS_BEFORE_RESTART: u64 = 220;

/// How long the restarted follower gets to probe, fetch, and install.
const TRANSFER_BUDGET: Duration = Duration::from_secs(30);

const MARKER_POLL: Duration = Duration::from_millis(200);

#[iggy_harness(cluster_nodes = 3, server(metadata.journal_slots = "256"))]
async fn given_checkpointed_cluster_when_node_restarts_should_state_transfer_metadata(
    harness: &mut TestHarness,
) {
    let addr = tcp_addr(harness);
    let (mut stream, session) = register(addr).await;
    for request in 1..=OPS_BEFORE_RESTART {
        commit_request(
            &mut stream,
            session,
            request,
            &create_stream_payload(&format!("iggy-transfer-{request}")),
        )
        .await;
    }
    drop(stream);

    harness.restart_server().await.unwrap();

    // Functional: the session (registered below every snapshot floor by now)
    // still continues cluster-wide.
    let addrs = tcp_addrs(harness);
    // A continuation is a fresh op, so there is no cached reply to compare.
    resume_request(
        &addrs,
        session,
        OPS_BEFORE_RESTART + 1,
        &create_stream_payload("iggy-transfer-continuation"),
        None,
    )
    .await;

    // The restarted node itself: it must have entered state transfer,
    // fetched the primary's snapshot + client table, and installed them.
    // Its own WAL no longer holds the pre-checkpoint history, so nothing
    // short of the transfer can restore that state on it.
    let deadline = Instant::now() + TRANSFER_BUDGET;
    loop {
        if harness
            .node(0)
            .stdout_contains("metadata state transfer installed")
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "restarted node never completed the metadata state transfer \
             within {TRANSFER_BUDGET:?}"
        );
        sleep(MARKER_POLL).await;
    }
}

/// A node that joins a live, already-checkpointed cluster with NO local
/// history must state-transfer, exactly like the restart-with-WAL case above.
///
/// This is the shape the user cares about beyond a plain restart: a fresh
/// operator-provisioned replacement, or the third node of a cluster whose
/// first two formed quorum and committed past a checkpoint before it arrived.
/// It has no WAL to probe from, so the restart-arms-transfer boot path does
/// not apply -- it joins as a plain view-0 backup, learns the frontier from
/// the primary's heartbeats, discovers via journal repair that the gap sits
/// below the retained floor, and converts THAT into a state transfer. The
/// same `RangeEvicted`-to-transfer path serves the restart, the late join,
/// and a partition heal; only the way each reaches repair differs.
///
/// Node 2 (a follower) is wiped rather than node 0 so the client keeps its
/// primary and the cluster never loses quorum -- a genuine late join, not a
/// restart of the whole cluster.
#[iggy_harness(cluster_nodes = 3, server(metadata.journal_slots = "256"))]
async fn given_checkpointed_cluster_when_fresh_node_joins_late_should_state_transfer_metadata(
    harness: &mut TestHarness,
) {
    let addr = tcp_addr(harness);
    let (mut stream, session) = register(addr).await;
    for request in 1..=OPS_BEFORE_RESTART {
        commit_request(
            &mut stream,
            session,
            request,
            &create_stream_payload(&format!("iggy-latejoin-{request}")),
        )
        .await;
    }

    // Wipe a follower and bring it back with an empty data directory while the
    // other two keep quorum. Its missing prefix (register at op 1 included) was
    // checkpointed away on every survivor, so nothing but a transfer can seed
    // its metadata state and client table.
    harness.restart_node_from_clean_slate(2).unwrap();

    let deadline = Instant::now() + TRANSFER_BUDGET;
    loop {
        if harness
            .node(2)
            .stdout_contains("metadata state transfer installed")
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "fresh late-joining node never completed the metadata state transfer \
             within {TRANSFER_BUDGET:?}; it must convert a repair-floor eviction \
             into a transfer, not wedge below the floor"
        );
        sleep(MARKER_POLL).await;
    }

    // Functional: the cluster still commits with the rejoined node present, and
    // the original session (registered below every floor) continues.
    drop(stream);
    let addrs = tcp_addrs(harness);
    resume_request(
        &addrs,
        session,
        OPS_BEFORE_RESTART + 1,
        &create_stream_payload("iggy-latejoin-continuation"),
        None,
    )
    .await;

    // The install replaced `snapshot.bin` with the primary's copy, so the
    // superblock's `(checkpoint_op, checksum)` pairing has to name THAT snapshot.
    // A stale pairing does not refuse boot -- `verify_checkpoint_pairing` reads
    // `checkpoint_op < snapshot_op` as a lagging local checkpoint and accepts --
    // which is exactly why this reads the durable record instead of inferring
    // health from a successful restart: a pairing that names a snapshot no longer
    // on disk silently disarms the integrity check it exists to power.
    //
    // A wiped node has `local_applied == 0`, so the transferred snapshot is always
    // ahead and the pairing is always recorded; the marker carries the op it
    // recorded, so the assertion compares against what install actually wrote.
    assert_pairing_matches_install(harness, 2).await;
}

/// Assert `node`'s durable superblock pairs with the checkpoint its state-transfer
/// install recorded.
async fn assert_pairing_matches_install(harness: &TestHarness, node: usize) {
    const PAIRING_MARKER: &str = "state transfer recorded its checkpoint pairing";

    // Escapes stripped by the harness: the server colors its tracing fields, so
    // `checkpoint_op=193` reaches the log as
    // `checkpoint_op\x1b[0m\x1b[2m=\x1b[0m193`.
    let plain = harness.node(node).stdout_plain();
    let recorded_op: u64 = plain
        .lines()
        .filter(|line| line.contains(PAIRING_MARKER))
        .filter_map(|line| {
            let rest = line.split("checkpoint_op=").nth(1)?;
            rest.split(|c: char| !c.is_ascii_digit())
                .find(|token| !token.is_empty())?
                .parse::<u64>()
                .ok()
        })
        .next_back()
        .expect("a wiped node's transfer must record a checkpoint pairing");

    // The superblock reads through `compio` file I/O and this test runs on tokio,
    // so give it a runtime of its own on a scratch thread.
    let metadata_dir = harness.node(node).data_path().join("metadata");
    let record = std::thread::spawn(move || {
        compio::runtime::Runtime::new()
            .expect("compio runtime")
            .block_on(async move {
                let superblock = journal::superblock::PingPongSuperblock::open(&metadata_dir)
                    .await
                    .expect("superblock must open");
                journal::superblock::SuperblockStore::read_latest(&superblock)
                    .await
                    .expect("superblock must be readable")
            })
    })
    .join()
    .expect("superblock reader thread");

    let journal::superblock::SuperblockContents::Present(record) = record else {
        panic!("a node that installed a transfer must have a durable superblock record");
    };
    let state =
        consensus::VsrState::try_from(record.as_slice()).expect("the durable record must decode");

    assert_eq!(
        state.checkpoint_op, recorded_op,
        "the superblock must pair with the snapshot the transfer installed \
         (durable pairing at op {}, install recorded {recorded_op}); a lagging \
         pairing names a snapshot that no longer exists",
        state.checkpoint_op
    );
    assert!(
        state.commit_max >= recorded_op,
        "the durable commit point ({}) must not sit below the transferred \
         checkpoint ({recorded_op})",
        state.commit_max
    );
}

/// A node that rejoins at a STALE VIEW must probe to catch up, not wedge.
///
/// The late-join test above rejoins a follower while the cluster is still at
/// view 0, so it adopts the frontier from same-view heartbeats. This one forces
/// the harder case: kill the view-0 primary so the survivors elect past it,
/// then rejoin that same node (replica 0) with an empty disk. It boots
/// primary-by-index at view 0 -- so it has NO heartbeat timeout to notice it is
/// behind, and thinks it is the primary of a view the cluster has abandoned.
///
/// It must observe the newer-view traffic it keeps dropping, convert its own
/// heartbeat-SEND timer into a view probe, adopt the current view from the
/// live primary's `StartView`, and from there follow the same repair ->
/// `RangeEvicted` -> transfer path. Without that it advertises a commit point
/// no peer accepts, forever.
#[iggy_harness(cluster_nodes = 3, server(metadata.journal_slots = "256"))]
async fn given_election_past_a_node_when_it_rejoins_stale_should_probe_then_state_transfer(
    harness: &mut TestHarness,
) {
    let addr = tcp_addr(harness);
    let (mut stream, session) = register(addr).await;
    for request in 1..=OPS_BEFORE_RESTART {
        commit_request(
            &mut stream,
            session,
            request,
            &create_stream_payload(&format!("iggy-stale-{request}")),
        )
        .await;
    }
    drop(stream);

    // Kill the view-0 primary (replica 0). The survivors (1, 2) hold quorum and
    // elect a new primary at a higher view, leaving replica 0 behind on view.
    harness.stop_node(0).unwrap();

    // Confirm the cluster recovered to a live primary at the new view before
    // rejoining: one continuation must commit against a survivor. The stopped
    // node's address is skipped by the round-robin.
    let addrs = tcp_addrs(harness);
    resume_request(
        &addrs,
        session,
        OPS_BEFORE_RESTART + 1,
        &create_stream_payload("iggy-stale-failover"),
        None,
    )
    .await;

    // Rejoin replica 0 with an empty disk. It boots primary-by-index at view 0
    // while the cluster sits at a higher view -- the stale-primary case.
    harness.restart_node_from_clean_slate(0).unwrap();

    // It must first PROBE (its heartbeat-send timer converting, since it has no
    // heartbeat-receive timer as a "primary"), then complete the transfer. The
    // probe marker distinguishes this path from the same-view backstop.
    let deadline = Instant::now() + TRANSFER_BUDGET;
    let mut probed = false;
    loop {
        probed = probed || harness.node(0).stdout_contains("probing to catch up");
        if harness
            .node(0)
            .stdout_contains("metadata state transfer installed")
        {
            assert!(
                probed,
                "the rejoined node transferred without first probing; the \
                 stale-view path must reach the transfer through a view probe"
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "node that rejoined at a stale view never state-transferred within \
             {TRANSFER_BUDGET:?}; a stale primary-by-index must convert its \
             heartbeat-send timer into a probe rather than wedge"
        );
        sleep(MARKER_POLL).await;
    }

    // Functional: the session continues cluster-wide with the node rejoined.
    resume_request(
        &addrs,
        session,
        OPS_BEFORE_RESTART + 2,
        &create_stream_payload("iggy-stale-continuation"),
        None,
    )
    .await;
}

/// A transfer whose serving peer dies must not wedge the rejoining node.
///
/// The stall retry re-requests from the SAME peer and has no peer re-selection,
/// so without a retry budget a node whose server died mid-transfer would retry
/// into a corpse forever -- and, being mid-transfer, it withholds `PrepareOk`
/// the whole time. The budget makes it abandon and fall back to journal repair,
/// which re-picks a target and (its gap still being below the retained floor)
/// arms a fresh transfer against whoever is primary now.
///
/// Node 1 is stopped rather than the primary: with 3 nodes the survivors (0, 2)
/// still hold quorum, so the cluster keeps committing and the rejoining node has
/// somewhere to converge to.
#[iggy_harness(cluster_nodes = 3, server(metadata.journal_slots = "256"))]
async fn given_transfer_peer_dies_when_stalled_should_abandon_and_recover(
    harness: &mut TestHarness,
) {
    let addr = tcp_addr(harness);
    let (mut stream, session) = register(addr).await;
    for request in 1..=OPS_BEFORE_RESTART {
        commit_request(
            &mut stream,
            session,
            request,
            &create_stream_payload(&format!("iggy-deadpeer-{request}")),
        )
        .await;
    }
    drop(stream);

    // Wipe node 2 so it must transfer, then take node 1 down. Whichever peer
    // node 2 targets, the cluster is now degraded mid-rejoin -- the shape the
    // retry budget exists for.
    harness.restart_node_from_clean_slate(2).unwrap();
    harness.stop_node(1).unwrap();

    // It must still converge: either the surviving peer served it directly, or
    // it burned its budget against the dead one, abandoned, and came back
    // through repair. Both are correct; wedging is not.
    let deadline = Instant::now() + TRANSFER_BUDGET;
    loop {
        if harness
            .node(2)
            .stdout_contains("metadata state transfer installed")
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the rejoining node never converged within {TRANSFER_BUDGET:?}; a transfer \
             whose peer died must abandon after its retry budget and fall back to \
             journal repair, not retry the dead peer forever"
        );
        sleep(MARKER_POLL).await;
    }

    // Convergence IS the assertion here, deliberately with no follow-up commit.
    // With one node stopped and another mid-rejoin the cluster is momentarily
    // quorum-marginal (a transferring replica withholds `PrepareOk`), so a
    // client write can legitimately go unanswered for a while -- and
    // `resume_request`'s login helper treats an unanswered read as a verdict
    // rather than something to wait out, by design, since that is how the
    // sibling specs catch a silently dropped frame. Asserting commit throughput
    // through a degraded window would be testing something this case is not
    // about; the sibling tests already cover committing after a transfer.
}
