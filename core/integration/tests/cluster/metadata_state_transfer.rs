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
}
