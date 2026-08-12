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

//! Checkpoint-shaped edge cases for metadata + client-table state transfer,
//! metadata-plane commands only (`CreateStream`). The 3-node cases drive the
//! raw VSR wire; the solo case at the bottom uses the regular TCP client.
//!
//! `metadata_state_transfer` pins the base case: one checkpoint, a live
//! post-checkpoint tail, one restart. This file covers the shapes around it:
//!
//! - restart exactly AT a checkpoint (snapshot present, post-snapshot journal
//!   empty), so the transfer installs snapshot-only and the tail repair has
//!   nothing to fetch;
//! - restart after MULTIPLE checkpoint generations (`snapshot.bin` was
//!   rewritten in place; only the latest generation may be served);
//! - a below-floor dedup retry after the restart, answered byte-identically
//!   from the TRANSFERRED reply ring - the WAL entry that produced the reply
//!   was drained on every node, so nothing but the shipped table can answer;
//! - a second restart of the same node (the installed snapshot must persist
//!   and serve as the local floor for another transfer round);
//! - a SOLO replica restarting after its own checkpoint: no transfer in the
//!   picture, so recovery has to fold the local snapshot back in as the floor
//!   and replay the WAL suffix on top.
//!
//! Checkpoint placement in the 3-node cases is config-driven: with
//! `metadata.journal_slots = 256` and the built-in checkpoint margin (64), a
//! checkpoint fires when the journal holds 192 committed ops, i.e. at op 192,
//! 384, ... The register commits at op 1 and request K at op K + 1, so request
//! 191 lands checkpoint one and request 383 lands checkpoint two. Requests
//! here are sequential with one in flight, which keeps that arithmetic exact;
//! the `forced checkpoint completed` markers below pin it rather than trust
//! it.

#![cfg(feature = "vsr")]

use super::client_table_restart::{
    commit_request, create_stream_payload, register, resume_request, tcp_addr, tcp_addrs,
};
use iggy::prelude::*;
use integration::harness::{TestBinary, TestHarness};
use integration::iggy_harness;
use std::time::Duration;
use tokio::time::{Instant, sleep};

/// With `journal_slots = 256` and the margin floor (64), a checkpoint fires
/// every 192 committed ops.
const CHECKPOINT_EVERY: u64 = 192;

/// Requests that land the commit log exactly on the first checkpoint:
/// register = op 1, request K = op K + 1.
const REQUESTS_TO_FIRST_CHECKPOINT: u64 = CHECKPOINT_EVERY - 1;

/// How long a node gets to probe, fetch, and install a transfer (or a
/// checkpoint marker gets to appear).
const MARKER_BUDGET: Duration = Duration::from_secs(30);

const MARKER_POLL: Duration = Duration::from_millis(200);

const INSTALL_MARKER: &str = "metadata state transfer installed";

/// Poll until `node`'s stdout contains `marker` `at_least` times.
async fn await_marker(
    harness: &TestHarness,
    node: usize,
    marker: &str,
    at_least: usize,
    context: &str,
) {
    let deadline = Instant::now() + MARKER_BUDGET;
    loop {
        if harness.node(node).stdout_occurrences(marker) >= at_least {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{context}: node {node} never logged {at_least}x {marker:?} within {MARKER_BUDGET:?}"
        );
        sleep(MARKER_POLL).await;
    }
}

/// Wait until EVERY node has completed its `generation`-th checkpoint. The
/// restart below must not race a lagging backup's checkpoint: the serving
/// primary needs its snapshot on disk to offer a transfer, and the restarted
/// node's own drained journal is the precondition the test is about.
///
/// Generations are counted per node, not matched by op: the trigger fires on
/// journal occupancy but each node snapshots at its own applied watermark,
/// so a backup checkpoints one op behind the primary (191 vs 192 for the
/// first generation here).
async fn await_checkpoint_on_all_nodes(harness: &TestHarness, generation: usize) {
    for node in 0..3 {
        await_marker(
            harness,
            node,
            "forced checkpoint completed",
            generation,
            "checkpoint wait",
        )
        .await;
    }
}

// Snapshot present, post-snapshot journal EMPTY: the restart lands exactly on
// a checkpoint, so the transfer descriptor's `commit_op == snapshot_seq` and
// the post-install tail repair has nothing to fetch (`commit_min ==
// commit_max` skips it). The below-floor retry then proves the reply ring
// rode the transferred table: request 191's reply was minted at op 192, which
// every node drained out of its WAL at that same checkpoint.
#[iggy_harness(cluster_nodes = 3, server(metadata.journal_slots = "256"))]
async fn given_drained_journal_when_node_restarts_should_install_snapshot_only(
    harness: &mut TestHarness,
) {
    let addr = tcp_addr(harness);
    let (mut stream, session) = register(addr).await;
    let mut last_committed = None;
    for request in 1..=REQUESTS_TO_FIRST_CHECKPOINT {
        let committed = commit_request(
            &mut stream,
            session,
            request,
            &create_stream_payload(&format!("iggy-ckpt-{request}")),
        )
        .await;
        last_committed = Some(committed);
    }
    await_checkpoint_on_all_nodes(harness, 1).await;

    // Restart BEFORE dropping the socket: a graceful disconnect commits a
    // Logout that releases the entry cluster-wide (see
    // `submit_disconnect_logout`), and this test needs the entry to survive.
    // A crash takes the process down with the connection open, which is
    // exactly what restarting first models (same ordering as the sibling
    // `client_table_restart` tests).
    harness.restart_server().await.unwrap();
    drop(stream);
    await_marker(harness, 0, INSTALL_MARKER, 1, "snapshot-only install").await;

    // Below-floor dedup: the retried request's reply exists nowhere but the
    // transferred table (its WAL entry was drained cluster-wide), and the
    // replay must be the original bytes, not a re-execution.
    let addrs = tcp_addrs(harness);
    resume_request(
        &addrs,
        session,
        REQUESTS_TO_FIRST_CHECKPOINT,
        &create_stream_payload(&format!("iggy-ckpt-{REQUESTS_TO_FIRST_CHECKPOINT}")),
        Some(last_committed.as_ref().expect("at least one commit")),
    )
    .await;

    // And the cluster still commits fresh work.
    resume_request(
        &addrs,
        session,
        REQUESTS_TO_FIRST_CHECKPOINT + 1,
        &create_stream_payload("iggy-ckpt-continuation"),
        None,
    )
    .await;
}

// Multiple snapshot generations before the restart: `snapshot.bin` is
// rewritten in place at each checkpoint, so the transfer must serve the
// LATEST generation, and the session registered below the FIRST floor (op 1,
// two generations deep) must still resume from the transferred table.
#[iggy_harness(cluster_nodes = 3, server(metadata.journal_slots = "256"))]
async fn given_multiple_checkpoints_when_node_restarts_should_resume_below_first_floor(
    harness: &mut TestHarness,
) {
    // Two full checkpoint rounds plus a small live tail.
    const TAIL: u64 = 20;
    const TOTAL_REQUESTS: u64 = 2 * CHECKPOINT_EVERY - 1 + TAIL;

    let addr = tcp_addr(harness);
    let (mut stream, session) = register(addr).await;
    let mut last_committed = None;
    for request in 1..=TOTAL_REQUESTS {
        let committed = commit_request(
            &mut stream,
            session,
            request,
            &create_stream_payload(&format!("iggy-multi-{request}")),
        )
        .await;
        last_committed = Some(committed);
    }
    // Both generations must exist before the restart; the second marker is
    // the proof the first snapshot was superseded.
    await_checkpoint_on_all_nodes(harness, 2).await;

    // Crash ordering: restart before the socket drops, see the sibling test.
    harness.restart_server().await.unwrap();
    drop(stream);
    await_marker(harness, 0, INSTALL_MARKER, 1, "multi-generation install").await;

    let addrs = tcp_addrs(harness);
    // The retry dedups against the latest generation's table (its reply is
    // in the shipped ring), and the register behind it sits below BOTH
    // snapshot floors.
    resume_request(
        &addrs,
        session,
        TOTAL_REQUESTS,
        &create_stream_payload(&format!("iggy-multi-{TOTAL_REQUESTS}")),
        Some(last_committed.as_ref().expect("at least one commit")),
    )
    .await;
    resume_request(
        &addrs,
        session,
        TOTAL_REQUESTS + 1,
        &create_stream_payload("iggy-multi-continuation"),
        None,
    )
    .await;
}

// A second restart of the same node: the first install persisted the
// transferred snapshot to the node's own disk, so the second boot recovers
// from it locally, arms another transfer, and installs again. Pins the
// install's persist-FIRST ordering - if the snapshot never hit disk, the
// second recovery would fall back to an empty STM whose journal floor
// disagrees with the cluster.
#[iggy_harness(cluster_nodes = 3, server(metadata.journal_slots = "256"))]
async fn given_installed_snapshot_when_node_restarts_again_should_transfer_again(
    harness: &mut TestHarness,
) {
    let addr = tcp_addr(harness);
    let (mut stream, session) = register(addr).await;
    for request in 1..=REQUESTS_TO_FIRST_CHECKPOINT {
        commit_request(
            &mut stream,
            session,
            request,
            &create_stream_payload(&format!("iggy-again-{request}")),
        )
        .await;
    }
    await_checkpoint_on_all_nodes(harness, 1).await;

    // Crash ordering: restart before the socket drops, see the sibling test.
    harness.restart_server().await.unwrap();
    drop(stream);
    await_marker(harness, 0, INSTALL_MARKER, 1, "first install").await;
    let addrs = tcp_addrs(harness);
    resume_request(
        &addrs,
        session,
        REQUESTS_TO_FIRST_CHECKPOINT + 1,
        &create_stream_payload("iggy-again-between-restarts"),
        None,
    )
    .await;

    // The stdout log is truncated per process run, so a marker seen after
    // this restart was logged by the freshly booted process.
    harness.restart_server().await.unwrap();
    await_marker(harness, 0, INSTALL_MARKER, 1, "second install").await;
    resume_request(
        &addrs,
        session,
        REQUESTS_TO_FIRST_CHECKPOINT + 2,
        &create_stream_payload("iggy-again-after-second-restart"),
        None,
    )
    .await;
}

/// The prepare WAL has `SLOT_COUNT = 1024` slots and the coordinator checkpoints at
/// `<= CHECKPOINT_MARGIN` (64) remaining, so after ~960 uncheckpointed ops. 1024
/// stream creates clears that with room for a WAL suffix above the checkpoint, and
/// stays under the 4096 stream namespace cap.
const STREAMS: u32 = 1024;

const RECOVER_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_millis(200);

fn stream_name(index: u32) -> String {
    format!("ckpt-stream-{index}")
}

/// Connect a root-authenticated TCP client to the solo node.
async fn connect(harness: &TestHarness) -> IggyClient {
    harness
        .node(0)
        .tcp_client()
        .expect("tcp client builder")
        .with_root_login()
        .connect()
        .await
        .expect("connect to the solo node")
}

/// Poll the solo node until it is back up and serving `stream`, returning the connected
/// client. Panics on timeout.
async fn wait_for_stream(harness: &TestHarness, stream: &str) -> IggyClient {
    let stream_id = Identifier::named(stream).unwrap();
    let deadline = Instant::now() + RECOVER_TIMEOUT;
    loop {
        if let Ok(builder) = harness.node(0).tcp_client()
            && let Ok(client) = builder.with_root_login().connect().await
            && matches!(client.get_stream(&stream_id).await, Ok(Some(_)))
        {
            return client;
        }
        assert!(
            Instant::now() < deadline,
            "solo node did not recover and serve {stream} within {RECOVER_TIMEOUT:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

// Metadata checkpoint-fold recovery across a solo restart, over
// `iggy-server-ng`'s production snapshot and WAL path.
//
// Between checkpoints a replica recovers its metadata by replaying the WAL. Once the
// WAL fills, the `SnapshotCoordinator` checkpoints: it persists `snapshot.bin`, pairs
// it in the superblock, and DRAINS the snapshotted prefix from the WAL. A restart
// after that must fold the snapshot back in as the recovery floor and replay only the
// committed suffix on top, rather than rely on a full WAL replay, which no longer
// holds the drained ops. This drives a real checkpoint by pushing the metadata WAL
// past `CHECKPOINT_MARGIN`, restarts the process, and asserts that a stream from the
// drained prefix, recoverable only from the snapshot, and one from the WAL suffix
// both survive.
//
// Solo on purpose: 1-of-1 quorum commits every op the instant it is journaled, so
// bulk creation is fast and the WAL is fully committed with no uncommitted suffix to
// reconcile, exercising checkpoint and snapshot-fold recovery in isolation without an
// election in the mix.
#[iggy_harness(cluster_nodes = 1, server(system.sharding.cpu_allocation = "0..1"))]
async fn given_checkpointed_metadata_when_solo_replica_restarts_should_recover_from_snapshot_and_wal(
    harness: &mut TestHarness,
) {
    let client = connect(harness).await;
    for index in 0..STREAMS {
        client
            .create_stream(&stream_name(index))
            .await
            .unwrap_or_else(|e| panic!("create stream {index}: {e}"));
    }
    drop(client);

    // Crossing CHECKPOINT_MARGIN must have driven the coordinator to persist a snapshot
    // and drain the WAL prefix behind it.
    let snapshot_path = harness
        .node(0)
        .data_path()
        .join("metadata")
        .join("snapshot.bin");
    let snapshot_len = std::fs::metadata(&snapshot_path).map(|m| m.len()).ok();
    assert!(
        snapshot_len.is_some_and(|len| len > 0),
        "{STREAMS} committed metadata ops must cross CHECKPOINT_MARGIN and persist a \
         non-empty snapshot at {}, got {snapshot_len:?}",
        snapshot_path.display()
    );

    // Restart the solo node: recovery loads the snapshot, holding the drained prefix,
    // and replays the committed WAL suffix on top.
    harness.node_mut(0).stop().expect("stop the solo node");
    harness.node_mut(0).start().expect("restart the solo node");

    // The first stream sits far below the checkpoint op, so it was drained from the WAL
    // and can come back only from the snapshot; the last stream is in the WAL suffix
    // above the checkpoint. Both surviving proves snapshot-fold plus suffix recovery,
    // not a bare WAL replay.
    let client = wait_for_stream(harness, &stream_name(0)).await;
    for name in [stream_name(0), stream_name(STREAMS - 1)] {
        assert!(
            client
                .get_stream(&Identifier::named(&name).unwrap())
                .await
                .expect("get stream after restart")
                .is_some(),
            "stream {name} must survive the checkpointed restart"
        );
    }
}
