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

//! Metadata-plane view durability across a real view change and process restarts,
//! over `iggy-server-ng`'s production superblock path.
//!
//! The superblock exists so a replica recovers a view it already acted in from its
//! OWN disk after a crash, instead of inferring a stale view from the WAL or
//! relearning it from a peer. That is the safety property stopping a recovered
//! replica from re-voting in an old view and splitting the log. This drives a genuine
//! metadata view change, crashing the view-0 primary so the survivors elect a new
//! one, then proves the elected view is durable: it lands in a survivor's on-disk
//! `VsrState` as `view` / `log_view >= 1` and survives that survivor's own process
//! restart. The stream committed before the crash comes back too, so metadata
//! op/commit recovery from the WAL rides along.
//!
//! The production counterpart to the simulator's
//! `given_advanced_view_when_metadata_replica_restarts_should_recover_view_from_superblock`,
//! which proves the same property over the in-memory `SimSuperblock`; this proves it
//! over the real `PingPongSuperblock` and `restore_metadata_consensus` the sim stubs
//! out.
//!
//! Deliberately metadata-only, a stream and no topic: a topic would create a
//! partition consensus group whose own view change is orthogonal to the plane under
//! test and would only add flakiness. Restarts keep a metadata quorum of 2 of 3 live
//! at every instant, bringing the crashed primary back before bouncing the survivor,
//! so recovery is exercised without wedging the cluster below quorum.
//!
//! vsr-only: a metadata view change has no analog on the single-process legacy
//! server, and the superblock is server-ng's durable consensus record.

use std::path::Path;
use std::time::{Duration, Instant};

use consensus::VsrState;
use iggy::prelude::*;
use integration::harness::{TestBinary, TestHarness};
use integration::iggy_harness;
use journal::superblock::{SLOT_FILE_NAMES, SuperblockContents, decode_slots};
use tokio::time::sleep;

const STREAM_NAME: &str = "view-durability-stream";

/// A view change plus a rejoin is not instant (primary timeout, SVC, DVC, StartView,
/// then the restarted node's probe and repair), and CI runners are slow. 60s bounds
/// the worst case without hanging the suite.
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_millis(250);

#[iggy_harness(cluster_nodes = 3, server(system.sharding.cpu_allocation = "0..1"))]
async fn given_advanced_metadata_view_when_survivor_restarts_should_recover_view_from_superblock(
    harness: &mut TestHarness,
) {
    // One shard per node keeps the metadata consensus on shard 0, the shard every
    // client connection lands on, so leadership shows up over the wire. Three
    // nodes give quorum 2, so the cluster keeps a quorum through a single crash.

    // Baseline: node 0 is the view-0 metadata primary (its replica id is 0, and
    // the primary of view 0 is replica 0). Commit a stream through it, so the
    // metadata group has committed state to recover later and exactly one leader
    // is visible.
    let client = harness
        .root_client_for_node(0)
        .await
        .expect("connect a root client to the node");
    client
        .create_stream(STREAM_NAME)
        .await
        .expect("create stream on the view-0 primary");
    assert_eq!(
        leader_count(&client).await,
        Some(1),
        "a healthy cluster must show exactly one metadata leader before the crash"
    );
    drop(client);
    // Let the committed prepare settle (fsync per entry) on the survivors before
    // the primary dies, so the stream is durable on a node that outlives it.
    sleep(Duration::from_secs(1)).await;

    // Force a metadata view change: crash the view-0 primary. Its silence trips
    // the survivors' primary timeout; they run SVC/DVC and elect a new primary at
    // view >= 1, each persisting the advanced view through the superblock gate
    // before it casts a view-scoped vote.
    harness
        .node_mut(0)
        .stop()
        .expect("crash the metadata primary (node 0)");

    // Wait for the advanced view to become DURABLE on a survivor's own disk. Node 2
    // has to take part in the election, since quorum needs both survivors, so its
    // superblock must reach view >= 1. Reading the durable record directly is the
    // race-free signal that the view change happened and was persisted, stronger than
    // a leadership poll, which could read the stale view-0 roster mid-election.
    let view_before = wait_for_advanced_view(harness, 2).await;
    assert!(
        view_before.view >= 1 && view_before.log_view >= 1,
        "a survivor that took part in the view change must persist view/log_view >= 1, \
         got {view_before:?}"
    );

    // Bring the crashed primary back so it rejoins from its own disk, via recover(),
    // the superblock, and a view probe. The two survivors still form a quorum while it
    // is down, so this never stalls; once back the cluster is 3/3, keeping a quorum
    // alive when the survivor is bounced next.
    harness
        .node_mut(0)
        .start()
        .expect("restart the crashed primary (node 0)");
    let rejoined = wait_for_advanced_view(harness, 0).await;
    assert!(
        rejoined.view >= view_before.view,
        "the rejoined primary must adopt the advanced view (>= {}), not resume the stale view 0, \
         got {rejoined:?}",
        view_before.view
    );
    // Let the 3/3 mesh settle so the survivor restart below keeps a live quorum.
    sleep(Duration::from_secs(1)).await;

    // Restart the survivor that advanced its view. It drops all in-memory
    // consensus state and must recover its advanced view from its own superblock
    // via `restore_metadata_consensus`, not reset to a fresh 0. The other two
    // nodes hold the quorum while it is down.
    harness
        .node_mut(2)
        .stop()
        .expect("stop the survivor (node 2)");
    harness
        .node_mut(2)
        .start()
        .expect("restart the survivor (node 2)");

    // The reformed cluster must serve again under exactly one leader, and the stream
    // committed before the view change must still resolve, proving the metadata
    // op/commit point and state machine recovered from disk alongside the view.
    wait_until_serving_with_single_leader(harness, &[0, 1, 2]).await;

    // The advanced view survived the survivor's own restart: re-read its superblock
    // once resettled and confirm it did not regress toward a fresh 0.
    let view_after = wait_for_advanced_view(harness, 2).await;
    assert!(
        view_after.view >= view_before.view && view_after.log_view >= view_before.log_view,
        "the durable view must survive node 2's restart without regressing: \
         before={view_before:?}, after={view_after:?}"
    );
}

/// Connect to the first node in `nodes` that accepts a connection, `None` when none
/// do (mid-election, or a node still restarting).
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

/// Number of nodes the metadata roster marks as leader, `None` if the query fails
/// (mid-election, connection dropped).
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

/// Decode a node's durable metadata `VsrState` from its on-disk superblock, `None` if
/// no record exists yet. Reads the two slot files with blocking I/O, since this is a
/// tokio test off any compio runtime, and decodes them through the journal's own
/// newest-verifying-wins selection, so the test sees exactly what
/// `PingPongSuperblock::read_latest` would.
///
/// # Panics
/// If a slot holds bytes that do not verify. No step here corrupts one, so that is a
/// real durability bug; returning `None` would let the pollers below read it as "not
/// written yet" and time out on a misleading message.
fn read_superblock_state(data_path: &Path) -> Option<VsrState> {
    // `<data_dir>/metadata/` is where shard 0 opens its `PingPongSuperblock`.
    let dir = data_path.join("metadata");
    let slot_a = std::fs::read(dir.join(SLOT_FILE_NAMES[0])).ok();
    let slot_b = std::fs::read(dir.join(SLOT_FILE_NAMES[1])).ok();
    match decode_slots(slot_a.as_deref(), slot_b.as_deref()) {
        SuperblockContents::Present(payload) => VsrState::try_from(payload.as_slice()).ok(),
        SuperblockContents::Empty => None,
        SuperblockContents::Unreadable { version } => panic!(
            "superblock at {} is unreadable (version {version:?}); no test step corrupts it",
            dir.display()
        ),
    }
}

/// Poll `node`'s superblock until it holds a settled advanced view, returning that
/// state. Panics on timeout.
///
/// The gate persists `view` the moment a replica advances it to VOTE, so a bare
/// `view >= 1` check races the election: it can observe the intermediate
/// `view = 1, log_view = 0` a backup writes before adopting the new primary's
/// `StartView`, which moves its head and sets `log_view = view`. Waiting for
/// `log_view >= 1`, which implies `view >= 1` since `log_view <= view`, is the settled
/// signal that the replica durably adopted the new view's log.
async fn wait_for_advanced_view(harness: &TestHarness, node: usize) -> VsrState {
    let data_path = harness.node(node).data_path();
    let deadline = Instant::now() + CONVERGE_TIMEOUT;
    loop {
        if let Some(state) = read_superblock_state(&data_path)
            && state.log_view >= 1
        {
            return state;
        }
        assert!(
            Instant::now() < deadline,
            "node {node} did not persist a settled advanced view (log_view >= 1) \
             within {CONVERGE_TIMEOUT:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// Poll until a node serves the pre-crash stream AND the roster shows exactly one
/// leader. Panics on timeout.
async fn wait_until_serving_with_single_leader(harness: &TestHarness, nodes: &[usize]) {
    let stream_id = Identifier::named(STREAM_NAME).unwrap();
    let deadline = Instant::now() + CONVERGE_TIMEOUT;
    loop {
        if let Some(client) = connect_any(harness, nodes).await
            && matches!(client.get_stream(&stream_id).await, Ok(Some(_)))
            && leader_count(&client).await == Some(1)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "cluster did not re-form (stream served, single leader) within {CONVERGE_TIMEOUT:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
}
