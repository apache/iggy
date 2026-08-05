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

//! Partition-plane view durability across a real view change and process restarts,
//! over the per-partition `PingPongSuperblock` path.
//!
//! The partition-plane sibling of `cluster_view_durability_vsr`: every partition
//! consensus group now records its `(view, log_view)` in a superblock inside its own
//! partition directory, and every view-scoped send for that group is gated on the
//! record being durable. This drives a genuine partition view change, crashing the
//! view-0 primary so the survivors elect a new one, then proves the elected view is
//! durable on the PARTITION group's own record: it lands in a survivor's on-disk
//! `VsrState` as `view` / `log_view >= 1` and survives that survivor's own process
//! restart. Messages committed before the crash still poll back at the end, so the
//! partition data path rides along.
//!
//! vsr-only: partition consensus groups and their superblocks exist only on
//! `iggy-server-ng`.

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, Instant};

use consensus::VsrState;
use iggy::prelude::*;
use integration::harness::{TestBinary, TestHarness};
use integration::iggy_harness;
use journal::superblock::{SLOT_FILE_NAMES, SuperblockContents, decode_slots};
use tokio::time::sleep;

const STREAM_NAME: &str = "partition-view-durability-stream";
const TOPIC_NAME: &str = "partition-view-durability-topic";
/// server-ng partition ids are 0-based (CreateTopic assigns them from 0).
const PARTITION_ID: u32 = 0;
const MESSAGES_COUNT: u32 = 10;

/// A view change plus a rejoin is not instant (primary timeout, SVC, DVC, StartView,
/// then the restarted node's probe and repair), and CI runners are slow. 60s bounds
/// the worst case without hanging the suite.
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(60);
/// Boot line reporting the `(view, log_view)` a partition group restored from its
/// superblock: the recovered replica's own account of what it read back.
const RESTORED_VIEW_MARKER: &str = "restored partition view from its superblock";
const POLL_INTERVAL: Duration = Duration::from_millis(250);

#[iggy_harness(cluster_nodes = 3, server(system.sharding.cpu_allocation = "0..1"))]
async fn given_advanced_partition_view_when_survivor_restarts_should_recover_view_from_superblock(
    harness: &mut TestHarness,
) {
    // Baseline: node 0 is the view-0 primary of every group (its replica id is 0).
    // Commit a topic with ONE partition and a batch of messages through it, so
    // exactly one partition consensus group exists and holds committed state.
    let client = connect(harness, 0).await;
    client
        .create_stream(STREAM_NAME)
        .await
        .expect("create stream on the view-0 primary");
    let stream_id = Identifier::named(STREAM_NAME).expect("stream identifier");
    client
        .create_topic(
            &stream_id,
            TOPIC_NAME,
            1,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .expect("create topic with one partition");
    let topic_id = Identifier::named(TOPIC_NAME).expect("topic identifier");
    let mut messages: Vec<IggyMessage> = (0..MESSAGES_COUNT)
        .map(|sequence| IggyMessage::from_str(&format!("message-{sequence}")).expect("message"))
        .collect();
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(PARTITION_ID),
            &mut messages,
        )
        .await
        .expect("send messages through the view-0 partition primary");
    assert_eq!(
        poll_all(&client).await.expect("poll on the view-0 primary"),
        MESSAGES_COUNT,
        "the pre-crash batch must commit before the view change"
    );
    drop(client);
    // Let the committed prepares settle on the survivors before the primary dies.
    sleep(Duration::from_secs(1)).await;

    // Force a partition view change: crash the view-0 primary. The survivors'
    // primary timeout trips, they run SVC/DVC for the partition group and elect a
    // new primary at view >= 1, each persisting the advanced view through the
    // per-partition superblock gate before it casts a view-scoped vote.
    harness
        .node_mut(0)
        .stop()
        .expect("crash the view-0 primary (node 0)");

    // Wait for the advanced PARTITION view to become durable on a survivor's own
    // disk. Node 2 has to take part in the election (quorum needs both survivors),
    // so its partition superblock must reach log_view >= 1. Reading the durable
    // record directly is the race-free signal, and `log_view >= 1` (not bare
    // `view >= 1`) is the settled form: the gate persists `view` the moment a
    // replica advances it to vote, before it adopts the new primary's log.
    let view_before = wait_for_advanced_partition_view(harness, 2).await;
    assert!(
        view_before.view >= 1 && view_before.log_view >= 1,
        "a survivor that took part in the partition view change must persist \
         view/log_view >= 1, got {view_before:?}"
    );

    // Bring the crashed primary back so it rejoins from its own disk: partition
    // recovery opens the superblock, restores its recorded view, and the boot
    // probe adopts the cluster's advanced view from there.
    harness
        .node_mut(0)
        .start()
        .expect("restart the crashed primary (node 0)");
    let rejoined = wait_for_advanced_partition_view(harness, 0).await;
    assert!(
        rejoined.view >= view_before.view,
        "the rejoined replica must adopt the advanced partition view (>= {}), not \
         resume the stale view 0, got {rejoined:?}",
        view_before.view
    );
    // Let the 3/3 mesh settle so the survivor restart below keeps a live quorum.
    sleep(Duration::from_secs(1)).await;

    // Restart the survivor that advanced its view. It drops all in-memory
    // consensus state and must recover the advanced partition view from its own
    // superblock, not reset to a fresh 0.
    harness
        .node_mut(2)
        .stop()
        .expect("stop the survivor (node 2)");
    harness
        .node_mut(2)
        .start()
        .expect("restart the survivor (node 2)");

    // NODE 2 specifically must serve the pre-crash batch: the helper returns on
    // the first node that answers, and nodes 0 and 1 were not restarted, so
    // asking the whole set would never contact the node under test.
    wait_until_partition_serves(harness, &[2]).await;

    // The recovered replica's own BEHAVIOR, not the file's contents: the
    // superblock is read only at boot and written only when the persist gate
    // fires, so re-reading it here would return the pre-restart record even if
    // recovery were broken and node 2 came back at view 0. Node 2's boot line
    // reports the view it actually restored, so it must name the recorded one.
    //
    // Skipped when the harness inherits the node's stdout instead of capturing
    // it (`IGGY_TEST_VERBOSE`): the log file is then empty, and asserting on it
    // would fail spuriously in exactly the mode someone debugging this would
    // use. The serve check above still ran.
    let log_captured = !harness.node(2).stdout_plain().is_empty();
    if !log_captured {
        eprintln!(
            "IGGY_TEST_VERBOSE inherits node stdout, so the restored-view oracle is \
             unavailable; skipping it"
        );
        return;
    }
    let restored = restored_partition_view(harness, 2)
        .expect("node 2 must log the partition view it restored from its superblock");
    assert!(
        restored.0 >= view_before.view && restored.1 >= view_before.log_view,
        "node 2 restored (view {}, log_view {}) but its superblock recorded {view_before:?}; \
         a replica that resumes below its recorded view can re-enter a view it already acted in",
        restored.0,
        restored.1
    );
}

/// `(view, log_view)` the node reports restoring at boot, parsed out of the
/// structured fields of its restore line. `None` while the line is absent.
///
/// The stdout log is truncated per process start, so a value read after a
/// restart was logged by the new process.
fn restored_partition_view(harness: &TestHarness, node: usize) -> Option<(u32, u32)> {
    let log = harness.node(node).stdout_plain();
    log.lines()
        .filter(|line| line.contains(RESTORED_VIEW_MARKER))
        .filter_map(|line| {
            // Leading space so the `view` key cannot match inside `log_view`.
            let view = field(line, " view=")?;
            let log_view = field(line, " log_view=")?;
            Some((view, log_view))
        })
        .max()
}

/// Value of a space-prefixed `key=<u32>` tracing field.
fn field(line: &str, key: &str) -> Option<u32> {
    let start = line.find(key)? + key.len();
    line[start..]
        .split(|character: char| !character.is_ascii_digit())
        .next()
        .and_then(|digits| digits.parse().ok())
}

/// Connect a root-authenticated TCP client to a specific node.
async fn connect(harness: &TestHarness, node: usize) -> IggyClient {
    harness
        .node(node)
        .tcp_client()
        .expect("tcp client builder")
        .with_root_login()
        .connect()
        .await
        .unwrap_or_else(|e| panic!("connect to node {node}: {e}"))
}

/// Poll the whole pre-crash batch from offset 0; `Ok(count)` of messages seen.
async fn poll_all(client: &IggyClient) -> Result<u32, IggyError> {
    let polled = client
        .poll_messages(
            &Identifier::named(STREAM_NAME).expect("stream identifier"),
            &Identifier::named(TOPIC_NAME).expect("topic identifier"),
            Some(PARTITION_ID),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            MESSAGES_COUNT,
            false,
        )
        .await?;
    Ok(polled.messages.len() as u32)
}

/// Find the single partition directory's superblock record on `node`, `None`
/// while no record exists yet (a partition group that never left view 0 has an
/// empty superblock, and its slot files may not exist at all).
///
/// Located by walking the node's data directory for `superblock.a`, skipping the
/// metadata plane's record, so the test does not hard-code the
/// `streams/<s>/topics/<t>/partitions/<p>` layout. Exactly one partition group
/// exists in this test.
///
/// # Panics
/// If a slot holds bytes that do not verify: no step here corrupts one, so that
/// is a real durability bug, and reading it as "not written yet" would time the
/// pollers out on a misleading message.
fn read_partition_superblock_state(data_path: &Path) -> Option<VsrState> {
    let dir = find_partition_superblock_dir(data_path)?;
    let slot_a = std::fs::read(dir.join(SLOT_FILE_NAMES[0])).ok();
    let slot_b = std::fs::read(dir.join(SLOT_FILE_NAMES[1])).ok();
    match decode_slots(slot_a.as_deref(), slot_b.as_deref()) {
        SuperblockContents::Present(payload) => VsrState::try_from(payload.as_slice()).ok(),
        SuperblockContents::Empty => None,
        SuperblockContents::Unreadable { version } => panic!(
            "partition superblock at {} is unreadable (version {version:?}); \
             no test step corrupts it",
            dir.display()
        ),
    }
}

fn find_partition_superblock_dir(root: &Path) -> Option<PathBuf> {
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        // The metadata plane keeps its own superblock under `<data>/metadata`;
        // only partition records are under test here.
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

/// Poll `node`'s partition superblock until it holds a settled advanced view
/// (`log_view >= 1`, which implies `view >= 1`). Panics on timeout.
async fn wait_for_advanced_partition_view(harness: &TestHarness, node: usize) -> VsrState {
    let data_path = harness.node(node).data_path();
    let deadline = Instant::now() + CONVERGE_TIMEOUT;
    loop {
        if let Some(state) = read_partition_superblock_state(&data_path)
            && state.log_view >= 1
        {
            return state;
        }
        assert!(
            Instant::now() < deadline,
            "node {node} did not persist a settled advanced partition view \
             (log_view >= 1) within {CONVERGE_TIMEOUT:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// Poll until some node serves the whole pre-crash batch. Panics on timeout.
async fn wait_until_partition_serves(harness: &TestHarness, nodes: &[usize]) {
    let deadline = Instant::now() + CONVERGE_TIMEOUT;
    loop {
        for &node in nodes {
            if let Ok(builder) = harness.node(node).tcp_client()
                && let Ok(client) = builder.with_root_login().connect().await
                && poll_all(&client).await == Ok(MESSAGES_COUNT)
            {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "no node served the {MESSAGES_COUNT} pre-crash messages within \
             {CONVERGE_TIMEOUT:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
}
