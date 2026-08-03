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

//! Metadata checkpoint-fold recovery across a solo restart, over
//! `iggy-server-ng`'s production snapshot and WAL path.
//!
//! Between checkpoints a replica recovers its metadata by replaying the WAL. Once the
//! WAL fills, the `SnapshotCoordinator` checkpoints: it persists `snapshot.bin`, pairs
//! it in the superblock, and DRAINS the snapshotted prefix from the WAL. A restart
//! after that must fold the snapshot back in as the recovery floor and replay only the
//! committed suffix on top, rather than rely on a full WAL replay, which no longer
//! holds the drained ops. This drives a real checkpoint by pushing the metadata WAL
//! past `CHECKPOINT_MARGIN`, restarts the process, and asserts that a stream from the
//! drained prefix, recoverable only from the snapshot, and one from the WAL suffix
//! both survive.
//!
//! Solo on purpose: 1-of-1 quorum commits every op the instant it is journaled, so
//! bulk creation is fast and the WAL is fully committed with no uncommitted suffix to
//! reconcile, exercising checkpoint and snapshot-fold recovery in isolation without an
//! election in the mix.
//!
//! vsr-only: the metadata snapshot/superblock checkpoint pairing is server-ng's.

use std::time::{Duration, Instant};

use iggy::prelude::*;
use integration::harness::{TestBinary, TestHarness};
use integration::iggy_harness;
use tokio::time::sleep;

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
