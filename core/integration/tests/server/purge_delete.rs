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

use crate::server::scenarios::purge_delete_scenario;
use integration::iggy_harness;
use test_case::test_matrix;

// Legacy-only: asserts the exact on-disk layout ([0, 7, 14, 21], 7 messages per
// 5KiB segment) and per-file byte sizes throughout. server-ng's per-message
// framing yields a different layout, so the scenario's offset anchors do not
// hold under vsr. Port to a framing-agnostic form to re-enable.
#[cfg(not(feature = "vsr"))]
#[iggy_harness(server(
    segment.size = "5KiB",
    segment.cache_indexes = ["all", "none", "open_segment"],
    partition.messages_required_to_save = "1",
    partition.enforce_fsync = "true",
))]
#[test_matrix([restart_off(), restart_on()])]
async fn should_delete_segments_and_validate_filesystem(
    harness: &mut TestHarness,
    restart_server: bool,
) {
    purge_delete_scenario::run(harness, restart_server).await;
}

#[iggy_harness(server(
    segment.size = "5KiB",
    segment.cache_indexes = ["all", "none", "open_segment"],
    partition.messages_required_to_save = "1",
    partition.enforce_fsync = "true",
))]
// server-ng restarts the cluster on restart_on, which currently fails in
// bootstrap independently of segment deletion: an `Index data must be exactly
// 16 bytes` recovery panic (cache_indexes all/none) and a replica-port rebind
// race. Run only the no-restart matrix under vsr until those are fixed.
#[cfg_attr(not(feature = "vsr"), test_matrix([restart_off(), restart_on()]))]
#[cfg_attr(feature = "vsr", test_matrix([restart_off()]))]
async fn should_delete_segments_without_consumers(harness: &mut TestHarness, restart_server: bool) {
    purge_delete_scenario::run_no_consumers(harness, restart_server).await;
}

// vsr-gated: asserts a segment is freed *exactly* when the polled offset
// reaches its end — a synchronous-commit assumption. Under vsr the
// consumer-group auto-commit lags the poll, so the per-message timing does not
// hold (the barrier itself is correct: the server retains un-consumed
// segments, verified by `min_committed_offset` + its runtime diag). Legacy-only
// until redesigned to sync on the committed offset for async commit.
#[cfg(not(feature = "vsr"))]
#[iggy_harness(server(
    segment.size = "5KiB",
    segment.cache_indexes = ["all", "none", "open_segment"],
    partition.messages_required_to_save = "1",
    partition.enforce_fsync = "true",
))]
async fn should_delete_segments_with_consumer_group_barrier(harness: &TestHarness) {
    let client = harness.tcp_root_client().await.unwrap();
    let data_path = harness.server().data_path();

    purge_delete_scenario::run_consumer_group_barrier(&client, &data_path).await;
}

// Legacy-only: pins the [0, 7, 14, 21] layout across a multi-consumer barrier.
// server-ng framing differs; port to framing-agnostic to re-enable.
#[cfg(not(feature = "vsr"))]
#[iggy_harness(server(
    segment.size = "5KiB",
    segment.cache_indexes = ["all", "none", "open_segment"],
    partition.messages_required_to_save = "1",
    partition.enforce_fsync = "true",
))]
#[test_matrix([restart_off(), restart_on()])]
async fn should_block_deletion_until_all_consumers_pass_segment(
    harness: &mut TestHarness,
    restart_server: bool,
) {
    purge_delete_scenario::run_multi_consumer_barrier(harness, restart_server).await;
}

#[iggy_harness(server(
    segment.size = "5KiB",
    segment.cache_indexes = ["all", "none", "open_segment"],
    partition.messages_required_to_save = "1",
    partition.enforce_fsync = "true",
))]
// restart_on is vsr-gated: cluster restart hits a pre-existing server-ng
// bootstrap failure (legacy 16-byte index reader vs server-ng 24-byte writer),
// unrelated to purge. The scenario asserts the exact [0, 7, 14, 21] layout only
// on the legacy path; under vsr it verifies the framing-agnostic purge outcome
// (offsets cleared, files deleted, partition reset to a single segment at 0).
#[cfg_attr(not(feature = "vsr"), test_matrix([restart_off(), restart_on()]))]
#[cfg_attr(feature = "vsr", test_matrix([restart_off()]))]
async fn should_purge_topic_and_clear_consumer_offsets(
    harness: &mut TestHarness,
    restart_server: bool,
) {
    purge_delete_scenario::run_purge_topic(harness, restart_server).await;
}

fn restart_off() -> bool {
    false
}

#[cfg(not(feature = "vsr"))]
fn restart_on() -> bool {
    true
}
