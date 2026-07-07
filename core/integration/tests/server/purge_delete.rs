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

#[iggy_harness(server(
    segment.size = "5KiB",
    segment.cache_indexes = ["all", "none", "open_segment"],
    partition.messages_required_to_save = "1",
    partition.enforce_fsync = "true",
))]
// The consumer polls in this scenario auto-commit offsets, so partition ops
// keep flowing while the restarted node rejoins. Ops prepared in that window
// commit on the surviving quorum and leave the pipeline before the rejoining
// replica can ack them, so its journal has a gap it cannot fill without
// message repair; when the commit frontier crosses the gap the replica
// correctly suicides ("replica is divergent"). Re-enable restart_on once
// message repair lands. The consumerless variant below exercises the
// restart + prune path without this window.
#[cfg_attr(not(feature = "vsr"), test_matrix([restart_off(), restart_on()]))]
#[cfg_attr(feature = "vsr", test_matrix([restart_off()]))]
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
#[test_matrix([restart_off(), restart_on()])]
async fn should_delete_segments_without_consumers(harness: &mut TestHarness, restart_server: bool) {
    purge_delete_scenario::run_no_consumers(harness, restart_server).await;
}

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

#[iggy_harness(server(
    segment.size = "5KiB",
    segment.cache_indexes = ["all", "none", "open_segment"],
    partition.messages_required_to_save = "1",
    partition.enforce_fsync = "true",
))]
// Consumer polls auto-commit offsets, so partition ops keep flowing while the
// restarted node rejoins. Ops committed by the surviving quorum in that window
// leave the pipeline before the rejoining replica can receive them; without
// state transfer its journal gap is unfillable and the replica correctly
// suicides ("replica is divergent") when the commit frontier crosses it.
// Re-enable restart_on once state transfer lands.
#[cfg_attr(not(feature = "vsr"), test_matrix([restart_off(), restart_on()]))]
#[cfg_attr(feature = "vsr", test_matrix([restart_off()]))]
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
// The scenario asserts the exact [0, 7, 14, 21] layout only on the legacy path;
// under vsr it verifies the framing-agnostic purge outcome (offsets cleared,
// files deleted, partition reset to a single segment at offset 0). restart_on
// is vsr-gated on the same rejoin-window state-transfer gap as
// `should_block_deletion_until_all_consumers_pass_segment` above (the old
// CG-reconnect `AlreadyAuthenticated` blocker is fixed by the SDK's
// fresh-session reconnect).
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

fn restart_on() -> bool {
    true
}
