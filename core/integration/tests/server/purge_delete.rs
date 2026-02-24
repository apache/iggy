/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::server::scenarios::purge_delete_scenario;
use integration::iggy_harness;
use test_case::test_matrix;

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
#[test_matrix([restart_off(), restart_on()])]
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
