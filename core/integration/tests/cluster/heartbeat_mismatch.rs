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

//! Nodes that differ in a heartbeat value refuse to link.

use integration::iggy_harness;
use std::time::Duration;
use tokio::time::{Instant, sleep};

const OPTION: &str = "cluster.commit_idle_heartbeat_timeout";
const REFUSAL_BUDGET: Duration = Duration::from_secs(30);

#[iggy_harness(cluster_nodes = 3, manual_start)]
async fn given_a_node_with_another_heartbeat_value_when_peers_connect_should_name_the_option(
    harness: &mut TestHarness,
) {
    harness
        .node_mut(2)
        .add_env("IGGY_CLUSTER_COMMIT_IDLE_HEARTBEAT_TIMEOUT", "20s");
    for node in 0..3 {
        harness.start_node(node).expect("start the node");
    }

    let deadline = Instant::now() + REFUSAL_BUDGET;
    loop {
        let named = (0..3).any(|node| {
            let log = harness.node(node).stdout_plain();
            log.contains("rejecting replica link") && log.contains(OPTION)
        });
        if named {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "no node logged a refused link that names {OPTION}"
        );
        sleep(Duration::from_millis(250)).await;
    }
}
