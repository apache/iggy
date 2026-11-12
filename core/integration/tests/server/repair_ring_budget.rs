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

//! The repair ring budget of a running server. Boot splits
//! `partition.evicted_ring_node_bytes_max` across the shards, and the tick of
//! every shard sheds against its share and reports it.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use integration::harness::{TestHarness, TestServerConfig};
use tokio::time::sleep;

use super::http_client::HttpClient;

/// Not the shipped 4 GiB, so the shares show that boot read the key.
const NODE_BUDGET: &str = "1 GiB";
const NODE_BUDGET_BYTES: u64 = 1024 * 1024 * 1024;
/// One sample per shard. A shard without a ring budget never sets its own.
const SHARE_GAUGE: &str = "partition_repair_ring_budget_bytes{";
const REPORT_DEADLINE: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(100);

#[tokio::test]
#[serial_test::parallel]
async fn given_a_node_ring_budget_when_the_shards_tick_should_report_an_equal_share_each() {
    let mut harness = TestHarness::builder()
        .server(
            TestServerConfig::builder()
                .extra_envs(HashMap::from([(
                    "IGGY_PARTITION_EVICTED_RING_NODE_BYTES_MAX".to_string(),
                    NODE_BUDGET.to_string(),
                )]))
                .build(),
        )
        .cluster_nodes(1)
        .build()
        .expect("build harness");
    harness.start().await.expect("start server");
    let http = HttpClient::login_root(&harness).await;

    let deadline = Instant::now() + REPORT_DEADLINE;
    loop {
        let shares = reported_shares(&http).await;
        let equal_share = NODE_BUDGET_BYTES.checked_div(shares.len() as u64);
        if shares.iter().all(|&share| Some(share) == equal_share) && !shares.is_empty() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "within {REPORT_DEADLINE:?} every shard must report an equal share of \
             {NODE_BUDGET_BYTES} bytes, last shares: {shares:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// The ring budget share that each shard reports on `/metrics`.
async fn reported_shares(http: &HttpClient) -> Vec<u64> {
    let exposition = http
        .get("/metrics")
        .await
        .text()
        .await
        .expect("metrics text");
    exposition
        .lines()
        .filter(|line| line.starts_with(SHARE_GAUGE))
        .map(|line| {
            line.rsplit(' ')
                .next()
                .and_then(|value| value.parse().ok())
                .unwrap_or_else(|| panic!("unreadable sample: {line}"))
        })
        .collect()
}
