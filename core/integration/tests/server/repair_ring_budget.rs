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
//! `partition.journal_memory_node_bytes_max` across the shards that own
//! partitions, and the tick of each of them sheds against that share and
//! reports it.

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
/// Two shards own partitions when shard 0 is skipped. One shard would refuse
/// to start with the skip on.
const SKIPPING_SHARDS: &str = "3";
const REPORT_DEADLINE: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(100);

#[tokio::test]
#[serial_test::parallel]
async fn given_a_node_ring_budget_when_the_shards_tick_should_report_an_equal_share_each() {
    assert_every_owner_reports_an_equal_share(false).await;
}

#[tokio::test]
#[serial_test::parallel]
async fn given_partitions_off_shard_zero_when_the_shards_tick_should_split_the_budget_between_the_others()
 {
    assert_every_owner_reports_an_equal_share(true).await;
}

/// A shard that owns no partition takes no share, so its sample stays 0 and
/// the samples add up to the node budget.
async fn assert_every_owner_reports_an_equal_share(skip_shard_zero: bool) {
    let mut envs = HashMap::from([(
        "IGGY_PARTITION_JOURNAL_MEMORY_NODE_BYTES_MAX".to_string(),
        NODE_BUDGET.to_string(),
    )]);
    if skip_shard_zero {
        envs.extend([
            (
                "IGGY_CLUSTER_COORDINATOR_SKIP_SHARD_ZERO_FOR_PARTITIONS".to_string(),
                "true".to_string(),
            ),
            (
                "IGGY_SHARDING_CPU_ALLOCATION".to_string(),
                SKIPPING_SHARDS.to_string(),
            ),
        ]);
    }
    let mut harness = TestHarness::builder()
        .server(TestServerConfig::builder().extra_envs(envs).build())
        .cluster_nodes(1)
        .build()
        .expect("build harness");
    harness.start().await.expect("start server");
    let http = HttpClient::login_root(&harness).await;

    let deadline = Instant::now() + REPORT_DEADLINE;
    loop {
        let shares = reported_shares(&http).await;
        // The exposition lists the shards in order, so shard 0 comes first.
        let first_owner = usize::from(skip_shard_zero).min(shares.len());
        let (without_partitions, owners) = shares.split_at(first_owner);
        let owning_shards = owners.len() as u64;
        let equal_share = NODE_BUDGET_BYTES.checked_div(owning_shards);
        if owners.iter().all(|&share| Some(share) == equal_share)
            && without_partitions.iter().all(|&share| share == 0)
            && !owners.is_empty()
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "within {REPORT_DEADLINE:?} each of {owning_shards} partition shards must \
             report its share of {NODE_BUDGET_BYTES} bytes and no other shard a share, \
             last shares: {shares:?}"
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
