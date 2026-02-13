/* Licensed to the Apache Software Foundation (ASF) under one
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

use super::{LaneSpec, NamespacePrefixes, Scenario, VerifyContext};
use crate::client::create_stale_client;
use crate::eventually::{await_members_count, await_rebalance_complete};
use crate::invariants::InvariantViolation;
use crate::safe_name::SafeResourceName;
use async_trait::async_trait;
use iggy::prelude::*;
use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;
use tokio::task::JoinSet;
use tracing::info;

const PARTITION_COUNT: u32 = 3;
const STALE_MEMBER_COUNT: u32 = 3;
const FRESH_MEMBER_COUNT: u32 = 3;
const SENTINEL_PAYLOAD: &str = "stale-consumers-sentinel";
/// Generous timeout for heartbeat eviction (server interval * 1.2 + propagation).
const EVICTION_TIMEOUT: Duration = Duration::from_secs(30);
const REBALANCE_TIMEOUT: Duration = Duration::from_secs(10);

const STREAM_SUFFIX: &str = "stale-cg-stream";
const TOPIC_SUFFIX: &str = "stale-cg-topic";
const GROUP_SUFFIX: &str = "stale-cg-group";

pub struct StaleConsumers;

impl StaleConsumers {
    fn stream_name(prefixes: &NamespacePrefixes) -> SafeResourceName {
        SafeResourceName::new(&prefixes.stable, STREAM_SUFFIX)
    }

    fn topic_name(prefixes: &NamespacePrefixes) -> SafeResourceName {
        SafeResourceName::new(&prefixes.base, TOPIC_SUFFIX)
    }

    fn group_name(prefixes: &NamespacePrefixes) -> SafeResourceName {
        SafeResourceName::new(&prefixes.base, GROUP_SUFFIX)
    }
}

#[async_trait]
impl Scenario for StaleConsumers {
    fn name(&self) -> &'static str {
        "stale-consumers"
    }

    fn describe(&self) -> &'static str {
        "Consumer group rebalancing after heartbeat eviction.\n\n\
         Phase 1: Setup — creates 1 stream, 1 topic (3 partitions), 1 consumer group,\n\
         and produces a single sentinel message to partition 1.\n\
         Phase 2: No chaos lanes (skip with --duration 1s or --ops 0).\n\
         Phase 3: Verify — joins stale members (1h heartbeat), lets them poll\n\
         without committing offsets (exercising the fetched-but-uncommitted path),\n\
         waits for server eviction, joins fresh members, polls concurrently, and asserts:\n\
         - stale members were evicted (members_count drops to 0)\n\
         - rebalance assigns all partitions with no duplicates\n\
         - exactly one fresh member receives the sentinel message\n\n\
         Requires: server with heartbeat.enabled=true and a short heartbeat.interval (e.g. 2s)."
    }

    async fn setup(
        &self,
        client: &IggyClient,
        prefixes: &NamespacePrefixes,
    ) -> Result<(), IggyError> {
        let stream_name = Self::stream_name(prefixes);
        let topic_name = Self::topic_name(prefixes);
        let group_name = Self::group_name(prefixes);

        info!(
            "Setup: creating stream, topic ({PARTITION_COUNT} partitions), consumer group, sentinel message"
        );

        client.create_stream(&stream_name).await?;
        let stream_id = Identifier::from_str_value(&stream_name).unwrap();

        client
            .create_topic(
                &stream_id,
                &topic_name,
                PARTITION_COUNT,
                CompressionAlgorithm::None,
                None,
                IggyExpiry::NeverExpire,
                MaxTopicSize::Unlimited,
            )
            .await?;

        let topic_id = Identifier::from_str_value(&topic_name).unwrap();
        client
            .create_consumer_group(&stream_id, &topic_id, &group_name)
            .await?;

        let sentinel = IggyMessage::from_str(SENTINEL_PAYLOAD).unwrap();
        let mut messages = vec![sentinel];
        client
            .send_messages(
                &stream_id,
                &topic_id,
                &Partitioning::partition_id(1),
                &mut messages,
            )
            .await?;

        Ok(())
    }

    fn lanes(&self) -> Vec<LaneSpec> {
        vec![]
    }

    async fn verify(&self, ctx: &VerifyContext<'_>) -> Vec<InvariantViolation> {
        match run_verification(ctx).await {
            Ok(violations) => violations,
            Err(v) => vec![v],
        }
    }
}

async fn run_verification(
    ctx: &VerifyContext<'_>,
) -> Result<Vec<InvariantViolation>, InvariantViolation> {
    let stream_name = StaleConsumers::stream_name(ctx.prefixes);
    let topic_name = StaleConsumers::topic_name(ctx.prefixes);
    let group_name = StaleConsumers::group_name(ctx.prefixes);

    let stream_id = Identifier::from_str_value(&stream_name).unwrap();
    let topic_id = Identifier::from_str_value(&topic_name).unwrap();
    let group_id = Identifier::from_str_value(&group_name).unwrap();

    let mut violations = Vec::new();

    // --- Step 1: Join stale members ---
    info!("Verify: joining {STALE_MEMBER_COUNT} stale members (1h heartbeat)");
    let mut stale_clients = Vec::with_capacity(STALE_MEMBER_COUNT as usize);
    for i in 0..STALE_MEMBER_COUNT {
        let client =
            create_stale_client(ctx.server_address)
                .await
                .map_err(|e| InvariantViolation {
                    kind: "stale_client_create_failed",
                    description: format!("Failed to create stale client #{i}: {e}"),
                    context: String::new(),
                })?;
        client
            .join_consumer_group(&stream_id, &topic_id, &group_id)
            .await
            .map_err(|e| InvariantViolation {
                kind: "stale_join_failed",
                description: format!("Stale client #{i} failed to join group: {e}"),
                context: String::new(),
            })?;
        stale_clients.push(client);
    }

    // --- Step 2: Confirm stale members joined ---
    await_members_count(
        ctx.client,
        &stream_id,
        &topic_id,
        &group_id,
        STALE_MEMBER_COUNT,
        EVICTION_TIMEOUT,
    )
    .await
    .map_err(|e| InvariantViolation {
        kind: "members_never_joined",
        description: format!("Expected {STALE_MEMBER_COUNT} members after join, timed out: {e}"),
        context: String::new(),
    })?;
    info!("Verify: {STALE_MEMBER_COUNT} stale members joined");

    // --- Step 2b: Wait for stale members to get partitions assigned ---
    await_rebalance_complete(
        ctx.client,
        &stream_id,
        &topic_id,
        &group_id,
        REBALANCE_TIMEOUT,
    )
    .await
    .map_err(|e| InvariantViolation {
        kind: "stale_rebalance_incomplete",
        description: format!(
            "Rebalance for stale members did not complete within {REBALANCE_TIMEOUT:?}: {e}"
        ),
        context: String::new(),
    })?;
    info!("Verify: stale members have partitions assigned, polling without commit");

    // --- Step 2c: Stale members poll WITHOUT committing offsets ---
    // Exercises the "fetched but uncommitted" path — offsets must remain
    // available for fresh members after eviction.
    // With PARTITION_COUNT partitions across STALE_MEMBER_COUNT members (1:1),
    // every partition is assigned, so the sentinel on partition 1 is reachable.
    let mut stale_saw_sentinel = false;
    for (i, client) in stale_clients.iter().enumerate() {
        let consumer = Consumer::group(group_id.clone());
        let polled = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &consumer,
                &PollingStrategy::next(),
                10,
                false,
            )
            .await
            .map_err(|e| InvariantViolation {
                kind: "stale_poll_failed",
                description: format!("Stale member #{i} poll failed: {e}"),
                context: String::new(),
            })?;
        if contains_sentinel(&polled) {
            stale_saw_sentinel = true;
            info!("Verify: stale member #{i} observed sentinel (not committed)");
        }
    }
    if !stale_saw_sentinel {
        return Err(InvariantViolation {
            kind: "stale_never_claimed_sentinel",
            description: "No stale member observed the sentinel during uncommitted poll".into(),
            context: format!(
                "All {PARTITION_COUNT} partitions assigned across {STALE_MEMBER_COUNT} members; \
                 sentinel on partition 1 should be reachable"
            ),
        });
    }
    info!("Verify: stale poll phase complete, waiting for eviction");

    // --- Step 3: Wait for heartbeat eviction ---
    // Keep stale clients alive (connected) — the server must evict them based on
    // missed heartbeats, not connection close.
    await_members_count(
        ctx.client,
        &stream_id,
        &topic_id,
        &group_id,
        0,
        EVICTION_TIMEOUT,
    )
    .await
    .map_err(|e| InvariantViolation {
        kind: "members_never_evicted",
        description: format!("Stale members were not evicted within {EVICTION_TIMEOUT:?}: {e}"),
        context: "Ensure server has heartbeat.enabled=true with a short interval (e.g. 2s)".into(),
    })?;
    info!("Verify: all stale members evicted");
    drop(stale_clients);

    // --- Step 4: Join fresh members ---
    // Fresh clients always use TCP because heartbeat eviction is a binary
    // protocol mechanism — HTTP/REST does not participate in heartbeat keepalive.
    info!("Verify: joining {FRESH_MEMBER_COUNT} fresh members");
    let mut fresh_clients = Vec::with_capacity(FRESH_MEMBER_COUNT as usize);
    for i in 0..FRESH_MEMBER_COUNT {
        let client = crate::client::create_client(ctx.server_address, crate::args::Transport::Tcp)
            .await
            .map_err(|e| InvariantViolation {
                kind: "fresh_client_create_failed",
                description: format!("Failed to create fresh client #{i}: {e}"),
                context: String::new(),
            })?;
        client
            .join_consumer_group(&stream_id, &topic_id, &group_id)
            .await
            .map_err(|e| InvariantViolation {
                kind: "fresh_join_failed",
                description: format!("Fresh client #{i} failed to join group: {e}"),
                context: String::new(),
            })?;
        fresh_clients.push(client);
    }

    // --- Step 5a: Confirm all fresh members joined ---
    await_members_count(
        ctx.client,
        &stream_id,
        &topic_id,
        &group_id,
        FRESH_MEMBER_COUNT,
        REBALANCE_TIMEOUT,
    )
    .await
    .map_err(|e| InvariantViolation {
        kind: "fresh_members_never_joined",
        description: format!("Expected {FRESH_MEMBER_COUNT} fresh members, timed out: {e}"),
        context: String::new(),
    })?;

    // --- Step 5b: Wait for rebalance ---
    let details = await_rebalance_complete(
        ctx.client,
        &stream_id,
        &topic_id,
        &group_id,
        REBALANCE_TIMEOUT,
    )
    .await
    .map_err(|e| InvariantViolation {
        kind: "rebalance_incomplete",
        description: format!("Rebalance did not complete within {REBALANCE_TIMEOUT:?}: {e}"),
        context: String::new(),
    })?;
    info!(
        "Verify: rebalance complete — {} members, {} partitions",
        details.members_count, details.partitions_count
    );

    // --- Step 6: Check partition assignments are unique ---
    let mut partition_owners: HashMap<u32, u32> = HashMap::new();
    for member in &details.members {
        for &partition in &member.partitions {
            if let Some(&prev_owner) = partition_owners.get(&partition) {
                violations.push(InvariantViolation {
                    kind: "duplicate_partition_assignment",
                    description: format!("Partition {partition} assigned to multiple members"),
                    context: format!("member_id={} and member_id={prev_owner}", member.id),
                });
            } else {
                partition_owners.insert(partition, member.id);
            }
        }
    }

    // --- Step 7: Poll concurrently from all fresh members ---
    // auto_commit=false so each poll is independent of group offset mutation —
    // prevents one fast commit from masking duplicate delivery.
    info!("Verify: polling concurrently from {FRESH_MEMBER_COUNT} fresh members");
    let mut poll_tasks = JoinSet::new();
    for client in fresh_clients {
        let s_id = stream_id.clone();
        let t_id = topic_id.clone();
        let g_id = group_id.clone();
        poll_tasks.spawn(async move {
            let consumer = Consumer::group(g_id);
            client
                .poll_messages(
                    &s_id,
                    &t_id,
                    None,
                    &consumer,
                    &PollingStrategy::next(),
                    10,
                    false,
                )
                .await
        });
    }

    let mut sentinel_receivers = 0u32;
    while let Some(result) = poll_tasks.join_next().await {
        match result {
            Ok(Ok(polled)) => {
                if contains_sentinel(&polled) {
                    sentinel_receivers += 1;
                }
            }
            Ok(Err(e)) => {
                violations.push(InvariantViolation {
                    kind: "poll_failed",
                    description: format!("Fresh member poll failed: {e}"),
                    context: String::new(),
                });
            }
            Err(e) => {
                violations.push(InvariantViolation {
                    kind: "poll_task_panicked",
                    description: format!("Poll task panicked: {e}"),
                    context: String::new(),
                });
            }
        }
    }

    // --- Step 8: Assert exactly one member received the sentinel ---
    if sentinel_receivers == 0 {
        violations.push(InvariantViolation {
            kind: "sentinel_lost",
            description: "No fresh member received the sentinel message".into(),
            context: String::new(),
        });
    } else if sentinel_receivers > 1 {
        violations.push(InvariantViolation {
            kind: "duplicate_sentinel_delivery",
            description: format!(
                "{sentinel_receivers} members received the sentinel (expected exactly 1)"
            ),
            context: String::new(),
        });
    }

    Ok(violations)
}

fn contains_sentinel(polled: &PolledMessages) -> bool {
    polled
        .messages
        .iter()
        .any(|m| m.payload == SENTINEL_PAYLOAD)
}
