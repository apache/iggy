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

use super::{LaneSpec, NamespacePrefixes, Scenario, Setup, TopicDefaults, VerifyContext};
use crate::client::create_stale_client;
use crate::eventually::{await_members_count, await_rebalance_complete};
use crate::invariants::{ConsumerGroupHealthCheck, InvariantViolation};
use crate::safe_name::SafeResourceName;
use crate::violation::ViolationCategory;
use async_trait::async_trait;
use iggy::prelude::*;
use std::time::Duration;
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
         Phase 1: Setup - creates 1 stream, 1 topic (3 partitions), 1 consumer group,\n\
         and produces a single sentinel message to partition 1.\n\
         Phase 2: No chaos lanes (skip with --duration 1s or --ops 0).\n\
         Phase 3: Verify - joins stale members (1h heartbeat), lets them poll\n\
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
        info!(
            "Setup: creating stream, topic ({PARTITION_COUNT} partitions), consumer group, sentinel message"
        );
        let setup = Setup::new(client, prefixes);
        let defaults = TopicDefaults {
            partitions: PARTITION_COUNT,
            ..Default::default()
        };
        let stream = setup.create_stream(STREAM_SUFFIX).await?;
        let topic = setup.create_topic(&stream, TOPIC_SUFFIX, &defaults).await?;
        setup
            .create_consumer_group(&stream, &topic, GROUP_SUFFIX)
            .await?;
        setup.send_str(&stream, &topic, 1, SENTINEL_PAYLOAD).await?;
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

    // --- Phase 1: Join stale members ---
    info!("Verify: joining {STALE_MEMBER_COUNT} stale members (1h heartbeat)");
    let mut stale_clients = Vec::with_capacity(STALE_MEMBER_COUNT as usize);
    for i in 0..STALE_MEMBER_COUNT {
        let client =
            create_stale_client(ctx.server_address)
                .await
                .map_err(|e| InvariantViolation {
                    category: ViolationCategory::Infrastructure,
                    kind: "stale_client_create_failed",
                    description: format!("Failed to create stale client #{i}: {e}"),
                    context: String::new(),
                    trace_anchor: None,
                })?;
        client
            .join_consumer_group(&stream_id, &topic_id, &group_id)
            .await
            .map_err(|e| InvariantViolation {
                category: ViolationCategory::ConsumerGroup,
                kind: "stale_join_failed",
                description: format!("Stale client #{i} failed to join group: {e}"),
                context: String::new(),
                trace_anchor: None,
            })?;
        stale_clients.push(client);
    }

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
        category: ViolationCategory::ConsumerGroup,
        kind: "members_never_joined",
        description: format!("Expected {STALE_MEMBER_COUNT} members after join, timed out: {e}"),
        context: String::new(),
        trace_anchor: None,
    })?;
    info!("Verify: {STALE_MEMBER_COUNT} stale members joined");

    await_rebalance_complete(
        ctx.client,
        &stream_id,
        &topic_id,
        &group_id,
        REBALANCE_TIMEOUT,
    )
    .await
    .map_err(|e| InvariantViolation {
        category: ViolationCategory::ConsumerGroup,
        kind: "stale_rebalance_incomplete",
        description: format!(
            "Rebalance for stale members did not complete within {REBALANCE_TIMEOUT:?}: {e}"
        ),
        context: String::new(),
        trace_anchor: None,
    })?;
    info!("Verify: stale members have partitions assigned, polling without commit");

    // --- Phase 2: Stale members poll WITHOUT committing offsets ---
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
                category: ViolationCategory::ConsumerGroup,
                kind: "stale_poll_failed",
                description: format!("Stale member #{i} poll failed: {e}"),
                context: String::new(),
                trace_anchor: None,
            })?;
        if contains_sentinel(&polled) {
            stale_saw_sentinel = true;
            info!("Verify: stale member #{i} observed sentinel (not committed)");
        }
    }
    if !stale_saw_sentinel {
        return Err(InvariantViolation {
            category: ViolationCategory::ConsumerGroup,
            kind: "stale_never_claimed_sentinel",
            description: "No stale member observed the sentinel during uncommitted poll".into(),
            context: format!(
                "All {PARTITION_COUNT} partitions assigned across {STALE_MEMBER_COUNT} members; \
                 sentinel on partition 1 should be reachable"
            ),
            trace_anchor: None,
        });
    }
    info!("Verify: stale poll phase complete, waiting for eviction");

    // --- Phase 3: Wait for heartbeat eviction ---
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
        category: ViolationCategory::ConsumerGroup,
        kind: "members_never_evicted",
        description: format!("Stale members were not evicted within {EVICTION_TIMEOUT:?}: {e}"),
        context: "Ensure server has heartbeat.enabled=true with a short interval (e.g. 2s)".into(),
        trace_anchor: None,
    })?;
    info!("Verify: all stale members evicted");
    drop(stale_clients);

    // --- Phase 4: Fresh member health check (shared logic) ---
    let health_check = ConsumerGroupHealthCheck {
        label: "stale-consumers",
        server_address: ctx.server_address,
        transport: ctx.transport,
        admin_client: ctx.client,
        stream_id: &stream_id,
        topic_id: &topic_id,
        group_id: &group_id,
        fresh_member_count: FRESH_MEMBER_COUNT,
        rebalance_timeout: REBALANCE_TIMEOUT,
        sentinel_payload: SENTINEL_PAYLOAD.as_bytes(),
    };

    Ok(health_check.run().await)
}

fn contains_sentinel(polled: &PolledMessages) -> bool {
    polled
        .messages
        .iter()
        .any(|m| m.payload == SENTINEL_PAYLOAD)
}
