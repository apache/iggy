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

use crate::args::Transport;
use crate::client::create_client;
use crate::ops::{Op, OpDetail, OpOutcome};
use crate::scenarios::{Namespace, NamespacePrefixes};
use crate::shadow::ShadowState;
use crate::violation::ViolationCategory;
use crate::watermark::WatermarkState;
use iggy::prelude::*;
use serde::Serialize;
use std::collections::HashMap;
use std::time::Duration;
use tokio::task::JoinSet;
use tracing::info;

pub type PartitionKey = (String, String, u32);

#[derive(Debug, Clone, Serialize)]
pub struct TraceAnchor {
    pub seq: u64,
    pub t_us: u64,
    pub worker: u32,
    pub lane: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct InvariantViolation {
    pub category: ViolationCategory,
    pub kind: &'static str,
    pub description: String,
    pub context: String,
    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    pub trace_anchor: Option<TraceAnchor>,
}

impl InvariantViolation {
    pub fn with_anchor(mut self, anchor: TraceAnchor) -> Self {
        self.trace_anchor = Some(anchor);
        self
    }
}

/// Check that polled offsets are monotonically increasing per partition.
/// Offset regressions after a recent purge are expected and not violations.
pub fn check_offset_monotonicity(
    last_offsets: &mut HashMap<PartitionKey, u64>,
    op: &Op,
    outcome: &OpOutcome,
    shadow: &ShadowState,
) -> Result<(), InvariantViolation> {
    let (stream, topic, partition) = match op {
        Op::PollMessages {
            stream,
            topic,
            partition,
            ..
        } => (stream, topic, partition),
        _ => return Ok(()),
    };
    let offset = match outcome {
        OpOutcome::Success {
            detail: Some(OpDetail::Polled { offset, .. }),
        } => *offset,
        _ => return Ok(()),
    };

    let key = (stream.clone(), topic.clone(), *partition);
    if let Some(&last) = last_offsets.get(&key)
        && offset < last
    {
        if shadow.was_recently_purged(stream, topic) {
            last_offsets.insert(key, offset);
            return Ok(());
        }
        return Err(InvariantViolation {
            category: ViolationCategory::Invariant,
            kind: "offset_regression",
            description: format!("Offset went backwards: {last} -> {offset}"),
            context: format!("stream={stream}, topic={topic}, partition={partition}"),
            trace_anchor: None,
        });
    }
    last_offsets.insert(key, offset);
    Ok(())
}

/// Post-run: verify server state matches shadow state for all prefixed resources.
/// Stable-namespace streams are checked strictly (mismatches are real bugs).
/// Churn-namespace streams are checked leniently (mismatches from concurrent races are expected).
pub async fn post_run_verify(
    client: &IggyClient,
    shadow: &ShadowState,
    prefixes: &NamespacePrefixes,
) -> Vec<InvariantViolation> {
    let mut violations = Vec::new();

    let streams = match client.get_streams().await {
        Ok(s) => s,
        Err(e) => {
            violations.push(InvariantViolation {
                category: ViolationCategory::Infrastructure,
                kind: "post_run_fetch_failed",
                description: format!("Failed to get streams: {e}"),
                context: String::new(),
                trace_anchor: None,
            });
            return violations;
        }
    };

    let server_streams: HashMap<&str, &Stream> = streams
        .iter()
        .filter(|s| prefixes.matches(&s.name))
        .map(|s| (s.name.as_str(), s))
        .collect();

    // Check all shadow streams exist on server (strict for stable, lenient for churn)
    for (name, shadow_stream) in &shadow.streams {
        if !server_streams.contains_key(name.as_str())
            && shadow_stream.namespace == Namespace::Stable
        {
            violations.push(InvariantViolation {
                category: ViolationCategory::ShadowMismatch,
                kind: "missing_stable_stream",
                description: format!("Shadow has stable stream '{name}' but server does not"),
                context: String::new(),
                trace_anchor: None,
            });
        }
    }

    // Check no extra prefixed streams on server
    for &name in server_streams.keys() {
        if !shadow.streams.contains_key(name) {
            let is_stable = prefixes
                .namespace_of(name)
                .is_some_and(|ns| ns == Namespace::Stable);
            if is_stable {
                violations.push(InvariantViolation {
                    category: ViolationCategory::ShadowMismatch,
                    kind: "extra_stable_stream",
                    description: format!("Server has stable stream '{name}' not in shadow"),
                    context: String::new(),
                    trace_anchor: None,
                });
            }
            // Churn: shadow tracking lag from concurrent ops - skip
        }
    }

    // Deep-check topics for each matching stream
    for (name, shadow_stream) in &shadow.streams {
        let Some(server_stream) = server_streams.get(name.as_str()) else {
            continue;
        };

        let is_stable = shadow_stream.namespace == Namespace::Stable;

        let stream_id = Identifier::numeric(server_stream.id).unwrap();
        let Ok(Some(details)) = client.get_stream(&stream_id).await else {
            violations.push(InvariantViolation {
                category: ViolationCategory::Infrastructure,
                kind: "stream_details_fetch_failed",
                description: format!("Failed to get details for stream '{name}'"),
                context: String::new(),
                trace_anchor: None,
            });
            continue;
        };

        let server_topics: HashMap<&str, &Topic> = details
            .topics
            .iter()
            .map(|t| (t.name.as_str(), t))
            .collect();

        for topic_name in shadow_stream.topics.keys() {
            if !server_topics.contains_key(topic_name.as_str()) && is_stable {
                violations.push(InvariantViolation {
                    category: ViolationCategory::ShadowMismatch,
                    kind: "missing_stable_topic",
                    description: format!(
                        "Shadow has topic '{topic_name}' in stable stream '{name}' but server does not"
                    ),
                    context: String::new(),
                    trace_anchor: None,
                });
            }
        }

        for &topic_name in server_topics.keys() {
            if !shadow_stream.topics.contains_key(topic_name) && is_stable {
                violations.push(InvariantViolation {
                    category: ViolationCategory::ShadowMismatch,
                    kind: "extra_stable_topic",
                    description: format!(
                        "Server has topic '{topic_name}' in stable stream '{name}' not in shadow"
                    ),
                    context: String::new(),
                    trace_anchor: None,
                });
            }
        }
    }

    violations
}

/// Post-run: verify that server-reported message counts do not exceed send watermarks.
/// Detects phantom reads (server returns messages that were never sent) and purge
/// failures (server retained data that should have been removed).
pub async fn watermark_verify(
    client: &IggyClient,
    watermarks: &WatermarkState,
    shadow: &ShadowState,
) -> Vec<InvariantViolation> {
    let mut violations = Vec::new();

    // For each purged (stream, topic), verify server's message count doesn't
    // exceed post-purge sends
    for (stream, topic) in watermarks.purge_history() {
        if !shadow.stream_exists(stream) {
            continue;
        }
        let stream_id = match Identifier::from_str_value(stream) {
            Ok(id) => id,
            Err(_) => continue,
        };

        let topics_to_check: Vec<String> = match topic {
            Some(t) => vec![t.clone()],
            None => shadow
                .streams
                .get(stream)
                .map(|s| s.topics.keys().cloned().collect())
                .unwrap_or_default(),
        };

        for topic_name in &topics_to_check {
            if !shadow.topic_exists(stream, topic_name) {
                continue;
            }
            let topic_id = match Identifier::from_str_value(topic_name) {
                Ok(id) => id,
                Err(_) => continue,
            };

            let details = match client.get_topic(&stream_id, &topic_id).await {
                Ok(Some(d)) => d,
                _ => continue,
            };

            // Sum post-purge sends across all partitions for this topic
            let post_purge_sends: u64 = (1..=details.partitions_count)
                .map(|p| watermarks.send_watermark(stream, topic_name, p))
                .sum();

            if details.messages_count > post_purge_sends && post_purge_sends > 0 {
                violations.push(InvariantViolation {
                    category: ViolationCategory::ShadowMismatch,
                    kind: "purge_data_retained",
                    description: format!(
                        "Server reports {} messages for {stream}/{topic_name} \
                         but only {post_purge_sends} were sent after purge",
                        details.messages_count
                    ),
                    context: String::new(),
                    trace_anchor: None,
                });
            }
        }
    }

    violations
}

/// Cross-client consistency: spawn a fresh client and compare get_streams results.
pub async fn cross_client_consistency(
    addr: &str,
    transport: Transport,
    prefixes: &NamespacePrefixes,
) -> Vec<InvariantViolation> {
    let mut violations = Vec::new();

    let client_a = match create_client(addr, transport).await {
        Ok(c) => c,
        Err(e) => {
            violations.push(InvariantViolation {
                category: ViolationCategory::Infrastructure,
                kind: "cross_client_connect_failed",
                description: format!("Client A: {e}"),
                context: String::new(),
                trace_anchor: None,
            });
            return violations;
        }
    };
    let client_b = match create_client(addr, transport).await {
        Ok(c) => c,
        Err(e) => {
            violations.push(InvariantViolation {
                category: ViolationCategory::Infrastructure,
                kind: "cross_client_connect_failed",
                description: format!("Client B: {e}"),
                context: String::new(),
                trace_anchor: None,
            });
            return violations;
        }
    };

    let (a_result, b_result) = tokio::join!(client_a.get_streams(), client_b.get_streams());

    let (streams_a, streams_b) = match (a_result, b_result) {
        (Ok(a), Ok(b)) => (a, b),
        (Err(e), _) | (_, Err(e)) => {
            violations.push(InvariantViolation {
                category: ViolationCategory::Infrastructure,
                kind: "cross_client_fetch_failed",
                description: format!("Failed to get streams: {e}"),
                context: String::new(),
                trace_anchor: None,
            });
            return violations;
        }
    };

    let names_a: std::collections::HashSet<&str> = streams_a
        .iter()
        .filter(|s| prefixes.matches(&s.name))
        .map(|s| s.name.as_str())
        .collect();
    let names_b: std::collections::HashSet<&str> = streams_b
        .iter()
        .filter(|s| prefixes.matches(&s.name))
        .map(|s| s.name.as_str())
        .collect();

    if names_a != names_b {
        violations.push(InvariantViolation {
            category: ViolationCategory::CrossClient,
            kind: "cross_client_inconsistency",
            description: format!(
                "Client A sees {} streams, Client B sees {}",
                names_a.len(),
                names_b.len()
            ),
            context: format!(
                "only_in_a={:?}, only_in_b={:?}",
                names_a.difference(&names_b).collect::<Vec<_>>(),
                names_b.difference(&names_a).collect::<Vec<_>>()
            ),
            trace_anchor: None,
        });
    }

    violations
}

/// Shared consumer group health check: joins fresh members, verifies rebalance,
/// checks partition assignment uniqueness, polls concurrently, asserts sentinel delivery.
pub struct ConsumerGroupHealthCheck<'a> {
    pub label: &'a str,
    pub server_address: &'a str,
    pub transport: Transport,
    pub admin_client: &'a IggyClient,
    pub stream_id: &'a Identifier,
    pub topic_id: &'a Identifier,
    pub group_id: &'a Identifier,
    pub fresh_member_count: u32,
    pub rebalance_timeout: Duration,
    pub sentinel_payload: &'a [u8],
}

impl ConsumerGroupHealthCheck<'_> {
    pub async fn run(&self) -> Vec<InvariantViolation> {
        match self.run_inner().await {
            Ok(violations) => violations,
            Err(v) => vec![v],
        }
    }

    async fn run_inner(&self) -> Result<Vec<InvariantViolation>, InvariantViolation> {
        let label = self.label;
        let mut violations = Vec::new();

        info!(
            "Verify [{label}]: joining {} fresh TCP members",
            self.fresh_member_count
        );
        let mut fresh_clients = Vec::with_capacity(self.fresh_member_count as usize);
        for i in 0..self.fresh_member_count {
            let client = create_client(self.server_address, self.transport)
                .await
                .map_err(|e| InvariantViolation {
                    category: ViolationCategory::Infrastructure,
                    kind: "fresh_client_create_failed",
                    description: format!("[{label}] Failed to create fresh client #{i}: {e}"),
                    context: String::new(),
                    trace_anchor: None,
                })?;
            client
                .join_consumer_group(self.stream_id, self.topic_id, self.group_id)
                .await
                .map_err(|e| InvariantViolation {
                    category: ViolationCategory::ConsumerGroup,
                    kind: "fresh_join_failed",
                    description: format!("[{label}] Fresh client #{i} failed to join group: {e}"),
                    context: String::new(),
                    trace_anchor: None,
                })?;
            fresh_clients.push(client);
        }

        crate::eventually::await_members_count(
            self.admin_client,
            self.stream_id,
            self.topic_id,
            self.group_id,
            self.fresh_member_count,
            self.rebalance_timeout,
        )
        .await
        .map_err(|e| InvariantViolation {
            category: ViolationCategory::ConsumerGroup,
            kind: "fresh_members_never_joined",
            description: format!(
                "[{label}] Expected {} fresh members, timed out: {e}",
                self.fresh_member_count
            ),
            context: String::new(),
            trace_anchor: None,
        })?;

        let details = crate::eventually::await_rebalance_complete(
            self.admin_client,
            self.stream_id,
            self.topic_id,
            self.group_id,
            self.rebalance_timeout,
        )
        .await
        .map_err(|e| InvariantViolation {
            category: ViolationCategory::ConsumerGroup,
            kind: "rebalance_incomplete",
            description: format!(
                "[{label}] Rebalance did not complete within {:?}: {e}",
                self.rebalance_timeout
            ),
            context: String::new(),
            trace_anchor: None,
        })?;

        info!(
            "Verify [{label}]: rebalance complete - {} members, {} partitions",
            details.members_count, details.partitions_count
        );

        let mut partition_owners: HashMap<u32, u32> = HashMap::new();
        for member in &details.members {
            for &partition in &member.partitions {
                if let Some(&prev_owner) = partition_owners.get(&partition) {
                    violations.push(InvariantViolation {
                        category: ViolationCategory::ConsumerGroup,
                        kind: "duplicate_partition_assignment",
                        description: format!(
                            "[{label}] Partition {partition} assigned to multiple members"
                        ),
                        context: format!("member_id={} and member_id={prev_owner}", member.id),
                        trace_anchor: None,
                    });
                } else {
                    partition_owners.insert(partition, member.id);
                }
            }
        }

        info!(
            "Verify [{label}]: polling concurrently from {} fresh members",
            self.fresh_member_count
        );
        let mut poll_tasks = JoinSet::new();
        for client in fresh_clients {
            let s_id = self.stream_id.clone();
            let t_id = self.topic_id.clone();
            let g_id = self.group_id.clone();
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

        let sentinel = self.sentinel_payload;
        let mut sentinel_receivers = 0u32;
        while let Some(result) = poll_tasks.join_next().await {
            match result {
                Ok(Ok(polled)) => {
                    if polled.messages.iter().any(|m| m.payload == sentinel) {
                        sentinel_receivers += 1;
                    }
                }
                Ok(Err(e)) => {
                    violations.push(InvariantViolation {
                        category: ViolationCategory::ConsumerGroup,
                        kind: "poll_failed",
                        description: format!("[{label}] Fresh member poll failed: {e}"),
                        context: String::new(),
                        trace_anchor: None,
                    });
                }
                Err(e) => {
                    violations.push(InvariantViolation {
                        category: ViolationCategory::Infrastructure,
                        kind: "poll_task_panicked",
                        description: format!("[{label}] Poll task panicked: {e}"),
                        context: String::new(),
                        trace_anchor: None,
                    });
                }
            }
        }

        if sentinel_receivers == 0 {
            violations.push(InvariantViolation {
                category: ViolationCategory::ConsumerGroup,
                kind: "sentinel_lost",
                description: format!("[{label}] No fresh member received the sentinel message"),
                context: String::new(),
                trace_anchor: None,
            });
        } else if sentinel_receivers > 1 {
            violations.push(InvariantViolation {
                category: ViolationCategory::ConsumerGroup,
                kind: "duplicate_sentinel_delivery",
                description: format!(
                    "[{label}] {sentinel_receivers} members received the sentinel (expected exactly 1)"
                ),
                context: String::new(),
                trace_anchor: None,
            });
        }

        info!("Verify [{label}]: health check passed");
        Ok(violations)
    }
}
