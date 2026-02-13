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
use crate::ops::{Op, OpOutcome};
use crate::scenarios::{Namespace, NamespacePrefixes};
use crate::shadow::ShadowState;
use iggy::prelude::*;
use serde::Serialize;
use std::collections::HashMap;

pub type PartitionKey = (String, String, u32);

#[derive(Debug, Clone, Serialize)]
pub struct InvariantViolation {
    pub kind: &'static str,
    pub description: String,
    pub context: String,
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
            detail: Some(detail),
        } => match parse_poll_offset(detail) {
            Some(o) => o,
            None => return Ok(()),
        },
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
            kind: "offset_regression",
            description: format!("Offset went backwards: {last} -> {offset}"),
            context: format!("stream={stream}, topic={topic}, partition={partition}"),
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
                kind: "post_run_fetch_failed",
                description: format!("Failed to get streams: {e}"),
                context: String::new(),
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
                kind: "missing_stable_stream",
                description: format!("Shadow has stable stream '{name}' but server does not"),
                context: String::new(),
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
                    kind: "extra_stable_stream",
                    description: format!("Server has stable stream '{name}' not in shadow"),
                    context: String::new(),
                });
            }
            // Churn: shadow tracking lag from concurrent ops — skip
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
                kind: "stream_details_fetch_failed",
                description: format!("Failed to get details for stream '{name}'"),
                context: String::new(),
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
                    kind: "missing_stable_topic",
                    description: format!(
                        "Shadow has topic '{topic_name}' in stable stream '{name}' but server does not"
                    ),
                    context: String::new(),
                });
            }
        }

        for &topic_name in server_topics.keys() {
            if !shadow_stream.topics.contains_key(topic_name) && is_stable {
                violations.push(InvariantViolation {
                    kind: "extra_stable_topic",
                    description: format!(
                        "Server has topic '{topic_name}' in stable stream '{name}' not in shadow"
                    ),
                    context: String::new(),
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
                kind: "cross_client_connect_failed",
                description: format!("Client A: {e}"),
                context: String::new(),
            });
            return violations;
        }
    };
    let client_b = match create_client(addr, transport).await {
        Ok(c) => c,
        Err(e) => {
            violations.push(InvariantViolation {
                kind: "cross_client_connect_failed",
                description: format!("Client B: {e}"),
                context: String::new(),
            });
            return violations;
        }
    };

    let (a_result, b_result) = tokio::join!(client_a.get_streams(), client_b.get_streams());

    let (streams_a, streams_b) = match (a_result, b_result) {
        (Ok(a), Ok(b)) => (a, b),
        (Err(e), _) | (_, Err(e)) => {
            violations.push(InvariantViolation {
                kind: "cross_client_fetch_failed",
                description: format!("Failed to get streams: {e}"),
                context: String::new(),
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
        });
    }

    violations
}

fn parse_poll_offset(detail: &str) -> Option<u64> {
    // detail format: "received=N, offset=M"
    detail
        .split(", ")
        .find_map(|part| part.strip_prefix("offset="))
        .and_then(|v| v.parse().ok())
}
