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

use crate::benchmarks::{BENCH_TOPIC_NAME, CHAOS_STREAM_PREFIX, STABLE_STREAM_PREFIX};
use crate::utils::{ClientFactory, login_root};
use iggy::clients::client::IggyClient;
use iggy::prelude::*;
use iggy_common::calculate_checksum;
use std::collections::BTreeSet;
use std::sync::Arc;
use tracing::{info, warn};

/// Post-test verification results for a single namespace.
#[derive(Debug, Default)]
pub struct VerificationResult {
    pub partitions_checked: u32,
    pub total_messages: u64,
    pub gaps_found: u64,
    pub duplicates_found: u64,
    pub checksum_mismatches: u64,
    pub payload_length_mismatches: u64,
    pub id_missing_fingerprint: u64,
    pub passed: bool,
}

/// Combined verification across stable and chaos namespaces.
#[derive(Debug)]
pub struct StressVerificationResult {
    pub stable: VerificationResult,
    pub chaos: VerificationResult,
    pub passed: bool,
}

/// Runs drain-phase verification with namespace-aware checking:
/// - Stable streams: strict (msgs > 0, no gaps, no dups, checksums OK)
/// - Chaos streams: integrity-only (checksums + payload length OK; gaps, dups, 0 msgs acceptable)
pub struct StressVerifier {
    client_factory: Arc<dyn ClientFactory>,
    stable_streams: u32,
    chaos_streams: u32,
    partitions: u32,
}

impl StressVerifier {
    pub fn new(
        client_factory: Arc<dyn ClientFactory>,
        stable_streams: u32,
        chaos_streams: u32,
        partitions: u32,
    ) -> Self {
        Self {
            client_factory,
            stable_streams,
            chaos_streams,
            partitions,
        }
    }

    pub async fn verify(&self) -> StressVerificationResult {
        let client = self.client_factory.create_client().await;
        let client = IggyClient::create(client, None, None);
        login_root(&client).await;

        let stable = self
            .verify_namespace(&client, STABLE_STREAM_PREFIX, self.stable_streams)
            .await;
        let chaos = self
            .verify_namespace(&client, CHAOS_STREAM_PREFIX, self.chaos_streams)
            .await;

        // Stable: strict verification
        let stable_passed = stable.total_messages > 0
            && stable.gaps_found == 0
            && stable.duplicates_found == 0
            && stable.checksum_mismatches == 0
            && stable.payload_length_mismatches == 0;

        // Chaos: integrity-only (gaps, dups, 0 msgs are acceptable)
        let chaos_passed = chaos.checksum_mismatches == 0 && chaos.payload_length_mismatches == 0;

        let stable = VerificationResult {
            passed: stable_passed,
            ..stable
        };
        let chaos = VerificationResult {
            passed: chaos_passed,
            ..chaos
        };

        let passed = stable.passed && chaos.passed;

        if stable.passed {
            info!(
                "Stable verification PASSED: {} partitions, {} msgs, 0 gaps, 0 dups, 0 checksum",
                stable.partitions_checked, stable.total_messages
            );
        } else {
            warn!(
                "Stable verification FAILED: {} partitions, {} msgs, {} gaps, {} dups, {} checksum, {} len",
                stable.partitions_checked,
                stable.total_messages,
                stable.gaps_found,
                stable.duplicates_found,
                stable.checksum_mismatches,
                stable.payload_length_mismatches,
            );
        }

        if chaos.passed {
            info!(
                "Chaos verification PASSED: {} partitions, {} msgs, integrity OK",
                chaos.partitions_checked, chaos.total_messages
            );
        } else {
            warn!(
                "Chaos verification FAILED: {} partitions, {} checksum, {} len",
                chaos.partitions_checked,
                chaos.checksum_mismatches,
                chaos.payload_length_mismatches,
            );
        }

        if stable.id_missing_fingerprint + chaos.id_missing_fingerprint > 0 {
            warn!(
                "Verification: {} messages missing producer ID fingerprint (server-assigned IDs)",
                stable.id_missing_fingerprint + chaos.id_missing_fingerprint
            );
        }

        StressVerificationResult {
            stable,
            chaos,
            passed,
        }
    }

    async fn verify_namespace(
        &self,
        client: &IggyClient,
        prefix: &str,
        stream_count: u32,
    ) -> VerificationResult {
        let mut result = VerificationResult::default();

        for stream_idx in 1..=stream_count {
            let stream_id: Identifier = format!("{prefix}-{stream_idx}")
                .as_str()
                .try_into()
                .expect("valid identifier");
            let topic_id: Identifier = BENCH_TOPIC_NAME.try_into().expect("valid identifier");

            for partition_id in 0..self.partitions {
                let partition_result = self
                    .verify_partition(client, &stream_id, &topic_id, partition_id)
                    .await;
                result.partitions_checked += 1;
                result.total_messages += partition_result.total_messages;
                result.gaps_found += partition_result.gaps_found;
                result.duplicates_found += partition_result.duplicates_found;
                result.checksum_mismatches += partition_result.checksum_mismatches;
                result.payload_length_mismatches += partition_result.payload_length_mismatches;
                result.id_missing_fingerprint += partition_result.id_missing_fingerprint;
            }
        }

        result
    }

    async fn verify_partition(
        &self,
        client: &IggyClient,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: u32,
    ) -> VerificationResult {
        let mut result = VerificationResult::default();
        let mut seen_offsets = BTreeSet::new();
        let mut current_offset = 0u64;
        let consumer = Consumer::new(Identifier::numeric(9999).expect("valid id"));
        let batch_size = 1000u32;

        loop {
            let strategy = PollingStrategy::offset(current_offset);
            match client
                .poll_messages(
                    stream_id,
                    topic_id,
                    Some(partition_id),
                    &consumer,
                    &strategy,
                    batch_size,
                    false,
                )
                .await
            {
                Ok(polled) => {
                    if polled.messages.is_empty() {
                        break;
                    }

                    for msg in &polled.messages {
                        let offset = msg.header.offset;
                        if !seen_offsets.insert(offset) {
                            result.duplicates_found += 1;
                        }

                        let raw = msg.to_bytes();
                        let recomputed = calculate_checksum(&raw[8..]);
                        if msg.header.checksum != recomputed {
                            result.checksum_mismatches += 1;
                        }

                        if msg.header.payload_length as usize != msg.payload.len() {
                            result.payload_length_mismatches += 1;
                        }

                        if msg.header.id == 0 {
                            result.id_missing_fingerprint += 1;
                        }

                        result.total_messages += 1;
                    }

                    current_offset = polled.messages.last().expect("non-empty").header.offset + 1;
                }
                Err(e) => {
                    warn!(
                        "Verifier: poll partition {partition_id} at offset {current_offset} failed: {e}"
                    );
                    break;
                }
            }
        }

        if let (Some(&min), Some(&max)) = (seen_offsets.first(), seen_offsets.last()) {
            let expected_count = max - min + 1;
            let actual_count = seen_offsets.len() as u64;
            if actual_count < expected_count {
                result.gaps_found = expected_count - actual_count;
            }
        }

        result
    }
}
