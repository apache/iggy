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

use crate::benchmarks::{BENCH_STREAM_PREFIX, BENCH_TOPIC_NAME};
use crate::utils::{ClientFactory, login_root};
use iggy::clients::client::IggyClient;
use iggy::prelude::*;
use iggy_common::calculate_checksum;
use std::collections::BTreeSet;
use std::sync::Arc;
use tracing::{info, warn};

/// Post-test verification results.
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

/// Runs drain-phase verification: polls all partitions and checks offset continuity.
///
/// During the stress test, messages may expire, so we verify that within each
/// partition the offsets we can still poll are monotonically increasing with no
/// gaps in the remaining range.
pub struct StressVerifier {
    client_factory: Arc<dyn ClientFactory>,
    streams: u32,
    partitions: u32,
}

impl StressVerifier {
    pub fn new(client_factory: Arc<dyn ClientFactory>, streams: u32, partitions: u32) -> Self {
        Self {
            client_factory,
            streams,
            partitions,
        }
    }

    pub async fn verify(&self) -> VerificationResult {
        let client = self.client_factory.create_client().await;
        let client = IggyClient::create(client, None, None);
        login_root(&client).await;

        let mut result = VerificationResult::default();

        for stream_idx in 1..=self.streams {
            let stream_id: Identifier = format!("{BENCH_STREAM_PREFIX}-{stream_idx}")
                .as_str()
                .try_into()
                .expect("valid identifier");
            let topic_id: Identifier = BENCH_TOPIC_NAME.try_into().expect("valid identifier");

            for partition_id in 0..self.partitions {
                let partition_result = self
                    .verify_partition(&client, &stream_id, &topic_id, partition_id)
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

        result.passed = result.total_messages > 0
            && result.gaps_found == 0
            && result.duplicates_found == 0
            && result.checksum_mismatches == 0
            && result.payload_length_mismatches == 0;

        if result.total_messages == 0 {
            warn!(
                "Verification FAILED: 0 messages found across {} partitions — topic may have been destroyed by chaos",
                result.partitions_checked
            );
        } else if result.passed {
            info!(
                "Verification PASSED: {} partitions, {} msgs, 0 gaps, 0 dups, 0 checksum, 0 len",
                result.partitions_checked, result.total_messages
            );
        } else {
            warn!(
                "Verification FAILED: {} partitions, {} msgs, {} gaps, {} dups, {} checksum, {} len",
                result.partitions_checked,
                result.total_messages,
                result.gaps_found,
                result.duplicates_found,
                result.checksum_mismatches,
                result.payload_length_mismatches,
            );
        }

        if result.id_missing_fingerprint > 0 {
            warn!(
                "Verification: {} messages missing producer ID fingerprint (server-assigned IDs)",
                result.id_missing_fingerprint
            );
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

                        // Checksum re-verification: serialize and hash everything after the checksum field
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

        // Check for gaps in the seen offsets
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
