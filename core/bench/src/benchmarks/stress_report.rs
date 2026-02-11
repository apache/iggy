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

use crate::actors::stress::{stress_context::StressStats, verifier::VerificationResult};
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Structured stress test report, serializable to JSON for `--output-dir`.
#[derive(Debug, Serialize)]
pub struct StressReport {
    pub total_duration_secs: f64,
    pub baseline_duration_secs: f64,
    pub chaos_duration_secs: f64,
    pub drain_duration_secs: f64,
    pub api_calls: ApiCallSummary,
    pub error_tiers: ErrorTierSummary,
    pub verification: VerificationSummary,
}

#[derive(Debug, Serialize)]
pub struct ApiCallSummary {
    pub send_messages: CallCount,
    pub poll_messages: CallCount,
    pub create_topic: CallCount,
    pub delete_topic: CallCount,
    pub create_partitions: CallCount,
    pub delete_partitions: CallCount,
    pub create_consumer_group: CallCount,
    pub delete_consumer_group: CallCount,
    pub join_consumer_group: CallCount,
    pub leave_consumer_group: CallCount,
    pub purge_topic: CallCount,
    pub delete_segments: CallCount,
    pub update_topic: CallCount,
    pub purge_stream: CallCount,
    pub stress_poll: CallCount,
    pub create_user: CallCount,
    pub delete_user: CallCount,
    pub create_pat: CallCount,
    pub delete_pat: CallCount,
    pub store_offset: CallCount,
    pub get_offset: CallCount,
    pub ping: CallCount,
    pub get_stats: CallCount,
    pub get_me: CallCount,
    pub get_clients: CallCount,
    pub flush: CallCount,
    pub total_ok: u64,
    pub total_err: u64,
}

#[derive(Debug, Serialize)]
pub struct CallCount {
    pub ok: u64,
    pub err: u64,
}

impl CallCount {
    fn load(ok: &AtomicU64, err: &AtomicU64) -> Self {
        Self {
            ok: ok.load(Ordering::Relaxed),
            err: err.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ErrorTierSummary {
    pub expected: u64,
    pub unexpected: u64,
}

#[derive(Debug, Serialize)]
pub struct VerificationSummary {
    pub partitions_checked: u32,
    pub total_messages: u64,
    pub gaps_found: u64,
    pub duplicates_found: u64,
    pub checksum_mismatches: u64,
    pub payload_length_mismatches: u64,
    pub id_missing_fingerprint: u64,
    pub passed: bool,
}

impl StressReport {
    pub fn build(
        stats: &StressStats,
        verification: &VerificationResult,
        total_duration: Duration,
        baseline_duration: Duration,
        chaos_duration: Duration,
        drain_duration: Duration,
    ) -> Self {
        let api_calls = ApiCallSummary::from_stats(stats);
        Self {
            total_duration_secs: total_duration.as_secs_f64(),
            baseline_duration_secs: baseline_duration.as_secs_f64(),
            chaos_duration_secs: chaos_duration.as_secs_f64(),
            drain_duration_secs: drain_duration.as_secs_f64(),
            error_tiers: ErrorTierSummary {
                expected: stats.expected_errors.load(Ordering::Relaxed),
                unexpected: stats.unexpected_errors.load(Ordering::Relaxed),
            },
            api_calls,
            verification: VerificationSummary {
                partitions_checked: verification.partitions_checked,
                total_messages: verification.total_messages,
                gaps_found: verification.gaps_found,
                duplicates_found: verification.duplicates_found,
                checksum_mismatches: verification.checksum_mismatches,
                payload_length_mismatches: verification.payload_length_mismatches,
                id_missing_fingerprint: verification.id_missing_fingerprint,
                passed: verification.passed,
            },
        }
    }

    pub fn print_summary(&self) {
        println!("\n=== STRESS TEST REPORT ===");
        println!(
            "Duration: {:.1}s (baseline: {:.1}s, chaos: {:.1}s, drain: {:.1}s)",
            self.total_duration_secs,
            self.baseline_duration_secs,
            self.chaos_duration_secs,
            self.drain_duration_secs
        );
        println!(
            "API calls: {} ok, {} err",
            self.api_calls.total_ok, self.api_calls.total_err
        );
        println!(
            "Errors: {} expected, {} unexpected",
            self.error_tiers.expected, self.error_tiers.unexpected
        );
        println!(
            "Verification: {} partitions, {} msgs, {} gaps, {} dups, {} checksum, {} len, {} id -> {}",
            self.verification.partitions_checked,
            self.verification.total_messages,
            self.verification.gaps_found,
            self.verification.duplicates_found,
            self.verification.checksum_mismatches,
            self.verification.payload_length_mismatches,
            self.verification.id_missing_fingerprint,
            if self.verification.passed {
                "PASSED"
            } else {
                "FAILED"
            }
        );
        println!("==========================\n");
    }
}

impl ApiCallSummary {
    fn from_stats(s: &StressStats) -> Self {
        Self {
            send_messages: CallCount::load(&s.send_messages_ok, &s.send_messages_err),
            poll_messages: CallCount::load(&s.poll_messages_ok, &s.poll_messages_err),
            create_topic: CallCount::load(&s.create_topic_ok, &s.create_topic_err),
            delete_topic: CallCount::load(&s.delete_topic_ok, &s.delete_topic_err),
            create_partitions: CallCount::load(&s.create_partitions_ok, &s.create_partitions_err),
            delete_partitions: CallCount::load(&s.delete_partitions_ok, &s.delete_partitions_err),
            create_consumer_group: CallCount::load(
                &s.create_consumer_group_ok,
                &s.create_consumer_group_err,
            ),
            delete_consumer_group: CallCount::load(
                &s.delete_consumer_group_ok,
                &s.delete_consumer_group_err,
            ),
            join_consumer_group: CallCount::load(
                &s.join_consumer_group_ok,
                &s.join_consumer_group_err,
            ),
            leave_consumer_group: CallCount::load(
                &s.leave_consumer_group_ok,
                &s.leave_consumer_group_err,
            ),
            purge_topic: CallCount::load(&s.purge_topic_ok, &s.purge_topic_err),
            delete_segments: CallCount::load(&s.delete_segments_ok, &s.delete_segments_err),
            update_topic: CallCount::load(&s.update_topic_ok, &s.update_topic_err),
            purge_stream: CallCount::load(&s.purge_stream_ok, &s.purge_stream_err),
            stress_poll: CallCount::load(&s.stress_poll_ok, &s.stress_poll_err),
            create_user: CallCount::load(&s.create_user_ok, &s.create_user_err),
            delete_user: CallCount::load(&s.delete_user_ok, &s.delete_user_err),
            create_pat: CallCount::load(&s.create_pat_ok, &s.create_pat_err),
            delete_pat: CallCount::load(&s.delete_pat_ok, &s.delete_pat_err),
            store_offset: CallCount::load(&s.store_offset_ok, &s.store_offset_err),
            get_offset: CallCount::load(&s.get_offset_ok, &s.get_offset_err),
            ping: CallCount::load(&s.ping_ok, &s.ping_err),
            get_stats: CallCount::load(&s.get_stats_ok, &s.get_stats_err),
            get_me: CallCount::load(&s.get_me_ok, &s.get_me_err),
            get_clients: CallCount::load(&s.get_clients_ok, &s.get_clients_err),
            flush: CallCount::load(&s.flush_ok, &s.flush_err),
            total_ok: s.total_ok(),
            total_err: s.total_err(),
        }
    }
}
