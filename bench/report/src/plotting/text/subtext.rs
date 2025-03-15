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

use crate::{
    benchmark_kind::BenchmarkKind, group_metrics::BenchmarkGroupMetrics,
    group_metrics_kind::GroupMetricsKind, params::BenchmarkParams, report::BenchmarkReport,
};
use byte_unit::{Byte, UnitType};
use human_repr::HumanCount;

impl BenchmarkReport {
    pub fn subtext(&self) -> String {
        let params_text = self.params.format_params();
        let mut stats = Vec::new();

        // First add latency stats
        let latency_stats = self.format_latency_stats();
        if !latency_stats.is_empty() {
            stats.push(latency_stats);
        }

        // Then add throughput stats
        let throughput_stats = self.format_throughput_stats();
        if !throughput_stats.is_empty() {
            stats.push(throughput_stats);
        }

        // For SendAndPoll tests, add total throughput as last line
        if matches!(
            self.params.benchmark_kind,
            BenchmarkKind::PinnedProducerAndConsumer
                | BenchmarkKind::BalancedProducerAndConsumerGroup
        ) {
            if let Some(total) = self.group_metrics.iter().find(|s| {
                s.summary.kind == GroupMetricsKind::ProducersAndConsumers
                    || s.summary.kind == GroupMetricsKind::ProducingConsumers
            }) {
                stats.push(format!(
                    "Total System Throughput: {:.2} MB/s, {:.0} msg/s",
                    total.summary.total_throughput_megabytes_per_second,
                    total.summary.total_throughput_messages_per_second
                ));
            }
        }
        let stats_text = stats.join("\n");

        format!("{params_text}\n{stats_text}")
    }

    fn format_latency_stats(&self) -> String {
        self.group_metrics
            .iter()
            .filter(|s| s.summary.kind != GroupMetricsKind::ProducersAndConsumers) // Skip total summary
            .map(|summary| summary.format_latency())
            .collect::<Vec<String>>()
            .join("\n")
    }

    fn format_throughput_stats(&self) -> String {
        self.group_metrics
            .iter()
            .filter(|s| s.summary.kind != GroupMetricsKind::ProducersAndConsumers) // Skip total summary as it will be added separately
            .map(|summary| summary.format_throughput_per_actor_kind())
            .collect::<Vec<String>>()
            .join("\n")
    }
}

impl BenchmarkGroupMetrics {
    fn format_throughput_per_actor_kind(&self) -> String {
        format!(
            "{} Throughput  •  Total: {:.2} MB/s, {:.0} msg/s  •  Avg Per {}: {:.2} MB/s, {:.0} msg/s",
            self.summary.kind,
            self.summary.total_throughput_megabytes_per_second,
            self.summary.total_throughput_messages_per_second,
            self.summary.kind.actor(),
            self.summary.average_throughput_megabytes_per_second,
            self.summary.average_throughput_messages_per_second,
        )
    }

    fn format_latency(&self) -> String {
        format!(
            "{} Latency  •  Avg: {:.2} ms  •  Med: {:.2} ms  •  P95: {:.2} ms  •  P99: {:.2} ms  •  P999: {:.2} ms  •  P9999: {:.2} ms",
            self.summary.kind,
            self.summary.average_latency_ms,
            self.summary.average_median_latency_ms,
            self.summary.average_p95_latency_ms,
            self.summary.average_p99_latency_ms,
            self.summary.average_p999_latency_ms,
            self.summary.average_p9999_latency_ms
        )
    }
}

impl BenchmarkParams {
    pub fn format_params(&self) -> String {
        let actors_info = self.format_actors_info();
        let message_batches = self.message_batches as u64;
        let messages_per_batch = self.messages_per_batch as u64;
        let message_size = self.message_size as u64;

        let sent = message_batches * messages_per_batch * message_size * self.producers as u64;
        let polled = message_batches * messages_per_batch * message_size * self.consumers as u64;

        let mut user_data_print = String::new();

        if sent > 0 && polled > 0 {
            user_data_print.push_str(&format!(
                "Sent {:.2}, Polled {:.2}",
                Byte::from_u64(sent).get_appropriate_unit(UnitType::Decimal),
                Byte::from_u64(polled).get_appropriate_unit(UnitType::Decimal)
            ));
        } else if polled > 0 {
            user_data_print.push_str(&format!(
                "Polled {:.2}",
                Byte::from_u64(polled).get_appropriate_unit(UnitType::Decimal)
            ));
        } else {
            user_data_print.push_str(&format!(
                "Sent {:.2}",
                Byte::from_u64(sent).get_appropriate_unit(UnitType::Decimal),
            ));
        }

        let message_batches = message_batches.human_count_bare();
        let messages_per_batch = messages_per_batch.human_count_bare();

        let topics = "1 topic per stream".to_owned();
        let partitions = if self.partitions == 0 {
            "".to_owned()
        } else {
            format!("  •  {} partitions per topic", self.partitions)
        };
        let streams = format!("{} streams", self.streams);

        format!(
            "{actors_info}  •  {streams}  •  {topics}{partitions}  •  {messages_per_batch} msg/batch  •  {message_batches} batches  •  {message_size} bytes/msg  •  {user_data_print}",
        )
    }
}
