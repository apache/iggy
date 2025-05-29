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

use crate::{BenchmarkGroupMetricsLight, BenchmarkReportLight};
use bench_report::{
    actor_kind::ActorKind, benchmark_kind::BenchmarkKind, group_metrics_kind::GroupMetricsKind,
};
use human_repr::HumanCount;

impl BenchmarkReportLight {
    pub fn subtext(&self) -> String {
        let params_text = self.format_params();
        let mut stats = Vec::new();

        let latency_stats = self.format_latency_stats();
        if !latency_stats.is_empty() {
            stats.push(latency_stats);
        }

        let throughput_stats = self.format_throughput_stats();
        if !throughput_stats.is_empty() {
            stats.push(throughput_stats);
        }

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

    pub fn identifier_with_cpu_and_version(&self) -> String {
        format!(
            "{} @ {} (server {})",
            self.hardware.identifier.as_deref().unwrap_or("identifier"),
            self.hardware.cpu_name,
            self.params.gitref.as_deref().unwrap_or("version")
        )
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

    pub fn format_params(&self) -> String {
        let actors_info = self.params.format_actors_info();
        let message_batches = self.params.message_batches;
        let messages_per_batch = self.params.messages_per_batch;
        let message_size = self.params.message_size;

        let sent_msgs = self.total_messages_sent();
        let polled_msgs = self.total_messages_received();
        let total_bytes = self.total_bytes();

        let mut user_data_print = String::new();

        if sent_msgs > 0 && polled_msgs > 0 {
            user_data_print.push_str(&format!(
                "Sent {} Messages, Polled {} Messages, {} in Total",
                sent_msgs.human_count_bare(),
                polled_msgs.human_count_bare(),
                total_bytes.human_count_bytes(),
            ));
        } else if polled_msgs > 0 {
            user_data_print.push_str(&format!(
                "Polled {} Messages, {} in Total",
                polled_msgs.human_count_bare(),
                total_bytes.human_count_bytes(),
            ));
        } else {
            user_data_print.push_str(&format!(
                "Sent {} Messages, {} in Total",
                sent_msgs.human_count_bare(),
                total_bytes.human_count_bytes(),
            ));
        }

        let topics = "1 Topic per Stream".to_owned();
        let partitions = if self.params.partitions == 0 {
            "".to_owned()
        } else {
            format!("  •  {} Partitions per Topic", self.params.partitions)
        };
        let streams = format!("{} Streams", self.params.streams);

        format!(
            "{actors_info}  •  {streams}  •  {topics}{partitions}  •  {messages_per_batch} Msg/batch  •  {message_batches} Batches  •  {message_size} Bytes/msg  •  {user_data_print}",
        )
    }

    pub fn total_messages(&self) -> u64 {
        self.individual_metrics
            .iter()
            .map(|s| s.summary.total_messages)
            .sum()
    }

    pub fn total_messages_sent(&self) -> u64 {
        self.individual_metrics
            .iter()
            .filter(|s| s.summary.actor_kind != ActorKind::Consumer)
            .map(|s| s.summary.total_messages)
            .sum()
    }

    pub fn total_messages_received(&self) -> u64 {
        self.individual_metrics
            .iter()
            .filter(|s| s.summary.actor_kind != ActorKind::Producer)
            .map(|s| s.summary.total_messages)
            .sum()
    }

    pub fn total_bytes_sent(&self) -> u64 {
        self.individual_metrics
            .iter()
            .filter(|s| s.summary.actor_kind != ActorKind::Consumer)
            .map(|s| s.summary.total_user_data_bytes)
            .sum()
    }

    pub fn total_bytes_received(&self) -> u64 {
        self.individual_metrics
            .iter()
            .filter(|s| s.summary.actor_kind != ActorKind::Producer)
            .map(|s| s.summary.total_user_data_bytes)
            .sum()
    }

    pub fn total_bytes(&self) -> u64 {
        self.individual_metrics
            .iter()
            .map(|s| s.summary.total_user_data_bytes)
            .sum()
    }

    pub fn total_message_batches(&self) -> u64 {
        let batches = self
            .individual_metrics
            .iter()
            .map(|s| s.summary.total_message_batches)
            .sum();

        if batches == 0 {
            self.params.message_batches
        } else {
            batches
        }
    }
}

impl BenchmarkGroupMetricsLight {
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
            "{} Latency  •  Avg: {:.2} ms  •  Med: {:.2} ms  •  P95: {:.2} ms  •  P99: {:.2} ms  •  P999: {:.2} ms  •  P9999: {:.2} ms  •  Min: {:.2} ms  •  Max: {:.2} ms  •  Std Dev: {:.2} ms",
            self.summary.kind,
            self.summary.average_latency_ms,
            self.summary.average_median_latency_ms,
            self.summary.average_p95_latency_ms,
            self.summary.average_p99_latency_ms,
            self.summary.average_p999_latency_ms,
            self.summary.average_p9999_latency_ms,
            self.summary.min_latency_ms,
            self.summary.max_latency_ms,
            self.summary.std_dev_latency_ms,
        )
    }
}
