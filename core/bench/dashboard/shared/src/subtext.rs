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

use crate::metrics::{GroupMetrics, is_member};
use crate::{BenchmarkGroupMetricsLight, BenchmarkIndividualMetricsLight, BenchmarkReportLight};
use bench_report::{
    actor_kind::ActorKind, benchmark_kind::BenchmarkKind, group_metrics_kind::GroupMetricsKind,
};
use human_repr::HumanCount;

impl BenchmarkReportLight {
    /// The subtext of the embed charts. The throughput lines give the group rates of the dashboard
    /// pages, not the report's sums of the actors' own rates.
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
        ) && let Some(total) = self.group_metrics.iter().find(|s| {
            s.summary.kind == GroupMetricsKind::ProducersAndConsumers
                || s.summary.kind == GroupMetricsKind::ProducingConsumers
        }) {
            let total = GroupMetrics::new(total, &self.individual_metrics);
            stats.push(format!(
                "Total System Throughput: {:.2} MB/s, {:.0} msg/s",
                total.throughput_megabytes_per_second, total.throughput_messages_per_second
            ));
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
            .map(|summary| summary.format_throughput_per_actor_kind(&self.individual_metrics))
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
        let cluster = match &self.cluster {
            Some(info) => format!("  •  {}", info.label()),
            None => String::new(),
        };

        format!(
            "{actors_info}  •  {streams}  •  {topics}{partitions}  •  {messages_per_batch} Msg/batch  •  {message_batches} Batches  •  {message_size} Bytes/msg  •  {user_data_print}{cluster}",
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
    /// The average is the group rate divided by the number of actors in the group, so the averages
    /// add up to the total.
    fn format_throughput_per_actor_kind(
        &self,
        actors: &[BenchmarkIndividualMetricsLight],
    ) -> String {
        let group = GroupMetrics::new(self, actors);
        let members = actors
            .iter()
            .filter(|actor| is_member(self.summary.kind, actor.summary.actor_kind))
            .count();
        let per_actor = |total: f64| {
            if members > 0 {
                total / members as f64
            } else {
                0.0
            }
        };
        format!(
            "{} Throughput  •  Total: {:.2} MB/s, {:.0} msg/s  •  Avg Per {}: {:.2} MB/s, {:.0} msg/s",
            self.summary.kind,
            group.throughput_megabytes_per_second,
            group.throughput_messages_per_second,
            self.summary.kind.actor(),
            per_actor(group.throughput_megabytes_per_second),
            per_actor(group.throughput_messages_per_second),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::{CLUSTER_RUN, OLD_ERA_RUN, report};
    use bench_report::cluster::{BenchmarkClusterInfo, BenchmarkClusterNode};

    fn node(name: &str, role: &str) -> BenchmarkClusterNode {
        BenchmarkClusterNode {
            name: name.to_string(),
            role: role.to_string(),
            status: "healthy".to_string(),
        }
    }

    fn three_node_cluster() -> BenchmarkClusterInfo {
        BenchmarkClusterInfo {
            name: "vsr".to_string(),
            nodes: vec![
                node("node-1", "leader"),
                node("node-2", "follower"),
                node("node-3", "follower"),
            ],
        }
    }

    #[test]
    fn given_single_node_report_when_formatting_params_should_omit_cluster_segment() {
        let report = BenchmarkReportLight::default();
        assert!(!report.format_params().contains("cluster"));
    }

    #[test]
    fn given_cluster_report_when_formatting_params_should_append_one_label_segment() {
        let single = BenchmarkReportLight::default();
        let clustered = BenchmarkReportLight {
            cluster: Some(three_node_cluster()),
            ..Default::default()
        };
        let expected = format!(
            "{}  •  {}",
            single.format_params(),
            three_node_cluster().label()
        );
        assert_eq!(clustered.format_params(), expected);
    }

    /// The report sums the actors' own rates to 801.85 MB/s. The slowest of the 20 producers ran
    /// 2 % longer than the fastest, so the group moved 784.25 MB/s.
    #[test]
    fn given_actors_finishing_apart_when_writing_subtext_should_print_dashboard_group_rate() {
        let subtext = report(CLUSTER_RUN).subtext();

        assert!(
            subtext.contains(
                "Producers Throughput  •  Total: 784.25 MB/s, 784254 msg/s  •  \
                 Avg Per Producer: 39.21 MB/s, 39213 msg/s"
            ),
            "{subtext}"
        );
        assert!(!subtext.contains("801.85"), "{subtext}");
    }

    #[test]
    fn given_producer_and_consumer_run_when_writing_subtext_should_print_dashboard_system_rate() {
        let subtext = report(OLD_ERA_RUN).subtext();

        for line in [
            "Producers Throughput  •  Total: 1415.68 MB/s, 1415679 msg/s  •  \
             Avg Per Producer: 176.96 MB/s, 176960 msg/s",
            "Consumers Throughput  •  Total: 1414.93 MB/s, 1414927 msg/s  •  \
             Avg Per Consumer: 176.87 MB/s, 176866 msg/s",
            "Total System Throughput: 2829.85 MB/s, 2829855 msg/s",
        ] {
            assert!(subtext.lines().any(|text| text == line), "{subtext}");
        }
    }
}
