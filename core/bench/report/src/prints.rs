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

use colored::{Color, Colorize};
use comfy_table::{ContentArrangement, Table, presets::UTF8_FULL};
use human_repr::HumanCount;
use tracing::info;

use crate::{
    actor_kind::ActorKind,
    benchmark_kind::BenchmarkKind,
    group_metrics::BenchmarkGroupMetrics,
    group_metrics_kind::GroupMetricsKind,
    report::BenchmarkReport,
    utils::{WIDE_LAYOUT_THRESHOLD, get_terminal_width},
};

impl BenchmarkReport {
    pub fn print_summary(&self, pretty: bool) {
        let kind = self.params.benchmark_kind;
        let total_messages = format!("{} messages, ", self.total_messages());
        let total_size = format!(
            "{} of data processed",
            self.total_bytes().human_count_bytes()
        );

        let streams = format!("{} streams, ", self.params.streams);
        // TODO: make this configurable
        let topics = "1 topic per stream, ";
        let messages_per_batch = format!("{} messages per batch, ", self.params.messages_per_batch);
        let message_batches = format!("{} message batches, ", self.params.message_batches);
        let message_size = format!("{} bytes per message, ", self.params.message_size);
        let producers = if self.params.producers == 0 {
            "".to_owned()
        } else if self.params.benchmark_kind == BenchmarkKind::EndToEndProducingConsumerGroup
            || self.params.benchmark_kind == BenchmarkKind::EndToEndProducingConsumer
        {
            format!("{} producing consumers, ", self.params.producers)
        } else {
            format!("{} producers, ", self.params.producers)
        };
        let consumers = if self.params.consumers == 0 {
            "".to_owned()
        } else {
            format!("{} consumers, ", self.params.consumers)
        };
        let partitions = if self.params.partitions == 0 {
            "".to_owned()
        } else {
            format!("{} partitions per topic, ", self.params.partitions)
        };
        let consumer_groups = if self.params.consumer_groups == 0 {
            "".to_owned()
        } else {
            format!("{} consumer groups, ", self.params.consumer_groups)
        };
        println!();
        let params_print = format!("Benchmark: {kind}, {producers}{consumers}{streams}{topics}{partitions}{consumer_groups}{total_messages}{messages_per_batch}{message_batches}{message_size}{total_size}\n",).blue();

        info!("{}", params_print);

        for group in &self.group_metrics {
            println!(
                "\n{}",
                group.formatted_string_with_duration(pretty, self.group_duration(group))
            );
        }
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
    fn group_duration(&self, group: &BenchmarkGroupMetrics) -> f64 {
        self.individual_metrics
            .iter()
            .filter(|metrics| match group.summary.kind {
                GroupMetricsKind::Producers => metrics.summary.actor_kind == ActorKind::Producer,
                GroupMetricsKind::Consumers => metrics.summary.actor_kind == ActorKind::Consumer,
                GroupMetricsKind::ProducingConsumers => {
                    metrics.summary.actor_kind == ActorKind::ProducingConsumer
                }
                GroupMetricsKind::ProducersAndConsumers => {
                    metrics.summary.actor_kind != ActorKind::ProducingConsumer
                }
            })
            .map(|metrics| metrics.summary.total_time_secs)
            .reduce(f64::max)
            .unwrap_or_else(|| group.time_series_duration())
    }
}

impl BenchmarkGroupMetrics {
    pub fn formatted_string(&self, pretty: bool) -> String {
        self.formatted_string_with_duration(pretty, self.time_series_duration())
    }

    fn formatted_string_with_duration(&self, pretty: bool, duration: f64) -> String {
        if pretty {
            let width = get_terminal_width();
            if width >= WIDE_LAYOUT_THRESHOLD {
                self.format_wide_layout(duration)
            } else {
                self.format_narrow_layout(duration)
            }
        } else {
            self.format_original(duration)
        }
    }

    fn time_series_duration(&self) -> f64 {
        [
            &self.avg_throughput_mb_ts,
            &self.avg_throughput_msg_ts,
            &self.avg_latency_ts,
        ]
        .into_iter()
        .filter_map(|series| series.points.last())
        .map(|point| point.time_s)
        .reduce(f64::max)
        .unwrap_or(0.0)
    }

    fn format_original(&self, duration: f64) -> String {
        let (prefix, color) = match self.summary.kind {
            GroupMetricsKind::Producers => ("Producers Results", Color::Green),
            GroupMetricsKind::Consumers => ("Consumers Results", Color::Green),
            GroupMetricsKind::ProducersAndConsumers => ("Aggregate Results", Color::Red),
            GroupMetricsKind::ProducingConsumers => ("Producing Consumer Results", Color::Red),
        };
        let actor = self.summary.kind.actor();
        let total_mb = format!("{:.2}", self.summary.total_throughput_megabytes_per_second);
        let total_msg = format!("{:.0}", self.summary.total_throughput_messages_per_second);
        let avg_mb = format!(
            "{:.2}",
            self.summary.average_throughput_megabytes_per_second
        );
        let p50 = format!("{:.2}", self.summary.average_p50_latency_ms);
        let p90 = format!("{:.2}", self.summary.average_p90_latency_ms);
        let p95 = format!("{:.2}", self.summary.average_p95_latency_ms);
        let p99 = format!("{:.2}", self.summary.average_p99_latency_ms);
        let p999 = format!("{:.2}", self.summary.average_p999_latency_ms);
        let p9999 = format!("{:.2}", self.summary.average_p9999_latency_ms);
        let avg = format!("{:.2}", self.summary.average_latency_ms);
        let median = format!("{:.2}", self.summary.average_median_latency_ms);
        let min = format!("{:.2}", self.summary.min_latency_ms);
        let max = format!("{:.2}", self.summary.max_latency_ms);
        let std_dev = format!("{:.2}", self.summary.std_dev_latency_ms);
        let total_test_time = format!("{:.2}", duration);

        format!(
        "{prefix}: Total throughput: {total_mb} MB/s, {total_msg} messages/s, average throughput per {actor}: {avg_mb} MB/s, \
        p50 latency: {p50} ms, p90 latency: {p90} ms, p95 latency: {p95} ms, \
        p99 latency: {p99} ms, p999 latency: {p999} ms, p9999 latency: {p9999} ms, average latency: {avg} ms, \
        median latency: {median} ms, min: {min} ms, max: {max} ms, std dev: {std_dev} ms, total time: {total_test_time} s"
    )
    .color(color)
    .to_string()
    }

    fn format_wide_layout(&self, duration: f64) -> String {
        let prefix = match self.summary.kind {
            GroupMetricsKind::Producers => "Producers Results",
            GroupMetricsKind::Consumers => "Consumers Results",
            GroupMetricsKind::ProducersAndConsumers => "Aggregate Results",
            GroupMetricsKind::ProducingConsumers => "Producing Consumer Results",
        };
        let actor = self.summary.kind.actor();

        let mut summary_table = Table::new();
        summary_table
            .load_style(UTF8_FULL)
            .set_content_arrangement(ContentArrangement::Dynamic);

        summary_table.add_row(vec![
            prefix.to_string(),
            format!("{:.2} s", duration),
            format!(
                "{:.2} MB/s",
                self.summary.total_throughput_megabytes_per_second
            ),
            format!(
                "{:.0} msg/s",
                self.summary.total_throughput_messages_per_second
            ),
            format!(
                "{:.2} MB/s per {}",
                self.summary.average_throughput_megabytes_per_second, actor
            ),
        ]);

        let mut latency_table = Table::new();
        latency_table
            .load_style(UTF8_FULL)
            .set_content_arrangement(ContentArrangement::Dynamic);

        latency_table.add_row(vec![
            "Latency", "p50", "p90", "p95", "p99", "p999", "p9999", "avg", "median", "min", "max",
            "std dev",
        ]);
        latency_table.add_row(vec![
            "(ms)".to_string(),
            format!("{:.2}", self.summary.average_p50_latency_ms),
            format!("{:.2}", self.summary.average_p90_latency_ms),
            format!("{:.2}", self.summary.average_p95_latency_ms),
            format!("{:.2}", self.summary.average_p99_latency_ms),
            format!("{:.2}", self.summary.average_p999_latency_ms),
            format!("{:.2}", self.summary.average_p9999_latency_ms),
            format!("{:.2}", self.summary.average_latency_ms),
            format!("{:.2}", self.summary.average_median_latency_ms),
            format!("{:.2}", self.summary.min_latency_ms),
            format!("{:.2}", self.summary.max_latency_ms),
            format!("{:.2}", self.summary.std_dev_latency_ms),
        ]);

        format!("\n{}\n{}", summary_table, latency_table)
    }

    fn format_narrow_layout(&self, duration: f64) -> String {
        let prefix = match self.summary.kind {
            GroupMetricsKind::Producers => "Producers Results",
            GroupMetricsKind::Consumers => "Consumers Results",
            GroupMetricsKind::ProducersAndConsumers => "Aggregate Results",
            GroupMetricsKind::ProducingConsumers => "Producing Consumer Results",
        };
        let actor = self.summary.kind.actor();

        let mut table = Table::new();
        table
            .load_style(UTF8_FULL)
            .set_content_arrangement(ContentArrangement::Dynamic)
            .set_width(60);

        table.add_row(vec![prefix.to_string(), String::new()]);

        table.add_row(vec!["Summary", ""]);
        table.add_row(vec!["Total Time".to_string(), format!("{:.2} s", duration)]);

        table.add_row(vec!["Throughput", ""]);
        table.add_row(vec![
            "Total (MB/s)".to_string(),
            format!("{:.2}", self.summary.total_throughput_megabytes_per_second),
        ]);
        table.add_row(vec![
            "Total (msg/s)".to_string(),
            format!("{:.0}", self.summary.total_throughput_messages_per_second),
        ]);
        table.add_row(vec![
            format!("Avg per {} (MB/s)", actor),
            format!(
                "{:.2}",
                self.summary.average_throughput_megabytes_per_second
            ),
        ]);

        table.add_row(vec!["Latency", ""]);
        table.add_row(vec![
            "p50".to_string(),
            format!("{:.2} ms", self.summary.average_p50_latency_ms),
        ]);
        table.add_row(vec![
            "p90".to_string(),
            format!("{:.2} ms", self.summary.average_p90_latency_ms),
        ]);
        table.add_row(vec![
            "p95".to_string(),
            format!("{:.2} ms", self.summary.average_p95_latency_ms),
        ]);
        table.add_row(vec![
            "p99".to_string(),
            format!("{:.2} ms", self.summary.average_p99_latency_ms),
        ]);
        table.add_row(vec![
            "p999".to_string(),
            format!("{:.2} ms", self.summary.average_p999_latency_ms),
        ]);
        table.add_row(vec![
            "p9999".to_string(),
            format!("{:.2} ms", self.summary.average_p9999_latency_ms),
        ]);
        table.add_row(vec![
            "avg".to_string(),
            format!("{:.2} ms", self.summary.average_latency_ms),
        ]);
        table.add_row(vec![
            "median".to_string(),
            format!("{:.2} ms", self.summary.average_median_latency_ms),
        ]);
        table.add_row(vec![
            "min".to_string(),
            format!("{:.2} ms", self.summary.min_latency_ms),
        ]);
        table.add_row(vec![
            "max".to_string(),
            format!("{:.2} ms", self.summary.max_latency_ms),
        ]);
        table.add_row(vec![
            "std dev".to_string(),
            format!("{:.2} ms", self.summary.std_dev_latency_ms),
        ]);

        format!("\n{}", table)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::{
        actor_kind::ActorKind, create_latency_chart, create_latency_distribution_chart,
        create_throughput_chart, group_metrics::BenchmarkGroupMetrics,
        individual_metrics::BenchmarkIndividualMetrics, report::BenchmarkReport,
    };

    #[test]
    fn empty_time_series_format_in_every_layout() {
        let group = group_without_time_series();
        assert!(group.formatted_string(false).contains("total time: 0.00 s"));
        assert!(
            group
                .format_wide_layout(group.time_series_duration())
                .contains("0.00 s")
        );
        assert!(
            group
                .format_narrow_layout(group.time_series_duration())
                .contains("0.00 s")
        );
    }

    #[test]
    fn report_duration_uses_matching_actors_when_time_series_are_empty() {
        let group = group_without_time_series();
        let mut consumer = individual_without_time_series();
        consumer.summary.actor_kind = ActorKind::Consumer;
        consumer.summary.total_time_secs = 9.0;
        let report = BenchmarkReport {
            group_metrics: vec![group.clone()],
            individual_metrics: vec![individual_without_time_series(), consumer],
            ..BenchmarkReport::default()
        };
        assert_eq!(report.group_duration(&group), 0.125);
        assert!(
            group
                .formatted_string_with_duration(false, report.group_duration(&group))
                .contains("total time: 0.12 s")
        );
        report.print_summary(false);
    }

    #[test]
    fn charts_accept_short_run_with_empty_time_series() {
        let report = BenchmarkReport {
            group_metrics: vec![group_without_time_series()],
            individual_metrics: vec![individual_without_time_series()],
            ..BenchmarkReport::default()
        };
        for chart in [
            create_throughput_chart(&report, false, false),
            create_latency_chart(&report, false, false),
            create_latency_distribution_chart(&report, false, false),
        ] {
            assert!(serde_json::to_value(chart).unwrap().is_object());
        }
    }

    fn group_without_time_series() -> BenchmarkGroupMetrics {
        serde_json::from_value(json!({
            "summary": {
                "kind": "producers",
                "total_throughput_megabytes_per_second": 1.0,
                "total_throughput_messages_per_second": 1.0,
                "average_throughput_megabytes_per_second": 1.0,
                "average_throughput_messages_per_second": 1.0,
                "average_p50_latency_ms": 1.0,
                "average_p90_latency_ms": 1.0,
                "average_p95_latency_ms": 1.0,
                "average_p99_latency_ms": 1.0,
                "average_p999_latency_ms": 1.0,
                "average_p9999_latency_ms": 1.0,
                "average_latency_ms": 1.0,
                "average_median_latency_ms": 1.0
            },
            "avg_throughput_mb_ts": { "points": [] },
            "avg_throughput_msg_ts": { "points": [] },
            "avg_latency_ts": { "points": [] }
        }))
        .unwrap()
    }

    fn individual_without_time_series() -> BenchmarkIndividualMetrics {
        serde_json::from_value(json!({
            "summary": {
                "benchmark_kind": "pinned_producer",
                "actor_kind": "producer",
                "actor_id": 1,
                "total_time_secs": 0.125,
                "total_user_data_bytes": 256,
                "total_bytes": 320,
                "total_messages": 1,
                "total_message_batches": 1,
                "throughput_megabytes_per_second": 1.0,
                "throughput_messages_per_second": 1.0,
                "p50_latency_ms": 1.0,
                "p90_latency_ms": 1.0,
                "p95_latency_ms": 1.0,
                "p99_latency_ms": 1.0,
                "p999_latency_ms": 1.0,
                "p9999_latency_ms": 1.0,
                "avg_latency_ms": 1.0,
                "median_latency_ms": 1.0
            },
            "throughput_mb_ts": { "points": [] },
            "throughput_msg_ts": { "points": [] },
            "latency_ts": { "points": [] }
        }))
        .unwrap()
    }
}
