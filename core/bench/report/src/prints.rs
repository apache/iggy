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

        self.group_metrics
            .iter()
            .for_each(|s| println!("\n{}", s.formatted_string(pretty)));
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

impl BenchmarkGroupMetrics {
    pub fn formatted_string(&self, pretty: bool) -> String {
        if pretty {
            let width = get_terminal_width();
            if width >= WIDE_LAYOUT_THRESHOLD {
                self.format_wide_layout()
            } else {
                self.format_narrow_layout()
            }
        } else {
            self.format_original()
        }
    }

    fn format_original(&self) -> String {
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
        let total_test_time = format!(
            "{:.2}",
            self.avg_throughput_mb_ts.points.last().unwrap().time_s
        );

        format!(
        "{prefix}: Total throughput: {total_mb} MB/s, {total_msg} messages/s, average throughput per {actor}: {avg_mb} MB/s, \
        p50 latency: {p50} ms, p90 latency: {p90} ms, p95 latency: {p95} ms, \
        p99 latency: {p99} ms, p999 latency: {p999} ms, p9999 latency: {p9999} ms, average latency: {avg} ms, \
        median latency: {median} ms, min: {min} ms, max: {max} ms, std dev: {std_dev} ms, total time: {total_test_time} s"
    )
    .color(color)
    .to_string()
    }

    fn format_wide_layout(&self) -> String {
        let prefix = match self.summary.kind {
            GroupMetricsKind::Producers => "Producers Results",
            GroupMetricsKind::Consumers => "Consumers Results",
            GroupMetricsKind::ProducersAndConsumers => "Aggregate Results",
            GroupMetricsKind::ProducingConsumers => "Producing Consumer Results",
        };
        let actor = self.summary.kind.actor();

        let mut summary_table = Table::new();
        summary_table
            .load_preset(UTF8_FULL)
            .set_content_arrangement(ContentArrangement::Dynamic);

        summary_table.add_row(vec![
            prefix.to_string(),
            format!(
                "{:.2} s",
                self.avg_throughput_mb_ts.points.last().unwrap().time_s
            ),
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
            .load_preset(UTF8_FULL)
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

    fn format_narrow_layout(&self) -> String {
        let prefix = match self.summary.kind {
            GroupMetricsKind::Producers => "Producers Results",
            GroupMetricsKind::Consumers => "Consumers Results",
            GroupMetricsKind::ProducersAndConsumers => "Aggregate Results",
            GroupMetricsKind::ProducingConsumers => "Producing Consumer Results",
        };
        let actor = self.summary.kind.actor();

        let mut table = Table::new();
        table
            .load_preset(UTF8_FULL)
            .set_content_arrangement(ContentArrangement::Dynamic)
            .set_width(60);

        table.add_row(vec![prefix.to_string(), String::new()]);

        table.add_row(vec!["Summary", ""]);
        table.add_row(vec![
            "Total Time".to_string(),
            format!(
                "{:.2} s",
                self.avg_throughput_mb_ts.points.last().unwrap().time_s
            ),
        ]);

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
