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

use colored::{Color, ColoredString, Colorize};
use human_repr::HumanCount;
use tracing::info;

use crate::{
    benchmark_kind::BenchmarkKind, group_metrics::BenchmarkGroupMetrics,
    group_metrics_kind::GroupMetricsKind, report::BenchmarkReport,
};

impl BenchmarkReport {
    pub fn print_summary(&self) {
        let kind = self.params.benchmark_kind;
        let total_messages_sent: u64 = self.params.messages_per_batch as u64
            * self.params.message_batches as u64
            * self.params.producers as u64;
        let total_messages_received: u64 = self.params.messages_per_batch as u64
            * self.params.message_batches as u64
            * self.params.consumers as u64;
        let total_messages = total_messages_sent + total_messages_received;
        let total_size_bytes: u64 = total_messages * self.params.message_size as u64;
        let total_size = format!("{} of data processed", total_size_bytes.human_count_bytes());
        let total_messages = format!("{} messages processed, ", total_messages.human_count_bare());

        let streams = format!("{} streams, ", self.params.streams);
        // TODO: make this configurable
        let topics = "1 topic per stream, ";
        let messages_per_batch = format!("{} messages per batch, ", self.params.messages_per_batch);
        let message_batches = format!("{} message batches, ", self.params.message_batches);
        let message_size = format!(
            "{} per message, ",
            self.params.message_size.human_count_bytes()
        );
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
            .for_each(|s| info!("{}\n", s.formatted_string()));
    }
}

impl BenchmarkGroupMetrics {
    pub fn formatted_string(&self) -> ColoredString {
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
        let total_test_time = format!(
            "{:.2}",
            self.avg_throughput_mb_ts.points.last().unwrap().time_s
        );

        format!(
            "{}: Total throughput: {} MB/s, {} messages/s, average throughput per {}: {} MB/s, \
            p50 latency: {} ms, p90 latency: {} ms, p95 latency: {} ms, \
            p99 latency: {} ms, p999 latency: {} ms, p9999 latency: {} ms, average latency: {} ms, \
            median latency: {} ms, total time: {} s",
            prefix,
            total_mb,
            total_msg,
            actor,
            avg_mb,
            p50,
            p90,
            p95,
            p99,
            p999,
            p9999,
            avg,
            median,
            total_test_time
        )
        .color(color)
    }
}
