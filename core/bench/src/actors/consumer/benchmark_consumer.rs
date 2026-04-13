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

use crate::actors::consumer::client::BenchmarkConsumerClient;
use crate::actors::consumer::client::interface::BenchmarkConsumerConfig;
use crate::analytics::metrics::individual::from_records;
use crate::analytics::record::BenchmarkRecord;
use crate::utils::finish_condition::BenchmarkFinishCondition;
use crate::utils::rate_limiter::BenchmarkRateLimiter;
use bench_report::actor_kind::ActorKind;
use bench_report::benchmark_kind::BenchmarkKind;
use bench_report::individual_metrics::BenchmarkIndividualMetrics;
use bench_report::numeric_parameter::BenchmarkNumericParameter;
use bench_report::utils::{WIDE_LAYOUT_THRESHOLD, get_terminal_width};
use comfy_table::presets::UTF8_FULL;
use comfy_table::{ContentArrangement, Table};
use human_repr::HumanCount;
use iggy::prelude::*;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tracing::info;

pub struct BenchmarkConsumer<C: BenchmarkConsumerClient> {
    pub client: C,
    pub benchmark_kind: BenchmarkKind,
    pub finish_condition: Arc<BenchmarkFinishCondition>,
    pub sampling_time: IggyDuration,
    pub moving_average_window: u32,
    pub limit_bytes_per_second: Option<IggyByteSize>,
    pub config: BenchmarkConsumerConfig,
}

impl<C: BenchmarkConsumerClient> BenchmarkConsumer<C> {
    pub const fn new(
        client: C,
        benchmark_kind: BenchmarkKind,
        finish_condition: Arc<BenchmarkFinishCondition>,
        sampling_time: IggyDuration,
        moving_average_window: u32,
        limit_bytes_per_second: Option<IggyByteSize>,
        config: BenchmarkConsumerConfig,
    ) -> Self {
        Self {
            client,
            benchmark_kind,
            finish_condition,
            sampling_time,
            moving_average_window,
            limit_bytes_per_second,
            config,
        }
    }

    #[allow(clippy::too_many_lines)]
    #[allow(clippy::cognitive_complexity)]
    pub async fn run(mut self) -> Result<BenchmarkIndividualMetrics, IggyError> {
        self.client.setup().await?;

        if self.config.warmup_time.get_duration() != Duration::from_millis(0) {
            self.log_warmup_info();
            let warmup_end = Instant::now() + self.config.warmup_time.get_duration();
            while Instant::now() < warmup_end {
                let _ = self.client.consume_batch().await?;
            }
        }

        self.log_setup_info();

        let max_capacity = self.finish_condition.max_capacity();
        let mut records = Vec::with_capacity(max_capacity);
        let mut messages_processed = 0;
        let mut batches_processed = 0;
        let mut bytes_processed = 0;
        let mut user_data_bytes_processed = 0;
        let start_timestamp = Instant::now();
        let rate_limiter = self.limit_bytes_per_second.map(BenchmarkRateLimiter::new);

        while !self.finish_condition.is_done() {
            let batch_opt = self.client.consume_batch().await?;

            let Some(batch) = batch_opt else {
                continue;
            };

            messages_processed += u64::from(batch.messages);
            batches_processed += 1;
            user_data_bytes_processed += batch.user_data_bytes;
            bytes_processed += batch.total_bytes;

            records.push(BenchmarkRecord {
                elapsed_time_us: u64::try_from(start_timestamp.elapsed().as_micros())
                    .unwrap_or(u64::MAX),
                latency_us: u64::try_from(batch.latency.as_micros()).unwrap_or(u64::MAX),
                messages: messages_processed,
                message_batches: batches_processed,
                user_data_bytes: user_data_bytes_processed,
                total_bytes: bytes_processed,
            });

            if let Some(rate_limiter) = &rate_limiter {
                rate_limiter
                    .wait_until_necessary(batch.user_data_bytes)
                    .await;
            }

            if self
                .finish_condition
                .account_and_check(batch.user_data_bytes)
            {
                break;
            }
        }

        let metrics = from_records(
            &records,
            self.benchmark_kind,
            ActorKind::Consumer,
            self.config.consumer_id,
            self.sampling_time,
            self.moving_average_window,
        );

        Self::log_statistics(
            self.config.consumer_id,
            messages_processed,
            u32::try_from(batches_processed).unwrap_or(u32::MAX),
            &self.config.messages_per_batch,
            &metrics,
            self.config.pretty,
        );

        Ok(metrics)
    }

    pub fn log_setup_info(&self) {
        if let Some(cg_id) = self.config.consumer_group_id {
            info!(
                "Consumer #{}, part of consumer group #{} → polling in {} messages per batch from stream {}, using {}...",
                self.config.consumer_id,
                cg_id,
                self.config.messages_per_batch,
                self.config.stream_id,
                C::API_LABEL,
            );
        } else {
            info!(
                "Consumer #{} → polling in {} messages per batch from stream {}, using {}...",
                self.config.consumer_id,
                self.config.messages_per_batch,
                self.config.stream_id,
                C::API_LABEL,
            );
        }
    }

    pub fn log_warmup_info(&self) {
        if let Some(cg_id) = self.config.consumer_group_id {
            info!(
                "Consumer #{}, group #{} → warming up for {}",
                self.config.consumer_id, cg_id, self.config.warmup_time,
            );
        } else {
            info!(
                "Consumer #{} → warming up for {}",
                self.config.consumer_id, self.config.warmup_time,
            );
        }
    }

    pub fn log_statistics(
        consumer_id: u32,
        total_messages: u64,
        message_batches: u32,
        messages_per_batch: &BenchmarkNumericParameter,
        metrics: &BenchmarkIndividualMetrics,
        pretty: bool,
    ) {
        if pretty {
            let width = get_terminal_width();

            if width >= WIDE_LAYOUT_THRESHOLD {
                Self::print_wide_layout(
                    consumer_id,
                    total_messages,
                    message_batches,
                    messages_per_batch,
                    metrics,
                );
            } else {
                Self::print_narrow_layout(
                    consumer_id,
                    total_messages,
                    message_batches,
                    messages_per_batch,
                    metrics,
                );
            }
        } else {
            info!(
                "Consumer #{} → polled {} messages, {} batches of {} messages in {:.2} s, total size: {}, average throughput: {:.2} MB/s, \
            p50 latency: {:.2} ms, p90 latency: {:.2} ms, p95 latency: {:.2} ms, p99 latency: {:.2} ms, p999 latency: {:.2} ms, \
            p9999 latency: {:.2} ms, average latency: {:.2} ms, median latency: {:.2} ms, min latency: {:.2} ms, max latency: {:.2} ms, std dev latency: {:.2} ms",
                consumer_id,
                total_messages.human_count_bare(),
                message_batches.human_count_bare(),
                messages_per_batch,
                metrics.summary.total_time_secs,
                IggyByteSize::from(metrics.summary.total_user_data_bytes),
                metrics.summary.throughput_megabytes_per_second,
                metrics.summary.p50_latency_ms,
                metrics.summary.p90_latency_ms,
                metrics.summary.p95_latency_ms,
                metrics.summary.p99_latency_ms,
                metrics.summary.p999_latency_ms,
                metrics.summary.p9999_latency_ms,
                metrics.summary.avg_latency_ms,
                metrics.summary.median_latency_ms,
                metrics.summary.min_latency_ms,
                metrics.summary.max_latency_ms,
                metrics.summary.std_dev_latency_ms,
            );
        }
    }

    fn print_wide_layout(
        consumer_id: u32,
        total_messages: u64,
        message_batches: u32,
        messages_per_batch: &BenchmarkNumericParameter,
        metrics: &BenchmarkIndividualMetrics,
    ) {
        let mut summary_table = Table::new();
        summary_table
            .load_preset(UTF8_FULL)
            .set_content_arrangement(ContentArrangement::Dynamic);

        summary_table.add_row(vec![
            format!("Consumer #{}", consumer_id),
            format!("{} msgs", total_messages.human_count_bare()),
            format!(
                "{} x {}",
                message_batches.human_count_bare(),
                messages_per_batch
            ),
            IggyByteSize::from(metrics.summary.total_user_data_bytes).to_string(),
            format!("{:.2} s", metrics.summary.total_time_secs),
            format!(
                "{:.2} MB/s",
                metrics.summary.throughput_megabytes_per_second
            ),
        ]);

        println!("\n{summary_table}");

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
            format!("{:.2}", metrics.summary.p50_latency_ms),
            format!("{:.2}", metrics.summary.p90_latency_ms),
            format!("{:.2}", metrics.summary.p95_latency_ms),
            format!("{:.2}", metrics.summary.p99_latency_ms),
            format!("{:.2}", metrics.summary.p999_latency_ms),
            format!("{:.2}", metrics.summary.p9999_latency_ms),
            format!("{:.2}", metrics.summary.avg_latency_ms),
            format!("{:.2}", metrics.summary.median_latency_ms),
            format!("{:.2}", metrics.summary.min_latency_ms),
            format!("{:.2}", metrics.summary.max_latency_ms),
            format!("{:.2}", metrics.summary.std_dev_latency_ms),
        ]);

        println!("{latency_table}");
    }

    fn print_narrow_layout(
        consumer_id: u32,
        total_messages: u64,
        message_batches: u32,
        messages_per_batch: &BenchmarkNumericParameter,
        metrics: &BenchmarkIndividualMetrics,
    ) {
        let mut table = Table::new();
        table
            .load_preset(UTF8_FULL)
            .set_content_arrangement(ContentArrangement::Dynamic)
            .set_width(60);

        table.add_row(vec![format!("Consumer #{}", consumer_id), String::new()]);

        table.add_row(vec!["Summary", ""]);
        table.add_row(vec![
            "Messages (polled)".to_string(),
            total_messages.human_count_bare().to_string(),
        ]);
        table.add_row(vec![
            "Batches".to_string(),
            format!(
                "{} x {}",
                message_batches.human_count_bare(),
                messages_per_batch
            ),
        ]);
        table.add_row(vec![
            "Total Size".to_string(),
            IggyByteSize::from(metrics.summary.total_user_data_bytes).to_string(),
        ]);
        table.add_row(vec![
            "Duration".to_string(),
            format!("{:.2} s", metrics.summary.total_time_secs),
        ]);

        table.add_row(vec!["Throughput", ""]);
        table.add_row(vec![
            "MB/s".to_string(),
            format!("{:.2}", metrics.summary.throughput_megabytes_per_second),
        ]);

        table.add_row(vec!["Latency", ""]);
        table.add_row(vec![
            "p50".to_string(),
            format!("{:.2} ms", metrics.summary.p50_latency_ms),
        ]);
        table.add_row(vec![
            "p90".to_string(),
            format!("{:.2} ms", metrics.summary.p90_latency_ms),
        ]);
        table.add_row(vec![
            "p95".to_string(),
            format!("{:.2} ms", metrics.summary.p95_latency_ms),
        ]);
        table.add_row(vec![
            "p99".to_string(),
            format!("{:.2} ms", metrics.summary.p99_latency_ms),
        ]);
        table.add_row(vec![
            "p999".to_string(),
            format!("{:.2} ms", metrics.summary.p999_latency_ms),
        ]);
        table.add_row(vec![
            "p9999".to_string(),
            format!("{:.2} ms", metrics.summary.p9999_latency_ms),
        ]);
        table.add_row(vec![
            "avg".to_string(),
            format!("{:.2} ms", metrics.summary.avg_latency_ms),
        ]);
        table.add_row(vec![
            "median".to_string(),
            format!("{:.2} ms", metrics.summary.median_latency_ms),
        ]);
        table.add_row(vec![
            "min".to_string(),
            format!("{:.2} ms", metrics.summary.min_latency_ms),
        ]);
        table.add_row(vec![
            "max".to_string(),
            format!("{:.2} ms", metrics.summary.max_latency_ms),
        ]);
        table.add_row(vec![
            "std dev".to_string(),
            format!("{:.2} ms", metrics.summary.std_dev_latency_ms),
        ]);

        println!("\n{table}");
    }
}
