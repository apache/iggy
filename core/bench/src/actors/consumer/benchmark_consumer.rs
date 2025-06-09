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

use super::backend::{
    BenchmarkConsumerBackend, BenchmarkConsumerConfig, ConsumerBackend, HighLevelBackend,
    LowLevelBackend,
};
use crate::analytics::metrics::individual::from_records;
use crate::analytics::record::BenchmarkRecord;
use crate::utils::finish_condition::BenchmarkFinishCondition;
use crate::utils::rate_limiter::BenchmarkRateLimiter;
use bench_report::actor_kind::ActorKind;
use bench_report::benchmark_kind::BenchmarkKind;
use bench_report::individual_metrics::BenchmarkIndividualMetrics;
use bench_report::numeric_parameter::BenchmarkNumericParameter;
use human_repr::HumanCount;
use iggy::prelude::*;
use integration::test_server::ClientFactory;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tracing::info;

pub struct BenchmarkConsumer {
    backend: ConsumerBackend,
    benchmark_kind: BenchmarkKind,
    finish_condition: Arc<BenchmarkFinishCondition>,
    sampling_time: IggyDuration,
    moving_average_window: u32,
    limit_bytes_per_second: Option<IggyByteSize>,
    config: BenchmarkConsumerConfig,
}

impl BenchmarkConsumer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client_factory: Arc<dyn ClientFactory>,
        benchmark_kind: BenchmarkKind,
        consumer_id: u32,
        consumer_group_id: Option<u32>,
        stream_id: u32,
        messages_per_batch: BenchmarkNumericParameter,
        finish_condition: Arc<BenchmarkFinishCondition>,
        warmup_time: IggyDuration,
        sampling_time: IggyDuration,
        moving_average_window: u32,
        polling_kind: PollingKind,
        limit_bytes_per_second: Option<IggyByteSize>,
        origin_timestamp_latency_calculation: bool,
        use_high_level_api: bool,
    ) -> Self {
        let config = BenchmarkConsumerConfig {
            consumer_id,
            consumer_group_id,
            stream_id,
            messages_per_batch,
            warmup_time,
            polling_kind,
            origin_timestamp_latency_calculation,
        };

        let backend = if use_high_level_api {
            ConsumerBackend::HighLevel(HighLevelBackend::new(client_factory, config.clone()))
        } else {
            ConsumerBackend::LowLevel(LowLevelBackend::new(client_factory, config.clone()))
        };

        Self {
            backend,
            benchmark_kind,
            finish_condition,
            sampling_time,
            moving_average_window,
            limit_bytes_per_second,
            config,
        }
    }

    pub async fn run(self) -> Result<BenchmarkIndividualMetrics, IggyError> {
        match self.backend {
            ConsumerBackend::LowLevel(backend) => {
                Self::run_with_backend(
                    self.benchmark_kind,
                    self.finish_condition,
                    self.sampling_time,
                    self.moving_average_window,
                    self.limit_bytes_per_second,
                    self.config,
                    backend,
                )
                .await
            }
            ConsumerBackend::HighLevel(backend) => {
                Self::run_with_backend(
                    self.benchmark_kind,
                    self.finish_condition,
                    self.sampling_time,
                    self.moving_average_window,
                    self.limit_bytes_per_second,
                    self.config,
                    backend,
                )
                .await
            }
        }
    }

    async fn run_with_backend<B: BenchmarkConsumerBackend>(
        benchmark_kind: BenchmarkKind,
        finish_condition: Arc<BenchmarkFinishCondition>,
        sampling_time: IggyDuration,
        moving_average_window: u32,
        limit_bytes_per_second: Option<IggyByteSize>,
        config: BenchmarkConsumerConfig,
        backend: B,
    ) -> Result<BenchmarkIndividualMetrics, IggyError> {
        let mut consumer = backend.setup().await?;

        if config.warmup_time.get_duration() != Duration::from_millis(0) {
            backend.log_warmup_info();
            backend.warmup(&mut consumer).await?;
        }

        backend.log_setup_info();

        let max_capacity = finish_condition.max_capacity();
        let mut records = Vec::with_capacity(max_capacity);
        let mut messages_processed = 0;
        let mut batches_processed = 0;
        let mut bytes_processed = 0;
        let mut user_data_bytes_processed = 0;
        let start_timestamp = Instant::now();
        let rate_limiter = limit_bytes_per_second.map(BenchmarkRateLimiter::new);

        while !finish_condition.is_done() {
            let batch_opt = backend.consume_batch(&mut consumer).await?;

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

            if finish_condition.account_and_check(batch.user_data_bytes) {
                break;
            }
        }

        let metrics = from_records(
            &records,
            benchmark_kind,
            ActorKind::Consumer,
            config.consumer_id,
            sampling_time,
            moving_average_window,
        );

        Self::log_statistics(
            config.consumer_id,
            messages_processed,
            u32::try_from(batches_processed).unwrap_or(u32::MAX),
            &config.messages_per_batch,
            &metrics,
        );

        Ok(metrics)
    }

    pub fn log_statistics(
        consumer_id: u32,
        total_messages: u64,
        message_batches: u32,
        messages_per_batch: &BenchmarkNumericParameter,
        metrics: &BenchmarkIndividualMetrics,
    ) {
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
