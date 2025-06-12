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
    actors::producer::backend::{
        BenchmarkProducerBackend, BenchmarkProducerConfig, HighLevelBackend, LowLevelBackend,
        ProducerBackend,
    },
    analytics::{metrics::individual::from_records, record::BenchmarkRecord},
    utils::{
        batch_generator::BenchmarkBatchGenerator, finish_condition::BenchmarkFinishCondition,
        rate_limiter::BenchmarkRateLimiter,
    },
};
use bench_report::actor_kind::ActorKind;
use bench_report::benchmark_kind::BenchmarkKind;
use bench_report::individual_metrics::BenchmarkIndividualMetrics;
use bench_report::numeric_parameter::BenchmarkNumericParameter;
use human_repr::HumanCount;
use iggy::prelude::*;
use integration::test_server::ClientFactory;
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;
use tracing::info;

pub struct BenchmarkProducer {
    pub backend: ProducerBackend,
    pub benchmark_kind: BenchmarkKind,
    pub finish_condition: Arc<BenchmarkFinishCondition>,
    pub sampling_time: IggyDuration,
    pub moving_average_window: u32,
    pub limit_bytes_per_second: Option<IggyByteSize>,
    pub config: BenchmarkProducerConfig,
}

impl BenchmarkProducer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client_factory: Arc<dyn ClientFactory>,
        benchmark_kind: BenchmarkKind,
        producer_id: u32,
        stream_id: u32,
        partitions: u32,
        messages_per_batch: BenchmarkNumericParameter,
        message_size: BenchmarkNumericParameter,
        finish_condition: Arc<BenchmarkFinishCondition>,
        warmup_time: IggyDuration,
        sampling_time: IggyDuration,
        moving_average_window: u32,
        limit_bytes_per_second: Option<IggyByteSize>,
        use_high_level_api: bool,
    ) -> Self {
        let config = BenchmarkProducerConfig {
            producer_id,
            stream_id,
            partitions,
            messages_per_batch,
            message_size,
            warmup_time,
        };

        let backend = if use_high_level_api {
            ProducerBackend::HighLevel(HighLevelBackend::new(client_factory, config.clone()))
        } else {
            ProducerBackend::LowLevel(LowLevelBackend::new(client_factory, config.clone()))
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
            ProducerBackend::LowLevel(backend) => {
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
            ProducerBackend::HighLevel(backend) => {
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

    async fn run_with_backend<B: BenchmarkProducerBackend>(
        benchmark_kind: BenchmarkKind,
        finish_condition: Arc<BenchmarkFinishCondition>,
        sampling_time: IggyDuration,
        moving_average_window: u32,
        limit_bytes_per_second: Option<IggyByteSize>,
        config: BenchmarkProducerConfig,
        backend: B,
    ) -> Result<BenchmarkIndividualMetrics, IggyError> {
        info!(
            "Producer #{} → sending {} ({} msgs/batch) on stream {}, rate limit: {:?}",
            config.producer_id,
            finish_condition.total_str(),
            config.messages_per_batch,
            config.stream_id,
            limit_bytes_per_second,
        );
        let mut producer = backend.setup().await?;
        let mut batch_generator =
            BenchmarkBatchGenerator::new(config.message_size, config.messages_per_batch);

        if config.warmup_time.get_duration() != Duration::from_millis(0) {
            backend.log_warmup_info();
            backend.warmup(&mut producer, &mut batch_generator).await?;
        }

        backend.log_setup_info();

        let max_capacity = finish_condition.max_capacity();
        let mut records = Vec::with_capacity(max_capacity);
        let mut messages_processed = 0;
        let mut batches_processed = 0;
        let mut user_data_bytes_processed = 0;
        let mut total_bytes_processed = 0;

        let rate_limiter = limit_bytes_per_second.map(BenchmarkRateLimiter::new);
        let start_timestamp = Instant::now();

        while !finish_condition.is_done() {
            let batch_opt = backend
                .produce_batch(&mut producer, &mut batch_generator)
                .await?;

            let Some(batch) = batch_opt else {
                continue;
            };

            messages_processed += u64::from(batch.messages);
            batches_processed += 1;
            user_data_bytes_processed += batch.user_data_bytes;
            total_bytes_processed += batch.total_bytes;

            records.push(BenchmarkRecord {
                elapsed_time_us: u64::try_from(start_timestamp.elapsed().as_micros())
                    .unwrap_or(u64::MAX),
                latency_us: u64::try_from(batch.latency.as_micros()).unwrap_or(u64::MAX),
                messages: messages_processed,
                message_batches: batches_processed,
                user_data_bytes: user_data_bytes_processed,
                total_bytes: total_bytes_processed,
            });

            if let Some(rate_limiter) = &rate_limiter {
                rate_limiter
                    .wait_until_necessary(batch.user_data_bytes)
                    .await;
            }
            if finish_condition.account_and_check(batch.user_data_bytes) {
                info!(
                    "Producer #{} → finished sending {} messages in {} batches ({} user bytes, {} total bytes), send finish condition: {}",
                    config.producer_id,
                    messages_processed.human_count_bare(),
                    batches_processed.human_count_bare(),
                    user_data_bytes_processed.human_count_bytes(),
                    total_bytes_processed.human_count_bytes(),
                    finish_condition.status(),
                );
                break;
            }
        }

        let metrics = from_records(
            &records,
            benchmark_kind,
            ActorKind::Producer,
            config.producer_id,
            sampling_time,
            moving_average_window,
        );

        Self::log_statistics(
            config.producer_id,
            messages_processed,
            batches_processed,
            &config.messages_per_batch,
            &metrics,
        );

        Ok(metrics)
    }
    fn log_statistics(
        producer_id: u32,
        total_messages: u64,
        message_batches: u64,
        messages_per_batch: &BenchmarkNumericParameter,
        metrics: &BenchmarkIndividualMetrics,
    ) {
        info!(
            "Producer #{} → sent {} messages in {} batches of {} messages in {:.2} s, total size: {}, average throughput: {:.2} MB/s, \
    p50 latency: {:.2} ms, p90 latency: {:.2} ms, p95 latency: {:.2} ms, p99 latency: {:.2} ms, p999 latency: {:.2} ms, p9999 latency: {:.2} ms, \
    average latency: {:.2} ms, median latency: {:.2} ms, min latency: {:.2} ms, max latency: {:.2} ms, std dev latency: {:.2} ms",
            producer_id,
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
