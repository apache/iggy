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
    actors::producer::client::{BenchmarkProducerClient, interface::BenchmarkProducerConfig},
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
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;
use tracing::info;

pub struct BenchmarkProducer<P: BenchmarkProducerClient> {
    pub client: P,
    pub benchmark_kind: BenchmarkKind,
    pub finish_condition: Arc<BenchmarkFinishCondition>,
    pub sampling_time: IggyDuration,
    pub moving_average_window: u32,
    pub limit_bytes_per_second: Option<IggyByteSize>,
    pub config: BenchmarkProducerConfig,
}

impl<P: BenchmarkProducerClient> BenchmarkProducer<P> {
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        client: P,
        benchmark_kind: BenchmarkKind,
        finish_condition: Arc<BenchmarkFinishCondition>,
        sampling_time: IggyDuration,
        moving_average_window: u32,
        limit_bytes_per_second: Option<IggyByteSize>,
        config: BenchmarkProducerConfig,
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
        info!(
            "Producer #{} → sending {} ({} msgs/batch) on stream {}, rate limit: {:?}",
            self.config.producer_id,
            self.finish_condition.total_str(),
            self.config.messages_per_batch,
            self.config.stream_id,
            self.limit_bytes_per_second,
        );

        self.client.setup().await?;

        let mut batch_generator =
            BenchmarkBatchGenerator::new(self.config.message_size, self.config.messages_per_batch);

        if self.config.warmup_time.get_duration() != Duration::from_millis(0) {
            self.log_warmup_info();
            let warmup_end = Instant::now() + self.config.warmup_time.get_duration();

            while Instant::now() < warmup_end {
                let _ = self.client.produce_batch(&mut batch_generator).await?;
            }
        }

        self.log_setup_info();

        let max_capacity = self.finish_condition.max_capacity();
        let mut records = Vec::with_capacity(max_capacity);
        let mut messages_processed = 0;
        let mut batches_processed = 0;
        let mut user_data_bytes_processed = 0;
        let mut total_bytes_processed = 0;

        let rate_limiter = self.limit_bytes_per_second.map(BenchmarkRateLimiter::new);
        let start_timestamp = Instant::now();

        while !self.finish_condition.is_done() {
            let batch_opt = self.client.produce_batch(&mut batch_generator).await?;

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

            if self
                .finish_condition
                .account_and_check(batch.user_data_bytes)
            {
                info!(
                    "Producer #{} → finished sending {} messages in {} batches ({} user bytes, {} total bytes), send finish condition: {}",
                    self.config.producer_id,
                    messages_processed.human_count_bare(),
                    batches_processed.human_count_bare(),
                    user_data_bytes_processed.human_count_bytes(),
                    total_bytes_processed.human_count_bytes(),
                    self.finish_condition.status(),
                );
                break;
            }
        }

        let metrics = from_records(
            &records,
            self.benchmark_kind,
            ActorKind::Producer,
            self.config.producer_id,
            self.sampling_time,
            self.moving_average_window,
        );

        Self::log_statistics(
            self.config.producer_id,
            messages_processed,
            batches_processed,
            &self.config.messages_per_batch,
            &metrics,
        );

        Ok(metrics)
    }

    fn log_setup_info(&self) {
        info!(
            "Producer #{} → sending {} messages per batch (each {} bytes) to stream {} across {} partition(s), using {}...",
            self.config.producer_id,
            self.config.messages_per_batch,
            self.config.message_size,
            self.config.stream_id,
            self.config.partitions,
            P::API_LABEL,
        );
    }

    fn log_warmup_info(&self) {
        info!(
            "Producer #{} → warming up for {}...",
            self.config.producer_id, self.config.warmup_time,
        );
    }

    fn log_statistics(
        producer_id: usize,
        total_messages: u64,
        message_batches: usize,
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
