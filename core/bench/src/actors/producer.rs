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

use crate::analytics::metrics::individual::from_records;
use crate::analytics::record::BenchmarkRecord;
use crate::utils::batch_generator::BenchmarkBatchGenerator;
use crate::utils::finish_condition::BenchmarkFinishCondition;
use crate::utils::rate_limiter::BenchmarkRateLimiter;
use human_repr::HumanCount;
use iggy::prelude::*;
use iggy_bench_report::actor_kind::ActorKind;
use iggy_bench_report::benchmark_kind::BenchmarkKind;
use iggy_bench_report::individual_metrics::BenchmarkIndividualMetrics;
use iggy_bench_report::numeric_parameter::BenchmarkNumericParameter;
use integration::test_server::{login_root, ClientFactory};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tracing::info;

pub struct BenchmarkProducer {
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
    ) -> Self {
        BenchmarkProducer {
            client_factory,
            benchmark_kind,
            producer_id,
            stream_id,
            partitions,
            messages_per_batch,
            message_size,
            finish_condition,
            warmup_time,
            sampling_time,
            moving_average_window,
            limit_bytes_per_second,
        }
    }

    pub async fn run(self) -> Result<BenchmarkIndividualMetrics, IggyError> {
        let mut batch_generator =
            BenchmarkBatchGenerator::new(self.message_size, self.messages_per_batch);
        let rate_limiter = self.limit_bytes_per_second.map(BenchmarkRateLimiter::new);

        let topic_id: u32 = 1;
        let default_partition_id: u32 = 1;
        let partitions = self.partitions;
        let client = self.client_factory.create_client().await;
        let client = IggyClient::create(client, None, None);
        login_root(&client).await;

        let stream_id = self.stream_id.try_into()?;
        let topic_id = topic_id.try_into()?;
        let partitioning = match partitions {
            0 => panic!("Partition count must be greater than 0"),
            1 => Partitioning::partition_id(default_partition_id),
            2.. => Partitioning::balanced(),
        };

        // -----------------------
        // WARM-UP
        // -----------------------

        if self.warmup_time.get_duration() != Duration::from_millis(0) {
            info!(
                "Producer #{} → warming up for {}...",
                self.producer_id, self.warmup_time
            );
            let warmup_end = Instant::now() + self.warmup_time.get_duration();

            while Instant::now() < warmup_end {
                let batch = batch_generator.generate_batch();
                client
                    .send_messages(&stream_id, &topic_id, &partitioning, &mut batch.messages)
                    .await?;
            }
        }
        // -----------------------
        // MAIN BENCHMARK
        // -----------------------

        info!(
            "Producer #{} → sending {} in batches of {} messages to stream {} with {} partitions, partitioning: {}, rate limit: {:?}...",
            self.producer_id,
            self.finish_condition.total_str(),
            self.messages_per_batch,
            stream_id,
            partitions,
            partitioning,
            self.limit_bytes_per_second
        );

        let max_capacity = self.finish_condition.max_capacity();
        let mut latencies: Vec<Duration> = Vec::with_capacity(max_capacity);
        let mut records: Vec<BenchmarkRecord> = Vec::with_capacity(max_capacity);
        let mut messages_processed = 0;
        let mut batches_processed = 0;
        let mut total_bytes_processed = 0;
        let mut user_data_bytes_processed = 0;
        let start_timestamp = Instant::now();

        loop {
            if self.finish_condition.is_done() {
                break;
            }
            let batch = batch_generator.generate_batch();
            let before_send = Instant::now();
            client
                .send_messages(&stream_id, &topic_id, &partitioning, &mut batch.messages)
                .await?;
            let latency = before_send.elapsed();

            messages_processed += batch.messages.len() as u64;
            batches_processed += 1;
            user_data_bytes_processed += batch.user_data_bytes;
            total_bytes_processed += batch.total_bytes;

            latencies.push(latency);
            records.push(BenchmarkRecord {
                elapsed_time_us: start_timestamp.elapsed().as_micros() as u64,
                latency_us: latency.as_micros() as u64,
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
                break;
            }
        }

        let metrics = from_records(
            records,
            self.benchmark_kind,
            ActorKind::Producer,
            self.producer_id,
            self.sampling_time,
            self.moving_average_window,
        );

        Self::log_statistics(
            self.producer_id,
            messages_processed,
            batches_processed,
            &self.messages_per_batch,
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
