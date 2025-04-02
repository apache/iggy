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
use crate::rate_limiter::RateLimiter;
use human_repr::HumanCount;
use iggy::client::MessageClient;
use iggy::clients::client::IggyClient;
use iggy::error::IggyError;
use iggy::prelude::IggyMessage;
use iggy::prelude::Partitioning;
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::duration::IggyDuration;
use iggy::utils::sizeable::Sizeable;
use iggy_bench_report::actor_kind::ActorKind;
use iggy_bench_report::benchmark_kind::BenchmarkKind;
use iggy_bench_report::individual_metrics::BenchmarkIndividualMetrics;
use integration::test_server::{login_root, ClientFactory};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tracing::info;

pub struct Producer {
    client_factory: Arc<dyn ClientFactory>,
    benchmark_kind: BenchmarkKind,
    producer_id: u32,
    stream_id: u32,
    partitions_count: u32,
    message_batches: u32,
    messages_per_batch: u32,
    message_size: u32,
    warmup_time: IggyDuration,
    sampling_time: IggyDuration,
    moving_average_window: u32,
    rate_limiter: Option<RateLimiter>,
}

impl Producer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client_factory: Arc<dyn ClientFactory>,
        benchmark_kind: BenchmarkKind,
        producer_id: u32,
        stream_id: u32,
        partitions_count: u32,
        messages_per_batch: u32,
        message_batches: u32,
        message_size: u32,
        warmup_time: IggyDuration,
        sampling_time: IggyDuration,
        moving_average_window: u32,
        rate_limiter: Option<RateLimiter>,
    ) -> Self {
        Producer {
            client_factory,
            benchmark_kind,
            producer_id,
            stream_id,
            partitions_count,
            messages_per_batch,
            message_batches,
            message_size,
            warmup_time,
            sampling_time,
            moving_average_window,
            rate_limiter,
        }
    }

    pub async fn run(self) -> Result<BenchmarkIndividualMetrics, IggyError> {
        let topic_id: u32 = 1;
        let default_partition_id: u32 = 1;
        let partitions_count = self.partitions_count;
        let message_batches = self.message_batches;
        let messages_per_batch = self.messages_per_batch;
        let message_size = self.message_size;

        let total_messages = (messages_per_batch * message_batches) as u64;
        let client = self.client_factory.create_client().await;
        let client = IggyClient::create(client, None, None);
        login_root(&client).await;
        info!(
            "Producer #{} → preparing the test messages...",
            self.producer_id
        );
        let payload = Self::create_payload(message_size);
        let mut batch_user_data_bytes = 0;
        let mut batch_total_bytes = 0;
        let mut messages = Vec::with_capacity(messages_per_batch as usize);
        for _ in 0..messages_per_batch {
            let message = IggyMessage::from_str(&payload).unwrap();
            batch_user_data_bytes += message.payload.len() as u64;
            batch_total_bytes += message.get_size_bytes().as_bytes_u64();
            messages.push(message);
        }
        let batch_user_data_bytes = batch_user_data_bytes;
        let batch_total_bytes = batch_total_bytes;

        let stream_id = self.stream_id.try_into()?;
        let topic_id = topic_id.try_into()?;
        let partitioning = match partitions_count {
            0 => panic!("Partition count must be greater than 0"),
            1 => Partitioning::partition_id(default_partition_id),
            2.. => Partitioning::balanced(),
        };

        if self.warmup_time.get_duration() != Duration::from_millis(0) {
            info!(
                "Producer #{} → warming up for {}...",
                self.producer_id, self.warmup_time
            );
            let warmup_end = Instant::now() + self.warmup_time.get_duration();
            while Instant::now() < warmup_end {
                client
                    .send_messages(&stream_id, &topic_id, &partitioning, &mut messages)
                    .await?;
            }
        }

        info!(
            "Producer #{} → sending {} messages in {} batches of {} messages to stream {} with {} partitions...",
            self.producer_id,
            total_messages.human_count_bare(),
            message_batches.human_count_bare(),
            messages_per_batch.human_count_bare(),
            stream_id,
            partitions_count
        );

        let start_timestamp = Instant::now();
        let mut latencies: Vec<Duration> = Vec::with_capacity(message_batches as usize);
        let mut records = Vec::with_capacity(message_batches as usize);
        let mut total_overhead_time = Duration::from_secs(0);
        for i in 1..=message_batches {
            let iteration_start = Instant::now();

            // Apply rate limiting if configured
            if let Some(limiter) = &self.rate_limiter {
                limiter.throttle(batch_total_bytes).await;
            }
            let before_send = Instant::now();
            client
                .send_messages(&stream_id, &topic_id, &partitioning, &mut messages)
                .await?;
            let latency = before_send.elapsed();

            let iteration_time = iteration_start.elapsed();
            let overhead_time = iteration_time - latency;
            total_overhead_time += overhead_time;

            let messages_processed = (i * messages_per_batch) as u64;
            let batches_processed = i as u64;
            let user_data_bytes = batches_processed * batch_user_data_bytes;
            let total_bytes = batches_processed * batch_total_bytes;

            latencies.push(latency);
            records.push(BenchmarkRecord {
                elapsed_time_us: start_timestamp.elapsed().as_micros() as u64,
                latency_us: latency.as_micros() as u64,
                messages: messages_processed,
                message_batches: batches_processed,
                user_data_bytes,
                total_bytes,
            });
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
            total_messages,
            message_batches,
            messages_per_batch,
            &metrics,
        );

        let avg_overhead_per_batch =
            total_overhead_time.as_micros() as f64 / message_batches as f64;
        let total_elapsed = start_timestamp.elapsed();
        let send_time = total_elapsed - total_overhead_time;
        info!(
            "Producer #{} → Overhead stats: Total time: {} ms, Send time: {} ms, Overhead time: {} ms ({:.2}%), Avg overhead per batch: {:.2} μs",
            self.producer_id,
            total_elapsed.as_millis(),
            send_time.as_millis(),
            total_overhead_time.as_millis(),
            (total_overhead_time.as_micros() as f64 / total_elapsed.as_micros() as f64) * 100.0,
            avg_overhead_per_batch
        );

        Ok(metrics)
    }

    fn create_payload(size: u32) -> String {
        let mut payload = String::with_capacity(size as usize);
        for i in 0..size {
            let char = (i % 26 + 97) as u8 as char;
            payload.push(char);
        }

        payload
    }

    fn log_statistics(
        producer_id: u32,
        total_messages: u64,
        message_batches: u32,
        messages_per_batch: u32,
        metrics: &BenchmarkIndividualMetrics,
    ) {
        info!(
            "Producer #{} → sent {} messages in {} batches of {} messages in {:.2} s, total size: {}, average throughput: {:.2} MB/s, \
    p50 latency: {:.2} ms, p90 latency: {:.2} ms, p95 latency: {:.2} ms, p99 latency: {:.2} ms, p999 latency: {:.2} ms, p9999 latency: {:.2} ms, \
    average latency: {:.2} ms, median latency: {:.2} ms",
            producer_id,
            total_messages,
            message_batches,
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
            metrics.summary.median_latency_ms
        );
    }
}
