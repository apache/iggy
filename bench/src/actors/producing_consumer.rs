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

use crate::actors::utils::calculate_latency_from_first_message;
use crate::analytics::metrics::individual::from_records;
use crate::analytics::record::BenchmarkRecord;
use crate::rate_limiter::RateLimiter;
use human_repr::HumanCount;
use iggy::client::ConsumerGroupClient;
use iggy::client::MessageClient;
use iggy::clients::client::IggyClient;
use iggy::consumer::Consumer as IggyConsumer;
use iggy::error::IggyError;
use iggy::prelude::*;
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::duration::IggyDuration;
use iggy::utils::sizeable::Sizeable;
use iggy_bench_report::actor_kind::ActorKind;
use iggy_bench_report::benchmark_kind::BenchmarkKind;
use iggy_bench_report::individual_metrics::BenchmarkIndividualMetrics;
use integration::test_server::{login_root, ClientFactory};
use std::str::FromStr;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tracing::{info, warn};

pub struct ProducingConsumer {
    client_factory: Arc<dyn ClientFactory>,
    benchmark_kind: BenchmarkKind,
    actor_id: u32,
    consumer_group_id: Option<u32>,
    stream_id: u32,
    partitions_count: u32,
    messages_per_batch: u32,
    message_batches: u32,
    message_size: u32,
    batches_left_to_receive: Arc<AtomicI64>,
    warmup_time: IggyDuration,
    sampling_time: IggyDuration,
    moving_average_window: u32,
    rate_limiter: Option<RateLimiter>,
    polling_kind: PollingKind,
    calculate_latency_from_timestamp_in_first_message: bool,
}

impl ProducingConsumer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client_factory: Arc<dyn ClientFactory>,
        benchmark_kind: BenchmarkKind,
        actor_id: u32,
        consumer_group_id: Option<u32>,
        stream_id: u32,
        partitions_count: u32,
        messages_per_batch: u32,
        message_batches: u32,
        message_size: u32,
        batches_left_to_receive: Arc<AtomicI64>,
        warmup_time: IggyDuration,
        sampling_time: IggyDuration,
        moving_average_window: u32,
        rate_limiter: Option<RateLimiter>,
        polling_kind: PollingKind,
        calculate_latency_from_timestamp_in_first_message: bool,
    ) -> Self {
        Self {
            client_factory,
            benchmark_kind,
            actor_id,
            consumer_group_id,
            stream_id,
            partitions_count,
            messages_per_batch,
            message_batches,
            message_size,
            batches_left_to_receive,
            warmup_time,
            sampling_time,
            moving_average_window,
            rate_limiter,
            polling_kind,
            calculate_latency_from_timestamp_in_first_message,
        }
    }

    pub async fn run(self) -> Result<BenchmarkIndividualMetrics, IggyError> {
        let topic_id: u32 = 1;
        let default_partition_id: u32 = 1;
        let message_batches = self.message_batches;
        let messages_per_batch = self.messages_per_batch;
        let message_size = self.message_size;
        let total_messages = (messages_per_batch * message_batches) as u64;
        let total_msg_batches = self.batches_left_to_receive.load(Ordering::Acquire);
        let client = self.client_factory.create_client().await;
        let client = IggyClient::create(client, None, None);
        login_root(&client).await;

        info!(
            "ProducingConsumer #{} → preparing test messages...",
            self.actor_id
        );
        let payload = self.create_payload(message_size);
        let mut batch_user_data_bytes = 0;
        let mut batch_total_bytes = 0;
        let mut messages = Vec::with_capacity(messages_per_batch as usize);
        for _ in 0..messages_per_batch {
            let message = IggyMessage::from_str(&payload).unwrap();
            batch_user_data_bytes += message.payload.len() as u64;
            batch_total_bytes += message.get_size_bytes().as_bytes_u64();
            messages.push(message);
        }

        let stream_id = self.stream_id.try_into()?;
        let topic_id = topic_id.try_into()?;
        let partitioning = match self.partitions_count {
            0 => panic!("Partition count must be greater than 0"),
            1 => Partitioning::partition_id(default_partition_id),
            2.. => Partitioning::balanced(),
        };
        let partition_id = if self.consumer_group_id.is_some() {
            None
        } else {
            Some(default_partition_id)
        };

        let consumer = match self.consumer_group_id {
            Some(consumer_group_id) => {
                client
                    .join_consumer_group(
                        &stream_id,
                        &topic_id,
                        &consumer_group_id.try_into().unwrap(),
                    )
                    .await
                    .expect("Failed to join consumer group");
                info!(
                    "ProducingConsumer #{} → joined consumer group {consumer_group_id}",
                    self.actor_id,
                );
                IggyConsumer::group(consumer_group_id.try_into().unwrap())
            }
            None => IggyConsumer::new(self.actor_id.try_into().unwrap()),
        };
        let mut current_offset: u64 = 0;
        let mut last_warning_time: Option<Instant> = None;
        let mut skipped_warnings_count: u32 = 0;

        // Warmup if needed
        if self.warmup_time.get_duration() != Duration::from_millis(0) {
            if let Some(cg_id) = self.consumer_group_id {
                info!(
                    "ProducingConsumer #{}, part of consumer group #{}, → warming up for {}...",
                    self.actor_id, cg_id, self.warmup_time
                );
            } else {
                info!(
                    "ProducingConsumer #{} → warming up for {}...",
                    self.actor_id, self.warmup_time
                );
            }
            let warmup_end = Instant::now() + self.warmup_time.get_duration();
            while Instant::now() < warmup_end {
                client
                    .send_messages(&stream_id, &topic_id, &partitioning, &mut messages)
                    .await?;

                let (strategy, auto_commit) = match self.polling_kind {
                    PollingKind::Offset => (PollingStrategy::offset(current_offset), false),
                    PollingKind::Next => (PollingStrategy::next(), true),
                    _ => panic!(
                        "Unsupported polling kind for benchmark: {:?}",
                        self.polling_kind
                    ),
                };

                let polled_messages = client
                    .poll_messages(
                        &stream_id,
                        &topic_id,
                        partition_id,
                        &consumer,
                        &strategy,
                        messages_per_batch,
                        auto_commit,
                    )
                    .await?;

                if polled_messages.messages.is_empty() {
                    let should_warn = last_warning_time
                        .map(|t| t.elapsed() >= Duration::from_secs(1))
                        .unwrap_or(true);

                    if should_warn {
                        warn!(
                            "ProducingConsumer #{} → expected {} messages but got {}, retrying... ({} warnings skipped)",
                            self.actor_id,
                            messages_per_batch,
                            polled_messages.messages.len(),
                            skipped_warnings_count
                        );
                        last_warning_time = Some(Instant::now());
                        skipped_warnings_count = 0;
                    } else {
                        skipped_warnings_count += 1;
                    }

                    continue;
                }

                current_offset += messages_per_batch as u64;
            }
        }

        info!(
            "ProducingConsumer #{} → sending and polling {} messages in {} batches of {} messages from/to stream {}...",
            self.actor_id,
            total_messages.human_count_bare(),
            message_batches.human_count_bare(),
            messages_per_batch.human_count_bare(),
            stream_id
        );

        let start_timestamp = Instant::now();
        let mut latencies: Vec<Duration> = Vec::with_capacity(message_batches as usize);
        let mut records: Vec<BenchmarkRecord> = Vec::with_capacity(message_batches as usize);
        let mut batch_id = 1;

        while self.batches_left_to_receive.load(Ordering::Acquire) > 0 {
            if let Some(rate_limiter) = &self.rate_limiter {
                rate_limiter.throttle(batch_user_data_bytes).await;
            }
            let before_send = Instant::now();
            client
                .send_messages(&stream_id, &topic_id, &partitioning, &mut messages)
                .await?;
            let (strategy, auto_commit) = match self.polling_kind {
                PollingKind::Offset => (PollingStrategy::offset(current_offset), false),
                PollingKind::Next => (PollingStrategy::next(), true),
                _ => panic!(
                    "Unsupported polling kind for benchmark: {:?}",
                    self.polling_kind
                ),
            };
            let polled_messages = client
                .poll_messages(
                    &stream_id,
                    &topic_id,
                    partition_id,
                    &consumer,
                    &strategy,
                    messages_per_batch,
                    auto_commit,
                )
                .await?;

            if polled_messages.messages.len() != messages_per_batch as usize {
                let should_warn = last_warning_time
                    .map(|t| t.elapsed() >= Duration::from_secs(1))
                    .unwrap_or(true);

                if should_warn {
                    warn!(
                        "ProducingConsumer #{} → expected {} messages but got {}, there is {} batches left to receive, retrying... ({} warnings skipped in last second)",
                        self.actor_id,
                        messages_per_batch,
                        polled_messages.messages.len(),
                        self.batches_left_to_receive.load(Ordering::Acquire),
                        skipped_warnings_count
                    );
                    last_warning_time = Some(Instant::now());
                    skipped_warnings_count = 0;
                } else {
                    skipped_warnings_count += 1;
                }

                continue;
            }

            // Extract send timestamp from first message in batch
            let latency = if self.calculate_latency_from_timestamp_in_first_message {
                calculate_latency_from_first_message(&polled_messages.messages[0])
            } else {
                before_send.elapsed()
            };
            latencies.push(latency);

            current_offset += messages_per_batch as u64;
            self.batches_left_to_receive.fetch_sub(1, Ordering::AcqRel);
            batch_id += 1;

            records.push(BenchmarkRecord {
                elapsed_time_us: start_timestamp.elapsed().as_micros() as u64,
                latency_us: latency.as_micros() as u64,
                messages: (batch_id * messages_per_batch * 2) as u64, // Count both sent and polled messages
                message_batches: batch_id as u64,
                user_data_bytes: batch_id as u64 * batch_user_data_bytes * 2, // Account for both sent and received bytes
                total_bytes: batch_id as u64 * batch_total_bytes * 2, // Account for both sent and received total bytes
            });
        }

        info!(
            "ProducingConsumer #{} → sent and received {} messages ({} sent + {} received) in {} batches out of {}",
            self.actor_id,
            batch_id * messages_per_batch * 2,
            batch_id * messages_per_batch,
            batch_id * messages_per_batch,
            batch_id,
            total_msg_batches
        );

        let metrics = from_records(
            records,
            self.benchmark_kind,
            ActorKind::ProducingConsumer,
            self.actor_id,
            self.sampling_time,
            self.moving_average_window,
        );

        Self::log_statistics(
            self.actor_id,
            total_messages,
            message_batches,
            messages_per_batch,
            &metrics,
        );
        Ok(metrics)
    }

    fn create_payload(&self, size: u32) -> String {
        "a".repeat(size as usize)
    }

    fn log_statistics(
        actor_id: u32,
        total_messages: u64,
        message_batches: u32,
        messages_per_batch: u32,
        metrics: &BenchmarkIndividualMetrics,
    ) {
        info!(
            "ProducingConsumer #{} → sent {} messages in {} batches of {} messages in {:.2} s, total size: {}, average throughput: {:.2} MB/s, \
    p50 latency: {:.2} ms, p90 latency: {:.2} ms, p95 latency: {:.2} ms, p99 latency: {:.2} ms, p999 latency: {:.2} ms, p9999 latency: {:.2} ms, \
    average latency: {:.2} ms, median latency: {:.2} ms",
            actor_id,
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
