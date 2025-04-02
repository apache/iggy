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

use super::utils::calculate_latency_from_first_message;
use crate::analytics::metrics::individual::from_records;
use crate::analytics::record::BenchmarkRecord;
use crate::rate_limiter::RateLimiter;
use human_repr::HumanCount;
use iggy::client::{ConsumerGroupClient, MessageClient};
use iggy::clients::client::IggyClient;
use iggy::consumer::Consumer as IggyConsumer;
use iggy::error::IggyError;
use iggy::messages::PollingKind;
use iggy::prelude::PollingStrategy;
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::duration::IggyDuration;
use iggy::utils::sizeable::Sizeable;
use iggy_bench_report::actor_kind::ActorKind;
use iggy_bench_report::benchmark_kind::BenchmarkKind;
use iggy_bench_report::individual_metrics::BenchmarkIndividualMetrics;
use integration::test_server::{login_root, ClientFactory};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tracing::{error, info, warn};

pub struct Consumer {
    client_factory: Arc<dyn ClientFactory>,
    benchmark_kind: BenchmarkKind,
    consumer_id: u32,
    consumer_group_id: Option<u32>,
    stream_id: u32,
    messages_per_batch: u32,
    message_batches: u32,
    batches_left_to_receive: Arc<AtomicI64>,
    warmup_time: IggyDuration,
    sampling_time: IggyDuration,
    moving_average_window: u32,
    polling_kind: PollingKind,
    calculate_latency_from_message_payload: bool,
    rate_limiter: Option<RateLimiter>,
}

impl Consumer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client_factory: Arc<dyn ClientFactory>,
        benchmark_kind: BenchmarkKind,
        consumer_id: u32,
        consumer_group_id: Option<u32>,
        stream_id: u32,
        messages_per_batch: u32,
        message_batches: u32,
        batches_left_to_receive: Arc<AtomicI64>,
        warmup_time: IggyDuration,
        sampling_time: IggyDuration,
        moving_average_window: u32,
        polling_kind: PollingKind,
        calculate_latency_from_message_payload: bool,
        rate_limiter: Option<RateLimiter>,
    ) -> Self {
        Self {
            client_factory,
            benchmark_kind,
            consumer_id,
            consumer_group_id,
            stream_id,
            messages_per_batch,
            message_batches,
            batches_left_to_receive,
            warmup_time,
            sampling_time,
            moving_average_window,
            polling_kind,
            calculate_latency_from_message_payload,
            rate_limiter,
        }
    }

    pub async fn run(self) -> Result<BenchmarkIndividualMetrics, IggyError> {
        let topic_id: u32 = 1;
        let default_partition_id: u32 = 1;
        let message_batches = self.message_batches as u64;
        let messages_per_batch = self.messages_per_batch;
        let total_messages = (self.messages_per_batch * self.message_batches) as u64;
        let client = self.client_factory.create_client().await;
        let client = IggyClient::create(client, None, None);
        login_root(&client).await;
        let stream_id = self.stream_id.try_into().unwrap();
        let topic_id = topic_id.try_into().unwrap();
        let partition_id = if self.consumer_group_id.is_some() {
            None
        } else {
            Some(default_partition_id)
        };
        let total_batches_to_receive = self.batches_left_to_receive.load(Ordering::Relaxed);
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
                IggyConsumer::group(consumer_group_id.try_into().unwrap())
            }
            None => IggyConsumer::new(self.consumer_id.try_into().unwrap()),
        };
        let mut latencies: Vec<Duration> = Vec::with_capacity(message_batches as usize);
        let mut total_user_data_bytes = IggyByteSize::default();
        let mut total_bytes = IggyByteSize::default();
        let mut topic_not_found_counter = 0;
        let mut initial_poll_timestamp: Option<Instant> = None;
        let mut last_warning_time: Option<Instant> = None;
        let mut skipped_warnings_count: u32 = 0;
        let mut current_iteration: u64 = 0;
        let mut received_messages = 0;
        let mut batch_user_size_bytes = 0;
        let mut batch_size_total_bytes = 0;

        if self.warmup_time.get_duration() != Duration::from_millis(0) {
            if let Some(cg_id) = self.consumer_group_id {
                info!(
                    "Consumer #{}, part of consumer group #{}, → warming up for {}...",
                    self.consumer_id, cg_id, self.warmup_time
                );
            } else {
                info!(
                    "Consumer #{} → warming up for {}...",
                    self.consumer_id, self.warmup_time
                );
            }

            let warmup_end = Instant::now() + self.warmup_time.get_duration();
            while Instant::now() < warmup_end {
                let offset = current_iteration * messages_per_batch as u64;
                let (strategy, auto_commit) = match self.polling_kind {
                    PollingKind::Offset => (PollingStrategy::offset(offset), false),
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
                    warn!(
                        "Consumer: {} - Messages are empty for offset: {}, retrying...",
                        self.consumer_id, offset
                    );
                    continue;
                }
                current_iteration += 1;
            }
        }

        if let Some(cg_id) = self.consumer_group_id {
            info!(
                "Consumer #{}, part of consumer group #{} → polling {} messages in {} batches of {} messages from stream {}...",
                self.consumer_id,
                cg_id,
                total_messages.human_count_bare(),
                message_batches.human_count_bare(),
                messages_per_batch.human_count_bare(),
                stream_id,
            );
        } else {
            info!(
                "Consumer #{} → polling {} messages in {} batches of {} messages from stream {}...",
                self.consumer_id,
                total_messages.human_count_bare(),
                message_batches.human_count_bare(),
                messages_per_batch.human_count_bare(),
                stream_id
            );
        }

        current_iteration = 0;
        let mut records = Vec::with_capacity(message_batches as usize);
        let start_timestamp = Instant::now();
        while self.batches_left_to_receive.load(Ordering::Acquire) > 0 {
            if let Some(limiter) = &self.rate_limiter {
                limiter.throttle(batch_size_total_bytes).await;
            }
            let offset = current_iteration * messages_per_batch as u64;

            let (strategy, auto_commit) = match self.polling_kind {
                PollingKind::Offset => (PollingStrategy::offset(offset), false),
                PollingKind::Next => (PollingStrategy::next(), true),
                _ => panic!(
                    "Unsupported polling kind for benchmark: {:?}",
                    self.polling_kind
                ),
            };
            let before_poll = Instant::now();
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
                .await;
            if let Err(e) = polled_messages {
                if matches!(e, IggyError::TopicIdNotFound(_, _)) {
                    topic_not_found_counter += 1;
                    if topic_not_found_counter > 1000 {
                        return Err(e);
                    }
                } else {
                    return Err(e);
                }
                error!("Unexpected error: {:?}", e);
                continue;
            }

            let polled_messages = polled_messages.unwrap();
            if polled_messages.messages.is_empty() {
                if initial_poll_timestamp.is_none() {
                    initial_poll_timestamp = Some(before_poll);
                }
                let should_warn = last_warning_time
                    .map(|t| t.elapsed() >= Duration::from_secs(1))
                    .unwrap_or(true);

                if should_warn {
                    warn!(
                    "Consumer #{} → Messages are empty for offset: {}, received {} of {} batches, retrying... ({} warnings skipped)",
                    self.consumer_id, offset, total_batches_to_receive - self.batches_left_to_receive.load(Ordering::Acquire), total_batches_to_receive,
                        skipped_warnings_count
                    );
                    last_warning_time = Some(Instant::now());
                    skipped_warnings_count = 0;
                } else {
                    skipped_warnings_count += 1;
                }
                continue;
            }

            if polled_messages.messages.len() != messages_per_batch as usize {
                let should_warn = last_warning_time
                    .map(|t| t.elapsed() >= Duration::from_secs(1))
                    .unwrap_or(true);

                if should_warn {
                    warn!(
                        "Consumer #{} → expected {} messages, but got {} messages ({} batches remaining), retrying... ({} warnings skipped)",
                        self.consumer_id,
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

            let latency = if self.calculate_latency_from_message_payload {
                calculate_latency_from_first_message(&polled_messages.messages[0])
            } else {
                initial_poll_timestamp.unwrap_or(before_poll).elapsed()
            };
            initial_poll_timestamp = None; // Reset the timestamp after successful poll
            latencies.push(latency);

            self.batches_left_to_receive.fetch_sub(1, Ordering::AcqRel);

            received_messages += polled_messages.messages.len() as u64;

            // We don't need to calculate the size whole batch every time by iterating over it - just always use the size of the first message
            if batch_user_size_bytes == 0 || batch_size_total_bytes == 0 {
                batch_user_size_bytes = polled_messages.messages[0].payload.len() as u64
                    * polled_messages.messages.len() as u64;
                batch_size_total_bytes =
                    polled_messages.messages[0].get_size_bytes().as_bytes_u64()
                        * polled_messages.messages.len() as u64;
            }

            total_user_data_bytes += IggyByteSize::from(batch_user_size_bytes);
            total_bytes += IggyByteSize::from(batch_size_total_bytes);
            current_iteration += 1;
            let message_batches = current_iteration;
            records.push(BenchmarkRecord {
                elapsed_time_us: start_timestamp.elapsed().as_micros() as u64,
                latency_us: latency.as_micros() as u64,
                messages: received_messages,
                message_batches,
                user_data_bytes: total_user_data_bytes.as_bytes_u64(),
                total_bytes: total_bytes.as_bytes_u64(),
            });
        }

        let metrics = from_records(
            records,
            self.benchmark_kind,
            ActorKind::Consumer,
            self.consumer_id,
            self.sampling_time,
            self.moving_average_window,
        );

        Self::log_statistics(
            self.consumer_id,
            total_messages,
            current_iteration as u32,
            messages_per_batch,
            &metrics,
        );

        Ok(metrics)
    }

    pub fn log_statistics(
        consumer_id: u32,
        total_messages: u64,
        message_batches: u32,
        messages_per_batch: u32,
        metrics: &BenchmarkIndividualMetrics,
    ) {
        info!(
            "Consumer #{} → polled {} messages, {} batches of {} messages in {:.2} s, total size: {}, average throughput: {:.2} MB/s, \
    p50 latency: {:.2} ms, p90 latency: {:.2} ms, p95 latency: {:.2} ms, p99 latency: {:.2} ms, p999 latency: {:.2} ms, \
    p9999 latency: {:.2} ms, average latency: {:.2} ms, median latency: {:.2} ms",
            consumer_id,
            total_messages.human_count_bare(),
            message_batches.human_count_bare(),
            messages_per_batch.human_count_bare(),
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
