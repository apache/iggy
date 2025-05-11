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
use crate::benchmarks::common::create_consumer;
use crate::utils::finish_condition::BenchmarkFinishCondition;
use crate::utils::rate_limiter::BenchmarkRateLimiter;
use crate::utils::{batch_total_size_bytes, batch_user_size_bytes};
use bench_report::actor_kind::ActorKind;
use bench_report::benchmark_kind::BenchmarkKind;
use bench_report::individual_metrics::BenchmarkIndividualMetrics;
use bench_report::numeric_parameter::BenchmarkNumericParameter;
use human_repr::HumanCount;
use iggy::prelude::*;
use integration::test_server::{ClientFactory, login_root};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tracing::{error, info, warn};

pub struct BenchmarkConsumer {
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
    ) -> Self {
        Self {
            client_factory,
            benchmark_kind,
            consumer_id,
            consumer_group_id,
            stream_id,
            messages_per_batch,
            finish_condition,
            warmup_time,
            sampling_time,
            moving_average_window,
            polling_kind,
            limit_bytes_per_second,
            origin_timestamp_latency_calculation,
        }
    }

    pub async fn run(self) -> Result<BenchmarkIndividualMetrics, IggyError> {
        let topic_id: u32 = 1;
        let default_partition_id: u32 = 1;
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
        let cg_id = self.consumer_group_id;
        let consumer =
            create_consumer(&client, &cg_id, &stream_id, &topic_id, self.consumer_id).await;

        let rate_limiter = self.limit_bytes_per_second.map(BenchmarkRateLimiter::new);

        // -----------------------
        // WARM-UP
        // -----------------------

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
            let mut messages_processed = 0;
            let mut last_batch_user_size_bytes = 0;
            while Instant::now() < warmup_end {
                if let Some(rate_limiter) = &rate_limiter {
                    if last_batch_user_size_bytes > 0 {
                        rate_limiter
                            .wait_until_necessary(last_batch_user_size_bytes)
                            .await;
                    }
                }
                let messages_to_receive = self.messages_per_batch.get();
                let offset = messages_processed;
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
                        messages_to_receive,
                        auto_commit,
                    )
                    .await?;

                if polled_messages.messages.is_empty() {
                    warn!(
                        "Consumer #{} → Messages are empty for offset: {}, retrying...",
                        self.consumer_id, offset
                    );
                    continue;
                }
                messages_processed = polled_messages.messages.len() as u64;
                last_batch_user_size_bytes = batch_user_size_bytes(&polled_messages);
            }
        }

        // -----------------------
        // MAIN BENCHMARK
        // -----------------------

        if let Some(cg_id) = self.consumer_group_id {
            info!(
                "Consumer #{}, part of consumer group #{} → polling {} in {} messages per batch from stream {}, rate limit: {:?}...",
                self.consumer_id,
                cg_id,
                self.finish_condition.total_str(),
                self.messages_per_batch,
                stream_id,
                self.limit_bytes_per_second
            );
        } else {
            info!(
                "Consumer #{} → polling {} in {} messages per batch from stream {}, rate limit: {:?}...",
                self.consumer_id,
                self.finish_condition.total_str(),
                self.messages_per_batch,
                stream_id,
                self.limit_bytes_per_second
            );
        }

        let max_capacity = self.finish_condition.max_capacity();
        let mut records = Vec::with_capacity(max_capacity);
        let mut latencies: Vec<Duration> = Vec::with_capacity(max_capacity);
        let mut skipped_warnings_count: u32 = 0;
        let mut topic_not_found_counter = 0;
        let mut initial_poll_timestamp: Option<Instant> = None;
        let mut last_warning_time: Option<Instant> = None;
        let mut messages_processed = 0;
        let mut batches_processed = 0;
        let mut bytes_processed = 0;
        let mut user_data_bytes_processed = 0;
        let start_timestamp = Instant::now();

        loop {
            if self.finish_condition.is_done() {
                break;
            }

            let messages_to_receive = self.messages_per_batch.get();
            let offset = messages_processed;
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
                    messages_to_receive,
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
                        "Consumer #{} → Messages are empty for offset: {}, received {}, retrying... ({} warnings skipped)",
                        self.consumer_id,
                        offset,
                        self.finish_condition.status(),
                        skipped_warnings_count
                    );
                    last_warning_time = Some(Instant::now());
                    skipped_warnings_count = 0;
                } else {
                    skipped_warnings_count += 1;
                }
                continue;
            }

            if polled_messages.messages.len() != messages_to_receive as usize {
                let should_warn = last_warning_time
                    .map(|t| t.elapsed() >= Duration::from_secs(1))
                    .unwrap_or(true);

                if should_warn {
                    warn!(
                        "Consumer #{} → expected {} messages, but got {} messages (received {}), retrying... ({} warnings skipped)",
                        self.consumer_id,
                        messages_to_receive,
                        polled_messages.messages.len(),
                        self.finish_condition.status(),
                        skipped_warnings_count
                    );
                    last_warning_time = Some(Instant::now());
                    skipped_warnings_count = 0;
                } else {
                    skipped_warnings_count += 1;
                }
            }
            let latency = if self.origin_timestamp_latency_calculation {
                let now = IggyTimestamp::now().as_micros();
                Duration::from_micros(now - polled_messages.messages[0].header.origin_timestamp)
            } else {
                initial_poll_timestamp.unwrap_or(before_poll).elapsed()
            };

            latencies.push(latency);

            let batch_user_size_bytes = batch_user_size_bytes(&polled_messages);
            let batch_total_size_bytes = batch_total_size_bytes(&polled_messages);

            initial_poll_timestamp = None;

            messages_processed += polled_messages.messages.len() as u64;
            batches_processed += 1;
            bytes_processed += batch_total_size_bytes;
            user_data_bytes_processed += batch_user_size_bytes;

            records.push(BenchmarkRecord {
                elapsed_time_us: start_timestamp.elapsed().as_micros() as u64,
                latency_us: latency.as_micros() as u64,
                messages: messages_processed,
                message_batches: batches_processed,
                user_data_bytes: user_data_bytes_processed,
                total_bytes: bytes_processed,
            });

            if let Some(rate_limiter) = &rate_limiter {
                rate_limiter
                    .wait_until_necessary(batch_user_size_bytes)
                    .await;
            }

            if self
                .finish_condition
                .account_and_check(batch_user_size_bytes)
            {
                break;
            }
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
            messages_processed,
            batches_processed as u32,
            &self.messages_per_batch,
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
