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

use crate::analytics::record::BenchmarkRecord;
use crate::benchmarks::common::create_consumer;
use crate::utils::batch_generator::BenchmarkBatchGenerator;
use crate::utils::finish_condition::BenchmarkFinishCondition;
use crate::utils::{batch_total_size_bytes, batch_user_size_bytes};
use crate::{
    analytics::metrics::individual::from_records, utils::rate_limiter::BenchmarkRateLimiter,
};
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
use tracing::{info, warn};

pub struct BenchmarkProducingConsumer {
    client_factory: Arc<dyn ClientFactory>,
    benchmark_kind: BenchmarkKind,
    actor_id: u32,
    consumer_group_id: Option<u32>,
    stream_id: u32,
    partitions_count: u32,
    messages_per_batch: BenchmarkNumericParameter,
    message_size: BenchmarkNumericParameter,
    send_finish_condition: Arc<BenchmarkFinishCondition>,
    poll_finish_condition: Arc<BenchmarkFinishCondition>,
    warmup_time: IggyDuration,
    sampling_time: IggyDuration,
    moving_average_window: u32,
    limit_bytes_per_second: Option<IggyByteSize>,
    polling_kind: PollingKind,
}

impl BenchmarkProducingConsumer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client_factory: Arc<dyn ClientFactory>,
        benchmark_kind: BenchmarkKind,
        actor_id: u32,
        consumer_group_id: Option<u32>,
        stream_id: u32,
        partitions_count: u32,
        messages_per_batch: BenchmarkNumericParameter,
        message_size: BenchmarkNumericParameter,
        send_finish_condition: Arc<BenchmarkFinishCondition>,
        poll_finish_condition: Arc<BenchmarkFinishCondition>,
        warmup_time: IggyDuration,
        sampling_time: IggyDuration,
        moving_average_window: u32,
        limit_bytes_per_second: Option<IggyByteSize>,
        polling_kind: PollingKind,
    ) -> Self {
        Self {
            client_factory,
            benchmark_kind,
            actor_id,
            consumer_group_id,
            stream_id,
            partitions_count,
            messages_per_batch,
            message_size,
            send_finish_condition,
            poll_finish_condition,
            warmup_time,
            sampling_time,
            moving_average_window,
            limit_bytes_per_second,
            polling_kind,
        }
    }

    pub async fn run(self) -> Result<BenchmarkIndividualMetrics, IggyError> {
        let topic_id: u32 = 1;
        let default_partition_id: u32 = 1;

        let client = self.client_factory.create_client().await;
        let client = IggyClient::create(client, None, None);

        login_root(&client).await;

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

        let consumer = create_consumer(
            &client,
            &self.consumer_group_id,
            &stream_id,
            &topic_id,
            self.actor_id,
        )
        .await;

        let mut batch_generator =
            BenchmarkBatchGenerator::new(self.message_size, self.messages_per_batch);

        let rate_limiter = self.limit_bytes_per_second.map(BenchmarkRateLimiter::new);

        // -----------------------
        // WARM-UP
        // -----------------------

        if self.warmup_time.get_duration() != Duration::from_millis(0) {
            let warmup_end = Instant::now() + self.warmup_time.get_duration();
            let mut offset: u64 = 0;
            let mut last_warning_time: Option<Instant> = None;
            let mut skipped_warnings_count: u32 = 0;

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
            while Instant::now() < warmup_end {
                let batch = batch_generator.generate_batch();
                client
                    .send_messages(&stream_id, &topic_id, &partitioning, &mut batch.messages)
                    .await?;

                let (strategy, auto_commit) = match self.polling_kind {
                    PollingKind::Offset => (PollingStrategy::offset(offset), false),
                    PollingKind::Next => (PollingStrategy::next(), true),
                    other => panic!("Unsupported polling kind for warmup: {:?}", other),
                };

                let polled_messages = client
                    .poll_messages(
                        &stream_id,
                        &topic_id,
                        partition_id,
                        &consumer,
                        &strategy,
                        batch.messages.len() as u32,
                        auto_commit,
                    )
                    .await?;

                if polled_messages.messages.is_empty() {
                    let should_warn = last_warning_time
                        .map(|t| t.elapsed() >= Duration::from_secs(1))
                        .unwrap_or(true);

                    if should_warn {
                        warn!(
                            "ProducingConsumer #{} (warmup) → expected {} messages but got {}, retrying... ({} warnings skipped)",
                            self.actor_id,
                            self.messages_per_batch,
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

                offset += batch.messages.len() as u64;
            }
        }

        // --------------------------------
        // MAIN BENCHMARK LOOP
        // --------------------------------
        info!(
            "ProducingConsumer #{} → sending {} and polling {} ({} msgs/batch) on stream {}, rate limit: {:?}",
            self.actor_id,
            self.send_finish_condition.total_str(),
            self.poll_finish_condition.total_str(),
            self.messages_per_batch,
            stream_id,
            self.limit_bytes_per_second
        );

        let max_capacity = self
            .send_finish_condition
            .max_capacity()
            .max(self.poll_finish_condition.max_capacity());
        let mut latencies: Vec<Duration> = Vec::with_capacity(max_capacity);
        let mut records: Vec<BenchmarkRecord> = Vec::with_capacity(max_capacity);
        let mut offset = 0;
        let mut last_warning_time: Option<Instant> = None;
        let mut skipped_warnings_count = 0;

        let mut total_user_data_bytes_processed;
        let mut total_bytes_processed;
        let mut total_messages_processed = 0;
        let mut total_batches_processed = 0;

        let mut sent_user_bytes_processed = 0;
        let mut sent_total_bytes_processed = 0;
        let mut sent_messages = 0;
        let mut sent_batches = 0;

        let mut received_user_bytes_processed = 0;
        let mut received_total_bytes_processed = 0;
        let mut received_messages = 0;
        let mut received_batches = 0;

        let mut last_received_batch_user_data_bytes;
        let mut rl_value = 0;

        let is_producer = self.send_finish_condition.total() > 0;
        let is_consumer = self.poll_finish_condition.total() > 0;

        let require_reply = is_producer && is_consumer && self.consumer_group_id.is_none();
        let mut awaiting_reply = false; // meaningful only if require_reply == true

        let start_timestamp = Instant::now();

        while !(self.send_finish_condition.is_done() && self.poll_finish_condition.is_done()) {
            let may_send = is_producer
                && !self.send_finish_condition.is_done()
                && (!require_reply || !awaiting_reply);

            if may_send {
                let batch = batch_generator.generate_batch();
                client
                    .send_messages(&stream_id, &topic_id, &partitioning, &mut batch.messages)
                    .await?;

                rl_value += batch.user_data_bytes;

                sent_total_bytes_processed += batch.total_bytes;
                sent_user_bytes_processed += batch.user_data_bytes;
                sent_messages += batch.messages.len() as u32;
                sent_batches += 1;

                awaiting_reply = is_consumer;

                if self
                    .send_finish_condition
                    .account_and_check(batch.user_data_bytes)
                {
                    info!(
                        "ProducingConsumer #{} → finished sending {} messages in {} batches ({} bytes of user data, {} bytes of total data), send finish condition: {}, poll finish condition: {}",
                        self.actor_id,
                        sent_messages.human_count_bare(),
                        sent_batches.human_count_bare(),
                        sent_user_bytes_processed.human_count_bytes(),
                        sent_total_bytes_processed.human_count_bytes(),
                        self.send_finish_condition.status(),
                        self.poll_finish_condition.status()
                    );
                }
            }

            if is_consumer && !self.poll_finish_condition.is_done() {
                let (strategy, auto_commit) = match self.polling_kind {
                    PollingKind::Offset => (PollingStrategy::offset(offset), false),
                    PollingKind::Next => (PollingStrategy::next(), true),
                    other => panic!("Unsupported polling kind for benchmark: {:?}", other),
                };

                let polled_messages = client
                    .poll_messages(
                        &stream_id,
                        &topic_id,
                        partition_id,
                        &consumer,
                        &strategy,
                        self.messages_per_batch.max(),
                        auto_commit,
                    )
                    .await?;

                if polled_messages.messages.is_empty() {
                    let should_warn = last_warning_time
                        .map(|t| t.elapsed() >= Duration::from_secs(1))
                        .unwrap_or(true);

                    if should_warn {
                        warn!(
                            "ProducingConsumer #{} → received empty batch, sent: {}, polled: {}, polling kind: {:?}, retrying... ({} warnings skipped in last second)",
                            self.actor_id,
                            self.send_finish_condition.status(),
                            self.poll_finish_condition.status(),
                            self.polling_kind,
                            skipped_warnings_count
                        );
                        last_warning_time = Some(Instant::now());
                        skipped_warnings_count = 0;
                    } else {
                        skipped_warnings_count += 1;
                    }

                    continue;
                }

                let now = IggyTimestamp::now().as_micros();
                let latency = Duration::from_micros(
                    now - polled_messages.messages[0].header.origin_timestamp,
                );
                latencies.push(latency);

                last_received_batch_user_data_bytes = batch_user_size_bytes(&polled_messages);
                rl_value += last_received_batch_user_data_bytes;

                received_user_bytes_processed += last_received_batch_user_data_bytes;
                received_total_bytes_processed += batch_total_size_bytes(&polled_messages);
                received_messages += polled_messages.messages.len() as u32;
                received_batches += 1;

                offset += polled_messages.messages.len() as u64;

                total_user_data_bytes_processed =
                    received_user_bytes_processed + sent_user_bytes_processed;
                total_bytes_processed = received_total_bytes_processed + sent_total_bytes_processed;
                total_messages_processed = received_messages + sent_messages;
                total_batches_processed = received_batches + sent_batches;

                records.push(BenchmarkRecord {
                    elapsed_time_us: start_timestamp.elapsed().as_micros() as u64,
                    latency_us: latency.as_micros() as u64,
                    messages: total_messages_processed as u64,
                    message_batches: total_batches_processed,
                    user_data_bytes: total_user_data_bytes_processed,
                    total_bytes: total_bytes_processed,
                });

                if let Some(rate_limiter) = &rate_limiter {
                    rate_limiter.wait_until_necessary(rl_value).await;
                    rl_value = 0;
                }

                self.poll_finish_condition
                    .account_and_check(last_received_batch_user_data_bytes);

                if require_reply {
                    awaiting_reply = false;
                }
            }
        }

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
            total_messages_processed as u64,
            total_batches_processed,
            &self.messages_per_batch,
            &metrics,
        );
        Ok(metrics)
    }

    fn log_statistics(
        actor_id: u32,
        total_messages: u64,
        total_batches: u64,
        messages_per_batch: &BenchmarkNumericParameter,
        metrics: &BenchmarkIndividualMetrics,
    ) {
        info!(
            "ProducingConsumer #{} → sent and received {} messages in {} batches of {} messages in {:.2} s, total size: {}, average throughput: {:.2} MB/s, \
    p50 latency: {:.2} ms, p90 latency: {:.2} ms, p95 latency: {:.2} ms, p99 latency: {:.2} ms, p999 latency: {:.2} ms, p9999 latency: {:.2} ms, \
    average latency: {:.2} ms, median latency: {:.2} ms, min latency: {:.2} ms, max latency: {:.2} ms, std dev latency: {:.2} ms",
            actor_id,
            total_messages.human_count_bare(),
            total_batches.human_count_bare(),
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
