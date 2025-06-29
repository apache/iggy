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

use std::{sync::Arc, time::Duration};

use crate::{
    actors::producing_consumer::backend::{
        BenchmarkProducingConsumerBackend, BenchmarkProducingConsumerConfig, HighLevelBackend,
        LowLevelBackend, ProducingConsumerBackend,
    },
    analytics::{metrics::individual::from_records, record::BenchmarkRecord},
    utils::{
        batch_generator::BenchmarkBatchGenerator, finish_condition::BenchmarkFinishCondition,
        rate_limiter::BenchmarkRateLimiter,
    },
};
use bench_report::{
    actor_kind::ActorKind, benchmark_kind::BenchmarkKind,
    individual_metrics::BenchmarkIndividualMetrics, numeric_parameter::BenchmarkNumericParameter,
};
use human_repr::HumanCount;
use iggy::prelude::*;
use integration::test_server::ClientFactory;
use tokio::time::Instant;
use tracing::info;

pub struct BenchmarkProducingConsumer {
    backend: ProducingConsumerBackend,
    benchmark_kind: BenchmarkKind,
    send_finish_condition: Arc<BenchmarkFinishCondition>,
    poll_finish_condition: Arc<BenchmarkFinishCondition>,
    sampling_time: IggyDuration,
    moving_average_window: u32,
    limit_bytes_per_second: Option<IggyByteSize>,
    config: BenchmarkProducingConsumerConfig,
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
        origin_timestamp_latency_calculation: bool,
        use_high_level_api: bool,
    ) -> Self {
        let config = BenchmarkProducingConsumerConfig {
            actor_id,
            consumer_group_id,
            stream_id,
            partitions_count,
            messages_per_batch,
            message_size,
            warmup_time,
            polling_kind,
            origin_timestamp_latency_calculation,
        };
        let backend = if use_high_level_api {
            ProducingConsumerBackend::HighLevel(HighLevelBackend::new(
                client_factory,
                config.clone(),
            ))
        } else {
            ProducingConsumerBackend::LowLevel(LowLevelBackend::new(client_factory, config.clone()))
        };

        Self {
            backend,
            benchmark_kind,
            send_finish_condition,
            poll_finish_condition,
            sampling_time,
            moving_average_window,
            limit_bytes_per_second,
            config,
        }
    }

    pub async fn run(self) -> Result<BenchmarkIndividualMetrics, IggyError> {
        match self.backend {
            ProducingConsumerBackend::LowLevel(backend) => {
                Self::run_with_backend(
                    self.benchmark_kind,
                    self.send_finish_condition,
                    self.poll_finish_condition,
                    self.sampling_time,
                    self.moving_average_window,
                    self.limit_bytes_per_second,
                    self.config,
                    backend,
                )
                .await
            }
            ProducingConsumerBackend::HighLevel(backend) => {
                Self::run_with_backend(
                    self.benchmark_kind,
                    self.send_finish_condition,
                    self.poll_finish_condition,
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

    #[allow(
        clippy::too_many_lines,
        clippy::too_many_arguments,
        clippy::cognitive_complexity
    )]
    async fn run_with_backend<B: BenchmarkProducingConsumerBackend>(
        benchmark_kind: BenchmarkKind,
        send_finish_condition: Arc<BenchmarkFinishCondition>,
        poll_finish_condition: Arc<BenchmarkFinishCondition>,
        sampling_time: IggyDuration,
        moving_average_window: u32,
        limit_bytes_per_second: Option<IggyByteSize>,
        config: BenchmarkProducingConsumerConfig,
        backend: B,
    ) -> Result<BenchmarkIndividualMetrics, IggyError> {
        info!(
            "ProducingConsumer #{producer_id} → sending {send_status} and polling {poll_status} ({messages_per_batch} msgs/batch) on stream {stream_id}, rate limit: {rate_limit:?}",
            producer_id = config.actor_id,
            send_status = send_finish_condition.total_str(),
            poll_status = poll_finish_condition.total_str(),
            messages_per_batch = config.messages_per_batch,
            stream_id = config.stream_id,
            rate_limit = limit_bytes_per_second,
        );
        let mut context = backend.setup().await?;
        let mut batch_generator =
            BenchmarkBatchGenerator::new(config.message_size, config.messages_per_batch);
        let rate_limiter = limit_bytes_per_second.map(BenchmarkRateLimiter::new);
        if config.warmup_time.get_duration() != Duration::from_millis(0) {
            backend.log_warmup_info();
            backend.warmup(&mut context, &mut batch_generator).await?;
        }

        backend.log_setup_info();

        let max_capacity = send_finish_condition
            .max_capacity()
            .max(poll_finish_condition.max_capacity());
        let mut records = Vec::with_capacity(max_capacity);

        let mut rl_value = 0;
        let mut sent_user_bytes = 0;
        let mut sent_total_bytes = 0;
        let mut sent_messages = 0;
        let mut sent_batches = 0;

        let mut recv_user_bytes = 0;
        let mut recv_total_bytes = 0;
        let mut recv_messages = 0;
        let mut recv_batches = 0;

        let is_producer = send_finish_condition.total() > 0;
        let is_consumer = poll_finish_condition.total() > 0;

        let require_reply = is_producer && is_consumer && config.consumer_group_id.is_none();
        let mut awaiting_reply = false;

        let start = Instant::now();

        while !(send_finish_condition.is_done() && poll_finish_condition.is_done()) {
            if is_producer
                && !send_finish_condition.is_done()
                && (!require_reply || !awaiting_reply)
            {
                if let Some(batch) = backend
                    .produce_batch(&mut context, &mut batch_generator)
                    .await?
                {
                    rl_value += batch.user_data_bytes;
                    sent_user_bytes += batch.user_data_bytes;
                    sent_total_bytes += batch.total_bytes;
                    sent_messages += u64::from(batch.messages);
                    sent_batches += 1;
                    awaiting_reply = is_consumer;

                    if send_finish_condition.account_and_check(batch.user_data_bytes) {
                        info!(
                            "ProducingConsumer #{actor_id} → finished sending {sent_messages} messages in {sent_batches} batches ({sent_user_bytes} bytes of user data, {sent_total_bytes} bytes of total data), send finish condition: {send_status}, poll finish condition: {poll_status}",
                            actor_id = config.actor_id,
                            sent_messages = sent_messages.human_count_bare(),
                            sent_batches = sent_batches.human_count_bare(),
                            sent_user_bytes = sent_user_bytes.human_count_bytes(),
                            sent_total_bytes = sent_total_bytes.human_count_bytes(),
                            send_status = send_finish_condition.status(),
                            poll_status = poll_finish_condition.status()
                        );
                    }
                }
            }

            if is_consumer && !poll_finish_condition.is_done() {
                if let Some(batch) = backend.consume_batch(&mut context).await? {
                    rl_value += batch.user_data_bytes;
                    recv_user_bytes += batch.user_data_bytes;
                    recv_total_bytes += batch.total_bytes;
                    recv_messages += u64::from(batch.messages);
                    recv_batches += 1;

                    let elapsed = u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);
                    let latency = u64::try_from(batch.latency.as_micros()).unwrap_or(u64::MAX);

                    records.push(BenchmarkRecord {
                        elapsed_time_us: elapsed,
                        latency_us: latency,
                        messages: sent_messages + recv_messages,
                        message_batches: sent_batches + recv_batches,
                        user_data_bytes: sent_user_bytes + recv_user_bytes,
                        total_bytes: sent_total_bytes + recv_total_bytes,
                    });

                    if let Some(limiter) = &rate_limiter {
                        limiter.wait_until_necessary(rl_value).await;
                        rl_value = 0;
                    }

                    poll_finish_condition.account_and_check(batch.user_data_bytes);
                    if require_reply {
                        awaiting_reply = false;
                    }
                }
            }
        }

        let metrics = from_records(
            &records,
            benchmark_kind,
            ActorKind::ProducingConsumer,
            config.actor_id,
            sampling_time,
            moving_average_window,
        );

        Self::log_statistics(
            config.actor_id,
            recv_messages,
            recv_batches,
            &config.messages_per_batch,
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
