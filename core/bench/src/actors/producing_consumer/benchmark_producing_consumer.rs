// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use crate::{
    actors::{
        consumer::client::{BenchmarkConsumerClient, interface::BenchmarkConsumerConfig},
        producer::client::{BenchmarkProducerClient, interface::BenchmarkProducerConfig},
    },
    analytics::{metrics::individual::from_records, record::BenchmarkRecord},
    utils::{
        batch_generator::BenchmarkBatchGenerator, finish_condition::BenchmarkFinishCondition,
        rate_limiter::BenchmarkRateLimiter,
    },
};
use bench_report::{
    actor_kind::ActorKind,
    benchmark_kind::BenchmarkKind,
    individual_metrics::BenchmarkIndividualMetrics,
    numeric_parameter::BenchmarkNumericParameter,
    utils::{WIDE_LAYOUT_THRESHOLD, get_terminal_width},
};
use comfy_table::{ContentArrangement, Table, presets::UTF8_FULL};
use human_repr::HumanCount;
use iggy::prelude::*;
use tokio::time::Instant;
use tracing::info;

pub struct BenchmarkProducingConsumer<P, C>
where
    P: BenchmarkProducerClient,
    C: BenchmarkConsumerClient,
{
    pub producer: P,
    pub consumer: C,
    pub benchmark_kind: BenchmarkKind,
    pub send_finish_condition: Arc<BenchmarkFinishCondition>,
    pub poll_finish_condition: Arc<BenchmarkFinishCondition>,
    pub sampling_time: IggyDuration,
    pub moving_average_window: u32,
    pub limit_bytes_per_second: Option<IggyByteSize>,
    pub producer_config: BenchmarkProducerConfig,
    pub consumer_config: BenchmarkConsumerConfig,
}

impl<P, C> BenchmarkProducingConsumer<P, C>
where
    P: BenchmarkProducerClient,
    C: BenchmarkConsumerClient,
{
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        producer: P,
        consumer: C,
        benchmark_kind: BenchmarkKind,
        send_finish_condition: Arc<BenchmarkFinishCondition>,
        poll_finish_condition: Arc<BenchmarkFinishCondition>,
        sampling_time: IggyDuration,
        moving_average_window: u32,
        limit_bytes_per_second: Option<IggyByteSize>,
        producer_config: BenchmarkProducerConfig,
        consumer_config: BenchmarkConsumerConfig,
    ) -> Self {
        Self {
            producer,
            consumer,
            benchmark_kind,
            send_finish_condition,
            poll_finish_condition,
            sampling_time,
            moving_average_window,
            limit_bytes_per_second,
            producer_config,
            consumer_config,
        }
    }
    #[allow(clippy::too_many_lines)]
    #[allow(clippy::cognitive_complexity)]
    pub async fn run(mut self) -> Result<BenchmarkIndividualMetrics, IggyError> {
        info!(
            "ProducingConsumer #{producer_id} → sending {send_status} and polling {poll_status} ({messages_per_batch} msgs/batch) on stream {stream_id}, rate limit: {rate_limit:?}",
            producer_id = self.producer_config.producer_id,
            send_status = self.send_finish_condition.total_str(),
            poll_status = self.poll_finish_condition.total_str(),
            messages_per_batch = self.producer_config.messages_per_batch,
            stream_id = self.producer_config.stream_id,
            rate_limit = self.limit_bytes_per_second,
        );
        self.producer.setup().await?;
        self.consumer.setup().await?;
        let mut batch_generator = BenchmarkBatchGenerator::new(
            self.producer_config.message_size,
            self.producer_config.messages_per_batch,
        );
        let rate_limiter = self.limit_bytes_per_second.map(BenchmarkRateLimiter::new);
        let is_producer = self.send_finish_condition.total() > 0;
        let is_consumer = self.poll_finish_condition.total() > 0;
        if self.producer_config.warmup_time.get_duration() != Duration::from_millis(0) {
            self.log_warmup_info();
            let warmup_end = Instant::now() + self.producer_config.warmup_time.get_duration();

            // Warmup sends under the same limiter as the measured phase, so the phase boundary
            // is a step rather than a burst far above the rate being measured.
            while Instant::now() < warmup_end {
                if is_producer
                    && let Ok(Some(batch)) = self.producer.produce_batch(&mut batch_generator).await
                    && let Some(limiter) = &rate_limiter
                {
                    limiter.wait_until_necessary(batch.user_data_bytes).await;
                }
                if is_consumer {
                    let _ = self.consumer.consume_batch().await;
                }
            }
        }

        // No offset reset here, unlike the standalone consumer: this kind reports the age of
        // the message, so re-reading warmup messages would report the whole warmup as latency.
        self.log_setup_info();
        let max_capacity = self
            .send_finish_condition
            .max_capacity()
            .max(self.poll_finish_condition.max_capacity());
        let mut records = Vec::with_capacity(max_capacity);

        let mut uncharged_send_bytes = 0;
        let mut sent_user_bytes = 0;
        let mut sent_total_bytes = 0;
        let mut sent_messages = 0;
        let mut sent_batches = 0;

        let mut recv_user_bytes = 0;
        let mut recv_total_bytes = 0;
        let mut recv_messages = 0;
        let mut recv_batches = 0;

        let require_reply =
            is_producer && is_consumer && self.consumer_config.consumer_group_id.is_none();
        let mut awaiting_reply = false;

        let start = Instant::now();

        while !(self.send_finish_condition.is_done() && self.poll_finish_condition.is_done()) {
            if is_producer
                && !self.send_finish_condition.is_done()
                && (!require_reply || !awaiting_reply)
                && let Some(batch) = self.producer.produce_batch(&mut batch_generator).await?
            {
                uncharged_send_bytes += batch.user_data_bytes;
                sent_user_bytes += batch.user_data_bytes;
                sent_total_bytes += batch.total_bytes;
                sent_messages += u64::from(batch.messages);
                sent_batches += 1;
                awaiting_reply = is_consumer;

                if self
                    .send_finish_condition
                    .account_and_check(batch.user_data_bytes)
                {
                    info!(
                        "ProducingConsumer #{actor_id} → finished sending {sent_messages} messages in {sent_batches} batches ({sent_user_bytes} bytes of user data, {sent_total_bytes} bytes of total data), send finish condition: {send_status}, poll finish condition: {poll_status}",
                        actor_id = self.producer_config.producer_id,
                        sent_messages = sent_messages.human_count_bare(),
                        sent_batches = sent_batches.human_count_bare(),
                        sent_user_bytes = sent_user_bytes.human_count_bytes(),
                        sent_total_bytes = sent_total_bytes.human_count_bytes(),
                        send_status = self.send_finish_condition.status(),
                        poll_status = self.poll_finish_condition.status()
                    );
                }
            }

            let pacing_finished = AtomicBool::new(false);
            let pace_group = async {
                if !require_reply
                    && uncharged_send_bytes > 0
                    && let Some(limiter) = &rate_limiter
                {
                    limiter.wait_until_necessary(uncharged_send_bytes).await;
                }
                pacing_finished.store(true, Ordering::Relaxed);
                Ok::<(), IggyError>(())
            };
            let consume = async {
                loop {
                    if !is_consumer || self.poll_finish_condition.is_done() {
                        break;
                    }
                    if let Some(batch) = self.consumer.consume_batch().await? {
                        recv_user_bytes += batch.user_data_bytes;
                        recv_total_bytes += batch.total_bytes;
                        recv_messages += u64::from(batch.messages);
                        recv_batches += 1;

                        let elapsed =
                            u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);
                        let latency = u64::try_from(batch.latency.as_micros()).unwrap_or(u64::MAX);

                        records.push(BenchmarkRecord {
                            elapsed_time_us: elapsed,
                            latency_us: latency,
                            messages: sent_messages + recv_messages,
                            message_batches: sent_batches + recv_batches,
                            user_data_bytes: sent_user_bytes + recv_user_bytes,
                            total_bytes: sent_total_bytes + recv_total_bytes,
                        });

                        self.poll_finish_condition
                            .account_and_check(batch.user_data_bytes);
                        if require_reply {
                            awaiting_reply = false;
                        }
                    }
                    if require_reply || pacing_finished.load(Ordering::Relaxed) {
                        break;
                    }
                }
                Ok::<(), IggyError>(())
            };
            // Group members keep polling while producer quota replenishes. Joining the
            // futures lets an in-flight poll finish before the next send.
            tokio::try_join!(pace_group, consume)?;
            if !require_reply {
                uncharged_send_bytes = 0;
            }

            // Pinned pairs wait after their reply, so pacing is excluded from its latency.
            if require_reply
                && uncharged_send_bytes > 0
                && !awaiting_reply
                && let Some(limiter) = &rate_limiter
            {
                limiter.wait_until_necessary(uncharged_send_bytes).await;
                uncharged_send_bytes = 0;
            }
        }

        let metrics = from_records(
            &records,
            self.benchmark_kind,
            ActorKind::ProducingConsumer,
            self.producer_config.producer_id,
            self.sampling_time,
            self.moving_average_window,
        );

        Self::log_statistics(
            self.producer_config.producer_id,
            recv_messages,
            recv_batches,
            &self.producer_config.messages_per_batch,
            &metrics,
            self.producer_config.pretty,
        );
        Ok(metrics)
    }

    fn log_setup_info(&self) {
        if let Some(cg_id) = self.consumer_config.consumer_group_id {
            info!(
                "ProducingConsumer #{} → sending & polling {} messages per batch ({} bytes each) on stream {} with {} partition(s), part of CG #{} ({})",
                self.producer_config.producer_id,
                self.producer_config.messages_per_batch,
                self.producer_config.message_size,
                self.producer_config.stream_id,
                self.producer_config.partitions,
                cg_id,
                C::API_LABEL
            );
        } else {
            info!(
                "ProducingConsumer #{} → sending & polling {} messages per batch ({} bytes each) on stream {} with {} partition(s) ({})",
                self.producer_config.producer_id,
                self.producer_config.messages_per_batch,
                self.producer_config.message_size,
                self.producer_config.stream_id,
                self.producer_config.partitions,
                C::API_LABEL
            );
        }
    }

    fn log_warmup_info(&self) {
        if let Some(cg_id) = self.consumer_config.consumer_group_id {
            info!(
                "ProducingConsumer #{}, part of consumer group #{} → warming up for {}...",
                self.consumer_config.consumer_id, cg_id, self.consumer_config.warmup_time
            );
        } else {
            info!(
                "ProducingConsumer #{} → warming up for {}...",
                self.consumer_config.consumer_id, self.consumer_config.warmup_time
            );
        }
    }

    fn log_statistics(
        actor_id: u32,
        total_messages: u64,
        total_batches: u64,
        messages_per_batch: &BenchmarkNumericParameter,
        metrics: &BenchmarkIndividualMetrics,
        pretty: bool,
    ) {
        if pretty {
            let width = get_terminal_width();

            if width >= WIDE_LAYOUT_THRESHOLD {
                Self::print_wide_layout(
                    actor_id,
                    total_messages,
                    total_batches,
                    messages_per_batch,
                    metrics,
                );
            } else {
                Self::print_narrow_layout(
                    actor_id,
                    total_messages,
                    total_batches,
                    messages_per_batch,
                    metrics,
                );
            }
        } else {
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

    fn print_wide_layout(
        actor_id: u32,
        total_messages: u64,
        total_batches: u64,
        messages_per_batch: &BenchmarkNumericParameter,
        metrics: &BenchmarkIndividualMetrics,
    ) {
        let mut summary_table = Table::new();
        summary_table
            .load_style(UTF8_FULL)
            .set_content_arrangement(ContentArrangement::Dynamic);

        summary_table.add_row(vec![
            format!("ProducingConsumer #{}", actor_id),
            format!("{} msgs (sent & recv)", total_messages.human_count_bare()),
            format!(
                "{} x {}",
                total_batches.human_count_bare(),
                messages_per_batch
            ),
            IggyByteSize::from(metrics.summary.total_user_data_bytes).to_string(),
            format!("{:.2} s", metrics.summary.total_time_secs),
            format!(
                "{:.2} MB/s",
                metrics.summary.throughput_megabytes_per_second
            ),
        ]);

        println!("\n{summary_table}");

        let mut latency_table = Table::new();
        latency_table
            .load_style(UTF8_FULL)
            .set_content_arrangement(ContentArrangement::Dynamic);

        latency_table.add_row(vec![
            "Latency", "p50", "p90", "p95", "p99", "p999", "p9999", "avg", "median", "min", "max",
            "std dev",
        ]);
        latency_table.add_row(vec![
            "(ms)".to_string(),
            format!("{:.2}", metrics.summary.p50_latency_ms),
            format!("{:.2}", metrics.summary.p90_latency_ms),
            format!("{:.2}", metrics.summary.p95_latency_ms),
            format!("{:.2}", metrics.summary.p99_latency_ms),
            format!("{:.2}", metrics.summary.p999_latency_ms),
            format!("{:.2}", metrics.summary.p9999_latency_ms),
            format!("{:.2}", metrics.summary.avg_latency_ms),
            format!("{:.2}", metrics.summary.median_latency_ms),
            format!("{:.2}", metrics.summary.min_latency_ms),
            format!("{:.2}", metrics.summary.max_latency_ms),
            format!("{:.2}", metrics.summary.std_dev_latency_ms),
        ]);

        println!("{latency_table}");
    }

    fn print_narrow_layout(
        actor_id: u32,
        total_messages: u64,
        total_batches: u64,
        messages_per_batch: &BenchmarkNumericParameter,
        metrics: &BenchmarkIndividualMetrics,
    ) {
        let mut table = Table::new();
        table
            .load_style(UTF8_FULL)
            .set_content_arrangement(ContentArrangement::Dynamic)
            .set_width(60);

        table.add_row(vec![
            format!("ProducingConsumer #{}", actor_id),
            String::new(),
        ]);

        table.add_row(vec!["Summary", ""]);
        table.add_row(vec![
            "Messages (sent & received)".to_string(),
            total_messages.human_count_bare().to_string(),
        ]);
        table.add_row(vec![
            "Batches".to_string(),
            format!(
                "{} x {}",
                total_batches.human_count_bare(),
                messages_per_batch
            ),
        ]);
        table.add_row(vec![
            "Total Size".to_string(),
            IggyByteSize::from(metrics.summary.total_user_data_bytes).to_string(),
        ]);
        table.add_row(vec![
            "Duration".to_string(),
            format!("{:.2} s", metrics.summary.total_time_secs),
        ]);

        table.add_row(vec!["Throughput", ""]);
        table.add_row(vec![
            "MB/s".to_string(),
            format!("{:.2}", metrics.summary.throughput_megabytes_per_second),
        ]);

        table.add_row(vec!["Latency", ""]);
        table.add_row(vec![
            "p50".to_string(),
            format!("{:.2} ms", metrics.summary.p50_latency_ms),
        ]);
        table.add_row(vec![
            "p90".to_string(),
            format!("{:.2} ms", metrics.summary.p90_latency_ms),
        ]);
        table.add_row(vec![
            "p95".to_string(),
            format!("{:.2} ms", metrics.summary.p95_latency_ms),
        ]);
        table.add_row(vec![
            "p99".to_string(),
            format!("{:.2} ms", metrics.summary.p99_latency_ms),
        ]);
        table.add_row(vec![
            "p999".to_string(),
            format!("{:.2} ms", metrics.summary.p999_latency_ms),
        ]);
        table.add_row(vec![
            "p9999".to_string(),
            format!("{:.2} ms", metrics.summary.p9999_latency_ms),
        ]);
        table.add_row(vec![
            "avg".to_string(),
            format!("{:.2} ms", metrics.summary.avg_latency_ms),
        ]);
        table.add_row(vec![
            "median".to_string(),
            format!("{:.2} ms", metrics.summary.median_latency_ms),
        ]);
        table.add_row(vec![
            "min".to_string(),
            format!("{:.2} ms", metrics.summary.min_latency_ms),
        ]);
        table.add_row(vec![
            "max".to_string(),
            format!("{:.2} ms", metrics.summary.max_latency_ms),
        ]);
        table.add_row(vec![
            "std dev".to_string(),
            format!("{:.2} ms", metrics.summary.std_dev_latency_ms),
        ]);

        println!("\n{table}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actors::{
        ApiLabel, BatchMetrics, BenchmarkInit,
        consumer::{benchmark_consumer::BenchmarkConsumer, client::interface::ConsumerClient},
        producer::client::interface::ProducerClient,
    };
    use crate::args::common::IggyBenchArgs;
    use crate::utils::finish_condition::BenchmarkFinishConditionMode;
    use clap::Parser;
    use std::{future::ready, sync::atomic::AtomicUsize};
    use tokio::{task::yield_now, time::timeout};

    const POLLS_PER_SEND: usize = 4;
    const PRODUCED_BYTES: u64 = 128;
    const CONSUMED_BYTES: u64 = PRODUCED_BYTES / POLLS_PER_SEND as u64;
    const POLLING_DEADLINE: Duration = Duration::from_millis(300);
    const WARMUP: Duration = Duration::from_millis(10);

    #[tokio::test]
    async fn given_paced_group_when_waiting_for_quota_should_keep_polling() {
        let actor = actor();
        let calls = actor.consumer.calls.clone();
        let poll_backlog = async {
            timeout(POLLING_DEADLINE, async {
                while calls.load(Ordering::Relaxed) < POLLS_PER_SEND {
                    yield_now().await;
                }
            })
            .await
            .expect("all queued batches must be read during the one-second producer wait");
        };

        let (result, ()) = tokio::join!(actor.run(), poll_backlog);
        assert_eq!(
            result.unwrap().summary.total_user_data_bytes,
            PRODUCED_BYTES * 2
        );
    }

    #[tokio::test]
    async fn given_single_role_actor_when_warming_up_should_only_run_that_role() {
        for producer_only in [false, true] {
            let mut actor = actor();
            actor.producer_config.warmup_time = WARMUP.into();
            actor.limit_bytes_per_second = None;
            let inactive_calls = if producer_only {
                actor.poll_finish_condition = BenchmarkFinishCondition::new_empty();
                actor.consumer.calls.clone()
            } else {
                actor.send_finish_condition = BenchmarkFinishCondition::new_empty();
                actor.producer.calls.clone()
            };

            actor.run().await.unwrap();
            assert_eq!(
                inactive_calls.load(Ordering::Relaxed),
                0,
                "warmup must respect the actor's producer_only={producer_only} role"
            );
        }
    }

    #[tokio::test]
    async fn given_no_warmup_when_consuming_should_preserve_committed_offsets() {
        let actor = actor();
        let resets = actor.consumer.resets.clone();
        let consumer = BenchmarkConsumer::new(
            actor.consumer,
            BenchmarkKind::BalancedConsumerGroup,
            actor.poll_finish_condition,
            actor.sampling_time,
            actor.moving_average_window,
            None,
            actor.consumer_config,
        );

        consumer.run().await.unwrap();
        assert_eq!(resets.load(Ordering::Relaxed), 0);
    }

    struct RecordingClient {
        calls: Arc<AtomicUsize>,
        resets: Arc<AtomicUsize>,
        bytes: u64,
    }

    impl RecordingClient {
        fn new(bytes: u64) -> Self {
            Self {
                calls: Arc::default(),
                resets: Arc::default(),
                bytes,
            }
        }

        fn record_batch(&self) -> BatchMetrics {
            self.calls.fetch_add(1, Ordering::Relaxed);
            BatchMetrics {
                messages: 1,
                user_data_bytes: self.bytes,
                total_bytes: self.bytes,
                latency: Duration::ZERO,
            }
        }
    }

    impl ProducerClient for RecordingClient {
        async fn produce_batch(
            &mut self,
            _: &mut BenchmarkBatchGenerator,
        ) -> Result<Option<BatchMetrics>, IggyError> {
            yield_now().await;
            Ok(Some(self.record_batch()))
        }
    }

    impl ConsumerClient for RecordingClient {
        async fn consume_batch(&mut self) -> Result<Option<BatchMetrics>, IggyError> {
            yield_now().await;
            Ok(Some(self.record_batch()))
        }

        fn reset_offsets(&mut self) -> impl Future<Output = Result<(), IggyError>> {
            self.resets.fetch_add(1, Ordering::Relaxed);
            ready(Ok(()))
        }
    }

    impl BenchmarkInit for RecordingClient {
        fn setup(&mut self) -> impl Future<Output = Result<(), IggyError>> {
            ready(Ok(()))
        }
    }

    impl ApiLabel for RecordingClient {
        const API_LABEL: &'static str = "recording";
    }

    impl BenchmarkProducerClient for RecordingClient {}
    impl BenchmarkConsumerClient for RecordingClient {}

    fn actor() -> BenchmarkProducingConsumer<RecordingClient, RecordingClient> {
        let args = IggyBenchArgs::parse_from([
            "iggy-bench",
            "--total-data",
            "256B",
            "--messages-per-batch",
            "1",
            "--message-size",
            "32",
            "end-to-end-producing-consumer-group",
            "--producers",
            "1",
            "--consumers",
            "1",
            "--streams",
            "1",
            "--consumer-groups",
            "1",
            "tcp",
        ]);
        BenchmarkProducingConsumer::new(
            RecordingClient::new(PRODUCED_BYTES),
            RecordingClient::new(CONSUMED_BYTES),
            args.kind(),
            BenchmarkFinishCondition::new(&args, BenchmarkFinishConditionMode::SharedHalf),
            BenchmarkFinishCondition::new(&args, BenchmarkFinishConditionMode::SharedHalf),
            args.sampling_time(),
            args.moving_average_window(),
            Some(PRODUCED_BYTES.into()),
            BenchmarkProducerConfig {
                producer_id: 1,
                stream_id: "bench-stream-1".to_owned(),
                partitions: 1,
                messages_per_batch: args.messages_per_batch(),
                message_size: args.message_size(),
                warmup_time: args.warmup_time(),
                pretty: false,
            },
            BenchmarkConsumerConfig {
                consumer_id: 1,
                consumer_group_id: Some(1),
                stream_id: "bench-stream-1".to_owned(),
                messages_per_batch: args.messages_per_batch(),
                warmup_time: args.warmup_time(),
                polling_kind: PollingKind::Next,
                origin_timestamp_latency_calculation: false,
                pretty: false,
            },
        )
    }
}
