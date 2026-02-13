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

use super::benchmark::Benchmarkable;
use super::common::{build_consumer_futures, build_producer_futures, init_consumer_groups};
use super::stress_report::StressReport;
use super::{BENCH_TOPIC_NAME, CHAOS_STREAM_PREFIX, STABLE_STREAM_PREFIX};
use crate::actors::stress::admin_exerciser::AdminExerciser;
use crate::actors::stress::control_plane_churner::{ChurnerConfig, ControlPlaneChurner};
use crate::actors::stress::error_classifier;
use crate::actors::stress::health_monitor::HealthMonitor;
use crate::actors::stress::stress_context::StressContext;
use crate::actors::stress::verifier::StressVerifier;
use crate::args::common::IggyBenchArgs;
use crate::args::kind::BenchmarkKindCommand;
use crate::args::kinds::stress::args::ApiMix;
use crate::utils::finish_condition::BenchmarkFinishCondition;
use crate::utils::{ClientFactory, login_root};
use async_trait::async_trait;
use bench_report::{
    actor_kind::ActorKind, benchmark_kind::BenchmarkKind,
    individual_metrics::BenchmarkIndividualMetrics,
};
use iggy::prelude::*;
use std::io::{Cursor, Read as _};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use tracing::{info, warn};

pub struct StressBenchmark {
    args: Arc<IggyBenchArgs>,
    client_factory: Arc<dyn ClientFactory>,
}

impl StressBenchmark {
    pub fn new(args: Arc<IggyBenchArgs>, client_factory: Arc<dyn ClientFactory>) -> Self {
        Self {
            args,
            client_factory,
        }
    }

    fn stress_args(&self) -> &crate::args::kinds::stress::args::StressArgs {
        match &self.args.benchmark_kind {
            BenchmarkKindCommand::Stress(args) => args,
            _ => unreachable!("StressBenchmark only used with Stress variant"),
        }
    }

    fn compute_phase_durations(&self) -> (Duration, Duration, Duration) {
        let total = self.stress_args().duration().get_duration();
        let baseline = self.stress_args().baseline_duration().get_duration();
        let quiesce = self.stress_args().quiesce_duration().get_duration();
        let chaos = total.saturating_sub(baseline + quiesce);
        (baseline, chaos, quiesce)
    }

    /// Spawns chaos actors: control-plane churner, admin exerciser, health monitor.
    fn spawn_chaos_actors(
        &self,
        tasks: &mut JoinSet<Result<BenchmarkIndividualMetrics, IggyError>>,
        ctx: &Arc<StressContext>,
    ) {
        let stress_args = self.stress_args();
        let api_mix = stress_args.api_mix();
        let chaos_seed = stress_args.chaos_seed();

        let monitor = HealthMonitor::new(self.client_factory.clone(), ctx.clone());
        tasks.spawn(async move {
            monitor.run().await;
            Ok(BenchmarkIndividualMetrics::placeholder("health_monitor"))
        });

        if !matches!(api_mix, ApiMix::DataPlaneOnly) {
            let churn_concurrency = stress_args.churn_concurrency().get();
            let churn_interval = stress_args.churn_interval();

            let churner_config = ChurnerConfig {
                api_mix,
                stable_streams: stress_args.stable_streams(),
                chaos_streams: stress_args.chaos_streams(),
                partitions: self.args.number_of_partitions(),
                message_expiry: stress_args.message_expiry,
                max_topic_size: MaxTopicSize::Custom(stress_args.max_topic_size),
            };

            for i in 0..churn_concurrency {
                let churner = ControlPlaneChurner::new(
                    i + 1,
                    self.client_factory.clone(),
                    ctx.clone(),
                    churn_interval,
                    chaos_seed,
                    &churner_config,
                );
                tasks.spawn(async move {
                    churner.run().await;
                    Ok(BenchmarkIndividualMetrics::placeholder(
                        "control_plane_churner",
                    ))
                });
            }
        }

        if matches!(api_mix, ApiMix::Mixed | ApiMix::All) {
            let admin = AdminExerciser::new(
                self.client_factory.clone(),
                ctx.clone(),
                format!("{STABLE_STREAM_PREFIX}-1"),
            );
            tasks.spawn(async move {
                admin.run().await;
                Ok(BenchmarkIndividualMetrics::placeholder("admin_exerciser"))
            });
        }
    }

    /// Best-effort validation of server config via snapshot API.
    /// Warns about common misconfigurations but never blocks the test.
    async fn validate_server_config(&self) {
        let client = self.client_factory.create_client().await;
        let client = IggyClient::create(client, None, None);
        login_root(&client).await;

        let snapshot = client
            .snapshot(
                SnapshotCompression::Stored,
                vec![SystemSnapshotType::ServerConfig],
            )
            .await;

        let Ok(snapshot) = snapshot else {
            warn!("Could not fetch server config snapshot for validation; skipping checks");
            return;
        };

        let cursor = Cursor::new(snapshot.0);
        let Ok(mut zip) = zip::ZipArchive::new(cursor) else {
            warn!("Could not parse server config snapshot as ZIP; skipping checks");
            return;
        };

        let Ok(mut entry) = zip.by_name("server_config.txt") else {
            warn!("server_config.txt not found in snapshot; skipping checks");
            return;
        };

        let mut config_str = String::new();
        if entry.read_to_string(&mut config_str).is_err() {
            warn!("Could not read server_config.txt from snapshot; skipping checks");
            return;
        }
        drop(entry);

        let Ok(config) = config_str.parse::<toml::Value>() else {
            warn!("Could not parse server config as TOML; skipping checks");
            return;
        };

        let stress_args = self.stress_args();

        // Check if message cleaner is enabled when message_expiry is set
        if !matches!(stress_args.message_expiry, IggyExpiry::NeverExpire) {
            let cleaner_enabled = config
                .get("data_maintenance")
                .and_then(|dm| dm.get("messages"))
                .and_then(|m| m.get("cleaner_enabled"))
                .and_then(toml::Value::as_bool)
                .unwrap_or(false);

            if !cleaner_enabled {
                warn!(
                    "Server has data_maintenance.messages.cleaner_enabled=false but stress test \
                     uses message_expiry={}. Messages will not be cleaned up. \
                     Set IGGY_DATA_MAINTENANCE_MESSAGES_CLEANER_ENABLED=true to enable cleanup.",
                    stress_args.message_expiry
                );
            }
        }

        // Check segment size vs max_topic_size
        let segment_size = config
            .get("system")
            .and_then(|s| s.get("segment"))
            .and_then(|s| s.get("size"))
            .and_then(toml::Value::as_str)
            .and_then(|s| IggyByteSize::from_str(s).ok());

        if let Some(seg_size) = segment_size {
            let max_reasonable = IggyByteSize::from_str("64MiB").expect("valid constant");
            if seg_size > max_reasonable {
                warn!(
                    "Server segment size ({seg_size}) is large for stress testing. \
                     Consider IGGY_SYSTEM_SEGMENT_SIZE=\"16MiB\" for faster segment \
                     rotation and more race surface."
                );
            }
        }

        info!("Server config validation complete");
    }

    /// Drains the `JoinSet` within the quiesce window, collecting data-plane batch
    /// counts and recording errors. Aborts remaining actors if the deadline expires.
    async fn drain_actors(
        tasks: &mut JoinSet<Result<BenchmarkIndividualMetrics, IggyError>>,
        ctx: &StressContext,
        quiesce_duration: Duration,
    ) -> Duration {
        let quiesce_start = Instant::now();
        let quiesce_deadline = quiesce_start + quiesce_duration;
        let mut producer_batches: u64 = 0;
        let mut consumer_batches: u64 = 0;

        while !tasks.is_empty() {
            let remaining = quiesce_deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                warn!(
                    "{} actors didn't finish within quiesce window, aborting",
                    tasks.len()
                );
                tasks.abort_all();
                break;
            }

            match tokio::time::timeout(remaining, tasks.join_next()).await {
                Ok(Some(Ok(Ok(metrics)))) => match metrics.summary.actor_kind {
                    ActorKind::Producer => {
                        producer_batches += metrics.summary.total_message_batches;
                    }
                    ActorKind::Consumer => {
                        consumer_batches += metrics.summary.total_message_batches;
                    }
                    _ => {}
                },
                Ok(Some(Ok(Err(e)))) => {
                    warn!("Actor failed: {e}");
                    error_classifier::record_error(&ctx.stats, &e);
                }
                Ok(Some(Err(e))) => {
                    warn!("Actor join error: {e}");
                }
                Ok(None) => break,
                Err(_) => {
                    warn!(
                        "{} actors didn't finish within quiesce window, aborting",
                        tasks.len()
                    );
                    tasks.abort_all();
                    break;
                }
            }
        }

        ctx.stats
            .send_messages_ok
            .fetch_add(producer_batches, Ordering::Relaxed);
        ctx.stats
            .poll_messages_ok
            .fetch_add(consumer_batches, Ordering::Relaxed);

        quiesce_start.elapsed()
    }
}

#[async_trait]
impl Benchmarkable for StressBenchmark {
    async fn run(
        &mut self,
    ) -> Result<JoinSet<Result<BenchmarkIndividualMetrics, IggyError>>, IggyError> {
        let overall_start = Instant::now();
        let stress_args = self.stress_args();
        let stable_count = stress_args.stable_streams();
        let chaos_count = stress_args.chaos_streams();
        let (baseline_duration, chaos_duration, quiesce_duration) = self.compute_phase_durations();

        info!(
            "Stress test starting: baseline={baseline_duration:?}, chaos={chaos_duration:?}, quiesce={quiesce_duration:?}"
        );

        self.validate_server_config().await;

        self.init_streams().await?;

        let stable_names: Vec<String> = (1..=stable_count)
            .map(|i| format!("{STABLE_STREAM_PREFIX}-{i}"))
            .collect();
        let chaos_names: Vec<String> = (1..=chaos_count)
            .map(|i| format!("{CHAOS_STREAM_PREFIX}-{i}"))
            .collect();
        let all_stream_names: Vec<String> =
            stable_names.iter().chain(&chaos_names).cloned().collect();

        init_consumer_groups(&self.client_factory, &stable_names).await?;

        let ctx = Arc::new(StressContext::new());

        let finish_condition = BenchmarkFinishCondition::new_cancellable(ctx.cancelled.clone());

        let mut tasks: JoinSet<Result<BenchmarkIndividualMetrics, IggyError>> = JoinSet::new();

        // === PHASE 1: Baseline (data-plane only) ===
        info!("=== Phase 1: Baseline ({baseline_duration:?}) ===");

        let producer_futures = build_producer_futures(
            &self.client_factory,
            &self.args,
            &all_stream_names,
            Some(finish_condition.clone()),
        );
        let consumer_futures = build_consumer_futures(
            &self.client_factory,
            &self.args,
            &stable_names,
            Some(finish_condition),
        );

        for fut in producer_futures {
            tasks.spawn(fut);
        }
        for fut in consumer_futures {
            tasks.spawn(fut);
        }

        tokio::time::sleep(baseline_duration).await;
        let baseline_elapsed = overall_start.elapsed();
        info!("Baseline phase completed in {baseline_elapsed:?}");

        // === PHASE 2: Chaos (add churners + admin + health alongside ongoing data-plane) ===
        info!("=== Phase 2: Chaos ({chaos_duration:?}) ===");

        self.spawn_chaos_actors(&mut tasks, &ctx);

        tokio::time::sleep(chaos_duration).await;
        let chaos_elapsed = overall_start.elapsed();
        info!("Chaos phase completed in {chaos_elapsed:?}");

        // === PHASE 3: Quiesce ===
        info!("=== Phase 3: Quiesce ({quiesce_duration:?}) ===");
        ctx.cancel();

        let quiesce_elapsed = Self::drain_actors(&mut tasks, &ctx, quiesce_duration).await;

        // === Verification ===
        info!("Running post-test verification...");
        let verifier = StressVerifier::new(
            self.client_factory.clone(),
            stable_count,
            chaos_count,
            self.args.number_of_partitions(),
        );
        let verification = verifier.verify().await;

        let report = StressReport::build(
            &ctx.stats,
            &verification,
            overall_start.elapsed(),
            baseline_duration,
            chaos_duration,
            quiesce_elapsed,
        );
        report.print_summary();

        if !verification.passed {
            warn!("Stress test verification FAILED");
        }

        Ok(JoinSet::new())
    }

    async fn init_streams(&self) -> Result<(), IggyError> {
        let stress_args = self.stress_args();
        let stable_count = stress_args.stable_streams();
        let chaos_count = stress_args.chaos_streams();
        let partitions_count = self.args.number_of_partitions();

        let client = self.client_factory.create_client().await;
        let client = IggyClient::create(client, None, None);
        login_root(&client).await;

        let streams = client.get_streams().await?;

        // Stable streams: NeverExpire, ServerDefault size
        for i in 1..=stable_count {
            let stream_name = format!("{STABLE_STREAM_PREFIX}-{i}");
            if streams.iter().all(|s| s.name != stream_name) {
                info!("Creating stable stream '{stream_name}'");
                client.create_stream(&stream_name).await?;
                let stream_id: Identifier = stream_name.as_str().try_into()?;
                client
                    .create_topic(
                        &stream_id,
                        BENCH_TOPIC_NAME,
                        partitions_count,
                        CompressionAlgorithm::default(),
                        None,
                        IggyExpiry::NeverExpire,
                        MaxTopicSize::ServerDefault,
                    )
                    .await?;
            }
        }

        // Chaos streams: user-configured expiry and max_topic_size
        for i in 1..=chaos_count {
            let stream_name = format!("{CHAOS_STREAM_PREFIX}-{i}");
            if streams.iter().all(|s| s.name != stream_name) {
                info!("Creating chaos stream '{stream_name}'");
                client.create_stream(&stream_name).await?;
                let stream_id: Identifier = stream_name.as_str().try_into()?;
                client
                    .create_topic(
                        &stream_id,
                        BENCH_TOPIC_NAME,
                        partitions_count,
                        CompressionAlgorithm::default(),
                        None,
                        stress_args.message_expiry,
                        MaxTopicSize::Custom(stress_args.max_topic_size),
                    )
                    .await?;
            }
        }

        Ok(())
    }

    fn kind(&self) -> BenchmarkKind {
        BenchmarkKind::Stress
    }

    fn args(&self) -> &IggyBenchArgs {
        &self.args
    }

    fn client_factory(&self) -> &Arc<dyn ClientFactory> {
        &self.client_factory
    }

    fn print_info(&self) {
        let stress_args = self.stress_args();
        info!(
            "Starting Stress benchmark: duration={}, producers={}, consumers={}, stable_streams={}, chaos_streams={}, churn_concurrency={}, churn_interval={}, api_mix={:?}, chaos_seed={}, baseline={}, quiesce={}",
            stress_args.duration(),
            stress_args.producers.get(),
            stress_args.consumers.get(),
            stress_args.stable_streams(),
            stress_args.chaos_streams(),
            stress_args.churn_concurrency().get(),
            stress_args.churn_interval(),
            stress_args.api_mix(),
            stress_args.chaos_seed(),
            stress_args.baseline_duration(),
            stress_args.quiesce_duration(),
        );
    }
}
