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
use crate::actors::stress::admin_exerciser::AdminExerciser;
use crate::actors::stress::control_plane_churner::{ChurnerConfig, ControlPlaneChurner};
use crate::actors::stress::error_classifier;
use crate::actors::stress::health_monitor::HealthMonitor;
use crate::actors::stress::stress_context::StressContext;
use crate::actors::stress::verifier::StressVerifier;
use crate::args::common::IggyBenchArgs;
use crate::args::kind::BenchmarkKindCommand;
use crate::args::kinds::stress::args::ApiMix;
use crate::utils::ClientFactory;
use async_trait::async_trait;
use bench_report::{
    actor_kind::ActorKind, benchmark_kind::BenchmarkKind,
    individual_metrics::BenchmarkIndividualMetrics,
};
use iggy::prelude::*;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use tracing::{info, warn};

/// Phase durations as fractions of total test time.
const BASELINE_FRACTION: f64 = 0.15;
const CHAOS_FRACTION: f64 = 0.65;
/// Max drain phase duration.
const MAX_DRAIN_SECS: u64 = 300;

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
        let total_secs = total.as_secs_f64();

        let baseline = Duration::from_secs_f64(total_secs * BASELINE_FRACTION);
        let chaos = Duration::from_secs_f64(total_secs * CHAOS_FRACTION);
        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let drain_secs = (total_secs * (1.0 - BASELINE_FRACTION - CHAOS_FRACTION)) as u64;
        let drain = Duration::from_secs(drain_secs.min(MAX_DRAIN_SECS));

        (baseline, chaos, drain)
    }

    /// Spawns chaos actors: control-plane churner, admin exerciser, health monitor.
    fn spawn_chaos_actors(
        &self,
        tasks: &mut JoinSet<Result<BenchmarkIndividualMetrics, IggyError>>,
        ctx: &Arc<StressContext>,
        chaos_duration: Duration,
    ) {
        let stress_args = self.stress_args();
        let api_mix = stress_args.api_mix();
        let chaos_seed = stress_args.chaos_seed();

        // Health monitor always runs
        let monitor = HealthMonitor::new(self.client_factory.clone(), ctx.clone());
        tasks.spawn(async move {
            monitor.run().await;
            Ok(BenchmarkIndividualMetrics::placeholder("health_monitor"))
        });

        // Control-plane churner(s) unless data-plane-only
        if !matches!(api_mix, ApiMix::DataPlaneOnly) {
            let churn_concurrency = stress_args.churn_concurrency().get();
            let churn_interval = stress_args.churn_interval();

            let purge_cutoff = Instant::now() + chaos_duration / 2;

            let churner_config = ChurnerConfig {
                api_mix,
                streams: self.args.streams(),
                partitions: self.args.number_of_partitions(),
                consumer_groups: self.args.number_of_consumer_groups(),
                message_expiry: stress_args.message_expiry,
                max_topic_size: MaxTopicSize::Custom(stress_args.max_topic_size),
                purge_cutoff,
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

        // Admin exerciser for mixed/all modes
        if matches!(api_mix, ApiMix::Mixed | ApiMix::All) {
            let admin = AdminExerciser::new(self.client_factory.clone(), ctx.clone());
            tasks.spawn(async move {
                admin.run().await;
                Ok(BenchmarkIndividualMetrics::placeholder("admin_exerciser"))
            });
        }
    }
}

#[async_trait]
impl Benchmarkable for StressBenchmark {
    async fn run(
        &mut self,
    ) -> Result<JoinSet<Result<BenchmarkIndividualMetrics, IggyError>>, IggyError> {
        let overall_start = Instant::now();
        let (baseline_duration, chaos_duration, drain_max) = self.compute_phase_durations();

        let total = self.stress_args().duration().get_duration();
        let remaining_after_chaos = total.saturating_sub(baseline_duration + chaos_duration);
        if drain_max < remaining_after_chaos {
            warn!(
                "Drain cap ({drain_max:?}) is shorter than remaining duration ({remaining_after_chaos:?}); \
                 data-plane actors may be aborted before finishing. Consider shorter --duration or raising MAX_DRAIN_SECS."
            );
        }

        info!(
            "Stress test starting: baseline={baseline_duration:?}, chaos={chaos_duration:?}, drain_max={drain_max:?}"
        );

        self.init_streams().await?;
        init_consumer_groups(&self.client_factory, &self.args).await?;

        let ctx = Arc::new(StressContext::new());

        let mut tasks: JoinSet<Result<BenchmarkIndividualMetrics, IggyError>> = JoinSet::new();

        // === PHASE 1: Baseline / Produce (data-plane only) ===
        info!("=== Phase 1: Baseline ({baseline_duration:?}) ===");

        let producer_futures = build_producer_futures(&self.client_factory, &self.args);
        let consumer_futures = build_consumer_futures(&self.client_factory, &self.args);

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

        self.spawn_chaos_actors(&mut tasks, &ctx, chaos_duration);

        tokio::time::sleep(chaos_duration).await;
        let chaos_elapsed = overall_start.elapsed();
        info!("Chaos phase completed in {chaos_elapsed:?}");

        // === PHASE 3: Drain ===
        info!("=== Phase 3: Drain (max {drain_max:?}) ===");
        ctx.cancel();

        let drain_start = Instant::now();
        let drain_deadline = drain_start + drain_max;
        let mut producer_batches: u64 = 0;
        let mut consumer_batches: u64 = 0;

        while !tasks.is_empty() {
            let remaining = drain_deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                warn!("Drain phase timed out, aborting remaining tasks");
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
                    warn!("Actor join failed: {e}");
                }
                Ok(None) => break,
                Err(_) => {
                    warn!("Drain phase timed out");
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

        let drain_elapsed = drain_start.elapsed();

        // === Verification ===
        info!("Running post-test verification...");
        let verifier = StressVerifier::new(
            self.client_factory.clone(),
            self.args.streams(),
            self.args.number_of_partitions(),
        );
        let verification = verifier.verify().await;

        let report = StressReport::build(
            &ctx.stats,
            &verification,
            overall_start.elapsed(),
            baseline_duration,
            chaos_duration,
            drain_elapsed,
        );
        report.print_summary();

        if !verification.passed {
            warn!("Stress test verification FAILED");
        }

        Ok(JoinSet::new())
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
            "Starting Stress benchmark: duration={}, producers={}, consumers={}, churn_concurrency={}, churn_interval={}, api_mix={:?}, chaos_seed={}",
            stress_args.duration(),
            stress_args.producers.get(),
            stress_args.consumers.get(),
            stress_args.churn_concurrency().get(),
            stress_args.churn_interval(),
            stress_args.api_mix(),
            stress_args.chaos_seed(),
        );
    }
}
