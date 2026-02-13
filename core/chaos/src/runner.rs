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

use crate::args::RunArgs;
use crate::client::create_client;
use crate::error::LabError;
use crate::in_flight::InFlightOps;
use crate::invariants::{self, InvariantViolation};
use crate::report::ArtifactBundle;
use crate::scenarios::{self, CleanupPolicy, NamespacePrefixes, VerifyContext};
use crate::shadow::{ResourceOrigin, ShadowState};
use crate::trace;
use crate::watermark::WatermarkState;
use crate::worker::{WorkerConfig, WorkerInit, WorkerResult, run_worker};
use iggy::prelude::*;
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use tracing::{error, info, warn};

pub struct LabRunner {
    args: RunArgs,
}

#[allow(dead_code)]
pub struct RunOutcome {
    pub passed: bool,
    pub run_id: String,
    pub ops_total: u64,
    pub violations: Vec<InvariantViolation>,
    pub duration: Duration,
}

impl LabRunner {
    pub fn new(args: RunArgs) -> Self {
        Self { args }
    }

    pub async fn run(self) -> Result<RunOutcome, LabError> {
        let seed = self.args.seed.unwrap_or_else(rand::random);
        let scenario = scenarios::create_scenario(self.args.scenario);
        let prefixes = NamespacePrefixes::from_base(&self.args.prefix);

        let start_wall = chrono::Utc::now();
        let start_instant = Instant::now();
        let run_id = format!(
            "{}_{seed}_{}",
            self.args.scenario,
            start_wall.format("%Y%m%dT%H%M%S")
        );
        let start_time_utc = start_wall.to_rfc3339();

        let explicit_output = self.args.output_dir.is_some();
        let bundle_dir = match self.args.output_dir {
            Some(ref dir) => dir.clone(),
            None => PathBuf::from("chaos_logs").join(&run_id),
        };
        std::fs::create_dir_all(&bundle_dir).map_err(|e| LabError::Io {
            context: format!("creating bundle dir {}", bundle_dir.display()),
            source: e,
        })?;

        info!("=== iggy-chaos ===");
        info!("Run ID: {run_id}");
        info!("Scenario: {}", scenario.name());
        info!("Seed: {seed}");
        info!(
            "Server: {} ({})",
            self.args.server_address, self.args.transport
        );
        info!("Workers: {}", self.args.workers);
        info!("Duration: {}", self.args.duration);
        if let Some(ops) = self.args.ops {
            info!("Ops target: {ops}");
        }
        info!("Prefix: {:?}", self.args.prefix);
        info!("Bundle: {}", bundle_dir.display());

        // Pre-flight check
        let admin_client = create_client(&self.args.server_address, self.args.transport).await?;
        self.preflight_check(&admin_client, &prefixes, self.args.force_cleanup)
            .await?;

        // Setup phase - delegated to the scenario
        scenario.setup(&admin_client, &prefixes).await?;

        let shared_state = Arc::new(tokio::sync::RwLock::new(ShadowState::new(prefixes.clone())));
        let watermarks = Arc::new(tokio::sync::Mutex::new(WatermarkState::new()));
        let in_flight = Arc::new(InFlightOps::new());
        let stop = Arc::new(AtomicBool::new(false));
        let seq_counter = Arc::new(AtomicU64::new(0));
        let ops_counter = Arc::new(AtomicU64::new(0));

        // Populate initial shadow state from setup
        {
            let mut state = shared_state.write().await;
            let streams = admin_client.get_streams().await?;
            for stream in streams.iter().filter(|s| prefixes.matches(&s.name)) {
                state.apply_create_stream(stream.name.clone(), ResourceOrigin::Setup);
                let stream_id = Identifier::numeric(stream.id).unwrap();
                if let Ok(Some(details)) = admin_client.get_stream(&stream_id).await {
                    for topic in &details.topics {
                        state.apply_create_topic(
                            &stream.name,
                            topic.name.clone(),
                            topic.partitions_count,
                            ResourceOrigin::Setup,
                        );
                    }
                }
            }
        }

        // Spawn workers per lane
        let lanes = scenario.lanes();
        let mut join_set = JoinSet::new();
        let mut worker_id: u32 = 0;

        let (trace_writer, sender_factory) = trace::create_trace_channel(start_instant);
        let trace_path = bundle_dir.join("trace.jsonl");
        let trace_handle = tokio::task::spawn_blocking(move || trace_writer.run(&trace_path));

        for lane in &lanes {
            let lane_workers = lane.worker_count.unwrap_or(self.args.workers);
            info!("Lane '{}': {} workers", lane.label, lane_workers);
            for _ in 0..lane_workers {
                let worker_rng = StdRng::seed_from_u64(derive_worker_seed(seed, worker_id));
                let client = create_client(&self.args.server_address, self.args.transport).await?;
                let config = WorkerConfig {
                    msg_size: self.args.message_size,
                    messages_per_batch: self.args.messages_per_batch,
                    prefixes: prefixes.clone(),
                    namespace: lane.namespace,
                    fail_fast: !self.args.no_fail_fast,
                    ops_target: self.args.ops,
                    scope: lane.scope,
                    rate: lane.rate,
                    allow_destructive: lane.allow_destructive,
                    consumer_id: worker_id + 1,
                    lane: lane.label,
                };
                let init = WorkerInit {
                    id: worker_id,
                    client,
                    rng: worker_rng,
                    shared_state: Arc::clone(&shared_state),
                    watermarks: Arc::clone(&watermarks),
                    in_flight: Arc::clone(&in_flight),
                    trace_sender: Some(sender_factory.sender(worker_id, lane.label)),
                    seq_counter: Arc::clone(&seq_counter),
                    ops_counter: Arc::clone(&ops_counter),
                    start: start_instant,
                };
                let weights = lane.op_weights.clone();
                let stop_clone = Arc::clone(&stop);
                join_set.spawn(async move { run_worker(init, &weights, config, stop_clone).await });
                worker_id += 1;
            }
        }

        let run_start = Instant::now();

        if worker_id == 0 {
            info!("No workers to run, skipping chaos phase.");
        } else {
            let duration: Duration = self.args.duration.get_duration();
            let ops_target = self.args.ops;
            tokio::select! {
                _ = tokio::time::sleep(duration) => {
                    info!("Duration elapsed, stopping workers...");
                }
                _ = shutdown_signal() => {
                    info!("Received shutdown signal, stopping workers...");
                }
                _ = wait_for_ops_target(&ops_counter, ops_target) => {
                    info!("Op count target reached, stopping workers...");
                }
            }
            stop.store(true, Ordering::Relaxed);
        }

        // Collect results
        let mut total_ops = 0u64;
        let mut violations = Vec::new();
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(WorkerResult::Ok { ops_executed }) => {
                    total_ops += ops_executed;
                }
                Ok(WorkerResult::ServerBug(v)) => {
                    violations.push(v);
                }
                Err(e) => {
                    error!("Worker panicked: {e}");
                }
            }
        }

        // Close the trace channel and wait for the writer to flush
        let trace_drops = sender_factory.dropped();
        drop(sender_factory);
        if let Err(e) = trace_handle.await {
            error!("Trace writer task panicked: {e}");
        }

        let run_duration = run_start.elapsed();
        info!("All workers stopped. Total ops: {total_ops}, duration: {run_duration:.1?}");

        // Post-run verification - delegated to the scenario
        if !self.args.skip_post_run_verify {
            info!("Running post-run verification...");
            let state = shared_state.read().await;
            let wm = watermarks.lock().await;
            let ctx = VerifyContext {
                client: &admin_client,
                shadow: &state,
                prefixes: &prefixes,
                server_address: &self.args.server_address,
                transport: self.args.transport,
            };
            let post_violations = scenario.verify(&ctx).await;
            if !post_violations.is_empty() {
                warn!("Post-run violations: {}", post_violations.len());
            }
            violations.extend(post_violations);

            // Watermark-based verification (purge + phantom read detection)
            let wm_violations = invariants::watermark_verify(&admin_client, &wm, &state).await;
            if !wm_violations.is_empty() {
                warn!("Watermark violations: {}", wm_violations.len());
            }
            violations.extend(wm_violations);
        }

        let passed = violations.is_empty();

        // Write artifacts
        {
            let bundle = ArtifactBundle {
                run_id: &run_id,
                seed,
                scenario_name: scenario.name(),
                server_address: &self.args.server_address,
                transport: self.args.transport.to_string(),
                workers: self.args.workers,
                duration: run_duration,
                total_ops,
                passed,
                violations: &violations,
                ops_target: self.args.ops,
                start_time_utc: &start_time_utc,
                message_size: self.args.message_size,
                messages_per_batch: self.args.messages_per_batch,
                prefix: &self.args.prefix,
                no_fail_fast: self.args.no_fail_fast,
                force_cleanup: self.args.force_cleanup,
                no_cleanup: self.args.no_cleanup,
                skip_post_run_verify: self.args.skip_post_run_verify,
                trace_drops,
            };
            if let Err(e) = bundle.write(&bundle_dir) {
                error!("Failed to write artifacts: {e}");
            }
        }

        // Generate Perfetto trace from the JSONL data
        match crate::perfetto::convert(&bundle_dir) {
            Ok(stats) => {
                info!(
                    "Perfetto trace: {} events → {}",
                    stats.events,
                    bundle_dir.join("trace.perfetto.json").display(),
                );
                if stats.unmatched_outcomes > 0 {
                    warn!(
                        "Perfetto: {} unmatched outcome entries (missing intents)",
                        stats.unmatched_outcomes,
                    );
                }
            }
            Err(e) => {
                warn!("Failed to generate Perfetto trace: {e}");
            }
        }

        // Cleanup - policy-driven by the scenario
        if !self.args.no_cleanup {
            match scenario.cleanup_policy() {
                CleanupPolicy::DeletePrefix => {
                    info!("Cleaning up lab resources...");
                    crate::client::cleanup_all(&admin_client, &prefixes).await;
                }
                CleanupPolicy::NoOp => {
                    info!("Cleanup skipped (scenario policy: NoOp).");
                }
            }
        }

        if passed {
            info!("PASSED - {total_ops} ops, 0 violations");
            if !explicit_output {
                let _ = std::fs::remove_dir_all(&bundle_dir);
            }
        } else {
            error!("FAILED - {total_ops} ops, {} violations", violations.len());
            for v in &violations {
                error!("  [{}] {}: {}", v.kind, v.description, v.context);
            }
            if !explicit_output {
                error!("Bundle preserved at: {}", bundle_dir.display());
            }
        }

        Ok(RunOutcome {
            passed,
            run_id,
            ops_total: total_ops,
            violations,
            duration: run_duration,
        })
    }

    async fn preflight_check(
        &self,
        client: &IggyClient,
        prefixes: &NamespacePrefixes,
        force: bool,
    ) -> Result<(), LabError> {
        let streams = client.get_streams().await?;
        let stale: Vec<_> = streams
            .iter()
            .filter(|s| prefixes.matches(&s.name))
            .collect();

        if stale.is_empty() {
            return Ok(());
        }

        if force {
            info!(
                "Force-cleaning {} stale resources with base prefix '{}'",
                stale.len(),
                prefixes.base,
            );
            for stream in &stale {
                let id = Identifier::numeric(stream.id).unwrap();
                if let Err(e) = client.delete_stream(&id).await {
                    warn!("Failed to delete stale stream '{}': {e}", stream.name);
                }
            }
            Ok(())
        } else {
            error!(
                "Found {} stale resources with base prefix '{}'. Use --force-cleanup to remove them.",
                stale.len(),
                prefixes.base,
            );
            Err(LabError::PrefixViolation(format!(
                "found {} stale resources with base prefix '{}'",
                stale.len(),
                prefixes.base,
            )))
        }
    }
}

async fn wait_for_ops_target(counter: &AtomicU64, target: Option<u64>) {
    let Some(target) = target else {
        std::future::pending::<()>().await;
        return;
    };
    let mut interval = tokio::time::interval(Duration::from_millis(50));
    loop {
        interval.tick().await;
        if counter.load(Ordering::Relaxed) >= target {
            return;
        }
    }
}

/// Derive a per-worker seed that avoids the collision problem of `seed + worker_id`
/// (where seed=0 worker=1 collides with seed=1 worker=0).
///
/// Uses ahash with fixed seeds for cross-version determinism (DefaultHasher
/// delegates to SipHash whose output is not guaranteed stable across rustc releases).
fn derive_worker_seed(base_seed: u64, worker_id: u32) -> u64 {
    const FIXED: ahash::RandomState = ahash::RandomState::with_seeds(0, 0, 0, 0);
    FIXED.hash_one((base_seed, worker_id))
}

/// Resolves when either SIGINT (Ctrl+C) or SIGTERM is received.
async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    let mut sigterm = signal(SignalKind::terminate()).expect("Failed to register SIGTERM handler");

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = sigterm.recv() => {}
    }
}
