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

use crate::error::LabError;
use crate::invariants::{self, InvariantViolation, PartitionKey};
use crate::ops::{ErrorClass, Op, OpKind, OpOutcome};
use crate::scenarios::{LaneNamespace, NamespacePrefixes, NamespaceScope, RateModel};
use crate::shadow::ShadowState;
use crate::trace::WorkerTraceWriter;
use iggy::prelude::IggyClient;
use rand::distr::weighted::WeightedIndex;
use rand::prelude::Distribution;
use rand::rngs::StdRng;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, error, warn};

const OP_EXECUTE_TIMEOUT: Duration = Duration::from_secs(30);

pub struct WorkerConfig {
    pub msg_size: u32,
    pub messages_per_batch: u32,
    pub prefixes: NamespacePrefixes,
    pub namespace: LaneNamespace,
    pub fail_fast: bool,
    pub ops_target: Option<u64>,
    pub scope: NamespaceScope,
    pub rate: RateModel,
    pub destructive: bool,
    pub consumer_id: u32,
}

pub struct WorkerInit {
    pub id: u32,
    pub client: IggyClient,
    pub rng: StdRng,
    pub shared_state: Arc<RwLock<ShadowState>>,
    pub trace_writer: Option<WorkerTraceWriter>,
    pub seq_counter: Arc<AtomicU64>,
    pub ops_counter: Arc<AtomicU64>,
}

struct Worker {
    id: u32,
    client: IggyClient,
    rng: StdRng,
    shared_state: Arc<RwLock<ShadowState>>,
    trace_writer: Option<WorkerTraceWriter>,
    op_kinds: Vec<OpKind>,
    weighted_dist: WeightedIndex<f64>,
    config: WorkerConfig,
    seq_counter: Arc<AtomicU64>,
    ops_counter: Arc<AtomicU64>,
}

pub enum WorkerResult {
    Ok { ops_executed: u64 },
    ServerBug(InvariantViolation),
}

impl Worker {
    fn try_new(
        init: WorkerInit,
        op_weights: &[(OpKind, f64)],
        config: WorkerConfig,
    ) -> Option<Self> {
        let effective: Vec<_> = op_weights
            .iter()
            .filter(|(kind, _)| {
                let scope_ok = config.scope != NamespaceScope::SetupOnly || !kind.is_creative();
                let destr_ok = config.destructive || !kind.is_destructive();
                scope_ok && destr_ok
            })
            .cloned()
            .collect();
        let op_kinds: Vec<OpKind> = effective.iter().map(|(k, _)| *k).collect();
        let weights: Vec<f64> = effective.iter().map(|(_, w)| *w).collect();
        let weighted_dist = WeightedIndex::new(&weights).ok()?;

        Some(Self {
            id: init.id,
            client: init.client,
            rng: init.rng,
            shared_state: init.shared_state,
            trace_writer: init.trace_writer,
            op_kinds,
            weighted_dist,
            config,
            seq_counter: init.seq_counter,
            ops_counter: init.ops_counter,
        })
    }

    async fn run(mut self, stop: Arc<AtomicBool>) -> WorkerResult {
        let mut last_offsets: HashMap<PartitionKey, u64> = HashMap::new();
        let mut ops_executed: u64 = 0;

        let op_interval = match self.config.rate {
            RateModel::TargetOpsPerSec(target) => {
                Some(Duration::from_secs_f64(1.0 / target as f64))
            }
            _ => None,
        };
        let mut next_deadline = tokio::time::Instant::now();

        while !stop.load(Ordering::Relaxed) && !self.ops_limit_reached() {
            let kind = self.pick_op_kind();

            let op = {
                let state = self.shared_state.read().await;
                Op::generate(
                    kind,
                    &state,
                    &mut self.rng,
                    &self.config.prefixes,
                    self.config.namespace,
                    self.config.messages_per_batch,
                    self.config.scope,
                    self.config.consumer_id,
                )
            };

            let Some(op) = op else {
                continue;
            };

            let seq = self.seq_counter.fetch_add(1, Ordering::Relaxed);
            if let Some(tw) = &mut self.trace_writer {
                tw.write_intent(seq, &op);
            }

            let start = Instant::now();
            let exec_result = tokio::select! {
                result = op.execute(
                    &self.client,
                    self.config.msg_size,
                    &mut self.rng,
                    &self.config.prefixes,
                ) => result,
                _ = tokio::time::sleep(OP_EXECUTE_TIMEOUT) => {
                    warn!(
                        "Worker {}: op {} timed out after {OP_EXECUTE_TIMEOUT:?}",
                        self.id, op.kind_tag()
                    );
                    Err(LabError::Timeout {
                        context: format!("op {} execution", op.kind_tag()),
                    })
                }
            };
            let latency = start.elapsed();

            // Lock strategy: read-lock for error classification (no mutation),
            // write-lock only for successful shadow-mutating ops.
            let outcome = match exec_result {
                Ok(detail) => {
                    let outcome = OpOutcome::success_from(detail);
                    if op.mutates_shadow() {
                        let mut state = self.shared_state.write().await;
                        op.update_shadow(&mut state, &outcome);
                    }
                    outcome
                }
                Err(ref e) => {
                    let class = {
                        let state = self.shared_state.read().await;
                        match e {
                            LabError::Iggy(ie) => op.classify_error(ie, &state),
                            LabError::Timeout { .. } => ErrorClass::Transient,
                            LabError::PrefixViolation(_) | LabError::InvalidIdentifier(_) => {
                                ErrorClass::ServerBug
                            }
                            LabError::Io { .. } | LabError::TraceParse { .. } => {
                                ErrorClass::ServerBug
                            }
                        }
                    };
                    OpOutcome::error(class, e.to_string(), op.context())
                }
            };

            if let Some(tw) = &mut self.trace_writer {
                tw.write_outcome(seq, &outcome, latency);
            }

            debug!(
                worker = self.id,
                op = op.kind_tag(),
                result = outcome.result_tag(),
                latency_us = latency.as_micros() as u64,
            );

            // Invariant checks (after shadow update so purge records are visible)
            {
                let state = self.shared_state.read().await;
                if let Err(violation) =
                    invariants::check_offset_monotonicity(&mut last_offsets, &op, &outcome, &state)
                {
                    error!(worker = self.id, violation = ?violation, "Offset monotonicity violated");
                    if self.config.fail_fast {
                        return WorkerResult::ServerBug(violation);
                    }
                }
            }
            match outcome.error_class() {
                Some(ErrorClass::ServerBug) => {
                    let error = match &outcome {
                        OpOutcome::Error { error, .. } => error.as_str(),
                        _ => unreachable!(),
                    };
                    error!(worker = self.id, "Server bug: {error}");
                    if self.config.fail_fast {
                        return WorkerResult::ServerBug(InvariantViolation {
                            kind: "server_bug",
                            description: error.to_owned(),
                            context: format!("op={:?}", op),
                        });
                    }
                }
                Some(ErrorClass::Transient) => {
                    let error = match &outcome {
                        OpOutcome::Error { error, .. } => error.as_str(),
                        _ => unreachable!(),
                    };
                    warn!(worker = self.id, "Transient error: {error}");
                }
                Some(ErrorClass::ExpectedConcurrent) => {
                    debug!(
                        worker = self.id,
                        op = op.kind_tag(),
                        "Expected concurrent error"
                    );
                }
                None => {}
            }

            ops_executed += 1;
            self.ops_counter.fetch_add(1, Ordering::Relaxed);

            if let Some(interval) = op_interval {
                next_deadline += interval;
                tokio::time::sleep_until(next_deadline).await;
            }
        }

        if let Some(tw) = &mut self.trace_writer {
            tw.flush();
        }

        WorkerResult::Ok { ops_executed }
    }

    fn ops_limit_reached(&self) -> bool {
        self.config
            .ops_target
            .is_some_and(|target| self.ops_counter.load(Ordering::Relaxed) >= target)
    }

    fn pick_op_kind(&mut self) -> OpKind {
        let idx = self.weighted_dist.sample(&mut self.rng);
        self.op_kinds[idx]
    }
}

pub async fn run_worker(
    init: WorkerInit,
    op_weights: &[(OpKind, f64)],
    config: WorkerConfig,
    stop: Arc<AtomicBool>,
) -> WorkerResult {
    let id = init.id;
    match Worker::try_new(init, op_weights, config) {
        Some(w) => w.run(stop).await,
        None => {
            warn!("Worker {id} has no eligible ops after scope/destructive filtering, exiting");
            WorkerResult::Ok { ops_executed: 0 }
        }
    }
}
