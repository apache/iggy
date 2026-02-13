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

use crate::args::ReplayArgs;
use crate::client::{cleanup_all, create_client};
use crate::error::LabError;
use crate::ops::{ErrorClass, Op, OpOutcome};
use crate::scenarios::NamespacePrefixes;
use rand::SeedableRng;
use rand::rngs::StdRng;
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io::{BufRead, BufReader};
use tracing::{error, info, warn};

pub struct LabReplay {
    args: ReplayArgs,
}

pub struct ReplayOutcome {
    pub total_ops: u64,
    pub divergences_total: u64,
    pub divergences_concerning: u64,
}

/// Raw JSON line from a trace file — intent and outcome share the same shape
/// with optional fields.
#[derive(Deserialize)]
struct RawTraceEntry {
    seq: u64,
    phase: String,
    // Intent fields
    op: Option<Op>,
    // Outcome fields
    result: Option<String>,
    detail: Option<String>,
}

struct ReplayIntent {
    seq: u64,
    op: Op,
}

struct OriginalOutcome {
    result: String,
    detail: Option<String>,
}

struct Divergence {
    seq: u64,
    kind: DivergenceKind,
    op: Op,
    original_result: String,
    replay_result: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DivergenceKind {
    /// Original succeeded, replay failed — concerning
    OriginalOkReplayFailed,
    /// Original succeeded, replay returned expected error — concerning
    OriginalOkReplayExpected,
    /// Original got expected error, replay succeeded — benign (no concurrent race in sequential replay)
    OriginalExpectedReplayOk,
    /// Original got unexpected error, replay succeeded — interesting
    OriginalUnexpectedReplayOk,
    /// Both failed with different classifications
    BothFailed,
}

impl DivergenceKind {
    fn is_concerning(self) -> bool {
        matches!(
            self,
            Self::OriginalOkReplayFailed | Self::OriginalOkReplayExpected
        )
    }
}

impl fmt::Display for DivergenceKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OriginalOkReplayFailed => write!(f, "original_ok→replay_failed"),
            Self::OriginalOkReplayExpected => write!(f, "original_ok→replay_expected_error"),
            Self::OriginalExpectedReplayOk => write!(f, "original_expected→replay_ok"),
            Self::OriginalUnexpectedReplayOk => write!(f, "original_unexpected→replay_ok"),
            Self::BothFailed => write!(f, "both_failed"),
        }
    }
}

impl LabReplay {
    pub fn new(args: ReplayArgs) -> Self {
        Self { args }
    }

    pub async fn run(self) -> Result<ReplayOutcome, LabError> {
        let (intents, outcomes) = self.load_traces()?;
        if intents.is_empty() {
            return Err(LabError::TraceParse {
                path: self.args.trace_dir.display().to_string(),
                line: 0,
                message: "no intent entries found in trace directory".into(),
            });
        }

        info!("=== iggy-lab replay ===");
        info!("Trace dir: {:?}", self.args.trace_dir);
        info!("Intents loaded: {}", intents.len());
        info!(
            "Server: {} ({})",
            self.args.server_address, self.args.transport
        );

        let client = create_client(&self.args.server_address, self.args.transport).await?;
        let prefixes = NamespacePrefixes::from_base(&self.args.prefix);

        if self.args.force_cleanup {
            info!("Cleaning up prefixed resources...");
            cleanup_all(&client, &prefixes).await;
        }

        // Deterministic RNG for message payloads (content is irrelevant to correctness)
        let mut rng = StdRng::seed_from_u64(0);
        let mut divergences: Vec<Divergence> = Vec::new();
        let total_ops = intents.len() as u64;

        for intent in &intents {
            let replay_outcome = match intent
                .op
                .execute(&client, self.args.message_size, &mut rng, &prefixes)
                .await
            {
                Ok(detail) => OpOutcome::success_from(detail),
                Err(ref e) => {
                    let class = match e {
                        LabError::Iggy(ie) => ErrorClass::coarse(ie),
                        LabError::Timeout { .. } => ErrorClass::Transient,
                        _ => ErrorClass::ServerBug,
                    };
                    OpOutcome::error(class, e.to_string(), intent.op.context())
                }
            };
            let replay_tag = replay_outcome.result_tag();

            if let Some(original) = outcomes.get(&intent.seq) {
                if replay_tag == original.result {
                    continue;
                }

                let kind = classify_divergence(&original.result, replay_tag);
                let divergence = Divergence {
                    seq: intent.seq,
                    kind,
                    op: intent.op.clone(),
                    original_result: original.result.clone(),
                    replay_result: replay_tag.to_owned(),
                };

                if kind.is_concerning() {
                    warn!(
                        seq = intent.seq,
                        kind = %kind,
                        op = intent.op.kind_tag(),
                        original = %original.result,
                        replay = replay_tag,
                        detail = original.detail.as_deref().unwrap_or(""),
                        "Concerning divergence"
                    );
                } else {
                    info!(
                        seq = intent.seq,
                        kind = %kind,
                        op = intent.op.kind_tag(),
                        "Benign divergence"
                    );
                }

                let is_concerning = kind.is_concerning();
                divergences.push(divergence);

                if self.args.fail_fast && is_concerning {
                    error!(
                        "Fail-fast: stopping on concerning divergence at seq={}",
                        intent.seq
                    );
                    break;
                }
            }
        }

        let divergences_concerning = divergences
            .iter()
            .filter(|d| d.kind.is_concerning())
            .count() as u64;
        let divergences_total = divergences.len() as u64;

        info!("--- Replay Summary ---");
        info!("Total ops replayed: {total_ops}");
        info!("Total divergences: {divergences_total}");
        info!("Concerning divergences: {divergences_concerning}");

        if divergences_concerning > 0 {
            error!("REPLAY FAILED — {divergences_concerning} concerning divergence(s):");
            for d in divergences.iter().filter(|d| d.kind.is_concerning()) {
                error!(
                    "  seq={} op={} {} (original={}, replay={})",
                    d.seq,
                    d.op.kind_tag(),
                    d.kind,
                    d.original_result,
                    d.replay_result,
                );
            }
        } else if divergences_total > 0 {
            info!("REPLAY OK — {divergences_total} benign divergence(s)");
        } else {
            info!("REPLAY OK — exact match");
        }

        // Cleanup after replay
        if self.args.force_cleanup {
            info!("Post-replay cleanup...");
            cleanup_all(&client, &prefixes).await;
        }

        Ok(ReplayOutcome {
            total_ops,
            divergences_total,
            divergences_concerning,
        })
    }

    fn load_traces(&self) -> Result<(Vec<ReplayIntent>, HashMap<u64, OriginalOutcome>), LabError> {
        let trace_dir = &self.args.trace_dir;
        let entries = std::fs::read_dir(trace_dir).map_err(|e| LabError::Io {
            context: format!("reading trace dir {}", trace_dir.display()),
            source: e,
        })?;

        let mut intents: Vec<ReplayIntent> = Vec::new();
        let mut outcomes: HashMap<u64, OriginalOutcome> = HashMap::new();

        for entry in entries {
            let entry = entry.map_err(|e| LabError::Io {
                context: format!("iterating trace dir {}", trace_dir.display()),
                source: e,
            })?;
            let path = entry.path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if !name.starts_with("trace-worker-") || !name.ends_with(".jsonl") {
                continue;
            }

            let file = File::open(&path).map_err(|e| LabError::Io {
                context: format!("opening trace file {}", path.display()),
                source: e,
            })?;
            let reader = BufReader::new(file);

            for (line_num, line) in reader.lines().enumerate() {
                let line = line.map_err(|e| LabError::Io {
                    context: format!("reading line {} of {}", line_num + 1, path.display()),
                    source: e,
                })?;
                if line.is_empty() {
                    continue;
                }
                let raw: RawTraceEntry =
                    serde_json::from_str(&line).map_err(|e| LabError::TraceParse {
                        path: path.display().to_string(),
                        line: line_num + 1,
                        message: e.to_string(),
                    })?;

                match raw.phase.as_str() {
                    "intent" => {
                        if let Some(op) = raw.op {
                            intents.push(ReplayIntent { seq: raw.seq, op });
                        }
                    }
                    "outcome" => {
                        if let Some(result) = raw.result {
                            outcomes.insert(
                                raw.seq,
                                OriginalOutcome {
                                    result,
                                    detail: raw.detail,
                                },
                            );
                        }
                    }
                    _ => {}
                }
            }
        }

        intents.sort_by_key(|i| i.seq);
        Ok((intents, outcomes))
    }
}

fn is_error_tag(tag: &str) -> bool {
    matches!(tag, "expected_error" | "server_bug" | "transient")
}

fn classify_divergence(original_result: &str, replay_result: &str) -> DivergenceKind {
    match (original_result, replay_result) {
        ("ok", t) if is_error_tag(t) && t != "expected_error" => {
            DivergenceKind::OriginalOkReplayFailed
        }
        ("ok", "expected_error") => DivergenceKind::OriginalOkReplayExpected,
        ("expected_error", "ok") => DivergenceKind::OriginalExpectedReplayOk,
        (t, "ok") if is_error_tag(t) && t != "expected_error" => {
            DivergenceKind::OriginalUnexpectedReplayOk
        }
        _ => DivergenceKind::BothFailed,
    }
}
