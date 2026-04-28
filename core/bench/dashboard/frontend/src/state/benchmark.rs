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

use crate::format::{finite_or, nan_safe_cmp};
use crate::version::{SemverRecency, parse_semver_recency};
use bench_dashboard_shared::BenchmarkReportLight;
use bench_report::benchmark_kind::BenchmarkKind;
use chrono::DateTime;
use gloo::console::log;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::rc::Rc;
use yew::prelude::*;

/// Represents the state of benchmarks in the application
#[derive(Clone, Debug, PartialEq, Default)]
pub struct BenchmarkState {
    /// Map of benchmark kinds to their corresponding benchmark reports
    pub entries: BTreeMap<BenchmarkKind, Vec<BenchmarkReportLight>>,
    /// Currently selected benchmark
    pub selected_benchmark: Option<BenchmarkReportLight>,
    /// Currently selected benchmark kind
    pub selected_kind: BenchmarkKind,
    /// Current hardware configuration identifier
    pub current_hardware: Option<String>,
    /// Current gitref for the loaded entries
    pub current_gitref: Option<String>,
}

impl BenchmarkState {
    /// Log the result of benchmark selection
    fn log_selection_result(
        selected_kind: &BenchmarkKind,
        selected_benchmark: &Option<BenchmarkReportLight>,
    ) {
        match selected_benchmark {
            Some(bm) => log!(format!(
                "Selected benchmark: kind={}, params={:?}",
                format!("{:?}", selected_kind), // Explicitly format kind
                bm.params
            )),
            None => log!(format!(
                "No benchmark selected, kind is {}",
                format!("{:?}", selected_kind)
            )),
        }
    }
}

impl Reducible for BenchmarkState {
    type Action = BenchmarkAction;

    fn reduce(self: Rc<Self>, action: Self::Action) -> Rc<Self> {
        let next_state = match action {
            BenchmarkAction::SelectBenchmark(benchmark) => {
                self.handle_benchmark_selection(*benchmark)
            }
            BenchmarkAction::SetBenchmarksForGitref(benchmarks, hardware, gitref) => {
                self.handle_gitref_benchmarks(benchmarks, hardware, gitref)
            }
        };

        Rc::new(next_state)
    }
}

impl BenchmarkState {
    /// Handle benchmark selection action
    fn handle_benchmark_selection(
        &self,
        benchmark: Option<BenchmarkReportLight>,
    ) -> BenchmarkState {
        log!(format!(
            "handle_benchmark_selection: Received benchmark: {:?}",
            benchmark
                .as_ref()
                .map(|b| b.params.params_identifier.clone())
        ));
        let mut new_state = self.clone();
        new_state.selected_benchmark = benchmark.clone();
        if let Some(bm) = benchmark {
            new_state.selected_kind = bm.params.benchmark_kind;
            let hardware_from_selection = bm.hardware.identifier.clone(); // Corrected line
            let gitref_from_selection = bm.params.gitref.clone();

            if new_state.current_hardware != hardware_from_selection {
                log!(format!(
                    "BenchmarkState: Updating current_hardware from {:?} to {:?} based on explicit selection.",
                    new_state.current_hardware, hardware_from_selection
                ));
                new_state.current_hardware = hardware_from_selection;
            }
            if new_state.current_gitref != gitref_from_selection {
                log!(format!(
                    "BenchmarkState: Updating current_gitref from {:?} to {:?} based on explicit selection.",
                    new_state.current_gitref, gitref_from_selection
                ));
                new_state.current_gitref = gitref_from_selection;
            }
            Self::log_selection_result(&new_state.selected_kind, &new_state.selected_benchmark);
        } else {
            // If benchmark is None, it means deselect or no selection possible.
            // We might want to clear current_hardware/current_gitref or leave them as is,
            // depending on desired behavior when no specific benchmark is chosen.
            // For now, let's leave them. If entries are later loaded for a (HW, GitRef) context,
            // those will override.
            log!("BenchmarkState: Benchmark explicitly deselected or set to None.");
            // Resetting kind to default if no benchmark is selected.
            new_state.selected_kind = BenchmarkKind::default();
            Self::log_selection_result(&new_state.selected_kind, &new_state.selected_benchmark);
        }
        new_state
    }

    /// Handle setting benchmarks for gitref action
    fn handle_gitref_benchmarks(
        &self,
        benchmarks: Vec<BenchmarkReportLight>,
        hardware: String,
        gitref_for_entries: String,
    ) -> BenchmarkState {
        log!(format!(
            "handle_gitref_benchmarks: Received {} benchmarks for HW: {}, GitRef: {}",
            benchmarks.len(),
            hardware,
            gitref_for_entries
        ));
        let mut entries: BTreeMap<BenchmarkKind, Vec<BenchmarkReportLight>> = BTreeMap::new();
        for benchmark in benchmarks {
            entries
                .entry(benchmark.params.benchmark_kind)
                .or_default()
                .push(benchmark);
        }

        let mut new_selected_benchmark: Option<BenchmarkReportLight> = None;
        let mut new_selected_kind: BenchmarkKind = self.selected_kind; // Default to old, will be updated

        let hardware_context_changed = Some(hardware.clone()) != self.current_hardware;
        let gitref_context_changed = Some(gitref_for_entries.clone()) != self.current_gitref;

        if hardware_context_changed || gitref_context_changed {
            if let Some(current) = &self.selected_benchmark
                && let Some(retained) = entries
                    .values()
                    .flatten()
                    .find(|candidate| candidate.uuid == current.uuid)
            {
                log!(format!(
                    "BenchmarkState: Context changed but retaining current selection {} by UUID.",
                    current.uuid
                ));
                new_selected_benchmark = Some(retained.clone());
                new_selected_kind = retained.params.benchmark_kind;
            } else {
                log!(format!(
                    "BenchmarkState: Context changed. HW: {:?}->{}. GitRef: {:?}->{}. Picking best.",
                    self.current_hardware, hardware, self.current_gitref, gitref_for_entries
                ));
                if let Some(best) = self.find_best_benchmark(&entries) {
                    new_selected_benchmark = Some(best.clone());
                    new_selected_kind = best.params.benchmark_kind;
                } else {
                    new_selected_kind = BenchmarkKind::default();
                }
            }
        } else {
            // Context (HW and GitRef) has NOT changed. Try to retain selection.
            log!("BenchmarkState: Context same. Trying to retain selection.");
            if let Some(current_sel_bm) = &self.selected_benchmark {
                if let Some(matched_in_new) = entries.values().flatten().find(|new_bm| {
                    new_bm.params.params_identifier == current_sel_bm.params.params_identifier
                }) {
                    new_selected_benchmark = Some(matched_in_new.clone());
                    new_selected_kind = matched_in_new.params.benchmark_kind;
                    log!(format!(
                        "BenchmarkState: Retained selected benchmark {:?}.",
                        current_sel_bm.params.pretty_name
                    ));
                } else {
                    log!(
                        "BenchmarkState: Old selection not found in new entries. Picking first available."
                    );
                }
            }
            // If no current selection OR old selection not found, pick first available from current entries
            if new_selected_benchmark.is_none() {
                log!(
                    "BenchmarkState: Attempting to pick first available as fallback for same context."
                );
                if let Some((kind, reports)) = entries.iter().next() {
                    if let Some(report) = reports.first() {
                        new_selected_benchmark = Some(report.clone());
                        new_selected_kind = *kind;
                    } else {
                        new_selected_kind = self.selected_kind; // Or *kind if we want to keep it to the (now empty) first kind
                    }
                } else {
                    new_selected_kind = self.selected_kind; // No entries, retain old kind
                }
            }
        }

        // Ensure kind is consistent if a benchmark is selected
        if let Some(bm) = &new_selected_benchmark {
            new_selected_kind = bm.params.benchmark_kind;
        } else {
            // If no benchmark selected (e.g., entries are empty for the context)
            // `new_selected_kind` would have been set by logic above (e.g. default or first kind from empty list)
            log!(format!(
                "BenchmarkState: No benchmark selected after processing. Final kind: {}",
                format!("{:?}", new_selected_kind)
            ));
        }

        Self::log_selection_result(&new_selected_kind, &new_selected_benchmark);

        BenchmarkState {
            entries,
            selected_benchmark: new_selected_benchmark,
            selected_kind: new_selected_kind,
            current_hardware: Some(hardware),
            current_gitref: Some(gitref_for_entries),
        }
    }

    /// Pick a sensible default benchmark from the freshest run batch.
    ///
    /// Scopes to `self.selected_kind` when that kind has entries (keeps the default
    /// consistent with the active sidebar tab); otherwise falls back to all entries.
    fn find_best_benchmark(
        &self,
        entries: &BTreeMap<BenchmarkKind, Vec<BenchmarkReportLight>>,
    ) -> Option<BenchmarkReportLight> {
        let in_kind: Vec<&BenchmarkReportLight> = entries
            .get(&self.selected_kind)
            .into_iter()
            .flatten()
            .collect();
        let scoped: Vec<&BenchmarkReportLight> = if in_kind.is_empty() {
            entries.values().flatten().collect()
        } else {
            in_kind
        };
        pick_best_from_recent_batch(&scoped)
    }
}

/// Pick the best benchmark from the most recent sweep.
///
/// "Sweep" = runs sharing the newest (hardware, gitref). Ranked by lowest P99,
/// then P99.9, then P99.99, then highest total throughput.
/// NaN values rank worst so corrupt runs never win.
pub fn pick_best_from_recent_batch(
    benchmarks: &[&BenchmarkReportLight],
) -> Option<BenchmarkReportLight> {
    let mut sweep = latest_sweep(benchmarks);
    sweep.sort_by_key(rank_sweep_candidate);
    sweep.first().map(|report| (*report).clone())
}

pub fn latest_sweep<'a>(benchmarks: &[&'a BenchmarkReportLight]) -> Vec<&'a BenchmarkReportLight> {
    let Some(newest) = benchmarks
        .iter()
        .copied()
        .max_by(|left, right| recency_cmp(left, right))
    else {
        return Vec::new();
    };
    let sweep_key = sweep_key_of(newest);
    benchmarks
        .iter()
        .copied()
        .filter(|benchmark| sweep_key_of(benchmark) == sweep_key)
        .collect()
}

/// Order benchmarks by "most recent" using semver-aware recency.
///
/// Benchmarks whose gitref parses as a semver-like tag (e.g. `0.7.0`,
/// `0.7.0-edge.1`) are newer than any plain-commit run. Within each
/// group we tie-break on RFC3339 timestamp so rebuilds of the same
/// gitref still order by wall clock.
pub fn recency_cmp(left: &BenchmarkReportLight, right: &BenchmarkReportLight) -> Ordering {
    let left_semver = left.params.gitref.as_deref().and_then(parse_semver_recency);
    let right_semver = right
        .params
        .gitref
        .as_deref()
        .and_then(parse_semver_recency);
    compare_semver_group(left_semver.as_ref(), right_semver.as_ref())
        .then_with(|| timestamp_cmp(&left.timestamp, &right.timestamp))
}

fn compare_semver_group(left: Option<&SemverRecency>, right: Option<&SemverRecency>) -> Ordering {
    match (left, right) {
        (Some(l), Some(r)) => l.cmp(r),
        (Some(_), None) => Ordering::Greater,
        (None, Some(_)) => Ordering::Less,
        (None, None) => Ordering::Equal,
    }
}

fn timestamp_cmp(left: &str, right: &str) -> Ordering {
    match (
        DateTime::parse_from_rfc3339(left),
        DateTime::parse_from_rfc3339(right),
    ) {
        (Ok(l), Ok(r)) => l.cmp(&r),
        _ => Ordering::Equal,
    }
}

fn sweep_key_of(benchmark: &BenchmarkReportLight) -> (Option<String>, Option<String>) {
    (
        benchmark.hardware.identifier.clone(),
        benchmark.params.gitref.clone(),
    )
}

fn rank_sweep_candidate(
    benchmark: &&BenchmarkReportLight,
) -> (FloatKey, FloatKey, FloatKey, FloatKey) {
    let summary = benchmark.group_metrics.first().map(|group| &group.summary);
    let p99 = summary
        .map(|summary| finite_or(summary.average_p99_latency_ms, f64::INFINITY))
        .unwrap_or(f64::INFINITY);
    let p999 = summary
        .map(|summary| finite_or(summary.average_p999_latency_ms, f64::INFINITY))
        .unwrap_or(f64::INFINITY);
    let p9999 = summary
        .map(|summary| finite_or(summary.average_p9999_latency_ms, f64::INFINITY))
        .unwrap_or(f64::INFINITY);
    let throughput = summary
        .map(|summary| finite_or(summary.total_throughput_megabytes_per_second, 0.0))
        .unwrap_or(0.0);
    (
        FloatKey(p99),
        FloatKey(p999),
        FloatKey(p9999),
        FloatKey(-throughput),
    )
}

#[derive(Clone, Copy, PartialEq)]
struct FloatKey(f64);

impl Eq for FloatKey {}

impl Ord for FloatKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        nan_safe_cmp(self.0, other.0)
    }
}

impl PartialOrd for FloatKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug)]
pub enum BenchmarkAction {
    SelectBenchmark(Box<Option<BenchmarkReportLight>>),
    SetBenchmarksForGitref(Vec<BenchmarkReportLight>, String, String),
}

/// Context for managing benchmark state
#[derive(Clone, PartialEq)]
pub struct BenchmarkContext {
    pub state: BenchmarkState,
    pub dispatch: Callback<BenchmarkAction>,
}

impl BenchmarkContext {
    pub fn new(state: BenchmarkState, dispatch: Callback<BenchmarkAction>) -> Self {
        Self { state, dispatch }
    }
}

#[derive(Properties, PartialEq)]
pub struct BenchmarkProviderProps {
    #[prop_or_default]
    pub children: Children,
}

#[function_component(BenchmarkProvider)]
pub fn benchmark_provider(props: &BenchmarkProviderProps) -> Html {
    let state = use_reducer(BenchmarkState::default);

    let context = BenchmarkContext::new(
        (*state).clone(),
        Callback::from(move |action| state.dispatch(action)),
    );

    html! {
        <ContextProvider<BenchmarkContext> context={context}>
            { for props.children.iter() }
        </ContextProvider<BenchmarkContext>>
    }
}

#[hook]
pub fn use_benchmark() -> BenchmarkContext {
    use_context::<BenchmarkContext>().expect("Benchmark context not found")
}

#[cfg(test)]
mod tests {
    use super::*;
    use bench_dashboard_shared::BenchmarkGroupMetricsLight;
    use bench_report::group_metrics_kind::GroupMetricsKind;
    use bench_report::group_metrics_summary::BenchmarkGroupMetricsSummary;

    #[test]
    fn given_empty_slice_when_picking_best_should_return_none() {
        let input: Vec<&BenchmarkReportLight> = Vec::new();
        assert!(pick_best_from_recent_batch(&input).is_none());
    }

    #[test]
    fn given_sweep_runs_when_picking_should_return_lowest_p99() {
        let a = run(
            "2026-04-21T10:00:00Z",
            "hw-a",
            "0.7.0",
            [5.0, 10.0, 20.0],
            100.0,
        );
        let b = run(
            "2026-04-21T10:30:00Z",
            "hw-a",
            "0.7.0",
            [2.0, 8.0, 18.0],
            80.0,
        );
        let c = run(
            "2026-04-21T11:00:00Z",
            "hw-a",
            "0.7.0",
            [3.5, 9.0, 19.0],
            120.0,
        );
        let picked = pick_best_from_recent_batch(&[&a, &b, &c]).expect("expected a pick");
        assert_eq!(picked.timestamp, b.timestamp);
    }

    #[test]
    fn given_tie_on_p99_when_picking_should_break_on_p999() {
        let worse_tail = run(
            "2026-04-21T10:00:00Z",
            "hw-a",
            "0.7.0",
            [2.0, 20.0, 30.0],
            500.0,
        );
        let better_tail = run(
            "2026-04-21T10:10:00Z",
            "hw-a",
            "0.7.0",
            [2.0, 12.0, 30.0],
            90.0,
        );
        let picked = pick_best_from_recent_batch(&[&worse_tail, &better_tail]).expect("pick");
        assert_eq!(picked.timestamp, better_tail.timestamp);
    }

    #[test]
    fn given_tie_on_p99_and_p999_when_picking_should_break_on_p9999() {
        let worse = run(
            "2026-04-21T10:00:00Z",
            "hw-a",
            "0.7.0",
            [2.0, 10.0, 30.0],
            500.0,
        );
        let better = run(
            "2026-04-21T10:10:00Z",
            "hw-a",
            "0.7.0",
            [2.0, 10.0, 20.0],
            90.0,
        );
        let picked = pick_best_from_recent_batch(&[&worse, &better]).expect("pick");
        assert_eq!(picked.timestamp, better.timestamp);
    }

    #[test]
    fn given_tie_on_all_latencies_when_picking_should_break_on_throughput() {
        let low_tput = run(
            "2026-04-21T10:00:00Z",
            "hw-a",
            "0.7.0",
            [2.0, 10.0, 20.0],
            100.0,
        );
        let high_tput = run(
            "2026-04-21T10:10:00Z",
            "hw-a",
            "0.7.0",
            [2.0, 10.0, 20.0],
            200.0,
        );
        let picked = pick_best_from_recent_batch(&[&low_tput, &high_tput]).expect("pick");
        assert_eq!(picked.timestamp, high_tput.timestamp);
    }

    #[test]
    fn given_runs_from_older_release_when_picking_should_restrict_to_latest_sweep() {
        let old_release = run(
            "2025-01-15T10:00:00Z",
            "hw-a",
            "0.5.0",
            [1.0, 5.0, 10.0],
            500.0,
        );
        let current_slow = run(
            "2026-04-21T12:00:00Z",
            "hw-a",
            "0.7.0",
            [10.0, 15.0, 25.0],
            100.0,
        );
        let current_fast = run(
            "2026-04-21T12:30:00Z",
            "hw-a",
            "0.7.0",
            [5.0, 12.0, 22.0],
            90.0,
        );
        let picked = pick_best_from_recent_batch(&[&old_release, &current_slow, &current_fast])
            .expect("pick");
        assert_eq!(picked.timestamp, current_fast.timestamp);
    }

    #[test]
    fn given_runs_on_different_hardware_when_picking_should_restrict_to_latest_sweep() {
        let other_hw = run(
            "2026-04-21T12:40:00Z",
            "hw-b",
            "0.7.0",
            [1.0, 5.0, 10.0],
            500.0,
        );
        let current = run(
            "2026-04-21T12:30:00Z",
            "hw-a",
            "0.7.0",
            [5.0, 12.0, 22.0],
            90.0,
        );
        let picked = pick_best_from_recent_batch(&[&other_hw, &current]).expect("pick");
        assert_eq!(picked.timestamp, other_hw.timestamp);
    }

    #[test]
    fn given_latest_sweep_helper_when_called_should_filter_to_newest_hardware_gitref_group() {
        let old = run(
            "2026-04-20T10:00:00Z",
            "hw-a",
            "0.6.0",
            [1.0, 5.0, 10.0],
            100.0,
        );
        let latest_a = run(
            "2026-04-21T12:00:00Z",
            "hw-a",
            "0.7.0",
            [5.0, 10.0, 20.0],
            100.0,
        );
        let latest_b = run(
            "2026-04-21T12:30:00Z",
            "hw-a",
            "0.7.0",
            [3.0, 8.0, 18.0],
            120.0,
        );
        let sweep = latest_sweep(&[&old, &latest_a, &latest_b]);
        assert_eq!(sweep.len(), 2);
        assert!(
            sweep
                .iter()
                .any(|report| report.timestamp == latest_a.timestamp)
        );
        assert!(
            sweep
                .iter()
                .any(|report| report.timestamp == latest_b.timestamp)
        );
    }

    #[test]
    fn given_nan_p99_when_picking_should_prefer_finite_values() {
        let mut nan_run = run(
            "2026-04-21T10:00:00Z",
            "hw-a",
            "0.7.0",
            [0.0, 0.0, 0.0],
            100.0,
        );
        nan_run.group_metrics[0].summary.average_p99_latency_ms = f64::NAN;
        let good = run(
            "2026-04-21T10:10:00Z",
            "hw-a",
            "0.7.0",
            [3.0, 6.0, 12.0],
            80.0,
        );
        let picked = pick_best_from_recent_batch(&[&nan_run, &good]).expect("pick");
        assert_eq!(picked.timestamp, good.timestamp);
    }

    #[test]
    fn given_shared_recent_feed_when_picking_should_match_sidebar_top_sweep() {
        let recent_feed_from_api = [
            run(
                "2026-04-21T13:30:00Z",
                "hw-a",
                "0.7.0",
                [5.0, 12.0, 22.0],
                140.0,
            ),
            run(
                "2026-04-21T13:20:00Z",
                "hw-a",
                "0.7.0",
                [2.0, 9.0, 19.0],
                90.0,
            ),
            run(
                "2026-04-21T13:10:00Z",
                "hw-a",
                "0.7.0",
                [3.0, 10.0, 21.0],
                120.0,
            ),
            run(
                "2024-02-01T10:00:00Z",
                "hw-legacy",
                "0.3.0",
                [1.0, 4.0, 8.0],
                999.0,
            ),
        ];
        let refs: Vec<&BenchmarkReportLight> = recent_feed_from_api.iter().collect();

        let sidebar_top_sweep_key = (
            recent_feed_from_api[0].hardware.identifier.clone(),
            recent_feed_from_api[0].params.gitref.clone(),
        );
        let sweep = latest_sweep(&refs);
        for report in &sweep {
            assert_eq!(
                (
                    report.hardware.identifier.clone(),
                    report.params.gitref.clone()
                ),
                sidebar_top_sweep_key,
                "hero sweep leaked outside sidebar's newest (hardware, gitref)"
            );
        }

        let picked = pick_best_from_recent_batch(&refs).expect("pick");
        assert_eq!(picked.hardware.identifier, sidebar_top_sweep_key.0);
        assert_eq!(picked.params.gitref, sidebar_top_sweep_key.1);
        assert_eq!(picked.timestamp, recent_feed_from_api[1].timestamp);
    }

    fn run(
        timestamp: &str,
        hardware: &str,
        gitref: &str,
        latencies: [f64; 3],
        throughput_mb_s: f64,
    ) -> BenchmarkReportLight {
        let [p99, p999, p9999] = latencies;
        let mut report = BenchmarkReportLight {
            timestamp: timestamp.to_string(),
            ..Default::default()
        };
        report.hardware.identifier = Some(hardware.to_string());
        report.params.gitref = Some(gitref.to_string());
        report.group_metrics.push(BenchmarkGroupMetricsLight {
            summary: summary_with(throughput_mb_s, p99, p999, p9999),
            latency_distribution: None,
        });
        report
    }

    fn summary_with(
        throughput_mb_s: f64,
        p99: f64,
        p999: f64,
        p9999: f64,
    ) -> BenchmarkGroupMetricsSummary {
        BenchmarkGroupMetricsSummary {
            kind: GroupMetricsKind::Producers,
            total_throughput_megabytes_per_second: throughput_mb_s,
            total_throughput_messages_per_second: 0.0,
            average_throughput_megabytes_per_second: 0.0,
            average_throughput_messages_per_second: 0.0,
            average_p50_latency_ms: 0.0,
            average_p90_latency_ms: 0.0,
            average_p95_latency_ms: 0.0,
            average_p99_latency_ms: p99,
            average_p999_latency_ms: p999,
            average_p9999_latency_ms: p9999,
            average_latency_ms: 0.0,
            average_median_latency_ms: 0.0,
            min_latency_ms: 0.0,
            max_latency_ms: 0.0,
            std_dev_latency_ms: 0.0,
        }
    }
}
