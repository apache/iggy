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

use bench_dashboard_shared::BenchmarkReportLight;
use bench_report::{
    benchmark_kind::BenchmarkKind, numeric_parameter::BenchmarkNumericParameter,
    transport::BenchmarkTransport,
};
use gloo::console::log;
use std::{collections::BTreeMap, rc::Rc};
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

/// Helper struct to compare benchmark parameters
#[derive(Debug)]
struct BenchmarkParams<'a> {
    message_size: BenchmarkNumericParameter,
    message_batches: usize,
    messages_per_batch: BenchmarkNumericParameter,
    transport: BenchmarkTransport,
    remark: &'a Option<String>,
}

impl BenchmarkState {
    /// Extract benchmark parameters for comparison
    fn extract_benchmark_params<'a>(
        &self,
        benchmark: &'a BenchmarkReportLight,
    ) -> BenchmarkParams<'a> {
        BenchmarkParams {
            message_size: benchmark.params.message_size,
            message_batches: benchmark.params.message_batches,
            messages_per_batch: benchmark.params.messages_per_batch,
            transport: benchmark.params.transport,
            remark: &benchmark.params.remark,
        }
    }

    /// Compare benchmark parameters
    fn params_match(&self, benchmark: &BenchmarkReportLight, params: &BenchmarkParams) -> bool {
        benchmark.params.message_size == params.message_size
            && benchmark.params.message_batches == params.message_batches
            && benchmark.params.messages_per_batch == params.messages_per_batch
            && benchmark.params.transport == params.transport
            && benchmark.params.remark == *params.remark
    }

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
            BenchmarkAction::SelectBenchmarkKind(kind) => self.handle_kind_selection(kind),
            BenchmarkAction::SetBenchmarksForGitref(benchmarks, hardware, gitref) => {
                self.handle_gitref_benchmarks(benchmarks, hardware, gitref)
            }
            BenchmarkAction::SelectBenchmarkByParamsIdentifier(params_identifier) => {
                self.handle_benchmark_selection_by_params_identifier(&params_identifier)
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

    /// Handle benchmark kind selection action
    fn handle_kind_selection(&self, kind: BenchmarkKind) -> BenchmarkState {
        log!(format!("Kind changed: {:?}", kind));

        let mut next_state = BenchmarkState {
            selected_kind: kind,
            selected_benchmark: None,
            ..(*self).clone()
        };

        if let Some(benchmarks) = self.entries.get(&kind) {
            next_state.selected_benchmark = if let Some(current) = &self.selected_benchmark {
                let params = self.extract_benchmark_params(current);
                benchmarks
                    .iter()
                    .find(|b| self.params_match(b, &params))
                    .or_else(|| {
                        log!("No matching benchmark found with the same parameters, selecting first available entry");
                        benchmarks.first()
                    })
                    .cloned()
            } else {
                log!("No previous selection, selecting first available benchmark");
                benchmarks.first().cloned()
            };
        }

        next_state
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
            log!(format!(
                "BenchmarkState: Context changed. HW: {:?}->{}. GitRef: {:?}->{}. Picking first available benchmark from new entries.",
                self.current_hardware, hardware, self.current_gitref, gitref_for_entries
            ));

            let best = Self::find_best_benchmark(&entries);
            if let Some(best) = best {
                new_selected_benchmark = Some(best.clone());
                new_selected_kind = best.params.benchmark_kind;
            } else {
                // No entries at all, reset kind to default
                new_selected_kind = BenchmarkKind::default();
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

    /// Select a benchmark by its params identifier
    fn handle_benchmark_selection_by_params_identifier(
        &self,
        params_identifier: &str,
    ) -> BenchmarkState {
        let mut found_benchmark: Option<BenchmarkReportLight> = None;
        let mut found_kind: Option<BenchmarkKind> = None;

        for (kind, reports) in &self.entries {
            if let Some(report) = reports
                .iter()
                .find(|r| r.params.params_identifier == params_identifier)
            {
                found_benchmark = Some(report.clone());
                found_kind = Some(*kind);
                break;
            }
        }

        if let Some(bm) = found_benchmark {
            let new_kind = found_kind.unwrap();
            log!(format!(
                "Selected benchmark by params identifier: {:?} (kind={})",
                bm.params.params_identifier,
                format!("{:?}", new_kind)
            ));
            BenchmarkState {
                selected_benchmark: Some(bm),
                selected_kind: new_kind,
                ..(*self).clone()
            }
        } else {
            log!(format!(
                "No matching benchmark with params identifier {} found in state entries",
                params_identifier
            ));
            self.clone()
        }
    }

    fn find_best_benchmark(
        benchmarks: &BTreeMap<BenchmarkKind, Vec<BenchmarkReportLight>>,
    ) -> Option<BenchmarkReportLight> {
        benchmarks
            .values()
            .flatten()
            .max_by_key(|bm| {
                bm.group_metrics
                    .first()
                    .unwrap()
                    .summary
                    .average_p99_latency_ms as u64
            })
            .cloned()
    }
}

/// Actions that can be performed on the benchmark state
#[derive(Debug)]
pub enum BenchmarkAction {
    /// Select a specific benchmark
    SelectBenchmark(Box<Option<BenchmarkReportLight>>),
    /// Select a benchmark kind
    SelectBenchmarkKind(BenchmarkKind),
    /// Set benchmarks for a specific git reference
    SetBenchmarksForGitref(Vec<BenchmarkReportLight>, String, String),
    /// Select a benchmark by its params identifier
    SelectBenchmarkByParamsIdentifier(String),
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
