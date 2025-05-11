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
}

/// Helper struct to compare benchmark parameters
#[derive(Debug)]
struct BenchmarkParams<'a> {
    message_size: BenchmarkNumericParameter,
    message_batches: u64,
    messages_per_batch: BenchmarkNumericParameter,
    transport: BenchmarkTransport,
    remark: &'a Option<String>,
}

impl BenchmarkState {
    /// Finds a benchmark that matches the parameters of the given benchmark
    pub fn find_matching_benchmark(
        &self,
        benchmark: &BenchmarkReportLight,
    ) -> Option<BenchmarkReportLight> {
        let params = self.extract_benchmark_params(benchmark);
        self.entries.values().find_map(|benchmarks| {
            benchmarks
                .iter()
                .find(|b| self.params_match(b, &params))
                .cloned()
        })
    }

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

    /// Creates a new BenchmarkState with the given entries and tries to find a matching benchmark
    fn create_with_entries(
        entries: BTreeMap<BenchmarkKind, Vec<BenchmarkReportLight>>,
        current_state: &BenchmarkState,
        hardware: String,
    ) -> BenchmarkState {
        let (selected_kind, selected_benchmark) =
            Self::determine_selection(&entries, current_state);

        Self::log_selection_result(&selected_kind, &selected_benchmark);

        BenchmarkState {
            entries,
            selected_benchmark,
            selected_kind,
            current_hardware: Some(hardware),
        }
    }

    /// Determine the selected benchmark and kind based on current state and entries
    fn determine_selection(
        entries: &BTreeMap<BenchmarkKind, Vec<BenchmarkReportLight>>,
        current_state: &BenchmarkState,
    ) -> (BenchmarkKind, Option<BenchmarkReportLight>) {
        let mut selected_kind = current_state.selected_kind;

        if let Some(current) = &current_state.selected_benchmark {
            Self::find_selection_with_current(entries, current, &mut selected_kind)
        } else {
            Self::find_selection_without_current(entries, &mut selected_kind)
        }
    }

    /// Find selection when there is a current benchmark
    fn find_selection_with_current(
        entries: &BTreeMap<BenchmarkKind, Vec<BenchmarkReportLight>>,
        current: &BenchmarkReportLight,
        selected_kind: &mut BenchmarkKind,
    ) -> (BenchmarkKind, Option<BenchmarkReportLight>) {
        let temp_state = BenchmarkState {
            entries: entries.clone(),
            selected_benchmark: None,
            selected_kind: *selected_kind,
            current_hardware: None,
        };

        if let Some(matched) = temp_state.find_matching_benchmark(current) {
            log!("Found exact matching benchmark");
            *selected_kind = matched.params.benchmark_kind;
            (*selected_kind, Some(matched))
        } else {
            Self::fallback_selection(entries, selected_kind)
        }
    }

    /// Find selection when there is no current benchmark
    fn find_selection_without_current(
        entries: &BTreeMap<BenchmarkKind, Vec<BenchmarkReportLight>>,
        selected_kind: &mut BenchmarkKind,
    ) -> (BenchmarkKind, Option<BenchmarkReportLight>) {
        let selected_benchmark = entries
            .get(selected_kind)
            .and_then(|benchmarks| benchmarks.first().cloned())
            .or_else(|| {
                entries
                    .values()
                    .next()
                    .and_then(|benchmarks| benchmarks.first().cloned())
            });

        if let Some(benchmark) = &selected_benchmark {
            *selected_kind = benchmark.params.benchmark_kind;
        }

        (*selected_kind, selected_benchmark)
    }

    /// Fallback selection when no exact match is found
    fn fallback_selection(
        entries: &BTreeMap<BenchmarkKind, Vec<BenchmarkReportLight>>,
        selected_kind: &mut BenchmarkKind,
    ) -> (BenchmarkKind, Option<BenchmarkReportLight>) {
        log!("No exact match found, trying to find benchmark in current kind");
        if let Some(benchmarks) = entries.get(selected_kind) {
            (*selected_kind, benchmarks.first().cloned())
        } else if let Some((&first_kind, benchmarks)) = entries.iter().next() {
            *selected_kind = first_kind;
            (first_kind, benchmarks.first().cloned())
        } else {
            (*selected_kind, None)
        }
    }

    /// Log the result of benchmark selection
    fn log_selection_result(
        selected_kind: &BenchmarkKind,
        selected_benchmark: &Option<BenchmarkReportLight>,
    ) {
        match selected_benchmark {
            Some(benchmark) => log!(format!(
                "Selected benchmark: kind={:?}, params={:?}",
                selected_kind, benchmark.params
            )),
            None => log!("No benchmark selected"),
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
            BenchmarkAction::SetBenchmarksForGitref(benchmarks, hardware) => {
                self.handle_gitref_benchmarks(benchmarks, hardware)
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
            "Benchmark selected: {:?}",
            benchmark.as_ref().map(|b| b.uuid)
        ));
        BenchmarkState {
            selected_benchmark: benchmark,
            ..(*self).clone()
        }
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
    ) -> BenchmarkState {
        let is_hardware_switch = self.current_hardware.as_ref() != Some(&hardware);
        if is_hardware_switch {
            log!("Hardware switch detected");
        }

        let entries = benchmarks.into_iter().fold(
            BTreeMap::new(),
            |mut acc: BTreeMap<BenchmarkKind, Vec<BenchmarkReportLight>>, benchmark| {
                acc.entry(benchmark.params.benchmark_kind)
                    .or_default()
                    .push(benchmark);
                acc
            },
        );

        Self::create_with_entries(entries, self, hardware)
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
                "Selected benchmark by params identifier: {:?} (kind={:?})",
                bm.params.params_identifier, new_kind
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
}

/// Actions that can be performed on the benchmark state
#[derive(Debug)]
pub enum BenchmarkAction {
    /// Select a specific benchmark
    SelectBenchmark(Box<Option<BenchmarkReportLight>>),
    /// Select a benchmark kind
    SelectBenchmarkKind(BenchmarkKind),
    /// Set benchmarks for a specific git reference
    SetBenchmarksForGitref(Vec<BenchmarkReportLight>, String),
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
