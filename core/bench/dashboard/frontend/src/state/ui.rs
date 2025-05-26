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

use crate::components::selectors::measurement_type_selector::MeasurementType;
use std::rc::Rc;
use yew::prelude::*;

#[derive(Clone, Debug, PartialEq)]
#[allow(dead_code)]
pub enum ViewMode {
    SingleGitref,     // View detailed performance for a specific gitref
    GitrefTrend,      // View performance trends across all gitrefs
    RecentBenchmarks, // View recently added benchmarks
}

#[derive(Clone, Debug, PartialEq)]
pub struct UiState {
    pub view_mode: ViewMode,
    pub selected_measurement: MeasurementType,
    pub is_benchmark_tooltip_visible: bool,
    pub is_server_stats_tooltip_visible: bool,
}

impl Default for UiState {
    fn default() -> Self {
        Self {
            view_mode: ViewMode::SingleGitref,
            selected_measurement: MeasurementType::Latency,
            is_benchmark_tooltip_visible: false,
            is_server_stats_tooltip_visible: false,
        }
    }
}

pub enum UiAction {
    SetMeasurementType(MeasurementType),
    ToggleBenchmarkTooltip,
    ToggleServerStatsTooltip,
    SetViewMode(ViewMode),
}

impl Reducible for UiState {
    type Action = UiAction;

    fn reduce(self: Rc<Self>, action: UiAction) -> Rc<Self> {
        let next = match action {
            UiAction::SetMeasurementType(mt) => UiState {
                selected_measurement: mt,
                ..(*self).clone()
            },
            UiAction::ToggleBenchmarkTooltip => UiState {
                is_benchmark_tooltip_visible: !self.is_benchmark_tooltip_visible,
                ..(*self).clone()
            },
            UiAction::ToggleServerStatsTooltip => UiState {
                is_server_stats_tooltip_visible: !self.is_server_stats_tooltip_visible,
                ..(*self).clone()
            },
            UiAction::SetViewMode(vm) => UiState {
                view_mode: vm,
                ..(*self).clone()
            },
        };
        next.into()
    }
}

#[derive(Properties, PartialEq)]
pub struct UiProviderProps {
    #[prop_or_default]
    pub children: Children,
}

#[function_component(UiProvider)]
pub fn ui_provider(props: &UiProviderProps) -> Html {
    let state = use_reducer(UiState::default);

    html! {
        <ContextProvider<UseReducerHandle<UiState>> context={state}>
            { for props.children.iter() }
        </ContextProvider<UseReducerHandle<UiState>>>
    }
}

#[hook]
pub fn use_ui() -> UseReducerHandle<UiState> {
    use_context::<UseReducerHandle<UiState>>().expect("Ui context not found")
}
