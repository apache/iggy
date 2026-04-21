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
use bench_dashboard_shared::BenchmarkReportLight;
use bench_report::benchmark_kind::BenchmarkKind;
use std::collections::HashSet;
use std::rc::Rc;
use yew::prelude::*;

#[derive(Clone, Debug, PartialEq)]
#[allow(dead_code)]
pub enum ViewMode {
    SingleGitref,
    GitrefTrend,
    RecentBenchmarks,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ParamRange {
    pub from: Option<u32>,
    pub to: Option<u32>,
}

impl ParamRange {
    pub fn is_active(&self) -> bool {
        self.from.is_some() || self.to.is_some()
    }

    pub fn matches(&self, value: u32) -> bool {
        self.from.is_none_or(|f| value >= f) && self.to.is_none_or(|t| value <= t)
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct MetricRange {
    pub from: Option<f64>,
    pub to: Option<f64>,
}

impl MetricRange {
    pub fn is_active(&self) -> bool {
        self.from.is_some() || self.to.is_some()
    }

    pub fn matches(&self, value: f64) -> bool {
        self.from.is_none_or(|f| value >= f) && self.to.is_none_or(|t| value <= t)
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ParamFilters {
    pub producers: ParamRange,
    pub consumers: ParamRange,
    pub streams: ParamRange,
    pub partitions: ParamRange,
    pub consumer_groups: ParamRange,
    pub throughput_mb_s: MetricRange,
    pub p99_latency_ms: MetricRange,
}

impl ParamFilters {
    pub fn active_count(&self) -> usize {
        let u32_active = [
            &self.producers,
            &self.consumers,
            &self.streams,
            &self.partitions,
            &self.consumer_groups,
        ]
        .iter()
        .filter(|r| r.is_active())
        .count();
        let metric_active = [&self.throughput_mb_s, &self.p99_latency_ms]
            .iter()
            .filter(|r| r.is_active())
            .count();
        u32_active + metric_active
    }

    pub fn matches(&self, benchmark: &BenchmarkReportLight) -> bool {
        let p = &benchmark.params;
        let params_ok = self.producers.matches(p.producers)
            && self.consumers.matches(p.consumers)
            && self.streams.matches(p.streams)
            && self.partitions.matches(p.partitions)
            && self.consumer_groups.matches(p.consumer_groups);
        if !params_ok {
            return false;
        }
        let metrics_active = self.throughput_mb_s.is_active() || self.p99_latency_ms.is_active();
        let Some(summary) = benchmark.group_metrics.first().map(|m| &m.summary) else {
            return !metrics_active;
        };
        self.throughput_mb_s
            .matches(summary.total_throughput_megabytes_per_second)
            && self.p99_latency_ms.matches(summary.average_p99_latency_ms)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParamField {
    Producers,
    Consumers,
    Streams,
    Partitions,
    ConsumerGroups,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetricField {
    ThroughputMbS,
    P99LatencyMs,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SidebarSort {
    #[default]
    MostRecent,
    PeakThroughput,
    LowestP99,
    Name,
}

impl SidebarSort {
    pub fn label(self) -> &'static str {
        match self {
            Self::MostRecent => "Most recent",
            Self::PeakThroughput => "Peak throughput",
            Self::LowestP99 => "Lowest P99",
            Self::Name => "Name",
        }
    }

    pub fn all() -> [Self; 4] {
        [
            Self::MostRecent,
            Self::PeakThroughput,
            Self::LowestP99,
            Self::Name,
        ]
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum KindGroup {
    Pinned,
    Balanced,
    EndToEnd,
}

impl KindGroup {
    pub fn label(self) -> &'static str {
        match self {
            Self::Pinned => "Pinned",
            Self::Balanced => "Balanced",
            Self::EndToEnd => "End-to-end",
        }
    }

    pub fn matches(self, kind: BenchmarkKind) -> bool {
        match self {
            Self::Pinned => matches!(
                kind,
                BenchmarkKind::PinnedProducer
                    | BenchmarkKind::PinnedConsumer
                    | BenchmarkKind::PinnedProducerAndConsumer
            ),
            Self::Balanced => matches!(
                kind,
                BenchmarkKind::BalancedProducer
                    | BenchmarkKind::BalancedConsumerGroup
                    | BenchmarkKind::BalancedProducerAndConsumerGroup
            ),
            Self::EndToEnd => matches!(
                kind,
                BenchmarkKind::EndToEndProducingConsumer
                    | BenchmarkKind::EndToEndProducingConsumerGroup
            ),
        }
    }

    pub fn all() -> [Self; 3] {
        [Self::Pinned, Self::Balanced, Self::EndToEnd]
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct UiState {
    pub view_mode: ViewMode,
    pub selected_measurement: MeasurementType,
    pub is_benchmark_tooltip_visible: bool,
    pub is_server_stats_tooltip_visible: bool,
    pub is_embed_modal_visible: bool,
    pub param_filters: ParamFilters,
    pub is_sidebar_collapsed: bool,
    pub compare_pin: Option<BenchmarkReportLight>,
    pub sidebar_search: String,
    pub sidebar_sort: SidebarSort,
    pub sidebar_kind_filter: HashSet<KindGroup>,
}

impl Default for UiState {
    fn default() -> Self {
        Self {
            view_mode: ViewMode::SingleGitref,
            selected_measurement: MeasurementType::Latency,
            is_benchmark_tooltip_visible: false,
            is_server_stats_tooltip_visible: false,
            is_embed_modal_visible: false,
            param_filters: ParamFilters::default(),
            is_sidebar_collapsed: false,
            compare_pin: None,
            sidebar_search: String::new(),
            sidebar_sort: SidebarSort::default(),
            sidebar_kind_filter: HashSet::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TopBarPopup {
    BenchmarkInfo,
    ServerStats,
    Embed,
}

pub enum UiAction {
    SetMeasurementType(MeasurementType),
    TogglePopup(TopBarPopup),
    SetViewMode(ViewMode),
    SetParamRange(ParamField, ParamRange),
    SetMetricRange(MetricField, MetricRange),
    ClearParamFilters,
    ToggleSidebar,
    SetComparePin(Box<Option<BenchmarkReportLight>>),
    CloseAllPopups,
    SetSidebarSearch(String),
    SetSidebarSort(SidebarSort),
    ToggleKindFilter(KindGroup),
}

impl Reducible for UiState {
    type Action = UiAction;

    fn reduce(self: Rc<Self>, action: UiAction) -> Rc<Self> {
        let next = match action {
            UiAction::SetMeasurementType(mt) => UiState {
                selected_measurement: mt,
                ..(*self).clone()
            },
            UiAction::TogglePopup(popup) => {
                let already_open = match popup {
                    TopBarPopup::BenchmarkInfo => self.is_benchmark_tooltip_visible,
                    TopBarPopup::ServerStats => self.is_server_stats_tooltip_visible,
                    TopBarPopup::Embed => self.is_embed_modal_visible,
                };
                let (info, stats, embed) = if already_open {
                    (false, false, false)
                } else {
                    (
                        matches!(popup, TopBarPopup::BenchmarkInfo),
                        matches!(popup, TopBarPopup::ServerStats),
                        matches!(popup, TopBarPopup::Embed),
                    )
                };
                UiState {
                    is_benchmark_tooltip_visible: info,
                    is_server_stats_tooltip_visible: stats,
                    is_embed_modal_visible: embed,
                    ..(*self).clone()
                }
            }
            UiAction::SetViewMode(vm) => UiState {
                view_mode: vm,
                ..(*self).clone()
            },
            UiAction::SetParamRange(field, range) => {
                let mut filters = self.param_filters.clone();
                match field {
                    ParamField::Producers => filters.producers = range,
                    ParamField::Consumers => filters.consumers = range,
                    ParamField::Streams => filters.streams = range,
                    ParamField::Partitions => filters.partitions = range,
                    ParamField::ConsumerGroups => filters.consumer_groups = range,
                }
                UiState {
                    param_filters: filters,
                    ..(*self).clone()
                }
            }
            UiAction::SetMetricRange(field, range) => {
                let mut filters = self.param_filters.clone();
                match field {
                    MetricField::ThroughputMbS => filters.throughput_mb_s = range,
                    MetricField::P99LatencyMs => filters.p99_latency_ms = range,
                }
                UiState {
                    param_filters: filters,
                    ..(*self).clone()
                }
            }
            UiAction::ClearParamFilters => UiState {
                param_filters: ParamFilters::default(),
                ..(*self).clone()
            },
            UiAction::ToggleSidebar => UiState {
                is_sidebar_collapsed: !self.is_sidebar_collapsed,
                ..(*self).clone()
            },
            UiAction::SetComparePin(pin) => UiState {
                compare_pin: *pin,
                ..(*self).clone()
            },
            UiAction::CloseAllPopups => UiState {
                is_benchmark_tooltip_visible: false,
                is_server_stats_tooltip_visible: false,
                is_embed_modal_visible: false,
                ..(*self).clone()
            },
            UiAction::SetSidebarSearch(query) => UiState {
                sidebar_search: query,
                ..(*self).clone()
            },
            UiAction::SetSidebarSort(sort) => UiState {
                sidebar_sort: sort,
                ..(*self).clone()
            },
            UiAction::ToggleKindFilter(group) => {
                let mut kinds = self.sidebar_kind_filter.clone();
                if !kinds.remove(&group) {
                    kinds.insert(group);
                }
                UiState {
                    sidebar_kind_filter: kinds,
                    ..(*self).clone()
                }
            }
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
