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
    pub hardware_filter: Option<String>,
    pub gitref_filter: Option<String>,
}

impl Default for UiState {
    fn default() -> Self {
        Self {
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
            hardware_filter: None,
            gitref_filter: None,
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
    SetParamRange(ParamField, ParamRange),
    SetMetricRange(MetricField, MetricRange),
    ClearParamFilters,
    ToggleSidebar,
    SetComparePin(Box<Option<BenchmarkReportLight>>),
    CloseAllPopups,
    SetSidebarSearch(String),
    SetSidebarSort(SidebarSort),
    ToggleKindFilter(KindGroup),
    SetHardwareFilter(Option<String>),
    SetGitrefFilter(Option<String>),
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
            UiAction::SetHardwareFilter(value) => UiState {
                hardware_filter: value,
                ..(*self).clone()
            },
            UiAction::SetGitrefFilter(value) => UiState {
                gitref_filter: value,
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

#[cfg(test)]
mod tests {
    use super::*;
    use bench_dashboard_shared::BenchmarkGroupMetricsLight;
    use bench_report::group_metrics_kind::GroupMetricsKind;
    use bench_report::group_metrics_summary::BenchmarkGroupMetricsSummary;

    #[test]
    fn given_empty_range_when_checking_activity_should_be_inactive() {
        let range = ParamRange::default();
        assert!(!range.is_active());
        assert!(range.matches(0));
        assert!(range.matches(u32::MAX));
    }

    #[test]
    fn given_from_bound_when_matching_should_reject_below_and_accept_above() {
        let range = ParamRange {
            from: Some(4),
            to: None,
        };
        assert!(range.is_active());
        assert!(!range.matches(3));
        assert!(range.matches(4));
        assert!(range.matches(100));
    }

    #[test]
    fn given_inclusive_range_when_matching_should_include_both_bounds() {
        let range = ParamRange {
            from: Some(2),
            to: Some(8),
        };
        assert!(range.matches(2));
        assert!(range.matches(8));
        assert!(!range.matches(1));
        assert!(!range.matches(9));
    }

    #[test]
    fn given_no_filters_when_counting_active_should_return_zero() {
        let filters = ParamFilters::default();
        assert_eq!(filters.active_count(), 0);
    }

    #[test]
    fn given_multiple_active_filters_when_counting_should_sum_all_kinds() {
        let filters = ParamFilters {
            producers: ParamRange {
                from: Some(1),
                to: None,
            },
            p99_latency_ms: MetricRange {
                from: None,
                to: Some(50.0),
            },
            ..Default::default()
        };
        assert_eq!(filters.active_count(), 2);
    }

    #[test]
    fn given_benchmark_matching_param_range_when_matching_should_pass() {
        let filters = ParamFilters {
            producers: ParamRange {
                from: Some(4),
                to: Some(16),
            },
            ..Default::default()
        };
        let benchmark = benchmark_with(8, 0, 1, 1, 0, 100.0, 1.0);
        assert!(filters.matches(&benchmark));
    }

    #[test]
    fn given_benchmark_outside_param_range_when_matching_should_fail() {
        let filters = ParamFilters {
            producers: ParamRange {
                from: Some(4),
                to: Some(16),
            },
            ..Default::default()
        };
        let benchmark = benchmark_with(2, 0, 1, 1, 0, 100.0, 1.0);
        assert!(!filters.matches(&benchmark));
    }

    #[test]
    fn given_metric_filter_when_benchmark_has_no_metrics_should_fail() {
        let filters = ParamFilters {
            p99_latency_ms: MetricRange {
                from: None,
                to: Some(10.0),
            },
            ..Default::default()
        };
        let mut benchmark = benchmark_with(1, 0, 1, 1, 0, 0.0, 0.0);
        benchmark.group_metrics.clear();
        assert!(!filters.matches(&benchmark));
    }

    #[test]
    fn given_no_metric_filter_when_benchmark_has_no_metrics_should_pass() {
        let filters = ParamFilters::default();
        let mut benchmark = benchmark_with(1, 0, 1, 1, 0, 0.0, 0.0);
        benchmark.group_metrics.clear();
        assert!(filters.matches(&benchmark));
    }

    #[test]
    fn given_pinned_kind_group_when_matching_should_accept_pinned_kinds_only() {
        assert!(KindGroup::Pinned.matches(BenchmarkKind::PinnedProducer));
        assert!(KindGroup::Pinned.matches(BenchmarkKind::PinnedConsumer));
        assert!(KindGroup::Pinned.matches(BenchmarkKind::PinnedProducerAndConsumer));
        assert!(!KindGroup::Pinned.matches(BenchmarkKind::BalancedProducer));
        assert!(!KindGroup::Pinned.matches(BenchmarkKind::EndToEndProducingConsumer));
    }

    #[test]
    fn given_balanced_kind_group_when_matching_should_accept_balanced_kinds_only() {
        assert!(KindGroup::Balanced.matches(BenchmarkKind::BalancedProducer));
        assert!(KindGroup::Balanced.matches(BenchmarkKind::BalancedConsumerGroup));
        assert!(KindGroup::Balanced.matches(BenchmarkKind::BalancedProducerAndConsumerGroup));
        assert!(!KindGroup::Balanced.matches(BenchmarkKind::PinnedProducer));
        assert!(!KindGroup::Balanced.matches(BenchmarkKind::EndToEndProducingConsumer));
    }

    #[test]
    fn given_end_to_end_kind_group_when_matching_should_accept_e2e_kinds_only() {
        assert!(KindGroup::EndToEnd.matches(BenchmarkKind::EndToEndProducingConsumer));
        assert!(KindGroup::EndToEnd.matches(BenchmarkKind::EndToEndProducingConsumerGroup));
        assert!(!KindGroup::EndToEnd.matches(BenchmarkKind::PinnedProducer));
        assert!(!KindGroup::EndToEnd.matches(BenchmarkKind::BalancedProducer));
    }

    fn benchmark_with(
        producers: u32,
        consumers: u32,
        streams: u32,
        partitions: u32,
        consumer_groups: u32,
        throughput_mb_s: f64,
        p99_ms: f64,
    ) -> BenchmarkReportLight {
        let mut report = BenchmarkReportLight {
            params: Default::default(),
            ..Default::default()
        };
        report.params.producers = producers;
        report.params.consumers = consumers;
        report.params.streams = streams;
        report.params.partitions = partitions;
        report.params.consumer_groups = consumer_groups;
        report.group_metrics.push(BenchmarkGroupMetricsLight {
            summary: summary_with(throughput_mb_s, p99_ms),
            latency_distribution: None,
        });
        report
    }

    fn summary_with(throughput_mb_s: f64, p99_ms: f64) -> BenchmarkGroupMetricsSummary {
        BenchmarkGroupMetricsSummary {
            kind: GroupMetricsKind::Producers,
            total_throughput_megabytes_per_second: throughput_mb_s,
            total_throughput_messages_per_second: 0.0,
            average_throughput_megabytes_per_second: 0.0,
            average_throughput_messages_per_second: 0.0,
            average_p50_latency_ms: 0.0,
            average_p90_latency_ms: 0.0,
            average_p95_latency_ms: 0.0,
            average_p99_latency_ms: p99_ms,
            average_p999_latency_ms: 0.0,
            average_p9999_latency_ms: 0.0,
            average_latency_ms: 0.0,
            average_median_latency_ms: 0.0,
            min_latency_ms: 0.0,
            max_latency_ms: 0.0,
            std_dev_latency_ms: 0.0,
        }
    }
}
