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

//! The time series of one run, reduced on the server for charts.

use std::fmt;

use bench_report::actor_kind::ActorKind;
use bench_report::group_metrics_kind::GroupMetricsKind;
use bench_report::report::BenchmarkReport;
use bench_report::time_series::{MAX_CHART_POINTS, TimePoint, TimeSeries};
use bench_report::utils::lttb_downsample;
use serde::{Deserialize, Serialize};

/// One metric of one run. LTTB keeps at most `MAX_CHART_POINTS` points of each series, the
/// same points that the bench-report charts draw.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BenchmarkSeries {
    pub metric: SeriesMetric,
    /// `ms` or `MB/s`.
    pub unit: String,
    pub series: Vec<ChartSeries>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChartSeries {
    pub source: SeriesSource,
    pub points: Vec<TimePoint>,
}

/// What a series measures. The `Display` of a kind names it in plain words, for example
/// "Producers", or "Producer 3" with the actor id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SeriesSource {
    /// A group's total MB/s, or the average latency of its actors.
    Group(GroupMetricsKind),
    Actor {
        kind: ActorKind,
        id: u32,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SeriesMetric {
    /// The average latency of each sampling window. Reports have no percentile series.
    Latency,
    /// The MB/s of each sampling window.
    Throughput,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SeriesScope {
    /// One aggregate series per actor group. Like the charts, it leaves out the combined group
    /// of producers and consumers.
    Group,
    /// One series per actor.
    Actors,
}

impl BenchmarkSeries {
    pub fn new(report: &BenchmarkReport, metric: SeriesMetric, scope: SeriesScope) -> Self {
        let series = match scope {
            SeriesScope::Group => report
                .group_metrics
                .iter()
                .filter(|group| group.summary.kind != GroupMetricsKind::ProducersAndConsumers)
                .map(|group| {
                    let stored = match metric {
                        SeriesMetric::Latency => &group.avg_latency_ts,
                        SeriesMetric::Throughput => &group.avg_throughput_mb_ts,
                    };
                    ChartSeries::new(SeriesSource::Group(group.summary.kind), stored)
                })
                .collect(),
            SeriesScope::Actors => report
                .individual_metrics
                .iter()
                .map(|actor| {
                    let stored = match metric {
                        SeriesMetric::Latency => &actor.latency_ts,
                        SeriesMetric::Throughput => &actor.throughput_mb_ts,
                    };
                    let source = SeriesSource::Actor {
                        kind: actor.summary.actor_kind,
                        id: actor.summary.actor_id,
                    };
                    ChartSeries::new(source, stored)
                })
                .collect(),
        };
        Self {
            metric,
            unit: metric.unit().to_owned(),
            series,
        }
    }
}

impl SeriesMetric {
    const fn unit(self) -> &'static str {
        match self {
            Self::Latency => "ms",
            Self::Throughput => "MB/s",
        }
    }
}

impl SeriesSource {
    /// The group itself, or the group that holds the actor.
    pub fn group(self) -> GroupMetricsKind {
        match self {
            Self::Group(kind) => kind,
            Self::Actor { kind, .. } => kind.into(),
        }
    }
}

impl ChartSeries {
    fn new(source: SeriesSource, stored: &TimeSeries) -> Self {
        Self {
            source,
            points: lttb_downsample(&stored.points, MAX_CHART_POINTS),
        }
    }
}

/// The legend name of a series, for example "Producers", or "Consumer 3" for one actor.
impl fmt::Display for SeriesSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Group(kind) => write!(f, "{kind}"),
            Self::Actor { kind, id } => write!(f, "{kind} {id}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::OLD_ERA_RUN;
    use serde_json::json;

    const METRICS: [SeriesMetric; 2] = [SeriesMetric::Latency, SeriesMetric::Throughput];
    const SCOPES: [SeriesScope; 2] = [SeriesScope::Group, SeriesScope::Actors];
    const LONG_SERIES_LEN: usize = 3 * MAX_CHART_POINTS;
    /// Off the stride of plain decimation, which would drop it.
    const SPIKE_AT: usize = LONG_SERIES_LEN / 3 + 1;
    const SAMPLING_WINDOWS_PER_SECOND: f64 = 100.0;
    const FLAT_VALUE: f64 = 1.0;
    const SPIKE_VALUE: f64 = 100.0;
    const ACTORS_PER_KIND: u32 = 8;
    const ACTOR_ID: u32 = 3;

    /// 8 producers, 8 consumers, and the groups of producers, consumers, and both. Each series
    /// has 3 points.
    fn producer_and_consumer_report() -> BenchmarkReport {
        serde_json::from_str(OLD_ERA_RUN).expect("the fixture parses as a full report")
    }

    fn flat_line_with_one_spike() -> TimeSeries {
        TimeSeries {
            points: (0..LONG_SERIES_LEN)
                .map(|window| {
                    let value = if window == SPIKE_AT {
                        SPIKE_VALUE
                    } else {
                        FLAT_VALUE
                    };
                    TimePoint::new(window as f64 / SAMPLING_WINDOWS_PER_SECOND, value)
                })
                .collect(),
            ..TimeSeries::default()
        }
    }

    fn sources(series: &BenchmarkSeries) -> Vec<SeriesSource> {
        series.series.iter().map(|series| series.source).collect()
    }

    fn actors(kind: ActorKind) -> impl Iterator<Item = SeriesSource> {
        (1..=ACTORS_PER_KIND).map(move |id| SeriesSource::Actor { kind, id })
    }

    #[test]
    fn given_series_longer_than_limit_when_reduced_should_keep_bound_ends_and_spike() {
        let line = flat_line_with_one_spike();
        let mut report = producer_and_consumer_report();
        for group in &mut report.group_metrics {
            group.avg_latency_ts = line.clone();
            group.avg_throughput_mb_ts = line.clone();
        }
        for actor in &mut report.individual_metrics {
            actor.latency_ts = line.clone();
            actor.throughput_mb_ts = line.clone();
        }
        let spike = &line.points[SPIKE_AT];

        for metric in METRICS {
            for scope in SCOPES {
                let reduced = BenchmarkSeries::new(&report, metric, scope);

                assert!(!reduced.series.is_empty(), "{metric:?} {scope:?}");
                for series in &reduced.series {
                    let context = format!("{metric:?} {scope:?} {:?}", series.source);
                    assert_eq!(series.points.len(), MAX_CHART_POINTS, "{context}");
                    assert_eq!(series.points.first(), line.points.first(), "{context}");
                    assert_eq!(series.points.last(), line.points.last(), "{context}");
                    assert!(series.points.contains(spike), "{context}");
                }
            }
        }
    }

    #[test]
    fn given_producer_and_consumer_run_when_reduced_should_identify_stored_series_by_kind() {
        let report = producer_and_consumer_report();
        let producers = &report.group_metrics[0];
        let first_actor = &report.individual_metrics[0];
        let groups = vec![
            SeriesSource::Group(GroupMetricsKind::Producers),
            SeriesSource::Group(GroupMetricsKind::Consumers),
        ];
        let actor_sources: Vec<_> = actors(ActorKind::Producer)
            .chain(actors(ActorKind::Consumer))
            .collect();

        for (metric, scope, unit, expected_sources, first_stored) in [
            (
                SeriesMetric::Latency,
                SeriesScope::Group,
                "ms",
                &groups,
                &producers.avg_latency_ts,
            ),
            (
                SeriesMetric::Throughput,
                SeriesScope::Group,
                "MB/s",
                &groups,
                &producers.avg_throughput_mb_ts,
            ),
            (
                SeriesMetric::Latency,
                SeriesScope::Actors,
                "ms",
                &actor_sources,
                &first_actor.latency_ts,
            ),
            (
                SeriesMetric::Throughput,
                SeriesScope::Actors,
                "MB/s",
                &actor_sources,
                &first_actor.throughput_mb_ts,
            ),
        ] {
            let reduced = BenchmarkSeries::new(&report, metric, scope);

            assert_eq!(reduced.metric, metric);
            assert_eq!(reduced.unit, unit, "{metric:?}");
            assert_eq!(&sources(&reduced), expected_sources, "{metric:?} {scope:?}");
            assert_eq!(
                reduced.series[0].points, first_stored.points,
                "{metric:?} {scope:?}"
            );
        }
    }

    #[test]
    fn given_end_to_end_run_when_reduced_should_identify_producing_consumers() {
        let mut report = producer_and_consumer_report();
        report.group_metrics.truncate(1);
        report.group_metrics[0].summary.kind = GroupMetricsKind::ProducingConsumers;
        report.individual_metrics.truncate(1);
        report.individual_metrics[0].summary.actor_kind = ActorKind::ProducingConsumer;
        let id = report.individual_metrics[0].summary.actor_id;

        for metric in METRICS {
            assert_eq!(
                sources(&BenchmarkSeries::new(&report, metric, SeriesScope::Group)),
                [SeriesSource::Group(GroupMetricsKind::ProducingConsumers)],
                "{metric:?}"
            );
            assert_eq!(
                sources(&BenchmarkSeries::new(&report, metric, SeriesScope::Actors)),
                [SeriesSource::Actor {
                    kind: ActorKind::ProducingConsumer,
                    id
                }],
                "{metric:?}"
            );
        }
    }

    #[test]
    fn given_sources_when_serialized_should_use_report_kind_names() {
        let group = SeriesSource::Group(GroupMetricsKind::ProducingConsumers);
        let actor = SeriesSource::Actor {
            kind: ActorKind::Consumer,
            id: ACTOR_ID,
        };

        assert_eq!(
            serde_json::to_value(group).expect("a source serializes"),
            json!({ "group": "producing_consumers" })
        );
        assert_eq!(
            serde_json::to_value(actor).expect("a source serializes"),
            json!({ "actor": { "kind": "consumer", "id": ACTOR_ID } })
        );
    }

    #[test]
    fn given_sources_when_asked_for_group_should_return_group_or_group_of_actor() {
        for (kind, group) in [
            (ActorKind::Producer, GroupMetricsKind::Producers),
            (ActorKind::Consumer, GroupMetricsKind::Consumers),
            (
                ActorKind::ProducingConsumer,
                GroupMetricsKind::ProducingConsumers,
            ),
        ] {
            let actor = SeriesSource::Actor { kind, id: ACTOR_ID };

            assert_eq!(actor.group(), group, "{kind:?}");
            assert_eq!(SeriesSource::Group(group).group(), group, "{group:?}");
        }
    }

    #[test]
    fn given_sources_when_displayed_should_name_group_or_actor_in_words() {
        let group = SeriesSource::Group(GroupMetricsKind::Producers);
        let actor = SeriesSource::Actor {
            kind: ActorKind::Consumer,
            id: ACTOR_ID,
        };

        assert_eq!(group.to_string(), "Producers");
        assert_eq!(actor.to_string(), "Consumer 3");
    }
}
