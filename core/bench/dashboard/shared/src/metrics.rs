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

//! Metrics of an actor group that compare across runs, and the badges for runs whose numbers
//! describe a different load from the one they asked for.

use std::fmt;

use bench_report::{
    actor_kind::ActorKind, group_metrics_kind::GroupMetricsKind, params::BenchmarkParams,
};
use serde::{Deserialize, Serialize};

use crate::config::Thresholds;
use crate::delta::Metric;
use crate::identity::KindFamily;
use crate::{BenchmarkGroupMetricsLight, BenchmarkIndividualMetricsLight};

pub const BYTES_PER_MEGABYTE: f64 = 1_000_000.0;
const PERCENT: f64 = 100.0;
/// The decimal units that `IggyByteSize` prints, in bytes.
const RATE_LIMIT_UNITS: [(&str, u64); 7] = [
    ("B", 1),
    ("KB", 1_000),
    ("MB", 1_000_000),
    ("GB", 1_000_000_000),
    ("TB", 1_000_000_000_000),
    ("PB", 1_000_000_000_000_000),
    ("EB", 1_000_000_000_000_000_000),
];

/// Metrics of one actor group, from the individual summaries of its actors.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GroupMetrics {
    pub kind: GroupMetricsKind,
    /// Total user data over the slowest actor's run time, in decimal megabytes. The report value
    /// adds up each actor's rate, which overstates the real rate when the actors finish at
    /// different times.
    pub throughput_megabytes_per_second: f64,
    pub throughput_messages_per_second: f64,
    /// The report's group values: the mean of the actor values.
    pub p50_latency_ms: f64,
    pub p90_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub p99_latency_ms: f64,
    pub p999_latency_ms: f64,
    /// (max - min) / mean of the actor run times. Actors without a run time are left out, as in
    /// the cloud-bench suite summary.
    pub time_spread_pct: f64,
}

/// A warning that a run's numbers describe a different load from the one it asked for. A run with a
/// badge never gets a colored delta.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Badge {
    UnevenLoad,
    BelowTargetRate,
}

impl GroupMetrics {
    pub(crate) fn new(
        group: &BenchmarkGroupMetricsLight,
        actors: &[BenchmarkIndividualMetricsLight],
    ) -> Self {
        let summary = &group.summary;
        let members: Vec<_> = actors
            .iter()
            .map(|actor| &actor.summary)
            .filter(|actor| is_member(summary.kind, actor.actor_kind))
            .collect();
        let slowest = members
            .iter()
            .map(|actor| actor.total_time_secs)
            .fold(0.0, f64::max);
        let per_second = |total: u64| {
            if slowest > 0.0 {
                total as f64 / slowest
            } else {
                0.0
            }
        };
        let user_bytes = members
            .iter()
            .map(|actor| actor.total_user_data_bytes)
            .sum();
        let messages = members.iter().map(|actor| actor.total_messages).sum();
        Self {
            kind: summary.kind,
            throughput_megabytes_per_second: per_second(user_bytes) / BYTES_PER_MEGABYTE,
            throughput_messages_per_second: per_second(messages),
            p50_latency_ms: summary.average_p50_latency_ms,
            p90_latency_ms: summary.average_p90_latency_ms,
            p95_latency_ms: summary.average_p95_latency_ms,
            p99_latency_ms: summary.average_p99_latency_ms,
            p999_latency_ms: summary.average_p999_latency_ms,
            time_spread_pct: time_spread_pct(members.iter().map(|actor| actor.total_time_secs)),
        }
    }

    pub const fn value(&self, metric: Metric) -> f64 {
        match metric {
            Metric::Throughput => self.throughput_megabytes_per_second,
            Metric::P50 => self.p50_latency_ms,
            Metric::P90 => self.p90_latency_ms,
            Metric::P95 => self.p95_latency_ms,
            Metric::P99 => self.p99_latency_ms,
            Metric::P999 => self.p999_latency_ms,
        }
    }
}

impl Badge {
    /// One plain sentence for a reader who doesn't know the bench.
    pub const fn explanation(self) -> &'static str {
        match self {
            Self::UnevenLoad => {
                "This run's actors finished at very different times, so they didn't share the load \
                 evenly."
            }
            Self::BelowTargetRate => {
                "This run didn't reach its rate limit, so the server handled less load than the \
                 run asked for."
            }
        }
    }

    /// Badges of a rate-limited run. A run without a rate limit shows throughput only, so it
    /// needs none.
    pub(crate) fn of_run(
        params: &BenchmarkParams,
        groups: &[GroupMetrics],
        thresholds: &Thresholds,
    ) -> Vec<Self> {
        let Some(rate_limit) = params.rate_limit.as_deref() else {
            return Vec::new();
        };
        // The combined group mixes two roles, so only a single-role group shows uneven load.
        let uneven = groups.iter().any(|group| {
            group.kind != GroupMetricsKind::ProducersAndConsumers
                && group.time_spread_pct > thresholds.max_time_spread_pct
        });
        let below_target = rate_limit_bytes(rate_limit)
            .and_then(|bytes| rate_limit_target(params, bytes))
            .is_some_and(|(limited_group, target_bytes)| {
                let target = target_bytes as f64 / BYTES_PER_MEGABYTE;
                groups
                    .iter()
                    .filter(|group| group.kind == limited_group)
                    .any(|group| {
                        target
                            > group.throughput_megabytes_per_second
                                * (1.0 + thresholds.throughput_pct / PERCENT)
                    })
            });
        [
            (uneven, Self::UnevenLoad),
            (below_target, Self::BelowTargetRate),
        ]
        .into_iter()
        .filter_map(|(applies, badge)| applies.then_some(badge))
        .collect()
    }
}

impl fmt::Display for Badge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::UnevenLoad => "uneven load",
            Self::BelowTargetRate => "below target rate",
        })
    }
}

/// The actors that `iggy-bench` puts into a group of the report.
const fn is_member(group: GroupMetricsKind, actor: ActorKind) -> bool {
    match group {
        GroupMetricsKind::Producers => matches!(actor, ActorKind::Producer),
        GroupMetricsKind::Consumers => matches!(actor, ActorKind::Consumer),
        GroupMetricsKind::ProducersAndConsumers => {
            matches!(actor, ActorKind::Producer | ActorKind::Consumer)
        }
        GroupMetricsKind::ProducingConsumers => matches!(actor, ActorKind::ProducingConsumer),
    }
}

/// The group whose actors got a producer rate limit, or the consumers of a consumer-only kind, and
/// the sum of the limits that the bench gave those actors, in bytes per second. `None` if the bench
/// didn't set a limit. Keep this in sync with `rate_limit_per_actor` and its callers in
/// `core/bench/src/benchmarks/common.rs`: each actor that shares the rate gets the integer bytes of
/// the rate divided by the sharing actors, and 0 bytes means no limit. In the producer and consumer
/// kinds the consumers share the rate too, but they only poll what the producers sent.
fn rate_limit_target(params: &BenchmarkParams, rate_bytes: u64) -> Option<(GroupMetricsKind, u64)> {
    let producers = u64::from(params.producers);
    let consumers = u64::from(params.consumers);
    let family = KindFamily::from(params.benchmark_kind);
    let (limited, sharing) = match family {
        KindFamily::Producer | KindFamily::ProducerAndConsumer => {
            (producers, producers + consumers)
        }
        KindFamily::Consumer => (consumers, producers + consumers),
        KindFamily::EndToEnd => (producers, producers),
    };
    let per_actor = rate_bytes.checked_div(sharing).filter(|bytes| *bytes > 0)?;
    Some((family.primary_group(), per_actor * limited))
}

/// Parses the rate limit as `iggy-bench` writes it, for example `800.00 MB` or `1.60 GB`.
pub fn rate_limit_bytes(rate_limit: &str) -> Option<u64> {
    let (value, unit) = rate_limit.split_once(' ')?;
    let (_, unit_bytes) = RATE_LIMIT_UNITS.iter().find(|(name, _)| *name == unit)?;
    let bytes = value.parse::<f64>().ok()? * *unit_bytes as f64;
    (bytes.is_finite() && bytes >= 0.0).then(|| bytes.round() as u64)
}

fn time_spread_pct(times: impl Iterator<Item = f64>) -> f64 {
    let times: Vec<_> = times.filter(|time| *time > 0.0).collect();
    if times.is_empty() {
        return 0.0;
    }
    let fastest = times.iter().copied().fold(f64::INFINITY, f64::min);
    let slowest = times.iter().copied().fold(0.0, f64::max);
    let mean = times.iter().sum::<f64>() / times.len() as f64;
    (slowest - fastest) / mean * PERCENT
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BenchmarkReportLight;
    use crate::summary::BenchmarkSummary;
    use crate::test_fixtures::{
        CLUSTER_RUN, LIVE_UNCAPPED_RUN, OLD_ERA_RUN, PERSISTED_RUN, THRESHOLDS, report,
    };
    use bench_report::benchmark_kind::BenchmarkKind;

    const RUN_SECS: f64 = 10.0;

    fn producers(summary: &BenchmarkSummary) -> &GroupMetrics {
        summary
            .group(GroupMetricsKind::Producers)
            .expect("every fixture run has producers")
    }

    fn consumers(summary: &BenchmarkSummary) -> &GroupMetrics {
        summary
            .group(GroupMetricsKind::Consumers)
            .expect("a producer and consumer run has consumers")
    }

    fn badges(report: &BenchmarkReportLight) -> Vec<Badge> {
        BenchmarkSummary::new(report, &THRESHOLDS).badges
    }

    /// A producer and consumer run from the actors of the old era run. Every actor runs
    /// `RUN_SECS`, and the producers together move `producers_mb_per_sec`.
    fn mixed_run(
        kind: BenchmarkKind,
        producers: u32,
        consumers: u32,
        rate_limit: &str,
        producers_mb_per_sec: f64,
    ) -> BenchmarkReportLight {
        let mut report = report(OLD_ERA_RUN);
        let template = |actor_kind| {
            report
                .individual_metrics
                .iter()
                .find(|actor| actor.summary.actor_kind == actor_kind)
                .cloned()
                .expect("the old era run has producers and consumers")
        };
        let producer = template(ActorKind::Producer);
        let consumer = template(ActorKind::Consumer);
        let user_bytes =
            (producers_mb_per_sec * BYTES_PER_MEGABYTE * RUN_SECS / f64::from(producers)) as u64;
        let actors = |template: BenchmarkIndividualMetricsLight, count| {
            (0..count).map(move |_| {
                let mut actor = template.clone();
                actor.summary.total_time_secs = RUN_SECS;
                actor.summary.total_user_data_bytes = user_bytes;
                actor
            })
        };
        report.individual_metrics = actors(producer, producers)
            .chain(actors(consumer, consumers))
            .collect();
        report.params.benchmark_kind = kind;
        report.params.producers = producers;
        report.params.consumers = consumers;
        report.params.rate_limit = Some(rate_limit.to_owned());
        report
    }

    fn consumers_of(
        report: &mut BenchmarkReportLight,
    ) -> Vec<&mut BenchmarkIndividualMetricsLight> {
        report
            .individual_metrics
            .iter_mut()
            .filter(|actor| actor.summary.actor_kind == ActorKind::Consumer)
            .collect()
    }

    #[test]
    fn given_uncapped_run_with_uneven_actors_when_measured_should_divide_by_slowest_actor() {
        let report = report(LIVE_UNCAPPED_RUN);
        let summary = BenchmarkSummary::new(&report, &THRESHOLDS);
        let group = producers(&summary);

        assert_eq!(
            report.group_metrics[0]
                .summary
                .total_throughput_megabytes_per_second
                .round(),
            2732.0
        );
        assert_eq!(group.throughput_megabytes_per_second.round(), 2062.0);
        assert_eq!(group.throughput_messages_per_second.round(), 2_062_111.0);
        assert_eq!(group.p99_latency_ms, 26.597);
        assert_eq!(group.time_spread_pct.round(), 78.0);
        assert_eq!(summary.badges, []);
    }

    #[test]
    fn given_persisted_run_at_313_of_500_mb_when_badged_should_be_below_target_rate() {
        let summary = BenchmarkSummary::new(&report(PERSISTED_RUN), &THRESHOLDS);

        assert_eq!(
            producers(&summary).throughput_megabytes_per_second.round(),
            313.0
        );
        assert_eq!(summary.badges, [Badge::BelowTargetRate]);
    }

    #[test]
    fn given_run_at_784_of_800_mb_when_badged_should_get_no_badge() {
        let summary = BenchmarkSummary::new(&report(CLUSTER_RUN), &THRESHOLDS);

        assert_eq!(
            producers(&summary).throughput_megabytes_per_second.round(),
            784.0
        );
        assert_eq!(summary.badges, []);
    }

    #[test]
    fn given_rate_limited_actor_finishing_early_when_badged_should_show_uneven_load() {
        let mut report = report(CLUSTER_RUN);
        report.individual_metrics[0].summary.total_time_secs *= 0.8;

        let summary = BenchmarkSummary::new(&report, &THRESHOLDS);

        assert!(producers(&summary).time_spread_pct > THRESHOLDS.max_time_spread_pct);
        assert_eq!(summary.badges, [Badge::UnevenLoad]);
    }

    #[test]
    fn given_uneven_run_without_rate_limit_when_badged_should_get_no_badge() {
        let mut report = report(CLUSTER_RUN);
        report.params.rate_limit = None;
        report.individual_metrics[0].summary.total_time_secs *= 0.5;

        assert_eq!(BenchmarkSummary::new(&report, &THRESHOLDS).badges, []);
    }

    #[test]
    fn given_ppc_20p_20c_at_800_mb_when_producers_reach_400_should_get_no_badge() {
        let run = mixed_run(
            BenchmarkKind::PinnedProducerAndConsumer,
            20,
            20,
            "800.00 MB",
            400.0,
        );

        assert_eq!(badges(&run), []);
    }

    #[test]
    fn given_bpcg_5p_20c_at_1000_mb_when_producers_measured_should_compare_with_200() {
        let kind = BenchmarkKind::BalancedProducerAndConsumerGroup;

        assert_eq!(badges(&mixed_run(kind, 5, 20, "1.00 GB", 200.0)), []);
        assert_eq!(
            badges(&mixed_run(kind, 5, 20, "1.00 GB", 180.0)),
            [Badge::BelowTargetRate]
        );
    }

    #[test]
    fn given_ppc_with_even_producers_and_uneven_consumers_when_badged_should_show_uneven_load() {
        let mut run = mixed_run(
            BenchmarkKind::PinnedProducerAndConsumer,
            20,
            20,
            "800.00 MB",
            400.0,
        );
        consumers_of(&mut run)[0].summary.total_time_secs = RUN_SECS * 0.85;

        let summary = BenchmarkSummary::new(&run, &THRESHOLDS);

        assert_eq!(producers(&summary).time_spread_pct, 0.0);
        assert!(consumers(&summary).time_spread_pct > 15.0);
        assert_eq!(summary.badges, [Badge::UnevenLoad]);
    }

    #[test]
    fn given_roles_finishing_apart_but_each_even_when_badged_should_ignore_combined_group() {
        let mut run = mixed_run(
            BenchmarkKind::PinnedProducerAndConsumer,
            20,
            20,
            "800.00 MB",
            400.0,
        );
        for consumer in consumers_of(&mut run) {
            consumer.summary.total_time_secs = RUN_SECS * 1.2;
        }

        let summary = BenchmarkSummary::new(&run, &THRESHOLDS);
        let combined = summary
            .group(GroupMetricsKind::ProducersAndConsumers)
            .expect("the old era run has a combined group");

        assert!(combined.time_spread_pct > THRESHOLDS.max_time_spread_pct);
        assert_eq!(summary.badges, []);
    }

    #[test]
    fn given_rate_below_one_byte_per_actor_when_badged_should_treat_run_as_unlimited() {
        let run = mixed_run(
            BenchmarkKind::PinnedProducerAndConsumer,
            20,
            20,
            "39 B",
            0.0,
        );

        assert_eq!(badges(&run), []);
    }

    #[test]
    fn given_producer_and_consumer_run_when_measured_should_split_actors_by_group() {
        let summary = BenchmarkSummary::new(&report(OLD_ERA_RUN), &THRESHOLDS);
        let throughput = |kind| {
            summary
                .group(kind)
                .map(|group| group.throughput_megabytes_per_second.round())
        };

        assert_eq!(throughput(GroupMetricsKind::Producers), Some(1416.0));
        assert_eq!(throughput(GroupMetricsKind::Consumers), Some(1415.0));
        assert_eq!(
            throughput(GroupMetricsKind::ProducersAndConsumers),
            Some(2830.0)
        );
    }

    #[test]
    fn given_actors_without_run_time_when_spread_is_measured_should_leave_them_out() {
        assert_eq!(time_spread_pct([0.0, 10.0, 10.0].into_iter()), 0.0);
        assert_eq!(time_spread_pct([6.0, 10.0].into_iter()), 50.0);
        assert_eq!(time_spread_pct(std::iter::empty()), 0.0);
    }

    #[test]
    fn given_rate_limits_as_the_bench_writes_them_when_parsed_should_give_bytes() {
        for (text, expected) in [
            ("800.00 MB", 800_000_000),
            ("62.50 MB", 62_500_000),
            ("1.60 GB", 1_600_000_000),
            ("1.00 TB", 1_000_000_000_000),
            ("250.00 KB", 250_000),
            ("500 B", 500),
        ] {
            assert_eq!(rate_limit_bytes(text), Some(expected), "{text}");
        }
        for text in [
            "",
            "800",
            "800.00 MiB",
            "800.00  MB",
            "fast MB",
            "-5.00 MB",
            "inf MB",
        ] {
            assert_eq!(rate_limit_bytes(text), None, "{text}");
        }
    }
}
