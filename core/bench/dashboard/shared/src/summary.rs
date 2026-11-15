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

//! The per-run summary that the run list sends.

use std::fmt;

use bench_report::{
    benchmark_kind::BenchmarkKind, group_metrics_kind::GroupMetricsKind,
    hardware::BenchmarkHardware, params::BenchmarkParams,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::BenchmarkReportLight;
use crate::config::Thresholds;
use crate::identity::{HistoryKey, KindFamily, RunVersion, VersionOrder, WorkloadKey};
use crate::metrics::{Badge, GroupMetrics};

/// Identity, labels, badges and comparable metrics of one run. The workload key holds the
/// kind, transport, message size, rate limit, options (durability among them) and the
/// workload label.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BenchmarkSummary {
    pub uuid: Uuid,
    /// Start of the run, as recorded in the report.
    pub timestamp: String,
    /// For example `Pinned producer, 20 producers`.
    pub title: String,
    pub machine: String,
    /// The machine that ran the bench. In a cluster run, the server nodes are other machines.
    pub hardware: BenchmarkHardware,
    pub nodes: usize,
    /// For display, for example `0.10.0-rc.1` or `0.10.0 at 91e020636`.
    pub version: String,
    pub version_order: VersionOrder,
    pub commit: Option<String>,
    /// The `gitref` is a release tag `server-X.Y.Z`. All other runs are builds.
    pub release: bool,
    pub gitref: Option<String>,
    pub gitref_date: Option<String>,
    pub suite: Option<String>,
    pub kind_family: KindFamily,
    pub workload_key: WorkloadKey,
    pub badges: Vec<Badge>,
    pub groups: Vec<GroupMetrics>,
}

/// How comparable two runs' results are. The levels go from closest to furthest, so the furthest
/// level among several pairs is their maximum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Comparability {
    LikeForLike,
    /// The same workload on another machine or topology.
    EnvironmentDiffers,
    /// The same kind family with another workload key.
    ParametersDiffer,
    /// Another kind family. No run is a winner.
    DifferentWorkloads,
}

impl BenchmarkSummary {
    /// The thresholds decide the badges. They come from the configuration file that the server
    /// reads at start, so a summary never outlives its thresholds.
    pub fn new(report: &BenchmarkReportLight, thresholds: &Thresholds) -> Self {
        let params = &report.params;
        let version = RunVersion::new(
            params.gitref.as_deref(),
            &report.server_stats.iggy_server_version,
        );
        let groups: Vec<_> = report
            .group_metrics
            .iter()
            .map(|group| GroupMetrics::new(group, &report.individual_metrics))
            .collect();
        Self {
            uuid: report.uuid,
            timestamp: report.timestamp.clone(),
            title: title(params),
            machine: report.machine().to_owned(),
            hardware: report.hardware.clone(),
            nodes: report.nodes(),
            version_order: VersionOrder::new(
                version.semver,
                params.gitref_date.as_deref(),
                &report.timestamp,
            ),
            version: version.text,
            commit: version.commit,
            release: version.release,
            gitref: params.gitref.clone(),
            gitref_date: params.gitref_date.clone(),
            suite: params.suite.clone(),
            kind_family: KindFamily::from(params.benchmark_kind),
            workload_key: WorkloadKey::from(params),
            badges: Badge::of_run(params, &groups, thresholds),
            groups,
        }
    }

    pub fn history_key(&self) -> HistoryKey {
        HistoryKey {
            workload: self.workload_key.clone(),
            machine: self.machine.clone(),
            nodes: self.nodes,
        }
    }

    pub fn group(&self, kind: GroupMetricsKind) -> Option<&GroupMetrics> {
        self.groups.iter().find(|group| group.kind == kind)
    }

    pub fn comparability(&self, other: &Self) -> Comparability {
        if self.kind_family != other.kind_family {
            Comparability::DifferentWorkloads
        } else if self.workload_key != other.workload_key {
            Comparability::ParametersDiffer
        } else if self.machine != other.machine || self.nodes != other.nodes {
            Comparability::EnvironmentDiffers
        } else {
            Comparability::LikeForLike
        }
    }
}

impl Comparability {
    /// One plain sentence for a reader who doesn't know the bench.
    pub const fn explanation(self) -> &'static str {
        match self {
            Self::LikeForLike => {
                "The runs ran the same workload on the same machine and topology, so a change \
                 comes from the build."
            }
            Self::EnvironmentDiffers => {
                "The runs ran the same workload on another machine or topology, so a change \
                 mixes the build with the environment."
            }
            Self::ParametersDiffer => {
                "The runs ran the same kind of workload with different parameters, so a change \
                 shows what the parameters did, not which build is better."
            }
            Self::DifferentWorkloads => {
                "The runs measured different kinds of workload, so no run is a winner."
            }
        }
    }
}

impl fmt::Display for Comparability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::LikeForLike => "Like for like",
            Self::EnvironmentDiffers => "Environment differs",
            Self::ParametersDiffer => "Parameters differ",
            Self::DifferentWorkloads => "Different workloads",
        })
    }
}

fn title(params: &BenchmarkParams) -> String {
    format!("{}, {}", kind_name(params.benchmark_kind), actors(params))
}

/// The actors of a run in plain words, for example `8 producers, 8 consumers`.
/// `BenchmarkParams::format_actors_info` would write "1 Producers".
pub fn actors(params: &BenchmarkParams) -> String {
    let end_to_end = KindFamily::from(params.benchmark_kind) == KindFamily::EndToEnd;
    // An end-to-end actor both sends and polls, and `producers` counts them.
    let counts = [
        (
            params.producers,
            if end_to_end {
                "producing consumer"
            } else {
                "producer"
            },
        ),
        (if end_to_end { 0 } else { params.consumers }, "consumer"),
        (params.consumer_groups, "consumer group"),
    ];
    let actors: Vec<_> = counts
        .into_iter()
        .filter(|(count, _)| *count > 0)
        .map(|(count, noun)| match count {
            1 => format!("1 {noun}"),
            _ => format!("{count} {noun}s"),
        })
        .collect();
    actors.join(", ")
}

/// The exact kind in plain words, for example `Pinned producer`.
pub const fn kind_name(kind: BenchmarkKind) -> &'static str {
    match kind {
        BenchmarkKind::PinnedProducer => "Pinned producer",
        BenchmarkKind::PinnedConsumer => "Pinned consumer",
        BenchmarkKind::PinnedProducerAndConsumer => "Pinned producer and consumer",
        BenchmarkKind::BalancedProducer => "Balanced producer",
        BenchmarkKind::BalancedConsumerGroup => "Balanced consumer group",
        BenchmarkKind::BalancedProducerAndConsumerGroup => "Balanced producer and consumer group",
        BenchmarkKind::EndToEndProducingConsumer => "End-to-end producing consumer",
        BenchmarkKind::EndToEndProducingConsumerGroup => "End-to-end producing consumer group",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::{
        CLUSTER_RUN, LIVE_UNCAPPED_RUN, OLD_ERA_RUN, PHASE_1A_RUN, THRESHOLDS, report,
    };
    use bench_report::{benchmark_kind::BenchmarkKind, transport::BenchmarkTransport};
    use semver::Version;
    use std::collections::HashMap;

    fn summary(report_json: &str) -> BenchmarkSummary {
        BenchmarkSummary::new(&report(report_json), &THRESHOLDS)
    }

    #[test]
    fn given_phase_1a_report_when_summarized_should_carry_identity_and_labels() {
        let summary = summary(PHASE_1A_RUN);

        assert_eq!(
            summary.uuid.to_string(),
            "b5c560ad-1ad2-49f0-88dd-c29db75126aa"
        );
        assert_eq!(summary.title, "Pinned producer, 2 producers");
        assert_eq!(summary.machine, "atlas");
        assert_eq!(
            summary.hardware.cpu_name,
            "AMD Ryzen 9 9950X3D 16-Core Processor"
        );
        assert_eq!(summary.hardware.cpu_cores, 32);
        assert_eq!(summary.hardware.total_memory_mb, 94075);
        assert_eq!(summary.nodes, 1);
        assert_eq!(summary.version, "0.10.0 at 53e3bf1e3");
        assert_eq!(summary.version_order.version, Version::parse("0.10.0").ok());
        assert_eq!(summary.commit.as_deref(), Some("53e3bf1e3"));
        assert!(!summary.release);
        assert_eq!(summary.suite.as_deref(), Some("atlas-single"));
        assert_eq!(summary.kind_family, KindFamily::Producer);
        assert_eq!(
            summary.workload_key.workload.as_deref(),
            Some("producer-2p@20MB")
        );
        assert_eq!(summary.workload_key.rate_limit.as_deref(), Some("20.00 MB"));
        assert_eq!(summary.groups.len(), 1);
        assert_eq!(summary.badges, []);
    }

    #[test]
    fn given_old_release_tag_when_summarized_should_show_tag_over_server_version() {
        let summary = summary(OLD_ERA_RUN);

        assert_eq!(summary.version, "0.4.202");
        assert!(summary.release);
        assert_eq!(
            summary.title,
            "Pinned producer and consumer, 8 producers, 8 consumers"
        );
        assert_eq!(summary.kind_family, KindFamily::ProducerAndConsumer);
    }

    #[test]
    fn given_runs_of_one_workload_when_keyed_by_history_should_share_one_line() {
        let first = summary(CLUSTER_RUN);
        let mut rerun = report(CLUSTER_RUN);
        rerun.uuid = Uuid::nil();
        rerun.params.gitref = Some("server-0.10.0".to_owned());
        rerun.params.remark = Some("rerun".to_owned());
        let rerun = BenchmarkSummary::new(&rerun, &THRESHOLDS);
        let other_machine = BenchmarkSummary {
            machine: "aws_i4i_4xlarge".to_owned(),
            ..first.clone()
        };
        let mut lines: HashMap<HistoryKey, Vec<Uuid>> = HashMap::new();

        for run in [&first, &rerun, &other_machine] {
            lines.entry(run.history_key()).or_default().push(run.uuid);
        }

        assert_eq!(lines.len(), 2);
        assert_eq!(lines[&first.history_key()], [first.uuid, rerun.uuid]);
    }

    #[test]
    fn given_pairs_of_runs_when_compared_should_name_comparability_level() {
        let base = summary(CLUSTER_RUN);
        let other_machine = BenchmarkSummary {
            machine: "aws_i4i_4xlarge".to_owned(),
            ..base.clone()
        };
        let single_node = BenchmarkSummary {
            nodes: 1,
            ..base.clone()
        };
        let mut other_rate = base.clone();
        other_rate.workload_key.rate_limit = Some("500.00 MB".to_owned());
        let mut other_rate_and_machine = other_machine.clone();
        other_rate_and_machine.workload_key.transport = BenchmarkTransport::Quic;
        let mut balanced = base.clone();
        balanced.workload_key.kind = BenchmarkKind::BalancedProducer;
        let mut consumer = base.clone();
        consumer.kind_family = KindFamily::Consumer;
        consumer.workload_key.kind = BenchmarkKind::PinnedConsumer;

        assert_eq!(base.comparability(&base), Comparability::LikeForLike);
        for (other, expected) in [
            (&other_machine, Comparability::EnvironmentDiffers),
            (&single_node, Comparability::EnvironmentDiffers),
            (&other_rate, Comparability::ParametersDiffer),
            (&other_rate_and_machine, Comparability::ParametersDiffer),
            (&balanced, Comparability::ParametersDiffer),
            (&consumer, Comparability::DifferentWorkloads),
        ] {
            assert_eq!(base.comparability(other), expected);
            assert_eq!(other.comparability(&base), expected);
        }
    }

    #[test]
    fn given_levels_when_ordered_should_go_from_like_for_like_to_different_workloads() {
        let levels = [
            Comparability::LikeForLike,
            Comparability::EnvironmentDiffers,
            Comparability::ParametersDiffer,
            Comparability::DifferentWorkloads,
        ];

        assert!(levels.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(
            [
                Comparability::EnvironmentDiffers,
                Comparability::LikeForLike,
                Comparability::ParametersDiffer,
            ]
            .into_iter()
            .max(),
            Some(Comparability::ParametersDiffer)
        );
    }

    #[test]
    fn given_kinds_and_actor_counts_when_titled_should_count_each_role_once() {
        let mut params = report(PHASE_1A_RUN).params;
        let mut titled = |kind, producers, consumers, consumer_groups| {
            params.benchmark_kind = kind;
            params.producers = producers;
            params.consumers = consumers;
            params.consumer_groups = consumer_groups;
            title(&params)
        };

        assert_eq!(
            titled(BenchmarkKind::PinnedProducer, 1, 0, 0),
            "Pinned producer, 1 producer"
        );
        assert_eq!(
            titled(BenchmarkKind::BalancedConsumerGroup, 0, 20, 1),
            "Balanced consumer group, 20 consumers, 1 consumer group"
        );
        assert_eq!(
            titled(BenchmarkKind::EndToEndProducingConsumer, 20, 20, 0),
            "End-to-end producing consumer, 20 producing consumers"
        );
        assert_eq!(
            titled(BenchmarkKind::EndToEndProducingConsumerGroup, 8, 8, 1),
            "End-to-end producing consumer group, 8 producing consumers, 1 consumer group"
        );
    }

    #[test]
    fn given_summary_when_serialized_should_round_trip() {
        let summary = summary(LIVE_UNCAPPED_RUN);
        let json = serde_json::to_string(&summary).expect("summaries serialize");

        assert_eq!(
            serde_json::from_str::<BenchmarkSummary>(&json).expect("summaries deserialize"),
            summary
        );
    }
}
