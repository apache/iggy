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

//! The headline workloads as the Overview shows them: the latest release's number with its change
//! against the previous release, the trend across releases, and the change since the release. The
//! maintainers choose the workloads in the configuration file, so the Overview never picks a run by
//! its numbers.

use bench_dashboard_shared::config::{DashboardConfig, HeadlineConfig, SuiteConfig, Thresholds};
use bench_dashboard_shared::delta::Metric;
use bench_dashboard_shared::identity::VersionOrder;
use bench_dashboard_shared::summary::BenchmarkSummary;
use uuid::Uuid;

use crate::pages::compare::releases::ReleasesQuery;
use crate::pages::run::change::{Change, Headline, HistoryLine, LineVersion, VersionStep, change};
use crate::pages::run::facts::conditions;
use crate::pages::run::history::HistoryChart;

/// One release isn't a trend.
const MIN_TREND_RELEASES: usize = 2;

/// Built once per load of the runs and the configuration.
#[derive(Debug, PartialEq)]
pub struct Headlines {
    /// The highest release among the official runs. `None` before the first release.
    pub latest_release: Option<String>,
    /// The machine behind every card that has a release, when they all share one.
    pub machine: Option<String>,
    /// One per headline workload, in configuration file order.
    pub cards: Vec<HeadlineCard>,
    /// One per headline workload that at least two releases ran.
    pub trends: Vec<ReleaseTrend>,
    /// One per headline workload that ran the latest release and a build after it.
    pub since_release: Vec<SinceRelease>,
    /// One per suite among the rows of `since_release`, in row order.
    pub release_links: Vec<ReleaseLink>,
}

#[derive(Debug, PartialEq)]
pub struct HeadlineCard {
    /// From the configuration file.
    pub title: String,
    /// The median of the runs of the card's release, and its change against the previous release.
    /// Without a release, it has no value.
    pub headline: Headline,
    /// `None` when no release ran the workload.
    pub release: Option<CardRelease>,
}

/// The newest release that ran a headline workload. That's the latest release, unless it didn't run
/// the workload.
#[derive(Debug, PartialEq)]
pub struct CardRelease {
    pub version: String,
    pub runs: usize,
    /// The version's newest run.
    pub newest: Uuid,
    /// For example `Pinned producer, 20 producers`.
    pub run_title: String,
    pub conditions: String,
    pub machine: String,
}

#[derive(Debug, PartialEq)]
pub struct ReleaseTrend {
    /// From the configuration file.
    pub title: String,
    /// A run on the line, used to name the chart.
    pub uuid: Uuid,
    pub chart: HistoryChart,
}

/// The latest release against the newest build after it on the same history line.
#[derive(Debug, PartialEq)]
pub struct SinceRelease {
    /// From the configuration file.
    pub title: String,
    pub suite: String,
    pub metric: Metric,
    pub release: String,
    pub build: String,
    /// The build's place in version order, which is the place of its first run.
    pub build_order: VersionOrder,
    /// The medians of each version's runs.
    pub release_value: f64,
    pub build_value: f64,
    pub change: Change,
}

/// The releases compare for one suite: the latest release against the newest build among the
/// suite's rows.
#[derive(Debug, PartialEq)]
pub struct ReleaseLink {
    /// The suite's title.
    pub title: String,
    pub query: ReleasesQuery,
}

/// The official runs of one headline workload, on the history line of its newest release run. Runs
/// with the same labels on another line ran a different workload, so they're never compared.
struct HeadlineLine<'a> {
    config: &'a HeadlineConfig,
    /// Releases and builds.
    all: HistoryLine<'a>,
    releases: HistoryLine<'a>,
}

impl Headlines {
    pub fn new(summaries: &[BenchmarkSummary], config: &DashboardConfig) -> Self {
        let official: Vec<_> = summaries
            .iter()
            .filter(|summary| config.is_official(summary.suite.as_deref()))
            .collect();
        let lines: Vec<_> = config
            .headlines
            .iter()
            .map(|headline| HeadlineLine::new(headline, &official))
            .collect();
        let thresholds = &config.thresholds;
        let latest_release =
            newest_release(official.iter().copied()).map(|summary| summary.version.as_str());
        let cards: Vec<_> = lines.iter().map(|line| line.card(thresholds)).collect();
        let since_release: Vec<_> = lines
            .iter()
            .filter_map(|line| line.since_release(latest_release, thresholds))
            .collect();
        Self {
            machine: shared_machine(&cards),
            trends: lines.iter().filter_map(HeadlineLine::trend).collect(),
            release_links: release_links(&since_release, &config.suites),
            since_release,
            latest_release: latest_release.map(str::to_owned),
            cards,
        }
    }
}

impl<'a> HeadlineLine<'a> {
    fn new(config: &'a HeadlineConfig, official: &[&'a BenchmarkSummary]) -> Self {
        let runs: Vec<_> = official
            .iter()
            .copied()
            .filter(|summary| {
                summary.suite.as_deref() == Some(config.suite.as_str())
                    && summary.workload_key.workload.as_deref() == Some(config.workload.as_str())
            })
            .collect();
        let key = newest_release(runs.iter().copied()).map(BenchmarkSummary::history_key);
        let on_line: Vec<_> = runs
            .into_iter()
            .filter(|summary| key.as_ref() == Some(&summary.history_key()))
            .collect();
        Self {
            config,
            releases: HistoryLine::from_runs(on_line.iter().copied().filter(|run| run.release)),
            all: HistoryLine::from_runs(on_line),
        }
    }

    fn card(&self, thresholds: &Thresholds) -> HeadlineCard {
        let metric = self.config.metric;
        let newest = self.newest_release();
        HeadlineCard {
            title: self.config.title.clone(),
            headline: Headline {
                metric,
                value: newest.and_then(|(version, _)| version.median(metric)),
                change: newest.and_then(|(_, run)| {
                    let step = self.releases.step(run)?;
                    change(metric, run, &step, thresholds)
                }),
            },
            release: newest.map(|(version, run)| CardRelease {
                version: version.version.to_owned(),
                runs: version.runs.len(),
                newest: run.uuid,
                run_title: run.title.clone(),
                conditions: conditions(run),
                machine: run.machine.clone(),
            }),
        }
    }

    fn trend(&self) -> Option<ReleaseTrend> {
        if self.releases.versions.len() < MIN_TREND_RELEASES {
            return None;
        }
        let (_, newest) = self.newest_release()?;
        Some(ReleaseTrend {
            title: self.config.title.clone(),
            uuid: newest.uuid,
            chart: HistoryChart::new(
                self.config.metric,
                newest.kind_family.primary_group(),
                &self.releases,
                None,
            ),
        })
    }

    /// `None` when the line didn't run the latest release, because a row from an older release
    /// could show a build from before the latest release.
    fn since_release(
        &self,
        latest_release: Option<&str>,
        thresholds: &Thresholds,
    ) -> Option<SinceRelease> {
        let (release, _) = self.newest_release()?;
        if latest_release != Some(release.version) {
            return None;
        }
        // Every version after the line's newest release is a build, so the line's last version is
        // the newest build, unless it's the release itself.
        let build = self
            .all
            .versions
            .last()
            .filter(|build| build.version != release.version)?;
        let newest_build = *build.runs.last()?;
        let metric = self.config.metric;
        let step = VersionStep::between(release, build);
        Some(SinceRelease {
            title: self.config.title.clone(),
            suite: self.config.suite.clone(),
            metric,
            release: release.version.to_owned(),
            build: build.version.to_owned(),
            build_order: build.runs.first()?.version_order.clone(),
            release_value: release.median(metric)?,
            build_value: build.median(metric)?,
            change: change(metric, newest_build, &step, thresholds)?,
        })
    }

    /// The line's newest release version and that version's newest run.
    fn newest_release(&self) -> Option<(&LineVersion<'a>, &'a BenchmarkSummary)> {
        let version = self.releases.versions.last()?;
        Some((version, *version.runs.last()?))
    }
}

/// The newest release run in the shared version order. Builds never count, release candidates
/// included.
fn newest_release<'a>(
    runs: impl IntoIterator<Item = &'a BenchmarkSummary>,
) -> Option<&'a BenchmarkSummary> {
    runs.into_iter()
        .filter(|summary| summary.release)
        .max_by(|left, right| left.version_order.cmp(&right.version_order))
}

/// Every row of a suite compares against the same latest release. The link takes the newest build
/// among the rows, so it opens on the newest numbers.
fn release_links(rows: &[SinceRelease], suites: &[SuiteConfig]) -> Vec<ReleaseLink> {
    let mut newest: Vec<&SinceRelease> = Vec::new();
    for row in rows {
        match newest.iter_mut().find(|known| known.suite == row.suite) {
            Some(known) if row.build_order > known.build_order => *known = row,
            Some(_) => {}
            None => newest.push(row),
        }
    }
    newest
        .into_iter()
        .map(|row| ReleaseLink {
            title: suites
                .iter()
                .find(|suite| suite.name == row.suite)
                .map_or_else(|| row.suite.clone(), |suite| suite.title.clone()),
            query: ReleasesQuery {
                suite: Some(row.suite.clone()),
                base: Some(row.release.clone()),
                head: Some(row.build.clone()),
            },
        })
        .collect()
}

fn shared_machine(cards: &[HeadlineCard]) -> Option<String> {
    let mut machines = cards
        .iter()
        .filter_map(|card| card.release.as_ref())
        .map(|release| release.machine.as_str());
    let first = machines.next()?;
    machines
        .all(|machine| machine == first)
        .then(|| first.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        CANDIDATE, COMMIT, MACHINE, OLD_RELEASE, RELEASE, THRESHOLDS, producer, rate_limited, run,
        with_numbers,
    };
    use bench_dashboard_shared::delta::DeltaVerdict;
    use bench_report::params::BenchmarkParams;

    const SUITE: &str = "i4i-single";
    const OTHER_SUITE: &str = "i4i-cluster3";
    const OTHER_SUITE_TITLE: &str = "AWS i4i.4xlarge, 3 nodes";
    const WORKLOAD: &str = "producer-20p-persisted@800MB";
    const OTHER_WORKLOAD: &str = "end-to-end-20@1600MB";
    const TITLE: &str = "Durable write latency";
    const OTHER_TITLE: &str = "End-to-end latency";
    const OTHER_MACHINE: &str = "aws_i4i_4xlarge";
    const THROUGHPUT: f64 = 800.0;
    const COMMIT_DATE: &str = "2026-11-20T12:00:00Z";
    const LATER_PRE_RELEASE: &str = "server-0.11.0-rc.1";
    const OLD_PRE_RELEASE: &str = "server-0.9.0-rc.1";
    const NEWER_RELEASE: &str = "server-0.12.0";

    fn labeled(workload: &str) -> BenchmarkParams {
        let mut params = rate_limited(producer());
        params.suite = Some(SUITE.to_owned());
        params.workload = Some(workload.to_owned());
        params
    }

    /// An official run of [`WORKLOAD`] with this P99.
    fn official(id: u128, gitref: &str, p99: f64) -> BenchmarkSummary {
        with_numbers(run(id, labeled(WORKLOAD), MACHINE, gitref), THROUGHPUT, p99)
    }

    fn config(headlines: &[(&str, &str)]) -> DashboardConfig {
        DashboardConfig {
            suites: vec![SuiteConfig {
                name: SUITE.to_owned(),
                title: "AWS i4i.4xlarge, 1 node".to_owned(),
            }],
            headlines: headlines
                .iter()
                .map(|(workload, title)| HeadlineConfig {
                    suite: SUITE.to_owned(),
                    workload: (*workload).to_owned(),
                    title: (*title).to_owned(),
                    metric: Metric::P99,
                })
                .collect(),
            thresholds: THRESHOLDS,
        }
    }

    fn headlines(runs: &[BenchmarkSummary]) -> Headlines {
        Headlines::new(runs, &config(&[(WORKLOAD, TITLE)]))
    }

    #[test]
    fn given_releases_and_builds_when_picking_latest_release_should_take_highest_release_version() {
        let runs = vec![
            official(1, RELEASE, 2.0),
            official(5, OLD_RELEASE, 2.0),
            official(6, CANDIDATE, 2.0),
            official(7, LATER_PRE_RELEASE, 2.0),
            official(8, COMMIT, 2.0),
            official(9, NEWER_RELEASE, 2.0),
        ];

        assert_eq!(
            newest_release(&runs[..5]).map(|summary| summary.version.as_str()),
            Some("0.9.0"),
            "0.8.0 ran later, and the candidates and the commit build are no releases"
        );
        assert_eq!(headlines(&runs).latest_release.as_deref(), Some("0.12.0"));
        assert_eq!(headlines(&runs[2..5]).latest_release, None);
    }

    #[test]
    fn given_unofficial_release_when_picking_latest_release_should_ignore_it() {
        let mut unofficial = official(2, NEWER_RELEASE, 2.0);
        unofficial.suite = None;

        let headlines = headlines(&[official(1, RELEASE, 2.0), unofficial]);

        assert_eq!(headlines.latest_release.as_deref(), Some("0.9.0"));
    }

    #[test]
    fn given_several_runs_of_release_when_carded_should_show_their_median_and_change() {
        let runs = vec![
            official(1, OLD_RELEASE, 4.0),
            official(2, RELEASE, 1.6),
            official(3, RELEASE, 2.14),
            official(4, RELEASE, 1.68),
        ];

        let headlines = headlines(&runs);

        let [card] = headlines.cards.as_slice() else {
            panic!("one headline gives one card");
        };
        assert_eq!(card.title, TITLE);
        assert_eq!(card.headline.metric, Metric::P99);
        assert_eq!(card.headline.value, Some(1.68));
        let change = card.headline.change.as_ref().expect("0.8.0 came before");
        assert_eq!(change.verdict, DeltaVerdict::Better);
        assert_eq!(change.text(), "-58.0% vs 0.8.0");
        let release = card.release.as_ref().expect("0.9.0 ran the workload");
        assert_eq!(release.version, "0.9.0");
        assert_eq!(release.runs, 3);
        assert_eq!(
            release.newest,
            Uuid::from_u128(4),
            "the newest run of 0.9.0"
        );
        assert_eq!(release.run_title, "Pinned producer, 1 producer");
        assert_eq!(headlines.machine.as_deref(), Some(MACHINE));
    }

    #[test]
    fn given_workload_without_run_of_latest_release_when_carded_should_fall_back_to_older_release()
    {
        let runs = vec![
            official(1, OLD_RELEASE, 2.0),
            with_numbers(
                run(2, labeled(OTHER_WORKLOAD), MACHINE, RELEASE),
                THROUGHPUT,
                3.0,
            ),
        ];

        let headlines = Headlines::new(
            &runs,
            &config(&[(WORKLOAD, TITLE), (OTHER_WORKLOAD, OTHER_TITLE)]),
        );

        assert_eq!(headlines.latest_release.as_deref(), Some("0.9.0"));
        let versions: Vec<_> = headlines
            .cards
            .iter()
            .map(|card| {
                card.release
                    .as_ref()
                    .map(|release| release.version.as_str())
            })
            .collect();
        assert_eq!(versions, [Some("0.8.0"), Some("0.9.0")]);
        assert_eq!(headlines.cards[0].headline.value, Some(2.0));
        assert_eq!(
            headlines.cards[0].headline.change, None,
            "no release before 0.8.0"
        );
    }

    #[test]
    fn given_workload_without_release_run_when_carded_should_hold_no_value() {
        let runs = vec![official(1, CANDIDATE, 2.0)];

        let headlines = headlines(&runs);

        let [card] = headlines.cards.as_slice() else {
            panic!("one headline gives one card");
        };
        assert_eq!(card.release, None);
        assert_eq!(card.headline.value, None);
        assert_eq!(card.headline.change, None);
        assert_eq!(headlines.machine, None);
        assert!(headlines.trends.is_empty());
        assert!(headlines.since_release.is_empty());
    }

    #[test]
    fn given_cards_on_two_machines_when_built_should_name_no_machine() {
        let runs = vec![
            official(1, RELEASE, 2.0),
            with_numbers(
                run(2, labeled(OTHER_WORKLOAD), OTHER_MACHINE, RELEASE),
                THROUGHPUT,
                3.0,
            ),
        ];

        let headlines = Headlines::new(
            &runs,
            &config(&[(WORKLOAD, TITLE), (OTHER_WORKLOAD, OTHER_TITLE)]),
        );

        assert_eq!(headlines.machine, None);
    }

    #[test]
    fn given_runs_on_another_line_when_carded_should_keep_line_of_newest_release() {
        let mut moved = official(2, RELEASE, 9.0);
        moved.machine = OTHER_MACHINE.to_owned();
        let runs = vec![official(1, OLD_RELEASE, 2.0), moved];

        let headlines = headlines(&runs);

        let card = &headlines.cards[0];
        assert_eq!(
            card.release
                .as_ref()
                .map(|release| release.version.as_str()),
            Some("0.9.0")
        );
        assert_eq!(card.headline.change, None, "0.8.0 ran on another machine");
        assert!(headlines.trends.is_empty());
    }

    #[test]
    fn given_one_release_when_trending_should_hide_chart() {
        let runs = vec![official(1, RELEASE, 2.0), official(2, CANDIDATE, 1.0)];

        assert!(headlines(&runs).trends.is_empty());
    }

    #[test]
    fn given_two_releases_when_trending_should_chart_releases_only_in_version_order() {
        let runs = vec![
            official(1, RELEASE, 2.0),
            official(2, OLD_RELEASE, 3.0),
            official(3, RELEASE, 1.0),
            official(4, CANDIDATE, 0.5),
        ];

        let headlines = headlines(&runs);

        let [trend] = headlines.trends.as_slice() else {
            panic!("two releases give one trend");
        };
        assert_eq!(trend.title, TITLE);
        assert_eq!(trend.uuid, Uuid::from_u128(3));
        assert_eq!(trend.chart.metric, Metric::P99);
        assert_eq!(trend.chart.versions, ["0.8.0", "0.9.0"]);
        let medians: Vec<_> = trend
            .chart
            .medians
            .iter()
            .map(|point| (point.version, point.value))
            .collect();
        assert_eq!(medians, [(0, 3.0), (1, 1.5)]);
        assert_eq!(trend.chart.runs.len(), 3, "every release run, no candidate");
        assert_eq!(trend.chart.current, None);
    }

    #[test]
    fn given_builds_after_release_when_compared_should_take_newest_build() {
        let commit_build = |id| {
            let mut params = labeled(WORKLOAD);
            params.gitref_date = Some(COMMIT_DATE.to_owned());
            with_numbers(run(id, params, MACHINE, COMMIT), THROUGHPUT, 1.0)
        };
        let runs = vec![
            official(1, OLD_PRE_RELEASE, 1.0),
            official(2, RELEASE, 2.0),
            official(3, RELEASE, 3.0),
            commit_build(4),
            commit_build(5),
            official(6, CANDIDATE, 3.0),
        ];

        let headlines = headlines(&runs);

        let [row] = headlines.since_release.as_slice() else {
            panic!("a build after the release gives one row");
        };
        assert_eq!(row.title, TITLE);
        assert_eq!(row.metric, Metric::P99);
        assert_eq!(row.release, "0.9.0");
        assert_eq!(
            row.build, "0.10.0 at 91e020636",
            "the commit build of 0.10.0 comes after its candidate"
        );
        assert_eq!(row.release_value, 2.5);
        assert_eq!(row.build_value, 1.0);
        assert_eq!(row.change.verdict, DeltaVerdict::Better);
        assert_eq!(row.change.text(), "-60.0% vs 0.9.0");
    }

    #[test]
    fn given_card_of_older_release_when_compared_should_hide_its_row() {
        let other = |id, gitref| {
            with_numbers(
                run(id, labeled(OTHER_WORKLOAD), MACHINE, gitref),
                THROUGHPUT,
                3.0,
            )
        };
        let runs = vec![
            official(1, OLD_RELEASE, 2.0),
            official(2, OLD_PRE_RELEASE, 1.0),
            other(3, RELEASE),
            other(4, CANDIDATE),
        ];

        let headlines = Headlines::new(
            &runs,
            &config(&[(WORKLOAD, TITLE), (OTHER_WORKLOAD, OTHER_TITLE)]),
        );

        let rows: Vec<_> = headlines
            .since_release
            .iter()
            .map(|row| (row.title.as_str(), row.release.as_str(), row.build.as_str()))
            .collect();
        assert_eq!(
            rows,
            [(OTHER_TITLE, "0.9.0", "0.10.0-rc.1")],
            "0.9.0-rc.1 ran after the 0.8.0 of its card, but before the latest release"
        );
    }

    #[test]
    fn given_rows_of_two_suites_when_linked_should_open_each_suite_on_its_newest_build() {
        let in_suite = |id, suite: &str, workload, gitref| {
            let mut params = labeled(workload);
            params.suite = Some(suite.to_owned());
            with_numbers(run(id, params, MACHINE, gitref), THROUGHPUT, 2.0)
        };
        let runs = vec![
            in_suite(1, SUITE, WORKLOAD, RELEASE),
            in_suite(2, SUITE, WORKLOAD, CANDIDATE),
            in_suite(3, SUITE, OTHER_WORKLOAD, RELEASE),
            in_suite(4, SUITE, OTHER_WORKLOAD, LATER_PRE_RELEASE),
            in_suite(5, OTHER_SUITE, WORKLOAD, RELEASE),
            in_suite(6, OTHER_SUITE, WORKLOAD, CANDIDATE),
        ];
        let mut config = config(&[(WORKLOAD, TITLE), (OTHER_WORKLOAD, OTHER_TITLE)]);
        config.suites.push(SuiteConfig {
            name: OTHER_SUITE.to_owned(),
            title: OTHER_SUITE_TITLE.to_owned(),
        });
        config.headlines.push(HeadlineConfig {
            suite: OTHER_SUITE.to_owned(),
            workload: WORKLOAD.to_owned(),
            title: TITLE.to_owned(),
            metric: Metric::P99,
        });

        let headlines = Headlines::new(&runs, &config);

        let link = |title: &str, suite: &str, head: &str| ReleaseLink {
            title: title.to_owned(),
            query: ReleasesQuery {
                suite: Some(suite.to_owned()),
                base: Some("0.9.0".to_owned()),
                head: Some(head.to_owned()),
            },
        };
        assert_eq!(headlines.since_release.len(), 3);
        assert_eq!(
            headlines.release_links,
            [
                link("AWS i4i.4xlarge, 1 node", SUITE, "0.11.0-rc.1"),
                link(OTHER_SUITE_TITLE, OTHER_SUITE, "0.10.0-rc.1"),
            ],
            "one link per suite, in the order of the rows, on the newest build of its rows"
        );
    }

    #[test]
    fn given_no_build_after_release_when_compared_should_hide_row() {
        let runs = vec![
            official(1, OLD_PRE_RELEASE, 1.0),
            official(2, OLD_RELEASE, 2.0),
            official(3, RELEASE, 2.0),
        ];

        assert!(headlines(&runs).since_release.is_empty());
    }
}
