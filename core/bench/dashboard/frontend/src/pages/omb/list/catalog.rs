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

//! The loaded OMB runs as rows of the run list, and the search, facets and sort over them. The rows
//! and their text are built once per load, so a view change only filters and sorts.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use bench_dashboard_shared::format::group_digits;
use bench_dashboard_shared::identity::VersionOrder;
use bench_dashboard_shared::labels::{RELEASE_LABEL, count_text, messages_text, milliseconds};
use bench_dashboard_shared::omb::id::OmbRunId;
use bench_dashboard_shared::omb::labels::{
    environment_text, mebibytes_text, topology_text, version_text,
};
use bench_dashboard_shared::omb::result::OmbPercentiles;
use bench_dashboard_shared::omb::summary::{OmbBadge, OmbRunSummary, OmbRunTime};
use bench_dashboard_shared::omb::workload::OmbWorkloadKey;

use super::query::{Facet, OmbQuery, Sort};

pub struct OmbCatalog {
    rows: Vec<Rc<OmbRunRow>>,
    /// The values of each facet over all loaded runs, in display order. Indexed by facet.
    options: [Vec<FacetOption>; Facet::COUNT],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FacetOption {
    /// As written in the URL.
    pub value: String,
    pub label: String,
}

/// One run as a table row. The workload name stands for the workload, because a name usually
/// spells out the topics, actors, message size and rate. The facets and the run page give each
/// value apart.
#[derive(Debug)]
pub struct OmbRunRow {
    pub id: OmbRunId,
    /// The `name` from the workload file.
    pub workload_name: String,
    /// For example `Iggy-persisted`.
    pub driver: String,
    /// The machines from the setup sidecar, for example `AWS i4i.4xlarge, 3 nodes`. `None`
    /// without them.
    pub environment: Option<String>,
    /// The Iggy version from the setup sidecar. `None` without one.
    pub version: Option<RowVersion>,
    pub time: OmbRunTime,
    pub badges: Vec<OmbBadge>,
    pub numbers: RowNumbers,
    workload: OmbWorkloadKey,
    /// The driver, workload name, file name, version, environment, suite and the property keys
    /// and values, in lowercase.
    search_text: String,
    /// Where each facet's value sits in that facet's options. `None` where the facet doesn't apply.
    /// Indexed by facet.
    values: [Option<usize>; Facet::COUNT],
    publish_mebibytes_per_second: f64,
    publish_p99_ms: f64,
    /// `None` without end-to-end numbers, so those runs sort last by end-to-end P99.
    end_to_end_p99_ms: Option<f64>,
}

/// The Iggy version of a run, as the iggy-bench rows show it: a short tag, and a detail under it.
#[derive(Debug, PartialEq, Eq)]
pub struct RowVersion {
    /// The version without its commit or build metadata, which `detail` holds.
    pub text: String,
    /// The commit, the build metadata, for example `main.91e020636`, or `release`. `None` for
    /// another version.
    pub detail: Option<String>,
    pub release: bool,
}

/// A run's headline numbers.
#[derive(Debug, PartialEq, Eq)]
pub struct RowNumbers {
    /// For example `143 MiB/s`.
    pub publish: String,
    /// For example `150.2k msg/s`.
    pub messages: String,
    pub publish_latency: Latency,
    /// `None` if the run didn't measure end-to-end latency, for example without consumers.
    pub end_to_end_latency: Option<Latency>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Latency {
    pub p99: String,
    pub p50: String,
}

/// What the page shows for one view of the URL.
#[derive(Debug)]
pub struct OmbListView {
    /// The URL's view, minus the facet values that no loaded run has.
    pub query: OmbQuery,
    /// The rows that pass every filter, in sort order.
    pub rows: Vec<Rc<OmbRunRow>>,
    /// Indexed by facet. `None` for a facet that no loaded run has a value of, such as the suite
    /// when no run has a setup sidecar, so the page leaves it out.
    pub facets: [Option<FacetCounts>; Facet::COUNT],
    pub stats: ViewStats,
}

/// One facet's options with their run counts. A facet counts the runs that pass every other filter,
/// so a count says how many runs the option would show.
#[derive(Debug)]
pub struct FacetCounts {
    /// The runs with any value of this facet.
    pub any: usize,
    /// The options that have runs, and the selected option even without runs.
    pub options: Vec<(FacetOption, usize)>,
}

/// The listed runs in numbers.
#[derive(Debug, PartialEq, Eq)]
pub struct ViewStats {
    pub runs: usize,
    pub drivers: usize,
    /// Distinct workload keys, so two workload files with the same name and other fields count
    /// twice.
    pub workloads: usize,
    /// The time of the newest listed run. `None` without runs.
    pub newest: Option<OmbRunTime>,
}

/// Runs per facet option, and per facet across all its options.
struct Tally {
    any: [usize; Facet::COUNT],
    per_option: [Vec<usize>; Facet::COUNT],
}

impl OmbCatalog {
    pub fn new(runs: &[OmbRunSummary]) -> Self {
        let options = Facet::ALL.map(|facet| facet_options(facet, runs));
        let rows = {
            let positions = options.each_ref().map(|options| {
                options
                    .iter()
                    .enumerate()
                    .map(|(position, option)| (option.value.as_str(), position))
                    .collect::<HashMap<_, _>>()
            });
            runs.iter()
                .map(|summary| {
                    let values = Facet::ALL.map(|facet| {
                        let option = facet_option(facet, summary)?;
                        positions[facet.index()].get(option.value.as_str()).copied()
                    });
                    Rc::new(OmbRunRow::new(summary, values))
                })
                .collect()
        };
        Self { rows, options }
    }

    pub fn view(&self, query: &OmbQuery) -> OmbListView {
        let selected = Facet::ALL.map(|facet| {
            let value = query.facet(facet)?;
            self.options[facet.index()]
                .iter()
                .position(|option| option.value == value)
        });
        let words: Vec<_> = query
            .search
            .split_whitespace()
            .map(str::to_lowercase)
            .collect();
        let mut tally = Tally::new(&self.options);
        let mut rows = Vec::new();
        for row in &self.rows {
            if !row.matches_words(&words) {
                continue;
            }
            let mut missed = Facet::ALL
                .into_iter()
                .filter(|facet| !row.matches(*facet, &selected));
            match (missed.next(), missed.next()) {
                (None, _) => {
                    for facet in Facet::ALL {
                        tally.add(facet, row);
                    }
                    rows.push(Rc::clone(row));
                }
                (Some(facet), None) => tally.add(facet, row),
                _ => {}
            }
        }
        rows.sort_by(|left, right| row_order(query.sort, left, right));

        let mut known = query.clone();
        for facet in Facet::ALL {
            let value = selected[facet.index()]
                .map(|position| self.options[facet.index()][position].value.clone());
            known.set_facet(facet, value);
        }
        OmbListView {
            query: known,
            facets: Facet::ALL.map(|facet| {
                let options = &self.options[facet.index()];
                (!options.is_empty()).then(|| tally.counts(facet, options, selected[facet.index()]))
            }),
            stats: ViewStats::new(&rows),
            rows,
        }
    }
}

impl OmbRunRow {
    fn new(summary: &OmbRunSummary, values: [Option<usize>; Facet::COUNT]) -> Self {
        let workload = &summary.workload;
        Self {
            id: summary.id,
            workload_name: workload.name.clone(),
            driver: summary.driver.clone(),
            environment: summary.environment().and_then(environment_text),
            version: RowVersion::new(summary),
            time: summary.time.clone(),
            badges: summary.badges.clone(),
            numbers: RowNumbers::new(summary),
            workload: workload.clone(),
            search_text: search_text(summary),
            values,
            publish_mebibytes_per_second: summary.publish_mebibytes_per_second,
            publish_p99_ms: summary.publish_latency_ms.p99,
            end_to_end_p99_ms: summary.end_to_end_latency_ms.map(|latency| latency.p99),
        }
    }

    fn matches(&self, facet: Facet, selected: &[Option<usize>; Facet::COUNT]) -> bool {
        selected[facet.index()].is_none_or(|position| self.values[facet.index()] == Some(position))
    }

    fn matches_words(&self, words: &[String]) -> bool {
        words
            .iter()
            .all(|word| self.search_text.contains(word.as_str()))
    }
}

impl RowVersion {
    fn new(summary: &OmbRunSummary) -> Option<Self> {
        let version = summary.version.as_ref()?;
        let text = version_text(summary)?;
        // Build metadata names a branch and a commit, so it goes under the tag as a commit does,
        // and the tag stays short enough for its column. The parsed version says that the text
        // after the first `+` is build metadata.
        let has_build = summary
            .version_order
            .version
            .as_ref()
            .is_some_and(|semver| !semver.build.is_empty());
        let (text, build) = match text.split_once('+') {
            Some((bare, build)) if has_build => (bare.to_owned(), Some(build.to_owned())),
            _ => (text, None),
        };
        Some(Self {
            text,
            detail: version
                .commit
                .clone()
                .or(build)
                .or_else(|| version.release.then(|| RELEASE_LABEL.to_owned())),
            release: version.release,
        })
    }
}

impl RowNumbers {
    fn new(summary: &OmbRunSummary) -> Self {
        Self {
            publish: mebibytes_text(summary.publish_mebibytes_per_second),
            messages: messages_text(summary.publish_messages_per_second),
            publish_latency: Latency::from(&summary.publish_latency_ms),
            end_to_end_latency: summary.end_to_end_latency_ms.as_ref().map(Latency::from),
        }
    }
}

impl From<&OmbPercentiles> for Latency {
    fn from(latency: &OmbPercentiles) -> Self {
        Self {
            p99: milliseconds(latency.p99),
            p50: milliseconds(latency.p50),
        }
    }
}

impl ViewStats {
    fn new(rows: &[Rc<OmbRunRow>]) -> Self {
        Self {
            runs: rows.len(),
            drivers: rows
                .iter()
                .map(|row| row.driver.as_str())
                .collect::<HashSet<_>>()
                .len(),
            workloads: rows
                .iter()
                .map(|row| &row.workload)
                .collect::<HashSet<_>>()
                .len(),
            newest: rows
                .iter()
                .map(|row| &row.time)
                .max_by_key(|time| time.instant)
                .cloned(),
        }
    }
}

impl Tally {
    fn new(options: &[Vec<FacetOption>; Facet::COUNT]) -> Self {
        Self {
            any: [0; Facet::COUNT],
            per_option: options.each_ref().map(|options| vec![0; options.len()]),
        }
    }

    fn add(&mut self, facet: Facet, row: &OmbRunRow) {
        let index = facet.index();
        self.any[index] += 1;
        if let Some(position) = row.values[index] {
            self.per_option[index][position] += 1;
        }
    }

    fn counts(
        &self,
        facet: Facet,
        options: &[FacetOption],
        selected: Option<usize>,
    ) -> FacetCounts {
        let index = facet.index();
        FacetCounts {
            any: self.any[index],
            options: options
                .iter()
                .zip(&self.per_option[index])
                .enumerate()
                .filter(|(position, (_, count))| **count > 0 || selected == Some(*position))
                .map(|(_, (option, count))| (option.clone(), *count))
                .collect(),
        }
    }
}

/// The order of `sort`, then the newest run first, then the run id, so equal values still have one
/// order.
fn row_order(sort: Sort, left: &OmbRunRow, right: &OmbRunRow) -> Ordering {
    let by_value = match sort {
        Sort::Newest => Ordering::Equal,
        Sort::PublishThroughput => right
            .publish_mebibytes_per_second
            .total_cmp(&left.publish_mebibytes_per_second),
        Sort::PublishP99 => left.publish_p99_ms.total_cmp(&right.publish_p99_ms),
        Sort::EndToEndP99 => match (left.end_to_end_p99_ms, right.end_to_end_p99_ms) {
            (Some(left), Some(right)) => left.total_cmp(&right),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        },
    };
    by_value
        .then_with(|| right.time.instant.cmp(&left.time.instant))
        .then_with(|| left.id.cmp(&right.id))
}

/// The values of `facet` over `runs`, once each, in display order.
fn facet_options(facet: Facet, runs: &[OmbRunSummary]) -> Vec<FacetOption> {
    let mut seen = HashSet::new();
    let mut first_runs: Vec<_> = runs
        .iter()
        .filter_map(|summary| facet_option(facet, summary).map(|option| (summary, option)))
        .filter(|(_, option)| seen.insert(option.value.clone()))
        .collect();
    first_runs.sort_by(|left, right| facet_order(facet, left, right));
    first_runs.into_iter().map(|(_, option)| option).collect()
}

/// The value of `summary` for `facet`, and its label. `None` where the facet doesn't apply, such as
/// the suite of a run without a setup sidecar.
fn facet_option(facet: Facet, summary: &OmbRunSummary) -> Option<FacetOption> {
    let workload = &summary.workload;
    let (value, label) = match facet {
        Facet::Driver => (summary.driver.clone(), summary.driver.clone()),
        Facet::Workload => (workload.name.clone(), workload.name.clone()),
        Facet::MessageSize => (
            workload.message_size.to_string(),
            format!("{} B", group_digits(workload.message_size)),
        ),
        Facet::Topology => (
            format!("{}x{}", workload.topics, workload.partitions),
            topology_text(workload),
        ),
        Facet::Actors => (
            format!(
                "{}p{}c",
                workload.producers_per_topic, workload.consumers_per_topic
            ),
            actors_label(workload),
        ),
        Facet::Environment => {
            let text = environment_text(summary.environment()?)?;
            (text.clone(), text)
        }
        Facet::Version => {
            let text = summary.version.as_ref()?.text.clone();
            (text.clone(), text)
        }
        Facet::Suite => {
            let suite = summary.setup.as_ref()?.suite.clone()?;
            (suite.clone(), suite)
        }
    };
    (!value.is_empty()).then_some(FacetOption { value, label })
}

/// Versions newest first, numbers in ascending order, texts in alphabetical order whatever their
/// case.
fn facet_order(
    facet: Facet,
    (left_run, left_option): &(&OmbRunSummary, FacetOption),
    (right_run, right_option): &(&OmbRunSummary, FacetOption),
) -> Ordering {
    let (left, right) = (&left_run.workload, &right_run.workload);
    match facet {
        Facet::Version => newer_version_first(&left_run.version_order, &right_run.version_order)
            .then_with(|| left_option.label.cmp(&right_option.label)),
        Facet::MessageSize => left.message_size.cmp(&right.message_size),
        Facet::Topology => (left.topics, left.partitions).cmp(&(right.topics, right.partitions)),
        Facet::Actors => (left.producers_per_topic, left.consumers_per_topic)
            .cmp(&(right.producers_per_topic, right.consumers_per_topic)),
        Facet::Driver | Facet::Workload | Facet::Environment | Facet::Suite => {
            let (left, right) = (&left_option.label, &right_option.label);
            left.to_lowercase()
                .cmp(&right.to_lowercase())
                .then_with(|| left.cmp(right))
        }
    }
}

/// The newer version first, and a version that doesn't parse last. Build metadata has no order,
/// so two builds of one version are ordered by their commit dates, then by their run times.
fn newer_version_first(left: &VersionOrder, right: &VersionOrder) -> Ordering {
    let precedence = match (&right.version, &left.version) {
        (Some(right), Some(left)) => right.cmp_precedence(left),
        (right, left) => right.is_some().cmp(&left.is_some()),
    };
    precedence
        .then_with(|| right.gitref_date_us.cmp(&left.gitref_date_us))
        .then_with(|| right.timestamp_us.cmp(&left.timestamp_us))
}

/// The actors under the facet's label, which already says "per topic", for example
/// `1 producer, 1 consumer`.
fn actors_label(workload: &OmbWorkloadKey) -> String {
    format!(
        "{}, {}",
        count_text(workload.producers_per_topic.into(), "producer", "producers"),
        count_text(workload.consumers_per_topic.into(), "consumer", "consumers")
    )
}

fn search_text(summary: &OmbRunSummary) -> String {
    let version = summary.version.as_ref();
    let environment = summary.environment();
    let setup = summary.setup.as_ref();
    [
        Some(summary.driver.clone()),
        Some(summary.workload.name.clone()),
        Some(summary.file_name.clone()),
        version.map(|version| version.text.clone()),
        version.and_then(|version| version.commit.clone()),
        environment.and_then(environment_text),
        environment.and_then(|environment| environment_text(&environment.without_label())),
        setup.and_then(|setup| setup.suite.clone()),
    ]
    .into_iter()
    .flatten()
    .chain(setup.into_iter().flat_map(|setup| {
        setup
            .properties()
            .flat_map(|(_, property)| [property.key.clone(), property.value.text.clone()])
    }))
    .collect::<Vec<_>>()
    .join(" ")
    .to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        OMB_EDGE8_100_PARTITIONS, OMB_EDGE8_DEFAULT, OMB_NO_CONSUMERS, OMB_PUBLISH_ERRORS,
        OMB_RC1_DEFAULT, OMB_RC1_PERSISTED, OmbFixture, omb_summaries,
    };

    const RC1_WORKLOAD: &str = "12 topics x 1 partition / 1p 1c per topic / 1000 B / 150 MB/s";
    /// Every fixture, newest first.
    const NEWEST_FIRST: [&OmbFixture; 6] = [
        &OMB_RC1_PERSISTED,
        &OMB_RC1_DEFAULT,
        &OMB_EDGE8_DEFAULT,
        &OMB_EDGE8_100_PARTITIONS,
        &OMB_NO_CONSUMERS,
        &OMB_PUBLISH_ERRORS,
    ];
    /// The runs whose setup sidecar names the same environment and suite.
    const I4I_RUNS: [&OmbFixture; 3] = [&OMB_RC1_PERSISTED, &OMB_RC1_DEFAULT, &OMB_EDGE8_DEFAULT];

    fn view(query: &OmbQuery) -> OmbListView {
        OmbCatalog::new(&omb_summaries()).view(query)
    }

    /// The view of a link with these parameters.
    fn link(pairs: &[(&str, &str)]) -> OmbQuery {
        OmbQuery::from_pairs(pairs.iter().copied())
    }

    fn ids(view: &OmbListView) -> Vec<OmbRunId> {
        view.rows.iter().map(|row| row.id).collect()
    }

    fn fixture_ids(fixtures: &[&OmbFixture]) -> Vec<OmbRunId> {
        fixtures.iter().map(|fixture| fixture.id()).collect()
    }

    fn row(fixture: &OmbFixture) -> OmbRunRow {
        OmbRunRow::new(&fixture.summary(), [None; Facet::COUNT])
    }

    fn facet(view: &OmbListView, facet: Facet) -> &FacetCounts {
        view.facets[facet.index()]
            .as_ref()
            .expect("a loaded run has a value of the facet")
    }

    fn counts(view: &OmbListView, of: Facet) -> Vec<(&str, usize)> {
        facet(view, of)
            .options
            .iter()
            .map(|(option, count)| (option.value.as_str(), *count))
            .collect()
    }

    fn labels(view: &OmbListView, of: Facet) -> Vec<(&str, &str)> {
        facet(view, of)
            .options
            .iter()
            .map(|(option, _)| (option.value.as_str(), option.label.as_str()))
            .collect()
    }

    #[test]
    fn given_run_with_sidecar_when_listed_should_give_text_of_each_column() {
        let shown = row(&OMB_RC1_DEFAULT);

        assert_eq!(shown.workload_name, RC1_WORKLOAD);
        assert_eq!(shown.driver, "Iggy");
        assert_eq!(
            shown.environment.as_deref(),
            Some("AWS i4i.4xlarge, 3 nodes")
        );
        assert_eq!(
            shown.version,
            Some(RowVersion {
                text: "0.10.0-rc.1".to_owned(),
                detail: None,
                release: false,
            })
        );
        assert_eq!(shown.time.text, "2026-11-06 13:22:01 UTC");
        assert_eq!(
            shown.numbers,
            RowNumbers {
                publish: "143 MiB/s".to_owned(),
                messages: "150.2k msg/s".to_owned(),
                publish_latency: Latency {
                    p99: "1.52 ms".to_owned(),
                    p50: "0.947 ms".to_owned(),
                },
                end_to_end_latency: Some(Latency {
                    p99: "2.00 ms".to_owned(),
                    p50: "1.00 ms".to_owned(),
                }),
            }
        );
        assert!(shown.badges.is_empty());
    }

    #[test]
    fn given_run_without_consumers_when_listed_should_have_no_end_to_end_numbers() {
        let shown = row(&OMB_NO_CONSUMERS);

        assert_eq!(shown.numbers.end_to_end_latency, None);
        assert_eq!(shown.version, None);
        assert_eq!(shown.environment, None);
        assert_eq!(shown.time.text, "2026-10-22 10:21:15");
    }

    #[test]
    fn given_version_with_build_metadata_when_listed_should_put_build_under_tag() {
        let text = "0.9.1-edge.1+main.91e020636";
        let mut summary = OMB_EDGE8_DEFAULT.summary();
        summary.version_order.version = Some(text.parse().expect("the version parses"));
        if let Some(version) = summary.version.as_mut() {
            version.text = text.to_owned();
        }

        let shown = OmbRunRow::new(&summary, [None; Facet::COUNT]);

        assert_eq!(
            shown.version,
            Some(RowVersion {
                text: "0.9.1-edge.1".to_owned(),
                detail: Some("main.91e020636".to_owned()),
                release: false,
            })
        );
    }

    #[test]
    fn given_version_text_with_plus_that_does_not_parse_when_listed_should_keep_whole_text() {
        let mut summary = OMB_EDGE8_DEFAULT.summary();
        summary.version_order.version = None;
        if let Some(version) = summary.version.as_mut() {
            version.text = "master+local".to_owned();
        }

        let shown = OmbRunRow::new(&summary, [None; Facet::COUNT]);

        assert_eq!(
            shown.version.map(|version| (version.text, version.detail)),
            Some(("master+local".to_owned(), None))
        );
    }

    #[test]
    fn given_run_with_publish_errors_when_listed_should_carry_its_badge() {
        assert_eq!(row(&OMB_PUBLISH_ERRORS).badges, [OmbBadge::PublishErrors]);
    }

    #[test]
    fn given_each_facet_when_filtered_should_list_only_runs_with_its_value() {
        let cases: [(&str, &str, &[&OmbFixture]); 13] = [
            (
                "driver",
                "Iggy-persisted",
                &[&OMB_RC1_PERSISTED, &OMB_PUBLISH_ERRORS],
            ),
            ("workload", RC1_WORKLOAD, &I4I_RUNS),
            ("size", "1024", &[&OMB_EDGE8_100_PARTITIONS]),
            (
                "size",
                "1000",
                &[
                    &OMB_RC1_PERSISTED,
                    &OMB_RC1_DEFAULT,
                    &OMB_EDGE8_DEFAULT,
                    &OMB_NO_CONSUMERS,
                    &OMB_PUBLISH_ERRORS,
                ],
            ),
            ("topology", "1x100", &[&OMB_EDGE8_100_PARTITIONS]),
            ("actors", "1p0c", &[&OMB_NO_CONSUMERS]),
            ("actors", "4p4c", &[&OMB_EDGE8_100_PARTITIONS]),
            ("environment", "AWS i4i.4xlarge, 3 nodes", &I4I_RUNS),
            (
                "environment",
                "AWS i7i.xlarge, 3 nodes",
                &[&OMB_EDGE8_100_PARTITIONS],
            ),
            (
                "version",
                "0.10.0-rc.1",
                &[&OMB_RC1_PERSISTED, &OMB_RC1_DEFAULT],
            ),
            (
                "version",
                "0.9.1-edge.8",
                &[&OMB_EDGE8_DEFAULT, &OMB_EDGE8_100_PARTITIONS],
            ),
            ("suite", "cluster-3-nodes", &I4I_RUNS),
            ("suite", "cluster-3-nodes-i7i", &[&OMB_EDGE8_100_PARTITIONS]),
        ];

        for (param, value, expected) in cases {
            let view = view(&link(&[(param, value)]));

            assert_eq!(ids(&view), fixture_ids(expected), "{param}={value}");
            assert_eq!(view.query.to_pairs(), [(param, value.to_owned())]);
        }
    }

    #[test]
    fn given_active_filters_when_counting_a_facet_should_apply_every_other_filter() {
        let view = view(&link(&[("driver", "Iggy"), ("suite", "cluster-3-nodes")]));

        assert_eq!(
            ids(&view),
            fixture_ids(&[&OMB_RC1_DEFAULT, &OMB_EDGE8_DEFAULT])
        );
        assert_eq!(
            counts(&view, Facet::Driver),
            [("Iggy", 2), ("Iggy-persisted", 1)]
        );
        assert_eq!(facet(&view, Facet::Driver).any, 3);
        assert_eq!(
            counts(&view, Facet::Suite),
            [("cluster-3-nodes", 2), ("cluster-3-nodes-i7i", 1)]
        );
        assert_eq!(facet(&view, Facet::Suite).any, 4);
        assert_eq!(counts(&view, Facet::Topology), [("12x1", 2)]);
    }

    #[test]
    fn given_fixtures_when_facets_listed_should_order_and_label_each_value() {
        let view = view(&OmbQuery::default());

        assert_eq!(
            labels(&view, Facet::Driver),
            [("Iggy", "Iggy"), ("Iggy-persisted", "Iggy-persisted")]
        );
        assert_eq!(
            labels(&view, Facet::MessageSize),
            [("1000", "1,000 B"), ("1024", "1,024 B")]
        );
        assert_eq!(
            labels(&view, Facet::Topology),
            [
                ("1x100", "1 topic × 100 partitions"),
                ("12x1", "12 topics × 1 partition"),
            ]
        );
        assert_eq!(
            labels(&view, Facet::Actors),
            [
                ("1p0c", "1 producer, 0 consumers"),
                ("1p1c", "1 producer, 1 consumer"),
                ("4p4c", "4 producers, 4 consumers"),
            ]
        );
        assert_eq!(
            counts(&view, Facet::Environment),
            [
                ("AWS i4i.4xlarge, 3 nodes", 3),
                ("AWS i7i.xlarge, 3 nodes", 1)
            ]
        );
        assert_eq!(facet(&view, Facet::Environment).any, 6);
        assert_eq!(
            counts(&view, Facet::Version),
            [("0.10.0-rc.1", 2), ("0.9.1-edge.8", 2)]
        );
        assert_eq!(facet(&view, Facet::Version).any, 6);
        assert_eq!(
            counts(&view, Facet::Workload),
            [
                ("1 topic x 100 partitions / 4p 4c / 1 KiB / 124.9 MB/s", 1),
                (
                    "12 topics x 1 partition / 1p 0c per topic / 1000 B / 75 MB/s",
                    1
                ),
                (RC1_WORKLOAD, 3),
                (
                    "12 topics x 1 partition / 1p 1c per topic / 1000 B / 75 MB/s",
                    1
                ),
            ]
        );
    }

    #[test]
    fn given_builds_of_one_version_when_listed_should_order_them_by_time_not_by_build_text() {
        let build = |fixture: &OmbFixture, text: &str, timestamp_us: i64| {
            let mut summary = fixture.summary();
            summary.version_order.version = Some(text.parse().expect("the version parses"));
            summary.version_order.timestamp_us = Some(timestamp_us);
            if let Some(version) = summary.version.as_mut() {
                version.text = text.to_owned();
            }
            summary
        };
        let mut unparsed = OMB_RC1_PERSISTED.summary();
        unparsed.version_order.version = None;
        if let Some(version) = unparsed.version.as_mut() {
            version.text = "master-91e0206".to_owned();
        }
        let runs = [
            build(&OMB_EDGE8_DEFAULT, "0.9.1-edge.1+a.0123abcd", 2),
            build(&OMB_EDGE8_100_PARTITIONS, "0.9.1-edge.1+b.4567cdef", 1),
            unparsed,
            OMB_RC1_DEFAULT.summary(),
        ];

        let view = OmbCatalog::new(&runs).view(&OmbQuery::default());

        assert_eq!(
            labels(&view, Facet::Version)
                .into_iter()
                .map(|(value, _)| value)
                .collect::<Vec<_>>(),
            [
                "0.10.0-rc.1",
                "0.9.1-edge.1+a.0123abcd",
                "0.9.1-edge.1+b.4567cdef",
                "master-91e0206",
            ]
        );
    }

    #[test]
    fn given_link_values_that_no_run_has_when_viewed_should_ignore_them() {
        let view = view(&link(&[
            ("driver", "Kafka"),
            ("topology", "12 topics"),
            ("suite", "nowhere"),
        ]));

        assert_eq!(ids(&view), fixture_ids(&NEWEST_FIRST));
        assert_eq!(view.query, OmbQuery::default());
    }

    #[test]
    fn given_selected_value_without_runs_when_counted_should_keep_it_with_zero() {
        let view = view(&link(&[("driver", "Iggy-persisted"), ("actors", "4p4c")]));

        assert!(view.rows.is_empty());
        assert_eq!(view.query.facet(Facet::Actors), Some("4p4c"));
        assert_eq!(counts(&view, Facet::Actors), [("1p1c", 2), ("4p4c", 0)]);
    }

    #[test]
    fn given_runs_without_sidecar_when_viewed_should_leave_sidecar_facets_out() {
        let runs = [OMB_NO_CONSUMERS.summary(), OMB_PUBLISH_ERRORS.summary()];

        let view = OmbCatalog::new(&runs).view(&link(&[("suite", "cluster-3-nodes")]));

        assert!(view.facets[Facet::Environment.index()].is_none());
        assert!(view.facets[Facet::Version.index()].is_none());
        assert!(view.facets[Facet::Suite.index()].is_none());
        assert_eq!(view.query, OmbQuery::default());
        assert_eq!(
            counts(&view, Facet::Driver),
            [("Iggy", 1), ("Iggy-persisted", 1)]
        );
    }

    #[test]
    fn given_search_words_when_viewed_should_match_each_word_in_any_label() {
        let search = |text: &str| ids(&view(&link(&[("q", text)])));

        assert_eq!(
            search("PERSISTED"),
            fixture_ids(&[&OMB_RC1_PERSISTED, &OMB_PUBLISH_ERRORS])
        );
        assert_eq!(search("4p 4c"), fixture_ids(&[&OMB_EDGE8_100_PARTITIONS]));
        assert_eq!(search("2026-10-22"), fixture_ids(&[&OMB_NO_CONSUMERS]));
        assert_eq!(
            search("rc.1"),
            fixture_ids(&[&OMB_RC1_PERSISTED, &OMB_RC1_DEFAULT])
        );
        assert_eq!(
            search("edge.8"),
            fixture_ids(&[&OMB_EDGE8_DEFAULT, &OMB_EDGE8_100_PARTITIONS])
        );
        assert_eq!(search("i7i"), fixture_ids(&[&OMB_EDGE8_100_PARTITIONS]));
        assert_eq!(
            search("2 client hosts"),
            fixture_ids(&[
                &OMB_RC1_PERSISTED,
                &OMB_RC1_DEFAULT,
                &OMB_EDGE8_DEFAULT,
                &OMB_EDGE8_100_PARTITIONS,
            ])
        );
        assert_eq!(
            search("cluster-3-nodes-i7i"),
            fixture_ids(&[&OMB_EDGE8_100_PARTITIONS])
        );
        assert_eq!(
            search("  iggy   75 "),
            fixture_ids(&[&OMB_NO_CONSUMERS, &OMB_PUBLISH_ERRORS])
        );
        assert_eq!(search("iggy nowhere"), Vec::<OmbRunId>::new());
    }

    #[test]
    fn given_property_key_or_value_when_searched_should_match_runs_with_that_property() {
        let search = |text: &str| ids(&view(&link(&[("q", text)])));
        let with_sidecar = fixture_ids(&[
            &OMB_RC1_PERSISTED,
            &OMB_RC1_DEFAULT,
            &OMB_EDGE8_DEFAULT,
            &OMB_EDGE8_100_PARTITIONS,
        ]);

        assert_eq!(search("huge_pages"), with_sidecar);
        assert_eq!(search("1972"), fixture_ids(&[&OMB_EDGE8_100_PARTITIONS]));
        assert_eq!(
            search("data_file_system"),
            fixture_ids(&[&OMB_RC1_PERSISTED, &OMB_RC1_DEFAULT])
        );
        assert_eq!(search("ext4 persisted"), fixture_ids(&[&OMB_RC1_PERSISTED]));
        assert_eq!(search("IGGY_SHARDING_CPU_ALLOCATION=all"), with_sidecar);
        assert_eq!(search("vm.dirty_bytes 2147483648"), with_sidecar);
    }

    #[test]
    fn given_each_sort_when_viewed_should_order_rows_and_break_ties_by_newest() {
        let sorted = |sort: &str| ids(&view(&link(&[("sort", sort)])));

        assert_eq!(sorted("newest"), fixture_ids(&NEWEST_FIRST));
        assert_eq!(
            sorted("publish_throughput"),
            fixture_ids(&[
                &OMB_RC1_DEFAULT,
                &OMB_EDGE8_DEFAULT,
                &OMB_RC1_PERSISTED,
                &OMB_EDGE8_100_PARTITIONS,
                &OMB_NO_CONSUMERS,
                &OMB_PUBLISH_ERRORS,
            ])
        );
        assert_eq!(
            sorted("publish_p99"),
            fixture_ids(&[
                &OMB_NO_CONSUMERS,
                &OMB_EDGE8_DEFAULT,
                &OMB_RC1_DEFAULT,
                &OMB_EDGE8_100_PARTITIONS,
                &OMB_RC1_PERSISTED,
                &OMB_PUBLISH_ERRORS,
            ])
        );
        // Three runs have an end-to-end P99 of 2 ms, so the newest of them comes first.
        assert_eq!(
            sorted("end_to_end_p99"),
            fixture_ids(&[
                &OMB_RC1_DEFAULT,
                &OMB_EDGE8_DEFAULT,
                &OMB_EDGE8_100_PARTITIONS,
                &OMB_RC1_PERSISTED,
                &OMB_PUBLISH_ERRORS,
                &OMB_NO_CONSUMERS,
            ])
        );
    }

    #[test]
    fn given_runs_of_same_time_and_numbers_when_sorted_should_order_them_by_id() {
        let mut runs = omb_summaries();
        let copy_id: OmbRunId = "0000000000000001".parse().expect("the id parses");
        let mut copy = OMB_RC1_DEFAULT.summary();
        copy.id = copy_id;
        runs.push(copy);

        let view = OmbCatalog::new(&runs).view(&link(&[("sort", "publish_throughput")]));

        assert_eq!(ids(&view)[..2], [copy_id, OMB_RC1_DEFAULT.id()]);
    }

    #[test]
    fn given_view_when_summed_up_should_count_runs_drivers_workloads_and_newest_run() {
        let all = view(&OmbQuery::default());
        let persisted = view(&link(&[("driver", "Iggy-persisted")]));
        let none = view(&link(&[("q", "nowhere")]));

        assert_eq!(
            all.stats,
            ViewStats {
                runs: 6,
                drivers: 2,
                workloads: 4,
                newest: Some(OMB_RC1_PERSISTED.summary().time),
            }
        );
        assert_eq!(
            persisted.stats,
            ViewStats {
                runs: 2,
                drivers: 1,
                workloads: 2,
                newest: Some(OMB_RC1_PERSISTED.summary().time),
            }
        );
        assert_eq!(
            none.stats,
            ViewStats {
                runs: 0,
                drivers: 0,
                workloads: 0,
                newest: None,
            }
        );
    }
}
