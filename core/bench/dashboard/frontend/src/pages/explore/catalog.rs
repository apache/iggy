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

//! The loaded runs as rows of the Explore table, and the scope, search, facets and sort over them.
//! The rows and their text are built once per load, so a view change only filters and sorts.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use bench_dashboard_shared::config::DashboardConfig;
use bench_dashboard_shared::identity::{KindFamily, VersionOrder};
use bench_dashboard_shared::metrics::{Badge, GroupMetrics};
use bench_dashboard_shared::summary::BenchmarkSummary;
use bench_report::hardware::BenchmarkHardware;
use bench_report::options::BenchmarkDurability;
use uuid::Uuid;

use super::query::{ExploreQuery, Facet, Sort};
use crate::api::api_name;
use crate::components::run_labels::{
    CONDITION_SEPARATOR, CpuName, DayLabel, MachineName, batch_size_text, data_volume_text,
    durability_text, logical_cpus_text, memory_text, message_size_text, messages_text,
    milliseconds, nodes_text, numeric_text, rate_limit_text, topic_durability, version_tag,
};
use crate::format::format_throughput_mb_s;

pub struct Catalog {
    rows: Vec<Rc<RunRow>>,
    /// The values of each facet over all loaded runs, in display order. Indexed by facet.
    options: [Vec<FacetOption>; Facet::COUNT],
}

#[derive(Debug, Clone)]
pub struct FacetOption {
    /// As written in the URL.
    pub value: String,
    pub label: String,
}

/// One run as a table row.
#[derive(Debug)]
pub struct RunRow {
    pub uuid: Uuid,
    pub family: KindFamily,
    /// For example `Pinned producer, 20 producers`.
    pub title: String,
    /// Message size, batch size, rate limit, durability and data volume.
    pub conditions: String,
    /// For example `AWS i4i.4xlarge`.
    pub machine: String,
    /// `None` when the report doesn't name the CPU.
    pub cpu: Option<RowCpu>,
    /// The logical CPUs and memory of the machine that ran the bench, for example
    /// `12 logical CPUs · 62.7 GiB`. `None` when an older report has neither.
    pub specs: Option<String>,
    /// For example `3 nodes`.
    pub topology: String,
    /// The version without the commit, which `commit` holds.
    pub version_tag: String,
    pub commit: Option<String>,
    pub release: bool,
    /// `None` when the report has no numbers for the run's main group.
    pub numbers: Option<RunNumbers>,
    pub badges: Vec<Badge>,
    pub start: Option<DayLabel>,
    version: String,
    version_order: VersionOrder,
    official: bool,
    /// The title, version, commit, suite and workload labels and the CPU, in lowercase.
    search_text: String,
    /// Where each facet's value sits in that facet's options. `None` where the facet doesn't apply.
    /// Indexed by facet.
    values: [Option<usize>; Facet::COUNT],
    throughput_mb_s: Option<f64>,
    /// `None` without a rate limit, so those runs sort last by P99.
    p99_ms: Option<f64>,
}

/// The CPU of the machine that ran the bench.
#[derive(Debug, PartialEq, Eq)]
pub struct RowCpu {
    /// For example `AMD Ryzen 5 3600`.
    pub name: String,
    /// As the report writes it, for example `AMD Ryzen 5 3600 6-Core Processor`.
    pub brand: Rc<str>,
}

/// A run's main group numbers.
#[derive(Debug, PartialEq, Eq)]
pub struct RunNumbers {
    /// For example `2.07 GB/s`.
    pub throughput: String,
    /// For example `2.07M msg/s`.
    pub messages: String,
    /// `None` without a rate limit, because such a run measures throughput only.
    pub latency: Option<Latency>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Latency {
    pub p99: String,
    pub p50: String,
}

/// What the page shows for one view of the URL.
#[derive(Debug)]
pub struct ExploreView {
    /// The URL's view, minus the facet values that no loaded run has.
    pub query: ExploreQuery,
    /// The rows that pass every filter, in sort order.
    pub rows: Vec<Rc<RunRow>>,
    /// Indexed by facet.
    pub facets: [FacetCounts; Facet::COUNT],
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
    pub machines: usize,
    pub versions: usize,
    pub latest_release: Option<String>,
}

/// Runs per facet option, and per facet across all its options.
struct Tally {
    any: [usize; Facet::COUNT],
    per_option: [Vec<usize>; Facet::COUNT],
}

impl Catalog {
    pub fn new(summaries: &[BenchmarkSummary], config: &DashboardConfig) -> Self {
        let options = Facet::ALL.map(|facet| facet_options(facet, summaries));
        let rows = {
            let positions = options.each_ref().map(|options| {
                options
                    .iter()
                    .enumerate()
                    .map(|(position, option)| (option.value.as_str(), position))
                    .collect::<HashMap<_, _>>()
            });
            summaries
                .iter()
                .map(|summary| {
                    let values = Facet::ALL.map(|facet| {
                        let option = facet_option(facet, summary)?;
                        positions[facet.index()].get(option.value.as_str()).copied()
                    });
                    let official = config.is_official(summary.suite.as_deref());
                    Rc::new(RunRow::new(summary, official, values))
                })
                .collect()
        };
        Self { rows, options }
    }

    pub fn view(&self, query: &ExploreQuery) -> ExploreView {
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
            if !(query.scope.includes(row.official) && row.matches_words(&words)) {
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
        ExploreView {
            query: known,
            facets: Facet::ALL.map(|facet| {
                tally.counts(facet, &self.options[facet.index()], selected[facet.index()])
            }),
            stats: ViewStats::new(&rows),
            rows,
        }
    }
}

impl RunRow {
    fn new(
        summary: &BenchmarkSummary,
        official: bool,
        values: [Option<usize>; Facet::COUNT],
    ) -> Self {
        let main = summary.group(summary.kind_family.primary_group());
        let rate_limited = summary.workload_key.rate_limit.is_some();
        Self {
            uuid: summary.uuid,
            family: summary.kind_family,
            title: summary.title.clone(),
            conditions: conditions(summary),
            machine: MachineName(&summary.machine).to_string(),
            cpu: CpuName::of(&summary.hardware).map(|cpu| RowCpu {
                name: cpu.to_string(),
                brand: Rc::from(cpu.brand()),
            }),
            specs: specs_text(&summary.hardware),
            topology: nodes_text(summary.nodes),
            version_tag: version_tag(summary),
            commit: summary.commit.clone(),
            release: summary.release,
            numbers: main.map(|group| RunNumbers::new(group, rate_limited)),
            badges: summary.badges.clone(),
            start: summary
                .version_order
                .timestamp_us
                .and_then(DayLabel::from_unix_micros),
            version: summary.version.clone(),
            version_order: summary.version_order.clone(),
            official,
            search_text: search_text(summary),
            values,
            throughput_mb_s: main.map(|group| group.throughput_megabytes_per_second),
            p99_ms: main
                .filter(|_| rate_limited)
                .map(|group| group.p99_latency_ms),
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

impl RunNumbers {
    fn new(group: &GroupMetrics, rate_limited: bool) -> Self {
        Self {
            throughput: format_throughput_mb_s(group.throughput_megabytes_per_second),
            messages: messages_text(group.throughput_messages_per_second),
            latency: rate_limited.then(|| Latency {
                p99: milliseconds(group.p99_latency_ms),
                p50: milliseconds(group.p50_latency_ms),
            }),
        }
    }
}

impl ViewStats {
    fn new(rows: &[Rc<RunRow>]) -> Self {
        let distinct = |facet: Facet| {
            rows.iter()
                .filter_map(|row| row.values[facet.index()])
                .collect::<HashSet<_>>()
                .len()
        };
        Self {
            runs: rows.len(),
            machines: distinct(Facet::Machine),
            versions: distinct(Facet::Version),
            latest_release: rows
                .iter()
                .filter(|row| row.release)
                .max_by(|left, right| left.version_order.cmp(&right.version_order))
                .map(|row| row.version.clone()),
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

    fn add(&mut self, facet: Facet, row: &RunRow) {
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
fn row_order(sort: Sort, left: &RunRow, right: &RunRow) -> Ordering {
    let by_value = match sort {
        Sort::Recent => Ordering::Equal,
        Sort::Throughput => missing_last(left.throughput_mb_s, right.throughput_mb_s, |l, r| {
            r.total_cmp(&l)
        }),
        Sort::P99 => missing_last(left.p99_ms, right.p99_ms, |l, r| l.total_cmp(&r)),
    };
    by_value
        .then_with(|| {
            right
                .version_order
                .timestamp_us
                .cmp(&left.version_order.timestamp_us)
        })
        .then_with(|| left.uuid.cmp(&right.uuid))
}

/// Orders two values with `order`, and puts a missing value after every present one.
fn missing_last(
    left: Option<f64>,
    right: Option<f64>,
    order: impl FnOnce(f64, f64) -> Ordering,
) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => order(left, right),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

/// The values of `facet` over `summaries`, once each, in display order.
fn facet_options(facet: Facet, summaries: &[BenchmarkSummary]) -> Vec<FacetOption> {
    let mut seen = HashSet::new();
    let mut first_runs: Vec<_> = summaries
        .iter()
        .filter_map(|summary| facet_option(facet, summary).map(|option| (summary, option)))
        .filter(|(_, option)| seen.insert(option.value.clone()))
        .collect();
    first_runs.sort_by(|(left, _), (right, _)| facet_order(facet, left, right));
    first_runs.into_iter().map(|(_, option)| option).collect()
}

/// The value of `summary` for `facet`, and its label. `None` where the facet doesn't apply.
fn facet_option(facet: Facet, summary: &BenchmarkSummary) -> Option<FacetOption> {
    let (value, label) = match facet {
        Facet::Machine => (
            summary.machine.clone(),
            MachineName(&summary.machine).to_string(),
        ),
        Facet::Version => (summary.version.clone(), summary.version.clone()),
        Facet::Kind => (
            api_name(summary.kind_family)?,
            summary.kind_family.to_string(),
        ),
        Facet::Topology => (summary.nodes.to_string(), nodes_text(summary.nodes)),
        Facet::Durability => {
            let durability = topic_durability(summary)?;
            (
                api_name(durability)?,
                durability_text(durability).to_owned(),
            )
        }
        Facet::MessageSize => {
            let size = summary.workload_key.message_size;
            (size.to_string(), format!("{} B", numeric_text(size)))
        }
    };
    (!value.is_empty()).then_some(FacetOption { value, label })
}

/// Versions newest first, the values of every other facet in ascending order.
fn facet_order(facet: Facet, left: &BenchmarkSummary, right: &BenchmarkSummary) -> Ordering {
    match facet {
        Facet::Machine => left
            .machine
            .to_lowercase()
            .cmp(&right.machine.to_lowercase()),
        Facet::Version => right.version_order.cmp(&left.version_order),
        Facet::Kind => left.kind_family.cmp(&right.kind_family),
        Facet::Topology => left.nodes.cmp(&right.nodes),
        Facet::Durability => {
            let persisted = |run: &BenchmarkSummary| {
                topic_durability(run) == Some(BenchmarkDurability::Persisted)
            };
            persisted(left).cmp(&persisted(right))
        }
        Facet::MessageSize => {
            let range = |run: &BenchmarkSummary| {
                let size = run.workload_key.message_size;
                (size.min(), size.max())
            };
            range(left).cmp(&range(right))
        }
    }
}

fn conditions(summary: &BenchmarkSummary) -> String {
    let key = &summary.workload_key;
    let mut parts = vec![
        message_size_text(key.message_size),
        batch_size_text(key.messages_per_batch),
        rate_limit_text(key.rate_limit.as_deref()),
    ];
    parts
        .extend(topic_durability(summary).map(|durability| durability_text(durability).to_owned()));
    parts.push(data_volume_text(key.data_volume));
    parts.join(CONDITION_SEPARATOR)
}

fn specs_text(hardware: &BenchmarkHardware) -> Option<String> {
    let specs: Vec<_> = [
        logical_cpus_text(hardware.cpu_cores),
        memory_text(hardware.total_memory_mb),
    ]
    .into_iter()
    .flatten()
    .collect();
    (!specs.is_empty()).then(|| specs.join(CONDITION_SEPARATOR))
}

fn search_text(summary: &BenchmarkSummary) -> String {
    [
        Some(summary.title.as_str()),
        Some(summary.version.as_str()),
        summary.commit.as_deref(),
        summary.suite.as_deref(),
        summary.workload_key.workload.as_deref(),
        Some(summary.hardware.cpu_name.as_str()),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join(" ")
    .to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pages::explore::query::Scope;
    use crate::test_support::{
        CANDIDATE, COMMIT, OLD_RELEASE, RATE_LIMIT, RELEASE, THRESHOLDS, consumer, params,
        producer, rate_limited, run, summarize, with_numbers,
    };
    use bench_dashboard_shared::config::SuiteConfig;
    use bench_report::benchmark_kind::BenchmarkKind;
    use bench_report::numeric_parameter::BenchmarkNumericParameter;
    use bench_report::options::BenchmarkOptions;
    use bench_report::params::BenchmarkDataVolume;

    const OFFICIAL_SUITE: &str = "i4i-single";

    fn config() -> DashboardConfig {
        DashboardConfig {
            suites: vec![SuiteConfig {
                name: OFFICIAL_SUITE.to_owned(),
                title: "AWS i4i.4xlarge, 1 node".to_owned(),
            }],
            headlines: Vec::new(),
            thresholds: THRESHOLDS,
        }
    }

    fn row(summary: &BenchmarkSummary) -> RunRow {
        RunRow::new(summary, false, [None; Facet::COUNT])
    }

    fn view(summaries: &[BenchmarkSummary], query: &ExploreQuery) -> ExploreView {
        Catalog::new(summaries, &config()).view(query)
    }

    fn all_runs() -> ExploreQuery {
        ExploreQuery {
            scope: Scope::All,
            ..ExploreQuery::default()
        }
    }

    /// The view of a link with these parameters over all runs.
    fn link(pairs: &[(&str, &str)]) -> ExploreQuery {
        ExploreQuery::from_pairs(pairs.iter().copied().chain([("scope", "all")]))
    }

    fn ids(view: &ExploreView) -> Vec<String> {
        view.rows.iter().map(|row| row.uuid.to_string()).collect()
    }

    fn id(id: u128) -> String {
        Uuid::from_u128(id).to_string()
    }

    fn counts(view: &ExploreView, facet: Facet) -> Vec<(&str, usize)> {
        view.facets[facet.index()]
            .options
            .iter()
            .map(|(option, count)| (option.value.as_str(), *count))
            .collect()
    }

    /// Two machines, two versions and two kind families.
    fn grid() -> Vec<BenchmarkSummary> {
        vec![
            run(1, producer(), "atlas", RELEASE),
            run(2, consumer(), "atlas", CANDIDATE),
            run(3, producer(), "iggy-ci", RELEASE),
            run(4, consumer(), "iggy-ci", RELEASE),
        ]
    }

    #[test]
    fn given_run_of_each_family_when_listed_should_give_title_and_conditions() {
        let mut producer = params(BenchmarkKind::PinnedProducer, 20, 0);
        producer.messages_per_batch = BenchmarkNumericParameter::Value(250);
        producer.rate_limit = Some(RATE_LIMIT.to_owned());
        producer.data_volume = Some(BenchmarkDataVolume::MessageBatchesPerActor(60_000));
        let mut options = BenchmarkOptions::legacy(BenchmarkKind::PinnedProducer);
        options.topic.durability = BenchmarkDurability::Persisted;
        producer.options = Some(options);

        let mut consumer = params(BenchmarkKind::BalancedConsumerGroup, 0, 20);
        consumer.consumer_groups = 1;
        consumer.rate_limit = Some("62.50 MB".to_owned());
        consumer.data_volume = Some(BenchmarkDataVolume::MessageBatchesPerActor(1));
        let mut options = BenchmarkOptions::legacy(BenchmarkKind::BalancedConsumerGroup);
        options.topic.durability = BenchmarkDurability::Persisted;
        consumer.options = Some(options);

        let mut mixed = params(BenchmarkKind::PinnedProducerAndConsumer, 8, 8);
        mixed.message_size = BenchmarkNumericParameter::Range {
            min: 100,
            max: 1000,
        };
        mixed.message_batches = 248_001;

        let mut end_to_end = params(BenchmarkKind::EndToEndProducingConsumer, 20, 20);
        end_to_end.rate_limit = Some("1.60 GB".to_owned());
        end_to_end.data_volume = Some(BenchmarkDataVolume::TotalDataBytes(60_000_000_000));

        let rows: Vec<_> = [producer, consumer, mixed, end_to_end]
            .into_iter()
            .map(|params| row(&summarize(params)))
            .collect();
        let texts: Vec<_> = rows
            .iter()
            .map(|row| (row.title.as_str(), row.conditions.as_str()))
            .collect();

        assert_eq!(
            texts,
            [
                (
                    "Pinned producer, 20 producers",
                    "1,000 B messages · 250 per batch · rate limit 800 MB/s · persisted · \
                     60,000 batches per actor",
                ),
                (
                    "Balanced consumer group, 20 consumers, 1 consumer group",
                    "1,000 B messages · 1,000 per batch · rate limit 62.5 MB/s · 1 batch per \
                     actor",
                ),
                (
                    "Pinned producer and consumer, 8 producers, 8 consumers",
                    "100 to 1,000 B messages · 1,000 per batch · no rate limit · replicated · \
                     248,001 batches in total",
                ),
                (
                    "End-to-end producing consumer, 20 producing consumers",
                    "1,000 B messages · 1,000 per batch · rate limit 1.60 GB/s · replicated · \
                     60.00 GB in total",
                ),
            ]
        );
    }

    #[test]
    fn given_runs_with_and_without_rate_limit_when_listed_should_give_latency_with_limit_only() {
        let limited = with_numbers(
            run(1, rate_limited(producer()), "atlas", RELEASE),
            2066.4,
            1.284,
        );
        let unlimited = with_numbers(run(2, producer(), "atlas", RELEASE), 790.0, 9.0);

        let limited = row(&limited);
        let unlimited = row(&unlimited);

        assert_eq!(
            limited.numbers,
            Some(RunNumbers {
                throughput: "2.07 GB/s".to_owned(),
                messages: "2.07M msg/s".to_owned(),
                latency: Some(Latency {
                    p99: "1.28 ms".to_owned(),
                    p50: "0.642 ms".to_owned(),
                }),
            })
        );
        assert_eq!(
            unlimited.numbers,
            Some(RunNumbers {
                throughput: "790 MB/s".to_owned(),
                messages: "790.0k msg/s".to_owned(),
                latency: None,
            })
        );
        assert_eq!(
            limited.start,
            Some(DayLabel {
                day: "Nov 1, 2026".to_owned(),
                iso: "2026-11-01T09:30:00Z".to_owned(),
            })
        );
    }

    #[test]
    fn given_release_candidate_and_build_when_listed_should_show_version_apart_from_commit() {
        let versions: Vec<_> = [RELEASE, CANDIDATE, COMMIT]
            .into_iter()
            .map(|gitref| row(&run(1, producer(), "atlas", gitref)))
            .map(|row| (row.version_tag, row.commit, row.release))
            .collect();

        assert_eq!(
            versions,
            [
                ("0.9.0".to_owned(), None, true),
                ("0.10.0-rc.1".to_owned(), None, false),
                ("0.10.0".to_owned(), Some("91e020636".to_owned()), false),
            ]
        );
    }

    #[test]
    fn given_active_filters_when_counting_a_facet_should_apply_every_other_filter() {
        let runs = grid();

        let by_machine = view(&runs, &link(&[("machine", "atlas")]));
        let by_machine_and_kind = view(&runs, &link(&[("machine", "atlas"), ("kind", "producer")]));

        assert_eq!(ids(&by_machine), [id(2), id(1)]);
        assert_eq!(
            counts(&by_machine, Facet::Machine),
            [("atlas", 2), ("iggy-ci", 2)]
        );
        assert_eq!(
            counts(&by_machine, Facet::Kind),
            [("producer", 1), ("consumer", 1)]
        );
        assert_eq!(by_machine.facets[Facet::Kind.index()].any, 2);
        assert_eq!(
            counts(&by_machine, Facet::Version),
            [("0.10.0-rc.1", 1), ("0.9.0", 1)]
        );
        assert_eq!(ids(&by_machine_and_kind), [id(1)]);
        assert_eq!(
            counts(&by_machine_and_kind, Facet::Machine),
            [("atlas", 1), ("iggy-ci", 1)]
        );
        assert_eq!(counts(&by_machine_and_kind, Facet::Version), [("0.9.0", 1)]);
    }

    #[test]
    fn given_selected_values_without_runs_when_counted_should_keep_them_with_zero() {
        let runs = grid();

        let view = view(
            &runs,
            &link(&[("machine", "iggy-ci"), ("version", "0.10.0-rc.1")]),
        );

        assert!(view.rows.is_empty());
        assert_eq!(view.query.facet(Facet::Version), Some("0.10.0-rc.1"));
        assert_eq!(
            counts(&view, Facet::Version),
            [("0.10.0-rc.1", 0), ("0.9.0", 2)]
        );
        assert_eq!(
            counts(&view, Facet::Machine),
            [("atlas", 1), ("iggy-ci", 0)]
        );
    }

    #[test]
    fn given_link_values_that_no_run_has_when_viewed_should_ignore_them() {
        let runs = grid();

        let view = view(
            &runs,
            &link(&[("machine", "nowhere"), ("kind", "bogus"), ("nodes", "3")]),
        );

        assert_eq!(view.rows.len(), runs.len());
        assert_eq!(view.query, all_runs());
        assert_eq!(counts(&view, Facet::Topology), [("1", 4)]);
    }

    #[test]
    fn given_official_scope_when_viewed_should_list_and_count_official_runs_only() {
        let mut runs = grid();
        runs[0].suite = Some(OFFICIAL_SUITE.to_owned());
        runs[3].suite = Some(OFFICIAL_SUITE.to_owned());

        let view = view(&runs, &ExploreQuery::default());

        assert_eq!(ids(&view), [id(4), id(1)]);
        assert_eq!(
            counts(&view, Facet::Machine),
            [("atlas", 1), ("iggy-ci", 1)]
        );
        assert_eq!(counts(&view, Facet::Version), [("0.9.0", 2)]);
    }

    #[test]
    fn given_runs_that_did_not_create_topics_when_filtered_by_durability_should_leave_them_out() {
        let mut reused = producer();
        let mut options = BenchmarkOptions::legacy(BenchmarkKind::PinnedProducer);
        options.client.reuse_streams = true;
        reused.options = Some(options);
        let mut runs = grid();
        runs.push(run(5, reused, "atlas", RELEASE));

        let replicated = view(&runs, &link(&[("durability", "replicated")]));

        assert_eq!(
            runs[4].workload_key.topic.durability,
            BenchmarkDurability::Replicated
        );
        assert_eq!(
            row(&runs[4]).conditions,
            "1,000 B messages · 1,000 per batch · no rate limit · 0 batches in total"
        );
        assert_eq!(ids(&replicated), [id(3), id(1)]);
        assert_eq!(counts(&replicated, Facet::Durability), [("replicated", 2)]);
        assert_eq!(replicated.facets[Facet::Durability.index()].any, 5);
    }

    #[test]
    fn given_search_words_when_viewed_should_match_each_word_in_any_label() {
        let mut runs = grid();
        runs[0].suite = Some("nightly-i3en".to_owned());
        runs[1].workload_key.workload = Some("consumer-20c-cold@800MB".to_owned());
        runs[2].commit = Some("71f29618b".to_owned());
        runs[3].hardware.cpu_name = "AMD Ryzen 5 3600 6-Core Processor".to_owned();
        let search = |text: &str| ids(&view(&runs, &link(&[("q", text)])));

        assert_eq!(search("NIGHTLY-I3EN"), [id(1)]);
        assert_eq!(search("cold@800"), [id(2)]);
        assert_eq!(search("71f2"), [id(3)]);
        assert_eq!(search("ryzen 3600"), [id(4)]);
        assert_eq!(search("rc.1"), [id(2)]);
        assert_eq!(search("  pinned   0.9.0 "), [id(4), id(3), id(1)]);
        assert_eq!(search("pinned consumer 0.9.0"), [id(4)]);
        assert_eq!(search("pinned nowhere"), Vec::<String>::new());
    }

    #[test]
    fn given_bench_host_when_shown_as_row_should_name_cpu_and_list_its_counts() {
        let mut summary = run(1, producer(), "mxpc", RELEASE);
        summary.hardware.cpu_name = "AMD Ryzen 5 3600 6-Core Processor".to_owned();
        summary.hardware.cpu_cores = 12;
        summary.hardware.total_memory_mb = 64218;

        let shown = row(&summary);

        assert_eq!(shown.machine, "mxpc");
        assert_eq!(
            shown.cpu,
            Some(RowCpu {
                name: "AMD Ryzen 5 3600".to_owned(),
                brand: Rc::from("AMD Ryzen 5 3600 6-Core Processor"),
            })
        );
        assert_eq!(shown.specs.as_deref(), Some("12 logical CPUs · 62.7 GiB"));
        assert_eq!(shown.topology, "1 node");
    }

    #[test]
    fn given_report_without_hardware_when_shown_as_row_should_show_topology_only() {
        let shown = row(&run(1, producer(), "atlas", RELEASE));

        assert_eq!(shown.cpu, None);
        assert_eq!(shown.specs, None);
        assert_eq!(shown.topology, "1 node");
    }

    #[test]
    fn given_sorts_when_viewed_should_order_rows_and_break_ties_by_recency_then_id() {
        let runs = vec![
            with_numbers(
                run(1, rate_limited(producer()), "atlas", RELEASE),
                500.0,
                2.0,
            ),
            with_numbers(run(2, producer(), "atlas", RELEASE), 900.0, 0.5),
            with_numbers(
                run(3, rate_limited(producer()), "atlas", RELEASE),
                500.0,
                1.0,
            ),
            run(4, producer(), "atlas", RELEASE),
            with_numbers(
                run(5, rate_limited(producer()), "atlas", RELEASE),
                700.0,
                1.0,
            ),
        ];
        let mut same_start = runs[2].clone();
        same_start.uuid = Uuid::from_u128(0);
        let mut with_tie = runs.clone();
        with_tie.push(same_start);
        let sorted =
            |runs: &[BenchmarkSummary], sort: &str| ids(&view(runs, &link(&[("sort", sort)])));

        assert_eq!(sorted(&runs, "recent"), [id(5), id(4), id(3), id(2), id(1)]);
        assert_eq!(
            sorted(&runs, "throughput"),
            [id(2), id(5), id(3), id(1), id(4)]
        );
        assert_eq!(sorted(&runs, "p99"), [id(5), id(3), id(1), id(4), id(2)]);
        assert_eq!(
            sorted(&with_tie, "p99"),
            [id(5), id(0), id(3), id(1), id(4), id(2)]
        );
    }

    #[test]
    fn given_view_when_summed_up_should_count_runs_machines_versions_and_latest_release() {
        let runs = vec![
            run(1, producer(), "atlas", RELEASE),
            run(2, consumer(), "atlas", COMMIT),
            run(3, producer(), "iggy-ci", OLD_RELEASE),
            run(4, consumer(), "iggy-ci", CANDIDATE),
        ];

        let all = view(&runs, &all_runs());
        let none = view(&runs, &link(&[("q", "nowhere")]));

        assert_eq!(
            all.stats,
            ViewStats {
                runs: 4,
                machines: 2,
                versions: 4,
                latest_release: Some("0.9.0".to_owned()),
            }
        );
        assert_eq!(
            none.stats,
            ViewStats {
                runs: 0,
                machines: 0,
                versions: 0,
                latest_release: None,
            }
        );
    }
}
