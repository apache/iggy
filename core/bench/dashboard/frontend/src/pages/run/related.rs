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

//! The Related runs section: runs on the same machine and topology as a run, whose workload differs
//! from it in exactly one field, grouped by that field like a sweep.

use bench_dashboard_shared::delta::Metric;
use bench_dashboard_shared::identity::WorkloadField;
use bench_dashboard_shared::summary::BenchmarkSummary;
use yew::prelude::*;

use super::change::metric_name;
use super::facts::{FieldValue, field_value};
use super::history::{NO_VALUE, VERSION_HEADER, link_cell, link_header, main_value_text};
use crate::components::run_labels::{NO_RATE_LIMIT_NOTE, version_tag, version_tag_view};

const NO_RELATED_NOTE: &str = "No run on this machine and topology differs from this run in \
                               exactly one parameter.";
const VALUE_SEPARATOR: &str = ", ";

#[derive(Debug, PartialEq)]
pub struct RelatedGroup {
    pub field: WorkloadField,
    /// One per value of the field, in value order. The page's own run holds its own value.
    pub entries: Vec<RelatedEntry>,
}

#[derive(Debug, PartialEq)]
pub struct RelatedEntry {
    pub value: FieldValue,
    /// The newest run with the value, by version order.
    pub summary: BenchmarkSummary,
    /// The page's own run.
    pub current: bool,
}

/// The groups, in workload key field order.
pub fn related_runs(run: &BenchmarkSummary, summaries: &[BenchmarkSummary]) -> Vec<RelatedGroup> {
    let mut groups: Vec<RelatedGroup> = Vec::new();
    let same_environment = summaries
        .iter()
        .filter(|summary| summary.machine == run.machine && summary.nodes == run.nodes);
    for candidate in same_environment {
        let &[field] = run
            .workload_key
            .differences(&candidate.workload_key)
            .as_slice()
        else {
            continue;
        };
        if !groups.iter().any(|group| group.field == field) {
            groups.push(RelatedGroup {
                field,
                entries: vec![RelatedEntry {
                    value: field_value(field, &run.workload_key),
                    summary: run.clone(),
                    current: true,
                }],
            });
        }
        let Some(group) = groups.iter_mut().find(|group| group.field == field) else {
            continue;
        };
        let value = field_value(field, &candidate.workload_key);
        match group.entries.iter_mut().find(|entry| entry.value == value) {
            Some(entry) => {
                if !entry.current && is_newer(candidate, &entry.summary) {
                    entry.summary = candidate.clone();
                }
            }
            None => group.entries.push(RelatedEntry {
                value,
                summary: candidate.clone(),
                current: false,
            }),
        }
    }
    for group in &mut groups {
        group
            .entries
            .sort_by(|left, right| left.value.cmp(&right.value));
    }
    groups.sort_by_key(|group| group.field);
    groups
}

/// One card per field, or a note when no run is one field apart from `run`.
pub fn related_view(groups: &[RelatedGroup], run: &BenchmarkSummary) -> Html {
    if groups.is_empty() {
        return html! { <p class="section-text">{ NO_RELATED_NOTE }</p> };
    }
    html! {
        <div class="run-related">
            { for groups.iter().map(|group| group_view(group, run)) }
        </div>
    }
}

fn group_view(group: &RelatedGroup, run: &BenchmarkSummary) -> Html {
    let latency = group
        .entries
        .iter()
        .any(|entry| entry.summary.workload_key.rate_limit.is_some());
    let values: Vec<_> = group
        .entries
        .iter()
        .map(|entry| entry.value.text.as_str())
        .collect();
    html! {
        <article class="card run-table-card">
            <h3>{ format!("{}: {}", group.field, values.join(VALUE_SEPARATOR)) }</h3>
            <table class="run-table">
                <thead>
                    <tr>
                        <th scope="col">{ group.field.to_string() }</th>
                        <th scope="col">{ VERSION_HEADER }</th>
                        <th scope="col" class="run-table-number">
                            { metric_name(Metric::Throughput) }
                        </th>
                        if latency {
                            <th scope="col" class="run-table-number">
                                { metric_name(Metric::P99) }
                            </th>
                        }
                        { link_header() }
                    </tr>
                </thead>
                <tbody>
                    { for group.entries.iter().map(|entry| entry_view(entry, latency, run)) }
                </tbody>
            </table>
        </article>
    }
}

/// `latency` says whether the table has a P99 column, which a run without a rate limit leaves
/// empty. `run` is the page's own run. On a phone, the first cell labels the row, and every other
/// cell names its column.
fn entry_view(entry: &RelatedEntry, latency: bool, run: &BenchmarkSummary) -> Html {
    let summary = &entry.summary;
    let rate_limited = summary.workload_key.rate_limit.is_some();
    let p99 = metric_name(Metric::P99);
    html! {
        <tr
            class={classes!(entry.current.then_some("run-table-current"))}
            aria-current={entry.current.then_some("page")}
        >
            <td>{ entry.value.text.as_str() }</td>
            <td data-label={VERSION_HEADER}>
                { version_tag_view(&version_tag(summary), summary.release) }
            </td>
            <td class="run-table-number" data-label={metric_name(Metric::Throughput)}>
                { main_value_text(summary, Metric::Throughput) }
            </td>
            if latency && rate_limited {
                <td class="run-table-number" data-label={p99}>
                    { main_value_text(summary, Metric::P99) }
                </td>
            } else if latency {
                <td class="run-table-number" data-label={p99} title={NO_RATE_LIMIT_NOTE}>
                    { NO_VALUE }
                </td>
            }
            { link_cell(summary, run) }
        </tr>
    }
}

/// Version order, and the higher id for runs that started at the same moment.
fn is_newer(candidate: &BenchmarkSummary, known: &BenchmarkSummary) -> bool {
    (&candidate.version_order, candidate.uuid) > (&known.version_order, known.uuid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{CANDIDATE, MACHINE, RELEASE, consumer, producer, rate_limited, run};
    use bench_report::numeric_parameter::BenchmarkNumericParameter;
    use bench_report::params::BenchmarkParams;
    use uuid::Uuid;

    const OTHER_MACHINE: &str = "iggy-ci";

    /// An entry's value text, run id and current mark.
    type EntryShape<'a> = (&'a str, u128, bool);

    fn producers(count: u32) -> BenchmarkParams {
        let mut params = rate_limited(producer());
        params.producers = count;
        params
    }

    fn related_of(id: u128, runs: &[BenchmarkSummary]) -> Vec<RelatedGroup> {
        let run = runs
            .iter()
            .find(|summary| summary.uuid == Uuid::from_u128(id))
            .expect("the run is in the list");
        related_runs(run, runs)
    }

    fn shape(groups: &[RelatedGroup]) -> Vec<(WorkloadField, Vec<EntryShape<'_>>)> {
        groups
            .iter()
            .map(|group| {
                let entries = group
                    .entries
                    .iter()
                    .map(|entry| {
                        (
                            entry.value.text.as_str(),
                            entry.summary.uuid.as_u128(),
                            entry.current,
                        )
                    })
                    .collect();
                (group.field, entries)
            })
            .collect()
    }

    #[test]
    fn given_runs_apart_in_one_field_when_related_should_group_them_by_that_field() {
        let mut uncapped = producers(20);
        uncapped.rate_limit = None;
        let mut small_messages = producers(200);
        small_messages.message_size = BenchmarkNumericParameter::Value(100);
        let runs = vec![
            run(1, producers(20), MACHINE, RELEASE),
            run(2, producers(200), MACHINE, RELEASE),
            run(3, producers(1), MACHINE, RELEASE),
            run(4, uncapped, MACHINE, RELEASE),
            run(5, small_messages, MACHINE, RELEASE),
            run(6, producers(20), MACHINE, CANDIDATE),
            run(7, rate_limited(consumer()), MACHINE, RELEASE),
        ];

        let groups = related_of(1, &runs);

        assert_eq!(
            shape(&groups),
            [
                (
                    WorkloadField::Producers,
                    vec![("1", 3, false), ("20", 1, true), ("200", 2, false)]
                ),
                (
                    WorkloadField::RateLimit,
                    vec![("none", 4, false), ("800 MB/s", 1, true)]
                ),
            ],
            "runs two fields apart, of the same workload or of another kind are left out"
        );
    }

    #[test]
    fn given_several_runs_with_one_value_when_related_should_show_newest_in_version_order() {
        let runs = vec![
            run(1, producers(20), MACHINE, RELEASE),
            run(2, producers(200), MACHINE, CANDIDATE),
            run(3, producers(200), MACHINE, RELEASE),
            run(4, producers(200), MACHINE, RELEASE),
        ];

        let groups = related_of(1, &runs);

        assert_eq!(
            shape(&groups),
            [(
                WorkloadField::Producers,
                vec![("20", 1, true), ("200", 2, false)]
            )],
            "the candidate is newer than the 0.9.0 runs that started later"
        );
    }

    #[test]
    fn given_runs_on_other_machine_or_topology_when_related_should_leave_them_out() {
        let mut three_nodes = run(3, producers(200), MACHINE, RELEASE);
        three_nodes.nodes = 3;
        let runs = vec![
            run(1, producers(20), MACHINE, RELEASE),
            run(2, producers(200), OTHER_MACHINE, RELEASE),
            three_nodes,
        ];

        assert_eq!(shape(&related_of(1, &runs)), []);
    }
}
