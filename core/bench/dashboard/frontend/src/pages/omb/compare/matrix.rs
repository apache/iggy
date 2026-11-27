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

//! The numbers of the OMB compare page: each run's value and its change from the baseline. OMB runs
//! have no noise rule yet, so a change is a plain percent without a verdict.

use std::iter;

use bench_dashboard_shared::labels::{messages_text, milliseconds};
use bench_dashboard_shared::omb::chart::{PERCENTILE_LABELS, percentile_values};
use bench_dashboard_shared::omb::compare::OmbComparability;
use bench_dashboard_shared::omb::labels::{mebibytes_text, short_id};
use bench_dashboard_shared::omb::result::OmbPercentiles;
use bench_dashboard_shared::omb::summary::OmbRunSummary;

use crate::pages::omb::common::{NOT_MEASURED, change_text};
use crate::pages::run::change::percent_change;

const AVERAGE_LABEL: &str = "Avg";
const THROUGHPUT_LABEL: &str = "Throughput";
const CONSUME_LABEL: &str = "Consume rate";
const PUBLISH_P99_LABEL: &str = "Publish P99";
const END_TO_END_P99_LABEL: &str = "End-to-end P99";

/// One number of every run, the baseline first.
#[derive(Debug, Clone, PartialEq)]
pub struct MatrixRow {
    pub label: &'static str,
    pub cells: Vec<Cell>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Cell {
    /// The value in its unit, or [`NOT_MEASURED`].
    pub text: String,
    pub measured: bool,
    /// The change from the baseline. `None` in the baseline's column, between different workloads,
    /// when one of the two runs didn't measure the number, and when the baseline's value is 0.
    pub change: Option<CellChange>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CellChange {
    /// For example `+4.6%`.
    pub text: String,
    /// The two values that the change compares.
    pub values: String,
}

/// The latency that a part of the latency table shows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Latency {
    Publish,
    EndToEnd,
}

impl Latency {
    /// The aggregated percentiles of `run` in milliseconds. `None` if the run didn't measure them.
    pub const fn of(self, run: &OmbRunSummary) -> Option<OmbPercentiles> {
        match self {
            Self::Publish => Some(run.publish_latency_ms),
            Self::EndToEnd => run.end_to_end_latency_ms,
        }
    }
}

/// The publish throughput in MiB/s.
pub fn throughput_row(runs: &[&OmbRunSummary]) -> MatrixRow {
    row(
        THROUGHPUT_LABEL,
        runs,
        |run| Some(run.publish_mebibytes_per_second),
        mebibytes_text,
    )
}

/// The messages that the consumers received per second. A run without consumers didn't measure it.
pub fn consume_row(runs: &[&OmbRunSummary]) -> MatrixRow {
    row(
        CONSUME_LABEL,
        runs,
        |run| (run.workload.consumers_per_topic > 0).then_some(run.consume_messages_per_second),
        messages_text,
    )
}

/// The average, then one row per percentile of [`PERCENTILE_LABELS`], from the merged histogram of
/// the whole run.
pub fn latency_rows(runs: &[&OmbRunSummary], latency: Latency) -> Vec<MatrixRow> {
    let average = row(
        AVERAGE_LABEL,
        runs,
        |run| latency.of(run).map(|percentiles| percentiles.avg),
        milliseconds,
    );
    let percentiles = PERCENTILE_LABELS
        .into_iter()
        .enumerate()
        .map(|(index, label)| {
            row(
                label,
                runs,
                |run| {
                    latency
                        .of(run)
                        .map(|percentiles| percentile_values(&percentiles)[index])
                },
                milliseconds,
            )
        });
    iter::once(average).chain(percentiles).collect()
}

/// The changes that the summary card shows for each run except the baseline, in run order: the
/// label and the change of the throughput, the publish P99 and the end-to-end P99. They come from
/// the same values as the cards and the table, so the summary never disagrees with them. A run of
/// another workload gets no change.
pub fn summary_changes(runs: &[&OmbRunSummary]) -> Vec<Vec<(&'static str, CellChange)>> {
    let rows = [
        throughput_row(runs),
        row(
            PUBLISH_P99_LABEL,
            runs,
            |run| Some(run.publish_latency_ms.p99),
            milliseconds,
        ),
        row(
            END_TO_END_P99_LABEL,
            runs,
            |run| run.end_to_end_latency_ms.map(|latency| latency.p99),
            milliseconds,
        ),
    ];
    (1..runs.len())
        .map(|position| {
            rows.iter()
                .filter_map(|row| Some((row.label, row.cells.get(position)?.change.clone()?)))
                .collect()
        })
        .collect()
}

/// Runs of different workloads show their values side by side, without a change.
pub fn shows_change(baseline: &OmbRunSummary, run: &OmbRunSummary) -> bool {
    !matches!(
        OmbComparability::between(baseline, run),
        OmbComparability::WorkloadDiffers(_)
    )
}

/// Each run's cell, with its change from the baseline for every run except the baseline itself.
fn row(
    label: &'static str,
    runs: &[&OmbRunSummary],
    value_of: impl Fn(&OmbRunSummary) -> Option<f64>,
    text_of: fn(f64) -> String,
) -> MatrixRow {
    let baseline = runs.first().copied();
    let cells = runs
        .iter()
        .enumerate()
        .map(|(position, run)| {
            let value = value_of(run);
            let change = baseline.filter(|_| position > 0).and_then(|baseline| {
                cell_change(baseline, run, value_of(baseline)?, value?, text_of)
            });
            Cell {
                text: value.map_or_else(|| NOT_MEASURED.to_owned(), text_of),
                measured: value.is_some(),
                change,
            }
        })
        .collect();
    MatrixRow { label, cells }
}

/// `None` between different workloads and when `base` is 0.
fn cell_change(
    baseline: &OmbRunSummary,
    run: &OmbRunSummary,
    base: f64,
    value: f64,
    text_of: fn(f64) -> String,
) -> Option<CellChange> {
    if !shows_change(baseline, run) {
        return None;
    }
    Some(CellChange {
        text: change_text(percent_change(base, value)?),
        values: format!(
            "{} (baseline): {}. {}: {}.",
            short_id(baseline.id),
            text_of(base),
            short_id(run.id),
            text_of(value)
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        OMB_EDGE8_100_PARTITIONS, OMB_EDGE8_DEFAULT, OMB_NO_CONSUMERS, OMB_RC1_DEFAULT,
        OMB_RC1_PERSISTED,
    };

    fn change_texts(row: &MatrixRow) -> Vec<Option<&str>> {
        row.cells
            .iter()
            .map(|cell| cell.change.as_ref().map(|change| change.text.as_str()))
            .collect()
    }

    fn labeled<'a>(rows: &'a [MatrixRow], label: &str) -> &'a MatrixRow {
        rows.iter()
            .find(|row| row.label == label)
            .expect("the table has a row with this label")
    }

    #[test]
    fn given_two_driver_configurations_of_one_workload_when_compared_should_show_percent_changes() {
        let (rc1, persisted) = (OMB_RC1_DEFAULT.summary(), OMB_RC1_PERSISTED.summary());
        let runs = [&rc1, &persisted];

        let publish = latency_rows(&runs, Latency::Publish);
        let end_to_end = latency_rows(&runs, Latency::EndToEnd);

        let p99 = labeled(&publish, "P99");
        assert_eq!(p99.cells[0].text, "1.52 ms");
        assert_eq!(p99.cells[1].text, "3.46 ms");
        assert_eq!(change_texts(p99), [None, Some("+127.5%")]);
        assert_eq!(
            p99.cells[1]
                .change
                .as_ref()
                .map(|change| change.values.as_str()),
            Some(
                format!(
                    "{} (baseline): 1.52 ms. {}: 3.46 ms.",
                    short_id(rc1.id),
                    short_id(persisted.id)
                )
                .as_str()
            )
        );
        assert_eq!(
            change_texts(labeled(&end_to_end, "P99")),
            [None, Some("+100.0%")]
        );
        let labels: Vec<&str> = publish.iter().map(|row| row.label).collect();
        assert_eq!(
            labels,
            ["Avg", "P50", "P75", "P95", "P99", "P99.9", "P99.99", "Max"]
        );
        assert_eq!(
            publish[0].cells[0].text,
            milliseconds(rc1.publish_latency_ms.avg)
        );
    }

    #[test]
    fn given_two_versions_of_one_workload_when_throughput_compared_should_change_from_baseline() {
        let (edge8, rc1) = (OMB_EDGE8_DEFAULT.summary(), OMB_RC1_DEFAULT.summary());

        let throughput = throughput_row(&[&edge8, &rc1]);
        let consume = consume_row(&[&edge8, &rc1]);

        assert_eq!(throughput.cells[0].text, "143 MiB/s");
        assert_eq!(
            change_texts(&throughput),
            [None, Some("0.0%")],
            "a change that rounds to zero has no sign"
        );
        assert_eq!(consume.cells[1].text, "150.2k msg/s");
        assert_eq!(change_texts(&consume), [None, Some("0.0%")]);
    }

    #[test]
    fn given_run_without_consumers_when_compared_should_show_not_measured_and_no_change() {
        let (rc1, no_consumers) = (OMB_RC1_DEFAULT.summary(), OMB_NO_CONSUMERS.summary());
        let runs = [&rc1, &no_consumers];

        let end_to_end = latency_rows(&runs, Latency::EndToEnd);
        let consume = consume_row(&runs);
        let throughput = throughput_row(&runs);

        for row in end_to_end.iter().chain([&consume]) {
            assert_eq!(row.cells[1].text, NOT_MEASURED, "{}", row.label);
            assert!(!row.cells[1].measured);
            assert!(row.cells[0].measured);
        }
        // Another workload shows its values side by side, with no change.
        assert_eq!(throughput.cells[1].text, "71.6 MiB/s");
        assert_eq!(change_texts(&throughput), [None, None]);
    }

    #[test]
    fn given_three_runs_when_summarized_should_list_changes_of_comparable_runs_only() {
        let (rc1, persisted, partitions) = (
            OMB_RC1_DEFAULT.summary(),
            OMB_RC1_PERSISTED.summary(),
            OMB_EDGE8_100_PARTITIONS.summary(),
        );

        let lines = summary_changes(&[&rc1, &persisted, &partitions]);

        assert_eq!(lines.len(), 2);
        let labels: Vec<&str> = lines[0].iter().map(|(label, _)| *label).collect();
        assert_eq!(
            labels,
            [THROUGHPUT_LABEL, PUBLISH_P99_LABEL, END_TO_END_P99_LABEL]
        );
        assert_eq!(lines[0][1].1.text, "+127.5%");
        assert!(lines[1].is_empty());
        assert!(shows_change(&rc1, &persisted));
        assert!(!shows_change(&rc1, &partitions));
    }

    #[test]
    fn given_zero_baseline_when_changed_should_give_no_change() {
        let rc1 = OMB_RC1_DEFAULT.summary();
        let mut zero = OMB_RC1_PERSISTED.summary();
        zero.publish_mebibytes_per_second = 0.0;

        let throughput = throughput_row(&[&zero, &rc1]);

        assert_eq!(change_texts(&throughput), [None, None]);
    }
}
