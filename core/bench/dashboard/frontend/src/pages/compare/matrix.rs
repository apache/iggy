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

//! The compare page's delta matrix: each run's main group numbers, and each run's change against
//! the baseline. How comparable a run is with the baseline decides whether the delta rule judges
//! the change.

use bench_dashboard_shared::BenchmarkReportLight;
use bench_dashboard_shared::config::Thresholds;
use bench_dashboard_shared::delta::{DeltaVerdict, Metric};
use bench_dashboard_shared::summary::{BenchmarkSummary, Comparability};
use bench_report::group_metrics_summary::BenchmarkGroupMetricsSummary;

use super::report_of;
use crate::components::headline::{Judgment, metric_hint};
use crate::components::run_labels::{NO_RATE_LIMIT_NOTE, messages_text, milliseconds, short_id};
use crate::pages::run::change::{metric_name, metric_value_text, percent_change, percent_text};
use crate::pages::run::facts::{AVERAGE_LABEL, MAX_LABEL, MIN_LABEL, P9999_LABEL, is_recorded};

const MESSAGES_LABEL: &str = "Messages per second";
const MESSAGES_HINT: &str =
    "The messages the group moved, divided by its slowest actor's run time.";
const LATENCY_METRICS: [Metric; 5] = [
    Metric::P50,
    Metric::P90,
    Metric::P95,
    Metric::P99,
    Metric::P999,
];

type GroupValue = fn(&BenchmarkGroupMetricsSummary) -> f64;

/// The rows that the report has and the summary doesn't. The delta rule doesn't compare them across
/// runs, so they only show values. P99.99 comes first, right under the percentiles of the summary.
const REPORT_ROWS: [(&str, GroupValue); 4] = [
    (P9999_LABEL, |group| group.average_p9999_latency_ms),
    (MIN_LABEL, |group| group.min_latency_ms),
    (AVERAGE_LABEL, |group| group.average_latency_ms),
    (MAX_LABEL, |group| group.max_latency_ms),
];
/// The rows whose changes the page summary repeats, in the summary's order.
const SUMMARY_METRICS: [Metric; 2] = [Metric::Throughput, Metric::P99];

#[derive(Debug, Clone, PartialEq)]
pub struct MatrixRow {
    pub kind: RowKind,
    pub label: &'static str,
    /// What the row measures, in the Methodology page's words.
    pub hint: Option<&'static str>,
    /// One per run, the baseline first.
    pub cells: Vec<Cell>,
}

/// The page shows the latency rows in a table under the latency chart, and the throughput and the
/// message rate in a card next to the throughput chart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowKind {
    Throughput,
    MessageRate,
    Latency,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Cell {
    /// `None` shows a dash.
    pub value: Option<String>,
    /// Why the cell has no value.
    pub note: Option<&'static str>,
    /// The change against the baseline. `None` in the baseline's column, in a row without changes,
    /// and between different workloads.
    pub change: Option<CellChange>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CellChange {
    pub judgment: Judgment,
    /// For example `+4.6%`.
    pub text: String,
    /// The two values that the change compares.
    pub values: String,
}

/// Throughput, messages per second and the delta rule's percentiles, for each run's main group. A
/// latency row with no value in any run is left out.
pub fn change_rows(runs: &[&BenchmarkSummary], thresholds: &Thresholds) -> Vec<MatrixRow> {
    let throughput = MatrixRow {
        kind: RowKind::Throughput,
        label: metric_name(Metric::Throughput),
        hint: Some(metric_hint(Metric::Throughput)),
        cells: cells(runs, |run, baseline| {
            metric_cell(Metric::Throughput, run, baseline, thresholds)
        }),
    };
    let messages = MatrixRow {
        kind: RowKind::MessageRate,
        label: MESSAGES_LABEL,
        hint: Some(MESSAGES_HINT),
        cells: cells(runs, |run, baseline| {
            messages_cell(run, baseline, thresholds)
        }),
    };
    let latency = LATENCY_METRICS.iter().map(|&metric| MatrixRow {
        kind: RowKind::Latency,
        label: metric_name(metric),
        hint: Some(metric_hint(metric)),
        cells: cells(runs, |run, baseline| {
            metric_cell(metric, run, baseline, thresholds)
        }),
    });
    [throughput, messages]
        .into_iter()
        .chain(latency.filter(has_value))
        .collect()
}

/// P99.99, min, average and max of the main group, from each run's report. Old reports lack min and
/// max, and a row with no value in any run is left out.
pub fn report_rows(runs: &[&BenchmarkSummary], reports: &[BenchmarkReportLight]) -> Vec<MatrixRow> {
    REPORT_ROWS
        .into_iter()
        .map(|(label, value_of)| MatrixRow {
            kind: RowKind::Latency,
            label,
            hint: None,
            cells: runs
                .iter()
                .map(|run| report_cell(run, report_of(run, reports), value_of))
                .collect(),
        })
        .filter(has_value)
        .collect()
}

/// The changes that the page summary shows for each run except the baseline, in run order: the row
/// label and the cell's change, throughput first. They're the cells of `rows`, so the summary and
/// the matrix never disagree, and a run of a different workload from the baseline gets no change.
/// Empty when no run has a change.
pub fn summary_changes(rows: &[MatrixRow]) -> Vec<Vec<(&'static str, &CellChange)>> {
    let summary_rows: Vec<&MatrixRow> = SUMMARY_METRICS
        .iter()
        .filter_map(|&metric| rows.iter().find(|row| row.label == metric_name(metric)))
        .collect();
    let runs = rows.first().map_or(0, |row| row.cells.len());
    let lines: Vec<Vec<_>> = (1..runs)
        .map(|position| {
            summary_rows
                .iter()
                .filter_map(|row| Some((row.label, row.cells.get(position)?.change.as_ref()?)))
                .collect()
        })
        .collect();
    if lines.iter().all(Vec::is_empty) {
        return Vec::new();
    }
    lines
}

/// Each run's cell, with the baseline for every run except the baseline itself.
fn cells(
    runs: &[&BenchmarkSummary],
    cell: impl Fn(&BenchmarkSummary, Option<&BenchmarkSummary>) -> Cell,
) -> Vec<Cell> {
    let baseline = runs.first().copied();
    runs.iter()
        .enumerate()
        .map(|(position, run)| cell(run, baseline.filter(|_| position > 0)))
        .collect()
}

/// A run without a rate limit measures throughput only, so its latency shows no value.
fn metric_cell(
    metric: Metric,
    run: &BenchmarkSummary,
    baseline: Option<&BenchmarkSummary>,
    thresholds: &Thresholds,
) -> Cell {
    let measured = metric == Metric::Throughput || run.workload_key.rate_limit.is_some();
    let value = run
        .group(run.kind_family.primary_group())
        .filter(|_| measured)
        .map(|group| group.value(metric))
        .filter(|&value| metric == Metric::Throughput || is_recorded(value));
    let change = baseline.filter(|_| value.is_some()).and_then(|baseline| {
        let delta = thresholds.delta(
            metric,
            baseline.kind_family.primary_group(),
            &[baseline],
            &[run],
        )?;
        cell_change(
            run,
            baseline,
            delta.verdict,
            (delta.base.median, delta.head.median),
            |value| metric_value_text(metric, value),
        )
    });
    Cell {
        value: value.map(|value| metric_value_text(metric, value)),
        note: (!measured).then_some(NO_RATE_LIMIT_NOTE),
        change,
    }
}

/// One workload's message rate moves with its throughput, so the throughput verdict judges it too.
fn messages_cell(
    run: &BenchmarkSummary,
    baseline: Option<&BenchmarkSummary>,
    thresholds: &Thresholds,
) -> Cell {
    let messages = |summary: &BenchmarkSummary| {
        summary
            .group(summary.kind_family.primary_group())
            .map(|group| group.throughput_messages_per_second)
    };
    let value = messages(run);
    let change = baseline.and_then(|baseline| {
        let delta = thresholds.delta(
            Metric::Throughput,
            baseline.kind_family.primary_group(),
            &[baseline],
            &[run],
        )?;
        cell_change(
            run,
            baseline,
            delta.verdict,
            (messages(baseline)?, value?),
            messages_text,
        )
    });
    Cell {
        value: value.map(messages_text),
        note: None,
        change,
    }
}

fn report_cell(
    run: &BenchmarkSummary,
    report: Option<&BenchmarkReportLight>,
    value_of: GroupValue,
) -> Cell {
    if run.workload_key.rate_limit.is_none() {
        return Cell {
            value: None,
            note: Some(NO_RATE_LIMIT_NOTE),
            change: None,
        };
    }
    let kind = run.kind_family.primary_group();
    let value = report
        .into_iter()
        .flat_map(|report| &report.group_metrics)
        .find(|group| group.summary.kind == kind)
        .map(|group| value_of(&group.summary))
        .filter(|&value| is_recorded(value));
    Cell {
        value: value.map(milliseconds),
        note: None,
        change: None,
    }
}

/// `None` between different workloads, which only show their values, and when the baseline value is
/// 0.
fn cell_change(
    run: &BenchmarkSummary,
    baseline: &BenchmarkSummary,
    verdict: DeltaVerdict,
    (base, head): (f64, f64),
    text_of: impl Fn(f64) -> String,
) -> Option<CellChange> {
    let judgment = match baseline.comparability(run) {
        Comparability::LikeForLike | Comparability::EnvironmentDiffers => Judgment::Rule(verdict),
        Comparability::ParametersDiffer => Judgment::ParametersDiffer,
        Comparability::DifferentWorkloads => return None,
    };
    Some(CellChange {
        judgment,
        text: percent_text(percent_change(base, head)?),
        values: format!(
            "{} (baseline): {}. {}: {}.",
            short_id(&baseline.uuid),
            text_of(base),
            short_id(&run.uuid),
            text_of(head)
        ),
    })
}

fn has_value(row: &MatrixRow) -> bool {
    row.cells.iter().any(|cell| cell.value.is_some())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        CANDIDATE, MACHINE, RELEASE, THRESHOLDS, consumer, group_summary, limited, producer,
        rate_limited, run, with_numbers,
    };
    use bench_dashboard_shared::BenchmarkGroupMetricsLight;
    use bench_dashboard_shared::metrics::Badge;
    use bench_report::numeric_parameter::BenchmarkNumericParameter;

    const BASE_THROUGHPUT: f64 = 1000.0;
    const BASE_P99: f64 = 2.0;
    const OTHER_MACHINE: &str = "aws_i4i_4xlarge";

    fn baseline() -> BenchmarkSummary {
        with_numbers(limited(1, RELEASE), BASE_THROUGHPUT, BASE_P99)
    }

    /// 10% more throughput and half the latency: beyond noise for both.
    fn faster(summary: BenchmarkSummary) -> BenchmarkSummary {
        with_numbers(summary, BASE_THROUGHPUT * 1.1, BASE_P99 / 2.0)
    }

    fn rows(runs: &[&BenchmarkSummary]) -> Vec<MatrixRow> {
        change_rows(runs, &THRESHOLDS)
    }

    fn row<'a>(rows: &'a [MatrixRow], label: &str) -> Option<&'a MatrixRow> {
        rows.iter().find(|row| row.label == label)
    }

    /// The second run's judgment in the row `label`.
    fn judgment(rows: &[MatrixRow], label: &str) -> Option<Judgment> {
        row(rows, label)?
            .cells
            .get(1)?
            .change
            .as_ref()
            .map(|change| change.judgment)
    }

    #[test]
    fn given_like_for_like_or_other_environment_when_matrixed_should_judge_by_delta_rule() {
        let base = baseline();
        let same = faster(limited(2, CANDIDATE));
        let elsewhere = faster(run(3, rate_limited(producer()), OTHER_MACHINE, CANDIDATE));

        for other in [&same, &elsewhere] {
            let rows = rows(&[&base, other]);

            assert_eq!(
                judgment(&rows, "Throughput"),
                Some(Judgment::Rule(DeltaVerdict::Better))
            );
            assert_eq!(
                judgment(&rows, MESSAGES_LABEL),
                Some(Judgment::Rule(DeltaVerdict::Better))
            );
            assert_eq!(
                judgment(&rows, "P99"),
                Some(Judgment::Rule(DeltaVerdict::Better))
            );
            let change = row(&rows, "Throughput")
                .and_then(|row| row.cells[1].change.clone())
                .map(|change| change.text);
            assert_eq!(change.as_deref(), Some("+10.0%"));
        }
    }

    #[test]
    fn given_badge_or_small_change_when_matrixed_should_flag_or_call_noise() {
        let base = baseline();
        let mut flagged = faster(limited(2, CANDIDATE));
        flagged.badges = vec![Badge::UnevenLoad];
        let close = with_numbers(limited(3, CANDIDATE), BASE_THROUGHPUT * 1.01, BASE_P99);

        assert_eq!(
            judgment(&rows(&[&base, &flagged]), "Throughput"),
            Some(Judgment::Rule(DeltaVerdict::Flagged))
        );
        assert_eq!(
            judgment(&rows(&[&base, &close]), "Throughput"),
            Some(Judgment::Rule(DeltaVerdict::Noise))
        );
    }

    #[test]
    fn given_other_parameters_when_matrixed_should_show_change_without_judgment() {
        let base = baseline();
        let mut params = rate_limited(producer());
        params.message_size = BenchmarkNumericParameter::Value(100);
        let other = faster(run(2, params, MACHINE, CANDIDATE));

        let rows = rows(&[&base, &other]);

        for label in ["Throughput", MESSAGES_LABEL, "P50", "P99.9"] {
            assert_eq!(
                judgment(&rows, label),
                Some(Judgment::ParametersDiffer),
                "{label}"
            );
        }
    }

    #[test]
    fn given_different_workloads_when_matrixed_should_show_values_without_change() {
        let base = baseline();
        let other = with_numbers(
            run(2, rate_limited(consumer()), MACHINE, CANDIDATE),
            BASE_THROUGHPUT,
            BASE_P99,
        );

        let rows = rows(&[&base, &other]);

        assert!(
            rows.iter()
                .all(|row| row.cells.iter().all(|cell| cell.change.is_none()))
        );
        assert!(rows.iter().all(|row| row.cells[1].value.is_some()));
    }

    #[test]
    fn given_baseline_column_when_matrixed_should_show_values_only() {
        let base = baseline();
        let other = faster(limited(2, CANDIDATE));

        let rows = rows(&[&base, &other]);

        assert!(rows.iter().all(|row| row.cells[0].change.is_none()));
        assert!(rows.iter().all(|row| row.cells[0].value.is_some()));
    }

    #[test]
    fn given_runs_without_rate_limit_when_matrixed_should_drop_latency_rows() {
        let base = with_numbers(
            run(1, producer(), MACHINE, RELEASE),
            BASE_THROUGHPUT,
            BASE_P99,
        );
        let other = with_numbers(
            run(2, producer(), MACHINE, CANDIDATE),
            BASE_THROUGHPUT,
            BASE_P99,
        );
        let limited_run = faster(limited(3, CANDIDATE));

        let uncapped = rows(&[&base, &other]);
        let mixed = rows(&[&base, &limited_run]);

        let labels: Vec<_> = uncapped.iter().map(|row| row.label).collect();
        assert_eq!(labels, ["Throughput", MESSAGES_LABEL]);
        let p99 = row(&mixed, "P99").map(|row| row.cells.clone());
        let p99 = p99.unwrap_or_default();
        assert_eq!(p99[0].value, None);
        assert_eq!(p99[0].note, Some(NO_RATE_LIMIT_NOTE));
        assert!(p99[1].value.is_some());
        assert!(p99[1].change.is_none());
    }

    #[test]
    fn given_old_reports_without_min_and_max_when_matrixed_should_drop_their_rows() {
        let base = baseline();
        let other = faster(limited(2, CANDIDATE));
        let report = |run: &BenchmarkSummary, average: f64| BenchmarkReportLight {
            uuid: run.uuid,
            group_metrics: vec![BenchmarkGroupMetricsLight {
                summary: BenchmarkGroupMetricsSummary {
                    min_latency_ms: 0.0,
                    max_latency_ms: 0.0,
                    std_dev_latency_ms: 0.0,
                    average_latency_ms: average,
                    average_p9999_latency_ms: average * 4.0,
                    ..group_summary()
                },
                latency_distribution: None,
            }],
            ..BenchmarkReportLight::default()
        };
        let reports = [report(&other, 0.8), report(&base, 1.5)];

        let rows = report_rows(&[&base, &other], &reports);

        let labels: Vec<_> = rows.iter().map(|row| row.label).collect();
        assert_eq!(labels, [P9999_LABEL, AVERAGE_LABEL]);
        let averages: Vec<_> = rows[1]
            .cells
            .iter()
            .map(|cell| cell.value.clone())
            .collect();
        assert_eq!(averages, [Some(milliseconds(1.5)), Some(milliseconds(0.8))]);
        assert!(
            rows.iter()
                .flat_map(|row| &row.cells)
                .all(|cell| cell.change.is_none())
        );
        assert!(rows.iter().all(|row| row.kind == RowKind::Latency));
    }

    #[test]
    fn given_rate_limited_runs_when_matrixed_should_mark_each_row_with_its_kind() {
        let rows = rows(&[&baseline(), &faster(limited(2, CANDIDATE))]);

        let kinds: Vec<_> = rows.iter().map(|row| (row.label, row.kind)).collect();
        assert_eq!(
            kinds,
            [
                ("Throughput", RowKind::Throughput),
                (MESSAGES_LABEL, RowKind::MessageRate),
                ("P50", RowKind::Latency),
                ("P90", RowKind::Latency),
                ("P95", RowKind::Latency),
                ("P99", RowKind::Latency),
                ("P99.9", RowKind::Latency),
            ]
        );
    }

    #[test]
    fn given_pair_at_each_level_when_summarized_should_repeat_matrix_unless_workloads_differ() {
        let base = baseline();
        let same = faster(limited(2, CANDIDATE));
        let elsewhere = faster(run(3, rate_limited(producer()), OTHER_MACHINE, CANDIDATE));
        let mut params = rate_limited(producer());
        params.message_size = BenchmarkNumericParameter::Value(100);
        let other_size = faster(run(4, params, MACHINE, CANDIDATE));
        let reader = faster(run(5, rate_limited(consumer()), MACHINE, CANDIDATE));
        let better = Some(Judgment::Rule(DeltaVerdict::Better));
        let cases = [
            (Comparability::LikeForLike, &same, better),
            (Comparability::EnvironmentDiffers, &elsewhere, better),
            (
                Comparability::ParametersDiffer,
                &other_size,
                Some(Judgment::ParametersDiffer),
            ),
            (Comparability::DifferentWorkloads, &reader, None),
        ];

        for (level, other, expected) in cases {
            assert_eq!(base.comparability(other), level);
            let rows = rows(&[&base, other]);

            let lines = summary_changes(&rows);

            let Some(expected) = expected else {
                assert!(lines.is_empty(), "{level}");
                continue;
            };
            assert_eq!(lines.len(), 1, "{level}");
            let labels: Vec<_> = lines[0].iter().map(|(label, _)| *label).collect();
            assert_eq!(labels, ["Throughput", "P99"], "{level}");
            for (label, change) in &lines[0] {
                assert_eq!(change.judgment, expected, "{level} {label}");
                let cell = row(&rows, label).and_then(|row| row.cells[1].change.as_ref());
                assert_eq!(cell, Some(*change), "{level} {label}");
            }
        }
    }

    #[test]
    fn given_three_runs_when_summarized_should_give_line_per_run_without_change_for_other_workload()
    {
        let base = baseline();
        let same = faster(limited(2, CANDIDATE));
        let elsewhere = faster(run(3, rate_limited(producer()), OTHER_MACHINE, CANDIDATE));
        let reader = faster(run(4, rate_limited(consumer()), MACHINE, CANDIDATE));

        let comparable_rows = rows(&[&base, &same, &elsewhere]);
        let mixed_rows = rows(&[&base, &same, &reader]);

        let comparable = summary_changes(&comparable_rows);
        let mixed = summary_changes(&mixed_rows);

        assert_eq!(comparable.len(), 2);
        assert!(comparable.iter().all(|line| line.len() == 2));
        assert_eq!(mixed.len(), 2);
        assert_eq!(mixed[0].len(), 2);
        assert!(mixed[1].is_empty());
    }

    #[test]
    fn given_pair_without_rate_limit_when_summarized_should_show_throughput_only() {
        let base = with_numbers(
            run(1, producer(), MACHINE, RELEASE),
            BASE_THROUGHPUT,
            BASE_P99,
        );
        let other = faster(run(2, producer(), MACHINE, CANDIDATE));
        let rows = rows(&[&base, &other]);

        let lines = summary_changes(&rows);

        assert_eq!(lines.len(), 1);
        let labels: Vec<_> = lines[0].iter().map(|(label, _)| *label).collect();
        assert_eq!(labels, ["Throughput"]);
        assert_eq!(lines[0][0].1.judgment, Judgment::Rule(DeltaVerdict::Better));
    }
}
