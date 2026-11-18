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

//! The charts of one OMB run: its numbers over the 10 second windows, its latency quantile curves
//! and its percentile ladder. The run page draws the window and quantile charts on cards and the
//! embeds draw them under a title, so a chart of a run has the same lines in both. Only the embeds
//! draw the ladder as a chart. Each builder takes the empty chart that it draws on.

use bench_report::plotting::chart::IggyChart;
use charming::Chart;
use charming::series::Line;

use crate::omb::detail::{OmbLatencySeries, OmbRunDetail};
use crate::omb::quantile::{QuantileCurve, quantile_options};
use crate::omb::result::OmbPercentiles;
use crate::omb::summary::OmbRunSummary;

pub const LATENCY_AXIS_NAME: &str = "Latency [ms]";
/// The lines of the OMB charts are solid, as on the iggy-bench run page.
pub const LINE_OPACITY: f64 = 1.0;
/// The labels of [`percentile_values`], in its order. The average isn't a percentile, so it isn't
/// among them.
pub const PERCENTILE_LABELS: [&str; 7] = ["P50", "P75", "P95", "P99", "P99.9", "P99.99", "Max"];
const DELAY_AXIS_NAME: &str = "Delay [µs]";
/// The ladder's labels name the percentiles, so its x axis needs no name.
const NO_AXIS_NAME: &str = "";
pub const PUBLISH_NAME: &str = "Publish";
pub const END_TO_END_NAME: &str = "End-to-end";

/// A number of the run that OMB records once per window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowMetric {
    /// Publish and consume, in MiB/s.
    Throughput,
    PublishLatency,
    EndToEndLatency,
    PublishDelay,
    Backlog,
    PublishErrors,
}

/// One line of a window chart.
#[derive(Debug, Clone, Copy, PartialEq)]
struct WindowLine<'a> {
    name: &'static str,
    /// One per window.
    values: &'a [f64],
}

/// The latency curves that share a chart, because they share a unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuantileGroup {
    /// Publish and end-to-end, in ms.
    Latency,
    /// In µs.
    PublishDelay,
}

/// One line of a percentile ladder chart.
#[derive(Debug, Clone, PartialEq)]
struct LadderLine {
    name: &'static str,
    /// In the order of [`PERCENTILE_LABELS`], up to the first value that a log axis can't show.
    values: Vec<f64>,
}

impl WindowMetric {
    pub const fn title(self) -> &'static str {
        match self {
            Self::Throughput => "Throughput",
            Self::PublishLatency => "Publish latency",
            Self::EndToEndLatency => "End-to-end latency",
            Self::PublishDelay => "Publish delay",
            Self::Backlog => "Backlog",
            Self::PublishErrors => "Publish errors",
        }
    }

    const fn axis_name(self) -> &'static str {
        match self {
            Self::Throughput => "Throughput [MiB/s]",
            Self::PublishLatency | Self::EndToEndLatency => LATENCY_AXIS_NAME,
            Self::PublishDelay => DELAY_AXIS_NAME,
            Self::Backlog => "Backlog [messages]",
            Self::PublishErrors => "Failed sends [msg/s]",
        }
    }
}

/// The lines of `metric` on `chart`, one point per window at its end. `None` when the run didn't
/// measure `metric`.
pub fn window_options(
    chart: IggyChart,
    metric: WindowMetric,
    detail: &OmbRunDetail,
) -> Option<Chart> {
    let lines = window_lines(metric, detail);
    if lines.is_empty() {
        return None;
    }
    let window_ends = detail.series.window_ends();
    let mut chart = chart.with_time_x_axis().with_y_axis(metric.axis_name());
    for line in lines {
        chart = chart.add_time_series(
            line.name,
            window_points(&window_ends, line.values),
            None,
            LINE_OPACITY,
        );
    }
    Some(chart.inner)
}

/// One `[seconds since the test started, value]` point per window, for a chart with a time axis.
/// `window_ends` comes from the series of the run detail.
pub fn window_points(window_ends: &[u64], values: &[f64]) -> Vec<Vec<f64>> {
    window_ends
        .iter()
        .zip(values)
        .map(|(end, value)| vec![*end as f64, *value])
        .collect()
}

/// The lines of `metric`. Empty when the run didn't measure it: no end-to-end latency without
/// consumers, no publish delay or errors in a file from an older OMB. A run without consumers has
/// no consume line.
fn window_lines(metric: WindowMetric, detail: &OmbRunDetail) -> Vec<WindowLine<'_>> {
    let series = &detail.series;
    let lines = match metric {
        WindowMetric::Throughput => {
            let mut lines = vec![WindowLine::new(
                PUBLISH_NAME,
                &series.publish_mebibytes_per_second,
            )];
            if detail.summary.workload.consumers_per_topic > 0 {
                lines.push(WindowLine::new(
                    "Consume",
                    &series.consume_mebibytes_per_second,
                ));
            }
            lines
        }
        WindowMetric::PublishLatency => percentile_lines(Some(&series.publish_latency_ms)),
        WindowMetric::EndToEndLatency => percentile_lines(series.end_to_end_latency_ms.as_ref()),
        WindowMetric::PublishDelay => percentile_lines(series.publish_delay_latency_us.as_ref()),
        WindowMetric::Backlog => vec![WindowLine::new("Backlog", &series.backlog)],
        WindowMetric::PublishErrors => vec![WindowLine::new(
            "Publish errors",
            &series.publish_errors_per_second,
        )],
    };
    lines
        .into_iter()
        .filter(|line| !line.values.is_empty())
        .collect()
}

impl<'a> WindowLine<'a> {
    const fn new(name: &'static str, values: &'a [f64]) -> Self {
        Self { name, values }
    }
}

impl QuantileGroup {
    pub const fn axis_name(self) -> &'static str {
        match self {
            Self::Latency => LATENCY_AXIS_NAME,
            Self::PublishDelay => DELAY_AXIS_NAME,
        }
    }
}

/// The curves of `group` on `chart`. `None` when the run measured none of them.
pub fn quantile_group_options(
    chart: IggyChart,
    group: QuantileGroup,
    detail: &OmbRunDetail,
) -> Option<Chart> {
    let curves = quantile_curves(group, detail);
    (!curves.is_empty()).then(|| quantile_options(chart, group.axis_name(), &curves))
}

/// The curves of `group`, without the curves that the run didn't measure.
fn quantile_curves(group: QuantileGroup, detail: &OmbRunDetail) -> Vec<QuantileCurve> {
    let quantiles = &detail.quantiles;
    let curves = match group {
        QuantileGroup::Latency => vec![
            (PUBLISH_NAME, Some(&quantiles.publish_latency_ms)),
            (END_TO_END_NAME, quantiles.end_to_end_latency_ms.as_ref()),
        ],
        QuantileGroup::PublishDelay => {
            vec![("Publish delay", quantiles.publish_delay_latency_us.as_ref())]
        }
    };
    curves
        .into_iter()
        .filter_map(|(name, curve)| QuantileCurve::new(name, curve?, None))
        .collect()
}

/// The values of `latency` in the order of [`PERCENTILE_LABELS`].
pub const fn percentile_values(latency: &OmbPercentiles) -> [f64; 7] {
    [
        latency.p50,
        latency.p75,
        latency.p95,
        latency.p99,
        latency.p999,
        latency.p9999,
        latency.max,
    ]
}

/// The ladders of the run on `chart` against the percentiles, on a log axis, as the iggy-bench run
/// page draws its percentiles. The values come from the merged histograms of the whole run. `None`
/// when no ladder has a value.
pub fn ladder_options(chart: IggyChart, summary: &OmbRunSummary) -> Option<Chart> {
    let lines = ladder_lines(summary);
    if lines.is_empty() {
        return None;
    }
    let categories = PERCENTILE_LABELS.map(str::to_owned).to_vec();
    let mut chart = chart
        .with_category_x_axis(NO_AXIS_NAME, categories)
        .with_log_y_axis(LATENCY_AXIS_NAME)
        .inner;
    for line in lines {
        chart = chart.series(Line::new().name(line.name).data(line.values));
    }
    Some(chart)
}

/// The publish ladder, and the end-to-end ladder of a run that measured it. A line stops before
/// its first value of 0 or less, because a log axis has no place for it, and a line without a
/// value is left out.
fn ladder_lines(summary: &OmbRunSummary) -> Vec<LadderLine> {
    [
        (PUBLISH_NAME, Some(&summary.publish_latency_ms)),
        (END_TO_END_NAME, summary.end_to_end_latency_ms.as_ref()),
    ]
    .into_iter()
    .filter_map(|(name, latency)| {
        let values: Vec<f64> = percentile_values(latency?)
            .into_iter()
            .take_while(|value| *value > 0.0)
            .collect();
        (!values.is_empty()).then_some(LadderLine { name, values })
    })
    .collect()
}

/// P50, P99 and P99.9, which a window chart of a latency draws. None for a latency that the run
/// didn't measure.
fn percentile_lines(latency: Option<&OmbLatencySeries>) -> Vec<WindowLine<'_>> {
    latency.map_or_else(Vec::new, |latency| {
        vec![
            WindowLine::new("P50", &latency.p50),
            WindowLine::new("P99", &latency.p99),
            WindowLine::new("P99.9", &latency.p999),
        ]
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::omb::quantile::curve_points;
    use crate::omb::test_fixtures::{NO_CONSUMERS, PUBLISH_ERRORS, RC1_DEFAULT};

    fn names(lines: &[WindowLine<'_>]) -> Vec<&'static str> {
        lines.iter().map(|line| line.name).collect()
    }

    fn curve_names(curves: &[QuantileCurve]) -> Vec<&str> {
        curves.iter().map(|curve| curve.name.as_str()).collect()
    }

    fn ladder_names(lines: &[LadderLine]) -> Vec<&'static str> {
        lines.iter().map(|line| line.name).collect()
    }

    #[test]
    fn given_fixture_detail_when_lines_picked_should_draw_one_value_per_window() {
        let detail = RC1_DEFAULT.detail();

        let metrics = [
            (WindowMetric::Throughput, vec!["Publish", "Consume"]),
            (WindowMetric::PublishLatency, vec!["P50", "P99", "P99.9"]),
            (WindowMetric::EndToEndLatency, vec!["P50", "P99", "P99.9"]),
            (WindowMetric::PublishDelay, vec!["P50", "P99", "P99.9"]),
            (WindowMetric::Backlog, vec!["Backlog"]),
            (WindowMetric::PublishErrors, vec!["Publish errors"]),
        ];

        for (metric, expected) in metrics {
            let lines = window_lines(metric, &detail);
            assert_eq!(names(&lines), expected, "{metric:?}");
            assert!(
                lines
                    .iter()
                    .all(|line| line.values.len() == detail.summary.windows),
                "{metric:?}"
            );
        }
    }

    #[test]
    fn given_run_without_consumers_when_lines_picked_should_leave_out_consume_and_end_to_end() {
        let detail = NO_CONSUMERS.detail();

        assert_eq!(
            names(&window_lines(WindowMetric::Throughput, &detail)),
            ["Publish"]
        );
        assert_eq!(window_lines(WindowMetric::EndToEndLatency, &detail), []);
        let curves = quantile_curves(QuantileGroup::Latency, &detail);
        assert_eq!(curve_names(&curves), ["Publish"]);
        assert_eq!(ladder_names(&ladder_lines(&detail.summary)), ["Publish"]);
    }

    #[test]
    fn given_run_with_publish_errors_when_lines_picked_should_draw_errors_per_window() {
        let detail = PUBLISH_ERRORS.detail();

        let lines = window_lines(WindowMetric::PublishErrors, &detail);

        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0].values.len(), 18);
        assert!(lines[0].values.iter().all(|rate| *rate > 0.0));
    }

    #[test]
    fn given_fixture_quantiles_when_grouped_should_draw_measured_curves_in_palette_colors() {
        let detail = RC1_DEFAULT.detail();

        let curves = quantile_curves(QuantileGroup::Latency, &detail);
        let delay = quantile_curves(QuantileGroup::PublishDelay, &detail);

        assert_eq!(curve_names(&curves), ["Publish", "End-to-end"]);
        assert_eq!(
            curves[0].points,
            curve_points(&detail.quantiles.publish_latency_ms)
        );
        assert!(curves.iter().all(|curve| curve.color.is_none()));
        assert_eq!(curve_names(&delay), ["Publish delay"]);
    }

    #[test]
    fn given_fixture_detail_when_charted_should_give_one_point_per_window() {
        let detail = RC1_DEFAULT.detail();
        let series = &detail.series;

        let points = window_points(&series.window_ends(), &series.publish_mebibytes_per_second);

        assert_eq!(points.len(), detail.summary.windows);
        assert_eq!(points[0][0], 10.0);
        assert_eq!(
            points.last().map(|point| point[0]),
            Some(detail.summary.length_seconds as f64)
        );
    }

    #[test]
    fn given_fixture_run_when_laddered_should_draw_publish_and_end_to_end_from_p50_to_max() {
        let summary = RC1_DEFAULT.summary();
        let end_to_end = summary
            .end_to_end_latency_ms
            .expect("the fixture run measured end-to-end latency");

        let lines = ladder_lines(&summary);

        assert_eq!(ladder_names(&lines), ["Publish", "End-to-end"]);
        assert_eq!(
            lines[0].values,
            percentile_values(&summary.publish_latency_ms)
        );
        assert_eq!(lines[1].values, percentile_values(&end_to_end));
        assert_eq!(lines[0].values.len(), PERCENTILE_LABELS.len());
        assert_eq!(lines[0].values[3], 1.523);
    }

    #[test]
    fn given_ladder_with_zero_value_when_laddered_should_stop_line_before_it() {
        let mut summary = RC1_DEFAULT.summary();
        summary.publish_latency_ms.p999 = 0.0;
        if let Some(end_to_end) = summary.end_to_end_latency_ms.as_mut() {
            end_to_end.p50 = 0.0;
        }

        let lines = ladder_lines(&summary);

        assert_eq!(ladder_names(&lines), ["Publish"]);
        assert_eq!(lines[0].values.len(), 4);
    }
}
