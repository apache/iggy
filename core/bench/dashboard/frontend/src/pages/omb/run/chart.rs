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

//! The OMB run page's charts, styled like the iggy-bench run charts: the run over its 10 second
//! windows, the history line by version or by run, and the latency percentile curves of the merged
//! histograms.

use std::rc::Rc;

use bench_dashboard_shared::omb::detail::{OmbLatencySeries, OmbRunDetail};
use bench_dashboard_shared::omb::id::OmbRunId;
use bench_report::plotting::chart::IggyChart;
use charming::Chart;
use charming::element::{ItemStyle, Tooltip, Trigger};
use charming::series::{Line, Scatter};
use yew::prelude::*;

use super::history::{CURRENT_LABEL, HistoryChart, HistoryPoint};
use crate::components::chart::{ChartCanvas, SharedChart};
use crate::pages::omb::common::{LineOrder, window_points};
use crate::pages::omb::quantile::{LATENCY_AXIS_NAME, QuantileCurve, quantile_options};
use crate::pages::run::chart::{CURRENT_COLOR, LINE_OPACITY, MEDIAN_NAME};

const NO_POINTS_NOTE: &str = "The result file has no values for this chart.";
/// A card chart has no room for a name at the end of its x axis, so the labels or the card's text
/// say what the axis shows.
const NO_AXIS_NAME: &str = "";
const DELAY_AXIS_NAME: &str = "Delay [µs]";
const RUNS_NAME: &str = "Runs";
const RUN_SYMBOL_SIZE: f64 = 8.0;
const CURRENT_SYMBOL_SIZE: f64 = 14.0;
/// The series name, then the name of the point, which holds the run and the value.
const ITEM_TOOLTIP: &str = "{a}<br/>{b}";

/// A run detail that compares by its allocation, so a chart only redraws when a new detail
/// arrives, and a re-render doesn't compare thousands of quantile points.
#[derive(Clone)]
pub struct DetailRef(pub Rc<OmbRunDetail>);

impl PartialEq for DetailRef {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.0, &other.0)
    }
}

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
pub struct WindowLine<'a> {
    pub name: &'static str,
    /// One per window.
    pub values: &'a [f64],
}

/// The latency curves that share a chart, because they share a unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuantileGroup {
    /// Publish and end-to-end, in ms.
    Latency,
    /// In µs.
    PublishDelay,
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

/// The lines of `metric`. Empty when the run didn't measure it: no end-to-end latency without
/// consumers, no publish delay or errors in a file from an older OMB. A run without consumers has
/// no consume line.
pub fn window_lines(metric: WindowMetric, detail: &OmbRunDetail) -> Vec<WindowLine<'_>> {
    let series = &detail.series;
    let lines = match metric {
        WindowMetric::Throughput => {
            let mut lines = vec![WindowLine::new(
                "Publish",
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
    const fn axis_name(self) -> &'static str {
        match self {
            Self::Latency => LATENCY_AXIS_NAME,
            Self::PublishDelay => DELAY_AXIS_NAME,
        }
    }
}

/// The curves of `group`, without the curves that the run didn't measure.
pub fn quantile_curves(group: QuantileGroup, detail: &OmbRunDetail) -> Vec<QuantileCurve> {
    let quantiles = &detail.quantiles;
    let curves = match group {
        QuantileGroup::Latency => vec![
            ("Publish", Some(&quantiles.publish_latency_ms)),
            ("End-to-end", quantiles.end_to_end_latency_ms.as_ref()),
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

#[derive(Properties, PartialEq)]
pub struct WindowChartProps {
    pub detail: DetailRef,
    pub metric: WindowMetric,
}

/// The lines of one metric over the run, one point per window at its end.
#[function_component(WindowChart)]
pub fn window_chart(props: &WindowChartProps) -> Html {
    let chart = use_memo((props.detail.clone(), props.metric), |(detail, metric)| {
        let lines = window_lines(*metric, &detail.0);
        (!lines.is_empty()).then(|| SharedChart::new(window_options(*metric, &detail.0, &lines)))
    });
    let id = format!(
        "omb-window-chart-{:?}-{}",
        props.metric, props.detail.0.summary.id
    );
    html! {
        <ChartCanvas {id} chart={(*chart).clone()}>{ no_points_status(chart.is_some()) }</ChartCanvas>
    }
}

#[derive(Properties, PartialEq)]
pub struct RunsChartProps {
    /// The page's run, used to name the canvas.
    pub run: OmbRunId,
    pub chart: HistoryChart,
}

/// The runs of the line, oldest first, with the page's run marked.
#[function_component(RunsChart)]
pub fn runs_chart(props: &RunsChartProps) -> Html {
    let chart = use_memo(props.chart.clone(), |chart| {
        SharedChart::new(runs_options(chart))
    });
    let id = format!("omb-runs-chart-{:?}-{}", props.chart.kpi, props.run);
    html! { <ChartCanvas {id} chart={Some((*chart).clone())} /> }
}

#[derive(Properties, PartialEq)]
pub struct QuantileChartProps {
    pub detail: DetailRef,
    pub group: QuantileGroup,
}

/// The latency at every percentile of the merged histogram, on a log axis, with the tail spread
/// out by nines.
#[function_component(QuantileChart)]
pub fn quantile_chart(props: &QuantileChartProps) -> Html {
    let chart = use_memo((props.detail.clone(), props.group), |(detail, group)| {
        let curves = quantile_curves(*group, &detail.0);
        (!curves.is_empty()).then(|| SharedChart::new(quantile_options(group.axis_name(), &curves)))
    });
    let id = format!(
        "omb-quantile-chart-{:?}-{}",
        props.group, props.detail.0.summary.id
    );
    html! {
        <ChartCanvas {id} chart={(*chart).clone()}>{ no_points_status(chart.is_some()) }</ChartCanvas>
    }
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

fn no_points_status(drawn: bool) -> Html {
    if drawn {
        return Html::default();
    }
    html! { <p class="data-status">{ NO_POINTS_NOTE }</p> }
}

fn window_options(metric: WindowMetric, detail: &OmbRunDetail, lines: &[WindowLine<'_>]) -> Chart {
    let window_ends = detail.series.window_ends();
    let mut chart = IggyChart::card()
        .with_time_x_axis()
        .with_y_axis(metric.axis_name());
    for line in lines {
        chart = chart.add_time_series(
            line.name,
            window_points(&window_ends, line.values),
            None,
            LINE_OPACITY,
        );
    }
    chart.inner
}

/// On a line in version order, a dot per run and a line through the median of each version, as on
/// the iggy-bench run page. On a line in time order, a line through the runs.
fn runs_options(chart: &HistoryChart) -> Chart {
    let named = |point: &HistoryPoint| (vec![point.index as f64, point.value], point.name.clone());
    let named_all =
        |points: &mut dyn Iterator<Item = &HistoryPoint>| -> Vec<_> { points.map(named).collect() };
    let current = named_all(&mut chart.points.iter().filter(|point| point.current));
    let base = IggyChart::card()
        .with_category_x_axis(NO_AXIS_NAME, chart.labels.clone())
        .with_y_axis(chart.kpi.axis_name())
        .inner
        .tooltip(
            Tooltip::new()
                .trigger(Trigger::Item)
                .formatter(ITEM_TOOLTIP),
        );
    let options = match chart.order {
        LineOrder::Version => base
            .series(
                Scatter::new()
                    .name(RUNS_NAME)
                    .symbol_size(RUN_SYMBOL_SIZE)
                    .data(named_all(
                        &mut chart.points.iter().filter(|point| !point.current),
                    )),
            )
            .series(
                Line::new()
                    .name(MEDIAN_NAME)
                    .data(named_all(&mut chart.medians.iter())),
            ),
        LineOrder::Time => base.series(
            Line::new()
                .name(RUNS_NAME)
                .symbol_size(RUN_SYMBOL_SIZE)
                .data(named_all(&mut chart.points.iter())),
        ),
    };
    // The legend names every series, so a chart without the page's run has no such series.
    if current.is_empty() {
        return options;
    }
    options.series(
        Scatter::new()
            .name(CURRENT_LABEL)
            .symbol_size(CURRENT_SYMBOL_SIZE)
            .item_style(ItemStyle::new().color(CURRENT_COLOR))
            .data(current),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pages::omb::quantile::curve_points;
    use crate::test_support::{OMB_NO_CONSUMERS, OMB_PUBLISH_ERRORS, OMB_RC1_DEFAULT};

    fn names(lines: &[WindowLine<'_>]) -> Vec<&'static str> {
        lines.iter().map(|line| line.name).collect()
    }

    fn curve_names(curves: &[QuantileCurve]) -> Vec<&str> {
        curves.iter().map(|curve| curve.name.as_str()).collect()
    }

    #[test]
    fn given_fixture_detail_when_lines_picked_should_draw_one_value_per_window() {
        let detail = OMB_RC1_DEFAULT.detail();

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
        let detail = OMB_NO_CONSUMERS.detail();

        assert_eq!(
            names(&window_lines(WindowMetric::Throughput, &detail)),
            ["Publish"]
        );
        assert_eq!(window_lines(WindowMetric::EndToEndLatency, &detail), []);
        let curves = quantile_curves(QuantileGroup::Latency, &detail);
        assert_eq!(curve_names(&curves), ["Publish"]);
    }

    #[test]
    fn given_run_with_publish_errors_when_lines_picked_should_draw_errors_per_window() {
        let detail = OMB_PUBLISH_ERRORS.detail();

        let lines = window_lines(WindowMetric::PublishErrors, &detail);

        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0].values.len(), 18);
        assert!(lines[0].values.iter().all(|rate| *rate > 0.0));
    }

    #[test]
    fn given_fixture_quantiles_when_grouped_should_draw_measured_curves_in_palette_colors() {
        let detail = OMB_RC1_DEFAULT.detail();

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
}
