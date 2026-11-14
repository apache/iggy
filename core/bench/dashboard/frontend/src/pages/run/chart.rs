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

//! The run page's charts, styled like the bench report charts: the run over time from the
//! downsampled series that the server keeps per run, and the history line by version.

use std::rc::Rc;

use bench_dashboard_shared::BenchmarkReportLight;
use bench_dashboard_shared::delta::Metric;
use bench_dashboard_shared::series::{BenchmarkSeries, ChartSeries, SeriesMetric, SeriesScope};
use bench_report::add_latency_distributions;
use bench_report::group_metrics_kind::GroupMetricsKind;
use bench_report::plotting::chart::IggyChart;
use charming::element::{AxisPointer, AxisPointerType, ItemStyle, Tooltip, Trigger};
use charming::series::{Line, Scatter};
use charming::theme::Theme;
use charming::{Chart, WasmRenderer};
use gloo::console::error;
use uuid::Uuid;
use web_sys::Element;
use yew::prelude::*;

use super::change::{metric_name, metric_value_text};
use super::facts::P9999_LABEL;
use super::history::{CURRENT_LABEL, HistoryChart, VersionPoint};
use crate::api::fetch_benchmark_series;
use crate::components::chart::{dispose_chart_at, reset_zoom_at};
use crate::components::page::loadable;
use crate::hooks::use_size;
use crate::state::data::{Loadable, fetch_into};

pub const LATENCY_CHART_NOTE: &str = "The average latency of the batches in each sampling window. \
                                      Reports have no P99 series, so the chart can't show one.";
pub const LINE_OPACITY: f64 = 1.0;
const NO_POINTS_NOTE: &str = "This run's report has no time series.";
/// A card chart has no room for a name at the end of its x axis, so the labels or the card's text
/// say what the axis shows.
const NO_AXIS_NAME: &str = "";
const LATENCY_AXIS_NAME: &str = "Latency [ms]";
const PERCENTILE_LABELS: [&str; 6] = [
    metric_name(Metric::P50),
    metric_name(Metric::P90),
    metric_name(Metric::P95),
    metric_name(Metric::P99),
    metric_name(Metric::P999),
    P9999_LABEL,
];
const RUNS_NAME: &str = "Runs";
const MEDIAN_NAME: &str = "Median of the version";
/// The stylesheet's `--accent`, so this run stands out the way it does in the table.
pub const CURRENT_COLOR: &str = "#ff9103";
const RUN_SYMBOL_SIZE: f64 = 8.0;
const CURRENT_SYMBOL_SIZE: f64 = 14.0;
/// The series name, then the name of the point, which holds the version and the value.
const ITEM_TOOLTIP: &str = "{a}<br/>{b}";

#[derive(Properties, PartialEq)]
pub struct SeriesChartProps {
    pub uuid: Uuid,
    pub metric: SeriesMetric,
    pub scope: SeriesScope,
}

/// The first two colors of the ECharts theme in light and dark mode. The group charts draw each
/// group in its theme color, so its actors and its average take that color too.
const LIGHT_GROUP_COLORS: [&str; 2] = ["#5470c6", "#91cc75"];
const DARK_GROUP_COLORS: [&str; 2] = ["#4992ff", "#7cffb2"];
const ACTOR_LINE_WIDTH: f64 = 1.0;
/// Light enough that the average stays readable over many actors, dark enough to show a peak.
const ACTOR_LINE_OPACITY: f64 = 0.5;
const AVERAGE_LINE_WIDTH: f64 = 3.0;
const LATENCY_AXIS_INDEX: usize = 0;

/// One line per actor group, or per actor.
#[function_component(SeriesChart)]
pub fn series_chart(props: &SeriesChartProps) -> Html {
    let series = use_state_eq(|| Loadable::Loading);
    let load = {
        let series = series.clone();
        let (uuid, metric, scope) = (props.uuid, props.metric, props.scope);
        Callback::from(move |()| {
            fetch_into(series.clone(), fetch_benchmark_series(uuid, metric, scope));
        })
    };
    {
        let load = load.clone();
        use_effect_with((props.uuid, props.metric, props.scope), move |_| {
            load.emit(())
        });
    }
    let chart = use_memo((*series).clone(), |series| match series {
        Loadable::Ready(series) if has_points(series) => {
            Some(SharedChart::new(series_options(series)))
        }
        _ => None,
    });

    let status = chart_status(&series, chart.is_some(), &load);
    let id = format!(
        "series-chart-{:?}-{:?}-{}",
        props.metric, props.scope, props.uuid
    );
    html! {
        <ChartCanvas {id} chart={(*chart).clone()}>{ status }</ChartCanvas>
    }
}

#[derive(Properties, PartialEq)]
pub struct ActorLatencyChartProps {
    pub uuid: Uuid,
}

/// A thin line per actor with its latency in each sampling window, under a thick line per group
/// with the group's moving average. The peaks of one actor stand out against the average, and the
/// slider zooms into them.
#[function_component(ActorLatencyChart)]
pub fn actor_latency_chart(props: &ActorLatencyChartProps) -> Html {
    let is_dark = use_context::<(bool, Callback<()>)>().is_some_and(|(is_dark, _)| is_dark);
    let lines = use_state_eq(|| Loadable::Loading);
    let load = {
        let (lines, uuid) = (lines.clone(), props.uuid);
        Callback::from(move |()| {
            fetch_into(lines.clone(), async move {
                let groups =
                    fetch_benchmark_series(uuid, SeriesMetric::Latency, SeriesScope::Group).await?;
                let actors =
                    fetch_benchmark_series(uuid, SeriesMetric::Latency, SeriesScope::Actors)
                        .await?;
                Ok(LatencyLines { groups, actors })
            });
        })
    };
    {
        let load = load.clone();
        use_effect_with(props.uuid, move |_| load.emit(()));
    }
    let chart = use_memo(
        ((*lines).clone(), is_dark),
        |(lines, is_dark)| match lines {
            Loadable::Ready(lines) if has_points(&lines.groups) || has_points(&lines.actors) => {
                Some(SharedChart::new(actor_latency_options(lines, *is_dark)))
            }
            _ => None,
        },
    );

    let status = chart_status(&lines, chart.is_some(), &load);
    let id = format!("actor-latency-chart-{}", props.uuid);
    html! {
        <ChartCanvas {id} chart={(*chart).clone()} class={classes!("run-chart-tall")}>
            { status }
        </ChartCanvas>
    }
}

/// The latency series of one run, of its groups and of its actors.
#[derive(Debug, Clone)]
struct LatencyLines {
    groups: BenchmarkSeries,
    actors: BenchmarkSeries,
}

#[derive(Properties, PartialEq)]
pub struct VersionChartProps {
    /// A run on the line, used to name the canvas.
    pub uuid: Uuid,
    pub chart: HistoryChart,
}

/// One point per run, a line through the median of each version, and the marked run, if any.
#[function_component(VersionChart)]
pub fn version_chart(props: &VersionChartProps) -> Html {
    let chart = use_memo(props.chart.clone(), |chart| {
        SharedChart::new(version_options(chart))
    });
    let id = format!("version-chart-{:?}-{}", props.chart.metric, props.uuid);
    html! { <ChartCanvas {id} chart={Some((*chart).clone())} /> }
}

#[derive(Properties, PartialEq)]
pub struct DistributionChartProps {
    pub uuid: Uuid,
    /// The light report that the page holds, which carries each group's distribution.
    pub report: Rc<BenchmarkReportLight>,
}

/// A histogram and its fitted curve per group, drawn from the light report, so the page never
/// downloads the full report.
#[function_component(DistributionChart)]
pub fn distribution_chart(props: &DistributionChartProps) -> Html {
    let chart = use_memo(props.report.clone(), |report| {
        SharedChart::new(distribution_options(report))
    });
    let id = format!("distribution-chart-{}", props.uuid);
    html! { <ChartCanvas {id} chart={Some((*chart).clone())} /> }
}

#[derive(Properties, PartialEq)]
pub struct PercentileChartProps {
    pub uuid: Uuid,
    /// The light report that the page holds, which carries each group's percentiles.
    pub report: Rc<BenchmarkReportLight>,
}

/// One line per actor group through its percentiles, from P50 to P99.99.
#[function_component(PercentileChart)]
pub fn percentile_chart(props: &PercentileChartProps) -> Html {
    let chart = use_memo(props.report.clone(), |report| {
        SharedChart::new(percentile_options(report))
    });
    let id = format!("percentile-chart-{}", props.uuid);
    html! { <ChartCanvas {id} chart={Some((*chart).clone())} /> }
}

/// A chart that compares by its allocation, because it can hold thousands of points per line.
/// Holding the `Rc` keeps the allocation alive, so a new chart never takes its address.
#[derive(Clone)]
pub struct SharedChart(Rc<Chart>);

impl SharedChart {
    pub fn new(chart: Chart) -> Self {
        Self(Rc::new(chart))
    }
}

impl PartialEq for SharedChart {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.0, &other.0)
    }
}

#[derive(Properties, PartialEq)]
pub struct ChartCanvasProps {
    /// Unique on the page.
    pub id: AttrValue,
    /// Nothing is drawn while `None`, for example while the data loads.
    pub chart: Option<SharedChart>,
    /// Over the chart, for example the loading state.
    #[prop_or_default]
    pub children: Html,
    /// Added to the frame, for example to change its height.
    #[prop_or_default]
    pub class: Classes,
}

/// Draws the chart at its frame's size and in the page's theme, and redraws on each change. A double
/// click zooms a chart with a zoom slider out to the whole run.
#[function_component(ChartCanvas)]
pub fn chart_canvas(props: &ChartCanvasProps) -> Html {
    let is_dark = use_context::<(bool, Callback<()>)>().is_some_and(|(is_dark, _)| is_dark);
    let frame = use_node_ref();
    let canvas = use_node_ref();
    let size = use_size(frame.clone());
    {
        let (id, canvas) = (props.id.clone(), canvas.clone());
        use_effect_with(
            (props.chart.clone(), size, is_dark),
            move |(chart, (width, height), is_dark)| {
                let element = canvas.cast::<Element>();
                if let (Some(SharedChart(chart)), Some(_)) = (chart, &element)
                    && *width > 0
                    && *height > 0
                {
                    render(&id, chart, (*width, *height), *is_dark);
                }
                move || {
                    if let Some(element) = element {
                        dispose_chart_at(&element);
                    }
                }
            },
        );
    }
    let ondblclick = {
        let canvas = canvas.clone();
        Callback::from(move |_: MouseEvent| {
            if let Some(element) = canvas.cast::<Element>() {
                reset_zoom_at(&element);
            }
        })
    };
    html! {
        <div class={classes!("run-chart", props.class.clone())} ref={frame}>
            <div id={props.id.clone()} class="run-chart-canvas" ref={canvas} {ondblclick}></div>
            <div class="run-chart-status">{ props.children.clone() }</div>
        </div>
    }
}

pub const fn metric_label(metric: SeriesMetric) -> &'static str {
    match metric {
        SeriesMetric::Latency => "Latency",
        SeriesMetric::Throughput => "Throughput",
    }
}

fn has_points(series: &BenchmarkSeries) -> bool {
    series.series.iter().any(|line| !line.points.is_empty())
}

/// Nothing once the chart is drawn, a note when the loaded report has no points, else the loading
/// or failed state.
fn chart_status<T>(data: &Loadable<T>, drawn: bool, reload: &Callback<()>) -> Html {
    match data {
        Loadable::Ready(_) if drawn => Html::default(),
        Loadable::Ready(_) => html! { <p class="data-status">{ NO_POINTS_NOTE }</p> },
        loading_or_failed => loadable(loading_or_failed, "the chart", reload, |_| Html::default()),
    }
}

fn distribution_options(report: &BenchmarkReportLight) -> Chart {
    let groups = report
        .group_metrics
        .iter()
        .map(|metrics| (metrics.summary.kind, metrics.latency_distribution.as_ref()));
    add_latency_distributions(IggyChart::card(), NO_AXIS_NAME, groups).inner
}

pub fn has_percentiles(report: &BenchmarkReportLight) -> bool {
    !percentile_lines(report).is_empty()
}

/// The percentiles span orders of magnitude, so a linear axis would flatten all but the last.
fn percentile_options(report: &BenchmarkReportLight) -> Chart {
    let categories = PERCENTILE_LABELS.map(str::to_owned).to_vec();
    let mut chart = IggyChart::card()
        .with_category_x_axis(NO_AXIS_NAME, categories)
        .with_log_y_axis(LATENCY_AXIS_NAME)
        .inner;
    for (name, values) in percentile_lines(report) {
        chart = chart.series(Line::new().name(name).data(values));
    }
    chart
}

/// Each actor group's name and percentiles, in the order of [`PERCENTILE_LABELS`]. A line stops
/// before the first value that the report left at 0, as older reports do for P99.99, because a log
/// axis has no place for 0. The combined group of producers and consumers is left out, as in the
/// series charts.
fn percentile_lines(report: &BenchmarkReportLight) -> Vec<(String, Vec<f64>)> {
    report
        .group_metrics
        .iter()
        .map(|group| &group.summary)
        .filter(|summary| summary.kind != GroupMetricsKind::ProducersAndConsumers)
        .map(|summary| {
            let values: Vec<f64> = [
                summary.average_p50_latency_ms,
                summary.average_p90_latency_ms,
                summary.average_p95_latency_ms,
                summary.average_p99_latency_ms,
                summary.average_p999_latency_ms,
                summary.average_p9999_latency_ms,
            ]
            .into_iter()
            .take_while(|value| *value > 0.0)
            .collect();
            (summary.kind.to_string(), values)
        })
        .filter(|(_, values)| !values.is_empty())
        .collect()
}

fn series_options(series: &BenchmarkSeries) -> Chart {
    let mut chart = IggyChart::card().with_time_x_axis().with_y_axis(&format!(
        "{} [{}]",
        metric_label(series.metric),
        series.unit
    ));
    for line in &series.series {
        chart = chart.add_time_series(
            &line.source.to_string(),
            chart_points(line),
            None,
            LINE_OPACITY,
        );
    }
    chart.inner
}

/// The averages come after the actors, so they're drawn over them. The cross pointer shows the time
/// and the latency at the pointer on the axes.
fn actor_latency_options(lines: &LatencyLines, is_dark: bool) -> Chart {
    let palette = if is_dark {
        DARK_GROUP_COLORS
    } else {
        LIGHT_GROUP_COLORS
    };
    let groups: Vec<GroupMetricsKind> = lines
        .groups
        .series
        .iter()
        .map(|line| line.source.group())
        .collect();
    let color = |line: &ChartSeries| {
        groups
            .iter()
            .position(|group| *group == line.source.group())
            .and_then(|index| palette.get(index).copied())
    };
    let mut chart = IggyChart::zoomable_card()
        .with_time_x_axis()
        .with_y_axis(LATENCY_AXIS_NAME);
    for line in &lines.actors.series {
        chart = chart.add_dual_time_line_series(
            &line.source.to_string(),
            chart_points(line),
            color(line),
            ACTOR_LINE_OPACITY,
            LATENCY_AXIS_INDEX,
            ACTOR_LINE_WIDTH,
        );
    }
    for line in &lines.groups.series {
        chart = chart.add_dual_time_line_series(
            &format!("{} average", line.source),
            chart_points(line),
            color(line),
            LINE_OPACITY,
            LATENCY_AXIS_INDEX,
            AVERAGE_LINE_WIDTH,
        );
    }
    chart.inner.tooltip(
        Tooltip::new()
            .trigger(Trigger::Axis)
            .axis_pointer(AxisPointer::new().type_(AxisPointerType::Cross)),
    )
}

fn chart_points(line: &ChartSeries) -> Vec<Vec<f64>> {
    line.points
        .iter()
        .map(|point| vec![point.time_s, point.value])
        .collect()
}

fn version_options(chart: &HistoryChart) -> Chart {
    let metric = chart.metric;
    let unit = match metric {
        Metric::Throughput => "MB/s",
        Metric::P50 | Metric::P90 | Metric::P95 | Metric::P99 | Metric::P999 => "ms",
    };
    let named = |point: &VersionPoint, what: &str| {
        let version = chart.versions.get(point.version).map_or("", String::as_str);
        (
            vec![point.version as f64, point.value],
            format!(
                "{version}{what}: {}",
                metric_value_text(metric, point.value)
            ),
        )
    };
    let named_all = |points: &[VersionPoint], what: &str| -> Vec<_> {
        points.iter().map(|point| named(point, what)).collect()
    };
    let base = IggyChart::card()
        .with_category_x_axis(NO_AXIS_NAME, chart.versions.clone())
        .with_y_axis(&format!("{} [{unit}]", metric_name(metric)));
    let options = base
        .inner
        .tooltip(
            Tooltip::new()
                .trigger(Trigger::Item)
                .formatter(ITEM_TOOLTIP),
        )
        .series(
            Scatter::new()
                .name(RUNS_NAME)
                .symbol_size(RUN_SYMBOL_SIZE)
                .data(named_all(&chart.runs, "")),
        )
        .series(
            Line::new()
                .name(MEDIAN_NAME)
                .data(named_all(&chart.medians, " median")),
        );
    // The legend names every series, so a chart without a run to mark has no such series.
    match &chart.current {
        Some(current) => options.series(
            Scatter::new()
                .name(CURRENT_LABEL)
                .symbol_size(CURRENT_SYMBOL_SIZE)
                .item_style(ItemStyle::new().color(CURRENT_COLOR))
                .data(vec![named(current, "")]),
        ),
        None => options,
    }
}

fn render(canvas_id: &str, chart: &Chart, (width, height): (u32, u32), is_dark: bool) {
    let theme = if is_dark { Theme::Dark } else { Theme::Default };
    if let Err(render_error) = WasmRenderer::new(width, height)
        .theme(theme)
        .render(canvas_id, chart)
    {
        error!(format!(
            "Couldn't draw the {canvas_id} chart: {render_error}"
        ));
    }
}
