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

//! The charts of the OMB compare page, with one line per run in the run's color: the publish
//! throughput and the publish P99 over the run, and the latency quantile curves.

use bench_dashboard_shared::omb::chart::{LATENCY_AXIS_NAME, LINE_OPACITY, window_points};
use bench_dashboard_shared::omb::detail::OmbRunDetail;
use bench_dashboard_shared::omb::id::OmbRunId;
use bench_dashboard_shared::omb::quantile::{QuantileCurve, quantile_options};
use bench_dashboard_shared::omb::result::OmbQuantile;
use bench_report::plotting::chart::IggyChart;
use charming::Chart;
use yew::prelude::*;

use crate::components::chart::{ChartCanvas, SharedChart};
use crate::components::page::loadable;
use crate::state::data::Loadable;

const THROUGHPUT_AXIS_NAME: &str = "Publish throughput [MiB/s]";
const P99_AXIS_NAME: &str = "Publish P99 [ms]";
const NO_POINTS_NOTE: &str = "The results of these runs have no points for this chart.";
const CHART_WHAT: &str = "the chart";

/// One run's line.
#[derive(Debug, Clone, PartialEq)]
pub struct ChartRun {
    pub id: OmbRunId,
    pub name: String,
    pub color: &'static str,
}

/// What a chart shows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChartKind {
    /// Publish MiB/s in each window.
    PublishThroughput,
    /// The publish P99 of each window.
    PublishP99,
    /// The quantile curve of the publish latency over the whole run.
    PublishQuantiles,
    /// The quantile curve of the end-to-end latency over the whole run. A run that didn't measure
    /// it has no line.
    EndToEndQuantiles,
}

#[derive(Properties, PartialEq)]
pub struct CompareChartProps {
    pub kind: ChartKind,
    /// In legend order.
    pub runs: Vec<ChartRun>,
    /// The details that the page fetched, in any order.
    pub details: Loadable<Vec<OmbRunDetail>>,
    /// Fetches the details again after a failure.
    pub reload: Callback<()>,
}

/// The page fetches each run's detail once for all of its charts, so a chart only draws.
#[function_component(CompareChart)]
pub fn compare_chart(props: &CompareChartProps) -> Html {
    let chart = use_memo(
        (props.kind, props.runs.clone(), props.details.clone()),
        |(kind, runs, details)| match details {
            Loadable::Ready(details) => chart_options(*kind, runs, details).map(SharedChart::new),
            Loadable::Loading | Loadable::Failed(_) => None,
        },
    );
    let status = match (&props.details, chart.is_some()) {
        (Loadable::Ready(_), true) => Html::default(),
        (Loadable::Ready(_), false) => html! { <p class="data-status">{ NO_POINTS_NOTE }</p> },
        (loading_or_failed, _) => loadable(loading_or_failed, CHART_WHAT, &props.reload, |_| {
            Html::default()
        }),
    };
    let ids: Vec<String> = props.runs.iter().map(|run| run.id.to_string()).collect();
    let id = format!("omb-compare-chart-{:?}-{}", props.kind, ids.join("-"));
    html! {
        <ChartCanvas {id} chart={(*chart).clone()}>{ status }</ChartCanvas>
    }
}

/// `None` when no run has a point to draw.
fn chart_options(kind: ChartKind, runs: &[ChartRun], details: &[OmbRunDetail]) -> Option<Chart> {
    match kind {
        ChartKind::PublishThroughput => time_options(THROUGHPUT_AXIS_NAME, runs, |run| {
            let series = &detail_of(details, run.id)?.series;
            Some(window_points(
                &series.window_ends(),
                &series.publish_mebibytes_per_second,
            ))
        }),
        ChartKind::PublishP99 => time_options(P99_AXIS_NAME, runs, |run| {
            let series = &detail_of(details, run.id)?.series;
            Some(window_points(
                &series.window_ends(),
                &series.publish_latency_ms.p99,
            ))
        }),
        ChartKind::PublishQuantiles => quantile_chart(&quantile_curves(runs, |run| {
            Some(
                detail_of(details, run.id)?
                    .quantiles
                    .publish_latency_ms
                    .as_slice(),
            )
        })),
        ChartKind::EndToEndQuantiles => quantile_chart(&quantile_curves(runs, |run| {
            detail_of(details, run.id)?
                .quantiles
                .end_to_end_latency_ms
                .as_deref()
        })),
    }
}

/// One curve per run that has one, in the run's color.
fn quantile_curves<'a>(
    runs: &[ChartRun],
    curve_of: impl Fn(&ChartRun) -> Option<&'a [OmbQuantile]>,
) -> Vec<QuantileCurve> {
    runs.iter()
        .filter_map(|run| QuantileCurve::new(run.name.as_str(), curve_of(run)?, Some(run.color)))
        .collect()
}

/// The run page's quantile chart with every run's curve. `None` without curves.
fn quantile_chart(curves: &[QuantileCurve]) -> Option<Chart> {
    (!curves.is_empty()).then(|| quantile_options(IggyChart::card(), LATENCY_AXIS_NAME, curves))
}

fn detail_of(details: &[OmbRunDetail], id: OmbRunId) -> Option<&OmbRunDetail> {
    details.iter().find(|detail| detail.summary.id == id)
}

/// One line per run against the seconds since the test phase started.
fn time_options(
    axis_name: &str,
    runs: &[ChartRun],
    points_of: impl Fn(&ChartRun) -> Option<Vec<Vec<f64>>>,
) -> Option<Chart> {
    let mut chart = IggyChart::card().with_time_x_axis().with_y_axis(axis_name);
    let mut drawn = false;
    for run in runs {
        if let Some(points) = points_of(run).filter(|points| !points.is_empty()) {
            chart = chart.add_time_series(&run.name, points, Some(run.color), LINE_OPACITY);
            drawn = true;
        }
    }
    drawn.then_some(chart.inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{OMB_NO_CONSUMERS, OMB_RC1_DEFAULT, OMB_RC1_PERSISTED};
    use bench_dashboard_shared::omb::quantile::curve_points;

    fn chart_run(detail: &OmbRunDetail, color: &'static str) -> ChartRun {
        ChartRun {
            id: detail.summary.id,
            name: detail.summary.driver.clone(),
            color,
        }
    }

    #[test]
    fn given_fixture_runs_when_curved_should_draw_run_page_points_in_run_colors() {
        let details = [OMB_RC1_DEFAULT.detail(), OMB_RC1_PERSISTED.detail()];
        let runs = [
            chart_run(&details[0], "#147ce5"),
            chart_run(&details[1], "#8b5cf6"),
        ];

        let curves = quantile_curves(&runs, |run| {
            Some(
                detail_of(&details, run.id)?
                    .quantiles
                    .publish_latency_ms
                    .as_slice(),
            )
        });

        assert_eq!(curves.len(), 2);
        for ((curve, run), detail) in curves.iter().zip(&runs).zip(&details) {
            assert_eq!(curve.name, run.name);
            assert_eq!(curve.color, Some(run.color));
            assert_eq!(
                curve.points,
                curve_points(&detail.quantiles.publish_latency_ms)
            );
        }
    }

    #[test]
    fn given_run_without_consumers_when_end_to_end_charted_should_draw_other_runs_only() {
        let details = [OMB_RC1_DEFAULT.detail(), OMB_NO_CONSUMERS.detail()];
        let runs = [
            chart_run(&details[0], "#147ce5"),
            chart_run(&details[1], "#8b5cf6"),
        ];

        let chart = chart_options(ChartKind::EndToEndQuantiles, &runs, &details);
        let alone = chart_options(ChartKind::EndToEndQuantiles, &runs[1..], &details);

        assert!(chart.is_some());
        assert_eq!(alone, None);
        assert!(chart_options(ChartKind::PublishQuantiles, &runs[1..], &details).is_some());
    }

    #[test]
    fn given_fetched_details_when_charted_over_time_should_draw_runs_found_in_details() {
        let details = [OMB_RC1_PERSISTED.detail()];
        let fetched = [chart_run(&details[0], "#147ce5")];
        let missing = [chart_run(&OMB_RC1_DEFAULT.detail(), "#8b5cf6")];

        for kind in [ChartKind::PublishThroughput, ChartKind::PublishP99] {
            assert!(
                chart_options(kind, &fetched, &details).is_some(),
                "{kind:?}"
            );
            assert_eq!(chart_options(kind, &missing, &details), None, "{kind:?}");
        }
    }
}
