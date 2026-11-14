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

//! A chart of one metric over the run time, with one line per compared run.

use bench_dashboard_shared::series::{BenchmarkSeries, SeriesMetric, SeriesScope, SeriesSource};
use bench_report::group_metrics_kind::GroupMetricsKind;
use bench_report::plotting::chart::IggyChart;
use uuid::Uuid;
use yew::prelude::*;

use super::holds_every;
use crate::api::fetch_benchmark_series;
use crate::components::page::loadable;
use crate::pages::run::chart::{ChartCanvas, LINE_OPACITY, SharedChart, metric_label};
use crate::state::data::{Loadable, fetch_into};

const NO_POINTS_NOTE: &str = "The reports for these runs have no time series.";

/// One run's line.
#[derive(Debug, Clone, PartialEq)]
pub struct ChartRun {
    pub uuid: Uuid,
    /// The run's main group.
    pub group: GroupMetricsKind,
    pub name: String,
    pub color: &'static str,
}

#[derive(Properties, PartialEq)]
pub struct OverlayChartProps {
    /// In legend order.
    pub runs: Vec<ChartRun>,
    pub metric: SeriesMetric,
}

/// Each run's series are fetched one after the other. Two or three small downsampled series don't
/// justify another dependency just to fetch them concurrently.
#[function_component(OverlayChart)]
pub fn overlay_chart(props: &OverlayChartProps) -> Html {
    let series = use_state_eq(|| Loadable::Loading);
    let uuids: Vec<Uuid> = props.runs.iter().map(|run| run.uuid).collect();
    let load = {
        let (series, uuids, metric) = (series.clone(), uuids.clone(), props.metric);
        Callback::from(move |()| {
            let uuids = uuids.clone();
            fetch_into(series.clone(), async move {
                let mut fetched = Vec::with_capacity(uuids.len());
                for uuid in uuids {
                    let run = fetch_benchmark_series(uuid, metric, SeriesScope::Group).await?;
                    fetched.push((uuid, run));
                }
                Ok(fetched)
            });
        })
    };
    // The chart only fetches when the series it holds are missing a run. After a removal or a new
    // baseline, it keeps them and draws the new lines without loading. A late answer for older runs
    // that's missing a run makes it fetch again.
    let held = holds_every(&series, &uuids, |(uuid, run_series), run| {
        uuid == run && run_series.metric == props.metric
    });
    {
        let load = load.clone();
        use_effect_with((uuids.clone(), props.metric, held), move |(_, _, held)| {
            if !held {
                load.emit(());
            }
        });
    }
    let chart = use_memo(
        ((*series).clone(), props.runs.clone()),
        |(series, runs)| match series {
            Loadable::Ready(series) => overlay_options(runs, series),
            _ => None,
        },
    );

    let status = match (&*series, &*chart) {
        (Loadable::Ready(_), Some(_)) => Html::default(),
        (Loadable::Ready(_), None) => html! { <p class="data-status">{ NO_POINTS_NOTE }</p> },
        (loading_or_failed, _) => {
            loadable(loading_or_failed, "the chart", &load, |_| Html::default())
        }
    };
    let ids: Vec<String> = uuids.iter().map(Uuid::to_string).collect();
    let id = format!("compare-chart-{:?}-{}", props.metric, ids.join("-"));
    html! {
        <ChartCanvas {id} chart={(*chart).clone()}>{ status }</ChartCanvas>
    }
}

/// `None` when no run has a point for its main group.
fn overlay_options(runs: &[ChartRun], series: &[(Uuid, BenchmarkSeries)]) -> Option<SharedChart> {
    let (_, first) = series.first()?;
    let mut chart = IggyChart::card().with_time_x_axis().with_y_axis(&format!(
        "{} [{}]",
        metric_label(first.metric),
        first.unit
    ));
    let mut drawn = false;
    for run in runs {
        let line = series
            .iter()
            .filter(|(uuid, _)| *uuid == run.uuid)
            .flat_map(|(_, run_series)| &run_series.series)
            .find(|line| line.source == SeriesSource::Group(run.group) && !line.points.is_empty());
        if let Some(line) = line {
            let points = line
                .points
                .iter()
                .map(|point| vec![point.time_s, point.value])
                .collect();
            chart = chart.add_time_series(&run.name, points, Some(run.color), LINE_OPACITY);
            drawn = true;
        }
    }
    drawn.then(|| SharedChart::new(chart.inner))
}
