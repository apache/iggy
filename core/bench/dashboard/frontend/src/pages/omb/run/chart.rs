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
//! histograms. The shared crate picks their lines, so the embeds draw the same ones.

use std::rc::Rc;

use bench_dashboard_shared::omb::chart::{
    QuantileGroup, WindowMetric, quantile_group_options, window_options,
};
use bench_dashboard_shared::omb::detail::OmbRunDetail;
use bench_dashboard_shared::omb::id::OmbRunId;
use bench_report::plotting::chart::IggyChart;
use charming::Chart;
use charming::element::{ItemStyle, Tooltip, Trigger};
use charming::series::{Line, Scatter};
use yew::prelude::*;

use super::history::{CURRENT_LABEL, HistoryChart, HistoryPoint};
use crate::components::chart::{ChartCanvas, SharedChart};
use crate::pages::omb::common::LineOrder;
use crate::pages::run::chart::{CURRENT_COLOR, MEDIAN_NAME};

const NO_POINTS_NOTE: &str = "The result file has no values for this chart.";
/// A card chart has no room for a name at the end of its x axis, so the labels or the card's text
/// say what the axis shows.
const NO_AXIS_NAME: &str = "";
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

#[derive(Properties, PartialEq)]
pub struct WindowChartProps {
    pub detail: DetailRef,
    pub metric: WindowMetric,
}

/// The lines of one metric over the run, one point per window at its end.
#[function_component(WindowChart)]
pub fn window_chart(props: &WindowChartProps) -> Html {
    let chart = use_memo((props.detail.clone(), props.metric), |(detail, metric)| {
        window_options(IggyChart::card(), *metric, &detail.0).map(SharedChart::new)
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
        quantile_group_options(IggyChart::card(), *group, &detail.0).map(SharedChart::new)
    });
    let id = format!(
        "omb-quantile-chart-{:?}-{}",
        props.group, props.detail.0.summary.id
    );
    html! {
        <ChartCanvas {id} chart={(*chart).clone()}>{ no_points_status(chart.is_some()) }</ChartCanvas>
    }
}

fn no_points_status(drawn: bool) -> Html {
    if drawn {
        return Html::default();
    }
    html! { <p class="data-status">{ NO_POINTS_NOTE }</p> }
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
