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

//! The Advanced section: views for engineers, closed by default. Its views only mount, and so only
//! fetch every actor's series, once the reader first opens the section.

use bench_dashboard_shared::BenchmarkReportLight;
use bench_dashboard_shared::series::{SeriesMetric, SeriesScope};
use uuid::Uuid;
use web_sys::Element;
use yew::prelude::*;

use super::chart::{
    ActorLatencyChart, DistributionChart, PercentileChart, SeriesChart, has_percentiles,
};
use super::fact_group_card;
use super::facts::server_facts;
use crate::components::page::loadable;
use crate::state::data::Loadable;

const OPEN_ATTRIBUTE: &str = "open";
const DISTRIBUTION_NOTE: &str = "Latency in ms on the x axis. The last bar also counts the slower \
                                 samples past the range, and the line is the fitted log-normal \
                                 curve.";
const ACTOR_LATENCY_NOTE: &str = "Each thin line is one actor: the average latency of its batches \
                                  in each sampling window, the finest detail that the report \
                                  keeps. The thick line is the group's moving average. Drag the \
                                  ends of the slider to zoom into a peak, and double click the \
                                  chart to see the whole run again.";
const SERVER_NOTE: &str = "What the server reported about itself at the end of the run.";
const PERCENTILE_NOTE: &str = "Each point is one percentile averaged over the actors. The axis is \
                               logarithmic because the tail can sit orders of magnitude above the \
                               median.";

#[derive(Properties, PartialEq)]
pub struct AdvancedProps {
    pub uuid: Uuid,
    /// The light report that the page holds.
    pub report: Loadable<BenchmarkReportLight>,
    pub reload: Callback<()>,
}

#[function_component(Advanced)]
pub fn advanced(props: &AdvancedProps) -> Html {
    let opened = use_state_eq(|| false);
    let ontoggle = {
        let opened = opened.clone();
        Callback::from(move |event: Event| {
            if event
                .target_dyn_into::<Element>()
                .is_some_and(|details| details.has_attribute(OPEN_ATTRIBUTE))
            {
                opened.set(true);
            }
        })
    };
    html! {
        <details class="run-advanced" {ontoggle}>
            <summary>{"Show the views for engineers"}</summary>
            if *opened {
                <div class="run-advanced-body">
                    { views(props) }
                </div>
            }
        </details>
    }
}

fn views(props: &AdvancedProps) -> Html {
    let uuid = props.uuid;
    let distribution = match &props.report {
        Loadable::Ready(report) if report.has_latency_distribution() => Some(report.clone()),
        _ => None,
    };
    let percentiles = match &props.report {
        Loadable::Ready(report) if has_percentiles(report) => Some(report.clone()),
        _ => None,
    };
    html! {
        <>
            <article class="card run-chart-card">
                <h3>{"Latency of every actor"}</h3>
                <ActorLatencyChart {uuid} />
                <p class="card-note">{ ACTOR_LATENCY_NOTE }</p>
            </article>
            <div class="run-charts">
                <article class="card run-chart-card">
                    <h3>{"Throughput per actor"}</h3>
                    <SeriesChart {uuid} metric={SeriesMetric::Throughput} scope={SeriesScope::Actors} />
                </article>
                if let Some(report) = distribution {
                    <article class="card run-chart-card">
                        <h3>{"Latency distribution"}</h3>
                        <DistributionChart {uuid} {report} />
                        <p class="card-note">{ DISTRIBUTION_NOTE }</p>
                    </article>
                }
                if let Some(report) = percentiles {
                    <article class="card run-chart-card">
                        <h3>{"Latency percentiles"}</h3>
                        <PercentileChart {uuid} {report} />
                        <p class="card-note">{ PERCENTILE_NOTE }</p>
                    </article>
                }
            </div>
            { loadable(&props.report, "the report of the run", &props.reload, |report| html! {
                <>
                    <p class="section-text">{ SERVER_NOTE }</p>
                    <div class="card-columns">
                        { for server_facts(&report.server_stats).iter().map(fact_group_card) }
                    </div>
                </>
            }) }
        </>
    }
}
