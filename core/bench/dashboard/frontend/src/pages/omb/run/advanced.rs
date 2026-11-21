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

//! The Advanced section: the additional insights, closed by default. Its charts only mount once the
//! reader first opens the section, because the percentile curves hold thousands of points.

use bench_dashboard_shared::embed::OmbChartType;
use bench_dashboard_shared::omb::chart::{QuantileGroup, WindowMetric};
use bench_dashboard_shared::omb::detail::{OmbRunDetail, OmbSeries};
use web_sys::Element;
use yew::prelude::*;

use super::chart::{DetailRef, QuantileChart, WindowChart};
use super::with_detail;
use crate::components::embed::{EmbedButton, EmbedChart};
use crate::state::data::Loadable;

const OPEN_ATTRIBUTE: &str = "open";
const LATENCY_CURVES_NOTE: &str = "The latency at every percentile of the histogram that OMB merges \
                                   over the measured phase. The x axis counts nines, so P99, P99.9 \
                                   and P99.99 get as much room as the median, and the y axis is \
                                   logarithmic. The end-to-end values are whole milliseconds.";
const DELAY_CURVE_NOTE: &str = "How late the producers sent their messages against the schedule \
                                of the target rate, at every percentile.";
const DELAY_NOTE: &str = "A growing delay means that the producers couldn't keep up with the \
                          target rate.";
const BACKLOG_NOTE: &str = "Messages sent but not yet received, over all subscriptions, at the end \
                           of each window. A backlog that grows means that the consumers fell \
                           behind.";
const ERRORS_NOTE: &str = "Failed sends per second in each window.";
const NO_ERRORS_NOTE: &str = "No publish errors in this run.";

#[derive(Properties, PartialEq)]
pub struct AdvancedProps {
    pub detail: Loadable<OmbRunDetail>,
    pub reload: Callback<()>,
}

/// What the publish errors card shows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublishErrors {
    /// A file from an older OMB doesn't record errors, so the section has no card for them.
    NotRecorded,
    /// Every window has zero errors, so a sentence says so in place of a flat chart.
    NoneFailed,
    SomeFailed,
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
            <summary>{"Show the additional insights"}</summary>
            if *opened {
                <div class="run-advanced-body">
                    { with_detail(&props.detail, &props.reload, views) }
                </div>
            }
        </details>
    }
}

impl PublishErrors {
    fn of(series: &OmbSeries) -> Self {
        let rates = &series.publish_errors_per_second;
        if rates.is_empty() {
            Self::NotRecorded
        } else if rates.iter().any(|rate| *rate > 0.0) {
            Self::SomeFailed
        } else {
            Self::NoneFailed
        }
    }
}

fn views(detail: &DetailRef) -> Html {
    let series = &detail.0.series;
    let delay = series.publish_delay_latency_us.is_some();
    html! {
        // The percentile curves take whole rows, so each nine of their axis has room for its label.
        <>
            <article class="card run-chart-card">
                <h3>{"Latency percentiles"}</h3>
                <EmbedButton chart={EmbedChart::Omb(OmbChartType::Quantiles)} />
                <QuantileChart detail={detail.clone()} group={QuantileGroup::Latency} />
                <p class="card-note">{ LATENCY_CURVES_NOTE }</p>
            </article>
            if delay {
                <article class="card run-chart-card">
                    <h3>{"Publish delay percentiles"}</h3>
                    <QuantileChart detail={detail.clone()} group={QuantileGroup::PublishDelay} />
                    <p class="card-note">{ DELAY_CURVE_NOTE }</p>
                </article>
            }
            <div class="run-charts">
                if delay {
                    { window_card(detail, WindowMetric::PublishDelay, DELAY_NOTE) }
                }
                { window_card(detail, WindowMetric::Backlog, BACKLOG_NOTE) }
                { errors_card(detail, PublishErrors::of(series)) }
            </div>
        </>
    }
}

fn window_card(detail: &DetailRef, metric: WindowMetric, note: &'static str) -> Html {
    html! {
        <article class="card run-chart-card">
            <h3>{ card_title(metric) }</h3>
            <WindowChart detail={detail.clone()} {metric} />
            <p class="card-note">{ note }</p>
        </article>
    }
}

fn errors_card(detail: &DetailRef, errors: PublishErrors) -> Html {
    match errors {
        PublishErrors::NotRecorded => Html::default(),
        PublishErrors::NoneFailed => html! {
            <article class="card run-chart-card">
                <h3>{ card_title(WindowMetric::PublishErrors) }</h3>
                <p>{ NO_ERRORS_NOTE }</p>
            </article>
        },
        PublishErrors::SomeFailed => window_card(detail, WindowMetric::PublishErrors, ERRORS_NOTE),
    }
}

fn card_title(metric: WindowMetric) -> String {
    format!("{} over the run", metric.title())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{OMB_PUBLISH_ERRORS, OMB_RC1_DEFAULT};

    #[test]
    fn given_fixture_errors_when_carded_should_chart_only_runs_with_errors() {
        let without_errors = OMB_RC1_DEFAULT.detail().series;
        let with_errors = OMB_PUBLISH_ERRORS.detail().series;
        let mut older_omb = without_errors.clone();
        older_omb.publish_errors_per_second.clear();

        assert!(!without_errors.publish_errors_per_second.is_empty());
        assert_eq!(
            PublishErrors::of(&without_errors),
            PublishErrors::NoneFailed
        );
        assert_eq!(PublishErrors::of(&with_errors), PublishErrors::SomeFailed);
        assert_eq!(PublishErrors::of(&older_omb), PublishErrors::NotRecorded);
    }
}
