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

//! A headline number with its change against the previous version: the run page's KPIs and the
//! Overview's cards. They share one look, so a change reads the same on every page.

use bench_dashboard_shared::delta::{DeltaVerdict, Metric};
use yew::prelude::*;

use crate::pages::run::change::{Change, Headline, metric_value_text};
use crate::pages::run::history::NO_VALUE;

const NOISE_NOTE: &str = "within run-to-run noise";
const FLAGGED_NOTE: &str = "not judged: a run has a badge";
const PARAMETERS_NOTE: &str = "not judged: the parameters differ";
const BETTER_LOOK: &str = "run-change-better";
const WORSE_LOOK: &str = "run-change-worse";
const GRAY_LOOK: &str = "run-change-gray";

/// How a change reads: judged by the delta rule, or shown without a judgment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Judgment {
    Rule(DeltaVerdict),
    /// Between runs whose parameters differ, a change says what the parameters did, not which
    /// build is better.
    ParametersDiffer,
}

/// `extra` follows the change, inside the card.
pub fn headline_card(
    label: &str,
    headline: &Headline,
    detail: Option<String>,
    extra: Html,
) -> Html {
    let value = headline.value.map_or_else(
        || NO_VALUE.to_owned(),
        |value| metric_value_text(headline.metric, value),
    );
    html! {
        <div class="run-kpi">
            <span class="run-kpi-label" title={metric_hint(headline.metric)}>
                { label.to_owned() }
            </span>
            <strong class="run-kpi-value">{ value }</strong>
            if let Some(detail) = detail {
                <small>{ detail }</small>
            }
            if let Some(change) = &headline.change {
                { change_chip(change) }
            }
            { extra }
        </div>
    }
}

pub fn change_chip(change: &Change) -> Html {
    judged_chip(
        change.text(),
        change.ranges.clone(),
        Judgment::Rule(change.verdict),
    )
}

/// `text` is the change, and `ranges` names the values that it compares.
pub fn judged_chip(text: String, ranges: String, judgment: Judgment) -> Html {
    html! {
        <span class="run-change-wrap">
            <span class={classes!("run-change", judgment_look(judgment))} title={ranges}>
                { text }
            </span>
            if let Some(note) = judgment_note(judgment) {
                <span class="run-change-note">{ note }</span>
            }
        </span>
    }
}

/// The chip alone, for a narrow card. Its note, if any, follows the compared values in its
/// tooltip, and its color still tells a real change from noise.
pub fn compact_judged_chip(text: String, ranges: String, judgment: Judgment) -> Html {
    let title = match judgment_note(judgment) {
        Some(note) => format!("{ranges} Change: {note}."),
        None => ranges,
    };
    html! {
        <span class={classes!("run-change", judgment_look(judgment))} {title}>{ text }</span>
    }
}

const fn judgment_look(judgment: Judgment) -> &'static str {
    match judgment {
        Judgment::Rule(verdict) => verdict_look(verdict),
        Judgment::ParametersDiffer => GRAY_LOOK,
    }
}

/// Why a gray change has no color. A better or worse change needs no note.
const fn judgment_note(judgment: Judgment) -> Option<&'static str> {
    match judgment {
        Judgment::Rule(DeltaVerdict::Better | DeltaVerdict::Worse) => None,
        Judgment::Rule(DeltaVerdict::Noise) => Some(NOISE_NOTE),
        Judgment::Rule(DeltaVerdict::Flagged) => Some(FLAGGED_NOTE),
        Judgment::ParametersDiffer => Some(PARAMETERS_NOTE),
    }
}

/// The chip color for `verdict`. Only a better or worse change gets a color.
pub const fn verdict_look(verdict: DeltaVerdict) -> &'static str {
    match verdict {
        DeltaVerdict::Better => BETTER_LOOK,
        DeltaVerdict::Worse => WORSE_LOOK,
        DeltaVerdict::Noise | DeltaVerdict::Flagged => GRAY_LOOK,
    }
}

/// What a headline number means, in the Methodology page's words.
pub const fn metric_hint(metric: Metric) -> &'static str {
    match metric {
        Metric::Throughput => {
            "The user data the group moved, divided by its slowest actor's run time."
        }
        Metric::P50 => {
            "The latency that half of an actor's batches didn't exceed, averaged over the actors."
        }
        Metric::P90 => {
            "The latency that 90 percent of an actor's batches didn't exceed, averaged over the \
             actors."
        }
        Metric::P95 => {
            "The latency that 95 percent of an actor's batches didn't exceed, averaged over the \
             actors."
        }
        Metric::P99 => {
            "The latency that 99 percent of an actor's batches didn't exceed, averaged over the \
             actors."
        }
        Metric::P999 => {
            "The latency that 99.9 percent of an actor's batches didn't exceed, averaged over the \
             actors."
        }
    }
}
