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

//! A row's toggle that picks its run to compare, and the tray at the bottom of the window that
//! holds the picked runs.

use bench_dashboard_shared::summary::BenchmarkSummary;
use uuid::Uuid;
use yew::prelude::*;

use crate::components::run_labels::run_name;
use crate::pages::compare::{CompareLink, oldest_first};
use crate::state::data::{Loadable, use_dashboard_data};
use crate::state::selection::{MAX_COMPARED_RUNS, SelectionAction, SelectionHandle, use_selection};

const ADD_LABEL: &str = "Add to compare";
const REMOVE_LABEL: &str = "Remove from compare";
const ONE_MORE_NOTE: &str = "Pick one more run";
const COMPARE_LABEL: &str = "Compare";
const CLEAR_LABEL: &str = "Clear";

#[derive(Properties, PartialEq)]
pub struct CompareToggleProps {
    pub run: Uuid,
}

/// Puts a row's run into the tray, or takes it out. A full tray doesn't take another run.
#[function_component(CompareToggle)]
pub fn compare_toggle(props: &CompareToggleProps) -> Html {
    let Some(selection) = use_selection() else {
        return Html::default();
    };
    let picked = selection.contains(props.run);
    let blocked = !picked && selection.is_full();
    let label = if picked { REMOVE_LABEL } else { ADD_LABEL };
    let title = if blocked {
        format!("Compare takes at most {MAX_COMPARED_RUNS} runs. Take one out of the tray first.")
    } else {
        label.to_owned()
    };
    let run = props.run;
    let onclick =
        Callback::from(move |_: MouseEvent| selection.dispatch(SelectionAction::Toggle(run)));
    html! {
        <button
            type="button"
            class="compare-toggle"
            aria-pressed={picked.to_string()}
            disabled={blocked}
            {title}
            {onclick}
        >
            { if picked { check_icon() } else { plus_icon() } }
            <span class="visually-hidden">{ label }</span>
        </button>
    }
}

/// Floats over the page while it holds a run.
#[function_component(CompareTray)]
pub fn compare_tray() -> Html {
    let data = use_dashboard_data();
    let selection = use_selection();
    let (Some(selection), Loadable::Ready(summaries)) = (selection, &data.summaries) else {
        return Html::default();
    };
    let picked = tray_runs(selection.runs(), summaries);
    if picked.is_empty() {
        return Html::default();
    }
    let ids: Vec<Uuid> = picked.iter().map(|run| run.uuid).collect();
    let clear = {
        let selection = selection.clone();
        Callback::from(move |_: MouseEvent| selection.dispatch(SelectionAction::Clear))
    };
    html! {
        <>
            // Keeps the end of the page clear of the tray.
            <div class="compare-tray-space" aria-hidden="true"></div>
            <aside class="compare-tray" aria-label="Runs to compare">
                <p class="compare-tray-count">
                    <strong>{ format!("{} of {MAX_COMPARED_RUNS}", picked.len()) }</strong>
                    <span>{"runs to compare"}</span>
                </p>
                <ul class="compare-tray-runs">
                    { for picked.iter().map(|run| tray_run(run, &selection)) }
                </ul>
                <div class="compare-tray-actions">
                    if ids.len() > 1 {
                        <CompareLink runs={ids} classes={classes!("run-action", "compare-tray-go")}>
                            { COMPARE_LABEL }
                        </CompareLink>
                    } else {
                        <span class="compare-tray-hint">{ ONE_MORE_NOTE }</span>
                    }
                    <button type="button" class="run-action" onclick={clear}>
                        { CLEAR_LABEL }
                    </button>
                </div>
            </aside>
        </>
    }
}

/// The picked runs that are in the loaded list, oldest first, which is the compare page's order.
pub fn tray_runs<'a>(
    picked: &[Uuid],
    summaries: &'a [BenchmarkSummary],
) -> Vec<&'a BenchmarkSummary> {
    oldest_first(
        picked
            .iter()
            .filter_map(|uuid| summaries.iter().find(|summary| summary.uuid == *uuid)),
    )
}

fn tray_run(run: &BenchmarkSummary, selection: &SelectionHandle) -> Html {
    let name = run_name(run);
    let remove = {
        let selection = selection.clone();
        let uuid = run.uuid;
        Callback::from(move |_: MouseEvent| selection.dispatch(SelectionAction::Toggle(uuid)))
    };
    html! {
        <li class="compare-tray-run" title={run.title.clone()}>
            <span>{ name.clone() }</span>
            <button
                type="button"
                aria-label={format!("Take {name} out of the tray")}
                onclick={remove}
            >
                { close_icon() }
            </button>
        </li>
    }
}

fn plus_icon() -> Html {
    html! {
        <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24"
             fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round"
             aria-hidden="true">
            <path d="M12 5v14M5 12h14" />
        </svg>
    }
}

fn check_icon() -> Html {
    html! {
        <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24"
             fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round"
             stroke-linejoin="round" aria-hidden="true">
            <polyline points="20 6 9 17 4 12" />
        </svg>
    }
}

fn close_icon() -> Html {
    html! {
        <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24"
             fill="none" stroke="currentColor" stroke-width="2.6" stroke-linecap="round"
             aria-hidden="true">
            <path d="M18 6 6 18M6 6l12 12" />
        </svg>
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{CANDIDATE, OLD_RELEASE, RELEASE, limited};

    #[test]
    fn given_picks_in_any_order_when_trayed_should_list_oldest_version_first() {
        let summaries = [
            limited(1, CANDIDATE),
            limited(2, RELEASE),
            limited(3, OLD_RELEASE),
        ];
        let picked = [1, 3, 2].map(Uuid::from_u128);

        let tray: Vec<Uuid> = tray_runs(&picked, &summaries)
            .into_iter()
            .map(|run| run.uuid)
            .collect();

        assert_eq!(tray, [3, 2, 1].map(Uuid::from_u128));
    }

    #[test]
    fn given_pick_missing_from_list_when_trayed_should_leave_it_out() {
        let summaries = [limited(1, RELEASE)];
        let picked = [9, 1].map(Uuid::from_u128);

        let tray = tray_runs(&picked, &summaries);

        assert_eq!(tray.len(), 1);
        assert_eq!(tray[0].uuid, Uuid::from_u128(1));
    }
}
