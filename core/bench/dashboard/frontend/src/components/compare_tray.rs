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
//! holds the picked runs. Both work on the selection of their id type, so the iggy-bench and the
//! OMB lists each fill their own tray.

use yew::prelude::*;

use crate::pages::compare::runs::{CompareLinkOf, RunId};
use crate::state::selection::{MAX_COMPARED_RUNS, SelectionAction, SelectionHandle};

const ADD_LABEL: &str = "Add to compare";
const REMOVE_LABEL: &str = "Remove from compare";
const ONE_MORE_NOTE: &str = "Pick one more run";
const COMPARE_LABEL: &str = "Compare";
const CLEAR_LABEL: &str = "Clear";

#[derive(Properties, PartialEq)]
pub struct CompareToggleProps<Id: PartialEq> {
    pub run: Id,
}

/// One picked run as the tray shows it.
#[derive(Debug, Clone, PartialEq)]
pub struct TrayRun<Id> {
    pub id: Id,
    /// Short, for example the short id and the version.
    pub name: String,
    /// The full name, on hover.
    pub title: String,
}

#[derive(Properties, PartialEq)]
pub struct CompareTrayProps<Id: PartialEq> {
    /// The picked runs that are in the loaded list, in the order of the compare page, which takes
    /// the first run as the baseline.
    pub runs: Vec<TrayRun<Id>>,
}

/// Puts a row's run into the tray, or takes it out. A full tray doesn't take another run.
#[function_component(CompareToggleOf)]
pub fn compare_toggle_of<Id: RunId>(props: &CompareToggleProps<Id>) -> Html {
    let Some(selection) = use_context::<SelectionHandle<Id>>() else {
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
#[function_component(CompareTrayOf)]
pub fn compare_tray_of<Id: RunId>(props: &CompareTrayProps<Id>) -> Html {
    let selection = use_context::<SelectionHandle<Id>>();
    let Some(selection) = selection.filter(|_| !props.runs.is_empty()) else {
        return Html::default();
    };
    let ids: Vec<Id> = props.runs.iter().map(|run| run.id).collect();
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
                    <strong>{ format!("{} of {MAX_COMPARED_RUNS}", props.runs.len()) }</strong>
                    <span>{"runs to compare"}</span>
                </p>
                <ul class="compare-tray-runs">
                    { for props.runs.iter().map(|run| tray_run(run, &selection)) }
                </ul>
                <div class="compare-tray-actions">
                    if ids.len() > 1 {
                        <CompareLinkOf<Id> runs={ids} classes={classes!("run-action", "compare-tray-go")}>
                            { COMPARE_LABEL }
                        </CompareLinkOf<Id>>
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

fn tray_run<Id: RunId>(run: &TrayRun<Id>, selection: &SelectionHandle<Id>) -> Html {
    let remove = {
        let selection = selection.clone();
        let id = run.id;
        Callback::from(move |_: MouseEvent| selection.dispatch(SelectionAction::Toggle(id)))
    };
    html! {
        <li class="compare-tray-run" title={run.title.clone()}>
            <span>{ run.name.clone() }</span>
            <button
                type="button"
                aria-label={format!("Take {} out of the tray", run.name)}
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
