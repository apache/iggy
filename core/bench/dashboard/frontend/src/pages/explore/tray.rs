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

//! The compare toggle and tray of the iggy-bench runs, on the shared tray component.

use bench_dashboard_shared::summary::BenchmarkSummary;
use uuid::Uuid;
use yew::prelude::*;

use crate::components::compare_tray::{CompareToggleOf, CompareTrayOf, TrayRun};
use crate::components::run_labels::run_name;
use crate::pages::compare::oldest_first;
use crate::state::data::{Loadable, use_dashboard_data};
use crate::state::selection::use_selection;

/// The toggle of an iggy-bench run, which fills the iggy-bench tray.
pub type CompareToggle = CompareToggleOf<Uuid>;

/// The picked iggy-bench runs. Floats over the page while it holds a run.
#[function_component(CompareTray)]
pub fn compare_tray() -> Html {
    let data = use_dashboard_data();
    let selection = use_selection();
    let (Some(selection), Loadable::Ready(summaries)) = (selection, &data.summaries) else {
        return Html::default();
    };
    let runs: Vec<TrayRun<Uuid>> = tray_runs(selection.runs(), summaries)
        .into_iter()
        .map(|run| TrayRun {
            id: run.uuid,
            name: run_name(run),
            title: run.title.clone(),
        })
        .collect();
    html! { <CompareTrayOf<Uuid> {runs} /> }
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
