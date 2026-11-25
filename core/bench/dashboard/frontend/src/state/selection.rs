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

//! The runs the reader picks to compare. The pick lives in a context for the page session, not in
//! the URL or in storage, so a reload empties it. iggy-bench runs and OMB runs have their own id
//! types, so each framework has its own selection, and a run of one never enters the other's.

use std::rc::Rc;

use bench_dashboard_shared::omb::id::OmbRunId;
use uuid::Uuid;
use yew::prelude::*;

/// The compare page shows at most this many runs, so a selection can't hold more.
pub const MAX_COMPARED_RUNS: usize = 3;

#[derive(Debug, Clone, PartialEq)]
pub struct Selection<Id> {
    /// In pick order, without repeats.
    runs: Vec<Id>,
}

pub enum SelectionAction<Id> {
    /// Adds the run, or drops it if the selection already has it. A full selection doesn't take
    /// another run.
    Toggle(Id),
    /// Takes over a compare page's runs, so the reader can add one more.
    Replace(Vec<Id>),
    Clear,
}

/// The selection of the runs with ids of type `Id`. The type picks the context, so the iggy-bench
/// and the OMB selections never meet.
pub type SelectionHandle<Id> = UseReducerHandle<Selection<Id>>;

#[derive(Properties, PartialEq)]
pub struct SelectionProviderProps {
    pub children: Html,
}

impl<Id> Default for Selection<Id> {
    fn default() -> Self {
        Self { runs: Vec::new() }
    }
}

impl<Id: Copy + PartialEq> Selection<Id> {
    pub fn runs(&self) -> &[Id] {
        &self.runs
    }

    pub fn contains(&self, run: Id) -> bool {
        self.runs.contains(&run)
    }

    pub fn is_full(&self) -> bool {
        self.runs.len() >= MAX_COMPARED_RUNS
    }
}

impl<Id: Copy + PartialEq> Reducible for Selection<Id> {
    type Action = SelectionAction<Id>;

    fn reduce(self: Rc<Self>, action: Self::Action) -> Rc<Self> {
        let runs = match action {
            SelectionAction::Toggle(run) if self.contains(run) => self
                .runs
                .iter()
                .copied()
                .filter(|picked| *picked != run)
                .collect(),
            SelectionAction::Toggle(_) if self.is_full() => return self,
            SelectionAction::Toggle(run) => self.runs.iter().copied().chain([run]).collect(),
            SelectionAction::Replace(runs) => {
                let mut unique: Vec<Id> = Vec::with_capacity(MAX_COMPARED_RUNS);
                for run in runs {
                    if unique.len() < MAX_COMPARED_RUNS && !unique.contains(&run) {
                        unique.push(run);
                    }
                }
                unique
            }
            SelectionAction::Clear => Vec::new(),
        };
        Rc::new(Self { runs })
    }
}

/// The selection of iggy-bench runs.
#[function_component(SelectionProvider)]
pub fn selection_provider(props: &SelectionProviderProps) -> Html {
    let selection = use_reducer_eq(Selection::<Uuid>::default);
    html! {
        <ContextProvider<SelectionHandle<Uuid>> context={selection}>
            { props.children.clone() }
        </ContextProvider<SelectionHandle<Uuid>>>
    }
}

/// The selection of OMB runs, apart from the iggy-bench one.
#[function_component(OmbSelectionProvider)]
pub fn omb_selection_provider(props: &SelectionProviderProps) -> Html {
    let selection = use_reducer_eq(Selection::<OmbRunId>::default);
    html! {
        <ContextProvider<SelectionHandle<OmbRunId>> context={selection}>
            { props.children.clone() }
        </ContextProvider<SelectionHandle<OmbRunId>>>
    }
}

/// The iggy-bench selection. `None` outside the provider, where a page has no selection to offer.
#[hook]
pub fn use_selection() -> Option<SelectionHandle<Uuid>> {
    use_context::<SelectionHandle<Uuid>>()
}

/// The OMB selection. `None` outside the provider, where a page has no selection to offer.
#[hook]
pub fn use_omb_selection() -> Option<SelectionHandle<OmbRunId>> {
    use_context::<SelectionHandle<OmbRunId>>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{OMB_ALL, OMB_EDGE8_DEFAULT, OMB_RC1_DEFAULT, OMB_RC1_PERSISTED};

    fn run(id: u128) -> Uuid {
        Uuid::from_u128(id)
    }

    fn after<Id: Copy + PartialEq>(
        selection: Selection<Id>,
        actions: impl IntoIterator<Item = SelectionAction<Id>>,
    ) -> Selection<Id> {
        let reduced = actions
            .into_iter()
            .fold(Rc::new(selection), |selection, action| {
                selection.reduce(action)
            });
        (*reduced).clone()
    }

    #[test]
    fn given_picks_when_toggled_should_keep_pick_order_and_drop_a_second_toggle() {
        let selection = after(
            Selection::default(),
            [
                SelectionAction::Toggle(run(2)),
                SelectionAction::Toggle(run(1)),
                SelectionAction::Toggle(run(3)),
                SelectionAction::Toggle(run(2)),
            ],
        );

        assert_eq!(selection.runs(), [run(1), run(3)]);
        assert!(!selection.contains(run(2)));
    }

    #[test]
    fn given_full_selection_when_another_run_toggled_should_keep_selection() {
        let full = after(
            Selection::default(),
            [1, 2, 3].map(|id| SelectionAction::Toggle(run(id))),
        );

        let selection = after(full.clone(), [SelectionAction::Toggle(run(4))]);

        assert!(full.is_full());
        assert_eq!(selection, full);
        assert_eq!(
            after(full, [SelectionAction::Toggle(run(1))]).runs(),
            [run(2), run(3)]
        );
    }

    #[test]
    fn given_runs_of_compare_page_when_replaced_should_drop_repeats_and_keep_limit() {
        let selection = after(
            Selection::default(),
            [SelectionAction::Replace(
                [5, 5, 6, 7, 8].map(run).into_iter().collect(),
            )],
        );

        assert_eq!(selection.runs(), [run(5), run(6), run(7)]);
        assert!(after(selection, [SelectionAction::Clear]).runs().is_empty());
    }

    #[test]
    fn given_omb_fixture_picks_when_toggled_should_keep_pick_order_and_drop_a_second_toggle() {
        let (rc1, persisted, edge8) = (
            OMB_RC1_DEFAULT.id(),
            OMB_RC1_PERSISTED.id(),
            OMB_EDGE8_DEFAULT.id(),
        );

        let selection = after(
            Selection::default(),
            [
                SelectionAction::Toggle(persisted),
                SelectionAction::Toggle(rc1),
                SelectionAction::Toggle(edge8),
                SelectionAction::Toggle(persisted),
            ],
        );

        assert_eq!(selection.runs(), [rc1, edge8]);
        assert!(!selection.contains(persisted));
    }

    #[test]
    fn given_every_omb_fixture_when_replaced_should_keep_first_three_without_repeats() {
        let ids: Vec<OmbRunId> = OMB_ALL.iter().map(|fixture| fixture.id()).collect();
        let repeated = ids.iter().copied().chain(ids.iter().copied()).collect();

        let selection = after(Selection::default(), [SelectionAction::Replace(repeated)]);

        assert!(selection.is_full());
        assert_eq!(selection.runs(), &ids[..MAX_COMPARED_RUNS]);
        let full = after(
            selection.clone(),
            [SelectionAction::Toggle(ids[MAX_COMPARED_RUNS])],
        );
        assert_eq!(full, selection);
    }
}
