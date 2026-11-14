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
//! the URL or in storage, so a reload empties it.

use std::rc::Rc;

use uuid::Uuid;
use yew::prelude::*;

/// The compare page shows at most this many runs, so a selection can't hold more.
pub const MAX_COMPARED_RUNS: usize = 3;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Selection {
    /// In pick order, without repeats.
    runs: Vec<Uuid>,
}

pub enum SelectionAction {
    /// Adds the run, or drops it if the selection already has it. A full selection doesn't take
    /// another run.
    Toggle(Uuid),
    /// Takes over a compare page's runs, so the reader can add one more.
    Replace(Vec<Uuid>),
    Clear,
}

pub type SelectionHandle = UseReducerHandle<Selection>;

#[derive(Properties, PartialEq)]
pub struct SelectionProviderProps {
    pub children: Html,
}

impl Selection {
    pub fn runs(&self) -> &[Uuid] {
        &self.runs
    }

    pub fn contains(&self, run: Uuid) -> bool {
        self.runs.contains(&run)
    }

    pub fn is_full(&self) -> bool {
        self.runs.len() >= MAX_COMPARED_RUNS
    }
}

impl Reducible for Selection {
    type Action = SelectionAction;

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
                let mut unique: Vec<Uuid> = Vec::with_capacity(MAX_COMPARED_RUNS);
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

#[function_component(SelectionProvider)]
pub fn selection_provider(props: &SelectionProviderProps) -> Html {
    let selection = use_reducer_eq(Selection::default);
    html! {
        <ContextProvider<SelectionHandle> context={selection}>
            { props.children.clone() }
        </ContextProvider<SelectionHandle>>
    }
}

/// `None` outside the provider, where a page has no selection to offer.
#[hook]
pub fn use_selection() -> Option<SelectionHandle> {
    use_context::<SelectionHandle>()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(id: u128) -> Uuid {
        Uuid::from_u128(id)
    }

    fn after(
        selection: Selection,
        actions: impl IntoIterator<Item = SelectionAction>,
    ) -> Selection {
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
}
