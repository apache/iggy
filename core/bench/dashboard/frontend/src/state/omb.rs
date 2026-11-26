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

//! The OMB run summaries. Unlike the iggy-bench runs, the provider fetches them only when the first
//! OMB page asks for them, so the iggy-bench pages never wait for them. The list then stays for the
//! session. After a failure, the next page that asks, or the "Try again" button, fetches it again.

use std::rc::Rc;

use bench_dashboard_shared::omb::id::OmbRunId;
use bench_dashboard_shared::omb::summary::OmbRunList;
use yew::prelude::*;

use crate::api;
use crate::state::data::{Loadable, fetch_once, start_value};
use crate::state::start::{PageRuns, StartData};

#[derive(Debug, Clone, PartialEq)]
pub struct OmbData {
    /// `Loading` until the list arrives, and also before any OMB page asked for it.
    pub runs: Loadable<OmbRunList>,
    /// The runs that came with the page of one OMB run. Only that page reads them.
    pub page_runs: Option<PageRuns<OmbRunId, Rc<OmbRunList>>>,
    /// Fetches the list unless it's loaded or on its way.
    pub reload: Callback<()>,
}

#[derive(Properties, PartialEq)]
pub struct OmbDataProviderProps {
    pub children: Html,
    /// The data of a page that the server rendered. With the OMB runs in it, the provider starts
    /// from them and fetches nothing.
    #[prop_or_default]
    pub start: Option<Rc<StartData>>,
}

#[function_component(OmbDataProvider)]
pub fn omb_data_provider(props: &OmbDataProviderProps) -> Html {
    let start = props.start.as_deref();
    let runs = use_state_eq(|| start_value(start.and_then(|start| start.omb_runs.clone())));
    let page_runs = use_memo((), |()| {
        start
            .and_then(|start| start.omb_run.as_ref())
            .map(|page| PageRuns {
                id: page.id,
                runs: Rc::new(page.runs.clone()),
            })
    });
    // Set while a fetch is on its way or has succeeded, or when the list came with the page. A ref
    // and not a state, so two pages that ask in the same moment start one fetch.
    let asked = use_mut_ref(|| matches!(*runs, Loadable::Ready(_)));
    let reload = {
        let runs = runs.clone();
        use_callback((), move |(), _| {
            fetch_once(runs.clone(), Rc::clone(&asked), api::fetch_omb_runs);
        })
    };
    let data = OmbData {
        runs: (*runs).clone(),
        page_runs: (*page_runs).clone(),
        reload,
    };
    html! {
        <ContextProvider<OmbData> context={data}>
            { props.children.clone() }
        </ContextProvider<OmbData>>
    }
}

impl OmbData {
    /// The runs that the page of the run `id` reads: every run once the list arrived, else the
    /// runs that came with the page of that run.
    pub fn runs_of(&self, id: Option<OmbRunId>) -> Loadable<OmbRunList> {
        match &self.page_runs {
            Some(page) if Some(page.id) == id && !matches!(self.runs, Loadable::Ready(_)) => {
                Loadable::Ready(Rc::clone(&page.runs))
            }
            _ => self.runs.clone(),
        }
    }

    /// Whether the runs of the page of `id` came with the page.
    fn has_page_runs(&self, id: Option<OmbRunId>) -> bool {
        self.page_runs
            .as_ref()
            .is_some_and(|page| Some(page.id) == id)
    }
}

/// The OMB runs, fetched on the first call of any page.
#[hook]
pub fn use_omb_data() -> OmbData {
    let data = use_context::<OmbData>().expect("OmbDataProvider wraps the router");
    {
        let reload = data.reload.clone();
        use_effect_with((), move |()| reload.emit(()));
    }
    data
}

/// The data of the page of the OMB run `id`, with the runs of [`OmbData::runs_of`]. Fetches the
/// list only when the runs of this page did not come with it.
#[hook]
pub fn use_omb_run_data(id: Option<OmbRunId>) -> OmbData {
    let data = use_context::<OmbData>().expect("OmbDataProvider wraps the router");
    {
        let reload = data.reload.clone();
        let fetch = !data.has_page_runs(id);
        use_effect_with(fetch, move |fetch| {
            if *fetch {
                reload.emit(());
            }
        });
    }
    OmbData {
        runs: data.runs_of(id),
        ..data
    }
}
