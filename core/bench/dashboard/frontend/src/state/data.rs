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

//! The run summaries and the dashboard configuration. The provider fetches each once, when the
//! first page that reads it asks for it, so every page reads the same data without its own request.
//! After a failure, the next page that asks, or the "Try again" button, fetches it again.

use crate::api;
use crate::error::{IggyBenchDashboardError, Result};
use crate::state::start::{PageRuns, StartData};
use bench_dashboard_shared::config::DashboardConfig;
use bench_dashboard_shared::summary::BenchmarkSummary;
use std::cell::RefCell;
use std::future::Future;
use std::rc::Rc;
use uuid::Uuid;
use yew::platform::spawn_local;
use yew::prelude::*;

#[derive(Debug, Clone)]
pub enum Loadable<T> {
    Loading,
    Ready(Rc<T>),
    Failed(IggyBenchDashboardError),
}

#[derive(Debug, Clone, PartialEq)]
pub struct DashboardData {
    /// Every run. `Loading` until it arrives, and also before any page asked for it.
    pub summaries: Loadable<Vec<BenchmarkSummary>>,
    pub config: Loadable<DashboardConfig>,
    /// The runs that came with the page of one run. Only that page reads them.
    pub page_runs: Option<PageRuns<Uuid, Rc<Vec<BenchmarkSummary>>>>,
    /// Fetches each part that is not loaded or on its way.
    pub reload: Callback<()>,
}

#[derive(Properties, PartialEq)]
pub struct DataProviderProps {
    pub children: Html,
    /// The data of a page that the server rendered. The provider starts from it and fetches
    /// nothing until a page asks for a part that it lacks, so the first render gives the HTML of
    /// the server.
    #[prop_or_default]
    pub start: Option<Rc<StartData>>,
}

#[function_component(DataProvider)]
pub fn data_provider(props: &DataProviderProps) -> Html {
    let start = props.start.as_deref();
    let summaries = use_state_eq(|| start_value(start.and_then(|start| start.summaries.clone())));
    let config = use_state_eq(|| start_value(start.and_then(|start| start.config.clone())));
    let page_runs = use_memo((), |()| {
        start
            .and_then(|start| start.run.as_ref())
            .map(|page| PageRuns {
                id: page.id,
                runs: Rc::new(page.runs.clone()),
            })
    });
    // Set while a fetch is on its way or has succeeded, or when the part came with the page. Refs
    // and not states, so two pages that ask in the same moment start one fetch.
    let summaries_asked = use_mut_ref(|| matches!(*summaries, Loadable::Ready(_)));
    let config_asked = use_mut_ref(|| matches!(*config, Loadable::Ready(_)));
    let reload = {
        let summaries = summaries.clone();
        let config = config.clone();
        use_callback((), move |(), _| {
            fetch_once(
                summaries.clone(),
                Rc::clone(&summaries_asked),
                api::fetch_benchmark_summaries,
            );
            fetch_once(
                config.clone(),
                Rc::clone(&config_asked),
                api::fetch_dashboard_config,
            );
        })
    };

    let data = DashboardData {
        summaries: (*summaries).clone(),
        config: (*config).clone(),
        page_runs: (*page_runs).clone(),
        reload,
    };
    html! {
        <ContextProvider<DashboardData> context={data}>
            { props.children.clone() }
        </ContextProvider<DashboardData>>
    }
}

impl DashboardData {
    /// The runs that the page of the run `id` reads: every run once it arrived, else the runs
    /// that came with the page of that run. Another run's page waits for every run, because the
    /// runs of this page are not all the runs that it reads.
    pub fn runs_of(&self, id: Option<Uuid>) -> Loadable<Vec<BenchmarkSummary>> {
        match &self.page_runs {
            Some(page) if Some(page.id) == id && !matches!(self.summaries, Loadable::Ready(_)) => {
                Loadable::Ready(Rc::clone(&page.runs))
            }
            _ => self.summaries.clone(),
        }
    }

    /// Whether the runs of the page of `id` came with the page.
    fn has_page_runs(&self, id: Option<Uuid>) -> bool {
        self.page_runs
            .as_ref()
            .is_some_and(|page| Some(page.id) == id)
    }
}

/// The data of a page that reads every run. The first call fetches what is not loaded yet.
#[hook]
pub fn use_dashboard_data() -> DashboardData {
    let data = use_context::<DashboardData>().expect("DataProvider wraps the router");
    {
        let reload = data.reload.clone();
        use_effect_with((), move |()| reload.emit(()));
    }
    data
}

/// The data of the page of the run `id`, with the runs of [`DashboardData::runs_of`]. Fetches
/// every run only when the runs of this page did not come with it.
#[hook]
pub fn use_run_data(id: Option<Uuid>) -> DashboardData {
    let data = use_context::<DashboardData>().expect("DataProvider wraps the router");
    {
        let reload = data.reload.clone();
        let fetch = !data.has_page_runs(id);
        use_effect_with(fetch, move |fetch| {
            if *fetch {
                reload.emit(());
            }
        });
    }
    DashboardData {
        summaries: data.runs_of(id),
        ..data
    }
}

/// Fetches `request` into `state` unless a fetch is on its way or has succeeded, as `asked`
/// records. A failed fetch clears `asked`, so the next call fetches again.
pub fn fetch_once<T: 'static, F: Future<Output = Result<T>> + 'static>(
    state: UseStateHandle<Loadable<T>>,
    asked: Rc<RefCell<bool>>,
    request: impl FnOnce() -> F,
) {
    if asked.replace(true) {
        return;
    }
    let request = request();
    fetch_into(state, async move {
        let value = request.await;
        if value.is_err() {
            asked.replace(false);
        }
        value
    });
}

/// `Ready` with a part of the start data, else `Loading` until the provider fetches it.
pub fn start_value<T>(value: Option<T>) -> Loadable<T> {
    value.map_or(Loadable::Loading, |value| Loadable::Ready(Rc::new(value)))
}

/// Shows the loading state until `request` ends, then its result.
pub fn fetch_into<T: 'static>(
    state: UseStateHandle<Loadable<T>>,
    request: impl Future<Output = Result<T>> + 'static,
) {
    state.set(Loadable::Loading);
    spawn_local(async move { state.set(request.await.into()) });
}

/// Two loaded values are equal only when they're the same allocation. The provider replaces a value
/// and never changes it in place, so the address identifies the value, and a context or memo check
/// costs one comparison instead of a walk over every run.
impl<T> PartialEq for Loadable<T> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Loading, Self::Loading) => true,
            (Self::Ready(left), Self::Ready(right)) => Rc::ptr_eq(left, right),
            (Self::Failed(left), Self::Failed(right)) => left == right,
            _ => false,
        }
    }
}

impl<T> From<Result<T>> for Loadable<T> {
    fn from(result: Result<T>) -> Self {
        match result {
            Ok(value) => Self::Ready(Rc::new(value)),
            Err(error) => Self::Failed(error),
        }
    }
}
