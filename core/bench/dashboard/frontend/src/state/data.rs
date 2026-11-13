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

//! The run summaries and the dashboard configuration. The provider fetches both once, at app start,
//! so every page reads the same data without its own request. After a failure, a page asks the
//! provider to fetch again.

use crate::api;
use crate::error::{IggyBenchDashboardError, Result};
use bench_dashboard_shared::config::DashboardConfig;
use bench_dashboard_shared::summary::BenchmarkSummary;
use std::future::Future;
use std::rc::Rc;
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
    pub summaries: Loadable<Vec<BenchmarkSummary>>,
    pub config: Loadable<DashboardConfig>,
    /// Refetches each part that failed.
    pub reload: Callback<()>,
}

#[derive(Properties, PartialEq)]
pub struct DataProviderProps {
    pub children: Html,
}

#[function_component(DataProvider)]
pub fn data_provider(props: &DataProviderProps) -> Html {
    let summaries = use_state_eq(|| Loadable::Loading);
    let config = use_state_eq(|| Loadable::Loading);
    {
        let summaries = summaries.clone();
        let config = config.clone();
        use_effect_with((), move |_| {
            fetch_into(summaries, api::fetch_benchmark_summaries());
            fetch_into(config, api::fetch_dashboard_config());
        });
    }
    let reload = {
        let summaries = summaries.clone();
        let config = config.clone();
        Callback::from(move |()| {
            if matches!(*summaries, Loadable::Failed(_)) {
                fetch_into(summaries.clone(), api::fetch_benchmark_summaries());
            }
            if matches!(*config, Loadable::Failed(_)) {
                fetch_into(config.clone(), api::fetch_dashboard_config());
            }
        })
    };

    let data = DashboardData {
        summaries: (*summaries).clone(),
        config: (*config).clone(),
        reload,
    };
    html! {
        <ContextProvider<DashboardData> context={data}>
            { props.children.clone() }
        </ContextProvider<DashboardData>>
    }
}

#[hook]
pub fn use_dashboard_data() -> DashboardData {
    use_context::<DashboardData>().expect("DataProvider wraps the router")
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
