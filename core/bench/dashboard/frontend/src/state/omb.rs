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

use std::cell::RefCell;
use std::rc::Rc;

use bench_dashboard_shared::omb::summary::OmbRunList;
use yew::prelude::*;

use crate::api;
use crate::state::data::{Loadable, fetch_into, start_value};
use crate::state::start::StartData;

#[derive(Debug, Clone, PartialEq)]
pub struct OmbData {
    /// `Loading` until the list arrives, and also before any OMB page asked for it.
    pub runs: Loadable<OmbRunList>,
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
    let runs = use_state_eq(|| {
        start_value(
            props
                .start
                .as_ref()
                .and_then(|start| start.omb_runs.clone()),
        )
    });
    // Set while a fetch is on its way or has succeeded, or when the list came with the page. A ref
    // and not a state, so two pages that ask in the same moment start one fetch.
    let asked = use_mut_ref(|| matches!(*runs, Loadable::Ready(_)));
    let reload = {
        let runs = runs.clone();
        use_callback((), move |(), _| fetch_once(runs.clone(), Rc::clone(&asked)))
    };
    let data = OmbData {
        runs: (*runs).clone(),
        reload,
    };
    html! {
        <ContextProvider<OmbData> context={data}>
            { props.children.clone() }
        </ContextProvider<OmbData>>
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

fn fetch_once(runs: UseStateHandle<Loadable<OmbRunList>>, asked: Rc<RefCell<bool>>) {
    if asked.replace(true) {
        return;
    }
    fetch_into(runs, async move {
        let list = api::fetch_omb_runs().await;
        if list.is_err() {
            asked.replace(false);
        }
        list
    });
}
