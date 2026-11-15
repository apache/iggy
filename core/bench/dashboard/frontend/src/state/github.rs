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

//! The star count of the Apache Iggy repository for the top bar. The provider asks once, at app
//! start, because the top bar mounts again on every page. A failed request leaves the badge
//! without a count, and the next app start asks again.

use bench_dashboard_shared::github::GithubStars;
use gloo::console::log;
use yew::platform::spawn_local;
use yew::prelude::*;

use crate::api;

#[derive(Properties, PartialEq)]
pub struct GithubStarsProviderProps {
    pub children: Html,
}

#[function_component(GithubStarsProvider)]
pub fn github_stars_provider(props: &GithubStarsProviderProps) -> Html {
    let stars = use_state_eq(GithubStars::default);
    {
        let stars = stars.clone();
        use_effect_with((), move |_| {
            spawn_local(async move {
                match api::fetch_github_stars().await {
                    Ok(fetched) => stars.set(fetched),
                    Err(e) => log!(format!("No GitHub star count: {e}")),
                }
            });
        });
    }
    html! {
        <ContextProvider<GithubStars> context={*stars}>
            { props.children.clone() }
        </ContextProvider<GithubStars>>
    }
}
