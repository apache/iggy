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

//! The page for a path that doesn't match any page, for example an old or mistyped link.

use yew::prelude::*;
use yew_router::prelude::Link;

use crate::components::page::{page_head, page_shell};
use crate::router::AppRoute;

const TITLE: &str = "Page not found";
const LEAD: &str = "This link doesn't match any page on the dashboard.";
const OVERVIEW_LINK: &str = "Open the Overview";
const EXPLORE_LINK: &str = "Explore all runs";

#[function_component(NotFound)]
pub fn not_found() -> Html {
    let scroll_ref = use_node_ref();
    let content = html! {
        <>
            { page_head(Html::default(), None, TITLE, LEAD, Html::default()) }
            <section class="page-container">
                <div class="page-actions">
                    <Link<AppRoute> to={AppRoute::Home} classes={classes!("run-action")}>
                        { OVERVIEW_LINK }
                    </Link<AppRoute>>
                    <Link<AppRoute> to={AppRoute::Explore} classes={classes!("run-action")}>
                        { EXPLORE_LINK }
                    </Link<AppRoute>>
                </div>
            </section>
        </>
    };
    page_shell(&scroll_ref, content)
}
