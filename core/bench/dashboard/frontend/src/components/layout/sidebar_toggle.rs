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

use crate::state::ui::{UiAction, use_ui};
use yew::prelude::*;

#[function_component(SidebarToggle)]
pub fn sidebar_toggle() -> Html {
    let ui = use_ui();
    let is_collapsed = ui.is_sidebar_collapsed;

    let on_click = {
        let ui = ui.clone();
        Callback::from(move |_| ui.dispatch(UiAction::ToggleSidebar))
    };

    let title = if is_collapsed {
        "Show sidebar"
    } else {
        "Hide sidebar"
    };

    html! {
        <button
            type="button"
            class={classes!("sidebar-toggle-floating", is_collapsed.then_some("collapsed"))}
            onclick={on_click}
            title={title}
            aria-label={title}
        >
            <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24"
                 fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                {
                    if is_collapsed {
                        html! { <polyline points="9 18 15 12 9 6" /> }
                    } else {
                        html! { <polyline points="15 18 9 12 15 6" /> }
                    }
                }
            </svg>
        </button>
    }
}
