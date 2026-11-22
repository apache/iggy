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

//! A button that copies the page's link, because the URL alone says what the page shows.

use crate::components::clipboard::{CopyState, use_copy};
use yew::prelude::*;

const IDLE_TITLE: &str = "Copy the link to this page";

#[derive(Properties, PartialEq)]
pub struct ShareButtonProps {
    /// How the button looks in the bar or page that holds it.
    pub class: Classes,
    /// Shows the action in words next to the icon. Without them, the icon and the title tell
    /// what the button does and did.
    #[prop_or_default]
    pub labeled: bool,
}

#[function_component(ShareButton)]
pub fn share_button(props: &ShareButtonProps) -> Html {
    let (state, copy) = use_copy();
    let onclick = Callback::from(move |_: MouseEvent| {
        if let Some(link) = page_link() {
            copy.emit(link.into());
        }
    });
    let text = match state {
        CopyState::Idle => "Share",
        CopyState::Copied => "Link copied",
        CopyState::Failed => "Copy failed",
    };
    let copied = state == CopyState::Copied;
    html! {
        <button
            type="button"
            class={classes!(props.class.clone(), copied.then_some("active"))}
            title={if state == CopyState::Idle { IDLE_TITLE } else { text }}
            aria-live="polite"
            {onclick}
        >
            { share_icon(copied) }
            <span class={classes!((!props.labeled).then_some("visually-hidden"))}>{ text }</span>
        </button>
    }
}

fn page_link() -> Option<String> {
    web_sys::window()?
        .location()
        .href()
        .ok()
        .filter(|link| !link.is_empty())
}

fn share_icon(copied: bool) -> Html {
    if copied {
        return html! {
            <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24"
                 fill="none" stroke="currentColor" stroke-width="2.3" stroke-linecap="round"
                 stroke-linejoin="round" aria-hidden="true">
                <polyline points="20 6 9 17 4 12" />
            </svg>
        };
    }
    html! {
        <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24"
             fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"
             stroke-linejoin="round" aria-hidden="true">
            <circle cx="18" cy="5" r="3" />
            <circle cx="6" cy="12" r="3" />
            <circle cx="18" cy="19" r="3" />
            <line x1="8.59" y1="13.51" x2="15.42" y2="17.49" />
            <line x1="15.41" y1="6.51" x2="8.59" y2="10.49" />
        </svg>
    }
}
