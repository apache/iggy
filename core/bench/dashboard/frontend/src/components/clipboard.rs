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

//! Copies text to the clipboard for a button, which then shows the result for a moment.

use gloo::timers::callback::Timeout;
use wasm_bindgen_futures::JsFuture;
use yew::platform::spawn_local;
use yew::prelude::*;

/// How long a button shows the result of a copy.
const FEEDBACK_MS: u32 = 1_600;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyState {
    Idle,
    Copied,
    Failed,
}

/// The state that the button shows, and a callback that copies its text.
#[hook]
pub fn use_copy() -> (CopyState, Callback<AttrValue>) {
    let state = use_state_eq(|| CopyState::Idle);
    // Dropping a timer cancels it, so a second copy restarts the feedback and an unmounted button
    // doesn't leave a timer running.
    let reset = use_mut_ref(|| None::<Timeout>);
    let copy = {
        let state = state.clone();
        Callback::from(move |text: AttrValue| {
            let state = state.clone();
            let reset = reset.clone();
            spawn_local(async move {
                state.set(if copy_to_clipboard(&text).await {
                    CopyState::Copied
                } else {
                    CopyState::Failed
                });
                let idle = state.clone();
                *reset.borrow_mut() = Some(Timeout::new(FEEDBACK_MS, move || {
                    idle.set(CopyState::Idle);
                }));
            });
        })
    };
    (*state, copy)
}

async fn copy_to_clipboard(text: &str) -> bool {
    let Some(window) = web_sys::window() else {
        return false;
    };
    JsFuture::from(window.navigator().clipboard().write_text(text))
        .await
        .is_ok()
}
