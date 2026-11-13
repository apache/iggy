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

//! A shell command with a button that copies it to the clipboard.

use crate::components::clipboard::{CopyState, use_copy};
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct CommandBlockProps {
    pub command: AttrValue,
}

#[function_component(CommandBlock)]
pub fn command_block(props: &CommandBlockProps) -> Html {
    let (state, copy) = use_copy();
    let on_copy = {
        let command = props.command.clone();
        copy.reform(move |_: MouseEvent| command.clone())
    };

    html! {
        <div class="command-block">
            <pre class="command-block-text"><code>{ command_words(&props.command) }</code></pre>
            <button
                type="button"
                class={classes!("command-copy", (state == CopyState::Copied).then_some("copied"))}
                onclick={on_copy}
                aria-live="polite"
            >
                { copy_label(state) }
            </button>
        </div>
    }
}

const fn copy_label(state: CopyState) -> &'static str {
    match state {
        CopyState::Idle => "Copy",
        CopyState::Copied => "Copied",
        CopyState::Failed => "Copy failed",
    }
}

/// Each word wraps as one unit. Otherwise a browser breaks lines at a flag's hyphens, and a narrow
/// screen shows `-` and `-rate-limit` on two lines.
fn command_words(command: &str) -> Html {
    command
        .split(' ')
        .enumerate()
        .map(|(index, word)| {
            let space = if index == 0 { "" } else { " " };
            html! { <>{ space }<span class="command-word">{ word.to_owned() }</span></> }
        })
        .collect()
}
