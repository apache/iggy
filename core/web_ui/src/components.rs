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

use dioxus::prelude::*;

use crate::Route;
use crate::api::{ApiClient, ApiError};
use crate::auth::Session;

pub fn use_session() -> Signal<Option<Session>> {
    use_context()
}

pub fn api_client(session: Signal<Option<Session>>) -> ApiClient {
    ApiClient::new(session.read().as_ref())
}

/// Breadcrumbs go in `children`, buttons for the page in `actions`.
#[component]
pub fn PageHeader(title: String, children: Element, actions: Option<Element>) -> Element {
    rsx! {
        header { class: "page-header",
            nav { class: "breadcrumbs", {children} }
            div { class: "title-row",
                h1 { "{title}" }
                if let Some(actions) = actions {
                    div { class: "actions", {actions} }
                }
            }
        }
    }
}

#[component]
pub fn Crumb(to: Route, label: String) -> Element {
    rsx! {
        Link { to, "{label}" }
        span { class: "crumb-separator", "/" }
    }
}

#[component]
pub fn Stat(label: String, value: String) -> Element {
    rsx! {
        div { class: "stat",
            span { class: "stat-label", "{label}" }
            span { class: "stat-value", "{value}" }
        }
    }
}

#[component]
pub fn Loading() -> Element {
    rsx! {
        p { class: "muted", "Loading…" }
    }
}

/// A dialog over the page. Clicking the backdrop or pressing Escape closes it.
#[component]
pub fn Modal(title: String, on_close: EventHandler<()>, children: Element) -> Element {
    rsx! {
        div {
            class: "modal-backdrop",
            onclick: move |_| on_close.call(()),
            onkeydown: move |event: KeyboardEvent| {
                if event.key() == Key::Escape {
                    on_close.call(());
                }
            },
            div {
                class: "modal",
                role: "dialog",
                aria_modal: "true",
                onclick: move |event| event.stop_propagation(),
                h2 { "{title}" }
                {children}
            }
        }
    }
}

/// Cancel and submit buttons plus the last error, shared by every form.
#[component]
pub fn FormActions(
    submit_label: String,
    busy: bool,
    error: Option<String>,
    on_cancel: EventHandler<()>,
    #[props(default)] danger: bool,
    #[props(default)] disabled: bool,
) -> Element {
    rsx! {
        if let Some(message) = error {
            p { class: "form-error", "{message}" }
        }
        div { class: "form-actions",
            button {
                class: "secondary",
                r#type: "button",
                onclick: move |_| on_cancel.call(()),
                "Cancel"
            }
            button {
                class: if danger { "danger" },
                r#type: "submit",
                disabled: busy || disabled,
                "{submit_label}"
            }
        }
    }
}

/// Asks the user to retype `confirm_text` before a destructive action runs.
#[component]
pub fn ConfirmDelete(
    title: String,
    confirm_text: String,
    on_confirm: EventHandler<()>,
    on_cancel: EventHandler<()>,
    busy: bool,
    error: Option<String>,
) -> Element {
    let mut typed = use_signal(String::new);
    let matches = typed() == confirm_text;

    rsx! {
        form {
            class: "form",
            onsubmit: move |event: FormEvent| {
                event.prevent_default();
                if matches {
                    on_confirm.call(());
                }
            },
            p { class: "muted",
                "This cannot be undone. Type "
                code { "{confirm_text}" }
                " to confirm."
            }
            input {
                aria_label: "Confirmation",
                autocomplete: "off",
                autofocus: true,
                value: typed,
                oninput: move |event| typed.set(event.value()),
            }
            FormActions {
                submit_label: title,
                busy,
                error,
                on_cancel,
                danger: true,
                disabled: !matches,
            }
        }
    }
}

/// Renders a failed request. A 401 signs the user out, and the dashboard
/// layout then redirects to sign-in. A 403 only shows the message.
#[component]
pub fn ErrorView(error: ApiError) -> Element {
    let mut session = use_session();
    let unauthorized = error == ApiError::Unauthorized;

    // Reactive on the prop, because a mounted view can change from another
    // error to a 401 without remounting.
    use_effect(use_reactive!(|unauthorized| {
        if !unauthorized {
            return;
        }
        let failed = session.write().take();
        if let Some(failed) = failed {
            failed.forget();
        }
    }));

    rsx! {
        div { class: "error-box", "{error}" }
    }
}
