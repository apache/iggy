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

/// A dialog over the page. Clicking the backdrop or pressing Escape closes it,
/// except while `busy`: closing drops the form's request, and the page would
/// not reload to show what the server did with it.
#[component]
pub fn Modal(
    title: String,
    on_close: EventHandler<()>,
    children: Element,
    #[props(default)] busy: bool,
) -> Element {
    let close = move || {
        if !busy {
            on_close.call(());
        }
    };

    rsx! {
        div {
            class: "modal-backdrop",
            onclick: move |_| close(),
            onkeydown: move |event: KeyboardEvent| {
                if event.key() == Key::Escape {
                    close();
                }
            },
            // Focusable, so a click inside keeps focus in the dialog and the
            // backdrop still hears Escape.
            div {
                class: "modal",
                role: "dialog",
                aria_modal: "true",
                tabindex: "-1",
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
                disabled: busy,
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
    let matches = *typed.read() == confirm_text;

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
                onmounted: focus_on_mount,
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

/// A warning and the button that starts a destructive action.
#[component]
pub fn DangerZone(
    warning: String,
    label: String,
    disabled: bool,
    on_click: EventHandler<()>,
) -> Element {
    rsx! {
        div { class: "danger-zone",
            span { "{warning}" }
            button {
                class: "danger",
                disabled,
                onclick: move |_| on_click.call(()),
                "{label}"
            }
        }
    }
}

/// Renders a failed request. A 401 signs the user out, and a 403 only shows
/// the message.
#[component]
pub fn ErrorView(error: ApiError) -> Element {
    let session = use_session();
    let unauthorized = error == ApiError::Unauthorized;

    // Reactive on the prop, because a mounted view can change from another
    // error to a 401 without remounting.
    use_effect(use_reactive!(|unauthorized| {
        if unauthorized {
            end_session(session);
        }
    }));

    rsx! {
        div { class: "error-box", "{error}" }
    }
}

/// Focuses the element when it mounts. Browsers honor the `autofocus`
/// attribute only once per page, so a dialog opened later needs this.
pub async fn focus_on_mount(event: MountedEvent) {
    // Focus only saves the user a click, so a failure to focus is ignored.
    let _ = event.set_focus(true).await;
}

/// Drops a session the server no longer accepts. The dashboard layout then
/// redirects to sign-in.
pub fn end_session(mut session: Signal<Option<Session>>) {
    let failed = session.write().take();
    if let Some(failed) = failed {
        failed.forget();
    }
}
