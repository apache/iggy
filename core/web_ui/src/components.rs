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

#[component]
pub fn PageHeader(title: String, children: Element) -> Element {
    rsx! {
        header { class: "page-header",
            nav { class: "breadcrumbs", {children} }
            h1 { "{title}" }
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
