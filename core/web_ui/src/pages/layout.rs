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
use crate::auth::Session;
use crate::components::{api_client, use_session};

const LOGO: Asset = asset!("/assets/iggy-apache-darkbg.svg");

/// Shell for every signed-in page. Without a session it redirects to sign-in.
#[component]
pub fn DashboardLayout() -> Element {
    let mut session = use_session();
    let navigator = navigator();

    use_effect(move || {
        if session.read().is_none() {
            navigator.replace(Route::SignIn {});
        }
    });

    let user = use_resource(move || async move {
        let user_id = session.read().as_ref()?.user_id;
        api_client(session).user(user_id).await.ok()
    });

    let sign_out = move |_| async move {
        // The token is dropped locally either way, so a failed revoke only
        // leaves it valid on the server until it expires.
        let _ = api_client(session).logout().await;
        Session::clear();
        session.set(None);
    };

    if session.read().is_none() {
        return rsx! {};
    }

    let username = user
        .read()
        .as_ref()
        .and_then(|user| user.as_ref().map(|user| user.username.clone()))
        .unwrap_or_default();

    rsx! {
        div { class: "dashboard",
            aside { class: "sidebar",
                img { class: "sidebar-logo", src: LOGO, alt: "Apache Iggy" }
                nav {
                    Link { class: "nav-link", active_class: "active", to: Route::Streams {}, "Streams" }
                }
                div { class: "sidebar-footer",
                    span { class: "muted", "{username}" }
                    button { class: "secondary", onclick: sign_out, "Sign out" }
                }
            }
            main { class: "content", Outlet::<Route> {} }
        }
    }
}
