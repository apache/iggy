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

use crate::api::ApiClient;
use crate::auth::Session;
use crate::components::use_session;
use crate::models::LoginUser;
use crate::{LOGO, Route};

#[component]
pub fn SignIn() -> Element {
    let mut session = use_session();
    let navigator = navigator();
    let mut username = use_signal(String::new);
    let mut password = use_signal(String::new);
    let mut error = use_signal(|| None::<String>);
    let mut submitting = use_signal(|| false);

    use_effect(move || {
        if session.read().is_some() {
            navigator.replace(Route::Streams {});
        }
    });

    let submit = move |event: FormEvent| async move {
        event.prevent_default();
        submitting.set(true);
        let credentials = LoginUser {
            username: username(),
            password: password(),
        };
        let result = ApiClient::default().login(&credentials).await;
        submitting.set(false);

        match result {
            Ok(identity) => {
                let new_session = Session::from(identity);
                new_session.persist();
                session.set(Some(new_session));
            }
            Err(failure) => error.set(Some(failure.to_string())),
        }
    };

    rsx! {
        main { class: "auth",
            form { class: "auth-card", onsubmit: submit,
                img { class: "auth-logo", src: LOGO, alt: "Apache Iggy" }
                label {
                    "Username"
                    input {
                        name: "username",
                        autocomplete: "username",
                        required: true,
                        value: username,
                        oninput: move |event| username.set(event.value()),
                    }
                }
                label {
                    "Password"
                    input {
                        name: "password",
                        r#type: "password",
                        autocomplete: "current-password",
                        required: true,
                        value: password,
                        oninput: move |event| password.set(event.value()),
                    }
                }
                if let Some(message) = error() {
                    p { class: "form-error", "{message}" }
                }
                button { r#type: "submit", disabled: submitting(), "Sign in" }
            }
        }
    }
}
