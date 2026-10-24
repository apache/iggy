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
use crate::components::{ConfirmDelete, FormActions, Modal, api_client, use_session};
use crate::format;
use crate::models::{CreateStream, StreamDetails};

use super::non_blank_options;

#[component]
pub fn NewStreamForm(on_close: EventHandler<()>, on_created: EventHandler<()>) -> Element {
    let session = use_session();
    let mut name = use_signal(String::new);
    let mut encryption_key = use_signal(String::new);
    let mut busy = use_signal(|| false);
    let mut error = use_signal(|| None::<String>);

    let submit = move |event: FormEvent| async move {
        event.prevent_default();
        busy.set(true);
        let stream = CreateStream {
            name: name().trim().to_owned(),
            options: non_blank_options([("encryption_key", encryption_key())]),
        };
        let result = api_client(session).create_stream(&stream).await;
        busy.set(false);
        match result {
            Ok(_) => on_created.call(()),
            Err(failure) => error.set(Some(failure.to_string())),
        }
    };

    rsx! {
        Modal { title: "New stream", on_close,
            form { class: "form", onsubmit: submit,
                label {
                    "Name"
                    input {
                        required: true,
                        autofocus: true,
                        value: name,
                        oninput: move |event| name.set(event.value()),
                    }
                }
                label {
                    "Encryption key (optional)"
                    input {
                        class: "mono",
                        autocomplete: "off",
                        placeholder: "Base64 of exactly 32 bytes",
                        value: encryption_key,
                        oninput: move |event| encryption_key.set(event.value()),
                    }
                }
                FormActions {
                    submit_label: "Create",
                    busy: busy(),
                    error: error(),
                    on_cancel: on_close,
                }
            }
        }
    }
}

#[component]
pub fn StreamSettingsForm(
    stream: StreamDetails,
    on_close: EventHandler<()>,
    on_saved: EventHandler<()>,
) -> Element {
    let session = use_session();
    let navigator = navigator();
    let stream_id = stream.id;
    // Taken once at mount, so a refreshed `stream` cannot make the untouched
    // name look edited and revert someone else's rename.
    let current_name = use_hook(|| stream.name.clone());
    let mut name = use_signal(|| stream.name.clone());
    let mut busy = use_signal(|| false);
    let mut error = use_signal(|| None::<String>);
    let mut deleting = use_signal(|| false);

    let rename = move |event: FormEvent| {
        let current_name = current_name.clone();
        async move {
            event.prevent_default();
            let next_name = name().trim().to_owned();
            if next_name == current_name {
                on_close.call(());
                return;
            }
            busy.set(true);
            let result = api_client(session)
                .rename_stream(stream_id, &next_name)
                .await;
            busy.set(false);
            match result {
                Ok(()) => on_saved.call(()),
                Err(failure) => error.set(Some(failure.to_string())),
            }
        }
    };

    let delete = move |_| async move {
        busy.set(true);
        let result = api_client(session).delete_stream(stream_id).await;
        busy.set(false);
        match result {
            Ok(()) => {
                navigator.replace(Route::Streams {});
            }
            Err(failure) => error.set(Some(failure.to_string())),
        }
    };

    let key_source = format::option_text(&stream.options, "encryption_key_source")
        .unwrap_or("unknown")
        .to_owned();
    let key_fingerprint =
        format::option_text(&stream.options, "encryption_key_fingerprint").map(str::to_owned);
    let kek_fingerprints = format::option_text(&stream.options, "encryption_kek_fingerprint")
        .map(|fingerprints| fingerprints.replace(',', ", "));

    if deleting() {
        return rsx! {
            Modal { title: "Delete stream {stream.name}", on_close,
                ConfirmDelete {
                    title: "Delete stream",
                    confirm_text: stream.name.clone(),
                    on_confirm: delete,
                    on_cancel: move |_| deleting.set(false),
                    busy: busy(),
                    error: error(),
                }
            }
        };
    }

    rsx! {
        Modal { title: "Stream settings", on_close,
            form { class: "form", onsubmit: rename,
                label {
                    "Name"
                    input {
                        required: true,
                        value: name,
                        oninput: move |event| name.set(event.value()),
                    }
                }
                dl { class: "details",
                    dt { "Encryption key source" }
                    dd { "{key_source}" }
                    if let Some(fingerprint) = key_fingerprint {
                        dt { "Key fingerprint" }
                        dd { class: "mono", "{fingerprint}" }
                    }
                    if let Some(fingerprints) = kek_fingerprints {
                        dt { "Server key fingerprints" }
                        dd { class: "mono", "{fingerprints}" }
                    }
                }
                FormActions {
                    submit_label: "Save",
                    busy: busy(),
                    error: error(),
                    on_cancel: on_close,
                }
            }
            div { class: "danger-zone",
                span { "Deleting a stream removes all its topics and messages." }
                button {
                    class: "danger",
                    onclick: move |_| {
                        error.set(None);
                        deleting.set(true);
                    },
                    "Delete stream"
                }
            }
        }
    }
}
