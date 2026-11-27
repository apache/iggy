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
use crate::components::{
    ConfirmDelete, DangerZone, FormActions, Modal, api_client, focus_on_mount, use_session,
};
use crate::format;
use crate::models::{CreateStream, ResourceOptions};

use super::{ResourceName, non_blank_options, run_request};

#[component]
pub fn NewStreamForm(on_close: EventHandler<()>, on_created: EventHandler<()>) -> Element {
    let session = use_session();
    let mut name = use_signal(String::new);
    let mut encryption_key = use_signal(String::new);
    let busy = use_signal(|| false);
    let mut error = use_signal(|| None::<String>);

    let submit = move |event: FormEvent| async move {
        event.prevent_default();
        let ResourceName(name) = match name.read().parse::<ResourceName>() {
            Ok(name) => name,
            Err(invalid) => {
                error.set(Some(invalid.to_string()));
                return;
            }
        };
        let stream = CreateStream {
            name,
            options: non_blank_options([("encryption_key", encryption_key())]),
        };
        let created = run_request(
            session,
            busy,
            error,
            api_client(session).create_stream(&stream),
        )
        .await;
        if created.is_some() {
            on_created.call(());
        }
    };

    rsx! {
        Modal { title: "New stream", on_close, busy: busy(),
            form { class: "form", onsubmit: submit,
                label {
                    "Name"
                    input {
                        required: true,
                        onmounted: focus_on_mount,
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
    stream_id: u32,
    stream_name: String,
    options: ResourceOptions,
    on_close: EventHandler<()>,
    on_saved: EventHandler<()>,
) -> Element {
    let session = use_session();
    let navigator = navigator();
    // Taken once at mount, so a refreshed `stream_name` cannot make the
    // untouched name look edited and revert someone else's rename.
    let initial_name = use_signal(|| stream_name.clone());
    let mut name = use_signal(|| stream_name.clone());
    let busy = use_signal(|| false);
    let mut error = use_signal(|| None::<String>);
    let mut deleting = use_signal(|| false);

    let rename = move |event: FormEvent| async move {
        event.prevent_default();
        let ResourceName(next_name) = match name.read().parse::<ResourceName>() {
            Ok(name) => name,
            Err(invalid) => {
                error.set(Some(invalid.to_string()));
                return;
            }
        };
        if next_name == *initial_name.read() {
            on_close.call(());
            return;
        }
        let renamed = run_request(
            session,
            busy,
            error,
            api_client(session).rename_stream(stream_id, &next_name),
        )
        .await;
        if renamed.is_some() {
            on_saved.call(());
        }
    };

    let delete = move |_| async move {
        let deleted = run_request(
            session,
            busy,
            error,
            api_client(session).delete_stream(stream_id),
        )
        .await;
        if deleted.is_some() {
            navigator.replace(Route::Streams {});
        }
    };

    if deleting() {
        return rsx! {
            Modal { title: "Delete stream {stream_name}", on_close, busy: busy(),
                ConfirmDelete {
                    title: "Delete stream",
                    confirm_text: stream_name.clone(),
                    on_confirm: delete,
                    on_cancel: move |_| deleting.set(false),
                    busy: busy(),
                    error: error(),
                }
            }
        };
    }

    let key_source = format::option_text(&options, "encryption_key_source")
        .unwrap_or("unknown")
        .to_owned();
    let key_fingerprint =
        format::option_text(&options, "encryption_key_fingerprint").map(str::to_owned);
    let kek_fingerprints = format::option_text(&options, "encryption_kek_fingerprint")
        .map(|fingerprints| fingerprints.replace(',', ", "));

    rsx! {
        Modal { title: "Stream settings", on_close, busy: busy(),
            form { class: "form", onsubmit: rename,
                label {
                    "Name"
                    input {
                        required: true,
                        onmounted: focus_on_mount,
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
            DangerZone {
                warning: "Deleting a stream removes all its topics and messages.",
                label: "Delete stream",
                disabled: busy(),
                on_click: move |_| {
                    error.set(None);
                    deleting.set(true);
                },
            }
        }
    }
}
