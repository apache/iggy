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
use crate::models::{CreateTopic, TopicDetails, TopicUpdate};

use super::{PartitionsCount, non_blank_options};

const EXPIRY_HINT: &str = "e.g. 7 days, 12h or unlimited";
const SIZE_HINT: &str = "e.g. 1 GiB or unlimited";

#[component]
pub fn NewTopicForm(
    stream_id: u32,
    on_close: EventHandler<()>,
    on_created: EventHandler<()>,
) -> Element {
    let session = use_session();
    let mut name = use_signal(String::new);
    let mut partitions = use_signal(|| "1".to_owned());
    let mut compression = use_signal(|| "none".to_owned());
    let mut message_expiry = use_signal(String::new);
    let mut max_topic_size = use_signal(String::new);
    let mut busy = use_signal(|| false);
    let mut error = use_signal(|| None::<String>);

    let submit = move |event: FormEvent| async move {
        event.prevent_default();
        let partitions_count = match partitions().parse() {
            Ok(PartitionsCount(count)) => count,
            Err(invalid) => {
                error.set(Some(invalid.to_string()));
                return;
            }
        };
        let compression_algorithm = match compression().as_str() {
            "none" => String::new(),
            algorithm => algorithm.to_owned(),
        };
        let topic = CreateTopic {
            name: name().trim().to_owned(),
            partitions_count,
            options: non_blank_options([
                ("compression_algorithm", compression_algorithm),
                ("message_expiry", message_expiry()),
                ("max_topic_size", max_topic_size()),
            ]),
        };
        busy.set(true);
        let result = api_client(session).create_topic(stream_id, &topic).await;
        busy.set(false);
        match result {
            Ok(_) => on_created.call(()),
            Err(failure) => error.set(Some(failure.to_string())),
        }
    };

    rsx! {
        Modal { title: "New topic", on_close,
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
                    "Partitions"
                    input {
                        r#type: "number",
                        min: "1",
                        required: true,
                        value: partitions,
                        oninput: move |event| partitions.set(event.value()),
                    }
                }
                label {
                    "Compression"
                    select {
                        onchange: move |event| compression.set(event.value()),
                        option { value: "none", selected: compression() == "none", "None" }
                        option { value: "gzip", selected: compression() == "gzip", "Gzip" }
                    }
                }
                label {
                    "Message expiry"
                    input {
                        placeholder: "Server default, {EXPIRY_HINT}",
                        value: message_expiry,
                        oninput: move |event| message_expiry.set(event.value()),
                    }
                }
                label {
                    "Max topic size"
                    input {
                        placeholder: "Server default, {SIZE_HINT}",
                        value: max_topic_size,
                        oninput: move |event| max_topic_size.set(event.value()),
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
pub fn TopicSettingsForm(
    stream_id: u32,
    topic: TopicDetails,
    on_close: EventHandler<()>,
    on_saved: EventHandler<()>,
) -> Element {
    let session = use_session();
    let navigator = navigator();
    let topic_id = topic.id;
    // Taken once at mount, so a refreshed `topic` cannot make an untouched
    // field look edited and revert someone else's change.
    let (initial_name, initial_expiry, initial_size) = use_hook(|| {
        (
            topic.name.clone(),
            format::message_expiry_input(&topic.options),
            format::max_topic_size(&topic.options),
        )
    });
    let mut name = use_signal(|| initial_name.clone());
    let mut message_expiry = use_signal(|| initial_expiry.clone());
    let mut max_topic_size = use_signal(|| initial_size.clone());
    let mut reset_expiry = use_signal(|| false);
    let mut reset_size = use_signal(|| false);
    let mut busy = use_signal(|| false);
    let mut error = use_signal(|| None::<String>);
    let mut deleting = use_signal(|| false);

    let save = move |event: FormEvent| {
        let initial = (
            initial_name.clone(),
            initial_expiry.clone(),
            initial_size.clone(),
        );
        async move {
            event.prevent_default();
            let (initial_name, initial_expiry, initial_size) = initial;
            let mut update = TopicUpdate::default();
            let next_name = name().trim().to_owned();
            if next_name != initial_name {
                update.name = Some(next_name);
            }
            // Only edited fields are sent, so a prefilled value that is just
            // display text never has to round-trip through the server parser.
            if reset_expiry() {
                update.reset.push("message_expiry".to_owned());
            } else if message_expiry().trim() != initial_expiry {
                update.message_expiry = Some(message_expiry().trim().to_owned());
            }
            if reset_size() {
                update.reset.push("max_topic_size".to_owned());
            } else if max_topic_size().trim() != initial_size {
                update.max_topic_size = Some(max_topic_size().trim().to_owned());
            }
            if update.is_empty() {
                on_close.call(());
                return;
            }
            busy.set(true);
            let result = api_client(session)
                .update_topic(stream_id, topic_id, &update)
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
        let result = api_client(session).delete_topic(stream_id, topic_id).await;
        busy.set(false);
        match result {
            Ok(()) => {
                navigator.replace(Route::StreamPage { stream_id });
            }
            Err(failure) => error.set(Some(failure.to_string())),
        }
    };

    if deleting() {
        return rsx! {
            Modal { title: "Delete topic {topic.name}", on_close,
                ConfirmDelete {
                    title: "Delete topic",
                    confirm_text: topic.name.clone(),
                    on_confirm: delete,
                    on_cancel: move |_| deleting.set(false),
                    busy: busy(),
                    error: error(),
                }
            }
        };
    }

    let compression = format::option_text(&topic.options, "compression_algorithm")
        .unwrap_or("none")
        .to_owned();

    rsx! {
        Modal { title: "Topic settings", on_close,
            form { class: "form", onsubmit: save,
                label {
                    "Name"
                    input {
                        required: true,
                        value: name,
                        oninput: move |event| name.set(event.value()),
                    }
                }
                label {
                    "Message expiry"
                    input {
                        placeholder: EXPIRY_HINT,
                        disabled: reset_expiry(),
                        value: message_expiry,
                        oninput: move |event| message_expiry.set(event.value()),
                    }
                }
                label { class: "checkbox",
                    input {
                        r#type: "checkbox",
                        checked: reset_expiry(),
                        onchange: move |event| reset_expiry.set(event.checked()),
                    }
                    "Reset message expiry to the server default"
                }
                label {
                    "Max topic size"
                    input {
                        placeholder: SIZE_HINT,
                        disabled: reset_size(),
                        value: max_topic_size,
                        oninput: move |event| max_topic_size.set(event.value()),
                    }
                }
                label { class: "checkbox",
                    input {
                        r#type: "checkbox",
                        checked: reset_size(),
                        onchange: move |event| reset_size.set(event.checked()),
                    }
                    "Reset max topic size to the server default"
                }
                dl { class: "details",
                    dt { "Compression" }
                    dd { "{compression}" }
                }
                FormActions {
                    submit_label: "Save",
                    busy: busy(),
                    error: error(),
                    on_cancel: on_close,
                }
            }
            div { class: "danger-zone",
                span { "Deleting a topic removes all its partitions and messages." }
                button {
                    class: "danger",
                    onclick: move |_| {
                        error.set(None);
                        deleting.set(true);
                    },
                    "Delete topic"
                }
            }
        }
    }
}

#[component]
pub fn AddPartitionsForm(
    stream_id: u32,
    topic_id: u32,
    on_close: EventHandler<()>,
    on_saved: EventHandler<()>,
) -> Element {
    let session = use_session();
    let mut count = use_signal(|| "1".to_owned());
    let mut busy = use_signal(|| false);
    let mut error = use_signal(|| None::<String>);

    let submit = move |event: FormEvent| async move {
        event.prevent_default();
        let partitions_count = match count().parse() {
            Ok(PartitionsCount(partitions_count)) => partitions_count,
            Err(invalid) => {
                error.set(Some(invalid.to_string()));
                return;
            }
        };
        busy.set(true);
        let result = api_client(session)
            .create_partitions(stream_id, topic_id, partitions_count)
            .await;
        busy.set(false);
        match result {
            Ok(()) => on_saved.call(()),
            Err(failure) => error.set(Some(failure.to_string())),
        }
    };

    rsx! {
        Modal { title: "Add partitions", on_close,
            form { class: "form", onsubmit: submit,
                label {
                    "Partitions to add"
                    input {
                        r#type: "number",
                        min: "1",
                        required: true,
                        autofocus: true,
                        value: count,
                        oninput: move |event| count.set(event.value()),
                    }
                }
                FormActions {
                    submit_label: "Add",
                    busy: busy(),
                    error: error(),
                    on_cancel: on_close,
                }
            }
        }
    }
}

#[component]
pub fn DeletePartitionsForm(
    stream_id: u32,
    topic: TopicDetails,
    on_close: EventHandler<()>,
    on_saved: EventHandler<()>,
) -> Element {
    let session = use_session();
    let topic_id = topic.id;
    let mut count = use_signal(|| "1".to_owned());
    let mut confirmed_count = use_signal(|| None::<u32>);
    let mut busy = use_signal(|| false);
    let mut error = use_signal(|| None::<String>);

    let delete = move |_| async move {
        let Some(partitions_count) = confirmed_count() else {
            return;
        };
        busy.set(true);
        let result = api_client(session)
            .delete_partitions(stream_id, topic_id, partitions_count)
            .await;
        busy.set(false);
        match result {
            Ok(()) => on_saved.call(()),
            Err(failure) => {
                // A failed delete can still have applied (a 504 says so), so
                // the count and the confirmation must be entered again.
                confirmed_count.set(None);
                error.set(Some(failure.to_string()));
            }
        }
    };

    if let Some(partitions_count) = confirmed_count() {
        let noun = if partitions_count == 1 {
            "partition"
        } else {
            "partitions"
        };
        return rsx! {
            Modal { title: "Delete {partitions_count} {noun}", on_close,
                ConfirmDelete {
                    title: "Delete {noun}",
                    confirm_text: format!("{}/partitions", topic.name),
                    on_confirm: delete,
                    on_cancel: move |_| confirmed_count.set(None),
                    busy: busy(),
                    error: error(),
                }
            }
        };
    }

    rsx! {
        Modal { title: "Delete partitions", on_close,
            form {
                class: "form",
                onsubmit: move |event: FormEvent| {
                    event.prevent_default();
                    match count().parse() {
                        Ok(PartitionsCount(partitions_count)) => {
                            error.set(None);
                            confirmed_count.set(Some(partitions_count));
                        }
                        Err(invalid) => error.set(Some(invalid.to_string())),
                    }
                },
                p { class: "muted",
                    "The newest partitions are removed first, along with their messages."
                }
                label {
                    "Partitions to delete"
                    input {
                        r#type: "number",
                        min: "1",
                        max: "{topic.partitions_count}",
                        required: true,
                        autofocus: true,
                        value: count,
                        oninput: move |event| count.set(event.value()),
                    }
                }
                FormActions {
                    submit_label: "Continue",
                    busy: busy(),
                    error: error(),
                    on_cancel: on_close,
                    danger: true,
                }
            }
        }
    }
}
