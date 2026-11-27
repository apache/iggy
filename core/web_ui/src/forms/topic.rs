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
use crate::models::{CreateTopic, ResourceOptions, TopicUpdate};

use super::{
    InitialPartitionsCount, MAX_PARTITIONS_COUNT, PartitionsCount, ResourceName, non_blank_options,
    run_request,
};

const EXPIRY_HINT: &str = "e.g. 7 days, 12h or unlimited";
const SIZE_HINT: &str = "e.g. 1 GiB or unlimited";
/// A failed partition change can still have applied (a 504 says so), so the
/// form reloads the topic and asks for the count again.
const RETRY_HINT: &str = "Check the partitions count before you try again.";

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
    let busy = use_signal(|| false);
    let mut error = use_signal(|| None::<String>);

    let submit = move |event: FormEvent| async move {
        event.prevent_default();
        let parsed = name
            .read()
            .parse::<ResourceName>()
            .map_err(|invalid| invalid.to_string())
            .and_then(|name| {
                let count = partitions
                    .read()
                    .parse::<InitialPartitionsCount>()
                    .map_err(|invalid| invalid.to_string())?;
                Ok((name, count))
            });
        let (ResourceName(name), InitialPartitionsCount(partitions_count)) = match parsed {
            Ok(parsed) => parsed,
            Err(invalid) => {
                error.set(Some(invalid));
                return;
            }
        };
        let compression_algorithm = match compression.read().as_str() {
            "none" => String::new(),
            algorithm => algorithm.to_owned(),
        };
        let topic = CreateTopic {
            name,
            partitions_count,
            options: non_blank_options([
                ("compression_algorithm", compression_algorithm),
                ("message_expiry", message_expiry()),
                ("max_topic_size", max_topic_size()),
            ]),
        };
        let created = run_request(
            session,
            busy,
            error,
            api_client(session).create_topic(stream_id, &topic),
        )
        .await;
        if created.is_some() {
            on_created.call(());
        }
    };

    rsx! {
        Modal { title: "New topic", on_close, busy: busy(),
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
                    "Partitions"
                    input {
                        r#type: "number",
                        min: "0",
                        max: "{MAX_PARTITIONS_COUNT}",
                        required: true,
                        value: partitions,
                        oninput: move |event| partitions.set(event.value()),
                    }
                }
                label {
                    "Compression"
                    select {
                        onchange: move |event| compression.set(event.value()),
                        option { value: "none", selected: *compression.read() == "none", "None" }
                        option { value: "gzip", selected: *compression.read() == "gzip", "Gzip" }
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

/// Changes the name, message expiry and max size. An option field holds a
/// value only when the client set one. A blank field means the server
/// default, as in [`NewTopicForm`], so clearing a field resets it.
#[component]
pub fn TopicSettingsForm(
    stream_id: u32,
    topic_id: u32,
    topic_name: String,
    options: ResourceOptions,
    on_close: EventHandler<()>,
    on_saved: EventHandler<()>,
) -> Element {
    let session = use_session();
    let navigator = navigator();
    // Taken once at mount, so a refreshed topic cannot make an untouched
    // field look edited and revert someone else's change.
    let initial_name = use_signal(|| topic_name.clone());
    let initial_expiry =
        use_signal(|| explicit_input(&options, "message_expiry", format::message_expiry_input));
    let initial_size =
        use_signal(|| explicit_input(&options, "max_topic_size", format::max_topic_size));
    let mut name = use_signal(|| topic_name.clone());
    let mut message_expiry = use_signal(|| initial_expiry.cloned());
    let mut max_topic_size = use_signal(|| initial_size.cloned());
    let busy = use_signal(|| false);
    let mut error = use_signal(|| None::<String>);
    let mut deleting = use_signal(|| false);

    let save = move |event: FormEvent| async move {
        event.prevent_default();
        let ResourceName(next_name) = match name.read().parse::<ResourceName>() {
            Ok(name) => name,
            Err(invalid) => {
                error.set(Some(invalid.to_string()));
                return;
            }
        };
        let mut update = TopicUpdate::default();
        if next_name != *initial_name.read() {
            update.name = Some(next_name);
        }
        // Only edited fields are sent, so a prefilled value that is just
        // display text never has to round-trip through the server parser.
        match edited(&message_expiry.read(), &initial_expiry.read()) {
            Some(Edit::Set(value)) => update.message_expiry = Some(value),
            Some(Edit::Reset) => update.reset.push("message_expiry".to_owned()),
            None => {}
        }
        match edited(&max_topic_size.read(), &initial_size.read()) {
            Some(Edit::Set(value)) => update.max_topic_size = Some(value),
            Some(Edit::Reset) => update.reset.push("max_topic_size".to_owned()),
            None => {}
        }
        if update.is_empty() {
            on_close.call(());
            return;
        }
        let saved = run_request(
            session,
            busy,
            error,
            api_client(session).update_topic(stream_id, topic_id, &update),
        )
        .await;
        if saved.is_some() {
            on_saved.call(());
        }
    };

    let delete = move |_| async move {
        let deleted = run_request(
            session,
            busy,
            error,
            api_client(session).delete_topic(stream_id, topic_id),
        )
        .await;
        if deleted.is_some() {
            navigator.replace(Route::StreamPage { stream_id });
        }
    };

    if deleting() {
        return rsx! {
            Modal { title: "Delete topic {topic_name}", on_close, busy: busy(),
                ConfirmDelete {
                    title: "Delete topic",
                    confirm_text: topic_name.clone(),
                    on_confirm: delete,
                    on_cancel: move |_| deleting.set(false),
                    busy: busy(),
                    error: error(),
                }
            }
        };
    }

    let expiry_placeholder = format!(
        "Server default ({}), {EXPIRY_HINT}",
        format::message_expiry(&options)
    );
    let size_placeholder = format!(
        "Server default ({}), {SIZE_HINT}",
        format::max_topic_size(&options)
    );
    let compression = format::compression_algorithm(&options);

    rsx! {
        Modal { title: "Topic settings", on_close, busy: busy(),
            form { class: "form", onsubmit: save,
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
                    "Message expiry"
                    input {
                        placeholder: expiry_placeholder,
                        value: message_expiry,
                        oninput: move |event| message_expiry.set(event.value()),
                    }
                }
                label {
                    "Max topic size"
                    input {
                        placeholder: size_placeholder,
                        value: max_topic_size,
                        oninput: move |event| max_topic_size.set(event.value()),
                    }
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
            DangerZone {
                warning: "Deleting a topic removes all its partitions and messages.",
                label: "Delete topic",
                disabled: busy(),
                on_click: move |_| {
                    error.set(None);
                    deleting.set(true);
                },
            }
        }
    }
}

#[component]
pub fn AddPartitionsForm(
    stream_id: u32,
    topic_id: u32,
    partitions_count: u32,
    on_close: EventHandler<()>,
    on_saved: EventHandler<()>,
    on_failed: EventHandler<()>,
) -> Element {
    let session = use_session();
    let mut count = use_signal(|| "1".to_owned());
    let busy = use_signal(|| false);
    let mut error = use_signal(|| None::<String>);

    let submit = move |event: FormEvent| async move {
        event.prevent_default();
        let PartitionsCount(to_add) = match count.read().parse::<PartitionsCount>() {
            Ok(count) => count,
            Err(invalid) => {
                error.set(Some(invalid.to_string()));
                return;
            }
        };
        let added = run_request(
            session,
            busy,
            error,
            api_client(session).create_partitions(stream_id, topic_id, to_add),
        )
        .await;
        if added.is_some() {
            on_saved.call(());
        } else {
            count.set(String::new());
            add_retry_hint(error);
            on_failed.call(());
        }
    };

    rsx! {
        Modal { title: "Add partitions", on_close, busy: busy(),
            form { class: "form", onsubmit: submit,
                p { class: "muted", {partitions_sentence(partitions_count)} }
                label {
                    "Partitions to add"
                    input {
                        r#type: "number",
                        min: "1",
                        max: "{MAX_PARTITIONS_COUNT}",
                        required: true,
                        onmounted: focus_on_mount,
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
    topic_id: u32,
    topic_name: String,
    partitions_count: u32,
    on_close: EventHandler<()>,
    on_saved: EventHandler<()>,
    on_failed: EventHandler<()>,
) -> Element {
    let session = use_session();
    let mut count = use_signal(|| "1".to_owned());
    let mut confirmed_count = use_signal(|| None::<u32>);
    let busy = use_signal(|| false);
    let mut error = use_signal(|| None::<String>);

    let delete = move |_| async move {
        let Some(to_delete) = confirmed_count() else {
            return;
        };
        let deleted = run_request(
            session,
            busy,
            error,
            api_client(session).delete_partitions(stream_id, topic_id, to_delete),
        )
        .await;
        if deleted.is_some() {
            on_saved.call(());
        } else {
            confirmed_count.set(None);
            count.set(String::new());
            add_retry_hint(error);
            on_failed.call(());
        }
    };

    if let Some(to_delete) = confirmed_count() {
        let noun = if to_delete == 1 {
            "partition"
        } else {
            "partitions"
        };
        return rsx! {
            Modal { title: "Delete {to_delete} {noun}", on_close, busy: busy(),
                ConfirmDelete {
                    title: "Delete {noun}",
                    confirm_text: format!("{topic_name}/partitions"),
                    on_confirm: delete,
                    on_cancel: move |_| confirmed_count.set(None),
                    busy: busy(),
                    error: error(),
                }
            }
        };
    }

    rsx! {
        Modal { title: "Delete partitions", on_close, busy: busy(),
            form {
                class: "form",
                onsubmit: move |event: FormEvent| {
                    event.prevent_default();
                    match count.read().parse() {
                        Ok(PartitionsCount(to_delete)) => {
                            error.set(None);
                            confirmed_count.set(Some(to_delete));
                        }
                        Err(invalid) => error.set(Some(invalid.to_string())),
                    }
                },
                p { class: "muted",
                    {partitions_sentence(partitions_count)}
                    " The newest partitions are removed first, along with their messages."
                }
                label {
                    "Partitions to delete"
                    input {
                        r#type: "number",
                        min: "1",
                        max: "{partitions_count.min(MAX_PARTITIONS_COUNT)}",
                        required: true,
                        onmounted: focus_on_mount,
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

/// A change to one option field of [`TopicSettingsForm`].
enum Edit {
    Set(String),
    Reset,
}

fn edited(current: &str, initial: &str) -> Option<Edit> {
    match current.trim() {
        unchanged if unchanged == initial => None,
        "" => Some(Edit::Reset),
        value => Some(Edit::Set(value.to_owned())),
    }
}

/// The form text of an option the client set, or blank for a server default.
fn explicit_input(
    options: &ResourceOptions,
    key: &str,
    input: fn(&ResourceOptions) -> String,
) -> String {
    if format::is_explicit(options, key) {
        input(options)
    } else {
        String::new()
    }
}

fn partitions_sentence(partitions_count: u32) -> String {
    match partitions_count {
        1 => "This topic has 1 partition.".to_owned(),
        count => format!("This topic has {count} partitions."),
    }
}

fn add_retry_hint(mut error: Signal<Option<String>>) {
    if let Some(message) = error.write().as_mut() {
        let separator = if message.ends_with('.') { " " } else { ". " };
        message.push_str(separator);
        message.push_str(RETRY_HINT);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_untouched_field_should_send_nothing() {
        assert!(edited(" 7d ", "7d").is_none());
        assert!(edited("", "").is_none());
    }

    #[test]
    fn given_cleared_field_should_reset_it() {
        assert!(matches!(edited("  ", "7d"), Some(Edit::Reset)));
    }

    #[test]
    fn given_new_value_should_set_it() {
        assert!(matches!(edited(" 1 day", ""), Some(Edit::Set(value)) if value == "1 day"));
    }
}
