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

use std::rc::Rc;

use dioxus::prelude::*;

use crate::Route;
use crate::api::ApiError;
use crate::components::{Crumb, ErrorView, Loading, PageHeader, api_client, use_session};
use crate::format;
use crate::models::IggyMessage;

const PAGE_SIZE: u32 = 20;
const PAYLOAD_PREVIEW_CHARS: usize = 60;

#[derive(Debug, Clone, PartialEq)]
struct MessagesPage {
    topic_name: String,
    /// Offset the page was polled from. Retention can make the first message
    /// later than this.
    requested_offset: u64,
    /// Newest offset in the partition, taken from the poll reply so it always
    /// agrees with `messages`.
    current_offset: u64,
    messages: Vec<Rc<IggyMessage>>,
}

/// `offset` is the first offset of the page. `None` follows the newest page,
/// so reloading picks up new messages.
#[component]
pub fn Messages(stream_id: u32, topic_id: u32, partition_id: u32, offset: Option<u64>) -> Element {
    let session = use_session();
    let navigator = navigator();

    let mut page = use_resource(use_reactive!(|(
        stream_id,
        topic_id,
        partition_id,
        offset,
    )| async move {
        let client = api_client(session);
        let topic = client.topic(stream_id, topic_id).await?;
        let partition = topic
            .partitions
            .iter()
            .find(|partition| partition.id == partition_id)
            .ok_or(ApiError::NotFound)?;
        let requested_offset = offset.unwrap_or_else(|| {
            partition
                .current_offset
                .saturating_sub(u64::from(PAGE_SIZE) - 1)
        });

        let polled = client
            .poll_messages(
                stream_id,
                topic_id,
                partition_id,
                requested_offset,
                PAGE_SIZE,
            )
            .await?;
        Ok::<_, ApiError>(MessagesPage {
            topic_name: topic.name,
            requested_offset,
            current_offset: polled.current_offset,
            messages: polled.messages.into_iter().map(Rc::new).collect(),
        })
    }));

    let mut show = move |target: Option<u64>| {
        // A second click would restart the poll in flight and fetch the same
        // page again.
        if page.pending() {
            return;
        }
        if target == offset {
            page.restart();
        } else {
            navigator.push(Route::Messages {
                stream_id,
                topic_id,
                partition_id,
                offset: target,
            });
        }
    };

    let header = rsx! {
        PageHeader { title: format!("Partition {partition_id} messages"),
            Crumb { to: Route::Streams {}, label: "Streams" }
            Crumb { to: Route::StreamPage { stream_id }, label: format!("Stream {stream_id}") }
            Crumb { to: Route::TopicPage { stream_id, topic_id }, label: format!("Topic {topic_id}") }
        }
    };

    let body = match &*page.read() {
        None => rsx! { Loading {} },
        Some(Err(error)) => rsx! { ErrorView { error: error.clone() } },
        Some(Ok(loaded)) => match (loaded.messages.first(), loaded.messages.last()) {
            (Some(first), Some(last)) => {
                let first_offset = first.header.offset;
                let shown_last_offset = last.header.offset;
                let current_offset = loaded.current_offset;
                // The server skipped ahead of the requested offset, so
                // nothing older is left.
                let at_oldest = first_offset > loaded.requested_offset;
                let has_older = first_offset > 0 && !at_oldest;
                let has_newer = shown_last_offset < current_offset;
                rsx! {
                    div { class: "pager",
                        span { class: "muted",
                            "Topic {loaded.topic_name}, showing offsets {first_offset} to {shown_last_offset} of {current_offset}"
                        }
                        button {
                            class: "secondary",
                            disabled: !has_older,
                            onclick: move |_| show(Some(first_offset.saturating_sub(u64::from(PAGE_SIZE)))),
                            "Older"
                        }
                        button {
                            class: "secondary",
                            disabled: !has_newer,
                            onclick: move |_| show(Some(shown_last_offset + 1)),
                            "Newer"
                        }
                        button { class: "secondary", onclick: move |_| show(None), "Latest" }
                    }
                    table {
                        thead {
                            tr {
                                th { class: "num", "Offset" }
                                th { "Timestamp" }
                                th { "ID" }
                                th { class: "num", "Bytes" }
                                th { "Payload" }
                            }
                        }
                        tbody {
                            for message in loaded.messages.iter().rev() {
                                MessageRow { key: "{message.header.offset}", message: Rc::clone(message) }
                            }
                        }
                    }
                }
            }
            _ if loaded.requested_offset == 0 => rsx! {
                p { class: "muted", "Topic {loaded.topic_name} has no messages in this partition." }
            },
            _ => rsx! {
                div { class: "pager",
                    span { class: "muted",
                        "Topic {loaded.topic_name} has no messages from offset {loaded.requested_offset}."
                    }
                    button { class: "secondary", onclick: move |_| show(None), "Latest" }
                }
            },
        },
    };

    rsx! {
        {header}
        {body}
    }
}

#[component]
fn MessageRow(message: Rc<IggyMessage>) -> Element {
    // The full payload can be up to 64 MB, so it is decoded only once the
    // row is opened.
    let mut open = use_signal(|| false);
    let preview = format::payload_preview(&message.payload, PAYLOAD_PREVIEW_CHARS);
    let header = &message.header;

    rsx! {
        tr {
            td { class: "num", "{header.offset}" }
            td { {format::timestamp_precise(header.timestamp)} }
            td { class: "mono", {format::message_id(header.id)} }
            td { class: "num", "{header.payload_length}" }
            td {
                details { ontoggle: move |_| open.toggle(),
                    summary { class: "mono", "{preview}" }
                    if open() {
                        pre { {format::payload(&message.payload)} }
                    }
                }
            }
        }
    }
}
