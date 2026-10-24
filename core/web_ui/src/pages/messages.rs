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
use crate::api::ApiError;
use crate::components::{Crumb, ErrorView, Loading, PageHeader, api_client, use_session};
use crate::format;
use crate::models::Message;

const PAGE_SIZE: u32 = 20;
const PAYLOAD_PREVIEW_CHARS: usize = 60;

#[derive(Debug, Clone, PartialEq)]
struct MessagesPage {
    topic_name: String,
    /// Offset of the newest message, `None` when the partition is empty.
    last_offset: Option<u64>,
    /// Offset of the first message shown. Retention can make it later than
    /// the offset that was asked for.
    first_offset: u64,
    /// The server answered with a later offset than requested, so nothing
    /// older is left.
    at_oldest: bool,
    messages: Vec<Message>,
}

#[component]
pub fn Messages(stream_id: u32, topic_id: u32, partition_id: u32) -> Element {
    let session = use_session();
    // `None` follows the newest page, so reloading picks up new messages.
    let mut requested_offset = use_signal(|| None::<u64>);

    let page = use_resource(use_reactive!(
        |(stream_id, topic_id, partition_id)| async move {
            let client = api_client(session);
            let topic = client.topic(stream_id, topic_id).await?;
            let partition = topic
                .partitions
                .iter()
                .find(|partition| partition.id == partition_id)
                .ok_or(ApiError::NotFound)?;
            let last_offset = (partition.messages_count > 0).then_some(partition.current_offset);
            let newest_page_start = last_offset
                .map(|last| last.saturating_sub(u64::from(PAGE_SIZE) - 1))
                .unwrap_or(0);
            let requested = requested_offset().unwrap_or(newest_page_start);

            let polled = client
                .poll_messages(stream_id, topic_id, partition_id, requested, PAGE_SIZE)
                .await?;
            let first_offset = polled
                .messages
                .first()
                .map_or(requested, |message| message.header.offset);
            Ok::<_, ApiError>(MessagesPage {
                topic_name: topic.name,
                last_offset,
                first_offset,
                at_oldest: first_offset > requested,
                messages: polled.messages,
            })
        }
    ));

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
        Some(Ok(page)) if page.last_offset.is_none() => rsx! {
            p { class: "muted", "Topic {page.topic_name} has no messages in this partition." }
        },
        Some(Ok(page)) => {
            let first_offset = page.first_offset;
            let last_offset = page.last_offset.unwrap_or(0);
            let page_size = u64::from(PAGE_SIZE);
            let has_older = first_offset > 0 && !page.at_oldest;
            let has_newer = first_offset + page_size <= last_offset;
            let shown_last_offset = page
                .messages
                .last()
                .map_or(first_offset, |message| message.header.offset);
            rsx! {
                div { class: "pager",
                    span { class: "muted",
                        "Topic {page.topic_name}, showing offsets {first_offset} to {shown_last_offset} of {last_offset}"
                    }
                    button {
                        class: "secondary",
                        disabled: !has_older,
                        onclick: move |_| requested_offset.set(Some(first_offset.saturating_sub(page_size))),
                        "Older"
                    }
                    button {
                        class: "secondary",
                        disabled: !has_newer,
                        onclick: move |_| requested_offset.set(Some(first_offset + page_size)),
                        "Newer"
                    }
                    button { class: "secondary", onclick: move |_| requested_offset.set(None), "Latest" }
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
                        for message in page.messages.iter().rev() {
                            MessageRow { key: "{message.header.offset}", message: message.clone() }
                        }
                    }
                }
            }
        }
    };

    rsx! {
        {header}
        {body}
    }
}

#[component]
fn MessageRow(message: Message) -> Element {
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
