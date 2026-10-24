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
use crate::components::{Crumb, ErrorView, Loading, PageHeader, Stat, api_client, use_session};
use crate::format;

#[component]
pub fn Streams() -> Element {
    let session = use_session();
    let streams = use_resource(move || async move {
        let mut streams = api_client(session).streams().await?;
        streams.sort_by_key(|stream| stream.id);
        Ok::<_, ApiError>(streams)
    });

    let body = match &*streams.read() {
        None => rsx! { Loading {} },
        Some(Err(error)) => rsx! { ErrorView { error: error.clone() } },
        Some(Ok(streams)) if streams.is_empty() => rsx! { p { class: "muted", "No streams yet." } },
        Some(Ok(streams)) => rsx! {
            table {
                thead {
                    tr {
                        th { "ID" }
                        th { "Name" }
                        th { class: "num", "Topics" }
                        th { class: "num", "Messages" }
                        th { class: "num", "Size" }
                        th { "Created" }
                    }
                }
                tbody {
                    for stream in streams {
                        tr { key: "{stream.id}",
                            td { "{stream.id}" }
                            td {
                                Link { to: Route::StreamPage { stream_id: stream.id }, "{stream.name}" }
                            }
                            td { class: "num", "{stream.topics_count}" }
                            td { class: "num", "{stream.messages_count}" }
                            td { class: "num", "{stream.size}" }
                            td { {format::timestamp(stream.created_at)} }
                        }
                    }
                }
            }
        },
    };

    rsx! {
        PageHeader { title: "Streams" }
        {body}
    }
}

#[component]
pub fn StreamPage(stream_id: u32) -> Element {
    let session = use_session();
    let stream = use_resource(use_reactive!(|(stream_id,)| async move {
        api_client(session).stream(stream_id).await
    }));

    let body = match &*stream.read() {
        None => rsx! { Loading {} },
        Some(Err(error)) => rsx! { ErrorView { error: error.clone() } },
        Some(Ok(stream)) => rsx! {
            PageHeader { title: stream.name.clone(),
                Crumb { to: Route::Streams {}, label: "Streams" }
            }
            div { class: "stats",
                Stat { label: "ID", value: stream.id.to_string() }
                Stat { label: "Topics", value: stream.topics_count.to_string() }
                Stat { label: "Messages", value: stream.messages_count.to_string() }
                Stat { label: "Size", value: stream.size.clone() }
                Stat { label: "Created", value: format::timestamp(stream.created_at) }
            }
            h2 { "Topics" }
            if stream.topics.is_empty() {
                p { class: "muted", "No topics in this stream." }
            } else {
                table {
                    thead {
                        tr {
                            th { "ID" }
                            th { "Name" }
                            th { class: "num", "Partitions" }
                            th { class: "num", "Messages" }
                            th { class: "num", "Size" }
                            th { "Message expiry" }
                            th { "Created" }
                        }
                    }
                    tbody {
                        for topic in &stream.topics {
                            tr { key: "{topic.id}",
                                td { "{topic.id}" }
                                td {
                                    Link {
                                        to: Route::TopicPage { stream_id, topic_id: topic.id },
                                        "{topic.name}"
                                    }
                                }
                                td { class: "num", "{topic.partitions_count}" }
                                td { class: "num", "{topic.messages_count}" }
                                td { class: "num", "{topic.size}" }
                                td { {format::message_expiry(&topic.options)} }
                                td { {format::timestamp(topic.created_at)} }
                            }
                        }
                    }
                }
            }
        },
    };

    rsx! { {body} }
}
