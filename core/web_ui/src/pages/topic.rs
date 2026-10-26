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
use crate::components::{Crumb, ErrorView, Loading, PageHeader, Stat, api_client, use_session};
use crate::format;

#[component]
pub fn TopicPage(stream_id: u32, topic_id: u32) -> Element {
    let session = use_session();
    let topic = use_resource(use_reactive!(|(stream_id, topic_id)| async move {
        api_client(session).topic(stream_id, topic_id).await
    }));

    let body = match &*topic.read() {
        None => rsx! { Loading {} },
        Some(Err(error)) => rsx! { ErrorView { error: error.clone() } },
        Some(Ok(topic)) => rsx! {
            PageHeader { title: topic.name.clone(),
                Crumb { to: Route::Streams {}, label: "Streams" }
                Crumb { to: Route::StreamPage { stream_id }, label: format!("Stream {stream_id}") }
            }
            div { class: "stats",
                Stat { label: "ID", value: topic.id.to_string() }
                Stat { label: "Partitions", value: topic.partitions_count.to_string() }
                Stat { label: "Messages", value: topic.messages_count.to_string() }
                Stat { label: "Size", value: topic.size.clone() }
                Stat { label: "Message expiry", value: format::message_expiry(&topic.options) }
                Stat { label: "Created", value: format::timestamp(topic.created_at) }
            }
            h2 { "Partitions" }
            table {
                thead {
                    tr {
                        th { "ID" }
                        th { class: "num", "Current offset" }
                        th { class: "num", "Segments" }
                        th { class: "num", "Messages" }
                        th { class: "num", "Size" }
                        th { "Created" }
                        th {}
                    }
                }
                tbody {
                    for partition in &topic.partitions {
                        tr { key: "{partition.id}",
                            td { "{partition.id}" }
                            td { class: "num", "{partition.current_offset}" }
                            td { class: "num", "{partition.segments_count}" }
                            td { class: "num", "{partition.messages_count}" }
                            td { class: "num", "{partition.size}" }
                            td { {format::timestamp(partition.created_at)} }
                            td {
                                Link {
                                    to: Route::Messages { stream_id, topic_id, partition_id: partition.id, offset: None },
                                    "Browse messages"
                                }
                            }
                        }
                    }
                }
            }
        },
    };

    rsx! { {body} }
}
