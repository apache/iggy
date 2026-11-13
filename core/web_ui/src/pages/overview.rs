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

use crate::components::{ErrorView, Loading, PageHeader, Stat, api_client, use_session};
use crate::format;
use crate::models::Stats;

#[component]
pub fn Overview() -> Element {
    let session = use_session();
    let stats = use_resource(move || async move { api_client(session).stats().await });

    let body = match &*stats.read() {
        None => rsx! { Loading {} },
        Some(Err(error)) => rsx! { ErrorView { error: error.clone() } },
        Some(Ok(stats)) => rsx! { StatsSections { stats: stats.clone() } },
    };

    rsx! {
        PageHeader { title: "Overview" }
        {body}
    }
}

#[component]
fn StatsSections(stats: Stats) -> Element {
    rsx! {
        h2 { "Server" }
        div { class: "stats",
            Stat { label: "Version", value: stats.iggy_server_version.clone() }
            Stat { label: "Host", value: stats.hostname.clone() }
            Stat { label: "System", value: format!("{} ({})", stats.os_name, stats.os_version) }
            Stat { label: "Kernel", value: stats.kernel_version.clone() }
            Stat { label: "Process ID", value: stats.process_id.to_string() }
            Stat { label: "Started", value: format::timestamp(stats.start_time) }
            Stat { label: "Uptime", value: format::elapsed(stats.run_time) }
        }
        h2 { "Resources" }
        div { class: "stats",
            Stat { label: "Server CPU", value: format!("{:.2} %", stats.cpu_usage) }
            Stat { label: "System CPU", value: format!("{:.2} %", stats.total_cpu_usage) }
            Stat { label: "Server memory", value: format::byte_size(&stats.memory_usage) }
            Stat {
                label: "Available memory",
                value: format!(
                    "{} of {}",
                    format::byte_size(&stats.available_memory),
                    format::byte_size(&stats.total_memory),
                ),
            }
            Stat {
                label: "Free disk",
                value: format!(
                    "{} of {}",
                    format::byte_size(&stats.free_disk_space),
                    format::byte_size(&stats.total_disk_space),
                ),
            }
            Stat {
                label: "Open files",
                value: format!("{} of {}", stats.open_files_count, stats.open_files_limit),
            }
            Stat { label: "Threads", value: stats.threads_count.to_string() }
            Stat { label: "Read", value: format::byte_size(&stats.read_bytes) }
            Stat { label: "Written", value: format::byte_size(&stats.written_bytes) }
        }
        h2 { "Data" }
        div { class: "stats",
            Stat { label: "Streams", value: stats.streams_count.to_string() }
            Stat { label: "Topics", value: stats.topics_count.to_string() }
            Stat { label: "Partitions", value: stats.partitions_count.to_string() }
            Stat { label: "Segments", value: stats.segments_count.to_string() }
            Stat { label: "Messages", value: stats.messages_count.to_string() }
            Stat { label: "Messages size", value: format::byte_size(&stats.messages_size_bytes) }
            Stat { label: "Clients", value: stats.clients_count.to_string() }
            Stat { label: "Consumer groups", value: stats.consumer_groups_count.to_string() }
        }
    }
}
