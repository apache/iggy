/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::VERSION;
use crate::shard::IggyShard;
use crate::slab::traits_ext::{EntityComponentSystem, IntoComponents};
use crate::versioning::SemanticVersion;
use iggy_common::{IggyDuration, IggyError, Stats};
use std::cell::RefCell;
use sysinfo::{Pid, ProcessesToUpdate, System as SysinfoSystem};

thread_local! {
    static SYSINFO: RefCell<Option<SysinfoSystem>> = const { RefCell::new(None) };
}

impl IggyShard {
    pub async fn get_stats(&self) -> Result<Stats, IggyError> {
        assert_eq!(self.id, 0, "GetStats should only be called on shard0");

        SYSINFO.with(|sysinfo_cell| {
            let mut sysinfo_opt = sysinfo_cell.borrow_mut();

            if sysinfo_opt.is_none() {
                let mut sys = SysinfoSystem::new_all();
                sys.refresh_all();
                *sysinfo_opt = Some(sys);
            }

            let sys = sysinfo_opt.as_mut().unwrap();
            let process_id = std::process::id();
            sys.refresh_cpu_all();
            sys.refresh_memory();
            sys.refresh_processes(ProcessesToUpdate::Some(&[Pid::from_u32(process_id)]), true);

            let total_cpu_usage = sys.global_cpu_usage();
            let total_memory = sys.total_memory().into();
            let available_memory = sys.available_memory().into();
            let clients_count = self.client_manager.get_clients().len() as u32;
            let hostname = sysinfo::System::host_name().unwrap_or("unknown_hostname".to_string());
            let os_name = sysinfo::System::name().unwrap_or("unknown_os_name".to_string());
            let os_version =
                sysinfo::System::long_os_version().unwrap_or("unknown_os_version".to_string());
            let kernel_version =
                sysinfo::System::kernel_version().unwrap_or("unknown_kernel_version".to_string());

            let mut stats = Stats {
                process_id,
                total_cpu_usage,
                total_memory,
                available_memory,
                clients_count,
                hostname,
                os_name,
                os_version,
                kernel_version,
                iggy_server_version: VERSION.to_owned(),
                iggy_server_semver: SemanticVersion::current()
                    .ok()
                    .and_then(|v| v.get_numeric_version().ok()),
                ..Default::default()
            };

            if let Some(process) = sys
                .processes()
                .values()
                .find(|p| p.pid() == Pid::from_u32(process_id))
            {
                stats.process_id = process.pid().as_u32();
                stats.cpu_usage = process.cpu_usage();
                stats.memory_usage = process.memory().into();
                stats.run_time = IggyDuration::new_from_secs(process.run_time());
                stats.start_time = IggyDuration::new_from_secs(process.start_time())
                    .as_micros()
                    .into();

                let disk_usage = process.disk_usage();
                stats.read_bytes = disk_usage.total_read_bytes.into();
                stats.written_bytes = disk_usage.total_written_bytes.into();
            }

            self.streams.with_components(|stream_components| {
                let (stream_roots, stream_stats) = stream_components.into_components();
                // Iterate through all streams
                for (stream_id, stream_root) in stream_roots.iter() {
                    stats.streams_count += 1;

                    // Get stream-level stats
                    if let Some(stream_stat) = stream_stats.get(stream_id) {
                        stats.messages_count += stream_stat.messages_count_inconsistent();
                        stats.segments_count += stream_stat.segments_count_inconsistent();
                        stats.messages_size_bytes += stream_stat.size_bytes_inconsistent().into();
                    }

                    // Access topics within this stream
                    stream_root.topics().with_components(|topic_components| {
                        let (topic_roots, ..) = topic_components.into_components();
                        stats.topics_count += topic_roots.len() as u32;

                        // Iterate through all topics in this stream
                        for (_, topic_root) in topic_roots.iter() {
                            // Count partitions in this topic
                            topic_root
                                .partitions()
                                .with_components(|partition_components| {
                                    let (partition_roots, ..) =
                                        partition_components.into_components();
                                    stats.partitions_count += partition_roots.len() as u32;
                                });

                            // Count consumer groups in this topic
                            stats.consumer_groups_count +=
                                topic_root.consumer_groups().len() as u32;
                        }
                    });
                }
            });

            Ok(stats)
        })
    }
}
