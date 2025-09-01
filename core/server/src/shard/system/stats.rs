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
use crate::versioning::SemanticVersion;
use iggy_common::locking::IggyRwLockFn;
use iggy_common::{IggyDuration, IggyError, Stats};
use std::sync::OnceLock;
use sysinfo::{Pid, ProcessesToUpdate, System as SysinfoSystem};
use tokio::sync::Mutex;

fn sysinfo() -> &'static Mutex<SysinfoSystem> {
    static SYSINFO: OnceLock<Mutex<SysinfoSystem>> = OnceLock::new();
    SYSINFO.get_or_init(|| {
        let mut sys = SysinfoSystem::new_all();
        sys.refresh_all();
        Mutex::new(sys)
    })
}

impl IggyShard {
    pub async fn get_stats(&self) -> Result<Stats, IggyError> {
        let mut sys = sysinfo().lock().await;
        let process_id = std::process::id();
        sys.refresh_cpu_all();
        sys.refresh_memory();
        sys.refresh_processes(ProcessesToUpdate::Some(&[Pid::from_u32(process_id)]), true);

        let total_cpu_usage = sys.global_cpu_usage();
        let total_memory = sys.total_memory().into();
        let available_memory = sys.available_memory().into();
        let clients_count = self.client_manager.borrow().get_clients().len() as u32;
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

        drop(sys);

        //TODO:
        /*
        for stream in self.streams.borrow().values() {
            stats.messages_count += stream.get_messages_count();
            stats.segments_count += stream.get_segments_count();
            stats.messages_size_bytes += stream.get_size();
            stats.streams_count += 1;
            stats.topics_count += stream.topics.len() as u32;
            stats.partitions_count += stream
                .topics
                .values()
                .map(|t| t.partitions.len() as u32)
                .sum::<u32>();
            stats.consumer_groups_count += stream
                .topics
                .values()
                .map(|t| t.consumer_groups.borrow().len() as u32)
                .sum::<u32>();
        }
        */

        Ok(stats)
    }
}
