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

use crate::shard::IggyShard;
use human_repr::HumanCount;
use iggy_common::IggyError;
use std::rc::Rc;
use tracing::{error, info, trace};

pub fn spawn_sysinfo_printer(shard: Rc<IggyShard>) {
    let period = shard
        .config
        .system
        .logging
        .sysinfo_print_interval
        .get_duration();
    info!(
        "System info logger is enabled, OS info will be printed every: {:?}",
        period
    );
    let shard_clone = shard.clone();
    shard
        .task_registry
        .periodic("print_sysinfo")
        .every(period)
        .tick(move |_shutdown| print_sysinfo(shard_clone.clone()))
        .spawn();
}

fn get_open_file_descriptors() -> Option<usize> {
    #[cfg(target_os = "linux")]
    {
        let pid = std::process::id();
        let fd_path = format!("/proc/{}/fd", pid);
        if let Ok(entries) = std::fs::read_dir(&fd_path) {
            return Some(entries.count());
        }
    }
    None
}

async fn print_sysinfo(shard: Rc<IggyShard>) -> Result<(), IggyError> {
    trace!("Printing system information...");

    let stats = match shard.get_stats().await {
        Ok(stats) => stats,
        Err(e) => {
            error!("Failed to get system information. Error: {e}");
            return Ok(());
        }
    };

    let free_memory_percent = (stats.available_memory.as_bytes_u64() as f64
        / stats.total_memory.as_bytes_u64() as f64)
        * 100f64;

    let open_files_info = if let Some(open_files) = get_open_file_descriptors() {
        format!(", OpenFDs: {}", open_files)
    } else {
        String::new()
    };

    info!(
        "CPU: {:.2}%/{:.2}% (IggyUsage/Total), Mem: {:.2}%/{}/{}/{} (Free/IggyUsage/TotalUsed/Total), Clients: {}, Messages: {}, Read: {}, Written: {}{}",
        stats.cpu_usage,
        stats.total_cpu_usage,
        free_memory_percent,
        stats.memory_usage,
        stats.total_memory - stats.available_memory,
        stats.total_memory,
        stats.clients_count.human_count_bare().to_string(),
        stats.messages_count.human_count_bare().to_string(),
        stats.read_bytes,
        stats.written_bytes,
        open_files_info,
    );

    Ok(())
}
