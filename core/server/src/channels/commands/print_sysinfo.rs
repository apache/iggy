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

use std::rc::Rc;

use crate::shard::IggyShard;
use crate::streaming::utils::memory_pool;
use human_repr::HumanCount;
use iggy_common::IggyError;
use tracing::{error, info, trace};

pub async fn print_sys_info(shard: Rc<IggyShard>) -> Result<(), IggyError> {
    let config = &shard.config.system.logging;
    let interval = config.sysinfo_print_interval;

    if interval.is_zero() {
        info!("SysInfoPrinter is disabled.");
        return Ok(());
    }
    info!("SysInfoPrinter is enabled, system information will be printed every {interval}.");
    let mut interval_timer = compio::time::interval(interval.get_duration());
    loop {
        interval_timer.tick().await;
        trace!("Printing system information...");

        let stats = match shard.get_stats().await {
            Ok(stats) => stats,
            Err(e) => {
                error!("Failed to get system information. Error: {e}");
                continue;
            }
        };

        let free_memory_percent = (stats.available_memory.as_bytes_u64() as f64
            / stats.total_memory.as_bytes_u64() as f64)
            * 100f64;

        info!(
            "CPU: {:.2}%/{:.2}% (IggyUsage/Total), Mem: {:.2}%/{}/{}/{} (Free/IggyUsage/TotalUsed/Total), Clients: {}, Messages processed: {}, Read: {}, Written: {}, Uptime: {}",
            stats.cpu_usage,
            stats.total_cpu_usage,
            free_memory_percent,
            stats.memory_usage,
            stats.total_memory - stats.available_memory,
            stats.total_memory,
            stats.clients_count,
            stats.messages_count.human_count_bare(),
            stats.read_bytes,
            stats.written_bytes,
            stats.run_time
        );

        memory_pool().log_stats();
    }
}