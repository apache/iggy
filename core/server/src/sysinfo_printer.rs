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

//! Periodic one-line log of process and host usage.
//!
//! Spawned on shard 0 only: the numbers describe the whole process, so one
//! line per node is enough, and the client count is already a cross-shard
//! gather.
//! CPU usage is the latest sampler delta (1 s), not the interval between logs.

use crate::shell::ServerShard;
use crate::sysinfo_probe::{SystemStats, probe_system_stats};
use consensus::MetadataHandle;
use iggy_common::IggyByteSize;
use metadata::impls::metadata::StreamsFrontend;
use shard::Receiver;
use std::fmt;
use std::rc::Rc;
use std::time::Duration;
use tracing::level_filters::LevelFilter;
use tracing::{info, trace};

/// Run the printer until `stop` fires, logging one line every `interval`.
pub async fn run_sysinfo_printer(shard: Rc<ServerShard>, stop: Receiver<()>, interval: Duration) {
    info!("System info logger is enabled, OS info will be printed every: {interval:?}");
    loop {
        // `Ok(_)`: stop signalled -> exit. `Err(_)`: interval elapsed -> print.
        match compio::time::timeout(interval, stop.recv()).await {
            Ok(_) => break,
            Err(_) => print_sysinfo(&shard).await,
        }
    }
    trace!(shard = shard.id, "sysinfo printer exited");
}

async fn print_sysinfo(shard: &Rc<ServerShard>) {
    // The global max level, not `tracing::enabled!`: the logger's idle
    // OpenTelemetry layers veto every `enabled` query, so that is always false.
    if LevelFilter::current() < LevelFilter::INFO {
        return;
    }
    let clients_count = shard.count_all_clients().await;
    let (messages_size_bytes, messages_count) = messages_totals(shard);
    let system = probe_system_stats();
    let line = SysinfoLine {
        system,
        messages_size_bytes,
        messages_count,
        clients_count,
    };
    info!("{line}");
}

/// Message bytes and count of the node, summed over the per-stream counters,
/// so a tick costs one step per stream and not per partition.
fn messages_totals(shard: &ServerShard) -> (u64, u64) {
    shard.plane.metadata().mux_stm.streams().read(|inner| {
        inner
            .items
            .iter()
            .fold((0u64, 0u64), |(size_bytes, count), (_, stream)| {
                (
                    size_bytes.saturating_add(stream.stats.size_bytes_inconsistent()),
                    count.saturating_add(stream.stats.messages_count_inconsistent()),
                )
            })
    })
}

/// One sample, rendered in the 0.8.2 server's layout plus open descriptors.
struct SysinfoLine {
    system: SystemStats,
    messages_size_bytes: u64,
    messages_count: u64,
    clients_count: usize,
}

impl SysinfoLine {
    // Precision loss starts above 2^53 bytes, far beyond any host's memory.
    #[allow(clippy::cast_precision_loss)]
    fn free_memory_percent(&self) -> f64 {
        if self.system.total_memory == 0 {
            return 0.0;
        }
        self.system.available_memory as f64 / self.system.total_memory as f64 * 100.0
    }
}

impl fmt::Display for SysinfoLine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let system = &self.system;
        write!(
            f,
            "CPU: {:.2}%/{:.2}% (IggyUsage/Total), Mem: {:.2}%/{}/{}/{} (Free/IggyUsage/TotalUsed/Total), Disk: {}/{} (Free/Total), IggyUsage: {}, Clients: {}, Messages: {}, Read: {}, Written: {}",
            system.cpu_usage,
            system.total_cpu_usage,
            self.free_memory_percent(),
            IggyByteSize::from(system.memory_usage),
            IggyByteSize::from(system.total_memory.saturating_sub(system.available_memory)),
            IggyByteSize::from(system.total_memory),
            IggyByteSize::from(system.free_disk_space),
            IggyByteSize::from(system.total_disk_space),
            IggyByteSize::from(self.messages_size_bytes),
            self.clients_count,
            self.messages_count,
            IggyByteSize::from(system.read_bytes),
            IggyByteSize::from(system.written_bytes),
        )?;
        if system.threads_count > 0 {
            write!(f, ", Threads: {}", system.threads_count)?;
        }
        match (system.open_files_count, system.open_files_limit) {
            (0, _) => {}
            (open_files, 0) => write!(f, ", OpenFDs: {open_files}/unknown (Current/Max)")?,
            (open_files, limit) => write!(f, ", OpenFDs: {open_files}/{limit} (Current/Max)")?,
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(open_files_count: u64, open_files_limit: u64) -> SysinfoLine {
        SysinfoLine {
            system: SystemStats {
                process_id: 1,
                cpu_usage: 1.5,
                total_cpu_usage: 20.0,
                memory_usage: 1_000_000,
                total_memory: 4_000_000,
                available_memory: 1_000_000,
                run_time: 0,
                start_time: 0,
                read_bytes: 0,
                written_bytes: 0,
                threads_count: 8,
                open_files_count,
                open_files_limit,
                hostname: String::new(),
                os_name: String::new(),
                os_version: String::new(),
                kernel_version: String::new(),
                free_disk_space: 0,
                total_disk_space: 0,
            },
            messages_size_bytes: 0,
            messages_count: 42,
            clients_count: 3,
        }
    }

    #[test]
    fn given_open_files_and_limit_when_rendering_should_print_current_and_max() {
        let rendered = line(12, 1024).to_string();

        assert!(rendered.starts_with("CPU: 1.50%/20.00% (IggyUsage/Total), Mem: 25.00%/"));
        assert!(rendered.contains(", Clients: 3, Messages: 42, "));
        assert!(rendered.ends_with(", Threads: 8, OpenFDs: 12/1024 (Current/Max)"));
    }

    #[test]
    fn given_unreadable_limit_when_rendering_should_print_unknown_max() {
        let rendered = line(12, 0).to_string();

        assert!(rendered.ends_with(", OpenFDs: 12/unknown (Current/Max)"));
    }

    #[test]
    fn given_uncountable_open_files_when_rendering_should_omit_open_fds() {
        let rendered = line(0, 1024).to_string();

        assert!(!rendered.contains("OpenFDs"));
    }

    #[test]
    fn given_zero_total_memory_when_rendering_should_report_zero_free_percent() {
        let mut sample = line(0, 0);
        sample.system.total_memory = 0;
        sample.system.available_memory = 0;

        assert!(sample.to_string().contains("Mem: 0.00%/"));
    }
}
