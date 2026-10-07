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

//! Process- and host-level probe behind the `GetStats` reply: `sysinfo`
//! sampling of this process, the cached host identity, and disk usage of the
//! volume holding the data directory.

use nix::sys::resource::{Resource, getrlimit};
use std::cell::RefCell;
use std::io;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;
use sysinfo::System as SysinfoSystem;
use system_stats::{SystemProbe, count_open_files, count_open_files_without_scan};

/// Process- and host-level portion of the stats reply, probed via `sysinfo`.
/// These describe the whole process, not shard or metadata state, so any one
/// shard can serve them without aggregation. The CPU fields are deltas since
/// the previous refresh of the probed `System`, so they vary by serving shard
/// (a shard's first probe reports zero CPU).
pub struct SystemStats {
    pub process_id: u32,
    pub cpu_usage: f32,
    pub total_cpu_usage: f32,
    pub memory_usage: u64,
    pub total_memory: u64,
    pub available_memory: u64,
    pub run_time: u64,
    pub start_time: u64,
    pub read_bytes: u64,
    pub written_bytes: u64,
    pub threads_count: u32,
    /// 0 when unknown. See [`start_open_files_scan`] for where the count
    /// comes from without a kernel count.
    pub open_files_count: u64,
    /// Soft `RLIMIT_NOFILE`, the ceiling `open()` fails at. 0 when unknown.
    pub open_files_limit: u64,
    pub hostname: String,
    pub os_name: String,
    pub os_version: String,
    pub kernel_version: String,
}

thread_local! {
    // `cpu_usage` is a delta since the previous refresh, so the sampled
    // `System` is kept alive across `GetStats` calls (a freshly created one
    // reports zero CPU). Mirrors the legacy shard-0 stats path.
    static SYSINFO: RefCell<Option<SysinfoSystem>> = const { RefCell::new(None) };
}

/// Host / OS identity is process-static (unlike the per-call CPU and memory
/// samples), so probe it once and clone from the cache on each `GetStats`
/// rather than re-querying sysinfo every call. Process-global, so a `OnceLock`
/// fits better than the per-thread [`SYSINFO`] cell.
struct HostIdentity {
    hostname: String,
    os_name: String,
    os_version: String,
    kernel_version: String,
}

impl HostIdentity {
    fn probe() -> Self {
        Self {
            hostname: SysinfoSystem::host_name().unwrap_or_else(|| "unknown_hostname".to_owned()),
            os_name: SysinfoSystem::name().unwrap_or_else(|| "unknown_os_name".to_owned()),
            os_version: SysinfoSystem::long_os_version()
                .unwrap_or_else(|| "unknown_os_version".to_owned()),
            kernel_version: SysinfoSystem::kernel_version()
                .unwrap_or_else(|| "unknown_kernel_version".to_owned()),
        }
    }
}

static HOST_IDENTITY: OnceLock<HostIdentity> = OnceLock::new();

/// Configured data directory, captured once at bootstrap so the sync stats
/// read path can report disk usage of the volume that holds iggy data rather
/// than an unrelated mount. Process-global because the shard does not carry
/// server config on the read path. Unset (disk stats fall back to 0) until
/// bootstrap.
static STATS_DATA_PATH: OnceLock<PathBuf> = OnceLock::new();

/// Last open-descriptor count from [`start_open_files_scan`], 0 before the
/// first. Process-global because the scan has a thread of its own and any
/// shard can serve `GetStats`.
static PUBLISHED_OPEN_FILES_COUNT: AtomicU64 = AtomicU64::new(0);

/// The longest period of the scan thread, and so the oldest the published
/// count gets.
const OPEN_FILES_SCAN_INTERVAL: Duration = Duration::from_secs(10);

/// Capture the configured data directory for `GetStats` disk reporting.
/// Idempotent: only the first call (process bootstrap) takes effect.
pub fn init_stats_data_path(path: PathBuf) {
    let _ = STATS_DATA_PATH.set(path);
}

/// Free and total bytes of the volume holding the configured data directory,
/// `(0, 0)` before bootstrap or on a probe error.
pub fn stats_disk_space() -> (u64, u64) {
    STATS_DATA_PATH.get().map_or((0, 0), |path| {
        (
            fs2::available_space(path).unwrap_or(0),
            fs2::total_space(path).unwrap_or(0),
        )
    })
}

/// Start the thread that counts open descriptors for `GetStats` and the
/// sysinfo line where the kernel keeps no count. Where the kernel keeps one,
/// start nothing.
///
/// The thread scans every [`OPEN_FILES_SCAN_INTERVAL`], or every
/// `sysinfo_print_interval` when that is shorter and not zero, so each line
/// shows a count from its own interval. The scan blocks for a time that grows
/// with the count, and a shard must not block. `spawn_blocking` is not
/// available, because shards run without the blocking pool of the runtime.
/// The thread runs until the process exits, also while the printer is
/// disabled.
pub fn start_open_files_scan(sysinfo_print_interval: Duration) -> io::Result<()> {
    if count_open_files_without_scan().is_some() {
        return Ok(());
    }
    let interval = if sysinfo_print_interval.is_zero() {
        OPEN_FILES_SCAN_INTERVAL
    } else {
        sysinfo_print_interval.min(OPEN_FILES_SCAN_INTERVAL)
    };
    thread::Builder::new()
        .name("iggy-open-files".to_owned())
        .spawn(move || {
            loop {
                publish_open_files_count(count_open_files());
                thread::sleep(interval);
            }
        })
        .map(drop)
}

/// A failed scan keeps the previous count. Before Linux 6.2, the scan needs a
/// free descriptor, so it fails when the table is full, and 0 would then read
/// as unknown.
fn publish_open_files_count(count: Option<u64>) {
    if let Some(count) = count {
        PUBLISHED_OPEN_FILES_COUNT.store(count, Ordering::Relaxed);
    }
}

/// Probe through this thread's [`SYSINFO`], the `GetStats` path.
pub fn probe_system_stats() -> SystemStats {
    SYSINFO
        .with_borrow_mut(|slot| SystemStats::capture(slot.get_or_insert_with(SysinfoSystem::new)))
}

impl SystemStats {
    /// Probe through `sys`. A caller that keeps its own `sys` gets CPU deltas
    /// over its own interval, and does not reset the window of `GetStats`.
    pub fn capture(sys: &mut SysinfoSystem) -> Self {
        let host = HOST_IDENTITY.get_or_init(HostIdentity::probe);
        let probe = SystemProbe::capture(sys);
        Self {
            process_id: probe.process_id,
            cpu_usage: probe.cpu_usage,
            total_cpu_usage: probe.total_cpu_usage,
            memory_usage: probe.memory_usage,
            total_memory: probe.total_memory,
            available_memory: probe.available_memory,
            // sysinfo reports whole seconds; the wire fields are micros (the
            // SDK decodes them via `IggyDuration` / `IggyTimestamp::from`, both
            // micro-based).
            run_time: probe.run_time_secs.saturating_mul(1_000_000),
            start_time: probe.start_time_secs.saturating_mul(1_000_000),
            read_bytes: probe.read_bytes,
            written_bytes: probe.written_bytes,
            threads_count: probe.threads_count,
            open_files_count: count_open_files_without_scan()
                .unwrap_or_else(|| PUBLISHED_OPEN_FILES_COUNT.load(Ordering::Relaxed)),
            open_files_limit: getrlimit(Resource::RLIMIT_NOFILE).map_or(0, |(soft, _)| soft),
            hostname: host.hostname.clone(),
            os_name: host.os_name.clone(),
            os_version: host.os_version.clone(),
            kernel_version: host.kernel_version.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_system_stats_reports_this_process_and_host_memory() {
        let stats = probe_system_stats();
        // Straight from `sysinfo`, independent of shard state: the pid is our
        // own and any host the test runs on has nonzero total memory. A zero
        // here means the probe wired nothing (the pre-fix stubbed literal).
        assert_eq!(stats.process_id, std::process::id());
        assert!(stats.total_memory > 0);
        assert_ne!(stats.hostname, "");
    }

    #[test]
    fn given_failed_scan_when_publishing_should_keep_previous_count() {
        publish_open_files_count(Some(42));
        publish_open_files_count(None);

        assert_eq!(PUBLISHED_OPEN_FILES_COUNT.load(Ordering::Relaxed), 42);
    }
}
