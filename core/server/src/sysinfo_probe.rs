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

use arc_swap::ArcSwap;
use nix::sys::resource::{Resource, getrlimit};
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, OnceLock};
use std::thread;
use std::time::{Duration, Instant};
use sysinfo::System as SysinfoSystem;
use system_stats::{SystemProbe, count_open_files, count_open_files_without_scan};

/// The last completed process-wide sample. CPU deltas cover the sampler interval.
/// Slow probes leave the previous sample available without delaying shard readers.
#[derive(Clone, Default)]
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
    /// 0 when unknown; directory-scan fallbacks refresh at most every ten seconds.
    pub open_files_count: u64,
    /// Soft `RLIMIT_NOFILE`, the ceiling `open()` fails at. 0 when unknown.
    pub open_files_limit: u64,
    pub hostname: String,
    pub os_name: String,
    pub os_version: String,
    pub kernel_version: String,
    pub free_disk_space: u64,
    pub total_disk_space: u64,
}

/// Host identity is sampled only once, before the shard threads start.
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

static PUBLISHED_STATS: LazyLock<ArcSwap<SystemStats>> =
    LazyLock::new(|| ArcSwap::from_pointee(SystemStats::default()));
static SAMPLER_STARTED: AtomicBool = AtomicBool::new(false);

const SYSTEM_STATS_INTERVAL: Duration = Duration::from_secs(1);
const OPEN_FILES_SCAN_INTERVAL: Duration = Duration::from_secs(10);

/// Capture the configured data directory before starting the sampler.
pub fn init_stats_data_path(path: PathBuf) {
    let _ = STATS_DATA_PATH.set(path);
}

/// Start one process-lifetime sampler before any shard threads exist.
/// Samples refresh every second, or the printer interval when shorter. The
/// descriptor-scan fallback refreshes every ten seconds or printer interval.
/// Sampling time extends these intervals; readers always get the last completed sample.
pub fn start_system_stats_sampler(sysinfo_print_interval: Duration) -> io::Result<()> {
    if SAMPLER_STARTED.swap(true, Ordering::AcqRel) {
        return Ok(());
    }
    let interval = if sysinfo_print_interval.is_zero() {
        SYSTEM_STATS_INTERVAL
    } else {
        sysinfo_print_interval.min(SYSTEM_STATS_INTERVAL)
    };
    let scan_interval = if sysinfo_print_interval.is_zero() {
        OPEN_FILES_SCAN_INTERVAL
    } else {
        sysinfo_print_interval.min(OPEN_FILES_SCAN_INTERVAL)
    };
    let mut system = SysinfoSystem::new();
    let initial = SystemStats::capture(&mut system, 0, true);
    let mut open_files_count = initial.open_files_count;
    PUBLISHED_STATS.store(Arc::new(initial));
    let spawned = thread::Builder::new()
        .name("iggy-system-stats".to_owned())
        .spawn(move || {
            let mut last_scan = Instant::now();
            loop {
                thread::sleep(interval);
                let scan = last_scan.elapsed() >= scan_interval;
                let sample = SystemStats::capture(&mut system, open_files_count, scan);
                open_files_count = sample.open_files_count;
                if scan {
                    last_scan = Instant::now();
                }
                PUBLISHED_STATS.store(Arc::new(sample));
            }
        });
    if spawned.is_err() {
        SAMPLER_STARTED.store(false, Ordering::Release);
    }
    spawned.map(drop)
}

/// Read the last completed sample without filesystem access or a blocking lock.
pub fn probe_system_stats() -> SystemStats {
    PUBLISHED_STATS.load().as_ref().clone()
}

impl SystemStats {
    fn capture(sys: &mut SysinfoSystem, previous_open_files: u64, scan_open_files: bool) -> Self {
        let host = HOST_IDENTITY.get_or_init(HostIdentity::probe);
        let probe = SystemProbe::capture(sys);
        let disk = STATS_DATA_PATH
            .get()
            .and_then(|path| fs2::statvfs(path).ok());
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
                .or_else(|| scan_open_files.then(count_open_files).flatten())
                .unwrap_or(previous_open_files),
            open_files_limit: getrlimit(Resource::RLIMIT_NOFILE).map_or(0, |(soft, _)| soft),
            hostname: host.hostname.clone(),
            os_name: host.os_name.clone(),
            os_version: host.os_version.clone(),
            kernel_version: host.kernel_version.clone(),
            free_disk_space: disk.as_ref().map_or(0, fs2::FsStats::available_space),
            total_disk_space: disk.as_ref().map_or(0, fs2::FsStats::total_space),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_completed_sample_when_reading_stats_should_reuse_it() {
        let mut system = SysinfoSystem::new();
        let sample = SystemStats::capture(&mut system, 0, true);
        assert_eq!(sample.process_id, std::process::id());
        assert!(sample.total_memory > 0);
        assert_ne!(sample.hostname, "");
        PUBLISHED_STATS.store(Arc::new(sample));

        let published = PUBLISHED_STATS.load_full();
        let first = probe_system_stats();
        let second = probe_system_stats();
        assert_eq!(first.read_bytes, second.read_bytes);
        assert_eq!(first.memory_usage, published.memory_usage);
        assert!(Arc::ptr_eq(&published, &PUBLISHED_STATS.load_full()));
    }
}
