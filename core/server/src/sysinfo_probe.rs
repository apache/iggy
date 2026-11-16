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
use configs::disk::DiskConfig;
use iggy_common::IggyByteSize;
use nix::sys::resource::{Resource, getrlimit};
use server_common::{DISK_USAGE, VolumeSample};
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, Once, OnceLock};
use std::thread;
use std::time::{Duration, Instant};
use sysinfo::System as SysinfoSystem;
use system_stats::{SystemProbe, count_open_files, count_open_files_without_scan};
use tracing::{info, warn};

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
            hostname: SysinfoSystem::host_name().unwrap_or_else(|_| "unknown_hostname".to_owned()),
            os_name: SysinfoSystem::name().unwrap_or_else(|_| "unknown_os_name".to_owned()),
            os_version: SysinfoSystem::long_os_version()
                .unwrap_or_else(|_| "unknown_os_version".to_owned()),
            kernel_version: SysinfoSystem::kernel_version()
                .unwrap_or_else(|_| "unknown_kernel_version".to_owned()),
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

/// The `[disk]` section, captured once at bootstrap. Unset, the samples carry
/// no limit.
static DISK_CONFIG: OnceLock<DiskConfig> = OnceLock::new();

/// Warns once that a volume without a size, such as ramfs, gets no disk usage
/// limit, since the samples repeat every second.
static SIZELESS_VOLUME_WARNING: Once = Once::new();

static PUBLISHED_STATS: LazyLock<ArcSwap<SystemStats>> =
    LazyLock::new(|| ArcSwap::from_pointee(SystemStats::default()));

const SYSTEM_STATS_INTERVAL: Duration = Duration::from_secs(1);
const OPEN_FILES_SCAN_INTERVAL: Duration = Duration::from_secs(10);

/// Capture the configured data directory before starting the sampler.
pub fn init_stats_data_path(path: PathBuf) {
    let _ = STATS_DATA_PATH.set(path);
}

/// Capture the disk usage limit before starting the sampler, which turns it
/// into bytes on every sample.
pub fn init_disk_usage_limit(disk: DiskConfig) {
    let _ = DISK_CONFIG.set(disk);
}

/// Start one process-lifetime sampler before any shard threads exist.
/// Samples refresh every second. The descriptor-scan fallback refreshes every
/// ten seconds or printer interval.
/// Sampling time extends these intervals; readers always get the last completed sample.
pub fn start_system_stats_sampler(sysinfo_print_interval: Duration) -> io::Result<()> {
    let scan_interval = if sysinfo_print_interval.is_zero() {
        OPEN_FILES_SCAN_INTERVAL
    } else {
        sysinfo_print_interval.min(OPEN_FILES_SCAN_INTERVAL)
    };
    let mut system = SysinfoSystem::new().map_err(io::Error::other)?;
    let initial = SystemStats::capture(&mut system, 0, true);
    let mut open_files_count = initial.open_files_count;
    PUBLISHED_STATS.store(Arc::new(initial));
    if DISK_CONFIG
        .get()
        .is_some_and(|disk| disk.usage_limit_enabled)
        && DISK_USAGE.used_bytes().is_none()
    {
        warn!(
            "cannot measure the volume holding the data directory, so the disk usage limit \
             applies only once a sample succeeds"
        );
    }
    let mut disk_blocked = log_disk_usage_transition(false);
    thread::Builder::new()
        .name("iggy-system-stats".to_owned())
        .spawn(move || {
            let mut last_scan = Instant::now();
            loop {
                thread::sleep(SYSTEM_STATS_INTERVAL);
                // A charge may have set the flag since the last sample, and
                // this sample may clear it again, so look before re-basing.
                disk_blocked = log_disk_usage_transition(disk_blocked);
                let scan = last_scan.elapsed() >= scan_interval;
                let sample = SystemStats::capture(&mut system, open_files_count, scan);
                open_files_count = sample.open_files_count;
                if scan {
                    last_scan = Instant::now();
                }
                PUBLISHED_STATS.store(Arc::new(sample));
                disk_blocked = log_disk_usage_transition(disk_blocked);
            }
        })
        .map(drop)
}

/// Logs when the disk usage limit is first seen reached or cleared, and
/// returns whether it is reached now. Charges set the flag on the shards,
/// which do not log it, so the sampler reports it, before and after each
/// sample.
fn log_disk_usage_transition(was_blocked: bool) -> bool {
    let blocked = DISK_USAGE.is_blocked();
    // Only a sample carrying a limit can block, and only a captured config
    // gives one. That sample also set the estimate.
    if blocked != was_blocked
        && let Some(disk) = DISK_CONFIG.get()
        && let Some(used_bytes) = DISK_USAGE.used_bytes()
    {
        let used = IggyByteSize::from(used_bytes);
        let usage_limit_percent = disk.usage_limit_percent;
        if blocked {
            warn!(%used, usage_limit_percent, "disk usage reached [disk] usage_limit_percent");
        } else {
            info!(%used, usage_limit_percent, "disk usage is back under [disk] usage_limit_percent");
        }
    }
    blocked
}

/// Read the last completed sample without filesystem access or a blocking lock.
pub fn probe_system_stats() -> SystemStats {
    PUBLISHED_STATS.load().as_ref().clone()
}

impl SystemStats {
    fn capture(sys: &mut SysinfoSystem, previous_open_files: u64, scan_open_files: bool) -> Self {
        let host = HOST_IDENTITY.get_or_init(HostIdentity::probe);
        let probe = SystemProbe::capture(sys);
        // Each sample also re-bases the disk usage estimate the write sites
        // charge, and the first one, taken before the shards start, seeds it.
        let disk = STATS_DATA_PATH.get().and_then(|path| {
            DISK_USAGE.rebase(|| {
                let stats = fs2::statvfs(path).ok()?;
                let used_bytes = stats.total_space().saturating_sub(stats.free_space());
                // What df calls the size: the space a writer without root can
                // fill, which a volume resize or another ZFS dataset can move.
                let usable_bytes = used_bytes.saturating_add(stats.available_space());
                let limit_bytes = DISK_CONFIG.get().and_then(|disk| {
                    if disk.usage_limit_enabled && usable_bytes == 0 {
                        SIZELESS_VOLUME_WARNING.call_once(|| {
                            warn!(
                                "the volume holding the data directory reports no size, so the \
                                 disk usage limit does not apply to it"
                            );
                        });
                    }
                    disk.usage_limit_bytes(usable_bytes)
                });
                Some((
                    stats,
                    VolumeSample {
                        used_bytes,
                        limit_bytes,
                    },
                ))
            })
        });
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
        let mut system = SysinfoSystem::new().expect("sysinfo supports this platform");
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
