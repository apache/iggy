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

use std::cell::RefCell;
use std::path::PathBuf;
use std::sync::OnceLock;
use sysinfo::System as SysinfoSystem;
use system_stats::SystemProbe;

/// Process- and host-level portion of the stats reply, probed via `sysinfo`.
/// These describe the whole process, not shard or metadata state, so any one
/// shard can serve them without aggregation. The CPU fields are deltas over the
/// serving thread's own [`SYSINFO`] refresh history, so they vary by serving
/// shard (a shard's first probe reports zero CPU).
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

pub fn probe_system_stats() -> SystemStats {
    let host = HOST_IDENTITY.get_or_init(HostIdentity::probe);
    let probe = SYSINFO.with_borrow_mut(|slot| {
        let sys = slot.get_or_insert_with(SysinfoSystem::new);
        SystemProbe::capture(sys)
    });

    SystemStats {
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
        hostname: host.hostname.clone(),
        os_name: host.os_name.clone(),
        os_version: host.os_version.clone(),
        kernel_version: host.kernel_version.clone(),
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
        assert!(!stats.hostname.is_empty());
    }
}
