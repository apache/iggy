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

//! Stats scoped to the resources a process may actually use.
//!
//! `sysinfo`'s host-wide numbers describe the whole machine, so a process
//! confined by a cpuset or a memory-capped cgroup (systemd `AllowedCPUs=` /
//! `MemoryMax=`, container limits) reports its neighbors' CPU load and a
//! memory total it can never allocate. On a multi-tenant host that leaks
//! host sizing to anyone who can read a stats endpoint.
//! [`SystemProbe::capture`] scopes the numbers to the process's allowed CPU
//! set and effective cgroup memory cap, and falls back to the host-wide
//! values when the process is unconfined.

use cpu_allocation::allowed_cpus;
use std::sync::OnceLock;
use sysinfo::{Pid, Process, ProcessesToUpdate, System};

mod cgroup_memory;

use cgroup_memory::cgroup_available_memory;

/// One sample of the calling process's resource usage plus system totals
/// scoped to what the process may actually use.
///
/// The CPU fields are deltas over the passed `System`'s refresh history,
/// so the first capture on a fresh `System` reports zero. `run_time_secs`
/// and `start_time_secs` are whole seconds (sysinfo's granularity);
/// callers convert to their wire units.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SystemProbe {
    pub process_id: u32,
    pub cpu_usage: f32,
    pub total_cpu_usage: f32,
    pub memory_usage: u64,
    pub total_memory: u64,
    pub available_memory: u64,
    pub run_time_secs: u64,
    pub start_time_secs: u64,
    pub read_bytes: u64,
    pub written_bytes: u64,
    pub threads_count: u32,
}

impl SystemProbe {
    /// Refresh `sys` and sample the calling process.
    ///
    /// Refreshes everything it reads, so `System::new()` is enough;
    /// `System::new_all()` would keep the full host process table alive
    /// for no benefit. Keep `sys` alive across captures: the CPU numbers
    /// are deltas since its previous refresh.
    pub fn capture(sys: &mut System) -> Self {
        let process_id = std::process::id();
        let pid = Pid::from_u32(process_id);
        sys.refresh_cpu_all();
        sys.refresh_memory();
        sys.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);

        let mut probe = Self {
            process_id,
            cpu_usage: 0.0,
            total_cpu_usage: scoped_total_cpu_usage(sys),
            memory_usage: 0,
            total_memory: sys.total_memory(),
            available_memory: sys.available_memory(),
            run_time_secs: 0,
            start_time_secs: 0,
            read_bytes: 0,
            written_bytes: 0,
            threads_count: 0,
        };

        if let Some(process) = sys.process(pid) {
            probe.cpu_usage = process.cpu_usage();
            probe.memory_usage = process.memory();
            probe.run_time_secs = process.run_time();
            probe.start_time_secs = process.start_time();
            let disk_usage = process.disk_usage();
            probe.read_bytes = disk_usage.total_read_bytes;
            probe.written_bytes = disk_usage.total_written_bytes;
            probe.threads_count = process
                .tasks()
                .map_or(0, |tasks| u32::try_from(tasks.len()).unwrap_or(u32::MAX));

            if let Some(memory) = cgroup_scoped_memory(sys, process) {
                probe.total_memory = memory.total;
                probe.available_memory = memory.available;
            }
        }

        probe
    }
}

static ALLOWED_CPUS: OnceLock<Vec<usize>> = OnceLock::new();

/// Snapshot the process's allowed CPU set for the scoped total CPU usage.
///
/// Call once from the main thread at startup, before any thread pins
/// itself to a core: `sched_getaffinity` reports the calling thread's
/// mask, so a capture from a pinned shard thread would record that single
/// core as the whole process's set.
pub fn capture_allowed_cpus() {
    ALLOWED_CPUS.get_or_init(allowed_cpus);
}

/// Memory totals scoped to the process's effective cgroup cap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CgroupMemory {
    total: u64,
    available: u64,
}

/// Average CPU usage over the cores this process may run on, or the
/// host-global average when the process is unrestricted.
///
/// `global_cpu_usage` averages every host core, so a cpuset-confined
/// process would report its neighbors' load. The allowed set is the
/// [`capture_allowed_cpus`] boot snapshot; without one this stays on the
/// host-global average.
fn scoped_total_cpu_usage(sys: &System) -> f32 {
    ALLOWED_CPUS
        .get()
        .and_then(|allowed| allowed_cores_cpu_usage(sys, allowed))
        .unwrap_or_else(|| sys.global_cpu_usage())
}

/// Memory totals scoped to the process's memory cgroup. `None` when no
/// ancestor caps memory below the host total; callers keep the host
/// numbers then.
///
/// `process` must be the calling process: the `available` refinement
/// walks `/proc/self/cgroup`, so a foreign process would silently get
/// self's reclaim numbers.
///
/// `available` adds reclaimable file cache back rather than taking the
/// kernel's `limit - current`, which trends toward zero on a cache-heavy
/// workload long before real OOM pressure.
fn cgroup_scoped_memory(sys: &System, process: &Process) -> Option<CgroupMemory> {
    let limits = process
        .cgroup_limits()
        .filter(|limits| limits.total_memory < sys.total_memory())?;
    let available = cgroup_available_memory(sys.total_memory())
        .unwrap_or(limits.free_memory)
        .min(limits.total_memory);
    Some(CgroupMemory {
        total: limits.total_memory,
        available,
    })
}

/// `None` when the allowed set is not a strict subset of the host's cores
/// (or an allowed core is missing from `sys.cpus()`), where the
/// host-global number is already the right one.
fn allowed_cores_cpu_usage(sys: &System, allowed: &[usize]) -> Option<f32> {
    let cpus = sys.cpus();
    if allowed.is_empty() || allowed.len() >= cpus.len() {
        return None;
    }

    let mut total_usage = 0.0f32;
    for cpu_id in allowed {
        let name = format!("cpu{cpu_id}");
        total_usage += cpus.iter().find(|cpu| cpu.name() == name)?.cpu_usage();
    }

    Some(total_usage / allowed.len() as f32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sysinfo::{Pid, ProcessesToUpdate};

    #[test]
    fn given_fresh_system_when_capturing_probe_should_sample_own_process() {
        let mut sys = System::new();

        let probe = SystemProbe::capture(&mut sys);

        assert_eq!(probe.process_id, std::process::id());
        assert!(probe.memory_usage > 0);
        assert!(probe.total_memory > 0);
        assert!(probe.available_memory <= probe.total_memory);
    }

    #[test]
    fn given_unrefreshed_system_when_probing_scoped_cpu_should_fall_back_to_global() {
        let sys = System::new();

        assert_eq!(scoped_total_cpu_usage(&sys), sys.global_cpu_usage());
    }

    // Linux-only: the by-name lookup relies on the kernel's cpu0..cpuN
    // naming, which is also the only platform where confinement exists.
    #[cfg(target_os = "linux")]
    #[test]
    fn given_allowed_core_subset_when_probing_scoped_cpu_should_average_those_cores() {
        let mut sys = System::new();
        sys.refresh_cpu_all();
        std::thread::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL);
        sys.refresh_cpu_all();

        let cpus = sys.cpus();
        if cpus.len() < 2 {
            return;
        }

        let allowed: Vec<usize> = (0..cpus.len() - 1).collect();
        let expected = allowed
            .iter()
            .map(|cpu_id| {
                cpus.iter()
                    .find(|cpu| cpu.name() == format!("cpu{cpu_id}"))
                    .expect("linux names cores cpu0..cpuN")
                    .cpu_usage()
            })
            .sum::<f32>()
            / allowed.len() as f32;

        assert_eq!(allowed_cores_cpu_usage(&sys, &allowed), Some(expected));
    }

    #[test]
    fn given_own_process_when_probing_cgroup_memory_should_reflect_confinement() {
        let mut sys = System::new();
        sys.refresh_memory();
        let pid = Pid::from_u32(std::process::id());
        sys.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
        let process = sys.process(pid).expect("own process must be visible");

        match cgroup_scoped_memory(&sys, process) {
            Some(memory) => {
                assert!(memory.total < sys.total_memory());
                assert!(memory.available <= memory.total);
            }
            None => assert!(
                process
                    .cgroup_limits()
                    .is_none_or(|limits| limits.total_memory >= sys.total_memory()),
                "None must mean no ancestor caps memory below the host total"
            ),
        }
    }
}
