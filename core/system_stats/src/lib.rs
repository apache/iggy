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
//! host sizing to anyone who can read a stats endpoint. These probes scope
//! the numbers to the process's allowed CPU set and effective cgroup memory
//! cap, and fall back to the host-wide values when the process is
//! unconfined.

use cpu_allocation::allowed_cpus;
use std::sync::OnceLock;
use sysinfo::{Process, System};

mod cgroup_memory;

use cgroup_memory::cgroup_available_memory;

/// Memory totals scoped to the process's effective cgroup cap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CgroupMemory {
    pub total: u64,
    pub available: u64,
}

static ALLOWED_CPUS: OnceLock<Vec<usize>> = OnceLock::new();

/// Snapshot the process's allowed CPU set for [`scoped_total_cpu_usage`].
///
/// Call once from the main thread at startup, before any thread pins
/// itself to a core: `sched_getaffinity` reports the calling thread's
/// mask, so a capture from a pinned shard thread would record that single
/// core as the whole process's set.
pub fn capture_allowed_cpus() {
    ALLOWED_CPUS.get_or_init(allowed_cpus);
}

/// Average CPU usage over the cores this process may run on, or the
/// host-global average when the process is unrestricted.
///
/// `global_cpu_usage` averages every host core, so a cpuset-confined
/// process would report its neighbors' load. The allowed set is the
/// [`capture_allowed_cpus`] boot snapshot; without one this stays on the
/// host-global average.
pub fn scoped_total_cpu_usage(sys: &System) -> f32 {
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
pub fn cgroup_scoped_memory(sys: &System, process: &Process) -> Option<CgroupMemory> {
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
