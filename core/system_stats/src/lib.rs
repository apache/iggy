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
use sysinfo::{Process, System};

mod cgroup_memory;

use cgroup_memory::cgroup_available_memory;

/// Memory totals scoped to the process's effective cgroup cap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CgroupMemory {
    pub total: u64,
    pub available: u64,
}

/// Average CPU usage over the cores this process may run on, or the
/// host-global average when the process is unrestricted.
///
/// `global_cpu_usage` averages every host core, so a cpuset-confined
/// process would report its neighbors' load.
pub fn scoped_total_cpu_usage(sys: &System) -> f32 {
    allowed_cores_cpu_usage(sys).unwrap_or_else(|| sys.global_cpu_usage())
}

/// Memory totals scoped to the process's memory cgroup. `None` when no
/// ancestor caps memory below the host total; callers keep the host
/// numbers then.
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

/// `None` when the process is unrestricted (or an allowed core is missing
/// from `sys.cpus()`), where the host-global number is already the right
/// one.
fn allowed_cores_cpu_usage(sys: &System) -> Option<f32> {
    let cpus = sys.cpus();
    let allowed = allowed_cpus();
    if allowed.is_empty() || allowed.len() >= cpus.len() {
        return None;
    }

    let mut total_usage = 0.0f32;
    for cpu_id in &allowed {
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

    #[test]
    fn given_own_process_when_probing_cgroup_memory_should_stay_within_host_total() {
        let mut sys = System::new();
        sys.refresh_memory();
        let pid = Pid::from_u32(std::process::id());
        sys.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
        let process = sys.process(pid).expect("own process must be visible");

        if let Some(memory) = cgroup_scoped_memory(&sys, process) {
            assert!(memory.total < sys.total_memory());
            assert!(memory.available <= memory.total);
        }
    }
}
