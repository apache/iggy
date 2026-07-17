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

#[cfg(target_os = "linux")]
use nix::{
    sched::{CpuSet, sched_getaffinity},
    unistd::Pid,
};
use std::thread::available_parallelism;

/// CPUs the calling process is currently allowed to run on, ascending.
///
/// Respects restrictions imposed by the parent environment (systemd
/// `AllowedCPUs=`, container cpusets, `taskset`), which absolute core ids
/// `0..n` would silently violate: `sched_setaffinity` to a core outside the
/// allowed set fails with `EINVAL`. Falls back to `0..available_parallelism()`
/// where the affinity mask is unavailable (non-Linux).
pub fn allowed_cpus() -> Vec<usize> {
    #[cfg(target_os = "linux")]
    {
        if let Ok(mask) = sched_getaffinity(Pid::from_raw(0)) {
            let cpus: Vec<usize> = (0..CpuSet::count())
                .filter(|&cpu| mask.is_set(cpu).unwrap_or(false))
                .collect();
            if !cpus.is_empty() {
                return cpus;
            }
        }
    }

    let fallback = available_parallelism().map(|n| n.get()).unwrap_or(1);
    (0..fallback).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allowed_cpus_is_non_empty_and_ascending() {
        let allowed = allowed_cpus();
        assert!(!allowed.is_empty());
        assert!(allowed.windows(2).all(|pair| pair[0] < pair[1]));
    }
}
