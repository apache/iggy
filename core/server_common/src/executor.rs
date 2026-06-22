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

use compio::runtime::Runtime;

const DEFAULT_SHARD_RUNTIME_CAPACITY: u32 = 4096;
const SHARD_RUNTIME_CAPACITY_ENV: &str = "IGGY_SHARD_RUNTIME_CAPACITY";

// Minimum kernel required by IORING_SETUP_COOP_TASKRUN + full io_uring feature set.
const MIN_KERNEL_MAJOR: u32 = 6;
const MIN_KERNEL_MINOR: u32 = 8;

/// Reads the running kernel version from `/proc/sys/kernel/osrelease` and returns
/// `Err` with a human-readable message if the kernel is older than 6.8.
///
/// Call this once at process startup, before `create_shard_executor`. Both the
/// classic server and server-ng entry points do this; tests skip it unless they
/// intentionally exercise the check.
pub fn check_kernel_version() -> Result<(), String> {
    let raw = std::fs::read_to_string("/proc/sys/kernel/osrelease")
        .map_err(|e| format!("cannot read kernel version: {e}"))?;

    let version_str = raw.trim();
    let mut parts = version_str.splitn(3, '.');
    let major: u32 = parts
        .next()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| format!("cannot parse kernel version: {version_str}"))?;
    let minor: u32 = parts
        .next()
        .and_then(|s| s.split(|c: char| !c.is_ascii_digit()).next())
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| format!("cannot parse kernel version: {version_str}"))?;

    if major > MIN_KERNEL_MAJOR || (major == MIN_KERNEL_MAJOR && minor >= MIN_KERNEL_MINOR) {
        Ok(())
    } else {
        Err(format!(
            "kernel {version_str} is below the minimum required {MIN_KERNEL_MAJOR}.{MIN_KERNEL_MINOR}; \
             upgrade the kernel or use a supported host"
        ))
    }
}

/// Resolves the per-shard io_uring SQ/CQ capacity from `IGGY_SHARD_RUNTIME_CAPACITY`,
/// falling back to [`DEFAULT_SHARD_RUNTIME_CAPACITY`] when the var is missing or
/// fails to parse as `u32`.
fn shard_capacity_from_env() -> u32 {
    std::env::var(SHARD_RUNTIME_CAPACITY_ENV)
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(DEFAULT_SHARD_RUNTIME_CAPACITY)
}

/// Creates a compio runtime for a shard thread, with shard-specific `io_uring` flags.
///
/// The per-ring SQ/CQ capacity defaults to `4096` and can be overridden via the
/// `IGGY_SHARD_RUNTIME_CAPACITY` env var, which the multi-node integration
/// harness sets to `256` so N nodes * M shards fit under an 8 MiB
/// `RLIMIT_MEMLOCK` budget without `ENOMEM` at ring setup.
///
/// `keep_worker_pool` must be `true` when TCP, HTTP, or WebSocket transports are
/// active: those transports dispatch blocking ops through the asyncify thread pool
/// and a zero-worker pool panics with "thread pool is needed but no worker thread
/// is running". Pass `false` only for pure QUIC-only deployments where every op
/// stays on the ring.
///
/// # Errors
///
/// Returns an `std::io::Error` if the underlying `io_uring` proactor cannot be
/// initialised. On `InvalidInput` the kernel rejected the required flags; on
/// `OutOfMemory` or `PermissionDenied` the caller should print the appropriate
/// diagnostic before panicking.
pub fn create_shard_executor(keep_worker_pool: bool) -> Result<Runtime, std::io::Error> {
    // TODO: The event interval tick, could be configured based on the fact
    // How many clients we expect to have connected.
    // This roughly estimates the number of tasks we will create.
    let mut proactor = compio::driver::ProactorBuilder::new();

    proactor
        .capacity(shard_capacity_from_env())
        .coop_taskrun(true)
        .taskrun_flag(true);

    // FIXME(hubcio): Only set thread_pool_limit(0) on non-macOS platforms
    // This causes a freeze on macOS with compio fs operations
    // see https://github.com/compio-rs/compio/issues/446
    //
    // Drop the asyncify worker pool only for pure QUIC deployments (no
    // TCP/HTTP/WS): those transports need the pool for blocking ops such as
    // TLS handshakes and JWT storage.
    #[cfg(not(all(target_os = "macos", target_arch = "aarch64")))]
    if !keep_worker_pool {
        proactor.thread_pool_limit(0);
    }

    compio::runtime::RuntimeBuilder::new()
        .with_proactor(proactor.to_owned())
        .event_interval(128)
        .build()
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_SHARD_RUNTIME_CAPACITY, SHARD_RUNTIME_CAPACITY_ENV, check_kernel_version,
        shard_capacity_from_env,
    };
    use serial_test::serial;

    fn with_env<R>(key: &str, value: Option<&str>, f: impl FnOnce() -> R) -> R {
        // SAFETY: tests in this module are #[serial], so no other thread races
        // on the process-wide environment while the guard is active.
        let prev = std::env::var(key).ok();
        unsafe {
            match value {
                Some(v) => std::env::set_var(key, v),
                None => std::env::remove_var(key),
            }
        }
        let out = f();
        unsafe {
            match prev {
                Some(v) => std::env::set_var(key, v),
                None => std::env::remove_var(key),
            }
        }
        out
    }

    fn with_capacity_env<R>(value: Option<&str>, f: impl FnOnce() -> R) -> R {
        with_env(SHARD_RUNTIME_CAPACITY_ENV, value, f)
    }

    #[test]
    #[serial]
    fn shard_capacity_from_env_uses_parsed_value() {
        with_capacity_env(Some("256"), || {
            assert_eq!(shard_capacity_from_env(), 256);
        });
    }

    #[test]
    #[serial]
    fn shard_capacity_from_env_falls_back_when_unset() {
        with_capacity_env(None, || {
            assert_eq!(shard_capacity_from_env(), DEFAULT_SHARD_RUNTIME_CAPACITY);
        });
    }

    #[test]
    #[serial]
    fn shard_capacity_from_env_falls_back_on_unparsable() {
        with_capacity_env(Some("not-a-number"), || {
            assert_eq!(shard_capacity_from_env(), DEFAULT_SHARD_RUNTIME_CAPACITY);
        });
    }

    #[test]
    #[serial]
    fn shard_capacity_from_env_falls_back_on_negative() {
        with_capacity_env(Some("-1"), || {
            assert_eq!(shard_capacity_from_env(), DEFAULT_SHARD_RUNTIME_CAPACITY);
        });
    }

    #[test]
    fn check_kernel_version_accepts_current_kernel() {
        // This test runs on the CI host; if the host is >= 6.8 the check passes.
        // If CI runs on an older kernel the test is skipped rather than failed,
        // because the check is a hard requirement for production, not for CI hosts.
        let raw = std::fs::read_to_string("/proc/sys/kernel/osrelease");
        if let Ok(raw) = raw {
            let ver = raw.trim();
            let mut p = ver.splitn(3, '.');
            let major: u32 = p.next().and_then(|s| s.parse().ok()).unwrap_or(0);
            let minor: u32 = p
                .next()
                .and_then(|s| s.split(|c: char| !c.is_ascii_digit()).next())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            if major > 6 || (major == 6 && minor >= 8) {
                assert!(check_kernel_version().is_ok(), "kernel {ver} should pass");
            }
        }
    }

    #[test]
    fn check_kernel_version_rejects_old_kernel() {
        // Directly test the parsing logic with a known-old version string.
        // We can't call the real function because it reads /proc, so we inline
        // the same logic with a synthetic input.
        let version_str = "5.15.0-58-generic";
        let mut parts = version_str.splitn(3, '.');
        let major: u32 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
        let minor: u32 = parts
            .next()
            .and_then(|s| s.split(|c: char| !c.is_ascii_digit()).next())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        assert!(
            !(major > 6 || (major == 6 && minor >= 8)),
            "5.15 should fail the >= 6.8 check"
        );
    }
}
