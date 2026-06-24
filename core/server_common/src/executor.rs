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

// IORING_SETUP_COOP_TASKRUN and IORING_SETUP_TASKRUN_FLAG both landed in Linux 5.19.
// diagnostics.rs encodes the same floor; keep both in sync when changing.
const MIN_KERNEL_MAJOR: u32 = 5;
const MIN_KERNEL_MINOR: u32 = 19;

fn parse_kernel_version(release: &str) -> Option<(u32, u32)> {
    let mut parts = release.trim().splitn(3, '.');
    let major: u32 = parts.next().and_then(|s| s.parse().ok())?;
    let minor: u32 = parts
        .next()
        .and_then(|s| s.split(|c: char| !c.is_ascii_digit()).next())
        .and_then(|s| s.parse().ok())?;
    Some((major, minor))
}

fn kernel_meets_min(major: u32, minor: u32) -> bool {
    major > MIN_KERNEL_MAJOR || (major == MIN_KERNEL_MAJOR && minor >= MIN_KERNEL_MINOR)
}

/// Reads the running kernel version from `/proc/sys/kernel/osrelease` and returns
/// `Err` with a human-readable message if the kernel is older than 5.19.
///
/// No-op on non-Linux platforms (always returns `Ok(())`).
///
/// Call this once at process startup, before `create_shard_executor`. Both the
/// classic server and server-ng entry points do this; tests skip it unless they
/// intentionally exercise the check.
pub fn check_kernel_version() -> Result<(), String> {
    #[cfg(target_os = "linux")]
    {
        let raw = std::fs::read_to_string("/proc/sys/kernel/osrelease")
            .map_err(|e| format!("cannot read kernel version: {e}"))?;
        let (major, minor) = parse_kernel_version(&raw)
            .ok_or_else(|| format!("cannot parse kernel version: {}", raw.trim()))?;
        if !kernel_meets_min(major, minor) {
            return Err(format!(
                "kernel {major}.{minor} is below the minimum required \
                 {MIN_KERNEL_MAJOR}.{MIN_KERNEL_MINOR}; \
                 upgrade the kernel or use a supported host"
            ));
        }
    }
    Ok(())
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
/// Pass `keep_worker_pool = true` when TCP, HTTP, or WebSocket transports are
/// active: those transports dispatch blocking ops through the asyncify thread pool
/// and a zero-worker pool panics with "thread pool is needed but no worker thread
/// is running".
///
/// Pass `false` only for pure QUIC-only deployments where **no** code path reaches
/// `compio::runtime::spawn_blocking`. Currently safe because io_uring opcodes cover
/// all fs I/O (fsync, read, write) and the QUIC server does not call DNS resolution
/// or `compio::fs::set_permissions`. If either changes, pass `true` instead.
///
/// # Errors
///
/// Returns an `std::io::Error` if the underlying `io_uring` proactor cannot be
/// initialised. On `InvalidInput` the kernel rejected the required flags; on
/// `OutOfMemory` or `PermissionDenied` the caller should print the appropriate
/// diagnostic before panicking.
pub fn create_shard_executor(_keep_worker_pool: bool) -> Result<Runtime, std::io::Error> {
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
    if !_keep_worker_pool {
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
        DEFAULT_SHARD_RUNTIME_CAPACITY, MIN_KERNEL_MAJOR, MIN_KERNEL_MINOR,
        SHARD_RUNTIME_CAPACITY_ENV, check_kernel_version, kernel_meets_min, parse_kernel_version,
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
    fn parse_kernel_version_accepts_plain() {
        assert_eq!(parse_kernel_version("5.19.0"), Some((5, 19)));
        assert_eq!(parse_kernel_version("6.8.0"), Some((6, 8)));
    }

    #[test]
    fn parse_kernel_version_accepts_distro_suffix() {
        assert_eq!(parse_kernel_version("5.19.0-58-generic"), Some((5, 19)));
        assert_eq!(parse_kernel_version("6.8.12-8-pve"), Some((6, 8)));
    }

    #[test]
    fn parse_kernel_version_rejects_malformed() {
        assert_eq!(parse_kernel_version("not-a-version"), None);
        assert_eq!(parse_kernel_version("5"), None);
    }

    #[test]
    fn kernel_meets_min_accepts_exact_boundary() {
        assert!(kernel_meets_min(MIN_KERNEL_MAJOR, MIN_KERNEL_MINOR));
    }

    #[test]
    fn kernel_meets_min_accepts_newer() {
        assert!(kernel_meets_min(6, 8));
        assert!(kernel_meets_min(6, 0));
    }

    #[test]
    fn kernel_meets_min_rejects_old() {
        assert!(!kernel_meets_min(5, 15));
        assert!(!kernel_meets_min(5, 18));
        assert!(!kernel_meets_min(4, 19));
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn check_kernel_version_matches_host() {
        // Only asserts when the host is >= 5.19; older CI environments are skipped
        // rather than failed because this is a production requirement, not a CI one.
        if let Some((major, minor)) = std::fs::read_to_string("/proc/sys/kernel/osrelease")
            .ok()
            .and_then(|s| parse_kernel_version(&s))
            .filter(|&(maj, min)| kernel_meets_min(maj, min))
        {
            assert!(
                check_kernel_version().is_ok(),
                "kernel {major}.{minor} should pass the \
                 >= {MIN_KERNEL_MAJOR}.{MIN_KERNEL_MINOR} check"
            );
        }
    }
}
