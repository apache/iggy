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
const SHARD_COOP_TASKRUN_ENV: &str = "IGGY_SHARD_RUNTIME_COOP_TASKRUN";

/// Resolves the per-shard io_uring SQ/CQ capacity from `IGGY_SHARD_RUNTIME_CAPACITY`,
/// falling back to [`DEFAULT_SHARD_RUNTIME_CAPACITY`] when the var is missing or
/// fails to parse as `u32`.
fn shard_capacity_from_env() -> u32 {
    std::env::var(SHARD_RUNTIME_CAPACITY_ENV)
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(DEFAULT_SHARD_RUNTIME_CAPACITY)
}

/// Whether to request `IORING_SETUP_COOP_TASKRUN` + `IORING_SETUP_TASKRUN_FLAG`.
///
/// These flags require Linux >= 5.19; on older kernels (e.g. 5.15) the shard
/// `io_uring` setup fails with `EINVAL` even though the default-flag main-thread
/// runtime initializes fine. Defaults to `true` (recommended, lowest latency).
/// Set `IGGY_SHARD_RUNTIME_COOP_TASKRUN=false` to drop the flags and run on a
/// 5.10..5.19 kernel at a small latency cost.
fn coop_taskrun_from_env() -> bool {
    std::env::var(SHARD_COOP_TASKRUN_ENV)
        .map(|v| {
            !matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "false" | "0" | "no" | "off"
            )
        })
        .unwrap_or(true)
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
/// even when COOP_TASKRUN is on, and a zero-worker pool panics with "thread pool
/// is needed but no worker thread is running". Pass `false` only for pure
/// QUIC-only deployments where every op stays on the ring.
///
/// # Errors
///
/// Returns an `std::io::Error` if the underlying `io_uring` proactor cannot be initialised.
/// On `InvalidInput` the kernel rejected the required flags; on `OutOfMemory` or
/// `PermissionDenied` the caller should print the appropriate diagnostic before panicking.
///
/// Shard executors request `IORING_SETUP_COOP_TASKRUN` for predictable latency.
/// This is opt-out (not a silent fallback): the flags stay on by default and are
/// only dropped when `IGGY_SHARD_RUNTIME_COOP_TASKRUN=false` is set explicitly,
/// which is the documented escape hatch for kernels older than 5.19.
pub fn create_shard_executor(keep_worker_pool: bool) -> Result<Runtime, std::io::Error> {
    // TODO: The event interval tick, could be configured based on the fact
    // How many clients we expect to have connected.
    // This roughly estimates the number of tasks we will create.
    let mut proactor = compio::driver::ProactorBuilder::new();
    let coop_taskrun = coop_taskrun_from_env();

    proactor.capacity(shard_capacity_from_env());
    if coop_taskrun {
        proactor.coop_taskrun(true).taskrun_flag(true);
    }

    // FIXME(hubcio): Only set thread_pool_limit(0) on non-macOS platforms
    // This causes a freeze on macOS with compio fs operations
    // see https://github.com/compio-rs/compio/issues/446
    //
    // Keep a worker pool whenever the caller signals it (TCP/HTTP/WS active),
    // or when COOP_TASKRUN is off: in both cases compio may dispatch blocking
    // ops (fs reads, JWT storage, TLS handshakes) through the asyncify pool,
    // and a zero-worker pool panics with "thread pool is needed". Dropping the
    // pool is only safe when modern io_uring flags handle every op on-ring AND
    // no transport needs asyncify-backed blocking calls.
    #[cfg(not(all(target_os = "macos", target_arch = "aarch64")))]
    if coop_taskrun && !keep_worker_pool {
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
        DEFAULT_SHARD_RUNTIME_CAPACITY, SHARD_COOP_TASKRUN_ENV, SHARD_RUNTIME_CAPACITY_ENV,
        coop_taskrun_from_env, shard_capacity_from_env,
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
    #[serial]
    fn coop_taskrun_defaults_to_true_when_unset() {
        with_env(SHARD_COOP_TASKRUN_ENV, None, || {
            assert!(coop_taskrun_from_env());
        });
    }

    #[test]
    #[serial]
    fn coop_taskrun_disabled_by_falsey_values() {
        for value in ["false", "0", "no", "off", "OFF", "False"] {
            with_env(SHARD_COOP_TASKRUN_ENV, Some(value), || {
                assert!(!coop_taskrun_from_env(), "{value} should disable");
            });
        }
    }

    #[test]
    #[serial]
    fn coop_taskrun_enabled_by_truthy_values() {
        for value in ["true", "1", "yes", "on"] {
            with_env(SHARD_COOP_TASKRUN_ENV, Some(value), || {
                assert!(coop_taskrun_from_env(), "{value} should enable");
            });
        }
    }
}
