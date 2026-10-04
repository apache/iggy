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

/// How many tasks the runtime polls between driver (io_uring) sweeps. The
/// default suits the stock task population; a deployment expecting far more
/// connected clients (each connection is roughly one task) can raise it via
/// [`SHARD_EVENT_INTERVAL_ENV`] to amortise driver sweeps across more work.
const DEFAULT_SHARD_EVENT_INTERVAL: usize = 128;
const SHARD_EVENT_INTERVAL_ENV: &str = "IGGY_SHARD_EVENT_INTERVAL";

/// Resolves the per-shard io_uring SQ/CQ capacity from `IGGY_SHARD_RUNTIME_CAPACITY`,
/// falling back to [`DEFAULT_SHARD_RUNTIME_CAPACITY`] when the var is missing or
/// fails to parse as `u32`.
fn shard_capacity_from_env() -> u32 {
    std::env::var(SHARD_RUNTIME_CAPACITY_ENV)
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(DEFAULT_SHARD_RUNTIME_CAPACITY)
}

/// Resolves the runtime event interval from `IGGY_SHARD_EVENT_INTERVAL`,
/// falling back to [`DEFAULT_SHARD_EVENT_INTERVAL`] when the var is missing,
/// fails to parse, or is zero (compio treats the interval as a divisor).
fn shard_event_interval_from_env() -> usize {
    std::env::var(SHARD_EVENT_INTERVAL_ENV)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&interval| interval > 0)
        .unwrap_or(DEFAULT_SHARD_EVENT_INTERVAL)
}

/// Creates a compio runtime for a shard thread, with shard-specific `io_uring` flags.
///
/// The per-ring SQ/CQ capacity defaults to `4096` and can be overridden via the
/// `IGGY_SHARD_RUNTIME_CAPACITY` env var, which the multi-node integration
/// harness sets to `256` so N nodes * M shards fit under an 8 MiB
/// `RLIMIT_MEMLOCK` budget without `ENOMEM` at ring setup. The runtime event
/// interval defaults to `128` and can be overridden via
/// `IGGY_SHARD_EVENT_INTERVAL` for deployments whose task count (roughly the
/// connected-client count) makes a different poll-to-sweep ratio pay off.
///
/// # Errors
///
/// Returns an `std::io::Error` if the underlying `io_uring` proactor cannot be initialised.
/// On `InvalidInput` the kernel rejected the required flags; on `OutOfMemory` or
/// `PermissionDenied` the caller should print the appropriate diagnostic before panicking.
///
/// Shard executors require `IORING_SETUP_COOP_TASKRUN` for predictable latency.
/// Falling back to default flags would silently degrade shard performance -
/// do not add a retry with reduced flags here.
pub fn create_shard_executor() -> Result<Runtime, std::io::Error> {
    let mut proactor = compio::driver::ProactorBuilder::new();

    proactor
        .capacity(shard_capacity_from_env())
        .coop_taskrun(true)
        .taskrun_flag(true);

    // Permanent divergence, not a workaround to revisit: macOS runs compio's
    // polling driver, which routes fs operations through the blocking pool, so
    // a zero limit cannot work there by design. Upstream closed
    // https://github.com/compio-rs/compio/issues/446 by making blocking-pool
    // dispatch with no workers panic ("the thread pool is needed but no worker
    // thread is running", compio-driver asyncify.rs) instead of freeze.
    // io_uring targets keep the zero limit: no blocking pool exists on shard
    // threads, which `core/partitions` messages_writer relies on to justify
    // running fallocate inline (`spawn_blocking` would hit that same panic).
    #[cfg(not(target_os = "macos"))]
    proactor.thread_pool_limit(0);

    compio::runtime::RuntimeBuilder::new()
        .with_proactor(proactor.to_owned())
        .event_interval(shard_event_interval_from_env())
        .build()
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_SHARD_EVENT_INTERVAL, DEFAULT_SHARD_RUNTIME_CAPACITY, SHARD_EVENT_INTERVAL_ENV,
        SHARD_RUNTIME_CAPACITY_ENV, shard_capacity_from_env, shard_event_interval_from_env,
    };
    use serial_test::serial;

    fn with_env<R>(name: &str, value: Option<&str>, f: impl FnOnce() -> R) -> R {
        // SAFETY: tests in this module are #[serial], so no other thread races
        // on the process-wide environment while the guard is active.
        let prev = std::env::var(name).ok();
        unsafe {
            match value {
                Some(v) => std::env::set_var(name, v),
                None => std::env::remove_var(name),
            }
        }
        let out = f();
        unsafe {
            match prev {
                Some(v) => std::env::set_var(name, v),
                None => std::env::remove_var(name),
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
    fn shard_event_interval_from_env_uses_parsed_value() {
        with_env(SHARD_EVENT_INTERVAL_ENV, Some("512"), || {
            assert_eq!(shard_event_interval_from_env(), 512);
        });
    }

    #[test]
    #[serial]
    fn shard_event_interval_from_env_falls_back_when_unset() {
        with_env(SHARD_EVENT_INTERVAL_ENV, None, || {
            assert_eq!(
                shard_event_interval_from_env(),
                DEFAULT_SHARD_EVENT_INTERVAL
            );
        });
    }

    #[test]
    #[serial]
    fn shard_event_interval_from_env_falls_back_on_unparsable() {
        with_env(SHARD_EVENT_INTERVAL_ENV, Some("not-a-number"), || {
            assert_eq!(
                shard_event_interval_from_env(),
                DEFAULT_SHARD_EVENT_INTERVAL
            );
        });
    }

    #[test]
    #[serial]
    fn shard_event_interval_from_env_falls_back_on_zero() {
        with_env(SHARD_EVENT_INTERVAL_ENV, Some("0"), || {
            assert_eq!(
                shard_event_interval_from_env(),
                DEFAULT_SHARD_EVENT_INTERVAL
            );
        });
    }
}
