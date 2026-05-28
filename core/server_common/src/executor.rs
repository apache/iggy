/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use compio::runtime::Runtime;

const DEFAULT_SHARD_RUNTIME_CAPACITY: u32 = 4096;
const SHARD_RUNTIME_CAPACITY_ENV: &str = "IGGY_SHARD_RUNTIME_CAPACITY";

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
    #[cfg(not(all(target_os = "macos", target_arch = "aarch64")))]
    proactor.thread_pool_limit(0);

    compio::runtime::RuntimeBuilder::new()
        .with_proactor(proactor.to_owned())
        .event_interval(128)
        .build()
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_SHARD_RUNTIME_CAPACITY, SHARD_RUNTIME_CAPACITY_ENV, shard_capacity_from_env,
    };
    use serial_test::serial;

    fn with_capacity_env<R>(value: Option<&str>, f: impl FnOnce() -> R) -> R {
        // SAFETY: tests in this module are #[serial], so no other thread races
        // on the process-wide environment while the guard is active.
        let prev = std::env::var(SHARD_RUNTIME_CAPACITY_ENV).ok();
        unsafe {
            match value {
                Some(v) => std::env::set_var(SHARD_RUNTIME_CAPACITY_ENV, v),
                None => std::env::remove_var(SHARD_RUNTIME_CAPACITY_ENV),
            }
        }
        let out = f();
        unsafe {
            match prev {
                Some(v) => std::env::set_var(SHARD_RUNTIME_CAPACITY_ENV, v),
                None => std::env::remove_var(SHARD_RUNTIME_CAPACITY_ENV),
            }
        }
        out
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
}
