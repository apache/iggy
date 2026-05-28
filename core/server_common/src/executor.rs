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

/// Creates a compio runtime for a shard thread, with shard-specific `io_uring` flags.
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
        .capacity(4096)
        .coop_taskrun(true)
        .taskrun_flag(true);

    // FIXME(hubcio): Only set thread_pool_limit(0) on non-macOS platforms
    // This causes a freeze on macOS with compio fs operations
    // see https://github.com/compio-rs/compio/issues/446
    #[cfg(not(all(target_os = "macos", target_arch = "aarch64")))]
    proactor.thread_pool_limit(0);

    compio::runtime::RuntimeBuilder::new()
        .with_proactor(proactor.clone())
        .event_interval(128)
        .build()
}
