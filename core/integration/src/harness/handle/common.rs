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

use std::fs;
use std::path::PathBuf;
use std::process::Child;
use std::thread;
use std::time::{Duration, Instant};

pub const DEFAULT_HEALTH_CHECK_RETRIES: u32 = 1000;
pub const DEFAULT_HEALTH_CHECK_INTERVAL_MS: u64 = 20;
const SIGTERM_TIMEOUT: Duration = Duration::from_secs(5);
const SIGKILL_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Collect stdout and stderr logs from file paths.
pub fn collect_logs(
    stdout_path: &Option<PathBuf>,
    stderr_path: &Option<PathBuf>,
) -> (String, String) {
    let stdout = stdout_path
        .as_ref()
        .and_then(|p| fs::read_to_string(p).ok())
        .unwrap_or_else(|| "[No stdout log]".to_string());

    let stderr = stderr_path
        .as_ref()
        .and_then(|p| fs::read_to_string(p).ok())
        .unwrap_or_else(|| "[No stderr log]".to_string());

    (stdout, stderr)
}

/// Dump logs to stderr if we're panicking (for Drop impls).
pub fn dump_logs_on_panic(
    binary_name: &str,
    stdout_path: &Option<PathBuf>,
    stderr_path: &Option<PathBuf>,
) {
    if std::thread::panicking() {
        let (stdout, stderr) = collect_logs(stdout_path, stderr_path);
        eprintln!("{} stdout:\n{}", binary_name, stdout);
        eprintln!("{} stderr:\n{}", binary_name, stderr);
    }
}

/// Check if a process is alive by PID.
#[cfg(target_os = "linux")]
pub fn is_process_alive(pid: u32) -> bool {
    let stat_path = format!("/proc/{}/stat", pid);
    match fs::read_to_string(&stat_path) {
        Ok(content) => {
            if let Some(state_start) = content.rfind(')') {
                let state = content[state_start + 1..].trim().chars().next();
                !matches!(state, Some('Z') | Some('X'))
            } else {
                false
            }
        }
        Err(_) => false,
    }
}

#[cfg(all(unix, not(target_os = "linux")))]
pub fn is_process_alive(pid: u32) -> bool {
    unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
}

/// Gracefully stop a child process: SIGTERM, wait up to 5s, then SIGKILL.
/// Returns the child for final cleanup (wait_with_output).
pub fn graceful_kill(mut child: Child) -> Child {
    let pid = child.id() as libc::pid_t;

    unsafe {
        libc::kill(pid, libc::SIGTERM);
    }

    let deadline = Instant::now() + SIGTERM_TIMEOUT;
    while Instant::now() < deadline {
        match child.try_wait() {
            Ok(Some(_)) => return child,
            Ok(None) => thread::sleep(SIGKILL_POLL_INTERVAL),
            Err(_) => return child,
        }
    }

    unsafe {
        libc::kill(pid, libc::SIGKILL);
    }

    child
}
