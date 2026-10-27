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

use std::fs;
use std::io;
use std::path::PathBuf;
use std::process::Child;
use std::thread;
use std::time::{Duration, Instant};

pub const DEFAULT_HEALTH_CHECK_RETRIES: u32 = 1000;
pub const DEFAULT_HEALTH_CHECK_INTERVAL_MS: u64 = 20;
/// How long a graceful stop waits after SIGTERM before it sends SIGKILL.
pub const SIGTERM_TIMEOUT: Duration = Duration::from_secs(5);
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

/// The binary's stderr content when it contains a panic report.
///
/// A dead async task leaves the process alive and every assertion green, so
/// without this check a test can pass over a server whose consensus pump
/// panicked; the report also drowns at the tail of the full log dump. Stderr
/// is quiet in normal operation, so returning it whole keeps the decisive
/// lines together with any abort context around them.
pub fn stderr_panic_report(stderr_path: &Option<PathBuf>) -> Option<String> {
    let stderr = stderr_path
        .as_ref()
        .and_then(|p| fs::read_to_string(p).ok())?;
    let mut panics = stderr.lines().filter(|line| line.contains("panicked at"));
    if panics.any(|line| !is_blocking_pool_dispatch(line)) {
        Some(stderr.trim().to_string())
    } else {
        None
    }
}

/// Whether a panic came from dispatching work to compio's blocking pool.
///
/// The runtime catches these and hands them to the awaiting caller as an
/// error, so they cannot leave a dead task behind, which is the only thing
/// the check above exists for. Segment preallocation dispatches `fallocate`
/// to that pool while the shard proactors run with it disabled, so every
/// segment creation prints one and then falls back to buffered allocation.
fn is_blocking_pool_dispatch(panic_line: &str) -> bool {
    panic_line.contains("asyncify.rs")
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

/// Reap a dead child by PID and describe how it exited: normal exit code,
/// or terminating signal plus whether a core was dumped. `waitpid` only
/// succeeds from the spawning process; returns None when the child is
/// still running or was already reaped by another waiter.
#[cfg(unix)]
pub fn reap_exit_status(pid: u32) -> Option<String> {
    let mut status: libc::c_int = 0;
    let reaped = unsafe { libc::waitpid(pid as libc::pid_t, &mut status, libc::WNOHANG) };
    if reaped != pid as libc::pid_t {
        return None;
    }
    if libc::WIFEXITED(status) {
        return Some(format!("exited with code {}", libc::WEXITSTATUS(status)));
    }
    if libc::WIFSIGNALED(status) {
        return Some(format!(
            "killed by signal {} (core dumped: {})",
            libc::WTERMSIG(status),
            libc::WCOREDUMP(status)
        ));
    }
    Some(format!("unrecognized wait status {status}"))
}

/// A child after [`graceful_kill`], ready for final cleanup
/// (`wait_with_output`).
pub struct GracefulKill {
    pub child: Child,
    /// The child outlived the SIGTERM grace period and got SIGKILL.
    pub killed_after_timeout: bool,
    /// Polling the child failed, so whether it exited inside the grace is
    /// unknown.
    pub wait_error: Option<io::ErrorKind>,
}

/// Gracefully stop a child process: SIGTERM, wait up to
/// [`SIGTERM_TIMEOUT`], then SIGKILL.
pub fn graceful_kill(child: Child) -> GracefulKill {
    send_sigterm(&child);
    wait_or_kill(child, Instant::now() + SIGTERM_TIMEOUT)
}

/// The first half of [`graceful_kill`], for callers that signal several
/// processes before waiting for any of them.
pub fn send_sigterm(child: &Child) {
    unsafe {
        libc::kill(child.id() as libc::pid_t, libc::SIGTERM);
    }
}

/// The second half of [`graceful_kill`]: wait for a child that already got
/// SIGTERM until `deadline`, then SIGKILL it.
pub fn wait_or_kill(mut child: Child, deadline: Instant) -> GracefulKill {
    // Polls once even past the deadline: a caller that waited for other
    // children first may reach this one after it already exited in time.
    loop {
        match child.try_wait() {
            Ok(None) if Instant::now() < deadline => thread::sleep(SIGKILL_POLL_INTERVAL),
            Ok(None) => break,
            Ok(Some(_)) => {
                return GracefulKill {
                    child,
                    killed_after_timeout: false,
                    wait_error: None,
                };
            }
            Err(error) => {
                return GracefulKill {
                    child,
                    killed_after_timeout: false,
                    wait_error: Some(error.kind()),
                };
            }
        }
    }

    unsafe {
        libc::kill(child.id() as libc::pid_t, libc::SIGKILL);
    }

    GracefulKill {
        child,
        killed_after_timeout: true,
        wait_error: None,
    }
}
