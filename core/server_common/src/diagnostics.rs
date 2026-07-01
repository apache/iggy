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

/// compio's `AsyncifyPool::dispatch` panics with this exact message when a
/// blocking fallback is required but the pool has zero worker threads. Iggy
/// shard executors set `thread_pool_limit(0)` in `create_shard_executor`, so
/// this panic means the kernel lacks an io_uring opcode a shard operation
/// needed and compio could not run it natively. Shard-panic handlers match it
/// to surface [`print_incomplete_io_uring_ops_info`] instead of the bare
/// compio text.
///
/// Best-effort: if a future compio release changes the wording, matching
/// degrades to logging the raw panic, which is still surfaced to the operator.
pub const ASYNCIFY_POOL_DISABLED_PANIC_MSG: &str =
    "the thread pool is needed but no worker thread is running";

#[cfg(target_os = "linux")]
const DISCORD_SUPPORT_URL: &str = "https://discord.gg/apache-iggy";

#[cfg(target_os = "linux")]
fn print_discord_link() {
    eprintln!("  Need help? Join our Discord: {DISCORD_SUPPORT_URL}");
    eprintln!();
}

/// Formats an `RLIMIT_MEMLOCK` value for diagnostic output.
///
/// `u64::MAX` is rendered as `unlimited` (the value getrlimit returns
/// for an uncapped limit); other values are rendered as raw bytes plus
/// a coarse MB-or-KB suffix so operators can eyeball whether the limit
/// is in the ballpark of the 4096-entry io_uring ring footprint.
#[cfg(target_os = "linux")]
fn format_limit(limit: u64) -> String {
    if limit == u64::MAX {
        "unlimited".to_string()
    } else {
        let kb = limit / 1024;
        let mb = kb / 1024;
        if mb > 0 {
            format!("{limit} bytes ({mb} MB)")
        } else {
            format!("{limit} bytes ({kb} KB)")
        }
    }
}

/// Prints information about locked memory limits when runtime creation fails.
/// This is typically needed when io_uring cannot allocate memory due to RLIMIT_MEMLOCK.
#[cfg(target_os = "linux")]
pub fn print_locked_memory_limit_info() {
    use nix::sys::resource::{Resource, getrlimit};

    let (soft, hard) = match getrlimit(Resource::RLIMIT_MEMLOCK) {
        Ok(limits) => limits,
        Err(_) => {
            eprintln!("Failed to retrieve locked memory limits");
            return;
        }
    };

    eprintln!();
    eprintln!("=== Locked Memory Limit Information ===");
    eprintln!("Current soft limit: {}", format_limit(soft));
    eprintln!("Current hard limit: {}", format_limit(hard));
    eprintln!();
    eprintln!("The io_uring runtime requires sufficient locked memory to operate.");
    eprintln!("To increase the limit, you can:");
    eprintln!();
    eprintln!("  1. Temporarily (current session only):");
    eprintln!("     ulimit -l unlimited");
    eprintln!();
    eprintln!("  2. Docker run:");
    eprintln!("     docker run --ulimit memlock=-1:-1 ...");
    eprintln!();
    eprintln!("  3. Docker Compose (add to service):");
    eprintln!("     ulimits:");
    eprintln!("       memlock:");
    eprintln!("         soft: -1");
    eprintln!("         hard: -1");
    eprintln!();
    eprintln!("  4. Persistently (add to /etc/security/limits.conf):");
    eprintln!("     * soft memlock unlimited");
    eprintln!("     * hard memlock unlimited");
    eprintln!();
    eprintln!("  5. For systemd services (add to service file):");
    eprintln!("     LimitMEMLOCK=infinity");
    eprintln!();
    print_discord_link();
}

/// Prints information about io_uring permission issues in containerized environments.
/// This occurs when seccomp blocks io_uring syscalls.
#[cfg(target_os = "linux")]
pub fn print_io_uring_permission_info() {
    eprintln!();
    eprintln!("=== io_uring Permission Denied ===");
    eprintln!();
    eprintln!("The io_uring runtime requires specific syscalls that are blocked by default");
    eprintln!("in containerized environments (Docker, Podman, etc.).");
    eprintln!();
    eprintln!("To resolve this issue:");
    eprintln!();
    eprintln!("  1. Docker Compose (add to service):");
    eprintln!("     security_opt:");
    eprintln!("       - seccomp:unconfined");
    eprintln!();
    eprintln!("  2. Docker run:");
    eprintln!("     docker run --security-opt seccomp=unconfined ...");
    eprintln!();
    eprintln!("  3. Custom seccomp profile (more secure):");
    eprintln!("     Create a profile allowing io_uring_setup, io_uring_enter,");
    eprintln!("     and io_uring_register syscalls.");
    eprintln!();
    eprintln!("  4. Kubernetes (add to pod spec):");
    eprintln!("     securityContext:");
    eprintln!("       seccompProfile:");
    eprintln!("         type: Unconfined");
    eprintln!();
    print_discord_link();
}

/// Minimum kernel version for IORING_SETUP_COOP_TASKRUN and IORING_SETUP_TASKRUN_FLAG.
#[cfg(target_os = "linux")]
const MIN_KERNEL_MAJOR: u32 = 5;
#[cfg(target_os = "linux")]
const MIN_KERNEL_MINOR: u32 = 19;

/// Minimum kernel version for kernel.io_uring_disabled sysctl.
#[cfg(target_os = "linux")]
const SYSCTL_IO_URING_DISABLED_KERNEL_MAJOR: u32 = 6;
#[cfg(target_os = "linux")]
const SYSCTL_IO_URING_DISABLED_KERNEL_MINOR: u32 = 1;

/// Prints diagnostic information when io_uring setup fails with EINVAL.
///
/// This typically occurs when the kernel does not support the io_uring flags
/// required by shard executors (IORING_SETUP_COOP_TASKRUN, IORING_SETUP_TASKRUN_FLAG).
/// The caller is responsible for deduplication (e.g., via `std::sync::Once`).
#[cfg(target_os = "linux")]
pub fn print_invalid_io_uring_args_info() {
    eprintln!();
    eprintln!("=== io_uring Invalid Argument (EINVAL) ===");
    eprintln!();
    eprintln!("The shard executor failed to initialize because the kernel rejected");
    eprintln!("io_uring setup flags required for shard operation.");
    eprintln!();
    eprintln!("  The main thread's io_uring runtime uses default settings and initialized");
    eprintln!("  successfully. Shard executors require additional flags:");
    eprintln!("    - IORING_SETUP_COOP_TASKRUN (cooperative task running)");
    eprintln!("    - IORING_SETUP_TASKRUN_FLAG (task runner flag notification)");
    eprintln!(
        "  These flags require Linux kernel >= {MIN_KERNEL_MAJOR}.{MIN_KERNEL_MINOR} with full io_uring support."
    );
    eprintln!();

    report_io_uring_environment();
}

/// Prints diagnostic information when a shard thread panicked because compio
/// had to run an io_uring operation on its blocking fallback pool, which Iggy
/// disables (`thread_pool_limit(0)`).
///
/// Unlike [`print_invalid_io_uring_args_info`], io_uring setup succeeded here:
/// the kernel accepted the ring flags and shards started, then at runtime
/// compio's opcode probe (`IORING_REGISTER_PROBE`) reported an opcode a shard
/// operation needed as unsupported. The caller is responsible for
/// deduplication (e.g., via `std::sync::Once`).
#[cfg(target_os = "linux")]
pub fn print_incomplete_io_uring_ops_info() {
    eprintln!();
    eprintln!("=== io_uring Incomplete Opcode Support ===");
    eprintln!();
    eprintln!("A shard thread aborted because an io_uring operation it issued is not");
    eprintln!("supported by this kernel. Iggy shards run thread-per-core with the");
    eprintln!("blocking fallback pool disabled, so an unsupported opcode is fatal");
    eprintln!("instead of being silently offloaded to a worker thread.");
    eprintln!();
    eprintln!("  io_uring setup succeeded (shards started), but at runtime compio");
    eprintln!("  probed the kernel and a required opcode was absent. This is common");
    eprintln!("  on WSL2 and on older or cut-down kernels whose io_uring support is");
    eprintln!("  incomplete. Some opcodes need a kernel newer than the shard-setup");
    eprintln!("  floor below.");
    eprintln!();

    report_io_uring_environment();
}

/// Probes the io_uring environment (kernel version, WSL2 detection,
/// `kernel.io_uring_disabled` sysctl, AppArmor confinement), prints the
/// findings plus any concrete issues, then prints the shared remediation
/// steps. Reused by both io_uring diagnostics above.
#[cfg(target_os = "linux")]
fn report_io_uring_environment() {
    use nix::sys::utsname::uname;
    use std::fs;

    let mut detected_issues: Vec<String> = Vec::new();

    // 1. Kernel version check
    let uname_info = match uname() {
        Ok(info) => Some(info),
        Err(_) => {
            eprintln!("  [!] Could not retrieve kernel information via uname(2).");
            None
        }
    };

    let mut kernel_version: Option<(u32, u32)> = None;

    if let Some(ref info) = uname_info {
        let release = info.release().to_string_lossy();
        eprintln!("  Kernel release: {release}");

        if let Some((major, minor)) = parse_kernel_version(&release) {
            kernel_version = Some((major, minor));
            if (major, minor) < (MIN_KERNEL_MAJOR, MIN_KERNEL_MINOR) {
                detected_issues.push(format!(
                    "Kernel {major}.{minor} is too old (need >= {MIN_KERNEL_MAJOR}.{MIN_KERNEL_MINOR})"
                ));
            }
        } else {
            eprintln!("  [!] Could not parse kernel version from release string.");
        }

        // 2. WSL2 detection
        let release_is_wsl = release.contains("microsoft") || release.contains("Microsoft");
        let proc_version_is_wsl = fs::read_to_string("/proc/version")
            .map(|v| v.contains("Microsoft") || v.contains("microsoft"))
            .unwrap_or(false);

        if release_is_wsl || proc_version_is_wsl {
            eprintln!("  Environment: WSL2 (Microsoft kernel fork detected)");
            detected_issues.push(
                "WSL2 kernels often ship incomplete io_uring: missing setup flags or \
                 opcodes even at version >= 5.19"
                    .to_string(),
            );
        }
    }

    // 3. kernel.io_uring_disabled sysctl (available since kernel 6.1)
    match fs::read_to_string("/proc/sys/kernel/io_uring_disabled") {
        Ok(value) => {
            let value = value.trim();
            eprintln!("  kernel.io_uring_disabled = {value}");
            match value {
                "1" => detected_issues
                    .push("io_uring is disabled for unprivileged users (sysctl = 1)".to_string()),
                "2" => detected_issues
                    .push("io_uring is fully disabled by sysctl (sysctl = 2)".to_string()),
                _ => {}
            }
        }
        Err(_) => {
            // The sysctl was introduced in kernel 6.1. If the file is absent on a kernel >= 6.1,
            // io_uring is likely not compiled in (CONFIG_IO_URING=n).
            if let Some((major, minor)) = kernel_version
                && (major, minor)
                    >= (
                        SYSCTL_IO_URING_DISABLED_KERNEL_MAJOR,
                        SYSCTL_IO_URING_DISABLED_KERNEL_MINOR,
                    )
            {
                detected_issues.push(format!(
                    "kernel.io_uring_disabled sysctl not found on kernel >= \
                     {SYSCTL_IO_URING_DISABLED_KERNEL_MAJOR}.{SYSCTL_IO_URING_DISABLED_KERNEL_MINOR} \
                     - io_uring may not be compiled in (CONFIG_IO_URING=n)"
                ));
            }
        }
    }

    // 4. AppArmor - informational only, not added to detected_issues
    let apparmor_profile = fs::read_to_string("/proc/self/attr/apparmor/current")
        .ok()
        .map(|s| s.trim().to_string());

    if let Some(ref profile) = apparmor_profile
        && profile != "unconfined"
        && !profile.is_empty()
    {
        eprintln!("  AppArmor profile: {profile}");
    }

    // Print detected issues
    if detected_issues.is_empty() {
        eprintln!();
        eprintln!("  No specific issue was detected. This kernel's io_uring support may be");
        eprintln!("  incomplete for the setup flags or opcodes Iggy's shard executors require.");
    } else {
        eprintln!();
        eprintln!("  Detected issues:");
        for (i, issue) in detected_issues.iter().enumerate() {
            eprintln!("    {}. {issue}", i + 1);
        }
    }

    eprintln!();
    eprintln!("  To resolve this:");
    eprintln!();
    eprintln!(
        "  1. Upgrade to Linux kernel >= {MIN_KERNEL_MAJOR}.{MIN_KERNEL_MINOR} (>= {SYSCTL_IO_URING_DISABLED_KERNEL_MAJOR}.{SYSCTL_IO_URING_DISABLED_KERNEL_MINOR} recommended)"
    );
    eprintln!();
    eprintln!("  2. If running under WSL2:");
    eprintln!("     - Update WSL: wsl --update  (from PowerShell)");
    eprintln!("     - Or build a custom kernel with full io_uring support:");
    eprintln!("       https://learn.microsoft.com/en-us/windows/wsl/wsl-config#wsl-2-settings");
    eprintln!("     - Or use Docker Desktop / a native Linux VM instead of WSL2");
    eprintln!();
    eprintln!("  3. If io_uring is disabled via sysctl:");
    eprintln!("     sudo sysctl -w kernel.io_uring_disabled=0");
    eprintln!();
    eprintln!("  4. If AppArmor is restricting io_uring:");
    eprintln!("     sudo aa-complain <profile-name>");
    eprintln!();
    eprintln!("  5. Check kernel logs for more details:");
    eprintln!("     dmesg | grep -i io_uring");
    eprintln!();
    print_discord_link();
}

/// Parses "major.minor[.patch...][-suffix]" from a kernel release string.
#[cfg(target_os = "linux")]
fn parse_kernel_version(release: &str) -> Option<(u32, u32)> {
    let mut parts = release
        .split(|c: char| !c.is_ascii_digit())
        .filter(|s| !s.is_empty());
    let major = parts.next()?.parse::<u32>().ok()?;
    let minor = parts.next()?.parse::<u32>().ok()?;
    Some((major, minor))
}

#[cfg(not(target_os = "linux"))]
pub const fn print_locked_memory_limit_info() {}

#[cfg(not(target_os = "linux"))]
pub const fn print_io_uring_permission_info() {}

#[cfg(not(target_os = "linux"))]
pub const fn print_invalid_io_uring_args_info() {}

#[cfg(not(target_os = "linux"))]
pub const fn print_incomplete_io_uring_ops_info() {}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::{format_limit, parse_kernel_version};

    #[test]
    fn parse_kernel_version_standard() {
        assert_eq!(parse_kernel_version("6.8.0-45-generic"), Some((6, 8)));
    }

    #[test]
    fn parse_kernel_version_wsl2() {
        assert_eq!(
            parse_kernel_version("5.15.153.1-microsoft-standard-WSL2"),
            Some((5, 15))
        );
    }

    #[test]
    fn parse_kernel_version_minimal() {
        assert_eq!(parse_kernel_version("5.19"), Some((5, 19)));
    }

    #[test]
    fn parse_kernel_version_garbage_returns_none() {
        assert_eq!(parse_kernel_version("not-a-version"), None);
    }

    #[test]
    fn parse_kernel_version_empty_returns_none() {
        assert_eq!(parse_kernel_version(""), None);
    }

    #[test]
    fn parse_kernel_version_overflow_returns_none() {
        // u32::MAX + 1 in the major slot must not silently wrap.
        assert_eq!(parse_kernel_version("4294967296.0"), None);
    }

    #[test]
    fn format_limit_unlimited() {
        assert_eq!(format_limit(u64::MAX), "unlimited");
    }

    #[test]
    fn format_limit_sub_mb_uses_kb_suffix() {
        assert_eq!(format_limit(64 * 1024), "65536 bytes (64 KB)");
    }

    #[test]
    fn format_limit_mb_range_uses_mb_suffix() {
        assert_eq!(format_limit(8 * 1024 * 1024), "8388608 bytes (8 MB)");
    }
}
