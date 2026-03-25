/* Licensed to the Apache Software Foundation (ASF) under one
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

#[cfg(target_os = "linux")]
const DISCORD_SUPPORT_URL: &str = "https://discord.gg/apache-iggy";

#[cfg(target_os = "linux")]
fn print_discord_link() {
    eprintln!("  Need help? Join our Discord: {DISCORD_SUPPORT_URL}");
    eprintln!();
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

    let format_limit = |limit: u64| -> String {
        if limit == u64::MAX {
            "unlimited".to_string()
        } else {
            let kb = limit / 1024;
            let mb = kb / 1024;
            if mb > 0 {
                format!("{} bytes ({} MB)", limit, mb)
            } else {
                format!("{} bytes ({} KB)", limit, kb)
            }
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
    use nix::sys::utsname::uname;
    use std::fs;

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
                "WSL2 kernel may not support IORING_SETUP_COOP_TASKRUN even if version >= 5.19"
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
        eprintln!("  No specific issue was detected. The kernel may lack io_uring support");
        eprintln!("  for the flags used by Iggy's shard executors.");
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
pub fn print_locked_memory_limit_info() {}

#[cfg(not(target_os = "linux"))]
pub fn print_io_uring_permission_info() {}

#[cfg(not(target_os = "linux"))]
pub fn print_invalid_io_uring_args_info() {}

#[cfg(test)]
mod tests {
    #[cfg(target_os = "linux")]
    use super::parse_kernel_version;

    #[test]
    #[cfg(target_os = "linux")]
    fn test_parse_standard_kernel_version() {
        assert_eq!(parse_kernel_version("6.8.0-45-generic"), Some((6, 8)));
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn test_parse_wsl2_kernel_version() {
        assert_eq!(
            parse_kernel_version("5.15.153.1-microsoft-standard-WSL2"),
            Some((5, 15))
        );
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn test_parse_minimal_version() {
        assert_eq!(parse_kernel_version("5.19"), Some((5, 19)));
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn test_parse_garbage_returns_none() {
        assert_eq!(parse_kernel_version("not-a-version"), None);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn test_parse_empty_returns_none() {
        assert_eq!(parse_kernel_version(""), None);
    }
}
