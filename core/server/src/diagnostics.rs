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
    eprintln!("  2. Persistently (add to /etc/security/limits.conf):");
    eprintln!("     * soft memlock unlimited");
    eprintln!("     * hard memlock unlimited");
    eprintln!();
    eprintln!("  3. For systemd services (add to service file):");
    eprintln!("     LimitMEMLOCK=infinity");
    eprintln!();
    eprintln!("  4. Docker Compose (add to service):");
    eprintln!("     ulimits:");
    eprintln!("       memlock:");
    eprintln!("         soft: -1");
    eprintln!("         hard: -1");
    eprintln!();
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
}

#[cfg(not(target_os = "linux"))]
pub fn print_locked_memory_limit_info() {}

#[cfg(not(target_os = "linux"))]
pub fn print_io_uring_permission_info() {}
