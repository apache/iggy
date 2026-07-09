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

//! `/proc` dump for the process-list snapshot section. Synchronous by design:
//! it runs on the dedicated snapshot collector thread, never on a shard.

// `fmt::Write` for `String` is infallible, so the `writeln!` results below are
// discarded.
use std::fmt::Write;

/// Get detailed information about the system's processes and related /proc data.
pub(super) fn proc_info() -> Result<String, std::io::Error> {
    let static_proc_files = [
        "/proc/uptime",
        "/proc/cpuinfo",
        "/proc/stat",
        "/proc/meminfo",
        "/proc/interrupts",
        "/proc/softirqs",
        "/proc/latency",
        "/proc/buddyinfo",
        "/proc/slabinfo",
        "/proc/vmstat",
        "/proc/loadavg",
        "/proc/cmdline",
        "/proc/version",
        "/proc/net/sockstat",
        "/proc/net/snmp",
        "/proc/net/netlink",
        "/proc/net/netstat",
        "/proc/net/dev",
        "/proc/net/packet",
        "/proc/net/tcp",
        "/proc/net/tcp6",
        "/proc/net/udp",
        "/proc/net/udp6",
        "/proc/net/raw",
        "/proc/net/raw6",
        "/proc/net/icmp",
        "/proc/net/icmp6",
        "/proc/net/udplite",
        "/proc/net/udplite6",
        "/proc/net/unix",
        "/proc/net/softnet_stat",
        "/proc/tty/drivers",
        "/proc/sys/kernel/pid_max",
        "/proc/sys/kernel/random/boot_id",
        "/proc/mounts",
        "/proc/modules",
    ];

    let mut result = String::new();
    for path in static_proc_files {
        dump_file(&mut result, path);
    }

    let mut proc_dir = std::fs::read_dir("/proc")?;
    while let Some(Ok(entry)) = proc_dir.next() {
        let file_type = entry.file_type()?;
        if file_type.is_dir()
            && let Ok(pid) = entry.file_name().to_string_lossy().parse::<u32>()
        {
            let pid_paths = [
                format!("/proc/{pid}/cmdline"),
                format!("/proc/{pid}/statm"),
                format!("/proc/{pid}/cgroup"),
                format!("/proc/{pid}/task/{pid}/stat"),
                format!("/proc/{pid}/task/{pid}/status"),
                format!("/proc/{pid}/task/{pid}/wchan"),
                format!("/proc/{pid}/task/{pid}/syscall"),
                format!("/proc/{pid}/task/{pid}/fd"),
            ];
            for path in &pid_paths {
                dump_file(&mut result, path);
            }
        }
    }

    Ok(result)
}

fn dump_file(result: &mut String, path: &str) {
    match std::fs::read_to_string(path) {
        Ok(contents) => {
            let _ = writeln!(result, "=== {path} ===");
            if path.ends_with("/stat") && path.contains("/task/") {
                result.push_str(&parse_stat(&contents));
            } else {
                result.push_str(&contents);
            }
            result.push_str("\n\n");
        }
        Err(error) => {
            // `/proc/[pid]/fd` is a directory of symlinks, not a readable
            // file; render the link targets instead of an error line.
            if let Ok(metadata) = std::fs::metadata(path)
                && metadata.is_dir()
                && path.ends_with("/fd")
            {
                let _ = writeln!(result, "=== {path} (directory) ===");
                if let Ok(mut entries) = std::fs::read_dir(path) {
                    while let Some(Ok(entry)) = entries.next() {
                        let fd_path = entry.path();
                        match std::fs::read_link(&fd_path) {
                            Ok(link) => {
                                let _ =
                                    writeln!(result, "{} -> {}", fd_path.display(), link.display());
                            }
                            Err(_) => {
                                let _ =
                                    writeln!(result, "{} (unreadable symlink)", fd_path.display());
                            }
                        }
                    }
                }
                result.push('\n');
            } else {
                let _ = writeln!(result, "=== {path} ERROR: {error} ===\n");
            }
        }
    }
}

/// Parse the contents of a /proc/[pid]/task/[tid]/stat file into a
/// human-readable format.
fn parse_stat(contents: &str) -> String {
    let fields: Vec<&str> = contents.split_whitespace().collect();
    if fields.len() < 52 {
        return format!("Invalid stat format: {contents}");
    }

    // Extract `comm` from between the parens in `pid (comm) state ...`. Guard the
    // ordering: a malformed line where ')' precedes '(' (or a paren is missing)
    // would panic on the reversed slice range and take down the collector thread.
    let comm = match (contents.find('('), contents.rfind(')')) {
        (Some(start), Some(end)) if start < end => &contents[start + 1..end],
        _ => return format!("Invalid stat format: {contents}"),
    };

    let mut result = String::new();
    let _ = writeln!(result, "PID: {}", fields[0]);
    let _ = writeln!(result, "Command: {comm}");
    let _ = writeln!(
        result,
        "State: {} ({})",
        fields[2],
        match fields[2] {
            "R" => "Running",
            "S" => "Sleeping (interruptible)",
            "D" => "Waiting in uninterruptible disk sleep",
            "Z" => "Zombie",
            "T" => "Stopped",
            "t" => "Tracing stop",
            "W" => "Paging",
            "X" | "x" => "Dead",
            "K" => "Wakekill",
            "P" => "Parked",
            _ => "Unknown",
        }
    );
    let _ = writeln!(result, "Parent PID: {}", fields[3]);
    let _ = writeln!(result, "Process Group: {}", fields[4]);
    let _ = writeln!(result, "Session ID: {}", fields[5]);
    let _ = writeln!(result, "TTY: {}", fields[6]);
    let _ = writeln!(result, "Foreground Process Group: {}", fields[7]);
    let _ = writeln!(result, "Kernel Flags: {}", fields[8]);
    let _ = writeln!(result, "Minor Faults: {}", fields[9]);
    let _ = writeln!(result, "Children Minor Faults: {}", fields[10]);
    let _ = writeln!(result, "Major Faults: {}", fields[11]);
    let _ = writeln!(result, "Children Major Faults: {}", fields[12]);
    let _ = writeln!(result, "User Mode Time: {} ticks", fields[13]);
    let _ = writeln!(result, "System Mode Time: {} ticks", fields[14]);
    let _ = writeln!(result, "Children User Mode Time: {} ticks", fields[15]);
    let _ = writeln!(result, "Children System Mode Time: {} ticks", fields[16]);
    let _ = writeln!(result, "Priority: {}", fields[17]);
    let _ = writeln!(result, "Nice Value: {}", fields[18]);
    let _ = writeln!(result, "Number of Threads: {}", fields[19]);
    let _ = writeln!(result, "Real-time Priority: {}", fields[39]);
    let _ = writeln!(
        result,
        "Policy: {} ({})",
        fields[40],
        match fields[40] {
            "0" => "SCHED_NORMAL/OTHER",
            "1" => "SCHED_FIFO",
            "2" => "SCHED_RR",
            "3" => "SCHED_BATCH",
            "5" => "SCHED_IDLE",
            "6" => "SCHED_DEADLINE",
            _ => "Unknown",
        }
    );
    let _ = writeln!(result, "Aggregated Block I/O Delays: {} ticks", fields[41]);
    let _ = writeln!(result, "Guest Time: {} ticks", fields[42]);
    let _ = writeln!(result, "Children Guest Time: {} ticks", fields[43]);

    result
}
