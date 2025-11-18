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

/// Parse the contents of a /proc/[pid]/task/[tid]/stat file into a human-readable format
fn parse_stat(contents: &str) -> String {
    let fields: Vec<&str> = contents.split_whitespace().collect();
    if fields.len() < 52 {
        return format!("Invalid stat format: {contents}");
    }

    let cmd_start = contents.find('(').unwrap_or(0);
    let cmd_end = contents.rfind(')').unwrap_or(contents.len());
    let comm = &contents[cmd_start + 1..cmd_end];

    let mut result = String::new();
    result.push_str(&format!("PID: {}\n", fields[0]));
    result.push_str(&format!("Command: {comm}\n"));
    result.push_str(&format!(
        "State: {} ({})\n",
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
    ));
    result.push_str(&format!("Parent PID: {}\n", fields[3]));
    result.push_str(&format!("Process Group: {}\n", fields[4]));
    result.push_str(&format!("Session ID: {}\n", fields[5]));
    result.push_str(&format!("TTY: {}\n", fields[6]));
    result.push_str(&format!("Foreground Process Group: {}\n", fields[7]));
    result.push_str(&format!("Kernel Flags: {}\n", fields[8]));
    result.push_str(&format!("Minor Faults: {}\n", fields[9]));
    result.push_str(&format!("Children Minor Faults: {}\n", fields[10]));
    result.push_str(&format!("Major Faults: {}\n", fields[11]));
    result.push_str(&format!("Children Major Faults: {}\n", fields[12]));
    result.push_str(&format!("User Mode Time: {} ticks\n", fields[13]));
    result.push_str(&format!("System Mode Time: {} ticks\n", fields[14]));
    result.push_str(&format!("Children User Mode Time: {} ticks\n", fields[15]));
    result.push_str(&format!(
        "Children System Mode Time: {} ticks\n",
        fields[16]
    ));
    result.push_str(&format!("Priority: {}\n", fields[17]));
    result.push_str(&format!("Nice Value: {}\n", fields[18]));
    result.push_str(&format!("Number of Threads: {}\n", fields[19]));
    result.push_str(&format!("Real-time Priority: {}\n", fields[39]));
    result.push_str(&format!(
        "Policy: {} ({})\n",
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
    ));
    result.push_str(&format!(
        "Aggregated Block I/O Delays: {} ticks\n",
        fields[41]
    ));
    result.push_str(&format!("Guest Time: {} ticks\n", fields[42]));
    result.push_str(&format!("Children Guest Time: {} ticks\n", fields[43]));

    result
}

/// Get detailed information about the system's processes and related /proc data
pub async fn get_proc_info() -> Result<String, std::io::Error> {
    let static_proc_files = vec![
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

    async fn dump_file(result: &mut String, path: &str) -> Result<(), std::io::Error> {
        match std::fs::read_to_string(path) {
            Ok(contents) => {
                result.push_str(&format!("=== {path} ===\n"));

                if path.ends_with("/stat") && path.contains("/task/") {
                    result.push_str(&parse_stat(&contents));
                } else {
                    result.push_str(&contents);
                }

                result.push_str("\n\n");
            }
            Err(e) => {
                if let Ok(metadata) = std::fs::metadata(path) {
                    if metadata.is_dir() && path.ends_with("/fd") {
                        result.push_str(&format!("=== {path} (directory) ===\n"));
                        if let Ok(mut rd) = std::fs::read_dir(path) {
                            while let Some(Ok(entry)) = rd.next() {
                                let fd_path = entry.path();
                                match std::fs::read_link(&fd_path) {
                                    Ok(link) => {
                                        result.push_str(&format!(
                                            "{} -> {}\n",
                                            fd_path.display(),
                                            link.display()
                                        ));
                                    }
                                    Err(_) => {
                                        result.push_str(&format!(
                                            "{} (unreadable symlink)\n",
                                            fd_path.display()
                                        ));
                                    }
                                }
                            }
                        }
                        result.push('\n');
                    } else {
                        result.push_str(&format!("=== {path} ERROR: {e} ===\n\n"));
                    }
                } else {
                    result.push_str(&format!("=== {path} ERROR: {e} ===\n\n"));
                }
            }
        }
        Ok(())
    }

    for path in &static_proc_files {
        dump_file(&mut result, path).await?;
    }

    let mut proc_dir = std::fs::read_dir("/proc")?;
    while let Some(Ok(entry)) = proc_dir.next() {
        let file_type = entry.file_type()?;
        if file_type.is_dir()
            && let Ok(pid) = entry.file_name().to_string_lossy().parse::<u32>()
        {
            let pid_paths = vec![
                format!("/proc/{}/cmdline", pid),
                format!("/proc/{}/statm", pid),
                format!("/proc/{}/cgroup", pid),
                format!("/proc/{}/task/{}/stat", pid, pid),
                format!("/proc/{}/task/{}/status", pid, pid),
                format!("/proc/{}/task/{}/wchan", pid, pid),
                format!("/proc/{}/task/{}/syscall", pid, pid),
                format!("/proc/{}/task/{}/fd", pid, pid),
            ];

            for p in pid_paths {
                dump_file(&mut result, &p).await?;
            }
        }
    }

    Ok(result)
}
