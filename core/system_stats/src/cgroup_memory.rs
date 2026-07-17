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

//! MemAvailable-equivalent headroom for a memory-capped cgroup.
//!
//! `memory.current` charges reclaimable page cache as used, so
//! `limit - current` trends toward zero on a cache-heavy server even
//! though the kernel reclaims that cache long before OOM. The honest
//! number adds the reclaimable file cache back:
//! `limit - (current - inactive_file - active_file)`, evaluated at
//! every capped ancestor with the minimum kept, since a parent's cap
//! binds descendants even when the leaf itself is unlimited.

use std::fs;
use std::path::{Path, PathBuf};

/// Reclaimable-aware available memory for the calling process's memory
/// cgroup. `None` when no ancestor caps memory below the host total or
/// the cgroup fs is unreadable; callers keep their fallback then.
pub fn cgroup_available_memory(host_total_memory: u64) -> Option<u64> {
    let cgroup = fs::read_to_string("/proc/self/cgroup").ok()?;

    let from_v2 = v2_cgroup_path(&cgroup).and_then(|path| {
        let root = Path::new("/sys/fs/cgroup");
        available_memory_within(&root.join(path), root, &CGROUP_V2, host_total_memory)
    });
    if from_v2.is_some() {
        return from_v2;
    }

    v1_cgroup_path(&cgroup).and_then(|path| {
        let root = Path::new("/sys/fs/cgroup/memory");
        available_memory_within(&root.join(path), root, &CGROUP_V1, host_total_memory)
    })
}

/// v1 and v2 name the limit/usage files differently, and v1 needs the
/// `total_`-prefixed keys for hierarchical (descendants included) stats,
/// which v2 reports by default.
struct CgroupMemoryFiles {
    limit: &'static str,
    usage: &'static str,
    reclaimable_keys: [&'static str; 2],
}

const CGROUP_V2: CgroupMemoryFiles = CgroupMemoryFiles {
    limit: "memory.max",
    usage: "memory.current",
    reclaimable_keys: ["inactive_file", "active_file"],
};

const CGROUP_V1: CgroupMemoryFiles = CgroupMemoryFiles {
    limit: "memory.limit_in_bytes",
    usage: "memory.usage_in_bytes",
    reclaimable_keys: ["total_inactive_file", "total_active_file"],
};

fn available_memory_within(
    base: &Path,
    root: &Path,
    files: &CgroupMemoryFiles,
    host_total_memory: u64,
) -> Option<u64> {
    let mut available: Option<u64> = None;

    for level in base.ancestors() {
        // An unlimited level reads as "max" (v2, unparsable) or a value
        // beyond the host total (v1); neither constrains anything.
        if let Some(limit) =
            read_u64(&level.join(files.limit)).filter(|limit| *limit <= host_total_memory)
        {
            let usage = read_u64(&level.join(files.usage))?;
            let reclaimable =
                read_reclaimable(&level.join("memory.stat"), &files.reclaimable_keys)?;
            let level_available = limit.saturating_sub(usage.saturating_sub(reclaimable));
            available =
                Some(available.map_or(level_available, |tightest| tightest.min(level_available)));
        }
        if level == root {
            break;
        }
    }

    available
}

fn v2_cgroup_path(cgroup: &str) -> Option<PathBuf> {
    cgroup_relative_path(cgroup, |hierarchy_id, controllers| {
        hierarchy_id == "0" && controllers.is_empty()
    })
}

fn v1_cgroup_path(cgroup: &str) -> Option<PathBuf> {
    cgroup_relative_path(cgroup, |_, controllers| {
        controllers
            .split(',')
            .any(|controller| controller == "memory")
    })
}

fn cgroup_relative_path(
    cgroup: &str,
    line_matches: impl Fn(&str, &str) -> bool,
) -> Option<PathBuf> {
    cgroup.lines().find_map(|line| {
        let mut fields = line.splitn(3, ':');
        let hierarchy_id = fields.next()?;
        let controllers = fields.next()?;
        let path = fields.next()?;

        if !line_matches(hierarchy_id, controllers) {
            return None;
        }

        Some(Path::new(path).strip_prefix("/").ok()?.to_path_buf())
    })
}

fn read_u64(path: &Path) -> Option<u64> {
    fs::read_to_string(path).ok()?.trim().parse().ok()
}

/// A key absent from `memory.stat` counts as zero (kernels omit
/// feature-gated counters); only an unreadable file yields `None`.
fn read_reclaimable(stat_path: &Path, keys: &[&'static str; 2]) -> Option<u64> {
    let stat = fs::read_to_string(stat_path).ok()?;
    let mut reclaimable = 0u64;

    for key in keys {
        let value = stat.lines().find_map(|line| {
            let (name, value) = line.split_once(' ')?;
            if name != *key {
                return None;
            }
            value.trim().parse::<u64>().ok()
        });
        reclaimable = reclaimable.saturating_add(value.unwrap_or(0));
    }

    Some(reclaimable)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{create_dir_all, write};
    use tempfile::tempdir;

    const HOST_TOTAL: u64 = 10_000;

    #[test]
    fn v2_available_memory_adds_reclaimable_file_cache_back() {
        let root = tempdir().unwrap();
        let leaf = root.path().join("iggy.slice");
        create_dir_all(&leaf).unwrap();
        write(root.path().join("memory.max"), "max").unwrap();
        write(leaf.join("memory.max"), "1000").unwrap();
        write(leaf.join("memory.current"), "900").unwrap();
        write(
            leaf.join("memory.stat"),
            "anon 300\ninactive_file 400\nactive_file 100\n",
        )
        .unwrap();

        let available = available_memory_within(&leaf, root.path(), &CGROUP_V2, HOST_TOTAL);

        // limit - current would report 100; 500 of the 900 is reclaimable cache.
        assert_eq!(available, Some(600));
    }

    #[test]
    fn v2_tighter_parent_cap_wins_over_unlimited_leaf() {
        let root = tempdir().unwrap();
        let parent = root.path().join("parent");
        let leaf = parent.join("leaf");
        create_dir_all(&leaf).unwrap();
        write(root.path().join("memory.max"), "max").unwrap();
        write(parent.join("memory.max"), "500").unwrap();
        write(parent.join("memory.current"), "450").unwrap();
        write(
            parent.join("memory.stat"),
            "inactive_file 50\nactive_file 0\n",
        )
        .unwrap();
        write(leaf.join("memory.max"), "max").unwrap();
        write(leaf.join("memory.current"), "100").unwrap();
        write(
            leaf.join("memory.stat"),
            "inactive_file 10\nactive_file 0\n",
        )
        .unwrap();

        let available = available_memory_within(&leaf, root.path(), &CGROUP_V2, HOST_TOTAL);

        assert_eq!(available, Some(100));
    }

    #[test]
    fn v2_missing_reclaimable_key_counts_as_zero() {
        let root = tempdir().unwrap();
        let leaf = root.path().join("leaf");
        create_dir_all(&leaf).unwrap();
        write(root.path().join("memory.max"), "max").unwrap();
        write(leaf.join("memory.max"), "1000").unwrap();
        write(leaf.join("memory.current"), "900").unwrap();
        write(leaf.join("memory.stat"), "anon 300\ninactive_file 400\n").unwrap();

        let available = available_memory_within(&leaf, root.path(), &CGROUP_V2, HOST_TOTAL);

        assert_eq!(available, Some(500));
    }

    #[test]
    fn v2_no_capped_ancestor_yields_none() {
        let root = tempdir().unwrap();
        let leaf = root.path().join("leaf");
        create_dir_all(&leaf).unwrap();
        write(root.path().join("memory.max"), "max").unwrap();
        write(leaf.join("memory.max"), "max").unwrap();
        write(leaf.join("memory.current"), "100").unwrap();
        write(
            leaf.join("memory.stat"),
            "inactive_file 10\nactive_file 0\n",
        )
        .unwrap();

        let available = available_memory_within(&leaf, root.path(), &CGROUP_V2, HOST_TOTAL);

        assert_eq!(available, None);
    }

    #[test]
    fn v2_unreadable_usage_at_capped_level_yields_none() {
        let root = tempdir().unwrap();
        let leaf = root.path().join("leaf");
        create_dir_all(&leaf).unwrap();
        write(root.path().join("memory.max"), "max").unwrap();
        write(leaf.join("memory.max"), "1000").unwrap();

        let available = available_memory_within(&leaf, root.path(), &CGROUP_V2, HOST_TOTAL);

        assert_eq!(available, None);
    }

    #[test]
    fn v1_available_memory_uses_hierarchical_stat_keys() {
        let root = tempdir().unwrap();
        let leaf = root.path().join("iggy");
        create_dir_all(&leaf).unwrap();
        write(
            root.path().join("memory.limit_in_bytes"),
            u64::MAX.to_string(),
        )
        .unwrap();
        write(leaf.join("memory.limit_in_bytes"), "1000").unwrap();
        write(leaf.join("memory.usage_in_bytes"), "800").unwrap();
        write(
            leaf.join("memory.stat"),
            "inactive_file 999\ntotal_inactive_file 250\ntotal_active_file 50\n",
        )
        .unwrap();

        let available = available_memory_within(&leaf, root.path(), &CGROUP_V1, HOST_TOTAL);

        assert_eq!(available, Some(500));
    }

    #[test]
    fn cgroup_paths_parse_v2_and_v1_lines() {
        let hybrid = "12:cpuset:/\n11:memory,cpuacct:/system.slice/iggy.service\n0::/user.slice";

        assert_eq!(v2_cgroup_path(hybrid), Some(PathBuf::from("user.slice")));
        assert_eq!(
            v1_cgroup_path(hybrid),
            Some(PathBuf::from("system.slice/iggy.service"))
        );
        assert_eq!(v1_cgroup_path("12:cpuset:/\n10:cpu:/"), None);
    }
}
