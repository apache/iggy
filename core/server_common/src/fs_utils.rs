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

use compio::fs;
use std::io;
use std::path::{Path, PathBuf};
use tracing::warn;

#[cfg(target_os = "linux")]
use nix::fcntl::{FallocateFlags, fallocate};

#[derive(Debug, Clone)]
pub struct DirEntry {
    pub path: PathBuf,
    pub is_dir: bool,
    pub name: Option<String>,
}

impl DirEntry {
    fn file(path: PathBuf, name: Option<String>) -> Self {
        Self {
            path,
            is_dir: false,
            name,
        }
    }

    fn dir(path: PathBuf, name: Option<String>) -> Self {
        Self {
            path,
            is_dir: true,
            name,
        }
    }
}

/// Reserve segment space without changing its contents or logical length.
/// Unsupported or failed reservations fall back to buffered allocation.
#[cfg(target_os = "linux")]
pub fn preallocate_file(file: &fs::File, file_path: &Path, len: u64) {
    let Ok(len) = i64::try_from(len) else {
        warn!(
            target: "iggy.partitions.storage",
            file = %file_path.display(),
            preallocate_len = len,
            "file preallocation size is unsupported, using buffered allocation"
        );
        return;
    };

    // Runs INLINE on the shard thread, deliberately. `server_common::executor`
    // sets `thread_pool_limit(0)` on the shard proactor, so `spawn_blocking`
    // has no worker to park a task on and compio panics the shard outright with
    // "the thread pool is needed but no worker thread is running". (That limit
    // is skipped on macOS, whose polling driver routes fs through the pool, so
    // the panic is Linux-and-most-targets, not universal. This arm is
    // Linux-only regardless.)
    //
    // The cost is acceptable only because of what this call is: a metadata-only
    // extent reservation, microseconds on the local filesystems this option
    // exists for, and an immediate `EOPNOTSUPP` where the filesystem cannot do
    // it. Where it can genuinely block -- NFSv4.2 `ALLOCATE`, FUSE, a badly
    // fragmented extent tree forcing a journal commit -- it stalls the whole
    // core, not one partition, because nothing here yields. Preallocation is
    // opt-in per topic at creation for that reason; on such a deployment,
    // create topics without `preallocate_segments` rather than reintroducing a
    // pool the shard runtime does not have.
    if let Err(error) = fallocate(file, FallocateFlags::FALLOC_FL_KEEP_SIZE, 0, len) {
        warn!(
            target: "iggy.partitions.storage",
            file = %file_path.display(),
            preallocate_len = len,
            %error,
            "file preallocation failed, using buffered allocation"
        );
    }
}

/// Reserve segment space when supported, without changing its logical length.
#[cfg(not(target_os = "linux"))]
pub fn preallocate_file(_file: &fs::File, file_path: &Path, _len: u64) {
    warn!(
        target: "iggy.partitions.storage",
        file = %file_path.display(),
        "file preallocation is unavailable on this platform, using buffered allocation"
    );
}

/// Asynchronously walks a directory tree iteratively (without recursion).
/// Returns all entries with directories listed after their contents to enable
/// safe deletion (contents before containers).
/// Symlinks are treated as files and not followed.
pub async fn walk_dir(root: impl AsRef<Path>) -> io::Result<Vec<DirEntry>> {
    let root = root.as_ref();

    let metadata = fs::metadata(root).await?;
    if !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "path is not a directory",
        ));
    }

    let mut files = Vec::new();
    let mut directories = Vec::new();
    let mut stack = vec![root.to_path_buf()];

    while let Some(current_dir) = stack.pop() {
        directories.push(DirEntry::dir(
            current_dir.clone(),
            current_dir.to_str().map(|s| s.to_string()),
        ));

        for entry in std::fs::read_dir(&current_dir)? {
            let entry = entry?;
            let entry_path = entry.path();
            let metadata = fs::symlink_metadata(&entry_path).await?;

            if metadata.is_dir() {
                stack.push(entry_path);
            } else {
                files.push(DirEntry::file(
                    entry_path,
                    entry.file_name().into_string().ok(),
                ));
            }
        }
    }

    directories.reverse();
    files.extend(directories);
    Ok(files)
}

/// Removes a directory and all its contents.
/// This is the equivalent of `tokio::fs::remove_dir_all` for compio.
/// Uses walk_dir to traverse the directory tree without recursion.
pub async fn remove_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    for entry in walk_dir(path).await? {
        match entry.is_dir {
            true => fs::remove_dir(&entry.path).await?,
            false => fs::remove_file(&entry.path).await?,
        }
    }
    Ok(())
}
