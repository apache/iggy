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
use futures::channel::oneshot;
use futures::lock::Mutex;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::warn;

use crate::fatal::NoteDescriptorExhaustion;

#[cfg(target_os = "linux")]
use nix::fcntl::{FallocateFlags, fallocate};

#[cfg(not(target_os = "linux"))]
static PREALLOCATION_UNAVAILABLE: std::sync::Once = std::sync::Once::new();

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
pub async fn preallocate_file(file: &fs::File, file_path: &Path, len: u64) {
    let Ok(len) = i64::try_from(len) else {
        warn!(
            target: "iggy.partitions.storage",
            file = %file_path.display(),
            preallocate_len = len,
            "file preallocation size is unsupported, using buffered allocation"
        );
        return;
    };

    let reservation = async {
        let descriptor = std::os::fd::AsFd::as_fd(file)
            .try_clone_to_owned()
            .note_descriptor_exhaustion(|| {
                "duplicating a file descriptor to preallocate".to_owned()
            })?;
        run_blocking("iggy-file-preallocate", move || {
            fallocate(descriptor, FallocateFlags::FALLOC_FL_KEEP_SIZE, 0, len)
                .map_err(io::Error::from)
        })
        .await
    };
    if let Err(error) = reservation.await {
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
pub async fn preallocate_file(_file: &fs::File, file_path: &Path, _len: u64) {
    PREALLOCATION_UNAVAILABLE.call_once(|| {
        warn!(
            target: "iggy.partitions.storage",
            file = %file_path.display(),
            "file preallocation is unavailable on this platform, using buffered allocation"
        );
    });
}

/// Asynchronously walks a directory tree iteratively (without recursion).
/// Returns all entries with directories listed after their contents to enable
/// safe deletion (contents before containers).
/// Symlinks are treated as files and not followed.
pub async fn walk_dir(root: impl AsRef<Path>) -> io::Result<Vec<DirEntry>> {
    let root = root.as_ref().to_path_buf();
    run_blocking("iggy-directory-walk", move || walk_dir_blocking(&root)).await
}

fn walk_dir_blocking(root: &Path) -> io::Result<Vec<DirEntry>> {
    let metadata = std::fs::metadata(root)?;
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
            let metadata = entry.file_type()?;

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

/// Truncate the open inode without requiring IORING_OP_FTRUNCATE support.
///
/// # Errors
/// Returns descriptor duplication or filesystem errors.
pub async fn truncate_file(file: &fs::File, length: u64) -> io::Result<()> {
    let descriptor = std::os::fd::AsFd::as_fd(file)
        .try_clone_to_owned()
        .note_descriptor_exhaustion(|| "duplicating a file descriptor to truncate".to_owned())?;
    run_blocking("iggy-file-truncate", move || {
        std::fs::File::from(descriptor).set_len(length)
    })
    .await
}

/// Run filesystem work unsupported by io_uring on one worker per calling shard.
/// The operation owns its state and continues to completion if the caller is dropped.
///
/// # Errors
/// Returns thread creation, operation, or interrupted-worker errors.
pub async fn run_blocking<T: Send + 'static>(
    name: &'static str,
    operation: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> io::Result<T> {
    // Cancellation must not admit another operation while this one owns filesystem state.
    thread_local! {
        static WORKER: Arc<Mutex<()>> = Arc::new(Mutex::new(()));
    }
    let permit = WORKER.with(Arc::clone).lock_owned().await;
    let (sender, receiver) = oneshot::channel();
    std::thread::Builder::new()
        .name(name.to_owned())
        .spawn(move || {
            let _permit = permit;
            let _ = sender.send(operation());
        })?;
    receiver
        .await
        .map_err(|_| io::Error::other(format!("{name} stopped")))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::create_shard_executor;

    #[test]
    fn given_nested_directory_with_symlink_when_removing_should_preserve_external_target() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("partition");
        let child = root.join("nested");
        let target = directory.path().join("external");
        std::fs::create_dir_all(&child).unwrap();
        std::fs::write(child.join("segment.log"), b"messages").unwrap();
        std::fs::write(&target, b"keep").unwrap();
        std::os::unix::fs::symlink(&target, root.join("link")).unwrap();
        let runtime = create_shard_executor().unwrap();
        runtime.block_on(async {
            let entries = walk_dir(&root).await.unwrap();
            assert_eq!(entries.last().unwrap().path, root);
            remove_dir_all(&root).await.unwrap();
        });
        assert!(!root.exists());
        assert_eq!(std::fs::read(&target).unwrap(), b"keep");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn given_segment_contents_when_preallocating_without_pool_should_preserve_bytes_and_length() {
        const RESERVATION_BYTES: u64 = 1024 * 1024;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("segment.log");
        std::fs::write(&path, b"messages").unwrap();
        let runtime = create_shard_executor().unwrap();
        runtime.block_on(async {
            let file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .await
                .unwrap();
            preallocate_file(&file, &path, RESERVATION_BYTES).await;
            assert_eq!(
                file.metadata().await.unwrap().len(),
                b"messages".len() as u64
            );
        });
        assert_eq!(std::fs::read(&path).unwrap(), b"messages");
    }
}
