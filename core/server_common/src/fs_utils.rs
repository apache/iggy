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

/// Fsync `dir` so a rename published into it is itself durable, not just the
/// bytes it publishes.
///
/// # Platform behaviour
///
/// On Windows this is a no-op. A directory cannot be opened through the
/// ordinary file path there (`CreateFile` needs `FILE_FLAG_BACKUP_SEMANTICS`,
/// which `File::open` does not pass, so the call fails with
/// `ERROR_ACCESS_DENIED`), and NTFS exposes no documented equivalent of the
/// Unix "fsync the directory" barrier. Every caller fsyncs the file itself
/// before renaming, so the published bytes stay durable; only the ordering
/// guarantee on the directory entry is weaker.
///
/// # Errors
///
/// Returns the underlying [`io::Error`] when the directory cannot be opened
/// or synced. Always returns `Ok(())` on Windows.
#[cfg(unix)]
pub fn fsync_dir(dir: impl AsRef<Path>) -> io::Result<()> {
    std::fs::File::open(dir)?.sync_all()
}

#[cfg(windows)]
pub fn fsync_dir(_dir: impl AsRef<Path>) -> io::Result<()> {
    Ok(())
}

/// Fsync the parent directory of `path`, if it has one.
///
/// See [`fsync_dir`] for the Windows behaviour.
///
/// # Errors
///
/// Returns the underlying [`io::Error`] when the parent directory cannot be
/// opened or synced.
pub fn fsync_parent_dir(path: impl AsRef<Path>) -> io::Result<()> {
    match path.as_ref().parent() {
        Some(parent) => fsync_dir(parent),
        None => Ok(()),
    }
}

/// Async counterpart of [`fsync_dir`] for callers already on the compio
/// runtime.
///
/// # Errors
///
/// Returns the underlying [`io::Error`] when the directory cannot be opened
/// or synced. Always returns `Ok(())` on Windows.
#[cfg(unix)]
pub async fn fsync_dir_async(dir: impl AsRef<Path>) -> io::Result<()> {
    fs::File::open(dir).await?.sync_all().await
}

#[cfg(windows)]
#[allow(clippy::unused_async)]
pub async fn fsync_dir_async(_dir: impl AsRef<Path>) -> io::Result<()> {
    Ok(())
}

/// Async counterpart of [`fsync_parent_dir`].
///
/// # Errors
///
/// Returns the underlying [`io::Error`] when the parent directory cannot be
/// opened or synced.
pub async fn fsync_parent_dir_async(path: impl AsRef<Path>) -> io::Result<()> {
    match path.as_ref().parent() {
        Some(parent) => fsync_dir_async(parent).await,
        None => Ok(()),
    }
}
