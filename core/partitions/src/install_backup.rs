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

use journal::durable_storage::{DiskStorage, DurableFile, DurableStorage, OpenMode};
use std::io;
use std::path::Path;

const BACKUP: &str = ".install-backup";
const BUILDING: &str = ".install-building";
const RETIRED: &str = ".install-retired";

/// Recover an interrupted install before opening any partition files.
///
/// # Errors
/// Returns an error if the previous materialization cannot be restored durably.
pub async fn recover(directory: &Path) -> io::Result<()> {
    recover_with_storage(directory, &DiskStorage).await
}

/// # Errors
/// Returns an error when the install transaction cannot complete durably.
pub async fn recover_with_storage<S: DurableStorage>(
    directory: &Path,
    storage: &S,
) -> io::Result<()> {
    storage.remove_tree(&directory.join(BUILDING)).await?;
    storage.remove_tree(&directory.join(RETIRED)).await?;
    let backup = directory.join(BACKUP);
    if !storage.exists(&backup).await? {
        return Ok(());
    }
    for entry in storage.entries(directory).await? {
        if entry.name == BACKUP {
            continue;
        }
        storage.remove_tree(&directory.join(&entry.name)).await?;
    }
    link_tree(&backup, directory, false, storage).await?;
    finish_with_storage(directory, storage).await
}

/// Freeze the old materialization and WAL together before a destructive install.
/// The caller drains persistence and holds the partition write lock until finish.
///
/// # Errors
/// Returns an error if the rollback state cannot be made durable.
/// After any failure the caller must stop serving until recovery.
pub async fn begin(directory: &Path) -> io::Result<()> {
    begin_with_storage(directory, &DiskStorage).await
}

/// # Errors
/// Returns an error when the install transaction cannot complete durably.
pub async fn begin_with_storage<S: DurableStorage>(
    directory: &Path,
    storage: &S,
) -> io::Result<()> {
    if storage.exists(&directory.join(BACKUP)).await? {
        return Err(io::Error::other("partition install recovery is pending"));
    }
    let building = directory.join(BUILDING);
    storage.remove_tree(&building).await?;
    storage.remove_tree(&directory.join(RETIRED)).await?;
    storage.create_directories(&building).await?;
    link_tree(directory, &building, true, storage).await?;
    storage.rename(&building, &directory.join(BACKUP)).await?;
    storage.sync_directory(directory).await
}

/// Publish a completed install before the group can resume voting or serving.
///
/// # Errors
/// Returns an error if the installation cannot be published durably.
pub async fn finish(directory: &Path) -> io::Result<()> {
    finish_with_storage(directory, &DiskStorage).await
}

/// # Errors
/// Returns an error when the install transaction cannot complete durably.
pub async fn finish_with_storage<S: DurableStorage>(
    directory: &Path,
    storage: &S,
) -> io::Result<()> {
    let retired = directory.join(RETIRED);
    storage.rename(&directory.join(BACKUP), &retired).await?;
    storage.sync_directory(directory).await?;
    // The durable rename is the commit point. Cleanup never changes recovery.
    if let Err(error) = storage.remove_tree(&retired).await {
        tracing::warn!(%error, path = %retired.display(), "cannot remove completed install backup");
    }
    Ok(())
}

async fn link_tree<S: DurableStorage>(
    source: &Path,
    target: &Path,
    skip_scratch: bool,
    storage: &S,
) -> io::Result<()> {
    let mut pending = vec![(source.to_path_buf(), target.to_path_buf())];
    let mut directories = Vec::new();
    while let Some((source, target)) = pending.pop() {
        directories.push(target.clone());
        for entry in storage.entries(&source).await? {
            let name = entry.name;
            if skip_scratch && is_scratch(&name.to_string_lossy()) {
                continue;
            }
            let destination = target.join(&name);
            if entry.directory {
                storage.create_directories(&destination).await?;
                pending.push((source.join(&name), destination));
            } else {
                // Transfer unlinks or atomically replaces these frozen files.
                // Hard links retain the old bytes without copying segment data.
                storage.hard_link(&source.join(&name), &destination).await?;
                storage
                    .open(&destination, OpenMode::Read)
                    .await?
                    .sync()
                    .await?;
            }
        }
    }
    for directory in directories.into_iter().rev() {
        storage.sync_directory(&directory).await?;
    }
    Ok(())
}

fn is_scratch(name: &str) -> bool {
    matches!(name, BACKUP | BUILDING | RETIRED)
        || Path::new(name)
            .extension()
            .is_some_and(|extension| extension == "staging" || extension == "tmp")
}

#[cfg(test)]
mod tests {
    use super::*;
    use compio::fs::File;

    #[compio::test]
    async fn interrupted_install_restores_segments_offsets_and_wal_together() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        std::fs::create_dir(root.join("prepares-1")).unwrap();
        std::fs::create_dir(root.join("offsets")).unwrap();
        for name in [
            "0.log",
            "0.index",
            "superblock.a",
            "offsets/1",
            "prepares-1/frontier",
            "prepares-1/prepares-0.wal",
        ] {
            std::fs::write(root.join(name), name.as_bytes()).unwrap();
        }
        begin(root).await.unwrap();
        // Model the install's unlink and atomic replacement operations.
        for name in ["0.log", "offsets/1", "prepares-1/frontier"] {
            std::fs::remove_file(root.join(name)).unwrap();
            std::fs::write(root.join(name), b"replacement").unwrap();
        }
        std::fs::write(root.join("99.log"), b"new segment").unwrap();
        recover(root).await.unwrap();
        for name in [
            "0.log",
            "0.index",
            "superblock.a",
            "offsets/1",
            "prepares-1/frontier",
            "prepares-1/prepares-0.wal",
        ] {
            assert_eq!(std::fs::read(root.join(name)).unwrap(), name.as_bytes());
        }
        assert!(!root.join("99.log").exists());
        recover(root).await.unwrap();
    }

    #[compio::test]
    async fn completed_install_never_restores_the_old_materialization() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        std::fs::write(root.join("0.log"), b"old").unwrap();
        begin(root).await.unwrap();
        std::fs::remove_file(root.join("0.log")).unwrap();
        std::fs::write(root.join("0.log"), b"new").unwrap();
        File::open(root.join("0.log"))
            .await
            .unwrap()
            .sync_all()
            .await
            .unwrap();
        finish(root).await.unwrap();
        recover(root).await.unwrap();
        assert_eq!(std::fs::read(root.join("0.log")).unwrap(), b"new");
    }
}
