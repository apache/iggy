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

use compio::{
    fs::{OpenOptions, create_dir_all, remove_file},
    io::{AsyncReadAtExt, AsyncWriteAtExt},
};
use iggy_common::IggyError;
use std::path::Path;

const OFFSET_SIZE: usize = core::mem::size_of::<u64>();

pub async fn persist_offset(path: &str, offset: u64, enforce_fsync: bool) -> Result<(), IggyError> {
    if let Some(parent) = Path::new(path).parent()
        && !parent.exists()
    {
        create_dir_all(parent).await.map_err(|_| {
            IggyError::CannotCreateConsumerOffsetsDirectory(parent.display().to_string())
        })?;
    }

    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .await
        .map_err(|_| IggyError::CannotOpenConsumerOffsetsFile(path.to_owned()))?;
    let buf = offset.to_le_bytes();
    file.write_all_at(buf, 0)
        .await
        .0
        .map_err(|_| IggyError::CannotWriteToFile)?;

    if enforce_fsync {
        file.sync_data()
            .await
            .map_err(|_| IggyError::CannotWriteToFile)?;
    }

    Ok(())
}

/// Monotone counterpart of [`persist_offset`] for a server auto-commit op:
/// folds `max(current_on_disk, offset)` and returns the value now on disk,
/// skipping the write when the file already holds it. Disk-tier polls
/// replicate their auto-committed offsets in IO-completion order, so a
/// committed op can carry a lower offset than an earlier one; a plain
/// overwrite would leave the file rewound and a restart would reload the
/// stale value and re-deliver. The on-disk value is committed-only (this path
/// never writes the eager serving map), so the fold is identical on every
/// replica applying the same op order.
///
/// The read makes this the cold-key path only: once the caller's
/// persisted-offset tracker knows the file's value, warm commits persist with
/// a blind [`persist_offset`] and skip covered offsets without any file read.
pub async fn persist_offset_max(
    path: &str,
    offset: u64,
    enforce_fsync: bool,
) -> Result<u64, IggyError> {
    let on_disk = read_persisted_offset(path).await?;
    let effective = on_disk.map_or(offset, |current| current.max(offset));
    if on_disk != Some(effective) {
        persist_offset(path, effective, enforce_fsync).await?;
    }
    Ok(effective)
}

/// Read a single persisted consumer offset. `None` if the file is absent or
/// torn (shorter than 8 bytes): a crash between `persist_offset`'s truncate
/// and write leaves a short file, and the boot-time loader already skips such
/// files, so the commit-path reader must agree or a torn file turns every
/// later commit-apply into an error. Real I/O errors still propagate: mapping
/// them to `None` would silently rewind a valid higher offset.
async fn read_persisted_offset(path: &str) -> Result<Option<u64>, IggyError> {
    if !Path::new(path).exists() {
        return Ok(None);
    }
    let file = OpenOptions::new()
        .read(true)
        .open(path)
        .await
        .map_err(|_| IggyError::CannotOpenConsumerOffsetsFile(path.to_owned()))?;
    let buf = vec![0u8; OFFSET_SIZE];
    let compio::BufResult(read, buf) = file.read_exact_at(buf, 0).await;
    match read {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(_) => return Err(IggyError::CannotReadConsumerOffsets(path.to_owned())),
    }
    let bytes: [u8; OFFSET_SIZE] = buf
        .try_into()
        .map_err(|_| IggyError::CannotReadConsumerOffsets(path.to_owned()))?;
    Ok(Some(u64::from_le_bytes(bytes)))
}

/// Unlink a persisted consumer-offset file. A no-op if the file is absent.
///
/// # Errors
/// Returns [`IggyError::CannotDeleteConsumerOffsetFile`] if the unlink fails.
pub async fn delete_persisted_offset(path: &str) -> Result<(), IggyError> {
    if !Path::new(path).exists() {
        return Ok(());
    }

    remove_file(path)
        .await
        .map_err(|_| IggyError::CannotDeleteConsumerOffsetFile(path.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_temp_dir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "iggy-offset-storage-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after epoch")
                .as_nanos(),
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    #[compio::test]
    async fn read_persisted_offset_absent_file_is_none() {
        let dir = unique_temp_dir();
        let path = dir.join("42").to_string_lossy().into_owned();

        let read = read_persisted_offset(&path).await.expect("absent file");
        assert_eq!(read, None);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio::test]
    async fn read_persisted_offset_round_trips_persisted_value() {
        let dir = unique_temp_dir();
        let path = dir.join("42").to_string_lossy().into_owned();

        persist_offset(&path, 114, false).await.expect("persist");
        let read = read_persisted_offset(&path).await.expect("valid file");
        assert_eq!(read, Some(114));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio::test]
    async fn read_persisted_offset_torn_file_is_none_not_error() {
        let dir = unique_temp_dir();
        let path = dir.join("42").to_string_lossy().into_owned();
        std::fs::write(&path, [0xAB, 0xCD, 0xEF]).expect("write torn file");

        let read = read_persisted_offset(&path)
            .await
            .expect("torn file must not error the commit path");
        assert_eq!(read, None, "short read maps to None like the boot loader");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio::test]
    async fn read_persisted_offset_real_io_error_propagates() {
        // A directory opens read-only but every read fails with EISDIR: a real
        // I/O error, not a short read. It must surface as Err, never as None
        // (a blanket None would silently rewind a valid higher offset).
        let dir = unique_temp_dir();
        let path = dir.to_string_lossy().into_owned();

        let result = read_persisted_offset(&path).await;
        assert!(
            matches!(result, Err(IggyError::CannotReadConsumerOffsets(_))),
            "real I/O error must propagate, got {result:?}",
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio::test]
    async fn persist_offset_max_recovers_torn_file() {
        let dir = unique_temp_dir();
        let path = dir.join("42").to_string_lossy().into_owned();
        std::fs::write(&path, [0xABu8; 5]).expect("write torn file");

        persist_offset_max(&path, 7, false)
            .await
            .expect("torn file folds as absent");
        let read = read_persisted_offset(&path).await.expect("repaired file");
        assert_eq!(read, Some(7));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
