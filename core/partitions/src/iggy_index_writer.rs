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

use compio::fs::{File, OpenOptions};
use compio::io::AsyncWriteAtExt;
use iggy_common::IggyError;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{error, trace};

#[derive(Debug)]
pub struct IggyIndexWriter {
    file_path: String,
    file: File,
    index_size_bytes: Rc<AtomicU64>,
    fsync: bool,
}

impl IggyIndexWriter {
    /// Creates an index writer backed by the sparse index file at `file_path`.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened, synchronized, or queried for
    /// metadata, or if the on-disk length does not match the seeded size counter.
    pub async fn new(
        file_path: &str,
        index_size_bytes: Rc<AtomicU64>,
        fsync: bool,
        file_exists: bool,
    ) -> Result<Self, IggyError> {
        let mut opts = OpenOptions::new();
        opts.write(true);
        if !file_exists {
            opts.create(true);
        }
        let file = opts
            .open(file_path)
            .await
            .map_err(|_| IggyError::CannotReadFile)?;

        if file_exists {
            file.sync_all()
                .await
                .map_err(|_| IggyError::CannotWriteToFile)?;

            let actual_index_size = file
                .metadata()
                .await
                .map_err(|_| IggyError::CannotReadFileMetadata)?
                .len();

            // Refusal rationale documented on `IggyError::SegmentSizeMismatchAtOpen`.
            let expected_index_size = index_size_bytes.load(Ordering::Relaxed);
            if actual_index_size != expected_index_size {
                error!(
                    target: "iggy.partitions.storage",
                    file = file_path,
                    on_disk_size = actual_index_size,
                    expected_size = expected_index_size,
                    "sparse index file size does not match the seeded size at open"
                );
                return Err(IggyError::SegmentSizeMismatchAtOpen(
                    actual_index_size,
                    expected_index_size,
                ));
            }
        }

        let size = index_size_bytes.load(Ordering::Relaxed);
        trace!(
            target: "iggy.partitions.storage",
            file = file_path,
            size,
            "opened sparse index file for writing"
        );

        Ok(Self {
            file_path: file_path.to_owned(),
            file,
            index_size_bytes,
            fsync,
        })
    }

    /// Appends encoded sparse index bytes at the current write cursor and
    /// returns how many bytes landed. The cursor is left where it was: the
    /// caller advances it with `advance` once the companion segment save has
    /// also succeeded.
    ///
    /// # Errors
    ///
    /// Returns an error if the index bytes cannot be written or synced to disk.
    pub async fn save_indexes(&self, indexes: Vec<u8>) -> Result<u64, IggyError> {
        if indexes.is_empty() {
            return Ok(0);
        }

        let len = indexes.len();
        let position = self.index_size_bytes.load(Ordering::Relaxed);
        let file = &self.file;
        (&*file)
            .write_all_at(indexes, position)
            .await
            .0
            .map_err(|_| IggyError::CannotSaveIndexToSegment)?;

        if self.fsync {
            self.fsync().await?;
        }

        trace!(
            target: "iggy.partitions.storage",
            file = self.file_path.as_str(),
            bytes = len,
            position,
            "saved sparse index bytes to file"
        );
        Ok(len as u64)
    }

    /// Move the write cursor forward over `bytes` that are now durable. Split
    /// out of the save so the index and segment cursors advance together, only
    /// once both halves have succeeded.
    pub(crate) fn advance(&self, bytes: u64) {
        self.index_size_bytes.fetch_add(bytes, Ordering::Release);
    }

    /// Flushes buffered index file contents to disk.
    ///
    /// Uses `fdatasync` (data only): index files are append-only and the
    /// size change is tracked in datasync metadata on Linux, so the inode
    /// metadata fsync of `sync_all` adds latency without correctness gain.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be synchronized.
    pub async fn fsync(&self) -> Result<(), IggyError> {
        self.file
            .sync_data()
            .await
            .map_err(|_| IggyError::CannotWriteToFile)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[compio::test]
    async fn given_seeded_size_diverging_from_disk_when_opening_existing_file_should_return_size_mismatch_error()
     {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("segment.index");
        std::fs::write(&path, [7u8; 96]).unwrap();

        let result = IggyIndexWriter::new(
            path.to_str().unwrap(),
            Rc::new(AtomicU64::new(32)),
            false,
            true,
        )
        .await;

        assert!(matches!(
            result,
            Err(IggyError::SegmentSizeMismatchAtOpen(96, 32))
        ));
    }
}
