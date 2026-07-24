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

use crate::iggy_index::{IGGY_INDEX_SIZE, IggyIndex, IggyIndexCache};
use bytes::Buf;
use compio::fs::{File, OpenOptions};
use compio::io::AsyncReadAtExt;
use iggy_common::IggyError;
use tracing::trace;

/// Reader for the sparse index file written by [`crate::IggyIndexWriter`].
///
/// The on-disk stride is [`IGGY_INDEX_SIZE`] (24 bytes: `offset` u64,
/// `timestamp` u64, `position` u64, little-endian) — distinct from the legacy
/// 16-byte dense per-message index that `server_common::IndexReader` parses.
/// Recovery reaches for this reader so the reader matches the writer.
#[derive(Debug)]
pub struct IggyIndexReader {
    file_path: String,
    file: File,
}

impl IggyIndexReader {
    /// Opens the sparse index file at `file_path` for reading.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened.
    pub async fn new(file_path: &str) -> Result<Self, IggyError> {
        let file = OpenOptions::new()
            .read(true)
            .open(file_path)
            .await
            .map_err(|_| IggyError::CannotReadFile)?;
        Ok(Self {
            file_path: file_path.to_owned(),
            file,
        })
    }

    /// Number of whole 24-byte entries in the file. A trailing partial entry
    /// (torn write) is truncated by the integer division and ignored.
    async fn entry_count(&self) -> Result<u64, IggyError> {
        let size = self
            .file
            .metadata()
            .await
            .map_err(|_| IggyError::CannotReadFileMetadata)?
            .len();
        Ok(size / IGGY_INDEX_SIZE as u64)
    }

    async fn read_entry_at(&self, byte_position: u64) -> Result<IggyIndex, IggyError> {
        // `with_capacity` (len 0): `read_exact_at` fills the spare capacity in
        // place and advances the length, so a `vec![0u8; N]` (no spare) would
        // read nothing.
        let buffer = Vec::with_capacity(IGGY_INDEX_SIZE);
        let (result, buffer): (std::io::Result<()>, Vec<u8>) =
            self.file.read_exact_at(buffer, byte_position).await.into();
        result.map_err(|_| IggyError::CannotReadFile)?;
        let mut view = buffer.as_slice();
        let offset = view.get_u64_le();
        let timestamp = view.get_u64_le();
        let position = view.get_u64_le();
        trace!(
            target: "iggy.partitions.storage",
            file = self.file_path.as_str(),
            offset,
            timestamp,
            position,
            "read sparse index entry"
        );
        Ok(IggyIndex::new(offset, timestamp, position))
    }

    /// First index entry, or `None` when the file holds no whole entry.
    ///
    /// # Errors
    ///
    /// Returns an error if the file metadata or bytes cannot be read.
    pub async fn load_first(&self) -> Result<Option<IggyIndex>, IggyError> {
        if self.entry_count().await? == 0 {
            return Ok(None);
        }
        Ok(Some(self.read_entry_at(0).await?))
    }

    /// Last index entry, or `None` when the file holds no whole entry.
    ///
    /// # Errors
    ///
    /// Returns an error if the file metadata or bytes cannot be read.
    pub async fn load_last(&self) -> Result<Option<IggyIndex>, IggyError> {
        let count = self.entry_count().await?;
        if count == 0 {
            return Ok(None);
        }
        Ok(Some(
            self.read_entry_at((count - 1) * IGGY_INDEX_SIZE as u64)
                .await?,
        ))
    }

    /// Load every whole entry into an [`IggyIndexCache`] for offset / timestamp
    /// lower-bound lookups. Reads the whole file in one pass (index files are
    /// tiny: one sparse entry per flushed chunk). A trailing partial entry
    /// (torn write) is ignored (see [`Self::entry_count`]).
    ///
    /// # Errors
    ///
    /// Returns an error if the file metadata or bytes cannot be read.
    pub async fn load_all(&self) -> Result<IggyIndexCache, IggyError> {
        let count = usize::try_from(self.entry_count().await?)
            .map_err(|_| IggyError::CannotReadFileMetadata)?;
        if count == 0 {
            return Ok(IggyIndexCache::empty());
        }

        // `with_capacity` (len 0): `read_exact_at` fills the spare capacity in
        // place and advances the length (see `read_entry_at`).
        let buffer = Vec::with_capacity(count * IGGY_INDEX_SIZE);
        let (result, buffer): (std::io::Result<()>, Vec<u8>) =
            self.file.read_exact_at(buffer, 0).await.into();
        result.map_err(|_| IggyError::CannotReadFile)?;

        let mut cache = IggyIndexCache::with_capacity(count);
        let mut view = buffer.as_slice();
        for _ in 0..count {
            let offset = view.get_u64_le();
            let timestamp = view.get_u64_le();
            let position = view.get_u64_le();
            cache.insert(offset, timestamp, position);
        }
        Ok(cache)
    }
}
