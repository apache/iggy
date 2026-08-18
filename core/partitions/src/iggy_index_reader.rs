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
    ///
    /// # Errors
    ///
    /// Returns an error if the file metadata cannot be read.
    pub async fn entry_count(&self) -> Result<u64, IggyError> {
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
    /// lower-bound lookups in one read. Density is one sparse entry per flushed
    /// chunk, so an aggressive flush cadence (`messages_required_to_save = 1`)
    /// makes the file track every message: callers that cannot afford an
    /// unbounded read must gate on [`Self::entry_count`] and fall back to the
    /// on-file lower-bound lookups. A trailing partial entry (torn write) is
    /// ignored (see [`Self::entry_count`]).
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

    /// Last entry with `offset` at or below the target, binary-searching the
    /// file with single-entry preads instead of materializing it, for indexes
    /// too large to load whole. `None` when every entry sits above the target;
    /// semantics match [`IggyIndexCache::offset_lower_bound`]. `entry_count`
    /// comes from [`Self::entry_count`]; entries are written in ascending
    /// offset and timestamp order.
    ///
    /// # Errors
    ///
    /// Returns an error if any probed entry cannot be read.
    pub async fn offset_lower_bound(
        &self,
        entry_count: u64,
        offset: u64,
    ) -> Result<Option<IggyIndex>, IggyError> {
        Ok(self
            .lower_bound_by(entry_count, |entry| entry.offset, offset)
            .await?
            .map(|(entry, _)| entry))
    }

    /// [`Self::offset_lower_bound`] that also reports the successor entry's
    /// offset (`None` when the match is the last entry), bounding the offset
    /// interval the match resolves for. Costs one extra entry read; callers
    /// memoizing the resolution use the bound to answer later in-interval
    /// queries without reopening the index.
    ///
    /// # Errors
    ///
    /// Returns an error if any probed entry cannot be read.
    pub async fn offset_lower_bound_with_successor(
        &self,
        entry_count: u64,
        offset: u64,
    ) -> Result<Option<(IggyIndex, Option<u64>)>, IggyError> {
        let Some((entry, successor_index)) = self
            .lower_bound_by(entry_count, |entry| entry.offset, offset)
            .await?
        else {
            return Ok(None);
        };
        let successor_offset = if successor_index < entry_count {
            Some(
                self.read_entry_at(successor_index * IGGY_INDEX_SIZE as u64)
                    .await?
                    .offset,
            )
        } else {
            None
        };
        Ok(Some((entry, successor_offset)))
    }

    /// Last entry with `timestamp` at or below the target; `None` semantics
    /// match [`Self::offset_lower_bound`].
    ///
    /// # Errors
    ///
    /// Returns an error if any probed entry cannot be read.
    pub async fn timestamp_lower_bound(
        &self,
        entry_count: u64,
        timestamp: u64,
    ) -> Result<Option<IggyIndex>, IggyError> {
        Ok(self
            .lower_bound_by(entry_count, |entry| entry.timestamp, timestamp)
            .await?
            .map(|(entry, _)| entry))
    }

    /// Binary search for the last entry with `key` at or below `target`,
    /// returning it with its successor's entry index (the standard
    /// lower-bound exit: `low` lands on the first entry above the target).
    async fn lower_bound_by(
        &self,
        entry_count: u64,
        key: impl Fn(&IggyIndex) -> u64,
        target: u64,
    ) -> Result<Option<(IggyIndex, u64)>, IggyError> {
        let mut low = 0u64;
        let mut high = entry_count;
        let mut result = None;
        while low < high {
            let middle = low + (high - low) / 2;
            let entry = self.read_entry_at(middle * IGGY_INDEX_SIZE as u64).await?;
            if key(&entry) <= target {
                result = Some(entry);
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        Ok(result.map(|entry| (entry, low)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use compio::io::AsyncWriteAtExt;

    async fn write_index_file(entries: &[IggyIndex]) -> (std::path::PathBuf, String) {
        let dir = std::env::temp_dir().join(format!(
            "iggy-index-reader-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after epoch")
                .as_nanos(),
        ));
        compio::fs::create_dir_all(&dir)
            .await
            .expect("create temp dir");
        let path = dir.join("test.index").to_string_lossy().into_owned();
        let mut bytes = Vec::with_capacity(entries.len() * IGGY_INDEX_SIZE);
        for entry in entries {
            bytes.extend_from_slice(&IggyIndexCache::serialize(entry));
        }
        let mut file = compio::fs::File::create(&path)
            .await
            .expect("create index file");
        let (written, _) = file.write_all_at(bytes, 0).await.into();
        written.expect("write index entries");
        file.sync_all().await.expect("flush index file");
        (dir, path)
    }

    fn entries() -> Vec<IggyIndex> {
        vec![
            IggyIndex::new(10, 100, 1),
            IggyIndex::new(20, 200, 2),
            IggyIndex::new(30, 300, 3),
        ]
    }

    #[compio::test]
    async fn offset_lower_bound_on_file_returns_predecessor() {
        let (dir, path) = write_index_file(&entries()).await;
        let reader = IggyIndexReader::new(&path).await.expect("open index");
        let count = reader.entry_count().await.expect("entry count");
        assert_eq!(count, 3);

        let at_or_below = |result: Option<IggyIndex>| result.map(|entry| entry.offset);
        let lookup = reader.offset_lower_bound(count, 25).await.expect("lookup");
        assert_eq!(at_or_below(lookup), Some(20));
        let exact = reader.offset_lower_bound(count, 20).await.expect("lookup");
        assert_eq!(at_or_below(exact), Some(20));
        let below_range = reader.offset_lower_bound(count, 5).await.expect("lookup");
        assert!(below_range.is_none());
        let above_range = reader.offset_lower_bound(count, 35).await.expect("lookup");
        assert_eq!(at_or_below(above_range), Some(30));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio::test]
    async fn offset_lower_bound_with_successor_reports_the_interval_ceiling() {
        let (dir, path) = write_index_file(&entries()).await;
        let reader = IggyIndexReader::new(&path).await.expect("open index");
        let count = reader.entry_count().await.expect("entry count");

        let mid = reader
            .offset_lower_bound_with_successor(count, 25)
            .await
            .expect("lookup")
            .expect("in range");
        assert_eq!((mid.0.offset, mid.1), (20, Some(30)));
        let last = reader
            .offset_lower_bound_with_successor(count, 35)
            .await
            .expect("lookup")
            .expect("in range");
        assert_eq!(
            (last.0.offset, last.1),
            (30, None),
            "the last entry has no successor to bound its interval",
        );
        let below_range = reader
            .offset_lower_bound_with_successor(count, 5)
            .await
            .expect("lookup");
        assert!(below_range.is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio::test]
    async fn timestamp_lower_bound_on_file_returns_predecessor() {
        let (dir, path) = write_index_file(&entries()).await;
        let reader = IggyIndexReader::new(&path).await.expect("open index");
        let count = reader.entry_count().await.expect("entry count");

        let lookup = reader
            .timestamp_lower_bound(count, 250)
            .await
            .expect("lookup");
        assert_eq!(lookup.map(|entry| entry.timestamp), Some(200));
        let below_range = reader
            .timestamp_lower_bound(count, 50)
            .await
            .expect("lookup");
        assert!(below_range.is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
