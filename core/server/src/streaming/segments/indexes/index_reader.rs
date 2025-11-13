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

use super::IggyIndexesMut;
use crate::streaming::utils::PooledBuffer;
use bytes::BytesMut;
use compio::{
    buf::{IntoInner, IoBuf},
    fs::{File, OpenOptions},
    io::AsyncReadAtExt,
};
use err_trail::ErrContext;
use iggy_common::{INDEX_SIZE, IggyError, IggyIndex, IggyIndexView};
use std::{
    io::ErrorKind,
    rc::Rc,
    sync::atomic::{AtomicU64, Ordering},
};
use tracing::{error, trace};

/// A dedicated struct for reading from the index file.
#[derive(Debug)]
pub struct IndexReader {
    file_path: String,
    file: File,
    index_size_bytes: Rc<AtomicU64>,
}

// Safety: We are guaranteeing that IndexWriter will never be used from multiple threads
unsafe impl Send for IndexReader {}

impl IndexReader {
    /// Opens the index file in read-only mode.
    pub async fn new(file_path: &str, index_size_bytes: Rc<AtomicU64>) -> Result<Self, IggyError> {
        let file = OpenOptions::new()
            .read(true)
            .open(file_path)
            .await
            .with_error(|error| format!("Failed to open index file: {file_path}. {error}"))
            .map_err(|_| IggyError::CannotReadFile)?;

        trace!(
            "Opened index file for reading: {file_path}, size: {}",
            index_size_bytes.load(Ordering::Acquire)
        );
        Ok(Self {
            file_path: file_path.to_string(),
            file,
            index_size_bytes,
        })
    }

    pub fn path(&self) -> String {
        self.file_path.clone()
    }

    /// Loads all indexes from the index file into the optimized binary format.
    /// Note that this function does not use the pool, as the messages are not cached.
    /// This is expected - this method is called at startup and we want to preserve
    /// memory pool usage.
    pub async fn load_all_indexes_from_disk(&self) -> Result<IggyIndexesMut, IggyError> {
        let file_size = self.file_size();
        if file_size == 0 {
            return Ok(IggyIndexesMut::empty());
        }

        let buf = match self.read_at(0, file_size, false).await {
            Ok(buf) => buf,
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => {
                return Ok(IggyIndexesMut::empty());
            }
            Err(error) => {
                error!(
                    "Error reading batch header at offset 0 in file {}: {error}",
                    self.file_path
                );
                return Err(IggyError::CannotReadFile);
            }
        };
        let index_count = file_size / INDEX_SIZE as u32;
        let mut indexes = IggyIndexesMut::from_bytes(buf, 0);
        if indexes.count() != index_count {
            error!(
                "Loaded {} indexes from disk, expected {}, file {} is probably corrupted!",
                indexes.count(),
                index_count,
                self.file_path
            );
        }
        indexes.mark_saved();
        Ok(indexes)
    }

    /// Loads a specific range of indexes from disk based on offset.
    ///
    /// Returns a slice of indexes starting at the relative_start_offset with the specified count,
    /// or None if the requested range is not available.
    pub async fn load_from_disk_by_offset(
        &self,
        relative_start_offset: u32,
        count: u32,
    ) -> Result<Option<IggyIndexesMut>, IggyError> {
        let file_size = self.file_size();
        let total_indexes = file_size / INDEX_SIZE as u32;

        if file_size == 0 || total_indexes == 0 {
            trace!(
                "Index file {} is empty, cannot load indexes",
                self.file_path
            );
            return Ok(None);
        }

        if relative_start_offset >= total_indexes {
            trace!(
                "Start offset {} is out of bounds. Total indexes: {}",
                relative_start_offset, total_indexes
            );
            return Ok(None);
        }

        let available_count = total_indexes.saturating_sub(relative_start_offset);
        let actual_count = std::cmp::min(count, available_count);

        if actual_count == 0 {
            trace!(
                "No available indexes to load. Start offset: {}, requested count: {}",
                relative_start_offset, count
            );
            return Ok(None);
        }

        let start_byte = relative_start_offset as usize * INDEX_SIZE;
        let end_byte = start_byte + (actual_count as usize * INDEX_SIZE);

        let indexes_bytes = match self
            .read_at(start_byte as u32, (end_byte - start_byte) as u32, true)
            .await
        {
            Ok(buf) => buf,
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => {
                error!("Unexpected EOF while reading indexes");
                return Ok(None);
            }
            Err(error) => {
                error!(
                    "Error reading {actual_count} indexes at position {relative_start_offset} in file {} of size {file_size}: {error}",
                    self.file_path
                );
                return Err(IggyError::CannotReadFile);
            }
        };

        let base_position = if relative_start_offset > 0 {
            match self.load_nth_index(relative_start_offset - 1).await? {
                Some(prev_index) => prev_index.position,
                None => {
                    trace!(
                        "Failed to load previous index at position {}",
                        relative_start_offset - 1
                    );
                    0
                }
            }
        } else {
            0
        };

        let indexes = IggyIndexesMut::from_bytes(indexes_bytes, base_position);

        trace!(
            "Loaded {actual_count} indexes from disk starting at offset {relative_start_offset}, base position: {base_position}, last position: {}",
            indexes.last_position()
        );

        Ok(Some(indexes))
    }

    /// Loads a specific range of indexes from disk based on timestamp.
    ///
    /// Returns a slice of indexes starting from the index with timestamp closest to
    /// (but not exceeding) the requested timestamp, with the specified count.
    pub async fn load_from_disk_by_timestamp(
        &self,
        timestamp: u64,
        count: u32,
    ) -> Result<Option<IggyIndexesMut>, IggyError> {
        let file_size = self.file_size();
        let total_indexes = file_size / INDEX_SIZE as u32;

        if file_size == 0 || total_indexes == 0 {
            trace!("Index file is empty");
            return Ok(None);
        }

        let start_index_pos = match self
            .binary_search_position_for_timestamp_async(timestamp)
            .await?
        {
            Some(pos) => pos,
            None => return Ok(None),
        };

        let available_count = total_indexes.saturating_sub(start_index_pos);
        let actual_count = std::cmp::min(count, available_count);

        if actual_count == 0 {
            trace!(
                "No available indexes to load. Start index pos: {}, requested count: {}",
                start_index_pos, count
            );
            return Ok(None);
        }

        let start_byte = start_index_pos as usize * INDEX_SIZE;
        let end_byte = start_byte + (actual_count as usize * INDEX_SIZE);

        let indexes_bytes = match self
            .read_at(start_byte as u32, (end_byte - start_byte) as u32, true)
            .await
        {
            Ok(buf) => buf,
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => return Ok(None),
            Err(error) => {
                error!(
                    "Error reading {actual_count} indexes at position {start_index_pos} in file {}: {error}",
                    self.file_path
                );
                return Err(IggyError::CannotReadFile);
            }
        };

        let base_position = if start_index_pos > 0 {
            match self.load_nth_index(start_index_pos - 1).await? {
                Some(prev_index) => prev_index.position,
                None => {
                    trace!(
                        "Failed to load previous index at position {}",
                        start_index_pos - 1
                    );
                    0
                }
            }
        } else {
            0
        };

        trace!(
            "Loaded {} indexes from disk starting at timestamp {}, base position: {}",
            actual_count, timestamp, base_position
        );

        Ok(Some(IggyIndexesMut::from_bytes(
            indexes_bytes,
            base_position,
        )))
    }

    /// Finds the position of the index with timestamp closest to (but not exceeding) the target
    async fn binary_search_position_for_timestamp_async(
        &self,
        target_timestamp: u64,
    ) -> Result<Option<u32>, IggyError> {
        let file_size = self.file_size();
        if file_size == 0 {
            return Ok(None);
        }

        let total_indexes = file_size / INDEX_SIZE as u32;
        if total_indexes == 0 {
            return Ok(None);
        }

        let last_idx = match self.load_nth_index(total_indexes - 1).await? {
            Some(idx) => idx,
            None => return Ok(None),
        };

        if target_timestamp > last_idx.timestamp {
            return Ok(Some(total_indexes - 1));
        }

        let first_idx = match self.load_nth_index(0).await? {
            Some(idx) => idx,
            None => return Ok(None),
        };

        if target_timestamp <= first_idx.timestamp {
            return Ok(Some(0));
        }

        let mut low = 0;
        let mut high = total_indexes - 1;

        while low <= high {
            let mid = low + (high - low) / 2;
            let mid_index = match self.load_nth_index(mid).await? {
                Some(idx) => idx,
                None => break,
            };

            match mid_index.timestamp.cmp(&target_timestamp) {
                std::cmp::Ordering::Equal => return Ok(Some(mid)),
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => {
                    if mid == 0 {
                        break;
                    }
                    high = mid - 1;
                }
            }
        }

        Ok(Some(low))
    }

    /// Returns the size of the index file in bytes.
    fn file_size(&self) -> u32 {
        self.index_size_bytes.load(Ordering::Acquire) as u32
    }

    /// Reads a specified number of bytes from the index file at a given offset.
    async fn read_at(
        &self,
        offset: u32,
        len: u32,
        use_pool: bool,
    ) -> Result<PooledBuffer, std::io::Error> {
        if use_pool {
            let len = len as usize;
            let buf = PooledBuffer::with_capacity(len);
            let (result, buf) = self
                .file
                .read_exact_at(buf.slice(..len), offset as u64)
                .await
                .into();
            let buf = buf.into_inner();
            result?;
            Ok(buf)
        } else {
            let mut buf = BytesMut::with_capacity(len as usize);
            unsafe { buf.set_len(len as usize) };
            let (result, buf) = self.file.read_exact_at(buf, offset as u64).await.into();
            result?;
            Ok(PooledBuffer::from_existing(buf))
        }
    }

    /// Gets the nth index from the index file.
    ///
    /// The index position is 0-based (first index is at position 0).
    /// Returns None if the specified position is out of bounds.
    async fn load_nth_index(&self, position: u32) -> Result<Option<IggyIndex>, IggyError> {
        let file_size = self.file_size();
        let total_indexes = file_size / INDEX_SIZE as u32;

        if position >= total_indexes {
            trace!(
                "Index position {} is out of bounds. Total indexes: {}",
                position, total_indexes
            );
            return Ok(None);
        }

        let offset = position * INDEX_SIZE as u32;

        let buf = match self.read_at(offset, INDEX_SIZE as u32, true).await {
            Ok(buf) => buf,
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => {
                return Ok(None);
            }
            Err(error) => {
                error!(
                    "Error reading index at position {} (offset {}) in file {}: {error}",
                    position, offset, self.file_path
                );
                return Err(IggyError::CannotReadFile);
            }
        };

        let index = IggyIndexView::new(&buf).to_index();

        Ok(Some(index))
    }
}
