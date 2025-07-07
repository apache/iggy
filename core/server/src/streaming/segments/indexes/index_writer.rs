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

use crate::streaming::segments::DirectFile;
use crate::streaming::utils::PooledBuffer;
use error_set::ErrContext;
use iggy_common::{INDEX_SIZE, IggyError};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tracing::trace;

/// A dedicated struct for writing to the index file.
#[derive(Debug)]
pub struct IndexWriter {
    direct_file: DirectFile,
    index_size_bytes: Arc<AtomicU64>,
    fsync: bool,
}

impl IndexWriter {
    /// Opens the index file in write mode.
    pub async fn new(
        file_path: &str,
        index_size_bytes: Arc<AtomicU64>,
        fsync: bool,
        file_exists: bool,
    ) -> Result<Self, IggyError> {
        let file_position = if file_exists {
            let current_size = index_size_bytes.load(Ordering::Acquire);
            (current_size + 511) & !511
        } else {
            index_size_bytes.store(0, Ordering::Release);
            0
        };

        trace!(
            "Opening index file for writing: {file_path}, file_position: {}",
            file_position
        );

        let mut direct_file = DirectFile::open(file_path, file_position, file_exists).await?;

        if file_exists {
            let actual_index_size = direct_file.get_file_size().await?;
            index_size_bytes.store(actual_index_size, Ordering::Release);

            trace!(
                "Opened existing index file: {file_path}, size: {}, file_position: {}",
                actual_index_size,
                direct_file.position()
            );
        }

        Ok(Self {
            direct_file,
            index_size_bytes,
            fsync,
        })
    }

    /// Appends multiple index buffer to the index file in a single operation.
    pub async fn save_indexes(&mut self, indexes: PooledBuffer) -> Result<(), IggyError> {
        if indexes.is_empty() {
            return Ok(());
        }

        let count = indexes.len() / INDEX_SIZE;
        let actual_len = indexes.len();

        trace!(
            "Saving {count} indexes to file: {} (size: {} bytes)",
            self.direct_file.file_path(),
            actual_len
        );

        let bytes_written = self
            .direct_file
            .write_all(indexes)
            .await
            .with_error_context(|error| {
                format!(
                    "Failed to write {} indexes to file: {}. {error}",
                    count,
                    self.direct_file.file_path()
                )
            })
            .map_err(|_| IggyError::CannotSaveIndexToSegment)?;

        let new_logical_size = self.index_size_bytes.load(Ordering::Relaxed) + bytes_written as u64;
        self.index_size_bytes
            .store(new_logical_size, Ordering::Release);

        trace!("Saved {count} indexes. Logical size: {}", new_logical_size);

        Ok(())
    }
}
