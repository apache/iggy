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

use error_set::ErrContext;
use iggy_common::INDEX_SIZE;
use iggy_common::IggyError;
use monoio::fs::OpenOptions;
use monoio::io::AsyncWriteRentExt;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tracing::trace;

use crate::io::file::IggyFile;
use crate::streaming::utils::PooledBuffer;

/// A dedicated struct for writing to the index file.
#[derive(Debug)]
pub struct IndexWriter {
    file_path: String,
    file: IggyFile,
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
        let file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(file_path)
            .await
            .with_error_context(|error| format!("Failed to open index file: {file_path}. {error}"))
            .map_err(|_| IggyError::CannotReadFile)?;

        let file = IggyFile::from(file);
        if file_exists {
            let _ = file.sync_all().await.with_error_context(|error| {
                format!("Failed to fsync index file after creation: {file_path}. {error}",)
            });

            let actual_index_size = file
                .metadata()
                .await
                .with_error_context(|error| {
                    format!("Failed to get metadata of index file: {file_path}. {error}")
                })
                .map_err(|_| IggyError::CannotReadFileMetadata)?
                .len();

            index_size_bytes.store(actual_index_size, Ordering::Release);
        }

        trace!(
            "Opened index file for writing: {file_path}, size: {}",
            index_size_bytes.load(Ordering::Acquire)
        );

        Ok(Self {
            file_path: file_path.to_string(),
            file,
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
        let len = indexes.len();

        self.file
            .write_all(indexes)
            .await
            .0
            .with_error_context(|error| {
                format!(
                    "Failed to write {} indexes to file: {}. {error}",
                    count, self.file_path
                )
            })
            .map_err(|_| IggyError::CannotSaveIndexToSegment)?;

        self.index_size_bytes
            .fetch_add(len as u64, Ordering::Release);

        if self.fsync {
            let _ = self.fsync().await;
        }
        trace!(
            "Saved {count} indexes of size {} to file: {}",
            INDEX_SIZE * count,
            self.file_path
        );

        Ok(())
    }

    pub async fn fsync(&self) -> Result<(), IggyError> {
        self.file
            .sync_all()
            .await
            .with_error_context(|error| {
                format!("Failed to fsync index file: {}. {error}", self.file_path)
            })
            .map_err(|_| IggyError::CannotWriteToFile)?;
        Ok(())
    }
}
