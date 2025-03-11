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

use super::{Index, INDEX_SIZE};
use error_set::ErrContext;
use iggy::error::IggyError;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::{
    fs::{File, OpenOptions},
    io::AsyncWriteExt,
};
use tracing::trace;

/// A dedicated struct for writing to the index file.
#[derive(Debug)]
pub struct SegmentIndexWriter {
    file_path: String,
    file: File,
    index_size_bytes: Arc<AtomicU64>,
    fsync: bool,
}

impl SegmentIndexWriter {
    /// Opens the index file in write mode.
    pub async fn new(
        file_path: &str,
        index_size_bytes: Arc<AtomicU64>,
        fsync: bool,
    ) -> Result<Self, IggyError> {
        let file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(file_path)
            .await
            .with_error_context(|error| format!("Failed to open index file: {file_path}. {error}"))
            .map_err(|_| IggyError::CannotReadFile)?;

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

        trace!("Opened index file for writing: {file_path}, size: {actual_index_size}");

        Ok(Self {
            file_path: file_path.to_string(),
            file,
            index_size_bytes,
            fsync,
        })
    }

    /// Append the given index record to the index file.
    pub async fn save_index(&mut self, index: Index) -> Result<(), IggyError> {
        let mut buf = [0u8; INDEX_SIZE as usize];
        buf[0..4].copy_from_slice(&index.offset.to_le_bytes());
        buf[4..8].copy_from_slice(&index.position.to_le_bytes());
        buf[8..16].copy_from_slice(&index.timestamp.to_le_bytes());

        {
            self.file
                .write_all(&buf)
                .await
                .with_error_context(|error| {
                    format!("Failed to write index to file: {}. {error}", self.file_path)
                })
                .map_err(|_| IggyError::CannotSaveIndexToSegment)?;
        }
        if self.fsync {
            let _ = self.fsync().await;
        }
        self.index_size_bytes
            .fetch_add(INDEX_SIZE, Ordering::Release);
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
