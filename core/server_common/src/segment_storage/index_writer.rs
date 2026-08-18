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

use compio::fs::File;
use compio::fs::OpenOptions;
use err_trail::ErrContext;
use iggy_common::IggyError;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::trace;

/// A dedicated struct for writing to the index file.
#[derive(Debug)]
pub struct IndexWriter {
    file_path: String,
    file: File,
    index_size_bytes: Rc<AtomicU64>,
}

// Safety: We are guaranteeing that IndexWriter will never be used from multiple threads
unsafe impl Send for IndexWriter {}

impl IndexWriter {
    /// Opens the index file in write mode.
    pub async fn new(
        file_path: &str,
        index_size_bytes: Rc<AtomicU64>,
        file_exists: bool,
    ) -> Result<Self, IggyError> {
        let mut opts = OpenOptions::new();
        opts.create(true).write(true);
        // Mirror MessagesWriter: truncate on fresh-build retry.
        if !file_exists {
            opts.truncate(true);
        }
        let file = opts
            .open(file_path)
            .await
            .error(|e: &std::io::Error| format!("Failed to open index file: {file_path}. {e}"))
            .map_err(|_| IggyError::CannotReadFile)?;

        if file_exists {
            let _ = file.sync_all().await.error(|e: &std::io::Error| {
                format!("Failed to fsync index file after creation: {file_path}. {e}",)
            });

            let actual_index_size = file
                .metadata()
                .await
                .error(|e: &std::io::Error| {
                    format!("Failed to get metadata of index file: {file_path}. {e}")
                })
                .map_err(|_| IggyError::CannotReadFileMetadata)?
                .len();

            index_size_bytes.store(actual_index_size, Ordering::Relaxed);
        }

        let size = index_size_bytes.load(Ordering::Relaxed);
        trace!("Opened index file for writing: {file_path}, size: {}", size);

        Ok(Self {
            file_path: file_path.to_string(),
            file,
            index_size_bytes,
        })
    }

    pub fn size_counter(&self) -> Rc<AtomicU64> {
        self.index_size_bytes.clone()
    }

    pub async fn fsync(&self) -> Result<(), IggyError> {
        self.file
            .sync_all()
            .await
            .error(|e: &std::io::Error| {
                format!("Failed to fsync index file: {}. {e}", self.file_path)
            })
            .map_err(|_| IggyError::CannotWriteToFile)?;
        Ok(())
    }
}
