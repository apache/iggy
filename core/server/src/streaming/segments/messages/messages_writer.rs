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

use crate::streaming::segments::messages::write_batch_frozen;
use compio::fs::{File, OpenOptions};
use err_trail::ErrContext;
use iggy_common::{IggyByteSize, IggyError, IggyMessagesBatch};
use std::{
    rc::Rc,
    sync::atomic::{AtomicU64, Ordering},
};
use tracing::trace;

/// A dedicated struct for writing to the messages file.
#[derive(Debug)]
pub struct MessagesWriter {
    file_path: String,
    file: File,
    messages_size_bytes: Rc<AtomicU64>,
    fsync: bool,
}

// Safety: We are guaranteeing that MessagesWriter will never be used from multiple threads
unsafe impl Send for MessagesWriter {}

impl MessagesWriter {
    /// Opens the messages file in write mode.
    ///
    /// If the server confirmation is set to `NoWait`, the file handle is transferred to the
    /// persister task (and stored in `persister_task`) so that writes are done asynchronously.
    /// Otherwise, the file is retained in `self.file` for synchronous writes.
    pub async fn new(
        file_path: &str,
        messages_size_bytes: Rc<AtomicU64>,
        fsync: bool,
        file_exists: bool,
    ) -> Result<Self, IggyError> {
        let mut opts = OpenOptions::new();
        opts.create(true).write(true);
        let file = opts
            .open(file_path)
            .await
            .error(|err: &std::io::Error| {
                format!("Failed to open messages file: {file_path}, error: {err}")
            })
            .map_err(|_| IggyError::CannotReadFile)?;

        if file_exists {
            let _ = file.sync_all().await.error(|e: &std::io::Error| {
                format!("Failed to fsync messages file after creation: {file_path}, error: {e}")
            });

            let actual_messages_size = file
                .metadata()
                .await
                .error(|e: &std::io::Error| {
                    format!("Failed to get metadata of messages file: {file_path}, error: {e}")
                })
                .map_err(|_| IggyError::CannotReadFileMetadata)?
                .len();

            messages_size_bytes.store(actual_messages_size, Ordering::Relaxed);
        }

        trace!(
            "Opened messages file for writing: {file_path}, size: {}",
            messages_size_bytes.load(Ordering::Acquire)
        );

        Ok(Self {
            file_path: file_path.to_string(),
            file,
            messages_size_bytes,
            fsync,
        })
    }

    /// Append frozen (immutable) batches to the messages file.
    /// The caller retains the batches (for use in in-flight buffer) while disk I/O proceeds.
    pub async fn save_frozen_batches(
        &self,
        batches: &[IggyMessagesBatch],
    ) -> Result<IggyByteSize, IggyError> {
        let messages_size: u64 = batches.iter().map(|b| b.size() as u64).sum();

        let position = self.messages_size_bytes.load(Ordering::Relaxed);
        let file = &self.file;
        write_batch_frozen(file, position, batches)
            .await
            .error(|e: &IggyError| {
                format!(
                    "Failed to write frozen batch to messages file: {}. {e}",
                    self.file_path
                )
            })?;

        if self.fsync {
            let _ = self.fsync().await;
        }

        self.messages_size_bytes
            .fetch_add(messages_size, Ordering::Release);

        Ok(IggyByteSize::from(messages_size))
    }

    pub fn path(&self) -> String {
        self.file_path.clone()
    }

    pub async fn fsync(&self) -> Result<(), IggyError> {
        self.file
            .sync_all()
            .await
            .error(|e: &std::io::Error| {
                format!("Failed to fsync messages file: {}. {e}", self.file_path)
            })
            .map_err(|_| IggyError::CannotWriteToFile)?;
        Ok(())
    }
}
