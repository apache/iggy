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

use super::PersisterTask;
use crate::streaming::batching::message_batch::RetainedMessageBatch;
use error_set::ErrContext;
use iggy::{
    confirmation::Confirmation,
    error::IggyError,
    utils::{byte_size::IggyByteSize, duration::IggyDuration, sizeable::Sizeable},
};
use std::{
    io::IoSlice,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::{
    fs::{File, OpenOptions},
    io::AsyncWriteExt,
};
use tracing::{error, trace};

/// A dedicated struct for writing to the log file.
#[derive(Debug)]
pub struct SegmentLogWriter {
    file_path: String,
    /// Holds the file for synchronous writes; when asynchronous persistence is enabled, this will be None.
    file: Option<File>,
    /// When set, asynchronous writes are handled by this persister task.
    persister_task: Option<PersisterTask>,
    log_size_bytes: Arc<AtomicU64>,
    fsync: bool,
}

impl SegmentLogWriter {
    /// Opens the log file in write mode.
    ///
    /// If the server confirmation is set to `NoWait`, the file handle is transferred to the
    /// persister task (and stored in `persister_task`) so that writes are done asynchronously.
    /// Otherwise, the file is retained in `self.file` for synchronous writes.
    pub async fn new(
        file_path: &str,
        log_size_bytes: Arc<AtomicU64>,
        fsync: bool,
        server_confirmation: Confirmation,
        max_file_operation_retries: u32,
        retry_delay: IggyDuration,
    ) -> Result<Self, IggyError> {
        let file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(file_path)
            .await
            .map_err(|_| IggyError::CannotReadFile)?;

        let _ = file.sync_all().await.with_error_context(|error| {
            format!("Failed to fsync log file after creation: {file_path}. {error}",)
        });

        let actual_log_size = file
            .metadata()
            .await
            .map_err(|_| IggyError::CannotReadFileMetadata)?
            .len();

        log_size_bytes.store(actual_log_size, Ordering::Release);

        trace!("Opened log file for writing: {file_path}, size: {actual_log_size}");

        let (file, persister_task) = match server_confirmation {
            Confirmation::NoWait => {
                let persister = PersisterTask::new(
                    file,
                    file_path.to_string(),
                    fsync,
                    log_size_bytes.clone(),
                    max_file_operation_retries,
                    retry_delay,
                );
                (None, Some(persister))
            }
            Confirmation::Wait => (Some(file), None),
        };

        Ok(Self {
            file_path: file_path.to_string(),
            file,
            persister_task,
            log_size_bytes,
            fsync,
        })
    }

    /// Append a message batch to the log file.
    pub async fn save_batches(
        &mut self,
        batch: RetainedMessageBatch,
        confirmation: Confirmation,
    ) -> Result<IggyByteSize, IggyError> {
        let batch_size = batch.get_size_bytes();
        match confirmation {
            Confirmation::Wait => {
                self.write_batch(batch).await?;
                self.log_size_bytes
                    .fetch_add(batch_size.as_bytes_u64(), Ordering::AcqRel);
                trace!(
                    "Written batch of size {batch_size} bytes to log file: {}",
                    self.file_path
                );
                if self.fsync {
                    let _ = self.fsync().await;
                }
            }
            Confirmation::NoWait => {
                if let Some(task) = &self.persister_task {
                    task.persist(batch).await;
                } else {
                    panic!(
                        "Confirmation::NoWait is used, but LogPersisterTask is not set for log file: {}",
                        self.file_path
                    );
                }
            }
        }

        Ok(batch_size)
    }

    /// Write a batch of bytes to the log file and return the new file position.
    async fn write_batch(&mut self, batch_to_write: RetainedMessageBatch) -> Result<(), IggyError> {
        if let Some(ref mut file) = self.file {
            let header = batch_to_write.header_as_bytes();
            let batch_bytes = batch_to_write.bytes;
            let slices = [IoSlice::new(&header), IoSlice::new(&batch_bytes)];

            file.write_vectored(&slices)
                .await
                .with_error_context(|error| {
                    format!("Failed to log to file: {}. {error}", self.file_path)
                })
                .map_err(|_| IggyError::CannotWriteToFile)?;

            Ok(())
        } else {
            error!("File handle is not available for synchronous write.");
            Err(IggyError::CannotWriteToFile)
        }
    }

    pub async fn fsync(&self) -> Result<(), IggyError> {
        if let Some(file) = self.file.as_ref() {
            file.sync_all()
                .await
                .with_error_context(|error| {
                    format!("Failed to fsync log file: {}. {error}", self.file_path)
                })
                .map_err(|_| IggyError::CannotWriteToFile)?;
        }

        Ok(())
    }

    pub async fn shutdown_persister_task(self) {
        if let Some(task) = self.persister_task {
            task.shutdown().await;
        }
    }
}
