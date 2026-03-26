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

use crate::{
    IggyByteSize, IggyError, IggyMessagesBatch, alloc::memory_pool::ALIGNMENT,
    types::segment_storage::direct_file::DirectFile,
};
use bytes::Bytes;
use compio::io::AsyncReadAtExt;
use std::{
    cell::RefCell,
    rc::Rc,
    sync::atomic::{AtomicU64, Ordering},
};
use tracing::{error, trace};

/// A dedicated struct for writing to the messages file.
#[derive(Debug)]
pub struct DirectMessagesWriter {
    file_path: String,
    file: Rc<RefCell<DirectFile>>,
    messages_size_bytes: Rc<AtomicU64>,
}

// Safety: We are guaranteeing that MessagesWriter will never be used from multiple threads
unsafe impl Send for DirectMessagesWriter {}

impl DirectMessagesWriter {
    /// Opens the messages file in write mode.
    ///
    /// If the server confirmation is set to `NoWait`, the file handle is transferred to the
    /// persister task (and stored in `persister_task`) so that writes are done asynchronously.
    /// Otherwise, the file is retained in `self.file` for synchronous writes.
    pub async fn new(
        file_path: &str,
        messages_size_bytes: Rc<AtomicU64>,
        file_exists: bool,
    ) -> Result<Self, IggyError> {
        let initial_position = if file_exists {
            let metadata = compio::fs::metadata(file_path).await.map_err(|err| {
                error!("Failed to get metadata of messages file: {file_path}, error: {err}");
                IggyError::CannotReadFileMetadata
            })?;

            let actual_size = metadata.len();
            messages_size_bytes.store(actual_size, Ordering::Relaxed);

            // DirectFile requires aligned initial position
            // Align down to ALIGNMENT boundary
            actual_size & !(ALIGNMENT as u64 - 1)
        } else {
            0
        };

        let mut direct_file = DirectFile::open(file_path, initial_position, file_exists).await?;

        if file_exists {
            let actual_size = messages_size_bytes.load(Ordering::Relaxed);
            let tail_bytes = (actual_size - initial_position) as usize;
            if tail_bytes > 0 {
                trace!(
                    "Recovering {} tail bytes from previous session for {}",
                    tail_bytes, file_path
                );

                let read_file = compio::fs::File::open(file_path).await.map_err(|err| {
                    error!("Failed to open file for tail recovery: {file_path}, error: {err}");
                    IggyError::CannotReadFile
                })?;

                let mut tail_buf = vec![0u8; tail_bytes];
                let (result, buf) = read_file
                    .read_exact_at(tail_buf, initial_position)
                    .await
                    .into();

                result.map_err(|err| {
                    error!("Failed to read tail bytes at pos {initial_position}: {file_path}, error: {err}");
                    IggyError::CannotReadFile
                })?;

                tail_buf = buf;
                direct_file.set_tail(&tail_buf);
            }
        }

        trace!(
            "Opened DirectFile messages writer: {file_path}, pos: {}, size: {}",
            direct_file.position(),
            messages_size_bytes.load(Ordering::Acquire)
        );

        Ok(Self {
            file_path: file_path.to_string(),
            file: Rc::new(RefCell::new(direct_file)),
            messages_size_bytes,
        })
    }

    pub fn direct_file_rc(&self) -> Rc<RefCell<DirectFile>> {
        Rc::clone(&self.file)
    }

    /// Append frozen (immutable) batches to the messages file.
    /// The caller retains the batches (for use in in-flight buffer) while disk I/O proceeds.
    pub async fn save_frozen_batches(
        &self,
        batches: &[IggyMessagesBatch],
    ) -> Result<IggyByteSize, IggyError> {
        if batches.is_empty() {
            return Ok(IggyByteSize::from(0u64));
        }

        let message_size: u64 = batches.iter().map(|b| b.size() as u64).sum();

        let write_buffers: Vec<Bytes> = batches
            .iter()
            .filter(|b| !b.is_empty())
            .map(|b| b.messages_bytes())
            .collect();

        if write_buffers.is_empty() {
            return Ok(IggyByteSize::from(0u64));
        }

        let mut df = self.file.borrow_mut();

        let written = if write_buffers.len() == 1 {
            df.write_all(write_buffers.into_iter().next().unwrap())
                .await?
        } else {
            df.write_vectored(write_buffers).await?
        };

        // df.flush().await?;
        let readable = df.position() + df.tail_len() as u64;
        self.messages_size_bytes.store(readable, Ordering::Release);

        trace!(
            "DirectFile wrote {} logical bytes ({} batches) to {}",
            written,
            batches.len(),
            self.file_path
        );

        Ok(IggyByteSize::from(message_size))
    }
    pub fn path(&self) -> String {
        self.file_path.clone()
    }

    pub fn size_counter(&self) -> Rc<AtomicU64> {
        self.messages_size_bytes.clone()
    }

    pub async fn flush(&self) -> Result<(), IggyError> {
        let logical_tail = self.file.borrow().tail_len();
        if logical_tail == 0 {
            return Ok(());
        }
        self.file.borrow_mut().flush().await?;
        let on_disk = self.file.borrow().position();
        let logical_size = on_disk - ALIGNMENT as u64 + logical_tail as u64;
        self.messages_size_bytes
            .store(logical_size, Ordering::Release);
        Ok(())
    }

    pub async fn flush_and_truncate(&self) -> Result<(), IggyError> {
        let logical_tail = self.file.borrow().tail_len();

        if logical_tail > 0 {
            self.file.borrow_mut().flush().await?;
            let on_disk = self.file.borrow().position();
            let logical_size = on_disk - ALIGNMENT as u64 + logical_tail as u64;
            self.messages_size_bytes
                .store(logical_size, Ordering::Release);
            self.file.borrow().truncate(logical_size).await?;
        } else {
            // Tail already flushed, but maybe paaded. So truncate to logical size
            let logical_size = self.messages_size_bytes.load(Ordering::Acquire);
            self.file.borrow().truncate(logical_size).await?;
        }

        Ok(())
    }
}
