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

use crate::types::segment_storage::direct_file::DirectFile;
use crate::{IggyError, IggyIndexesMut, IggyMessagesBatchMut, PooledBuffer};
use compio::buf::{IntoInner, IoBuf};
use compio::fs::{File, OpenOptions};
use compio::io::AsyncReadAtExt;
use err_trail::ErrContext;
use std::cell::RefCell;
use std::rc::Rc;
use std::{
    io::ErrorKind,
    sync::atomic::{AtomicU64, Ordering},
};
use tracing::{error, trace};

/// A dedicated struct for reading from the messages file.
#[derive(Debug)]
pub struct MessagesReader {
    file_path: String,
    file: File,
    messages_size_bytes: Rc<AtomicU64>,
    writer_file: Option<Rc<RefCell<DirectFile>>>,
}

// Safety: We are guaranteeing that MessagesReader will never be used from multiple threads
unsafe impl Send for MessagesReader {}

impl MessagesReader {
    /// Opens the messages file in read mode.
    pub async fn new(
        file_path: &str,
        messages_size_bytes: Rc<AtomicU64>,
        writer_file: Option<Rc<RefCell<DirectFile>>>,
    ) -> Result<Self, IggyError> {
        let file = OpenOptions::new()
            .read(true)
            .open(file_path)
            .await
            .error(|e: &std::io::Error| format!("Failed to open messages file: {file_path}. {e}"))
            .map_err(|_| IggyError::CannotReadFile)?;

        // posix_fadvise() is Linux-only in the nix crate
        #[cfg(target_os = "linux")]
        {
            let _ = nix::fcntl::posix_fadvise(
                &file,
                0,
                0, // 0 means the entire file
                nix::fcntl::PosixFadviseAdvice::POSIX_FADV_SEQUENTIAL,
            )
            .info(|e: &nix::errno::Errno| {
                format!(
                    "Failed to set sequential access pattern on messages file: {file_path}. {e}"
                )
            });
        }

        let size_bytes = messages_size_bytes.load(Ordering::Relaxed);
        trace!(
            "Opened messages file for reading: {file_path}, size: {}",
            size_bytes
        );

        Ok(Self {
            file_path: file_path.to_string(),
            file,
            messages_size_bytes,
            writer_file,
        })
    }

    pub fn path(&self) -> String {
        self.file_path.clone()
    }

    /// Loads and returns a batch of messages from the messages file.
    pub async fn load_messages_from_disk(
        &self,
        indexes: IggyIndexesMut,
    ) -> Result<IggyMessagesBatchMut, IggyError> {
        let file_size = self.file_size();
        if file_size == 0 {
            return Ok(IggyMessagesBatchMut::empty());
        }

        let start_pos = indexes.base_position();
        let count_bytes = indexes.messages_size();
        let messages_count = indexes.count();

        if start_pos + count_bytes > file_size {
            return Ok(IggyMessagesBatchMut::empty());
        }

        let messages_bytes = match self.read_at(start_pos, count_bytes, true).await {
            Ok(buf) => buf,
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                return Ok(IggyMessagesBatchMut::empty());
            }
            Err(e) => {
                error!(
                    "Error reading {messages_count} messages at position {start_pos} in file {} of size {}: {e}",
                    self.file_path, file_size
                );
                return Err(IggyError::CannotReadMessage);
            }
        };

        Ok(IggyMessagesBatchMut::from_indexes_and_messages(
            indexes,
            messages_bytes,
        ))
    }

    /// Returns the size of the messages file in bytes.
    pub fn file_size(&self) -> u32 {
        self.messages_size_bytes.load(Ordering::Acquire) as u32
    }

    #[allow(clippy::await_holding_refcell_ref)]
    async fn read_at(
        &self,
        offset: u32,
        len: u32,
        _use_pool: bool,
    ) -> Result<PooledBuffer, std::io::Error> {
        if let Some(ref writer) = self.writer_file {
            let df = writer.borrow();
            let tail_start = df.position() as u32;
            let tail_len = df.tail_len() as u32;

            if tail_len > 0 && offset + len > tail_start {
                let mut result = PooledBuffer::with_capacity(len as usize);

                if offset < tail_start {
                    // Part from disk, part from tail
                    let disk_len = tail_start - offset;
                    let tail_read_len = (len - disk_len) as usize;
                    let tail_len_usize = tail_len as usize;
                    assert!(
                        tail_read_len <= tail_len_usize,
                        "tail read out of bounds: need {} but tail has {}",
                        tail_read_len,
                        tail_len_usize
                    );

                    // Copy tail data into an owned Vec before dropping the RefCell borrow.
                    //
                    // Safety of the data:
                    // This is not atomic but the writer is append-only.
                    // The tail bytes we copied and the disk region we're about to read are both historical data where content is fixed.
                    // Even if the writer flushes or appends new data during our .await, this read range remains valid and unchanged
                    //
                    // TODO(tungtose): share mmap, ring buffer,...?
                    let tail_data = df.tail_buffer()[..tail_read_len].to_vec();
                    drop(df);

                    let disk_buf = PooledBuffer::with_capacity(disk_len as usize);
                    let (res, disk_buf) = self
                        .file
                        .read_exact_at(disk_buf.slice(..disk_len as usize), offset as u64)
                        .await
                        .into();
                    let disk_buf = disk_buf.into_inner();
                    res?;
                    result.extend_from_slice(&disk_buf[..disk_len as usize]);
                    result.extend_from_slice(&tail_data);
                } else {
                    // Read all from tail
                    let tail_offset = (offset - tail_start) as usize;
                    let tail_len_usize = tail_len as usize;
                    assert!(
                        tail_offset + len as usize <= tail_len_usize,
                        "tail read out of bounds: offset {} + len {} exceeds tail {}",
                        tail_offset,
                        len,
                        tail_len_usize
                    );

                    result.extend_from_slice(
                        &df.tail_buffer()[tail_offset..tail_offset + len as usize],
                    );
                }
                return Ok(result);
            }
        }

        // Normal disk read
        let buf = PooledBuffer::with_capacity(len as usize);
        let (result, buf) = self
            .file
            .read_exact_at(buf.slice(..len as usize), offset as u64)
            .await
            .into();
        let buf = buf.into_inner();
        result?;
        Ok(buf)
    }
}
