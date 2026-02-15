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

use crate::{IggyError, IggyIndexesMut, IggyMessagesBatchMut, PooledBuffer};
use bytes::BytesMut;
use compio::buf::{IntoInner, IoBuf};
use compio::fs::{File, OpenOptions};
use compio::io::AsyncReadAtExt;
use err_trail::ErrContext;
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
}

// Safety: We are guaranteeing that MessagesReader will never be used from multiple threads
unsafe impl Send for MessagesReader {}

impl MessagesReader {
    /// Opens the messages file in read mode.
    pub async fn new(
        file_path: &str,
        messages_size_bytes: Rc<AtomicU64>,
    ) -> Result<Self, IggyError> {
        let file = OpenOptions::new()
            .read(true)
            .open(file_path)
            .await
            .error(|e: &std::io::Error| format!("Failed to open messages file: {file_path}. {e}"))
            .map_err(|_| IggyError::CannotReadFile)?;

        // posix_fadvise() doesn't exist on MacOS
        #[cfg(not(target_os = "macos"))]
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

    /// Reads `len` bytes from the messages file at the specified `offset`.
    async fn read_at(
        &self,
        offset: u32,
        len: u32,
        use_pool: bool,
    ) -> Result<PooledBuffer, std::io::Error> {
        if use_pool {
            let buf = PooledBuffer::with_capacity(len as usize);
            let len = len as usize;
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
}
