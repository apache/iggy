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

use compio::fs::{File, OpenOptions};
use err_trail::ErrContext;
use iggy_common::IggyError;
use std::{
    rc::Rc,
    sync::atomic::{AtomicU64, Ordering},
};
use tracing::{error, trace};

/// A dedicated struct for writing to the messages file.
#[derive(Debug)]
pub struct MessagesWriter {
    file_path: String,
    file: File,
    messages_size_bytes: Rc<AtomicU64>,
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
        file_exists: bool,
    ) -> Result<Self, IggyError> {
        let mut opts = OpenOptions::new();
        opts.create(true).write(true);
        // `file_exists = false` asserts a fresh start; truncate so a
        // stale file from a partial prior attempt doesn't survive.
        if !file_exists {
            opts.truncate(true);
        }
        let file = opts
            .open(file_path)
            .await
            .error(|err: &std::io::Error| {
                format!("Failed to open messages file: {file_path}, error: {err}")
            })
            .map_err(|_| IggyError::CannotReadFile)?;

        if file_exists {
            let actual_messages_size = file
                .metadata()
                .await
                .error(|e: &std::io::Error| {
                    format!("Failed to get metadata of messages file: {file_path}, error: {e}")
                })
                .map_err(|_| IggyError::CannotReadFileMetadata)?
                .len();

            // The caller seeds the size counter from recovered, validated bounds
            // and recovery truncates the file to them. A divergent on-disk length
            // means appending would resurrect or shear bytes those bounds
            // exclude, so refuse the open.
            let expected_messages_size = messages_size_bytes.load(Ordering::Relaxed);
            if actual_messages_size != expected_messages_size {
                error!(
                    "Messages file size on disk: {actual_messages_size} does not match expected size: {expected_messages_size}, file: {file_path}"
                );
                return Err(IggyError::SegmentSizeMismatchAtOpen(
                    actual_messages_size,
                    expected_messages_size,
                ));
            }
        }

        trace!(
            "Opened messages file for writing: {file_path}, size: {}",
            messages_size_bytes.load(Ordering::Acquire)
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

    pub fn size_counter(&self) -> Rc<AtomicU64> {
        self.messages_size_bytes.clone()
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
