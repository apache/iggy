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

use compio::{
    fs::{File, OpenOptions},
    io::AsyncWriteAtExt,
};
use iggy_binary_protocol::consensus::iobuf::Frozen;
use iggy_common::{IggyByteSize, IggyError};
use std::{
    rc::Rc,
    sync::atomic::{AtomicU64, Ordering},
};
use tracing::error;

const MAX_IOV_COUNT: usize = 1024;

#[derive(Debug)]
pub struct MessagesWriter {
    file_path: String,
    file: File,
    messages_size_bytes: Rc<AtomicU64>,
    fsync: bool,
}

impl MessagesWriter {
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
            .map_err(|_| IggyError::CannotReadFile)?;

        if file_exists {
            let _ = file
                .sync_all()
                .await
                .map_err(|_| IggyError::CannotWriteToFile);

            let actual_messages_size = file
                .metadata()
                .await
                .map_err(|_| IggyError::CannotReadFileMetadata)?
                .len();

            messages_size_bytes.store(actual_messages_size, Ordering::Relaxed);
        }

        Ok(Self {
            file_path: file_path.to_string(),
            file,
            messages_size_bytes,
            fsync,
        })
    }

    pub async fn save_frozen_batches<const ALIGN: usize>(
        &self,
        buffers: &[Frozen<ALIGN>],
    ) -> Result<IggyByteSize, IggyError> {
        let messages_size: u64 = buffers.iter().map(|buffer| buffer.len() as u64).sum();

        if messages_size == 0 {
            return Ok(IggyByteSize::from(0));
        }

        let position = self.messages_size_bytes.load(Ordering::Relaxed);
        write_frozen_chunked(&self.file, &self.file_path, position, buffers).await?;

        if self.fsync {
            self.fsync().await?;
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
            .map_err(|_| IggyError::CannotWriteToFile)?;
        Ok(())
    }
}

async fn write_frozen_chunked<const ALIGN: usize>(
    file: &File,
    file_path: &str,
    mut position: u64,
    buffers: &[Frozen<ALIGN>],
) -> Result<(), IggyError> {
    for chunk in buffers.chunks(MAX_IOV_COUNT) {
        let chunk_size: usize = chunk.iter().map(Frozen::len).sum();
        let chunk_vec: Vec<_> = chunk.to_vec();

        let (result, _) = (&*file)
            .write_vectored_all_at(chunk_vec, position)
            .await
            .into();
        result.map_err(|err| {
            error!(
                target: "iggy.partitions.storage",
                file = file_path,
                write_position = position,
                %err,
                "failed to write frozen messages to segment file"
            );
            IggyError::CannotWriteToFile
        })?;

        position += chunk_size as u64;
    }

    Ok(())
}
