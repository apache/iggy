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
use compio::io::AsyncWriteAtExt;
use iggy_common::IggyError;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::trace;

#[derive(Debug)]
pub struct IggyIndexWriter {
    file_path: String,
    file: File,
    index_size_bytes: Rc<AtomicU64>,
    fsync: bool,
}

impl IggyIndexWriter {
    pub async fn new(
        file_path: &str,
        index_size_bytes: Rc<AtomicU64>,
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
            let _ = file.sync_all().await;

            let actual_index_size = file
                .metadata()
                .await
                .map_err(|_| IggyError::CannotReadFileMetadata)?
                .len();

            index_size_bytes.store(actual_index_size, Ordering::Relaxed);
        }

        let size = index_size_bytes.load(Ordering::Relaxed);
        trace!(
            target: "iggy.partitions.storage",
            file = file_path,
            size,
            "opened sparse index file for writing"
        );

        Ok(Self {
            file_path: file_path.to_owned(),
            file,
            index_size_bytes,
            fsync,
        })
    }

    pub async fn save_indexes(&self, indexes: Vec<u8>) -> Result<(), IggyError> {
        if indexes.is_empty() {
            return Ok(());
        }
        let start = std::time::Instant::now();

        let len = indexes.len();
        let position = self.index_size_bytes.load(Ordering::Relaxed);
        let file = &self.file;
        (&*file)
            .write_all_at(indexes, position)
            .await
            .0
            .map_err(|_| IggyError::CannotSaveIndexToSegment)?;

        self.index_size_bytes
            .fetch_add(len as u64, Ordering::Release);

        if self.fsync {
            let fsync_start = std::time::Instant::now();
            let _ = self.fsync().await;
            tracing::warn!(
                "IndexWriter: fsync took {}ms, file={}",
                fsync_start.elapsed().as_millis(),
                self.file_path
            );
        }

        tracing::warn!(
            "IndexWriter: save_indexes took {}ms, file={}",
            start.elapsed().as_millis(),
            self.file_path
        );

        trace!(
            target: "iggy.partitions.storage",
            file = self.file_path.as_str(),
            bytes = len,
            position,
            "saved sparse index bytes to file"
        );
        Ok(())
    }

    pub async fn fsync(&self) -> Result<(), IggyError> {
        self.file
            .sync_all()
            .await
            .map_err(|_| IggyError::CannotWriteToFile)?;
        Ok(())
    }
}
