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

mod index_reader;
mod index_writer;
mod messages_reader;
mod messages_writer;

use crate::{IggyError, IggyMessagesBatch};
use bytes::Bytes;
use compio::{fs::File, io::AsyncWriteAtExt};
use std::rc::Rc;
use tracing::error;

pub use index_reader::IndexReader;
pub use index_writer::IndexWriter;
pub use messages_reader::MessagesReader;
pub use messages_writer::MessagesWriter;

/// Maximum number of IO vectors for a single writev() call.
/// Linux typically has IOV_MAX=1024, but we use a conservative value to ensure
/// cross-platform compatibility and leave room for any internal overhead.
const MAX_IOV_COUNT: usize = 1024;

/// Vectored write frozen (immutable) batches to file.
pub async fn write_batch_frozen(
    file: &File,
    position: u64,
    batches: &[IggyMessagesBatch],
) -> Result<usize, IggyError> {
    let (total_written, buffers) = batches.iter().fold(
        (0usize, Vec::with_capacity(batches.len())),
        |(size, mut bufs), batch| {
            bufs.push(batch.messages_bytes());
            (size + batch.size() as usize, bufs)
        },
    );

    write_vectored_chunked_bytes(file, position, buffers).await?;
    Ok(total_written)
}

/// Writes Bytes buffers to file using vectored I/O, chunking to respect IOV_MAX limits.
async fn write_vectored_chunked_bytes(
    file: &File,
    mut position: u64,
    buffers: Vec<Bytes>,
) -> Result<(), IggyError> {
    for chunk in buffers.chunks(MAX_IOV_COUNT) {
        let chunk_size: usize = chunk.iter().map(|b| b.len()).sum();
        let chunk_vec: Vec<Bytes> = chunk.to_vec();

        let (result, _) = (&*file)
            .write_vectored_all_at(chunk_vec, position)
            .await
            .into();
        result.map_err(|e| {
            error!("Failed to write frozen batch to messages file: {e}");
            IggyError::CannotWriteToFile
        })?;

        position += chunk_size as u64;
    }
    Ok(())
}

unsafe impl Send for SegmentStorage {}

#[derive(Debug, Clone, Default)]
pub struct SegmentStorage {
    pub messages_writer: Option<Rc<MessagesWriter>>,
    pub messages_reader: Option<Rc<MessagesReader>>,
    pub index_writer: Option<Rc<IndexWriter>>,
    pub index_reader: Option<Rc<IndexReader>>,
}

impl SegmentStorage {
    pub async fn new(
        messages_path: &str,
        index_path: &str,
        messages_size: u64,
        indexes_size: u64,
        log_fsync: bool,
        index_fsync: bool,
        file_exists: bool,
    ) -> Result<Self, IggyError> {
        let size = Rc::new(std::sync::atomic::AtomicU64::new(messages_size));
        let indexes_size = Rc::new(std::sync::atomic::AtomicU64::new(indexes_size));
        let messages_writer = Rc::new(
            MessagesWriter::new(messages_path, size.clone(), log_fsync, file_exists).await?,
        );

        let index_writer = Rc::new(
            IndexWriter::new(index_path, indexes_size.clone(), index_fsync, file_exists).await?,
        );

        if file_exists {
            messages_writer.fsync().await?;
            index_writer.fsync().await?;
        }

        let messages_reader = Rc::new(MessagesReader::new(messages_path, size).await?);
        let index_reader = Rc::new(IndexReader::new(index_path, indexes_size).await?);
        Ok(Self {
            messages_writer: Some(messages_writer),
            messages_reader: Some(messages_reader),
            index_writer: Some(index_writer),
            index_reader: Some(index_reader),
        })
    }

    pub fn shutdown(&mut self) -> (Option<Rc<MessagesWriter>>, Option<Rc<IndexWriter>>) {
        let messages_writer = self.messages_writer.take();
        let index_writer = self.index_writer.take();
        (messages_writer, index_writer)
    }

    pub fn segment_and_index_paths(&self) -> (Option<String>, Option<String>) {
        let index_path = self.index_reader.as_ref().map(|reader| reader.path());
        let segment_path = self.messages_reader.as_ref().map(|reader| reader.path());
        (segment_path, index_path)
    }
}
