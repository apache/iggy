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

mod messages_reader;
mod messages_writer;

use super::IggyMessagesBatchSet;
use bytes::Bytes;
use compio::{fs::File, io::AsyncWriteAtExt};
use iggy_common::{IggyError, IggyMessagesBatch, PooledBuffer};

pub use messages_reader::MessagesReader;
pub use messages_writer::MessagesWriter;

/// Maximum number of IO vectors for a single writev() call.
/// Linux typically has IOV_MAX=1024, but we use a conservative value to ensure
/// cross-platform compatibility and leave room for any internal overhead.
const MAX_IOV_COUNT: usize = 1024;

/// Vectored write a batches of messages to file
async fn write_batch(
    file: &File,
    position: u64,
    mut batches: IggyMessagesBatchSet,
) -> Result<usize, IggyError> {
    let (total_written, buffers) =
        batches
            .iter_mut()
            .fold((0usize, Vec::new()), |(size, mut bufs), batch| {
                let batch_size = batch.size() as usize;
                bufs.push(batch.take_messages());
                (size + batch_size, bufs)
            });

    write_vectored_chunked_pooled(file, position, buffers).await?;
    Ok(total_written)
}

/// Vectored write frozen (immutable) batches to file.
///
/// This function writes `IggyMessagesBatch` (immutable, Arc-backed) directly
/// without transferring ownership. The caller retains the batches for reads
/// during the async I/O operation.
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

/// Writes PooledBuffer buffers to file using vectored I/O, chunking to respect IOV_MAX limits.
async fn write_vectored_chunked_pooled(
    file: &File,
    mut position: u64,
    buffers: Vec<PooledBuffer>,
) -> Result<(), IggyError> {
    let mut iter = buffers.into_iter().peekable();

    while iter.peek().is_some() {
        let chunk: Vec<PooledBuffer> = iter.by_ref().take(MAX_IOV_COUNT).collect();
        let chunk_size: usize = chunk.iter().map(|b| b.len()).sum();

        let (result, _) = (&*file).write_vectored_all_at(chunk, position).await.into();
        result.map_err(|_| IggyError::CannotWriteToFile)?;

        position += chunk_size as u64;
    }
    Ok(())
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
        result.map_err(|_| IggyError::CannotWriteToFile)?;

        position += chunk_size as u64;
    }
    Ok(())
}
