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
use futures::future::join_all;
use iggy_common::{IggyError, IggyMessagesBatch, PooledBuffer};

pub use messages_reader::MessagesReader;
pub use messages_writer::MessagesWriter;

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

    write_parallel_pooled(file, position, buffers).await?;
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

    write_parallel_bytes(file, position, buffers).await?;
    Ok(total_written)
}

/// Writes PooledBuffer buffers to file in parallel using positional writes.
async fn write_parallel_pooled(
    file: &File,
    position: u64,
    buffers: Vec<PooledBuffer>,
) -> Result<(), IggyError> {
    let mut next_position = position;
    let futures = buffers.into_iter().map(|buffer| {
        let write_position = next_position;
        next_position += buffer.len() as u64;

        async move {
            let (result, _) = (&*file).write_all_at(buffer, write_position).await.into();
            result.map_err(|_| IggyError::CannotWriteToFile)
        }
    });

    for result in join_all(futures).await {
        result?;
    }

    Ok(())
}

/// Writes Bytes buffers to file in parallel using positional writes.
async fn write_parallel_bytes(
    file: &File,
    position: u64,
    buffers: Vec<Bytes>,
) -> Result<(), IggyError> {
    let mut next_position = position;
    let futures = buffers.into_iter().map(|buffer| {
        let write_position = next_position;
        next_position += buffer.len() as u64;

        async move {
            let (result, _) = (&*file).write_all_at(buffer, write_position).await.into();
            result.map_err(|_| IggyError::CannotWriteToFile)
        }
    });

    for result in join_all(futures).await {
        result?;
    }

    Ok(())
}
