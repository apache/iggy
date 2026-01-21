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

use bytes::Bytes;
use compio::{fs::File, io::AsyncWriteAtExt};
use iggy_common::{IggyError, IggyMessagesBatch};
use tracing::error;

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
