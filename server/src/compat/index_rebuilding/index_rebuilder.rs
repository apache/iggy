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

use crate::streaming::utils::file;
use crate::{
    server_error::CompatError, streaming::batching::message_batch::RETAINED_BATCH_HEADER_LEN,
};
use std::io::SeekFrom;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter};

// Same struct as RetainedMessageBatch, but without payload
pub struct BatchHeader {
    base_offset: u64,
    last_offset_delta: u32,
    max_timestamp: u64,
    length: u32,
}

pub struct IndexRebuilder {
    pub log_path: String,
    pub index_path: String,
    pub start_offset: u64,
}

impl IndexRebuilder {
    pub fn new(log_path: String, index_path: String, start_offset: u64) -> Self {
        Self {
            log_path,
            index_path,
            start_offset,
        }
    }

    async fn read_batch_header(
        reader: &mut BufReader<tokio::fs::File>,
    ) -> Result<BatchHeader, std::io::Error> {
        let base_offset = reader.read_u64_le().await?;
        let length = reader.read_u32_le().await?;
        let last_offset_delta = reader.read_u32_le().await?;
        let max_timestamp = reader.read_u64_le().await?;

        Ok(BatchHeader {
            base_offset,
            length,
            last_offset_delta,
            max_timestamp,
        })
    }

    async fn write_index_entry(
        writer: &mut BufWriter<tokio::fs::File>,
        header: &BatchHeader,
        position: u32,
        start_offset: u64,
    ) -> Result<(), CompatError> {
        // Write offset (4 bytes) - base_offset + last_offset_delta - start_offset
        let offset = (header.base_offset + header.last_offset_delta as u64 - start_offset) as u32;
        writer.write_u32_le(offset).await?;

        // Write position (4 bytes)
        writer.write_u32_le(position).await?;

        // Write timestamp (8 bytes)
        writer.write_u64_le(header.max_timestamp).await?;

        Ok(())
    }

    pub async fn rebuild(&self) -> Result<(), CompatError> {
        let mut reader = BufReader::new(file::open(&self.log_path).await?);
        let mut writer = BufWriter::new(file::overwrite(&self.index_path).await?);
        let mut position = 0;
        let mut next_position;

        loop {
            match Self::read_batch_header(&mut reader).await {
                Ok(header) => {
                    // Calculate next position before writing current entry
                    next_position = position + RETAINED_BATCH_HEADER_LEN as u32 + header.length;

                    // Write index entry using current position
                    Self::write_index_entry(&mut writer, &header, position, self.start_offset)
                        .await?;

                    // Skip batch messages
                    reader.seek(SeekFrom::Current(header.length as i64)).await?;

                    // Update position for next iteration
                    position = next_position;
                }
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
        }

        writer.flush().await?;
        Ok(())
    }
}
