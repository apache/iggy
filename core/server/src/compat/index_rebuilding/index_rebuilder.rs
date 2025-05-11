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

use crate::server_error::CompatError;
use crate::streaming::utils::file;
use iggy_common::{IGGY_MESSAGE_HEADER_SIZE, IggyMessageHeader};
use std::io::SeekFrom;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter};

pub struct IndexRebuilder {
    pub messages_file_path: String,
    pub index_path: String,
    pub start_offset: u64,
}

impl IndexRebuilder {
    pub fn new(messages_file_path: String, index_path: String, start_offset: u64) -> Self {
        Self {
            messages_file_path,
            index_path,
            start_offset,
        }
    }

    async fn read_message_header(
        reader: &mut BufReader<tokio::fs::File>,
    ) -> Result<IggyMessageHeader, std::io::Error> {
        let mut buf = [0u8; IGGY_MESSAGE_HEADER_SIZE];
        reader.read_exact(&mut buf).await?;
        IggyMessageHeader::from_raw_bytes(&buf)
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidData))
    }

    async fn write_index_entry(
        writer: &mut BufWriter<tokio::fs::File>,
        header: &IggyMessageHeader,
        position: usize,
        start_offset: u64,
    ) -> Result<(), CompatError> {
        // Write offset (4 bytes) - base_offset + last_offset_delta - start_offset
        let offset = start_offset - header.offset;
        debug_assert!(offset <= u32::MAX as u64);
        writer.write_u32_le(offset as u32).await?;

        // Write position (4 bytes)
        writer.write_u32_le(position as u32).await?;

        // Write timestamp (8 bytes)
        writer.write_u64_le(header.timestamp).await?;

        Ok(())
    }

    pub async fn rebuild(&self) -> Result<(), CompatError> {
        let mut reader = BufReader::new(file::open(&self.messages_file_path).await?);
        let mut writer = BufWriter::new(file::overwrite(&self.index_path).await?);
        let mut position = 0;
        let mut next_position;

        loop {
            match Self::read_message_header(&mut reader).await {
                Ok(header) => {
                    next_position = position
                        + IGGY_MESSAGE_HEADER_SIZE
                        + header.payload_length as usize
                        + header.user_headers_length as usize;

                    Self::write_index_entry(&mut writer, &header, position, self.start_offset)
                        .await?;

                    // Skip message payload and headers
                    reader
                        .seek(SeekFrom::Current(
                            header.payload_length as i64 + header.user_headers_length as i64,
                        ))
                        .await?;

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
