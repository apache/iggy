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
use compio::{
    fs::File,
    io::{AsyncBufRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter},
};
use iggy_common::{IGGY_MESSAGE_HEADER_SIZE, IggyMessageHeader};

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
        reader: &mut BufReader<std::io::Cursor<File>>,
    ) -> Result<IggyMessageHeader, std::io::Error> {
        let buf = [0u8; IGGY_MESSAGE_HEADER_SIZE];
        let (result, buf) = reader.read_exact(Box::new(buf)).await.into();
        result?;
        IggyMessageHeader::from_raw_bytes(&*buf)
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidData))
    }

    async fn write_index_entry(
        writer: &mut BufWriter<std::io::Cursor<File>>,
        header: &IggyMessageHeader,
        position: usize,
        start_offset: u64,
    ) -> Result<(), CompatError> {
        // Write offset (4 bytes) - base_offset + last_offset_delta - start_offset
        let offset = start_offset - header.offset;
        debug_assert!(offset <= u32::MAX as u64);
        let (result, _) = writer
            .write_all(Box::new(offset.to_le_bytes()))
            .await
            .into();
        result?;

        // Write position (4 bytes)
        let (result, _) = writer
            .write_all(Box::new(position.to_le_bytes()))
            .await
            .into();
        result?;

        // Write timestamp (8 bytes)
        let (result, _) = writer
            .write_all(Box::new(header.timestamp.to_le_bytes()))
            .await
            .into();
        result?;

        Ok(())
    }

    pub async fn rebuild(&self) -> Result<(), CompatError> {
        let read_cursor = std::io::Cursor::new(file::open(&self.messages_file_path).await?);
        let write_cursor = std::io::Cursor::new(file::overwrite(&self.index_path).await?);
        let mut reader = BufReader::new(read_cursor);
        let mut writer = BufWriter::new(write_cursor);
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
                    reader.consume(
                        header.payload_length as usize + header.user_headers_length as usize,
                    );

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
