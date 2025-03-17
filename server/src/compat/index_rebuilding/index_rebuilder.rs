use crate::server_error::CompatError;
use crate::streaming::utils::file;
use iggy::prelude::{IggyMessageHeader, IGGY_MESSAGE_HEADER_SIZE};
use std::io::SeekFrom;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter};

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

    async fn read_message_header(
        reader: &mut BufReader<tokio::fs::File>,
    ) -> Result<IggyMessageHeader, std::io::Error> {
        let mut buf = [0u8; IGGY_MESSAGE_HEADER_SIZE as usize];
        reader.read_exact(&mut buf).await?;
        Ok(IggyMessageHeader::from_raw_bytes(&buf)
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidData))?)
    }

    async fn write_index_entry(
        writer: &mut BufWriter<tokio::fs::File>,
        header: &IggyMessageHeader,
        position: u32,
        start_offset: u64,
    ) -> Result<(), CompatError> {
        // Write offset (4 bytes) - base_offset + last_offset_delta - start_offset
        let offset = start_offset - header.offset;
        debug_assert!(offset <= u32::MAX as u64);
        writer.write_u32_le(offset as u32).await?;

        // Write position (4 bytes)
        writer.write_u32_le(position).await?;

        // Write timestamp (8 bytes)
        writer.write_u64_le(header.timestamp).await?;

        Ok(())
    }

    pub async fn rebuild(&self) -> Result<(), CompatError> {
        let mut reader = BufReader::new(file::open(&self.log_path).await?);
        let mut writer = BufWriter::new(file::overwrite(&self.index_path).await?);
        let mut position = 0;
        let mut next_position;

        loop {
            match Self::read_message_header(&mut reader).await {
                Ok(header) => {
                    // Calculate next position before writing current entry
                    next_position = position
                        + IGGY_MESSAGE_HEADER_SIZE as u32
                        + header.payload_length
                        + header.headers_length;

                    // Write index entry using current position
                    Self::write_index_entry(&mut writer, &header, position, self.start_offset)
                        .await?;

                    // Skip message payload and headers
                    reader
                        .seek(SeekFrom::Current(
                            header.payload_length as i64 + header.headers_length as i64,
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
