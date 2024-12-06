use crate::compat::message_conversion::binary_schema::BinarySchema;
use crate::compat::message_conversion::schema_sampler::BinarySchemaSampler;
use crate::compat::message_conversion::snapshots::message_snapshot::MessageSnapshot;
use crate::compat::message_conversion::samplers::COMPONENT;
use crate::server_error::CompatError;
use crate::streaming::utils::file;
use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use error_set::ResultContext;
use tokio::io::AsyncReadExt;

pub struct MessageSampler {
    pub segment_start_offset: u64,
    pub log_path: String,
    pub index_path: String,
}
impl MessageSampler {
    pub fn new(segment_start_offset: u64, log_path: String, index_path: String) -> MessageSampler {
        MessageSampler {
            segment_start_offset,
            log_path,
            index_path,
        }
    }
}

unsafe impl Send for MessageSampler {}
unsafe impl Sync for MessageSampler {}

#[async_trait]
impl BinarySchemaSampler for MessageSampler {
    async fn try_sample(&self) -> Result<BinarySchema, CompatError> {
        let mut index_file = file::open(&self.index_path).await.with_error(|_| {
            format!(
                "{COMPONENT} - failed to open index file at path: {}",
                self.index_path
            )
        })?;
        let mut log_file = file::open(&self.log_path).await.with_error(|_| {
            format!(
                "{COMPONENT} - failed to open log file at path: {}",
                self.log_path
            )
        })?;
        let log_file_size = log_file.metadata().await.with_error(|_| {
            format!("{COMPONENT} - failed to retrieve metadata for log file at path: {}", self.log_path)
        })?.len();

        if log_file_size == 0 {
            return Ok(BinarySchema::RetainedMessageSchema);
        }

        let _ = index_file.read_u32_le().await.with_error(|_| {
            format!(
                "{COMPONENT} - failed to read the first index entry from file: {}",
                self.index_path
            )
        })?;
        let end_position = index_file.read_u32_le().await.with_error(|_| {
            format!(
                "{COMPONENT} - failed to read the second index entry from file: {}",
                self.index_path
            )
        })?;

        let buffer_size = end_position as usize;
        let mut buffer = BytesMut::with_capacity(buffer_size);
        buffer.put_bytes(0, buffer_size);
        let _ = log_file.read_exact(&mut buffer).await.with_error(|_| {
            format!(
                "{COMPONENT} - failed to read log file into buffer from path: {}",
                self.log_path
            )
        })?;

        let message = MessageSnapshot::try_from(buffer.freeze())?;
        if message.offset != self.segment_start_offset {
            return Err(CompatError::InvalidMessageOffsetFormatConversion);
        }
        Ok(BinarySchema::RetainedMessageSchema)
    }
}
