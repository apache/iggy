use crate::binary::binary_client::BinaryClient;
use crate::binary::{fail_if_not_authenticated, mapper};
use crate::client::MessageClient;
use crate::command::{POLL_MESSAGES_CODE, SEND_MESSAGES_CODE};
use crate::consumer::Consumer;
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::messages::flush_unsaved_buffer::FlushUnsavedBuffer;
use crate::messages::poll_messages::PollingStrategy;
use crate::messages::send_messages::{Message, Partitioning};
use crate::messages::{poll_messages, send_messages};
use crate::models::batch::{IggyBatch, IggyHeader, IGGY_BATCH_OVERHEAD};

#[async_trait::async_trait]
impl<B: BinaryClient> MessageClient for B {
    async fn poll_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
        consumer: &Consumer,
        strategy: &PollingStrategy,
        count: u32,
        auto_commit: bool,
    ) -> Result<IggyBatch, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_raw_with_response(
                POLL_MESSAGES_CODE,
                poll_messages::as_bytes(
                    stream_id,
                    topic_id,
                    partition_id,
                    consumer,
                    strategy,
                    count,
                    auto_commit,
                ),
            )
            .await?;
        let mut position = 0;
        let header = IggyHeader::from_bytes(&response[..IGGY_BATCH_OVERHEAD as usize]);
        position += IGGY_BATCH_OVERHEAD as usize;
        let bytes = response.slice(position..);
        let batch = IggyBatch::new(header, bytes);
        Ok(batch)
    }

    async fn send_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning: &Partitioning,
        messages: &mut [Message],
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_raw_with_response(
            SEND_MESSAGES_CODE,
            send_messages::as_bytes(stream_id, topic_id, partitioning, messages),
        )
        .await?;
        Ok(())
    }

    async fn flush_unsaved_buffer(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: u32,
        fsync: bool,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(&FlushUnsavedBuffer {
            stream_id: stream_id.clone(),
            topic_id: topic_id.clone(),
            partition_id,
            fsync,
        })
        .await?;
        Ok(())
    }
}
