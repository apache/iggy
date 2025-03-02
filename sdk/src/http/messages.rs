use crate::client::MessageClient;
use crate::consumer::Consumer;
use crate::error::IggyError;
use crate::http::client::HttpClient;
use crate::http::HttpTransport;
use crate::identifier::Identifier;
use crate::messages::flush_unsaved_buffer::FlushUnsavedBuffer;
use crate::messages::poll_messages::{PollMessages, PollingStrategy};
use crate::messages::send_messages::{Message, Partitioning, SendMessages};
use crate::models::batch::IggyBatch;
use async_trait::async_trait;

#[async_trait]
impl MessageClient for HttpClient {
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
        /*
        let response = self
            .get_with_query(
                &get_path(&stream_id.as_cow_str(), &topic_id.as_cow_str()),
                &PollMessages {
                    stream_id: stream_id.clone(),
                    topic_id: topic_id.clone(),
                    partition_id,
                    consumer: consumer.clone(),
                    strategy: *strategy,
                    count,
                    auto_commit,
                },
            )
            .await?;
        let messages = response
            .json()
            .await
            .map_err(|_| IggyError::InvalidJsonResponse)?;
        Ok(messages)
        */
        todo!()
    }

    async fn send_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning: &Partitioning,
        messages: &mut [Message],
    ) -> Result<(), IggyError> {
        /*
        self.post(
            &get_path(&stream_id.as_cow_str(), &topic_id.as_cow_str()),
            &SendMessages {
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
                partitioning: partitioning.clone(),
                messages: messages.to_vec(),
            },
        )
        .await?;
        Ok(())
        */
        todo!()
    }

    async fn flush_unsaved_buffer(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: u32,
        fsync: bool,
    ) -> Result<(), IggyError> {
        /*
        let _ = self
            .get_with_query(
                &get_path_flush_unsaved_buffer(
                    &stream_id.as_cow_str(),
                    &topic_id.as_cow_str(),
                    partition_id,
                    fsync,
                ),
                &FlushUnsavedBuffer {
                    stream_id: stream_id.clone(),
                    topic_id: topic_id.clone(),
                    partition_id,
                    fsync,
                },
            )
            .await?;
        Ok(())
        */
        todo!()
    }
}

fn get_path(stream_id: &str, topic_id: &str) -> String {
    format!("streams/{stream_id}/topics/{topic_id}/messages")
}

fn get_path_flush_unsaved_buffer(
    stream_id: &str,
    topic_id: &str,
    partition_id: u32,
    fsync: bool,
) -> String {
    format!("streams/{stream_id}/topics/{topic_id}/messages/flush/{partition_id}/fsync={fsync}")
}
