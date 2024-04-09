use crate::client::TopicClient;
use crate::compression::compression_algorithm::CompressionAlgorithm;
use crate::error::IggyError;
use crate::http::client::HttpClient;
use crate::http::HttpTransport;
use crate::identifier::Identifier;
use crate::models::topic::{Topic, TopicDetails};
use crate::topics::create_topic::CreateTopic;
use crate::topics::update_topic::UpdateTopic;
use crate::utils::byte_size::IggyByteSize;
use crate::utils::expiry::IggyExpiry;
use async_trait::async_trait;

#[async_trait]
impl TopicClient for HttpClient {
    async fn get_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<TopicDetails, IggyError> {
        let response = self
            .get(&get_details_path(
                &stream_id.as_cow_str(),
                &topic_id.as_cow_str(),
            ))
            .await?;
        let topic = response.json().await?;
        Ok(topic)
    }

    async fn get_topics(&self, stream_id: &Identifier) -> Result<Vec<Topic>, IggyError> {
        let response = self.get(&get_path(&stream_id.as_cow_str())).await?;
        let topics = response.json().await?;
        Ok(topics)
    }

    async fn create_topic(
        &self,
        stream_id: &Identifier,
        name: &str,
        partitions_count: u32,
        compression_algorithm: CompressionAlgorithm,
        replication_factor: Option<u8>,
        topic_id: Option<u32>,
        message_expiry: IggyExpiry,
        max_topic_size: Option<IggyByteSize>,
    ) -> Result<(), IggyError> {
        self.post(
            &get_path(&stream_id.as_cow_str()),
            &CreateTopic {
                stream_id: stream_id.clone(),
                name: name.to_string(),
                partitions_count,
                compression_algorithm,
                replication_factor,
                topic_id,
                message_expiry: message_expiry.into(),
                max_topic_size,
            },
        )
        .await?;
        Ok(())
    }

    async fn update_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: &str,
        compression_algorithm: CompressionAlgorithm,
        replication_factor: Option<u8>,
        message_expiry: IggyExpiry,
        max_topic_size: Option<IggyByteSize>,
    ) -> Result<(), IggyError> {
        self.put(
            &get_details_path(&stream_id.as_cow_str(), &topic_id.as_cow_str()),
            &UpdateTopic {
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
                name: name.to_string(),
                compression_algorithm,
                replication_factor,
                message_expiry: message_expiry.into(),
                max_topic_size,
            },
        )
        .await?;
        Ok(())
    }

    async fn delete_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.delete(&get_details_path(
            &stream_id.as_cow_str(),
            &topic_id.as_cow_str(),
        ))
        .await?;
        Ok(())
    }

    async fn purge_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.delete(&format!(
            "{}/purge",
            &get_details_path(&stream_id.as_cow_str(), &topic_id.as_cow_str(),)
        ))
        .await?;
        Ok(())
    }
}

fn get_path(stream_id: &str) -> String {
    format!("streams/{stream_id}/topics")
}

fn get_details_path(stream_id: &str, topic_id: &str) -> String {
    format!("{}/{topic_id}", get_path(stream_id))
}
