use crate::client::SegmentClient;
use crate::error::IggyError;
use crate::http::client::HttpClient;
use crate::http::HttpTransport;
use crate::identifier::Identifier;
use crate::segments::delete_segments::DeleteSegments;
use async_trait::async_trait;

#[async_trait]
impl SegmentClient for HttpClient {
    async fn delete_segments(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: u32,
        segments_count: u32,
    ) -> Result<(), IggyError> {
        self.delete_with_query(
            &get_path(
                &stream_id.as_cow_str(),
                &topic_id.as_cow_str(),
                partition_id,
            ),
            &DeleteSegments {
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
                partition_id,
                segments_count,
            },
        )
        .await?;
        Ok(())
    }
}

fn get_path(stream_id: &str, topic_id: &str, partition_id: u32) -> String {
    format!("streams/{stream_id}/topics/{topic_id}/partitions/{partition_id}")
}
