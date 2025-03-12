#[allow(deprecated)]
use crate::binary::binary_client::BinaryClient;
use crate::binary::fail_if_not_authenticated;
use crate::client::SegmentClient;
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::segments::delete_segments::DeleteSegments;

#[async_trait::async_trait]
impl<B: BinaryClient> SegmentClient for B {
    async fn delete_segments(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: u32,
        segments_count: u32,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(&DeleteSegments {
            stream_id: stream_id.clone(),
            topic_id: topic_id.clone(),
            partition_id,
            segments_count,
        })
        .await?;
        Ok(())
    }
}
