use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::identifier::Identifier;
use crate::models::partition;
use crate::segments::delete_segments::DeleteSegments;
use anyhow::Context;
use async_trait::async_trait;
use tracing::{event, Level};

pub struct DeleteSegmentsCmd {
    delete_segments: DeleteSegments,
}

impl DeleteSegmentsCmd {
    pub fn new(stream_id: Identifier, topic_id: Identifier, partition_id: Identifier, segments_count: u32) -> Self {
        Self {
            delete_segments: DeleteSegments {
                stream_id,
                topic_id,
                partition_id,
                segments_count,
            },
        }
    }
}

#[async_trait]
impl CliCommand for DeleteSegmentsCmd {
    fn explain(&self) -> String {
        let mut segments = String::from("segment");
        if self.delete_segments.segments_count > 1 {
            segments.push('s');
        };

        format!(
            "delete {} {segments} for topic with ID: {}, stream with ID: {} and partition with ID: {}",
            self.delete_segments.segments_count,
            self.delete_segments.topic_id,
            self.delete_segments.stream_id,
            self.delete_segments.partition_id
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let mut segments = String::from("segment");
        if self.delete_segments.segments_count > 1 {
            segments.push('s');
        };

        client
            .delete_segments(
                &self.delete_segments.stream_id,
                &self.delete_segments.topic_id,
                &self.delete_segments.partition_id,
                self.delete_segments.segments_count,
            )
            .await
            .with_context(|| {
                format!(
                    "Problem deleting {} {segments} for topic with ID: {}, stream with ID: {} and partition with ID: {}",
                    self.delete_segments.segments_count,
                    self.delete_segments.topic_id,
                    self.delete_segments.stream_id,
                    self.delete_segments.partition_id
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Deleted {} {segments} for topic with ID: {}, stream with ID: {} and partition with ID: {}",
            self.delete_segments.segments_count,
            self.delete_segments.topic_id,
            self.delete_segments.stream_id,
            self.delete_segments.partition_id
        );

        Ok(())
    }
}
