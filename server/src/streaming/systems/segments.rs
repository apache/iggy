use crate::streaming::session::Session;
use crate::streaming::systems::system::System;
use crate::streaming::systems::COMPONENT;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::identifier::Identifier;
use iggy::locking::IggySharedMutFn;

impl System {
    pub async fn delete_segments(
        &mut self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: u32,
        segments_count: u32,
    ) -> Result<(), IggyError> {
        // Assert authentication.
        self.ensure_authenticated(session)?;

        {
            let topic = self.find_topic(session, stream_id, topic_id).with_error_context(|error| format!("{COMPONENT} (error: {error}) - topic not found for stream_id: {stream_id}, topic_id: {topic_id}"))?;

            self.permissioner.delete_segments(
                session.get_user_id(),
                topic.stream_id,
                topic.topic_id,
            ).with_error_context(|error| format!(
                "{COMPONENT} (error: {error}) - permission denied to delete segments for user {} on stream_id: {}, topic_id: {}",
                session.get_user_id(),
                topic.stream_id,
                topic.topic_id
            ))?;
        }

        let topic = self
            .get_stream_mut(stream_id)?
            .get_topic_mut(topic_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to get mutable reference to stream with id: {stream_id}"
                    )
            })?;

        // Lock the current partition.
        let partition_lock = topic.get_partition(partition_id)?;
        let mut partition = partition_lock.write().await;

        // Retrieve the oldest segments for this partition.
        let segments = partition
            .segments
            .iter()
            .rev()
            .take(
                segments_count
                    .try_into()
                    .map_err(|_| IggyError::InvalidSegmentSize(segments_count.into()))?,
            )
            // coerce to tuple of u64 as this has copy implicit.
            .map(|segment| (segment.start_offset, segment.get_messages_count()))
            .collect::<Vec<_>>();

        // Delete the segments in sequence.
        let (deleted_segments_count, deleted_messages_count) = {
            let mut segments_count = 0;
            let mut messages_count = 0;

            for segment in segments {
                // delete the segment.
                let _ = partition.delete_segment(segment.0).await?;

                // increment metrics.
                segments_count += 1;
                messages_count += segment.1;
            }

            (segments_count, messages_count)
        };
        topic.reassign_consumer_groups().await;

        self.metrics.decrement_segments(deleted_segments_count);
        self.metrics.decrement_messages(deleted_messages_count);
        Ok(())
    }
}
