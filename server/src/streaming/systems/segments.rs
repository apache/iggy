                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         use crate::streaming::partitions::partition;
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

        // 
        {
            let topic = self.find_topic(session, stream_id, topic_id).with_error_context(|error| format!("{COMPONENT} (error: {error}) - topic not found for stream_id: {stream_id}, topic_id: {topic_id}"))?;
            // TODO
            self.permissioner.delete_partitions(
                session.get_user_id(),
                topic.stream_id,
                topic.topic_id,
            ).with_error_context(|error| format!(
                "{COMPONENT} (error: {error}) - permission denied to delete partitions for user {} on stream_id: {}, topic_id: {}",
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

        let partition = topic.get_partition(partition_id)?.read().await;


        //     partition.

        // let partitions = topic
        //     .delete_persisted_partitions(partitions_count)
        //     .await
        //     .with_error_context(|error| {
        //         format!("{COMPONENT} (error: {error}) - failed to delete persisted partitions for topic: {topic}")
        //     })?;
        // topic.reassign_consumer_groups().await;
        // if let Some(partitions) = partitions {
        //     self.metrics.decrement_partitions(partitions_count);
        //     self.metrics.decrement_segments(partitions.segments_count);
        //     self.metrics.decrement_messages(partitions.messages_count);
        // }
        Ok(())
    }
}
