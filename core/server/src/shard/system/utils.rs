use iggy_common::{Consumer, ConsumerKind, Identifier, IggyError};

use crate::{
    shard::IggyShard,
    streaming::{
        polling_consumer::PollingConsumer,
        topics::{self},
    },
};

impl IggyShard {
    pub fn ensure_stream_exists(&self, stream_id: &Identifier) -> Result<(), IggyError> {
        if !self.streams2.exists(stream_id) {
            return Err(IggyError::StreamIdNotFound(0));
        }
        Ok(())
    }

    pub fn ensure_topic_exists(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.ensure_stream_exists(stream_id)?;
        let exists = self
            .streams2
            .with_topics(stream_id, topics::helpers::exists(topic_id));
        if !exists {
            return Err(IggyError::TopicIdNotFound(0, 0));
        }
        Ok(())
    }

    pub fn ensure_consumer_group_exists(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.ensure_stream_exists(stream_id)?;
        self.ensure_topic_exists(stream_id, topic_id)?;
        let exists = self.streams2.with_topic_by_id(
            stream_id,
            topic_id,
            topics::helpers::cg_exists(group_id),
        );
        if !exists {
            return Err(IggyError::ConsumerGroupIdNotFound(0, 0));
        }
        Ok(())
    }

    pub fn resolve_consumer_with_partition_id(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        consumer: &Consumer,
        client_id: u32,
        partition_id: Option<usize>,
        calculate_partition_id: bool,
    ) -> Option<(PollingConsumer, usize)> {
        match consumer.kind {
            ConsumerKind::Consumer => {
                let partition_id = partition_id.unwrap_or(0);
                Some((
                    PollingConsumer::consumer(&consumer.id, partition_id as usize),
                    partition_id as usize,
                ))
            }
            ConsumerKind::ConsumerGroup => {
                let cg_id = self.streams2.with_consumer_group_by_id(
                    stream_id,
                    topic_id,
                    &consumer.id,
                    topics::helpers::get_consumer_group_id(),
                );
                let member_id = self.streams2.with_consumer_group_by_id(
                    stream_id,
                    topic_id,
                    &consumer.id,
                    topics::helpers::get_consumer_group_member_id(client_id),
                );
                if let Some(partition_id) = partition_id {
                    return Some((
                        PollingConsumer::consumer_group(cg_id, member_id),
                        partition_id as usize,
                    ));
                }

                let partition_id = if calculate_partition_id {
                    self.streams2.with_consumer_group_by_id(
                        stream_id,
                        topic_id,
                        &consumer.id,
                        topics::helpers::calculate_partition_id_unchecked(member_id),
                    )
                } else {
                    self.streams2.with_consumer_group_by_id(
                        stream_id,
                        topic_id,
                        &consumer.id,
                        topics::helpers::get_current_partition_id_unchecked(member_id),
                    )
                };
                let Some(partition_id) = partition_id else {
                    return None;
                };

                Some((
                    PollingConsumer::consumer_group(cg_id, member_id),
                    partition_id,
                ))
            }
        }
    }
}
