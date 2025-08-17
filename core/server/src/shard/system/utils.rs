use iggy_common::{Identifier, IggyError};

use crate::{shard::IggyShard, slab::traits_ext::EntityComponentSystem};

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
        //self.ensure_stream_exists(stream_id)?;
        let stream_id = self.streams2.get_index(stream_id);
        let exists = self
            .streams2
            .with_by_id(stream_id, |(root, _)| root.topics().exists(topic_id));
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
        //self.ensure_stream_exists(stream_id)?;
        //self.ensure_topic_exists(stream_id, topic_id)?;
        let exists = self
            .streams2
            .with_topic_root_by_id(stream_id, topic_id, |root| {
                root.consumer_groups().exists(group_id)
            });
        if !exists {
            return Err(IggyError::ConsumerGroupIdNotFound(0, 0));
        }
        Ok(())
    }
}
