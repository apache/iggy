use std::sync::atomic::Ordering;

use crate::state::system::PartitionState;
use crate::streaming::partitions::partition::Partition;
use iggy::error::IggyError;

impl Partition {
    pub async fn load(&mut self, state: PartitionState) -> Result<(), IggyError> {
        let storage = self.storage.clone();
        storage.partition.load(self, state).await
    }

    pub async fn persist(&self) -> Result<(), IggyError> {
        self.storage.partition.save(self).await
    }

    pub async fn delete(&self) -> Result<(), IggyError> {
        for segment in &self.segments {
            self.storage.segment.delete(segment).await?;
            self.segments_count_of_parent_stream
                .fetch_sub(1, Ordering::SeqCst);
        }
        self.storage.partition.delete(self).await
    }

    pub async fn purge(&mut self) -> Result<(), IggyError> {
        self.current_offset = 0;
        self.unsaved_messages_count = 0;
        self.should_increment_offset = false;
        self.consumer_offsets.clear();
        self.consumer_group_offsets.clear();
        if let Some(cache) = self.cache.as_mut() {
            cache.purge();
        }
        for segment in &self.segments {
            self.storage.segment.delete(segment).await?;
            self.segments_count_of_parent_stream
                .fetch_sub(1, Ordering::SeqCst);
        }
        self.segments.clear();
        self.storage
            .partition
            .delete_consumer_offsets(&self.consumer_offsets_path)
            .await?;
        self.storage
            .partition
            .delete_consumer_offsets(&self.consumer_group_offsets_path)
            .await?;
        self.add_persisted_segment(0).await?;

        Ok(())
    }
}
