use super::COMPONENT;
use compio::fs::create_dir_all;
use iggy_common::IggyError;
use std::path::Path;

use crate::{configs::system::SystemConfig, shard_info, streaming::topics::topic2};

pub async fn create_topic_file_hierarchy(
    shard_id: u16,
    stream_id: usize,
    topic_id: usize,
    config: &SystemConfig,
) -> Result<(), IggyError> {
    let topic_path = config.get_topic_path(stream_id, topic_id);
    let partitions_path = config.get_partitions_path(stream_id, topic_id);
    if !Path::new(&topic_path).exists() && create_dir_all(&topic_path).await.is_err() {
        return Err(IggyError::CannotCreateTopicDirectory(
            topic_id as u32,
            stream_id as u32,
            topic_path,
        ));
    }
    shard_info!(
        shard_id,
        "Saved topic with ID: {}. for stream with ID: {}",
        topic_id,
        stream_id
    );

    if !Path::new(&partitions_path).exists() && create_dir_all(&partitions_path).await.is_err() {
        return Err(IggyError::CannotCreatePartitionsDirectory(
            stream_id as u32,
            topic_id as u32,
        ));
    }

    /*
    shard_info!(shard_id,
        "Saving {} partition(s) for topic {topic}...",
        topic.partitions.len()
    );
    for (_, partition) in topic.partitions.iter() {
        let mut partition = partition.write().await;
        partition.persist().await.with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to persist partition, topic: {topic}"
            )
        })?;
    }

    shard_info!(shard_id, "Saved topic {topic}");
    */
    Ok(())
}
