use crate::slab::traits_ext::{DeleteCell, EntityComponentSystem, IntoComponents};
use crate::streaming::streams::stream2;
use crate::streaming::topics::storage2::delete_topic_from_disk;
use crate::{configs::system::SystemConfig, io::fs_utils::remove_dir_all, shard_info};
use compio::fs::create_dir_all;
use iggy_common::IggyError;
use std::path::Path;

pub async fn create_stream_file_hierarchy(
    shard_id: u16,
    id: usize,
    config: &SystemConfig,
) -> Result<(), IggyError> {
    let path = config.get_stream_path(id);

    if !Path::new(&path).exists() && create_dir_all(&path).await.is_err() {
        return Err(IggyError::CannotCreateStreamDirectory(
            id as u32,
            path.clone(),
        ));
    }

    shard_info!(shard_id, "Saved stream with ID: {}.", id);
    Ok(())
}

pub async fn delete_stream_from_disk(
    shard_id: u16,
    stream: &mut stream2::Stream,
    config: &SystemConfig,
) -> Result<(), IggyError> {
    let stream_id = stream.id();
    let stream_path = config.get_stream_path(stream_id);
    if !Path::new(&stream_path).exists() {
        return Err(IggyError::StreamIdNotFound(stream_id as u32));
    }

    // Gather all topic ids.
    let ids = stream.root().topics().with_components(|topics| {
        let (roots, ..) = topics.into_components();
        roots.iter().map(|(_, root)| root.id())
    });

    // Delete all topics from the stream.
    for id in ids {
        let topic = stream.root_mut().topics_mut().delete(id);
        delete_topic_from_disk(shard_id, stream_id, &mut topic, config).await?;
    }

    // Then delete the stream directory itself
    remove_dir_all(&stream_path).await?;
    shard_info!(
        shard_id,
        "Deleted stream files for stream with ID: {}.",
        stream_id
    );
    Ok(())
}
