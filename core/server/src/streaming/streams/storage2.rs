use crate::{configs::system::SystemConfig, shard_info};
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
