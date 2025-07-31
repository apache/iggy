use compio::fs::create_dir_all;
use iggy_common::IggyError;
use std::path::Path;

use crate::configs::system::SystemConfig;

pub async fn create_stream_file_hierarchy(
    id: usize,
    config: &SystemConfig,
) -> Result<(), IggyError> {
    let id = id as u32;
    let path = config.get_stream_path(id);
    let topics_path = config.get_topics_path(id);

    if !Path::new(&path).exists() && create_dir_all(&path).await.is_err() {
        return Err(IggyError::CannotCreateStreamDirectory(id, path.clone()));
    }

    if !Path::new(&topics_path).exists() && create_dir_all(&topics_path).await.is_err() {
        return Err(IggyError::CannotCreateTopicsDirectory(
            id,
            topics_path.clone(),
        ));
    }

    tracing::info!("Saved stream with ID: {}.", id);
    Ok(())
}
