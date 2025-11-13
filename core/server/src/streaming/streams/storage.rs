// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::slab::traits_ext::{DeleteCell, EntityComponentSystem, EntityMarker, IntoComponents};
use crate::streaming::streams::stream;
use crate::streaming::topics::storage::delete_topic_from_disk;
use crate::{configs::system::SystemConfig, io::fs_utils::remove_dir_all};
use compio::fs::create_dir_all;
use iggy_common::IggyError;
use std::path::Path;

pub async fn create_stream_file_hierarchy(
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

    tracing::info!("Saved stream with ID: {}.", id);
    Ok(())
}

pub async fn delete_stream_from_disk(
    stream: &mut stream::Stream,
    config: &SystemConfig,
) -> Result<(), IggyError> {
    let stream_id = stream.id();
    let stream_path = config.get_stream_path(stream_id);
    if !Path::new(&stream_path).exists() {
        return Err(IggyError::StreamDirectoryNotFound(stream_path));
    }

    // Gather all topic ids.
    let ids = stream.root().topics().with_components(|topics| {
        let (roots, ..) = topics.into_components();
        roots.iter().map(|(_, root)| root.id()).collect::<Vec<_>>()
    });

    // Delete all topics from the stream.
    for id in ids {
        let mut topic = stream.root_mut().topics_mut().delete(id);
        delete_topic_from_disk(stream_id, &mut topic, config).await?;
    }

    remove_dir_all(&stream_path)
        .await
        .map_err(|_| IggyError::CannotDeleteStreamDirectory(stream_id as u32))?;
    tracing::info!("Deleted stream files for stream with ID: {}.", stream_id);
    Ok(())
}
