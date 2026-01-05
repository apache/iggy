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

use crate::configs::system::SystemConfig;
use compio::fs::create_dir_all;
use iggy_common::IggyError;
use std::path::Path;

pub async fn create_topic_file_hierarchy(
    stream_id: usize,
    topic_id: usize,
    config: &SystemConfig,
) -> Result<(), IggyError> {
    let topic_path = config.get_topic_path(stream_id, topic_id);
    let partitions_path = config.get_partitions_path(stream_id, topic_id);
    if !Path::new(&topic_path).exists() && create_dir_all(&topic_path).await.is_err() {
        return Err(IggyError::CannotCreateTopicDirectory(
            topic_id, stream_id, topic_path,
        ));
    }
    tracing::info!(
        "Saved topic with ID: {}. for stream with ID: {}",
        topic_id,
        stream_id
    );

    if !Path::new(&partitions_path).exists() && create_dir_all(&partitions_path).await.is_err() {
        return Err(IggyError::CannotCreatePartitionsDirectory(
            stream_id, topic_id,
        ));
    }
    Ok(())
}
