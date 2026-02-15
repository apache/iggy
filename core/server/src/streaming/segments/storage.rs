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

pub use iggy_common::SegmentStorage as Storage;

use crate::configs::system::SystemConfig;
use iggy_common::IggyError;

/// Creates a new storage for the specified partition with the given start offset
pub async fn create_segment_storage(
    config: &SystemConfig,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
    messages_size: u64,
    indexes_size: u64,
    start_offset: u64,
) -> Result<Storage, IggyError> {
    let messages_path =
        config.get_messages_file_path(stream_id, topic_id, partition_id, start_offset);
    let index_path = config.get_index_path(stream_id, topic_id, partition_id, start_offset);
    let log_fsync = config.partition.enforce_fsync;
    let index_fsync = config.partition.enforce_fsync;
    let file_exists = false;

    Storage::new(
        &messages_path,
        &index_path,
        messages_size,
        indexes_size,
        log_fsync,
        index_fsync,
        file_exists,
    )
    .await
}
