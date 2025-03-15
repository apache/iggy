/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::utils::byte_size::IggyByteSize;
use crate::utils::timestamp::IggyTimestamp;
use serde::{Deserialize, Serialize};

/// `Partition` represents the information about a partition.
/// It consists of the following fields:
/// - `id`: unique identifier of the partition.
/// - `created_at`: the timestamp of the partition creation.
/// - `segments_count`: the number of segments in the partition.
/// - `current_offset`: the current offset of the partition.
/// - `size_bytes`: the size of the partition in bytes.
/// - `messages_count`: the number of messages in the partition.
#[derive(Debug, Serialize, Deserialize)]
pub struct Partition {
    /// Unique identifier of the partition.
    pub id: u32,
    /// The timestamp of the partition creation.
    pub created_at: IggyTimestamp,
    /// The number of segments in the partition.
    pub segments_count: u32,
    /// The current offset of the partition.
    pub current_offset: u64,
    /// The size of the partition in bytes.
    pub size: IggyByteSize,
    /// The number of messages in the partition.
    pub messages_count: u64,
}
