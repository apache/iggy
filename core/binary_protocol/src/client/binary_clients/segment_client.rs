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

use async_trait::async_trait;
use iggy_common::{Identifier, IggyError};

/// This trait defines the methods to interact with the partition module.
#[async_trait]
pub trait SegmentClient {
    /// Delete last N segments for a partition by unique ID or name.
    ///
    /// For example, given a partition with 5 segments, if you delete 2 segments, the topic will have 3 segments left (from 1 to 3).
    ///
    /// Authentication is required, and the permission to manage the segments.
    async fn delete_segments(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: usize,
        segments_count: u32,
    ) -> Result<(), IggyError>;
}
