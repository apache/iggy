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

use crate::metadata::SharedMetadata;
use crate::streaming::utils::ptr::EternalPtr;
use iggy_common::UserId;
use iggy_common::{GlobalPermissions, StreamPermissions, TopicPermissions};

/// Permissioner reads permissions directly from SharedMetadata.
///
/// This eliminates the need for per-shard permission caches and
/// ensures all shards see permission updates immediately.
pub struct Permissioner {
    shared_metadata: EternalPtr<SharedMetadata>,
}

impl Permissioner {
    pub fn new(shared_metadata: EternalPtr<SharedMetadata>) -> Self {
        Self { shared_metadata }
    }

    /// Get global permissions for a user from SharedMetadata.
    pub(super) fn get_global_permissions(&self, user_id: UserId) -> Option<GlobalPermissions> {
        let metadata = self.shared_metadata.load();
        metadata
            .users
            .get(&user_id)
            .and_then(|user| user.permissions.as_ref().map(|p| p.global.clone()))
    }

    /// Get stream-specific permissions for a user from SharedMetadata.
    pub(super) fn get_stream_permissions(
        &self,
        user_id: UserId,
        stream_id: usize,
    ) -> Option<StreamPermissions> {
        let metadata = self.shared_metadata.load();
        metadata
            .users
            .get(&user_id)
            .and_then(|user| user.permissions.as_ref())
            .and_then(|p| p.streams.as_ref())
            .and_then(|streams| streams.get(&stream_id).cloned())
    }

    /// Get topic-specific permissions for a user from SharedMetadata.
    pub(super) fn get_topic_permissions(
        &self,
        user_id: UserId,
        stream_id: usize,
        topic_id: usize,
    ) -> Option<TopicPermissions> {
        self.get_stream_permissions(user_id, stream_id)
            .and_then(|sp| sp.topics)
            .and_then(|topics| topics.get(&topic_id).cloned())
    }
}
