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

use crate::metadata::Metadata;
use crate::streaming::utils::ptr::EternalPtr;
use iggy_common::{Permissions, UserId};

/// Permissioner reads permissions directly from SharedMetadata.
/// No caching - SharedMetadata is the single source of truth.
pub struct Permissioner {
    shared_metadata: EternalPtr<Metadata>,
}

impl std::fmt::Debug for Permissioner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Permissioner")
            .field("shared_metadata", &"<SharedMetadata>")
            .finish()
    }
}

impl Permissioner {
    pub fn new(shared_metadata: EternalPtr<Metadata>) -> Self {
        Self { shared_metadata }
    }

    /// Get permissions for a user from SharedMetadata.
    /// Returns None if user doesn't exist or has no permissions.
    pub(super) fn get_permissions(&self, user_id: UserId) -> Option<Permissions> {
        let metadata = self.shared_metadata.load();
        metadata
            .users
            .get(user_id as usize)
            .and_then(|user| user.permissions.as_ref().map(|p| (**p).clone()))
    }

    /// Check if user has global permission to poll messages from all streams.
    pub(super) fn can_poll_messages_globally(&self, user_id: UserId) -> bool {
        self.get_permissions(user_id)
            .map(|p| p.global.poll_messages)
            .unwrap_or(false)
    }

    /// Check if user has global permission to send messages to all streams.
    pub(super) fn can_send_messages_globally(&self, user_id: UserId) -> bool {
        self.get_permissions(user_id)
            .map(|p| p.global.send_messages)
            .unwrap_or(false)
    }

    /// Get stream-specific permissions for a user.
    pub(super) fn get_stream_permissions(
        &self,
        user_id: UserId,
        stream_id: usize,
    ) -> Option<iggy_common::StreamPermissions> {
        self.get_permissions(user_id)
            .and_then(|p| p.streams)
            .and_then(|streams| streams.get(&stream_id).cloned())
    }
}
