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

use crate::streaming::users::permissioner::Permissioner;
use iggy_common::IggyError;

impl Permissioner {
    pub fn poll_messages(
        &self,
        user_id: u32,
        stream_id: usize,
        topic_id: usize,
    ) -> Result<(), IggyError> {
        // Check global poll_messages permission
        if let Some(global_permissions) = self.get_global_permissions(user_id)
            && global_permissions.poll_messages
        {
            return Ok(());
        }

        // Check stream-specific permissions
        let Some(stream_permissions) = self.get_stream_permissions(user_id, stream_id) else {
            return Err(IggyError::Unauthorized);
        };

        if stream_permissions.poll_messages {
            return Ok(());
        }

        if stream_permissions.read_stream {
            return Ok(());
        }

        if stream_permissions.manage_topics {
            return Ok(());
        }

        if stream_permissions.read_topics {
            return Ok(());
        }

        // Check topic-specific permissions
        if let Some(topic_permissions) = self.get_topic_permissions(user_id, stream_id, topic_id) {
            if topic_permissions.poll_messages
                || topic_permissions.read_topic
                || topic_permissions.manage_topic
            {
                return Ok(());
            }
        }

        Err(IggyError::Unauthorized)
    }

    pub fn append_messages(
        &self,
        user_id: u32,
        stream_id: usize,
        topic_id: usize,
    ) -> Result<(), IggyError> {
        // Check global send_messages permission
        if let Some(global_permissions) = self.get_global_permissions(user_id)
            && global_permissions.send_messages
        {
            return Ok(());
        }

        // Check stream-specific permissions
        let Some(stream_permissions) = self.get_stream_permissions(user_id, stream_id) else {
            return Err(IggyError::Unauthorized);
        };

        if stream_permissions.send_messages {
            return Ok(());
        }

        if stream_permissions.manage_stream {
            return Ok(());
        }

        if stream_permissions.manage_topics {
            return Ok(());
        }

        // Check topic-specific permissions
        if let Some(topic_permissions) = self.get_topic_permissions(user_id, stream_id, topic_id) {
            if topic_permissions.send_messages || topic_permissions.manage_topic {
                return Ok(());
            }
        }

        Err(IggyError::Unauthorized)
    }
}
