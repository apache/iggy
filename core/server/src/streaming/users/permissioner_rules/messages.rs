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
        if self
            .users_that_can_poll_messages_from_all_streams
            .contains(&user_id)
        {
            return Ok(());
        }

        if self
            .users_that_can_poll_messages_from_specific_streams
            .contains(&(user_id, stream_id))
        {
            return Ok(());
        }

        let stream_permissions = self.users_streams_permissions.get(&(user_id, stream_id));
        if stream_permissions.is_none() {
            return Err(IggyError::Unauthorized);
        }

        let stream_permissions = stream_permissions.unwrap();
        if stream_permissions.read_stream {
            return Ok(());
        }

        if stream_permissions.manage_topics {
            return Ok(());
        }

        if stream_permissions.read_topics {
            return Ok(());
        }

        if stream_permissions.poll_messages {
            return Ok(());
        }

        if stream_permissions.topics.is_none() {
            return Err(IggyError::Unauthorized);
        }

        let topic_permissions = stream_permissions.topics.as_ref().unwrap();
        if let Some(topic_permissions) = topic_permissions.get(&topic_id) {
            return match topic_permissions.poll_messages
                | topic_permissions.read_topic
                | topic_permissions.manage_topic
            {
                true => Ok(()),
                false => Err(IggyError::Unauthorized),
            };
        }

        Err(IggyError::Unauthorized)
    }

    pub fn append_messages(
        &self,
        user_id: u32,
        stream_id: usize,
        topic_id: usize,
    ) -> Result<(), IggyError> {
        if self
            .users_that_can_send_messages_to_all_streams
            .contains(&user_id)
        {
            return Ok(());
        }

        if self
            .users_that_can_send_messages_to_specific_streams
            .contains(&(user_id, stream_id))
        {
            return Ok(());
        }

        let stream_permissions = self.users_streams_permissions.get(&(user_id, stream_id));
        if stream_permissions.is_none() {
            return Err(IggyError::Unauthorized);
        }

        let stream_permissions = stream_permissions.unwrap();
        if stream_permissions.manage_stream {
            return Ok(());
        }

        if stream_permissions.manage_topics {
            return Ok(());
        }

        if stream_permissions.send_messages {
            return Ok(());
        }

        if stream_permissions.topics.is_none() {
            return Err(IggyError::Unauthorized);
        }

        let topic_permissions = stream_permissions.topics.as_ref().unwrap();
        if let Some(topic_permissions) = topic_permissions.get(&topic_id) {
            return match topic_permissions.send_messages | topic_permissions.manage_topic {
                true => Ok(()),
                false => Err(IggyError::Unauthorized),
            };
        }

        Err(IggyError::Unauthorized)
    }
}
