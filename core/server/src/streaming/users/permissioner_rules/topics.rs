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
    pub fn get_topic(
        &self,
        user_id: u32,
        stream_id: usize,
        topic_id: usize,
    ) -> Result<(), IggyError> {
        if let Some(global_permissions) = self.users_permissions.get(&user_id)
            && (global_permissions.read_streams
                || global_permissions.manage_streams
                || global_permissions.manage_topics
                || global_permissions.read_topics)
        {
            return Ok(());
        }

        if let Some(stream_permissions) = self.users_streams_permissions.get(&(user_id, stream_id))
        {
            if stream_permissions.manage_topics || stream_permissions.read_topics {
                return Ok(());
            }

            if let Some(topic_permissions) =
                stream_permissions.topics.as_ref().unwrap().get(&topic_id)
                && (topic_permissions.manage_topic || topic_permissions.read_topic)
            {
                return Ok(());
            }
        }

        Err(IggyError::Unauthorized)
    }

    pub fn get_topics(&self, user_id: u32, stream_id: usize) -> Result<(), IggyError> {
        if let Some(global_permissions) = self.users_permissions.get(&user_id)
            && (global_permissions.read_streams
                || global_permissions.manage_streams
                || global_permissions.manage_topics
                || global_permissions.read_topics)
        {
            return Ok(());
        }

        if let Some(stream_permissions) = self.users_streams_permissions.get(&(user_id, stream_id))
        {
            if stream_permissions.manage_topics || stream_permissions.read_topics {
                return Ok(());
            }

            if let Some(topic_permissions) =
                stream_permissions.topics.as_ref().unwrap().get(&stream_id)
                && (topic_permissions.manage_topic || topic_permissions.read_topic)
            {
                return Ok(());
            }
        }

        Err(IggyError::Unauthorized)
    }

    pub fn create_topic(&self, user_id: u32, stream_id: usize) -> Result<(), IggyError> {
        if let Some(global_permissions) = self.users_permissions.get(&user_id)
            && (global_permissions.manage_streams || global_permissions.manage_topics)
        {
            return Ok(());
        }

        if let Some(stream_permissions) = self.users_streams_permissions.get(&(user_id, stream_id))
            && stream_permissions.manage_topics
        {
            return Ok(());
        }

        Err(IggyError::Unauthorized)
    }

    pub fn update_topic(
        &self,
        user_id: u32,
        stream_id: usize,
        topic_id: usize,
    ) -> Result<(), IggyError> {
        self.manage_topic(user_id, stream_id, topic_id)
    }

    pub fn delete_topic(
        &self,
        user_id: u32,
        stream_id: usize,
        topic_id: usize,
    ) -> Result<(), IggyError> {
        self.manage_topic(user_id, stream_id, topic_id)
    }

    pub fn purge_topic(
        &self,
        user_id: u32,
        stream_id: usize,
        topic_id: usize,
    ) -> Result<(), IggyError> {
        self.manage_topic(user_id, stream_id, topic_id)
    }

    fn manage_topic(
        &self,
        user_id: u32,
        stream_id: usize,
        topic_id: usize,
    ) -> Result<(), IggyError> {
        if let Some(global_permissions) = self.users_permissions.get(&user_id)
            && (global_permissions.manage_streams || global_permissions.manage_topics)
        {
            return Ok(());
        }

        if let Some(stream_permissions) = self.users_streams_permissions.get(&(user_id, stream_id))
        {
            if stream_permissions.manage_topics {
                return Ok(());
            }

            if let Some(topic_permissions) =
                stream_permissions.topics.as_ref().unwrap().get(&topic_id)
                && topic_permissions.manage_topic
            {
                return Ok(());
            }
        }

        Err(IggyError::Unauthorized)
    }
}
