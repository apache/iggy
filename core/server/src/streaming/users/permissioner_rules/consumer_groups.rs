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
    pub fn create_consumer_group(
        &self,
        user_id: u32,
        stream_id: usize,
        topic_id: usize,
    ) -> Result<(), IggyError> {
        self.get_topic(user_id, stream_id, topic_id)
    }

    pub fn delete_consumer_group(
        &self,
        user_id: u32,
        stream_id: usize,
        topic_id: usize,
    ) -> Result<(), IggyError> {
        self.get_topic(user_id, stream_id, topic_id)
    }

    pub fn get_consumer_group(
        &self,
        user_id: u32,
        stream_id: usize,
        topic_id: usize,
    ) -> Result<(), IggyError> {
        self.get_topic(user_id, stream_id, topic_id)
    }

    pub fn get_consumer_groups(
        &self,
        user_id: u32,
        stream_id: usize,
        topic_id: usize,
    ) -> Result<(), IggyError> {
        self.get_topic(user_id, stream_id, topic_id)
    }

    pub fn join_consumer_group(
        &self,
        user_id: u32,
        stream_id: usize,
        topic_id: usize,
    ) -> Result<(), IggyError> {
        self.get_topic(user_id, stream_id, topic_id)
    }

    pub fn leave_consumer_group(
        &self,
        user_id: u32,
        stream_id: usize,
        topic_id: usize,
    ) -> Result<(), IggyError> {
        self.get_topic(user_id, stream_id, topic_id)
    }
}
