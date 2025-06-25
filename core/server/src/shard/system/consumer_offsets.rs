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

use crate::streaming::session::Session;
use error_set::ErrContext;
use iggy_common::{Consumer, ConsumerOffsetInfo, Identifier, IggyError};

impl IggyShard {
    pub async fn store_consumer_offset(
        &self,
        session: &Session,
        consumer: Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
        offset: u64,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        let topic = self.find_topic(session, stream_id, topic_id)
            .with_error_context(|error| format!("{COMPONENT} (error: {error}) - topic with ID: {topic_id} was not found in stream with ID: {stream_id}"))?;
        self.permissioner.store_consumer_offset(
            session.get_user_id(),
            topic.stream_id,
            topic.topic_id,
        )?;

        topic
            .store_consumer_offset(consumer, offset, partition_id, session.client_id)
            .await
    }

    pub async fn get_consumer_offset(
        &self,
        session: &Session,
        consumer: &Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
    ) -> Result<Option<ConsumerOffsetInfo>, IggyError> {
        self.ensure_authenticated(session)?;
        let Some(topic) = self.try_find_topic(session, stream_id, topic_id)? else {
            return Ok(None);
        };

        self.permissioner.get_consumer_offset(
            session.get_user_id(),
            topic.stream_id,
            topic.topic_id,
        ).with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - permission denied to get consumer offset for user with ID: {}, consumer: {consumer} in topic with ID: {topic_id} and stream with ID: {stream_id}",
                session.get_user_id(),
            )
        })?;

        topic
            .get_consumer_offset(consumer, partition_id, session.client_id)
            .await
    }

    pub async fn delete_consumer_offset(
        &self,
        session: &Session,
        consumer: Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        let topic = self.find_topic(session, stream_id, topic_id)
            .with_error_context(|error| format!("{COMPONENT} (error: {error}) - topic with ID: {topic_id} was not found in stream with ID: {stream_id}"))?;
        self.permissioner.delete_consumer_offset(
            session.get_user_id(),
            topic.stream_id,
            topic.topic_id,
        ).with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - permission denied to delete consumer offset for user with ID: {}, consumer: {consumer} in topic with ID: {topic_id} and stream with ID: {stream_id}",
                session.get_user_id(),
            )
        })?;

        topic
            .delete_consumer_offset(consumer, partition_id, session.client_id)
            .await
    }
}
