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

use crate::state::system::StreamState;
use crate::streaming::streams::COMPONENT;
use crate::streaming::streams::stream::Stream;
use error_set::ErrContext;
use iggy_common::IggyError;

impl Stream {
    pub async fn load(&mut self, state: StreamState) -> Result<(), IggyError> {
        let storage = self.storage.clone();
        let state_id = state.id;
        storage.stream
            .load(self, state)
            .await
            .with_error_context(|error| format!("{COMPONENT} (error: {error}) - failed to load stream with state, state ID: {state_id}, stream: {self}"))
    }

    pub async fn persist(&self) -> Result<(), IggyError> {
        self.storage
            .stream
            .save(self)
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to persist stream: {self}")
            })
    }

    pub async fn delete(&self) -> Result<(), IggyError> {
        self.storage
            .stream
            .delete(self)
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to delete stream: {self}")
            })
    }

    pub async fn persist_messages(&self) -> Result<usize, IggyError> {
        let mut saved_messages_number = 0;
        for topic in self.get_topics() {
            saved_messages_number += topic.persist_messages().await.with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to persist messages for topic: {topic} in stream: {self}"
                )
            })?;
        }

        Ok(saved_messages_number)
    }
}
