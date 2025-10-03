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
use iggy_common::{
    Consumer, Identifier, IggyError, IggyMessage, Partitioning, PolledMessages, PollingStrategy,
};

/// This trait defines the methods to interact with the messaging module.
#[async_trait]
pub trait MessageClient {
    /// Poll given amount of messages using the specified consumer and strategy from the specified stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to poll the messages.
    #[allow(clippy::too_many_arguments)]
    async fn poll_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<usize>,
        consumer: &Consumer,
        strategy: &PollingStrategy,
        count: u32,
        auto_commit: bool,
    ) -> Result<PolledMessages, IggyError>;

    /// Send messages using specified partitioning strategy to the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to send the messages.
    async fn send_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning: &Partitioning,
        messages: &mut [IggyMessage],
    ) -> Result<(), IggyError>;

    /// Force flush of the `unsaved_messages` buffer to disk, optionally fsyncing the data.
    #[allow(clippy::too_many_arguments)]
    async fn flush_unsaved_buffer(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: usize,
        fsync: bool,
    ) -> Result<(), IggyError>;
}
