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

use crate::prelude::IggyClient;
use async_trait::async_trait;
use iggy_binary_protocol::ConsumerOffsetClient;
use iggy_common::locking::IggyRwLockFn;
use iggy_common::{Consumer, ConsumerOffsetInfo, Identifier, IggyError};

#[async_trait]
impl ConsumerOffsetClient for IggyClient {
    async fn store_consumer_offset(
        &self,
        consumer: &Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
        offset: u64,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .store_consumer_offset(consumer, stream_id, topic_id, partition_id, offset)
            .await
    }

    async fn get_consumer_offset(
        &self,
        consumer: &Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
    ) -> Result<Option<ConsumerOffsetInfo>, IggyError> {
        self.client
            .read()
            .await
            .get_consumer_offset(consumer, stream_id, topic_id, partition_id)
            .await
    }

    async fn delete_consumer_offset(
        &self,
        consumer: &Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .delete_consumer_offset(consumer, stream_id, topic_id, partition_id)
            .await
    }
}
