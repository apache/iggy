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
use iggy_binary_protocol::PartitionClient;
use iggy_common::locking::IggyRwLockFn;
use iggy_common::{Identifier, IggyError};

#[async_trait]
impl PartitionClient for IggyClient {
    async fn create_partitions(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .create_partitions(stream_id, topic_id, partitions_count)
            .await
    }

    async fn delete_partitions(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .delete_partitions(stream_id, topic_id, partitions_count)
            .await
    }
}
