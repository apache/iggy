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

use crate::utils::auth::fail_if_not_authenticated;
use crate::{BinaryClient, PartitionClient};
use iggy_common::create_partitions::CreatePartitions;
use iggy_common::delete_partitions::DeletePartitions;
use iggy_common::{Identifier, IggyError};

#[async_trait::async_trait]
impl<B: BinaryClient> PartitionClient for B {
    async fn create_partitions(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: usize,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(&CreatePartitions {
            stream_id: stream_id.clone(),
            topic_id: topic_id.clone(),
            partitions_count,
        })
        .await?;
        Ok(())
    }

    async fn delete_partitions(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: usize,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(&DeletePartitions {
            stream_id: stream_id.clone(),
            topic_id: topic_id.clone(),
            partitions_count,
        })
        .await?;
        Ok(())
    }
}
