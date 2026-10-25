// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::client_wrappers::client_wrapper::ClientWrapper;
use async_dropper::AsyncDrop;
use async_trait::async_trait;
use iggy_common::{
    ConsumerGroup, ConsumerGroupClient, ConsumerGroupDetails, Identifier, IggyError, UserClient,
};

#[async_trait]
impl ConsumerGroupClient for ClientWrapper {
    async fn get_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<Option<ConsumerGroupDetails>, IggyError> {
        super::dispatch_client!(
            self,
            ConsumerGroupClient::get_consumer_group(stream_id, topic_id, group_id)
        )
        .await
    }

    async fn get_consumer_groups(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<Vec<ConsumerGroup>, IggyError> {
        super::dispatch_client!(
            self,
            ConsumerGroupClient::get_consumer_groups(stream_id, topic_id)
        )
        .await
    }

    async fn create_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: &str,
    ) -> Result<ConsumerGroupDetails, IggyError> {
        super::dispatch_client!(
            self,
            ConsumerGroupClient::create_consumer_group(stream_id, topic_id, name)
        )
        .await
    }

    async fn delete_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError> {
        super::dispatch_client!(
            self,
            ConsumerGroupClient::delete_consumer_group(stream_id, topic_id, group_id)
        )
        .await
    }

    async fn join_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError> {
        super::dispatch_client!(
            self,
            ConsumerGroupClient::join_consumer_group(stream_id, topic_id, group_id)
        )
        .await
    }

    async fn leave_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError> {
        super::dispatch_client!(
            self,
            ConsumerGroupClient::leave_consumer_group(stream_id, topic_id, group_id)
        )
        .await
    }
}

#[async_trait]
impl AsyncDrop for ClientWrapper {
    async fn async_drop(&mut self) {
        let _ = super::dispatch_client!(self, UserClient::logout_user()).await;
    }
}
