use std::{rc::Rc, sync::Arc};

use iggy_common::PollingStrategy;

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
use crate::{
    shard::{system::messages::PollingArgs, transmission::event::ShardEvent},
    streaming::{polling_consumer::PollingConsumer, segments::IggyMessagesBatchMut},
};

#[derive(Debug)]
pub enum ShardMessage {
    Request(ShardRequest),
    Event(Arc<ShardEvent>),
}

#[derive(Debug)]
pub struct ShardRequest {
    pub stream_id: u32,
    pub topic_id: u32,
    pub partition_id: u32,
    pub payload: ShardRequestPayload,
}

impl ShardRequest {
    pub fn new(
        stream_id: u32,
        topic_id: u32,
        partition_id: u32,
        payload: ShardRequestPayload,
    ) -> Self {
        Self {
            stream_id,
            topic_id,
            partition_id,
            payload,
        }
    }
}

#[derive(Debug)]
pub enum ShardRequestPayload {
    SendMessages {
        batch: IggyMessagesBatchMut,
    },
    PollMessages {
        args: PollingArgs,
        consumer: PollingConsumer,
        count: u32,
    },
}

impl From<ShardRequest> for ShardMessage {
    fn from(request: ShardRequest) -> Self {
        ShardMessage::Request(request)
    }
}

impl From<Arc<ShardEvent>> for ShardMessage {
    fn from(event: Arc<ShardEvent>) -> Self {
        ShardMessage::Event(event)
    }
}
