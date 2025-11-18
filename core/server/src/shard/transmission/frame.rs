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
use async_channel::Sender;
use iggy_common::{IggyError, Stats};

use crate::{
    binary::handlers::messages::poll_messages_handler::IggyPollMetadata,
    shard::transmission::message::ShardMessage,
    streaming::{
        segments::IggyMessagesBatchSet, streams::stream, topics::topic, users::user::User,
    },
};

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum ShardResponse {
    PollMessages((IggyPollMetadata, IggyMessagesBatchSet)),
    SendMessages,
    FlushUnsavedBuffer,
    DeleteSegments,
    Event,
    CreateStreamResponse(stream::Stream),
    DeleteStreamResponse(stream::Stream),
    CreateTopicResponse(topic::Topic),
    UpdateTopicResponse,
    DeleteTopicResponse(topic::Topic),
    CreateUserResponse(User),
    DeletedUser(User),
    GetStatsResponse(Stats),
    ErrorResponse(IggyError),
}

#[derive(Debug)]
pub struct ShardFrame {
    pub message: ShardMessage,
    pub response_sender: Option<Sender<ShardResponse>>,
}

impl ShardFrame {
    pub fn new(message: ShardMessage, response_sender: Option<Sender<ShardResponse>>) -> Self {
        Self {
            message,
            response_sender,
        }
    }
}

#[macro_export]
macro_rules! handle_response {
    ($sender:expr, $response:expr) => {
        match $response {
            ShardResponse::BinaryResponse(payload) => $sender.send_ok_response(&payload).await?,
            ShardResponse::ErrorResponse(err) => $sender.send_error_response(err).await?,
        }
    };
}
