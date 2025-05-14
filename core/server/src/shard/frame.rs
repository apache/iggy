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
use bytes::Bytes;
use flume::Sender;
use iggy_common::IggyError;

#[derive(Debug, Clone)]
pub enum ShardMessage {
    //TODO: Fixme
    //Command(ServerCommand),
    Event(ShardEvent),
}

#[derive(Debug, Clone)]
pub enum ShardEvent {}

#[derive(Debug)]
pub enum ShardResponse {
    BinaryResponse(Bytes),
    ErrorResponse(IggyError),
}

#[derive(Debug, Clone)]
pub struct ShardFrame {
    pub client_id: u32,
    pub message: ShardMessage,
    pub response_sender: Option<Sender<ShardResponse>>,
}

impl ShardFrame {
    pub fn new(
        client_id: u32,
        message: ShardMessage,
        response_sender: Option<Sender<ShardResponse>>,
    ) -> Self {
        Self {
            client_id,
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
