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

use crate::binary::dispatch::HandlerResult;
use crate::shard::IggyShard;
use crate::streaming::clients::client_manager::Client;
use crate::streaming::session::Session;
use iggy_binary_protocol::codec::WireEncode;
use iggy_binary_protocol::responses::clients::{
    ClientDetailsResponse, ClientResponse, ConsumerGroupInfoResponse,
};
use iggy_common::IggyError;
use iggy_common::SenderKind;
use std::rc::Rc;

pub async fn handle_get_me(
    sender: &mut SenderKind,
    session: &Session,
    shard: &Rc<IggyShard>,
) -> Result<HandlerResult, IggyError> {
    shard.ensure_authenticated(session)?;
    let Some(client) = shard.get_client(session.client_id) else {
        return Err(IggyError::ClientNotFound(session.client_id));
    };

    let response = build_client_details_response(&client);
    sender.send_ok_response(&response.to_bytes()).await?;
    Ok(HandlerResult::Finished)
}

pub(crate) fn build_client_details_response(client: &Client) -> ClientDetailsResponse {
    ClientDetailsResponse {
        client: build_client_response(client),
        consumer_groups: client
            .consumer_groups
            .iter()
            .map(|cg| ConsumerGroupInfoResponse {
                stream_id: cg.stream_id,
                topic_id: cg.topic_id,
                group_id: cg.group_id,
            })
            .collect(),
    }
}

pub(crate) fn build_client_response(client: &Client) -> ClientResponse {
    ClientResponse {
        client_id: client.session.client_id,
        user_id: client.user_id.unwrap_or(u32::MAX),
        transport: transport_to_u8(&client.transport),
        address: client.session.ip_address.to_string(),
        consumer_groups_count: client.consumer_groups.len() as u32,
    }
}

pub(crate) fn transport_to_u8(transport: &iggy_common::TransportProtocol) -> u8 {
    match transport {
        iggy_common::TransportProtocol::Tcp => 1,
        iggy_common::TransportProtocol::Quic => 2,
        iggy_common::TransportProtocol::Http => 3,
        iggy_common::TransportProtocol::WebSocket => 4,
    }
}
