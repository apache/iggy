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
use crate::streaming::session::Session;
use iggy_binary_protocol::codec::WireEncode;
use iggy_binary_protocol::responses::system::get_cluster_metadata::{
    ClusterMetadataResponse, ClusterNodeResponse,
};
use iggy_common::{IggyError, SenderKind};
use std::rc::Rc;
use tracing::{debug, instrument};

#[instrument(skip_all, name = "trace_get_cluster_metadata", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
pub async fn handle_get_cluster_metadata(
    sender: &mut SenderKind,
    session: &Session,
    shard: &Rc<IggyShard>,
) -> Result<HandlerResult, IggyError> {
    debug!("session: {session}, command: get_cluster_metadata");
    shard.ensure_authenticated(session)?;

    let cluster_metadata = shard.get_cluster_metadata();

    let response = ClusterMetadataResponse {
        name: cluster_metadata.name,
        nodes: cluster_metadata
            .nodes
            .into_iter()
            .map(|node| ClusterNodeResponse {
                name: node.name,
                ip: node.ip,
                tcp_port: node.endpoints.tcp,
                quic_port: node.endpoints.quic,
                http_port: node.endpoints.http,
                websocket_port: node.endpoints.websocket,
                role: node.role as u8,
                status: node.status as u8,
            })
            .collect(),
    };
    sender.send_ok_response(&response.to_bytes()).await?;
    Ok(HandlerResult::Finished)
}
