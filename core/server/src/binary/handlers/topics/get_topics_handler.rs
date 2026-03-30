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

use crate::binary::dispatch::{HandlerResult, wire_id_to_identifier};
use crate::binary::handlers::streams::get_stream_handler::build_topic_header;
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use iggy_binary_protocol::codec::WireEncode;
use iggy_binary_protocol::requests::topics::GetTopicsRequest;
use iggy_binary_protocol::responses::topics::get_topics::GetTopicsResponse;
use iggy_common::{IggyError, SenderKind};
use std::rc::Rc;
use tracing::debug;

pub async fn handle_get_topics(
    req: GetTopicsRequest,
    sender: &mut SenderKind,
    session: &Session,
    shard: &Rc<IggyShard>,
) -> Result<HandlerResult, IggyError> {
    let stream_id = wire_id_to_identifier(&req.stream_id)?;
    debug!("session: {session}, command: get_topics, stream_id: {stream_id}");
    shard.ensure_authenticated(session)?;

    let Some(topics) = shard
        .metadata
        .query_topics(session.get_user_id(), &stream_id)?
    else {
        sender.send_empty_ok_response().await?;
        return Ok(HandlerResult::Finished);
    };

    let mut sorted: Vec<_> = topics.iter().collect();
    sorted.sort_by_key(|t| t.id);

    let mut wire_topics = Vec::with_capacity(sorted.len());
    for topic in sorted {
        wire_topics.push(build_topic_header(topic)?);
    }

    let response = GetTopicsResponse {
        topics: wire_topics,
    };
    sender.send_ok_response(&response.to_bytes()).await?;
    Ok(HandlerResult::Finished)
}
