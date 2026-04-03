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

use super::get_stream_handler::compute_stream_stats;
use crate::binary::dispatch::HandlerResult;
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use iggy_binary_protocol::WireName;
use iggy_binary_protocol::codec::WireEncode;
use iggy_binary_protocol::responses::streams::StreamResponse;
use iggy_binary_protocol::responses::streams::get_streams::GetStreamsResponse;
use iggy_common::IggyError;
use iggy_common::SenderKind;
use std::rc::Rc;
use tracing::debug;

pub async fn handle_get_streams(
    sender: &mut SenderKind,
    session: &Session,
    shard: &Rc<IggyShard>,
) -> Result<HandlerResult, IggyError> {
    debug!("session: {session}, command: get_streams");
    shard.ensure_authenticated(session)?;

    let streams = shard.metadata.query_streams(session.get_user_id())?;

    let mut sorted: Vec<_> = streams.iter().collect();
    sorted.sort_by_key(|s| s.id);

    let mut wire_streams = Vec::with_capacity(sorted.len());
    for stream in sorted {
        let (total_size, total_messages) = compute_stream_stats(stream);
        wire_streams.push(StreamResponse {
            id: stream.id as u32,
            created_at: stream.created_at.into(),
            topics_count: stream.topics.len() as u32,
            size_bytes: total_size,
            messages_count: total_messages,
            name: WireName::new(stream.name.as_ref()).map_err(|_| IggyError::InvalidCommand)?,
        });
    }

    let response = GetStreamsResponse {
        streams: wire_streams,
    };
    sender.send_ok_response(&response.to_bytes()).await?;
    Ok(HandlerResult::Finished)
}
