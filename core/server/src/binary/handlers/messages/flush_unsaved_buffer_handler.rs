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
use crate::binary::handlers::messages::COMPONENT;
use crate::shard::IggyShard;
use crate::shard::transmission::message::ResolvedPartition;
use crate::streaming::session::Session;
use err_trail::ErrContext;
use iggy_binary_protocol::requests::messages::FlushUnsavedBufferRequest;
use iggy_common::{IggyError, SenderKind};
use std::rc::Rc;
use tracing::{debug, instrument};

#[instrument(skip_all, name = "trace_flush_unsaved_buffer", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_partition_id = req.partition_id, iggy_fsync = req.fsync))]
pub async fn handle_flush_unsaved_buffer(
    req: FlushUnsavedBufferRequest,
    sender: &mut SenderKind,
    session: &Session,
    shard: &Rc<IggyShard>,
) -> Result<HandlerResult, IggyError> {
    let stream_id = wire_id_to_identifier(&req.stream_id)?;
    let topic_id = wire_id_to_identifier(&req.topic_id)?;
    let partition_id = req.partition_id;
    let fsync = req.fsync;
    debug!(
        "session: {session}, command: flush_unsaved_buffer, stream_id: {stream_id}, topic_id: {topic_id}, partition_id: {partition_id}, fsync: {fsync}"
    );
    shard.ensure_authenticated(session)?;

    let user_id = session.get_user_id();
    let topic = shard.resolve_topic(&stream_id, &topic_id)?;
    let partition = ResolvedPartition {
        stream_id: topic.stream_id,
        topic_id: topic.topic_id,
        partition_id: partition_id as usize,
    };

    shard
        .flush_unsaved_buffer(user_id, partition, fsync)
        .await
        .error(|e: &IggyError| {
            format!(
                "{COMPONENT} (error: {e}) - failed to flush unsaved buffer for stream_id: {}, topic_id: {}, partition_id: {}, session: {}",
                stream_id, topic_id, partition_id, session
            )
        })?;
    sender.send_empty_ok_response().await?;
    Ok(HandlerResult::Finished)
}
