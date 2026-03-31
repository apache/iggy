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

use std::rc::Rc;

use crate::binary::dispatch::{HandlerResult, wire_consumer_to_consumer, wire_id_to_identifier};
use crate::binary::handlers::consumer_offsets::COMPONENT;
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use err_trail::ErrContext;
use iggy_binary_protocol::requests::consumer_offsets::StoreConsumerOffsetRequest;
use iggy_common::IggyError;
use iggy_common::SenderKind;
use tracing::debug;

pub async fn handle_store_consumer_offset(
    req: StoreConsumerOffsetRequest,
    sender: &mut SenderKind,
    session: &Session,
    shard: &Rc<IggyShard>,
) -> Result<HandlerResult, IggyError> {
    let consumer = wire_consumer_to_consumer(&req.consumer)?;
    let stream_id = wire_id_to_identifier(&req.stream_id)?;
    let topic_id = wire_id_to_identifier(&req.topic_id)?;
    debug!(
        "session: {session}, command: store_consumer_offset, stream_id: {stream_id}, topic_id: {topic_id}, partition_id: {:?}, offset: {}",
        req.partition_id, req.offset
    );
    shard.ensure_authenticated(session)?;
    let topic = shard.resolve_topic_for_store_consumer_offset(
        session.get_user_id(),
        &stream_id,
        &topic_id,
    )?;
    shard
        .store_consumer_offset(
            session.client_id,
            consumer,
            topic,
            req.partition_id,
            req.offset,
        )
        .await
        .error(|e: &IggyError| format!("{COMPONENT} (error: {e}) - failed to store consumer offset for stream_id: {}, topic_id: {}, partition_id: {:?}, offset: {}, session: {}",
            stream_id, topic_id, req.partition_id, req.offset, session
        ))?;
    sender.send_empty_ok_response().await?;
    Ok(HandlerResult::Finished)
}
