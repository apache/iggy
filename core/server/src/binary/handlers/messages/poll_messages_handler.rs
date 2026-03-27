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

use crate::binary::dispatch::{
    HandlerResult, wire_consumer_to_consumer, wire_id_to_identifier, wire_polling_to_strategy,
};
use crate::shard::IggyShard;
use crate::shard::system::messages::PollingArgs;
use crate::streaming::session::Session;
use iggy_binary_protocol::requests::messages::PollMessagesRequest;
use iggy_common::SenderKind;
use iggy_common::{IggyError, PooledBuffer};
use std::rc::Rc;
use tracing::{debug, trace};

pub async fn handle_poll_messages(
    req: PollMessagesRequest,
    sender: &mut SenderKind,
    session: &Session,
    shard: &Rc<IggyShard>,
) -> Result<HandlerResult, IggyError> {
    let consumer = wire_consumer_to_consumer(&req.consumer)?;
    let stream_id = wire_id_to_identifier(&req.stream_id)?;
    let topic_id = wire_id_to_identifier(&req.topic_id)?;
    let strategy = wire_polling_to_strategy(&req.strategy)?;
    let partition_id = req.partition_id;
    let count = req.count;
    let auto_commit = req.auto_commit;

    debug!(
        "session: {session}, command: poll_messages, stream_id: {stream_id}, topic_id: {topic_id}, partition_id: {partition_id:?}"
    );
    shard.ensure_authenticated(session)?;

    let args = PollingArgs::new(strategy, count, auto_commit);

    let user_id = session.get_user_id();
    let client_id = session.client_id;
    let topic = shard.resolve_topic_for_poll(user_id, &stream_id, &topic_id)?;
    let (metadata, mut batch) = shard
        .poll_messages(client_id, topic, consumer, partition_id, args)
        .await?;

    let response_length = 4 + 8 + 4 + batch.size();
    let response_length_bytes = response_length.to_le_bytes();

    let mut bufs = Vec::with_capacity(batch.containers_count() + 3);
    let mut partition_id_buf = PooledBuffer::with_capacity(4);
    let mut current_offset_buf = PooledBuffer::with_capacity(8);
    let mut count_buf = PooledBuffer::with_capacity(4);
    partition_id_buf.put_u32_le(metadata.partition_id);
    current_offset_buf.put_u64_le(metadata.current_offset);
    count_buf.put_u32_le(batch.count());

    bufs.push(partition_id_buf);
    bufs.push(current_offset_buf);
    bufs.push(count_buf);

    batch.iter_mut().for_each(|m| {
        bufs.push(m.take_messages());
    });
    trace!(
        "Sending {} messages to client ({} bytes) to client",
        batch.count(),
        response_length
    );

    sender
        .send_ok_response_vectored(&response_length_bytes, bufs)
        .await?;
    Ok(HandlerResult::Finished)
}
