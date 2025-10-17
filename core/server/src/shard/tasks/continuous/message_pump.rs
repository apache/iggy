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

use crate::shard::IggyShard;
use crate::shard::task_registry::ShutdownToken;
use crate::shard::transmission::frame::ShardFrame;
use crate::{shard_debug, shard_info};
use futures::{FutureExt, StreamExt};
use std::rc::Rc;

pub fn spawn_message_pump(shard: Rc<IggyShard>) {
    let shard_clone = shard.clone();
    shard
        .task_registry
        .continuous("message_pump")
        .critical(true)
        .run(move |shutdown| message_pump(shard_clone, shutdown))
        .spawn();
}

async fn message_pump(
    shard: Rc<IggyShard>,
    shutdown: ShutdownToken,
) -> Result<(), iggy_common::IggyError> {
    let Some(mut messages_receiver) = shard.messages_receiver.take() else {
        shard_info!(shard.id, "Message receiver already taken; pump not started");
        return Ok(());
    };

    shard_info!(shard.id, "Starting message passing task");

    loop {
        futures::select! {
            _ = shutdown.wait().fuse() => {
                shard_debug!(shard.id, "Message receiver shutting down");
                break;
            }
            frame = messages_receiver.next().fuse() => {
                match frame {
                    Some(ShardFrame { message, response_sender }) => {
                        if let (Some(response), Some(tx)) =
                            (shard.handle_shard_message(message).await, response_sender)
                        {
                             let _ = tx.send(response).await;
                        }
                    }
                    None => {
                        shard_debug!(shard.id, "Message receiver closed; exiting pump");
                        break;
                    }
                }
            }
        }
    }

    // Explicitly drop the receiver to ensure any registered wakers in the
    // underlying AtomicWaker are cleared on the correct thread before this
    // task completes. This prevents cross-thread SendWrapper drops during
    // shard shutdown.
    drop(messages_receiver);

    Ok(())
}
