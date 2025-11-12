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
use futures::FutureExt;
use std::rc::Rc;
use tracing::{debug, info};

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
    let Some(messages_receiver) = shard.messages_receiver.take() else {
        info!("Message receiver already taken; pump not started");
        return Ok(());
    };

    info!("Starting message passing task");

    // Get the inner flume receiver directly
    let receiver = messages_receiver.inner;

    loop {
        futures::select! {
            _ = shutdown.wait().fuse() => {
                debug!("Message receiver shutting down");
                break;
            }
            frame = receiver.recv_async().fuse() => {
                match frame {
                    Ok(ShardFrame { message, response_sender }) => {
                        if let (Some(response), Some(tx)) =
                            (shard.handle_shard_message(message).await, response_sender)
                        {
                             let _ = tx.send(response).await;
                        }
                    }
                    Err(_) => {
                        debug!("Message receiver closed; exiting pump");
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}
