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

use crate::shard::task_registry::ShutdownToken;
use crate::shard::transmission::frame::ShardFrame;
use crate::shard::{IggyShard, handlers::handle_shard_message};
use futures::FutureExt;
use std::rc::Rc;
use tracing::{debug, error, info};

pub fn spawn_message_pump(shard: Rc<IggyShard>) {
    let shard_clone = shard.clone();
    shard
        .task_registry
        .continuous("message_pump")
        .critical(true)
        .run(move |shutdown| message_pump(shard_clone, shutdown))
        .spawn();
}

/// Single serialization point for all partition mutations on this shard.
///
/// Every operation that mutates `local_partitions` — appends, segment rotation, flush,
/// segment deletion — is dispatched exclusively through this pump. The loop awaits each
/// `process_frame` to completion before dequeuing the next message, so handlers never
/// interleave even across internal `.await` points (disk I/O, fsync).
///
/// Periodic tasks (message_saver, message_cleaner) run as separate futures on the same
/// compio thread but **cannot** mutate partitions directly. They read partition metadata
/// via `borrow()` and enqueue mutation requests back into this pump's channel. Those
/// requests block on a response that is only sent after the current frame completes,
/// guaranteeing strict ordering.
///
/// This invariant replaces per-partition write locks and eliminates TOCTOU races between
/// concurrent handlers. All `pub(crate)` mutation methods on `IggyShard` (e.g.
/// `append_messages_to_local_partition`, `delete_expired_segments`,
/// `rotate_segment_in_local_partitions`) assume they are called from within this pump.
async fn message_pump(
    shard: Rc<IggyShard>,
    shutdown: ShutdownToken,
) -> Result<(), iggy_common::IggyError> {
    let Some(messages_receiver) = shard.messages_receiver.take() else {
        info!("Message receiver already taken; pump not started");
        return Ok(());
    };

    info!("Starting message passing task");

    let receiver = messages_receiver.inner;

    loop {
        futures::select! {
            _ = shutdown.wait().fuse() => {
                debug!("Message pump shutting down");
                break;
            }
            frame = receiver.recv_async().fuse() => {
                match frame {
                    Ok(frame) => process_frame(&shard, frame).await,
                    Err(_) => {
                        debug!("Message receiver closed; exiting pump");
                        break;
                    }
                }
            }
        }
    }

    // Drain remaining frames before flushing — any in-flight appends must
    // complete so their data lands in the journal before we flush to disk.
    while let Ok(frame) = receiver.try_recv() {
        process_frame(&shard, frame).await;
    }

    flush_and_fsync_all_partitions(&shard).await;

    Ok(())
}

async fn process_frame(shard: &Rc<IggyShard>, frame: ShardFrame) {
    let ShardFrame {
        message,
        response_sender,
    } = frame;
    if let (Some(response), Some(tx)) =
        (handle_shard_message(shard, message).await, response_sender)
    {
        let _ = tx.send(response).await;
    }
}

/// Final flush + fsync of all local partitions. Runs inside the pump after
/// the main loop exits, so no other pump frame can interleave.
async fn flush_and_fsync_all_partitions(shard: &Rc<IggyShard>) {
    let namespaces = shard.get_current_shard_namespaces();
    if namespaces.is_empty() {
        return;
    }

    let mut flushed = 0u32;
    for ns in &namespaces {
        match shard
            .flush_unsaved_buffer_from_local_partitions(ns, false)
            .await
        {
            Ok(saved) if saved > 0 => flushed += 1,
            Ok(_) => {}
            Err(e) => error!("Shutdown flush failed for partition {:?}: {}", ns, e),
        }
    }
    if flushed > 0 {
        info!("Shutdown: flushed {flushed} partitions.");
    }

    for ns in &namespaces {
        if let Err(e) = shard.fsync_all_messages_from_local_partitions(ns).await {
            error!("Shutdown fsync failed for partition {:?}: {}", ns, e);
        }
    }
    info!("Shutdown: fsync complete for all partitions.");
}
