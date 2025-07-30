use futures::{FutureExt, StreamExt};
use iggy_common::IggyError;
use std::{rc::Rc, time::Duration};

use crate::{
    shard::{IggyShard, transmission::frame::ShardFrame},
    shard_error, shard_info,
};

async fn run_shard_messages_receiver(shard: Rc<IggyShard>) -> Result<(), IggyError> {
    let mut messages_receiver = shard.messages_receiver.take().unwrap();

    shard_info!(shard.id, "Starting message passing task");
    loop {
        let shutdown_check = async {
            loop {
                if shard.is_shutting_down() {
                    return;
                }
                compio::time::sleep(Duration::from_millis(100)).await;
            }
        };

        futures::select! {
            _ = shutdown_check.fuse() => {
                shard_info!(shard.id, "Message receiver shutting down");
                break;
            }
            frame = messages_receiver.next().fuse() => {
                if let Some(frame) = frame {
                    let ShardFrame {
                        message,
                        response_sender,
                    } = frame;
                    match (shard.handle_shard_message(message).await, response_sender) {
                        (Some(response), Some(response_sender)) => {
                            response_sender
                                .send(response)
                                .await
                                .expect("Failed to send response back to origin shard.");
                        }
                        _ => {}
                    };
                }
            }
        }
    }

    Ok(())
}

pub async fn spawn_shard_message_task(shard: Rc<IggyShard>) -> Result<(), IggyError> {
    let result = run_shard_messages_receiver(shard.clone()).await;
    if let Err(err) = result {
        shard_error!(shard.id, "Error running shard message receiver: {err}");
        return Err(err);
    }
    Ok(())
}
