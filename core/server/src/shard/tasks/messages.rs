use futures::StreamExt;
use iggy_common::IggyError;
use std::rc::Rc;
use tracing::error;

use crate::shard::{IggyShard, transmission::frame::ShardFrame};

async fn run_shard_messages_receiver(shard: Rc<IggyShard>) -> Result<(), IggyError> {
    let mut messages_receiver = shard.messages_receiver.take().unwrap();
    loop {
        if let Some(frame) = messages_receiver.next().await {
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

pub async fn spawn_shard_message_task(shard: Rc<IggyShard>) -> Result<(), IggyError> {
    monoio::spawn(async move {
        let result = run_shard_messages_receiver(shard).await;
        if let Err(err) = &result {
            error!("Error running shard: {err}");
        }
        result
    })
    .await
}
