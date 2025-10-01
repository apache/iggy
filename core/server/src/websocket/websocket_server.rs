use crate::configs::websocket::WebSocketConfig;
use crate::shard::IggyShard;
use crate::shard::task_registry::ShutdownToken;
use crate::websocket::websocket_listener::start;
use crate::{shard_error, shard_info};
use iggy_common::IggyError;
use std::rc::Rc;

pub async fn spawn_websocket_server(
    shard: Rc<IggyShard>,
    shutdown: ShutdownToken,
) -> Result<(), IggyError> {
    let config = shard.config.websocket.clone();

    if !config.enabled {
        shard_info!(shard.id, "WebSocket server is disabled.");
        return Ok(());
    }

    shard_info!(
        shard.id,
        "Starting WebSocket server on: {} for shard: {}...",
        config.address,
        shard.id
    );

    if let Err(error) = start(config, shard.clone(), shutdown).await {
        shard_error!(
            shard.id,
            "WebSocket server has failed to start, error: {error}"
        );
        return Err(error);
    }

    Ok(())
}
