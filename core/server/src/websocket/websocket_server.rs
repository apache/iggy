use crate::configs::websocket::WebSocketConfig;
use crate::shard::IggyShard;
use crate::websocket::websocket_listener::start;
use crate::{shard_error, shard_info};
use iggy_common::IggyError;
use std::net::SocketAddr;
use std::rc::Rc;

pub async fn spawn_websocket_server(
    shard: Rc<IggyShard>,
    config: WebSocketConfig,
) -> Result<(), IggyError> {
    if !config.enabled {
        shard_info!(shard.id, "WebSocket server is disabled.");
        return Ok(());
    }

    let address = config.address.parse::<SocketAddr>();
    if address.is_err() {
        shard_error!(
            shard.id,
            "Invalid WebSocket server address: {}",
            config.address
        );
        return Err(IggyError::InvalidConfiguration);
    }

    let address = address.unwrap();
    shard_info!(
        shard.id,
        "Starting WebSocket server on: {} for shard: {}...",
        address,
        shard.id
    );

    if let Err(error) = start(address, shard.clone(), &config).await {
        shard_error!(
            shard.id,
            "WebSocket server has failed to start on: {address}, error: {error}"
        );
        return Err(error);
    }

    Ok(())
}
