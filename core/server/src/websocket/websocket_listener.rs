use crate::binary::sender::SenderKind;
use crate::configs::websocket::WebSocketConfig;
use crate::shard::IggyShard;
use crate::shard::transmission::event::ShardEvent;
use crate::streaming::clients::client_manager::Transport;
use crate::websocket::connection_handler::{handle_connection, handle_error};
use crate::websocket::websocket_sender::WebSocketSender;
use crate::{shard_error, shard_info};
use compio::net::TcpListener;
use compio_ws::accept_async_with_config;
use error_set::ErrContext;
use futures::FutureExt;
use iggy_common::IggyError;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;
use tracing::{error, info};

pub async fn start(
    addr: SocketAddr,
    shard: Rc<IggyShard>,
    config: &WebSocketConfig,
) -> Result<(), IggyError> {
    let listener = TcpListener::bind(addr)
        .await
        .with_error_context(|error| {
            format!("WebSocket (error: {error}) - failed to bind to address: {addr}")
        })
        .map_err(|_| IggyError::CannotBindToSocket(addr.to_string()))?;

    let local_addr = listener.local_addr().unwrap();
    shard_info!(
        shard.id,
        "{} has started on: ws://{}",
        "WebSocket Server",
        local_addr
    );

    let ws_config = config.to_tungstenite_config();
    shard_info!(
        shard.id,
        "WebSocket config: max_message_size: {:?}, max_frame_size: {:?}, accept_unmasked_frames: {}",
        config.max_message_size,
        config.max_frame_size,
        config.accept_unmasked_frames
    );

    loop {
        let shard = shard.clone();
        let ws_config = ws_config.clone();
        let shutdown_check = async {
            loop {
                if shard.is_shutting_down() {
                    return;
                }
                compio::time::sleep(Duration::from_millis(100)).await;
            }
        };

        let accept_future = listener.accept();
        futures::select! {
            _ = shutdown_check.fuse() => {
                shard_info!(shard.id, "WebSocket Server detected shutdown flag, no longer accepting connections");
                break;
            }
            result = accept_future.fuse() => {
                match result {
                    Ok((tcp_stream, remote_addr)) => {
                        if shard.is_shutting_down() {
                            shard_info!(shard.id, "Rejecting new connection from {} during shutdown", remote_addr);
                            continue;
                        }
                        shard_info!(shard.id, "Accepted new WebSocket connection from: {}", remote_addr);
                        let shard_clone = shard.clone();
                        let ws_config_clone = ws_config.clone();
                        shard.task_registry.spawn_tracked(async move {
                            // Use the configured WebSocket config during handshake
                            match accept_async_with_config(tcp_stream, ws_config_clone).await {
                                Ok(websocket) => {
                                    info!("WebSocket handshake successful from: {}", remote_addr);

                                    let session = shard_clone.add_client(&remote_addr, Transport::WebSocket);
                                    let client_id = session.client_id;
                                    shard_clone.add_active_session(session.clone());

                                    let event = ShardEvent::NewSession {
                                        address: remote_addr,
                                        transport: Transport::WebSocket,
                                    };
                                    let _ = shard_clone.broadcast_event_to_all_shards(event.into()).await;

                                    let sender = WebSocketSender::new(websocket);
                                    let mut sender_kind = SenderKind::get_websocket_sender(sender);
                                    let client_stop_receiver = shard_clone.task_registry.add_connection(client_id);

                                    if let Err(error) = handle_connection(&session, &mut sender_kind, &shard_clone, client_stop_receiver).await {
                                        handle_error(error);
                                    }
                                    shard_clone.task_registry.remove_connection(&client_id);
                                }
                                Err(error) => {
                                    error!("WebSocket handshake failed from {}: {:?}", remote_addr, error);
                                }
                            }
                        });
                    }
                    Err(error) => {
                        shard_error!(shard.id, "Failed to accept WebSocket connection: {}", error);
                    }
                }
            }
        }
    }

    shard_info!(shard.id, "WebSocket Server listener has stopped");
    Ok(())
}
