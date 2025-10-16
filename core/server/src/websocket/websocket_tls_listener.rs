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

use crate::binary::sender::SenderKind;
use crate::configs::websocket::WebSocketConfig;
use crate::shard::IggyShard;
use crate::shard::task_registry::ShutdownToken;
use crate::shard::transmission::event::ShardEvent;
use crate::websocket::connection_handler::{handle_connection, handle_error};
use crate::{shard_debug, shard_error, shard_info, shard_warn};
use compio::net::TcpListener;
use compio_tls::TlsAcceptor;
use compio_ws::accept_async_with_config;
use error_set::ErrContext;
use futures::FutureExt;
use iggy_common::{IggyError, TransportProtocol};
use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls_pemfile::{certs, private_key};
use std::io::BufReader;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use tracing::{error, info, trace};

pub async fn start(
    config: WebSocketConfig,
    shard: Rc<IggyShard>,
    shutdown: ShutdownToken,
) -> Result<(), IggyError> {
    let addr: SocketAddr = config
        .address
        .parse()
        .with_error_context(|error| {
            format!(
                "WebSocket TLS (error: {error}) - failed to parse address: {}",
                config.address
            )
        })
        .map_err(|_| IggyError::InvalidConfiguration)?;

    let listener = TcpListener::bind(addr)
        .await
        .with_error_context(|error| {
            format!("WebSocket TLS (error: {error}) - failed to bind to address: {addr}")
        })
        .map_err(|_| IggyError::CannotBindToSocket(addr.to_string()))?;

    let local_addr = listener.local_addr().unwrap();

    // Notify shard about the bound address
    let event = ShardEvent::AddressBound {
        protocol: TransportProtocol::WebSocket,
        address: local_addr,
    };
    shard.handle_event(event).await.ok();

    // Ensure rustls crypto provider is installed
    if rustls::crypto::CryptoProvider::get_default().is_none()
        && let Err(e) = rustls::crypto::ring::default_provider().install_default()
    {
        shard_warn!(
            shard.id,
            "Failed to install rustls crypto provider: {:?}. This may be normal if another thread installed it first.",
            e
        );
    } else {
        trace!("Rustls crypto provider installed or already present");
    }

    // Load or generate TLS certificates
    let tls_config = &shard.config.tcp.tls;
    let (certs, key) =
        if tls_config.self_signed && !std::path::Path::new(&tls_config.cert_file).exists() {
            shard_info!(
                shard.id,
                "Generating self-signed certificate for WebSocket TLS server"
            );
            generate_self_signed_cert()
                .unwrap_or_else(|e| panic!("Failed to generate self-signed certificate: {e}"))
        } else {
            shard_info!(
                shard.id,
                "Loading certificates from cert_file: {}, key_file: {}",
                tls_config.cert_file,
                tls_config.key_file
            );
            load_certificates(&tls_config.cert_file, &tls_config.key_file)
                .unwrap_or_else(|e| panic!("Failed to load certificates: {e}"))
        };

    let server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .unwrap_or_else(|e| panic!("Unable to create TLS server config: {e}"));

    let acceptor = TlsAcceptor::from(Arc::new(server_config));

    shard_info!(
        shard.id,
        "{} has started on: wss://{}",
        "WebSocket TLS Server",
        local_addr
    );

    let ws_config = config.to_tungstenite_config();
    shard_info!(
        shard.id,
        "WebSocket TLS config: max_message_size: {:?}, max_frame_size: {:?}, accept_unmasked_frames: {}",
        config.max_message_size,
        config.max_frame_size,
        config.accept_unmasked_frames
    );

    let result = accept_loop(listener, acceptor, ws_config, shard.clone(), shutdown).await;

    shard_info!(
        shard.id,
        "WebSocket TLS listener task exiting with result: {:?}",
        result
    );

    result
}

async fn accept_loop(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    ws_config: Option<compio_ws::WebSocketConfig>,
    shard: Rc<IggyShard>,
    shutdown: ShutdownToken,
) -> Result<(), IggyError> {
    shard_info!(
        shard.id,
        "WebSocket TLS accept loop started, waiting for connections..."
    );

    loop {
        let shard = shard.clone();
        let acceptor = acceptor.clone();
        let accept_future = listener.accept();

        futures::select! {
            _ = shutdown.wait().fuse() => {
                shard_debug!(shard.id, "WebSocket TLS Server received shutdown signal, no longer accepting connections");
                break;
            }
            result = accept_future.fuse() => {
                match result {
                    Ok((tcp_stream, remote_addr)) => {
                        if shard.is_shutting_down() {
                            shard_info!(shard.id, "Rejecting new WebSocket TLS connection from {} during shutdown", remote_addr);
                            continue;
                        }
                        shard_info!(shard.id, "Accepted new TCP connection for WebSocket TLS handshake from: {}", remote_addr);

                        let shard_clone = shard.clone();
                        let ws_config_clone = ws_config;
                        let registry = shard.task_registry.clone();
                        let registry_clone = registry.clone();

                        registry.spawn_connection(async move {
                            match acceptor.accept(tcp_stream).await {
                                Ok(tls_stream) => {
                                    shard_info!(shard_clone.id, "TLS handshake successful for {}, performing WebSocket upgrade...", remote_addr);

                                    match accept_async_with_config(tls_stream, ws_config_clone).await {
                                        Ok(websocket) => {
                                            info!("WebSocket TLS handshake successful from: {}", remote_addr);

                                            let session = shard_clone.add_client(&remote_addr, TransportProtocol::WebSocket);
                                            let client_id = session.client_id;
                                            shard_clone.add_active_session(session.clone());

                                            let event = ShardEvent::NewSession {
                                                address: remote_addr,
                                                transport: TransportProtocol::WebSocket,
                                            };
                                            let _ = shard_clone.broadcast_event_to_all_shards(event).await;

                                            let sender = crate::websocket::websocket_tls_sender::WebSocketTlsSender::new(websocket);
                                            let mut sender_kind = SenderKind::WebSocketTls(sender);
                                            let client_stop_receiver = registry_clone.add_connection(client_id);

                                            if let Err(error) = handle_connection(&session, &mut sender_kind, &shard_clone, client_stop_receiver).await {
                                                handle_error(error);
                                            }
                                            registry_clone.remove_connection(&client_id);

                                            if let Err(error) = sender_kind.shutdown().await {
                                                shard_error!(shard_clone.id, "Failed to shutdown WebSocket TLS stream for client: {}, address: {}. {}", client_id, remote_addr, error);
                                            } else {
                                                shard_info!(shard_clone.id, "Successfully closed WebSocket TLS stream for client: {}, address: {}.", client_id, remote_addr);
                                            }
                                        }
                                        Err(error) => {
                                            error!("WebSocket handshake failed on TLS connection from {}: {:?}", remote_addr, error);
                                        }
                                    }
                                }
                                Err(error) => {
                                    error!("TLS handshake failed from {}: {:?}", remote_addr, error);
                                }
                            }
                        });
                    }
                    Err(error) => {
                        shard_error!(shard.id, "Failed to accept WebSocket TLS connection: {}", error);
                    }
                }
            }
        }
    }

    shard_info!(shard.id, "WebSocket TLS Server listener has stopped");
    Ok(())
}

fn generate_self_signed_cert()
-> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), Box<dyn std::error::Error>> {
    iggy_common::generate_self_signed_certificate("localhost")
}

fn load_certificates(
    cert_file: &str,
    key_file: &str,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), Box<dyn std::error::Error>> {
    let cert_file = std::fs::File::open(cert_file)?;
    let mut cert_reader = BufReader::new(cert_file);
    let certs: Vec<_> = certs(&mut cert_reader).collect::<Result<Vec<_>, _>>()?;

    if certs.is_empty() {
        return Err("No certificates found in certificate file".into());
    }

    let key_file = std::fs::File::open(key_file)?;
    let mut key_reader = BufReader::new(key_file);
    let key = private_key(&mut key_reader)?.ok_or("No private key found in key file")?;

    Ok((certs, key))
}
