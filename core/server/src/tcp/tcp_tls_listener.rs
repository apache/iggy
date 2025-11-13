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
use crate::configs::tcp::TcpSocketConfig;
use crate::shard::IggyShard;
use crate::shard::task_registry::ShutdownToken;
use crate::shard::transmission::event::ShardEvent;
use crate::tcp::connection_handler::{handle_connection, handle_error};
use compio::net::{TcpListener, TcpOpts};
use compio_tls::TlsAcceptor;
use err_trail::ErrContext;
use futures::FutureExt;
use iggy_common::{IggyError, TransportProtocol};
use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls_pemfile::{certs, private_key};
use std::io::BufReader;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, trace, warn};

pub(crate) async fn start(
    server_name: &'static str,
    mut addr: SocketAddr,
    config: &TcpSocketConfig,
    shard: Rc<IggyShard>,
    shutdown: ShutdownToken,
) -> Result<(), IggyError> {
    if shard.id != 0 && addr.port() == 0 {
        info!("Waiting for TCP address from shard 0...");
        loop {
            if let Some(bound_addr) = shard.tcp_bound_address.get() {
                addr = bound_addr;
                info!("Received TCP address: {}", addr);
                break;
            }
            compio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    let listener = create_listener(addr, config)
        .await
        .map_err(|_| IggyError::CannotBindToSocket(addr.to_string()))
        .with_error(|err| {
            format!("Failed to bind {server_name} server to address: {addr}, {err}")
        })?;

    let actual_addr = listener.local_addr().map_err(|_e| {
        error!("Failed to get local address: {_e}");
        IggyError::CannotBindToSocket(addr.to_string())
    })?;

    if shard.id == 0 {
        shard.tcp_bound_address.set(Some(actual_addr));
        if addr.port() == 0 {
            // Notify config writer on shard 0
            let _ = shard.config_writer_notify.try_send(());

            let event = ShardEvent::AddressBound {
                protocol: TransportProtocol::Tcp,
                address: actual_addr,
            };
            shard.broadcast_event_to_all_shards(event).await?;
        }
    }

    // Ensure rustls crypto provider is installed
    if rustls::crypto::CryptoProvider::get_default().is_none()
        && let Err(e) = rustls::crypto::ring::default_provider().install_default()
    {
        warn!(
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
            info!("Generating self-signed certificate for TCP TLS server");
            generate_self_signed_cert()
                .unwrap_or_else(|e| panic!("Failed to generate self-signed certificate: {e}"))
        } else {
            info!(
                "Loading certificates from cert_file: {}, key_file: {}",
                tls_config.cert_file, tls_config.key_file
            );
            load_certificates(&tls_config.cert_file, &tls_config.key_file)
                .unwrap_or_else(|e| panic!("Failed to load certificates: {e}"))
        };

    let server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .unwrap_or_else(|e| panic!("Unable to create TLS server config: {e}"));

    let acceptor = TlsAcceptor::from(Arc::new(server_config));

    info!("{} server has started on: {:?}", server_name, actual_addr);

    accept_loop(server_name, listener, acceptor, shard, shutdown).await
}

async fn create_listener(
    addr: SocketAddr,
    config: &TcpSocketConfig,
) -> Result<TcpListener, std::io::Error> {
    // Required by the thread-per-core model...
    // We create bunch of sockets on different threads, that bind to exactly the same address and port.
    let opts = TcpOpts::new().reuse_port(true);
    let opts = if config.override_defaults {
        let recv_buffer_size = config
            .recv_buffer_size
            .as_bytes_u64()
            .try_into()
            .expect("Failed to parse recv_buffer_size for TCP socket");

        let send_buffer_size = config
            .send_buffer_size
            .as_bytes_u64()
            .try_into()
            .expect("Failed to parse send_buffer_size for TCP socket");

        opts.recv_buffer_size(recv_buffer_size)
            .send_buffer_size(send_buffer_size)
            .keepalive(config.keepalive)
            .linger(config.linger.get_duration())
            .nodelay(config.nodelay)
    } else {
        opts
    };
    TcpListener::bind_with_options(addr, opts).await
}

async fn accept_loop(
    server_name: &'static str,
    listener: TcpListener,
    acceptor: TlsAcceptor,
    shard: Rc<IggyShard>,
    shutdown: ShutdownToken,
) -> Result<(), IggyError> {
    loop {
        let shard = shard.clone();
        let accept_future = listener.accept();
        futures::select! {
            _ = shutdown.wait().fuse() => {
                info!("{} received shutdown signal, no longer accepting connections", server_name);
                break;
            }
            result = accept_future.fuse() => {
                match result {
                    Ok((stream, address)) => {
                        if shard.is_shutting_down() {
                            info!("Rejecting new TLS connection from {} during shutdown", address);
                            continue;
                        }
                        info!("Accepted new TCP connection for TLS handshake: {}", address);
                        let shard_clone = shard.clone();
                        let acceptor = acceptor.clone();

                        // Perform TLS handshake in a separate task to avoid blocking the accept loop
                        let registry = shard.task_registry.clone();
                        let registry_clone = registry.clone();
                        registry.spawn_connection(async move {
                            match acceptor.accept(stream).await {
                                Ok(tls_stream) => {
                                    // TLS handshake successful, now create session
                                    info!("TLS handshake successful, adding TCP client: {}", address);
                                    let transport = TransportProtocol::Tcp;
                                    let session = shard_clone.add_client(&address, transport);
                                    info!("Added {} client with session: {} for IP address: {}", transport, session, address);

                                    let client_id = session.client_id;
                                    info!("Created new session: {}", session);

                                    let conn_stop_receiver = registry_clone.add_connection(client_id);
                                    let shard_for_conn = shard_clone.clone();
                                    let mut sender = SenderKind::get_tcp_tls_sender(tls_stream);
                                    if let Err(error) = handle_connection(&session, &mut sender, &shard_for_conn, conn_stop_receiver).await {
                                        handle_error(error);
                                    }
                                    shard_for_conn.delete_client(session.client_id);
                                    registry_clone.remove_connection(&client_id);

                                    if let Err(error) = sender.shutdown().await {
                                        error!("Failed to shutdown TCP TLS stream for client: {}, address: {}. {}", client_id, address, error);
                                    } else {
                                        info!("Successfully closed TCP TLS stream for client: {}, address: {}.", client_id, address);
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to accept TLS connection from '{}': {}", address, e);
                                    // No session was created, so no cleanup needed
                                }
                            }
                        });
                    }
                    Err(error) => error!("Unable to accept TCP TLS socket. {}", error),
                }
            }
        }
    }
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
