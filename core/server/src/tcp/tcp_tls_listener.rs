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
use crate::shard::transmission::event::ShardEvent;
use crate::{shard_error, shard_info, shard_warn};
use compio::net::{TcpListener, TcpOpts};
use compio_tls::TlsAcceptor;
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
use std::time::Duration;
use tracing::trace;

pub(crate) async fn start(
    server_name: &'static str,
    mut addr: SocketAddr,
    config: &TcpSocketConfig,
    shard: Rc<IggyShard>,
) -> Result<(), IggyError> {
    //TODO: Fix me, this needs to take into account that first shard id potentially can be greater than 0.
    if shard.id != 0 && addr.port() == 0 {
        shard_info!(shard.id, "Waiting for TCP address from shard 0...");
        loop {
            if let Some(bound_addr) = shard.tcp_bound_address.get() {
                addr = bound_addr;
                shard_info!(shard.id, "Received TCP address: {}", addr);
                break;
            }
            compio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    let listener = create_listener(addr, config)
        .await
        .map_err(|_| IggyError::CannotBindToSocket(addr.to_string()))
        .with_error_context(|err| {
            format!("Failed to bind {server_name} server to address: {addr}, {err}")
        })?;

    let actual_addr = listener.local_addr().map_err(|_e| {
        shard_error!(shard.id, "Failed to get local address: {_e}");
        IggyError::CannotBindToSocket(addr.to_string())
    })?;

    //TODO: Fix me, this needs to take into account that first shard id potentially can be greater than 0.
    if shard.id == 0 {
        if addr.port() == 0 {
            let event = ShardEvent::TcpBound {
                address: actual_addr,
            };
            shard.broadcast_event_to_all_shards(event).await;
        }

        let mut current_config = shard.config.clone();
        current_config.tcp.address = actual_addr.to_string();

        let runtime_path = current_config.system.get_runtime_path();
        let current_config_path = format!("{runtime_path}/current_config.toml");
        let current_config_content =
            toml::to_string(&current_config).expect("Cannot serialize current_config");

        let buf_result = compio::fs::write(&current_config_path, current_config_content).await;
        match buf_result.0 {
            Ok(_) => shard_info!(
                shard.id,
                "Current config written to: {}",
                current_config_path
            ),
            Err(e) => shard_error!(
                shard.id,
                "Failed to write current config to {}: {}",
                current_config_path,
                e
            ),
        }
    }

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
                "Generating self-signed certificate for TCP TLS server"
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
        "{} server has started on: {:?}",
        server_name,
        actual_addr
    );

    accept_loop(server_name, listener, acceptor, shard).await
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
) -> Result<(), IggyError> {
    loop {
        let shard = shard.clone();
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
                shard_info!(shard.id, "{} detected shutdown flag, no longer accepting connections", server_name);
                break;
            }
            result = accept_future.fuse() => {
                match result {
                    Ok((stream, address)) => {
                        if shard.is_shutting_down() {
                            shard_info!(shard.id, "Rejecting new TLS connection from {} during shutdown", address);
                            continue;
                        }
                        shard_info!(shard.id, "Accepted new TCP connection for TLS handshake: {}", address);
                        let shard_clone = shard.clone();
                        let acceptor = acceptor.clone();

                        // Perform TLS handshake in a separate task to avoid blocking the accept loop
                        let task_shard = shard_clone.clone();
                        compio::runtime::spawn(async move {
                            match acceptor.accept(stream).await {
                                Ok(tls_stream) => {
                                    // TLS handshake successful, now create session
                                    shard_info!(task_shard.id, "TLS handshake successful, adding TCP client: {}", address);
                                    let transport = TransportProtocol::Tcp;
                                    let session = task_shard.add_client(&address, transport);
                                    shard_info!(task_shard.id, "Added {} client with session: {} for IP address: {}", transport, session, address);
                                    //TODO: Those can be shared with other shards.
                                    task_shard.add_active_session(session.clone());
                                    // Broadcast session to all shards.
                                    let event = ShardEvent::NewSession { address, transport };
                                    // TODO: Fixme look inside of broadcast_event_to_all_shards method.
                                    let _responses = task_shard.broadcast_event_to_all_shards(event.into()).await;

                                    let client_id = session.client_id;
                                    shard_info!(task_shard.id, "Created new session: {}", session);

                                    let sender = SenderKind::get_tcp_tls_sender(tls_stream);

                                    // TODO: Update to use new TaskManager system
                                    // use crate::shard::task::connection::tcp_connection_task;
                                    // let task = tcp_connection_task(session, sender, task_shard.clone());
                                    // task_shard.task_registry.spawn_connection(
                                    //     client_id,
                                    //     transport,
                                    //     task,
                                    // );
                                }
                                Err(e) => {
                                    shard_error!(task_shard.id, "Failed to accept TLS connection from '{}': {}", address, e);
                                    // No session was created, so no cleanup needed
                                }
                            }
                        }).detach();
                    }
                    Err(error) => shard_error!(shard.id, "Unable to accept TCP TLS socket. {}", error),
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
