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
use crate::configs::tcp::TcpTlsConfig;
use crate::shard::IggyShard;
use crate::shard::transmission::event::ShardEvent;
use crate::streaming::clients::client_manager::Transport;
use crate::tcp::connection_handler::{handle_connection, handle_error};
use futures::FutureExt;
use iggy_common::IggyError;
use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls_pemfile::{certs, private_key};
use socket2::Socket;
use std::io::BufReader;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

pub(crate) async fn start(
    server_name: &'static str,
    addr: SocketAddr,
    socket: Socket,
    shard: Rc<IggyShard>,
) -> Result<(), IggyError> {
    /*
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let config = &shard.config.tcp.tls;

        let (certs, key) =
            if config.self_signed && !std::path::Path::new(&config.cert_file).exists() {
                info!("Generating self-signed certificate for TCP TLS server");
                generate_self_signed_cert()
                    .unwrap_or_else(|e| panic!("Failed to generate self-signed certificate: {e}"))
            } else {
                info!(
                    "Loading certificates from cert_file: {}, key_file: {}",
                    config.cert_file, config.key_file
                );
                load_certificates(&config.cert_file, &config.key_file)
                    .unwrap_or_else(|e| panic!("Failed to load certificates: {e}"))
            };

    let server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .unwrap_or_else(|e| panic!("Unable to create TLS server config: {e}"));

    let acceptor = TlsAcceptor::from(Arc::new(server_config));

    socket
        .bind(&addr.into())
        .unwrap_or_else(|e| panic!("Unable to bind socket to address '{addr}': {e}",));

    let listener: std::net::TcpListener = socket.into();
    let listener = monoio::net::TcpListener::from_std(listener).unwrap();
    info!("{server_name} server has started on: {:?}", addr);

    loop {
        let shutdown_check = async {
            loop {
                if shard.is_shutting_down() {
                    return;
                }
                monoio::time::sleep(Duration::from_millis(100)).await;
            }
        };

        let accept_future = listener.accept();
        futures::select! {
            _ = shutdown_check.fuse() => {
                info!("TCP TLS server detected shutdown flag, no longer accepting connections");
                break;
            }
            result = accept_future.fuse() => {
                match result {
                    Ok((stream, address)) => {
                        if shard.is_shutting_down() {
                            info!("Rejecting new TLS connection from {} during shutdown", address);
                            continue;
                        }
                        let shard_clone = shard.clone();
                        info!("Accepted new TCP TLS connection: {}", address);
                        let transport = Transport::Tcp;
                        let session = shard_clone.add_client(&address, transport);
                        //TODO: Those can be shared with other shards.
                        shard_clone.add_active_session(session.clone());
                        // Broadcast session to all shards.
                        let event = ShardEvent::NewSession { address, transport };
                        // TODO: Fixme look inside of broadcast_event_to_all_shards method.
                        let _responses = shard_clone.broadcast_event_to_all_shards(event.into()).await;

                        let client_id = session.client_id;
                        info!("Created new session: {session}");
                        let acceptor = acceptor.clone();

                        let conn_stop_receiver = shard_clone.task_registry.add_connection(client_id);

                        let shard_for_conn = shard_clone.clone();
                        shard_clone.task_registry.spawn_tracked(async move {
                            match acceptor.accept(stream).await {
                                Ok(tls_stream) => {
                                    let mut sender = SenderKind::get_tcp_tls_sender(tls_stream.into());
                                    if let Err(error) = handle_connection(&session, &mut sender, &shard_for_conn, conn_stop_receiver).await {
                                        handle_error(error);
                                    }
                                    shard_for_conn.task_registry.remove_connection(&client_id);

                                    if let Err(error) = sender.shutdown().await {
                                        error!(
                                            "Failed to shutdown TCP TLS stream for client: {client_id}, address: {address}. {error}"
                                        );
                                    } else {
                                        info!(
                                            "Successfully closed TCP TLS stream for client: {client_id}, address: {address}."
                                        );
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to accept TLS connection from '{address}': {e}");
                                    shard_for_conn.task_registry.remove_connection(&client_id);
                                }
                            }
                        });
                    }
                    Err(error) => error!("Unable to accept TCP TLS socket. {error}"),
                }
            }
        }
    }
    */
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
