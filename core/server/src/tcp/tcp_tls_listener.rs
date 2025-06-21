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
use crate::streaming::clients::client_manager::Transport;
use crate::streaming::systems::system::SharedSystem;
use crate::tcp::connection_handler::{handle_connection, handle_error};
use rustls::ServerConfig;
use rustls_pemfile::{certs, private_key};
use std::io::BufReader;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpSocket;
use tokio::sync::oneshot;
use tokio_rustls::{TlsAcceptor, rustls};
use tracing::{error, info};

pub(crate) async fn start(
    address: &str,
    config: TcpTlsConfig,
    socket: TcpSocket,
    system: SharedSystem,
) -> SocketAddr {
    let address = address.to_string();
    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let cert_file_path = &config.cert_file;
        let cert_file = std::fs::File::open(cert_file_path)
            .unwrap_or_else(|e| panic!("Unable to open certificate file '{cert_file_path}': {e}"));
        let mut cert_reader = BufReader::new(cert_file);

        let certs: Vec<_> = certs(&mut cert_reader)
            .collect::<Result<Vec<_>, _>>()
            .unwrap_or_else(|e| {
                panic!("Unable to parse certificates from '{cert_file_path}': {e}")
            });

        if certs.is_empty() {
            panic!("No certificates found in certificate file '{cert_file_path}'");
        }

        let key_file_path = &config.key_file;
        let key_file = std::fs::File::open(key_file_path)
            .unwrap_or_else(|e| panic!("Unable to open key file '{key_file_path}': {e}"));
        let mut key_reader = BufReader::new(key_file);

        let key = private_key(&mut key_reader)
            .unwrap_or_else(|e| panic!("Unable to parse private key from '{key_file_path}': {e}"))
            .unwrap_or_else(|| panic!("No private key found in key file '{key_file_path}'"));

        let server_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .unwrap_or_else(|e| {
                panic!("Unable to create TLS server config with cert '{cert_file_path}' and key '{key_file_path}': {e}")
            });

        let acceptor = TlsAcceptor::from(Arc::new(server_config));

        let addr = address.parse();
        if addr.is_err() {
            panic!("Unable to parse address {:?}", address);
        }

        let addr = addr.unwrap();
        socket
            .bind(addr)
            .unwrap_or_else(|e| panic!("Unable to bind socket to address '{addr}': {e}",));

        let listener = socket
            .listen(1024)
            .unwrap_or_else(|e| panic!("Unable to start TCP TLS server on '{address}': {e}",));

        let local_addr = listener
            .local_addr()
            .unwrap_or_else(|e| panic!("Failed to get local address for TCP TLS listener: {e}",));

        tx.send(local_addr).unwrap_or_else(|_| {
            panic!("Failed to send the local address '{local_addr}' for TCP TLS listener")
        });

        loop {
            match listener.accept().await {
                Ok((stream, address)) => {
                    info!("Accepted new TCP TLS connection: {}", address);
                    let session = system
                        .read()
                        .await
                        .add_client(&address, Transport::Tcp)
                        .await;

                    let client_id = session.client_id;
                    let acceptor = acceptor.clone();
                    let stream = acceptor.accept(stream).await.unwrap_or_else(|e| {
                        panic!("Failed to accept TLS connection from '{address}': {e}",);
                    });
                    let system = system.clone();
                    let mut sender = SenderKind::get_tcp_tls_sender(stream);
                    tokio::spawn(async move {
                        if let Err(error) =
                            handle_connection(session, &mut sender, system.clone()).await
                        {
                            handle_error(error);
                            system.read().await.delete_client(client_id).await;
                            if let Err(error) = sender.shutdown().await {
                                error!(
                                    "Failed to shutdown TCP stream for client: {client_id}, address: {address}. {error}"
                                );
                            } else {
                                info!(
                                    "Successfully closed TCP stream for client: {client_id}, address: {address}."
                                );
                            }
                        }
                    });
                }
                Err(error) => error!("Unable to accept TCP TLS socket. {error}"),
            }
        }
    });
    match rx.await {
        Ok(addr) => addr,
        Err(_) => panic!("Failed to get the local address for TCP TLS listener."),
    }
}
