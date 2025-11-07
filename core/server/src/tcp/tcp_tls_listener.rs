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
use crate::streaming::systems::system::SharedSystem;
use crate::tcp::connection_handler::{handle_connection, handle_error};
use iggy_common::TransportProtocol;
use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
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

        let addr = address.parse();
        if addr.is_err() {
            panic!("Unable to parse address {address:?}");
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
                        .add_client(&address, TransportProtocol::Tcp)
                        .await;

                    let client_id = session.client_id;
                    let acceptor = acceptor.clone();
                    let system_clone = system.clone();
                    match acceptor.accept(stream).await {
                        Ok(stream) => {
                            let mut sender = SenderKind::get_tcp_tls_sender(stream);
                            tokio::spawn(async move {
                                if let Err(error) =
                                    handle_connection(session, &mut sender, system_clone.clone())
                                        .await
                                {
                                    handle_error(error);
                                    system_clone.read().await.delete_client(client_id).await;
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
                        Err(e) => {
                            error!("Failed to accept TLS connection from '{address}': {e}");
                            system_clone.read().await.delete_client(client_id).await;
                        }
                    }
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
