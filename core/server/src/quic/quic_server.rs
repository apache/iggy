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

use crate::configs::quic::QuicConfig;
use crate::quic::{COMPONENT, listener, quic_socket};
use crate::server_error::QuicError;
use crate::shard::IggyShard;
use crate::shard::task_registry::ShutdownToken;
use crate::shard::transmission::event::ShardEvent;
use anyhow::Result;
use compio_quic::{
    Endpoint, EndpointConfig, IdleTimeout, ServerBuilder, ServerConfig, TransportConfig, VarInt,
};
use err_trail::ErrContext;
use rustls::crypto::ring::default_provider;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use tracing::info;
use tracing::{error, trace, warn};

/// Starts the QUIC server.
/// Returns the address the server is listening on.
pub async fn spawn_quic_server(
    shard: Rc<IggyShard>,
    shutdown: ShutdownToken,
) -> Result<(), iggy_common::IggyError> {
    // Ensure rustls crypto provider is installed (thread-safe, idempotent)
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        if let Err(e) = default_provider().install_default() {
            warn!(
                "Failed to install rustls crypto provider: {:?}. This may be normal if another thread installed it first.",
                e
            );
        } else {
            trace!("Rustls crypto provider installed successfully");
        }
    } else {
        trace!("Rustls crypto provider already installed");
    }

    let config = shard.config.quic.clone();
    let mut addr: SocketAddr = config.address.parse().map_err(|e| {
        error!("Failed to parse QUIC address '{}': {}", config.address, e);
        iggy_common::IggyError::QuicError
    })?;

    if shard.id != 0 && addr.port() == 0 {
        info!("Waiting for QUIC address from shard 0...");
        loop {
            if let Some(bound_addr) = shard.quic_bound_address.get() {
                addr = bound_addr;
                info!("Received QUIC address: {}", addr);
                break;
            }
            compio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    info!(
        "Initializing Iggy QUIC server on shard {} for address {}",
        shard.id, addr
    );

    let server_config = configure_quic(&config).map_err(|e| {
        error!("Failed to configure QUIC server: {:?}", e);
        iggy_common::IggyError::QuicError
    })?;
    trace!("Building UDP socket for QUIC endpoint on {}", addr);

    let socket = quic_socket::build(&addr, &config.socket);
    socket.bind(&addr.into()).map_err(|e| {
        error!("Failed to bind socket: {}", e);
        iggy_common::IggyError::CannotBindToSocket(addr.to_string())
    })?;
    socket.set_nonblocking(true).map_err(|e| {
        error!("Failed to set nonblocking: {}", e);
        iggy_common::IggyError::QuicError
    })?;

    let std_socket: std::net::UdpSocket = socket.into();
    let socket = compio_net::UdpSocket::from_std(std_socket).map_err(|e| {
        error!("Failed to convert std socket to compio socket: {:?}", e);
        iggy_common::IggyError::QuicError
    })?;
    trace!("Creating QUIC endpoint with server config");

    let endpoint = Endpoint::new(socket, EndpointConfig::default(), Some(server_config), None)
        .map_err(|e| {
            error!("Failed to create QUIC endpoint: {:?}", e);
            iggy_common::IggyError::QuicError
        })?;

    let actual_addr = endpoint.local_addr().map_err(|e| {
        error!("Failed to get local address: {e}");
        iggy_common::IggyError::CannotBindToSocket(addr.to_string())
    })?;

    info!("Iggy QUIC server has started on: {:?}", actual_addr);

    if shard.id == 0 {
        // Store bound address locally
        shard.quic_bound_address.set(Some(actual_addr));

        if addr.port() == 0 {
            // Notify config writer on shard 0
            let _ = shard.config_writer_notify.try_send(());

            // Broadcast to other shards for SO_REUSEPORT binding
            let event = ShardEvent::AddressBound {
                protocol: iggy_common::TransportProtocol::Quic,
                address: actual_addr,
            };
            shard.broadcast_event_to_all_shards(event).await?;
        }
    } else {
        shard.quic_bound_address.set(Some(actual_addr));
    }

    listener::start(endpoint, shard, shutdown).await
}

fn configure_quic(config: &QuicConfig) -> Result<ServerConfig, QuicError> {
    let (certificates, private_key) = match config.certificate.self_signed {
        true => generate_self_signed_cert()?,
        false => load_certificates(&config.certificate.cert_file, &config.certificate.key_file)?,
    };

    let builder = ServerBuilder::new_with_single_cert(certificates, private_key)
        .with_error(|error| {
            format!("{COMPONENT} (error: {error}) - failed to create QUIC server builder")
        })
        .map_err(|_| QuicError::ConfigCreationError)?;
    let mut transport = TransportConfig::default();
    transport.initial_mtu(config.initial_mtu.as_bytes_u64() as u16);
    transport.send_window(config.send_window.as_bytes_u64());
    transport.receive_window(
        VarInt::try_from(config.receive_window.as_bytes_u64())
            .with_error(|error| format!("{COMPONENT} (error: {error}) - invalid receive window"))
            .map_err(|_| QuicError::TransportConfigError)?,
    );
    transport.datagram_send_buffer_size(config.datagram_send_buffer_size.as_bytes_u64() as usize);
    transport.max_concurrent_bidi_streams(
        VarInt::try_from(config.max_concurrent_bidi_streams)
            .with_error(|error| format!("{COMPONENT} (error: {error}) - invalid bidi stream limit"))
            .map_err(|_| QuicError::TransportConfigError)?,
    );

    if !config.keep_alive_interval.is_zero() {
        transport.keep_alive_interval(Some(config.keep_alive_interval.get_duration()));
    }
    if !config.max_idle_timeout.is_zero() {
        let max_idle_timeout = IdleTimeout::try_from(config.max_idle_timeout.get_duration())
            .with_error(|error| format!("{COMPONENT} (error: {error}) - invalid idle timeout"))
            .map_err(|_| QuicError::TransportConfigError)?;
        transport.max_idle_timeout(Some(max_idle_timeout));
    }

    let mut server_config = builder.build();
    server_config.transport_config(Arc::new(transport));
    Ok(server_config)
}

fn generate_self_signed_cert<'a>() -> Result<(Vec<CertificateDer<'a>>, PrivateKeyDer<'a>), QuicError>
{
    iggy_common::generate_self_signed_certificate("localhost")
        .with_error(|error| {
            format!("{COMPONENT} (error: {error}) - failed to generate self-signed certificate")
        })
        .map_err(|_| QuicError::CertGenerationError)
}

fn load_certificates(
    cert_file: &str,
    key_file: &str,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), QuicError> {
    let mut cert_chain_reader = BufReader::new(
        File::open(cert_file)
            .with_error(|error| {
                format!("{COMPONENT} (error: {error}) - failed to open cert file: {cert_file}")
            })
            .map_err(|_| QuicError::CertLoadError)?,
    );
    let certs = rustls_pemfile::certs(&mut cert_chain_reader)
        .map(|x| CertificateDer::from(x.unwrap().to_vec()))
        .collect();
    let mut key_reader = BufReader::new(
        File::open(key_file)
            .with_error(|error| {
                format!("{COMPONENT} (error: {error}) - failed to open key file: {key_file}")
            })
            .map_err(|_| QuicError::CertLoadError)?,
    );
    let mut keys = rustls_pemfile::rsa_private_keys(&mut key_reader)
        .filter(|key| key.is_ok())
        .map(|key| PrivateKeyDer::try_from(key.unwrap().secret_pkcs1_der().to_vec()))
        .collect::<Result<Vec<_>, _>>()
        .with_error(|error| format!("{COMPONENT} (error: {error}) - failed to parse private key"))
        .map_err(|_| QuicError::CertLoadError)?;
    let key = keys.remove(0);
    Ok((certs, key))
}
