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

use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;

use anyhow::Result;
use compio_quic::{Endpoint, IdleTimeout, ServerConfig, TransportConfig, VarInt};
use error_set::ErrContext;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tracing::{error, info};

use crate::configs::quic::QuicConfig;
use crate::quic::COMPONENT;
use crate::quic::listener;
use crate::server_error::QuicError;
use crate::shard::IggyShard;

/// Starts the QUIC server.
/// Returns the address the server is listening on.
pub async fn start(
    server_name: &'static str,
    addr: SocketAddr,
    config: &QuicConfig,
    shard: Rc<IggyShard>,
) -> Result<(), iggy_common::IggyError> {
    if shard.id != 0 {
        info!(
            "QUIC server restricted to shard 0, skipping on shard {}",
            shard.id
        );
        return Ok(());
    }

    info!("Initializing Iggy QUIC server on shard 0...");

    // Configure QUIC server
    let server_config = configure_quic(config).map_err(|e| {
        error!("Failed to configure QUIC: {:?}", e);
        iggy_common::IggyError::QuicError
    })?;

    // Create QUIC endpoint
    let endpoint = Endpoint::server(addr, server_config).await.map_err(|e| {
        error!("Failed to create QUIC endpoint: {:?}", e);
        iggy_common::IggyError::CannotBindToSocket(addr.to_string())
    })?;

    let actual_addr = endpoint.local_addr().map_err(|e| {
        error!("Failed to get local address: {e}");
        iggy_common::IggyError::CannotBindToSocket(addr.to_string())
    })?;

    info!("{} server has started on: {:?}", server_name, actual_addr);

    // Store the bound address (only shard 0 runs QUIC server)
    shard.quic_bound_address.set(Some(actual_addr));

    listener::start(endpoint, shard).await
}

fn configure_quic(config: &QuicConfig) -> Result<ServerConfig, QuicError> {
    let (certificates, private_key) = match config.certificate.self_signed {
        true => generate_self_signed_cert()?,
        false => load_certificates(&config.certificate.cert_file, &config.certificate.key_file)?,
    };

    let mut server_config = ServerConfig::with_single_cert(certificates, private_key)
        .with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to create server config")
        })
        .map_err(|_| QuicError::ConfigCreationError)?;

    let mut transport = TransportConfig::default();

    // Configure transport parameters
    transport.initial_mtu(config.initial_mtu.as_bytes_u64() as u16);
    transport.send_window(config.send_window.as_bytes_u64());
    transport.receive_window(VarInt::from_u64(config.receive_window.as_bytes_u64()).unwrap());
    transport.datagram_send_buffer_size(config.datagram_send_buffer_size.as_bytes_u64() as usize);
    transport
        .max_concurrent_bidi_streams(VarInt::from_u64(config.max_concurrent_bidi_streams).unwrap());

    if !config.keep_alive_interval.is_zero() {
        transport.keep_alive_interval(Some(config.keep_alive_interval.get_duration()));
    }

    if !config.max_idle_timeout.is_zero() {
        // Create IdleTimeout from Duration - different API than quinn
        let idle_timeout = IdleTimeout::try_from(config.max_idle_timeout.get_duration())
            .map_err(|_| QuicError::TransportConfigError)?;
        transport.max_idle_timeout(Some(idle_timeout));
    }

    server_config.transport_config(Arc::new(transport));
    Ok(server_config)
}

fn generate_self_signed_cert()
-> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), QuicError> {
    iggy_common::generate_self_signed_certificate("localhost")
        .with_error_context(|error| {
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
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to open cert file: {cert_file}")
            })
            .map_err(|_| QuicError::CertLoadError)?,
    );
    let certs = rustls_pemfile::certs(&mut cert_chain_reader)
        .map(|x| CertificateDer::from(x.unwrap().to_vec()))
        .collect();
    let mut key_reader = BufReader::new(
        File::open(key_file)
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to open key file: {key_file}")
            })
            .map_err(|_| QuicError::CertLoadError)?,
    );
    let mut keys = rustls_pemfile::rsa_private_keys(&mut key_reader)
        .filter(|key| key.is_ok())
        .map(|key| PrivateKeyDer::try_from(key.unwrap().secret_pkcs1_der().to_vec()))
        .collect::<Result<Vec<_>, _>>()
        .with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to parse private key")
        })
        .map_err(|_| QuicError::CertLoadError)?;
    let key = keys.remove(0);
    Ok((certs, key))
}
