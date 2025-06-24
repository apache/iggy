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
use std::sync::Arc;

use anyhow::Result;
use error_set::ErrContext;
use quinn::{Endpoint, IdleTimeout, VarInt};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tracing::info;

use crate::configs::quic::QuicConfig;
use crate::quic::COMPONENT;
use crate::quic::listener;
use crate::server_error::QuicError;

//TODO: Fixme
/*
/// Starts the QUIC server.
/// Returns the address the server is listening on.
pub fn start(config: QuicConfig, system: SharedSystem) -> SocketAddr {
    info!("Initializing Iggy QUIC server...");
    let address = config.address.parse().unwrap();
    let quic_config = configure_quic(config);
    if let Err(error) = quic_config {
        panic!("Error when configuring QUIC: {error:?}");
    }

    let endpoint = Endpoint::server(quic_config.unwrap(), address).unwrap();
    let addr = endpoint.local_addr().unwrap();
    listener::start(endpoint, system);
    info!("Iggy QUIC server has started on: {:?}", addr);
    addr
}

fn configure_quic(config: QuicConfig) -> Result<quinn::ServerConfig, QuicError> {
    let (certificate, key) = match config.certificate.self_signed {
        true => generate_self_signed_cert()?,
        false => load_certificates(&config.certificate.cert_file, &config.certificate.key_file)?,
    };

    let mut server_config = quinn::ServerConfig::with_single_cert(certificate, key)
        .with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to create server config")
        })
        .map_err(|_| QuicError::ConfigCreationError)?;
    let mut transport = quinn::TransportConfig::default();
    transport.initial_mtu(config.initial_mtu.as_bytes_u64() as u16);
    transport.send_window(config.send_window.as_bytes_u64());
    transport.receive_window(
        VarInt::try_from(config.receive_window.as_bytes_u64())
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - invalid receive window")
            })
            .map_err(|_| QuicError::TransportConfigError)?,
    );
    transport.datagram_send_buffer_size(config.datagram_send_buffer_size.as_bytes_u64() as usize);
    transport.max_concurrent_bidi_streams(
        VarInt::try_from(config.max_concurrent_bidi_streams)
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - invalid bidi stream limit")
            })
            .map_err(|_| QuicError::TransportConfigError)?,
    );
    if !config.keep_alive_interval.is_zero() {
        transport.keep_alive_interval(Some(config.keep_alive_interval.get_duration()));
    }
    if !config.max_idle_timeout.is_zero() {
        let max_idle_timeout = IdleTimeout::try_from(config.max_idle_timeout.get_duration())
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - invalid idle timeout")
            })
            .map_err(|_| QuicError::TransportConfigError)?;
        transport.max_idle_timeout(Some(max_idle_timeout));
    }

    server_config.transport_config(Arc::new(transport));
    Ok(server_config)
}

fn generate_self_signed_cert<'a>() -> Result<(Vec<CertificateDer<'a>>, PrivateKeyDer<'a>), QuicError>
{
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
    */
