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

use crate::{
    AuthType, ClickHouseInsertFormat, ClickHouseSinkConfig, clickhouse_inserter::ClickHouseInserter,
};
use clickhouse::{Client, Compression};
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::{client::legacy::Client as HyperClient, rt::TokioExecutor};
use rustls::{ClientConfig, RootCertStore};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use std::fmt;

/// read from  config.toml and create client
/// client level configs
/// compression , cred - username/pwd or jwt, cert - client & server(cloud path), format
/// new json| column| blob(string)(goes into the `insert into` sql), role, default role
pub(crate) struct ClickHouseClient {
    client: Client,
    config: ClickHouseSinkConfig,
}

impl fmt::Debug for ClickHouseClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClickHouseClient")
            .field("client", &"<ClickHouse Client>")
            .finish()
    }
}

impl ClickHouseClient {
    pub(crate) fn init(
        config: ClickHouseSinkConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // ── TLS / mTLS ───────────────────────────────────────────────────────────
        // The `clickhouse` crate is compiled with `rustls-tls` (webpki bundle) and
        // `rustls-tls-native-roots` (OS trust store), so `Client::default()` already
        // handles the two most common cases without any custom connector:
        //
        //   • ClickHouse Cloud / public CA  → covered by `rustls-tls` (webpki)
        //   • Self-signed cert installed    → covered by `rustls-tls-native-roots`
        //     in the host OS trust store       (reads /etc/ssl/certs at runtime)
        //
        // A custom hyper+rustls connector is only needed when the config supplies:
        //   • `tls_root_ca_cert`  – a cert file path NOT in the OS store, OR
        //   • `tls_client_cert` + `tls_client_key` – mTLS (no crate feature covers this)
        let needs_custom_http_client = config.tls_enabled()
            && (config.tls_root_ca_cert.is_some()
                || (config.tls_client_cert.is_some() && config.tls_client_key.is_some()));

        let base: Client = if needs_custom_http_client {
            // ── Root-CA store ────────────────────────────────────────────────────
            // Start with the webpki built-in bundle (same roots that `rustls-tls`
            // uses in `Client::default()`), then optionally append any custom CA
            // provided via `tls_root_ca_cert`.  This means a custom-CA-only config
            // still trusts publicly-signed certs, and an mTLS-only config (no
            // custom CA) also trusts public CAs correctly.
            let mut root_store = RootCertStore {
                roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
            };

            if let Some(ref ca_path) = config.tls_root_ca_cert {
                // Append custom / self-signed CA on top of the public bundle.
                for cert in CertificateDer::pem_file_iter(ca_path)? {
                    root_store.add(cert?)?;
                }
            }

            // ── rustls ClientConfig ──────────────────────────────────────────────
            let tls_config = if let (Some(cert_path), Some(key_path)) =
                (&config.tls_client_cert, &config.tls_client_key)
            {
                // mTLS: load the client certificate chain and private key.
                let certs: Vec<CertificateDer<'static>> =
                    CertificateDer::pem_file_iter(cert_path)?.collect::<Result<_, _>>()?;
                let key = PrivateKeyDer::from_pem_file(key_path)?;

                ClientConfig::builder()
                    .with_root_certificates(root_store)
                    .with_client_auth_cert(certs, key)?
            } else {
                // Custom CA only, no client cert.
                ClientConfig::builder()
                    .with_root_certificates(root_store)
                    .with_no_client_auth()
            };

            // ── hyper connector ──────────────────────────────────────────────────
            let connector = HttpsConnectorBuilder::new()
                .with_tls_config(tls_config)
                .https_or_http()
                .enable_http1()
                .build();

            let hyper_client = HyperClient::builder(TokioExecutor::new()).build(connector);
            Client::with_http_client(hyper_client)
        } else {
            // No cert file or mTLS needed – the default transport covers everything:
            //   `rustls-tls`              → webpki bundle (ClickHouse Cloud, public CAs)
            //   `rustls-tls-native-roots` → OS trust store (self-signed certs on the host)
            Client::default()
        };

        // ── Required: URL ────────────────────────────────────────────────────────
        let mut client = base.with_url(&config.url);

        // ── Compression ──────────────────────────────────────────────────────────
        let compression = if config.compression_enabled() {
            Compression::Lz4
        } else {
            Compression::None
        };
        client = client.with_compression(compression);

        // ── Authentication ───────────────────────────────────────────────────────
        // The clickhouse crate panics if credentials and JWT are mixed
        match (&config.username, &config.password) {
            (Some(_), None) | (None, Some(_)) => {
                return Err("ClickHouseSink config: when using credential 'username' and 'password' both must be set".into());
            }
            _ => {}
        }

        let has_credentials = config.username.is_some();

        client = match config.auth_type {
            AuthType::None => client,
            AuthType::Credential if has_credentials => {
                client = client.with_user(config.username.as_deref().unwrap());
                client.with_password(config.password.as_deref().unwrap())
            }
            AuthType::Credential => {
                return Err("ClickHouseSink config: 'auth_type' is 'credential' but 'username' and 'password' are not set".into());
            }
            AuthType::Jwt if !has_credentials => {
                if let Some(ref token) = config.jwt_token {
                    client = client.with_access_token(token.clone());
                    client
                } else {
                    return Err(
                        "ClickHouseSink config: 'auth_type' is 'jwt' but 'jwt_token' is not set"
                            .into(),
                    );
                }
            }
            AuthType::Jwt => {
                return Err("ClickHouseSink config: 'auth_type' is 'jwt' but 'username'/'password' are also set".into());
            }
        };

        if let Some(role) = &config.role {
            client = client.with_roles(role);
        }

        // Enable new JSON type usage
        client = client.with_option("allow_experimental_json_type", "1");
        client = client.with_option("input_format_binary_read_json_as_string", "1");
        client = client.with_option("output_format_binary_write_json_as_string", "1");
        // Enable nested json objects in input json
        client = client.with_option("input_format_import_nested_json", "1");

        if let Some(database) = &config.database {
            client = client.with_database(database)
        }

        client = client.with_validation(false); //use rowbinary, disable Row type validation against schema, better for streaming

        Ok(Self { client, config })
    }

    /// Creates a formatted inserter for JSONEachRow format.
    pub(crate) fn create_formatted_inserter(&self) -> ClickHouseInserter {
        ClickHouseInserter::new(
            &self.client,
            &self.config.table,
            ClickHouseInsertFormat::JsonEachRow,
            self.config.chunk_size(),
        )
    }

    /// Creates and returns a typed inserter for structured Row data.
    pub(crate) fn create_typed_inserter<T: clickhouse::Row>(
        &self,
        table: &str,
    ) -> clickhouse::inserter::Inserter<T> {
        self.client.inserter(table)
    }
}
