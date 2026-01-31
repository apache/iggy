/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IpAddrKind {
    #[default]
    V4,
    V6,
}

#[derive(Debug, Clone)]
pub struct EncryptionConfig {
    pub key: String,
}

impl EncryptionConfig {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub cert_dir: PathBuf,
    pub domain: String,
    pub validate_certificate: bool,
    pub self_signed: bool,
    pub generate_certs: bool,
}

impl TlsConfig {
    /// Create a TlsConfig with custom certificates.
    pub fn new(cert_dir: impl Into<PathBuf>, domain: impl Into<String>, validate: bool) -> Self {
        Self {
            cert_dir: cert_dir.into(),
            domain: domain.into(),
            validate_certificate: validate,
            self_signed: false,
            generate_certs: false,
        }
    }

    /// Create a TlsConfig for server-generated self-signed certificates.
    /// The server will generate the certificates automatically.
    /// Client cannot validate these certs as no CA file is available.
    pub fn self_signed() -> Self {
        Self {
            cert_dir: PathBuf::new(),
            domain: "localhost".to_string(),
            validate_certificate: false,
            self_signed: true,
            generate_certs: false,
        }
    }

    /// Create a TlsConfig that generates test certificates during harness setup.
    /// The generated CA cert can be used by clients for certificate validation.
    pub fn generated() -> Self {
        Self {
            cert_dir: PathBuf::new(), // Will be set by ServerHandle during setup
            domain: "localhost".to_string(),
            validate_certificate: true,
            self_signed: false,
            generate_certs: true,
        }
    }

    /// Get the CA certificate path for client use.
    /// Returns None for self-signed mode (no CA file available).
    pub fn ca_cert_path(&self) -> Option<PathBuf> {
        if self.self_signed || self.cert_dir.as_os_str().is_empty() {
            None
        } else {
            Some(self.cert_dir.join("test_cert.pem"))
        }
    }
}
