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

use iggy::error::IggyError;
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::duration::IggyDuration;
use iggy::utils::expiry::IggyExpiry;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use serde_with::DisplayFromStr;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct HttpConfig {
    pub enabled: bool,
    pub address: String,
    pub max_request_size: IggyByteSize,
    pub cors: HttpCorsConfig,
    pub jwt: HttpJwtConfig,
    pub metrics: HttpMetricsConfig,
    pub tls: HttpTlsConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct HttpCorsConfig {
    pub enabled: bool,
    pub allowed_methods: Vec<String>,
    pub allowed_origins: Vec<String>,
    pub allowed_headers: Vec<String>,
    pub exposed_headers: Vec<String>,
    pub allow_credentials: bool,
    pub allow_private_network: bool,
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct HttpJwtConfig {
    pub algorithm: String,
    pub issuer: String,
    pub audience: String,
    pub valid_issuers: Vec<String>,
    pub valid_audiences: Vec<String>,
    #[serde_as(as = "DisplayFromStr")]
    pub access_token_expiry: IggyExpiry,
    #[serde_as(as = "DisplayFromStr")]
    pub clock_skew: IggyDuration,
    #[serde_as(as = "DisplayFromStr")]
    pub not_before: IggyDuration,
    pub encoding_secret: String,
    pub decoding_secret: String,
    pub use_base64_secret: bool,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct HttpMetricsConfig {
    pub enabled: bool,
    pub endpoint: String,
}

#[derive(Debug)]
pub enum JwtSecret {
    Default(String),
    Base64(String),
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct HttpTlsConfig {
    pub enabled: bool,
    pub cert_file: String,
    pub key_file: String,
}

impl HttpJwtConfig {
    pub fn get_algorithm(&self) -> Result<Algorithm, IggyError> {
        match self.algorithm.as_str() {
            "HS256" => Ok(Algorithm::HS256),
            "HS384" => Ok(Algorithm::HS384),
            "HS512" => Ok(Algorithm::HS512),
            "RS256" => Ok(Algorithm::RS256),
            "RS384" => Ok(Algorithm::RS384),
            "RS512" => Ok(Algorithm::RS512),
            _ => Err(IggyError::InvalidJwtAlgorithm(self.algorithm.clone())),
        }
    }

    pub fn get_decoding_secret(&self) -> JwtSecret {
        self.get_secret(&self.decoding_secret)
    }

    pub fn get_encoding_secret(&self) -> JwtSecret {
        self.get_secret(&self.encoding_secret)
    }

    pub fn get_decoding_key(&self) -> Result<DecodingKey, IggyError> {
        if self.decoding_secret.is_empty() {
            return Err(IggyError::InvalidJwtSecret);
        }

        Ok(match self.get_decoding_secret() {
            JwtSecret::Default(ref secret) => DecodingKey::from_secret(secret.as_ref()),
            JwtSecret::Base64(ref secret) => {
                DecodingKey::from_base64_secret(secret).map_err(|_| IggyError::InvalidJwtSecret)?
            }
        })
    }

    pub fn get_encoding_key(&self) -> Result<EncodingKey, IggyError> {
        if self.encoding_secret.is_empty() {
            return Err(IggyError::InvalidJwtSecret);
        }

        Ok(match self.get_encoding_secret() {
            JwtSecret::Default(ref secret) => EncodingKey::from_secret(secret.as_ref()),
            JwtSecret::Base64(ref secret) => {
                EncodingKey::from_base64_secret(secret).map_err(|_| IggyError::InvalidJwtSecret)?
            }
        })
    }

    fn get_secret(&self, secret: &str) -> JwtSecret {
        if self.use_base64_secret {
            JwtSecret::Base64(secret.to_string())
        } else {
            JwtSecret::Default(secret.to_string())
        }
    }
}
