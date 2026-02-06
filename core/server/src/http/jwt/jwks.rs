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

use dashmap::DashMap;
use iggy_common::IggyError;
use jsonwebtoken::DecodingKey;
use serde::Deserialize;
use serde_json;
use std::hash::Hash;
use strum::{Display, EnumString};

/// JWK key type enumeration
#[derive(Debug, Clone, Copy, Display, EnumString, Deserialize, PartialEq, Eq)]
#[strum(serialize_all = "UPPERCASE")]
#[serde(rename_all = "UPPERCASE")]
enum JwkKeyType {
    /// RSA key type
    #[strum(serialize = "RSA")]
    Rsa,
    /// EC (Elliptic Curve) key type
    #[strum(serialize = "EC")]
    Ec,
}

#[derive(Debug, Deserialize)]
struct Jwk {
    kty: JwkKeyType,
    kid: Option<String>,
    n: Option<String>,
    e: Option<String>,
    x: Option<String>,
    y: Option<String>,
    crv: Option<String>,
}

#[derive(Debug, Deserialize)]
struct JwkSet {
    keys: Vec<Jwk>,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct CacheKey {
    issuer: String,
    kid: String,
}

#[derive(Debug, Clone)]
pub struct JwksClient {
    cache: DashMap<CacheKey, DecodingKey>,
}

impl Default for JwksClient {
    fn default() -> Self {
        Self {
            cache: DashMap::new(),
        }
    }
}

impl JwksClient {
    pub async fn get_key(&self, issuer: &str, jwks_url: &str, kid: &str) -> Option<DecodingKey> {
        let cache_key = CacheKey {
            issuer: issuer.to_string(),
            kid: kid.to_string(),
        };

        // try to get from cache first
        if let Some(key) = self.cache.get(&cache_key) {
            return Some(key.clone());
        }

        // fetch and cache if not found
        if let Ok(key) = self.fetch_and_cache_key(issuer, jwks_url, kid).await {
            return Some(key);
        }

        None
    }

    async fn fetch_and_cache_key(
        &self,
        issuer: &str,
        jwks_url: &str,
        kid: &str,
    ) -> Result<DecodingKey, IggyError> {
        if let Err(e) = self.refresh_keys(issuer, jwks_url).await {
            return Err(IggyError::CannotFetchJwks(format!(
                "Failed to refresh keys: {}",
                e
            )));
        }

        let cache_key = CacheKey {
            issuer: issuer.to_string(),
            kid: kid.to_string(),
        };

        self.cache
            .get(&cache_key)
            .map(|entry| entry.clone())
            .ok_or(IggyError::InvalidAccessToken)
    }

    async fn refresh_keys(&self, issuer: &str, jwks_url: &str) -> Result<(), IggyError> {
        let response = ureq::get(jwks_url)
            .call()
            .map_err(|_| IggyError::CannotFetchJwks(jwks_url.to_string()))?;

        let body = response
            .into_string()
            .map_err(|_| IggyError::CannotFetchJwks(jwks_url.to_string()))?;

        let jwks: JwkSet = serde_json::from_str(&body)
            .map_err(|e| IggyError::CannotFetchJwks(format!("Failed to parse JWKS: {}", e)))?;

        for key in jwks.keys {
            if let Some(kid) = key.kid {
                let decoding_key: DecodingKey = match key.kty {
                    JwkKeyType::Rsa => {
                        if let (Some(n), Some(e)) = (key.n.as_deref(), key.e.as_deref()) {
                            DecodingKey::from_rsa_components(n, e).map_err(|e| {
                                IggyError::CannotFetchJwks(format!("Invalid RSA key: {}", e))
                            })?
                        } else {
                            continue;
                        }
                    }
                    JwkKeyType::Ec => {
                        if let (Some(x), Some(y), Some(crv)) =
                            (key.x.as_deref(), key.y.as_deref(), key.crv.as_deref())
                        {
                            match crv.to_ascii_uppercase().as_str() {
                                "P-256" | "P-384" | "P-521" => {
                                    DecodingKey::from_ec_components(x, y).map_err(|e| {
                                        IggyError::CannotFetchJwks(format!("Invalid EC key: {}", e))
                                    })?
                                }
                                _ => continue,
                            }
                        } else {
                            continue;
                        }
                    }
                };

                let cache_key = CacheKey {
                    issuer: issuer.to_string(),
                    kid,
                };
                self.cache.insert(cache_key, decoding_key);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::DecodingKey;

    const TEST_ISSUER: &str = "https://test-issuer.com";
    const TEST_KID: &str = "test-key";

    fn create_test_decoding_key() -> DecodingKey {
        // Use HMAC secret to create a simple test DecodingKey
        // Note: This is only for testing cache logic, not a real RSA/EC key
        DecodingKey::from_secret(b"test-secret-key-for-cache-testing-only")
    }

    #[test]
    fn test_cache_key_equality() {
        let key1 = CacheKey {
            issuer: TEST_ISSUER.to_string(),
            kid: TEST_KID.to_string(),
        };
        let key2 = CacheKey {
            issuer: TEST_ISSUER.to_string(),
            kid: TEST_KID.to_string(),
        };
        assert_eq!(key1, key2);
    }

    #[test]
    fn test_cache_key_different_issuer() {
        let key1 = CacheKey {
            issuer: "issuer1".to_string(),
            kid: TEST_KID.to_string(),
        };
        let key2 = CacheKey {
            issuer: "issuer2".to_string(),
            kid: TEST_KID.to_string(),
        };
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_cache_key_different_kid() {
        let key1 = CacheKey {
            issuer: TEST_ISSUER.to_string(),
            kid: "kid1".to_string(),
        };
        let key2 = CacheKey {
            issuer: TEST_ISSUER.to_string(),
            kid: "kid2".to_string(),
        };
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_jwks_client_default() {
        let client = JwksClient::default();
        assert!(client.cache.is_empty());
    }

    #[test]
    fn test_cache_insert_and_get() {
        let client = JwksClient::default();
        let cache_key = CacheKey {
            issuer: TEST_ISSUER.to_string(),
            kid: TEST_KID.to_string(),
        };
        let decoding_key = create_test_decoding_key();

        client.cache.insert(cache_key.clone(), decoding_key.clone());

        let cached = client.cache.get(&cache_key);
        assert!(cached.is_some());
    }

    #[test]
    fn test_cache_multiple_keys() {
        let client = JwksClient::default();

        let key1 = CacheKey {
            issuer: "issuer1".to_string(),
            kid: "kid1".to_string(),
        };
        let key2 = CacheKey {
            issuer: "issuer2".to_string(),
            kid: "kid2".to_string(),
        };

        let decoding_key1 = create_test_decoding_key();
        let decoding_key2 = create_test_decoding_key();

        client.cache.insert(key1.clone(), decoding_key1);
        client.cache.insert(key2.clone(), decoding_key2);

        assert_eq!(client.cache.len(), 2);
        assert!(client.cache.get(&key1).is_some());
        assert!(client.cache.get(&key2).is_some());
    }
}
