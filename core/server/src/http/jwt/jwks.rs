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

use iggy_common::locking::{IggyRwLock, IggyRwLockFn};
use jsonwebtoken::DecodingKey;
use serde::Deserialize;
use serde_json;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
struct Jwk {
    kty: String,
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
    cache: Arc<IggyRwLock<HashMap<CacheKey, DecodingKey>>>,
}

impl Default for JwksClient {
    fn default() -> Self {
        Self {
            cache: Arc::new(IggyRwLock::new(HashMap::new())),
        }
    }
}

impl JwksClient {
    pub async fn get_key(&self, issuer: &str, jwks_url: &str, kid: &str) -> Option<DecodingKey> {
        let cache_key = CacheKey {
            issuer: issuer.to_string(),
            kid: kid.to_string(),
        };

        {
            let cache = self.cache.read().await;
            if let Some(key) = cache.get(&cache_key) {
                return Some(key.clone());
            }
        }

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
    ) -> Result<DecodingKey, anyhow::Error> {
        if let Err(e) = self.refresh_keys(issuer, jwks_url).await {
            return Err(anyhow::anyhow!("Failed to refresh keys: {}", e));
        }

        let cache_key = CacheKey {
            issuer: issuer.to_string(),
            kid: kid.to_string(),
        };

        let cache = self.cache.read().await;
        cache
            .get(&cache_key)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Key not found in cache after refresh"))
    }

    async fn refresh_keys(&self, issuer: &str, jwks_url: &str) -> Result<(), anyhow::Error> {
        let response = ureq::get(jwks_url)
            .call()
            .map_err(|e| anyhow::anyhow!("Failed to fetch JWKS: {}", e))?;

        let body = response
            .into_string()
            .map_err(|e| anyhow::anyhow!("Failed to read response body: {}", e))?;

        let jwks: JwkSet = serde_json::from_str(&body)
            .map_err(|e| anyhow::anyhow!("Failed to parse JWKS: {}", e))?;

        let mut cache = self.cache.write().await;

        for key in jwks.keys {
            if let Some(kid) = key.kid {
                let decoding_key: DecodingKey = match key.kty.as_str() {
                    "RSA" => {
                        if let (Some(n), Some(e)) = (key.n.as_deref(), key.e.as_deref()) {
                            DecodingKey::from_rsa_components(n, e)
                                .map_err(|e| anyhow::anyhow!("Invalid RSA key: {}", e))?
                        } else {
                            continue;
                        }
                    }
                    "EC" => {
                        if let (Some(x), Some(y), Some(crv)) =
                            (key.x.as_deref(), key.y.as_deref(), key.crv.as_deref())
                        {
                            match crv {
                                "P-256" => DecodingKey::from_ec_components(x, y)
                                    .map_err(|e| anyhow::anyhow!("Invalid EC key: {}", e))?,
                                "P-384" => DecodingKey::from_ec_components(x, y)
                                    .map_err(|e| anyhow::anyhow!("Invalid EC key: {}", e))?,
                                "P-521" => DecodingKey::from_ec_components(x, y)
                                    .map_err(|e| anyhow::anyhow!("Invalid EC key: {}", e))?,
                                _ => continue,
                            }
                        } else {
                            continue;
                        }
                    }
                    _ => continue,
                };

                let cache_key = CacheKey {
                    issuer: issuer.to_string(),
                    kid,
                };
                cache.insert(cache_key, decoding_key);
            }
        }

        Ok(())
    }
}
