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

use crate::http::jwt::COMPONENT;
use crate::{
    http::jwt::json_web_token::RevokedAccessToken, streaming::persistence::persister::PersisterKind,
};
use ahash::AHashMap;
use anyhow::Context;
use err_trail::ErrContext;
use iggy_common::IggyError;
use std::sync::Arc;
use tracing::{error, info};

#[derive(Debug)]
pub struct TokenStorage {
    persister: Arc<PersisterKind>,
    path: String,
}

impl TokenStorage {
    pub fn new(persister: Arc<PersisterKind>, path: &str) -> Self {
        Self {
            persister,
            path: path.to_owned(),
        }
    }

    pub async fn load_all_revoked_access_tokens(
        &self,
    ) -> Result<Vec<RevokedAccessToken>, IggyError> {
        // Check if file exists by trying to get metadata (equivalent to original file open check)
        let file_size = match compio::fs::metadata(&self.path).await {
            Err(_) => {
                info!("No revoked access tokens found to load.");
                return Ok(vec![]);
            }
            Ok(metadata) => metadata.len() as usize,
        };

        info!("Loading revoked access tokens from: {}", self.path);

        let buffer = compio::fs::read(&self.path)
            .await
            .with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to read file into buffer, path: {}",
                    self.path
                )
            })
            .map_err(|error| {
                error!("Cannot open revoked access tokens file: {error}");
                IggyError::CannotReadFile
            })?;

        if buffer.len() != file_size {
            error!(
                "File size mismatch: expected {file_size}, got {}",
                buffer.len()
            );
            return Err(IggyError::CannotReadFile);
        }

        let tokens: AHashMap<String, u64> =
            bincode::serde::decode_from_slice(&buffer, bincode::config::standard())
                .with_context(|| "Failed to deserialize revoked access tokens")
                .map_err(|_| IggyError::CannotDeserializeResource)?
                .0;

        let tokens = tokens
            .into_iter()
            .map(|(id, expiry)| RevokedAccessToken { id, expiry })
            .collect::<Vec<RevokedAccessToken>>();

        info!("Loaded {} revoked access tokens", tokens.len());
        Ok(tokens)
    }

    pub async fn save_revoked_access_token(
        &self,
        token: &RevokedAccessToken,
    ) -> Result<(), IggyError> {
        let tokens = self.load_all_revoked_access_tokens().await?;
        let mut map = tokens
            .into_iter()
            .map(|token| (token.id, token.expiry))
            .collect::<AHashMap<_, _>>();
        map.insert(token.id.to_owned(), token.expiry);
        let bytes = bincode::serde::encode_to_vec(&map, bincode::config::standard())
            .with_context(|| "Failed to serialize revoked access tokens")
            .map_err(|_| IggyError::CannotSerializeResource)?;
        self.persister
            .overwrite(&self.path, bytes)
            .await
            .with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to overwrite file, path: {}",
                    self.path
                )
            })?;
        Ok(())
    }

    pub async fn delete_revoked_access_tokens(&self, id: &[String]) -> Result<(), IggyError> {
        let tokens = self
            .load_all_revoked_access_tokens()
            .await
            .with_error(|error| {
                format!("{COMPONENT} (error: {error}) - failed to load revoked access tokens")
            })?;
        if tokens.is_empty() {
            return Ok(());
        }

        let mut map = tokens
            .into_iter()
            .map(|token| (token.id, token.expiry))
            .collect::<AHashMap<_, _>>();
        for id in id {
            map.remove(id);
        }

        let bytes = bincode::serde::encode_to_vec(&map, bincode::config::standard())
            .with_context(|| "Failed to serialize revoked access tokens")
            .map_err(|_| IggyError::CannotSerializeResource)?;
        self.persister
            .overwrite(&self.path, bytes)
            .await
            .with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to overwrite file, path: {}",
                    self.path
                )
            })?;
        Ok(())
    }
}
