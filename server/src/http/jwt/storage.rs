use crate::http::jwt::COMPONENT;
use crate::streaming::utils::file;
use crate::{
    http::jwt::json_web_token::RevokedAccessToken, streaming::persistence::persister::PersisterKind,
};
use ahash::AHashMap;
use anyhow::Context;
use bytes::{BufMut, BytesMut};
use error_set::ErrContext;
use iggy::error::IggyError;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
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
        let file = file::open(&self.path).await;
        if file.is_err() {
            info!("No revoked access tokens found to load.");
            return Ok(vec![]);
        }

        info!("Loading revoked access tokens from: {}", self.path);
        let mut file = file.map_err(|error| {
            error!("Cannot open revoked access tokens file: {error}");
            IggyError::CannotReadFile
        })?;
        let file_size = file
            .metadata()
            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to read file metadata, path: {}",
                    self.path
                )
            })
            .map_err(|_| IggyError::CannotReadFileMetadata)?
            .len() as usize;
        let mut buffer = BytesMut::with_capacity(file_size);
        buffer.put_bytes(0, file_size);
        file.read_exact(&mut buffer)
            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to read file into buffer, path: {}",
                    self.path
                )
            })
            .map_err(|_| IggyError::CannotReadFile)?;

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
            .overwrite(&self.path, &bytes)
            .await
            .with_error_context(|error| {
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
            .with_error_context(|error| {
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
            .overwrite(&self.path, &bytes)
            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to overwrite file, path: {}",
                    self.path
                )
            })?;
        Ok(())
    }
}
