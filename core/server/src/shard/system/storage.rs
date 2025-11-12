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

use super::COMPONENT;
use crate::shard::system::info::SystemInfo;
use crate::streaming::persistence::persister::PersisterKind;
use crate::streaming::utils::PooledBuffer;
use crate::streaming::utils::file;
use anyhow::Context;
use compio::buf::IoBuf;
use compio::io::AsyncReadAtExt;
use err_trail::ErrContext;
use iggy_common::IggyError;
use std::sync::Arc;
use tracing::info;

#[derive(Debug)]
pub struct FileSystemInfoStorage {
    persister: Arc<PersisterKind>,
    path: String,
}

impl FileSystemInfoStorage {
    pub fn new(path: String, persister: Arc<PersisterKind>) -> Self {
        Self { path, persister }
    }

    pub async fn load(&self) -> Result<SystemInfo, IggyError> {
        let file = file::open(&self.path).await;
        if file.is_err() {
            return Err(IggyError::ResourceNotFound(self.path.to_owned()));
        }

        let file = file.unwrap();
        let file_size = file
            .metadata()
            .await
            .with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to retrieve metadata for file at path: {}",
                    self.path
                )
            })
            .map_err(|_| IggyError::CannotReadFileMetadata)?
            .len() as usize;

        let file = file::open(&self.path)
            .await
            .map_err(|_| IggyError::CannotReadFile)?;
        let buffer = PooledBuffer::with_capacity(file_size);
        let (result, buffer) = file
            .read_exact_at(buffer.slice(0..file_size), 0)
            .await
            .into();
        result
            .with_error(|error| {
                format!(
                    "{COMPONENT} Failed to read system info from file at path: {} (error: {error})",
                    self.path
                )
            })
            .map_err(|_| IggyError::CannotReadFile)?;
        let (system_info, _) =
            bincode::serde::decode_from_slice(&buffer, bincode::config::standard())
                .with_context(|| "Failed to deserialize system info")
                .map_err(|_| IggyError::CannotDeserializeResource)?;
        Ok(system_info)
    }

    pub async fn save(&self, system_info: &SystemInfo) -> Result<(), IggyError> {
        let data = bincode::serde::encode_to_vec(system_info, bincode::config::standard())
            .with_context(|| "Failed to serialize system info")
            .map_err(|_| IggyError::CannotSerializeResource)?;
        self.persister
            .overwrite(&self.path, data)
            .await
            .with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to overwrite file at path: {}",
                    self.path
                )
            })?;
        info!("Saved system info, {system_info}");
        Ok(())
    }
}
