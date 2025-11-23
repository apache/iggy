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

use std::io::SeekFrom;

use iggy_connector_sdk::{ConnectorState, Error};
use strum::Display;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::Mutex,
};
use tracing::{debug, error, info};

pub trait StateProvider {
    async fn load(&self) -> Result<Option<ConnectorState>, Error>;
    async fn save(&self, state: ConnectorState) -> Result<(), Error>;
}

#[non_exhaustive]
#[derive(Debug, Display)]
pub enum StateStorage {
    #[strum(to_string = "file")]
    File(FileStateProvider),
}

#[derive(Debug)]
pub struct FileStateProvider {
    path: String,
    file: Mutex<Option<File>>,
}

impl FileStateProvider {
    pub fn new(path: String) -> Self {
        FileStateProvider {
            path,
            file: Mutex::new(None),
        }
    }
}

impl StateProvider for FileStateProvider {
    async fn load(&self) -> Result<Option<ConnectorState>, Error> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&self.path)
            .await
            .map_err(|_| Error::CannotOpenStateFile)?;

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await.map_err(|error| {
            error!("Cannot read state file: {}. {error}.", self.path);
            Error::CannotReadStateFile
        })?;
        self.file.lock().await.replace(file);
        if buffer.is_empty() {
            info!("State file is empty: {}", self.path);
            Ok(None)
        } else {
            info!("Loaded state file: {}", self.path);
            Ok(Some(ConnectorState(buffer)))
        }
    }

    async fn save(&self, state: ConnectorState) -> Result<(), Error> {
        let mut file = self.file.lock().await;
        let Some(file) = file.as_mut() else {
            return Err(Error::CannotReadStateFile);
        };

        file.set_len(0).await.map_err(|error| {
            error!("Cannot truncate state file: {}. {error}.", self.path);
            Error::CannotWriteStateFile
        })?;

        file.seek(SeekFrom::Start(0)).await.map_err(|error| {
            error!("Cannot seek state file: {}. {error}.", self.path);
            Error::CannotWriteStateFile
        })?;

        file.write_all(&state.0).await.map_err(|error| {
            error!("Cannot write state file: {}. {error}.", self.path);
            Error::CannotWriteStateFile
        })?;

        debug!("Saved state file: {}", self.path);
        Ok(())
    }
}
