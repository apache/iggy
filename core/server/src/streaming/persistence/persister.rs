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

use crate::streaming::persistence::COMPONENT;
use crate::streaming::utils::file;
use compio::buf::IoBuf;
use compio::fs::remove_file;
use compio::io::AsyncWriteAtExt;
use err_trail::ErrContext;
use iggy_common::IggyError;
use std::fmt::Debug;

#[derive(Debug)]
pub enum PersisterKind {
    File(FilePersister),
    FileWithSync(FileWithSyncPersister),
}

impl PersisterKind {
    pub async fn append<B: IoBuf>(&self, path: &str, bytes: B) -> Result<(), IggyError> {
        match self {
            PersisterKind::File(p) => p.append(path, bytes).await,
            PersisterKind::FileWithSync(p) => p.append(path, bytes).await,
        }
    }

    pub async fn overwrite<B: IoBuf>(&self, path: &str, bytes: B) -> Result<(), IggyError> {
        match self {
            PersisterKind::File(p) => p.overwrite(path, bytes).await,
            PersisterKind::FileWithSync(p) => p.overwrite(path, bytes).await,
        }
    }

    pub async fn delete(&self, path: &str) -> Result<(), IggyError> {
        match self {
            PersisterKind::File(p) => p.delete(path).await,
            PersisterKind::FileWithSync(p) => p.delete(path).await,
        }
    }
}

#[derive(Debug)]
pub struct FilePersister;

impl FilePersister {
    pub async fn append<B: IoBuf>(&self, path: &str, bytes: B) -> Result<(), IggyError> {
        let (mut file, position) = file::append(path)
            .await
            .with_error(|error| {
                format!("{COMPONENT} (error: {error}) - failed to append to file: {path}")
            })
            .map_err(|_| IggyError::CannotAppendToFile)?;
        file.write_all_at(bytes, position)
            .await
            .0
            .with_error(|error| {
                format!("{COMPONENT} (error: {error}) - failed to write data to file: {path}")
            })
            .map_err(|_| IggyError::CannotWriteToFile)?;
        Ok(())
    }

    pub async fn overwrite<B: IoBuf>(&self, path: &str, bytes: B) -> Result<(), IggyError> {
        let mut file = file::overwrite(path)
            .await
            .with_error(|error| {
                format!("{COMPONENT} (error: {error}) - failed to overwrite file: {path}")
            })
            .map_err(|_| IggyError::CannotOverwriteFile)?;
        let position = 0;
        file.write_all_at(bytes, position)
            .await
            .0
            .with_error(|error| {
                format!("{COMPONENT} (error: {error}) - failed to write data to file: {path}")
            })
            .map_err(|_| IggyError::CannotWriteToFile)?;
        Ok(())
    }

    pub async fn delete(&self, path: &str) -> Result<(), IggyError> {
        remove_file(path)
            .await
            .with_error(|error| {
                format!("{COMPONENT} (error: {error}) - failed to delete file: {path}")
            })
            .map_err(|_| IggyError::CannotDeleteFile)?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct FileWithSyncPersister;

impl FileWithSyncPersister {
    pub async fn append<B: IoBuf>(&self, path: &str, bytes: B) -> Result<(), IggyError> {
        let (mut file, position) = file::append(path)
            .await
            .with_error(|error| {
                format!("{COMPONENT} (error: {error}) - failed to append to file: {path}")
            })
            .map_err(|_| IggyError::CannotAppendToFile)?;
        file.write_all_at(bytes, position)
            .await
            .0
            .with_error(|error| {
                format!("{COMPONENT} (error: {error}) - failed to write data to file: {path}")
            })
            .map_err(|_| IggyError::CannotWriteToFile)?;
        file.sync_all()
            .await
            .with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to sync file after appending: {path}"
                )
            })
            .map_err(|_| IggyError::CannotSyncFile)?;
        Ok(())
    }

    pub async fn overwrite<B: IoBuf>(&self, path: &str, bytes: B) -> Result<(), IggyError> {
        let mut file = file::overwrite(path)
            .await
            .with_error(|error| {
                format!("{COMPONENT} (error: {error}) - failed to overwrite file: {path}")
            })
            .map_err(|_| IggyError::CannotOverwriteFile)?;
        let position = 0;
        file.write_all_at(bytes, position)
            .await
            .0
            .with_error(|error| {
                format!("{COMPONENT} (error: {error}) - failed to write data to file: {path}")
            })
            .map_err(|_| IggyError::CannotWriteToFile)?;
        file.sync_all()
            .await
            .with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to sync file after overwriting: {path}"
                )
            })
            .map_err(|_| IggyError::CannotSyncFile)?;
        Ok(())
    }

    pub async fn delete(&self, path: &str) -> Result<(), IggyError> {
        remove_file(path)
            .await
            .with_error(|error| {
                format!("{COMPONENT} (error: {error}) - failed to delete file: {path}")
            })
            .map_err(|_| IggyError::CannotDeleteFile)?;
        Ok(())
    }
}
