// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use compio::fs::{File, OpenOptions, create_dir_all};
use iggy_common::IggyError;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::info;

#[derive(Debug, Clone)]
struct DirEntry {
    path: PathBuf,
    is_dir: bool,
}

impl DirEntry {
    fn file(path: PathBuf) -> Self {
        Self {
            path,
            is_dir: false,
        }
    }

    fn dir(path: PathBuf) -> Self {
        Self { path, is_dir: true }
    }
}

pub trait SystemPaths {
    fn get_system_path(&self) -> String;

    fn get_state_path(&self) -> String;

    fn get_state_messages_file_path(&self) -> String;

    fn get_streams_path(&self) -> String;

    fn get_runtime_path(&self) -> String;
}

impl<T: SystemPaths> SystemPaths for Arc<T> {
    fn get_system_path(&self) -> String {
        self.as_ref().get_system_path()
    }

    fn get_state_path(&self) -> String {
        self.as_ref().get_state_path()
    }

    fn get_state_messages_file_path(&self) -> String {
        self.as_ref().get_state_messages_file_path()
    }

    fn get_streams_path(&self) -> String {
        self.as_ref().get_streams_path()
    }

    fn get_runtime_path(&self) -> String {
        self.as_ref().get_runtime_path()
    }
}

pub async fn create_directories(config: &impl SystemPaths) -> Result<(), IggyError> {
    let system_path = config.get_system_path();
    if !Path::new(&system_path).exists() && create_dir_all(&system_path).await.is_err() {
        return Err(IggyError::CannotCreateBaseDirectory(system_path));
    }

    let state_path = config.get_state_path();
    if !Path::new(&state_path).exists() && create_dir_all(&state_path).await.is_err() {
        return Err(IggyError::CannotCreateStateDirectory(state_path));
    }
    let state_log = config.get_state_messages_file_path();
    if !Path::new(&state_log).exists() && overwrite(&state_log).await.is_err() {
        return Err(IggyError::CannotCreateStateDirectory(state_log));
    }

    let streams_path = config.get_streams_path();
    if !Path::new(&streams_path).exists() && create_dir_all(&streams_path).await.is_err() {
        return Err(IggyError::CannotCreateStreamsDirectory(streams_path));
    }

    let runtime_path = config.get_runtime_path();
    if Path::new(&runtime_path).exists() && remove_dir_all(&runtime_path).await.is_err() {
        return Err(IggyError::CannotRemoveRuntimeDirectory(runtime_path));
    }

    if create_dir_all(&runtime_path).await.is_err() {
        return Err(IggyError::CannotCreateRuntimeDirectory(runtime_path));
    }

    info!(
        "Initializing system, data will be stored at: {}",
        config.get_system_path()
    );
    Ok(())
}

async fn overwrite(path: &str) -> Result<File, io::Error> {
    OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(path)
        .await
}

async fn walk_dir(root: impl AsRef<Path>) -> io::Result<Vec<DirEntry>> {
    let root = root.as_ref();

    let metadata = compio::fs::metadata(root).await?;
    if !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "path is not a directory",
        ));
    }

    let mut files = Vec::new();
    let mut directories = Vec::new();
    let mut stack = vec![root.to_path_buf()];

    while let Some(current_dir) = stack.pop() {
        directories.push(DirEntry::dir(current_dir.clone()));

        for entry in std::fs::read_dir(&current_dir)? {
            let entry = entry?;
            let entry_path = entry.path();
            let metadata = compio::fs::symlink_metadata(&entry_path).await?;

            if metadata.is_dir() {
                stack.push(entry_path);
            } else {
                files.push(DirEntry::file(entry_path));
            }
        }
    }

    directories.reverse();
    files.extend(directories);
    Ok(files)
}

async fn remove_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    for entry in walk_dir(path).await? {
        match entry.is_dir {
            true => compio::fs::remove_dir(&entry.path).await?,
            false => compio::fs::remove_file(&entry.path).await?,
        }
    }
    Ok(())
}
