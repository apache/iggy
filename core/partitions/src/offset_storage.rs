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

use compio::{
    fs::{OpenOptions, create_dir_all, remove_file},
    io::AsyncWriteAtExt,
};
use iggy_common::IggyError;
use std::path::Path;

pub async fn persist_offset(path: &str, offset: u64, enforce_fsync: bool) -> Result<(), IggyError> {
    if let Some(parent) = Path::new(path).parent()
        && !parent.exists()
    {
        create_dir_all(parent).await.map_err(|_| {
            IggyError::CannotCreateConsumerOffsetsDirectory(parent.display().to_string())
        })?;
    }

    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .await
        .map_err(|_| IggyError::CannotOpenConsumerOffsetsFile(path.to_owned()))?;
    let buf = offset.to_le_bytes();
    file.write_all_at(buf, 0)
        .await
        .0
        .map_err(|_| IggyError::CannotWriteToFile)?;

    if enforce_fsync {
        file.sync_data()
            .await
            .map_err(|_| IggyError::CannotWriteToFile)?;
    }

    Ok(())
}

pub async fn delete_persisted_offset(path: &str) -> Result<(), IggyError> {
    if !Path::new(path).exists() {
        return Ok(());
    }

    remove_file(path)
        .await
        .map_err(|_| IggyError::CannotDeleteConsumerOffsetFile(path.to_owned()))
}
