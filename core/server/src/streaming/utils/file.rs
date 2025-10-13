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

use compio::fs::{File, OpenOptions, remove_file};
use std::path::Path;

pub async fn open(path: &str) -> Result<File, std::io::Error> {
    OpenOptions::new().read(true).open(path).await
}

pub async fn append(path: &str) -> Result<(File, u64), std::io::Error> {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(path)
        .await?;
    let position = file.metadata().await?.len();
    Ok((file, position))
}

pub async fn overwrite(path: &str) -> Result<File, std::io::Error> {
    OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(path)
        .await
}

pub async fn remove(path: &str) -> Result<(), std::io::Error> {
    remove_file(path).await
}

pub async fn rename(old_path: &str, new_path: &str) -> Result<(), std::io::Error> {
    compio::fs::rename(Path::new(old_path), Path::new(new_path)).await
}

pub async fn exists(path: &str) -> Result<bool, std::io::Error> {
    std::fs::exists(path)
}
