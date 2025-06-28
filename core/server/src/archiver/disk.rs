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

use crate::archiver::{Archiver, COMPONENT};
use crate::configs::server::DiskArchiverConfig;
use crate::server_error::ArchiverError;
use error_set::ErrContext;
use std::path::Path;
use tokio::fs;
use tracing::{debug, info};

#[derive(Debug)]
pub struct DiskArchiver {
    config: DiskArchiverConfig,
}

impl DiskArchiver {
    #[must_use]
    pub const fn new(config: DiskArchiverConfig) -> Self {
        Self { config }
    }
}

impl Archiver for DiskArchiver {
    async fn init(&self) -> Result<(), ArchiverError> {
        if !Path::new(&self.config.path).exists() {
            info!("Creating disk archiver directory: {}", self.config.path);
            fs::create_dir_all(&self.config.path)
                .await
                .with_error_context(|error| {
                    format!(
                        "ARCHIVER - failed to create directory: {}. {error}",
                        self.config.path
                    )
                })?;
        }
        Ok(())
    }

    async fn is_archived(
        &self,
        file: &str,
        base_directory: Option<String>,
    ) -> Result<bool, ArchiverError> {
        debug!("Checking if file: {file} is archived on disk.");
        let base_directory = base_directory.as_deref().unwrap_or_default();
        let path = Path::new(&self.config.path).join(base_directory).join(file);
        let is_archived = path.exists();
        debug!("File: {file} is archived: {is_archived}");
        Ok(is_archived)
    }

    async fn archive(
        &self,
        files: &[&str],
        base_directory: Option<String>,
    ) -> Result<(), ArchiverError> {
        //TODO: Fixme figure this out, we can't use tokio methods there.
        /* debug!("Archiving files on disk: {:?}", files);
        for file in files {
            debug!("Archiving file: {file}");
            let source = Path::new(file);
            if !source.exists() {
                return Err(ArchiverError::FileToArchiveNotFound {
                    file_path: (*file).to_string(),
                });
            }

            let base_directory = base_directory.as_deref().unwrap_or_default();
            let destination = Path::new(&self.config.path).join(base_directory).join(file);
            let destination_path = destination.to_str().unwrap_or_default().to_owned();
            fs::create_dir_all(destination.parent().expect("Path should have a parent directory"))
                .await
                .with_error_context(|error| {
                    format!("{COMPONENT} (error: {error}) - failed to create file: {file} at path: {destination_path}",)
                })?;
            fs::copy(source, destination).await.with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to copy file: {file} to destination: {destination_path}")
            })?;
            debug!("Archived file: {file} at: {destination_path}");
        }
        */
        Ok(())
    }
}
