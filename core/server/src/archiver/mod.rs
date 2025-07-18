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

pub mod disk;
pub mod s3;

use crate::configs::server::{DiskArchiverConfig, S3ArchiverConfig};
use crate::server_error::ArchiverError;
use derive_more::Display;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::future::Future;
use std::str::FromStr;

use crate::archiver::disk::DiskArchiver;
use crate::archiver::s3::S3Archiver;

pub(crate) struct PutObjectStreamResponse {
    status: u16,
    total_size: usize,
}
pub(crate) type Region = String;
pub const COMPONENT: &str = "ARCHIVER";

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Default, Display, Copy, Clone)]
#[serde(rename_all = "lowercase")]
pub enum ArchiverKindType {
    #[default]
    #[display("disk")]
    Disk,
    #[display("s3")]
    S3,
}

impl FromStr for ArchiverKindType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "disk" => Ok(Self::Disk),
            "s3" => Ok(Self::S3),
            _ => Err(format!("Unknown archiver kind: {s}")),
        }
    }
}

pub trait Archiver {
    fn init(&self) -> impl Future<Output = Result<(), ArchiverError>>;
    fn is_archived(
        &self,
        file: &str,
        base_directory: Option<String>,
    ) -> impl Future<Output = Result<bool, ArchiverError>>;
    fn archive(
        &self,
        files: &[&str],
        base_directory: Option<String>,
    ) -> impl Future<Output = Result<(), ArchiverError>>;
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)] // TODO(hubcio): consider `Box`ing
pub enum ArchiverKind {
    Disk(DiskArchiver),
    S3(S3Archiver),
}

impl ArchiverKind {
    #[must_use]
    pub const fn get_disk_archiver(config: DiskArchiverConfig) -> Self {
        Self::Disk(DiskArchiver::new(config))
    }

    /// Creates an S3 archiver.
    ///
    /// # Errors
    ///
    /// Returns an error if the S3 archiver cannot be initialized.
    pub fn get_s3_archiver(config: S3ArchiverConfig) -> Result<Self, ArchiverError> {
        let archiver = S3Archiver::new(config)?;
        Ok(Self::S3(archiver))
    }

    /// Initializes the archiver.
    ///
    /// # Errors
    ///
    /// Returns an error if the archiver cannot be initialized.
    pub async fn init(&self) -> Result<(), ArchiverError> {
        match self {
            Self::Disk(a) => a.init().await,
            Self::S3(a) => a.init().await,
        }
    }

    /// Checks if a file is archived.
    ///
    /// # Errors
    ///
    /// Returns an error if the check cannot be performed.
    pub async fn is_archived(
        &self,
        file: &str,
        base_directory: Option<String>,
    ) -> Result<bool, ArchiverError> {
        match self {
            Self::Disk(d) => d.is_archived(file, base_directory).await,
            Self::S3(d) => d.is_archived(file, base_directory).await,
        }
    }

    /// Archives the specified files.
    ///
    /// # Errors
    ///
    /// Returns an error if the files cannot be archived.
    pub async fn archive(
        &self,
        files: &[&str],
        base_directory: Option<String>,
    ) -> Result<(), ArchiverError> {
        match self {
            Self::Disk(d) => d.archive(files, base_directory).await,
            Self::S3(d) => d.archive(files, base_directory).await,
        }
    }
}
