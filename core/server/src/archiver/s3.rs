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

use crate::archiver::{Archiver, COMPONENT, PutObjectStreamResponse, Region};
use crate::configs::server::S3ArchiverConfig;
use crate::io;
use crate::server_error::ArchiverError;
use crate::streaming::utils::{PooledBuffer, file};
use bytes::BytesMut;
use compio::buf::{IntoInner, IoBuf};
use compio::fs;
use compio::io::{AsyncRead, AsyncReadExt, copy};
use cyper::{Client, ClientBuilder};
use error_set::ErrContext;
use futures::future::try_join_all;
use rusty_s3::actions::{CompleteMultipartUpload, CreateMultipartUpload, GetObject, UploadPart};
use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};
use std::collections::HashMap;
use std::io::Cursor;
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info};

#[derive(Debug, Clone)]
pub struct S3Archiver {
    client: Client,
    bucket: Bucket,
    credentials: Credentials,
    tmp_upload_dir: String,
    expiration: Duration,
}

pub const CHUNK_SIZE: usize = 8_388_608; // 8 Mebibytes, min is 5 (5_242_880);

impl S3Archiver {
    /// Creates a new S3 archiver.
    ///
    /// # Errors
    ///
    /// Returns an error if the S3 client cannot be initialized or credentials are invalid.
    pub fn new(config: S3ArchiverConfig) -> Result<Self, ArchiverError> {
        let credentials = Credentials::new(&config.key_id, &config.key_secret);
        let region: Region = config.region.map_or_else(String::new, |r| r);
        let endpoint = config
            .endpoint
            .map_or_else(String::new, |e| e)
            .parse()
            .expect("Endpoint should be valid URL");
        let path_style = UrlStyle::VirtualHost;
        let name = config.bucket;

        let bucket = Bucket::new(endpoint, path_style, name, region)?;
        //TODO: Make this configurable ?
        let expiration = Duration::from_secs(60);
        let client = ClientBuilder::new().use_rustls_default().build();
        Ok(Self {
            bucket,
            client,
            credentials,
            expiration,
            tmp_upload_dir: config.tmp_upload_dir,
        })
    }

    async fn copy_file_to_tmp(&self, path: &str) -> Result<String, ArchiverError> {
        debug!(
            "Copying file: {path} to temporary S3 upload directory: {}",
            self.tmp_upload_dir
        );
        let source = file::open(&path).await?;
        let mut source = std::io::Cursor::new(source);
        let destination = Path::new(&self.tmp_upload_dir).join(path);
        let destination_path = self.tmp_upload_dir.to_owned();
        debug!("Creating temporary S3 upload directory: {destination_path}");
        fs::create_dir_all(destination.parent().expect("Path should have a parent directory"))
            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to create temporary S3 upload directory for path: {destination_path}"
                )
            })?;
        let destination = file::open(&self.tmp_upload_dir).await?;
        let mut destination = std::io::Cursor::new(destination);
        debug!("Copying file: {path} to temporary S3 upload path: {destination_path}");
        copy(&mut source, &mut destination).await.with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to copy file: {path} to temporary S3 upload path: {destination_path}")
        })?;
        debug!("File: {path} copied to temporary S3 upload path: {destination_path}");
        Ok(destination_path)
    }

    async fn put_object_stream(
        &self,
        reader: &mut impl AsyncReadExt,
        destination_path: &str,
    ) -> Result<PutObjectStreamResponse, ArchiverError> {
        let buf = BytesMut::with_capacity(CHUNK_SIZE);
        let (result, chunk) = reader.read_exact(buf.slice(..CHUNK_SIZE)).await.into();
        result?;
        let buf = chunk.into_inner().freeze();
        if buf.len() < CHUNK_SIZE {
            // Normal upload
            let action = self
                .bucket
                .put_object(Some(&self.credentials), destination_path);
            let url = action.sign(self.expiration);
            let buf_len = buf.len();
            let response = self.client.put(url)?.body(buf).send().await?;
            return Ok(PutObjectStreamResponse {
                status: response.status().as_u16(),
                total_size: buf_len,
            });
        }

        let action = self
            .bucket
            .create_multipart_upload(Some(&self.credentials), destination_path);
        let url = action.sign(self.expiration);
        let response = self.client.post(url)?.send().await?;
        let body = response.text().await?;
        let response =
            CreateMultipartUpload::parse_response(&body).expect("Failed to parse response");
        let upload_id = response.upload_id();

        let mut total_size = 0;
        let mut part_number = 0;
        let mut handles = Vec::new();
        let action = self.bucket.upload_part(
            Some(&self.credentials),
            destination_path,
            part_number,
            upload_id,
        );
        let url = action.sign(self.expiration);
        let buf_size = buf.len();
        let response = self.client.put(url)?.body(buf).send();
        total_size += buf_size;
        part_number += 1;
        handles.push(response);
        loop {
            let buf = BytesMut::with_capacity(CHUNK_SIZE);
            let (result, chunk) = reader.read_exact(buf.slice(..CHUNK_SIZE)).await.into();
            result?;
            let buf = chunk.into_inner().freeze();
            let buf_len = buf.len();
            let done = buf_len < CHUNK_SIZE;
            let action = self.bucket.upload_part(
                Some(&self.credentials),
                destination_path,
                part_number,
                upload_id,
            );
            let url = action.sign(self.expiration);
            let response = self.client.put(url)?.body(buf).send();
            part_number += 1;
            total_size += buf_len;

            handles.push(response);
            if done {
                break;
            }
        }
        let responses = try_join_all(handles).await?;
        let mut etags = Vec::new();
        for response in responses {
            let status_code = response.status().as_u16();
            if status_code == 200 {
                let etag = response
                    .headers()
                    .get("etag")
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_owned();
                etags.push(etag);
            }
        }
        let action = self.bucket.complete_multipart_upload(
            Some(&self.credentials),
            destination_path,
            upload_id,
            etags.iter().map(|s| s.as_str()),
        );
        let url = action.sign(self.expiration);
        let body = CompleteMultipartUpload::body(action);
        let response = self.client.post(url)?.body(body).send().await?;
        Ok(PutObjectStreamResponse {
            status: response.status().as_u16(),
            total_size,
        })
    }
}

impl Archiver for S3Archiver {
    async fn init(&self) -> Result<(), ArchiverError> {
        let action = self.bucket.list_objects_v2(Some(&self.credentials));
        let url = action.sign(self.expiration);
        let response = self.client.get(url)?.send().await;
        if let Err(error) = response {
            error!("Cannot initialize S3 archiver: {error}");
            return Err(ArchiverError::CannotInitializeS3Archiver);
        }

        if Path::new(&self.tmp_upload_dir).exists() {
            info!(
                "Removing existing S3 archiver temporary upload directory: {}",
                self.tmp_upload_dir
            );
            io::fs_utils::remove_dir_all(&self.tmp_upload_dir).await?;
        }
        info!(
            "Creating S3 archiver temporary upload directory: {}",
            self.tmp_upload_dir
        );
        fs::create_dir_all(&self.tmp_upload_dir).await?;
        Ok(())
    }

    async fn is_archived(
        &self,
        file: &str,
        base_directory: Option<String>,
    ) -> Result<bool, ArchiverError> {
        debug!("Checking if file: {file} is archived on S3.");
        let base_directory = base_directory.as_deref().unwrap_or_default();
        let destination = Path::new(&base_directory).join(file);
        let destination_path = destination.to_str().unwrap_or_default().to_owned();
        let mut object_tagging = self
            .bucket
            .get_object(Some(&self.credentials), &destination_path);
        object_tagging.query_mut().insert("tagging", "");
        let url = object_tagging.sign(self.expiration);
        let response = self.client.get(url)?.send().await;
        if response.is_err() {
            debug!("File: {file} is not archived on S3.");
            return Ok(false);
        }

        let response = response.expect("Response should be valid if not an error");
        if response.status() == 200 {
            debug!("File: {file} is archived on S3.");
            return Ok(true);
        }

        debug!("File: {file} is not archived on S3.");
        Ok(false)
    }

    async fn archive(
        &self,
        files: &[&str],
        base_directory: Option<String>,
    ) -> Result<(), ArchiverError> {
        for path in files {
            if !Path::new(path).exists() {
                return Err(ArchiverError::FileToArchiveNotFound {
                    file_path: (*path).to_string(),
                });
            }

            let source = self.copy_file_to_tmp(path).await?;
            debug!("Archiving file: {source} on S3.");
            let file = file::open(&source)
                .await
                .with_error_context(|error| format!("{COMPONENT} (error: {error}) - failed to open source file: {source} for archiving"))?;
            let mut reader = std::io::Cursor::new(file);
            let base_directory = base_directory.as_deref().unwrap_or_default();
            let destination = Path::new(&base_directory).join(path);
            let destination_path = destination.to_str().unwrap_or_default().to_owned();
            // Egh.. multi part upload.
            let response = self.put_object_stream(&mut reader, &destination_path).await;
            if let Err(error) = response {
                error!("Cannot archive file: {path} on S3: {}", error);
                fs::remove_file(&source).await.with_error_context(|error| {
                    format!("{COMPONENT} (error: {error}) - failed to remove temporary file: {source} after S3 failure")
                })?;
                return Err(ArchiverError::CannotArchiveFile {
                    file_path: (*path).to_string(),
                });
            }
            let PutObjectStreamResponse { status, total_size } = response.unwrap();
            if status == 200 {
                debug!("Archived file: {path} on S3.");
                fs::remove_file(&source).await.with_error_context(|error| {
                    format!("{COMPONENT} (error: {error}) - failed to remove temporary file: {source} after successful archive")
                })?;
                continue;
            }

            error!("Cannot archive file: {path} on S3, received an invalid status code: {status}.");
            fs::remove_file(&source).await.with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to remove temporary file: {source} after invalid status code")
            })?;
            return Err(ArchiverError::CannotArchiveFile {
                file_path: (*path).to_string(),
            });
        }
        Ok(())
    }
}
