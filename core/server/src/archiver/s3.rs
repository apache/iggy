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

use crate::archiver::{Archiver, Region, COMPONENT};
use crate::configs::server::S3ArchiverConfig;
use crate::io;
use crate::server_error::ArchiverError;
use crate::streaming::utils::{file, PooledBuffer};
use compio::buf::{IntoInner, IoBuf};
use compio::io::{copy, AsyncRead, AsyncReadExt};
use error_set::ErrContext;
use rusty_s3::actions::{CompleteMultipartUpload, CreateMultipartUpload, UploadPart};
use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};
use std::io::Cursor;
use std::path::Path;
use std::time::Duration;
use compio::fs;
use tracing::{debug, error, info};

#[derive(Debug)]
pub struct S3Archiver {
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
        let credentials = Credentials::new(
            &config.key_id,
            &config.key_secret,
        );
        let region: Region =
            config.region.map_or_else(String::new, |r| r);
        let endpoint = config.endpoint.map_or_else(String::new, |e| e).parse().expect("Endpoint should be valid URL");
        let path_style = UrlStyle::VirtualHost;
        let name = config.bucket;


        let bucket = Bucket::new(endpoint, path_style, name, region)?;
        //TODO: Make this configurable ?
        let expiration = Duration::from_secs(1);
        Ok(Self {
            bucket,
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

    async fn put_object_stream(&self, reader: &mut impl AsyncReadExt, destination_path: &str) -> Result<(), ArchiverError> {
        let buf = PooledBuffer::with_capacity(CHUNK_SIZE);
        let (result, chunk) = reader.read_exact(buf.slice(..CHUNK_SIZE)).await.into();
        result?;
        let buf = chunk.into_inner();
        if buf.len() < CHUNK_SIZE {
            // Normal upload
            let action = self.bucket.put_object(Some(&self.credentials), destination_path);
            // TODO: use `cypher` client to send the request.
            return Ok(());
        }

        let action = self.bucket.create_multipart_upload(Some(&self.credentials), destination_path);
        let url = action.sign(self.expiration);
        let response = CreateMultipartUpload::parse_response(String::new()).expect("Failed to parse response");
        let upload_id = response.upload_id();
        let part_number = 0;

        let mut handles = Vec::new();
        let mut total_size = 0;

        let action = self.bucket.upload_part(Some(&self.credentials), destination_path, part_number, upload_id);
        let url = action.sign(self.expiration);
        // TODO: use `cypher` client to send the request.
        // put bytes in the body.
        total_size += buf.len();
        handles.push(());
        let mut buf = buf;
        loop {
            let (result, chunk) = reader.read_exact(buf.slice(..CHUNK_SIZE)).await.into();
            result?;
            buf = chunk.into_inner();
            total_size += buf.len();
            let done = buf.len() < CHUNK_SIZE;
            let action = self.bucket.upload_part(Some(&self.credentials), destination_path, part_number, upload_id);
            let url = action.sign(self.expiration);
            // TODO: use `cypher` client to send the request.
            // put bytes in the body.

            handles.push(());
            if done {
                break;
            }
        }
        // join all handles.
        for handle in handles {
            // get response status
            // if any of those is != 200, we should abort the upload.
        }

        // TODO: Send using `cypher` client.
        let etags: Vec<&'static str> = Vec::new();
        let action = self.bucket.complete_multipart_upload(Some(&self.credentials), destination_path, upload_id, etags.into_iter());
        let url = action.sign(self.expiration);
        let body = CompleteMultipartUpload::body(action);

        Ok(())
    }
}

impl Archiver for S3Archiver {
    async fn init(&self) -> Result<(), ArchiverError> {
        let action = self.bucket.list_objects_v2(Some(&self.credentials));
        let url = action.sign(self.expiration);
        // TODO: Send request to S3 using the `cypher` client.
        /*
        //let response = self.bucket.list("/".to_string(), None).await;
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
        */
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
        let mut object_tagging = self.bucket.get_object(Some(&self.credentials), &destination_path);
        object_tagging.query_mut().insert("tagging", "");
        // Filter all the tags.
        // TODO: Send request to S3 using the `cypher` client.
        /*
        let response = self.bucket.get_object_tagging(destination_path).await;
        if response.is_err() {
            debug!("File: {file} is not archived on S3.");
            return Ok(false);
        }

        let (_, status) = response.expect("Response should be valid if not an error");
        if status == 200 {
            debug!("File: {file} is archived on S3.");
            return Ok(true);
        }

        debug!("File: {file} is not archived on S3.");
        */
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
            let response = self
                .put_object_stream(&mut reader, &destination_path)
                .await;
            if let Err(error) = response {
                error!("Cannot archive file: {path} on S3: {}", error);
                fs::remove_file(&source).await.with_error_context(|error| {
                    format!("{COMPONENT} (error: {error}) - failed to remove temporary file: {source} after S3 failure")
                })?;
                return Err(ArchiverError::CannotArchiveFile {
                    file_path: (*path).to_string(),
                });
            }

            if response.is_ok() {
                debug!("Archived file: {path} on S3.");
                fs::remove_file(&source).await.with_error_context(|error| {
                    format!("{COMPONENT} (error: {error}) - failed to remove temporary file: {source} after successful archive")
                })?;
                continue;
            }

            error!("Cannot archive file: {path} on S3");
            // TODO: Return response status from the S3 client.
            //error!("Cannot archive file: {path} on S3, received an invalid status code: {status}.");
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
