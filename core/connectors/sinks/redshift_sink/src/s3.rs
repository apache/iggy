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

use iggy_connector_sdk::Error;
use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::region::Region;
use tracing::{error, info};
use uuid::Uuid;

/// S3 uploader for staging CSV files before Redshift COPY.
#[derive(Debug)]
pub struct S3Uploader {
    bucket: Box<Bucket>,
    prefix: String,
}

impl S3Uploader {
    /// Creates a new S3 uploader with the specified configuration.
    pub fn new(
        bucket_name: &str,
        prefix: &str,
        region: &str,
        access_key_id: Option<&str>,
        secret_access_key: Option<&str>,
        endpoint: Option<&str>,
    ) -> Result<Self, Error> {
        let region = match endpoint {
            Some(ep) => Region::Custom {
                region: region.to_string(),
                endpoint: ep.to_string(),
            },
            None => Region::Custom {
                region: region.to_string(),
                endpoint: format!("https://s3.{}.amazonaws.com", region),
            },
        };

        let credentials = match (access_key_id, secret_access_key) {
            (Some(key_id), Some(secret)) => {
                Credentials::new(Some(key_id), Some(secret), None, None, None).map_err(|e| {
                    error!("Failed to create S3 credentials: {}", e);
                    Error::InitError(format!("Invalid AWS credentials: {e}"))
                })?
            }
            _ => {
                // Use default credential chain (environment variables, instance profile, etc.)
                Credentials::default().map_err(|e| {
                    error!("Failed to load default S3 credentials: {}", e);
                    Error::InitError(format!("Failed to load AWS credentials: {e}"))
                })?
            }
        };

        let mut bucket = Bucket::new(bucket_name, region, credentials).map_err(|e| {
            error!("Failed to create S3 bucket client: {}", e);
            Error::InitError(format!("Failed to initialize S3 bucket: {e}"))
        })?;

        // Use path-style access for custom endpoints (LocalStack, MinIO, etc.)
        if endpoint.is_some() {
            bucket = bucket.with_path_style();
        }

        let prefix = prefix.trim_end_matches('/').to_string();

        Ok(S3Uploader { bucket, prefix })
    }

    /// Uploads CSV data to S3 and returns the S3 key.
    pub async fn upload_csv(&self, data: &[u8]) -> Result<String, Error> {
        let file_id = Uuid::new_v4();
        let key = if self.prefix.is_empty() {
            format!("{}.csv", file_id)
        } else {
            format!("{}/{}.csv", self.prefix, file_id)
        };

        let response = self.bucket.put_object(&key, data).await.map_err(|e| {
            error!("Failed to upload to S3 key '{}': {}", key, e);
            Error::Storage(format!("S3 upload failed: {e}"))
        })?;

        if response.status_code() != 200 {
            error!(
                "S3 upload returned status {}: {}",
                response.status_code(),
                String::from_utf8_lossy(response.as_slice())
            );
            return Err(Error::Storage(format!(
                "S3 upload failed with status {}",
                response.status_code()
            )));
        }

        info!(
            "Uploaded {} bytes to s3://{}/{}",
            data.len(),
            self.bucket.name(),
            key
        );
        Ok(key)
    }

    /// Deletes a file from S3 by key.
    pub async fn delete_file(&self, key: &str) -> Result<(), Error> {
        let response = self.bucket.delete_object(key).await.map_err(|e| {
            error!("Failed to delete S3 object '{}': {}", key, e);
            Error::Storage(format!("S3 delete failed: {e}"))
        })?;

        if response.status_code() != 204 && response.status_code() != 200 {
            error!(
                "S3 delete returned unexpected status {}: {}",
                response.status_code(),
                String::from_utf8_lossy(response.as_slice())
            );
            return Err(Error::Storage(format!(
                "S3 delete failed with status {}",
                response.status_code()
            )));
        }

        info!("Deleted s3://{}/{}", self.bucket.name(), key);
        Ok(())
    }

    /// Checks if the bucket is accessible by performing a HEAD request.
    #[allow(dead_code)]
    pub async fn check_connectivity(&self) -> Result<(), Error> {
        self.bucket.head_object("/").await.map_err(|e| {
            error!("S3 connectivity check failed: {}", e);
            Error::Connection(format!("Cannot access S3 bucket: {e}"))
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_uploader_creation_with_credentials() {
        let result = S3Uploader::new(
            "test-bucket",
            "prefix/",
            "us-east-1",
            Some("AKIAIOSFODNN7EXAMPLE"),
            Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"),
            None,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_prefix_normalization() {
        let uploader = S3Uploader::new(
            "test-bucket",
            "staging/redshift/",
            "us-east-1",
            Some("AKIAIOSFODNN7EXAMPLE"),
            Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"),
            None,
        )
        .unwrap();

        assert_eq!(uploader.prefix, "staging/redshift");
    }

    #[test]
    fn test_empty_prefix() {
        let uploader = S3Uploader::new(
            "test-bucket",
            "",
            "us-east-1",
            Some("AKIAIOSFODNN7EXAMPLE"),
            Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"),
            None,
        )
        .unwrap();

        assert_eq!(uploader.prefix, "");
    }
}
