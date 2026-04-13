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

use crate::S3SinkConfig;
use iggy_connector_sdk::Error;
use s3::creds::Credentials;
use s3::{Bucket, Region};
use tracing::info;

pub fn validate_credentials(config: &S3SinkConfig) -> Result<(), Error> {
    match (&config.access_key_id, &config.secret_access_key) {
        (Some(_), Some(_)) | (None, None) => Ok(()),
        _ => Err(Error::InvalidConfigValue(
            "Partially configured credentials. You must provide both access_key_id \
             and secret_access_key, or omit both."
                .to_owned(),
        )),
    }
}

pub async fn create_bucket(config: &S3SinkConfig) -> Result<Box<Bucket>, Error> {
    validate_credentials(config)?;

    let credentials = match (&config.access_key_id, &config.secret_access_key) {
        (Some(key), Some(secret)) => {
            let redacted_key = key.chars().take(3).collect::<String>();
            info!("Using explicit S3 credentials (access key: {redacted_key}***)");
            Credentials::new(Some(key), Some(secret), None, None, None).map_err(|e| {
                Error::InitError(format!("Failed to create S3 credentials: {e}"))
            })?
        }
        _ => {
            info!("No explicit credentials provided, using default credential chain (env vars / instance profile)");
            Credentials::default()
                .map_err(|e| Error::InitError(format!("Failed to load default S3 credentials: {e}")))?
        }
    };

    let region = match &config.endpoint {
        Some(endpoint) => {
            info!("Using custom S3 endpoint: {endpoint}");
            Region::Custom {
                region: config.region.clone(),
                endpoint: endpoint.clone(),
            }
        }
        None => config.region.parse::<Region>().map_err(|e| {
            Error::InvalidConfigValue(format!("Invalid S3 region '{}': {e}", config.region))
        })?,
    };

    let mut bucket = Bucket::new(&config.bucket, region, credentials)
        .map_err(|e| Error::InitError(format!("Failed to create S3 bucket handle: {e}")))?;

    let use_path_style = config.path_style.unwrap_or(config.endpoint.is_some());
    if use_path_style {
        bucket.set_path_style();
    }

    Ok(bucket)
}

pub async fn verify_bucket(bucket: &Bucket) -> Result<(), Error> {
    bucket
        .head_object("/")
        .await
        .map_err(|e| {
            Error::InitError(format!(
                "S3 bucket '{}' connectivity check failed: {e}",
                bucket.name
            ))
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FileRotation, default_max_file_size, default_output_format, default_path_template};

    fn base_config() -> S3SinkConfig {
        S3SinkConfig {
            bucket: "test".to_string(),
            region: "us-east-1".to_string(),
            prefix: None,
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            path_template: default_path_template(),
            file_rotation: FileRotation::Size,
            max_file_size: default_max_file_size(),
            max_messages_per_file: None,
            output_format: default_output_format(),
            include_metadata: true,
            include_headers: false,
            max_retries: None,
            retry_delay: None,
            path_style: None,
        }
    }

    #[test]
    fn validate_both_credentials_present() {
        let config = S3SinkConfig {
            access_key_id: Some("AKIAIOSFODNN7EXAMPLE".to_string()),
            secret_access_key: Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
            ..base_config()
        };
        assert!(validate_credentials(&config).is_ok());
    }

    #[test]
    fn validate_no_credentials() {
        let config = base_config();
        assert!(validate_credentials(&config).is_ok());
    }

    #[test]
    fn validate_partial_access_key_only() {
        let config = S3SinkConfig {
            access_key_id: Some("AKIAIOSFODNN7EXAMPLE".to_string()),
            secret_access_key: None,
            ..base_config()
        };
        assert!(validate_credentials(&config).is_err());
    }

    #[test]
    fn validate_partial_secret_key_only() {
        let config = S3SinkConfig {
            access_key_id: None,
            secret_access_key: Some("secret".to_string()),
            ..base_config()
        };
        assert!(validate_credentials(&config).is_err());
    }
}
