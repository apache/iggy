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
use serde::{Deserialize, Serialize};

/// Configuration for the Redshift Sink Connector.
///
/// This connector loads data from Iggy streams into Amazon Redshift using S3 staging,
/// which is the recommended approach for high-volume data loading.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedshiftSinkConfig {
    /// Redshift connection string in PostgreSQL format.
    /// Example: `postgres://user:password@cluster.region.redshift.amazonaws.com:5439/database`
    pub connection_string: String,

    /// Target table name in Redshift. Can include schema prefix.
    /// Example: `public.events` or `analytics.user_actions`
    pub target_table: String,

    /// IAM role ARN for Redshift to access S3. Preferred over access keys.
    /// Example: `arn:aws:iam::123456789012:role/RedshiftS3Access`
    pub iam_role: Option<String>,

    /// S3 bucket name for staging CSV files before COPY.
    pub s3_bucket: String,

    /// S3 key prefix for staged files. Defaults to empty string.
    /// Example: `staging/redshift/`
    pub s3_prefix: Option<String>,

    /// AWS region for the S3 bucket.
    /// Example: `us-east-1`
    pub s3_region: String,

    /// Custom S3 endpoint URL for testing with LocalStack or MinIO.
    /// If not specified, uses the default AWS S3 endpoint.
    /// Example: `http://localhost:4566`
    pub s3_endpoint: Option<String>,

    /// AWS access key ID. Required if IAM role is not specified.
    pub aws_access_key_id: Option<String>,

    /// AWS secret access key. Required if IAM role is not specified.
    pub aws_secret_access_key: Option<String>,

    /// Number of messages to batch before uploading to S3 and executing COPY.
    /// Defaults to 10000.
    pub batch_size: Option<u32>,

    /// Maximum time in milliseconds to wait before flushing a partial batch.
    /// Defaults to 30000 (30 seconds).
    pub flush_interval_ms: Option<u64>,

    /// CSV field delimiter character. Defaults to `,`.
    pub csv_delimiter: Option<char>,

    /// CSV quote character for escaping. Defaults to `"`.
    pub csv_quote: Option<char>,

    /// Number of header rows to skip. Defaults to 0.
    pub ignore_header: Option<u32>,

    /// Maximum number of errors allowed before COPY fails. Defaults to 0.
    pub max_errors: Option<u32>,

    /// Compression format for staged files: `gzip`, `lzop`, `bzip2`, or `none`.
    pub compression: Option<String>,

    /// Whether to delete staged S3 files after successful COPY. Defaults to true.
    pub delete_staged_files: Option<bool>,

    /// Maximum number of database connections. Defaults to 5.
    pub max_connections: Option<u32>,

    /// Maximum number of retry attempts for transient failures. Defaults to 3.
    pub max_retries: Option<u32>,

    /// Initial delay in milliseconds between retries. Uses exponential backoff.
    /// Defaults to 1000.
    pub retry_delay_ms: Option<u64>,

    /// Whether to include Iggy metadata columns (offset, timestamp, stream, topic, partition).
    /// Defaults to false.
    pub include_metadata: Option<bool>,

    /// Whether to auto-create the target table if it doesn't exist. Defaults to false.
    pub auto_create_table: Option<bool>,
}

impl RedshiftSinkConfig {
    /// Validates the configuration and returns an error if invalid.
    pub fn validate(&self) -> Result<(), Error> {
        if self.connection_string.is_empty() {
            return Err(Error::InvalidConfig);
        }

        if self.target_table.is_empty() {
            return Err(Error::InvalidConfig);
        }

        if self.s3_bucket.is_empty() {
            return Err(Error::InvalidConfig);
        }

        if self.s3_region.is_empty() {
            return Err(Error::InvalidConfig);
        }

        // Validate AWS credentials: either IAM role or access keys must be provided
        let has_iam_role = self.iam_role.as_ref().is_some_and(|r| !r.is_empty());
        let has_access_key = self
            .aws_access_key_id
            .as_ref()
            .is_some_and(|k| !k.is_empty());
        let has_secret_key = self
            .aws_secret_access_key
            .as_ref()
            .is_some_and(|s| !s.is_empty());

        if !(has_iam_role || (has_access_key && has_secret_key)) {
            return Err(Error::InvalidConfig);
        }

        // If using access keys, both must be provided
        if (has_access_key && !has_secret_key) || (!has_access_key && has_secret_key) {
            return Err(Error::InvalidConfig);
        }

        // Validate compression if specified
        if let Some(compression) = &self.compression {
            let valid = ["gzip", "lzop", "bzip2", "none"];
            if !valid.contains(&compression.to_lowercase().as_str()) {
                return Err(Error::InvalidConfig);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config() -> RedshiftSinkConfig {
        RedshiftSinkConfig {
            connection_string: "postgres://user:pass@host:5439/db".to_string(),
            target_table: "test_table".to_string(),
            iam_role: Some("arn:aws:iam::123:role/Test".to_string()),
            s3_bucket: "bucket".to_string(),
            s3_prefix: None,
            s3_region: "us-east-1".to_string(),
            s3_endpoint: None,
            aws_access_key_id: None,
            aws_secret_access_key: None,
            batch_size: None,
            flush_interval_ms: None,
            csv_delimiter: None,
            csv_quote: None,
            ignore_header: None,
            max_errors: None,
            compression: None,
            delete_staged_files: None,
            max_connections: None,
            max_retries: None,
            retry_delay_ms: None,
            include_metadata: None,
            auto_create_table: None,
        }
    }

    #[test]
    fn test_valid_config_with_iam_role() {
        let config = valid_config();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_valid_config_with_access_keys() {
        let mut config = valid_config();
        config.iam_role = None;
        config.aws_access_key_id = Some("AKIAIOSFODNN7EXAMPLE".to_string());
        config.aws_secret_access_key = Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_invalid_empty_connection_string() {
        let mut config = valid_config();
        config.connection_string = String::new();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_invalid_empty_table() {
        let mut config = valid_config();
        config.target_table = String::new();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_invalid_empty_bucket() {
        let mut config = valid_config();
        config.s3_bucket = String::new();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_invalid_compression() {
        let mut config = valid_config();
        config.compression = Some("invalid".to_string());
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_valid_compression_options() {
        for comp in ["gzip", "GZIP", "lzop", "bzip2", "none"] {
            let mut config = valid_config();
            config.compression = Some(comp.to_string());
            assert!(
                config.validate().is_ok(),
                "compression '{}' should be valid",
                comp
            );
        }
    }
}
