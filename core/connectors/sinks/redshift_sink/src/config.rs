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

use arrow::datatypes::DataType;
use iggy_connector_sdk::Error;
use secrecy::{ExposeSecret, SecretString};

/// Configuration for the Redshift Sink
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RedshiftSinkConfig {
    #[serde(serialize_with = "iggy_common::serde_secret::serialize_secret")]
    pub connection_string: SecretString,
    pub target_table: String,
    pub batch_size: Option<u32>,
    pub max_connections: Option<u32>,
    pub include_metadata: Option<bool>,
    pub include_checksum: Option<bool>,
    pub include_origin_timestamp: Option<bool>,
    pub payload_format: Option<String>,
    pub verbose_logging: Option<bool>,
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,
    /// aws_access_key_id and aws_secret_access_key MUST be provided
    #[serde(serialize_with = "iggy_common::serde_secret::serialize_secret")]
    pub aws_access_key_id: SecretString,
    #[serde(serialize_with = "iggy_common::serde_secret::serialize_secret")]
    pub aws_secret_access_key: SecretString,
    pub s3_bucket: String,
    pub s3_prefix: String,
    pub s3_endpoint: Option<String>,
    pub aws_region: String,
    /// Offers the option to archive staged S3 files after COPY
    /// Defaults to deletion once COPY completes
    /// Files are moved to different prefix within the same bucket
    pub archive: Option<bool>,
}

impl RedshiftSinkConfig {
    pub fn validate(&self) -> Result<(), Error> {
        let mut errors = String::new();

        if self.connection_string.expose_secret().is_empty() {
            errors.push_str("connection_string is empty\n");
        }

        if self.target_table.is_empty() {
            errors.push_str(", target_table is empty\n");
        }

        if self.s3_bucket.is_empty() {
            errors.push_str(", s3_bucket is empty\n");
        }

        if self.aws_region.is_empty() {
            errors.push_str(", aws_region is empty\n");
        }

        // Validate AWS credentials: access keys must be provided
        let has_access_key = !self.aws_access_key_id.expose_secret().is_empty();

        let has_secret_key = !self.aws_secret_access_key.expose_secret().is_empty();

        if !(has_access_key && has_secret_key) {
            errors.push_str(", aws_access_key_id and aws_secret_access_key are empty\n");
        }

        if !errors.is_empty() {
            Err(Error::InvalidConfigValue(errors))
        } else {
            Ok(())
        }
    }
}

/// This connector supports:
/// 1. Byte -> which has VARBYTE as the Redshift equivalent
/// 2. Text -> which has VARCHAR as the Redshift equivalent
///
/// We dont have Json because we are using parquet as a means to sink ingestion
/// As at the development of this connector there's no direct parquet type that matches JSON
/// For JSON needs Reshshift has SUPER(VARCHAR can be parsed by JSON_PARSE)
#[allow(unused)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PayloadFormat {
    Json,
    Text,
    #[default]
    Varbyte,
}

impl PayloadFormat {
    pub fn from_config(s: Option<&str>) -> Self {
        match s.map(|s| s.to_lowercase()).as_deref() {
            Some("text") | Some("json") => PayloadFormat::Text,
            _ => PayloadFormat::Varbyte,
        }
    }

    pub fn sql_type(&self) -> &'static str {
        match self {
            PayloadFormat::Varbyte => "VARBYTE",
            PayloadFormat::Text | PayloadFormat::Json => "VARCHAR",
        }
    }

    pub fn arrow_type(&self) -> DataType {
        match self {
            PayloadFormat::Varbyte => DataType::Binary,
            PayloadFormat::Text => DataType::Utf8,
            PayloadFormat::Json => DataType::Utf8,
        }
    }
}
