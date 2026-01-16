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

use thiserror::Error;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),
    #[error("Failed to serialize topic metadata")]
    FailedToSerializeTopicMetadata,
    #[error("Failed to serialize messages metadata")]
    FailedToSerializeMessagesMetadata,
    #[error("Failed to serialize raw messages")]
    FailedToSerializeRawMessages,
    #[error("Failed to serialize headers")]
    FailedToSerializeHeaders,
    #[error("Connector SDK error")]
    ConnectorSdkError(#[from] iggy_connector_sdk::Error),
    #[error("Iggy client error")]
    IggyClient(#[from] iggy::prelude::ClientError),
    #[error("Iggy error")]
    IggyError(#[from] iggy::prelude::IggyError),
    #[error("Missing Iggy credentials")]
    MissingIggyCredentials,
    #[error("Missing TLS certificate file")]
    MissingTlsCertificateFile,
    #[error("JSON error")]
    JsonError(#[from] serde_json::Error),
    #[error("Sink not found with key: {0}")]
    SinkNotFound(String),
    #[error("Sink config not found with key: {0}, version: {1}")]
    SinkConfigNotFound(String, u64),
    #[error("Source not found with key: {0}")]
    SourceNotFound(String),
    #[error("Source config not found with key: {0}, version: {1}")]
    SourceConfigNotFound(String, u64),
    #[error("Cannot convert configuration")]
    CannotConvertConfiguration,
    #[error("IO operation failed with error: {0:?}")]
    IoError(#[from] std::io::Error),
    #[error("HTTP request failed: {0}")]
    HttpRequestFailed(String),
    #[error("Token file not found: {0}")]
    TokenFileNotFound(String),
    #[error("Failed to read token file '{0}': {1}")]
    TokenFileReadError(String, String),
    #[error("Token file is empty: {0}")]
    TokenFileEmpty(String),
}

impl RuntimeError {
    pub fn as_code(&self) -> &'static str {
        match self {
            RuntimeError::SinkNotFound(_) => "sink_not_found",
            RuntimeError::SinkConfigNotFound(_, _) => "sink_config_not_found",
            RuntimeError::SourceNotFound(_) => "source_not_found",
            RuntimeError::SourceConfigNotFound(_, _) => "source_config_not_found",
            RuntimeError::MissingIggyCredentials => "invalid_configuration",
            RuntimeError::InvalidConfiguration(_) => "invalid_configuration",
            RuntimeError::HttpRequestFailed(_) => "http_request_failed",
            RuntimeError::TokenFileNotFound(_) => "invalid_configuration",
            RuntimeError::TokenFileReadError(_, _) => "invalid_configuration",
            RuntimeError::TokenFileEmpty(_) => "invalid_configuration",
            _ => "error",
        }
    }
}
