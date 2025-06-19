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

use crate::error::RuntimeError;
use axum::{Json, http::StatusCode, response::IntoResponse};
use serde::Serialize;
use thiserror::Error;
use tracing::error;

#[derive(Debug, Error)]
pub enum ApiError {
    #[error(transparent)]
    Error(#[from] RuntimeError),
    #[error(transparent)]
    JsonError(#[from] serde_json::Error),
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub code: String,
    pub reason: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        match self {
            ApiError::Error(error) => {
                error!("There was an error: {error}");
                let status_code = match error {
                    RuntimeError::MissingIggyCredentials => StatusCode::BAD_REQUEST,
                    RuntimeError::InvalidConfiguration(_) => StatusCode::BAD_REQUEST,
                    RuntimeError::CannotConvertConfiguration => StatusCode::BAD_REQUEST,
                    RuntimeError::SinkNotFound(_) => StatusCode::NOT_FOUND,
                    RuntimeError::SourceNotFound(_) => StatusCode::NOT_FOUND,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                };
                (
                    status_code,
                    Json(ErrorResponse {
                        code: error.as_code().to_owned(),
                        reason: error.to_string(),
                    }),
                )
            }
            ApiError::JsonError(error) => {
                error!("There was a JSON error: {error}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        code: "json_error".to_owned(),
                        reason: error.to_string(),
                    }),
                )
            }
        }
        .into_response()
    }
}
