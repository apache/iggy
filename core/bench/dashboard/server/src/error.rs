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

use actix_web::{HttpResponse, ResponseError};
use serde_json::json;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum IggyBenchDashboardServerError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Invalid path: {0}")]
    InvalidPath(String),
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("Invalid JSON: {0}")]
    InvalidJson(String),
    #[error("Invalid UUID format: {0}")]
    InvalidUuid(String),
    #[error("Internal error: {0}")]
    InternalError(String),
}

impl ResponseError for IggyBenchDashboardServerError {
    fn error_response(&self) -> HttpResponse {
        match self {
            IggyBenchDashboardServerError::NotFound(msg) => {
                HttpResponse::NotFound().json(json!({ "error": msg }))
            }
            _ => HttpResponse::InternalServerError().json(json!({ "error": self.to_string() })),
        }
    }
}

impl From<octocrab::Error> for IggyBenchDashboardServerError {
    fn from(err: octocrab::Error) -> Self {
        Self::InternalError(err.to_string())
    }
}

impl From<zip::result::ZipError> for IggyBenchDashboardServerError {
    fn from(err: zip::result::ZipError) -> Self {
        Self::InternalError(err.to_string())
    }
}

impl From<std::env::VarError> for IggyBenchDashboardServerError {
    fn from(err: std::env::VarError) -> Self {
        Self::InternalError(err.to_string())
    }
}
