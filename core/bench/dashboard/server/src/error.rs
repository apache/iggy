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
use bench_dashboard_shared::config::ConfigError;
use serde_json::json;
use std::path::PathBuf;
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
    #[error("Bad request: {0}")]
    BadRequest(String),
    #[error("Internal error: {0}")]
    InternalError(String),
    #[error("Can't read the dashboard configuration file '{}': {source}", path.display())]
    ConfigRead {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("Can't parse the dashboard configuration file '{}': {source}", path.display())]
    ConfigParse {
        path: PathBuf,
        source: toml::de::Error,
    },
    #[error("Invalid dashboard configuration file '{}': {source}", path.display())]
    ConfigInvalid { path: PathBuf, source: ConfigError },
}

impl ResponseError for IggyBenchDashboardServerError {
    fn error_response(&self) -> HttpResponse {
        match self {
            IggyBenchDashboardServerError::NotFound(msg) => {
                HttpResponse::NotFound().json(json!({ "error": msg }))
            }
            IggyBenchDashboardServerError::InvalidUuid(msg)
            | IggyBenchDashboardServerError::BadRequest(msg) => {
                HttpResponse::BadRequest().json(json!({ "error": msg }))
            }
            _ => HttpResponse::InternalServerError().json(json!({ "error": self.to_string() })),
        }
    }
}
