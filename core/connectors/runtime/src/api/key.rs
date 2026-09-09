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

use super::error::ApiError;
use crate::configs::connectors::ConnectorKey;
use axum::extract::{FromRequestParts, Path};
use axum::http::request::Parts;
use serde::Deserialize;

/// The `{key}` route segment parsed as a `ConnectorKey`. Deserializing
/// `Path<ConnectorKey>` directly would answer a bad key with axum's plain-text
/// rejection; going through `ApiError` keeps the `{code, reason}` envelope the
/// rest of the API returns.
pub struct KeyPath(pub ConnectorKey);

#[derive(Deserialize)]
struct KeyParam {
    key: String,
}

impl<S: Send + Sync> FromRequestParts<S> for KeyPath {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let Path(KeyParam { key }) = Path::from_request_parts(parts, state).await?;
        Ok(Self(ConnectorKey::try_from(key)?))
    }
}
