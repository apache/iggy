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

use crate::context::RuntimeContext;
use axum::body::Body;
use axum::extract::State;
use axum::{
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};
use std::sync::Arc;

const API_KEY_HEADER: &str = "api-key";
const PUBLIC_PATHS: &[&str] = &["/", "/health"];

pub async fn resolve_api_key(
    State(context): State<Arc<RuntimeContext>>,
    request: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    if PUBLIC_PATHS.contains(&request.uri().path()) {
        return Ok(next.run(request).await);
    }

    if context.api_key.is_empty() {
        return Ok(next.run(request).await);
    };

    let Some(api_key) = request
        .headers()
        .get(API_KEY_HEADER)
        .and_then(|value| value.to_str().ok())
    else {
        return Err(StatusCode::UNAUTHORIZED);
    };

    if api_key != context.api_key {
        return Err(StatusCode::UNAUTHORIZED);
    }

    Ok(next.run(request).await)
}
