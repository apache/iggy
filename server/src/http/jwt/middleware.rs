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

use crate::http::jwt::json_web_token::Identity;
use crate::http::shared::{AppState, RequestDetails};
use axum::body::Body;
use axum::{
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};
use error_set::ErrContext;
use std::sync::Arc;

const COMPONENT: &str = "JWT_MIDDLEWARE";
const AUTHORIZATION: &str = "authorization";
const BEARER: &str = "Bearer ";
const UNAUTHORIZED: StatusCode = StatusCode::UNAUTHORIZED;

const PUBLIC_PATHS: &[&str] = &[
    "/",
    "/metrics",
    "/ping",
    "/stats",
    "/users/login",
    "/users/refresh-token",
    "/personal-access-tokens/login",
];

pub async fn jwt_auth(
    State(state): State<Arc<AppState>>,
    mut request: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    if PUBLIC_PATHS.contains(&request.uri().path()) {
        return Ok(next.run(request).await);
    }

    let bearer = request
        .headers()
        .get(AUTHORIZATION)
        .ok_or(UNAUTHORIZED)
        .with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - missing or inaccessible Authorization header")
        })?
        .to_str()
        .with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - invalid authorization header format")
        })
        .map_err(|_| UNAUTHORIZED)?;

    if !bearer.starts_with(BEARER) {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let jwt_token = &bearer[BEARER.len()..];
    let token_header = jsonwebtoken::decode_header(jwt_token)
        .with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to decode JWT header")
        })
        .map_err(|_| UNAUTHORIZED)?;
    let jwt_claims = state
        .jwt_manager
        .decode(jwt_token, token_header.alg)
        .with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to decode JWT with provided algorithm")
        })
        .map_err(|_| UNAUTHORIZED)?;
    if state
        .jwt_manager
        .is_token_revoked(&jwt_claims.claims.jti)
        .await
    {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let request_details = request.extensions().get::<RequestDetails>().unwrap();
    let identity = Identity {
        token_id: jwt_claims.claims.jti,
        token_expiry: jwt_claims.claims.exp,
        user_id: jwt_claims.claims.sub,
        ip_address: request_details.ip_address,
    };
    request.extensions_mut().insert(identity);
    Ok(next.run(request).await)
}
