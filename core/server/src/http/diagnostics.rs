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

use crate::http::http_server::CompioSocketAddr;
use crate::http::shared::RequestDetails;
use crate::streaming::utils::random_id;
use axum::body::Body;
use axum::{
    extract::ConnectInfo,
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};
use std::time::Instant;
use tracing::{debug, error};

pub async fn request_diagnostics(
    ConnectInfo(ip_address): ConnectInfo<CompioSocketAddr>,
    mut request: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    let request_id = random_id::get_ulid();
    let path_and_query = request
        .uri()
        .path_and_query()
        .map(|p| p.as_str())
        .unwrap_or("/");
    let ip_address = ip_address.0;
    debug!(
        "Processing a request {} {} with ID: {request_id} from client with IP address: {ip_address}...",
        request.method(),
        path_and_query,
    );
    request.extensions_mut().insert(RequestDetails {
        request_id,
        ip_address,
    });
    let now = Instant::now();
    let result = Ok(next.run(request).await);
    if let Ok(response) = &result {
        let status = response.status();
        if status != StatusCode::NOT_FOUND && status >= StatusCode::BAD_REQUEST {
            error!(
                "Returning an invalid status code: {status}, IP address: {ip_address}, request ID: {request_id}"
            );
        }
    }
    let elapsed = now.elapsed();
    debug!(
        "Processed a request with ID: {request_id} from client with IP address: {ip_address} in {} ms.",
        elapsed.as_millis()
    );
    result
}
