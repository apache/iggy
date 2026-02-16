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

use std::sync::Arc;
use axum::body::Body;
use axum::extract::{ConnectInfo, State};
use axum::http::{Request, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use iggy_common::TransportProtocol;
use crate::http::http_server::CompioSocketAddr;
use crate::http::shared::AppState;

pub async fn manage_clients(
    State(state): State<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<CompioSocketAddr>,
    request: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    // add client
    let addr = addr.0;
    let session = state.shard.shard().add_client(&addr, TransportProtocol::Http);
    let client_id = session.client_id;
    println!(">>> before: {:?}, {:?}", request, addr);

    // handle request
    let response = next.run(request).await;

    let _ = compio::runtime::spawn(async move {
        state.shard.shard().delete_client(client_id).await;
    }).await;

    // remove client
    println!(">>> after");

    Ok(response)
}