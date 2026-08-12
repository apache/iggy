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

use axum::Router;
use axum::body::Body;
use axum::extract::Path;
use axum::http::{Response, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::get;
use rust_embed::{Embed, EmbeddedFile};

#[derive(Embed)]
#[folder = "../../web/build/static/"]
#[allow_missing = true]
struct WebAssets;

impl WebAssets {
    fn get_file(path: &str) -> Option<EmbeddedFile> {
        <Self as Embed>::get(path)
    }
}

pub fn router() -> Router {
    Router::new()
        .route("/ui/{*wildcard}", get(serve_web_asset))
        .route("/ui", get(serve_index))
        .route("/ui/", get(serve_index))
}

async fn serve_index() -> impl IntoResponse {
    serve_file("index.html")
}

async fn serve_web_asset(Path(wildcard): Path<String>) -> impl IntoResponse {
    if let Some(response) = try_serve_file(&wildcard) {
        return response;
    }

    if !wildcard.contains('.') {
        return serve_file("index.html");
    }

    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Body::from("Not Found"))
        .unwrap()
}

fn try_serve_file(path: &str) -> Option<Response<Body>> {
    let asset = WebAssets::get_file(path)?;
    let mime = mime_guess::from_path(path).first_or_octet_stream();

    Some(
        Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, mime.as_ref())
            .body(Body::from(asset.data.into_owned()))
            .unwrap(),
    )
}

fn serve_file(path: &str) -> Response<Body> {
    try_serve_file(path).unwrap_or_else(|| {
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("Not Found"))
            .unwrap()
    })
}
