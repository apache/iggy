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

use async_trait::async_trait;
use iggy_common::{IdentityInfo, IggyError};
use reqwest::{Response, Url};
use serde::Serialize;

#[async_trait]
pub trait HttpTransport {
    /// Get full URL for the provided path.
    fn get_url(&self, path: &str) -> Result<Url, IggyError>;

    /// Invoke HTTP GET request to the Iggy API.
    async fn get(&self, path: &str) -> Result<Response, IggyError>;

    /// Invoke HTTP GET request to the Iggy API with query parameters.
    async fn get_with_query<T: Serialize + Sync + ?Sized>(
        &self,
        path: &str,
        query: &T,
    ) -> Result<Response, IggyError>;

    /// Invoke HTTP POST request to the Iggy API.
    async fn post<T: Serialize + Sync + ?Sized>(
        &self,
        path: &str,
        payload: &T,
    ) -> Result<Response, IggyError>;

    /// Invoke HTTP PUT request to the Iggy API.
    async fn put<T: Serialize + Sync + ?Sized>(
        &self,
        path: &str,
        payload: &T,
    ) -> Result<Response, IggyError>;

    /// Invoke HTTP DELETE request to the Iggy API.
    async fn delete(&self, path: &str) -> Result<Response, IggyError>;

    /// Invoke HTTP DELETE request to the Iggy API with query parameters.
    async fn delete_with_query<T: Serialize + Sync + ?Sized>(
        &self,
        path: &str,
        query: &T,
    ) -> Result<Response, IggyError>;

    /// Returns true if the client is authenticated.
    async fn is_authenticated(&self) -> bool;

    /// Refresh the access token using the provided refresh token.
    //method `refresh_access_token` is never used
    async fn _refresh_access_token(&self) -> Result<(), IggyError>;

    /// Set the access token.
    async fn set_access_token(&self, token: Option<String>);

    /// Set the access token and refresh token from the provided identity.
    async fn set_token_from_identity(&self, identity: &IdentityInfo) -> Result<(), IggyError>;
}
