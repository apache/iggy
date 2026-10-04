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

//! Request/response header version selection.
//!
//! `kafka_protocol::messages::ApiKey` owns the per-API flexible-encoding threshold table
//! (schema-generated from the Kafka message JSON, not hand-transcribed here). These wrappers
//! just add the gateway's policy for API keys the crate doesn't know about: an unrecognized
//! key was never negotiated through `ApiVersions`, so it can only have arrived on header v1
//! (never flexible) and gets no response (`response_header_version` is unused for it).

use kafka_protocol::messages::ApiKey;

#[must_use]
pub fn request_header_version(api_key: i16, api_version: i16) -> i16 {
    ApiKey::try_from(api_key).map_or(1, |key| key.request_header_version(api_version))
}

/// KIP-511 special case for `ApiVersions` (18): always header v0.
///
/// `ApiKey::response_header_version` already handles it - clients probing an unknown server
/// must be able to parse the discovery response before they know it supports flexible encoding.
#[must_use]
pub fn response_header_version(api_key: i16, api_version: i16) -> i16 {
    ApiKey::try_from(api_key).map_or(0, |key| key.response_header_version(api_version))
}
