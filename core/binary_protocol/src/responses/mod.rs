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

pub mod clients;
pub mod consumer_groups;
pub mod consumer_offsets;
pub mod messages;
pub mod personal_access_tokens;
pub mod streams;
pub mod system;
pub mod topics;
pub mod users;

use crate::WireError;
use crate::codec::{WireDecode, WireEncode};
use bytes::BytesMut;

/// Marker type for commands that return an empty response payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmptyResponse;

impl WireEncode for EmptyResponse {
    fn encoded_size(&self) -> usize {
        0
    }

    fn encode(&self, _buf: &mut BytesMut) {}
}

impl WireDecode for EmptyResponse {
    fn decode(_buf: &[u8]) -> Result<(Self, usize), WireError> {
        Ok((Self, 0))
    }
}
