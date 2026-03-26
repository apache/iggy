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

use crate::kafka::protocol::codec::{write_i16, write_i32};
use crate::kafka::protocol::types::{ApiKeyVersion, ApiVersionsResponse};
use crate::kafka::protocol::{self, RequestHeader, UNSUPPORTED_VERSION};

const SUPPORTED: &[ApiKeyVersion] = &[
    ApiKeyVersion {
        api_key: protocol::PRODUCE,
        min_version: 3,
        max_version: 3,
    },
    ApiKeyVersion {
        api_key: protocol::FETCH,
        min_version: 4,
        max_version: 4,
    },
    ApiKeyVersion {
        api_key: protocol::LIST_OFFSETS,
        min_version: 1,
        max_version: 1,
    },
    ApiKeyVersion {
        api_key: protocol::METADATA,
        min_version: 1,
        max_version: 1,
    },
    ApiKeyVersion {
        api_key: protocol::FIND_COORDINATOR,
        min_version: 0,
        max_version: 0,
    },
    ApiKeyVersion {
        api_key: protocol::API_VERSIONS,
        min_version: 0,
        max_version: 3,
    },
    ApiKeyVersion {
        api_key: protocol::CREATE_TOPICS,
        min_version: 0,
        max_version: 0,
    },
    ApiKeyVersion {
        api_key: protocol::DELETE_TOPICS,
        min_version: 0,
        max_version: 0,
    },
];

pub fn handle(header: &RequestHeader) -> Vec<u8> {
    if header.api_version > 3 {
        return ApiVersionsResponse {
            error_code: UNSUPPORTED_VERSION,
            api_keys: vec![],
        }
        .encode();
    }

    if header.api_version >= 3 {
        return encode_v3();
    }

    ApiVersionsResponse {
        error_code: 0,
        api_keys: SUPPORTED.to_vec(),
    }
    .encode()
}

/// ApiVersions v3 uses flexible encoding: compact arrays, tagged fields, throttle_time_ms.
fn encode_v3() -> Vec<u8> {
    let mut buf = Vec::new();
    write_i16(&mut buf, 0); // error_code
    // compact array: count + 1 as unsigned varint
    buf.push((SUPPORTED.len() + 1) as u8);
    for k in SUPPORTED {
        write_i16(&mut buf, k.api_key);
        write_i16(&mut buf, k.min_version);
        write_i16(&mut buf, k.max_version);
        buf.push(0); // per-entry _tagged_fields
    }
    write_i32(&mut buf, 0); // throttle_time_ms
    buf.push(0); // top-level _tagged_fields
    buf
}
