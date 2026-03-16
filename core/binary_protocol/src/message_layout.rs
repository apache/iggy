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

//! 64-byte message frame layout constants.
//!
//! This module is the single source of truth for the on-wire message
//! header layout shared between encoders, decoders, and zero-copy views.
//!
//! ```text
//! [checksum:8][id:16][offset:8][timestamp:8][origin_timestamp:8]
//! [user_headers_length:4][payload_length:4][reserved:8]
//! ```

/// Fixed-size message header on the wire (64 bytes).
pub const WIRE_MESSAGE_HEADER_SIZE: usize = 64;

/// Fixed-size index entry per message (16 bytes).
///
/// Layout: `[0x00000000:4][cumulative_size:u32_le:4][0x0000000000000000:8]`
pub const WIRE_MESSAGE_INDEX_SIZE: usize = 16;

// Field offsets within the 64-byte message header.
pub const MSG_CHECKSUM_OFFSET: usize = 0;
pub const MSG_ID_OFFSET: usize = 8;
pub const MSG_OFFSET_OFFSET: usize = 24;
pub const MSG_TIMESTAMP_OFFSET: usize = 32;
pub const MSG_ORIGIN_TIMESTAMP_OFFSET: usize = 40;
pub const MSG_USER_HEADERS_LEN_OFFSET: usize = 48;
pub const MSG_PAYLOAD_LEN_OFFSET: usize = 52;
pub const MSG_RESERVED_OFFSET: usize = 56;

// Compile-time verification that field offsets are contiguous and sum to the header size.
const _: () = {
    assert!(MSG_ID_OFFSET == MSG_CHECKSUM_OFFSET + 8);
    assert!(MSG_OFFSET_OFFSET == MSG_ID_OFFSET + 16);
    assert!(MSG_TIMESTAMP_OFFSET == MSG_OFFSET_OFFSET + 8);
    assert!(MSG_ORIGIN_TIMESTAMP_OFFSET == MSG_TIMESTAMP_OFFSET + 8);
    assert!(MSG_USER_HEADERS_LEN_OFFSET == MSG_ORIGIN_TIMESTAMP_OFFSET + 8);
    assert!(MSG_PAYLOAD_LEN_OFFSET == MSG_USER_HEADERS_LEN_OFFSET + 4);
    assert!(MSG_RESERVED_OFFSET == MSG_PAYLOAD_LEN_OFFSET + 4);
    assert!(MSG_RESERVED_OFFSET + 8 == WIRE_MESSAGE_HEADER_SIZE);
};
