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

use crate::ffi;
use bytes::Bytes;
use iggy::prelude::{IggyMessage as RustIggyMessage, PolledMessages as RustPolledMessages};

pub fn make_message(payload: Vec<u8>) -> ffi::IggyMessageToSend {
    ffi::IggyMessageToSend {
        id_lo: 0,
        id_hi: 0,
        payload,
        user_headers: Vec::new(),
    }
}

impl From<RustIggyMessage> for ffi::IggyMessagePolled {
    fn from(m: RustIggyMessage) -> Self {
        let id_bytes = m.header.id.to_le_bytes();
        let id_lo = u64::from_le_bytes(id_bytes[0..8].try_into().unwrap());
        let id_hi = u64::from_le_bytes(id_bytes[8..16].try_into().unwrap());
        ffi::IggyMessagePolled {
            checksum: m.header.checksum,
            id_lo,
            id_hi,
            offset: m.header.offset,
            timestamp: m.header.timestamp,
            origin_timestamp: m.header.origin_timestamp,
            user_headers_length: m.header.user_headers_length,
            payload_length: m.header.payload_length,
            reserved: m.header.reserved,
            payload: m.payload.to_vec(),
            user_headers: m.user_headers.map(|h| h.to_vec()).unwrap_or_default(),
        }
    }
}

impl TryFrom<ffi::IggyMessageToSend> for RustIggyMessage {
    type Error = String;

    fn try_from(m: ffi::IggyMessageToSend) -> Result<Self, Self::Error> {
        if !m.user_headers.is_empty() {
            return Err(
                "Could not convert message: user_headers are not yet supported in the C++ SDK"
                    .to_string(),
            );
        }
        let id = ((m.id_hi as u128) << 64) | (m.id_lo as u128);
        let payload = Bytes::from(m.payload);
        RustIggyMessage::builder()
            .id(id)
            .payload(payload)
            .build()
            .map_err(|error| format!("Could not convert message: {error}"))
    }
}

impl From<RustPolledMessages> for ffi::PolledMessages {
    fn from(p: RustPolledMessages) -> Self {
        ffi::PolledMessages {
            partition_id: p.partition_id,
            current_offset: p.current_offset,
            count: p.count,
            messages: p
                .messages
                .into_iter()
                .map(ffi::IggyMessagePolled::from)
                .collect(),
        }
    }
}
