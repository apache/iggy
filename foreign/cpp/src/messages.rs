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

impl ffi::Message {
    pub fn new_message(&mut self, payload: Vec<u8>) {
        let payload_length = payload.len() as u32;
        *self = Self {
            checksum: 0,
            id_lo: 0,
            id_hi: 0,
            offset: 0,
            timestamp: 0,
            origin_timestamp: 0,
            user_headers_length: 0,
            payload_length,
            reserved: 0,
            payload,
            user_headers: Vec::new(),
        };
    }
}

impl From<RustIggyMessage> for ffi::Message {
    fn from(m: RustIggyMessage) -> Self {
        let id = m.header.id;
        ffi::Message {
            checksum: m.header.checksum,
            id_lo: id as u64,
            id_hi: (id >> 64) as u64,
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

impl TryFrom<ffi::Message> for RustIggyMessage {
    type Error = String;

    fn try_from(m: ffi::Message) -> Result<Self, Self::Error> {
        let id = ((m.id_hi as u128) << 64) | (m.id_lo as u128);
        let payload = Bytes::from(m.payload);
        let msg = if id > 0 {
            RustIggyMessage::builder().id(id).payload(payload).build()
        } else {
            RustIggyMessage::builder().payload(payload).build()
        };
        msg.map_err(|error| format!("Could not convert message: {error}"))
    }
}

impl From<RustPolledMessages> for ffi::PolledMessages {
    fn from(p: RustPolledMessages) -> Self {
        ffi::PolledMessages {
            partition_id: p.partition_id,
            current_offset: p.current_offset,
            count: p.count,
            messages: p.messages.into_iter().map(ffi::Message::from).collect(),
        }
    }
}
