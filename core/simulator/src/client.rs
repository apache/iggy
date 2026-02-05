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

use iggy_common::{
    BytesSerializable, Identifier,
    create_stream::CreateStream,
    delete_stream::DeleteStream,
    header::{Operation, RequestHeader},
    message::Message,
};
use std::cell::Cell;

// TODO: Proper client which implements the full client SDK API
pub struct SimClient {
    client_id: u128,
    request_counter: Cell<u64>,
}

impl SimClient {
    pub fn new(client_id: u128) -> Self {
        Self {
            client_id,
            request_counter: Cell::new(0),
        }
    }

    fn next_request_number(&self) -> u64 {
        let current = self.request_counter.get();
        self.request_counter.set(current + 1);
        current
    }

    pub fn create_stream(&self, name: &str) -> Message<RequestHeader> {
        let create_stream = CreateStream {
            name: name.to_string(),
        };
        let payload = create_stream.to_bytes();

        self.build_request(Operation::CreateStream, payload)
    }

    pub fn delete_stream(&self, name: &str) -> Message<RequestHeader> {
        let delete_stream = DeleteStream {
            stream_id: Identifier::named(name).unwrap(),
        };
        let payload = delete_stream.to_bytes();

        self.build_request(Operation::DeleteStream, payload)
    }

    fn build_request(&self, operation: Operation, payload: bytes::Bytes) -> Message<RequestHeader> {
        use bytes::Bytes;

        let header_size = std::mem::size_of::<RequestHeader>();
        let total_size = header_size + payload.len();

        let header = RequestHeader {
            command: iggy_common::header::Command2::Request,
            operation,
            size: total_size as u32,
            cluster: 0, // TODO: Get from config
            checksum: 0,
            checksum_body: 0,
            epoch: 0,
            view: 0,
            release: 0,
            protocol: 0,
            replica: 0,
            reserved_frame: [0; 12],
            client: self.client_id,
            request_checksum: 0,
            timestamp: 0, // TODO: Use actual timestamp
            request: self.next_request_number(),
            ..Default::default()
        };

        let header_bytes = bytemuck::bytes_of(&header);
        let mut buffer = Vec::with_capacity(total_size);
        buffer.extend_from_slice(header_bytes);
        buffer.extend_from_slice(&payload);

        Message::<RequestHeader>::from_bytes(Bytes::from(buffer))
            .expect("failed to build request message")
    }
}
