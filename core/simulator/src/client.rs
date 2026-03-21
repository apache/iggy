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
    BytesSerializable, IGGY_MESSAGE_HEADER_SIZE, INDEX_SIZE, Identifier,
    create_stream::CreateStream,
    delete_stream::DeleteStream,
    header::{Operation, RequestHeader},
    message::Message,
    sharding::IggyNamespace,
};
use std::cell::Cell;

// TODO: Proper client which implements the full client SDK API
pub struct SimClient {
    client_id: u128,
    request_counter: Cell<u64>,
}

impl SimClient {
    #[must_use]
    pub const fn new(client_id: u128) -> Self {
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

        self.build_request(Operation::CreateStream, &payload)
    }

    /// # Panics
    /// Panics if the stream name cannot be converted to an `Identifier`.
    pub fn delete_stream(&self, name: &str) -> Message<RequestHeader> {
        let delete_stream = DeleteStream {
            stream_id: Identifier::named(name).unwrap(),
        };
        let payload = delete_stream.to_bytes();

        self.build_request(Operation::DeleteStream, &payload)
    }

    #[allow(clippy::cast_possible_truncation)]
    pub fn send_messages(
        &self,
        namespace: IggyNamespace,
        messages: &[&[u8]],
    ) -> Message<RequestHeader> {
        let count = messages.len() as u32;
        let mut indexes = Vec::with_capacity(count as usize * INDEX_SIZE);
        let mut messages_buf = Vec::new();

        let mut current_position = 0u32;
        for (i, msg) in messages.iter().enumerate() {
            let msg_total_len = (IGGY_MESSAGE_HEADER_SIZE + msg.len()) as u32;

            // Index: offset(u32) + position(u32) + timestamp(u64)
            indexes.extend_from_slice(&(i as u32).to_le_bytes()); // offset (relative)
            indexes.extend_from_slice(&current_position.to_le_bytes()); // position
            indexes.extend_from_slice(&0u64.to_le_bytes()); // timestamp (set in prepare)

            // Message header (64 bytes)
            messages_buf.extend_from_slice(&0u64.to_le_bytes()); // checksum
            messages_buf.extend_from_slice(&0u128.to_le_bytes()); // id
            messages_buf.extend_from_slice(&0u64.to_le_bytes()); // offset
            messages_buf.extend_from_slice(&0u64.to_le_bytes()); // timestamp
            messages_buf.extend_from_slice(&0u64.to_le_bytes()); // origin_timestamp
            messages_buf.extend_from_slice(&0u32.to_le_bytes()); // user_headers_length
            messages_buf.extend_from_slice(&(msg.len() as u32).to_le_bytes()); // payload_length
            messages_buf.extend_from_slice(&0u64.to_le_bytes()); // reserved

            // Payload
            messages_buf.extend_from_slice(msg);
            current_position += msg_total_len;
        }

        let mut payload = Vec::with_capacity(4 + indexes.len() + messages_buf.len());
        payload.extend_from_slice(&count.to_le_bytes());
        payload.extend_from_slice(&indexes);
        payload.extend_from_slice(&messages_buf);

        self.build_request_with_namespace(Operation::SendMessages, &payload, namespace)
    }

    pub fn store_consumer_offset(
        &self,
        namespace: IggyNamespace,
        consumer_kind: u8,
        consumer_id: u32,
        offset: u64,
    ) -> Message<RequestHeader> {
        let mut payload = Vec::with_capacity(13);
        payload.push(consumer_kind);
        payload.extend_from_slice(&consumer_id.to_le_bytes());
        payload.extend_from_slice(&offset.to_le_bytes());

        self.build_request_with_namespace(Operation::StoreConsumerOffset, &payload, namespace)
    }

    #[allow(clippy::cast_possible_truncation)]
    fn build_request_with_namespace(
        &self,
        operation: Operation,
        payload: &[u8],
        namespace: IggyNamespace,
    ) -> Message<RequestHeader> {
        use bytes::Bytes;

        let header_size = std::mem::size_of::<RequestHeader>();
        let total_size = header_size + payload.len();

        let header = RequestHeader {
            command: iggy_common::header::Command2::Request,
            operation,
            size: total_size as u32,
            cluster: 0,
            checksum: 0,
            checksum_body: 0,
            view: 0,
            release: 0,
            replica: 0,
            reserved_frame: [0; 66],
            client: self.client_id,
            request_checksum: 0,
            timestamp: 0,
            request: self.next_request_number(),
            namespace: namespace.inner(),
            ..Default::default()
        };

        let header_bytes = bytemuck::bytes_of(&header);
        let mut buffer = Vec::with_capacity(total_size);
        buffer.extend_from_slice(header_bytes);
        buffer.extend_from_slice(payload);

        Message::<RequestHeader>::from_bytes(Bytes::from(buffer))
            .expect("failed to build request message")
    }

    #[allow(clippy::cast_possible_truncation)]
    fn build_request(&self, operation: Operation, payload: &[u8]) -> Message<RequestHeader> {
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
            view: 0,
            release: 0,
            replica: 0,
            reserved_frame: [0; 66],
            client: self.client_id,
            request_checksum: 0,
            timestamp: 0, // TODO: Use actual timestamp
            request: self.next_request_number(),
            ..Default::default()
        };

        let header_bytes = bytemuck::bytes_of(&header);
        let mut buffer = Vec::with_capacity(total_size);
        buffer.extend_from_slice(header_bytes);
        buffer.extend_from_slice(payload);

        Message::<RequestHeader>::from_bytes(Bytes::from(buffer))
            .expect("failed to build request message")
    }
}
