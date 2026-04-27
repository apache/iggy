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

use bytes::Bytes;
use iggy_binary_protocol::consensus::iobuf::Owned;
use iggy_binary_protocol::requests::streams::{CreateStreamRequest, DeleteStreamRequest};
use iggy_binary_protocol::{
    Message, Operation, RequestHeader, WireEncode, WireIdentifier, WireName,
};
use iggy_common::send_messages2::{
    IggyMessage2, IggyMessage2Header, IggyMessages2, SendMessages2Owned,
};
use iggy_common::sharding::IggyNamespace;
use std::cell::Cell;

// TODO: Proper client which implements the full client SDK API
pub struct SimClient {
    client_id: u128,
    request_counter: Cell<u64>,
    session: Cell<u64>,
}

impl SimClient {
    #[must_use]
    pub const fn new(client_id: u128) -> Self {
        Self {
            client_id,
            request_counter: Cell::new(0),
            session: Cell::new(0),
        }
    }

    #[must_use]
    pub const fn client_id(&self) -> u128 {
        self.client_id
    }

    /// Bind the session assigned by the consensus layer after registration.
    ///
    /// # Panics
    /// Panics if `session` is 0.
    pub fn bind_session(&self, session: u64) {
        assert!(session > 0, "bind_session: session must be > 0");
        self.session.set(session);
    }

    fn next_request_number(&self) -> u64 {
        let next = self.request_counter.get() + 1;
        self.request_counter.set(next);
        next
    }

    fn session_id(&self) -> u64 {
        let s = self.session.get();
        assert!(
            s > 0,
            "session not bound — call register() + bind_session() first"
        );
        s
    }

    /// Build a `Register` request for this client.
    ///
    /// Register uses `session=0, request=0` per the protocol spec.
    /// The consensus layer assigns a session on commit.
    ///
    /// # Panics
    /// Panics if the register request buffer is invalid.
    #[allow(clippy::cast_possible_truncation)]
    pub fn register(&self) -> Message<RequestHeader> {
        let header_size = std::mem::size_of::<RequestHeader>();
        let header = RequestHeader {
            command: iggy_binary_protocol::Command2::Request,
            operation: Operation::Register,
            size: header_size as u32,
            client: self.client_id,
            session: 0,
            request: 0,
            ..Default::default()
        };

        let header_bytes = bytemuck::bytes_of(&header);
        let buffer = header_bytes.to_vec();

        Message::try_from(Owned::<4096>::copy_from_slice(&buffer))
            .expect("register request must be valid")
    }

    /// # Panics
    /// Panics if the stream name is not a valid wire name.
    pub fn create_stream(&self, name: &str) -> Message<RequestHeader> {
        let wire = CreateStreamRequest {
            name: WireName::new(name).expect("stream name must be valid"),
        };
        let payload = wire.to_bytes();

        self.build_request(Operation::CreateStream, &payload)
    }

    /// # Panics
    /// Panics if the stream name cannot be converted to a `WireIdentifier`.
    pub fn delete_stream(&self, name: &str) -> Message<RequestHeader> {
        let wire = DeleteStreamRequest {
            stream_id: WireIdentifier::named(name).expect("stream name must be valid"),
        };
        let payload = wire.to_bytes();

        self.build_request(Operation::DeleteStream, &payload)
    }

    #[allow(clippy::cast_possible_truncation)]
    /// # Panics
    ///
    /// Panics if the simulator cannot encode the provided messages into a valid
    /// `SendMessages2` request.
    pub fn send_messages(
        &self,
        namespace: IggyNamespace,
        messages: &[&[u8]],
    ) -> Message<RequestHeader> {
        let mut batch = IggyMessages2::with_capacity(messages.len());
        for message in messages {
            batch.push(IggyMessage2 {
                header: IggyMessage2Header {
                    payload_length: message.len() as u32,
                    ..Default::default()
                },
                payload: Bytes::copy_from_slice(message),
                user_headers: None,
            });
        }

        let batch = SendMessages2Owned::from_messages(namespace, &batch)
            .expect("simulator must build a valid send_messages2 batch");
        let total_size = std::mem::size_of::<RequestHeader>() + batch.header.total_size();
        let request_header = self.request_header(Operation::SendMessages, namespace, total_size);
        batch
            .encode_request(request_header)
            .expect("simulator must build a valid send_messages2 request")
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

    pub fn delete_consumer_offset(
        &self,
        namespace: IggyNamespace,
        consumer_kind: u8,
        consumer_id: u32,
    ) -> Message<RequestHeader> {
        let mut payload = Vec::with_capacity(5);
        payload.push(consumer_kind);
        payload.extend_from_slice(&consumer_id.to_le_bytes());

        self.build_request_with_namespace(Operation::DeleteConsumerOffset, &payload, namespace)
    }

    #[allow(clippy::cast_possible_truncation)]
    fn build_request_with_namespace(
        &self,
        operation: Operation,
        payload: &[u8],
        namespace: IggyNamespace,
    ) -> Message<RequestHeader> {
        let header_size = std::mem::size_of::<RequestHeader>();
        let total_size = header_size + payload.len();

        let header = self.request_header(operation, namespace, total_size);

        let header_bytes = bytemuck::bytes_of(&header);
        let mut buffer = Vec::with_capacity(total_size);
        buffer.extend_from_slice(header_bytes);
        buffer.extend_from_slice(payload);

        Message::try_from(Owned::<4096>::copy_from_slice(&buffer))
            .expect("request buffer must contain a valid request message")
    }

    #[allow(clippy::cast_possible_truncation)]
    fn build_request(&self, operation: Operation, payload: &[u8]) -> Message<RequestHeader> {
        let header_size = std::mem::size_of::<RequestHeader>();
        let total_size = header_size + payload.len();

        let header = RequestHeader {
            command: iggy_binary_protocol::Command2::Request,
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
            session: self.session_id(),
            request: self.next_request_number(),
            ..Default::default()
        };

        let header_bytes = bytemuck::bytes_of(&header);
        let mut buffer = Vec::with_capacity(total_size);
        buffer.extend_from_slice(header_bytes);
        buffer.extend_from_slice(payload);

        Message::try_from(Owned::<4096>::copy_from_slice(&buffer))
            .expect("request buffer must contain a valid request message")
    }

    #[allow(clippy::cast_possible_truncation)]
    fn request_header(
        &self,
        operation: Operation,
        namespace: IggyNamespace,
        total_size: usize,
    ) -> RequestHeader {
        RequestHeader {
            command: iggy_binary_protocol::Command2::Request,
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
            session: self.session_id(),
            request: self.next_request_number(),
            namespace: namespace.inner(),
            ..Default::default()
        }
    }
}
