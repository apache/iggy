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

use bytes::{Bytes, BytesMut};
use iggy_binary_protocol::codes::POLL_MESSAGES_CODE;
use iggy_binary_protocol::primitives::consumer::WireConsumer;
use iggy_binary_protocol::requests::consumer_groups::{
    CreateConsumerGroupRequest, DeleteConsumerGroupRequest,
};
use iggy_binary_protocol::requests::consumer_offsets::{
    DeleteConsumerOffset2Request, DeleteConsumerOffsetRequest, StoreConsumerOffset2Request,
    StoreConsumerOffsetRequest,
};
use iggy_binary_protocol::requests::messages::{
    PollMessagesRequest, RawMessage, SendMessagesEncoder,
};
use iggy_binary_protocol::requests::partitions::{
    CreatePartitionsRequest, DeletePartitionsRequest,
};
use iggy_binary_protocol::requests::segments::DeleteSegmentsRequest;
use iggy_binary_protocol::requests::streams::{
    CreateStreamRequest, DeleteStreamRequest, PurgeStreamRequest, UpdateStreamRequest,
};
use iggy_binary_protocol::requests::topics::{
    CreateTopicRequest, DeleteTopicRequest, PurgeTopicRequest, UpdateTopicRequest,
};
use iggy_binary_protocol::requests::users::{
    ChangePasswordRequest, CreateUserRequest, DeleteUserRequest, LoginRegisterRequest,
    UpdatePermissionsRequest, UpdateUserRequest,
};
use iggy_binary_protocol::{
    AckLevel, ClientVersionInfo, IGGY_PROTOCOL_VERSION, Operation, RequestHeader, WireEncode,
    WireIdentifier, WireName, WirePartitioning, WirePollingStrategy,
};
use metadata::stm::user::{CreatePersonalAccessTokenRequest, DeletePersonalAccessTokenRequest};
use secrecy::SecretString;
use server_common::sharding::{IggyNamespace, METADATA_CONSENSUS_NAMESPACE};
use server_common::{Message, iobuf::Owned};
use std::cell::Cell;

/// Partition-plane request ids are offset into the top half of the `u64` space
/// so they never collide with the small, contiguous metadata ids in the
/// auditor's `(client, request)` map. The metadata sequence would have to reach
/// `2^63` to overlap, which no run approaches.
const PARTITION_ID_BASE: u64 = 1 << 63;

// TODO: Proper client which implements the full client SDK API
pub struct SimClient {
    client_id: u128,
    /// Contiguous `1, 2, 3, …` request ids for metadata/replicated ops, the
    /// sequence the server's `ClientTable` dedups and requires gap-free.
    request_counter: Cell<u64>,
    /// Separate id sequence for partition-plane ops, offset into a disjoint
    /// range ([`PARTITION_ID_BASE`]). The partition plane has no client-table
    /// dedup and treats the id as an opaque echo, so a partition id never
    /// collides with a metadata id, even under reply duplication. See
    /// [`SimClient::request_id_for`].
    partition_counter: Cell<u64>,
    /// Deterministic per-message id source for produced messages. The real SDK
    /// sends `id: 0` and lets the server mint a random UUID
    /// (`transcode_legacy_request` -> `random_id::get_uuid`); that
    /// mint is unseeded, so under the deterministic executor a produce's
    /// replicated body bytes (and their checksums) would differ run to run,
    /// silently breaking seeded replay. Stamping a deterministic id here keeps
    /// the body a pure function of the seed. See [`SimClient::next_message_id`].
    message_counter: Cell<u64>,
    session: Cell<u64>,
}

impl SimClient {
    #[must_use]
    pub const fn new(client_id: u128) -> Self {
        Self {
            client_id,
            request_counter: Cell::new(0),
            partition_counter: Cell::new(0),
            message_counter: Cell::new(0),
            session: Cell::new(0),
        }
    }

    #[must_use]
    pub const fn client_id(&self) -> u128 {
        self.client_id
    }

    /// Next deterministic, non-zero, cross-client-unique message id, standing
    /// in for the SDK's `id: 0` / server-side random UUID mint so a produce's
    /// replicated bytes are a pure function of the seed. The `client_id` fills
    /// the high 64 bits (ids stay disjoint across clients, whose ids are minted
    /// small) and a monotonic counter fills the low 64 bits (starts at 1, so
    /// the id is never 0 and never re-triggers the server's random mint). See
    /// [`SimClient::message_counter`].
    fn next_message_id(&self) -> u128 {
        let seq = self.message_counter.get() + 1;
        self.message_counter.set(seq);
        (self.client_id << 64) | u128::from(seq)
    }

    /// Bind the session assigned by the consensus layer after registration.
    ///
    /// # Panics
    /// Panics if `session` is 0.
    pub fn bind_session(&self, session: u64) {
        assert!(session > 0, "bind_session: session must be > 0");
        self.session.set(session);
    }

    /// Assign the wire request id for `operation`, keyed by plane.
    ///
    /// Metadata/replicated ops advance a contiguous `1, 2, 3, …` counter: the
    /// `ClientTable` dedups them and rejects anything but `committed + 1`, so a
    /// gap opens a permanent `RequestGap` and wedges the client's metadata
    /// plane. Partition ops are at-least-once with no dedup and the server
    /// treats their id as an opaque echo, so they draw from a separate counter
    /// offset into a disjoint range ([`PARTITION_ID_BASE`]). A partition id can
    /// therefore never equal a metadata id, so a delayed or duplicated partition
    /// reply is never misattributed to a metadata entry in the auditor's
    /// `(client, request)` map (which would trip the namespace guard and drop a
    /// live metadata op). This holds regardless of reply duplication, not only
    /// while clients are one-in-flight.
    fn request_id_for(&self, operation: Operation) -> u64 {
        if operation.is_partition() {
            let next = self.partition_counter.get() + 1;
            self.partition_counter.set(next);
            PARTITION_ID_BASE + next
        } else {
            let next = self.request_counter.get() + 1;
            self.request_counter.set(next);
            next
        }
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
            // Register is a vsr-reserved op: the shard router picks its
            // target by comparing this against the metadata consensus
            // namespace, not by op class.
            namespace: METADATA_CONSENSUS_NAMESPACE,
            ..Default::default()
        };

        let header_bytes = bytemuck::bytes_of(&header);
        let buffer = header_bytes.to_vec();

        Message::try_from(Owned::<4096>::copy_from_slice(&buffer))
            .expect("register request must be valid")
    }

    /// Build a credentialed login-register request: the shell-mode
    /// counterpart of [`SimClient::register`].
    ///
    /// Same `Register` / `session=0` / `request=0` envelope, but the body
    /// carries the `ClientVersionInfo` prefix plus username/password that
    /// `handle_login_register_request` verifies against the seeded root
    /// user before the consensus layer assigns the session on commit. Used
    /// only on the dispatch-shell path (the raw fast path uses `register`).
    ///
    /// # Panics
    /// Panics if a credential exceeds the wire name/secret bounds or the
    /// request buffer is invalid.
    #[allow(clippy::cast_possible_truncation)]
    pub fn login(&self, username: &str, password: &str) -> Message<RequestHeader> {
        let body = LoginRegisterRequest {
            version_info: ClientVersionInfo {
                protocol_version: IGGY_PROTOCOL_VERSION,
                sdk_name: WireName::new("sim-sdk").expect("sim sdk name is valid"),
                sdk_version: WireName::new("1.0.0").expect("sim sdk version is valid"),
            },
            username: WireName::new(username).expect("login username is a valid wire name"),
            password: SecretString::from(password.to_owned()),
            client_context: None,
        }
        .to_bytes();

        let header_size = std::mem::size_of::<RequestHeader>();
        let total_size = header_size + body.len();
        let header = RequestHeader {
            command: iggy_binary_protocol::Command2::Request,
            operation: Operation::Register,
            size: total_size as u32,
            client: self.client_id,
            session: 0,
            request: 0,
            namespace: METADATA_CONSENSUS_NAMESPACE,
            ..Default::default()
        };

        let mut buffer = Vec::with_capacity(total_size);
        buffer.extend_from_slice(bytemuck::bytes_of(&header));
        buffer.extend_from_slice(&body);
        Message::try_from(Owned::<4096>::copy_from_slice(&buffer))
            .expect("login request must be valid")
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

    /// # Panics
    /// Panics if the new name or the existing stream name is not a valid
    /// `WireName`.
    pub fn update_stream(&self, stream: &str, new_name: &str) -> Message<RequestHeader> {
        let wire = UpdateStreamRequest {
            stream_id: WireIdentifier::named(stream).expect("stream name must be valid"),
            name: WireName::new(new_name).expect("stream name must be valid"),
        };
        self.build_request(Operation::UpdateStream, &wire.to_bytes())
    }

    /// # Panics
    /// Panics if `stream` is not a valid `WireName`.
    pub fn purge_stream(&self, stream: &str) -> Message<RequestHeader> {
        let wire = PurgeStreamRequest {
            stream_id: WireIdentifier::named(stream).expect("stream name must be valid"),
        };
        self.build_request(Operation::PurgeStream, &wire.to_bytes())
    }

    /// # Panics
    /// Panics if `stream` or `name` is not a valid `WireName`.
    pub fn create_topic(
        &self,
        stream: &str,
        name: &str,
        partitions_count: u32,
    ) -> Message<RequestHeader> {
        let wire = CreateTopicRequest {
            stream_id: WireIdentifier::named(stream).expect("stream name must be valid"),
            partitions_count,
            compression_algorithm: 0,
            message_expiry: 0,
            max_topic_size: 0,
            replication_factor: 1,
            name: WireName::new(name).expect("topic name must be valid"),
        };
        self.build_request(Operation::CreateTopic, &wire.to_bytes())
    }

    /// # Panics
    /// Panics if `stream`, `topic`, or `new_name` is not a valid `WireName`.
    pub fn update_topic(
        &self,
        stream: &str,
        topic: &str,
        new_name: &str,
    ) -> Message<RequestHeader> {
        let wire = UpdateTopicRequest {
            stream_id: WireIdentifier::named(stream).expect("stream name must be valid"),
            topic_id: WireIdentifier::named(topic).expect("topic name must be valid"),
            compression_algorithm: 0,
            message_expiry: 0,
            max_topic_size: 0,
            replication_factor: 1,
            name: WireName::new(new_name).expect("topic name must be valid"),
        };
        self.build_request(Operation::UpdateTopic, &wire.to_bytes())
    }

    /// # Panics
    /// Panics if `stream` or `topic` is not a valid `WireName`.
    pub fn delete_topic(&self, stream: &str, topic: &str) -> Message<RequestHeader> {
        let wire = DeleteTopicRequest {
            stream_id: WireIdentifier::named(stream).expect("stream name must be valid"),
            topic_id: WireIdentifier::named(topic).expect("topic name must be valid"),
        };
        self.build_request(Operation::DeleteTopic, &wire.to_bytes())
    }

    /// # Panics
    /// Panics if `stream` or `topic` is not a valid `WireName`.
    pub fn purge_topic(&self, stream: &str, topic: &str) -> Message<RequestHeader> {
        let wire = PurgeTopicRequest {
            stream_id: WireIdentifier::named(stream).expect("stream name must be valid"),
            topic_id: WireIdentifier::named(topic).expect("topic name must be valid"),
        };
        self.build_request(Operation::PurgeTopic, &wire.to_bytes())
    }

    /// # Panics
    /// Panics if `stream` or `topic` is not a valid `WireName`.
    pub fn create_partitions(
        &self,
        stream: &str,
        topic: &str,
        partitions_count: u32,
    ) -> Message<RequestHeader> {
        let wire = CreatePartitionsRequest {
            stream_id: WireIdentifier::named(stream).expect("stream name must be valid"),
            topic_id: WireIdentifier::named(topic).expect("topic name must be valid"),
            partitions_count,
        };
        self.build_request(Operation::CreatePartitions, &wire.to_bytes())
    }

    /// # Panics
    /// Panics if `stream` or `topic` is not a valid `WireName`.
    pub fn delete_partitions(
        &self,
        stream: &str,
        topic: &str,
        partitions_count: u32,
    ) -> Message<RequestHeader> {
        let wire = DeletePartitionsRequest {
            stream_id: WireIdentifier::named(stream).expect("stream name must be valid"),
            topic_id: WireIdentifier::named(topic).expect("topic name must be valid"),
            partitions_count,
        };
        self.build_request(Operation::DeletePartitions, &wire.to_bytes())
    }

    /// # Panics
    /// Panics if `stream` or `topic` is not a valid `WireName`.
    pub fn delete_segments(
        &self,
        stream: &str,
        topic: &str,
        partition_id: u32,
        segments_count: u32,
    ) -> Message<RequestHeader> {
        let wire = DeleteSegmentsRequest {
            stream_id: WireIdentifier::named(stream).expect("stream name must be valid"),
            topic_id: WireIdentifier::named(topic).expect("topic name must be valid"),
            partition_id,
            segments_count,
        };
        self.build_request(Operation::DeleteSegments, &wire.to_bytes())
    }

    /// # Panics
    /// Panics if `stream`, `topic`, or `name` is not a valid `WireName`.
    pub fn create_consumer_group(
        &self,
        stream: &str,
        topic: &str,
        name: &str,
    ) -> Message<RequestHeader> {
        let wire = CreateConsumerGroupRequest {
            stream_id: WireIdentifier::named(stream).expect("stream name must be valid"),
            topic_id: WireIdentifier::named(topic).expect("topic name must be valid"),
            name: WireName::new(name).expect("consumer group name must be valid"),
        };
        self.build_request(Operation::CreateConsumerGroup, &wire.to_bytes())
    }

    /// # Panics
    /// Panics if `stream`, `topic`, or `group` is not a valid `WireName`.
    pub fn delete_consumer_group(
        &self,
        stream: &str,
        topic: &str,
        group: &str,
    ) -> Message<RequestHeader> {
        let wire = DeleteConsumerGroupRequest {
            stream_id: WireIdentifier::named(stream).expect("stream name must be valid"),
            topic_id: WireIdentifier::named(topic).expect("topic name must be valid"),
            group_id: WireIdentifier::named(group).expect("group name must be valid"),
        };
        self.build_request(Operation::DeleteConsumerGroup, &wire.to_bytes())
    }

    /// # Panics
    /// Panics if `username` is not a valid `WireName`.
    pub fn create_user(
        &self,
        username: &str,
        password: &str,
        status: u8,
    ) -> Message<RequestHeader> {
        let wire = CreateUserRequest {
            username: WireName::new(username).expect("username must be valid"),
            password: password.to_string(),
            status,
            permissions: None,
        };
        self.build_request(Operation::CreateUser, &wire.to_bytes())
    }

    /// # Panics
    /// Panics if `user` (existing username) or `new_username` (when
    /// provided) is not a valid `WireName`.
    pub fn update_user(
        &self,
        user: &str,
        new_username: Option<&str>,
        status: Option<u8>,
    ) -> Message<RequestHeader> {
        let wire = UpdateUserRequest {
            user_id: WireIdentifier::named(user).expect("username must be valid"),
            username: new_username.map(|n| WireName::new(n).expect("username must be valid")),
            status,
        };
        self.build_request(Operation::UpdateUser, &wire.to_bytes())
    }

    /// # Panics
    /// Panics if `user` is not a valid `WireName`.
    pub fn delete_user(&self, user: &str) -> Message<RequestHeader> {
        let wire = DeleteUserRequest {
            user_id: WireIdentifier::named(user).expect("username must be valid"),
        };
        self.build_request(Operation::DeleteUser, &wire.to_bytes())
    }

    /// # Panics
    /// Panics if `user` is not a valid `WireName`.
    pub fn change_password(
        &self,
        user: &str,
        current_password: &str,
        new_password: &str,
    ) -> Message<RequestHeader> {
        let wire = ChangePasswordRequest {
            user_id: WireIdentifier::named(user).expect("username must be valid"),
            current_password: current_password.to_string(),
            new_password: new_password.to_string(),
        };
        self.build_request(Operation::ChangePassword, &wire.to_bytes())
    }

    /// # Panics
    /// Panics if `user` is not a valid `WireName`.
    pub fn update_permissions(&self, user: &str) -> Message<RequestHeader> {
        let wire = UpdatePermissionsRequest {
            user_id: WireIdentifier::named(user).expect("username must be valid"),
            permissions: None,
        };
        self.build_request(Operation::UpdatePermissions, &wire.to_bytes())
    }

    /// # Panics
    /// Panics if `name` is not a valid `WireName`.
    pub fn create_personal_access_token(&self, name: &str, expiry: u64) -> Message<RequestHeader> {
        let wire = CreatePersonalAccessTokenRequest {
            user_id: 0,
            name: WireName::new(name).expect("PAT name must be valid"),
            expiry,
            // Deterministic stub for the simulator. Production servers mint
            // this in `maybe_rewrite_pat_request` on the primary; the
            // simulator drives the wire path directly without that rewrite
            // step.
            token_hash: [b'a'; 64],
        };
        self.build_request(Operation::CreatePersonalAccessToken, &wire.to_bytes())
    }

    /// # Panics
    /// Panics if `name` is not a valid `WireName`.
    pub fn delete_personal_access_token(&self, name: &str) -> Message<RequestHeader> {
        let wire = DeletePersonalAccessTokenRequest {
            user_id: 0,
            name: WireName::new(name).expect("PAT name must be valid"),
            only_if_expired: false,
        };
        self.build_request(Operation::DeletePersonalAccessToken, &wire.to_bytes())
    }

    /// Build a `SendMessages` request in the legacy `SendMessagesEncoder` wire
    /// shape, byte-compatible with what the real SDK sends (`common` binary
    /// client). VSR clients resolve to an explicit partition before sending, so
    /// the sim always emits `WirePartitioning::PartitionId`: that is the shape
    /// the shell's `resolve_partition_request_namespace` decodes, and the raw
    /// path converts it to `SendMessages2` via `transcode_legacy_request`.
    ///
    /// # Panics
    /// Panics if a namespace id exceeds `u32` or the request buffer is invalid.
    pub fn send_messages(
        &self,
        namespace: IggyNamespace,
        messages: &[Bytes],
    ) -> Message<RequestHeader> {
        let to_u32 = |v: usize| u32::try_from(v).expect("namespace id fits u32");
        let stream_id = WireIdentifier::Numeric(to_u32(namespace.stream_id()));
        let topic_id = WireIdentifier::Numeric(to_u32(namespace.topic_id()));
        let partitioning = WirePartitioning::PartitionId(to_u32(namespace.partition_id()));

        // Stamp a deterministic, non-zero id per message. `id: 0` (the real
        // SDK's server-assigned path) would make the server mint an unseeded
        // random UUID into the replicated body, breaking seeded replay of any
        // produce; see `next_message_id`. `origin_timestamp: 0` keeps the batch
        // bytes seed-independent.
        let raw: Vec<RawMessage<'_>> = messages
            .iter()
            .map(|message| RawMessage {
                id: self.next_message_id(),
                origin_timestamp: 0,
                headers: None,
                payload: message.as_ref(),
            })
            .collect();

        let size = SendMessagesEncoder::encoded_size(&stream_id, &topic_id, &partitioning, &raw);
        let mut buf = BytesMut::with_capacity(size);
        SendMessagesEncoder::encode(&mut buf, &stream_id, &topic_id, &partitioning, &raw);

        self.build_request_with_namespace(Operation::SendMessages, &buf, namespace)
    }

    /// Build a `POLL_MESSAGES` request for an individual consumer, reading
    /// `count` messages from offset 0 of `namespace`'s partition.
    ///
    /// A `NonReplicated` read: the command code sits in the header's
    /// `reserved` prefix, and the request id ECHOES the current metadata
    /// counter without advancing it (matching the SDK), so a read never
    /// gaps the replicated sequence the server's `ClientTable` requires
    /// gap-free. Requires a bound session (polls are auth-gated).
    ///
    /// # Panics
    /// Panics if the session is unbound or the request buffer is invalid.
    #[allow(clippy::cast_possible_truncation)]
    pub fn poll_messages(&self, namespace: IggyNamespace, count: u32) -> Message<RequestHeader> {
        let (stream_id, topic_id, partition_id) = namespace_ids(namespace);
        let body = PollMessagesRequest {
            consumer: WireConsumer::consumer(WireIdentifier::Numeric(self.client_id as u32)),
            stream_id,
            topic_id,
            partition_id,
            strategy: WirePollingStrategy::first(),
            count,
            auto_commit: false,
        }
        .to_bytes();

        let header_size = std::mem::size_of::<RequestHeader>();
        let total_size = header_size + body.len();
        let mut reserved = [0u8; 52];
        reserved[..4].copy_from_slice(&POLL_MESSAGES_CODE.to_le_bytes());
        let header = RequestHeader {
            command: iggy_binary_protocol::Command2::Request,
            operation: Operation::NonReplicated,
            size: total_size as u32,
            client: self.client_id,
            session: self.session_id(),
            request: self.request_counter.get(),
            reserved,
            namespace: namespace.inner(),
            ..Default::default()
        };

        let mut buffer = Vec::with_capacity(total_size);
        buffer.extend_from_slice(bytemuck::bytes_of(&header));
        buffer.extend_from_slice(&body);
        Message::try_from(Owned::<4096>::copy_from_slice(&buffer))
            .expect("poll request must be valid")
    }

    pub fn store_consumer_offset(
        &self,
        namespace: IggyNamespace,
        consumer_kind: u8,
        consumer_id: u32,
        offset: u64,
    ) -> Message<RequestHeader> {
        let (stream_id, topic_id, partition_id) = namespace_ids(namespace);
        let request = StoreConsumerOffsetRequest {
            consumer: namespace_consumer(consumer_kind, consumer_id),
            stream_id,
            topic_id,
            partition_id,
            offset,
        };
        self.build_request_with_namespace(
            Operation::StoreConsumerOffset,
            &request.to_bytes(),
            namespace,
        )
    }

    pub fn delete_consumer_offset(
        &self,
        namespace: IggyNamespace,
        consumer_kind: u8,
        consumer_id: u32,
    ) -> Message<RequestHeader> {
        let (stream_id, topic_id, partition_id) = namespace_ids(namespace);
        let request = DeleteConsumerOffsetRequest {
            consumer: namespace_consumer(consumer_kind, consumer_id),
            stream_id,
            topic_id,
            partition_id,
        };
        self.build_request_with_namespace(
            Operation::DeleteConsumerOffset,
            &request.to_bytes(),
            namespace,
        )
    }

    /// Store offset with explicit `AckLevel`. `NoAck` takes the primary's
    /// fast path (no replication); `Quorum` goes through VSR.
    ///
    /// # Panics
    /// Panics on payload too large for `Owned::<4096>` or invalid
    /// `Message<RequestHeader>` parse; both are simulator misconfig.
    pub fn store_consumer_offset_2(
        &self,
        namespace: IggyNamespace,
        consumer_kind: u8,
        consumer_id: u32,
        offset: u64,
        ack: AckLevel,
    ) -> Message<RequestHeader> {
        let (stream_id, topic_id, partition_id) = namespace_ids(namespace);
        let request = StoreConsumerOffset2Request {
            consumer: namespace_consumer(consumer_kind, consumer_id),
            stream_id,
            topic_id,
            partition_id,
            offset,
            ack,
        };
        self.build_request_with_namespace(
            Operation::StoreConsumerOffset2,
            &request.to_bytes(),
            namespace,
        )
    }

    /// Delete offset with explicit `AckLevel`.
    ///
    /// # Panics
    /// Panics on payload too large for `Owned::<4096>` or invalid
    /// `Message<RequestHeader>` parse; both are simulator misconfig.
    pub fn delete_consumer_offset_2(
        &self,
        namespace: IggyNamespace,
        consumer_kind: u8,
        consumer_id: u32,
        ack: AckLevel,
    ) -> Message<RequestHeader> {
        let (stream_id, topic_id, partition_id) = namespace_ids(namespace);
        let request = DeleteConsumerOffset2Request {
            consumer: namespace_consumer(consumer_kind, consumer_id),
            stream_id,
            topic_id,
            partition_id,
            ack,
        };
        self.build_request_with_namespace(
            Operation::DeleteConsumerOffset2,
            &request.to_bytes(),
            namespace,
        )
    }

    fn build_request_with_namespace(
        &self,
        operation: Operation,
        payload: &[u8],
        namespace: IggyNamespace,
    ) -> Message<RequestHeader> {
        let header_size = std::mem::size_of::<RequestHeader>();
        let total_size = header_size + payload.len();

        let header = self.header(operation, namespace.inner(), total_size);

        let header_bytes = bytemuck::bytes_of(&header);
        let mut buffer = Vec::with_capacity(total_size);
        buffer.extend_from_slice(header_bytes);
        buffer.extend_from_slice(payload);

        Message::try_from(Owned::<4096>::copy_from_slice(&buffer))
            .expect("request buffer must contain a valid request message")
    }

    fn build_request(&self, operation: Operation, payload: &[u8]) -> Message<RequestHeader> {
        let header_size = std::mem::size_of::<RequestHeader>();
        let total_size = header_size + payload.len();

        // Every `build_request` caller is a metadata-plane op (partition
        // ops go through `build_request_with_namespace`), and metadata
        // requests carry the metadata consensus namespace on the wire.
        let header = self.header(operation, METADATA_CONSENSUS_NAMESPACE, total_size);

        let header_bytes = bytemuck::bytes_of(&header);
        let mut buffer = Vec::with_capacity(total_size);
        buffer.extend_from_slice(header_bytes);
        buffer.extend_from_slice(payload);

        Message::try_from(Owned::<4096>::copy_from_slice(&buffer))
            .expect("request buffer must contain a valid request message")
    }

    #[allow(clippy::cast_possible_truncation)]
    fn header(&self, operation: Operation, namespace: u64, total_size: usize) -> RequestHeader {
        RequestHeader {
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
            request: self.request_id_for(operation),
            namespace,
            ..Default::default()
        }
    }
}

/// Build a numeric-id `WireConsumer` for the offset-store/delete requests.
/// The partition plane resolves numeric consumer ids verbatim, so this
/// mirrors the real SDK wire shape (`[kind][WireIdentifier]`) rather than
/// the old fixed `[kind][u32]` prefix.
const fn namespace_consumer(kind: u8, consumer_id: u32) -> WireConsumer {
    WireConsumer {
        kind,
        id: WireIdentifier::Numeric(consumer_id),
    }
}

/// Decompose a namespace into the `(stream_id, topic_id, partition_id)` wire
/// identifiers the consumer-offset requests carry. Namespace ids are small
/// test values that always fit `u32`.
fn namespace_ids(ns: IggyNamespace) -> (WireIdentifier, WireIdentifier, Option<u32>) {
    let to_u32 = |v: usize| u32::try_from(v).expect("namespace id fits u32");
    (
        WireIdentifier::Numeric(to_u32(ns.stream_id())),
        WireIdentifier::Numeric(to_u32(ns.topic_id())),
        Some(to_u32(ns.partition_id())),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // A partition send between two metadata ops must not consume a metadata
    // request number: the server's `ClientTable` requires the metadata sequence
    // gap-free (`committed + 1`), else a gap permanently wedges the client's
    // metadata plane. Partition ops draw from a separate counter in a disjoint
    // range (`PARTITION_ID_BASE`), so the metadata sequence stays `1, 2, 3`
    // regardless of interleaved sends and a partition id never collides with a
    // metadata id.
    #[test]
    fn partition_ops_do_not_gap_the_metadata_request_sequence() {
        let client = SimClient::new(7);

        // Metadata ops advance (1, 2, 3); interleaved sends draw their own
        // disjoint sequence and leave the metadata counter untouched.
        assert_eq!(client.request_id_for(Operation::CreateStream), 1);
        assert_eq!(
            client.request_id_for(Operation::SendMessages),
            PARTITION_ID_BASE + 1
        );
        assert_eq!(client.request_id_for(Operation::CreateStream), 2);
        assert_eq!(
            client.request_id_for(Operation::SendMessages),
            PARTITION_ID_BASE + 2
        );
        assert_eq!(client.request_id_for(Operation::CreateStream), 3);
    }
}
