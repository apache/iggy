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

mod client;
mod consumer;
mod identifier;
mod messages;
mod pending_poll;
mod producer;
mod reader;
mod type_conversion;

use client::{
    Client, decode_metadata_change_record, decode_metadata_changes, decode_metadata_current_item,
    delete_connection as delete_client, encode_metadata_changes_query, encryption_key_fingerprint,
    encryption_key_fingerprint_base64, from_connection_string, new_connection,
    parse_metadata_change_cursor, parse_metadata_current_page_token,
    validate_metadata_changes_query,
};
use consumer::Consumer;
use messages::{BatchPositions, Checkpoint, Continuation, default_poll_options, make_message};
use pending_poll::PendingPoll;
use producer::Producer;
use reader::PartitionReader;
use std::sync::LazyLock;

static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
});

#[cxx::bridge(namespace = "iggy::ffi")]
mod ffi {
    /// Local to one call. Read after a bridge exception; IDs remain exact u64 values.
    #[derive(Default)]
    struct ErrorInfo {
        code: u32,
        kind: String,
        has_operation_id: bool,
        operation_id: u64,
    }

    struct Identifier {
        kind: String,
        length: u8,
        value: Vec<u8>,
    }

    struct Topic {
        id: u32,
        created_at: u64,
        name: String,
        size_bytes: u64,
        message_expiry: u64,
        compression_algorithm: String,
        max_topic_size: u64,
        messages_count: u64,
        partitions_count: u32,
        /// Options the creating client set explicitly. Carried as
        /// `HeaderEntry` because options ride the user-headers codec: the same
        /// TLV a message's `user_headers` uses, with string keys.
        options: Vec<HeaderEntry>,
        /// Options admission resolved for the keys the client left unset. These
        /// would have resolved differently under another server config.
        derived_options: Vec<HeaderEntry>,
    }

    /// Partition incarnation and consumer-group owner. Checkpoints and
    /// continuations keep the context of their batch privately, so a delayed
    /// commit or read fails instead of landing on a recreated partition or
    /// under a newer owner.
    struct PartitionContext {
        incarnation: u64,
        owner_generation: u64,
        metadata_op: u64,
    }

    struct Partition {
        id: u32,
        created_at: u64,
        segments_count: u32,
        current_offset: u64,
        size_bytes: u64,
        messages_count: u64,
        /// The owner generation is zero: topic details belong to no consumer
        /// group.
        context: PartitionContext,
    }

    struct TopicDetails {
        id: u32,
        created_at: u64,
        name: String,
        size_bytes: u64,
        message_expiry: u64,
        compression_algorithm: String,
        max_topic_size: u64,
        messages_count: u64,
        partitions_count: u32,
        partitions: Vec<Partition>,
        /// See [`Topic::options`].
        options: Vec<HeaderEntry>,
        /// See [`Topic::derived_options`].
        derived_options: Vec<HeaderEntry>,
    }

    struct Stream {
        id: u32,
        created_at: u64,
        name: String,
        size_bytes: u64,
        messages_count: u64,
        topics_count: u32,
        /// Options the creating client set explicitly. Key material is never
        /// returned.
        options: Vec<HeaderEntry>,
        /// Options the server filled in, including the read-only encryption
        /// projections.
        derived_options: Vec<HeaderEntry>,
        has_encryption_key_source: bool,
        encryption_key_source: StreamEncryptionSource,
        has_encryption_key_fingerprint: bool,
        encryption_key_fingerprint: String,
        /// One per server key that wraps the stream's key, empty for a
        /// plaintext stream.
        encryption_kek_fingerprints: Vec<String>,
    }

    /// Who chose a stream's at-rest data key.
    #[repr(u8)]
    enum StreamEncryptionSource {
        None = 0,
        Stream = 1,
        Server = 2,
    }

    #[repr(u8)]
    enum HeaderKind {
        Raw = 1,
        String = 2,
        Bool = 3,
        Int8 = 4,
        Int16 = 5,
        Int32 = 6,
        Int64 = 7,
        Int128 = 8,
        Uint8 = 9,
        Uint16 = 10,
        Uint32 = 11,
        Uint64 = 12,
        Uint128 = 13,
        Float32 = 14,
        Float64 = 15,
    }

    struct HeaderField {
        kind: u8,
        value: Vec<u8>,
    }

    struct HeaderEntry {
        key: HeaderField,
        value: HeaderField,
    }

    /// One key of a resource's option catalog, as served by
    /// `describe_options`.
    ///
    /// This is the discovery surface for the keys a create or an update takes.
    /// A key outside the server catalog is refused, and the binary transports
    /// carry back only an error code, so nothing in the rejection names the
    /// keys that would have worked.
    struct OptionSpec {
        key: String,
        /// Wire kind code the value is encoded under, the same encoding
        /// [`HeaderField::kind`] carries.
        kind: u8,
        /// The default in `kind`'s encoding. Empty when the key has no default.
        default_value: Vec<u8>,
        description: String,
        updatable: bool,
        resettable: bool,
        /// Write-only: accepted at create, never returned.
        secret: bool,
        /// Computed by the server and returned on reads, never accepted.
        read_only: bool,
        /// A node that does not understand the key refuses to serve the
        /// resource.
        critical: bool,
    }

    struct IggyMessageToSend {
        id_lo: u64,
        id_hi: u64,
        payload: Vec<u8>,
        user_headers: Vec<HeaderEntry>,
    }

    struct IggyMessagePolled {
        checksum: u64,
        id_lo: u64,
        id_hi: u64,
        offset: u64,
        timestamp: u64,
        origin_timestamp: u64,
        user_headers_length: u32,
        payload_length: u32,
        reserved: u64,
        payload: Vec<u8>,
        user_headers: Vec<HeaderEntry>,
    }

    /// When the offsets of the messages that a read returns are stored.
    #[repr(u8)]
    enum CommitPolicy {
        /// Nothing is stored. The caller commits checkpoints. The default.
        Manual = 0,
        /// The server stores the offset of the last message of each read as
        /// part of the read: one round trip, at most once.
        OnRead = 1,
        /// The reader stores the latest counted position, as the `auto_`
        /// fields of `PollOptions` say. A one-shot poll refuses it with
        /// InvalidConfiguration (12), as it has nothing to count.
        Auto = 2,
    }

    /// The event on which `CommitPolicy::Auto` also stores.
    #[repr(u8)]
    enum CommitEvery {
        /// Each counted message, or each counted batch of a reader.
        Message = 0,
        /// `auto_every_messages` counted messages of a partition since its
        /// last store.
        Messages = 1,
        /// The last message of each batch.
        Batch = 2,
    }

    /// When a message counts for `CommitPolicy::Auto`.
    #[repr(u8)]
    enum CommitAfter {
        /// When the caller asks for the next batch or closes the reader.
        Processing = 0,
        /// When the batch is handed over.
        Delivery = 1,
        /// Only when the caller marks its checkpoint.
        Marked = 2,
    }

    /// The limits of one poll of one partition and when its offsets are
    /// stored. `default_poll_options` returns the defaults, whose `auto_`
    /// fields hold the defaults of `CommitPolicy::Auto`: every 10,000
    /// messages or 5 s, after processing. The `auto_` fields count only
    /// with `CommitPolicy::Auto`, which needs an interval or an event.
    #[derive(Clone, Copy)]
    struct PollOptions {
        /// Most messages in one batch. Default 1000.
        count: u32,
        /// Zero answers at once. At most 600 s.
        max_wait_micros: u64,
        /// At least 1 and at most `count`. Default 1.
        min_count: u32,
        /// Default 1 MiB.
        max_bytes: u32,
        /// Default `CommitPolicy::Manual`.
        commit: CommitPolicy,
        /// Store the latest counted position at this interval, when it
        /// moved. Not zero.
        has_auto_interval: bool,
        auto_interval_micros: u64,
        /// Also store it on this event.
        has_auto_every: bool,
        auto_every: CommitEvery,
        /// The count of `CommitEvery::Messages`. Not zero.
        auto_every_messages: u32,
        auto_after: CommitAfter,
    }

    /// Where a read starts. `kind` is offset, timestamp, first, last or
    /// next, and `value` is the offset or the timestamp. To read on after a
    /// batch, pass its `Continuation` instead.
    struct PollingStrategy {
        kind: String,
        value: u64,
    }

    struct PolledMessages {
        partition_id: u32,
        current_offset: u64,
        count: u32,
        messages: Vec<IggyMessagePolled>,
        /// The private part that `checkpoint`, `checkpoint_at` and
        /// `continuation` read.
        positions: Box<BatchPositions>,
    }

    /// The settings of a reader. A setting whose `has_` flag is false keeps
    /// its default, so a zero-initialized value gives the default reader.
    struct ReaderOptions {
        /// The partitions of a standalone consumer, which requires them. Empty
        /// for a consumer group, which reads the assignment of its member.
        partitions: Vec<u32>,
        /// The start of a partition without a position. Default next.
        has_start: bool,
        start: PollingStrategy,
        /// The options of each request. Default: the poll defaults with a
        /// five-second wait.
        has_poll_options: bool,
        poll_options: PollOptions,
        /// The encoded bytes of the requests in flight and the queued
        /// batches. Default 16 MiB.
        has_prefetch_bytes: bool,
        prefetch_bytes: u32,
        /// The messages of the requests in flight and the queued batches.
        /// Default: the larger of 16,000 and `count`.
        has_prefetch_messages: bool,
        prefetch_messages: u32,
    }

    /// The settings of a consumer. A setting whose `has_` flag is false keeps
    /// its default, so a zero-initialized value gives the default consumer.
    struct ConsumerOptions {
        /// The partitions of a standalone consumer, which requires them. A
        /// consumer group reads the assignment of its member and ignores them.
        partitions: Vec<u32>,
        /// The start of a partition without a stored offset. Default next.
        has_start: bool,
        start: PollingStrategy,
        /// The options of each request. Default: the poll defaults with a
        /// five-second wait.
        has_poll_options: bool,
        poll_options: PollOptions,
        has_prefetch_bytes: bool,
        prefetch_bytes: u32,
        has_prefetch_messages: bool,
        prefetch_messages: u32,
        /// Hands over again the messages that a seek back reads twice.
        allow_replay: bool,
    }

    /// One message that a consumer handed over.
    struct ReceivedMessage {
        partition_id: u32,
        /// The offset of the newest message of the partition when it was polled.
        current_offset: u64,
        message: IggyMessagePolled,
        /// The position after the message, to commit or mark it.
        checkpoint: Box<Checkpoint>,
    }

    /// What a wait for a pending poll found. `batch` is empty and belongs to
    /// no partition while `ready` is false.
    struct PollOutcome {
        ready: bool,
        batch: PolledMessages,
    }

    /// Local to one `next` call of a reader. Read after a bridge exception.
    #[derive(Default)]
    struct ReadErrorInfo {
        error: ErrorInfo,
        /// False for an error of the whole reader.
        has_partition_id: bool,
        partition_id: u32,
        /// For a message that does not decode: one continuation after it. The
        /// reader reads the message again until a `seek` of its partition to
        /// this continuation passes it. The messages before it arrive in a
        /// batch first. Empty for every other error: cxx has no `Option`, and
        /// a `Box` is never empty.
        // A `rust::Vec` holds an opaque Rust type only behind a `Box`.
        #[allow(clippy::vec_box)]
        skip: Vec<Box<Continuation>>,
    }

    /// Commit confirmation for one partition written by `send_messages`.
    struct SendMessagesConfirmation {
        stream_id: u32,
        topic_id: u32,
        partition_id: u32,
        /// Offset assigned to the first message of the batch in this partition.
        ///
        /// Sends are at-least-once: an earlier retry may already have committed
        /// the same batch at a lower offset, so this never identifies a batch
        /// uniquely.
        ///
        /// Confirmation follows VSR quorum commit. Persisted message durability
        /// also requires recoverable stable-storage copies on the quorum.
        base_offset: u64,
    }

    /// Reply to `send_messages`.
    struct SendMessagesResponse {
        /// One entry per partition the batch was written to. Empty whenever the
        /// server reports no offsets, which is every send against the legacy
        /// server, so call `empty()` before indexing.
        confirmations: Vec<SendMessagesConfirmation>,
    }

    struct StreamDetails {
        id: u32,
        created_at: u64,
        name: String,
        size_bytes: u64,
        messages_count: u64,
        topics_count: u32,
        topics: Vec<Topic>,
        /// See [`Stream::options`].
        options: Vec<HeaderEntry>,
        /// See [`Stream::derived_options`].
        derived_options: Vec<HeaderEntry>,
        has_encryption_key_source: bool,
        encryption_key_source: StreamEncryptionSource,
        has_encryption_key_fingerprint: bool,
        encryption_key_fingerprint: String,
        encryption_kek_fingerprints: Vec<String>,
    }

    struct ConsumerGroupMember {
        id: u32,
        partitions_count: u32,
        partitions: Vec<u32>,
    }

    struct ConsumerGroupDetails {
        id: u32,
        name: String,
        partitions_count: u32,
        members_count: u32,
        members: Vec<ConsumerGroupMember>,
    }

    struct ConsumerGroup {
        id: u32,
        name: String,
        partitions_count: u32,
        members_count: u32,
    }

    struct ConsumerGroupInfo {
        stream_id: u32,
        topic_id: u32,
        group_id: u32,
    }

    struct ConsumerOffsetInfo {
        partition_id: u32,
        current_offset: u64,
        stored_offset: u64,
    }

    struct ClientInfo {
        client_id: u32,
        has_user_id: bool,
        user_id: u32,
        address: String,
        transport: String,
        consumer_groups_count: u32,
    }

    struct ClientInfoDetails {
        client_id: u32,
        has_user_id: bool,
        user_id: u32,
        address: String,
        transport: String,
        consumer_groups_count: u32,
        consumer_groups: Vec<ConsumerGroupInfo>,
    }

    struct CacheMetricEntry {
        stream_id: u32,
        topic_id: u32,
        partition_id: u32,
        hits: u64,
        misses: u64,
        hit_ratio: f32,
    }

    struct Stats {
        process_id: u32,
        cpu_usage: f32,
        total_cpu_usage: f32,
        memory_usage: u64,
        total_memory: u64,
        available_memory: u64,
        run_time_micros: u64,
        start_time_epoch_micros: u64,
        read_bytes: u64,
        written_bytes: u64,
        messages_size_bytes: u64,
        streams_count: u32,
        topics_count: u32,
        partitions_count: u32,
        segments_count: u32,
        messages_count: u64,
        clients_count: u32,
        consumer_groups_count: u32,
        hostname: String,
        os_name: String,
        os_version: String,
        kernel_version: String,
        iggy_server_version: String,
        // `iggy_server_semver` is only meaningful when this flag is true.
        has_server_semver: bool,
        // Uses `0` when the Rust `Option<u32>` is absent; check `has_server_semver`
        // before reading this field.
        iggy_server_semver: u32,
        cache_metrics: Vec<CacheMetricEntry>,
        threads_count: u32,
        free_disk_space: u64,
        total_disk_space: u64,
        // `0` when the server cannot count its open file descriptors.
        open_files_count: u64,
        // `0` when the server cannot read its soft `RLIMIT_NOFILE`.
        open_files_limit: u64,
    }

    struct TransportEndpoints {
        tcp: u16,
        quic: u16,
        http: u16,
        websocket: u16,
    }

    struct ClusterNode {
        replica_id: u8,
        name: String,
        ip: String,
        endpoints: TransportEndpoints,
        role: String,
        status: String,
    }

    struct ClusterMetadata {
        name: String,
        nodes: Vec<ClusterNode>,
    }

    struct GlobalPermissions {
        manage_servers: bool,
        read_servers: bool,
        manage_users: bool,
        read_users: bool,
        manage_streams: bool,
        read_streams: bool,
        manage_topics: bool,
        read_topics: bool,
        poll_messages: bool,
        send_messages: bool,
    }

    struct TopicPermissions {
        manage_topic: bool,
        read_topic: bool,
        poll_messages: bool,
        send_messages: bool,
    }

    struct TopicPermissionEntry {
        topic_id: u32,
        permissions: TopicPermissions,
    }

    struct StreamPermissions {
        manage_stream: bool,
        read_stream: bool,
        manage_topics: bool,
        read_topics: bool,
        poll_messages: bool,
        send_messages: bool,
        topics: Vec<TopicPermissionEntry>,
    }

    struct StreamPermissionEntry {
        stream_id: u32,
        permissions: StreamPermissions,
    }

    struct Permissions {
        global: GlobalPermissions,
        streams: Vec<StreamPermissionEntry>,
    }

    #[repr(u8)]
    enum UserStatus {
        Active = 1,
        Inactive = 2,
    }

    struct UserInfo {
        id: u32,
        created_at: u64,
        status: UserStatus,
        username: String,
        options: Vec<HeaderEntry>,
        derived_options: Vec<HeaderEntry>,
    }

    struct UserInfoDetails {
        id: u32,
        created_at: u64,
        status: UserStatus,
        username: String,
        has_permissions: bool,
        permissions: Permissions,
        options: Vec<HeaderEntry>,
        derived_options: Vec<HeaderEntry>,
    }

    struct LoginInfo {
        user_id: u32,
        has_access_token: bool,
        access_token: String,
        access_token_expiry: u64,
    }

    #[repr(u8)]
    enum AutoLoginKind {
        Disabled = 0,
        UsernamePassword,
        PersonalAccessToken,
    }

    struct IggyClientConfig {
        server_address: String,
        auto_login_kind: AutoLoginKind,
        username: String,
        password: String,
        personal_access_token: String,
        has_reconnection_max_retries: bool,
        reconnection_max_retries: u32,
        has_reconnection_interval: bool,
        reconnection_interval_micros: u64,
        has_reestablish_after: bool,
        reestablish_after_micros: u64,
        tls_enabled: bool,
        tls_domain: String,
        tls_ca_file: String,
        has_tls_validate_certificate: bool,
        tls_validate_certificate: bool,
        no_delay: bool,
        has_pending_operations: bool,
        pending_max_operations: u64,
        pending_max_bytes: u64,
    }

    struct TopicCreateOptions {
        has_partitions_count: bool,
        partitions_count: u32,
        has_compression_algorithm: bool,
        compression_algorithm: String,
        has_message_expiry: bool,
        message_expiry_kind: String,
        message_expiry_value: u64,
        has_max_topic_size: bool,
        max_topic_size: String,
        has_segment_size: bool,
        segment_size: u64,
        has_durability: bool,
        durability: String,
        has_consumer_offset_durability: bool,
        consumer_offset_durability: String,
        has_messages_required_to_save: bool,
        messages_required_to_save: u32,
        has_size_of_messages_required_to_save: bool,
        size_of_messages_required_to_save: u64,
        has_preallocate_segments: bool,
        preallocate_segments: bool,
        has_partition_resize_policy: bool,
        partition_resize_policy: String,
        raw_options: Vec<HeaderEntry>,
    }

    /// Reset keys travel as catalog names and are validated against the
    /// scope's catalog before anything is sent. Raw reset names are left to
    /// the server.
    struct TopicUpdateOptions {
        has_name: bool,
        name: String,
        has_message_expiry: bool,
        message_expiry_kind: String,
        message_expiry_value: u64,
        has_max_topic_size: bool,
        max_topic_size: String,
        reset: Vec<String>,
        raw_reset: Vec<String>,
        raw_options: Vec<HeaderEntry>,
    }

    /// The key is either 32 raw bytes or their canonical padded base64, never
    /// both. An invalid key is refused before anything is sent.
    struct StreamCreateOptions {
        has_encryption_key: bool,
        encryption_key: Vec<u8>,
        has_encryption_key_base64: bool,
        encryption_key_base64: String,
        raw_options: Vec<HeaderEntry>,
    }

    struct StreamUpdateOptions {
        has_name: bool,
        name: String,
        reset: Vec<String>,
        raw_reset: Vec<String>,
        raw_options: Vec<HeaderEntry>,
    }

    struct UserCreateOptions {
        raw_options: Vec<HeaderEntry>,
    }

    struct UserUpdateOptions {
        has_username: bool,
        username: String,
        has_status: bool,
        status: UserStatus,
        reset: Vec<String>,
        raw_reset: Vec<String>,
        raw_options: Vec<HeaderEntry>,
    }

    /// Where a metadata change query starts. The values are the wire codes.
    #[repr(u8)]
    enum MetadataChangeStart {
        Earliest = 0,
        Latest = 1,
        After = 2,
        Current = 3,
    }

    /// Position of the last change record a caller consumed, with its `mc1_`
    /// text form.
    #[derive(Default)]
    struct MetadataChangeCursor {
        log_id_lo: u64,
        log_id_hi: u64,
        op: u64,
        checksum_lo: u64,
        checksum_hi: u64,
        ordinal: u32,
        text: String,
    }

    /// Continuation of a current state scan, with its `cs1_` text form.
    #[derive(Default)]
    struct MetadataCurrentPageToken {
        text: String,
        anchor: MetadataChangeCursor,
        required_head: u64,
        groups_mask: u8,
        last_group: u8,
        last_key: Vec<u8>,
    }

    /// A metadata change query. Cursor and page token travel as their text
    /// forms, an empty page token starts a current state scan.
    struct MetadataChangesQuery {
        start: MetadataChangeStart,
        cursor: String,
        page_token: String,
        groups: Vec<u8>,
        has_since: bool,
        since: u64,
        has_until: bool,
        until: u64,
        limit: u32,
    }

    /// Whether this build knows a record's or item's group and kind.
    #[repr(u8)]
    enum MetadataChangeForm {
        Known = 0,
        /// A binary body this build does not know, kept in `unknown_body`.
        Unknown = 1,
        /// A JSON object this build does not know, kept whole in `unknown_body`.
        UnknownJson = 2,
    }

    /// Every field a record or item body can carry. A body fills the fields
    /// its kind carries and leaves the rest at zero.
    struct MetadataChangeBody {
        stream_id: u32,
        stream_created_op: u64,
        topic_id: u32,
        topic_created_op: u64,
        group_id: u32,
        has_member_id: bool,
        member_id: u32,
        partition_id: u32,
        created_at: u64,
        /// The stream, topic, consumer group or token name, or the username.
        name: String,
        partitions_count: u32,
        consumer_groups_count: u32,
        members_count: u32,
        client_id_lo: u64,
        client_id_hi: u64,
        /// Removed ids of a partition count change, installed ids of a member.
        partition_ids: Vec<u32>,
        has_requested_op: bool,
        requested_op: u64,
        /// Wire code of a removal or permission change reason.
        reason: u8,
        user_id: u32,
        user_created_at: u64,
        status: UserStatus,
        has_permissions: bool,
        permissions: Permissions,
        /// False when the server holds option state this build cannot read.
        options_readable: bool,
        options: Vec<HeaderEntry>,
        derived_options: Vec<HeaderEntry>,
        has_expiry: bool,
        expiry: u64,
        session: u64,
        unknown_body: Vec<u8>,
    }

    /// One change record. `group` and `kind` are wire codes, the names of a
    /// JSON record this build does not know are in `group_name` and
    /// `kind_name`.
    struct MetadataChangeRecord {
        op: u64,
        ordinal: u32,
        checksum_lo: u64,
        checksum_hi: u64,
        timestamp: u64,
        has_actor_user_id: bool,
        actor_user_id: u32,
        form: MetadataChangeForm,
        group: u8,
        kind: u8,
        group_name: String,
        kind_name: String,
        body: MetadataChangeBody,
    }

    /// One current state item. See [`MetadataChangeRecord`] for the codes.
    struct MetadataCurrentItem {
        form: MetadataChangeForm,
        group: u8,
        group_name: String,
        body: MetadataChangeBody,
    }

    /// One page of the change log or of the current state. `status` and
    /// `phase` are wire codes.
    struct MetadataChanges {
        status: u8,
        phase: u8,
        has_log_id: bool,
        log_id_lo: u64,
        log_id_hi: u64,
        oldest_op: u64,
        retained_since: u64,
        head_op: u64,
        has_next: bool,
        next: MetadataChangeCursor,
        has_more: bool,
        records: Vec<MetadataChangeRecord>,
        items: Vec<MetadataCurrentItem>,
        has_current_page_token: bool,
        current_page_token: MetadataCurrentPageToken,
    }

    extern "Rust" {
        /// The private part of a batch: the partition context and the reader
        /// stamp of its checkpoints, and its continuation.
        type BatchPositions;
        /// The position after one delivered message, from `checkpoint` or
        /// `checkpoint_at` of a batch. A commit stores its offset, which
        /// covers every earlier message of its partition. It keeps the
        /// context of its batch, so a delayed commit fails after a change of
        /// the partition history, and a reader commits only the checkpoints
        /// that it delivered.
        type Checkpoint;
        type Client;
        type Consumer;
        /// The position after a batch, from `continuation` of the batch or
        /// `skip` of a read error. It keeps the partition and the context of
        /// its batch: a read of another partition refuses it, and a read after
        /// a change of the partition history fails with HistoryUnavailable
        /// (87).
        type Continuation;
        type PartitionReader;
        /// A poll that runs in the background, from `start_poll_messages`.
        /// Destroying it cancels the poll.
        type PendingPoll;
        type Producer;

        // Client functions
        fn new_connection(config: IggyClientConfig) -> Result<*mut Client>;
        fn from_connection_string(connection_string: String) -> Result<*mut Client>;
        fn login_user(
            self: &Client,
            username: String,
            password: String,
            error_info: &mut ErrorInfo,
        ) -> Result<LoginInfo>;
        fn logout_user(self: &Client, error_info: &mut ErrorInfo) -> Result<()>;
        fn connect(self: &Client) -> Result<()>;
        fn resolve_operation(
            self: &Client,
            op: u64,
            timeout_micros: u64,
            error_info: &mut ErrorInfo,
        ) -> Result<()>;
        fn abandon_operation(self: &Client, op: u64, error_info: &mut ErrorInfo) -> Result<()>;
        fn create_stream(
            self: &Client,
            stream_name: String,
            options: StreamCreateOptions,
            error_info: &mut ErrorInfo,
        ) -> Result<StreamDetails>;
        fn update_stream(
            self: &Client,
            stream_id: Identifier,
            options: StreamUpdateOptions,
            error_info: &mut ErrorInfo,
        ) -> Result<()>;
        fn get_streams(self: &Client) -> Result<Vec<Stream>>;
        fn get_stream(self: &Client, stream_id: Identifier) -> Result<StreamDetails>;
        fn delete_stream(
            self: &Client,
            stream_id: Identifier,
            error_info: &mut ErrorInfo,
        ) -> Result<()>;
        fn create_topic(
            self: &Client,
            stream_id: Identifier,
            topic_name: String,
            options: TopicCreateOptions,
            error_info: &mut ErrorInfo,
        ) -> Result<TopicDetails>;
        fn get_topic(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
        ) -> Result<TopicDetails>;
        fn get_topics(self: &Client, stream_id: Identifier) -> Result<Vec<Topic>>;
        fn update_topic(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            options: TopicUpdateOptions,
            error_info: &mut ErrorInfo,
        ) -> Result<()>;
        fn delete_topic(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            error_info: &mut ErrorInfo,
        ) -> Result<()>;
        fn create_partitions(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            partitions_count: u32,
            error_info: &mut ErrorInfo,
        ) -> Result<()>;
        fn delete_partitions(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            partitions_count: u32,
            error_info: &mut ErrorInfo,
        ) -> Result<()>;
        fn create_consumer_group(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            name: String,
            error_info: &mut ErrorInfo,
        ) -> Result<ConsumerGroupDetails>;
        fn get_consumer_group(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            group_id: Identifier,
        ) -> Result<ConsumerGroupDetails>;
        fn get_consumer_groups(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
        ) -> Result<Vec<ConsumerGroup>>;
        fn delete_consumer_group(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            group_id: Identifier,
            error_info: &mut ErrorInfo,
        ) -> Result<()>;
        fn join_consumer_group(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            group_id: Identifier,
            error_info: &mut ErrorInfo,
        ) -> Result<()>;
        fn leave_consumer_group(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            group_id: Identifier,
            error_info: &mut ErrorInfo,
        ) -> Result<()>;
        #[allow(clippy::too_many_arguments)]
        fn store_consumer_offset(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            partition_id: u32,
            consumer_kind: String,
            consumer_id: Identifier,
            offset: u64,
            error_info: &mut ErrorInfo,
        ) -> Result<()>;
        fn get_consumer_offset(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            partition_id: u32,
            consumer_kind: String,
            consumer_id: Identifier,
        ) -> Result<ConsumerOffsetInfo>;
        fn delete_consumer_offset(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            partition_id: u32,
            consumer_kind: String,
            consumer_id: Identifier,
            error_info: &mut ErrorInfo,
        ) -> Result<()>;
        /// Stores the offset of `checkpoint` under its context. Every retry
        /// keeps the context, so a deleted or recreated partition fails with
        /// HistoryUnavailable (87) and a lost consumer-group owner with
        /// ConsumerGroupPartitionNotOwned (5009).
        fn store_consumer_position(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            consumer_kind: String,
            consumer_id: Identifier,
            checkpoint: &Checkpoint,
            error_info: &mut ErrorInfo,
        ) -> Result<()>;
        /// Deletes the stored offset of the partition of `checkpoint` under
        /// its context, so a deleted or recreated partition fails with
        /// HistoryUnavailable (87), a lost consumer-group owner with
        /// ConsumerGroupPartitionNotOwned (5009), and a missing offset with
        /// ConsumerOffsetNotFound (3021). The offset of `checkpoint` is not
        /// used.
        fn delete_consumer_position(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            consumer_kind: String,
            consumer_id: Identifier,
            checkpoint: &Checkpoint,
            error_info: &mut ErrorInfo,
        ) -> Result<()>;

        fn default_poll_options() -> PollOptions;
        /// Reads one partition once. A strategy takes the context that the
        /// cached route reports. After another client deleted and recreated
        /// the partition, one call can fail with HistoryUnavailable (87), or
        /// ConsumerGroupPartitionNotOwned (5009) for a consumer group. The
        /// failed route is dropped and the next call routes again.
        ///
        /// With `CommitPolicy::OnRead`, a lost reply fails with
        /// FetchProgressUncertain (14010) and blocks the partition on this
        /// client: later polls of it fail with PartitionLaneBlocked (14009)
        /// until `abandon_operation` of the operation that the error names.
        #[allow(clippy::too_many_arguments)]
        fn poll_messages(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            partition_id: u32,
            consumer_kind: String,
            consumer_id: Identifier,
            strategy: PollingStrategy,
            options: PollOptions,
            error_info: &mut ErrorInfo,
        ) -> Result<PolledMessages>;
        /// Reads on from the continuation of a batch of the same partition.
        /// A continuation of another partition fails with InvalidCommand (3).
        #[cxx_name = "poll_messages"]
        #[allow(clippy::too_many_arguments)]
        fn poll_messages_from(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            partition_id: u32,
            consumer_kind: String,
            consumer_id: Identifier,
            continuation: &Continuation,
            options: PollOptions,
            error_info: &mut ErrorInfo,
        ) -> Result<PolledMessages>;
        /// The position after the last message, to commit the whole batch.
        /// Fails for an empty batch.
        fn checkpoint(self: &PolledMessages) -> Result<Box<Checkpoint>>;
        /// The position after `messages[index]`, to commit a processed part
        /// of the batch. It names the message delivered at `index`, from
        /// positions kept apart from `partition_id` and `messages`, so a
        /// batch edited in C++ commits no offset the reader did not deliver.
        /// Fails for an index out of range.
        fn checkpoint_at(self: &PolledMessages, index: usize) -> Result<Box<Checkpoint>>;
        /// The position after the batch, for the next read of its partition.
        /// An empty batch keeps the start of its request.
        fn continuation(self: &PolledMessages) -> Box<Continuation>;
        fn partition_id(self: &Checkpoint) -> u32;
        /// The offset of the delivered message, which a commit stores.
        fn offset(self: &Checkpoint) -> u64;
        fn copy(self: &Checkpoint) -> Box<Checkpoint>;
        /// offset after a batch with messages. After an empty batch, the kind
        /// of its start: offset, timestamp, last or next, where first becomes
        /// offset 0.
        fn kind(self: &Continuation) -> String;
        /// The offset or the timestamp of the next read, zero for last and
        /// next.
        fn value(self: &Continuation) -> u64;
        fn copy(self: &Continuation) -> Box<Continuation>;

        /// Starts a poll in the background, as `poll_messages` does, so the
        /// caller can wait for it in steps or cancel it.
        #[allow(clippy::too_many_arguments)]
        fn start_poll_messages(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            partition_id: u32,
            consumer_kind: String,
            consumer_id: Identifier,
            strategy: PollingStrategy,
            options: PollOptions,
        ) -> Result<Box<PendingPoll>>;
        /// Waits up to `timeout_micros` for the batch of the poll. After the
        /// batch or an error was returned, or after `cancel`, it fails.
        fn wait(
            self: &PendingPoll,
            timeout_micros: u64,
            error_info: &mut ErrorInfo,
        ) -> Result<PollOutcome>;
        /// Drops the request, also while another thread waits for it. A parked
        /// deferred poll is then cancelled on the server. It returns after the
        /// request was dropped and does nothing after the poll ended.
        fn cancel(self: &PendingPoll);

        /// Builds a reader of the partitions of a topic, with routing and
        /// prefetch. A consumer group reads the assignment of its member: the
        /// reader joins the group here, joins again after a session loss, and
        /// leaves it in `close`.
        fn reader(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            consumer_kind: String,
            consumer_id: Identifier,
            options: ReaderOptions,
            error_info: &mut ErrorInfo,
        ) -> Result<Box<PartitionReader>>;
        /// Waits up to `timeout_micros` for the next batch of one partition.
        /// An empty batch means that the time ran out. It belongs to no
        /// partition, so take no checkpoint and no continuation from it. A
        /// batch that arrives later stays in the reader. The reader stays
        /// usable after an error and retries by itself. Calls of one reader
        /// from several threads run one at a time, so another call waits for
        /// a running `next`, at most for its timeout.
        fn next(
            self: &PartitionReader,
            timeout_micros: u64,
            error_info: &mut ReadErrorInfo,
        ) -> Result<PolledMessages>;
        /// Stores the offset of a checkpoint that this reader delivered. A
        /// checkpoint of an older generation of its partition, or at or below
        /// the last commit, stores nothing. Refused with
        /// `CommitPolicy::OnRead`.
        fn commit(
            self: &PartitionReader,
            checkpoint: &Checkpoint,
            error_info: &mut ErrorInfo,
        ) -> Result<()>;
        /// Counts a checkpoint that this reader delivered under
        /// `CommitPolicy::Auto` and stores nothing now: the policy stores the
        /// latest counted position. With `CommitAfter::Marked` only marks
        /// count. Refused with InvalidCommand (3) without `Auto` and for a
        /// checkpoint that this reader did not deliver.
        fn mark(
            self: &PartitionReader,
            checkpoint: &Checkpoint,
            error_info: &mut ErrorInfo,
        ) -> Result<()>;
        /// Sets the next read position of a partition and drops its queued
        /// batches. It stores no offset, and it resumes a stopped partition.
        fn seek(
            self: &PartitionReader,
            partition_id: u32,
            start: PollingStrategy,
            error_info: &mut ErrorInfo,
        ) -> Result<()>;
        /// Moves a partition to the continuation of one of its batches, or
        /// past a message that does not decode with the `skip` of its read
        /// error. A continuation of another partition fails with
        /// InvalidCommand (3).
        #[cxx_name = "seek"]
        fn seek_to(
            self: &PartitionReader,
            partition_id: u32,
            continuation: &Continuation,
            error_info: &mut ErrorInfo,
        ) -> Result<()>;
        /// The partitions of a standalone consumer, or the current assignment
        /// of a group member.
        fn partitions(self: &PartitionReader) -> Result<Vec<u32>>;
        /// Stops the reader. With `CommitPolicy::Auto`, it stores the latest
        /// counted position of each partition. With `CommitPolicy::OnRead`,
        /// it moves the offset of each partition back to its last delivered
        /// message. A group reader then leaves its group. A second call does nothing,
        /// and every other call on a closed reader fails. Dropping a reader
        /// without `close` moves no offset back.
        fn close(self: &PartitionReader, error_info: &mut ErrorInfo) -> Result<()>;

        fn make_message(payload: Vec<u8>, user_headers: Vec<HeaderEntry>) -> IggyMessageToSend;

        #[allow(clippy::too_many_arguments)]
        fn send_messages(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            partitioning_kind: String,
            partitioning_value: Vec<u8>,
            messages: Vec<IggyMessageToSend>,
            error_info: &mut ErrorInfo,
        ) -> Result<SendMessagesResponse>;
        fn get_stats(self: &Client) -> Result<Stats>;
        fn get_me(self: &Client) -> Result<ClientInfoDetails>;
        fn get_client(self: &Client, client_id: u32) -> Result<ClientInfoDetails>;
        fn get_clients(self: &Client) -> Result<Vec<ClientInfo>>;
        /// Serves the option catalog of one scope, named "topic", "stream" or
        /// "user". A scope with no keys yet answers with an empty vector, which
        /// is an empty catalog rather than a failure.
        fn describe_options(self: &Client, scope: String) -> Result<Vec<OptionSpec>>;
        /// Reads one page of the metadata change log or of the current state.
        /// Needs root or the global manage_servers permission. An invalid
        /// query fails with InvalidCommand (3) before anything is sent.
        fn get_metadata_changes(
            self: &Client,
            query: MetadataChangesQuery,
            error_info: &mut ErrorInfo,
        ) -> Result<MetadataChanges>;
        /// Fingerprint of a 32-byte stream key, computed locally.
        fn encryption_key_fingerprint(key: &[u8], error_info: &mut ErrorInfo) -> Result<String>;
        /// Fingerprint of a stream key given as canonical padded base64.
        fn encryption_key_fingerprint_base64(
            key: &str,
            error_info: &mut ErrorInfo,
        ) -> Result<String>;
        fn parse_metadata_change_cursor(
            text: &[u8],
            error_info: &mut ErrorInfo,
        ) -> Result<MetadataChangeCursor>;
        fn parse_metadata_current_page_token(
            text: &[u8],
            error_info: &mut ErrorInfo,
        ) -> Result<MetadataCurrentPageToken>;
        fn validate_metadata_changes_query(
            query: MetadataChangesQuery,
            error_info: &mut ErrorInfo,
        ) -> Result<()>;
        /// The binary request payload of a query, without validating it.
        fn encode_metadata_changes_query(
            query: MetadataChangesQuery,
            error_info: &mut ErrorInfo,
        ) -> Result<Vec<u8>>;
        /// Decodes a response payload as the binary transports carry it.
        fn decode_metadata_changes(
            payload: &[u8],
            error_info: &mut ErrorInfo,
        ) -> Result<MetadataChanges>;
        fn decode_metadata_change_record(
            bytes: &[u8],
            error_info: &mut ErrorInfo,
        ) -> Result<MetadataChangeRecord>;
        fn decode_metadata_current_item(
            bytes: &[u8],
            error_info: &mut ErrorInfo,
        ) -> Result<MetadataCurrentItem>;
        fn ping(self: &Client) -> Result<()>;
        fn heartbeat_interval(self: &Client) -> u64;
        fn snapshot(
            self: &Client,
            snapshot_compression: String,
            snapshot_types: Vec<String>,
        ) -> Result<Vec<u8>>;
        fn send_binary_request(
            self: &Client,
            code: u32,
            payload: Vec<u8>,
            error_info: &mut ErrorInfo,
        ) -> Result<Vec<u8>>;
        fn send_binary_request_to_replica(
            self: &Client,
            code: u32,
            payload: Vec<u8>,
            replica_id: u8,
            error_info: &mut ErrorInfo,
        ) -> Result<Vec<u8>>;

        fn disconnect(self: &Client) -> Result<()>;
        fn shutdown(self: &Client) -> Result<()>;
        // fn subscribe_events(self: &Client) -> Result<()>;
        fn delete_segments(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            partition_id: u32,
            segments_count: u32,
            error_info: &mut ErrorInfo,
        ) -> Result<()>;
        fn get_user(self: &Client, user_id: Identifier) -> Result<UserInfoDetails>;
        fn get_users(self: &Client) -> Result<Vec<UserInfo>>;
        #[allow(clippy::too_many_arguments)]
        fn create_user(
            self: &Client,
            username: String,
            password: String,
            status: UserStatus,
            has_permissions: bool,
            permissions: Permissions,
            options: UserCreateOptions,
            error_info: &mut ErrorInfo,
        ) -> Result<UserInfoDetails>;
        fn delete_user(
            self: &Client,
            user_id: Identifier,
            error_info: &mut ErrorInfo,
        ) -> Result<()>;
        fn update_user(
            self: &Client,
            user_id: Identifier,
            options: UserUpdateOptions,
            error_info: &mut ErrorInfo,
        ) -> Result<()>;
        fn update_permissions(
            self: &Client,
            user_id: Identifier,
            has_permissions: bool,
            permissions: Permissions,
            error_info: &mut ErrorInfo,
        ) -> Result<()>;
        fn change_password(
            self: &Client,
            user_id: Identifier,
            current_password: String,
            new_password: String,
            error_info: &mut ErrorInfo,
        ) -> Result<()>;
        fn get_cluster_metadata(self: &Client) -> Result<ClusterMetadata>;
        // fn get_personal_access_tokens(self: &Client) -> Result<Vec<PersonalAccessTokenInfo>>;
        fn create_personal_access_token(
            self: &Client,
            name: String,
            expiry_kind: String,
            expiry_value: u64,
        ) -> Result<String>;
        // fn delete_personal_access_token(self: &Client, name: String) -> Result<()>;
        // fn login_with_personal_access_token(self: &Client, token: String) -> Result<IdentityInfo>;

        unsafe fn delete_client(client: *mut Client);

        // Identifier functions
        fn set_string(self: &mut Identifier, id: String) -> Result<()>;
        fn set_numeric(self: &mut Identifier, id: u32) -> Result<()>;

        /// Builds and initializes a consumer named `name`. With `group` it is a
        /// member of the consumer group `name`, which it creates if needed,
        /// joins here and leaves in `close`. Otherwise it reads the partitions
        /// of `options`.
        fn consumer(
            self: &Client,
            name: String,
            stream_id: Identifier,
            topic_id: Identifier,
            group: bool,
            options: ConsumerOptions,
            error_info: &mut ErrorInfo,
        ) -> Result<Box<Consumer>>;
        /// Waits up to `timeout_micros` for the next message. An empty vector
        /// means that the time ran out. Under `CommitAfter::Processing` it
        /// counts the message handed over before. The consumer stays usable
        /// after an error and retries by itself.
        fn next(
            self: &Consumer,
            timeout_micros: u64,
            error_info: &mut ReadErrorInfo,
        ) -> Result<Vec<ReceivedMessage>>;
        /// Stores the offset of a checkpoint that this consumer handed over.
        /// Refused with `CommitPolicy::OnRead`.
        fn commit(
            self: &Consumer,
            checkpoint: &Checkpoint,
            error_info: &mut ErrorInfo,
        ) -> Result<()>;
        /// Counts a checkpoint that this consumer handed over, under
        /// `CommitPolicy::Auto`.
        fn mark(self: &Consumer, checkpoint: &Checkpoint, error_info: &mut ErrorInfo)
        -> Result<()>;
        /// Sets the next read position of a partition. It stores no offset.
        fn seek(
            self: &Consumer,
            partition_id: u32,
            start: PollingStrategy,
            error_info: &mut ErrorInfo,
        ) -> Result<()>;
        #[cxx_name = "seek"]
        fn seek_to(
            self: &Consumer,
            partition_id: u32,
            continuation: &Continuation,
            error_info: &mut ErrorInfo,
        ) -> Result<()>;
        /// Stops the consumer and stores as its commit policy says, as a
        /// reader does. A second call does nothing.
        fn close(self: &Consumer, error_info: &mut ErrorInfo) -> Result<()>;

        // Producer methods
        // fn stream(self: &Producer) -> Result<Identifier>;
        // fn topic(self: &Producer) -> Result<Identifier>;
        // fn init(self: &Producer) -> Result<()>;
        // fn send(self: &Producer, messages: Vec<IggyMessageToSend>) -> Result<()>;
        // fn send_one(self: &Producer, message: IggyMessageToSend) -> Result<()>;
        // fn send_with_partitioning(self: &Producer, partitioning_kind: String, partitioning_value: Vec<u8>, messages: Vec<IggyMessageToSend>) -> Result<()>;
        // fn send_to(self: &Producer, stream_id: Identifier, topic_id: Identifier, partitioning_kind: String, partitioning_value: Vec<u8>, messages: Vec<IggyMessageToSend>) -> Result<()>;
        // fn shutdown(self: &mut Producer) -> Result<()>;
        // unsafe fn delete_producer(producer: *mut Producer) -> Result<()>;
    }
}
