pub mod bus;
pub mod deps;
pub mod replica;

use bus::MemBus;
use iggy_common::header::{PrepareHeader, PrepareOkHeader, RequestHeader};
use iggy_common::message::{Message, MessageBag};
use iggy_common::{
    ClientInfo, ClientInfoDetails, ClusterMetadata, CompressionAlgorithm, Consumer,
    ConsumerGroup, ConsumerGroupDetails, ConsumerOffsetInfo, Identifier, IdentityInfo,
    IggyDuration, IggyError, IggyExpiry, IggyMessage, MaxTopicSize, Partitioning, Permissions,
    PersonalAccessTokenExpiry, PersonalAccessTokenInfo, PolledMessages, PollingStrategy,
    RawPersonalAccessToken, Snapshot, SnapshotCompression, Stats, Stream, StreamDetails,
    SystemSnapshotType, Topic, TopicDetails, UserInfo, UserInfoDetails, UserStatus,
};
use message_bus::MessageBus;
use metadata::Metadata;
use replica::Replica;
use std::rc::Rc;

/// The main simulator struct that manages all replicas and exposes the SDK API
#[derive(Debug)]
pub struct Simulator {
    pub replicas: Vec<Replica>,
    pub message_bus: Rc<MemBus>,
}

impl Simulator {
    pub fn new(replica_count: usize) -> Self {
        // Create message bus and preseed all replica connections
        let mut message_bus = MemBus::new();

        // Register all replicas with the message bus
        for i in 0..replica_count as u8 {
            message_bus.add_replica(i);
        }

        let message_bus = Rc::new(message_bus);
        let replicas = (0..replica_count)
            .map(|i| Replica::new(i as u8, format!("replica-{}", i), Rc::clone(&message_bus)))
            .collect();

        Self {
            replicas,
            message_bus,
        }
    }

    pub fn with_message_bus(replica_count: usize, mut message_bus: MemBus) -> Self {
        // Preseed replica connections
        for i in 0..replica_count as u8 {
            message_bus.add_replica(i);
        }

        let message_bus = Rc::new(message_bus);
        let replicas = (0..replica_count)
            .map(|i| Replica::new(i as u8, format!("replica-{}", i), Rc::clone(&message_bus)))
            .collect();

        Self {
            replicas,
            message_bus,
        }
    }
}

// =============================================================================
// Internal message dispatch - Implementation details
// =============================================================================

impl Simulator {
    /// Dispatch a message to the appropriate subsystem
    /// Private - called by public client methods after building the message
    ///
    /// Routes based on message type and operation:
    /// - Request messages → check operation type
    ///   - Metadata operations (1-99) → metadata shard
    ///   - Partition operations (100+) → partition shard
    /// - Other message types → routed directly
    fn dispatch(&self, message: MessageBag) {
        match message {
            MessageBag::Request(request) => {
                // Route request based on operation type
                let operation_value = request.header().operation as u32;

                if operation_value < 100 {
                    // Metadata operation
                    self.dispatch_to_metadata(request);
                } else {
                    // Partition operation
                    self.dispatch_to_partition(request);
                }
            }
            MessageBag::Prepare(prepare) => {
                // TODO: Handle prepare messages (chain replication)
                todo!("handle prepare: op={}", prepare.header().op);
            }
            MessageBag::PrepareOk(prepare_ok) => {
                // TODO: Handle acknowledgments
                todo!("handle prepare_ok: op={}", prepare_ok.header().op);
            }
            MessageBag::Reply(_reply) => {
                // TODO: Handle replies back to client
                todo!("handle reply message");
            }
            MessageBag::Commit(_commit) => {
                // TODO: Handle commit messages
                todo!("handle commit message");
            }
            MessageBag::Generic(_generic) => {
                // TODO: Handle generic messages
                todo!("handle generic message");
            }
        }
    }

    /// Dispatch metadata operation to metadata shard
    fn dispatch_to_metadata(&self, request: Message<RequestHeader>) {
        // TODO: Determine which replica is primary (for now, use replica 0)
        let primary_id = 0;
        let primary = &self.replicas[primary_id];

        // Route to metadata's on_request handler
        primary.metadata.on_request(request);
    }

    /// Dispatch partition operation to partition shard
    fn dispatch_to_partition(&self, request: Message<RequestHeader>) {
        // TODO: Determine which partition/replica handles this request
        // For now, just route to replica 0's partitions
        let replica_id = 0;
        let _replica = &self.replicas[replica_id];

        // TODO: Route to replica.partitions.on_request(request)
        todo!(
            "dispatch partition operation to replica {}: operation={:?}",
            replica_id,
            request.header().operation
        );
    }

}

// =============================================================================
// Client methods
// =============================================================================

impl Simulator {
    pub fn connect(&self) -> Result<(), IggyError> {
        todo!()
    }

    pub fn disconnect(&self) -> Result<(), IggyError> {
        todo!()
    }

    pub fn shutdown(&self) -> Result<(), IggyError> {
        todo!()
    }
}

// =============================================================================
// System methods
// =============================================================================

impl Simulator {
    pub fn get_stats(&self) -> Result<Stats, IggyError> {
        todo!()
    }

    pub fn get_me(&self) -> Result<ClientInfoDetails, IggyError> {
        todo!()
    }

    pub fn get_client(&self, _client_id: u32) -> Result<Option<ClientInfoDetails>, IggyError> {
        todo!()
    }

    pub fn get_clients(&self) -> Result<Vec<ClientInfo>, IggyError> {
        todo!()
    }

    pub fn ping(&self) -> Result<(), IggyError> {
        todo!()
    }

    pub fn heartbeat_interval(&self) -> IggyDuration {
        todo!()
    }

    pub fn snapshot(
        &self,
        _compression: SnapshotCompression,
        _snapshot_types: Vec<SystemSnapshotType>,
    ) -> Result<Snapshot, IggyError> {
        todo!()
    }
}

// =============================================================================
// User methods
// =============================================================================

impl Simulator {
    pub fn get_user(&self, _user_id: &Identifier) -> Result<Option<UserInfoDetails>, IggyError> {
        todo!()
    }

    pub fn get_users(&self) -> Result<Vec<UserInfo>, IggyError> {
        todo!()
    }

    pub fn create_user(
        &self,
        _username: &str,
        _password: &str,
        _status: UserStatus,
        _permissions: Option<Permissions>,
    ) -> Result<UserInfoDetails, IggyError> {
        todo!()
    }

    pub fn delete_user(&self, _user_id: &Identifier) -> Result<(), IggyError> {
        todo!()
    }

    pub fn update_user(
        &self,
        _user_id: &Identifier,
        _username: Option<&str>,
        _status: Option<UserStatus>,
    ) -> Result<(), IggyError> {
        todo!()
    }

    pub fn update_permissions(
        &self,
        _user_id: &Identifier,
        _permissions: Option<Permissions>,
    ) -> Result<(), IggyError> {
        todo!()
    }

    pub fn change_password(
        &self,
        _user_id: &Identifier,
        _current_password: &str,
        _new_password: &str,
    ) -> Result<(), IggyError> {
        todo!()
    }

    pub fn login_user(&self, _username: &str, _password: &str) -> Result<IdentityInfo, IggyError> {
        todo!()
    }

    pub fn logout_user(&self) -> Result<(), IggyError> {
        todo!()
    }
}

// =============================================================================
// Personal access token methods
// =============================================================================

impl Simulator {
    pub fn get_personal_access_tokens(&self) -> Result<Vec<PersonalAccessTokenInfo>, IggyError> {
        todo!()
    }

    pub fn create_personal_access_token(
        &self,
        _name: &str,
        _expiry: PersonalAccessTokenExpiry,
    ) -> Result<RawPersonalAccessToken, IggyError> {
        todo!()
    }

    pub fn delete_personal_access_token(&self, _name: &str) -> Result<(), IggyError> {
        todo!()
    }

    pub fn login_with_personal_access_token(
        &self,
        _token: &str,
    ) -> Result<IdentityInfo, IggyError> {
        todo!()
    }
}

// =============================================================================
// Stream methods - Public API
// =============================================================================
//
// Pattern for all client methods:
// 1. Build Message<RequestHeader> from parameters
// 2. Convert to MessageBag::Request(message)
// 3. Call self.dispatch(bag) - private method
// 4. Wait for reply from primary
// 5. Parse and return result

impl Simulator {
    pub fn get_stream(&self, _stream_id: &Identifier) -> Result<Option<StreamDetails>, IggyError> {
        // TODO: Build message with Operation::GetStream (not yet in enum)
        // self.dispatch_request(message);
        // Wait for reply and parse
        todo!()
    }

    pub fn get_streams(&self) -> Result<Vec<Stream>, IggyError> {
        // TODO: Build message with Operation::ListStreams (not yet in enum)
        // self.dispatch_request(message);
        // Wait for reply and parse
        todo!()
    }

    pub fn create_stream(&self, name: &str) -> Result<StreamDetails, IggyError> {
        // TODO: Build Message<RequestHeader> with:
        //    - header.operation = Operation::CreateStream
        //    - body containing serialized stream name
        // Convert to MessageBag (using From impl) and dispatch:
        //   let message: Message<RequestHeader> = build_request(name);
        //   self.dispatch(message.into());  // ← .into() converts to MessageBag
        // Wait for reply and parse StreamDetails
        todo!("build message for: {}", name)
    }

    pub fn update_stream(&self, _stream_id: &Identifier, _name: &str) -> Result<(), IggyError> {
        todo!()
    }

    pub fn delete_stream(&self, _stream_id: &Identifier) -> Result<(), IggyError> {
        todo!()
    }

    pub fn purge_stream(&self, _stream_id: &Identifier) -> Result<(), IggyError> {
        todo!()
    }
}

// =============================================================================
// Topic methods
// =============================================================================

impl Simulator {
    pub fn get_topic(
        &self,
        _stream_id: &Identifier,
        _topic_id: &Identifier,
    ) -> Result<Option<TopicDetails>, IggyError> {
        todo!()
    }

    pub fn get_topics(&self, _stream_id: &Identifier) -> Result<Vec<Topic>, IggyError> {
        todo!()
    }

    pub fn create_topic(
        &self,
        _stream_id: &Identifier,
        _name: &str,
        _partitions_count: u32,
        _compression_algorithm: CompressionAlgorithm,
        _replication_factor: Option<u8>,
        _message_expiry: IggyExpiry,
        _max_topic_size: MaxTopicSize,
    ) -> Result<TopicDetails, IggyError> {
        todo!()
    }

    pub fn update_topic(
        &self,
        _stream_id: &Identifier,
        _topic_id: &Identifier,
        _name: &str,
        _compression_algorithm: CompressionAlgorithm,
        _replication_factor: Option<u8>,
        _message_expiry: IggyExpiry,
        _max_topic_size: MaxTopicSize,
    ) -> Result<(), IggyError> {
        todo!()
    }

    pub fn delete_topic(
        &self,
        _stream_id: &Identifier,
        _topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        todo!()
    }

    pub fn purge_topic(
        &self,
        _stream_id: &Identifier,
        _topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        todo!()
    }
}

// =============================================================================
// Partition methods
// =============================================================================

impl Simulator {
    pub fn create_partitions(
        &self,
        _stream_id: &Identifier,
        _topic_id: &Identifier,
        _partitions_count: u32,
    ) -> Result<(), IggyError> {
        todo!()
    }

    pub fn delete_partitions(
        &self,
        _stream_id: &Identifier,
        _topic_id: &Identifier,
        _partitions_count: u32,
    ) -> Result<(), IggyError> {
        todo!()
    }
}

// =============================================================================
// Segment methods
// =============================================================================

impl Simulator {
    pub fn delete_segments(
        &self,
        _stream_id: &Identifier,
        _topic_id: &Identifier,
        _partition_id: u32,
        _segments_count: u32,
    ) -> Result<(), IggyError> {
        todo!()
    }
}

// =============================================================================
// Message methods
// =============================================================================

impl Simulator {
    pub fn poll_messages(
        &self,
        _stream_id: &Identifier,
        _topic_id: &Identifier,
        _partition_id: Option<u32>,
        _consumer: &Consumer,
        _strategy: &PollingStrategy,
        _count: u32,
        _auto_commit: bool,
    ) -> Result<PolledMessages, IggyError> {
        todo!()
    }

    pub fn send_messages(
        &self,
        _stream_id: &Identifier,
        _topic_id: &Identifier,
        _partitioning: &Partitioning,
        _messages: &mut [IggyMessage],
    ) -> Result<(), IggyError> {
        todo!()
    }

    pub fn flush_unsaved_buffer(
        &self,
        _stream_id: &Identifier,
        _topic_id: &Identifier,
        _partition_id: u32,
        _fsync: bool,
    ) -> Result<(), IggyError> {
        todo!()
    }
}

// =============================================================================
// Consumer offset methods
// =============================================================================

impl Simulator {
    pub fn store_consumer_offset(
        &self,
        _consumer: &Consumer,
        _stream_id: &Identifier,
        _topic_id: &Identifier,
        _partition_id: Option<u32>,
        _offset: u64,
    ) -> Result<(), IggyError> {
        todo!()
    }

    pub fn get_consumer_offset(
        &self,
        _consumer: &Consumer,
        _stream_id: &Identifier,
        _topic_id: &Identifier,
        _partition_id: Option<u32>,
    ) -> Result<Option<ConsumerOffsetInfo>, IggyError> {
        todo!()
    }

    pub fn delete_consumer_offset(
        &self,
        _consumer: &Consumer,
        _stream_id: &Identifier,
        _topic_id: &Identifier,
        _partition_id: Option<u32>,
    ) -> Result<(), IggyError> {
        todo!()
    }
}

// =============================================================================
// Consumer group methods
// =============================================================================

impl Simulator {
    pub fn get_consumer_group(
        &self,
        _stream_id: &Identifier,
        _topic_id: &Identifier,
        _group_id: &Identifier,
    ) -> Result<Option<ConsumerGroupDetails>, IggyError> {
        todo!()
    }

    pub fn get_consumer_groups(
        &self,
        _stream_id: &Identifier,
        _topic_id: &Identifier,
    ) -> Result<Vec<ConsumerGroup>, IggyError> {
        todo!()
    }

    pub fn create_consumer_group(
        &self,
        _stream_id: &Identifier,
        _topic_id: &Identifier,
        _name: &str,
    ) -> Result<ConsumerGroupDetails, IggyError> {
        todo!()
    }

    pub fn delete_consumer_group(
        &self,
        _stream_id: &Identifier,
        _topic_id: &Identifier,
        _group_id: &Identifier,
    ) -> Result<(), IggyError> {
        todo!()
    }

    pub fn join_consumer_group(
        &self,
        _stream_id: &Identifier,
        _topic_id: &Identifier,
        _group_id: &Identifier,
    ) -> Result<(), IggyError> {
        todo!()
    }

    pub fn leave_consumer_group(
        &self,
        _stream_id: &Identifier,
        _topic_id: &Identifier,
        _group_id: &Identifier,
    ) -> Result<(), IggyError> {
        todo!()
    }
}

// =============================================================================
// Cluster methods
// =============================================================================

impl Simulator {
    pub fn get_cluster_metadata(&self) -> Result<ClusterMetadata, IggyError> {
        todo!()
    }
}
