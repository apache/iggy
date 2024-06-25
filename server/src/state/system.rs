use crate::state::models::CreatePersonalAccessTokenWithHash;
use crate::state::StateEntry;
use crate::streaming::personal_access_tokens::personal_access_token::PersonalAccessToken;
use iggy::bytes_serializable::BytesSerializable;
use iggy::command::*;
use iggy::compression::compression_algorithm::CompressionAlgorithm;
use iggy::consumer_groups::create_consumer_group::CreateConsumerGroup;
use iggy::consumer_groups::delete_consumer_group::DeleteConsumerGroup;
use iggy::error::IggyError;
use iggy::identifier::{IdKind, Identifier};
use iggy::models::permissions::Permissions;
use iggy::models::user_status::UserStatus;
use iggy::partitions::create_partitions::CreatePartitions;
use iggy::partitions::delete_partitions::DeletePartitions;
use iggy::personal_access_tokens::delete_personal_access_token::DeletePersonalAccessToken;
use iggy::streams::create_stream::CreateStream;
use iggy::streams::delete_stream::DeleteStream;
use iggy::streams::update_stream::UpdateStream;
use iggy::topics::create_topic::CreateTopic;
use iggy::topics::delete_topic::DeleteTopic;
use iggy::topics::update_topic::UpdateTopic;
use iggy::users::change_password::ChangePassword;
use iggy::users::create_user::CreateUser;
use iggy::users::delete_user::DeleteUser;
use iggy::users::update_permissions::UpdatePermissions;
use iggy::users::update_user::UpdateUser;
use iggy::utils::expiry::IggyExpiry;
use iggy::utils::timestamp::IggyTimestamp;
use iggy::utils::topic_size::MaxTopicSize;
use std::collections::HashMap;
use std::fmt::Display;
use tracing::{debug, error};

#[derive(Debug)]
pub struct SystemState {
    pub streams: HashMap<u32, StreamState>,
    pub users: HashMap<u32, UserState>,
}

#[derive(Debug)]
pub struct StreamState {
    pub id: u32,
    pub name: String,
    pub created_at: IggyTimestamp,
    pub topics: HashMap<u32, TopicState>,
    pub current_topic_id: u32,
}

#[derive(Debug)]
pub struct TopicState {
    pub id: u32,
    pub name: String,
    pub partitions: HashMap<u32, PartitionState>,
    pub consumer_groups: HashMap<u32, ConsumerGroupState>,
    pub compression_algorithm: CompressionAlgorithm,
    pub message_expiry: IggyExpiry,
    pub max_topic_size: MaxTopicSize,
    pub replication_factor: Option<u8>,
    pub created_at: IggyTimestamp,
    pub current_consumer_group_id: u32,
}

#[derive(Debug)]
pub struct PartitionState {
    pub id: u32,
    pub created_at: IggyTimestamp,
}

#[derive(Debug)]
pub struct PersonalAccessTokenState {
    pub name: String,
    pub token_hash: String,
    pub expiry_at: Option<IggyTimestamp>,
}

#[derive(Debug)]
pub struct UserState {
    pub id: u32,
    pub username: String,
    pub password_hash: String,
    pub status: UserStatus,
    pub permissions: Option<Permissions>,
    pub personal_access_tokens: HashMap<String, PersonalAccessTokenState>,
}

#[derive(Debug)]
pub struct ConsumerGroupState {
    pub id: u32,
    pub name: String,
}

// TODO: Consider handling stream and topic purge
impl SystemState {
    pub async fn init(entries: Vec<StateEntry>) -> Result<Self, IggyError> {
        let mut streams = HashMap::new();
        let mut users = HashMap::new();
        let mut current_stream_id = 0;
        let mut current_user_id = 0;
        for entry in entries {
            debug!(
                "Processing state entry with code: {}, name: {}",
                entry.code,
                get_name_from_code(entry.code).unwrap_or("invalid_command")
            );
            match entry.code {
                CREATE_STREAM_CODE => {
                    let command = CreateStream::from_bytes(entry.payload)?;
                    let stream_id = command.stream_id.unwrap_or_else(|| {
                        current_stream_id += 1;
                        current_stream_id
                    });
                    let stream = StreamState {
                        id: stream_id,
                        name: command.name.clone(),
                        topics: HashMap::new(),
                        current_topic_id: 0,
                        created_at: entry.timestamp,
                    };
                    streams.insert(stream.id, stream);
                }
                UPDATE_STREAM_CODE => {
                    let command = UpdateStream::from_bytes(entry.payload)?;
                    let stream_id = find_stream_id(&streams, &command.stream_id);
                    let stream = streams
                        .get_mut(&stream_id)
                        .unwrap_or_else(|| panic!("{}", format!("Stream: {stream_id} not found")));
                    stream.name = command.name;
                }
                DELETE_STREAM_CODE => {
                    let command = DeleteStream::from_bytes(entry.payload)?;
                    let stream_id = find_stream_id(&streams, &command.stream_id);
                    streams.remove(&stream_id);
                }
                CREATE_TOPIC_CODE => {
                    let command = CreateTopic::from_bytes(entry.payload)?;
                    let stream_id = find_stream_id(&streams, &command.stream_id);
                    let stream = streams
                        .get_mut(&stream_id)
                        .unwrap_or_else(|| panic!("{}", format!("Stream: {stream_id} not found")));
                    let topic_id = command.topic_id.unwrap_or_else(|| {
                        stream.current_topic_id += 1;
                        stream.current_topic_id
                    });
                    let topic = TopicState {
                        id: topic_id,
                        name: command.name,
                        consumer_groups: HashMap::new(),
                        current_consumer_group_id: 0,
                        compression_algorithm: command.compression_algorithm,
                        message_expiry: command.message_expiry,
                        max_topic_size: command.max_topic_size,
                        replication_factor: command.replication_factor,
                        created_at: entry.timestamp,
                        partitions: if command.partitions_count > 0 {
                            let mut partitions = HashMap::new();
                            for i in 1..=command.partitions_count {
                                partitions.insert(
                                    i,
                                    PartitionState {
                                        id: i,
                                        created_at: entry.timestamp,
                                    },
                                );
                            }
                            partitions
                        } else {
                            HashMap::new()
                        },
                    };
                    stream.topics.insert(topic.id, topic);
                }
                UPDATE_TOPIC_CODE => {
                    let command = UpdateTopic::from_bytes(entry.payload)?;
                    let stream_id = find_stream_id(&streams, &command.stream_id);
                    let stream = streams
                        .get_mut(&stream_id)
                        .unwrap_or_else(|| panic!("{}", format!("Stream: {stream_id} not found")));
                    let topic_id = find_topic_id(&stream.topics, &command.topic_id);
                    let topic = stream
                        .topics
                        .get_mut(&topic_id)
                        .unwrap_or_else(|| panic!("{}", format!("Topic: {topic_id} not found")));
                    topic.name = command.name;
                    topic.compression_algorithm = command.compression_algorithm;
                    topic.message_expiry = command.message_expiry;
                    topic.max_topic_size = command.max_topic_size;
                    topic.replication_factor = command.replication_factor;
                }
                DELETE_TOPIC_CODE => {
                    let command = DeleteTopic::from_bytes(entry.payload)?;
                    let stream_id = find_stream_id(&streams, &command.stream_id);
                    let stream = streams
                        .get_mut(&stream_id)
                        .unwrap_or_else(|| panic!("{}", format!("Stream: {stream_id} not found")));
                    let topic_id = find_topic_id(&stream.topics, &command.topic_id);
                    stream.topics.remove(&topic_id);
                }
                CREATE_PARTITIONS_CODE => {
                    let command = CreatePartitions::from_bytes(entry.payload)?;
                    let stream_id = find_stream_id(&streams, &command.stream_id);
                    let stream = streams
                        .get_mut(&stream_id)
                        .unwrap_or_else(|| panic!("{}", format!("Stream: {stream_id} not found")));
                    let topic_id = find_topic_id(&stream.topics, &command.topic_id);
                    let topic = stream
                        .topics
                        .get_mut(&topic_id)
                        .unwrap_or_else(|| panic!("{}", format!("Topic: {topic_id} not found")));
                    let last_partition_id = if topic.partitions.is_empty() {
                        0
                    } else {
                        topic
                            .partitions
                            .values()
                            .map(|p| p.id)
                            .max()
                            .unwrap_or_else(|| panic!("No partition found"))
                    };
                    for i in 1..=command.partitions_count {
                        topic.partitions.insert(
                            last_partition_id + i,
                            PartitionState {
                                id: last_partition_id + i,
                                created_at: entry.timestamp,
                            },
                        );
                    }
                }
                DELETE_PARTITIONS_CODE => {
                    let command = DeletePartitions::from_bytes(entry.payload)?;
                    let stream_id = find_stream_id(&streams, &command.stream_id);
                    let stream = streams
                        .get_mut(&stream_id)
                        .unwrap_or_else(|| panic!("{}", format!("Stream: {stream_id} not found")));
                    let topic_id = find_topic_id(&stream.topics, &command.topic_id);
                    let topic = stream
                        .topics
                        .get_mut(&topic_id)
                        .unwrap_or_else(|| panic!("{}", format!("Topic: {topic_id} not found")));
                    if topic.partitions.is_empty() {
                        continue;
                    }

                    let last_partition_id = topic
                        .partitions
                        .values()
                        .map(|p| p.id)
                        .max()
                        .unwrap_or_else(|| panic!("No partition found"));
                    for i in 0..command.partitions_count {
                        topic.partitions.remove(&(last_partition_id - i));
                    }
                }
                CREATE_CONSUMER_GROUP_CODE => {
                    let command = CreateConsumerGroup::from_bytes(entry.payload)?;
                    let stream_id = find_stream_id(&streams, &command.stream_id);
                    let stream = streams
                        .get_mut(&stream_id)
                        .unwrap_or_else(|| panic!("{}", format!("Stream: {stream_id} not found")));
                    let topic_id = find_topic_id(&stream.topics, &command.topic_id);
                    let topic = stream
                        .topics
                        .get_mut(&topic_id)
                        .unwrap_or_else(|| panic!("{}", format!("Topic: {topic_id} not found")));
                    let consumer_group_id = command.group_id.unwrap_or_else(|| {
                        topic.current_consumer_group_id += 1;
                        topic.current_consumer_group_id
                    });
                    let consumer_group = ConsumerGroupState {
                        id: consumer_group_id,
                        name: command.name,
                    };
                    topic
                        .consumer_groups
                        .insert(consumer_group.id, consumer_group);
                }
                DELETE_CONSUMER_GROUP_CODE => {
                    let command = DeleteConsumerGroup::from_bytes(entry.payload)?;
                    let stream_id = find_stream_id(&streams, &command.stream_id);
                    let stream = streams
                        .get_mut(&stream_id)
                        .unwrap_or_else(|| panic!("{}", format!("Stream: {stream_id} not found")));
                    let topic_id = find_topic_id(&stream.topics, &command.topic_id);
                    let topic = stream
                        .topics
                        .get_mut(&topic_id)
                        .unwrap_or_else(|| panic!("{}", format!("Topic: {topic_id} not found")));
                    let consumer_group_id =
                        find_consumer_group_id(&topic.consumer_groups, &command.group_id);
                    topic.consumer_groups.remove(&consumer_group_id);
                }
                CREATE_USER_CODE => {
                    let command = CreateUser::from_bytes(entry.payload)?;
                    current_user_id += 1;
                    let user = UserState {
                        id: current_user_id,
                        username: command.username,
                        password_hash: command.password, // This is already hashed
                        status: command.status,
                        permissions: command.permissions,
                        personal_access_tokens: HashMap::new(),
                    };
                    users.insert(user.id, user);
                }
                UPDATE_USER_CODE => {
                    let command = UpdateUser::from_bytes(entry.payload)?;
                    let user_id = find_user_id(&users, &command.user_id);
                    let user = users
                        .get_mut(&user_id)
                        .unwrap_or_else(|| panic!("{}", format!("User: {user_id} not found")));
                    if let Some(username) = &command.username {
                        user.username.clone_from(username);
                    }
                    if let Some(status) = &command.status {
                        user.status = *status;
                    }
                }
                DELETE_USER_CODE => {
                    let command = DeleteUser::from_bytes(entry.payload)?;
                    let user_id = find_user_id(&users, &command.user_id);
                    users.remove(&user_id);
                }
                CHANGE_PASSWORD_CODE => {
                    let command = ChangePassword::from_bytes(entry.payload)?;
                    let user_id = find_user_id(&users, &command.user_id);
                    let user = users
                        .get_mut(&user_id)
                        .unwrap_or_else(|| panic!("{}", format!("User: {user_id} not found")));
                    user.password_hash = command.new_password // This is already hashed
                }
                UPDATE_PERMISSIONS_CODE => {
                    let command = UpdatePermissions::from_bytes(entry.payload)?;
                    let user_id = find_user_id(&users, &command.user_id);
                    let user = users
                        .get_mut(&user_id)
                        .unwrap_or_else(|| panic!("{}", format!("User: {user_id} not found")));
                    user.permissions = command.permissions;
                }
                CREATE_PERSONAL_ACCESS_TOKEN_CODE => {
                    let command = CreatePersonalAccessTokenWithHash::from_bytes(entry.payload)?;
                    let token_hash = command.hash;
                    let user_id = find_user_id(&users, &entry.user_id.try_into()?);
                    let user = users
                        .get_mut(&user_id)
                        .unwrap_or_else(|| panic!("{}", format!("User: {user_id} not found")));
                    let expiry_at = PersonalAccessToken::calculate_expiry_at(
                        entry.timestamp,
                        command.command.expiry,
                    );
                    if let Some(expiry_at) = expiry_at {
                        if expiry_at.to_micros() <= IggyTimestamp::now().to_micros() {
                            debug!("Personal access token: {token_hash} has already expired.");
                            continue;
                        }
                    }

                    user.personal_access_tokens.insert(
                        command.command.name.clone(),
                        PersonalAccessTokenState {
                            name: command.command.name,
                            token_hash,
                            expiry_at,
                        },
                    );
                }
                DELETE_PERSONAL_ACCESS_TOKEN_CODE => {
                    let command = DeletePersonalAccessToken::from_bytes(entry.payload)?;
                    let user_id = find_user_id(&users, &entry.user_id.try_into()?);
                    let user = users
                        .get_mut(&user_id)
                        .unwrap_or_else(|| panic!("{}", format!("User: {user_id} not found")));
                    user.personal_access_tokens.remove(&command.name);
                }
                code => {
                    error!("Unsupported state entry code: {code}");
                }
            }
        }

        let state = SystemState { streams, users };
        debug!("+++ State +++");
        debug!("{state}");
        debug!("+++ State +++");
        Ok(state)
    }
}

fn find_stream_id(streams: &HashMap<u32, StreamState>, stream_id: &Identifier) -> u32 {
    match stream_id.kind {
        IdKind::Numeric => stream_id
            .get_u32_value()
            .unwrap_or_else(|_| panic!("{}", format!("Invalid stream ID: {stream_id}"))),
        IdKind::String => {
            let name = stream_id
                .get_cow_str_value()
                .unwrap_or_else(|_| panic!("{}", format!("Invalid stream name: {stream_id}")));
            let stream = streams
                .values()
                .find(|s| s.name == name)
                .unwrap_or_else(|| panic!("{}", format!("Stream: {name} not found")));
            stream.id
        }
    }
}

fn find_topic_id(topics: &HashMap<u32, TopicState>, topic_id: &Identifier) -> u32 {
    match topic_id.kind {
        IdKind::Numeric => topic_id
            .get_u32_value()
            .unwrap_or_else(|_| panic!("{}", format!("Invalid topic ID: {topic_id}"))),
        IdKind::String => {
            let name = topic_id
                .get_cow_str_value()
                .unwrap_or_else(|_| panic!("{}", format!("Invalid topic name: {topic_id}")));
            let topic = topics
                .values()
                .find(|s| s.name == name)
                .unwrap_or_else(|| panic!("{}", format!("Topic: {name} not found")));
            topic.id
        }
    }
}

fn find_consumer_group_id(groups: &HashMap<u32, ConsumerGroupState>, group_id: &Identifier) -> u32 {
    match group_id.kind {
        IdKind::Numeric => group_id
            .get_u32_value()
            .unwrap_or_else(|_| panic!("{}", format!("Invalid group ID: {group_id}"))),
        IdKind::String => {
            let name = group_id
                .get_cow_str_value()
                .unwrap_or_else(|_| panic!("{}", format!("Invalid group name: {group_id}")));
            let group = groups
                .values()
                .find(|s| s.name == name)
                .unwrap_or_else(|| panic!("{}", format!("Consumer group: {name} not found")));
            group.id
        }
    }
}

fn find_user_id(users: &HashMap<u32, UserState>, user_id: &Identifier) -> u32 {
    match user_id.kind {
        IdKind::Numeric => user_id
            .get_u32_value()
            .unwrap_or_else(|_| panic!("{}", format!("Invalid user ID: {user_id}"))),
        IdKind::String => {
            let username = user_id
                .get_cow_str_value()
                .unwrap_or_else(|_| panic!("{}", format!("Invalid username: {user_id}")));
            let user = users
                .values()
                .find(|s| s.username == username)
                .unwrap_or_else(|| panic!("{}", format!("User: {username} not found")));
            user.id
        }
    }
}

impl Display for SystemState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Streams:")?;
        for stream in self.streams.iter() {
            write!(f, "\n================\n")?;
            write!(f, "{}", stream.1)?;
        }
        write!(f, "Users:")?;
        for user in self.users.iter() {
            write!(f, "\n================\n")?;
            write!(f, "{}", user.1)?;
        }
        Ok(())
    }
}

impl Display for ConsumerGroupState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ConsumerGroup -> ID: {}, Name: {}", self.id, self.name)
    }
}

impl Display for UserState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let permissions = if let Some(permissions) = &self.permissions {
            permissions.to_string()
        } else {
            "no_permissions".to_string()
        };
        write!(
            f,
            "User -> ID: {}, Username: {}, Status: {}, Permissions: {}",
            self.id, self.username, self.status, permissions
        )
    }
}

impl Display for StreamState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Stream -> ID: {}, Name: {}", self.id, self.name,)?;
        for topic in self.topics.iter() {
            write!(f, "\n {}", topic.1)?;
        }
        Ok(())
    }
}

impl Display for TopicState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Topic -> ID: {}, Name: {}", self.id, self.name,)?;
        for partition in self.partitions.iter() {
            write!(f, "\n  {}", partition.1)?;
        }
        write!(f, "\nConsumer Groups:")?;
        for consumer_group in self.consumer_groups.iter() {
            write!(f, "\n  {}", consumer_group.1)?;
        }
        Ok(())
    }
}

impl Display for PartitionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Partition -> ID: {}, Created At: {}",
            self.id, self.created_at
        )
    }
}
