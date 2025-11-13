/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use bytes::Bytes;
use iggy_common::{
    BytesSerializable, CacheMetrics, CacheMetricsKey, ClientInfo, ClientInfoDetails,
    ClusterMetadata, CompressionAlgorithm, ConsumerGroup, ConsumerGroupDetails, ConsumerGroupInfo,
    ConsumerGroupMember, ConsumerOffsetInfo, IdentityInfo, IggyByteSize, IggyError, IggyExpiry,
    MaxTopicSize, Partition, Permissions, PersonalAccessTokenInfo, RawPersonalAccessToken, Stats,
    Stream, StreamDetails, Topic, TopicDetails, UserInfo, UserInfoDetails, UserStatus,
};
use std::collections::HashMap;
use std::str::from_utf8;

const EMPTY_TOPICS: Vec<Topic> = vec![];
const EMPTY_STREAMS: Vec<Stream> = vec![];
const EMPTY_CLIENTS: Vec<ClientInfo> = vec![];
const EMPTY_USERS: Vec<UserInfo> = vec![];
const EMPTY_PERSONAL_ACCESS_TOKENS: Vec<PersonalAccessTokenInfo> = vec![];
const EMPTY_CONSUMER_GROUPS: Vec<ConsumerGroup> = vec![];

pub fn map_stats(payload: Bytes) -> Result<Stats, IggyError> {
    let process_id = u32::from_le_bytes(
        payload[..4]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    let cpu_usage = f32::from_le_bytes(
        payload[4..8]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    let total_cpu_usage = f32::from_le_bytes(
        payload[8..12]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    let memory_usage = u64::from_le_bytes(
        payload[12..20]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    )
    .into();
    let total_memory = u64::from_le_bytes(
        payload[20..28]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    )
    .into();
    let available_memory = u64::from_le_bytes(
        payload[28..36]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    )
    .into();
    let run_time = u64::from_le_bytes(
        payload[36..44]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    )
    .into();
    let start_time = u64::from_le_bytes(
        payload[44..52]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    )
    .into();
    let read_bytes = u64::from_le_bytes(
        payload[52..60]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    )
    .into();
    let written_bytes = u64::from_le_bytes(
        payload[60..68]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    )
    .into();
    let messages_size_bytes = u64::from_le_bytes(
        payload[68..76]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    )
    .into();
    let streams_count = u32::from_le_bytes(
        payload[76..80]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    let topics_count = u32::from_le_bytes(
        payload[80..84]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    let partitions_count = u32::from_le_bytes(
        payload[84..88]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    let segments_count = u32::from_le_bytes(
        payload[88..92]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    let messages_count = u64::from_le_bytes(
        payload[92..100]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    let clients_count = u32::from_le_bytes(
        payload[100..104]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    let consumer_groups_count = u32::from_le_bytes(
        payload[104..108]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );

    let mut current_position = 108;

    //
    // Safely decode hostname
    //
    if current_position + 4 > payload.len() {
        return Err(IggyError::InvalidNumberEncoding);
    }
    let hostname_length = u32::from_le_bytes(
        payload[current_position..current_position + 4]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    ) as usize;
    current_position += 4;
    if current_position + hostname_length > payload.len() {
        return Err(IggyError::InvalidNumberEncoding);
    }
    let hostname = from_utf8(&payload[current_position..current_position + hostname_length])
        .map_err(|_| IggyError::InvalidUtf8)?
        .to_string();
    current_position += hostname_length;

    //
    // Safely Decode OS name
    //
    if current_position + 4 > payload.len() {
        return Err(IggyError::InvalidNumberEncoding);
    }
    let os_name_length = u32::from_le_bytes(
        payload[current_position..current_position + 4]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    ) as usize;
    current_position += 4;
    if current_position + os_name_length > payload.len() {
        return Err(IggyError::InvalidNumberEncoding);
    }
    let os_name = from_utf8(&payload[current_position..current_position + os_name_length])
        .map_err(|_| IggyError::InvalidUtf8)?
        .to_string();
    current_position += os_name_length;

    //
    // Safely decode OS version
    //
    if current_position + 4 > payload.len() {
        return Err(IggyError::InvalidNumberEncoding);
    }
    let os_version_length = u32::from_le_bytes(
        payload[current_position..current_position + 4]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    ) as usize;
    current_position += 4;
    if current_position + os_version_length > payload.len() {
        return Err(IggyError::InvalidNumberEncoding);
    }
    let os_version = from_utf8(&payload[current_position..current_position + os_version_length])
        .map_err(|_| IggyError::InvalidUtf8)?
        .to_string();
    current_position += os_version_length;

    //
    // Safely decode kernel version (NEW) + server version (NEW) + server semver (NEW)
    // We'll check if there's enough bytes before reading each new field.
    //

    // Default them in case payload doesn't have them (older server)
    let mut kernel_version = String::new();
    let mut iggy_server_version = String::new();
    let mut iggy_server_semver: Option<u32> = None;

    // kernel_version (if it exists)
    if current_position + 4 <= payload.len() {
        let kernel_version_length = u32::from_le_bytes(
            payload[current_position..current_position + 4]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        ) as usize;
        current_position += 4;
        if current_position + kernel_version_length <= payload.len() {
            let kv =
                from_utf8(&payload[current_position..current_position + kernel_version_length])
                    .map_err(|_| IggyError::InvalidUtf8)?
                    .to_string();
            kernel_version = kv;
            current_position += kernel_version_length;
        } else {
            // Not enough bytes for kernel version string, treat as empty or error out
            // return Err(IggyError::InvalidNumberEncoding);
            kernel_version = String::new(); // fallback
        }
    } else {
        // This means older server didn't send kernel_version, so remain empty
    }

    // iggy_server_version (if it exists)
    if current_position + 4 <= payload.len() {
        let iggy_version_length = u32::from_le_bytes(
            payload[current_position..current_position + 4]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        ) as usize;
        current_position += 4;
        if current_position + iggy_version_length <= payload.len() {
            let iv = from_utf8(&payload[current_position..current_position + iggy_version_length])
                .map_err(|_| IggyError::InvalidUtf8)?
                .to_string();
            iggy_server_version = iv;
            current_position += iggy_version_length;
        } else {
            // Not enough bytes for iggy version string, treat as empty or error out
            // return Err(IggyError::InvalidNumberEncoding);
            iggy_server_version = String::new(); // fallback
        }
    } else {
        // older server didn't send iggy_server_version, so remain empty
    }

    // iggy_server_semver (if it exists)
    if current_position + 4 <= payload.len() {
        let semver = u32::from_le_bytes(
            payload[current_position..current_position + 4]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        current_position += 4; // Increment position after reading semver
        if semver != 0 {
            iggy_server_semver = Some(semver);
        }
    } else {
        // older server didn't send semver
    }

    // Read cache metrics (if they exist)
    let mut cache_metrics = HashMap::new();
    if current_position + 4 <= payload.len() {
        let metrics_count = u32::from_le_bytes(
            payload[current_position..current_position + 4]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        ) as usize;
        current_position += 4;

        for _ in 0..metrics_count {
            // Read CacheMetricsKey
            let stream_id = u32::from_le_bytes(
                payload[current_position..current_position + 4]
                    .try_into()
                    .map_err(|_| IggyError::InvalidNumberEncoding)?,
            );
            current_position += 4;

            let topic_id = u32::from_le_bytes(
                payload[current_position..current_position + 4]
                    .try_into()
                    .map_err(|_| IggyError::InvalidNumberEncoding)?,
            );
            current_position += 4;

            let partition_id = u32::from_le_bytes(
                payload[current_position..current_position + 4]
                    .try_into()
                    .map_err(|_| IggyError::InvalidNumberEncoding)?,
            );
            current_position += 4;

            // Read CacheMetrics
            let hits = u64::from_le_bytes(
                payload[current_position..current_position + 8]
                    .try_into()
                    .map_err(|_| IggyError::InvalidNumberEncoding)?,
            );
            current_position += 8;

            let misses = u64::from_le_bytes(
                payload[current_position..current_position + 8]
                    .try_into()
                    .map_err(|_| IggyError::InvalidNumberEncoding)?,
            );
            current_position += 8;

            let hit_ratio = f32::from_le_bytes(
                payload[current_position..current_position + 4]
                    .try_into()
                    .map_err(|_| IggyError::InvalidNumberEncoding)?,
            );
            current_position += 4;

            let key = CacheMetricsKey {
                stream_id,
                topic_id,
                partition_id,
            };

            let metrics = CacheMetrics {
                hits,
                misses,
                hit_ratio,
            };

            cache_metrics.insert(key, metrics);
        }
    }

    Ok(Stats {
        process_id,
        cpu_usage,
        total_cpu_usage,
        memory_usage,
        total_memory,
        available_memory,
        run_time,
        start_time,
        read_bytes,
        written_bytes,
        messages_size_bytes,
        streams_count,
        topics_count,
        partitions_count,
        segments_count,
        messages_count,
        clients_count,
        consumer_groups_count,
        hostname,
        os_name,
        os_version,
        kernel_version,
        iggy_server_version,
        iggy_server_semver,
        cache_metrics,
    })
}

pub fn map_consumer_offset(payload: Bytes) -> Result<ConsumerOffsetInfo, IggyError> {
    let partition_id = u32::from_le_bytes(
        payload[..4]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    let current_offset = u64::from_le_bytes(
        payload[4..12]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    let stored_offset = u64::from_le_bytes(
        payload[12..20]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    Ok(ConsumerOffsetInfo {
        partition_id,
        current_offset,
        stored_offset,
    })
}

pub fn map_user(payload: Bytes) -> Result<UserInfoDetails, IggyError> {
    let (user, position) = map_to_user_info(payload.clone(), 0)?;
    let has_permissions = payload[position];
    let permissions = if has_permissions == 1 {
        let permissions_length = u32::from_le_bytes(
            payload[position + 1..position + 5]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        ) as usize;
        let permissions = payload.slice(position + 5..position + 5 + permissions_length);
        Some(Permissions::from_bytes(permissions)?)
    } else {
        None
    };

    let user = UserInfoDetails {
        id: user.id,
        created_at: user.created_at,
        status: user.status,
        username: user.username,
        permissions,
    };
    Ok(user)
}

pub fn map_users(payload: Bytes) -> Result<Vec<UserInfo>, IggyError> {
    if payload.is_empty() {
        return Ok(EMPTY_USERS);
    }

    let mut users = Vec::new();
    let length = payload.len();
    let mut position = 0;
    while position < length {
        let (user, read_bytes) = map_to_user_info(payload.clone(), position)?;
        users.push(user);
        position += read_bytes;
    }
    users.sort_by(|x, y| x.id.cmp(&y.id));
    Ok(users)
}

pub fn map_personal_access_tokens(
    payload: Bytes,
) -> Result<Vec<PersonalAccessTokenInfo>, IggyError> {
    if payload.is_empty() {
        return Ok(EMPTY_PERSONAL_ACCESS_TOKENS);
    }

    let mut personal_access_tokens = Vec::new();
    let length = payload.len();
    let mut position = 0;
    while position < length {
        let (personal_access_token, read_bytes) = map_to_pat_info(payload.clone(), position)?;
        personal_access_tokens.push(personal_access_token);
        position += read_bytes;
    }
    personal_access_tokens.sort_by(|x, y| x.name.cmp(&y.name));
    Ok(personal_access_tokens)
}

pub fn map_identity_info(payload: Bytes) -> Result<IdentityInfo, IggyError> {
    let user_id = u32::from_le_bytes(
        payload[..4]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    Ok(IdentityInfo {
        user_id,
        access_token: None,
    })
}

pub fn map_raw_pat(payload: Bytes) -> Result<RawPersonalAccessToken, IggyError> {
    let token_length = payload[0];
    let token = from_utf8(&payload[1..1 + token_length as usize])
        .map_err(|_| IggyError::InvalidUtf8)?
        .to_string();
    Ok(RawPersonalAccessToken { token })
}

pub fn map_client(payload: Bytes) -> Result<ClientInfoDetails, IggyError> {
    let (client, mut position) = map_to_client_info(payload.clone(), 0)?;
    let mut consumer_groups = Vec::new();
    let length = payload.len();
    while position < length {
        for _ in 0..client.consumer_groups_count {
            let stream_id = u32::from_le_bytes(
                payload[position..position + 4]
                    .try_into()
                    .map_err(|_| IggyError::InvalidNumberEncoding)?,
            );
            let topic_id = u32::from_le_bytes(
                payload[position + 4..position + 8]
                    .try_into()
                    .map_err(|_| IggyError::InvalidNumberEncoding)?,
            );
            let group_id = u32::from_le_bytes(
                payload[position + 8..position + 12]
                    .try_into()
                    .map_err(|_| IggyError::InvalidNumberEncoding)?,
            );
            let consumer_group = ConsumerGroupInfo {
                stream_id,
                topic_id,
                group_id,
            };
            consumer_groups.push(consumer_group);
            position += 12;
        }
    }

    consumer_groups.sort_by(|x, y| x.group_id.cmp(&y.group_id));
    let client = ClientInfoDetails {
        client_id: client.client_id,
        user_id: client.user_id,
        address: client.address,
        transport: client.transport,
        consumer_groups_count: client.consumer_groups_count,
        consumer_groups,
    };
    Ok(client)
}

pub fn map_clients(payload: Bytes) -> Result<Vec<ClientInfo>, IggyError> {
    if payload.is_empty() {
        return Ok(EMPTY_CLIENTS);
    }

    let mut clients = Vec::new();
    let length = payload.len();
    let mut position = 0;
    while position < length {
        let (client, read_bytes) = map_to_client_info(payload.clone(), position)?;
        clients.push(client);
        position += read_bytes;
    }
    clients.sort_by(|x, y| x.client_id.cmp(&y.client_id));
    Ok(clients)
}

pub fn map_streams(payload: Bytes) -> Result<Vec<Stream>, IggyError> {
    if payload.is_empty() {
        return Ok(EMPTY_STREAMS);
    }

    let mut streams = Vec::new();
    let length = payload.len();
    let mut position = 0;
    while position < length {
        let (stream, read_bytes) = map_to_stream(payload.clone(), position)?;
        streams.push(stream);
        position += read_bytes;
    }
    streams.sort_by(|x, y| x.id.cmp(&y.id));
    Ok(streams)
}

pub fn map_stream(payload: Bytes) -> Result<StreamDetails, IggyError> {
    let (stream, mut position) = map_to_stream(payload.clone(), 0)?;
    let mut topics = Vec::new();
    let length = payload.len();
    while position < length {
        let (topic, read_bytes) = map_to_topic(payload.clone(), position)?;
        topics.push(topic);
        position += read_bytes;
    }

    topics.sort_by(|x, y| x.id.cmp(&y.id));
    let stream = StreamDetails {
        id: stream.id,
        created_at: stream.created_at,
        topics_count: stream.topics_count,
        size: stream.size,
        messages_count: stream.messages_count,
        name: stream.name,
        topics,
    };
    Ok(stream)
}

fn map_to_stream(payload: Bytes, position: usize) -> Result<(Stream, usize), IggyError> {
    let id = u32::from_le_bytes(
        payload[position..position + 4]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    let created_at = u64::from_le_bytes(
        payload[position + 4..position + 12]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    )
    .into();
    let topics_count = u32::from_le_bytes(
        payload[position + 12..position + 16]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    let size_bytes = u64::from_le_bytes(
        payload[position + 16..position + 24]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    )
    .into();
    let messages_count = u64::from_le_bytes(
        payload[position + 24..position + 32]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    let name_length = payload[position + 32];
    let name = from_utf8(&payload[position + 33..position + 33 + name_length as usize])
        .map_err(|_| IggyError::InvalidUtf8)?
        .to_string();
    let read_bytes = 4 + 8 + 4 + 8 + 8 + 1 + name_length as usize;
    Ok((
        Stream {
            id,
            created_at,
            name,
            size: size_bytes,
            messages_count,
            topics_count,
        },
        read_bytes,
    ))
}

pub fn map_topics(payload: Bytes) -> Result<Vec<Topic>, IggyError> {
    if payload.is_empty() {
        return Ok(EMPTY_TOPICS);
    }

    let mut topics = Vec::new();
    let length = payload.len();
    let mut position = 0;
    while position < length {
        let (topic, read_bytes) = map_to_topic(payload.clone(), position)?;
        topics.push(topic);
        position += read_bytes;
    }
    topics.sort_by(|x, y| x.id.cmp(&y.id));
    Ok(topics)
}

pub fn map_topic(payload: Bytes) -> Result<TopicDetails, IggyError> {
    let (topic, mut position) = map_to_topic(payload.clone(), 0)?;
    let mut partitions = Vec::new();
    let length = payload.len();
    while position < length {
        let (partition, read_bytes) = map_to_partition(payload.clone(), position)?;
        partitions.push(partition);
        position += read_bytes;
    }

    partitions.sort_by(|x, y| x.id.cmp(&y.id));
    let topic = TopicDetails {
        id: topic.id,
        created_at: topic.created_at,
        name: topic.name,
        size: topic.size,
        messages_count: topic.messages_count,
        message_expiry: topic.message_expiry,
        compression_algorithm: topic.compression_algorithm,
        max_topic_size: topic.max_topic_size,
        replication_factor: topic.replication_factor,
        #[allow(clippy::cast_possible_truncation)]
        partitions_count: partitions.len() as u32,
        partitions,
    };
    Ok(topic)
}

fn map_to_topic(payload: Bytes, position: usize) -> Result<(Topic, usize), IggyError> {
    let id = u32::from_le_bytes(
        payload[position..position + 4]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    let created_at = u64::from_le_bytes(
        payload[position + 4..position + 12]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    let created_at = created_at.into();
    let partitions_count = u32::from_le_bytes(
        payload[position + 12..position + 16]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    let message_expiry = match u64::from_le_bytes(
        payload[position + 16..position + 24]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    ) {
        0 => IggyExpiry::NeverExpire,
        message_expiry => message_expiry.into(),
    };
    let compression_algorithm = CompressionAlgorithm::from_code(payload[position + 24])?;
    let max_topic_size = u64::from_le_bytes(
        payload[position + 25..position + 33]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    let max_topic_size: MaxTopicSize = max_topic_size.into();
    let replication_factor = payload[position + 33];
    let size_bytes = IggyByteSize::from(u64::from_le_bytes(
        payload[position + 34..position + 42]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    ));
    let messages_count = u64::from_le_bytes(
        payload[position + 42..position + 50]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    let name_length = payload[position + 50];
    let name = from_utf8(&payload[position + 51..position + 51 + name_length as usize])
        .map_err(|_| IggyError::InvalidUtf8)?
        .to_string();
    let read_bytes = 4 + 8 + 4 + 8 + 8 + 8 + 8 + 1 + 1 + 1 + name_length as usize;
    Ok((
        Topic {
            id,
            created_at,
            name,
            partitions_count,
            size: size_bytes,
            messages_count,
            message_expiry,
            compression_algorithm,
            max_topic_size,
            replication_factor,
        },
        read_bytes,
    ))
}

fn map_to_partition(payload: Bytes, position: usize) -> Result<(Partition, usize), IggyError> {
    let id = u32::from_le_bytes(
        payload[position..position + 4]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    let created_at = u64::from_le_bytes(
        payload[position + 4..position + 12]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    let created_at = created_at.into();
    let segments_count = u32::from_le_bytes(
        payload[position + 12..position + 16]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    let current_offset = u64::from_le_bytes(
        payload[position + 16..position + 24]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    let size_bytes = u64::from_le_bytes(
        payload[position + 24..position + 32]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    )
    .into();
    let messages_count = u64::from_le_bytes(
        payload[position + 32..position + 40]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    let read_bytes = 4 + 8 + 4 + 8 + 8 + 8;
    Ok((
        Partition {
            id,
            created_at,
            segments_count,
            current_offset,
            size: size_bytes,
            messages_count,
        },
        read_bytes,
    ))
}

pub fn map_consumer_groups(payload: Bytes) -> Result<Vec<ConsumerGroup>, IggyError> {
    if payload.is_empty() {
        return Ok(EMPTY_CONSUMER_GROUPS);
    }

    let mut consumer_groups = Vec::new();
    let length = payload.len();
    let mut position = 0;
    while position < length {
        let (consumer_group, read_bytes) = map_to_consumer_group(payload.clone(), position)?;
        consumer_groups.push(consumer_group);
        position += read_bytes;
    }
    consumer_groups.sort_by(|x, y| x.id.cmp(&y.id));
    Ok(consumer_groups)
}

pub fn map_consumer_group(payload: Bytes) -> Result<ConsumerGroupDetails, IggyError> {
    let (consumer_group, mut position) = map_to_consumer_group(payload.clone(), 0)?;
    let mut members = Vec::new();
    let length = payload.len();
    while position < length {
        let (member, read_bytes) = map_to_consumer_group_member(payload.clone(), position)?;
        members.push(member);
        position += read_bytes;
    }
    members.sort_by(|x, y| x.id.cmp(&y.id));
    let consumer_group_details = ConsumerGroupDetails {
        id: consumer_group.id,
        name: consumer_group.name,
        partitions_count: consumer_group.partitions_count,
        members_count: consumer_group.members_count,
        members,
    };
    Ok(consumer_group_details)
}

fn map_to_consumer_group(
    payload: Bytes,
    position: usize,
) -> Result<(ConsumerGroup, usize), IggyError> {
    let id = u32::from_le_bytes(
        payload[position..position + 4]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    let partitions_count = u32::from_le_bytes(
        payload[position + 4..position + 8]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    let members_count = u32::from_le_bytes(
        payload[position + 8..position + 12]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    let name_length = payload[position + 12];
    let name = from_utf8(&payload[position + 13..position + 13 + name_length as usize])
        .map_err(|_| IggyError::InvalidUtf8)?
        .to_string();
    let read_bytes = 13 + name_length as usize;
    Ok((
        ConsumerGroup {
            id,
            partitions_count,
            members_count,
            name,
        },
        read_bytes,
    ))
}

fn map_to_consumer_group_member(
    payload: Bytes,
    position: usize,
) -> Result<(ConsumerGroupMember, usize), IggyError> {
    let id = u32::from_le_bytes(
        payload[position..position + 4]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    let partitions_count = u32::from_le_bytes(
        payload[position + 4..position + 8]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    let mut partitions = Vec::new();
    for i in 0..partitions_count {
        let partition_id = u32::from_le_bytes(
            payload[position + 8 + (i * 4) as usize..position + 8 + ((i + 1) * 4) as usize]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        partitions.push(partition_id);
    }

    let read_bytes = (4 + 4 + partitions_count * 4) as usize;
    Ok((
        ConsumerGroupMember {
            id,
            partitions_count,
            partitions,
        },
        read_bytes,
    ))
}

fn map_to_client_info(
    payload: Bytes,
    mut position: usize,
) -> Result<(ClientInfo, usize), IggyError> {
    let mut read_bytes;
    let client_id = u32::from_le_bytes(
        payload[position..position + 4]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    let user_id = u32::from_le_bytes(
        payload[position + 4..position + 8]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    let user_id = match user_id {
        u32::MAX => None,
        _ => Some(user_id),
    };

    let transport = payload[position + 8];
    let transport = match transport {
        1 => "TCP",
        2 => "QUIC",
        _ => "Unknown",
    }
    .to_string();

    let address_length = u32::from_le_bytes(
        payload[position + 9..position + 13]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    ) as usize;
    let address = from_utf8(&payload[position + 13..position + 13 + address_length])
        .map_err(|_| IggyError::InvalidUtf8)?
        .to_string();
    read_bytes = 4 + 4 + 1 + 4 + address_length;
    position += read_bytes;
    let consumer_groups_count = u32::from_le_bytes(
        payload[position..position + 4]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    read_bytes += 4;
    Ok((
        ClientInfo {
            client_id,
            user_id,
            address,
            transport,
            consumer_groups_count,
        },
        read_bytes,
    ))
}

fn map_to_user_info(payload: Bytes, position: usize) -> Result<(UserInfo, usize), IggyError> {
    let id = u32::from_le_bytes(
        payload[position..position + 4]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    let created_at = u64::from_le_bytes(
        payload[position + 4..position + 12]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    let created_at = created_at.into();
    let status = payload[position + 12];
    let status = UserStatus::from_code(status)?;
    let username_length = payload[position + 13];
    let username = from_utf8(&payload[position + 14..position + 14 + username_length as usize])
        .map_err(|_| IggyError::InvalidUtf8)?
        .to_string();
    let read_bytes = 4 + 8 + 1 + 1 + username_length as usize;

    Ok((
        UserInfo {
            id,
            created_at,
            status,
            username,
        },
        read_bytes,
    ))
}

fn map_to_pat_info(
    payload: Bytes,
    position: usize,
) -> Result<(PersonalAccessTokenInfo, usize), IggyError> {
    let name_length = payload[position];
    let name = from_utf8(&payload[position + 1..position + 1 + name_length as usize])
        .map_err(|_| IggyError::InvalidUtf8)?
        .to_string();
    let position = position + 1 + name_length as usize;
    let expiry_at = u64::from_le_bytes(
        payload[position..position + 8]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    let expiry_at = match expiry_at {
        0 => None,
        value => Some(value.into()),
    };
    let read_bytes = 1 + name_length as usize + 8;
    Ok((PersonalAccessTokenInfo { name, expiry_at }, read_bytes))
}

pub fn map_cluster_metadata(payload: Bytes) -> Result<ClusterMetadata, IggyError> {
    ClusterMetadata::from_bytes(payload)
}
