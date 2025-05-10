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
#pragma once

#include <optional>
#include <string>
#include <unordered_map>
#include <vector>
#include "types.h"

namespace icp {

/**
 * @brief Value objects used in the Iggy protocol.
 */
namespace model {

/**
 * @brief Base class for all value objects used in the Iggy protocol.
 */
class Model {
public:
    virtual ~Model() = default;
};

/**
 * @brief Models shared across multiple functional groups.
 */
namespace shared {
/**
 * @brief Discriminator to allow correct decoding of identifier bytes.
 */
enum IdKind { NUMERIC = 1, STRING = 2 };

/**
 * @brief An identifier for a stream or a topic.
 */
class Identifier : Model {
private:
    const IdKind kind;
    const uint8_t length;
    const std::vector<unsigned char>& value;

public:
    Identifier(IdKind kind, uint8_t length, const std::vector<unsigned char>& value)
        : kind(kind)
        , length(length)
        , value(value) {}
    IdKind getKind() { return kind; }
    uint8_t getLength() { return length; }
    const std::vector<unsigned char>& getValue() { return value; }
};

/**
 * @brief Discriminator to allow correct decoding of consumer kind bytes.
 */
enum ConsumerKind { CONSUMER = 1, CONSUMER_GROUP = 2 };

/**
 * @brief A consumer of a message stream; may be part of a consumer group.
 */
class Consumer : Model {
private:
    const ConsumerKind kind;
    const uint32_t id;

public:
    Consumer(ConsumerKind kind, uint32_t id)
        : kind(kind)
        , id(id) {}
    ConsumerKind getKind() { return kind; }
    uint32_t getId() { return id; }
};
};  // namespace shared

namespace partition {

/**
 * @brief Metadata describing a partition of a topic.
 */
class Partition : Model {
private:
    const uint32_t id;
    const uint64_t createdAt;
    const uint32_t segmentsCount;
    const uint64_t currentOffset;
    const uint64_t sizeBytes;
    uint64_t messagesCount;

public:
    Partition(uint32_t id, uint64_t createdAt, uint32_t segmentsCount, uint64_t currentOffset, uint64_t sizeBytes, uint64_t messagesCount)
        : id(id)
        , createdAt(createdAt)
        , segmentsCount(segmentsCount)
        , currentOffset(currentOffset)
        , sizeBytes(sizeBytes)
        , messagesCount(messagesCount) {}
    uint32_t getId() { return id; }
    uint64_t getCreatedAt() { return createdAt; }
    uint32_t getSegmentsCount() { return segmentsCount; }
    uint64_t getCurrentOffset() { return currentOffset; }
    uint64_t getSizeBytes() { return sizeBytes; }
    uint64_t getMessagesCount() { return messagesCount; }
};
};  // namespace partition

/**
 * @brief Models related to topic metadata.
 */
namespace topic {

/**
 * @brief Metadata describing a topic at a summary level.
 */
class Topic : Model {
private:
    const uint32_t id;
    const uint64_t createdAt;
    const std::string& name;
    const uint64_t sizeBytes;
    const std::optional<uint32_t> messageExpiry;
    const std::optional<uint64_t> maxTopicSize;
    const uint8_t replicationFactor;
    const uint64_t messagesCount;
    const uint32_t partitionsCount;

public:
    Topic(uint32_t id,
          uint64_t createdAt,
          const std::string& name,
          uint64_t sizeBytes,
          std::optional<uint32_t> messageExpiry,
          std::optional<uint64_t> maxTopicSize,
          uint8_t replicationFactor,
          uint64_t messagesCount,
          uint32_t partitionsCount)
        : id(id)
        , createdAt(createdAt)
        , name(name)
        , sizeBytes(sizeBytes)
        , messageExpiry(messageExpiry)
        , maxTopicSize(maxTopicSize)
        , replicationFactor(replicationFactor)
        , messagesCount(messagesCount)
        , partitionsCount(partitionsCount) {}
    uint32_t getId() { return id; }
    uint64_t getCreatedAt() { return createdAt; }
    const std::string& getName() { return name; }
    uint64_t getSizeBytes() { return sizeBytes; }
    std::optional<uint32_t> getMessageExpiry() { return messageExpiry; }
    std::optional<uint64_t> getMaxTopicSize() { return maxTopicSize; }
    uint8_t getReplicationFactor() { return replicationFactor; }
    uint64_t getMessagesCount() { return messagesCount; }
    uint32_t getPartitionsCount() { return partitionsCount; }
};

/**
 * @brief Metadata describing a topic, including partition details.
 */
class TopicDetails : Model {
private:
    uint32_t id;
    uint64_t createdAt;
    std::string name;
    uint64_t sizeBytes;
    const std::optional<uint32_t> messageExpiry;
    const std::optional<uint64_t> maxTopicSize;
    uint8_t replicationFactor;
    uint64_t messagesCount;
    uint32_t partitionsCount;
    const std::vector<partition::Partition>& partitions;

public:
    TopicDetails(uint32_t id,
                 uint64_t createdAt,
                 const std::string& name,
                 uint64_t sizeBytes,
                 std::optional<uint32_t> messageExpiry,
                 std::optional<uint64_t> maxTopicSize,
                 uint8_t replicationFactor,
                 uint64_t messagesCount,
                 uint32_t partitionsCount,
                 const std::vector<partition::Partition>& partitions)
        : id(id)
        , createdAt(createdAt)
        , name(name)
        , sizeBytes(sizeBytes)
        , messageExpiry(messageExpiry)
        , maxTopicSize(maxTopicSize)
        , replicationFactor(replicationFactor)
        , messagesCount(messagesCount)
        , partitionsCount(partitionsCount)
        , partitions(partitions) {}

    uint32_t getId() { return id; }
    uint64_t getCreatedAt() { return createdAt; }
    const std::string& getName() { return name; }
    uint64_t getSizeBytes() { return sizeBytes; }
    std::optional<uint32_t> getMessageExpiry() { return messageExpiry; }
    std::optional<uint64_t> getMaxTopicSize() { return maxTopicSize; }
    uint8_t getReplicationFactor() { return replicationFactor; }
    uint64_t getMessagesCount() { return messagesCount; }
    uint32_t getPartitionsCount() { return partitionsCount; }
    const std::vector<partition::Partition>& getPartitions() { return partitions; }
};
};  // namespace topic

/**
 * @brief Models related to message stream metadata.
 */
namespace stream {

/**
 * @brief Metadata describing a message stream including topic details.
 */
class StreamDetails : Model {
private:
    const uint32_t id;
    const uint64_t createdAt;
    const std::string& name;
    const uint64_t sizeBytes;
    const uint64_t messagesCount;
    const uint32_t topicsCount;
    const std::vector<topic::Topic>& topics;

public:
    StreamDetails(uint32_t id,
                  uint64_t createdAt,
                  const std::string& name,
                  uint64_t sizeBytes,
                  uint64_t messagesCount,
                  uint32_t topicsCount,
                  const std::vector<topic::Topic>& topics)
        : id(id)
        , createdAt(createdAt)
        , name(name)
        , sizeBytes(sizeBytes)
        , messagesCount(messagesCount)
        , topicsCount(topicsCount)
        , topics(topics) {}
    uint32_t getId() { return id; }
    uint64_t getCreatedAt() { return createdAt; }
    const std::string getName() & { return name; }
    uint64_t getSizeBytes() { return sizeBytes; }
    uint64_t getMessagesCount() { return messagesCount; }
    uint32_t getTopicsCount() { return topicsCount; }
    const std::vector<topic::Topic>& getTopics() { return topics; }
};
};  // namespace stream

/**
 * @brief Models related to messages consumed and sent to the Iggy server.
 */
namespace message {

/**
 * @brief Current state of the message on the server.
 */
enum MessageState { AVAILABLE = 1, UNAVAILABLE = 10, POISONED = 20, MARKED_FOR_DELETION = 30 };

typedef std::string HeaderKey;

/**
 * @brief Discriminator to allow correct decoding of header value bytes representing different value types.
 */
enum HeaderKind {
    RAW = 1,
    STRING = 2,
    BOOL = 3,
    INT8 = 4,
    INT16 = 5,
    INT32 = 6,
    INT64 = 7,
    INT128 = 8,
    UINT8 = 9,
    UINT16 = 10,
    UINT32 = 11,
    UINT64 = 12,
    UINT128 = 13,
    FLOAT32 = 14,
    FLOAT64 = 15
};

/**
 * @brief A value of various types associated with the message header -- message metadata.
 */
class HeaderValue : Model {
private:
    const HeaderKind kind;
    const std::vector<unsigned char>& value;

public:
    HeaderValue(HeaderKind kind, const std::vector<unsigned char>& value)
        : kind(kind)
        , value(value) {}

    HeaderKind getKind() const { return kind; }
    const std::vector<unsigned char>& getValue() const { return value; }
};

/**
 * @brief A message consumed or sent to the server, with binary payload and flexible metadata.
 */
class Message : Model {
private:
    // core message state
    const uint128_t id;
    const std::unordered_map<HeaderKey, HeaderValue>& headers;
    const uint32_t length;
    const std::vector<unsigned char>& payload;

    // message state set on the server-side
    const std::optional<uint64_t> offset;
    const std::optional<MessageState> state;
    const std::optional<uint64_t> timestamp;
    const std::optional<uint32_t> checksum;

public:
    /**
     * @brief Fully-qualified message constructor; @ref isComplete will be true.
     */
    Message(uint128_t id,
            const std::unordered_map<HeaderKey, HeaderValue>& headers,
            uint32_t length,
            const std::vector<unsigned char>& payload,
            std::optional<uint64_t> offset,
            std::optional<MessageState> state,
            std::optional<uint64_t> timestamp,
            std::optional<uint32_t> checksum)
        : id(id)
        , headers(headers)
        , length(length)
        , payload(payload)
        , offset(offset)
        , state(state)
        , timestamp(timestamp)
        , checksum(checksum) {}

    /**
     * @brief Simpler constructor for a message to be delivered to the server; @ref isComplete will be false.
     */
    Message(uint128_t id,
            const std::unordered_map<HeaderKey, HeaderValue>& headers,
            uint32_t length,
            const std::vector<unsigned char>& payload)
        : Message(id,
                  headers,
                  length,
                  payload,
                  std::optional<uint64_t>(),
                  std::optional<MessageState>(),
                  std::optional<uint64_t>(),
                  std::optional<uint32_t>()) {}

    uint128_t getId() const { return id; }
    const std::unordered_map<HeaderKey, HeaderValue>& getHeaders() const { return headers; }
    uint32_t getLength() const { return length; }
    const std::vector<unsigned char>& getPayload() const { return payload; }
    std::optional<uint64_t> getOffset() const { return offset; }
    std::optional<MessageState> getState() const { return state; }
    std::optional<uint64_t> getTimestamp() const { return timestamp; }
    std::optional<uint32_t> getChecksum() const { return checksum; }

    /**
     * @brief Check if the message has all the server-side fields set.
     */
    bool isComplete() const { return offset.has_value() && state.has_value() && timestamp.has_value() && checksum.has_value(); }
};

/**
 * @brief Carrier object for all the messages polled from the server,
 */
class PolledMessages : Model {
private:
    const uint32_t partition_id;
    const uint64_t current_offset;
    const std::vector<Message>& messages;

public:
    PolledMessages(uint32_t partition_id, uint64_t current_offset, const std::vector<Message>& messages)
        : partition_id(partition_id)
        , current_offset(current_offset)
        , messages(messages) {}

    uint32_t getPartitionId() const { return partition_id; }
    uint64_t getCurrentOffset() const { return current_offset; }
    const std::vector<Message>& getMessages() const { return messages; }
};

}  // namespace message

/**
 * @brief Information about server-managed stream consumer offsets.
 */
namespace consumeroffset {

/**
 * @brief Current state of the client's position reading the stream via a consumer or consumer group.
 */
class ConsumerOffsetInfo : Model {
private:
    const uint32_t partitionId;
    const uint64_t currentOffset;
    const uint64_t storedOffset;

public:
    ConsumerOffsetInfo(uint32_t partitionId, uint64_t currentOffset, uint64_t storedOffset)
        : partitionId(partitionId)
        , currentOffset(currentOffset)
        , storedOffset(storedOffset) {}
    uint32_t getPartitionId() { return partitionId; }
    uint64_t getCurrentOffset() { return currentOffset; }
    uint64_t getStoredOffset() { return storedOffset; }
};
};  // namespace consumeroffset

/**
 * @brief Models related to consumer groups, which are used to coordinate message consumption across multiple clients.
 */
namespace consumergroup {

class ConsumerGroupMember : Model {
private:
    const uint32_t id;
    const uint32_t partitionsCount;
    const std::vector<uint32_t>& partitions;

public:
    ConsumerGroupMember(uint32_t id, uint32_t partitionsCount, const std::vector<uint32_t>& partitions)
        : id(id)
        , partitionsCount(partitionsCount)
        , partitions(partitions) {}
    uint32_t getId() { return id; }
    uint32_t getPartitionsCount() { return partitionsCount; }
    const std::vector<uint32_t>& getPartitions() { return partitions; }
};

class ConsumerGroupDetails : Model {
private:
    uint32_t id;
    const std::string& name;
    uint32_t paritionsCount;
    uint32_t membersCount;
    const std::vector<ConsumerGroupMember>& members;

public:
    ConsumerGroupDetails(uint32_t id,
                         const std::string& name,
                         uint32_t paritionsCount,
                         uint32_t membersCount,
                         const std::vector<ConsumerGroupMember>& members)
        : id(id)
        , name(name)
        , paritionsCount(paritionsCount)
        , membersCount(membersCount)
        , members(members) {}
    uint32_t getId() const { return id; }
    const std::string& getName() const { return name; }
    uint32_t getParitionsCount() const { return paritionsCount; }
    uint32_t getMembersCount() const { return membersCount; }
    const std::vector<ConsumerGroupMember>& getMembers() const { return members; }
};
};  // namespace consumergroup

/**
 * @brief Models related to global system state.
 */
namespace sys {

/**
 * @brief Summary of the consumer groups that a client has joined.
 */
class ConsumerGroupInfo : Model {
private:
    const uint32_t streamId;
    const uint32_t topicId;
    const uint32_t consumerGroupId;

public:
    ConsumerGroupInfo(uint32_t streamId, uint32_t topicId, uint32_t consumerGroupId)
        : streamId(streamId)
        , topicId(topicId)
        , consumerGroupId(consumerGroupId) {}
    uint32_t getStreamId() { return streamId; }
    uint32_t getTopicId() { return topicId; }
    uint32_t getConsumerGroupId() { return consumerGroupId; }
};

/**
 * @brief Information about a client connected to the server.
 */
class ClientInfoDetails : Model {
private:
    const uint32_t clientId;
    const std::optional<uint32_t> userId;
    const std::string& address;
    const std::string& transport;
    const uint32_t consumerGroupsCount;
    const std::vector<ConsumerGroupInfo>& consumerGroups;

public:
    ClientInfoDetails(uint32_t clientId,
                      std::optional<uint32_t> userId,
                      const std::string& address,
                      const std::string& transport,
                      uint32_t consumerGroupsCount,
                      const std::vector<ConsumerGroupInfo>& consumerGroups)
        : clientId(clientId)
        , userId(userId)
        , address(address)
        , transport(transport)
        , consumerGroupsCount(consumerGroupsCount)
        , consumerGroups(consumerGroups) {}
    uint32_t getClientId() const { return clientId; }
    std::optional<uint32_t> getUserId() const { return userId; }
    const std::string& getAddress() const { return address; }
    const std::string& getTransport() const { return transport; }
    uint32_t getConsumerGroupsCount() const { return consumerGroupsCount; }
    const std::vector<ConsumerGroupInfo>& getConsumerGroups() const { return consumerGroups; }
};

/**
 * @brief Model class holding server performance statistics.
 *
 * This class is used to represent the result of the GetStats call,
 * which gets a snapshot of latest server performance statistics.
 *
 * @see [GetStats definition](https://docs.iggy.rs/specification/binary#get-stats)
 * @see [stats.rs](https://github.com/iggy-rs/iggy/blob/master/iggy/src/models/stats.rs)
 */
class Stats : Model {
private:
    const pid_t process_id;
    const percent_t cpu_usage;
    const byte_cnt_t memory_usage;
    const byte_cnt_t total_memory;
    const byte_cnt_t available_memory;
    const time_val_t run_time;
    const time_val_t start_time;
    const byte_cnt_t read_bytes;
    const byte_cnt_t written_bytes;
    const byte_cnt_t messages_size_bytes;
    const obj_cnt_t streams_count;
    const obj_cnt_t topics_count;
    const obj_cnt_t partitions_count;
    const obj_cnt_t segments_count;
    const msg_cnt_t messages_count;
    const obj_cnt_t clients_count;
    const obj_cnt_t consumer_groups_count;
    const std::string& hostname;
    const std::string& os_name;
    const std::string& os_version;
    const std::string& kernel_version;

public:
    Stats(pid_t process_id,
          percent_t cpu_usage,
          byte_cnt_t memory_usage,
          byte_cnt_t total_memory,
          byte_cnt_t available_memory,
          time_val_t run_time,
          time_val_t start_time,
          byte_cnt_t read_bytes,
          byte_cnt_t written_bytes,
          byte_cnt_t messages_size_bytes,
          obj_cnt_t streams_count,
          obj_cnt_t topics_count,
          obj_cnt_t partitions_count,
          obj_cnt_t segments_count,
          msg_cnt_t messages_count,
          obj_cnt_t clients_count,
          obj_cnt_t consumer_groups_count,
          const std::string& hostname,
          const std::string& os_name,
          const std::string& os_version,
          const std::string& kernel_version)
        : process_id(process_id)
        , cpu_usage(cpu_usage)
        , memory_usage(memory_usage)
        , total_memory(total_memory)
        , available_memory(available_memory)
        , run_time(run_time)
        , start_time(start_time)
        , read_bytes(read_bytes)
        , written_bytes(written_bytes)
        , messages_size_bytes(messages_size_bytes)
        , streams_count(streams_count)
        , topics_count(topics_count)
        , partitions_count(partitions_count)
        , segments_count(segments_count)
        , messages_count(messages_count)
        , clients_count(clients_count)
        , consumer_groups_count(consumer_groups_count)
        , hostname(hostname)
        , os_name(os_name)
        , os_version(os_version)
        , kernel_version(kernel_version) {}

    /// @brief Get the server process ID (PID)
    pid_t getProcessId() const { return process_id; }

    /// @brief Get the server process CPU usage.
    percent_t getCpuUsage() const { return cpu_usage; }

    /// @brief Get the server total memory usage.
    byte_cnt_t getMemoryUsage() const { return memory_usage; }

    /// @brief Get the server total memory.
    byte_cnt_t getTotalMemory() const { return total_memory; }

    /// @brief Get the server available memory.
    byte_cnt_t getAvailableMemory() const { return available_memory; }

    /// @brief Get how long the server has been running, in seconds.
    time_val_t getRunTime() const { return run_time; }

    /// @brief Get the server start time, in seconds since the UNIX epoch.
    time_val_t getStartTime() const { return start_time; }

    /// @brief Get the number of bytes read since process start.
    byte_cnt_t getReadBytes() const { return read_bytes; }

    /// @brief Get the number of bytes written since process start.
    byte_cnt_t getWrittenBytes() const { return written_bytes; }

    /// @brief Get the total size of all messages processed.
    byte_cnt_t getMessagesSizeBytes() const { return messages_size_bytes; }

    /// @brief Get the total number of streams in the server.
    obj_cnt_t getStreamsCount() const { return streams_count; }

    /// @brief Get the total number of topics on the server.
    obj_cnt_t getTopicsCount() const { return topics_count; }

    /// @brief Get the total number of topic partitions on the server across all topics.
    obj_cnt_t getPartitionsCount() const { return partitions_count; }

    /// @brief Get the total number of fixed-size segments used for topic storage on disk.
    obj_cnt_t getSegmentsCount() const { return segments_count; }

    /// @brief Get the total number of messages processed by the server across all topics.
    msg_cnt_t getMessagesCount() const { return messages_count; }

    /// @brief Get the total number of active connected clients.
    obj_cnt_t getClientsCount() const { return clients_count; }

    /// @brief Get the total number of active consumer groups on the server.
    obj_cnt_t getConsumerGroupsCount() const { return consumer_groups_count; }

    /// @brief Get the name of the host that the server process is running on.
    const std::string& getHostname() const { return hostname; }

    /// @brief Get the name of the operating system that the server process is running on.
    const std::string& getOsName() const { return os_name; }

    /// @brief Get the version of the operating system that the server process is running on.
    const std::string& getOsVersion() const { return os_version; }

    /// @brief Get the version of the OS kernel that the server process is running on.
    const std::string& getKernelVersion() const { return kernel_version; }
};

}  // namespace sys
}  // namespace model
}  // namespace icp
