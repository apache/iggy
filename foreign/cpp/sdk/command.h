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

#include <string>
#include "model.h"
#include "types.h"

namespace icp {

/**
 * @brief All supported commands in the Iggy protocol.
 *
 * Serialization-agnostic Command classes that you can send via an @ref icp::client::Client.
 */
namespace command {

class Command {
public:
    virtual ~Command() = default;
};

/**
 * @brief Commands related to managing Iggy streams.
 */
namespace stream {
class GetStream : Command {
private:
    const icp::model::shared::Identifier& streamId;

public:
    explicit GetStream(const icp::model::shared::Identifier& streamId)
        : streamId(streamId) {}

    const icp::model::shared::Identifier& getStreamId() const { return streamId; }
};

class GetStreams : Command {};
class CreateStream : Command {
private:
    const icp::model::shared::Identifier& streamId;
    const std::string& name;

public:
    CreateStream(const icp::model::shared::Identifier& streamId, const std::string& name)
        : streamId(streamId)
        , name(name) {}

    const icp::model::shared::Identifier& getStreamId() const { return streamId; }
    const std::string& getName() const { return name; }
};

class DeleteStream : Command {
private:
    const icp::model::shared::Identifier& streamId;

public:
    explicit DeleteStream(const icp::model::shared::Identifier& streamId)
        : streamId(streamId) {}

    const icp::model::shared::Identifier& getStreamId() const { return streamId; }
};
}  // namespace stream

/**
 * @brief Commands related to managing Iggy topics.
 */
namespace topic {
class GetTopic : Command {
private:
    const icp::model::shared::Identifier& streamId;
    const icp::model::shared::Identifier& topicId;

public:
    GetTopic(const icp::model::shared::Identifier& streamId, const icp::model::shared::Identifier& topicId)
        : streamId(streamId)
        , topicId(topicId) {}

    const icp::model::shared::Identifier& getStreamId() const { return streamId; }
    const icp::model::shared::Identifier& getTopicId() const { return topicId; }
};

class GetTopics : Command {
private:
    const icp::model::shared::Identifier& streamId;

public:
    explicit GetTopics(const icp::model::shared::Identifier& streamId)
        : streamId(streamId) {}

    const icp::model::shared::Identifier& getStreamId() const { return streamId; }
};

class CreateTopic : Command {
private:
    const icp::model::shared::Identifier& streamId;
    const uint32_t topicId;
    const uint32_t partitionsCount;
    const std::optional<uint32_t> messageExpiry;
    const std::string& name;

public:
    CreateTopic(const icp::model::shared::Identifier& streamId,
                uint32_t topicId,
                uint32_t partitionsCount,
                const std::optional<uint32_t> messageExpiry,
                const std::string& name)
        : streamId(streamId)
        , topicId(topicId)
        , partitionsCount(partitionsCount)
        , messageExpiry(messageExpiry)
        , name(name) {}

    const icp::model::shared::Identifier& getStreamId() const { return streamId; }
    uint32_t getTopicId() const { return topicId; }
    uint32_t getPartitionsCount() const { return partitionsCount; }
    const std::optional<uint32_t> getMessageExpiry() const { return messageExpiry; }
    const std::string& getName() const { return name; }
};
}  // namespace topic

/**
 * @brief Commands related to managing Iggy partitions.
 */
namespace partition {
class CreatePartitions : Command {
private:
    const icp::model::shared::Identifier& streamId;
    const icp::model::shared::Identifier& topicId;
    const uint32_t partitionsCount;

public:
    CreatePartitions(const icp::model::shared::Identifier& streamId,
                     const icp::model::shared::Identifier& topicId,
                     uint32_t partitionsCount)
        : streamId(streamId)
        , topicId(topicId)
        , partitionsCount(partitionsCount) {}

    const icp::model::shared::Identifier& getStreamId() const { return streamId; }
    const icp::model::shared::Identifier& getTopicId() const { return topicId; }
    uint32_t getPartitionsCount() const { return partitionsCount; }
};

class DeletePartitions : Command {
private:
    const icp::model::shared::Identifier& streamId;
    const icp::model::shared::Identifier& topicId;
    const uint32_t partitionsCount;

public:
    DeletePartitions(const icp::model::shared::Identifier& streamId,
                     const icp::model::shared::Identifier& topicId,
                     uint32_t partitionsCount)
        : streamId(streamId)
        , topicId(topicId)
        , partitionsCount(partitionsCount) {}

    const icp::model::shared::Identifier& getStreamId() const { return streamId; }
    const icp::model::shared::Identifier& getTopicId() const { return topicId; }
    uint32_t getPartitionsCount() const { return partitionsCount; }
};
}  // namespace partition

/**
 * @brief Commands related to reading and writing messages.
 */
namespace message {
enum PollingKind { OFFSET = 1, TIMESTAMP = 2, FIRST = 3, LAST = 4, NEXT = 5 };
class PollingStrategy {
private:
    const PollingKind kind;
    const uint64_t value;

public:
    PollingStrategy(PollingKind kind, uint64_t value)
        : kind(kind)
        , value(value) {}
    PollingKind getKind() const { return kind; }
    uint64_t getValue() const { return value; }
};

class PollMessages : Command {
private:
    const icp::model::shared::Consumer& consumer;
    const icp::model::shared::Identifier& streamId;
    const icp::model::shared::Identifier& topicId;
    const uint32_t partitionId;
    const PollingStrategy strategy;
    const uint32_t count;
    const bool autoCommit;

public:
    PollMessages(const icp::model::shared::Consumer& consumer,
                 const icp::model::shared::Identifier& streamId,
                 const icp::model::shared::Identifier& topicId,
                 uint32_t partitionId,
                 PollingStrategy strategy,
                 uint32_t count,
                 bool autoCommit)
        : consumer(consumer)
        , streamId(streamId)
        , topicId(topicId)
        , partitionId(partitionId)
        , strategy(strategy)
        , count(count)
        , autoCommit(autoCommit) {}

    const icp::model::shared::Consumer& getConsumer() const { return consumer; }
    const icp::model::shared::Identifier& getStreamId() const { return streamId; }
    const icp::model::shared::Identifier& getTopicId() const { return topicId; }
    uint32_t getPartitionId() const { return partitionId; }
    PollingStrategy getStrategy() const { return strategy; }
    uint32_t getCount() const { return count; }
    bool getAutoCommit() const { return autoCommit; }
};

enum PartitioningKind { BALANCED = 1, PARITION_ID = 2, MESSAGES_KEY = 3 };

/**
 * @brief Layout for how to partition the messages being sent in @ref SendMessages.
 */
class Partitioning {
private:
    const PartitioningKind kind;
    const uint8_t length;
    const std::vector<unsigned char>& value;

public:
    Partitioning(PartitioningKind kind, uint8_t length, const std::vector<unsigned char>& value)
        : kind(kind)
        , length(length)
        , value(value) {}

    PartitioningKind getKind() const { return kind; }
    uint8_t getLength() const { return length; }
    const std::vector<unsigned char>& getValue() const { return value; }
};
class SendMessages : Command {};

}  // namespace message

/**
 * @brief Commands related to managing server-side managed stream consumer offsets.
 */
namespace consumeroffset {

/**
 * @brief Command to retrieve a server-side managed consumer offset.
 */
class GetConsumerOffset : Command {
private:
    const icp::model::shared::Consumer& consumer;
    const icp::model::shared::Identifier& streamId;
    const icp::model::shared::Identifier& topicId;
    const uint32_t partitionId;

public:
    GetConsumerOffset(const icp::model::shared::Consumer& consumer,
                      const icp::model::shared::Identifier& streamId,
                      const icp::model::shared::Identifier& topicId,
                      uint32_t partitionId)
        : consumer(consumer)
        , streamId(streamId)
        , topicId(topicId)
        , partitionId(partitionId) {}

    const icp::model::shared::Consumer& getConsumer() const { return consumer; }
    const icp::model::shared::Identifier& getStreamId() const { return streamId; }
    const icp::model::shared::Identifier& getTopicId() const { return topicId; }
    uint32_t getPartitionId() const { return partitionId; }
};

/**
 * @brief Command to store a server-side managed consumer offset.
 */
class StoreConsumerOffset : Command {
private:
    const icp::model::shared::Consumer& consumer;
    const icp::model::shared::Identifier& streamId;
    const icp::model::shared::Identifier& topicId;
    const uint32_t partitionId;
    const uint64_t offset;

public:
    StoreConsumerOffset(const icp::model::shared::Consumer& consumer,
                        const icp::model::shared::Identifier& streamId,
                        const icp::model::shared::Identifier& topicId,
                        uint32_t partitionId,
                        uint64_t offset)
        : consumer(consumer)
        , streamId(streamId)
        , topicId(topicId)
        , partitionId(partitionId)
        , offset(offset) {}

    const icp::model::shared::Consumer& getConsumer() const { return consumer; }
    const icp::model::shared::Identifier& getStreamId() const { return streamId; }
    const icp::model::shared::Identifier& getTopicId() const { return topicId; }
    uint32_t getPartitionId() const { return partitionId; }
    uint64_t getOffset() const { return offset; }
};

}  // namespace consumeroffset

/**
 * @brief Commands related to managing server-side managed stream consumer groups.
 */
namespace consumergroup {

/**
 * @brief Command to retrieve details on a server-side managed consumer group.
 */
class GetConsumerGroup : Command {
private:
    const icp::model::shared::Identifier& streamId;
    const icp::model::shared::Identifier& topicId;
    const uint32_t consumerGroupId;

public:
    GetConsumerGroup(const icp::model::shared::Identifier& streamId,
                     const icp::model::shared::Identifier& topicId,
                     uint32_t consumerGroupId)
        : streamId(streamId)
        , topicId(topicId)
        , consumerGroupId(consumerGroupId) {}

    const icp::model::shared::Identifier& getStreamId() const { return streamId; }
    const icp::model::shared::Identifier& getTopicId() const { return topicId; }
    uint32_t getConsumerGroupId() const { return consumerGroupId; }
};

/**
 * @brief Command to retrieve details on all server-side managed consumer groups for a given stream and topic.
 */
class GetConsumerGroups : Command {
private:
    const icp::model::shared::Identifier& streamId;
    const icp::model::shared::Identifier& topicId;

public:
    GetConsumerGroups(const icp::model::shared::Identifier& streamId, const icp::model::shared::Identifier& topicId)
        : streamId(streamId)
        , topicId(topicId) {}

    const icp::model::shared::Identifier& getStreamId() const { return streamId; }
    const icp::model::shared::Identifier& getTopicId() const { return topicId; }
};

class CreateConsumerGroup : Command {
private:
    const icp::model::shared::Identifier& streamId;
    const icp::model::shared::Identifier& topicId;
    const uint32_t consumerGroupId;

public:
    CreateConsumerGroup(const icp::model::shared::Identifier& streamId,
                        const icp::model::shared::Identifier& topicId,
                        uint32_t consumerGroupId)
        : streamId(streamId)
        , topicId(topicId)
        , consumerGroupId(consumerGroupId) {}

    const icp::model::shared::Identifier& getStreamId() const { return streamId; }
    const icp::model::shared::Identifier& getTopicId() const { return topicId; }
    uint32_t getConsumerGroupId() const { return consumerGroupId; }
};

class DeleteConsumerGroup : Command {
private:
    const icp::model::shared::Identifier& streamId;
    const icp::model::shared::Identifier& topicId;
    const uint32_t consumerGroupId;

public:
    DeleteConsumerGroup(const icp::model::shared::Identifier& streamId,
                        const icp::model::shared::Identifier& topicId,
                        uint32_t consumerGroupId)
        : streamId(streamId)
        , topicId(topicId)
        , consumerGroupId(consumerGroupId) {}

    const icp::model::shared::Identifier& getStreamId() const { return streamId; }
    const icp::model::shared::Identifier& getTopicId() const { return topicId; }
    uint32_t getConsumerGroupId() const { return consumerGroupId; }
};

class JoinConsumerGroup : Command {
private:
    const icp::model::shared::Identifier& streamId;
    const icp::model::shared::Identifier& topicId;
    const uint32_t consumerGroupId;

public:
    JoinConsumerGroup(const icp::model::shared::Identifier& streamId,
                      const icp::model::shared::Identifier& topicId,
                      uint32_t consumerGroupId)
        : streamId(streamId)
        , topicId(topicId)
        , consumerGroupId(consumerGroupId) {}

    const icp::model::shared::Identifier& getStreamId() const { return streamId; }
    const icp::model::shared::Identifier& getTopicId() const { return topicId; }
    uint32_t getConsumerGroupId() const { return consumerGroupId; }
};

class LeaveConsumerGroup : Command {
private:
    const icp::model::shared::Identifier& streamId;
    const icp::model::shared::Identifier& topicId;
    const uint32_t consumerGroupId;

public:
    LeaveConsumerGroup(const icp::model::shared::Identifier& streamId,
                       const icp::model::shared::Identifier& topicId,
                       uint32_t consumerGroupId)
        : streamId(streamId)
        , topicId(topicId)
        , consumerGroupId(consumerGroupId) {}

    const icp::model::shared::Identifier& getStreamId() const { return streamId; }
    const icp::model::shared::Identifier& getTopicId() const { return topicId; }
    uint32_t getConsumerGroupId() const { return consumerGroupId; }
};

}  // namespace consumergroup

/**
 * @brief Commands related to global system state.
 */
namespace sys {
/**
 * @brief Simple ping command to check if the server is alive.
 */
class Ping : Command {};

/**
 * @brief Command to get information about the calling client.
 */
class GetMe : Command {};

/**
 * @brief Command to get information about a specific client.
 */
class GetClient : Command {
private:
    const uint32_t clientId;

public:
    explicit GetClient(uint32_t clientId)
        : clientId(clientId) {}

    uint32_t getClientId() const { return clientId; }
};

/**
 * @brief Command to get the full list of clients on the server.
 */
class GetClients : Command {};

/**
 * @brief Command to get information about the server's performance
 */
class GetStats : Command {};
}  // namespace sys
}  // namespace command
}  // namespace icp
