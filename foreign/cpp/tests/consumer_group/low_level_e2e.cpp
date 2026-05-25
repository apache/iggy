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

// TODO(slbotbm): create fixture for setup/teardown.

#include <cstdint>
#include <string>

#include <gtest/gtest.h>

#include "lib.rs.h"
#include "tests/common/test_helpers.hpp"

class LowLevelE2E_ConsumerGroup : public E2ETestFixture {};

TEST_F(LowLevelE2E_ConsumerGroup, CreateConsumerGroupSucceeds) {
    RecordProperty("description", "Creates a consumer group successfully for an existing stream and topic.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));

    ASSERT_NO_THROW({
        const auto group = client->create_consumer_group(make_string_identifier(stream_name),
                                                         make_string_identifier(topic_name), group_name);
        ASSERT_EQ(group.name, group_name);
        ASSERT_EQ(group.members_count, 0);
        ASSERT_TRUE(group.members.empty());
    });
}

TEST_F(LowLevelE2E_ConsumerGroup, CreateConsumerGroupWithInvalidIdentifiersThrows) {
    RecordProperty("description", "Rejects malformed stream and topic identifiers before creating a consumer group.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));

    iggy::ffi::Identifier invalid_stream_id;
    invalid_stream_id.kind   = "invalid";
    invalid_stream_id.length = 4;
    invalid_stream_id.value  = {1, 2, 3, 4};

    iggy::ffi::Identifier invalid_topic_id;
    invalid_topic_id.kind   = "numeric";
    invalid_topic_id.length = 3;
    invalid_topic_id.value  = {1, 2, 3};

    const std::string invalid_stream_group_name = GetRandomName();
    const std::string invalid_topic_group_name  = GetRandomName();

    ASSERT_THROW(client->create_consumer_group(std::move(invalid_stream_id), make_string_identifier(topic_name),
                                               invalid_stream_group_name),
                 std::exception);
    ASSERT_THROW(client->create_consumer_group(make_string_identifier(stream_name), std::move(invalid_topic_id),
                                               invalid_topic_group_name),
                 std::exception);
}

TEST_F(LowLevelE2E_ConsumerGroup, CreateConsumerGroupOnNonExistentResourcesThrows) {
    RecordProperty("description", "Rejects creating a consumer group on streams or topics that do not exist.");
    const std::string stream_name         = GetRandomName();
    const std::string topic_name          = GetRandomName();
    const std::string missing_stream_name = GetRandomName();
    const std::string missing_topic_name  = GetRandomName();
    iggy::ffi::Client *client             = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));

    ASSERT_THROW(
        client->create_consumer_group(make_string_identifier(missing_stream_name), make_string_identifier(topic_name),
                                      GetRandomName()),
        std::exception);
    ASSERT_THROW(
        client->create_consumer_group(make_string_identifier(stream_name), make_string_identifier(missing_topic_name),
                                      GetRandomName()),
        std::exception);
}

TEST_F(LowLevelE2E_ConsumerGroup, CreateConsumerGroupTwiceOnSameInputThrows) {
    RecordProperty("description", "Rejects creating the same consumer group twice for the same stream and topic.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), group_name));

    ASSERT_THROW(client->create_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                               group_name),
                 std::exception);
}

TEST_F(LowLevelE2E_ConsumerGroup, CreateConsumerGroupWithNumericIdentifiersSucceeds) {
    RecordProperty("description",
                   "Creates a consumer group successfully when stream and topic are addressed by numeric identifiers.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));

    const auto stream_details = client->get_stream(make_string_identifier(stream_name));
    ASSERT_EQ(stream_details.topics.size(), 1);

    ASSERT_NO_THROW({
        const auto group =
            client->create_consumer_group(make_numeric_identifier(stream_details.id),
                                          make_numeric_identifier(stream_details.topics[0].id), group_name);
        ASSERT_EQ(group.name, group_name);
        ASSERT_EQ(group.members_count, 0);
        ASSERT_TRUE(group.members.empty());
    });
}

TEST_F(LowLevelE2E_ConsumerGroup, CreateConsumerGroupWithInvalidNamesThrows) {
    RecordProperty("description", "Rejects empty and overlong consumer group names.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));

    const std::string invalid_names[] = {"", std::string(256, 'a')};
    for (const std::string &invalid_name : invalid_names) {
        SCOPED_TRACE(invalid_name.size());
        ASSERT_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                   make_string_identifier(topic_name), invalid_name),
                     std::exception);
    }
}

TEST_F(LowLevelE2E_ConsumerGroup, CreateConsumerGroupAfterStreamDeletionThrows) {
    RecordProperty("description", "Rejects creating a consumer group after deleting the stream that owned the topic.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));
    ForgetTrackedStream(stream_name);

    ASSERT_THROW(client->create_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                               group_name),
                 std::exception);
}

TEST_F(LowLevelE2E_ConsumerGroup, CreateConsumerGroupBeforeLoginThrows) {
    RecordProperty("description",
                   "Rejects creating a consumer group before connect, and after connect but before login.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();

    iggy::ffi::Client *setup_client = GetLoggedInClient();
    ASSERT_NO_THROW(setup_client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(setup_client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                               "server_default", 0, "server_default"));

    iggy::ffi::Client *unauthenticated_client = GetLoggedOutClient();

    ASSERT_THROW(unauthenticated_client->create_consumer_group(make_string_identifier(stream_name),
                                                               make_string_identifier(topic_name), group_name),
                 std::exception);
    ASSERT_NO_THROW(unauthenticated_client->connect());
    ASSERT_THROW(unauthenticated_client->create_consumer_group(make_string_identifier(stream_name),
                                                               make_string_identifier(topic_name), group_name),
                 std::exception);
}

TEST_F(LowLevelE2E_ConsumerGroup, GetConsumerGroupReturnsSameInfoAsCreateConsumerGroup) {
    RecordProperty("description",
                   "Returns the same consumer group details from get_consumer_group as create_consumer_group.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));

    const auto created_group = client->create_consumer_group(make_string_identifier(stream_name),
                                                             make_string_identifier(topic_name), group_name);
    const auto fetched_group = client->get_consumer_group(
        make_string_identifier(stream_name), make_string_identifier(topic_name), make_string_identifier(group_name));

    ASSERT_EQ(fetched_group.id, created_group.id);
    ASSERT_EQ(fetched_group.name, created_group.name);
    ASSERT_EQ(fetched_group.partitions_count, created_group.partitions_count);
    ASSERT_EQ(fetched_group.members_count, created_group.members_count);
    ASSERT_EQ(fetched_group.members.size(), created_group.members.size());
}

TEST_F(LowLevelE2E_ConsumerGroup, GetConsumerGroupBeforeLoginThrows) {
    RecordProperty("description", "Rejects get_consumer_group before connect, and after connect but before login.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();

    iggy::ffi::Client *setup_client = GetLoggedInClient();
    ASSERT_NO_THROW(setup_client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(setup_client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                               "server_default", 0, "server_default"));
    ASSERT_NO_THROW(setup_client->create_consumer_group(make_string_identifier(stream_name),
                                                        make_string_identifier(topic_name), group_name));

    iggy::ffi::Client *unauthenticated_client = GetLoggedOutClient();

    ASSERT_THROW(unauthenticated_client->get_consumer_group(make_string_identifier(stream_name),
                                                            make_string_identifier(topic_name),
                                                            make_string_identifier(group_name)),
                 std::exception);
    ASSERT_NO_THROW(unauthenticated_client->connect());
    ASSERT_THROW(unauthenticated_client->get_consumer_group(make_string_identifier(stream_name),
                                                            make_string_identifier(topic_name),
                                                            make_string_identifier(group_name)),
                 std::exception);
}

TEST_F(LowLevelE2E_ConsumerGroup, GetConsumerGroupWithInvalidIdentifiersThrows) {
    RecordProperty("description", "Rejects get_consumer_group when the stream or topic identifier is invalid.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), group_name));

    ASSERT_THROW(client->get_consumer_group(make_string_identifier(""), make_string_identifier(topic_name),
                                            make_string_identifier(group_name)),
                 std::exception);
    ASSERT_THROW(client->get_consumer_group(make_string_identifier(stream_name), make_string_identifier(""),
                                            make_string_identifier(group_name)),
                 std::exception);
}

TEST_F(LowLevelE2E_ConsumerGroup, GetConsumerGroupOnNonExistentResourcesThrows) {
    RecordProperty("description", "Rejects get_consumer_group for streams, topics, or groups that do not exist.");
    const std::string stream_name         = GetRandomName();
    const std::string topic_name          = GetRandomName();
    const std::string created_group_name  = GetRandomName();
    const std::string missing_stream_name = GetRandomName();
    const std::string missing_topic_name  = GetRandomName();
    const std::string missing_group_name  = GetRandomName();
    iggy::ffi::Client *client             = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), created_group_name));

    ASSERT_THROW(
        client->get_consumer_group(make_string_identifier(missing_stream_name), make_string_identifier(topic_name),
                                   make_string_identifier(created_group_name)),
        std::exception);
    ASSERT_THROW(
        client->get_consumer_group(make_string_identifier(stream_name), make_string_identifier(missing_topic_name),
                                   make_string_identifier(created_group_name)),
        std::exception);
    ASSERT_THROW(client->get_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                            make_string_identifier(missing_group_name)),
                 std::exception);
}

TEST_F(LowLevelE2E_ConsumerGroup, GetConsumerGroupAfterStreamDeletionThrows) {
    RecordProperty("description", "Rejects get_consumer_group after deleting the stream that owned the group.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), group_name));
    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));
    ForgetTrackedStream(stream_name);

    ASSERT_THROW(client->get_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                            make_string_identifier(group_name)),
                 std::exception);
}

TEST_F(LowLevelE2E_ConsumerGroup, DeleteConsumerGroupSucceeds) {
    RecordProperty("description", "Deletes an existing consumer group successfully.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), group_name));

    ASSERT_NO_THROW(client->delete_consumer_group(
        make_string_identifier(stream_name), make_string_identifier(topic_name), make_string_identifier(group_name)));

    ASSERT_THROW(client->get_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                            make_string_identifier(group_name)),
                 std::exception);
}

TEST_F(LowLevelE2E_ConsumerGroup, DeleteConsumerGroupBeforeLoginThrows) {
    RecordProperty("description", "Rejects delete_consumer_group before connect, and after connect but before login.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();

    iggy::ffi::Client *setup_client = GetLoggedInClient();
    ASSERT_NO_THROW(setup_client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(setup_client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                               "server_default", 0, "server_default"));
    ASSERT_NO_THROW(setup_client->create_consumer_group(make_string_identifier(stream_name),
                                                        make_string_identifier(topic_name), group_name));

    iggy::ffi::Client *unauthenticated_client = GetLoggedOutClient();

    ASSERT_THROW(unauthenticated_client->delete_consumer_group(make_string_identifier(stream_name),
                                                               make_string_identifier(topic_name),
                                                               make_string_identifier(group_name)),
                 std::exception);
    ASSERT_NO_THROW(unauthenticated_client->connect());
    ASSERT_THROW(unauthenticated_client->delete_consumer_group(make_string_identifier(stream_name),
                                                               make_string_identifier(topic_name),
                                                               make_string_identifier(group_name)),
                 std::exception);
}

TEST_F(LowLevelE2E_ConsumerGroup, DeleteConsumerGroupWithInvalidIdentifiersThrows) {
    RecordProperty("description",
                   "Rejects delete_consumer_group when the stream, topic, or group identifier is invalid.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), group_name));

    iggy::ffi::Identifier invalid_group_id;
    invalid_group_id.kind   = "numeric";
    invalid_group_id.length = 3;
    invalid_group_id.value  = {1, 2, 3};

    ASSERT_THROW(client->delete_consumer_group(make_string_identifier(""), make_string_identifier(topic_name),
                                               make_string_identifier(group_name)),
                 std::exception);
    ASSERT_THROW(client->delete_consumer_group(make_string_identifier(stream_name), make_string_identifier(""),
                                               make_string_identifier(group_name)),
                 std::exception);
    ASSERT_THROW(client->delete_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                               std::move(invalid_group_id)),
                 std::exception);
}

TEST_F(LowLevelE2E_ConsumerGroup, DeleteConsumerGroupOnNonExistentResourcesThrows) {
    RecordProperty("description", "Rejects delete_consumer_group for streams, topics, or groups that do not exist.");
    const std::string stream_name         = GetRandomName();
    const std::string topic_name          = GetRandomName();
    const std::string created_group_name  = GetRandomName();
    const std::string missing_stream_name = GetRandomName();
    const std::string missing_topic_name  = GetRandomName();
    const std::string missing_group_name  = GetRandomName();
    iggy::ffi::Client *client             = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), created_group_name));

    ASSERT_THROW(
        client->delete_consumer_group(make_string_identifier(missing_stream_name), make_string_identifier(topic_name),
                                      make_string_identifier(created_group_name)),
        std::exception);
    ASSERT_THROW(
        client->delete_consumer_group(make_string_identifier(stream_name), make_string_identifier(missing_topic_name),
                                      make_string_identifier(created_group_name)),
        std::exception);
    ASSERT_THROW(client->delete_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                               make_string_identifier(missing_group_name)),
                 std::exception);
}

TEST_F(LowLevelE2E_ConsumerGroup, DeleteConsumerGroupTwiceThrows) {
    RecordProperty("description", "Rejects deleting the same consumer group twice.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), group_name));
    ASSERT_NO_THROW(client->delete_consumer_group(
        make_string_identifier(stream_name), make_string_identifier(topic_name), make_string_identifier(group_name)));

    ASSERT_THROW(client->delete_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                               make_string_identifier(group_name)),
                 std::exception);
}

TEST_F(LowLevelE2E_ConsumerGroup, DeleteConsumerGroupWithNumericIdentifiersSucceeds) {
    RecordProperty(
        "description",
        "Deletes a consumer group successfully when stream, topic, and group are addressed by numeric identifiers.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    const auto created_group  = client->create_consumer_group(make_string_identifier(stream_name),
                                                              make_string_identifier(topic_name), group_name);
    const auto stream_details = client->get_stream(make_string_identifier(stream_name));

    ASSERT_NO_THROW(client->delete_consumer_group(make_numeric_identifier(stream_details.id),
                                                  make_numeric_identifier(stream_details.topics[0].id),
                                                  make_numeric_identifier(created_group.id)));

    ASSERT_THROW(client->get_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                            make_string_identifier(group_name)),
                 std::exception);
}

TEST_F(LowLevelE2E_ConsumerGroup, DeleteConsumerGroupAfterStreamDeletionThrows) {
    RecordProperty("description",
                   "Rejects delete_consumer_group after deleting the stream that owned the consumer group.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), group_name));
    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));
    ForgetTrackedStream(stream_name);

    ASSERT_THROW(client->delete_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                               make_string_identifier(group_name)),
                 std::exception);
}

TEST_F(LowLevelE2E_ConsumerGroup, DeleteConsumerGroupAndRecreateWithSameNameSucceeds) {
    RecordProperty("description",
                   "Allows recreating a consumer group with the same name after the previous group is deleted.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));

    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), group_name));
    ASSERT_NO_THROW(client->delete_consumer_group(
        make_string_identifier(stream_name), make_string_identifier(topic_name), make_string_identifier(group_name)));

    ASSERT_NO_THROW({
        const auto recreated_group = client->create_consumer_group(make_string_identifier(stream_name),
                                                                   make_string_identifier(topic_name), group_name);
        ASSERT_EQ(recreated_group.id, 0u);
        ASSERT_EQ(recreated_group.name, group_name);
        ASSERT_EQ(recreated_group.members_count, 0);
        ASSERT_TRUE(recreated_group.members.empty());
    });
}
