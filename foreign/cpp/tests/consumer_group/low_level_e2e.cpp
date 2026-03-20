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

#include <string>

#include <gtest/gtest.h>

#include "lib.rs.h"
#include "tests/common/test_helpers.hpp"

TEST(LowLevelE2E_ConsumerGroup, CreateConsumerGroupHappyPath) {
    RecordProperty("description", "Creates a consumer group successfully for an existing stream and topic.");
    const std::string stream_name = "client-create-group-happy-stream";
    const std::string topic_name  = "client-create-group-happy-topic";
    const std::string group_name  = "client-create-group-happy";
    iggy::ffi::Client *client     = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));

    ASSERT_NO_THROW({
        const auto group = client->create_consumer_group(make_string_identifier(stream_name),
                                                         make_string_identifier(topic_name), group_name);
        ASSERT_EQ(group.name, group_name);
        ASSERT_EQ(group.members_count, 0);
        ASSERT_TRUE(group.members.empty());
    });

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, CreateConsumerGroupWithInvalidStreamIdentifierThrows) {
    RecordProperty("description", "Rejects malformed stream identifiers before creating a consumer group.");
    const std::string stream_name = "client-create-group-invalid-stream-id-stream";
    const std::string topic_name  = "client-create-group-invalid-stream-id-topic";
    iggy::ffi::Client *client     = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));

    iggy::ffi::Identifier invalid_stream_id;
    invalid_stream_id.kind   = "invalid";
    invalid_stream_id.length = 4;
    invalid_stream_id.value  = {1, 2, 3, 4};

    ASSERT_THROW(client->create_consumer_group(std::move(invalid_stream_id), make_string_identifier(topic_name),
                                               "client-create-group-invalid-stream-id"),
                 std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, CreateConsumerGroupWithInvalidTopicIdentifierThrows) {
    RecordProperty("description", "Rejects malformed topic identifiers before creating a consumer group.");
    const std::string stream_name = "client-create-group-invalid-topic-id-stream";
    const std::string topic_name  = "client-create-group-invalid-topic-id-topic";
    iggy::ffi::Client *client     = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));

    iggy::ffi::Identifier invalid_topic_id;
    invalid_topic_id.kind   = "numeric";
    invalid_topic_id.length = 3;
    invalid_topic_id.value  = {1, 2, 3};

    ASSERT_THROW(client->create_consumer_group(make_string_identifier(stream_name), std::move(invalid_topic_id),
                                               "client-create-group-invalid-topic-id"),
                 std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, CreateConsumerGroupOnNonExistentStreamThrows) {
    RecordProperty("description", "Rejects creating a consumer group on a stream that does not exist.");
    const std::string missing_stream_name = "client-create-group-missing-stream";
    const std::string topic_name          = "client-create-group-missing-stream-topic";
    iggy::ffi::Client *client             = login_to_server();

    ASSERT_THROW(
        client->create_consumer_group(make_string_identifier(missing_stream_name), make_string_identifier(topic_name),
                                      "client-create-group-missing-stream-group"),
        std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, CreateConsumerGroupOnNonExistentTopicThrows) {
    RecordProperty("description", "Rejects creating a consumer group on a topic that does not exist.");
    const std::string stream_name        = "client-create-group-missing-topic-stream";
    const std::string missing_topic_name = "client-create-group-missing-topic";
    iggy::ffi::Client *client            = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));

    ASSERT_THROW(
        client->create_consumer_group(make_string_identifier(stream_name), make_string_identifier(missing_topic_name),
                                      "client-create-group-missing-topic-group"),
        std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, CreateConsumerGroupTwiceOnSameInputThrows) {
    RecordProperty("description", "Rejects creating the same consumer group twice for the same stream and topic.");
    const std::string stream_name = "client-create-group-duplicate-stream";
    const std::string topic_name  = "client-create-group-duplicate-topic";
    const std::string group_name  = "client-create-group-duplicate";
    iggy::ffi::Client *client     = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), group_name));

    ASSERT_THROW(client->create_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                               group_name),
                 std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, CreateConsumerGroupWithNumericIdentifiersSucceeds) {
    RecordProperty("description",
                   "Creates a consumer group successfully when stream and topic are addressed by numeric identifiers.");
    const std::string stream_name = "client-create-group-numeric-stream";
    const std::string topic_name  = "client-create-group-numeric-topic";
    const std::string group_name  = "client-create-group-numeric";
    iggy::ffi::Client *client     = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
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

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, CreateConsumerGroupWithInvalidNamesThrows) {
    RecordProperty("description", "Rejects empty and overlong consumer group names.");
    const std::string stream_name = "client-create-group-invalid-name-stream";
    const std::string topic_name  = "client-create-group-invalid-name-topic";
    iggy::ffi::Client *client     = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));

    const std::string invalid_names[] = {"", std::string(256, 'a')};
    for (const std::string &invalid_name : invalid_names) {
        SCOPED_TRACE(invalid_name.size());
        ASSERT_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                   make_string_identifier(topic_name), invalid_name),
                     std::exception);
    }

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, CreateConsumerGroupAfterStreamDeletionThrows) {
    RecordProperty("description", "Rejects creating a consumer group after deleting the stream that owned the topic.");
    const std::string stream_name = "client-create-group-after-stream-delete-stream";
    const std::string topic_name  = "client-create-group-after-stream-delete-topic";
    const std::string group_name  = "client-create-group-after-stream-delete";
    iggy::ffi::Client *client     = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));

    ASSERT_THROW(client->create_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                               group_name),
                 std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, GetConsumerGroupReturnsSameInfoAsCreateConsumerGroup) {
    RecordProperty("description",
                   "Returns the same consumer group details from get_consumer_group as create_consumer_group.");
    const std::string stream_name = "client-get-group-happy-stream";
    const std::string topic_name  = "client-get-group-happy-topic";
    const std::string group_name  = "client-get-group-happy";
    iggy::ffi::Client *client     = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
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

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, GetConsumerGroupWithInvalidStreamNameThrows) {
    RecordProperty("description", "Rejects get_consumer_group when the stream name identifier is invalid.");
    const std::string stream_name = "client-get-group-invalid-stream-name-stream";
    const std::string topic_name  = "client-get-group-invalid-stream-name-topic";
    const std::string group_name  = "client-get-group-invalid-stream-name-group";
    iggy::ffi::Client *client     = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), group_name));

    ASSERT_THROW(client->get_consumer_group(make_string_identifier(""), make_string_identifier(topic_name),
                                            make_string_identifier(group_name)),
                 std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, GetConsumerGroupWithInvalidTopicNameThrows) {
    RecordProperty("description", "Rejects get_consumer_group when the topic name identifier is invalid.");
    const std::string stream_name = "client-get-group-invalid-topic-name-stream";
    const std::string topic_name  = "client-get-group-invalid-topic-name-topic";
    const std::string group_name  = "client-get-group-invalid-topic-name-group";
    iggy::ffi::Client *client     = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), group_name));

    ASSERT_THROW(client->get_consumer_group(make_string_identifier(stream_name), make_string_identifier(""),
                                            make_string_identifier(group_name)),
                 std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, GetConsumerGroupOnNonExistentStreamThrows) {
    RecordProperty("description", "Rejects get_consumer_group on a stream that does not exist.");
    const std::string stream_name         = "client-get-group-missing-stream";
    const std::string topic_name          = "client-get-group-missing-stream-topic";
    const std::string group_name          = "client-get-group-missing-stream-group";
    const std::string missing_stream_name = "client-get-group-missing-stream-name";
    iggy::ffi::Client *client             = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), group_name));

    ASSERT_THROW(client->get_consumer_group(make_string_identifier(missing_stream_name),
                                            make_string_identifier(topic_name), make_string_identifier(group_name)),
                 std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, GetConsumerGroupOnNonExistentTopicThrows) {
    RecordProperty("description", "Rejects get_consumer_group on a topic that does not exist.");
    const std::string stream_name        = "client-get-group-missing-topic-stream";
    const std::string topic_name         = "client-get-group-missing-topic-topic";
    const std::string group_name         = "client-get-group-missing-topic-group";
    const std::string missing_topic_name = "client-get-group-missing-topic-name";
    iggy::ffi::Client *client            = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), group_name));

    ASSERT_THROW(
        client->get_consumer_group(make_string_identifier(stream_name), make_string_identifier(missing_topic_name),
                                   make_string_identifier(group_name)),
        std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, GetConsumerGroupOnNonExistentGroupThrows) {
    RecordProperty("description", "Rejects get_consumer_group when the consumer group does not exist.");
    const std::string stream_name        = "client-get-group-missing-group-stream";
    const std::string topic_name         = "client-get-group-missing-group-topic";
    const std::string created_group_name = "client-get-group-existing-group";
    const std::string missing_group_name = "client-get-group-missing-group";
    iggy::ffi::Client *client            = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), created_group_name));

    ASSERT_THROW(client->get_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                            make_string_identifier(missing_group_name)),
                 std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, GetConsumerGroupAfterStreamDeletionThrows) {
    RecordProperty("description", "Rejects get_consumer_group after deleting the stream that owned the group.");
    const std::string stream_name = "client-get-group-after-stream-delete-stream";
    const std::string topic_name  = "client-get-group-after-stream-delete-topic";
    const std::string group_name  = "client-get-group-after-stream-delete";
    iggy::ffi::Client *client     = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), group_name));
    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));

    ASSERT_THROW(client->get_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                            make_string_identifier(group_name)),
                 std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}
