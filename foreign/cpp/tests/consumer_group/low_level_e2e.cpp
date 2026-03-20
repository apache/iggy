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
#include <iostream>

#include "lib.rs.h"
#include "tests/common/test_helpers.hpp"

TEST(LowLevelE2E_ConsumerGroup, CreateConsumerGroupSucceeds) {
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

TEST(LowLevelE2E_ConsumerGroup, CreateConsumerGroupWithInvalidIdentifiersThrows) {
    RecordProperty("description", "Rejects malformed stream and topic identifiers before creating a consumer group.");
    const std::string stream_name = "client-create-group-invalid-id-stream";
    const std::string topic_name  = "client-create-group-invalid-id-topic";
    iggy::ffi::Client *client     = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
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

    ASSERT_THROW(client->create_consumer_group(std::move(invalid_stream_id), make_string_identifier(topic_name),
                                               "client-create-group-invalid-stream-id"),
                 std::exception);
    ASSERT_THROW(client->create_consumer_group(make_string_identifier(stream_name), std::move(invalid_topic_id),
                                               "client-create-group-invalid-topic-id"),
                 std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, CreateConsumerGroupOnNonExistentResourcesThrows) {
    RecordProperty("description", "Rejects creating a consumer group on streams or topics that do not exist.");
    const std::string stream_name         = "client-create-group-missing-resource-stream";
    const std::string topic_name          = "client-create-group-missing-resource-topic";
    const std::string missing_stream_name = "client-create-group-missing-stream";
    const std::string missing_topic_name  = "client-create-group-missing-topic";
    iggy::ffi::Client *client             = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));

    ASSERT_THROW(
        client->create_consumer_group(make_string_identifier(missing_stream_name), make_string_identifier(topic_name),
                                      "client-create-group-missing-stream-group"),
        std::exception);
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

TEST(LowLevelE2E_ConsumerGroup, GetConsumerGroupWithInvalidIdentifiersThrows) {
    RecordProperty("description", "Rejects get_consumer_group when the stream or topic identifier is invalid.");
    const std::string stream_name = "client-get-group-invalid-id-stream";
    const std::string topic_name  = "client-get-group-invalid-id-topic";
    const std::string group_name  = "client-get-group-invalid-id-group";
    iggy::ffi::Client *client     = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
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

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, GetConsumerGroupOnNonExistentResourcesThrows) {
    RecordProperty("description", "Rejects get_consumer_group for streams, topics, or groups that do not exist.");
    const std::string stream_name         = "client-get-group-missing-resource-stream";
    const std::string topic_name          = "client-get-group-missing-resource-topic";
    const std::string created_group_name  = "client-get-group-existing-group";
    const std::string missing_stream_name = "client-get-group-missing-stream";
    const std::string missing_topic_name  = "client-get-group-missing-topic";
    const std::string missing_group_name  = "client-get-group-missing-group";
    iggy::ffi::Client *client             = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
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

TEST(LowLevelE2E_ConsumerGroup, JoinConsumerGroupSucceeds) {
    RecordProperty("description", "Joins an existing consumer group successfully.");
    const std::string stream_name = "client-join-group-happy-stream";
    const std::string topic_name  = "client-join-group-happy-topic";
    const std::string group_name  = "client-join-group-happy";
    iggy::ffi::Client *client     = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), group_name));

    ASSERT_NO_THROW(client->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                                make_string_identifier(group_name)));

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, JoinConsumerGroupWithInvalidIdentifiersThrows) {
    RecordProperty("description", "Rejects join_consumer_group when the stream or topic identifier is invalid.");
    const std::string stream_name = "client-join-group-invalid-id-stream";
    const std::string topic_name  = "client-join-group-invalid-id-topic";
    const std::string group_name  = "client-join-group-invalid-id-group";
    iggy::ffi::Client *client     = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), group_name));

    ASSERT_THROW(client->join_consumer_group(make_string_identifier(""), make_string_identifier(topic_name),
                                             make_string_identifier(group_name)),
                 std::exception);
    ASSERT_THROW(client->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(""),
                                             make_string_identifier(group_name)),
                 std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, JoinConsumerGroupOnNonExistentResourcesThrows) {
    RecordProperty("description", "Rejects join_consumer_group for streams, topics, or groups that do not exist.");
    const std::string stream_name         = "client-join-group-missing-resource-stream";
    const std::string topic_name          = "client-join-group-missing-resource-topic";
    const std::string created_group_name  = "client-join-group-existing-group";
    const std::string missing_stream_name = "client-join-group-missing-stream";
    const std::string missing_topic_name  = "client-join-group-missing-topic";
    const std::string missing_group_name  = "client-join-group-missing-group";
    iggy::ffi::Client *client             = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), created_group_name));

    ASSERT_THROW(
        client->join_consumer_group(make_string_identifier(missing_stream_name), make_string_identifier(topic_name),
                                    make_string_identifier(created_group_name)),
        std::exception);
    ASSERT_THROW(
        client->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(missing_topic_name),
                                    make_string_identifier(created_group_name)),
        std::exception);
    ASSERT_THROW(client->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                             make_string_identifier(missing_group_name)),
                 std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, JoinConsumerGroupTwiceWithSameDetailsIsIdempotent) {
    RecordProperty("description", "Keeps membership stable when the same client joins the same consumer group twice.");
    const std::string stream_name = "client-join-group-duplicate-stream";
    const std::string topic_name  = "client-join-group-duplicate-topic";
    const std::string group_name  = "client-join-group-duplicate";
    iggy::ffi::Client *client     = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), group_name));

    ASSERT_NO_THROW(client->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                                make_string_identifier(group_name)));
    ASSERT_NO_THROW(client->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                                make_string_identifier(group_name)));

    const auto group = client->get_consumer_group(
        make_string_identifier(stream_name), make_string_identifier(topic_name), make_string_identifier(group_name));
    ASSERT_EQ(group.members_count, 1);
    ASSERT_EQ(group.members.size(), 1);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, JoinConsumerGroupAndThenGetConsumerGroupShowsMembership) {
    RecordProperty("description", "Reflects the joined client in get_consumer_group after joining.");
    const std::string stream_name = "client-join-group-then-get-stream";
    const std::string topic_name  = "client-join-group-then-get-topic";
    const std::string group_name  = "client-join-group-then-get";
    iggy::ffi::Client *client     = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), group_name));
    ASSERT_NO_THROW(client->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                                make_string_identifier(group_name)));

    const auto group = client->get_consumer_group(
        make_string_identifier(stream_name), make_string_identifier(topic_name), make_string_identifier(group_name));
    ASSERT_EQ(group.members_count, 1);
    ASSERT_EQ(group.members.size(), 1);
    ASSERT_EQ(group.members[0].partitions_count, 1);
    ASSERT_EQ(group.members[0].partitions.size(), 1);
    ASSERT_EQ(group.members[0].partitions[0], 0);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, JoinConsumerGroupWithNumericIdentifiersSucceeds) {
    RecordProperty(
        "description",
        "Joins a consumer group successfully when stream, topic, and group are addressed by numeric identifiers.");
    const std::string stream_name = "client-join-group-numeric-stream";
    const std::string topic_name  = "client-join-group-numeric-topic";
    const std::string group_name  = "client-join-group-numeric";
    iggy::ffi::Client *client     = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    const auto created_group  = client->create_consumer_group(make_string_identifier(stream_name),
                                                              make_string_identifier(topic_name), group_name);
    const auto stream_details = client->get_stream(make_string_identifier(stream_name));

    ASSERT_NO_THROW(client->join_consumer_group(make_numeric_identifier(stream_details.id),
                                                make_numeric_identifier(stream_details.topics[0].id),
                                                make_numeric_identifier(created_group.id)));

    const auto group = client->get_consumer_group(
        make_string_identifier(stream_name), make_string_identifier(topic_name), make_string_identifier(group_name));
    ASSERT_EQ(group.members_count, 1);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, JoinConsumerGroupAfterStreamDeletionThrows) {
    RecordProperty("description",
                   "Rejects join_consumer_group after deleting the stream that owned the consumer group.");
    const std::string stream_name = "client-join-group-after-stream-delete-stream";
    const std::string topic_name  = "client-join-group-after-stream-delete-topic";
    const std::string group_name  = "client-join-group-after-stream-delete";
    iggy::ffi::Client *client     = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), group_name));
    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));

    ASSERT_THROW(client->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                             make_string_identifier(group_name)),
                 std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, TwoDifferentClientsCanJoinSameConsumerGroup) {
    RecordProperty("description",
                   "Tracks two different connected clients as distinct members of the same consumer group.");
    const std::string stream_name = "client-join-group-two-clients-stream";
    const std::string topic_name  = "client-join-group-two-clients-topic";
    const std::string group_name  = "client-join-group-two-clients";
    iggy::ffi::Client *first      = login_to_server();
    iggy::ffi::Client *second     = login_to_server();

    ASSERT_NO_THROW(first->create_stream(stream_name));
    ASSERT_NO_THROW(first->create_topic(make_string_identifier(stream_name), topic_name, 2, "none", 0, "server_default",
                                        0, "server_default"));
    ASSERT_NO_THROW(first->create_consumer_group(make_string_identifier(stream_name),
                                                 make_string_identifier(topic_name), group_name));

    ASSERT_NO_THROW(first->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                               make_string_identifier(group_name)));
    ASSERT_NO_THROW(second->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                                make_string_identifier(group_name)));

    const auto group = first->get_consumer_group(
        make_string_identifier(stream_name), make_string_identifier(topic_name), make_string_identifier(group_name));
    ASSERT_EQ(group.members_count, 2);
    ASSERT_EQ(group.members.size(), 2);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(second));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(first));
}

TEST(LowLevelE2E_ConsumerGroup, JoinConsumerGroupAssignsPartitionsAcrossConsumers) {
    RecordProperty("description", "Assigns partitions cooperatively across four joined consumers.");
    const std::string stream_name = "client-join-group-assignment-stream";
    const std::string topic_name  = "client-join-group-assignment-topic";
    const std::string group_name  = "client-join-group-assignment";
    iggy::ffi::Client *first      = login_to_server();
    iggy::ffi::Client *second     = login_to_server();
    iggy::ffi::Client *third      = login_to_server();
    iggy::ffi::Client *fourth     = login_to_server();

    ASSERT_NO_THROW(first->create_stream(stream_name));
    ASSERT_NO_THROW(first->create_topic(make_string_identifier(stream_name), topic_name, 8, "none", 0, "server_default",
                                        0, "server_default"));
    ASSERT_NO_THROW(first->create_consumer_group(make_string_identifier(stream_name),
                                                 make_string_identifier(topic_name), group_name));
    ASSERT_NO_THROW(first->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                               make_string_identifier(group_name)));
    ASSERT_NO_THROW(second->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                                make_string_identifier(group_name)));
    ASSERT_NO_THROW(third->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                               make_string_identifier(group_name)));
    ASSERT_NO_THROW(fourth->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                                make_string_identifier(group_name)));

    const auto group = first->get_consumer_group(
        make_string_identifier(stream_name), make_string_identifier(topic_name), make_string_identifier(group_name));

    ASSERT_EQ(group.members_count, 4);
    ASSERT_EQ(group.members.size(), 4);
    ASSERT_EQ(group.partitions_count, 8);

    ASSERT_EQ(group.members[0].partitions_count, 5);
    ASSERT_EQ(group.members[0].partitions.size(), 5);
    ASSERT_EQ(group.members[0].partitions[0], 0);
    ASSERT_EQ(group.members[0].partitions[1], 1);
    ASSERT_EQ(group.members[0].partitions[2], 2);
    ASSERT_EQ(group.members[0].partitions[3], 3);
    ASSERT_EQ(group.members[0].partitions[4], 4);

    ASSERT_EQ(group.members[1].partitions_count, 1);
    ASSERT_EQ(group.members[1].partitions.size(), 1);
    ASSERT_EQ(group.members[1].partitions[0], 7);

    ASSERT_EQ(group.members[2].partitions_count, 1);
    ASSERT_EQ(group.members[2].partitions.size(), 1);
    ASSERT_EQ(group.members[2].partitions[0], 6);

    ASSERT_EQ(group.members[3].partitions_count, 1);
    ASSERT_EQ(group.members[3].partitions.size(), 1);
    ASSERT_EQ(group.members[3].partitions[0], 5);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(fourth));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(third));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(second));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(first));
}

TEST(LowLevelE2E_ConsumerGroup, LeaveConsumerGroupSucceeds) {
    RecordProperty("description", "Leaves an existing consumer group successfully.");
    const std::string stream_name = "client-leave-group-happy-stream";
    const std::string topic_name  = "client-leave-group-happy-topic";
    const std::string group_name  = "client-leave-group-happy";
    iggy::ffi::Client *client     = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), group_name));
    ASSERT_NO_THROW(client->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                                make_string_identifier(group_name)));

    ASSERT_NO_THROW(client->leave_consumer_group(
        make_string_identifier(stream_name), make_string_identifier(topic_name), make_string_identifier(group_name)));

    const auto group = client->get_consumer_group(
        make_string_identifier(stream_name), make_string_identifier(topic_name), make_string_identifier(group_name));
    ASSERT_EQ(group.members_count, 0);
    ASSERT_TRUE(group.members.empty());

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, LeaveConsumerGroupWithInvalidIdentifiersThrows) {
    RecordProperty("description",
                   "Rejects leave_consumer_group when the stream, topic, or group identifier is invalid.");
    const std::string stream_name = "client-leave-group-invalid-id-stream";
    const std::string topic_name  = "client-leave-group-invalid-id-topic";
    const std::string group_name  = "client-leave-group-invalid-id-group";
    iggy::ffi::Client *client     = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), group_name));
    ASSERT_NO_THROW(client->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                                make_string_identifier(group_name)));

    iggy::ffi::Identifier invalid_group_id;
    invalid_group_id.kind   = "numeric";
    invalid_group_id.length = 3;
    invalid_group_id.value  = {1, 2, 3};

    ASSERT_THROW(client->leave_consumer_group(make_string_identifier(""), make_string_identifier(topic_name),
                                              make_string_identifier(group_name)),
                 std::exception);
    ASSERT_THROW(client->leave_consumer_group(make_string_identifier(stream_name), make_string_identifier(""),
                                              make_string_identifier(group_name)),
                 std::exception);
    ASSERT_THROW(client->leave_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                              std::move(invalid_group_id)),
                 std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, LeaveConsumerGroupOnNonExistentResourcesThrows) {
    RecordProperty("description", "Rejects leave_consumer_group for streams, topics, or groups that do not exist.");
    const std::string stream_name         = "client-leave-group-missing-resource-stream";
    const std::string topic_name          = "client-leave-group-missing-resource-topic";
    const std::string created_group_name  = "client-leave-group-existing-group";
    const std::string missing_stream_name = "client-leave-group-missing-stream";
    const std::string missing_topic_name  = "client-leave-group-missing-topic";
    const std::string missing_group_name  = "client-leave-group-missing-group";
    iggy::ffi::Client *client             = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), created_group_name));
    ASSERT_NO_THROW(client->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                                make_string_identifier(created_group_name)));

    ASSERT_THROW(
        client->leave_consumer_group(make_string_identifier(missing_stream_name), make_string_identifier(topic_name),
                                     make_string_identifier(created_group_name)),
        std::exception);
    ASSERT_THROW(
        client->leave_consumer_group(make_string_identifier(stream_name), make_string_identifier(missing_topic_name),
                                     make_string_identifier(created_group_name)),
        std::exception);
    ASSERT_THROW(client->leave_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                              make_string_identifier(missing_group_name)),
                 std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, LeaveConsumerGroupWithoutJoiningThrows) {
    RecordProperty("description",
                   "Rejects leave_consumer_group when the client is not a member of the consumer group.");
    const std::string stream_name = "client-leave-group-without-joining-stream";
    const std::string topic_name  = "client-leave-group-without-joining-topic";
    const std::string group_name  = "client-leave-group-without-joining";
    iggy::ffi::Client *client     = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), group_name));

    ASSERT_THROW(client->leave_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                              make_string_identifier(group_name)),
                 std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, LeaveConsumerGroupTwiceWithSameDetailsThrows) {
    RecordProperty("description",
                   "Rejects leave_consumer_group when the same client leaves the same consumer group twice.");
    const std::string stream_name = "client-leave-group-twice-stream";
    const std::string topic_name  = "client-leave-group-twice-topic";
    const std::string group_name  = "client-leave-group-twice";
    iggy::ffi::Client *client     = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), group_name));
    ASSERT_NO_THROW(client->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                                make_string_identifier(group_name)));
    ASSERT_NO_THROW(client->leave_consumer_group(
        make_string_identifier(stream_name), make_string_identifier(topic_name), make_string_identifier(group_name)));

    ASSERT_THROW(client->leave_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                              make_string_identifier(group_name)),
                 std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, LeaveConsumerGroupWithNumericIdentifiersSucceeds) {
    RecordProperty(
        "description",
        "Leaves a consumer group successfully when stream, topic, and group are addressed by numeric identifiers.");
    const std::string stream_name = "client-leave-group-numeric-stream";
    const std::string topic_name  = "client-leave-group-numeric-topic";
    const std::string group_name  = "client-leave-group-numeric";
    iggy::ffi::Client *client     = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    const auto created_group  = client->create_consumer_group(make_string_identifier(stream_name),
                                                              make_string_identifier(topic_name), group_name);
    const auto stream_details = client->get_stream(make_string_identifier(stream_name));
    ASSERT_NO_THROW(client->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                                make_string_identifier(group_name)));

    ASSERT_NO_THROW(client->leave_consumer_group(make_numeric_identifier(stream_details.id),
                                                 make_numeric_identifier(stream_details.topics[0].id),
                                                 make_numeric_identifier(created_group.id)));

    const auto group = client->get_consumer_group(
        make_string_identifier(stream_name), make_string_identifier(topic_name), make_string_identifier(group_name));
    ASSERT_EQ(group.members_count, 0);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, LeaveConsumerGroupAfterStreamDeletionThrows) {
    RecordProperty("description",
                   "Rejects leave_consumer_group after deleting the stream that owned the consumer group.");
    const std::string stream_name = "client-leave-group-after-stream-delete-stream";
    const std::string topic_name  = "client-leave-group-after-stream-delete-topic";
    const std::string group_name  = "client-leave-group-after-stream-delete";
    iggy::ffi::Client *client     = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), group_name));
    ASSERT_NO_THROW(client->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                                make_string_identifier(group_name)));
    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));

    ASSERT_THROW(client->leave_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                              make_string_identifier(group_name)),
                 std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, LeaveConsumerGroupRemovesOnlyThatClient) {
    RecordProperty("description",
                   "Removes only the leaving client and keeps the remaining consumer group membership intact.");
    const std::string stream_name = "client-leave-group-remove-one-stream";
    const std::string topic_name  = "client-leave-group-remove-one-topic";
    const std::string group_name  = "client-leave-group-remove-one";
    iggy::ffi::Client *first      = login_to_server();
    iggy::ffi::Client *second     = login_to_server();

    ASSERT_NO_THROW(first->create_stream(stream_name));
    ASSERT_NO_THROW(first->create_topic(make_string_identifier(stream_name), topic_name, 2, "none", 0, "server_default",
                                        0, "server_default"));
    ASSERT_NO_THROW(first->create_consumer_group(make_string_identifier(stream_name),
                                                 make_string_identifier(topic_name), group_name));
    ASSERT_NO_THROW(first->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                               make_string_identifier(group_name)));
    ASSERT_NO_THROW(second->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                                make_string_identifier(group_name)));

    ASSERT_NO_THROW(first->leave_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                                make_string_identifier(group_name)));

    const auto group = second->get_consumer_group(
        make_string_identifier(stream_name), make_string_identifier(topic_name), make_string_identifier(group_name));
    ASSERT_EQ(group.members_count, 1);
    ASSERT_EQ(group.members.size(), 1);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(second));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(first));
}

TEST(LowLevelE2E_ConsumerGroup, LeaveConsumerGroupRebalancesPartitionsAcrossRemainingConsumers) {
    RecordProperty("description",
                   "Rebalances partitions across the remaining consumers after one member leaves the group.");
    const std::string stream_name = "client-leave-group-rebalance-stream";
    const std::string topic_name  = "client-leave-group-rebalance-topic";
    const std::string group_name  = "client-leave-group-rebalance";
    iggy::ffi::Client *first      = login_to_server();
    iggy::ffi::Client *second     = login_to_server();
    iggy::ffi::Client *third      = login_to_server();
    iggy::ffi::Client *fourth     = login_to_server();

    ASSERT_NO_THROW(first->create_stream(stream_name));
    ASSERT_NO_THROW(first->create_topic(make_string_identifier(stream_name), topic_name, 8, "none", 0, "server_default",
                                        0, "server_default"));
    ASSERT_NO_THROW(first->create_consumer_group(make_string_identifier(stream_name),
                                                 make_string_identifier(topic_name), group_name));
    ASSERT_NO_THROW(first->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                               make_string_identifier(group_name)));
    ASSERT_NO_THROW(second->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                                make_string_identifier(group_name)));
    ASSERT_NO_THROW(third->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                               make_string_identifier(group_name)));
    ASSERT_NO_THROW(fourth->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                                make_string_identifier(group_name)));

    ASSERT_NO_THROW(fourth->leave_consumer_group(
        make_string_identifier(stream_name), make_string_identifier(topic_name), make_string_identifier(group_name)));

    const auto group = first->get_consumer_group(
        make_string_identifier(stream_name), make_string_identifier(topic_name), make_string_identifier(group_name));

    ASSERT_EQ(group.members_count, 3);
    ASSERT_EQ(group.members.size(), 3);
    ASSERT_EQ(group.partitions_count, 8);

    bool seen_partitions[8] = {false, false, false, false, false, false, false, false};
    size_t assigned_count   = 0;
    for (const auto &member : group.members) {
        ASSERT_EQ(member.partitions_count, member.partitions.size());
        for (const auto partition : member.partitions) {
            ASSERT_LT(partition, 8);
            ASSERT_FALSE(seen_partitions[partition]);
            seen_partitions[partition] = true;
            assigned_count++;
        }
    }

    ASSERT_EQ(assigned_count, 8);
    for (const bool seen_partition : seen_partitions) {
        ASSERT_TRUE(seen_partition);
    }

    ASSERT_NO_THROW(iggy::ffi::delete_connection(fourth));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(third));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(second));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(first));
}

TEST(LowLevelE2E_ConsumerGroup, DeleteConsumerGroupSucceeds) {
    RecordProperty("description", "Deletes an existing consumer group successfully.");
    const std::string stream_name = "client-delete-group-happy-stream";
    const std::string topic_name  = "client-delete-group-happy-topic";
    const std::string group_name  = "client-delete-group-happy";
    iggy::ffi::Client *client     = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), group_name));

    ASSERT_NO_THROW(client->delete_consumer_group(
        make_string_identifier(stream_name), make_string_identifier(topic_name), make_string_identifier(group_name)));

    ASSERT_THROW(client->get_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                            make_string_identifier(group_name)),
                 std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, DeleteConsumerGroupWithInvalidIdentifiersThrows) {
    RecordProperty("description",
                   "Rejects delete_consumer_group when the stream, topic, or group identifier is invalid.");
    const std::string stream_name = "client-delete-group-invalid-id-stream";
    const std::string topic_name  = "client-delete-group-invalid-id-topic";
    const std::string group_name  = "client-delete-group-invalid-id-group";
    iggy::ffi::Client *client     = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
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

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, DeleteConsumerGroupOnNonExistentResourcesThrows) {
    RecordProperty("description", "Rejects delete_consumer_group for streams, topics, or groups that do not exist.");
    const std::string stream_name         = "client-delete-group-missing-resource-stream";
    const std::string topic_name          = "client-delete-group-missing-resource-topic";
    const std::string created_group_name  = "client-delete-group-existing-group";
    const std::string missing_stream_name = "client-delete-group-missing-stream";
    const std::string missing_topic_name  = "client-delete-group-missing-topic";
    const std::string missing_group_name  = "client-delete-group-missing-group";
    iggy::ffi::Client *client             = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
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

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, DeleteConsumerGroupTwiceThrows) {
    RecordProperty("description", "Rejects deleting the same consumer group twice.");
    const std::string stream_name = "client-delete-group-twice-stream";
    const std::string topic_name  = "client-delete-group-twice-topic";
    const std::string group_name  = "client-delete-group-twice";
    iggy::ffi::Client *client     = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), group_name));
    ASSERT_NO_THROW(client->delete_consumer_group(
        make_string_identifier(stream_name), make_string_identifier(topic_name), make_string_identifier(group_name)));

    ASSERT_THROW(client->delete_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                               make_string_identifier(group_name)),
                 std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, DeleteConsumerGroupWithNumericIdentifiersSucceeds) {
    RecordProperty(
        "description",
        "Deletes a consumer group successfully when stream, topic, and group are addressed by numeric identifiers.");
    const std::string stream_name = "client-delete-group-numeric-stream";
    const std::string topic_name  = "client-delete-group-numeric-topic";
    const std::string group_name  = "client-delete-group-numeric";
    iggy::ffi::Client *client     = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
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

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, DeleteConsumerGroupAfterStreamDeletionThrows) {
    RecordProperty("description",
                   "Rejects delete_consumer_group after deleting the stream that owned the consumer group.");
    const std::string stream_name = "client-delete-group-after-stream-delete-stream";
    const std::string topic_name  = "client-delete-group-after-stream-delete-topic";
    const std::string group_name  = "client-delete-group-after-stream-delete";
    iggy::ffi::Client *client     = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), group_name));
    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));

    ASSERT_THROW(client->delete_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                               make_string_identifier(group_name)),
                 std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, DeleteConsumerGroupWithActiveMembersSucceeds) {
    RecordProperty("description", "Deletes a consumer group successfully even when it still has active members.");
    const std::string stream_name = "client-delete-group-active-members-stream";
    const std::string topic_name  = "client-delete-group-active-members-topic";
    const std::string group_name  = "client-delete-group-active-members";
    iggy::ffi::Client *first      = login_to_server();
    iggy::ffi::Client *second     = login_to_server();

    ASSERT_NO_THROW(first->create_stream(stream_name));
    ASSERT_NO_THROW(first->create_topic(make_string_identifier(stream_name), topic_name, 2, "none", 0, "server_default",
                                        0, "server_default"));
    ASSERT_NO_THROW(first->create_consumer_group(make_string_identifier(stream_name),
                                                 make_string_identifier(topic_name), group_name));
    ASSERT_NO_THROW(first->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                               make_string_identifier(group_name)));
    ASSERT_NO_THROW(second->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                                make_string_identifier(group_name)));

    ASSERT_NO_THROW(first->delete_consumer_group(
        make_string_identifier(stream_name), make_string_identifier(topic_name), make_string_identifier(group_name)));

    ASSERT_THROW(first->get_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                           make_string_identifier(group_name)),
                 std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(second));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(first));
}

TEST(LowLevelE2E_ConsumerGroup, DeleteConsumerGroupPreventsFutureJoin) {
    RecordProperty("description", "Rejects join_consumer_group after the consumer group has been deleted.");
    const std::string stream_name = "client-delete-group-prevent-join-stream";
    const std::string topic_name  = "client-delete-group-prevent-join-topic";
    const std::string group_name  = "client-delete-group-prevent-join";
    iggy::ffi::Client *client     = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), group_name));
    ASSERT_NO_THROW(client->delete_consumer_group(
        make_string_identifier(stream_name), make_string_identifier(topic_name), make_string_identifier(group_name)));

    ASSERT_THROW(client->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                             make_string_identifier(group_name)),
                 std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, DeleteConsumerGroupPreventsFutureLeave) {
    RecordProperty("description", "Rejects leave_consumer_group after the consumer group has been deleted.");
    const std::string stream_name = "client-delete-group-prevent-leave-stream";
    const std::string topic_name  = "client-delete-group-prevent-leave-topic";
    const std::string group_name  = "client-delete-group-prevent-leave";
    iggy::ffi::Client *client     = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), group_name));
    ASSERT_NO_THROW(client->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                                make_string_identifier(group_name)));
    ASSERT_NO_THROW(client->delete_consumer_group(
        make_string_identifier(stream_name), make_string_identifier(topic_name), make_string_identifier(group_name)));

    ASSERT_THROW(client->leave_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                              make_string_identifier(group_name)),
                 std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, DeleteConsumerGroupAndRecreateWithSameNameSucceeds) {
    RecordProperty("description",
                   "Allows recreating a consumer group with the same name after the previous group is deleted.");
    const std::string stream_name = "client-delete-group-recreate-stream";
    const std::string topic_name  = "client-delete-group-recreate-topic";
    const std::string group_name  = "client-delete-group-recreate";
    iggy::ffi::Client *client     = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
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

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_ConsumerGroup, DeleteConsumerGroupDoesNotAffectOtherGroupsOnSameTopic) {
    RecordProperty("description",
                   "Deletes one consumer group without affecting another consumer group on the same topic.");
    const std::string stream_name     = "client-delete-group-isolation-stream";
    const std::string topic_name      = "client-delete-group-isolation-topic";
    const std::string deleted_group   = "client-delete-group-isolation-deleted";
    const std::string surviving_group = "client-delete-group-isolation-surviving";
    iggy::ffi::Client *client         = login_to_server();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 2, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), deleted_group));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), surviving_group));
    ASSERT_NO_THROW(client->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                                make_string_identifier(surviving_group)));

    ASSERT_NO_THROW(client->delete_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name),
                                                  make_string_identifier(deleted_group)));

    ASSERT_THROW(client->get_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                            make_string_identifier(deleted_group)),
                 std::exception);

    ASSERT_NO_THROW({
        const auto group =
            client->get_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                       make_string_identifier(surviving_group));
        ASSERT_EQ(group.name, surviving_group);
        ASSERT_EQ(group.members_count, 1);
        ASSERT_EQ(group.members.size(), 1);
    });

    ASSERT_NO_THROW(client->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                                make_string_identifier(surviving_group)));

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}
