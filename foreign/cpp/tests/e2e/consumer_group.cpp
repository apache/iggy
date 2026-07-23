/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

#include <cstddef>
#include <string>

#include <gtest/gtest.h>

#include "lib.rs.h"
#include "tests/e2e/test_helpers.hpp"

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
        TrackConsumerGroup(stream_name, topic_name, group_name);
        ASSERT_EQ(group.name, group_name);
        ASSERT_EQ(group.members_count, 0u);
        ASSERT_TRUE(group.members.empty());
    });
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

    ASSERT_THROW(client->create_consumer_group(make_string_identifier(missing_stream_name),
                                               make_string_identifier(topic_name), GetRandomName()),
                 std::exception);
    ASSERT_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                               make_string_identifier(missing_topic_name), GetRandomName()),
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
    TrackConsumerGroup(stream_name, topic_name, group_name);
    ASSERT_THROW(client->create_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                               group_name),
                 std::exception);
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
    ASSERT_NO_THROW(unauthenticated_client->login_user("iggy", "iggy"));
    ASSERT_NO_THROW(unauthenticated_client->disconnect());
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
    TrackConsumerGroup(stream_name, topic_name, group_name);

    const auto fetched_group = client->get_consumer_group(
        make_string_identifier(stream_name), make_string_identifier(topic_name), make_string_identifier(group_name));

    ASSERT_EQ(fetched_group.id, created_group.id);
    ASSERT_EQ(fetched_group.name, created_group.name);
    ASSERT_EQ(fetched_group.partitions_count, created_group.partitions_count);
    ASSERT_EQ(fetched_group.members_count, created_group.members_count);
    ASSERT_EQ(fetched_group.members.size(), created_group.members.size());
}

TEST_F(LowLevelE2E_ConsumerGroup, GetConsumerGroupsReturnsCreatedGroups) {
    RecordProperty("description", "Returns created consumer groups for an existing stream and topic.");
    const std::string stream_name       = GetRandomName();
    const std::string topic_name        = GetRandomName();
    const std::string first_group_name  = GetRandomName();
    const std::string second_group_name = GetRandomName();
    iggy::ffi::Client *client           = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));

    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), first_group_name));
    TrackConsumerGroup(stream_name, topic_name, first_group_name);
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), second_group_name));
    TrackConsumerGroup(stream_name, topic_name, second_group_name);

    const auto groups =
        client->get_consumer_groups(make_string_identifier(stream_name), make_string_identifier(topic_name));

    EXPECT_EQ(groups.size(), std::size_t{2});
    EXPECT_EQ(groups[0].name, first_group_name);
    EXPECT_EQ(groups[1].name, second_group_name);
}

TEST_F(LowLevelE2E_ConsumerGroup, GetConsumerGroupsBeforeLoginThrows) {
    RecordProperty("description", "Rejects get_consumer_groups before connect, and after connect but before login.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    iggy::ffi::Client *setup_client = GetLoggedInClient();
    ASSERT_NO_THROW(setup_client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(setup_client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                               "server_default", 0, "server_default"));

    iggy::ffi::Client *unauthenticated_client = GetLoggedOutClient();

    ASSERT_THROW(unauthenticated_client->get_consumer_groups(make_string_identifier(stream_name),
                                                             make_string_identifier(topic_name)),
                 std::exception);
    ASSERT_NO_THROW(unauthenticated_client->connect());
    ASSERT_THROW(unauthenticated_client->get_consumer_groups(make_string_identifier(stream_name),
                                                             make_string_identifier(topic_name)),
                 std::exception);
    ASSERT_NO_THROW(unauthenticated_client->login_user("iggy", "iggy"));
    ASSERT_NO_THROW(unauthenticated_client->disconnect());
    ASSERT_THROW(unauthenticated_client->get_consumer_groups(make_string_identifier(stream_name),
                                                             make_string_identifier(topic_name)),
                 std::exception);
}

TEST_F(LowLevelE2E_ConsumerGroup, JoinConsumerGroupSucceeds) {
    RecordProperty("description", "Joins an existing consumer group successfully.");
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
    TrackConsumerGroup(stream_name, topic_name, group_name);

    ASSERT_NO_THROW(client->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                                make_string_identifier(group_name)));
}

TEST_F(LowLevelE2E_ConsumerGroup, JoinConsumerGroupBeforeLoginThrows) {
    RecordProperty("description", "Rejects join_consumer_group before connect, and after connect but before login.");
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
    TrackConsumerGroup(stream_name, topic_name, group_name);

    iggy::ffi::Client *unauthenticated_client = GetLoggedOutClient();

    ASSERT_THROW(unauthenticated_client->join_consumer_group(make_string_identifier(stream_name),
                                                             make_string_identifier(topic_name),
                                                             make_string_identifier(group_name)),
                 std::exception);
    ASSERT_NO_THROW(unauthenticated_client->connect());
    ASSERT_THROW(unauthenticated_client->join_consumer_group(make_string_identifier(stream_name),
                                                             make_string_identifier(topic_name),
                                                             make_string_identifier(group_name)),
                 std::exception);
    ASSERT_NO_THROW(unauthenticated_client->login_user("iggy", "iggy"));
    ASSERT_NO_THROW(unauthenticated_client->disconnect());
    ASSERT_THROW(unauthenticated_client->join_consumer_group(make_string_identifier(stream_name),
                                                             make_string_identifier(topic_name),
                                                             make_string_identifier(group_name)),
                 std::exception);
}

TEST_F(LowLevelE2E_ConsumerGroup, JoinConsumerGroupOnNonExistentResourcesThrows) {
    RecordProperty("description", "Rejects join_consumer_group for streams, topics, or groups that do not exist.");
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
    TrackConsumerGroup(stream_name, topic_name, created_group_name);

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
}

TEST_F(LowLevelE2E_ConsumerGroup, JoinConsumerGroupAfterStreamDeletionThrows) {
    RecordProperty("description", "Rejects join_consumer_group after deleting the stream that owned the group.");
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
    TrackConsumerGroup(stream_name, topic_name, group_name);
    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));
    ForgetTrackedStream(stream_name);

    ASSERT_THROW(client->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                             make_string_identifier(group_name)),
                 std::exception);
}

TEST_F(LowLevelE2E_ConsumerGroup, JoinConsumerGroupAfterTopicDeletionThrows) {
    RecordProperty("description", "Rejects join_consumer_group after deleting the topic that owned the group.");
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
    TrackConsumerGroup(stream_name, topic_name, group_name);
    ASSERT_NO_THROW(client->delete_topic(make_string_identifier(stream_name), make_string_identifier(topic_name)));
    ForgetTrackedConsumerGroup(stream_name, topic_name, group_name);
    ASSERT_THROW(client->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                             make_string_identifier(group_name)),
                 std::exception);
}

TEST_F(LowLevelE2E_ConsumerGroup, JoinConsumerGroupReflectsInGetConsumerGroup) {
    RecordProperty("description", "Reflects a joined consumer group in get_consumer_group member details.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));

    iggy::ffi::ConsumerGroupDetails created_group;
    ASSERT_NO_THROW({
        created_group = client->create_consumer_group(make_string_identifier(stream_name),
                                                      make_string_identifier(topic_name), group_name);
    });
    TrackConsumerGroup(stream_name, topic_name, group_name);
    ASSERT_NO_THROW(client->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                                make_string_identifier(group_name)));

    const auto fetched_group = client->get_consumer_group(
        make_string_identifier(stream_name), make_string_identifier(topic_name), make_string_identifier(group_name));

    EXPECT_EQ(fetched_group.id, created_group.id);
    EXPECT_EQ(fetched_group.name, created_group.name);
    EXPECT_EQ(fetched_group.partitions_count, created_group.partitions_count);
    EXPECT_EQ(fetched_group.members_count, 1u);
    ASSERT_EQ(fetched_group.members.size(), std::size_t{1});
}

TEST_F(LowLevelE2E_ConsumerGroup, JoinConsumerGroupTwiceKeepsSingleMember) {
    RecordProperty("description",
                   "Allows joining the same consumer group twice in a row without duplicating membership.");
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
    TrackConsumerGroup(stream_name, topic_name, group_name);

    ASSERT_NO_THROW(client->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                                make_string_identifier(group_name)));
    ASSERT_NO_THROW(client->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                                make_string_identifier(group_name)));

    const auto fetched_group = client->get_consumer_group(
        make_string_identifier(stream_name), make_string_identifier(topic_name), make_string_identifier(group_name));
    EXPECT_EQ(fetched_group.members_count, 1u);
    ASSERT_EQ(fetched_group.members.size(), std::size_t{1});
}

TEST_F(LowLevelE2E_ConsumerGroup, JoinConsumerGroupFromTwoClientsIncreasesMembersCount) {
    RecordProperty("description", "Reflects two joined clients as two members in the same consumer group.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    iggy::ffi::Client *first      = GetLoggedInClient();
    iggy::ffi::Client *second     = GetLoggedInClient();

    ASSERT_NO_THROW(first->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(first->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0, "server_default",
                                        0, "server_default"));
    ASSERT_NO_THROW(first->create_consumer_group(make_string_identifier(stream_name),
                                                 make_string_identifier(topic_name), group_name));
    TrackConsumerGroup(stream_name, topic_name, group_name);

    ASSERT_NO_THROW(first->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                               make_string_identifier(group_name)));
    ASSERT_NO_THROW(second->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                                make_string_identifier(group_name)));

    const auto fetched_group = first->get_consumer_group(
        make_string_identifier(stream_name), make_string_identifier(topic_name), make_string_identifier(group_name));
    EXPECT_EQ(fetched_group.members_count, 2u);
    ASSERT_EQ(fetched_group.members.size(), std::size_t{2});
}

TEST_F(LowLevelE2E_ConsumerGroup, JoinConsumerGroupThenLeaveRestoresMembersCount) {
    RecordProperty("description", "Restores the consumer group member count after a client joins and then leaves.");
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
    TrackConsumerGroup(stream_name, topic_name, group_name);

    ASSERT_NO_THROW(client->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                                make_string_identifier(group_name)));
    auto joined_group = client->get_consumer_group(
        make_string_identifier(stream_name), make_string_identifier(topic_name), make_string_identifier(group_name));
    EXPECT_EQ(joined_group.members_count, 1u);
    ASSERT_EQ(joined_group.members.size(), std::size_t{1});

    ASSERT_NO_THROW(client->leave_consumer_group(
        make_string_identifier(stream_name), make_string_identifier(topic_name), make_string_identifier(group_name)));

    const auto left_group = client->get_consumer_group(
        make_string_identifier(stream_name), make_string_identifier(topic_name), make_string_identifier(group_name));
    EXPECT_EQ(left_group.members_count, 0u);
    EXPECT_TRUE(left_group.members.empty());
}

TEST_F(LowLevelE2E_ConsumerGroup, LeaveConsumerGroupReducesMembersCount) {
    RecordProperty("description", "Reduces the consumer group member count after one of two joined clients leaves.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    iggy::ffi::Client *first      = GetLoggedInClient();
    iggy::ffi::Client *second     = GetLoggedInClient();

    ASSERT_NO_THROW(first->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(first->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0, "server_default",
                                        0, "server_default"));
    ASSERT_NO_THROW(first->create_consumer_group(make_string_identifier(stream_name),
                                                 make_string_identifier(topic_name), group_name));
    TrackConsumerGroup(stream_name, topic_name, group_name);

    ASSERT_NO_THROW(first->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                               make_string_identifier(group_name)));
    ASSERT_NO_THROW(second->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                                make_string_identifier(group_name)));

    const auto joined_group = first->get_consumer_group(
        make_string_identifier(stream_name), make_string_identifier(topic_name), make_string_identifier(group_name));
    EXPECT_EQ(joined_group.members_count, 2u);
    ASSERT_EQ(joined_group.members.size(), std::size_t{2});

    ASSERT_NO_THROW(second->leave_consumer_group(
        make_string_identifier(stream_name), make_string_identifier(topic_name), make_string_identifier(group_name)));

    const auto left_group = first->get_consumer_group(
        make_string_identifier(stream_name), make_string_identifier(topic_name), make_string_identifier(group_name));
    EXPECT_EQ(left_group.members_count, 1u);
    ASSERT_EQ(left_group.members.size(), std::size_t{1});
}

TEST_F(LowLevelE2E_ConsumerGroup, LeaveConsumerGroupBeforeLoginThrows) {
    RecordProperty("description", "Rejects leave_consumer_group before connect, and after connect but before login.");
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
    TrackConsumerGroup(stream_name, topic_name, group_name);
    ASSERT_NO_THROW(setup_client->join_consumer_group(
        make_string_identifier(stream_name), make_string_identifier(topic_name), make_string_identifier(group_name)));

    iggy::ffi::Client *unauthenticated_client = GetLoggedOutClient();

    ASSERT_THROW(unauthenticated_client->leave_consumer_group(make_string_identifier(stream_name),
                                                              make_string_identifier(topic_name),
                                                              make_string_identifier(group_name)),
                 std::exception);
    ASSERT_NO_THROW(unauthenticated_client->connect());
    ASSERT_THROW(unauthenticated_client->leave_consumer_group(make_string_identifier(stream_name),
                                                              make_string_identifier(topic_name),
                                                              make_string_identifier(group_name)),
                 std::exception);
    ASSERT_NO_THROW(unauthenticated_client->login_user("iggy", "iggy"));
    ASSERT_NO_THROW(unauthenticated_client->disconnect());
    ASSERT_THROW(unauthenticated_client->leave_consumer_group(make_string_identifier(stream_name),
                                                              make_string_identifier(topic_name),
                                                              make_string_identifier(group_name)),
                 std::exception);
}

TEST_F(LowLevelE2E_ConsumerGroup, LeaveConsumerGroupOnNonExistentResourcesThrows) {
    RecordProperty("description", "Rejects leave_consumer_group for streams, topics, or groups that do not exist.");
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
    TrackConsumerGroup(stream_name, topic_name, created_group_name);
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
}

TEST_F(LowLevelE2E_ConsumerGroup, LeaveConsumerGroupAfterStreamDeletionThrows) {
    RecordProperty("description", "Rejects leave_consumer_group after deleting the stream that owned the group.");
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
    TrackConsumerGroup(stream_name, topic_name, group_name);
    ASSERT_NO_THROW(client->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                                make_string_identifier(group_name)));
    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));
    ForgetTrackedStream(stream_name);

    ASSERT_THROW(client->leave_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                              make_string_identifier(group_name)),
                 std::exception);
}

TEST_F(LowLevelE2E_ConsumerGroup, LeaveConsumerGroupAfterTopicDeletionThrows) {
    RecordProperty("description", "Rejects leave_consumer_group after deleting the topic that owned the group.");
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
    TrackConsumerGroup(stream_name, topic_name, group_name);
    ASSERT_NO_THROW(client->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                                make_string_identifier(group_name)));
    ASSERT_NO_THROW(client->delete_topic(make_string_identifier(stream_name), make_string_identifier(topic_name)));
    ForgetTrackedConsumerGroup(stream_name, topic_name, group_name);

    ASSERT_THROW(client->leave_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                              make_string_identifier(group_name)),
                 std::exception);
}

TEST_F(LowLevelE2E_ConsumerGroup, LeaveConsumerGroupTwiceThrows) {
    RecordProperty("description", "Rejects leaving the same consumer group twice.");
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
    TrackConsumerGroup(stream_name, topic_name, group_name);
    ASSERT_NO_THROW(client->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                                make_string_identifier(group_name)));
    ASSERT_NO_THROW(client->leave_consumer_group(
        make_string_identifier(stream_name), make_string_identifier(topic_name), make_string_identifier(group_name)));

    ASSERT_THROW(client->leave_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                              make_string_identifier(group_name)),
                 std::exception);
}

TEST_F(LowLevelE2E_ConsumerGroup, LeaveConsumerGroupWithoutJoiningThrows) {
    RecordProperty("description", "Rejects leaving a consumer group when the client is not a member.");
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
    TrackConsumerGroup(stream_name, topic_name, group_name);

    ASSERT_THROW(client->leave_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                              make_string_identifier(group_name)),
                 std::exception);
}

TEST_F(LowLevelE2E_ConsumerGroup, GetConsumerGroupsReflectsJoinedGroupMembersCount) {
    RecordProperty("description", "Reflects a joined consumer group in get_consumer_groups members_count.");
    const std::string stream_name       = GetRandomName();
    const std::string topic_name        = GetRandomName();
    const std::string joined_group_name = GetRandomName();
    const std::string other_group_name  = GetRandomName();
    iggy::ffi::Client *client           = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));

    iggy::ffi::ConsumerGroupDetails joined_group;
    iggy::ffi::ConsumerGroupDetails other_group;
    ASSERT_NO_THROW({
        joined_group = client->create_consumer_group(make_string_identifier(stream_name),
                                                     make_string_identifier(topic_name), joined_group_name);
    });
    TrackConsumerGroup(stream_name, topic_name, joined_group_name);
    ASSERT_NO_THROW({
        other_group = client->create_consumer_group(make_string_identifier(stream_name),
                                                    make_string_identifier(topic_name), other_group_name);
    });
    TrackConsumerGroup(stream_name, topic_name, other_group_name);

    ASSERT_NO_THROW(client->join_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                                make_string_identifier(joined_group_name)));

    const auto groups =
        client->get_consumer_groups(make_string_identifier(stream_name), make_string_identifier(topic_name));

    ASSERT_EQ(groups.size(), std::size_t{2});

    EXPECT_EQ(groups[0].id, joined_group.id);
    EXPECT_EQ(groups[0].name, joined_group.name);
    EXPECT_EQ(groups[0].members_count, joined_group.members_count + 1u);

    EXPECT_EQ(groups[1].id, other_group.id);
    EXPECT_EQ(groups[1].name, other_group.name);
    EXPECT_EQ(groups[1].members_count, other_group.members_count);
    EXPECT_NE(groups[0].members_count, groups[1].members_count);
}

TEST_F(LowLevelE2E_ConsumerGroup, GetConsumerGroupsOnNonExistentStreamReturnsEmpty) {
    RecordProperty("description", "Returns an empty list when the stream does not exist.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    const auto groups =
        client->get_consumer_groups(make_string_identifier(stream_name), make_string_identifier(topic_name));
    EXPECT_TRUE(groups.empty());
}

TEST_F(LowLevelE2E_ConsumerGroup, GetConsumerGroupsOnNonExistentTopicReturnsEmpty) {
    RecordProperty("description", "Returns an empty list when the topic does not exist.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);

    const auto groups =
        client->get_consumer_groups(make_string_identifier(stream_name), make_string_identifier(topic_name));
    EXPECT_TRUE(groups.empty());
}

TEST_F(LowLevelE2E_ConsumerGroup, GetConsumerGroupsIsStableAcrossBackToBackCalls) {
    RecordProperty("description", "Returns the same consumer groups across back-to-back get_consumer_groups calls.");
    const std::string stream_name       = GetRandomName();
    const std::string topic_name        = GetRandomName();
    const std::string first_group_name  = GetRandomName();
    const std::string second_group_name = GetRandomName();
    iggy::ffi::Client *client           = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));

    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), first_group_name));
    TrackConsumerGroup(stream_name, topic_name, first_group_name);
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), second_group_name));
    TrackConsumerGroup(stream_name, topic_name, second_group_name);

    const auto first_groups =
        client->get_consumer_groups(make_string_identifier(stream_name), make_string_identifier(topic_name));
    const auto second_groups =
        client->get_consumer_groups(make_string_identifier(stream_name), make_string_identifier(topic_name));

    EXPECT_EQ(second_groups.size(), first_groups.size());
    EXPECT_EQ(second_groups.size(), std::size_t{2});

    for (std::size_t i = 0; i < first_groups.size(); ++i) {
        EXPECT_EQ(second_groups[i].id, first_groups[i].id);
        EXPECT_EQ(second_groups[i].name, first_groups[i].name);
        EXPECT_EQ(second_groups[i].partitions_count, first_groups[i].partitions_count);
        EXPECT_EQ(second_groups[i].members_count, first_groups[i].members_count);
    }
}

TEST_F(LowLevelE2E_ConsumerGroup, GetConsumerGroupsReturnsCorrectNumberOfGroups) {
    RecordProperty("description", "Returns the last remaining consumer group after deleting two groups.");
    const std::string stream_name          = GetRandomName();
    const std::string topic_name           = GetRandomName();
    const std::string deleted_group_name   = GetRandomName();
    const std::string other_deleted_name   = GetRandomName();
    const std::string remaining_group_name = GetRandomName();
    iggy::ffi::Client *client              = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));

    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), deleted_group_name));
    TrackConsumerGroup(stream_name, topic_name, deleted_group_name);
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), other_deleted_name));
    TrackConsumerGroup(stream_name, topic_name, other_deleted_name);
    iggy::ffi::ConsumerGroupDetails remaining_group;
    ASSERT_NO_THROW({
        remaining_group = client->create_consumer_group(make_string_identifier(stream_name),
                                                        make_string_identifier(topic_name), remaining_group_name);
    });
    TrackConsumerGroup(stream_name, topic_name, remaining_group_name);
    ASSERT_NO_THROW(client->delete_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name),
                                                  make_string_identifier(deleted_group_name)));
    ForgetTrackedConsumerGroup(stream_name, topic_name, deleted_group_name);
    ASSERT_NO_THROW(client->delete_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name),
                                                  make_string_identifier(other_deleted_name)));
    ForgetTrackedConsumerGroup(stream_name, topic_name, other_deleted_name);
    const auto groups =
        client->get_consumer_groups(make_string_identifier(stream_name), make_string_identifier(topic_name));

    ASSERT_EQ(groups.size(), std::size_t{1});
    EXPECT_EQ(groups[0].id, remaining_group.id);
    EXPECT_EQ(groups[0].name, remaining_group.name);
    EXPECT_EQ(groups[0].partitions_count, remaining_group.partitions_count);
    EXPECT_EQ(groups[0].members_count, remaining_group.members_count);

    ASSERT_NO_THROW(client->delete_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name),
                                                  make_string_identifier(remaining_group_name)));
    ForgetTrackedConsumerGroup(stream_name, topic_name, remaining_group_name);

    const auto groups_after_delete =
        client->get_consumer_groups(make_string_identifier(stream_name), make_string_identifier(topic_name));
    EXPECT_TRUE(groups_after_delete.empty());
}

TEST_F(LowLevelE2E_ConsumerGroup, GetConsumerGroupsAfterStreamDeletionReturnsEmpty) {
    RecordProperty("description", "Returns an empty list after deleting the stream that owned the groups.");
    const std::string stream_name       = GetRandomName();
    const std::string topic_name        = GetRandomName();
    const std::string first_group_name  = GetRandomName();
    const std::string second_group_name = GetRandomName();
    iggy::ffi::Client *client           = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), first_group_name));
    TrackConsumerGroup(stream_name, topic_name, first_group_name);
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), second_group_name));
    TrackConsumerGroup(stream_name, topic_name, second_group_name);
    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));
    ForgetTrackedConsumerGroup(stream_name, topic_name, first_group_name);
    ForgetTrackedConsumerGroup(stream_name, topic_name, second_group_name);
    ForgetTrackedStream(stream_name);

    const auto groups =
        client->get_consumer_groups(make_string_identifier(stream_name), make_string_identifier(topic_name));
    EXPECT_TRUE(groups.empty());
}

TEST_F(LowLevelE2E_ConsumerGroup, GetConsumerGroupsAfterTopicDeletionReturnsEmpty) {
    RecordProperty("description", "Returns an empty list after deleting the topic that owned the groups.");
    const std::string stream_name       = GetRandomName();
    const std::string topic_name        = GetRandomName();
    const std::string first_group_name  = GetRandomName();
    const std::string second_group_name = GetRandomName();
    iggy::ffi::Client *client           = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), first_group_name));
    TrackConsumerGroup(stream_name, topic_name, first_group_name);
    ASSERT_NO_THROW(client->create_consumer_group(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), second_group_name));
    TrackConsumerGroup(stream_name, topic_name, second_group_name);

    ASSERT_NO_THROW(client->delete_topic(make_string_identifier(stream_name), make_string_identifier(topic_name)));
    ForgetTrackedConsumerGroup(stream_name, topic_name, first_group_name);
    ForgetTrackedConsumerGroup(stream_name, topic_name, second_group_name);

    const auto groups =
        client->get_consumer_groups(make_string_identifier(stream_name), make_string_identifier(topic_name));
    EXPECT_TRUE(groups.empty());
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
    TrackConsumerGroup(stream_name, topic_name, group_name);

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
    ASSERT_NO_THROW(unauthenticated_client->login_user("iggy", "iggy"));
    ASSERT_NO_THROW(unauthenticated_client->disconnect());
    ASSERT_THROW(unauthenticated_client->get_consumer_group(make_string_identifier(stream_name),
                                                            make_string_identifier(topic_name),
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
    TrackConsumerGroup(stream_name, topic_name, created_group_name);

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
    TrackConsumerGroup(stream_name, topic_name, group_name);

    ASSERT_NO_THROW(client->delete_consumer_group(
        make_string_identifier(stream_name), make_string_identifier(topic_name), make_string_identifier(group_name)));
    ForgetTrackedConsumerGroup(stream_name, topic_name, group_name);

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
    TrackConsumerGroup(stream_name, topic_name, group_name);

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
    ASSERT_NO_THROW(unauthenticated_client->login_user("iggy", "iggy"));
    ASSERT_NO_THROW(unauthenticated_client->disconnect());
    ASSERT_THROW(unauthenticated_client->delete_consumer_group(make_string_identifier(stream_name),
                                                               make_string_identifier(topic_name),
                                                               make_string_identifier(group_name)),
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
    TrackConsumerGroup(stream_name, topic_name, created_group_name);
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
    TrackConsumerGroup(stream_name, topic_name, group_name);
    ASSERT_NO_THROW(client->delete_consumer_group(
        make_string_identifier(stream_name), make_string_identifier(topic_name), make_string_identifier(group_name)));
    ForgetTrackedConsumerGroup(stream_name, topic_name, group_name);
    ASSERT_THROW(client->delete_consumer_group(make_string_identifier(stream_name), make_string_identifier(topic_name),
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
    TrackConsumerGroup(stream_name, topic_name, group_name);

    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));
    ForgetTrackedStream(stream_name);
    ForgetTrackedConsumerGroup(stream_name, topic_name, group_name);
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
        TrackConsumerGroup(stream_name, topic_name, group_name);
        ASSERT_EQ(recreated_group.id, 0u);
        ASSERT_EQ(recreated_group.name, group_name);
        ASSERT_EQ(recreated_group.members_count, 0u);
        ASSERT_TRUE(recreated_group.members.empty());
    });
}
