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

#include "iggy.hpp"
#include "lib.rs.h"
#include "tests/e2e/test_helpers.hpp"

class E2E_ConsumerGroup : public E2ETestFixture {};

TEST_F(E2E_ConsumerGroup, CreateConsumerGroupSucceeds) {
    RecordProperty("description", "Creates a consumer group successfully for an existing stream and topic.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));

    ASSERT_NO_THROW({
        const auto group = client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                                      iggy::Identifier::String(topic_name), group_name);
        TrackConsumerGroup(stream_name, topic_name, group_name);
        ASSERT_EQ(group.Name(), group_name);
        ASSERT_EQ(group.MembersCount(), 0u);
        ASSERT_TRUE(group.Members().empty());
    });
}

TEST_F(E2E_ConsumerGroup, CreateConsumerGroupOnNonExistentResourcesThrows) {
    RecordProperty("description", "Rejects creating a consumer group on streams or topics that do not exist.");
    const std::string stream_name         = GetRandomName();
    const std::string topic_name          = GetRandomName();
    const std::string missing_stream_name = GetRandomName();
    const std::string missing_topic_name  = GetRandomName();
    auto client                           = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));

    ASSERT_THROW(client.CreateConsumerGroup(iggy::Identifier::String(missing_stream_name),
                                            iggy::Identifier::String(topic_name), GetRandomName()),
                 iggy::IggyException);
    ASSERT_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                            iggy::Identifier::String(missing_topic_name), GetRandomName()),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, CreateConsumerGroupTwiceOnSameInputThrows) {
    RecordProperty("description", "Rejects creating the same consumer group twice for the same stream and topic.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), group_name));
    TrackConsumerGroup(stream_name, topic_name, group_name);
    ASSERT_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name),
                                            group_name),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, CreateConsumerGroupWithInvalidNamesThrows) {
    RecordProperty("description", "Rejects empty and overlong consumer group names.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));

    const std::string invalid_names[] = {"", std::string(256, 'a')};
    for (const std::string &invalid_name : invalid_names) {
        SCOPED_TRACE(invalid_name.size());
        ASSERT_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                                iggy::Identifier::String(topic_name), invalid_name),
                     iggy::IggyException);
    }
}

TEST_F(E2E_ConsumerGroup, CreateConsumerGroupAfterStreamDeletionThrows) {
    RecordProperty("description", "Rejects creating a consumer group after deleting the stream that owned the topic.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(client.DeleteStream(iggy::Identifier::String(stream_name)));
    ForgetTrackedStream(stream_name);

    ASSERT_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name),
                                            group_name),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, CreateConsumerGroupBeforeLoginThrows) {
    RecordProperty("description",
                   "Rejects creating a consumer group before connect, and after connect but before login.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();

    auto setup_client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(setup_client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(setup_client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                             iggy::TopicCreateOptions().SetPartitionsCount(1)));

    auto unauthenticated_client = GetLoggedOutHighLevelClient();

    ASSERT_THROW(unauthenticated_client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                                            iggy::Identifier::String(topic_name), group_name),
                 iggy::IggyException);
    ASSERT_NO_THROW(unauthenticated_client.Connect());
    ASSERT_THROW(unauthenticated_client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                                            iggy::Identifier::String(topic_name), group_name),
                 iggy::IggyException);
    ASSERT_NO_THROW(unauthenticated_client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(unauthenticated_client.Disconnect());
    ASSERT_THROW(unauthenticated_client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                                            iggy::Identifier::String(topic_name), group_name),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, GetConsumerGroupReturnsSameInfoAsCreateConsumerGroup) {
    RecordProperty("description",
                   "Returns the same consumer group details from get_consumer_group as create_consumer_group.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));

    const auto created_group = client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                                          iggy::Identifier::String(topic_name), group_name);
    TrackConsumerGroup(stream_name, topic_name, group_name);

    const auto fetched_group =
        client.GetConsumerGroup(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name),
                                iggy::Identifier::String(group_name));

    ASSERT_EQ(fetched_group.Id(), created_group.Id());
    ASSERT_EQ(fetched_group.Name(), created_group.Name());
    ASSERT_EQ(fetched_group.PartitionsCount(), created_group.PartitionsCount());
    ASSERT_EQ(fetched_group.MembersCount(), created_group.MembersCount());
    ASSERT_EQ(fetched_group.Members().size(), created_group.Members().size());
}

TEST_F(E2E_ConsumerGroup, GetConsumerGroupsReturnsCreatedGroups) {
    RecordProperty("description", "Returns created consumer groups for an existing stream and topic.");
    const std::string stream_name       = GetRandomName();
    const std::string topic_name        = GetRandomName();
    const std::string first_group_name  = GetRandomName();
    const std::string second_group_name = GetRandomName();
    auto client                         = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));

    ASSERT_NO_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), first_group_name));
    TrackConsumerGroup(stream_name, topic_name, first_group_name);
    ASSERT_NO_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), second_group_name));
    TrackConsumerGroup(stream_name, topic_name, second_group_name);

    const auto groups =
        client.GetConsumerGroups(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name));

    EXPECT_EQ(groups.size(), std::size_t{2});
    EXPECT_EQ(groups[0].Name(), first_group_name);
    EXPECT_EQ(groups[1].Name(), second_group_name);
}

TEST_F(E2E_ConsumerGroup, GetConsumerGroupsBeforeLoginThrows) {
    RecordProperty("description", "Rejects get_consumer_groups before connect, and after connect but before login.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto setup_client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(setup_client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(setup_client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                             iggy::TopicCreateOptions().SetPartitionsCount(1)));

    auto unauthenticated_client = GetLoggedOutHighLevelClient();

    ASSERT_THROW(unauthenticated_client.GetConsumerGroups(iggy::Identifier::String(stream_name),
                                                          iggy::Identifier::String(topic_name)),
                 iggy::IggyException);
    ASSERT_NO_THROW(unauthenticated_client.Connect());
    ASSERT_THROW(unauthenticated_client.GetConsumerGroups(iggy::Identifier::String(stream_name),
                                                          iggy::Identifier::String(topic_name)),
                 iggy::IggyException);
    ASSERT_NO_THROW(unauthenticated_client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(unauthenticated_client.Disconnect());
    ASSERT_THROW(unauthenticated_client.GetConsumerGroups(iggy::Identifier::String(stream_name),
                                                          iggy::Identifier::String(topic_name)),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, JoinConsumerGroupSucceeds) {
    RecordProperty("description", "Joins an existing consumer group successfully.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), group_name));
    TrackConsumerGroup(stream_name, topic_name, group_name);

    ASSERT_NO_THROW(client.JoinConsumerGroup(iggy::Identifier::String(stream_name),
                                             iggy::Identifier::String(topic_name),
                                             iggy::Identifier::String(group_name)));
}

TEST_F(E2E_ConsumerGroup, JoinConsumerGroupBeforeLoginThrows) {
    RecordProperty("description", "Rejects join_consumer_group before connect, and after connect but before login.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();

    auto setup_client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(setup_client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(setup_client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                             iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(setup_client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                                     iggy::Identifier::String(topic_name), group_name));
    TrackConsumerGroup(stream_name, topic_name, group_name);

    auto unauthenticated_client = GetLoggedOutHighLevelClient();

    ASSERT_THROW(unauthenticated_client.JoinConsumerGroup(iggy::Identifier::String(stream_name),
                                                          iggy::Identifier::String(topic_name),
                                                          iggy::Identifier::String(group_name)),
                 iggy::IggyException);
    ASSERT_NO_THROW(unauthenticated_client.Connect());
    ASSERT_THROW(unauthenticated_client.JoinConsumerGroup(iggy::Identifier::String(stream_name),
                                                          iggy::Identifier::String(topic_name),
                                                          iggy::Identifier::String(group_name)),
                 iggy::IggyException);
    ASSERT_NO_THROW(unauthenticated_client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(unauthenticated_client.Disconnect());
    ASSERT_THROW(unauthenticated_client.JoinConsumerGroup(iggy::Identifier::String(stream_name),
                                                          iggy::Identifier::String(topic_name),
                                                          iggy::Identifier::String(group_name)),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, JoinConsumerGroupOnNonExistentResourcesThrows) {
    RecordProperty("description", "Rejects join_consumer_group for streams, topics, or groups that do not exist.");
    const std::string stream_name         = GetRandomName();
    const std::string topic_name          = GetRandomName();
    const std::string created_group_name  = GetRandomName();
    const std::string missing_stream_name = GetRandomName();
    const std::string missing_topic_name  = GetRandomName();
    const std::string missing_group_name  = GetRandomName();
    auto client                           = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), created_group_name));
    TrackConsumerGroup(stream_name, topic_name, created_group_name);

    ASSERT_THROW(
        client.JoinConsumerGroup(iggy::Identifier::String(missing_stream_name), iggy::Identifier::String(topic_name),
                                 iggy::Identifier::String(created_group_name)),
        iggy::IggyException);
    ASSERT_THROW(
        client.JoinConsumerGroup(iggy::Identifier::String(stream_name), iggy::Identifier::String(missing_topic_name),
                                 iggy::Identifier::String(created_group_name)),
        iggy::IggyException);
    ASSERT_THROW(client.JoinConsumerGroup(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name),
                                          iggy::Identifier::String(missing_group_name)),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, JoinConsumerGroupAfterStreamDeletionThrows) {
    RecordProperty("description", "Rejects join_consumer_group after deleting the stream that owned the group.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), group_name));
    TrackConsumerGroup(stream_name, topic_name, group_name);
    ASSERT_NO_THROW(client.DeleteStream(iggy::Identifier::String(stream_name)));
    ForgetTrackedStream(stream_name);

    ASSERT_THROW(client.JoinConsumerGroup(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name),
                                          iggy::Identifier::String(group_name)),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, JoinConsumerGroupAfterTopicDeletionThrows) {
    RecordProperty("description", "Rejects join_consumer_group after deleting the topic that owned the group.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), group_name));
    TrackConsumerGroup(stream_name, topic_name, group_name);
    ASSERT_NO_THROW(client.DeleteTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name)));
    ForgetTrackedConsumerGroup(stream_name, topic_name, group_name);
    ASSERT_THROW(client.JoinConsumerGroup(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name),
                                          iggy::Identifier::String(group_name)),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, JoinConsumerGroupReflectsInGetConsumerGroup) {
    RecordProperty("description", "Reflects a joined consumer group in get_consumer_group member details.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));

    const auto created_group = client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                                          iggy::Identifier::String(topic_name), group_name);
    TrackConsumerGroup(stream_name, topic_name, group_name);
    ASSERT_NO_THROW(client.JoinConsumerGroup(iggy::Identifier::String(stream_name),
                                             iggy::Identifier::String(topic_name),
                                             iggy::Identifier::String(group_name)));

    const auto fetched_group =
        client.GetConsumerGroup(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name),
                                iggy::Identifier::String(group_name));

    EXPECT_EQ(fetched_group.Id(), created_group.Id());
    EXPECT_EQ(fetched_group.Name(), created_group.Name());
    EXPECT_EQ(fetched_group.PartitionsCount(), created_group.PartitionsCount());
    EXPECT_EQ(fetched_group.MembersCount(), 1u);
    ASSERT_EQ(fetched_group.Members().size(), std::size_t{1});
}

TEST_F(E2E_ConsumerGroup, JoinConsumerGroupTwiceKeepsSingleMember) {
    RecordProperty("description",
                   "Allows joining the same consumer group twice in a row without duplicating membership.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), group_name));
    TrackConsumerGroup(stream_name, topic_name, group_name);

    ASSERT_NO_THROW(client.JoinConsumerGroup(iggy::Identifier::String(stream_name),
                                             iggy::Identifier::String(topic_name),
                                             iggy::Identifier::String(group_name)));
    ASSERT_NO_THROW(client.JoinConsumerGroup(iggy::Identifier::String(stream_name),
                                             iggy::Identifier::String(topic_name),
                                             iggy::Identifier::String(group_name)));

    const auto fetched_group =
        client.GetConsumerGroup(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name),
                                iggy::Identifier::String(group_name));
    EXPECT_EQ(fetched_group.MembersCount(), 1u);
    ASSERT_EQ(fetched_group.Members().size(), std::size_t{1});
}

TEST_F(E2E_ConsumerGroup, JoinConsumerGroupFromTwoClientsIncreasesMembersCount) {
    RecordProperty("description", "Reflects two joined clients as two members in the same consumer group.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    auto first                    = GetLoggedInHighLevelClient();
    auto second                   = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(first.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(first.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                      iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(first.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                              iggy::Identifier::String(topic_name), group_name));
    TrackConsumerGroup(stream_name, topic_name, group_name);

    ASSERT_NO_THROW(first.JoinConsumerGroup(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name),
                                            iggy::Identifier::String(group_name)));
    ASSERT_NO_THROW(second.JoinConsumerGroup(iggy::Identifier::String(stream_name),
                                             iggy::Identifier::String(topic_name),
                                             iggy::Identifier::String(group_name)));

    const auto fetched_group =
        first.GetConsumerGroup(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name),
                               iggy::Identifier::String(group_name));
    EXPECT_EQ(fetched_group.MembersCount(), 2u);
    ASSERT_EQ(fetched_group.Members().size(), std::size_t{2});
}

TEST_F(E2E_ConsumerGroup, JoinConsumerGroupThenLeaveRestoresMembersCount) {
    RecordProperty("description", "Restores the consumer group member count after a client joins and then leaves.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), group_name));
    TrackConsumerGroup(stream_name, topic_name, group_name);

    ASSERT_NO_THROW(client.JoinConsumerGroup(iggy::Identifier::String(stream_name),
                                             iggy::Identifier::String(topic_name),
                                             iggy::Identifier::String(group_name)));
    const auto joined_group =
        client.GetConsumerGroup(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name),
                                iggy::Identifier::String(group_name));
    EXPECT_EQ(joined_group.MembersCount(), 1u);
    ASSERT_EQ(joined_group.Members().size(), std::size_t{1});

    ASSERT_NO_THROW(client.LeaveConsumerGroup(iggy::Identifier::String(stream_name),
                                              iggy::Identifier::String(topic_name),
                                              iggy::Identifier::String(group_name)));

    const auto left_group =
        client.GetConsumerGroup(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name),
                                iggy::Identifier::String(group_name));
    EXPECT_EQ(left_group.MembersCount(), 0u);
    EXPECT_TRUE(left_group.Members().empty());
}

TEST_F(E2E_ConsumerGroup, LeaveConsumerGroupReducesMembersCount) {
    RecordProperty("description", "Reduces the consumer group member count after one of two joined clients leaves.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    auto first                    = GetLoggedInHighLevelClient();
    auto second                   = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(first.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(first.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                      iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(first.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                              iggy::Identifier::String(topic_name), group_name));
    TrackConsumerGroup(stream_name, topic_name, group_name);

    ASSERT_NO_THROW(first.JoinConsumerGroup(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name),
                                            iggy::Identifier::String(group_name)));
    ASSERT_NO_THROW(second.JoinConsumerGroup(iggy::Identifier::String(stream_name),
                                             iggy::Identifier::String(topic_name),
                                             iggy::Identifier::String(group_name)));

    const auto joined_group =
        first.GetConsumerGroup(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name),
                               iggy::Identifier::String(group_name));
    EXPECT_EQ(joined_group.MembersCount(), 2u);
    ASSERT_EQ(joined_group.Members().size(), std::size_t{2});

    ASSERT_NO_THROW(second.LeaveConsumerGroup(iggy::Identifier::String(stream_name),
                                              iggy::Identifier::String(topic_name),
                                              iggy::Identifier::String(group_name)));

    const auto left_group =
        first.GetConsumerGroup(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name),
                               iggy::Identifier::String(group_name));
    EXPECT_EQ(left_group.MembersCount(), 1u);
    ASSERT_EQ(left_group.Members().size(), std::size_t{1});
}

TEST_F(E2E_ConsumerGroup, LeaveConsumerGroupBeforeLoginThrows) {
    RecordProperty("description", "Rejects leave_consumer_group before connect, and after connect but before login.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();

    auto setup_client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(setup_client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(setup_client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                             iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(setup_client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                                     iggy::Identifier::String(topic_name), group_name));
    TrackConsumerGroup(stream_name, topic_name, group_name);
    ASSERT_NO_THROW(setup_client.JoinConsumerGroup(iggy::Identifier::String(stream_name),
                                                   iggy::Identifier::String(topic_name),
                                                   iggy::Identifier::String(group_name)));

    auto unauthenticated_client = GetLoggedOutHighLevelClient();

    ASSERT_THROW(unauthenticated_client.LeaveConsumerGroup(iggy::Identifier::String(stream_name),
                                                           iggy::Identifier::String(topic_name),
                                                           iggy::Identifier::String(group_name)),
                 iggy::IggyException);
    ASSERT_NO_THROW(unauthenticated_client.Connect());
    ASSERT_THROW(unauthenticated_client.LeaveConsumerGroup(iggy::Identifier::String(stream_name),
                                                           iggy::Identifier::String(topic_name),
                                                           iggy::Identifier::String(group_name)),
                 iggy::IggyException);
    ASSERT_NO_THROW(unauthenticated_client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(unauthenticated_client.Disconnect());
    ASSERT_THROW(unauthenticated_client.LeaveConsumerGroup(iggy::Identifier::String(stream_name),
                                                           iggy::Identifier::String(topic_name),
                                                           iggy::Identifier::String(group_name)),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, LeaveConsumerGroupOnNonExistentResourcesThrows) {
    RecordProperty("description", "Rejects leave_consumer_group for streams, topics, or groups that do not exist.");
    const std::string stream_name         = GetRandomName();
    const std::string topic_name          = GetRandomName();
    const std::string created_group_name  = GetRandomName();
    const std::string missing_stream_name = GetRandomName();
    const std::string missing_topic_name  = GetRandomName();
    const std::string missing_group_name  = GetRandomName();
    auto client                           = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), created_group_name));
    TrackConsumerGroup(stream_name, topic_name, created_group_name);
    ASSERT_NO_THROW(client.JoinConsumerGroup(iggy::Identifier::String(stream_name),
                                             iggy::Identifier::String(topic_name),
                                             iggy::Identifier::String(created_group_name)));

    ASSERT_THROW(
        client.LeaveConsumerGroup(iggy::Identifier::String(missing_stream_name), iggy::Identifier::String(topic_name),
                                  iggy::Identifier::String(created_group_name)),
        iggy::IggyException);
    ASSERT_THROW(
        client.LeaveConsumerGroup(iggy::Identifier::String(stream_name), iggy::Identifier::String(missing_topic_name),
                                  iggy::Identifier::String(created_group_name)),
        iggy::IggyException);
    ASSERT_THROW(client.LeaveConsumerGroup(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name),
                                           iggy::Identifier::String(missing_group_name)),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, LeaveConsumerGroupAfterStreamDeletionThrows) {
    RecordProperty("description", "Rejects leave_consumer_group after deleting the stream that owned the group.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), group_name));
    TrackConsumerGroup(stream_name, topic_name, group_name);
    ASSERT_NO_THROW(client.JoinConsumerGroup(iggy::Identifier::String(stream_name),
                                             iggy::Identifier::String(topic_name),
                                             iggy::Identifier::String(group_name)));
    ASSERT_NO_THROW(client.DeleteStream(iggy::Identifier::String(stream_name)));
    ForgetTrackedStream(stream_name);

    ASSERT_THROW(client.LeaveConsumerGroup(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name),
                                           iggy::Identifier::String(group_name)),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, LeaveConsumerGroupAfterTopicDeletionThrows) {
    RecordProperty("description", "Rejects leave_consumer_group after deleting the topic that owned the group.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), group_name));
    TrackConsumerGroup(stream_name, topic_name, group_name);
    ASSERT_NO_THROW(client.JoinConsumerGroup(iggy::Identifier::String(stream_name),
                                             iggy::Identifier::String(topic_name),
                                             iggy::Identifier::String(group_name)));
    ASSERT_NO_THROW(client.DeleteTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name)));
    ForgetTrackedConsumerGroup(stream_name, topic_name, group_name);

    ASSERT_THROW(client.LeaveConsumerGroup(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name),
                                           iggy::Identifier::String(group_name)),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, LeaveConsumerGroupTwiceThrows) {
    RecordProperty("description", "Rejects leaving the same consumer group twice.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), group_name));
    TrackConsumerGroup(stream_name, topic_name, group_name);
    ASSERT_NO_THROW(client.JoinConsumerGroup(iggy::Identifier::String(stream_name),
                                             iggy::Identifier::String(topic_name),
                                             iggy::Identifier::String(group_name)));
    ASSERT_NO_THROW(client.LeaveConsumerGroup(iggy::Identifier::String(stream_name),
                                              iggy::Identifier::String(topic_name),
                                              iggy::Identifier::String(group_name)));

    ASSERT_THROW(client.LeaveConsumerGroup(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name),
                                           iggy::Identifier::String(group_name)),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, LeaveConsumerGroupWithoutJoiningThrows) {
    RecordProperty("description", "Rejects leaving a consumer group when the client is not a member.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), group_name));
    TrackConsumerGroup(stream_name, topic_name, group_name);

    ASSERT_THROW(client.LeaveConsumerGroup(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name),
                                           iggy::Identifier::String(group_name)),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, GetConsumerGroupsReflectsJoinedGroupMembersCount) {
    RecordProperty("description", "Reflects a joined consumer group in get_consumer_groups members_count.");
    const std::string stream_name       = GetRandomName();
    const std::string topic_name        = GetRandomName();
    const std::string joined_group_name = GetRandomName();
    const std::string other_group_name  = GetRandomName();
    auto client                         = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));

    ASSERT_NO_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), joined_group_name));
    TrackConsumerGroup(stream_name, topic_name, joined_group_name);
    ASSERT_NO_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), other_group_name));
    TrackConsumerGroup(stream_name, topic_name, other_group_name);

    ASSERT_NO_THROW(client.JoinConsumerGroup(iggy::Identifier::String(stream_name),
                                             iggy::Identifier::String(topic_name),
                                             iggy::Identifier::String(joined_group_name)));

    const auto groups =
        client.GetConsumerGroups(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name));

    ASSERT_EQ(groups.size(), std::size_t{2});

    EXPECT_EQ(groups[0].Name(), joined_group_name);
    EXPECT_EQ(groups[0].MembersCount(), 1u);

    EXPECT_EQ(groups[1].Name(), other_group_name);
    EXPECT_EQ(groups[1].MembersCount(), 0u);
    EXPECT_NE(groups[0].MembersCount(), groups[1].MembersCount());
}

// The VSR server rejects consumer-group reads whose parent stream or topic is
// absent with the legacy typed not-found; the legacy server answered them with
// an empty list.
TEST_F(E2E_ConsumerGroup, GetConsumerGroupsOnNonExistentStreamThrows) {
    RecordProperty("description", "Throws when the stream does not exist.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    ASSERT_THROW(client.GetConsumerGroups(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name)),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, GetConsumerGroupsOnNonExistentTopicThrows) {
    RecordProperty("description", "Throws when the topic does not exist.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);

    ASSERT_THROW(client.GetConsumerGroups(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name)),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, GetConsumerGroupsIsStableAcrossBackToBackCalls) {
    RecordProperty("description", "Returns the same consumer groups across back-to-back get_consumer_groups calls.");
    const std::string stream_name       = GetRandomName();
    const std::string topic_name        = GetRandomName();
    const std::string first_group_name  = GetRandomName();
    const std::string second_group_name = GetRandomName();
    auto client                         = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));

    ASSERT_NO_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), first_group_name));
    TrackConsumerGroup(stream_name, topic_name, first_group_name);
    ASSERT_NO_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), second_group_name));
    TrackConsumerGroup(stream_name, topic_name, second_group_name);

    const auto first_groups =
        client.GetConsumerGroups(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name));
    const auto second_groups =
        client.GetConsumerGroups(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name));

    EXPECT_EQ(second_groups.size(), first_groups.size());
    EXPECT_EQ(second_groups.size(), std::size_t{2});

    for (std::size_t i = 0; i < first_groups.size(); ++i) {
        EXPECT_EQ(second_groups[i].Id(), first_groups[i].Id());
        EXPECT_EQ(second_groups[i].Name(), first_groups[i].Name());
        EXPECT_EQ(second_groups[i].PartitionsCount(), first_groups[i].PartitionsCount());
        EXPECT_EQ(second_groups[i].MembersCount(), first_groups[i].MembersCount());
    }
}

TEST_F(E2E_ConsumerGroup, GetConsumerGroupsReturnsCorrectNumberOfGroups) {
    RecordProperty("description", "Returns the last remaining consumer group after deleting two groups.");
    const std::string stream_name          = GetRandomName();
    const std::string topic_name           = GetRandomName();
    const std::string deleted_group_name   = GetRandomName();
    const std::string other_deleted_name   = GetRandomName();
    const std::string remaining_group_name = GetRandomName();
    auto client                            = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));

    ASSERT_NO_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), deleted_group_name));
    TrackConsumerGroup(stream_name, topic_name, deleted_group_name);
    ASSERT_NO_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), other_deleted_name));
    TrackConsumerGroup(stream_name, topic_name, other_deleted_name);
    ASSERT_NO_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), remaining_group_name));
    TrackConsumerGroup(stream_name, topic_name, remaining_group_name);
    ASSERT_NO_THROW(client.DeleteConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name),
                                               iggy::Identifier::String(deleted_group_name)));
    ForgetTrackedConsumerGroup(stream_name, topic_name, deleted_group_name);
    ASSERT_NO_THROW(client.DeleteConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name),
                                               iggy::Identifier::String(other_deleted_name)));
    ForgetTrackedConsumerGroup(stream_name, topic_name, other_deleted_name);
    const auto groups =
        client.GetConsumerGroups(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name));

    ASSERT_EQ(groups.size(), std::size_t{1});
    EXPECT_EQ(groups[0].Name(), remaining_group_name);
    EXPECT_EQ(groups[0].MembersCount(), 0u);

    ASSERT_NO_THROW(client.DeleteConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name),
                                               iggy::Identifier::String(remaining_group_name)));
    ForgetTrackedConsumerGroup(stream_name, topic_name, remaining_group_name);

    const auto groups_after_delete =
        client.GetConsumerGroups(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name));
    EXPECT_TRUE(groups_after_delete.empty());
}

TEST_F(E2E_ConsumerGroup, GetConsumerGroupsAfterStreamDeletionThrows) {
    RecordProperty("description", "Throws after deleting the stream that owned the groups.");
    const std::string stream_name       = GetRandomName();
    const std::string topic_name        = GetRandomName();
    const std::string first_group_name  = GetRandomName();
    const std::string second_group_name = GetRandomName();
    auto client                         = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), first_group_name));
    TrackConsumerGroup(stream_name, topic_name, first_group_name);
    ASSERT_NO_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), second_group_name));
    TrackConsumerGroup(stream_name, topic_name, second_group_name);
    ASSERT_NO_THROW(client.DeleteStream(iggy::Identifier::String(stream_name)));
    ForgetTrackedConsumerGroup(stream_name, topic_name, first_group_name);
    ForgetTrackedConsumerGroup(stream_name, topic_name, second_group_name);
    ForgetTrackedStream(stream_name);

    ASSERT_THROW(client.GetConsumerGroups(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name)),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, GetConsumerGroupsAfterTopicDeletionThrows) {
    RecordProperty("description", "Throws after deleting the topic that owned the groups.");
    const std::string stream_name       = GetRandomName();
    const std::string topic_name        = GetRandomName();
    const std::string first_group_name  = GetRandomName();
    const std::string second_group_name = GetRandomName();
    auto client                         = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), first_group_name));
    TrackConsumerGroup(stream_name, topic_name, first_group_name);
    ASSERT_NO_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), second_group_name));
    TrackConsumerGroup(stream_name, topic_name, second_group_name);

    ASSERT_NO_THROW(client.DeleteTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name)));
    ForgetTrackedConsumerGroup(stream_name, topic_name, first_group_name);
    ForgetTrackedConsumerGroup(stream_name, topic_name, second_group_name);

    ASSERT_THROW(client.GetConsumerGroups(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name)),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, GetConsumerGroupBeforeLoginThrows) {
    RecordProperty("description", "Rejects get_consumer_group before connect, and after connect but before login.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();

    auto setup_client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(setup_client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(setup_client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                             iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(setup_client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                                     iggy::Identifier::String(topic_name), group_name));
    TrackConsumerGroup(stream_name, topic_name, group_name);

    auto unauthenticated_client = GetLoggedOutHighLevelClient();

    ASSERT_THROW(unauthenticated_client.GetConsumerGroup(iggy::Identifier::String(stream_name),
                                                         iggy::Identifier::String(topic_name),
                                                         iggy::Identifier::String(group_name)),
                 iggy::IggyException);
    ASSERT_NO_THROW(unauthenticated_client.Connect());
    ASSERT_THROW(unauthenticated_client.GetConsumerGroup(iggy::Identifier::String(stream_name),
                                                         iggy::Identifier::String(topic_name),
                                                         iggy::Identifier::String(group_name)),
                 iggy::IggyException);
    ASSERT_NO_THROW(unauthenticated_client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(unauthenticated_client.Disconnect());
    ASSERT_THROW(unauthenticated_client.GetConsumerGroup(iggy::Identifier::String(stream_name),
                                                         iggy::Identifier::String(topic_name),
                                                         iggy::Identifier::String(group_name)),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, GetConsumerGroupOnNonExistentResourcesThrows) {
    RecordProperty("description", "Rejects get_consumer_group for streams, topics, or groups that do not exist.");
    const std::string stream_name         = GetRandomName();
    const std::string topic_name          = GetRandomName();
    const std::string created_group_name  = GetRandomName();
    const std::string missing_stream_name = GetRandomName();
    const std::string missing_topic_name  = GetRandomName();
    const std::string missing_group_name  = GetRandomName();
    auto client                           = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), created_group_name));
    TrackConsumerGroup(stream_name, topic_name, created_group_name);

    ASSERT_THROW(
        client.GetConsumerGroup(iggy::Identifier::String(missing_stream_name), iggy::Identifier::String(topic_name),
                                iggy::Identifier::String(created_group_name)),
        iggy::IggyException);
    ASSERT_THROW(
        client.GetConsumerGroup(iggy::Identifier::String(stream_name), iggy::Identifier::String(missing_topic_name),
                                iggy::Identifier::String(created_group_name)),
        iggy::IggyException);
    ASSERT_THROW(client.GetConsumerGroup(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name),
                                         iggy::Identifier::String(missing_group_name)),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, GetConsumerGroupAfterStreamDeletionThrows) {
    RecordProperty("description", "Rejects get_consumer_group after deleting the stream that owned the group.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), group_name));
    ASSERT_NO_THROW(client.DeleteStream(iggy::Identifier::String(stream_name)));
    ForgetTrackedStream(stream_name);

    ASSERT_THROW(client.GetConsumerGroup(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name),
                                         iggy::Identifier::String(group_name)),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, DeleteConsumerGroupSucceeds) {
    RecordProperty("description", "Deletes an existing consumer group successfully.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), group_name));
    TrackConsumerGroup(stream_name, topic_name, group_name);

    ASSERT_NO_THROW(client.DeleteConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name),
                                               iggy::Identifier::String(group_name)));
    ForgetTrackedConsumerGroup(stream_name, topic_name, group_name);

    ASSERT_THROW(client.GetConsumerGroup(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name),
                                         iggy::Identifier::String(group_name)),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, DeleteConsumerGroupBeforeLoginThrows) {
    RecordProperty("description", "Rejects delete_consumer_group before connect, and after connect but before login.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();

    auto setup_client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(setup_client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(setup_client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                             iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(setup_client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                                     iggy::Identifier::String(topic_name), group_name));
    TrackConsumerGroup(stream_name, topic_name, group_name);

    auto unauthenticated_client = GetLoggedOutHighLevelClient();

    ASSERT_THROW(unauthenticated_client.DeleteConsumerGroup(iggy::Identifier::String(stream_name),
                                                            iggy::Identifier::String(topic_name),
                                                            iggy::Identifier::String(group_name)),
                 iggy::IggyException);
    ASSERT_NO_THROW(unauthenticated_client.Connect());
    ASSERT_THROW(unauthenticated_client.DeleteConsumerGroup(iggy::Identifier::String(stream_name),
                                                            iggy::Identifier::String(topic_name),
                                                            iggy::Identifier::String(group_name)),
                 iggy::IggyException);
    ASSERT_NO_THROW(unauthenticated_client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(unauthenticated_client.Disconnect());
    ASSERT_THROW(unauthenticated_client.DeleteConsumerGroup(iggy::Identifier::String(stream_name),
                                                            iggy::Identifier::String(topic_name),
                                                            iggy::Identifier::String(group_name)),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, DeleteConsumerGroupOnNonExistentResourcesThrows) {
    RecordProperty("description", "Rejects delete_consumer_group for streams, topics, or groups that do not exist.");
    const std::string stream_name         = GetRandomName();
    const std::string topic_name          = GetRandomName();
    const std::string created_group_name  = GetRandomName();
    const std::string missing_stream_name = GetRandomName();
    const std::string missing_topic_name  = GetRandomName();
    const std::string missing_group_name  = GetRandomName();
    auto client                           = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), created_group_name));
    TrackConsumerGroup(stream_name, topic_name, created_group_name);
    ASSERT_THROW(
        client.DeleteConsumerGroup(iggy::Identifier::String(missing_stream_name), iggy::Identifier::String(topic_name),
                                   iggy::Identifier::String(created_group_name)),
        iggy::IggyException);
    ASSERT_THROW(
        client.DeleteConsumerGroup(iggy::Identifier::String(stream_name), iggy::Identifier::String(missing_topic_name),
                                   iggy::Identifier::String(created_group_name)),
        iggy::IggyException);
    ASSERT_THROW(client.DeleteConsumerGroup(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name),
                                            iggy::Identifier::String(missing_group_name)),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, DeleteConsumerGroupTwiceThrows) {
    RecordProperty("description", "Rejects deleting the same consumer group twice.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), group_name));
    TrackConsumerGroup(stream_name, topic_name, group_name);
    ASSERT_NO_THROW(client.DeleteConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name),
                                               iggy::Identifier::String(group_name)));
    ForgetTrackedConsumerGroup(stream_name, topic_name, group_name);
    ASSERT_THROW(client.DeleteConsumerGroup(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name),
                                            iggy::Identifier::String(group_name)),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, DeleteConsumerGroupAfterStreamDeletionThrows) {
    RecordProperty("description",
                   "Rejects delete_consumer_group after deleting the stream that owned the consumer group.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), group_name));
    TrackConsumerGroup(stream_name, topic_name, group_name);

    ASSERT_NO_THROW(client.DeleteStream(iggy::Identifier::String(stream_name)));
    ForgetTrackedStream(stream_name);
    ForgetTrackedConsumerGroup(stream_name, topic_name, group_name);
    ASSERT_THROW(client.DeleteConsumerGroup(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name),
                                            iggy::Identifier::String(group_name)),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, DeleteConsumerGroupAndRecreateWithSameNameSucceeds) {
    RecordProperty("description",
                   "Allows recreating a consumer group with the same name after the previous group is deleted.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));

    ASSERT_NO_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), group_name));
    ASSERT_NO_THROW(client.DeleteConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name),
                                               iggy::Identifier::String(group_name)));

    ASSERT_NO_THROW({
        const auto recreated_group = client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                                                iggy::Identifier::String(topic_name), group_name);
        TrackConsumerGroup(stream_name, topic_name, group_name);
        // The VSR server mints group ids monotonically; a recreate gets a
        // fresh id (the deleted group held 0), unlike the legacy server which
        // reused the freed slot.
        ASSERT_GT(recreated_group.Id(), 0u);
        ASSERT_EQ(recreated_group.Name(), group_name);
        ASSERT_EQ(recreated_group.MembersCount(), 0u);
        ASSERT_TRUE(recreated_group.Members().empty());
    });
}

TEST_F(E2E_ConsumerGroup, StoreGetAndDeleteConsumerOffsetSucceeds) {
    RecordProperty("description", "Retrieves a partition-0 offset with no partition specified.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    auto *message_client          = GetLoggedInClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    messages.push_back(iggy::ffi::make_message(to_payload("offset-test"), rust::Vec<iggy::ffi::HeaderEntry>{}));
    ASSERT_NO_THROW(message_client->send_messages(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), "partition_id",
                                                  partition_id_bytes(0), std::move(messages)));

    const auto consumer = iggy::Consumer::Single(iggy::Identifier::Numeric(1));
    ASSERT_NO_THROW(client.StoreConsumerOffset(consumer, iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), 0, 0));

    const auto offset =
        client.GetConsumerOffset(consumer, iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name));
    EXPECT_EQ(offset.PartitionId(), 0u);
    EXPECT_EQ(offset.CurrentOffset(), 0u);
    EXPECT_EQ(offset.StoredOffset(), 0u);

    ASSERT_NO_THROW(client.DeleteConsumerOffset(consumer, iggy::Identifier::String(stream_name),
                                                iggy::Identifier::String(topic_name), 0));
    ASSERT_THROW(
        client.GetConsumerOffset(consumer, iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name)),
        iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, StoreConsumerOffsetOnEmptyPartitionThrows) {
    RecordProperty("description", "Rejects offsets for a partition that has not issued any message offsets.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    const auto consumer           = iggy::Consumer::Single(iggy::Identifier::Numeric(1));

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));

    for (const std::uint64_t offset : {0u, 1u}) {
        SCOPED_TRACE(offset);
        ASSERT_THROW(client.StoreConsumerOffset(consumer, iggy::Identifier::String(stream_name),
                                                iggy::Identifier::String(topic_name), 0, offset),
                     iggy::IggyException);
    }
}

TEST_F(E2E_ConsumerGroup, StoreConsumerOffsetAcceptsOffsetsAtValidBounds) {
    RecordProperty("description", "Stores offsets below and at the partition's current offset.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    auto *message_client          = GetLoggedInClient();
    const auto consumer           = iggy::Consumer::Single(iggy::Identifier::Numeric(1));

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t index = 0; index < 5; ++index) {
        messages.push_back(iggy::ffi::make_message(to_payload("offset-test-" + std::to_string(index)),
                                                   rust::Vec<iggy::ffi::HeaderEntry>{}));
    }
    ASSERT_NO_THROW(message_client->send_messages(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), "partition_id",
                                                  partition_id_bytes(0), std::move(messages)));

    ASSERT_NO_THROW(client.StoreConsumerOffset(consumer, iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), 0, 2));
    EXPECT_EQ(
        client
            .GetConsumerOffset(consumer, iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name), 0)
            .StoredOffset(),
        2u);

    ASSERT_NO_THROW(client.StoreConsumerOffset(consumer, iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), 0, 4));
    const auto current = client.GetConsumerOffset(consumer, iggy::Identifier::String(stream_name),
                                                  iggy::Identifier::String(topic_name), 0);
    EXPECT_EQ(current.CurrentOffset(), 4u);
    EXPECT_EQ(current.StoredOffset(), 4u);
}

TEST_F(E2E_ConsumerGroup, StoreConsumerOffsetPastCurrentOffsetThrowsWithoutChangingStoredOffset) {
    RecordProperty("description", "Rejects an offset past the partition head without replacing the stored offset.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    auto *message_client          = GetLoggedInClient();
    const auto consumer           = iggy::Consumer::Single(iggy::Identifier::Numeric(1));

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t index = 0; index < 5; ++index) {
        messages.push_back(iggy::ffi::make_message(to_payload("offset-test-" + std::to_string(index)),
                                                   rust::Vec<iggy::ffi::HeaderEntry>{}));
    }
    ASSERT_NO_THROW(message_client->send_messages(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), "partition_id",
                                                  partition_id_bytes(0), std::move(messages)));
    ASSERT_NO_THROW(client.StoreConsumerOffset(consumer, iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), 0, 2));

    ASSERT_THROW(client.StoreConsumerOffset(consumer, iggy::Identifier::String(stream_name),
                                            iggy::Identifier::String(topic_name), 0, 5),
                 iggy::IggyException);
    EXPECT_EQ(
        client
            .GetConsumerOffset(consumer, iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name), 0)
            .StoredOffset(),
        2u);
}

TEST_F(E2E_ConsumerGroup, StoreConsumerOffsetBeforeLoginThrows) {
    RecordProperty("description", "Rejects storing an offset before login and after disconnect.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    auto setup_client             = GetLoggedInHighLevelClient();
    auto *message_client          = GetLoggedInClient();
    auto client                   = GetLoggedOutHighLevelClient();
    const auto consumer           = iggy::Consumer::Single(iggy::Identifier::Numeric(1));

    ASSERT_NO_THROW(setup_client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(setup_client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                             iggy::TopicCreateOptions().SetPartitionsCount(1)));
    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t index = 0; index < 1; ++index) {
        messages.push_back(iggy::ffi::make_message(to_payload("offset-test-" + std::to_string(index)),
                                                   rust::Vec<iggy::ffi::HeaderEntry>{}));
    }
    ASSERT_NO_THROW(message_client->send_messages(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), "partition_id",
                                                  partition_id_bytes(0), std::move(messages)));

    ASSERT_THROW(client.StoreConsumerOffset(consumer, iggy::Identifier::String(stream_name),
                                            iggy::Identifier::String(topic_name), 0, 0),
                 iggy::IggyException);
    ASSERT_NO_THROW(client.Connect());
    ASSERT_THROW(client.StoreConsumerOffset(consumer, iggy::Identifier::String(stream_name),
                                            iggy::Identifier::String(topic_name), 0, 0),
                 iggy::IggyException);
    ASSERT_NO_THROW(client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(client.Disconnect());
    ASSERT_THROW(client.StoreConsumerOffset(consumer, iggy::Identifier::String(stream_name),
                                            iggy::Identifier::String(topic_name), 0, 0),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, StoreConsumerOffsetOnNonExistentResourcesThrows) {
    RecordProperty("description", "Rejects missing streams, topics, and partitions.");
    const std::string stream_name         = GetRandomName();
    const std::string topic_name          = GetRandomName();
    const std::string missing_stream_name = GetRandomName();
    const std::string missing_topic_name  = GetRandomName();
    auto client                           = GetLoggedInHighLevelClient();
    auto *message_client                  = GetLoggedInClient();
    const auto consumer                   = iggy::Consumer::Single(iggy::Identifier::Numeric(1));

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t index = 0; index < 1; ++index) {
        messages.push_back(iggy::ffi::make_message(to_payload("offset-test-" + std::to_string(index)),
                                                   rust::Vec<iggy::ffi::HeaderEntry>{}));
    }
    ASSERT_NO_THROW(message_client->send_messages(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), "partition_id",
                                                  partition_id_bytes(0), std::move(messages)));

    ASSERT_THROW(client.StoreConsumerOffset(consumer, iggy::Identifier::String(missing_stream_name),
                                            iggy::Identifier::String(topic_name), 0, 0),
                 iggy::IggyException);
    ASSERT_THROW(client.StoreConsumerOffset(consumer, iggy::Identifier::String(stream_name),
                                            iggy::Identifier::String(missing_topic_name), 0, 0),
                 iggy::IggyException);
    ASSERT_THROW(client.StoreConsumerOffset(consumer, iggy::Identifier::String(stream_name),
                                            iggy::Identifier::String(topic_name), 1, 0),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, StoreConsumerOffsetUpdatesExistingOffset) {
    RecordProperty("description", "Replaces a previously stored offset for the same consumer and partition.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    auto *message_client          = GetLoggedInClient();
    const auto consumer           = iggy::Consumer::Single(iggy::Identifier::Numeric(1));

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t index = 0; index < 4; ++index) {
        messages.push_back(iggy::ffi::make_message(to_payload("offset-test-" + std::to_string(index)),
                                                   rust::Vec<iggy::ffi::HeaderEntry>{}));
    }
    ASSERT_NO_THROW(message_client->send_messages(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), "partition_id",
                                                  partition_id_bytes(0), std::move(messages)));
    ASSERT_NO_THROW(client.StoreConsumerOffset(consumer, iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), 0, 1));
    ASSERT_NO_THROW(client.StoreConsumerOffset(consumer, iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), 0, 3));

    EXPECT_EQ(
        client
            .GetConsumerOffset(consumer, iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name), 0)
            .StoredOffset(),
        3u);
}

TEST_F(E2E_ConsumerGroup, StoreConsumerOffsetKeepsConsumerOffsetsIndependent) {
    RecordProperty("description", "Stores independent offsets for different consumers on the same partition.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    auto *message_client          = GetLoggedInClient();
    const auto first_consumer     = iggy::Consumer::Single(iggy::Identifier::Numeric(1));
    const auto second_consumer    = iggy::Consumer::Single(iggy::Identifier::Numeric(2));

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t index = 0; index < 3; ++index) {
        messages.push_back(iggy::ffi::make_message(to_payload("offset-test-" + std::to_string(index)),
                                                   rust::Vec<iggy::ffi::HeaderEntry>{}));
    }
    ASSERT_NO_THROW(message_client->send_messages(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), "partition_id",
                                                  partition_id_bytes(0), std::move(messages)));
    ASSERT_NO_THROW(client.StoreConsumerOffset(first_consumer, iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), 0, 0));
    ASSERT_NO_THROW(client.StoreConsumerOffset(second_consumer, iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), 0, 2));

    EXPECT_EQ(client
                  .GetConsumerOffset(first_consumer, iggy::Identifier::String(stream_name),
                                     iggy::Identifier::String(topic_name), 0)
                  .StoredOffset(),
              0u);
    EXPECT_EQ(client
                  .GetConsumerOffset(second_consumer, iggy::Identifier::String(stream_name),
                                     iggy::Identifier::String(topic_name), 0)
                  .StoredOffset(),
              2u);
}

TEST_F(E2E_ConsumerGroup, StoreConsumerOffsetForOwnedConsumerGroupPartitionSucceeds) {
    RecordProperty("description", "Stores an offset for a partition owned by the current consumer group member.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    auto *message_client          = GetLoggedInClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t index = 0; index < 1; ++index) {
        messages.push_back(iggy::ffi::make_message(to_payload("offset-test-" + std::to_string(index)),
                                                   rust::Vec<iggy::ffi::HeaderEntry>{}));
    }
    ASSERT_NO_THROW(message_client->send_messages(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), "partition_id",
                                                  partition_id_bytes(0), std::move(messages)));
    ASSERT_NO_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), group_name));
    TrackConsumerGroup(stream_name, topic_name, group_name);
    ASSERT_NO_THROW(client.JoinConsumerGroup(iggy::Identifier::String(stream_name),
                                             iggy::Identifier::String(topic_name),
                                             iggy::Identifier::String(group_name)));
    const auto consumer_group = iggy::Consumer::Group(iggy::Identifier::String(group_name));

    ASSERT_NO_THROW(client.StoreConsumerOffset(consumer_group, iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), 0, 0));
    EXPECT_EQ(client
                  .GetConsumerOffset(consumer_group, iggy::Identifier::String(stream_name),
                                     iggy::Identifier::String(topic_name), 0)
                  .StoredOffset(),
              0u);
}

TEST_F(E2E_ConsumerGroup, StoreConsumerGroupOffsetForUnownedPartitionThrows) {
    RecordProperty("description", "Rejects storing a group offset when the current client does not own the partition.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    auto *message_client          = GetLoggedInClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t index = 0; index < 1; ++index) {
        messages.push_back(iggy::ffi::make_message(to_payload("offset-test-" + std::to_string(index)),
                                                   rust::Vec<iggy::ffi::HeaderEntry>{}));
    }
    ASSERT_NO_THROW(message_client->send_messages(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), "partition_id",
                                                  partition_id_bytes(0), std::move(messages)));
    ASSERT_NO_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), group_name));
    TrackConsumerGroup(stream_name, topic_name, group_name);
    const auto consumer_group = iggy::Consumer::Group(iggy::Identifier::String(group_name));

    ASSERT_THROW(client.StoreConsumerOffset(consumer_group, iggy::Identifier::String(stream_name),
                                            iggy::Identifier::String(topic_name), 0, 0),
                 iggy::IggyException);
    ASSERT_THROW(client.GetConsumerOffset(consumer_group, iggy::Identifier::String(stream_name),
                                          iggy::Identifier::String(topic_name), 0),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, StoreConsumerOffsetForNonExistentConsumerGroupThrows) {
    RecordProperty("description", "Rejects named and numeric consumer groups that do not exist.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    auto *message_client          = GetLoggedInClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t index = 0; index < 1; ++index) {
        messages.push_back(iggy::ffi::make_message(to_payload("offset-test-" + std::to_string(index)),
                                                   rust::Vec<iggy::ffi::HeaderEntry>{}));
    }
    ASSERT_NO_THROW(message_client->send_messages(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), "partition_id",
                                                  partition_id_bytes(0), std::move(messages)));

    const auto missing_by_name = iggy::Consumer::Group(iggy::Identifier::String(GetRandomName()));
    const auto missing_by_id   = iggy::Consumer::Group(iggy::Identifier::Numeric(999'999));
    for (const auto *consumer_group : {&missing_by_name, &missing_by_id}) {
        ASSERT_THROW(client.StoreConsumerOffset(*consumer_group, iggy::Identifier::String(stream_name),
                                                iggy::Identifier::String(topic_name), 0, 0),
                     iggy::IggyException);
    }
}

TEST_F(E2E_ConsumerGroup, StoreConsumerOffsetSupportsNamedAndNumericIdentifiers) {
    RecordProperty("description", "Stores offsets using named and numeric stream, topic, and consumer identifiers.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    auto *message_client          = GetLoggedInClient();

    const auto stream = client.CreateStream(stream_name);
    TrackStream(stream_name);
    const auto topic = client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                          iggy::TopicCreateOptions().SetPartitionsCount(1));
    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t index = 0; index < 2; ++index) {
        messages.push_back(iggy::ffi::make_message(to_payload("offset-test-" + std::to_string(index)),
                                                   rust::Vec<iggy::ffi::HeaderEntry>{}));
    }
    ASSERT_NO_THROW(message_client->send_messages(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), "partition_id",
                                                  partition_id_bytes(0), std::move(messages)));

    const auto named_consumer   = iggy::Consumer::Single(iggy::Identifier::String(GetRandomName()));
    const auto numeric_consumer = iggy::Consumer::Single(iggy::Identifier::Numeric(42));
    ASSERT_NO_THROW(client.StoreConsumerOffset(named_consumer, iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), 0, 0));
    ASSERT_NO_THROW(client.StoreConsumerOffset(numeric_consumer, iggy::Identifier::Numeric(stream.Id()),
                                               iggy::Identifier::Numeric(topic.Id()), 0, 1));

    EXPECT_EQ(client
                  .GetConsumerOffset(named_consumer, iggy::Identifier::Numeric(stream.Id()),
                                     iggy::Identifier::Numeric(topic.Id()), 0)
                  .StoredOffset(),
              0u);
    EXPECT_EQ(client
                  .GetConsumerOffset(numeric_consumer, iggy::Identifier::String(stream_name),
                                     iggy::Identifier::String(topic_name), 0)
                  .StoredOffset(),
              1u);
}

TEST_F(E2E_ConsumerGroup, StoreConsumerOffsetWithoutPermissionThrowsWithoutChangingOffset) {
    RecordProperty("description", "Rejects an unauthorized offset write without changing the existing value.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string username    = GetRandomName(50);
    const std::string password    = "secret123";
    auto client                   = GetLoggedInHighLevelClient();
    auto *message_client          = GetLoggedInClient();
    auto *user_admin_client       = GetLoggedInClient();
    const auto consumer           = iggy::Consumer::Single(iggy::Identifier::Numeric(1));

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t index = 0; index < 3; ++index) {
        messages.push_back(iggy::ffi::make_message(to_payload("offset-test-" + std::to_string(index)),
                                                   rust::Vec<iggy::ffi::HeaderEntry>{}));
    }
    ASSERT_NO_THROW(message_client->send_messages(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), "partition_id",
                                                  partition_id_bytes(0), std::move(messages)));
    ASSERT_NO_THROW(client.StoreConsumerOffset(consumer, iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), 0, 1));
    ASSERT_NO_THROW(CreateUser(user_admin_client, username, password, iggy::ffi::UserStatus::Active, true,
                               iggy::ffi::Permissions{}));
    auto restricted_client = GetLoggedInHighLevelClient(username, password);

    ASSERT_THROW(restricted_client.StoreConsumerOffset(consumer, iggy::Identifier::String(stream_name),
                                                       iggy::Identifier::String(topic_name), 0, 2),
                 iggy::IggyException);
    EXPECT_EQ(
        client
            .GetConsumerOffset(consumer, iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name), 0)
            .StoredOffset(),
        1u);
}

TEST_F(E2E_ConsumerGroup, GetConsumerOffsetReturnsAllFieldsForNonZeroPartition) {
    RecordProperty("description", "Returns the requested partition, its current offset, and the stored offset.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    auto *message_client          = GetLoggedInClient();
    const auto consumer           = iggy::Consumer::Single(iggy::Identifier::Numeric(1));

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(2)));
    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t index = 0; index < 5; ++index) {
        messages.push_back(iggy::ffi::make_message(to_payload("offset-test-" + std::to_string(index)),
                                                   rust::Vec<iggy::ffi::HeaderEntry>{}));
    }
    ASSERT_NO_THROW(message_client->send_messages(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), "partition_id",
                                                  partition_id_bytes(1), std::move(messages)));
    ASSERT_NO_THROW(client.StoreConsumerOffset(consumer, iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), 1, 2));

    const auto offset = client.GetConsumerOffset(consumer, iggy::Identifier::String(stream_name),
                                                 iggy::Identifier::String(topic_name), 1);
    EXPECT_EQ(offset.PartitionId(), 1u);
    EXPECT_EQ(offset.CurrentOffset(), 4u);
    EXPECT_EQ(offset.StoredOffset(), 2u);
}

TEST_F(E2E_ConsumerGroup, GetConsumerOffsetWithoutStoredOffsetThrows) {
    RecordProperty("description", "Rejects retrieving an offset that has not been stored for the consumer.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    auto *message_client          = GetLoggedInClient();
    const auto consumer           = iggy::Consumer::Single(iggy::Identifier::Numeric(1));

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    messages.push_back(iggy::ffi::make_message(to_payload("offset-test"), rust::Vec<iggy::ffi::HeaderEntry>{}));
    ASSERT_NO_THROW(message_client->send_messages(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), "partition_id",
                                                  partition_id_bytes(0), std::move(messages)));

    ASSERT_THROW(client.GetConsumerOffset(consumer, iggy::Identifier::String(stream_name),
                                          iggy::Identifier::String(topic_name), 0),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, GetConsumerOffsetBeforeLoginThrows) {
    RecordProperty("description", "Rejects retrieving an offset before login and after disconnect.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    auto setup_client             = GetLoggedInHighLevelClient();
    auto *message_client          = GetLoggedInClient();
    auto client                   = GetLoggedOutHighLevelClient();
    const auto consumer           = iggy::Consumer::Single(iggy::Identifier::Numeric(1));

    ASSERT_NO_THROW(setup_client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(setup_client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                             iggy::TopicCreateOptions().SetPartitionsCount(1)));
    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    messages.push_back(iggy::ffi::make_message(to_payload("offset-test"), rust::Vec<iggy::ffi::HeaderEntry>{}));
    ASSERT_NO_THROW(message_client->send_messages(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), "partition_id",
                                                  partition_id_bytes(0), std::move(messages)));
    ASSERT_NO_THROW(setup_client.StoreConsumerOffset(consumer, iggy::Identifier::String(stream_name),
                                                     iggy::Identifier::String(topic_name), 0, 0));

    ASSERT_THROW(client.GetConsumerOffset(consumer, iggy::Identifier::String(stream_name),
                                          iggy::Identifier::String(topic_name), 0),
                 iggy::IggyException);
    ASSERT_NO_THROW(client.Connect());
    ASSERT_THROW(client.GetConsumerOffset(consumer, iggy::Identifier::String(stream_name),
                                          iggy::Identifier::String(topic_name), 0),
                 iggy::IggyException);
    ASSERT_NO_THROW(client.Login("iggy", "iggy"));
    ASSERT_NO_THROW({
        const auto offset = client.GetConsumerOffset(consumer, iggy::Identifier::String(stream_name),
                                                     iggy::Identifier::String(topic_name), 0);
        EXPECT_EQ(offset.StoredOffset(), 0u);
    });
    ASSERT_NO_THROW(client.Disconnect());
    ASSERT_THROW(client.GetConsumerOffset(consumer, iggy::Identifier::String(stream_name),
                                          iggy::Identifier::String(topic_name), 0),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, GetConsumerOffsetOnNonExistentResourcesThrows) {
    RecordProperty("description", "Rejects missing streams, topics, and partitions when retrieving an offset.");
    const std::string stream_name         = GetRandomName();
    const std::string topic_name          = GetRandomName();
    const std::string missing_stream_name = GetRandomName();
    const std::string missing_topic_name  = GetRandomName();
    auto client                           = GetLoggedInHighLevelClient();
    const auto consumer                   = iggy::Consumer::Single(iggy::Identifier::Numeric(1));

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));

    ASSERT_THROW(client.GetConsumerOffset(consumer, iggy::Identifier::String(missing_stream_name),
                                          iggy::Identifier::String(topic_name), 0),
                 iggy::IggyException);
    ASSERT_THROW(client.GetConsumerOffset(consumer, iggy::Identifier::String(stream_name),
                                          iggy::Identifier::String(missing_topic_name), 0),
                 iggy::IggyException);
    ASSERT_THROW(client.GetConsumerOffset(consumer, iggy::Identifier::String(stream_name),
                                          iggy::Identifier::String(topic_name), 1),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, GetConsumerOffsetWithoutPermissionThrows) {
    RecordProperty("description", "Rejects retrieving an existing offset without poll permission.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string username    = GetRandomName(50);
    const std::string password    = "secret123";
    auto client                   = GetLoggedInHighLevelClient();
    auto *message_client          = GetLoggedInClient();
    auto *user_admin_client       = GetLoggedInClient();
    const auto consumer           = iggy::Consumer::Single(iggy::Identifier::Numeric(1));

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    messages.push_back(iggy::ffi::make_message(to_payload("offset-test"), rust::Vec<iggy::ffi::HeaderEntry>{}));
    ASSERT_NO_THROW(message_client->send_messages(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), "partition_id",
                                                  partition_id_bytes(0), std::move(messages)));
    ASSERT_NO_THROW(client.StoreConsumerOffset(consumer, iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), 0, 0));
    ASSERT_NO_THROW(CreateUser(user_admin_client, username, password, iggy::ffi::UserStatus::Active, true,
                               iggy::ffi::Permissions{}));
    auto restricted_client = GetLoggedInHighLevelClient(username, password);

    ASSERT_THROW(restricted_client.GetConsumerOffset(consumer, iggy::Identifier::String(stream_name),
                                                     iggy::Identifier::String(topic_name), 0),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, GetConsumerGroupOffsetCanBeReadByNonMember) {
    RecordProperty("description", "Allows an authenticated non-member to retrieve an existing consumer group offset.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    auto owner_client             = GetLoggedInHighLevelClient();
    auto reader_client            = GetLoggedInHighLevelClient();
    auto *message_client          = GetLoggedInClient();

    ASSERT_NO_THROW(owner_client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(owner_client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                             iggy::TopicCreateOptions().SetPartitionsCount(1)));
    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t index = 0; index < 3; ++index) {
        messages.push_back(iggy::ffi::make_message(to_payload("offset-test-" + std::to_string(index)),
                                                   rust::Vec<iggy::ffi::HeaderEntry>{}));
    }
    ASSERT_NO_THROW(message_client->send_messages(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), "partition_id",
                                                  partition_id_bytes(0), std::move(messages)));
    const auto group = owner_client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                                        iggy::Identifier::String(topic_name), group_name);
    TrackConsumerGroup(stream_name, topic_name, group_name);
    ASSERT_NO_THROW(owner_client.JoinConsumerGroup(iggy::Identifier::String(stream_name),
                                                   iggy::Identifier::String(topic_name),
                                                   iggy::Identifier::String(group_name)));
    const auto named_group = iggy::Consumer::Group(iggy::Identifier::String(group_name));
    ASSERT_NO_THROW(owner_client.StoreConsumerOffset(named_group, iggy::Identifier::String(stream_name),
                                                     iggy::Identifier::String(topic_name), 0, 1));

    const auto numeric_group = iggy::Consumer::Group(iggy::Identifier::Numeric(group.Id()));
    const auto offset        = reader_client.GetConsumerOffset(numeric_group, iggy::Identifier::String(stream_name),
                                                               iggy::Identifier::String(topic_name), 0);
    EXPECT_EQ(offset.PartitionId(), 0u);
    EXPECT_EQ(offset.CurrentOffset(), 2u);
    EXPECT_EQ(offset.StoredOffset(), 1u);
}

TEST_F(E2E_ConsumerGroup, GetConsumerOffsetForNonExistentConsumerGroupThrows) {
    RecordProperty("description",
                   "Rejects retrieving offsets for named and numeric consumer groups that do not exist.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));

    const auto missing_by_name = iggy::Consumer::Group(iggy::Identifier::String(GetRandomName()));
    const auto missing_by_id   = iggy::Consumer::Group(iggy::Identifier::Numeric(999'999));
    for (const auto *consumer_group : {&missing_by_name, &missing_by_id}) {
        ASSERT_THROW(client.GetConsumerOffset(*consumer_group, iggy::Identifier::String(stream_name),
                                              iggy::Identifier::String(topic_name), 0),
                     iggy::IggyException);
    }
}

TEST_F(E2E_ConsumerGroup, GetConsumerOffsetReflectsAutoCommittedPoll) {
    RecordProperty("description", "Returns the offset created by polling messages with auto-commit enabled.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    auto *message_client          = GetLoggedInClient();
    const auto consumer           = iggy::Consumer::Single(iggy::Identifier::Numeric(77));

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t index = 0; index < 5; ++index) {
        messages.push_back(iggy::ffi::make_message(to_payload("offset-test-" + std::to_string(index)),
                                                   rust::Vec<iggy::ffi::HeaderEntry>{}));
    }
    ASSERT_NO_THROW(message_client->send_messages(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), "partition_id",
                                                  partition_id_bytes(0), std::move(messages)));

    iggy::ffi::PolledMessages polled{};
    ASSERT_NO_THROW(polled = message_client->poll_messages(make_string_identifier(stream_name),
                                                           make_string_identifier(topic_name), 0, "consumer",
                                                           make_numeric_identifier(77), "next", 0, 3, true));
    ASSERT_EQ(polled.count, 3u);
    ASSERT_EQ(polled.messages.size(), 3u);
    EXPECT_EQ(polled.messages.front().offset, 0u);
    EXPECT_EQ(polled.messages.back().offset, 2u);

    const auto offset = client.GetConsumerOffset(consumer, iggy::Identifier::String(stream_name),
                                                 iggy::Identifier::String(topic_name), 0);
    EXPECT_EQ(offset.PartitionId(), 0u);
    EXPECT_EQ(offset.CurrentOffset(), 4u);
    EXPECT_EQ(offset.StoredOffset(), 2u);
}

TEST_F(E2E_ConsumerGroup, DeleteConsumerOffsetForMissingOffsetThrows) {
    RecordProperty("description", "Rejects deleting offsets that were never stored or were already deleted.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    auto *message_client          = GetLoggedInClient();
    const auto missing_consumer   = iggy::Consumer::Single(iggy::Identifier::Numeric(1));
    const auto stored_consumer    = iggy::Consumer::Single(iggy::Identifier::Numeric(2));

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    messages.push_back(iggy::ffi::make_message(to_payload("offset-test"), rust::Vec<iggy::ffi::HeaderEntry>{}));
    ASSERT_NO_THROW(message_client->send_messages(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), "partition_id",
                                                  partition_id_bytes(0), std::move(messages)));

    ASSERT_THROW(client.DeleteConsumerOffset(missing_consumer, iggy::Identifier::String(stream_name),
                                             iggy::Identifier::String(topic_name), 0),
                 iggy::IggyException);

    ASSERT_NO_THROW(client.StoreConsumerOffset(stored_consumer, iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), 0, 0));
    ASSERT_NO_THROW(client.DeleteConsumerOffset(stored_consumer, iggy::Identifier::String(stream_name),
                                                iggy::Identifier::String(topic_name), 0));
    ASSERT_THROW(client.DeleteConsumerOffset(stored_consumer, iggy::Identifier::String(stream_name),
                                             iggy::Identifier::String(topic_name), 0),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, DeleteConsumerOffsetRemovesOnlyRequestedConsumerAndPartition) {
    RecordProperty("description", "Deletes only the requested consumer and partition offset.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    auto *message_client          = GetLoggedInClient();
    const auto first_consumer     = iggy::Consumer::Single(iggy::Identifier::Numeric(1));
    const auto second_consumer    = iggy::Consumer::Single(iggy::Identifier::Numeric(2));

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(2)));
    rust::Vec<iggy::ffi::IggyMessageToSend> first_partition_messages;
    rust::Vec<iggy::ffi::IggyMessageToSend> second_partition_messages;
    for (std::uint32_t index = 0; index < 4; ++index) {
        first_partition_messages.push_back(iggy::ffi::make_message(
            to_payload("first-partition-offset-test-" + std::to_string(index)), rust::Vec<iggy::ffi::HeaderEntry>{}));
        second_partition_messages.push_back(iggy::ffi::make_message(
            to_payload("second-partition-offset-test-" + std::to_string(index)), rust::Vec<iggy::ffi::HeaderEntry>{}));
    }
    ASSERT_NO_THROW(message_client->send_messages(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), "partition_id",
                                                  partition_id_bytes(0), std::move(first_partition_messages)));
    ASSERT_NO_THROW(message_client->send_messages(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), "partition_id",
                                                  partition_id_bytes(1), std::move(second_partition_messages)));
    ASSERT_NO_THROW(client.StoreConsumerOffset(first_consumer, iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), 0, 1));
    ASSERT_NO_THROW(client.StoreConsumerOffset(first_consumer, iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), 1, 2));
    ASSERT_NO_THROW(client.StoreConsumerOffset(second_consumer, iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), 0, 3));

    ASSERT_NO_THROW(client.DeleteConsumerOffset(first_consumer, iggy::Identifier::String(stream_name),
                                                iggy::Identifier::String(topic_name), 0));

    ASSERT_THROW(client.GetConsumerOffset(first_consumer, iggy::Identifier::String(stream_name),
                                          iggy::Identifier::String(topic_name), 0),
                 iggy::IggyException);
    EXPECT_EQ(client
                  .GetConsumerOffset(first_consumer, iggy::Identifier::String(stream_name),
                                     iggy::Identifier::String(topic_name), 1)
                  .StoredOffset(),
              2u);
    EXPECT_EQ(client
                  .GetConsumerOffset(second_consumer, iggy::Identifier::String(stream_name),
                                     iggy::Identifier::String(topic_name), 0)
                  .StoredOffset(),
              3u);
}

TEST_F(E2E_ConsumerGroup, DeleteConsumerOffsetBeforeLoginThrows) {
    RecordProperty("description", "Rejects deleting an offset before login and after disconnect.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    auto setup_client             = GetLoggedInHighLevelClient();
    auto *message_client          = GetLoggedInClient();
    auto client                   = GetLoggedOutHighLevelClient();
    const auto first_consumer     = iggy::Consumer::Single(iggy::Identifier::Numeric(1));
    const auto second_consumer    = iggy::Consumer::Single(iggy::Identifier::Numeric(2));

    ASSERT_NO_THROW(setup_client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(setup_client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                             iggy::TopicCreateOptions().SetPartitionsCount(1)));
    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    messages.push_back(iggy::ffi::make_message(to_payload("offset-test"), rust::Vec<iggy::ffi::HeaderEntry>{}));
    ASSERT_NO_THROW(message_client->send_messages(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), "partition_id",
                                                  partition_id_bytes(0), std::move(messages)));
    ASSERT_NO_THROW(setup_client.StoreConsumerOffset(first_consumer, iggy::Identifier::String(stream_name),
                                                     iggy::Identifier::String(topic_name), 0, 0));
    ASSERT_NO_THROW(setup_client.StoreConsumerOffset(second_consumer, iggy::Identifier::String(stream_name),
                                                     iggy::Identifier::String(topic_name), 0, 0));

    ASSERT_THROW(client.DeleteConsumerOffset(first_consumer, iggy::Identifier::String(stream_name),
                                             iggy::Identifier::String(topic_name), 0),
                 iggy::IggyException);
    ASSERT_NO_THROW(client.Connect());
    ASSERT_THROW(client.DeleteConsumerOffset(first_consumer, iggy::Identifier::String(stream_name),
                                             iggy::Identifier::String(topic_name), 0),
                 iggy::IggyException);
    ASSERT_NO_THROW(client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(client.DeleteConsumerOffset(first_consumer, iggy::Identifier::String(stream_name),
                                                iggy::Identifier::String(topic_name), 0));
    ASSERT_NO_THROW(client.Disconnect());
    ASSERT_THROW(client.DeleteConsumerOffset(second_consumer, iggy::Identifier::String(stream_name),
                                             iggy::Identifier::String(topic_name), 0),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, DeleteConsumerOffsetOnNonExistentResourcesThrows) {
    RecordProperty("description", "Rejects missing streams, topics, and partitions when deleting an offset.");
    const std::string stream_name         = GetRandomName();
    const std::string topic_name          = GetRandomName();
    const std::string missing_stream_name = GetRandomName();
    const std::string missing_topic_name  = GetRandomName();
    auto client                           = GetLoggedInHighLevelClient();
    const auto consumer                   = iggy::Consumer::Single(iggy::Identifier::Numeric(1));

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));

    ASSERT_THROW(client.DeleteConsumerOffset(consumer, iggy::Identifier::String(missing_stream_name),
                                             iggy::Identifier::String(topic_name), 0),
                 iggy::IggyException);
    ASSERT_THROW(client.DeleteConsumerOffset(consumer, iggy::Identifier::String(stream_name),
                                             iggy::Identifier::String(missing_topic_name), 0),
                 iggy::IggyException);
    ASSERT_THROW(client.DeleteConsumerOffset(consumer, iggy::Identifier::String(stream_name),
                                             iggy::Identifier::String(topic_name), 1),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, DeleteConsumerOffsetWithoutPermissionThrowsWithoutRemovingOffset) {
    RecordProperty("description", "Rejects an unauthorized offset deletion without removing the existing value.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string username    = GetRandomName(50);
    const std::string password    = "secret123";
    auto client                   = GetLoggedInHighLevelClient();
    auto *message_client          = GetLoggedInClient();
    auto *user_admin_client       = GetLoggedInClient();
    const auto consumer           = iggy::Consumer::Single(iggy::Identifier::Numeric(1));

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t index = 0; index < 3; ++index) {
        messages.push_back(iggy::ffi::make_message(to_payload("offset-test-" + std::to_string(index)),
                                                   rust::Vec<iggy::ffi::HeaderEntry>{}));
    }
    ASSERT_NO_THROW(message_client->send_messages(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), "partition_id",
                                                  partition_id_bytes(0), std::move(messages)));
    ASSERT_NO_THROW(client.StoreConsumerOffset(consumer, iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), 0, 1));
    ASSERT_NO_THROW(CreateUser(user_admin_client, username, password, iggy::ffi::UserStatus::Active, true,
                               iggy::ffi::Permissions{}));
    auto restricted_client = GetLoggedInHighLevelClient(username, password);

    ASSERT_THROW(restricted_client.DeleteConsumerOffset(consumer, iggy::Identifier::String(stream_name),
                                                        iggy::Identifier::String(topic_name), 0),
                 iggy::IggyException);
    EXPECT_EQ(
        client
            .GetConsumerOffset(consumer, iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name), 0)
            .StoredOffset(),
        1u);
}

TEST_F(E2E_ConsumerGroup, DeleteConsumerOffsetForOwnedConsumerGroupPartitionSucceeds) {
    RecordProperty("description", "Deletes an offset for a consumer group partition owned by the current client.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    auto *message_client          = GetLoggedInClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    messages.push_back(iggy::ffi::make_message(to_payload("offset-test"), rust::Vec<iggy::ffi::HeaderEntry>{}));
    ASSERT_NO_THROW(message_client->send_messages(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), "partition_id",
                                                  partition_id_bytes(0), std::move(messages)));
    ASSERT_NO_THROW(client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), group_name));
    TrackConsumerGroup(stream_name, topic_name, group_name);
    ASSERT_NO_THROW(client.JoinConsumerGroup(iggy::Identifier::String(stream_name),
                                             iggy::Identifier::String(topic_name),
                                             iggy::Identifier::String(group_name)));
    const auto consumer_group = iggy::Consumer::Group(iggy::Identifier::String(group_name));
    ASSERT_NO_THROW(client.StoreConsumerOffset(consumer_group, iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), 0, 0));

    ASSERT_NO_THROW(client.DeleteConsumerOffset(consumer_group, iggy::Identifier::String(stream_name),
                                                iggy::Identifier::String(topic_name), 0));
    ASSERT_THROW(client.GetConsumerOffset(consumer_group, iggy::Identifier::String(stream_name),
                                          iggy::Identifier::String(topic_name), 0),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, DeleteConsumerGroupOffsetForUnownedPartitionThrowsWithoutRemovingOffset) {
    RecordProperty("description", "Rejects deleting a group offset from an unowned partition without removing it.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    auto owner_client             = GetLoggedInHighLevelClient();
    auto non_member_client        = GetLoggedInHighLevelClient();
    auto *message_client          = GetLoggedInClient();

    ASSERT_NO_THROW(owner_client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(owner_client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                             iggy::TopicCreateOptions().SetPartitionsCount(1)));
    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t index = 0; index < 3; ++index) {
        messages.push_back(iggy::ffi::make_message(to_payload("offset-test-" + std::to_string(index)),
                                                   rust::Vec<iggy::ffi::HeaderEntry>{}));
    }
    ASSERT_NO_THROW(message_client->send_messages(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), "partition_id",
                                                  partition_id_bytes(0), std::move(messages)));
    ASSERT_NO_THROW(owner_client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                                     iggy::Identifier::String(topic_name), group_name));
    TrackConsumerGroup(stream_name, topic_name, group_name);
    ASSERT_NO_THROW(owner_client.JoinConsumerGroup(iggy::Identifier::String(stream_name),
                                                   iggy::Identifier::String(topic_name),
                                                   iggy::Identifier::String(group_name)));
    const auto consumer_group = iggy::Consumer::Group(iggy::Identifier::String(group_name));
    ASSERT_NO_THROW(owner_client.StoreConsumerOffset(consumer_group, iggy::Identifier::String(stream_name),
                                                     iggy::Identifier::String(topic_name), 0, 1));

    ASSERT_THROW(non_member_client.DeleteConsumerOffset(consumer_group, iggy::Identifier::String(stream_name),
                                                        iggy::Identifier::String(topic_name), 0),
                 iggy::IggyException);
    EXPECT_EQ(owner_client
                  .GetConsumerOffset(consumer_group, iggy::Identifier::String(stream_name),
                                     iggy::Identifier::String(topic_name), 0)
                  .StoredOffset(),
              1u);
}

TEST_F(E2E_ConsumerGroup, DeleteConsumerOffsetForNonExistentConsumerGroupThrows) {
    RecordProperty("description", "Rejects deleting offsets for named and numeric consumer groups that do not exist.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));

    const auto missing_by_name = iggy::Consumer::Group(iggy::Identifier::String(GetRandomName()));
    const auto missing_by_id   = iggy::Consumer::Group(iggy::Identifier::Numeric(999'999));
    for (const auto *consumer_group : {&missing_by_name, &missing_by_id}) {
        ASSERT_THROW(client.DeleteConsumerOffset(*consumer_group, iggy::Identifier::String(stream_name),
                                                 iggy::Identifier::String(topic_name), 0),
                     iggy::IggyException);
    }
}

TEST_F(E2E_ConsumerGroup, DeleteConsumerOffsetSupportsNamedAndNumericIdentifiers) {
    RecordProperty("description", "Deletes offsets using named and numeric stream, topic, and consumer identifiers.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    auto *message_client          = GetLoggedInClient();

    const auto stream = client.CreateStream(stream_name);
    TrackStream(stream_name);
    const auto topic = client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                          iggy::TopicCreateOptions().SetPartitionsCount(1));
    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t index = 0; index < 2; ++index) {
        messages.push_back(iggy::ffi::make_message(to_payload("offset-test-" + std::to_string(index)),
                                                   rust::Vec<iggy::ffi::HeaderEntry>{}));
    }
    ASSERT_NO_THROW(message_client->send_messages(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), "partition_id",
                                                  partition_id_bytes(0), std::move(messages)));

    const auto named_consumer   = iggy::Consumer::Single(iggy::Identifier::String(GetRandomName()));
    const auto numeric_consumer = iggy::Consumer::Single(iggy::Identifier::Numeric(42));
    ASSERT_NO_THROW(client.StoreConsumerOffset(named_consumer, iggy::Identifier::String(stream_name),
                                               iggy::Identifier::String(topic_name), 0, 0));
    ASSERT_NO_THROW(client.StoreConsumerOffset(numeric_consumer, iggy::Identifier::Numeric(stream.Id()),
                                               iggy::Identifier::Numeric(topic.Id()), 0, 1));

    ASSERT_NO_THROW(client.DeleteConsumerOffset(named_consumer, iggy::Identifier::Numeric(stream.Id()),
                                                iggy::Identifier::Numeric(topic.Id()), 0));
    ASSERT_NO_THROW(client.DeleteConsumerOffset(numeric_consumer, iggy::Identifier::String(stream_name),
                                                iggy::Identifier::String(topic_name), 0));
    ASSERT_THROW(client.GetConsumerOffset(named_consumer, iggy::Identifier::String(stream_name),
                                          iggy::Identifier::String(topic_name), 0),
                 iggy::IggyException);
    ASSERT_THROW(client.GetConsumerOffset(numeric_consumer, iggy::Identifier::Numeric(stream.Id()),
                                          iggy::Identifier::Numeric(topic.Id()), 0),
                 iggy::IggyException);
}

TEST_F(E2E_ConsumerGroup, DeleteConsumerOffsetRemovesAutoCommittedOffset) {
    RecordProperty("description", "Deletes an offset created by polling messages with auto-commit enabled.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    auto *message_client          = GetLoggedInClient();
    const auto consumer           = iggy::Consumer::Single(iggy::Identifier::Numeric(88));

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t index = 0; index < 5; ++index) {
        messages.push_back(iggy::ffi::make_message(to_payload("offset-test-" + std::to_string(index)),
                                                   rust::Vec<iggy::ffi::HeaderEntry>{}));
    }
    ASSERT_NO_THROW(message_client->send_messages(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), "partition_id",
                                                  partition_id_bytes(0), std::move(messages)));

    iggy::ffi::PolledMessages polled{};
    ASSERT_NO_THROW(polled = message_client->poll_messages(make_string_identifier(stream_name),
                                                           make_string_identifier(topic_name), 0, "consumer",
                                                           make_numeric_identifier(88), "next", 0, 3, true));
    ASSERT_EQ(polled.count, 3u);
    EXPECT_EQ(
        client
            .GetConsumerOffset(consumer, iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name), 0)
            .StoredOffset(),
        2u);

    ASSERT_NO_THROW(client.DeleteConsumerOffset(consumer, iggy::Identifier::String(stream_name),
                                                iggy::Identifier::String(topic_name), 0));
    ASSERT_THROW(client.GetConsumerOffset(consumer, iggy::Identifier::String(stream_name),
                                          iggy::Identifier::String(topic_name), 0),
                 iggy::IggyException);
}
