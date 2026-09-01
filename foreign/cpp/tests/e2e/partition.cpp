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

#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "iggy.hpp"
#include "tests/e2e/test_helpers.hpp"

class E2E_Partition : public E2ETestFixture {};

TEST_F(E2E_Partition, CreatePartitionsSucceeds) {
    RecordProperty("description", "Creates partitions for an existing topic and verifies the resulting count.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name, 1));
    ASSERT_NO_THROW(
        client.CreatePartitions(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name), 43));

    ASSERT_NO_THROW({
        const auto stream_details = client.GetStream(iggy::Identifier::String(stream_name));
        ASSERT_EQ(stream_details.Topics().size(), 1u);
        EXPECT_EQ(stream_details.Topics()[0].Name(), topic_name);
        EXPECT_EQ(stream_details.Topics()[0].PartitionsCount(), 44u);
    });
}

TEST_F(E2E_Partition, CreatePartitionsBeforeLoginThrows) {
    RecordProperty("description",
                   "Throws when create_partitions is called before connect, and after connect but before login.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedOutHighLevelClient();

    ASSERT_THROW(
        client.CreatePartitions(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name), 1),
        std::exception);
    ASSERT_NO_THROW(client.Connect());
    ASSERT_THROW(
        client.CreatePartitions(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name), 1),
        std::exception);
    ASSERT_NO_THROW(client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(client.Disconnect());
    ASSERT_THROW(
        client.CreatePartitions(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name), 1),
        std::exception);
}

TEST_F(E2E_Partition, CreatePartitionsOnNonExistentResourcesThrows) {
    RecordProperty("description", "Throws when create_partitions is called for a stream or topic that does not exist.");
    const std::string stream_name         = GetRandomName();
    const std::string topic_name          = GetRandomName();
    const std::string missing_stream_name = GetRandomName();
    const std::string missing_topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name, 1));

    ASSERT_THROW(
        client.CreatePartitions(iggy::Identifier::String(missing_stream_name), iggy::Identifier::String(topic_name), 1),
        std::exception);
    ASSERT_THROW(
        client.CreatePartitions(iggy::Identifier::String(stream_name), iggy::Identifier::String(missing_topic_name), 1),
        std::exception);
}

TEST_F(E2E_Partition, CreatePartitionsWithInvalidIdentifiersThrows) {
    RecordProperty("description", "Rejects invalid stream or topic identifiers before creating partitions.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name, 1));

    ASSERT_THROW(client.CreatePartitions(iggy::Identifier::String(""), iggy::Identifier::String(topic_name), 1),
                 std::exception);
    ASSERT_THROW(client.CreatePartitions(iggy::Identifier::String(std::string(256, 'a')),
                                         iggy::Identifier::String(topic_name), 1),
                 std::exception);
    ASSERT_THROW(client.CreatePartitions(iggy::Identifier::String(stream_name), iggy::Identifier::String(""), 1),
                 std::exception);
    ASSERT_THROW(client.CreatePartitions(iggy::Identifier::String(stream_name),
                                         iggy::Identifier::String(std::string(256, 'a')), 1),
                 std::exception);
}

TEST_F(E2E_Partition, CreatePartitionsWithBoundaryPartitionsCountValues) {
    RecordProperty("description",
                   "Accepts supported create_partitions counts and rejects values outside the allowed range.");
    const std::string stream_name = GetRandomName();

    auto client = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);

    struct TestCase {
        std::string topic_name;
        std::uint32_t partitions_count;
        bool should_succeed;
        std::uint32_t expected_total_partitions;
    };

    const std::vector<TestCase> test_cases = {
        {GetRandomName(), static_cast<std::uint32_t>(-1), false, 1},
        {GetRandomName(), 0, false, 1},
        {GetRandomName(), 1, true, 2},
        {GetRandomName(), 43, true, 44},
        {GetRandomName(), 1000, true, 1001},
        {GetRandomName(), 1001, false, 1},
    };

    for (const auto &test_case : test_cases) {
        SCOPED_TRACE(test_case.topic_name);
        ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), test_case.topic_name, 1));

        if (test_case.should_succeed) {
            ASSERT_NO_THROW(client.CreatePartitions(iggy::Identifier::String(stream_name),
                                                    iggy::Identifier::String(test_case.topic_name),
                                                    test_case.partitions_count));
        } else {
            ASSERT_THROW(
                client.CreatePartitions(iggy::Identifier::String(stream_name),
                                        iggy::Identifier::String(test_case.topic_name), test_case.partitions_count),
                std::exception);
        }
    }

    ASSERT_NO_THROW({
        const auto stream_details = client.GetStream(iggy::Identifier::String(stream_name));
        ASSERT_EQ(stream_details.Topics().size(), test_cases.size());
        for (const auto &test_case : test_cases) {
            bool found = false;
            for (const auto &topic : stream_details.Topics()) {
                if (topic.Name() == test_case.topic_name) {
                    EXPECT_EQ(topic.PartitionsCount(), test_case.expected_total_partitions);
                    found = true;
                    break;
                }
            }
            EXPECT_TRUE(found) << "Missing topic " << test_case.topic_name;
        }
    });
}

TEST_F(E2E_Partition, CreatePartitionsWithNumericIdentifiersSucceeds) {
    RecordProperty("description",
                   "Creates partitions successfully when valid numeric stream and topic identifiers are used.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name, 1));

    const auto stream_details = client.GetStream(iggy::Identifier::String(stream_name));
    ASSERT_EQ(stream_details.Topics().size(), 1u);

    ASSERT_NO_THROW(client.CreatePartitions(iggy::Identifier::Numeric(stream_details.Id()),
                                            iggy::Identifier::Numeric(stream_details.Topics()[0].Id()), 43));

    ASSERT_NO_THROW({
        const auto updated_stream_details = client.GetStream(iggy::Identifier::Numeric(stream_details.Id()));
        ASSERT_EQ(updated_stream_details.Topics().size(), 1u);
        EXPECT_EQ(updated_stream_details.Topics()[0].Id(), stream_details.Topics()[0].Id());
        EXPECT_EQ(updated_stream_details.Topics()[0].Name(), topic_name);
        EXPECT_EQ(updated_stream_details.Topics()[0].PartitionsCount(), 44u);
    });
}

TEST_F(E2E_Partition, DeletePartitionsSucceeds) {
    RecordProperty("description", "Deletes partitions from an existing topic and verifies the resulting count.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name, 44));
    ASSERT_NO_THROW(
        client.DeletePartitions(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name), 43));

    ASSERT_NO_THROW({
        const auto stream_details = client.GetStream(iggy::Identifier::String(stream_name));
        ASSERT_EQ(stream_details.Topics().size(), 1u);
        EXPECT_EQ(stream_details.Topics()[0].Name(), topic_name);
        EXPECT_EQ(stream_details.Topics()[0].PartitionsCount(), 1u);
    });
}

TEST_F(E2E_Partition, DeleteMorePartitionsThanExistingThrows) {
    RecordProperty("description",
                   "Rejects delete_partitions counts outside the allowed range and counts greater than existing.");
    const std::string stream_name = GetRandomName();

    auto client = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);

    struct TestCase {
        std::string topic_name;
        std::uint32_t partitions_count;
        bool should_succeed;
        std::uint32_t initial_partitions;
        std::uint32_t expected_total_partitions;
    };

    const std::vector<TestCase> test_cases = {
        {GetRandomName(), static_cast<std::uint32_t>(-1), false, 3, 3},
        {GetRandomName(), 0, false, 3, 3},
        {GetRandomName(), 1, true, 3, 2},
        {GetRandomName(), 4, false, 3, 3},
    };

    for (const auto &test_case : test_cases) {
        SCOPED_TRACE(test_case.topic_name);
        ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), test_case.topic_name,
                                           test_case.initial_partitions));

        if (test_case.should_succeed) {
            ASSERT_NO_THROW(client.DeletePartitions(iggy::Identifier::String(stream_name),
                                                    iggy::Identifier::String(test_case.topic_name),
                                                    test_case.partitions_count));
        } else {
            ASSERT_THROW(
                client.DeletePartitions(iggy::Identifier::String(stream_name),
                                        iggy::Identifier::String(test_case.topic_name), test_case.partitions_count),
                std::exception);
        }
    }

    ASSERT_NO_THROW({
        const auto stream_details = client.GetStream(iggy::Identifier::String(stream_name));
        ASSERT_EQ(stream_details.Topics().size(), test_cases.size());
        for (const auto &test_case : test_cases) {
            bool found = false;
            for (const auto &topic : stream_details.Topics()) {
                if (topic.Name() == test_case.topic_name) {
                    EXPECT_EQ(topic.PartitionsCount(), test_case.expected_total_partitions);
                    found = true;
                    break;
                }
            }
            EXPECT_TRUE(found) << "Missing topic " << test_case.topic_name;
        }
    });
}

TEST_F(E2E_Partition, DeletePartitionsBeforeCreatingAdditionalPartitionsSucceeds) {
    RecordProperty("description",
                   "Deletes partitions from the initial topic allocation without calling create_partitions first.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name, 3));
    ASSERT_NO_THROW(
        client.DeletePartitions(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name), 1));

    ASSERT_NO_THROW({
        const auto stream_details = client.GetStream(iggy::Identifier::String(stream_name));
        ASSERT_EQ(stream_details.Topics().size(), 1u);
        EXPECT_EQ(stream_details.Topics()[0].PartitionsCount(), 2u);
    });
}

TEST_F(E2E_Partition, DeletePartitionsFromTopicWithZeroPartitionsThrows) {
    RecordProperty("description",
                   "Throws when delete_partitions is called with count 1 for a topic that currently has 0 partitions.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name, 0));

    ASSERT_THROW(
        client.DeletePartitions(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name), 1),
        std::exception);

    ASSERT_NO_THROW({
        const auto stream_details = client.GetStream(iggy::Identifier::String(stream_name));
        ASSERT_EQ(stream_details.Topics().size(), 1u);
        EXPECT_EQ(stream_details.Topics()[0].Name(), topic_name);
        EXPECT_EQ(stream_details.Topics()[0].PartitionsCount(), 0u);
    });
}

TEST_F(E2E_Partition, DeletePartitionsBeforeLoginThrows) {
    RecordProperty("description",
                   "Throws when delete_partitions is called before connect, and after connect but before login.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedOutHighLevelClient();

    ASSERT_THROW(
        client.DeletePartitions(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name), 1),
        std::exception);
    ASSERT_NO_THROW(client.Connect());
    ASSERT_THROW(
        client.DeletePartitions(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name), 1),
        std::exception);
    ASSERT_NO_THROW(client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(client.Disconnect());
    ASSERT_THROW(
        client.DeletePartitions(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name), 1),
        std::exception);
}

TEST_F(E2E_Partition, DeletePartitionsOnNonExistentResourcesThrows) {
    RecordProperty("description", "Throws when delete_partitions is called for a stream or topic that does not exist.");
    const std::string stream_name         = GetRandomName();
    const std::string topic_name          = GetRandomName();
    const std::string missing_stream_name = GetRandomName();
    const std::string missing_topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name, 3));

    ASSERT_THROW(
        client.DeletePartitions(iggy::Identifier::String(missing_stream_name), iggy::Identifier::String(topic_name), 1),
        std::exception);
    ASSERT_THROW(
        client.DeletePartitions(iggy::Identifier::String(stream_name), iggy::Identifier::String(missing_topic_name), 1),
        std::exception);
}

TEST_F(E2E_Partition, DeletePartitionsWithInvalidIdentifiersThrows) {
    RecordProperty("description", "Rejects invalid stream or topic identifiers before deleting partitions.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name, 3));

    ASSERT_THROW(client.DeletePartitions(iggy::Identifier::String(""), iggy::Identifier::String(topic_name), 1),
                 std::exception);
    ASSERT_THROW(client.DeletePartitions(iggy::Identifier::String(std::string(256, 'a')),
                                         iggy::Identifier::String(topic_name), 1),
                 std::exception);
    ASSERT_THROW(client.DeletePartitions(iggy::Identifier::String(stream_name), iggy::Identifier::String(""), 1),
                 std::exception);
    ASSERT_THROW(client.DeletePartitions(iggy::Identifier::String(stream_name),
                                         iggy::Identifier::String(std::string(256, 'a')), 1),
                 std::exception);
}

TEST_F(E2E_Partition, DeletePartitionsTwiceForSameTopicSucceeds) {
    RecordProperty("description", "Allows delete_partitions to be called twice for the same stream and topic.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name, 45));
    ASSERT_NO_THROW(
        client.DeletePartitions(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name), 20));
    ASSERT_NO_THROW(
        client.DeletePartitions(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name), 20));

    ASSERT_NO_THROW({
        const auto stream_details = client.GetStream(iggy::Identifier::String(stream_name));
        ASSERT_EQ(stream_details.Topics().size(), 1u);
        EXPECT_EQ(stream_details.Topics()[0].Name(), topic_name);
        EXPECT_EQ(stream_details.Topics()[0].PartitionsCount(), 5u);
    });
}

TEST_F(E2E_Partition, DeletePartitionsAfterStreamDeletionThrows) {
    RecordProperty("description", "Throws when delete_partitions is called after the stream has been deleted.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name, 3));

    const auto stream_details = client.GetStream(iggy::Identifier::String(stream_name));
    ASSERT_EQ(stream_details.Topics().size(), 1u);

    ASSERT_NO_THROW(client.DeleteStream(iggy::Identifier::Numeric(stream_details.Id())));
    ForgetTrackedStream(stream_name);

    ASSERT_THROW(client.DeletePartitions(iggy::Identifier::Numeric(stream_details.Id()),
                                         iggy::Identifier::Numeric(stream_details.Topics()[0].Id()), 1),
                 std::exception);
}

TEST_F(E2E_Partition, CreatePartitionsAfterTopicDeletionThrows) {
    RecordProperty("description", "Throws when creating partitions after the topic has been deleted.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name, 1));
    ASSERT_NO_THROW(client.DeleteTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name)));

    ASSERT_THROW(
        client.CreatePartitions(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name), 1),
        std::exception);
}
