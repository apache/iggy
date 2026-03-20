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
#include <vector>

#include <gtest/gtest.h>

#include "lib.rs.h"
#include "tests/common/test_helpers.hpp"

TEST(LowLevelE2E_Partition, CreatePartitionsSucceeds) {
    RecordProperty("description", "Creates partitions for an existing topic and verifies the resulting count.");
    const std::string stream_name = "cpp-create-partitions-happy-path";
    const std::string topic_name  = "topic-create-partitions-happy-path";

    iggy::ffi::Client *client = login_to_server();
    ASSERT_NE(client, nullptr);

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(
        client->create_partitions(make_string_identifier(stream_name), make_string_identifier(topic_name), 43));

    ASSERT_NO_THROW({
        const auto stream_details = client->get_stream(make_string_identifier(stream_name));
        ASSERT_EQ(stream_details.topics.size(), 1u);
        EXPECT_EQ(stream_details.topics[0].name, topic_name);
        EXPECT_EQ(stream_details.topics[0].partitions_count, 44u);
    });

    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Partition, CreatePartitionsBeforeLoginThrows) {
    RecordProperty("description",
                   "Throws when create_partitions is called before connect, and after connect but before login.");
    const std::string stream_name = "cpp-create-partitions-before-login";
    const std::string topic_name  = "topic-create-partitions-before-login";

    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);

    ASSERT_THROW(client->create_partitions(make_string_identifier(stream_name), make_string_identifier(topic_name), 1),
                 std::exception);
    ASSERT_NO_THROW(client->connect());
    ASSERT_THROW(client->create_partitions(make_string_identifier(stream_name), make_string_identifier(topic_name), 1),
                 std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Partition, CreatePartitionsOnNonExistentResourcesThrows) {
    RecordProperty("description", "Throws when create_partitions is called for a stream or topic that does not exist.");
    const std::string stream_name         = "cpp-create-partitions-missing-resource-stream";
    const std::string topic_name          = "topic-create-partitions-missing-resource-topic";
    const std::string missing_stream_name = "cpp-create-partitions-missing-stream";
    const std::string missing_topic_name  = "topic-create-partitions-missing-topic";

    iggy::ffi::Client *client = login_to_server();
    ASSERT_NE(client, nullptr);

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));

    ASSERT_THROW(
        client->create_partitions(make_string_identifier(missing_stream_name), make_string_identifier(topic_name), 1),
        std::exception);
    ASSERT_THROW(
        client->create_partitions(make_string_identifier(stream_name), make_string_identifier(missing_topic_name), 1),
        std::exception);

    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Partition, CreatePartitionsWithInvalidIdentifiersThrows) {
    RecordProperty("description", "Rejects create_partitions requests that use invalid stream or topic identifiers.");
    const std::string stream_name = "cpp-create-partitions-invalid-identifier";
    const std::string topic_name  = "topic-create-partitions-invalid-identifier";

    iggy::ffi::Client *client = login_to_server();
    ASSERT_NE(client, nullptr);

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));

    iggy::ffi::Identifier invalid_stream_kind_id;
    invalid_stream_kind_id.kind   = "invalid";
    invalid_stream_kind_id.length = 4;
    invalid_stream_kind_id.value  = {1, 0, 0, 0};
    ASSERT_THROW(client->create_partitions(std::move(invalid_stream_kind_id), make_string_identifier(topic_name), 1),
                 std::exception);

    iggy::ffi::Identifier invalid_stream_numeric_id;
    invalid_stream_numeric_id.kind   = "numeric";
    invalid_stream_numeric_id.length = 1;
    invalid_stream_numeric_id.value.push_back(1);
    ASSERT_THROW(client->create_partitions(std::move(invalid_stream_numeric_id), make_string_identifier(topic_name), 1),
                 std::exception);

    iggy::ffi::Identifier invalid_topic_kind_id;
    invalid_topic_kind_id.kind   = "invalid";
    invalid_topic_kind_id.length = 4;
    invalid_topic_kind_id.value  = {1, 0, 0, 0};
    ASSERT_THROW(client->create_partitions(make_string_identifier(stream_name), std::move(invalid_topic_kind_id), 1),
                 std::exception);

    iggy::ffi::Identifier invalid_topic_numeric_id;
    invalid_topic_numeric_id.kind   = "numeric";
    invalid_topic_numeric_id.length = 1;
    invalid_topic_numeric_id.value.push_back(1);
    ASSERT_THROW(client->create_partitions(make_string_identifier(stream_name), std::move(invalid_topic_numeric_id), 1),
                 std::exception);

    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Partition, CreatePartitionsWithBoundaryPartitionsCountValues) {
    RecordProperty("description",
                   "Accepts supported create_partitions counts and rejects values outside the allowed range.");
    const std::string stream_name = "cpp-create-partitions-boundary-values";

    iggy::ffi::Client *client = login_to_server();
    ASSERT_NE(client, nullptr);

    ASSERT_NO_THROW(client->create_stream(stream_name));

    struct TestCase {
        std::string topic_name;
        std::uint32_t partitions_count;
        bool should_succeed;
        std::uint32_t expected_total_partitions;
    };

    const std::vector<TestCase> test_cases = {
        {"topic-partitions-minus-1", static_cast<std::uint32_t>(-1), false, 1},
        {"topic-partitions-0", 0, false, 1},
        {"topic-partitions-1", 1, true, 2},
        {"topic-partitions-43", 43, true, 44},
        {"topic-partitions-1000", 1000, true, 1001},
        {"topic-partitions-1001", 1001, false, 1},
    };

    for (const auto &test_case : test_cases) {
        SCOPED_TRACE(test_case.topic_name);
        ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), test_case.topic_name, 1, "none", 0,
                                             "server_default", 0, "server_default"));

        if (test_case.should_succeed) {
            ASSERT_NO_THROW(client->create_partitions(make_string_identifier(stream_name),
                                                      make_string_identifier(test_case.topic_name),
                                                      test_case.partitions_count));
        } else {
            ASSERT_THROW(
                client->create_partitions(make_string_identifier(stream_name),
                                          make_string_identifier(test_case.topic_name), test_case.partitions_count),
                std::exception);
        }
    }

    ASSERT_NO_THROW({
        const auto stream_details = client->get_stream(make_string_identifier(stream_name));
        ASSERT_EQ(stream_details.topics.size(), test_cases.size());
        for (const auto &test_case : test_cases) {
            bool found = false;
            for (const auto &topic : stream_details.topics) {
                if (topic.name == test_case.topic_name) {
                    EXPECT_EQ(topic.partitions_count, test_case.expected_total_partitions);
                    found = true;
                    break;
                }
            }
            EXPECT_TRUE(found) << "Missing topic " << test_case.topic_name;
        }
    });

    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Partition, CreatePartitionsWithNumericIdentifiersSucceeds) {
    RecordProperty("description",
                   "Creates partitions successfully when valid numeric stream and topic identifiers are used.");
    const std::string stream_name = "cpp-create-partitions-numeric-identifiers";
    const std::string topic_name  = "topic-create-partitions-numeric-identifiers";

    iggy::ffi::Client *client = login_to_server();
    ASSERT_NE(client, nullptr);

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));

    const auto stream_details = client->get_stream(make_string_identifier(stream_name));
    ASSERT_EQ(stream_details.topics.size(), 1u);

    ASSERT_NO_THROW(client->create_partitions(make_numeric_identifier(stream_details.id),
                                              make_numeric_identifier(stream_details.topics[0].id), 43));

    ASSERT_NO_THROW({
        const auto updated_stream_details = client->get_stream(make_numeric_identifier(stream_details.id));
        ASSERT_EQ(updated_stream_details.topics.size(), 1u);
        EXPECT_EQ(updated_stream_details.topics[0].id, stream_details.topics[0].id);
        EXPECT_EQ(updated_stream_details.topics[0].name, topic_name);
        EXPECT_EQ(updated_stream_details.topics[0].partitions_count, 44u);
    });

    ASSERT_NO_THROW(client->delete_stream(make_numeric_identifier(stream_details.id)));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Partition, DeletePartitionsSucceeds) {
    RecordProperty("description", "Deletes partitions from an existing topic and verifies the resulting count.");
    const std::string stream_name = "cpp-delete-partitions-happy-path";
    const std::string topic_name  = "topic-delete-partitions-happy-path";

    iggy::ffi::Client *client = login_to_server();
    ASSERT_NE(client, nullptr);

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 44, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(
        client->delete_partitions(make_string_identifier(stream_name), make_string_identifier(topic_name), 43));

    ASSERT_NO_THROW({
        const auto stream_details = client->get_stream(make_string_identifier(stream_name));
        ASSERT_EQ(stream_details.topics.size(), 1u);
        EXPECT_EQ(stream_details.topics[0].name, topic_name);
        EXPECT_EQ(stream_details.topics[0].partitions_count, 1u);
    });

    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Partition, DeleteMorePartitionsThanExistingThrows) {
    RecordProperty("description",
                   "Rejects delete_partitions counts outside the allowed range and counts greater than existing.");
    const std::string stream_name = "cpp-delete-partitions-boundary-values";

    iggy::ffi::Client *client = login_to_server();
    ASSERT_NE(client, nullptr);

    ASSERT_NO_THROW(client->create_stream(stream_name));

    struct TestCase {
        std::string topic_name;
        std::uint32_t partitions_count;
        bool should_succeed;
        std::uint32_t initial_partitions;
        std::uint32_t expected_total_partitions;
    };

    const std::vector<TestCase> test_cases = {
        {"topic-delete-partitions-minus-1", static_cast<std::uint32_t>(-1), false, 3, 3},
        {"topic-delete-partitions-0", 0, false, 3, 3},
        {"topic-delete-partitions-1", 1, true, 3, 2},
        {"topic-delete-partitions-4", 4, false, 3, 3},
    };

    for (const auto &test_case : test_cases) {
        SCOPED_TRACE(test_case.topic_name);
        ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), test_case.topic_name,
                                             test_case.initial_partitions, "none", 0, "server_default", 0,
                                             "server_default"));

        if (test_case.should_succeed) {
            ASSERT_NO_THROW(client->delete_partitions(make_string_identifier(stream_name),
                                                      make_string_identifier(test_case.topic_name),
                                                      test_case.partitions_count));
        } else {
            ASSERT_THROW(
                client->delete_partitions(make_string_identifier(stream_name),
                                          make_string_identifier(test_case.topic_name), test_case.partitions_count),
                std::exception);
        }
    }

    ASSERT_NO_THROW({
        const auto stream_details = client->get_stream(make_string_identifier(stream_name));
        ASSERT_EQ(stream_details.topics.size(), test_cases.size());
        for (const auto &test_case : test_cases) {
            bool found = false;
            for (const auto &topic : stream_details.topics) {
                if (topic.name == test_case.topic_name) {
                    EXPECT_EQ(topic.partitions_count, test_case.expected_total_partitions);
                    found = true;
                    break;
                }
            }
            EXPECT_TRUE(found) << "Missing topic " << test_case.topic_name;
        }
    });

    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Partition, DeletePartitionsBeforeCreatingAdditionalPartitionsSucceeds) {
    RecordProperty("description",
                   "Deletes partitions from the initial topic allocation without calling create_partitions first.");
    const std::string stream_name = "cpp-delete-partitions-before-create-partitions";
    const std::string topic_name  = "topic-delete-partitions-before-create-partitions";

    iggy::ffi::Client *client = login_to_server();
    ASSERT_NE(client, nullptr);

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 3, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(
        client->delete_partitions(make_string_identifier(stream_name), make_string_identifier(topic_name), 1));

    ASSERT_NO_THROW({
        const auto stream_details = client->get_stream(make_string_identifier(stream_name));
        ASSERT_EQ(stream_details.topics.size(), 1u);
        EXPECT_EQ(stream_details.topics[0].partitions_count, 2u);
    });

    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Partition, DeletePartitionsFromTopicWithZeroPartitionsThrows) {
    RecordProperty("description",
                   "Throws when delete_partitions is called with count 1 for a topic that currently has 0 partitions.");
    const std::string stream_name = "cpp-delete-partitions-zero-existing-partitions";
    const std::string topic_name  = "topic-delete-partitions-zero-existing-partitions";

    iggy::ffi::Client *client = login_to_server();
    ASSERT_NE(client, nullptr);

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 0, "none", 0,
                                         "server_default", 0, "server_default"));

    ASSERT_THROW(client->delete_partitions(make_string_identifier(stream_name), make_string_identifier(topic_name), 1),
                 std::exception);

    ASSERT_NO_THROW({
        const auto stream_details = client->get_stream(make_string_identifier(stream_name));
        ASSERT_EQ(stream_details.topics.size(), 1u);
        EXPECT_EQ(stream_details.topics[0].name, topic_name);
        EXPECT_EQ(stream_details.topics[0].partitions_count, 0u);
    });

    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Partition, DeletePartitionsBeforeLoginThrows) {
    RecordProperty("description",
                   "Throws when delete_partitions is called before connect, and after connect but before login.");
    const std::string stream_name = "cpp-delete-partitions-before-login";
    const std::string topic_name  = "topic-delete-partitions-before-login";

    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);

    ASSERT_THROW(client->delete_partitions(make_string_identifier(stream_name), make_string_identifier(topic_name), 1),
                 std::exception);
    ASSERT_NO_THROW(client->connect());
    ASSERT_THROW(client->delete_partitions(make_string_identifier(stream_name), make_string_identifier(topic_name), 1),
                 std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Partition, DeletePartitionsOnNonExistentResourcesThrows) {
    RecordProperty("description", "Throws when delete_partitions is called for a stream or topic that does not exist.");
    const std::string stream_name         = "cpp-delete-partitions-missing-resource-stream";
    const std::string topic_name          = "topic-delete-partitions-missing-resource-topic";
    const std::string missing_stream_name = "cpp-delete-partitions-missing-stream";
    const std::string missing_topic_name  = "topic-delete-partitions-missing-topic";

    iggy::ffi::Client *client = login_to_server();
    ASSERT_NE(client, nullptr);

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 3, "none", 0,
                                         "server_default", 0, "server_default"));

    ASSERT_THROW(
        client->delete_partitions(make_string_identifier(missing_stream_name), make_string_identifier(topic_name), 1),
        std::exception);
    ASSERT_THROW(
        client->delete_partitions(make_string_identifier(stream_name), make_string_identifier(missing_topic_name), 1),
        std::exception);

    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Partition, DeletePartitionsWithInvalidIdentifiersThrows) {
    RecordProperty("description", "Rejects delete_partitions requests that use invalid stream or topic identifiers.");
    const std::string stream_name = "cpp-delete-partitions-invalid-identifier";
    const std::string topic_name  = "topic-delete-partitions-invalid-identifier";

    iggy::ffi::Client *client = login_to_server();
    ASSERT_NE(client, nullptr);

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 3, "none", 0,
                                         "server_default", 0, "server_default"));

    iggy::ffi::Identifier invalid_stream_kind_id;
    invalid_stream_kind_id.kind   = "invalid";
    invalid_stream_kind_id.length = 4;
    invalid_stream_kind_id.value  = {1, 0, 0, 0};
    ASSERT_THROW(client->delete_partitions(std::move(invalid_stream_kind_id), make_string_identifier(topic_name), 1),
                 std::exception);

    iggy::ffi::Identifier invalid_stream_numeric_id;
    invalid_stream_numeric_id.kind   = "numeric";
    invalid_stream_numeric_id.length = 1;
    invalid_stream_numeric_id.value.push_back(1);
    ASSERT_THROW(client->delete_partitions(std::move(invalid_stream_numeric_id), make_string_identifier(topic_name), 1),
                 std::exception);

    iggy::ffi::Identifier invalid_topic_kind_id;
    invalid_topic_kind_id.kind   = "invalid";
    invalid_topic_kind_id.length = 4;
    invalid_topic_kind_id.value  = {1, 0, 0, 0};
    ASSERT_THROW(client->delete_partitions(make_string_identifier(stream_name), std::move(invalid_topic_kind_id), 1),
                 std::exception);

    iggy::ffi::Identifier invalid_topic_numeric_id;
    invalid_topic_numeric_id.kind   = "numeric";
    invalid_topic_numeric_id.length = 1;
    invalid_topic_numeric_id.value.push_back(1);
    ASSERT_THROW(client->delete_partitions(make_string_identifier(stream_name), std::move(invalid_topic_numeric_id), 1),
                 std::exception);

    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Partition, DeletePartitionsTwiceForSameTopicSucceeds) {
    RecordProperty("description", "Allows delete_partitions to be called twice for the same stream and topic.");
    const std::string stream_name = "cpp-delete-partitions-twice-same-topic";
    const std::string topic_name  = "topic-delete-partitions-twice-same-topic";

    iggy::ffi::Client *client = login_to_server();
    ASSERT_NE(client, nullptr);

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 45, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(
        client->delete_partitions(make_string_identifier(stream_name), make_string_identifier(topic_name), 20));
    ASSERT_NO_THROW(
        client->delete_partitions(make_string_identifier(stream_name), make_string_identifier(topic_name), 20));

    ASSERT_NO_THROW({
        const auto stream_details = client->get_stream(make_string_identifier(stream_name));
        ASSERT_EQ(stream_details.topics.size(), 1u);
        EXPECT_EQ(stream_details.topics[0].name, topic_name);
        EXPECT_EQ(stream_details.topics[0].partitions_count, 5u);
    });

    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Partition, DeletePartitionsAfterStreamDeletionThrows) {
    RecordProperty("description", "Throws when delete_partitions is called after the stream has been deleted.");
    const std::string stream_name = "cpp-delete-partitions-after-stream-deletion";
    const std::string topic_name  = "topic-delete-partitions-after-stream-deletion";

    iggy::ffi::Client *client = login_to_server();
    ASSERT_NE(client, nullptr);

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 3, "none", 0,
                                         "server_default", 0, "server_default"));

    const auto stream_details = client->get_stream(make_string_identifier(stream_name));
    ASSERT_EQ(stream_details.topics.size(), 1u);

    ASSERT_NO_THROW(client->delete_stream(make_numeric_identifier(stream_details.id)));

    ASSERT_THROW(client->delete_partitions(make_numeric_identifier(stream_details.id),
                                           make_numeric_identifier(stream_details.topics[0].id), 1),
                 std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

// TODO(slbotbm): Add CreatePartitionsAfterTopicDeletionThrows test case after delete_topic function is added.
