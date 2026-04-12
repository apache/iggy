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
#include <iostream>
#include <limits>
#include <string>
#include <utility>

#include <gtest/gtest.h>

#include "lib.rs.h"
#include "tests/common/test_helpers.hpp"

TEST(LowLevelE2E_Client, ConnectAndLogin) {
    RecordProperty("description", "Connects and logs in successfully using each supported connection string format.");
    const std::string username             = "iggy";
    const std::string password             = "iggy";
    const std::string connection_strings[] = {"iggy://iggy:iggy@127.0.0.1:8090", "iggy+tcp://iggy:iggy@127.0.0.1:8090",
                                              ""};

    for (const std::string &connection_string : connection_strings) {
        SCOPED_TRACE(connection_string);
        iggy::ffi::Client *client = nullptr;
        ASSERT_NO_THROW({ client = iggy::ffi::new_connection(connection_string); });
        ASSERT_NE(client, nullptr);

        ASSERT_NO_THROW(client->connect());
        ASSERT_NO_THROW(client->login_user(username, password));
        ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
        client = nullptr;
    }
}

TEST(LowLevelE2E_Client, NewConnectionWithMalformedConnectionStringsThrow) {
    RecordProperty("description", "Rejects malformed connection strings when creating a new client connection.");
    const std::string malformed_connection_strings[] = {
        "iggy+invalid://iggy:iggy@127.0.0.1:8090", "iggy+tcp://iggy:iggy@:8090",      "iggy+tcp://iggy:iggy@127.0.0.1",
        "iggy+tcp://iggy:iggy@127.0.0.1:abc",      "iggy+tcp://:iggy@127.0.0.1:8090", "iggy+tcp://iggy:@127.0.0.1:8090",
        "iggy+tcp://iggy:iggy127.0.0.1:8090",      "not-a-connection-string",         "iggy://iggy:iggy@",
    };

    for (const std::string &connection_string : malformed_connection_strings) {
        SCOPED_TRACE(connection_string);
        ASSERT_THROW({ iggy::ffi::new_connection(connection_string); }, std::exception);
    }
}

TEST(LowLevelE2E_Client, LoginWithInvalidCredentialsThrows) {
    RecordProperty("description", "Throws when authentication uses invalid credentials after connecting.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);

    ASSERT_NO_THROW(client->connect());
    ASSERT_THROW(client->login_user("biggy", "biggy"), std::exception);
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Client, LoginTwiceWithDifferentCredentials) {
    RecordProperty("description", "Rejects a second login attempt that switches to invalid credentials.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);

    ASSERT_NO_THROW(client->connect());
    ASSERT_NO_THROW(client->login_user("iggy", "iggy"));
    ASSERT_THROW(client->login_user("biggy", "biggy"), std::exception);
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Client, LogoutWithoutLogin) {
    RecordProperty("description", "Allows deleting a new unauthenticated client without logging in.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Client, DeleteWhileUnauthenticatedAfterFailedLogin) {
    RecordProperty("description", "Allows client cleanup after a failed login leaves the connection unauthenticated.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);

    ASSERT_NO_THROW(client->connect());
    ASSERT_THROW(client->login_user("biggy", "biggy"), std::exception);
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Client, LoginWithoutConnect) {
    RecordProperty("description", "Supports login without an explicit prior connect call.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);

    ASSERT_NO_THROW(client->login_user("iggy", "iggy"));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Client, ConnectWithoutLoginThenDelete) {
    RecordProperty("description", "Allows connecting without logging in and then deleting the client.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);

    ASSERT_NO_THROW(client->connect());
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Client, RepeatedClientMethodCallsHaveStableBehavior) {
    RecordProperty("description",
                   "Keeps repeated connect, login, and delete calls stable across duplicate invocations.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);

    ASSERT_NO_THROW(client->connect());
    ASSERT_NO_THROW(client->connect());
    ASSERT_NO_THROW(client->login_user("iggy", "iggy"));
    ASSERT_NO_THROW(client->login_user("iggy", "iggy"));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_Client, DeleteNullConnectionIsNoop) {
    RecordProperty("description", "Treats deleting a null client pointer as a no-op.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_Client, GetStatsBeforeLoginThrows) {
    RecordProperty("description", "Rejects get_stats before connect, and after connect but before login.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);

    ASSERT_THROW(client->get_stats(), std::exception);
    ASSERT_NO_THROW(client->connect());
    ASSERT_THROW(client->get_stats(), std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

// TODO(slbotbm-this PR): add a test to create some streams, topics, partitions, and segments, send messages, and create
// consumer groups and verify it.
TEST(LowLevelE2E_Client, GetStatsReturnsServerStats) {
    RecordProperty("description",
                   "Returns empty resource counts first, then reflects aggregated streams, topics, partitions, "
                   "consumer groups, and clients.");
    const std::string first_stream_name                 = "cpp-get-stats-stream-1";
    const std::string second_stream_name                = "cpp-get-stats-stream-2";
    const std::string first_topic_name                  = "cpp-get-stats-topic-1";
    const std::string second_topic_name                 = "cpp-get-stats-topic-2";
    const std::string third_topic_name                  = "cpp-get-stats-topic-3";
    const std::string first_group_name                  = "cpp-get-stats-group-1";
    const std::string second_group_name                 = "cpp-get-stats-group-2";
    const std::string third_group_name                  = "cpp-get-stats-group-3";
    constexpr std::uint32_t additional_partitions_count = 2;
    iggy::ffi::Client *client                           = login_to_server();
    ASSERT_NE(client, nullptr);

    iggy::ffi::Client *second_client = nullptr;
    iggy::ffi::Client *third_client  = nullptr;

    iggy::ffi::Stats empty_stats{};
    ASSERT_NO_THROW({
        empty_stats = client->get_stats();
        EXPECT_NE(empty_stats.process_id, 0u);
        EXPECT_GT(empty_stats.threads_count, 0u);
        EXPECT_GT(empty_stats.total_memory, 0u);
        EXPECT_GE(empty_stats.available_memory, 0u);
        EXPECT_GE(empty_stats.total_disk_space, empty_stats.free_disk_space);
        EXPECT_FALSE(static_cast<std::string>(empty_stats.hostname).empty());
        EXPECT_FALSE(static_cast<std::string>(empty_stats.os_name).empty());
        EXPECT_FALSE(static_cast<std::string>(empty_stats.os_version).empty());
        EXPECT_FALSE(static_cast<std::string>(empty_stats.kernel_version).empty());
        EXPECT_FALSE(static_cast<std::string>(empty_stats.iggy_server_version).empty());
        EXPECT_TRUE(empty_stats.iggy_server_semver == std::numeric_limits<std::uint32_t>::max() ||
                    empty_stats.iggy_server_semver > 0u);
        EXPECT_GE(empty_stats.cache_metrics.size(), 0u);
        EXPECT_EQ(empty_stats.streams_count, 0u);
        EXPECT_EQ(empty_stats.topics_count, 0u);
        EXPECT_EQ(empty_stats.partitions_count, 0u);
        EXPECT_EQ(empty_stats.consumer_groups_count, 0u);
    });

    ASSERT_NO_THROW(client->create_stream(first_stream_name));
    ASSERT_NO_THROW(client->create_stream(second_stream_name));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(first_stream_name), first_topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(first_stream_name), second_topic_name, 2, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(second_stream_name), third_topic_name, 3, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_partitions(make_string_identifier(first_stream_name),
                                              make_string_identifier(first_topic_name), additional_partitions_count));
    const auto first_group  = client->create_consumer_group(make_string_identifier(first_stream_name),
                                                            make_string_identifier(first_topic_name), first_group_name);
    const auto second_group = client->create_consumer_group(
        make_string_identifier(first_stream_name), make_string_identifier(second_topic_name), second_group_name);
    const auto third_group = client->create_consumer_group(make_string_identifier(second_stream_name),
                                                           make_string_identifier(third_topic_name), third_group_name);

    ASSERT_NO_THROW({ second_client = login_to_server(); });
    ASSERT_NE(second_client, nullptr);
    ASSERT_NO_THROW({ third_client = login_to_server(); });
    ASSERT_NE(third_client, nullptr);

    const auto first_stream_details           = client->get_stream(make_string_identifier(first_stream_name));
    const auto second_stream_details          = client->get_stream(make_string_identifier(second_stream_name));
    const std::uint32_t expected_topics_count = first_stream_details.topics_count + second_stream_details.topics_count;
    std::uint32_t first_topic_partitions      = 0;
    std::uint32_t second_topic_partitions     = 0;
    std::uint32_t third_topic_partitions      = 0;
    for (const auto &topic : first_stream_details.topics) {
        if (topic.name == first_topic_name) {
            first_topic_partitions = topic.partitions_count;
        }
        if (topic.name == second_topic_name) {
            second_topic_partitions = topic.partitions_count;
        }
    }
    for (const auto &topic : second_stream_details.topics) {
        if (topic.name == third_topic_name) {
            third_topic_partitions = topic.partitions_count;
        }
    }
    const std::uint32_t expected_partitions_count =
        first_topic_partitions + second_topic_partitions + third_topic_partitions;

    ASSERT_NO_THROW({
        const auto stats = client->get_stats();
        EXPECT_EQ(stats.streams_count, 2u);
        EXPECT_EQ(stats.topics_count, expected_topics_count);
        EXPECT_EQ(stats.partitions_count, expected_partitions_count);
        EXPECT_EQ(stats.segments_count, expected_partitions_count);
        EXPECT_EQ(stats.consumer_groups_count, 3u);
        EXPECT_EQ(stats.clients_count, empty_stats.clients_count + 2u);
        EXPECT_EQ(first_group.partitions_count, first_topic_partitions);
        EXPECT_EQ(second_group.partitions_count, second_topic_partitions);
        EXPECT_EQ(third_group.partitions_count, third_topic_partitions);
    });

    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(second_stream_name)));
    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(first_stream_name)));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(third_client));
    third_client = nullptr;
    ASSERT_NO_THROW(iggy::ffi::delete_connection(second_client));
    second_client = nullptr;

    ASSERT_NO_THROW({
        const auto stats = client->get_stats();
        EXPECT_EQ(stats.streams_count, 0u);
        EXPECT_EQ(stats.topics_count, 0u);
        EXPECT_EQ(stats.partitions_count, 0u);
        EXPECT_EQ(stats.segments_count, 0u);
        EXPECT_EQ(stats.consumer_groups_count, 0u);
    });

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Client, GetStatsIsStableAcrossBackToBackCalls) {
    RecordProperty(
        "description",
        "Returns sane invariant fields across back-to-back get_stats calls on an idle authenticated client.");
    iggy::ffi::Client *client = login_to_server();
    ASSERT_NE(client, nullptr);

    iggy::ffi::Stats first_stats{};
    iggy::ffi::Stats second_stats{};
    ASSERT_NO_THROW({
        first_stats  = client->get_stats();
        second_stats = client->get_stats();
    });

    EXPECT_NE(first_stats.process_id, 0u);
    EXPECT_NE(second_stats.process_id, 0u);
    EXPECT_EQ(second_stats.process_id, first_stats.process_id);
    EXPECT_GT(first_stats.threads_count, 0u);
    EXPECT_GT(second_stats.threads_count, 0u);
    EXPECT_GT(first_stats.total_memory, 0u);
    EXPECT_GT(second_stats.total_memory, 0u);
    EXPECT_FALSE(static_cast<std::string>(first_stats.hostname).empty());
    EXPECT_FALSE(static_cast<std::string>(second_stats.hostname).empty());
    EXPECT_FALSE(static_cast<std::string>(first_stats.os_name).empty());
    EXPECT_FALSE(static_cast<std::string>(second_stats.os_name).empty());
    EXPECT_FALSE(static_cast<std::string>(first_stats.os_version).empty());
    EXPECT_FALSE(static_cast<std::string>(second_stats.os_version).empty());
    EXPECT_FALSE(static_cast<std::string>(first_stats.kernel_version).empty());
    EXPECT_FALSE(static_cast<std::string>(second_stats.kernel_version).empty());
    EXPECT_FALSE(static_cast<std::string>(first_stats.iggy_server_version).empty());
    EXPECT_FALSE(static_cast<std::string>(second_stats.iggy_server_version).empty());
    EXPECT_EQ(static_cast<std::string>(second_stats.hostname), static_cast<std::string>(first_stats.hostname));
    EXPECT_EQ(static_cast<std::string>(second_stats.os_name), static_cast<std::string>(first_stats.os_name));
    EXPECT_EQ(static_cast<std::string>(second_stats.os_version), static_cast<std::string>(first_stats.os_version));
    EXPECT_EQ(static_cast<std::string>(second_stats.kernel_version),
              static_cast<std::string>(first_stats.kernel_version));
    EXPECT_EQ(static_cast<std::string>(second_stats.iggy_server_version),
              static_cast<std::string>(first_stats.iggy_server_version));
    EXPECT_EQ(second_stats.iggy_server_semver, first_stats.iggy_server_semver);
    EXPECT_EQ(second_stats.clients_count, first_stats.clients_count);
    EXPECT_EQ(second_stats.streams_count, first_stats.streams_count);
    EXPECT_EQ(second_stats.topics_count, first_stats.topics_count);
    EXPECT_EQ(second_stats.partitions_count, first_stats.partitions_count);
    EXPECT_EQ(second_stats.consumer_groups_count, first_stats.consumer_groups_count);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Client, GetMeBeforeLoginThrows) {
    RecordProperty("description", "Rejects get_me before connect, and after connect but before login.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);

    ASSERT_THROW(client->get_me(), std::exception);
    ASSERT_NO_THROW(client->connect());
    ASSERT_THROW(client->get_me(), std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

// TODO(slbotbm-this PR): add additional validation for get_me after merging join_consumer_group PR.
TEST(LowLevelE2E_Client, GetMeReturnsCurrentClientDetails) {
    RecordProperty("description", "Returns the current authenticated client details.");
    iggy::ffi::Client *client = login_to_server();
    ASSERT_NE(client, nullptr);

    ASSERT_NO_THROW({
        const auto me = client->get_me();
        EXPECT_NE(me.client_id, 0u);
        EXPECT_NE(me.user_id, std::numeric_limits<std::uint32_t>::max());
        EXPECT_FALSE(static_cast<std::string>(me.address).empty());
        EXPECT_EQ(static_cast<std::string>(me.transport), "TCP");
        EXPECT_EQ(me.consumer_groups_count, 0u);
        EXPECT_TRUE(me.consumer_groups.empty());
    });

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Client, GetMeIsStableAcrossBackToBackCalls) {
    RecordProperty("description", "Returns stable current-client details across back-to-back get_me calls.");
    iggy::ffi::Client *client = login_to_server();
    ASSERT_NE(client, nullptr);

    iggy::ffi::ClientInfoDetails first_me{};
    iggy::ffi::ClientInfoDetails second_me{};
    ASSERT_NO_THROW({
        first_me  = client->get_me();
        second_me = client->get_me();
    });

    EXPECT_NE(first_me.client_id, 0u);
    EXPECT_EQ(second_me.client_id, first_me.client_id);
    EXPECT_EQ(second_me.user_id, first_me.user_id);
    EXPECT_EQ(static_cast<std::string>(second_me.address), static_cast<std::string>(first_me.address));
    EXPECT_EQ(static_cast<std::string>(first_me.transport), "TCP");
    EXPECT_EQ(static_cast<std::string>(second_me.transport), "TCP");
    EXPECT_EQ(static_cast<std::string>(second_me.transport), static_cast<std::string>(first_me.transport));
    EXPECT_EQ(second_me.consumer_groups_count, first_me.consumer_groups_count);
    EXPECT_EQ(second_me.consumer_groups.size(), first_me.consumer_groups.size());

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Client, GetMeReturnsDistinctClientIdsForDifferentSessions) {
    RecordProperty(
        "description",
        "Returns different client ids for separate authenticated sessions while keeping the same user identity.");
    iggy::ffi::Client *first_client  = login_to_server();
    iggy::ffi::Client *second_client = login_to_server();
    ASSERT_NE(first_client, nullptr);
    ASSERT_NE(second_client, nullptr);

    iggy::ffi::ClientInfoDetails first_me{};
    iggy::ffi::ClientInfoDetails second_me{};
    ASSERT_NO_THROW({
        first_me  = first_client->get_me();
        second_me = second_client->get_me();
    });

    EXPECT_NE(first_me.client_id, 0u);
    EXPECT_NE(second_me.client_id, 0u);
    EXPECT_NE(second_me.client_id, first_me.client_id);
    EXPECT_EQ(second_me.user_id, first_me.user_id);
    EXPECT_EQ(static_cast<std::string>(first_me.transport), "TCP");
    EXPECT_EQ(static_cast<std::string>(second_me.transport), "TCP");

    ASSERT_NO_THROW(iggy::ffi::delete_connection(second_client));
    second_client = nullptr;
    ASSERT_NO_THROW(iggy::ffi::delete_connection(first_client));
    first_client = nullptr;
}

TEST(LowLevelE2E_Client, GetMeReturnsValidDetailsAfterReconnect) {
    RecordProperty("description",
                   "Returns valid current-client details after reconnecting with a fresh authenticated session.");
    iggy::ffi::Client *first_client = login_to_server();
    ASSERT_NE(first_client, nullptr);

    iggy::ffi::ClientInfoDetails first_me{};
    ASSERT_NO_THROW({ first_me = first_client->get_me(); });
    EXPECT_NE(first_me.client_id, 0u);
    EXPECT_NE(first_me.user_id, std::numeric_limits<std::uint32_t>::max());

    ASSERT_NO_THROW(iggy::ffi::delete_connection(first_client));
    first_client = nullptr;

    iggy::ffi::Client *second_client = login_to_server();
    ASSERT_NE(second_client, nullptr);

    iggy::ffi::ClientInfoDetails second_me{};
    ASSERT_NO_THROW({ second_me = second_client->get_me(); });
    EXPECT_NE(second_me.client_id, 0u);
    EXPECT_NE(second_me.user_id, std::numeric_limits<std::uint32_t>::max());
    EXPECT_EQ(second_me.user_id, first_me.user_id);
    EXPECT_EQ(static_cast<std::string>(first_me.transport), "TCP");
    EXPECT_EQ(static_cast<std::string>(second_me.transport), "TCP");
    EXPECT_EQ(static_cast<std::string>(second_me.transport), static_cast<std::string>(first_me.transport));
    EXPECT_FALSE(static_cast<std::string>(second_me.address).empty());
    EXPECT_EQ(second_me.consumer_groups_count, 0u);
    EXPECT_TRUE(second_me.consumer_groups.empty());

    ASSERT_NO_THROW(iggy::ffi::delete_connection(second_client));
    second_client = nullptr;
}

TEST(LowLevelE2E_Client, GetClientBeforeLoginThrows) {
    RecordProperty("description", "Rejects get_client before connect, and after connect but before login.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);

    ASSERT_THROW(client->get_client(1), std::exception);
    ASSERT_NO_THROW(client->connect());
    ASSERT_THROW(client->get_client(1), std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Client, GetClientWithWrongClientIdThrows) {
    RecordProperty("description", "Rejects querying invalid or non-existent client ids.");
    const std::uint32_t wrong_client_ids[] = {0u, std::numeric_limits<std::uint32_t>::max()};
    iggy::ffi::Client *client              = login_to_server();
    ASSERT_NE(client, nullptr);

    for (const std::uint32_t wrong_client_id : wrong_client_ids) {
        SCOPED_TRACE(wrong_client_id);
        ASSERT_THROW(client->get_client(wrong_client_id), std::exception);
    }

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Client, GetClientReturnsDetailsForMatchingClientId) {
    RecordProperty("description", "Returns current client details when querying with the authenticated client id.");
    iggy::ffi::Client *client = login_to_server();
    ASSERT_NE(client, nullptr);

    iggy::ffi::ClientInfoDetails current_client{};
    iggy::ffi::ClientInfoDetails looked_up_client{};
    ASSERT_NO_THROW({
        current_client   = client->get_me();
        looked_up_client = client->get_client(current_client.client_id);
    });

    EXPECT_NE(current_client.client_id, 0u);
    EXPECT_EQ(looked_up_client.client_id, current_client.client_id);
    EXPECT_EQ(looked_up_client.user_id, current_client.user_id);
    EXPECT_EQ(static_cast<std::string>(looked_up_client.address), static_cast<std::string>(current_client.address));
    EXPECT_EQ(static_cast<std::string>(looked_up_client.transport), "TCP");
    EXPECT_EQ(static_cast<std::string>(looked_up_client.transport), static_cast<std::string>(current_client.transport));
    EXPECT_EQ(looked_up_client.consumer_groups_count, current_client.consumer_groups_count);
    EXPECT_EQ(looked_up_client.consumer_groups.size(), current_client.consumer_groups.size());

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Client, GetClientIsStableAcrossBackToBackCalls) {
    RecordProperty("description", "Returns stable client details across back-to-back get_client calls.");
    iggy::ffi::Client *client = login_to_server();
    ASSERT_NE(client, nullptr);

    iggy::ffi::ClientInfoDetails current_client{};
    iggy::ffi::ClientInfoDetails first_lookup{};
    iggy::ffi::ClientInfoDetails second_lookup{};
    ASSERT_NO_THROW({
        current_client = client->get_me();
        first_lookup   = client->get_client(current_client.client_id);
        second_lookup  = client->get_client(current_client.client_id);
    });

    EXPECT_NE(current_client.client_id, 0u);
    EXPECT_EQ(first_lookup.client_id, current_client.client_id);
    EXPECT_EQ(second_lookup.client_id, first_lookup.client_id);
    EXPECT_EQ(second_lookup.user_id, first_lookup.user_id);
    EXPECT_EQ(static_cast<std::string>(second_lookup.address), static_cast<std::string>(first_lookup.address));
    EXPECT_EQ(static_cast<std::string>(first_lookup.transport), "TCP");
    EXPECT_EQ(static_cast<std::string>(second_lookup.transport), "TCP");
    EXPECT_EQ(static_cast<std::string>(second_lookup.transport), static_cast<std::string>(first_lookup.transport));
    EXPECT_EQ(second_lookup.consumer_groups_count, first_lookup.consumer_groups_count);
    EXPECT_EQ(second_lookup.consumer_groups.size(), first_lookup.consumer_groups.size());

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Client, GetClientsBeforeLoginThrows) {
    RecordProperty("description", "Rejects get_clients before connect, and after connect but before login.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);

    ASSERT_THROW(client->get_clients(), std::exception);
    ASSERT_NO_THROW(client->connect());
    ASSERT_THROW(client->get_clients(), std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Client, GetClientsReturnsActiveClientSessions) {
    RecordProperty("description", "Returns the currently active authenticated client sessions.");
    iggy::ffi::Client *first_client  = login_to_server();
    iggy::ffi::Client *second_client = login_to_server();
    ASSERT_NE(first_client, nullptr);
    ASSERT_NE(second_client, nullptr);

    iggy::ffi::ClientInfoDetails first_me{};
    iggy::ffi::ClientInfoDetails second_me{};
    rust::Vec<iggy::ffi::ClientInfo> clients;
    ASSERT_NO_THROW({
        first_me  = first_client->get_me();
        second_me = second_client->get_me();
        clients   = first_client->get_clients();
    });

    ASSERT_GE(clients.size(), 2u);

    bool found_first  = false;
    bool found_second = false;
    for (const auto &client : clients) {
        EXPECT_NE(client.client_id, 0u);
        EXPECT_EQ(static_cast<std::string>(client.transport), "TCP");

        if (client.client_id == first_me.client_id) {
            found_first = true;
            EXPECT_EQ(client.user_id, first_me.user_id);
            EXPECT_EQ(static_cast<std::string>(client.address), static_cast<std::string>(first_me.address));
            EXPECT_EQ(client.consumer_groups_count, first_me.consumer_groups_count);
        }

        if (client.client_id == second_me.client_id) {
            found_second = true;
            EXPECT_EQ(client.user_id, second_me.user_id);
            EXPECT_EQ(static_cast<std::string>(client.address), static_cast<std::string>(second_me.address));
            EXPECT_EQ(client.consumer_groups_count, second_me.consumer_groups_count);
        }
    }

    EXPECT_TRUE(found_first);
    EXPECT_TRUE(found_second);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(second_client));
    second_client = nullptr;
    ASSERT_NO_THROW(iggy::ffi::delete_connection(first_client));
    first_client = nullptr;
}

TEST(LowLevelE2E_Client, GetClientsIsStableAcrossBackToBackCalls) {
    RecordProperty("description", "Returns stable client lists across back-to-back get_clients calls.");
    iggy::ffi::Client *first_client  = login_to_server();
    iggy::ffi::Client *second_client = login_to_server();
    ASSERT_NE(first_client, nullptr);
    ASSERT_NE(second_client, nullptr);

    rust::Vec<iggy::ffi::ClientInfo> first_clients;
    rust::Vec<iggy::ffi::ClientInfo> second_clients;
    ASSERT_NO_THROW({
        first_clients  = first_client->get_clients();
        second_clients = first_client->get_clients();
    });

    ASSERT_EQ(second_clients.size(), first_clients.size());
    for (const auto &first_entry : first_clients) {
        bool found_match = false;
        for (const auto &second_entry : second_clients) {
            if (second_entry.client_id != first_entry.client_id) {
                continue;
            }

            found_match = true;
            EXPECT_EQ(second_entry.user_id, first_entry.user_id);
            EXPECT_EQ(static_cast<std::string>(second_entry.address), static_cast<std::string>(first_entry.address));
            EXPECT_EQ(static_cast<std::string>(second_entry.transport),
                      static_cast<std::string>(first_entry.transport));
            EXPECT_EQ(second_entry.consumer_groups_count, first_entry.consumer_groups_count);
            break;
        }
        EXPECT_TRUE(found_match);
    }

    ASSERT_NO_THROW(iggy::ffi::delete_connection(second_client));
    second_client = nullptr;
    ASSERT_NO_THROW(iggy::ffi::delete_connection(first_client));
    first_client = nullptr;
}

TEST(LowLevelE2E_Client, GetClientsMatchesGetClientForReturnedIds) {
    RecordProperty("description", "Returns list entries that agree with get_client for each returned client id.");
    iggy::ffi::Client *first_client  = login_to_server();
    iggy::ffi::Client *second_client = login_to_server();
    ASSERT_NE(first_client, nullptr);
    ASSERT_NE(second_client, nullptr);

    rust::Vec<iggy::ffi::ClientInfo> clients;
    ASSERT_NO_THROW({ clients = first_client->get_clients(); });
    ASSERT_GE(clients.size(), 2u);

    for (const auto &client : clients) {
        SCOPED_TRACE(client.client_id);
        iggy::ffi::ClientInfoDetails details{};
        ASSERT_NO_THROW({ details = first_client->get_client(client.client_id); });

        EXPECT_EQ(details.client_id, client.client_id);
        EXPECT_EQ(details.user_id, client.user_id);
        EXPECT_EQ(static_cast<std::string>(details.address), static_cast<std::string>(client.address));
        EXPECT_EQ(static_cast<std::string>(details.transport), static_cast<std::string>(client.transport));
        EXPECT_EQ(details.consumer_groups_count, client.consumer_groups_count);
    }

    ASSERT_NO_THROW(iggy::ffi::delete_connection(second_client));
    second_client = nullptr;
    ASSERT_NO_THROW(iggy::ffi::delete_connection(first_client));
    first_client = nullptr;
}

TEST(LowLevelE2E_Client, GetClientsReflectsAdditionalSession) {
    RecordProperty("description", "Reflects a newly added authenticated session in subsequent get_clients results.");
    iggy::ffi::Client *first_client = login_to_server();
    ASSERT_NE(first_client, nullptr);

    rust::Vec<iggy::ffi::ClientInfo> clients_before;
    ASSERT_NO_THROW({ clients_before = first_client->get_clients(); });

    iggy::ffi::Client *second_client = login_to_server();
    ASSERT_NE(second_client, nullptr);

    iggy::ffi::ClientInfoDetails second_me{};
    rust::Vec<iggy::ffi::ClientInfo> clients_after;
    ASSERT_NO_THROW({
        second_me     = second_client->get_me();
        clients_after = first_client->get_clients();
    });

    bool found_before = false;
    for (const auto &client : clients_before) {
        if (client.client_id == second_me.client_id) {
            found_before = true;
            break;
        }
    }
    EXPECT_FALSE(found_before);

    bool found_after = false;
    for (const auto &client : clients_after) {
        if (client.client_id != second_me.client_id) {
            continue;
        }

        found_after = true;
        EXPECT_EQ(client.user_id, second_me.user_id);
        EXPECT_EQ(static_cast<std::string>(client.address), static_cast<std::string>(second_me.address));
        EXPECT_EQ(static_cast<std::string>(client.transport), "TCP");
        EXPECT_EQ(client.consumer_groups_count, second_me.consumer_groups_count);
        break;
    }
    EXPECT_TRUE(found_after);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(second_client));
    second_client = nullptr;
    ASSERT_NO_THROW(iggy::ffi::delete_connection(first_client));
    first_client = nullptr;
}

TEST(LowLevelE2E_Client, PingSucceedsForNewConnection) {
    RecordProperty("description", "Successfully pings the server from a fresh unauthenticated client session.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);

    ASSERT_NO_THROW(client->ping());

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Client, HeartbeatIntervalReturnsDefaultValueForNewConnection) {
    RecordProperty("description",
                   "Returns the default heartbeat interval in microseconds for a fresh unauthenticated client.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);

    const auto heartbeat_interval = client->heartbeat_interval();
    EXPECT_EQ(heartbeat_interval, 5'000'000u);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Client, HeartbeatIntervalReturnsConfiguredValueFromConnectionString) {
    RecordProperty("description",
                   "Returns the configured heartbeat interval in microseconds from the connection string.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection("iggy://iggy:iggy@127.0.0.1:8090?heartbeat_interval=10s"); });
    ASSERT_NE(client, nullptr);

    const auto heartbeat_interval = client->heartbeat_interval();
    EXPECT_EQ(heartbeat_interval, 10'000'000u);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Client, SnapshotBeforeLoginThrows) {
    RecordProperty("description", "Rejects snapshot before connect, and after connect but before login.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);

    rust::Vec<rust::String> snapshot_types_before_connect;
    snapshot_types_before_connect.push_back("test");
    ASSERT_THROW(client->snapshot("deflated", std::move(snapshot_types_before_connect)), std::exception);

    ASSERT_NO_THROW(client->connect());
    rust::Vec<rust::String> snapshot_types_before_login;
    snapshot_types_before_login.push_back("test");
    ASSERT_THROW(client->snapshot("deflated", std::move(snapshot_types_before_login)), std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Client, SnapshotAllCombinedWithOtherTypeThrows) {
    RecordProperty("description", "Rejects combining the all snapshot type with any other snapshot type.");
    iggy::ffi::Client *client = login_to_server();
    ASSERT_NE(client, nullptr);

    rust::Vec<rust::String> snapshot_types;
    snapshot_types.push_back("all");
    snapshot_types.push_back("test");

    ASSERT_THROW(client->snapshot("deflated", std::move(snapshot_types)), std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Client, SnapshotWithInvalidCompressionThrows) {
    RecordProperty("description", "Rejects invalid snapshot compression values in the wrapper before sending.");
    iggy::ffi::Client *client = login_to_server();
    ASSERT_NE(client, nullptr);

    rust::Vec<rust::String> snapshot_types;
    snapshot_types.push_back("test");

    ASSERT_THROW(client->snapshot("invalid-compression", std::move(snapshot_types)), std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Client, SnapshotWithInvalidSnapshotTypeThrows) {
    RecordProperty("description", "Rejects invalid snapshot type values in the wrapper before sending.");
    iggy::ffi::Client *client = login_to_server();
    ASSERT_NE(client, nullptr);

    rust::Vec<rust::String> snapshot_types;
    snapshot_types.push_back("not-a-real-type");

    ASSERT_THROW(client->snapshot("deflated", std::move(snapshot_types)), std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}
