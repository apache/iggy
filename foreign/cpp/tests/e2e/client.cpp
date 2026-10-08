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

// TODO(slbotbm): Add tests for update_permissions after creating create_user, get_user, etc. functions
#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <optional>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include <gtest/gtest.h>

#include "lib.rs.h"
#include "tests/e2e/test_helpers.hpp"

class LowLevelE2E_Client : public E2ETestFixture {};
class E2E_Client : public E2ETestFixture {};

TEST_F(E2E_Client, ConnectAndLogin) {
    RecordProperty("description",
                   "Connects and returns matching login information using binary connection string formats.");
    constexpr std::uint32_t root_user_id   = 0;
    const std::string username             = "iggy";
    const std::string password             = "iggy";
    const std::string connection_strings[] = {
        "iggy://iggy:iggy@127.0.0.1:8090",
        "iggy+tcp://iggy:iggy@127.0.0.1:8090",
        "",
    };

    for (const std::string &connection_string : connection_strings) {
        SCOPED_TRACE(connection_string);
        std::optional<iggy::IggyBlockingClient> client;
        ASSERT_NO_THROW({
            if (connection_string.empty()) {
                client.emplace(iggy::IggyBlockingClient::Builder().Build());
            } else {
                client.emplace(iggy::IggyBlockingClient::FromConnectionString(connection_string));
            }
        });
        ASSERT_TRUE(client.has_value());

        std::optional<iggy::LoginInfo> login_info;
        std::optional<iggy::ClientInfoDetails> me;
        ASSERT_NO_THROW(client->Connect());
        ASSERT_NO_THROW({ login_info.emplace(client->Login(username, password)); });
        ASSERT_NO_THROW({ me.emplace(client->GetMe()); });

        ASSERT_TRUE(login_info.has_value());
        ASSERT_TRUE(me.has_value());
        EXPECT_EQ(login_info->UserId(), root_user_id);
        ASSERT_TRUE(me->UserId().has_value());
        EXPECT_EQ(login_info->UserId(), me->UserId().value());
        EXPECT_FALSE(login_info->AccessToken().has_value());
        EXPECT_FALSE(login_info->AccessTokenExpiry().has_value());
    }
}

TEST_F(E2E_Client, HttpLoginReturnsAccessToken) {
    RecordProperty("description", "Returns root user information and an access token after HTTP login.");
    constexpr std::uint32_t root_user_id = 0;
    std::optional<iggy::IggyBlockingClient> client;
    ASSERT_NO_THROW(
        { client.emplace(iggy::IggyBlockingClient::FromConnectionString("iggy+http://iggy:iggy@127.0.0.1:3000")); });
    ASSERT_TRUE(client.has_value());

    std::optional<iggy::LoginInfo> login_info;
    ASSERT_NO_THROW(client->Connect());
    ASSERT_NO_THROW({ login_info.emplace(client->Login("iggy", "iggy")); });

    ASSERT_TRUE(login_info.has_value());
    EXPECT_EQ(login_info->UserId(), root_user_id);
    ASSERT_TRUE(login_info->AccessToken().has_value());
    EXPECT_FALSE(login_info->AccessToken()->empty());
    ASSERT_TRUE(login_info->AccessTokenExpiry().has_value());
    EXPECT_NE(login_info->AccessTokenExpiry().value(), 0u);
}

TEST_F(E2E_Client, NewConnectionWithMalformedConnectionStringsThrow) {
    RecordProperty("description", "Rejects malformed connection strings when creating a new client connection.");
    const std::string malformed_connection_strings[] = {
        "iggy+invalid://iggy:iggy@127.0.0.1:8090", "iggy+tcp://iggy:iggy@:8090",      "iggy+tcp://iggy:iggy@127.0.0.1",
        "iggy+tcp://iggy:iggy@127.0.0.1:abc",      "iggy+tcp://:iggy@127.0.0.1:8090", "iggy+tcp://iggy:@127.0.0.1:8090",
        "iggy+tcp://iggy:iggy127.0.0.1:8090",      "not-a-connection-string",         "iggy://iggy:iggy@",
    };

    for (const std::string &connection_string : malformed_connection_strings) {
        SCOPED_TRACE(connection_string);
        ASSERT_THROW({ iggy::IggyBlockingClient::FromConnectionString(connection_string); }, std::exception);
    }
}

TEST_F(E2E_Client, LoginWithInvalidCredentialsThrows) {
    RecordProperty("description", "Throws when authentication uses invalid credentials after connecting.");
    auto client = GetLoggedOutHighLevelClient();

    ASSERT_NO_THROW(client.Connect());
    ASSERT_THROW(client.Login("biggy", "biggy"), std::exception);
}

TEST_F(E2E_Client, LoginTwiceWithDifferentCredentials) {
    RecordProperty("description", "Rejects a second login attempt that switches to invalid credentials.");
    auto client = GetLoggedOutHighLevelClient();

    ASSERT_NO_THROW(client.Connect());
    ASSERT_NO_THROW(client.Login("iggy", "iggy"));
    ASSERT_THROW(client.Login("biggy", "biggy"), std::exception);
}

TEST_F(E2E_Client, LogoutWithoutLogin) {
    RecordProperty("description",
                   "Rejects logout before authentication, both before and after connect, then succeeds after login.");
    auto client = GetLoggedOutHighLevelClient();

    ASSERT_THROW(client.Logout(), std::exception);

    ASSERT_NO_THROW(client.Connect());
    ASSERT_THROW(client.Logout(), std::exception);

    ASSERT_NO_THROW(client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(client.Logout());
}

TEST_F(E2E_Client, ReloginOnSameClientAfterLogout) {
    RecordProperty("description", "Returns the same user identity when reauthenticating after a successful logout.");
    auto client = GetLoggedOutHighLevelClient();

    std::optional<iggy::LoginInfo> first_login;
    std::optional<iggy::LoginInfo> second_login;
    std::optional<iggy::ClientInfoDetails> first_me;
    std::optional<iggy::ClientInfoDetails> second_me;
    ASSERT_NO_THROW(client.Connect());
    ASSERT_NO_THROW({ first_login.emplace(client.Login("iggy", "iggy")); });
    ASSERT_NO_THROW({ first_me.emplace(client.GetMe()); });
    ASSERT_NO_THROW(client.Logout());
    ASSERT_NO_THROW({ second_login.emplace(client.Login("iggy", "iggy")); });
    ASSERT_NO_THROW({ second_me.emplace(client.GetMe()); });

    ASSERT_TRUE(first_login.has_value());
    ASSERT_TRUE(second_login.has_value());
    ASSERT_TRUE(first_me.has_value());
    ASSERT_TRUE(second_me.has_value());
    ASSERT_TRUE(first_me->UserId().has_value());
    ASSERT_TRUE(second_me->UserId().has_value());
    EXPECT_EQ(first_login->UserId(), first_me->UserId().value());
    EXPECT_EQ(second_login->UserId(), second_me->UserId().value());
    EXPECT_EQ(second_login->UserId(), first_login->UserId());
    EXPECT_EQ(second_me->ClientId(), first_me->ClientId());
    EXPECT_EQ(second_me->UserId(), first_me->UserId());
}

TEST_F(E2E_Client, LogoutErrorsWhenCalledMoreThanOnce) {
    RecordProperty("description",
                   "Rejects repeated logout calls once the authenticated session has already logged out.");
    auto client = GetLoggedOutHighLevelClient();

    ASSERT_NO_THROW(client.Connect());
    ASSERT_NO_THROW(client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(client.Logout());
    ASSERT_THROW(client.Logout(), std::exception);
}

TEST_F(E2E_Client, CreateUserWithUsernameOutsideLengthBoundsThrows) {
    RecordProperty("description", "Rejects 2-byte and 51-byte usernames over TCP without creating users.");
    auto client = GetLoggedInHighLevelClient();
    const std::string too_short_username(2, 'a');
    const std::string too_long_username(51, 'a');
    const std::string usernames[] = {too_short_username, too_long_username};

    ASSERT_EQ(too_short_username.size(), 2u);
    ASSERT_EQ(too_long_username.size(), 51u);
    for (const auto &username : usernames) {
        SCOPED_TRACE(username.size());
        ASSERT_THROW(client.CreateUser(username, "secret123", iggy::UserStatus::Active), std::exception);
        ASSERT_THROW(client.GetUser(iggy::Identifier::String(username)), std::exception);
    }
}

TEST_F(E2E_Client, CreateUserAcceptsNonAsciiAndNonAlphabeticUsernames) {
    RecordProperty("description",
                   "Creates and retrieves usernames containing punctuation, multilingual UTF-8, and emoji over TCP.");
    auto client                   = GetLoggedInHighLevelClient();
    const std::string suffix      = GetRandomName(12);
    const std::string usernames[] = {
        "!@#_" + suffix, "ユーザー_" + suffix, "用户_" + suffix, "नाम_" + suffix, "사용자_" + suffix, "😀🚀_" + suffix,
    };

    for (const auto &username : usernames) {
        SCOPED_TRACE(username);
        ASSERT_LE(username.size(), 50u);

        const auto created_user = CreateUser(client, username, "secret123", iggy::UserStatus::Active);
        const auto fetched_user = client.GetUser(iggy::Identifier::String(username));

        EXPECT_EQ(fetched_user.Id(), created_user.Id());
        EXPECT_EQ(created_user.Username(), username);
        EXPECT_EQ(fetched_user.Username(), username);
    }
}

TEST_F(E2E_Client, CreateUserBeforeLoginThrows) {
    RecordProperty("description", "Rejects user creation without an active authenticated session.");
    auto client                             = GetLoggedOutHighLevelClient();
    auto root                               = GetLoggedInHighLevelClient();
    const std::string before_login_username = GetRandomName(50);
    const std::string logged_out_username   = GetRandomName(50);
    const std::string disconnected_username = GetRandomName(50);

    ASSERT_THROW(client.CreateUser(before_login_username, "secret123", iggy::UserStatus::Active), std::exception);
    ASSERT_NO_THROW(client.Connect());
    ASSERT_THROW(client.CreateUser(before_login_username, "secret123", iggy::UserStatus::Active), std::exception);

    ASSERT_NO_THROW(client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(client.Logout());
    ASSERT_THROW(client.CreateUser(logged_out_username, "secret123", iggy::UserStatus::Active), std::exception);

    ASSERT_NO_THROW(client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(client.Disconnect());
    ASSERT_THROW(client.CreateUser(disconnected_username, "secret123", iggy::UserStatus::Active), std::exception);

    ASSERT_THROW(root.GetUser(iggy::Identifier::String(before_login_username)), std::exception);
    ASSERT_THROW(root.GetUser(iggy::Identifier::String(logged_out_username)), std::exception);
    ASSERT_THROW(root.GetUser(iggy::Identifier::String(disconnected_username)), std::exception);
}

TEST_F(E2E_Client, CreateUserAcceptsUsernameAndPasswordLengthBounds) {
    RecordProperty("description",
                   "Creates users with shortest and longest ASCII usernames and passwords that can authenticate.");
    auto root_client              = GetLoggedInHighLevelClient();
    auto shortest_client          = GetLoggedOutHighLevelClient();
    auto longest_client           = GetLoggedOutHighLevelClient();
    std::string shortest_username = GetRandomName(3);
    std::string longest_username  = GetRandomName(50);
    const std::string shortest_password(3, 'a');
    const std::string longest_password(100, 'a');
    longest_username.resize(50, 'a');
    ASSERT_EQ(shortest_username.size(), 3u);
    ASSERT_EQ(longest_username.size(), 50u);
    ASSERT_EQ(shortest_password.size(), 3u);
    ASSERT_EQ(longest_password.size(), 100u);

    const auto shortest_user = CreateUser(root_client, shortest_username, shortest_password, iggy::UserStatus::Active);
    const auto longest_user  = CreateUser(root_client, longest_username, longest_password, iggy::UserStatus::Active);
    const auto fetched_shortest = root_client.GetUser(iggy::Identifier::String(shortest_username));
    const auto fetched_longest  = root_client.GetUser(iggy::Identifier::String(longest_username));
    ASSERT_NO_THROW(shortest_client.Connect());
    ASSERT_NO_THROW(longest_client.Connect());
    ASSERT_NO_THROW(shortest_client.Login(shortest_username, shortest_password));
    ASSERT_NO_THROW(longest_client.Login(longest_username, longest_password));

    EXPECT_EQ(shortest_user.Username(), shortest_username);
    EXPECT_EQ(longest_user.Username(), longest_username);
    EXPECT_EQ(fetched_shortest.Id(), shortest_user.Id());
    EXPECT_EQ(fetched_longest.Id(), longest_user.Id());
}

TEST_F(E2E_Client, CreateUserWithPasswordOutsideLengthBoundsThrows) {
    RecordProperty("description", "Rejects 2-byte and 101-byte passwords without creating users.");
    auto client                      = GetLoggedInHighLevelClient();
    const std::string short_username = GetRandomName(50);
    const std::string long_username  = GetRandomName(50);
    const std::string short_password(2, 'a');
    const std::string long_password(101, 'a');
    ASSERT_EQ(short_password.size(), 2u);
    ASSERT_EQ(long_password.size(), 101u);

    ASSERT_THROW(client.CreateUser(short_username, short_password, iggy::UserStatus::Active), std::exception);
    ASSERT_THROW(client.CreateUser(long_username, long_password, iggy::UserStatus::Active), std::exception);
    ASSERT_THROW(client.GetUser(iggy::Identifier::String(short_username)), std::exception);
    ASSERT_THROW(client.GetUser(iggy::Identifier::String(long_username)), std::exception);
}

TEST_F(E2E_Client, CreateUserWithInvalidStatusThrows) {
    RecordProperty("description", "Rejects invalid status codes before creating users.");
    auto client                       = GetLoggedInHighLevelClient();
    const iggy::UserStatus statuses[] = {
        static_cast<iggy::UserStatus>(0),
        static_cast<iggy::UserStatus>(3),
        static_cast<iggy::UserStatus>(std::numeric_limits<std::uint8_t>::max()),
    };

    for (const auto status : statuses) {
        const std::string username = GetRandomName(50);
        SCOPED_TRACE(static_cast<std::uint8_t>(status));
        ASSERT_THROW(client.CreateUser(username, "secret123", status), std::exception);
        ASSERT_THROW(client.GetUser(iggy::Identifier::String(username)), std::exception);
    }
}

TEST_F(E2E_Client, CreateUserReturnsCreatedActiveUserDetails) {
    RecordProperty("description", "Returns and persists active user details.");
    auto client                = GetLoggedInHighLevelClient();
    const std::string username = GetRandomName(50);
    ASSERT_NO_THROW({
        const auto created_user = CreateUser(client, username, "secret123", iggy::UserStatus::Active);
        const auto fetched_user = client.GetUser(iggy::Identifier::String(username));

        EXPECT_EQ(fetched_user.Id(), created_user.Id());
        EXPECT_EQ(created_user.Username(), username);
        EXPECT_EQ(fetched_user.Username(), username);
        EXPECT_EQ(created_user.Status(), iggy::UserStatus::Active);
        EXPECT_EQ(fetched_user.Status(), iggy::UserStatus::Active);
    });
}

TEST_F(E2E_Client, CreateUserRejectsDuplicateUsernameWithoutChangingOriginal) {
    RecordProperty("description", "Rejects duplicate usernames without changing the existing user.");
    auto root_client           = GetLoggedInHighLevelClient();
    auto user_client           = GetLoggedOutHighLevelClient();
    const std::string username = GetRandomName(50);
    const std::string password = "original-secret";
    ASSERT_NO_THROW({
        const auto original = CreateUser(root_client, username, password, iggy::UserStatus::Active);

        ASSERT_THROW(root_client.CreateUser(username, "replacement-secret", iggy::UserStatus::Inactive),
                     std::exception);

        const auto fetched = root_client.GetUser(iggy::Identifier::String(username));
        EXPECT_EQ(fetched.Id(), original.Id());
        EXPECT_EQ(fetched.Status(), iggy::UserStatus::Active);
    });
    ASSERT_NO_THROW(user_client.Connect());
    ASSERT_NO_THROW(user_client.Login(username, password));
    auto replacement_client = GetLoggedOutHighLevelClient();
    ASSERT_NO_THROW(replacement_client.Connect());
    ASSERT_THROW(replacement_client.Login(username, "replacement-secret"), std::exception);
}

TEST_F(E2E_Client, CreateUserPreservesNestedPermissionsInCreateAndGetResponses) {
    RecordProperty("description",
                   "Creates a user with global and per-resource permissions, then verifies create_user and get_user "
                   "return the same flags and numeric stream/topic IDs.");
    auto client                = GetLoggedInHighLevelClient();
    const std::string username = GetRandomName(50);
    iggy::GlobalPermissions global;
    global.SetManageServers(true).SetReadUsers(true).SetManageStreams(true).SetReadTopics(true).SetSendMessages(true);

    iggy::TopicPermissions first_topic;
    first_topic.SetManageTopic(true).SetPollMessages(true);
    iggy::TopicPermissions second_topic;
    second_topic.SetReadTopic(true).SetSendMessages(true);
    iggy::StreamPermissions first_stream;
    first_stream.SetManageStream(true).SetReadTopics(true).SetSendMessages(true).SetTopics(
        {{7, first_topic}, {9, second_topic}});

    iggy::TopicPermissions third_topic;
    third_topic.SetReadTopic(true);
    iggy::StreamPermissions second_stream;
    second_stream.SetReadStream(true).SetManageTopics(true).SetPollMessages(true).SetTopics({{3, third_topic}});

    iggy::Permissions permissions;
    permissions.SetGlobal(global).SetStreams({{42, first_stream}, {84, second_stream}});

    ASSERT_NO_THROW({
        const auto created = CreateUser(client, username, "secret123", iggy::UserStatus::Active, permissions);
        const auto fetched = client.GetUser(iggy::Identifier::String(username));
        for (const auto *user : {&created, &fetched}) {
            ASSERT_TRUE(user->Permissions().has_value());
            const auto &user_permissions = user->Permissions().value();
            EXPECT_TRUE(user_permissions.Global().ManageServers());
            EXPECT_FALSE(user_permissions.Global().ReadServers());
            EXPECT_FALSE(user_permissions.Global().ManageUsers());
            EXPECT_TRUE(user_permissions.Global().ReadUsers());
            EXPECT_TRUE(user_permissions.Global().ManageStreams());
            EXPECT_FALSE(user_permissions.Global().ReadStreams());
            EXPECT_FALSE(user_permissions.Global().ManageTopics());
            EXPECT_TRUE(user_permissions.Global().ReadTopics());
            EXPECT_FALSE(user_permissions.Global().PollMessages());
            EXPECT_TRUE(user_permissions.Global().SendMessages());
            ASSERT_EQ(user_permissions.Streams().size(), 2u);

            const auto stream_42 = user_permissions.Streams().find(42);
            const auto stream_84 = user_permissions.Streams().find(84);
            ASSERT_NE(stream_42, user_permissions.Streams().end());
            ASSERT_NE(stream_84, user_permissions.Streams().end());
            EXPECT_TRUE(stream_42->second.ManageStream());
            EXPECT_FALSE(stream_42->second.ReadStream());
            EXPECT_FALSE(stream_42->second.ManageTopics());
            EXPECT_TRUE(stream_42->second.ReadTopics());
            EXPECT_FALSE(stream_42->second.PollMessages());
            EXPECT_TRUE(stream_42->second.SendMessages());
            ASSERT_EQ(stream_42->second.Topics().size(), 2u);
            const auto topic_7 = stream_42->second.Topics().find(7);
            const auto topic_9 = stream_42->second.Topics().find(9);
            ASSERT_NE(topic_7, stream_42->second.Topics().end());
            ASSERT_NE(topic_9, stream_42->second.Topics().end());
            EXPECT_TRUE(topic_7->second.ManageTopic());
            EXPECT_FALSE(topic_7->second.ReadTopic());
            EXPECT_TRUE(topic_7->second.PollMessages());
            EXPECT_FALSE(topic_7->second.SendMessages());
            EXPECT_FALSE(topic_9->second.ManageTopic());
            EXPECT_TRUE(topic_9->second.ReadTopic());
            EXPECT_FALSE(topic_9->second.PollMessages());
            EXPECT_TRUE(topic_9->second.SendMessages());
            EXPECT_FALSE(stream_84->second.ManageStream());
            EXPECT_TRUE(stream_84->second.ReadStream());
            EXPECT_TRUE(stream_84->second.ManageTopics());
            EXPECT_FALSE(stream_84->second.ReadTopics());
            EXPECT_TRUE(stream_84->second.PollMessages());
            EXPECT_FALSE(stream_84->second.SendMessages());
            ASSERT_EQ(stream_84->second.Topics().size(), 1u);
            const auto topic_3 = stream_84->second.Topics().find(3);
            ASSERT_NE(topic_3, stream_84->second.Topics().end());
            EXPECT_EQ(topic_3->first, 3u);
            EXPECT_FALSE(topic_3->second.ManageTopic());
            EXPECT_TRUE(topic_3->second.ReadTopic());
            EXPECT_FALSE(topic_3->second.PollMessages());
            EXPECT_FALSE(topic_3->second.SendMessages());
        }
    });
}

TEST_F(E2E_Client, CreatedUserCanReadOnlyTopicGrantedByPermissions) {
    RecordProperty("description",
                   "Creates a user with read access to one topic, then verifies that topic can be fetched and a topic "
                   "in another stream is denied.");
    auto root_client                      = GetLoggedInHighLevelClient();
    auto user_client                      = GetLoggedOutHighLevelClient();
    const std::string allowed_stream_name = GetRandomName();
    const std::string denied_stream_name  = GetRandomName();
    const std::string allowed_topic_name  = GetRandomName();
    const std::string denied_topic_name   = GetRandomName();
    const std::string username            = GetRandomName(50);

    iggy::TopicCreateOptions topic_options;
    topic_options.SetPartitionsCount(1).SetCompressionAlgorithm(iggy::CompressionAlgorithm::None());

    const auto allowed_stream = root_client.CreateStream(allowed_stream_name);
    TrackStream(allowed_stream_name);
    const auto denied_stream = root_client.CreateStream(denied_stream_name);
    TrackStream(denied_stream_name);

    const auto allowed_topic =
        root_client.CreateTopic(iggy::Identifier::Numeric(allowed_stream.Id()), allowed_topic_name, topic_options);
    const auto denied_topic =
        root_client.CreateTopic(iggy::Identifier::Numeric(denied_stream.Id()), denied_topic_name, topic_options);

    iggy::TopicPermissions topic_permissions;
    topic_permissions.SetReadTopic(true);
    iggy::StreamPermissions stream_permissions;
    stream_permissions.SetTopics({{allowed_topic.Id(), topic_permissions}});
    iggy::Permissions permissions;
    permissions.SetStreams({{allowed_stream.Id(), stream_permissions}});
    ASSERT_NO_THROW({ CreateUser(root_client, username, "secret123", iggy::UserStatus::Active, permissions); });
    ASSERT_NO_THROW(user_client.Connect());
    ASSERT_NO_THROW(user_client.Login(username, "secret123"));

    const auto fetched_topic = user_client.GetTopic(iggy::Identifier::Numeric(allowed_stream.Id()),
                                                    iggy::Identifier::Numeric(allowed_topic.Id()));
    EXPECT_EQ(fetched_topic.Id(), allowed_topic.Id());
    EXPECT_EQ(fetched_topic.Name(), allowed_topic_name);
    ASSERT_THROW(user_client.GetTopic(iggy::Identifier::Numeric(denied_stream.Id()),
                                      iggy::Identifier::Numeric(denied_topic.Id())),
                 std::exception);
}

TEST_F(E2E_Client, CreateUserDuplicateStreamPermissionIdsCollapseToSingleEntry) {
    RecordProperty("description",
                   "Assigns two permission entries for stream ID 42, then verifies the second entry wins and the "
                   "user is stored with a single stream entry.");
    auto client                = GetLoggedInHighLevelClient();
    const std::string username = GetRandomName(50);
    iggy::StreamPermissions first_stream;
    iggy::StreamPermissions second_stream;
    second_stream.SetReadStream(true);
    std::map<std::uint32_t, iggy::StreamPermissions> streams;
    streams.insert_or_assign(42, first_stream);
    streams.insert_or_assign(42, second_stream);
    iggy::Permissions permissions;
    permissions.SetStreams(std::move(streams));

    ASSERT_NO_THROW({ CreateUser(client, username, "secret123", iggy::UserStatus::Active, permissions); });
    const auto fetched = client.GetUser(iggy::Identifier::String(username));
    ASSERT_TRUE(fetched.Permissions().has_value());
    ASSERT_EQ(fetched.Permissions()->Streams().size(), 1u);
    EXPECT_TRUE(fetched.Permissions()->Streams().at(42).ReadStream());
}

TEST_F(E2E_Client, CreateUserDuplicateTopicPermissionIdsCollapseToSingleEntry) {
    RecordProperty("description",
                   "Assigns two permission entries for topic ID 7 in the same stream, then verifies the second "
                   "entry wins and the user is stored with a single topic entry.");
    auto client                = GetLoggedInHighLevelClient();
    const std::string username = GetRandomName(50);
    iggy::TopicPermissions first_topic;
    iggy::TopicPermissions second_topic;
    second_topic.SetReadTopic(true);
    std::map<std::uint32_t, iggy::TopicPermissions> topics;
    topics.insert_or_assign(7, first_topic);
    topics.insert_or_assign(7, second_topic);
    iggy::StreamPermissions stream;
    stream.SetTopics(std::move(topics));
    iggy::Permissions permissions;
    permissions.SetStreams({{42, stream}});

    ASSERT_NO_THROW({ CreateUser(client, username, "secret123", iggy::UserStatus::Active, permissions); });
    const auto fetched = client.GetUser(iggy::Identifier::String(username));
    ASSERT_TRUE(fetched.Permissions().has_value());
    ASSERT_EQ(fetched.Permissions()->Streams().size(), 1u);
    ASSERT_EQ(fetched.Permissions()->Streams().at(42).Topics().size(), 1u);
    EXPECT_TRUE(fetched.Permissions()->Streams().at(42).Topics().at(7).ReadTopic());
}

TEST_F(E2E_Client, ReadUsersPermissionDoesNotAllowCreateUser) {
    RecordProperty("description", "Rejects user creation by a user with read_users but not manage_users.");
    auto root_client           = GetLoggedInHighLevelClient();
    auto user_client           = GetLoggedOutHighLevelClient();
    const std::string username = GetRandomName(50);
    const std::string target   = GetRandomName(50);
    iggy::GlobalPermissions global;
    global.SetReadUsers(true);
    iggy::Permissions permissions;
    permissions.SetGlobal(global);
    ASSERT_NO_THROW({ CreateUser(root_client, username, "secret123", iggy::UserStatus::Active, permissions); });
    ASSERT_NO_THROW(user_client.Connect());
    ASSERT_NO_THROW(user_client.Login(username, "secret123"));

    ASSERT_THROW(user_client.CreateUser(target, "secret123", iggy::UserStatus::Active), std::exception);
    ASSERT_THROW(root_client.GetUser(iggy::Identifier::String(target)), std::exception);
}

TEST_F(E2E_Client, ManageUsersPermissionAllowsGrantingAdditionalPermissions) {
    RecordProperty("description", "Allows a user manager to grant a child a permission the manager does not have.");
    auto root_client                   = GetLoggedInHighLevelClient();
    auto manager_client                = GetLoggedOutHighLevelClient();
    auto child_client                  = GetLoggedOutHighLevelClient();
    const std::string manager_username = GetRandomName(50);
    const std::string child_username   = GetRandomName(50);
    const std::string denied_stream    = GetRandomName();
    const std::string child_stream     = GetRandomName();
    iggy::GlobalPermissions manager_global;
    manager_global.SetManageUsers(true);
    iggy::Permissions manager_permissions;
    manager_permissions.SetGlobal(manager_global);
    ASSERT_NO_THROW(
        { CreateUser(root_client, manager_username, "secret123", iggy::UserStatus::Active, manager_permissions); });
    ASSERT_NO_THROW(manager_client.Connect());
    ASSERT_NO_THROW(manager_client.Login(manager_username, "secret123"));
    ASSERT_THROW(manager_client.CreateStream(denied_stream), std::exception);

    iggy::GlobalPermissions child_global;
    child_global.SetManageStreams(true);
    iggy::Permissions child_permissions;
    child_permissions.SetGlobal(child_global);
    const auto child =
        CreateUser(manager_client, child_username, "child-secret", iggy::UserStatus::Active, child_permissions);
    EXPECT_EQ(child.Username(), child_username);
    EXPECT_EQ(child.Status(), iggy::UserStatus::Active);
    const auto fetched = root_client.GetUser(iggy::Identifier::String(child_username));
    EXPECT_EQ(fetched.Id(), child.Id());
    ASSERT_TRUE(fetched.Permissions().has_value());
    EXPECT_TRUE(fetched.Permissions()->Global().ManageStreams());

    ASSERT_NO_THROW(child_client.Connect());
    ASSERT_NO_THROW(child_client.Login(child_username, "child-secret"));
    ASSERT_NO_THROW(child_client.CreateStream(child_stream));
    TrackStream(child_stream);
}

TEST_F(E2E_Client, CreatedActiveUserAuthenticatesOnlyWithSuppliedPassword) {
    RecordProperty("description", "Authenticates an active user only with its supplied password.");
    auto root_client           = GetLoggedInHighLevelClient();
    auto valid_client          = GetLoggedOutHighLevelClient();
    auto wrong_client          = GetLoggedOutHighLevelClient();
    const std::string username = GetRandomName(50);
    const std::string password = "known-secret";
    ASSERT_NO_THROW({ CreateUser(root_client, username, password, iggy::UserStatus::Active); });
    ASSERT_NO_THROW(valid_client.Connect());
    ASSERT_NO_THROW(wrong_client.Connect());
    ASSERT_NO_THROW(valid_client.Login(username, password));
    ASSERT_THROW(wrong_client.Login(username, "other-secret"), std::exception);
}

TEST_F(E2E_Client, CreatedInactiveUserCannotAuthenticate) {
    RecordProperty("description", "Persists inactive users but rejects authentication for them.");
    auto root_client           = GetLoggedInHighLevelClient();
    auto user_client           = GetLoggedOutHighLevelClient();
    const std::string username = GetRandomName(50);
    const std::string password = "inactive-secret";
    const auto created         = CreateUser(root_client, username, password, iggy::UserStatus::Inactive);
    const auto fetched         = root_client.GetUser(iggy::Identifier::String(username));
    EXPECT_EQ(created.Status(), iggy::UserStatus::Inactive);
    EXPECT_EQ(fetched.Status(), iggy::UserStatus::Inactive);
    ASSERT_NO_THROW(user_client.Connect());
    ASSERT_THROW(user_client.Login(username, password), std::exception);
}

TEST_F(E2E_Client, UpdateUserRejectsUnauthenticatedClientWithoutChangingTarget) {
    RecordProperty("description", "Rejects user updates without an active authenticated session.");
    auto root_client              = GetLoggedInHighLevelClient();
    const std::string username    = GetRandomName(50);
    const std::string replacement = GetRandomName(50);
    ASSERT_NO_THROW({
        const auto created = CreateUser(root_client, username, "secret123", iggy::UserStatus::Active);

        auto client = GetLoggedOutHighLevelClient();
        ASSERT_THROW(client.UpdateUser(iggy::Identifier::String(username), replacement, iggy::UserStatus::Inactive,
                                       iggy::UserUpdateOptions{}),
                     std::exception);
        ASSERT_NO_THROW(client.Connect());
        ASSERT_THROW(client.UpdateUser(iggy::Identifier::String(username), replacement, iggy::UserStatus::Inactive,
                                       iggy::UserUpdateOptions{}),
                     std::exception);
        ASSERT_NO_THROW(client.Login("iggy", "iggy"));
        ASSERT_NO_THROW(client.Logout());
        ASSERT_THROW(client.UpdateUser(iggy::Identifier::String(username), replacement, iggy::UserStatus::Inactive,
                                       iggy::UserUpdateOptions{}),
                     std::exception);
        ASSERT_NO_THROW(client.Login("iggy", "iggy"));
        ASSERT_NO_THROW(client.Disconnect());
        ASSERT_THROW(client.UpdateUser(iggy::Identifier::String(username), replacement, iggy::UserStatus::Inactive,
                                       iggy::UserUpdateOptions{}),
                     std::exception);

        const auto fetched = root_client.GetUser(iggy::Identifier::String(username));
        EXPECT_EQ(fetched.Id(), created.Id());
        EXPECT_EQ(fetched.Username(), username);
        EXPECT_EQ(fetched.Status(), iggy::UserStatus::Active);
    });
}

TEST_F(E2E_Client, UpdateUserRejectsUnknownUsernameAndNumericId) {
    RecordProperty("description", "Rejects updates for unknown username and numeric identifiers.");
    auto client                         = GetLoggedInHighLevelClient();
    const std::string unknown_username  = GetRandomName(50);
    const std::string proposed_username = GetRandomName(50);
    const auto unknown_id               = std::numeric_limits<std::uint32_t>::max();

    ASSERT_THROW(client.UpdateUser(iggy::Identifier::String(unknown_username), proposed_username,
                                   iggy::UserStatus::Inactive, iggy::UserUpdateOptions{}),
                 std::exception);
    ASSERT_THROW(client.UpdateUser(iggy::Identifier::Numeric(unknown_id), GetRandomName(50), iggy::UserStatus::Inactive,
                                   iggy::UserUpdateOptions{}),
                 std::exception);
    ASSERT_THROW(client.GetUser(iggy::Identifier::String(proposed_username)), std::exception);
}

TEST_F(E2E_Client, UpdateUserByUsernameChangesUsernameAndStatus) {
    RecordProperty("description", "Updates a user by username and changes both username and status.");
    auto client                   = GetLoggedInHighLevelClient();
    const std::string username    = GetRandomName(50);
    const std::string replacement = GetRandomName(50);
    ASSERT_NO_THROW({
        const auto created = CreateUser(client, username, "secret123", iggy::UserStatus::Active);
        ASSERT_NO_THROW(client.UpdateUser(iggy::Identifier::String(username), replacement, iggy::UserStatus::Inactive,
                                          iggy::UserUpdateOptions{}));
        RenameTrackedUser(username, replacement);

        ASSERT_THROW(client.GetUser(iggy::Identifier::String(username)), std::exception);
        const auto fetched = client.GetUser(iggy::Identifier::String(replacement));
        EXPECT_EQ(fetched.Id(), created.Id());
        EXPECT_EQ(fetched.Username(), replacement);
        EXPECT_EQ(fetched.Status(), iggy::UserStatus::Inactive);
    });
}

TEST_F(E2E_Client, UpdateUserByNumericIdChangesUsernameAndStatus) {
    RecordProperty("description", "Updates a user by numeric ID and changes both username and status.");
    auto client                   = GetLoggedInHighLevelClient();
    const std::string username    = GetRandomName(50);
    const std::string replacement = GetRandomName(50);
    ASSERT_NO_THROW({
        const auto created = CreateUser(client, username, "secret123", iggy::UserStatus::Inactive);
        ASSERT_NO_THROW(client.UpdateUser(iggy::Identifier::Numeric(created.Id()), replacement,
                                          iggy::UserStatus::Active, iggy::UserUpdateOptions{}));
        RenameTrackedUser(username, replacement);

        const auto by_id   = client.GetUser(iggy::Identifier::Numeric(created.Id()));
        const auto by_name = client.GetUser(iggy::Identifier::String(replacement));
        EXPECT_EQ(by_id.Id(), created.Id());
        EXPECT_EQ(by_name.Id(), created.Id());
        EXPECT_EQ(by_id.Username(), replacement);
        EXPECT_EQ(by_name.Username(), replacement);
        EXPECT_EQ(by_id.Status(), iggy::UserStatus::Active);
        EXPECT_EQ(by_name.Status(), iggy::UserStatus::Active);
    });
}

TEST_F(E2E_Client, UpdateUserAllowsUsernameAndStatusToBeUpdatedIndependently) {
    RecordProperty("description", "Updates either username or status without changing the other field.");
    auto client                   = GetLoggedInHighLevelClient();
    const std::string username    = GetRandomName(50);
    const std::string replacement = GetRandomName(50);
    ASSERT_NO_THROW({
        const auto created = CreateUser(client, username, "secret123", iggy::UserStatus::Active);

        ASSERT_NO_THROW(client.UpdateUser(iggy::Identifier::Numeric(created.Id()), std::nullopt,
                                          iggy::UserStatus::Inactive, iggy::UserUpdateOptions{}));
        const auto status_updated = client.GetUser(iggy::Identifier::Numeric(created.Id()));
        EXPECT_EQ(status_updated.Username(), username);
        EXPECT_EQ(status_updated.Status(), iggy::UserStatus::Inactive);

        ASSERT_NO_THROW(client.UpdateUser(iggy::Identifier::Numeric(created.Id()), replacement, std::nullopt,
                                          iggy::UserUpdateOptions{}));
        RenameTrackedUser(username, replacement);

        const auto username_updated = client.GetUser(iggy::Identifier::Numeric(created.Id()));
        EXPECT_EQ(username_updated.Username(), replacement);
        EXPECT_EQ(username_updated.Status(), iggy::UserStatus::Inactive);
    });
}

TEST_F(E2E_Client, UpdateUserWithoutChangesIsNoOp) {
    RecordProperty("description", "Accepts an empty user update without changing the user.");
    auto client                = GetLoggedInHighLevelClient();
    const std::string username = GetRandomName(50);
    ASSERT_NO_THROW({
        const auto created = CreateUser(client, username, "secret123", iggy::UserStatus::Active);

        ASSERT_NO_THROW(client.UpdateUser(iggy::Identifier::Numeric(created.Id()), std::nullopt, std::nullopt,
                                          iggy::UserUpdateOptions{}));

        const auto fetched = client.GetUser(iggy::Identifier::Numeric(created.Id()));
        EXPECT_EQ(fetched.Id(), created.Id());
        EXPECT_EQ(fetched.CreatedAt(), created.CreatedAt());
        EXPECT_EQ(fetched.Username(), created.Username());
        EXPECT_EQ(fetched.Status(), created.Status());
    });
}

TEST_F(E2E_Client, UpdateUserAcceptsUsernameLengthBounds) {
    RecordProperty("description", "Accepts exact three-byte and fifty-byte username boundaries.");
    auto client                         = GetLoggedInHighLevelClient();
    const std::string first_username    = GetRandomName(50);
    const std::string second_username   = GetRandomName(50);
    const std::string first_replacement = GetRandomName(3);
    std::string second_replacement      = GetRandomName(50);
    second_replacement.resize(50, 'a');
    ASSERT_EQ(second_replacement.size(), 50u);
    ASSERT_NO_THROW({
        const auto first  = CreateUser(client, first_username, "secret123", iggy::UserStatus::Active);
        const auto second = CreateUser(client, second_username, "secret123", iggy::UserStatus::Active);
        ASSERT_NO_THROW(client.UpdateUser(iggy::Identifier::String(first_username), first_replacement,
                                          iggy::UserStatus::Active, iggy::UserUpdateOptions{}));
        RenameTrackedUser(first_username, first_replacement);
        ASSERT_NO_THROW(client.UpdateUser(iggy::Identifier::String(second_username), second_replacement,
                                          iggy::UserStatus::Active, iggy::UserUpdateOptions{}));
        RenameTrackedUser(second_username, second_replacement);

        const auto fetched_first  = client.GetUser(iggy::Identifier::String(first_replacement));
        const auto fetched_second = client.GetUser(iggy::Identifier::String(second_replacement));
        EXPECT_EQ(fetched_first.Id(), first.Id());
        EXPECT_EQ(fetched_second.Id(), second.Id());
        EXPECT_EQ(fetched_first.Username(), first_replacement);
        EXPECT_EQ(fetched_second.Username(), second_replacement);
    });
}

TEST_F(E2E_Client, UpdateUserRejectsUsernameOutsideLengthBounds) {
    RecordProperty("description", "Rejects username sizes outside the SDK and server limits.");
    auto client                           = GetLoggedInHighLevelClient();
    const std::string source              = GetRandomName(50);
    const std::string invalid_usernames[] = {
        "", "a", "aa", std::string(51, 'c'), std::string(255, 'd'), std::string(256, 'e')};
    ASSERT_NO_THROW({
        const auto created = CreateUser(client, source, "secret123", iggy::UserStatus::Active);

        for (const auto &replacement : invalid_usernames) {
            ASSERT_THROW(client.UpdateUser(iggy::Identifier::String(source), replacement, iggy::UserStatus::Active,
                                           iggy::UserUpdateOptions{}),
                         std::exception);
        }
        const auto fetched = client.GetUser(iggy::Identifier::String(source));
        EXPECT_EQ(fetched.Id(), created.Id());
        EXPECT_EQ(fetched.Status(), iggy::UserStatus::Active);
        EXPECT_EQ(fetched.Username(), source);
    });
}

TEST_F(E2E_Client, UpdateUserAcceptsNonAsciiAndNonAlphabeticUsername) {
    RecordProperty("description", "Accepts representative non-ASCII and non-alphabetic usernames.");
    auto client                      = GetLoggedInHighLevelClient();
    const std::string suffix         = GetRandomName(8);
    const std::string sources[]      = {GetRandomName(50), GetRandomName(50), GetRandomName(50)};
    const std::string replacements[] = {"!@#_" + suffix, "ユーザー_" + suffix, "😀🚀_" + suffix};
    ASSERT_LE(replacements[0].size(), 50u);
    ASSERT_LE(replacements[1].size(), 50u);
    ASSERT_LE(replacements[2].size(), 50u);
    for (std::size_t index = 0; index < 3; ++index) {
        ASSERT_NO_THROW({
            const auto created = CreateUser(client, sources[index], "secret123", iggy::UserStatus::Active);
            ASSERT_NO_THROW(client.UpdateUser(iggy::Identifier::String(sources[index]), replacements[index],
                                              iggy::UserStatus::Active, iggy::UserUpdateOptions{}));
            RenameTrackedUser(sources[index], replacements[index]);
            const auto fetched = client.GetUser(iggy::Identifier::String(replacements[index]));
            EXPECT_EQ(fetched.Id(), created.Id());
            EXPECT_EQ(fetched.Username(), replacements[index]);
        });
    }
}

TEST_F(E2E_Client, UpdateUserRejectsInvalidStatusWithoutRenamingTarget) {
    RecordProperty("description", "Rejects invalid status codes atomically with username changes.");
    auto client                       = GetLoggedInHighLevelClient();
    const std::string username        = GetRandomName(50);
    const iggy::UserStatus statuses[] = {
        static_cast<iggy::UserStatus>(0),
        static_cast<iggy::UserStatus>(3),
        static_cast<iggy::UserStatus>(std::numeric_limits<std::uint8_t>::max()),
    };
    ASSERT_NO_THROW({
        const auto created = CreateUser(client, username, "secret123", iggy::UserStatus::Active);
        for (const auto status : statuses) {
            const std::string replacement = GetRandomName(50);
            ASSERT_THROW(
                client.UpdateUser(iggy::Identifier::String(username), replacement, status, iggy::UserUpdateOptions{}),
                std::exception);
            ASSERT_THROW(client.GetUser(iggy::Identifier::String(replacement)), std::exception);
        }
        const auto fetched = client.GetUser(iggy::Identifier::String(username));
        EXPECT_EQ(fetched.Id(), created.Id());
        EXPECT_EQ(fetched.Status(), iggy::UserStatus::Active);
        EXPECT_EQ(fetched.Username(), username);
    });
}

TEST_F(E2E_Client, UpdateUserRejectsDuplicateUsernameWithoutChangingStatus) {
    RecordProperty("description", "Rejects duplicate usernames without partially applying the status update.");
    auto client                         = GetLoggedInHighLevelClient();
    const std::string target_username   = GetRandomName(50);
    const std::string conflict_username = GetRandomName(50);
    ASSERT_NO_THROW({
        const auto target   = CreateUser(client, target_username, "secret123", iggy::UserStatus::Active);
        const auto conflict = CreateUser(client, conflict_username, "secret123", iggy::UserStatus::Inactive);
        ASSERT_THROW(client.UpdateUser(iggy::Identifier::String(target_username), conflict_username,
                                       iggy::UserStatus::Inactive, iggy::UserUpdateOptions{}),
                     std::exception);
        const auto fetched_target   = client.GetUser(iggy::Identifier::String(target_username));
        const auto fetched_conflict = client.GetUser(iggy::Identifier::String(conflict_username));
        EXPECT_EQ(fetched_target.Id(), target.Id());
        EXPECT_EQ(fetched_conflict.Id(), conflict.Id());
        EXPECT_EQ(fetched_target.Status(), iggy::UserStatus::Active);
        EXPECT_EQ(fetched_conflict.Status(), iggy::UserStatus::Inactive);
    });
}

TEST_F(E2E_Client, UpdateUserAllowsCurrentUsernameWhileChangingStatus) {
    RecordProperty("description", "Allows a same-username update that changes status.");
    auto client                = GetLoggedInHighLevelClient();
    const std::string username = GetRandomName(50);
    ASSERT_NO_THROW({
        const auto created = CreateUser(client, username, "secret123", iggy::UserStatus::Active);
        ASSERT_NO_THROW(client.UpdateUser(iggy::Identifier::String(username), username, iggy::UserStatus::Inactive,
                                          iggy::UserUpdateOptions{}));
        const auto fetched = client.GetUser(iggy::Identifier::String(username));
        EXPECT_EQ(fetched.Id(), created.Id());
        EXPECT_EQ(fetched.Status(), iggy::UserStatus::Inactive);
        EXPECT_EQ(fetched.Username(), username);
    });
}

TEST_F(E2E_Client, UpdateUserPreservesPasswordPermissionsAndCreationData) {
    RecordProperty("description", "Preserves password, permissions, ID, and creation timestamp after a rename.");
    auto root_client              = GetLoggedInHighLevelClient();
    auto valid_client             = GetLoggedOutHighLevelClient();
    auto old_client               = GetLoggedOutHighLevelClient();
    const std::string username    = GetRandomName(50);
    const std::string replacement = GetRandomName(50);
    iggy::GlobalPermissions global;
    global.SetReadUsers(true).SetReadStreams(true).SetSendMessages(true);
    iggy::Permissions permissions;
    permissions.SetGlobal(global);
    ASSERT_NO_THROW({
        const auto created = CreateUser(root_client, username, "known-secret", iggy::UserStatus::Active, permissions);
        ASSERT_NO_THROW(root_client.UpdateUser(iggy::Identifier::String(username), replacement,
                                               iggy::UserStatus::Active, iggy::UserUpdateOptions{}));
        RenameTrackedUser(username, replacement);
        const auto fetched = root_client.GetUser(iggy::Identifier::String(replacement));
        EXPECT_EQ(fetched.Id(), created.Id());
        EXPECT_EQ(fetched.CreatedAt(), created.CreatedAt());
        ASSERT_TRUE(fetched.Permissions().has_value());
        EXPECT_TRUE(fetched.Permissions()->Global().ReadUsers());
        EXPECT_TRUE(fetched.Permissions()->Global().ReadStreams());
        EXPECT_TRUE(fetched.Permissions()->Global().SendMessages());
    });
    ASSERT_NO_THROW(valid_client.Connect());
    ASSERT_NO_THROW(valid_client.Login(replacement, "known-secret"));
    ASSERT_NO_THROW(old_client.Connect());
    ASSERT_THROW(old_client.Login(username, "known-secret"), std::exception);
}

TEST_F(E2E_Client, UpdateUserToInactiveBlocksFreshLoginUntilReactivated) {
    RecordProperty("description", "Blocks fresh login while inactive and restores it when active.");
    auto root_client           = GetLoggedInHighLevelClient();
    auto user_client           = GetLoggedOutHighLevelClient();
    const std::string username = GetRandomName(50);
    ASSERT_NO_THROW({ CreateUser(root_client, username, "known-secret", iggy::UserStatus::Active); });
    ASSERT_NO_THROW(root_client.UpdateUser(iggy::Identifier::String(username), username, iggy::UserStatus::Inactive,
                                           iggy::UserUpdateOptions{}));
    ASSERT_NO_THROW(user_client.Connect());
    ASSERT_THROW(user_client.Login(username, "known-secret"), std::exception);
    ASSERT_NO_THROW(root_client.UpdateUser(iggy::Identifier::String(username), username, iggy::UserStatus::Active,
                                           iggy::UserUpdateOptions{}));
    ASSERT_NO_THROW(user_client.Login(username, "known-secret"));
}

TEST_F(E2E_Client, UpdateUserRenameMakesOldUsernameReusable) {
    RecordProperty("description", "Releases the old username for a new user after a successful rename.");
    auto client                    = GetLoggedInHighLevelClient();
    const std::string old_username = GetRandomName(50);
    const std::string new_username = GetRandomName(50);
    ASSERT_NO_THROW({
        const auto first = CreateUser(client, old_username, "secret123", iggy::UserStatus::Active);
        ASSERT_NO_THROW(client.UpdateUser(iggy::Identifier::String(old_username), new_username,
                                          iggy::UserStatus::Active, iggy::UserUpdateOptions{}));
        RenameTrackedUser(old_username, new_username);
        const auto second = CreateUser(client, old_username, "secret123", iggy::UserStatus::Active);
        EXPECT_EQ(client.GetUser(iggy::Identifier::String(new_username)).Username(), new_username);
        EXPECT_EQ(client.GetUser(iggy::Identifier::String(old_username)).Username(), old_username);
        EXPECT_NE(first.Id(), second.Id());
    });
}

TEST_F(E2E_Client, UpdateUserRejectsRenameToRootUsername) {
    RecordProperty("description", "Rejects changing a non-root user's username to root's username.");
    auto client                = GetLoggedInHighLevelClient();
    const std::string username = GetRandomName(50);
    ASSERT_NO_THROW({
        const auto target = CreateUser(client, username, "secret123", iggy::UserStatus::Active);
        ASSERT_THROW(client.UpdateUser(iggy::Identifier::String(username), "iggy", iggy::UserStatus::Inactive,
                                       iggy::UserUpdateOptions{}),
                     std::exception);
        const auto root    = client.GetUser(iggy::Identifier::String("iggy"));
        const auto fetched = client.GetUser(iggy::Identifier::String(username));
        EXPECT_EQ(root.Id(), 0u);
        EXPECT_EQ(root.Username(), "iggy");
        EXPECT_EQ(root.Status(), iggy::UserStatus::Active);
        EXPECT_EQ(fetched.Id(), target.Id());
        EXPECT_EQ(fetched.Status(), iggy::UserStatus::Active);
        EXPECT_EQ(fetched.Username(), username);
    });
}

TEST_F(E2E_Client, UpdateUserWithUnsupportedOptionsRejectsAndPreservesUser) {
    RecordProperty("description", "Rejects unsupported user options without changing the user.");
    auto client                   = GetLoggedInHighLevelClient();
    const std::string username    = GetRandomName(50);
    const std::string replacement = GetRandomName(50);
    ASSERT_NO_THROW({
        const auto created = CreateUser(client, username, "secret123", iggy::UserStatus::Active);
        iggy::UserUpdateOptions options;
        options.SetRawEntries({{"not_a_real_option", "true"}});

        ASSERT_THROW(
            client.UpdateUser(iggy::Identifier::String(username), replacement, iggy::UserStatus::Inactive, options),
            iggy::IggyException);

        const auto fetched = client.GetUser(iggy::Identifier::String(username));
        EXPECT_EQ(fetched.Id(), created.Id());
        EXPECT_EQ(fetched.Username(), username);
        EXPECT_EQ(fetched.Status(), iggy::UserStatus::Active);
        ASSERT_THROW(client.GetUser(iggy::Identifier::String(replacement)), iggy::IggyException);
    });
}

TEST_F(E2E_Client, ReadUsersPermissionDoesNotAllowUpdateUser) {
    RecordProperty("description", "Rejects updates from a user with read_users but not manage_users.");
    auto root_client                  = GetLoggedInHighLevelClient();
    auto caller_client                = GetLoggedOutHighLevelClient();
    const std::string caller_username = GetRandomName(50);
    const std::string target_username = GetRandomName(50);
    iggy::GlobalPermissions global;
    global.SetReadUsers(true);
    iggy::Permissions permissions;
    permissions.SetGlobal(global);
    ASSERT_NO_THROW({ CreateUser(root_client, caller_username, "secret123", iggy::UserStatus::Active, permissions); });
    const auto target = CreateUser(root_client, target_username, "secret123", iggy::UserStatus::Active);
    ASSERT_NO_THROW(caller_client.Connect());
    ASSERT_NO_THROW(caller_client.Login(caller_username, "secret123"));
    ASSERT_THROW(caller_client.UpdateUser(iggy::Identifier::String(target_username), GetRandomName(50),
                                          iggy::UserStatus::Inactive, iggy::UserUpdateOptions{}),
                 std::exception);
    ASSERT_THROW(caller_client.UpdateUser(iggy::Identifier::String(caller_username), GetRandomName(50),
                                          iggy::UserStatus::Inactive, iggy::UserUpdateOptions{}),
                 std::exception);
    const auto fetched = root_client.GetUser(iggy::Identifier::String(target_username));
    EXPECT_EQ(fetched.Id(), target.Id());
    EXPECT_EQ(fetched.Status(), iggy::UserStatus::Active);
}

TEST_F(E2E_Client, ManageUsersPermissionAllowsUpdateWithoutReadUsers) {
    RecordProperty("description", "Allows updates from a manager without read_users permission.");
    auto root_client                   = GetLoggedInHighLevelClient();
    auto manager_client                = GetLoggedOutHighLevelClient();
    const std::string manager_username = GetRandomName(50);
    const std::string target_username  = GetRandomName(50);
    const std::string replacement      = GetRandomName(50);
    iggy::GlobalPermissions global;
    global.SetManageUsers(true);
    iggy::Permissions permissions;
    permissions.SetGlobal(global);
    ASSERT_NO_THROW({ CreateUser(root_client, manager_username, "secret123", iggy::UserStatus::Active, permissions); });
    const auto target = CreateUser(root_client, target_username, "secret123", iggy::UserStatus::Active);
    ASSERT_NO_THROW(manager_client.Connect());
    ASSERT_NO_THROW(manager_client.Login(manager_username, "secret123"));
    ASSERT_NO_THROW(manager_client.UpdateUser(iggy::Identifier::String(target_username), replacement,
                                              iggy::UserStatus::Inactive, iggy::UserUpdateOptions{}));
    RenameTrackedUser(target_username, replacement);
    const auto fetched = root_client.GetUser(iggy::Identifier::String(replacement));
    EXPECT_EQ(fetched.Id(), target.Id());
    EXPECT_EQ(fetched.Status(), iggy::UserStatus::Inactive);
    EXPECT_EQ(fetched.Username(), replacement);
}

TEST_F(E2E_Client, GetUserBeforeLoginThrows) {
    RecordProperty("description", "Rejects user lookup without an active authenticated session.");
    auto client = GetLoggedOutHighLevelClient();

    ASSERT_THROW(client.GetUser(iggy::Identifier::String("iggy")), std::exception);
    ASSERT_NO_THROW(client.Connect());
    ASSERT_THROW(client.GetUser(iggy::Identifier::String("iggy")), std::exception);
    ASSERT_NO_THROW(client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(client.Logout());
    ASSERT_THROW(client.GetUser(iggy::Identifier::String("iggy")), std::exception);
    ASSERT_NO_THROW(client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(client.Disconnect());
    ASSERT_THROW(client.GetUser(iggy::Identifier::String("iggy")), std::exception);
}

TEST_F(E2E_Client, GetUserByUsernameReturnsRootDetails) {
    RecordProperty("description", "Returns deterministic root details for a username lookup.");
    auto client = GetLoggedInHighLevelClient();

    const auto user = client.GetUser(iggy::Identifier::String("iggy"));

    EXPECT_EQ(user.Id(), 0u);
    EXPECT_EQ(user.Username(), "iggy");
    EXPECT_EQ(user.Status(), iggy::UserStatus::Active);
}

TEST_F(E2E_Client, GetUserByNumericIdMatchesUsernameLookup) {
    RecordProperty("description", "Returns equivalent root details for username and numeric identifiers.");
    auto client = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW({
        const auto by_username = client.GetUser(iggy::Identifier::String("iggy"));
        const auto by_id       = client.GetUser(iggy::Identifier::Numeric(0));

        EXPECT_EQ(by_id.Id(), by_username.Id());
        EXPECT_EQ(by_id.Status(), by_username.Status());
        EXPECT_EQ(by_id.Username(), by_username.Username());
        ASSERT_TRUE(by_id.Permissions().has_value());
        ASSERT_TRUE(by_username.Permissions().has_value());
        const auto &by_id_permissions       = by_id.Permissions().value();
        const auto &by_username_permissions = by_username.Permissions().value();
        EXPECT_EQ(by_id_permissions.Global().ManageServers(), by_username_permissions.Global().ManageServers());
        EXPECT_EQ(by_id_permissions.Global().ReadServers(), by_username_permissions.Global().ReadServers());
        EXPECT_EQ(by_id_permissions.Global().ManageUsers(), by_username_permissions.Global().ManageUsers());
        EXPECT_EQ(by_id_permissions.Global().ReadUsers(), by_username_permissions.Global().ReadUsers());
        EXPECT_EQ(by_id_permissions.Global().ManageStreams(), by_username_permissions.Global().ManageStreams());
        EXPECT_EQ(by_id_permissions.Global().ReadStreams(), by_username_permissions.Global().ReadStreams());
        EXPECT_EQ(by_id_permissions.Global().ManageTopics(), by_username_permissions.Global().ManageTopics());
        EXPECT_EQ(by_id_permissions.Global().ReadTopics(), by_username_permissions.Global().ReadTopics());
        EXPECT_EQ(by_id_permissions.Global().PollMessages(), by_username_permissions.Global().PollMessages());
        EXPECT_EQ(by_id_permissions.Global().SendMessages(), by_username_permissions.Global().SendMessages());
        EXPECT_EQ(by_id_permissions.Streams().size(), by_username_permissions.Streams().size());
    });
}

TEST_F(E2E_Client, GetUserWithUnknownIdentifierThrows) {
    RecordProperty("description", "Rejects lookups for unknown username and numeric identifiers.");
    auto client                    = GetLoggedInHighLevelClient();
    const std::string unknown_user = GetRandomName(50);

    ASSERT_THROW(client.GetUser(iggy::Identifier::String(unknown_user)), std::exception);
    ASSERT_THROW(client.GetUser(iggy::Identifier::Numeric(std::numeric_limits<std::uint32_t>::max())), std::exception);
}

TEST_F(E2E_Client, GetUsersBeforeLoginThrows) {
    RecordProperty("description", "Rejects listing users without an active authenticated session.");
    auto client = GetLoggedOutHighLevelClient();

    ASSERT_THROW(client.GetUsers(), std::exception);
    ASSERT_NO_THROW(client.Connect());
    ASSERT_THROW(client.GetUsers(), std::exception);
    ASSERT_NO_THROW(client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(client.Logout());
    ASSERT_THROW(client.GetUsers(), std::exception);
    ASSERT_NO_THROW(client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(client.Disconnect());
    ASSERT_THROW(client.GetUsers(), std::exception);
}

TEST_F(E2E_Client, GetUserAllowsSelfLookupWithoutReadUsersPermission) {
    RecordProperty("description", "Allows a user without read_users permission to read its own details.");
    auto root_client           = GetLoggedInHighLevelClient();
    const std::string username = GetRandomName(50);
    iggy::GlobalPermissions global;
    global.SetReadUsers(false);
    iggy::Permissions permissions;
    permissions.SetGlobal(global);
    ASSERT_NO_THROW({
        const auto created_user = CreateUser(root_client, username, "secret123", iggy::UserStatus::Active, permissions);

        auto user_client = GetLoggedOutHighLevelClient();
        ASSERT_NO_THROW(user_client.Connect());
        ASSERT_NO_THROW(user_client.Login(username, "secret123"));

        const auto by_username = user_client.GetUser(iggy::Identifier::String(username));
        const auto by_id       = user_client.GetUser(iggy::Identifier::Numeric(created_user.Id()));

        EXPECT_EQ(by_username.Id(), created_user.Id());
        EXPECT_EQ(by_id.Id(), created_user.Id());
        EXPECT_EQ(by_username.Username(), username);
        EXPECT_EQ(by_id.Username(), username);
        EXPECT_EQ(by_username.Status(), iggy::UserStatus::Active);
        EXPECT_EQ(by_id.Status(), iggy::UserStatus::Active);
        ASSERT_TRUE(by_username.Permissions().has_value());
        ASSERT_TRUE(by_id.Permissions().has_value());
        EXPECT_FALSE(by_username.Permissions()->Global().ReadUsers());
        EXPECT_FALSE(by_id.Permissions()->Global().ReadUsers());
    });
}

TEST_F(E2E_Client, UserWithoutReadUsersPermissionCannotQueryOtherUsers) {
    RecordProperty("description", "Rejects other-user lookup and listing without read_users permission.");
    auto root_client           = GetLoggedInHighLevelClient();
    const std::string username = GetRandomName(50);
    iggy::GlobalPermissions global;
    global.SetReadUsers(false);
    iggy::Permissions permissions;
    permissions.SetGlobal(global);
    ASSERT_NO_THROW({ CreateUser(root_client, username, "secret123", iggy::UserStatus::Active, permissions); });

    auto user_client = GetLoggedOutHighLevelClient();
    ASSERT_NO_THROW(user_client.Connect());
    ASSERT_NO_THROW(user_client.Login(username, "secret123"));

    ASSERT_THROW(user_client.GetUser(iggy::Identifier::Numeric(0)), std::exception);
    ASSERT_THROW(user_client.GetUsers(), std::exception);
}

TEST_F(E2E_Client, ReadUsersPermissionAllowsUserQueries) {
    RecordProperty("description", "Allows user lookup and listing with only read_users permission.");
    auto root_client           = GetLoggedInHighLevelClient();
    const std::string username = GetRandomName(50);
    iggy::GlobalPermissions global;
    global.SetReadUsers(true);
    iggy::Permissions permissions;
    permissions.SetGlobal(global);
    ASSERT_NO_THROW({ CreateUser(root_client, username, "secret123", iggy::UserStatus::Active, permissions); });

    auto user_client = GetLoggedOutHighLevelClient();
    ASSERT_NO_THROW(user_client.Connect());
    ASSERT_NO_THROW(user_client.Login(username, "secret123"));

    const auto root  = user_client.GetUser(iggy::Identifier::Numeric(0));
    const auto users = user_client.GetUsers();

    EXPECT_EQ(root.Id(), 0u);
    EXPECT_EQ(root.Username(), "iggy");
    bool found_self = false;
    for (const auto &user : users) {
        if (user.Username() == username) {
            found_self = true;
        }
    }
    EXPECT_TRUE(found_self);
}

TEST_F(E2E_Client, ManageUsersPermissionImpliesReadUsers) {
    RecordProperty("description", "Allows user lookup and listing when manage_users is set without read_users.");
    auto root_client           = GetLoggedInHighLevelClient();
    const std::string username = GetRandomName(50);
    iggy::GlobalPermissions global;
    global.SetManageUsers(true);
    global.SetReadUsers(false);
    iggy::Permissions permissions;
    permissions.SetGlobal(global);
    ASSERT_NO_THROW({ CreateUser(root_client, username, "secret123", iggy::UserStatus::Active, permissions); });

    auto user_client = GetLoggedOutHighLevelClient();
    ASSERT_NO_THROW(user_client.Connect());
    ASSERT_NO_THROW(user_client.Login(username, "secret123"));

    const auto root  = user_client.GetUser(iggy::Identifier::Numeric(0));
    const auto users = user_client.GetUsers();

    EXPECT_EQ(root.Id(), 0u);
    EXPECT_EQ(root.Username(), "iggy");
    bool found_self = false;
    for (const auto &user : users) {
        if (user.Username() == username) {
            found_self = true;
        }
    }
    EXPECT_TRUE(found_self);
}

TEST_F(E2E_Client, GetUsersContainsCreatedUser) {
    RecordProperty("description", "Lists a created user with the input username and status.");
    auto client                = GetLoggedInHighLevelClient();
    const std::string username = GetRandomName(50);
    ASSERT_NO_THROW({
        const auto created_user = CreateUser(client, username, "secret123", iggy::UserStatus::Active);

        const auto users = client.GetUsers();

        bool found_user = false;
        for (const auto &user : users) {
            if (user.Id() == created_user.Id()) {
                found_user = true;
                EXPECT_EQ(user.Username(), username);
                EXPECT_EQ(user.Status(), iggy::UserStatus::Active);
            }
        }
        EXPECT_TRUE(found_user);
    });
}

TEST_F(E2E_Client, GetUsersReturnsAllCreatedUsersOrderedById) {
    RecordProperty("description", "Returns every created user without pagination, ordered by ID over TCP.");
    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW({
        const auto users_before = client.GetUsers();

        const std::string first_username  = GetRandomName(50);
        const std::string second_username = GetRandomName(50);
        const std::string third_username  = GetRandomName(50);
        const auto first_user             = CreateUser(client, first_username, "secret123", iggy::UserStatus::Active);
        const auto second_user            = CreateUser(client, second_username, "secret123", iggy::UserStatus::Active);
        const auto third_user             = CreateUser(client, third_username, "secret123", iggy::UserStatus::Active);

        const auto users_after = client.GetUsers();
        ASSERT_EQ(users_after.size(), users_before.size() + 3);

        bool found_first  = false;
        bool found_second = false;
        bool found_third  = false;
        for (std::size_t index = 0; index < users_after.size(); ++index) {
            if (index > 0) {
                EXPECT_LT(users_after[index - 1].Id(), users_after[index].Id());
            }
            const auto &user           = users_after[index];
            const std::string username = user.Username();
            if (user.Id() == first_user.Id() && username == first_username) {
                found_first = true;
            }
            if (user.Id() == second_user.Id() && username == second_username) {
                found_second = true;
            }
            if (user.Id() == third_user.Id() && username == third_username) {
                found_third = true;
            }
        }
        EXPECT_TRUE(found_first);
        EXPECT_TRUE(found_second);
        EXPECT_TRUE(found_third);
    });
}

TEST_F(E2E_Client, GetUsersMatchesGetUserDetails) {
    RecordProperty("description", "Returns consistent ID, username, and status from user query APIs.");
    auto client                = GetLoggedInHighLevelClient();
    const std::string username = GetRandomName(50);
    ASSERT_NO_THROW({
        const auto created_user = CreateUser(client, username, "secret123", iggy::UserStatus::Active);

        const auto user_details = client.GetUser(iggy::Identifier::String(username));
        const auto users        = client.GetUsers();

        EXPECT_EQ(user_details.Id(), created_user.Id());
        EXPECT_EQ(user_details.Username(), username);
        EXPECT_EQ(user_details.Status(), iggy::UserStatus::Active);
        bool found_user = false;
        for (const auto &user : users) {
            if (user.Id() == user_details.Id()) {
                found_user = true;
                EXPECT_EQ(user.Username(), username);
                EXPECT_EQ(user.Status(), iggy::UserStatus::Active);
            }
        }
        EXPECT_TRUE(found_user);
    });
}

TEST_F(E2E_Client, DeletedUserDisappearsFromGetUserAndGetUsers) {
    RecordProperty("description", "Removes a deleted user from detail and list queries.");
    auto client                = GetLoggedInHighLevelClient();
    const std::string username = GetRandomName(50);
    const auto created_user    = CreateUser(client, username, "secret123", iggy::UserStatus::Active);
    ASSERT_NO_THROW(client.DeleteUser(iggy::Identifier::String(username)));
    ForgetUser(username);

    ASSERT_THROW(client.GetUser(iggy::Identifier::String(username)), std::exception);
    const auto users = client.GetUsers();

    for (const auto &user : users) {
        EXPECT_FALSE(user.Id() == created_user.Id() && user.Username() == username);
    }
}

TEST_F(E2E_Client, DeleteUserRejectsUnauthenticatedClientWithoutDeletingTarget) {
    RecordProperty("description", "Rejects user deletion without an active authenticated session.");
    auto root_client           = GetLoggedInHighLevelClient();
    auto client                = GetLoggedOutHighLevelClient();
    const std::string username = GetRandomName(50);
    ASSERT_NO_THROW({
        const auto created_user = CreateUser(root_client, username, "secret123", iggy::UserStatus::Active);

        ASSERT_THROW(client.DeleteUser(iggy::Identifier::String(username)), std::exception);
        ASSERT_NO_THROW(client.Connect());
        ASSERT_THROW(client.DeleteUser(iggy::Identifier::String(username)), std::exception);
        ASSERT_NO_THROW(client.Login("iggy", "iggy"));
        ASSERT_NO_THROW(client.Logout());
        ASSERT_THROW(client.DeleteUser(iggy::Identifier::String(username)), std::exception);
        ASSERT_NO_THROW(client.Login("iggy", "iggy"));
        ASSERT_NO_THROW(client.Disconnect());
        ASSERT_THROW(client.DeleteUser(iggy::Identifier::String(username)), std::exception);

        const auto fetched_user = root_client.GetUser(iggy::Identifier::String(username));
        EXPECT_EQ(fetched_user.Id(), created_user.Id());
    });
}

TEST_F(E2E_Client, DeleteUserRejectsUnknownUsernameAndNumericId) {
    RecordProperty("description", "Rejects deletion of unknown string and numeric user identifiers.");
    auto client                        = GetLoggedInHighLevelClient();
    const std::string unknown_user     = GetRandomName(50);
    constexpr std::uint32_t unknown_id = std::numeric_limits<std::uint32_t>::max();

    ASSERT_THROW(client.DeleteUser(iggy::Identifier::String(unknown_user)), std::exception);
    ASSERT_THROW(client.DeleteUser(iggy::Identifier::Numeric(unknown_id)), std::exception);
}

TEST_F(E2E_Client, DeleteUserRejectsRootWithoutChangingRoot) {
    RecordProperty("description", "Rejects root deletion by username and numeric ID without changing root.");
    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW({
        const auto before_by_username = client.GetUser(iggy::Identifier::String("iggy"));
        const auto before_by_id       = client.GetUser(iggy::Identifier::Numeric(0));
        ASSERT_EQ(before_by_username.Id(), 0u);
        ASSERT_EQ(before_by_id.Id(), 0u);

        ASSERT_THROW(client.DeleteUser(iggy::Identifier::String("iggy")), std::exception);
        ASSERT_THROW(client.DeleteUser(iggy::Identifier::Numeric(0)), std::exception);

        const auto after_by_username = client.GetUser(iggy::Identifier::String("iggy"));
        const auto after_by_id       = client.GetUser(iggy::Identifier::Numeric(0));
        for (const auto *root : {&after_by_username, &after_by_id}) {
            EXPECT_EQ(root->Id(), 0u);
            EXPECT_EQ(root->Username(), "iggy");
            EXPECT_EQ(root->Status(), iggy::UserStatus::Active);
        }
    });
}

TEST_F(E2E_Client, DeleteUserByNumericIdRemovesTarget) {
    RecordProperty("description", "Deletes a user selected by its numeric ID.");
    auto client                = GetLoggedInHighLevelClient();
    const std::string username = GetRandomName(50);
    ASSERT_NO_THROW({
        const auto created_user = CreateUser(client, username, "secret123", iggy::UserStatus::Active);

        ASSERT_NO_THROW(client.DeleteUser(iggy::Identifier::Numeric(created_user.Id())));
        ForgetUser(username);

        ASSERT_THROW(client.GetUser(iggy::Identifier::String(username)), std::exception);
        const auto users = client.GetUsers();
        for (const auto &user : users) {
            EXPECT_FALSE(user.Id() == created_user.Id() && user.Username() == username);
        }
    });
}

TEST_F(E2E_Client, DeleteUserRejectsRepeatedDeletion) {
    RecordProperty("description", "Rejects deleting the same user twice.");
    auto client                = GetLoggedInHighLevelClient();
    const std::string username = GetRandomName(50);
    ASSERT_NO_THROW({
        const auto created_user = CreateUser(client, username, "secret123", iggy::UserStatus::Active);

        ASSERT_NO_THROW(client.DeleteUser(iggy::Identifier::String(username)));
        ForgetUser(username);
        ASSERT_THROW(client.DeleteUser(iggy::Identifier::String(username)), std::exception);

        const auto users = client.GetUsers();
        for (const auto &user : users) {
            EXPECT_FALSE(user.Id() == created_user.Id() && user.Username() == username);
        }
    });
}

TEST_F(E2E_Client, ReadUsersPermissionDoesNotAllowDeleteUser) {
    RecordProperty("description", "Rejects other-user and self deletion with read_users but not manage_users.");
    auto root_client              = GetLoggedInHighLevelClient();
    auto caller_client            = GetLoggedOutHighLevelClient();
    const std::string caller_name = GetRandomName(50);
    const std::string target_name = GetRandomName(50);
    iggy::GlobalPermissions global;
    global.SetReadUsers(true);
    global.SetManageUsers(false);
    iggy::Permissions permissions;
    permissions.SetGlobal(global);
    const auto caller = CreateUser(root_client, caller_name, "caller-secret", iggy::UserStatus::Active, permissions);
    const auto target = CreateUser(root_client, target_name, "target-secret", iggy::UserStatus::Active);
    ASSERT_NO_THROW(caller_client.Connect());
    ASSERT_NO_THROW(caller_client.Login(caller_name, "caller-secret"));

    ASSERT_THROW(caller_client.DeleteUser(iggy::Identifier::String(target_name)), std::exception);
    ASSERT_THROW(caller_client.DeleteUser(iggy::Identifier::String(caller_name)), std::exception);

    const auto fetched_caller = root_client.GetUser(iggy::Identifier::String(caller_name));
    const auto fetched_target = root_client.GetUser(iggy::Identifier::String(target_name));
    EXPECT_EQ(fetched_caller.Id(), caller.Id());
    EXPECT_EQ(fetched_target.Id(), target.Id());
}

TEST_F(E2E_Client, ManageUsersPermissionAllowsDeleteUser) {
    RecordProperty("description", "Allows deletion with manage_users even when read_users is false.");
    auto root_client               = GetLoggedInHighLevelClient();
    auto manager_client            = GetLoggedOutHighLevelClient();
    const std::string manager_name = GetRandomName(50);
    const std::string target_name  = GetRandomName(50);
    iggy::GlobalPermissions global;
    global.SetManageUsers(true);
    global.SetReadUsers(false);
    iggy::Permissions permissions;
    permissions.SetGlobal(global);
    const auto target = CreateUser(root_client, target_name, "target-secret", iggy::UserStatus::Active);
    ASSERT_NO_THROW(
        { CreateUser(root_client, manager_name, "manager-secret", iggy::UserStatus::Active, permissions); });
    ASSERT_NO_THROW(manager_client.Connect());
    ASSERT_NO_THROW(manager_client.Login(manager_name, "manager-secret"));

    ASSERT_NO_THROW(manager_client.DeleteUser(iggy::Identifier::String(target_name)));
    ForgetUser(target_name);

    ASSERT_THROW(root_client.GetUser(iggy::Identifier::String(target_name)), std::exception);
    const auto users = root_client.GetUsers();
    for (const auto &user : users) {
        EXPECT_FALSE(user.Id() == target.Id() && user.Username() == target_name);
    }
}

TEST_F(E2E_Client, DeleteUserRemovesOnlyTheTarget) {
    RecordProperty("description", "Deletes only the selected user and leaves another user usable.");
    auto root_client                = GetLoggedInHighLevelClient();
    auto survivor_client            = GetLoggedOutHighLevelClient();
    const std::string target_name   = GetRandomName(50);
    const std::string survivor_name = GetRandomName(50);
    ASSERT_NO_THROW({
        const auto target   = CreateUser(root_client, target_name, "target-secret", iggy::UserStatus::Active);
        const auto survivor = CreateUser(root_client, survivor_name, "survivor-secret", iggy::UserStatus::Active);

        ASSERT_NO_THROW(root_client.DeleteUser(iggy::Identifier::Numeric(target.Id())));
        ForgetUser(target_name);

        ASSERT_THROW(root_client.GetUser(iggy::Identifier::String(target_name)), std::exception);
        const auto users = root_client.GetUsers();
        for (const auto &user : users) {
            EXPECT_FALSE(user.Id() == target.Id() && user.Username() == target_name);
        }

        const auto fetched_survivor = root_client.GetUser(iggy::Identifier::String(survivor_name));
        EXPECT_EQ(fetched_survivor.Id(), survivor.Id());
        EXPECT_EQ(fetched_survivor.Username(), survivor_name);
        EXPECT_EQ(fetched_survivor.Status(), survivor.Status());
    });
    ASSERT_NO_THROW(survivor_client.Connect());
    ASSERT_NO_THROW(survivor_client.Login(survivor_name, "survivor-secret"));
}

TEST_F(E2E_Client, DeleteInactiveUserSucceeds) {
    RecordProperty("description", "Deletes an inactive user by username.");
    auto client                = GetLoggedInHighLevelClient();
    const std::string username = GetRandomName(50);
    const auto created_user    = CreateUser(client, username, "secret123", iggy::UserStatus::Inactive);

    ASSERT_NO_THROW(client.DeleteUser(iggy::Identifier::String(username)));
    ForgetUser(username);

    ASSERT_THROW(client.GetUser(iggy::Identifier::String(username)), std::exception);
    const auto users = client.GetUsers();
    for (const auto &user : users) {
        EXPECT_FALSE(user.Id() == created_user.Id() && user.Username() == username);
    }
}

TEST_F(E2E_Client, DeleteUserRevokesExistingSessions) {
    RecordProperty("description", "Revokes an existing session and prevents fresh login after user deletion.");
    auto root_client           = GetLoggedInHighLevelClient();
    auto target_client         = GetLoggedOutHighLevelClient();
    auto fresh_client          = GetLoggedOutHighLevelClient();
    const std::string username = GetRandomName(50);
    const std::string password = "target-secret";
    iggy::GlobalPermissions global;
    global.SetReadServers(true);
    iggy::Permissions permissions;
    permissions.SetGlobal(global);
    ASSERT_NO_THROW({ CreateUser(root_client, username, password, iggy::UserStatus::Active, permissions); });
    ASSERT_NO_THROW(target_client.Connect());
    ASSERT_NO_THROW(target_client.Login(username, password));
    ASSERT_NO_THROW({ (void)target_client.GetStats(); });

    ASSERT_NO_THROW(root_client.DeleteUser(iggy::Identifier::String(username)));
    ForgetUser(username);

    ASSERT_THROW((void)target_client.GetStats(), std::exception);
    ASSERT_NO_THROW(fresh_client.Connect());
    ASSERT_THROW(fresh_client.Login(username, password), std::exception);
}

TEST_F(E2E_Client, DeletedUsernameCanBeRecreatedWithoutOldCredentialsOrPermissions) {
    RecordProperty("description", "Recreates a deleted username without retaining old credentials or permissions.");
    auto root_client               = GetLoggedInHighLevelClient();
    auto new_password_client       = GetLoggedOutHighLevelClient();
    auto old_password_client       = GetLoggedOutHighLevelClient();
    const std::string username     = GetRandomName(50);
    const std::string old_password = "old-secret";
    const std::string new_password = "new-secret";
    iggy::GlobalPermissions global;
    global.SetReadServers(true);
    iggy::Permissions permissions;
    permissions.SetGlobal(global);
    ASSERT_NO_THROW({ CreateUser(root_client, username, old_password, iggy::UserStatus::Active, permissions); });

    ASSERT_NO_THROW(root_client.DeleteUser(iggy::Identifier::String(username)));
    ForgetUser(username);

    const auto replacement = CreateUser(root_client, username, new_password, iggy::UserStatus::Active);
    EXPECT_EQ(replacement.Username(), username);
    EXPECT_FALSE(replacement.Permissions().has_value());

    const auto fetched_replacement = root_client.GetUser(iggy::Identifier::String(username));
    EXPECT_EQ(fetched_replacement.Id(), replacement.Id());

    ASSERT_NO_THROW(new_password_client.Connect());
    ASSERT_NO_THROW(new_password_client.Login(username, new_password));
    ASSERT_NO_THROW(old_password_client.Connect());
    ASSERT_THROW(old_password_client.Login(username, old_password), std::exception);
}

TEST_F(E2E_Client, ChangePasswordBeforeLoginThrows) {
    RecordProperty("description",
                   "Rejects change_password before connect, after connect but before login, and after disconnect.");
    auto client                    = GetLoggedOutHighLevelClient();
    const auto user_id             = iggy::Identifier::String("iggy");
    const std::string old_password = "iggy";
    const std::string new_password = "iggy-updated-secret";

    ASSERT_THROW(client.ChangePassword(user_id, old_password, new_password), std::exception);
    ASSERT_NO_THROW(client.Connect());
    ASSERT_THROW(client.ChangePassword(user_id, old_password, new_password), std::exception);
    ASSERT_NO_THROW(client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(client.Disconnect());
    ASSERT_THROW(client.ChangePassword(user_id, old_password, new_password), std::exception);
}

TEST_F(E2E_Client, ChangePasswordWithInvalidCurrentPasswordThrows) {
    RecordProperty("description", "Rejects change_password when the provided current password is incorrect.");
    auto client                      = GetLoggedInHighLevelClient();
    const auto user_id               = iggy::Identifier::String("iggy");
    const std::string wrong_password = "not-the-current-password";
    const std::string new_password   = "iggy-updated-secret";

    ASSERT_THROW(client.ChangePassword(user_id, wrong_password, new_password), std::exception);
    ASSERT_NO_THROW(client.Logout());
    ASSERT_NO_THROW(client.Login("iggy", "iggy"));
}

TEST_F(E2E_Client, ChangePasswordWithInvalidNewPasswordThrows) {
    RecordProperty("description",
                   "Rejects change_password when the replacement password violates client-side length bounds.");
    auto client                    = GetLoggedInHighLevelClient();
    const auto user_id             = iggy::Identifier::String("iggy");
    const std::string old_password = "iggy";
    const std::string too_short;
    const std::string too_long(256, 'a');

    ASSERT_THROW(client.ChangePassword(user_id, old_password, too_short), std::exception);
    ASSERT_THROW(client.ChangePassword(user_id, old_password, too_long), std::exception);
    ASSERT_NO_THROW(client.Logout());
    ASSERT_NO_THROW(client.Login("iggy", "iggy"));
}

TEST_F(E2E_Client, ChangePasswordForWrongUserThrows) {
    RecordProperty("description", "Rejects change_password when targeting a user that does not exist.");
    auto client                          = GetLoggedInHighLevelClient();
    const auto wrong_user_id             = iggy::Identifier::String(GetRandomName());
    const std::string current_password   = "iggy";
    const std::string replacement_secret = "iggy-updated-secret";

    ASSERT_THROW(client.ChangePassword(wrong_user_id, current_password, replacement_secret), std::exception);
    ASSERT_NO_THROW(client.Logout());
    ASSERT_NO_THROW(client.Login("iggy", "iggy"));
}

TEST_F(E2E_Client, UserWithoutManageUsersCannotChangeAnotherUsersPassword) {
    RecordProperty("description",
                   "Rejects another user's password change without manage_users and preserves the old credentials.");
    auto root_client                  = GetLoggedInHighLevelClient();
    auto actor_client                 = GetLoggedOutHighLevelClient();
    auto target_client                = GetLoggedOutHighLevelClient();
    const std::string actor_name      = GetRandomName(50);
    const std::string target_name     = GetRandomName(50);
    const std::string target_password = "target-secret";
    const std::string new_password    = "replacement-secret";
    ASSERT_NO_THROW({ CreateUser(root_client, actor_name, "actor-secret", iggy::UserStatus::Active); });
    const auto target = CreateUser(root_client, target_name, target_password, iggy::UserStatus::Active);
    ASSERT_NO_THROW(actor_client.Connect());
    ASSERT_NO_THROW(actor_client.Login(actor_name, "actor-secret"));

    ASSERT_THROW(actor_client.ChangePassword(iggy::Identifier::Numeric(target.Id()), target_password, new_password),
                 std::exception);

    ASSERT_NO_THROW(target_client.Connect());
    ASSERT_NO_THROW(target_client.Login(target_name, target_password));
}

TEST_F(E2E_Client, ManageUsersPermissionAllowsChangingAnotherUsersPassword) {
    RecordProperty("description", "Allows a user with manage_users to change another user's password.");
    auto root_client                  = GetLoggedInHighLevelClient();
    auto manager_client               = GetLoggedOutHighLevelClient();
    auto old_password_client          = GetLoggedOutHighLevelClient();
    auto new_password_client          = GetLoggedOutHighLevelClient();
    const std::string manager_name    = GetRandomName(50);
    const std::string target_name     = GetRandomName(50);
    const std::string target_password = "target-secret";
    const std::string new_password    = "replacement-secret";
    iggy::GlobalPermissions global;
    global.SetManageUsers(true);
    iggy::Permissions permissions;
    permissions.SetGlobal(global);
    ASSERT_NO_THROW(
        { CreateUser(root_client, manager_name, "manager-secret", iggy::UserStatus::Active, permissions); });
    const auto target = CreateUser(root_client, target_name, target_password, iggy::UserStatus::Active);
    ASSERT_NO_THROW(manager_client.Connect());
    ASSERT_NO_THROW(manager_client.Login(manager_name, "manager-secret"));

    ASSERT_NO_THROW(
        manager_client.ChangePassword(iggy::Identifier::Numeric(target.Id()), target_password, new_password));

    ASSERT_NO_THROW(old_password_client.Connect());
    ASSERT_THROW(old_password_client.Login(target_name, target_password), std::exception);
    ASSERT_NO_THROW(new_password_client.Connect());
    ASSERT_NO_THROW(new_password_client.Login(target_name, new_password));
}

TEST_F(E2E_Client, ChangePasswordUpdatesCredentialsAndCanBeRestored) {
    RecordProperty("description",
                   "Changes the password for the current user, updates login behavior, and restores the original "
                   "password before the test exits.");
    auto client                    = GetLoggedInHighLevelClient();
    auto second_client             = GetLoggedOutHighLevelClient();
    auto third_client              = GetLoggedOutHighLevelClient();
    const auto user_id             = iggy::Identifier::String("iggy");
    const std::string old_password = "iggy";
    const std::string new_password = "iggy-updated-secret";
    bool password_changed          = false;

    ASSERT_NO_THROW(client.ChangePassword(user_id, old_password, new_password));
    password_changed = true;

    EXPECT_THROW(second_client.Login("iggy", old_password), std::exception);
    EXPECT_NO_THROW(second_client.Login("iggy", new_password));

    if (password_changed) {
        EXPECT_NO_THROW(client.ChangePassword(user_id, new_password, old_password));
    }

    EXPECT_NO_THROW(third_client.Login("iggy", old_password));
}

TEST_F(E2E_Client, UpdatePermissionsAssignsAndClearsPermissions) {
    RecordProperty("description",
                   "Assigns global permissions to a user without any and clears them back to no assignment.");
    auto client                = GetLoggedInHighLevelClient();
    const std::string username = GetRandomName(50);
    ASSERT_NO_THROW({ CreateUser(client, username, "secret123", iggy::UserStatus::Active); });
    EXPECT_FALSE(client.GetUser(iggy::Identifier::String(username)).Permissions().has_value());

    iggy::GlobalPermissions global;
    global.SetReadUsers(true);
    iggy::Permissions permissions;
    permissions.SetGlobal(global);
    ASSERT_NO_THROW(client.UpdatePermissions(iggy::Identifier::String(username), permissions));

    const auto fetched = client.GetUser(iggy::Identifier::String(username));
    ASSERT_TRUE(fetched.Permissions().has_value());
    EXPECT_TRUE(fetched.Permissions()->Global().ReadUsers());
    EXPECT_FALSE(fetched.Permissions()->Global().ManageUsers());

    ASSERT_NO_THROW(client.UpdatePermissions(iggy::Identifier::String(username), std::nullopt));
    EXPECT_FALSE(client.GetUser(iggy::Identifier::String(username)).Permissions().has_value());
}

TEST_F(E2E_Client, DeleteWhileUnauthenticatedAfterFailedLogin) {
    RecordProperty("description", "Allows client cleanup after a failed login leaves the connection unauthenticated.");
    {
        auto client = GetLoggedOutHighLevelClient();
        ASSERT_NO_THROW(client.Connect());
        ASSERT_THROW(client.Login("biggy", "biggy"), std::exception);
    }
}

TEST_F(E2E_Client, ConnectLoginThenDisconnect) {
    RecordProperty("description",
                   "Connects, logs in, disconnects successfully, and rejects authenticated operations afterward.");
    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.Disconnect());
    ASSERT_THROW(client.GetMe(), std::exception);
}

TEST_F(E2E_Client, DisconnectWithoutConnect) {
    RecordProperty("description", "Allows disconnect to be called on a client that was never explicitly connected.");
    auto client = GetLoggedOutHighLevelClient();
    ASSERT_NO_THROW(client.Disconnect());
}

TEST_F(E2E_Client, DisconnectWithoutLogin) {
    RecordProperty("description", "Allows disconnect after connect even when no user has authenticated.");
    auto client = GetLoggedOutHighLevelClient();
    ASSERT_NO_THROW(client.Connect());
    ASSERT_NO_THROW(client.Disconnect());
    ASSERT_THROW(client.GetMe(), std::exception);
}

TEST_F(E2E_Client, DisconnectThenReconnectWithoutRelogin) {
    RecordProperty("description",
                   "Requires logging in again after a disconnect and reconnect before authenticated operations work.");
    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.Disconnect());
    ASSERT_NO_THROW(client.Connect());
    ASSERT_THROW(client.GetMe(), std::exception);
}

TEST_F(E2E_Client, DisconnectAfterFailedLogin) {
    RecordProperty("description", "Allows disconnect after a failed login attempt leaves the client unauthenticated.");
    auto client = GetLoggedOutHighLevelClient();
    ASSERT_NO_THROW(client.Connect());
    ASSERT_THROW(client.Login("biggy", "biggy"), std::exception);
    ASSERT_NO_THROW(client.Disconnect());
    ASSERT_THROW(client.GetMe(), std::exception);
}

TEST_F(E2E_Client, ConnectLoginThenShutdown) {
    RecordProperty("description",
                   "Connects, logs in, shuts down successfully, and rejects further operations afterward.");
    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.GetMe());
    ASSERT_NO_THROW(client.Shutdown());
    ASSERT_THROW(client.GetMe(), std::exception);
    ASSERT_THROW(client.GetClients(), std::exception);
}

TEST_F(E2E_Client, ShutdownWithoutConnect) {
    RecordProperty("description", "Allows shutdown to be called on a client that was never explicitly connected.");
    auto client = GetLoggedOutHighLevelClient();
    ASSERT_NO_THROW(client.Shutdown());
}

TEST_F(E2E_Client, ShutdownWithoutLogin) {
    RecordProperty("description", "Allows shutdown after connect even when no user has authenticated.");
    auto client = GetLoggedOutHighLevelClient();
    ASSERT_NO_THROW(client.Connect());
    ASSERT_NO_THROW(client.Shutdown());
    ASSERT_THROW(client.GetMe(), std::exception);
}

TEST_F(E2E_Client, ShutdownAfterFailedLogin) {
    RecordProperty("description", "Allows shutdown after a failed login attempt leaves the client unauthenticated.");
    auto client = GetLoggedOutHighLevelClient();
    ASSERT_NO_THROW(client.Connect());
    ASSERT_THROW(client.Login("biggy", "biggy"), std::exception);
    ASSERT_NO_THROW(client.Shutdown());
    ASSERT_THROW(client.GetMe(), std::exception);
}

TEST_F(E2E_Client, RepeatedShutdownCallsHaveStableBehavior) {
    RecordProperty("description", "Keeps repeated shutdown calls stable across duplicate invocations.");
    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.Shutdown());
    ASSERT_NO_THROW(client.Shutdown());
    ASSERT_THROW(client.GetMe(), std::exception);
}

TEST_F(E2E_Client, ShutdownThenConnectThrows) {
    RecordProperty("description", "Rejects reconnecting a client after shutdown transitions it to a terminal state.");
    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.Shutdown());
    ASSERT_THROW(client.Connect(), std::exception);
}

TEST_F(E2E_Client, ShutdownThenLoginThrows) {
    RecordProperty("description",
                   "Rejects logging in again after shutdown, even when login would normally auto-connect.");
    auto client = GetLoggedOutHighLevelClient();
    ASSERT_NO_THROW(client.Shutdown());
    ASSERT_THROW(client.Login("iggy", "iggy"), std::exception);
}

TEST_F(E2E_Client, GetClientsReflectsSessionRemovalAfterShutdown) {
    RecordProperty("description",
                   "Removes a shut down authenticated session from subsequent GetClients and GetClient results.");
    auto first_client  = GetLoggedInHighLevelClient();
    auto second_client = GetLoggedInHighLevelClient();

    const auto first_client_id = first_client.GetMe().ClientId();

    ASSERT_NO_THROW(first_client.Shutdown());
    constexpr auto removal_timeout       = std::chrono::seconds(5);
    constexpr auto removal_poll_interval = std::chrono::milliseconds(10);
    const auto deadline                  = std::chrono::steady_clock::now() + removal_timeout;
    bool removed                         = false;
    do {
        const auto clients = second_client.GetClients();
        removed            = std::none_of(clients.begin(), clients.end(),
                                          [first_client_id](const auto &client) { return client.ClientId() == first_client_id; });
        if (removed) {
            break;
        }
        std::this_thread::sleep_for(removal_poll_interval);
    } while (std::chrono::steady_clock::now() < deadline);
    ASSERT_TRUE(removed);
    ASSERT_THROW(second_client.GetClient(first_client_id), iggy::IggyException);
}

TEST_F(E2E_Client, GetClientsReflectsSessionRemovalAfterDisconnect) {
    RecordProperty("description",
                   "Removes a disconnected authenticated session from subsequent GetClients and GetClient results.");
    auto first_client  = GetLoggedInHighLevelClient();
    auto second_client = GetLoggedInHighLevelClient();

    const auto first_client_id = first_client.GetMe().ClientId();

    ASSERT_NO_THROW(first_client.Disconnect());
    constexpr auto removal_timeout       = std::chrono::seconds(5);
    constexpr auto removal_poll_interval = std::chrono::milliseconds(10);
    const auto deadline                  = std::chrono::steady_clock::now() + removal_timeout;
    bool removed                         = false;
    do {
        const auto clients = second_client.GetClients();
        removed            = std::none_of(clients.begin(), clients.end(),
                                          [first_client_id](const auto &client) { return client.ClientId() == first_client_id; });
        if (removed) {
            break;
        }
        std::this_thread::sleep_for(removal_poll_interval);
    } while (std::chrono::steady_clock::now() < deadline);
    ASSERT_TRUE(removed);
    ASSERT_THROW(second_client.GetClient(first_client_id), iggy::IggyException);
}

TEST_F(E2E_Client, GetClientsReflectsLoggedOutSessionAsUnauthenticated) {
    RecordProperty("description", "Drops a logged out session from GetClients and reports it missing in GetClient.");
    auto first_client  = GetLoggedInHighLevelClient();
    auto second_client = GetLoggedInHighLevelClient();

    const auto first_client_id = first_client.GetMe().ClientId();

    // The VSR server drops the client-table entry on logout (an unauthenticated
    // session is not tracked), unlike the legacy server which kept it visible
    // without a user id.
    ASSERT_NO_THROW(first_client.Logout());
    constexpr auto removal_timeout       = std::chrono::seconds(5);
    constexpr auto removal_poll_interval = std::chrono::milliseconds(10);
    const auto deadline                  = std::chrono::steady_clock::now() + removal_timeout;
    bool removed                         = false;
    do {
        const auto clients = second_client.GetClients();
        removed            = std::none_of(clients.begin(), clients.end(),
                                          [first_client_id](const auto &client) { return client.ClientId() == first_client_id; });
        if (removed) {
            break;
        }
        std::this_thread::sleep_for(removal_poll_interval);
    } while (std::chrono::steady_clock::now() < deadline);
    ASSERT_TRUE(removed);
    ASSERT_THROW(second_client.GetClient(first_client_id), iggy::IggyException);
}

TEST_F(E2E_Client, LoginWithoutConnect) {
    RecordProperty("description", "Supports login without an explicit prior connect call.");
    auto client = GetLoggedOutHighLevelClient();

    ASSERT_NO_THROW(client.Login("iggy", "iggy"));
}

TEST_F(E2E_Client, ConnectWithoutLoginThenDelete) {
    RecordProperty("description", "Allows connecting without logging in and then deleting the client.");
    {
        auto client = GetLoggedOutHighLevelClient();
        ASSERT_NO_THROW(client.Connect());
    }
}

TEST_F(E2E_Client, DeleteWithoutDisconnect) {
    RecordProperty("description", "Allows deleting a connected and authenticated client without disconnecting first.");
    { auto client = GetLoggedInHighLevelClient(); }
}

TEST_F(E2E_Client, RepeatedClientMethodCallsHaveStableBehavior) {
    RecordProperty("description",
                   "Keeps repeated connect, login, and delete calls stable across duplicate invocations.");
    {
        auto client = GetLoggedOutHighLevelClient();
        ASSERT_NO_THROW(client.Connect());
        ASSERT_NO_THROW(client.Connect());
        ASSERT_NO_THROW(client.Login("iggy", "iggy"));
        ASSERT_NO_THROW(client.Login("iggy", "iggy"));
    }
}

TEST_F(E2E_Client, RepeatedDisconnectCallsHaveStableBehavior) {
    RecordProperty("description", "Keeps repeated disconnect calls stable across duplicate invocations.");
    auto client = GetLoggedOutHighLevelClient();

    ASSERT_NO_THROW(client.Connect());
    ASSERT_NO_THROW(client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(client.Disconnect());
    ASSERT_NO_THROW(client.Disconnect());
    ASSERT_THROW(client.GetMe(), std::exception);
}

TEST_F(LowLevelE2E_Client, DeleteNullConnectionIsNoop) {
    RecordProperty("description", "Treats deleting a null client pointer as a no-op.");
    iggy::ffi::Client *client = nullptr;
    iggy::ffi::delete_client(client);
}

TEST_F(E2E_Client, GetStatsBeforeLoginThrows) {
    RecordProperty("description",
                   "Rejects GetStats before connect, after connect but before login, and after disconnect.");
    auto client = GetLoggedOutHighLevelClient();

    ASSERT_THROW((void)client.GetStats(), iggy::IggyException);
    ASSERT_NO_THROW(client.Connect());
    ASSERT_THROW((void)client.GetStats(), iggy::IggyException);
    ASSERT_NO_THROW(client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(client.Disconnect());
    ASSERT_THROW((void)client.GetStats(), iggy::IggyException);
}

TEST_F(E2E_Client, DeleteSegmentsBeforeLoginThrows) {
    RecordProperty("description",
                   "Rejects DeleteSegments before connect, after connect but before login, and after disconnect.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    auto setup_client             = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(setup_client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(setup_client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                             iggy::TopicCreateOptions().SetPartitionsCount(1)));

    auto unauthenticated_client = GetLoggedOutHighLevelClient();
    ASSERT_THROW(unauthenticated_client.DeleteSegments(iggy::Identifier::String(stream_name),
                                                       iggy::Identifier::String(topic_name), 0, 1),
                 iggy::IggyException);
    ASSERT_NO_THROW(unauthenticated_client.Connect());
    ASSERT_THROW(unauthenticated_client.DeleteSegments(iggy::Identifier::String(stream_name),
                                                       iggy::Identifier::String(topic_name), 0, 1),
                 iggy::IggyException);
    ASSERT_NO_THROW(unauthenticated_client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(unauthenticated_client.Disconnect());
    ASSERT_THROW(unauthenticated_client.DeleteSegments(iggy::Identifier::String(stream_name),
                                                       iggy::Identifier::String(topic_name), 0, 1),
                 iggy::IggyException);
}

TEST_F(E2E_Client, DeleteSegmentsOnNonExistentStreamThrows) {
    RecordProperty("description", "Throws when deleting segments from a stream that does not exist.");
    const std::string stream_name         = GetRandomName();
    const std::string topic_name          = GetRandomName();
    const std::string missing_stream_name = GetRandomName();
    auto client                           = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));

    ASSERT_THROW(client.DeleteSegments(iggy::Identifier::String(missing_stream_name),
                                       iggy::Identifier::String(topic_name), 0, 1),
                 iggy::IggyException);
}

TEST_F(E2E_Client, DeleteSegmentsOnNonExistentTopicThrows) {
    RecordProperty("description", "Throws when deleting segments from a topic that does not exist.");
    const std::string stream_name        = GetRandomName();
    const std::string topic_name         = GetRandomName();
    const std::string missing_topic_name = GetRandomName();
    auto client                          = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));

    ASSERT_THROW(client.DeleteSegments(iggy::Identifier::String(stream_name),
                                       iggy::Identifier::String(missing_topic_name), 0, 1),
                 iggy::IggyException);
}

TEST_F(E2E_Client, DeleteSegmentsOnNonExistentPartitionThrows) {
    RecordProperty("description", "Throws when deleting segments from a partition that does not exist.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));

    ASSERT_THROW(
        client.DeleteSegments(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name), 999, 1),
        iggy::IggyException);
}

TEST_F(E2E_Client, DeleteSegmentsWithZeroCountIsNoOp) {
    RecordProperty("description", "Treats DeleteSegments with count 0 as a no-op.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));

    const auto stream_details = client.GetStream(iggy::Identifier::String(stream_name));
    ASSERT_EQ(stream_details.Topics().size(), 1u);
    const std::uint32_t stream_id = stream_details.Id();
    const std::uint32_t topic_id  = stream_details.Topics().front().Id();

    std::vector<iggy::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 5; ++i) {
        messages.push_back(iggy::IggyMessageToSend::Create("zero-count-" + std::to_string(i), {}));
    }
    ASSERT_NO_THROW(client.SendMessages(iggy::Identifier::Numeric(stream_id), iggy::Identifier::Numeric(topic_id),
                                        iggy::Partitioning::PartitionId(0), messages));

    const auto find_partition = [&client, stream_id, topic_id](std::uint32_t partition_id) {
        const auto topic_details =
            client.GetTopic(iggy::Identifier::Numeric(stream_id), iggy::Identifier::Numeric(topic_id));
        for (const auto &partition : topic_details.Partitions()) {
            if (partition.Id() == partition_id) {
                return partition;
            }
        }
        throw iggy::IggyException("Partition was not found");
    };

    const auto partition_before_delete = find_partition(0);

    std::optional<iggy::PolledMessages> polled_before_delete;
    ASSERT_NO_THROW({
        polled_before_delete.emplace(client.PollMessages(
            iggy::Identifier::Numeric(stream_id), iggy::Identifier::Numeric(topic_id), 0,
            iggy::Consumer::Single(iggy::Identifier::Numeric(1005)), iggy::PollingStrategy::Offset(0), 1000, false));
    });

    ASSERT_NO_THROW(
        client.DeleteSegments(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name), 0, 0));

    const auto partition_after_delete = find_partition(0);

    std::optional<iggy::PolledMessages> polled_after_delete;
    ASSERT_NO_THROW({
        polled_after_delete.emplace(client.PollMessages(
            iggy::Identifier::Numeric(stream_id), iggy::Identifier::Numeric(topic_id), 0,
            iggy::Consumer::Single(iggy::Identifier::Numeric(1006)), iggy::PollingStrategy::Offset(0), 1000, false));
    });

    EXPECT_EQ(partition_after_delete.SegmentsCount(), partition_before_delete.SegmentsCount());
    EXPECT_EQ(partition_after_delete.CurrentOffset(), partition_before_delete.CurrentOffset());
    EXPECT_EQ(partition_after_delete.MessagesCount(), partition_before_delete.MessagesCount());
    EXPECT_EQ(partition_after_delete.SizeBytes(), partition_before_delete.SizeBytes());
    EXPECT_EQ(polled_after_delete->Count(), polled_before_delete->Count());
    ASSERT_EQ(polled_after_delete->Messages().size(), polled_before_delete->Messages().size());
    for (std::size_t i = 0; i < polled_before_delete->Messages().size(); ++i) {
        EXPECT_EQ(polled_after_delete->Messages()[i].Offset(), polled_before_delete->Messages()[i].Offset());
    }
}

TEST_F(E2E_Client, DeleteSegmentsWhenOnlyActiveSegmentRemainsIsNoOp) {
    RecordProperty("description",
                   "Keeps the partition unchanged when DeleteSegments is called with only the active segment.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));

    const auto stream_details = client.GetStream(iggy::Identifier::String(stream_name));
    ASSERT_EQ(stream_details.Topics().size(), 1u);
    const std::uint32_t stream_id = stream_details.Id();
    const std::uint32_t topic_id  = stream_details.Topics().front().Id();

    std::vector<iggy::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 5; ++i) {
        messages.push_back(iggy::IggyMessageToSend::Create("active-only-" + std::to_string(i), {}));
    }
    ASSERT_NO_THROW(client.SendMessages(iggy::Identifier::Numeric(stream_id), iggy::Identifier::Numeric(topic_id),
                                        iggy::Partitioning::PartitionId(0), messages));

    const auto find_partition = [&client, stream_id, topic_id](std::uint32_t partition_id) {
        const auto topic_details =
            client.GetTopic(iggy::Identifier::Numeric(stream_id), iggy::Identifier::Numeric(topic_id));
        for (const auto &partition : topic_details.Partitions()) {
            if (partition.Id() == partition_id) {
                return partition;
            }
        }
        throw iggy::IggyException("Partition was not found");
    };

    const auto partition_before_delete = find_partition(0);
    ASSERT_EQ(partition_before_delete.SegmentsCount(), 1u);

    std::optional<iggy::PolledMessages> polled_before_delete;
    ASSERT_NO_THROW({
        polled_before_delete.emplace(client.PollMessages(
            iggy::Identifier::Numeric(stream_id), iggy::Identifier::Numeric(topic_id), 0,
            iggy::Consumer::Single(iggy::Identifier::Numeric(1007)), iggy::PollingStrategy::Offset(0), 1000, false));
    });

    ASSERT_NO_THROW(
        client.DeleteSegments(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name), 0, 1));

    const auto partition_after_delete = find_partition(0);

    std::optional<iggy::PolledMessages> polled_after_delete;
    ASSERT_NO_THROW({
        polled_after_delete.emplace(client.PollMessages(
            iggy::Identifier::Numeric(stream_id), iggy::Identifier::Numeric(topic_id), 0,
            iggy::Consumer::Single(iggy::Identifier::Numeric(1008)), iggy::PollingStrategy::Offset(0), 1000, false));
    });

    EXPECT_EQ(partition_after_delete.SegmentsCount(), partition_before_delete.SegmentsCount());
    EXPECT_EQ(partition_after_delete.CurrentOffset(), partition_before_delete.CurrentOffset());
    EXPECT_EQ(partition_after_delete.MessagesCount(), partition_before_delete.MessagesCount());
    EXPECT_EQ(partition_after_delete.SizeBytes(), partition_before_delete.SizeBytes());
    EXPECT_EQ(polled_after_delete->Count(), polled_before_delete->Count());
    ASSERT_EQ(polled_after_delete->Messages().size(), polled_before_delete->Messages().size());
    for (std::size_t i = 0; i < polled_before_delete->Messages().size(); ++i) {
        EXPECT_EQ(polled_after_delete->Messages()[i].Offset(), polled_before_delete->Messages()[i].Offset());
    }
}

// TODO(slbotbm): add a test to create some streams, topics, partitions, and segments, send messages, and create
// consumer groups and verify it.
TEST_F(E2E_Client, GetStatsReturnsServerStats) {
    RecordProperty("description",
                   "Returns empty resource counts first, then reflects aggregated streams, topics, partitions, "
                   "consumer groups, and clients.");
    const std::string first_stream_name                 = GetRandomName();
    const std::string second_stream_name                = GetRandomName();
    const std::string first_topic_name                  = GetRandomName();
    const std::string second_topic_name                 = GetRandomName();
    const std::string third_topic_name                  = GetRandomName();
    const std::string first_group_name                  = GetRandomName();
    const std::string second_group_name                 = GetRandomName();
    const std::string third_group_name                  = GetRandomName();
    constexpr std::uint32_t additional_partitions_count = 2;
    auto client                                         = GetLoggedInHighLevelClient();

    const auto empty_stats = client.GetStats();
    EXPECT_NE(empty_stats.ProcessId(), 0u);
    EXPECT_GT(empty_stats.ThreadsCount(), 0u);
    EXPECT_GT(empty_stats.TotalMemory(), 0u);
    EXPECT_LE(empty_stats.AvailableMemory(), empty_stats.TotalMemory());
    EXPECT_GE(empty_stats.TotalDiskSpace(), empty_stats.FreeDiskSpace());
    EXPECT_GT(empty_stats.OpenFilesLimit(), 0u);
    EXPECT_LE(empty_stats.OpenFilesCount(), empty_stats.OpenFilesLimit());
    EXPECT_FALSE(empty_stats.Hostname().empty());
    EXPECT_FALSE(empty_stats.OsName().empty());
    EXPECT_FALSE(empty_stats.OsVersion().empty());
    EXPECT_FALSE(empty_stats.KernelVersion().empty());
    EXPECT_FALSE(empty_stats.IggyServerVersion().empty());

    ASSERT_NO_THROW(client.CreateStream(first_stream_name));
    TrackStream(first_stream_name);
    ASSERT_NO_THROW(client.CreateStream(second_stream_name));
    TrackStream(second_stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(first_stream_name), first_topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(first_stream_name), second_topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(2)));
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(second_stream_name), third_topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(3)));
    ASSERT_NO_THROW(client.CreatePartitions(iggy::Identifier::String(first_stream_name),
                                            iggy::Identifier::String(first_topic_name), additional_partitions_count));
    const auto first_group  = client.CreateConsumerGroup(iggy::Identifier::String(first_stream_name),
                                                         iggy::Identifier::String(first_topic_name), first_group_name);
    const auto second_group = client.CreateConsumerGroup(
        iggy::Identifier::String(first_stream_name), iggy::Identifier::String(second_topic_name), second_group_name);
    const auto third_group = client.CreateConsumerGroup(iggy::Identifier::String(second_stream_name),
                                                        iggy::Identifier::String(third_topic_name), third_group_name);

    auto second_client = GetLoggedInHighLevelClient();
    auto third_client  = GetLoggedInHighLevelClient();
    (void)second_client;
    (void)third_client;

    const auto first_stream_details  = client.GetStream(iggy::Identifier::String(first_stream_name));
    const auto second_stream_details = client.GetStream(iggy::Identifier::String(second_stream_name));
    const std::uint32_t expected_topics_count =
        first_stream_details.TopicsCount() + second_stream_details.TopicsCount();
    std::uint32_t first_topic_partitions  = 0;
    std::uint32_t second_topic_partitions = 0;
    std::uint32_t third_topic_partitions  = 0;
    for (const auto &topic : first_stream_details.Topics()) {
        if (topic.Name() == first_topic_name) {
            first_topic_partitions = topic.PartitionsCount();
        }
        if (topic.Name() == second_topic_name) {
            second_topic_partitions = topic.PartitionsCount();
        }
    }
    for (const auto &topic : second_stream_details.Topics()) {
        if (topic.Name() == third_topic_name) {
            third_topic_partitions = topic.PartitionsCount();
        }
    }
    const std::uint32_t expected_partitions_count =
        first_topic_partitions + second_topic_partitions + third_topic_partitions;

    const auto stats_after_create = client.GetStats();
    EXPECT_GE(stats_after_create.StreamsCount(), empty_stats.StreamsCount() + 2u);
    EXPECT_GE(stats_after_create.TopicsCount(), empty_stats.TopicsCount() + expected_topics_count);
    EXPECT_GE(stats_after_create.PartitionsCount(), empty_stats.PartitionsCount() + expected_partitions_count);
    EXPECT_GE(stats_after_create.SegmentsCount(), empty_stats.SegmentsCount() + expected_partitions_count);
    EXPECT_GE(stats_after_create.ConsumerGroupsCount(), empty_stats.ConsumerGroupsCount() + 3u);
    EXPECT_GE(stats_after_create.ClientsCount(), empty_stats.ClientsCount() + 2u);
    EXPECT_EQ(first_group.PartitionsCount(), first_topic_partitions);
    EXPECT_EQ(second_group.PartitionsCount(), second_topic_partitions);
    EXPECT_EQ(third_group.PartitionsCount(), third_topic_partitions);

    ASSERT_NO_THROW(client.DeleteStream(iggy::Identifier::String(second_stream_name)));
    ForgetTrackedStream(second_stream_name);
    ASSERT_NO_THROW(client.DeleteStream(iggy::Identifier::String(first_stream_name)));
    ForgetTrackedStream(first_stream_name);

    const auto stats = client.GetStats();
    EXPECT_LE(stats.StreamsCount(), stats_after_create.StreamsCount());
    EXPECT_LE(stats.TopicsCount(), stats_after_create.TopicsCount());
    EXPECT_LE(stats.PartitionsCount(), stats_after_create.PartitionsCount());
    EXPECT_LE(stats.SegmentsCount(), stats_after_create.SegmentsCount());
    EXPECT_LE(stats.ConsumerGroupsCount(), stats_after_create.ConsumerGroupsCount());
    EXPECT_LE(stats.ClientsCount(), stats_after_create.ClientsCount());
}

TEST_F(E2E_Client, GetStatsIsStableAcrossBackToBackCalls) {
    RecordProperty("description",
                   "Returns sane invariant fields across back-to-back GetStats calls on an idle authenticated client.");
    auto client = GetLoggedInHighLevelClient();

    const auto first_stats  = client.GetStats();
    const auto second_stats = client.GetStats();

    EXPECT_NE(first_stats.ProcessId(), 0u);
    EXPECT_NE(second_stats.ProcessId(), 0u);
    EXPECT_EQ(second_stats.ProcessId(), first_stats.ProcessId());
    EXPECT_GT(first_stats.ThreadsCount(), 0u);
    EXPECT_GT(second_stats.ThreadsCount(), 0u);
    EXPECT_GT(first_stats.TotalMemory(), 0u);
    EXPECT_GT(second_stats.TotalMemory(), 0u);
    EXPECT_FALSE(first_stats.Hostname().empty());
    EXPECT_FALSE(second_stats.Hostname().empty());
    EXPECT_FALSE(first_stats.OsName().empty());
    EXPECT_FALSE(second_stats.OsName().empty());
    EXPECT_FALSE(first_stats.OsVersion().empty());
    EXPECT_FALSE(second_stats.OsVersion().empty());
    EXPECT_FALSE(first_stats.KernelVersion().empty());
    EXPECT_FALSE(second_stats.KernelVersion().empty());
    EXPECT_FALSE(first_stats.IggyServerVersion().empty());
    EXPECT_FALSE(second_stats.IggyServerVersion().empty());
    EXPECT_EQ(second_stats.Hostname(), first_stats.Hostname());
    EXPECT_EQ(second_stats.OsName(), first_stats.OsName());
    EXPECT_EQ(second_stats.OsVersion(), first_stats.OsVersion());
    EXPECT_EQ(second_stats.KernelVersion(), first_stats.KernelVersion());
    EXPECT_EQ(second_stats.IggyServerVersion(), first_stats.IggyServerVersion());
    EXPECT_EQ(second_stats.ServerSemver(), first_stats.ServerSemver());
    EXPECT_GE(first_stats.ClientsCount(), 1u);
    EXPECT_GE(second_stats.ClientsCount(), 1u);
}

TEST_F(E2E_Client, GetMeBeforeLoginThrows) {
    RecordProperty("description",
                   "Rejects GetMe before connect, after connect but before login, and after disconnect.");
    auto client = GetLoggedOutHighLevelClient();

    ASSERT_THROW(client.GetMe(), iggy::IggyException);
    ASSERT_NO_THROW(client.Connect());
    ASSERT_THROW(client.GetMe(), iggy::IggyException);
    ASSERT_NO_THROW(client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(client.Disconnect());
    ASSERT_THROW(client.GetMe(), iggy::IggyException);
}

TEST_F(E2E_Client, GetMeReturnsCurrentClientDetails) {
    RecordProperty("description", "Returns the current authenticated client details.");
    auto client = GetLoggedInHighLevelClient();

    const auto me = client.GetMe();
    EXPECT_NE(me.ClientId(), 0u);
    EXPECT_TRUE(me.UserId().has_value());
    EXPECT_FALSE(me.Address().empty());
    EXPECT_EQ(me.Transport(), "TCP");
    EXPECT_EQ(me.ConsumerGroupsCount(), 0u);
    EXPECT_TRUE(me.ConsumerGroups().empty());
}

TEST_F(E2E_Client, GetMeReflectsConsumerGroupMembershipChanges) {
    RecordProperty("description", "Reflects joined consumer groups in GetMe and removes them again after leaving.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    const auto topic_details = client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                                  iggy::TopicCreateOptions().SetPartitionsCount(1));

    const auto stream_details = client.GetStream(iggy::Identifier::String(stream_name));
    const auto created_group  = client.CreateConsumerGroup(iggy::Identifier::String(stream_name),
                                                           iggy::Identifier::String(topic_name), group_name);

    const auto baseline_me           = client.GetMe();
    const auto baseline_groups_count = baseline_me.ConsumerGroupsCount();
    const auto baseline_groups_size  = baseline_me.ConsumerGroups().size();

    ASSERT_NO_THROW(client.JoinConsumerGroup(iggy::Identifier::Numeric(stream_details.Id()),
                                             iggy::Identifier::Numeric(topic_details.Id()),
                                             iggy::Identifier::Numeric(created_group.Id())));

    {
        const auto me = client.GetMe();
        EXPECT_GT(me.ConsumerGroupsCount(), baseline_groups_count);
        EXPECT_GT(me.ConsumerGroups().size(), baseline_groups_size);

        bool found_group = false;
        for (const auto &group : me.ConsumerGroups()) {
            if (group.StreamId() != stream_details.Id() || group.TopicId() != topic_details.Id() ||
                group.GroupId() != created_group.Id()) {
                continue;
            }
            found_group = true;
            break;
        }
        EXPECT_TRUE(found_group);
    }

    ASSERT_NO_THROW(client.LeaveConsumerGroup(iggy::Identifier::Numeric(stream_details.Id()),
                                              iggy::Identifier::Numeric(topic_details.Id()),
                                              iggy::Identifier::Numeric(created_group.Id())));

    {
        const auto me = client.GetMe();
        EXPECT_GE(me.ConsumerGroupsCount(), baseline_groups_count);
        EXPECT_GE(me.ConsumerGroups().size(), baseline_groups_size);

        bool found_group = false;
        for (const auto &group : me.ConsumerGroups()) {
            if (group.StreamId() != stream_details.Id() || group.TopicId() != topic_details.Id() ||
                group.GroupId() != created_group.Id()) {
                continue;
            }
            found_group = true;
            break;
        }
        EXPECT_FALSE(found_group);
    }
}

TEST_F(E2E_Client, GetMeIsStableAcrossBackToBackCalls) {
    RecordProperty("description", "Returns stable current-client details across back-to-back GetMe calls.");
    auto client = GetLoggedInHighLevelClient();

    const auto first_me  = client.GetMe();
    const auto second_me = client.GetMe();

    EXPECT_NE(first_me.ClientId(), 0u);
    EXPECT_TRUE(first_me.UserId().has_value());
    EXPECT_TRUE(second_me.UserId().has_value());
    EXPECT_EQ(second_me.ClientId(), first_me.ClientId());
    EXPECT_EQ(second_me.UserId(), first_me.UserId());
    EXPECT_EQ(second_me.Address(), first_me.Address());
    EXPECT_EQ(first_me.Transport(), "TCP");
    EXPECT_EQ(second_me.Transport(), "TCP");
    EXPECT_EQ(second_me.Transport(), first_me.Transport());
    EXPECT_EQ(second_me.ConsumerGroupsCount(), first_me.ConsumerGroupsCount());
    EXPECT_EQ(second_me.ConsumerGroups().size(), first_me.ConsumerGroups().size());
}

TEST_F(E2E_Client, GetMeReturnsDistinctClientIdsForDifferentSessions) {
    RecordProperty(
        "description",
        "Returns different client ids for separate authenticated sessions while keeping the same user identity.");
    auto first_client  = GetLoggedInHighLevelClient();
    auto second_client = GetLoggedInHighLevelClient();

    const auto first_me  = first_client.GetMe();
    const auto second_me = second_client.GetMe();

    EXPECT_NE(first_me.ClientId(), 0u);
    EXPECT_NE(second_me.ClientId(), 0u);
    EXPECT_TRUE(first_me.UserId().has_value());
    EXPECT_TRUE(second_me.UserId().has_value());
    EXPECT_NE(second_me.ClientId(), first_me.ClientId());
    EXPECT_EQ(second_me.UserId(), first_me.UserId());
    EXPECT_EQ(first_me.Transport(), "TCP");
    EXPECT_EQ(second_me.Transport(), "TCP");
}

TEST_F(E2E_Client, GetMeReturnsValidDetailsAfterReconnect) {
    RecordProperty("description",
                   "Returns valid current-client details after reconnecting with a fresh authenticated session.");
    const auto first_me = [this] {
        auto first_client = GetLoggedInHighLevelClient();
        return first_client.GetMe();
    }();
    EXPECT_NE(first_me.ClientId(), 0u);
    EXPECT_TRUE(first_me.UserId().has_value());

    auto second_client   = GetLoggedInHighLevelClient();
    const auto second_me = second_client.GetMe();
    EXPECT_NE(second_me.ClientId(), 0u);
    EXPECT_TRUE(second_me.UserId().has_value());
    EXPECT_EQ(second_me.UserId(), first_me.UserId());
    EXPECT_EQ(first_me.Transport(), "TCP");
    EXPECT_EQ(second_me.Transport(), "TCP");
    EXPECT_EQ(second_me.Transport(), first_me.Transport());
    EXPECT_FALSE(second_me.Address().empty());
    EXPECT_EQ(second_me.ConsumerGroupsCount(), 0u);
    EXPECT_TRUE(second_me.ConsumerGroups().empty());
}

TEST_F(E2E_Client, GetClientBeforeLoginThrows) {
    RecordProperty("description",
                   "Rejects GetClient before connect, after connect but before login, and after disconnect.");
    auto client = GetLoggedOutHighLevelClient();

    ASSERT_THROW(client.GetClient(1), iggy::IggyException);
    ASSERT_NO_THROW(client.Connect());
    ASSERT_THROW(client.GetClient(1), iggy::IggyException);
    ASSERT_NO_THROW(client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(client.Disconnect());
    ASSERT_THROW(client.GetClient(1), iggy::IggyException);
}

TEST_F(E2E_Client, GetClientWithWrongClientIdThrows) {
    RecordProperty("description", "Rejects querying invalid or non-existent client ids.");
    auto client = GetLoggedInHighLevelClient();

    std::uint32_t non_existent_client_id = 1u;
    const auto clients                   = client.GetClients();
    std::unordered_set<std::uint32_t> client_ids;
    for (const auto &entry : clients) {
        client_ids.insert(entry.ClientId());
    }

    while (client_ids.find(non_existent_client_id) != client_ids.end()) {
        ++non_existent_client_id;
    }

    const std::uint32_t wrong_client_ids[] = {0u, non_existent_client_id};
    for (const std::uint32_t wrong_client_id : wrong_client_ids) {
        SCOPED_TRACE(wrong_client_id);
        ASSERT_THROW(client.GetClient(wrong_client_id), iggy::IggyException);
    }
}

TEST_F(E2E_Client, GetClientReturnsDetailsForMatchingClientId) {
    RecordProperty("description", "Returns current client details when querying with the authenticated client id.");
    auto client = GetLoggedInHighLevelClient();

    const auto current_client   = client.GetMe();
    const auto looked_up_client = client.GetClient(current_client.ClientId());

    EXPECT_NE(current_client.ClientId(), 0u);
    EXPECT_TRUE(current_client.UserId().has_value());
    EXPECT_TRUE(looked_up_client.UserId().has_value());
    EXPECT_EQ(looked_up_client.ClientId(), current_client.ClientId());
    EXPECT_EQ(looked_up_client.UserId(), current_client.UserId());
    EXPECT_EQ(looked_up_client.Address(), current_client.Address());
    EXPECT_EQ(looked_up_client.Transport(), "TCP");
    EXPECT_EQ(looked_up_client.Transport(), current_client.Transport());
    EXPECT_EQ(looked_up_client.ConsumerGroupsCount(), current_client.ConsumerGroupsCount());
    EXPECT_EQ(looked_up_client.ConsumerGroups().size(), current_client.ConsumerGroups().size());
}

TEST_F(E2E_Client, GetClientIsStableAcrossBackToBackCalls) {
    RecordProperty("description", "Returns stable client details across back-to-back GetClient calls.");
    auto client = GetLoggedInHighLevelClient();

    const auto current_client = client.GetMe();
    const auto first_lookup   = client.GetClient(current_client.ClientId());
    const auto second_lookup  = client.GetClient(current_client.ClientId());

    EXPECT_NE(current_client.ClientId(), 0u);
    EXPECT_TRUE(current_client.UserId().has_value());
    EXPECT_TRUE(first_lookup.UserId().has_value());
    EXPECT_TRUE(second_lookup.UserId().has_value());
    EXPECT_EQ(first_lookup.ClientId(), current_client.ClientId());
    EXPECT_EQ(second_lookup.ClientId(), first_lookup.ClientId());
    EXPECT_EQ(second_lookup.UserId(), first_lookup.UserId());
    EXPECT_EQ(second_lookup.Address(), first_lookup.Address());
    EXPECT_EQ(first_lookup.Transport(), "TCP");
    EXPECT_EQ(second_lookup.Transport(), "TCP");
    EXPECT_EQ(second_lookup.Transport(), first_lookup.Transport());
    EXPECT_EQ(second_lookup.ConsumerGroupsCount(), first_lookup.ConsumerGroupsCount());
    EXPECT_EQ(second_lookup.ConsumerGroups().size(), first_lookup.ConsumerGroups().size());
}

TEST_F(E2E_Client, GetClientsBeforeLoginThrows) {
    RecordProperty("description",
                   "Rejects GetClients before connect, after connect but before login, and after disconnect.");
    auto client = GetLoggedOutHighLevelClient();

    ASSERT_THROW(client.GetClients(), iggy::IggyException);
    ASSERT_NO_THROW(client.Connect());
    ASSERT_THROW(client.GetClients(), iggy::IggyException);
    ASSERT_NO_THROW(client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(client.Disconnect());
    ASSERT_THROW(client.GetClients(), iggy::IggyException);
}

TEST_F(E2E_Client, GetClientsReturnsActiveClientSessions) {
    RecordProperty("description", "Returns the currently active authenticated client sessions.");
    auto first_client  = GetLoggedInHighLevelClient();
    auto second_client = GetLoggedInHighLevelClient();

    const auto first_me  = first_client.GetMe();
    const auto second_me = second_client.GetMe();
    const auto clients   = first_client.GetClients();

    ASSERT_GE(clients.size(), 2u);

    bool found_first  = false;
    bool found_second = false;
    for (const auto &client : clients) {
        EXPECT_NE(client.ClientId(), 0u);
        EXPECT_EQ(client.Transport(), "TCP");

        if (client.ClientId() == first_me.ClientId()) {
            found_first = true;
            EXPECT_EQ(client.UserId(), first_me.UserId());
            EXPECT_EQ(client.Address(), first_me.Address());
            EXPECT_EQ(client.ConsumerGroupsCount(), first_me.ConsumerGroupsCount());
        }

        if (client.ClientId() == second_me.ClientId()) {
            found_second = true;
            EXPECT_EQ(client.UserId(), second_me.UserId());
            EXPECT_EQ(client.Address(), second_me.Address());
            EXPECT_EQ(client.ConsumerGroupsCount(), second_me.ConsumerGroupsCount());
        }
    }

    EXPECT_TRUE(found_first);
    EXPECT_TRUE(found_second);
}

TEST_F(E2E_Client, GetClientsIsStableAcrossBackToBackCalls) {
    RecordProperty("description", "Returns stable client lists across back-to-back GetClients calls.");
    auto first_client  = GetLoggedInHighLevelClient();
    auto second_client = GetLoggedInHighLevelClient();

    const auto first_me       = first_client.GetMe();
    const auto second_me      = second_client.GetMe();
    const auto first_clients  = first_client.GetClients();
    const auto second_clients = first_client.GetClients();

    ASSERT_GE(first_clients.size(), 2u);
    ASSERT_GE(second_clients.size(), 2u);

    const auto expect_entry_matches = [](const std::vector<iggy::ClientInfo> &clients,
                                         const iggy::ClientInfoDetails &expected) {
        bool found = false;
        for (const auto &entry : clients) {
            if (entry.ClientId() != expected.ClientId()) {
                continue;
            }

            found = true;
            EXPECT_EQ(entry.UserId(), expected.UserId());
            EXPECT_EQ(entry.Address(), expected.Address());
            EXPECT_EQ(entry.Transport(), expected.Transport());
            EXPECT_EQ(entry.ConsumerGroupsCount(), expected.ConsumerGroupsCount());
            break;
        }

        EXPECT_TRUE(found);
    };

    expect_entry_matches(first_clients, first_me);
    expect_entry_matches(first_clients, second_me);
    expect_entry_matches(second_clients, first_me);
    expect_entry_matches(second_clients, second_me);
}

TEST_F(E2E_Client, GetClientsMatchesGetClientForReturnedIds) {
    RecordProperty("description", "Returns list entries that agree with GetClient for each returned client id.");
    auto first_client  = GetLoggedInHighLevelClient();
    auto second_client = GetLoggedInHighLevelClient();

    const auto clients = first_client.GetClients();
    ASSERT_GE(clients.size(), 2u);

    for (const auto &client : clients) {
        SCOPED_TRACE(client.ClientId());
        const auto details = first_client.GetClient(client.ClientId());

        EXPECT_EQ(details.ClientId(), client.ClientId());
        EXPECT_EQ(details.UserId(), client.UserId());
        EXPECT_EQ(details.Address(), client.Address());
        EXPECT_EQ(details.Transport(), client.Transport());
        EXPECT_EQ(details.ConsumerGroupsCount(), client.ConsumerGroupsCount());
    }
}

TEST_F(E2E_Client, GetClientsReflectsAdditionalSession) {
    RecordProperty("description", "Reflects a newly added authenticated session in subsequent GetClients results.");
    auto first_client = GetLoggedInHighLevelClient();

    const auto clients_before = first_client.GetClients();

    auto second_client = GetLoggedInHighLevelClient();

    const auto second_me     = second_client.GetMe();
    const auto clients_after = first_client.GetClients();

    bool found_before = false;
    for (const auto &client : clients_before) {
        if (client.ClientId() == second_me.ClientId()) {
            found_before = true;
            break;
        }
    }
    EXPECT_FALSE(found_before);

    bool found_after = false;
    for (const auto &client : clients_after) {
        if (client.ClientId() != second_me.ClientId()) {
            continue;
        }

        found_after = true;
        EXPECT_EQ(client.UserId(), second_me.UserId());
        EXPECT_EQ(client.Address(), second_me.Address());
        EXPECT_EQ(client.Transport(), "TCP");
        EXPECT_EQ(client.ConsumerGroupsCount(), second_me.ConsumerGroupsCount());
        break;
    }
    EXPECT_TRUE(found_after);
}

TEST_F(E2E_Client, GetClusterMetadataBeforeLoginThrows) {
    RecordProperty("description",
                   "Rejects get_cluster_metadata before connect, after connect but before login, and after disconnect, "
                   "and serves it once authenticated.");
    auto client = GetLoggedOutHighLevelClient();

    ASSERT_THROW(client.GetClusterMetadata(), std::exception);
    ASSERT_NO_THROW(client.Connect());
    // The roster is private, so the read is auth-gated. No pre-login read is
    // needed: a client that dialed a backup logs in there and the server
    // forwards the register to the primary.
    ASSERT_THROW(client.GetClusterMetadata(), std::exception);
    ASSERT_NO_THROW(client.Login("iggy", "iggy"));
    ASSERT_NO_THROW({
        const auto metadata = client.GetClusterMetadata();
        ASSERT_GE(metadata.Nodes().size(), 1u);
    });
    ASSERT_NO_THROW(client.Disconnect());
    ASSERT_THROW(client.GetClusterMetadata(), std::exception);
}

TEST_F(E2E_Client, GetClusterMetadataReturnsSingleNodeMetadata) {
    RecordProperty("description",
                   "Returns the expected single-node cluster metadata shape from the default test server.");
    auto client = GetLoggedInHighLevelClient();

    const auto metadata = client.GetClusterMetadata();

    EXPECT_EQ(metadata.Name(), "single-node");
    ASSERT_EQ(metadata.Nodes().size(), 1u);

    const auto &node = metadata.Nodes()[0];
    EXPECT_FALSE(node.Name().empty());
    EXPECT_FALSE(node.Ip().empty());
    EXPECT_EQ(node.Role(), "leader");
    EXPECT_EQ(node.Status(), "healthy");
    EXPECT_NE(node.Endpoints().Tcp(), 0u);
    EXPECT_NE(node.Endpoints().Http(), 0u);
}

TEST_F(E2E_Client, GetClusterMetadataIsStableAcrossBackToBackCalls) {
    RecordProperty("description", "Returns stable single-node cluster metadata across back-to-back calls.");
    auto client = GetLoggedInHighLevelClient();

    const auto first_metadata  = client.GetClusterMetadata();
    const auto second_metadata = client.GetClusterMetadata();

    EXPECT_EQ(first_metadata.Name(), second_metadata.Name());
    ASSERT_EQ(first_metadata.Nodes().size(), 1u);
    ASSERT_EQ(second_metadata.Nodes().size(), 1u);

    const auto &first_node  = first_metadata.Nodes()[0];
    const auto &second_node = second_metadata.Nodes()[0];
    EXPECT_EQ(first_node.Name(), second_node.Name());
    EXPECT_EQ(first_node.Ip(), second_node.Ip());
    EXPECT_EQ(first_node.Role(), second_node.Role());
    EXPECT_EQ(first_node.Status(), second_node.Status());
    EXPECT_EQ(first_node.Endpoints().Tcp(), second_node.Endpoints().Tcp());
    EXPECT_EQ(first_node.Endpoints().Quic(), second_node.Endpoints().Quic());
    EXPECT_EQ(first_node.Endpoints().Http(), second_node.Endpoints().Http());
    EXPECT_EQ(first_node.Endpoints().Websocket(), second_node.Endpoints().Websocket());
}

TEST_F(E2E_Client, PingSucceedsForNewConnection) {
    RecordProperty("description", "Successfully pings the server from a fresh unauthenticated client session.");
    auto client = GetLoggedOutHighLevelClient();

    // The VSR client has no lazy connect; ping still needs no authentication.
    ASSERT_NO_THROW(client.Connect());
    ASSERT_NO_THROW(client.Ping());
}

TEST_F(E2E_Client, HeartbeatIntervalReturnsDefaultValueForNewConnection) {
    RecordProperty("description",
                   "Returns the default heartbeat interval in microseconds for a fresh unauthenticated client.");
    constexpr std::int64_t default_heartbeat_micros = 5'000'000;
    auto client                                     = GetLoggedOutHighLevelClient();

    EXPECT_EQ(client.HeartbeatInterval().count(), default_heartbeat_micros);
}

TEST_F(E2E_Client, HeartbeatIntervalReturnsConfiguredValueFromConnectionString) {
    RecordProperty("description",
                   "Returns the configured heartbeat interval in microseconds from the connection string.");
    constexpr std::int64_t configured_heartbeat_micros = 10'000'000;
    auto client =
        iggy::IggyBlockingClient::FromConnectionString("iggy://iggy:iggy@127.0.0.1:8090?heartbeat_interval=10s");

    EXPECT_EQ(client.HeartbeatInterval().count(), configured_heartbeat_micros);
}

TEST_F(E2E_Client, SnapshotBeforeLoginThrows) {
    RecordProperty("description",
                   "Rejects snapshot before connect, after connect but before login, and after disconnect.");
    auto client = GetLoggedOutHighLevelClient();

    ASSERT_THROW(client.Snapshot(iggy::SnapshotCompression::Deflated(), {iggy::SystemSnapshotType::Test()}),
                 std::exception);

    ASSERT_NO_THROW(client.Connect());
    ASSERT_THROW(client.Snapshot(iggy::SnapshotCompression::Deflated(), {iggy::SystemSnapshotType::Test()}),
                 std::exception);
    ASSERT_NO_THROW(client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(client.Disconnect());
    ASSERT_THROW(client.Snapshot(iggy::SnapshotCompression::Deflated(), {iggy::SystemSnapshotType::Test()}),
                 std::exception);
}

TEST_F(E2E_Client, SnapshotAllCombinedWithOtherTypeThrows) {
    RecordProperty("description", "Rejects combining the all snapshot type with any other snapshot type.");
    auto client = GetLoggedInHighLevelClient();

    ASSERT_THROW(client.Snapshot(iggy::SnapshotCompression::Deflated(),
                                 {iggy::SystemSnapshotType::All(), iggy::SystemSnapshotType::Test()}),
                 std::exception);
}

TEST_F(E2E_Client, SnapshotWithEmptySnapshotTypesThrows) {
    RecordProperty("description", "Rejects an empty snapshot type list in the wrapper before sending.");
    auto client = GetLoggedInHighLevelClient();

    ASSERT_THROW(client.Snapshot(iggy::SnapshotCompression::Deflated(), std::vector<iggy::SystemSnapshotType>{}),
                 std::exception);
}

TEST_F(E2E_Client, SnapshotReturnsNonEmptyBytes) {
    RecordProperty("description", "Returns a non-empty snapshot for a valid compression and snapshot type.");
    auto client = GetLoggedInHighLevelClient();

    std::vector<std::uint8_t> snapshot_bytes;
    ASSERT_NO_THROW({
        snapshot_bytes = client.Snapshot(iggy::SnapshotCompression::Deflated(), {iggy::SystemSnapshotType::Test()});
    });
    EXPECT_FALSE(snapshot_bytes.empty());
}

TEST_F(E2E_Client, SendBinaryRequestPingReturnsEmptyBytes) {
    RecordProperty("description", "Returns an empty response body for a raw ping command with an empty payload.");
    constexpr std::uint32_t ping_command_code = 1;
    auto client                               = GetLoggedInHighLevelClient();

    const std::vector<std::uint8_t> empty_payload;
    std::vector<std::uint8_t> response;
    ASSERT_NO_THROW({ response = client.SendBinaryRequest(ping_command_code, empty_payload); });
    EXPECT_TRUE(response.empty());
}

TEST_F(E2E_Client, SendBinaryRequestGetStatsReturnsNonEmptyBytes) {
    RecordProperty("description",
                   "Returns a non-empty response body for a raw get-stats command with an empty payload.");
    constexpr std::uint32_t get_stats_command_code = 10;
    auto client                                    = GetLoggedInHighLevelClient();

    const std::vector<std::uint8_t> empty_payload;
    std::vector<std::uint8_t> response;
    ASSERT_NO_THROW({ response = client.SendBinaryRequest(get_stats_command_code, empty_payload); });
    EXPECT_FALSE(response.empty());
}

TEST_F(E2E_Client, SendBinaryRequestLoginUserCodeThrows) {
    RecordProperty("description",
                   "Rejects the login-user session-control code client-side before it reaches the server.");
    constexpr std::uint32_t login_user_command_code = 38;
    auto client                                     = GetLoggedInHighLevelClient();

    const std::vector<std::uint8_t> empty_payload;
    ASSERT_THROW(client.SendBinaryRequest(login_user_command_code, empty_payload), std::exception);
}

TEST_F(E2E_Client, SendBinaryRequestUnknownCommandCodeThrows) {
    RecordProperty("description", "Rejects an unknown command code with an invalid-command error from the server.");
    constexpr std::uint32_t unknown_command_code = 60000;
    auto client                                  = GetLoggedInHighLevelClient();

    const std::vector<std::uint8_t> empty_payload;
    ASSERT_THROW(client.SendBinaryRequest(unknown_command_code, empty_payload), std::exception);
}

TEST_F(E2E_Client, ConsumerOffsetStoreGetDeleteRoundTrip) {
    RecordProperty("description", "Stores a consumer offset, reads it back, and deletes it.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name,
                       iggy::TopicCreateOptions().SetPartitionsCount(1));

    std::vector<iggy::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 5; i++) {
        messages.push_back(
            iggy::IggyMessageToSend::Create("offset-" + std::to_string(i), std::vector<iggy::HeaderEntry>()));
    }
    client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                        iggy::Partitioning::PartitionId(0), messages);

    const auto consumer = iggy::Consumer::Single(iggy::Identifier::String("offset-consumer"));
    ASSERT_NO_THROW(client.StoreConsumerOffset(consumer, iggy::Identifier::Numeric(stream.Id()),
                                               iggy::Identifier::Numeric(0), 0, 2));

    auto stored =
        client.GetConsumerOffset(consumer, iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0);
    EXPECT_EQ(stored.PartitionId(), 0u);
    EXPECT_EQ(stored.StoredOffset(), 2u);

    ASSERT_NO_THROW(
        client.DeleteConsumerOffset(consumer, iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0));

    ASSERT_THROW(
        client.GetConsumerOffset(consumer, iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0),
        iggy::IggyException);
}

TEST_F(E2E_Client, ConsumerOffsetKeepsDistinctConsumersApart) {
    RecordProperty("description", "Each consumer owns its stored offset on the same partition.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name,
                       iggy::TopicCreateOptions().SetPartitionsCount(1));

    std::vector<iggy::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 5; i++) {
        messages.push_back(
            iggy::IggyMessageToSend::Create("offset-" + std::to_string(i), std::vector<iggy::HeaderEntry>()));
    }
    client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                        iggy::Partitioning::PartitionId(0), messages);

    const auto consumer_a = iggy::Consumer::Single(iggy::Identifier::String("consumer-a"));
    const auto consumer_b = iggy::Consumer::Single(iggy::Identifier::String("consumer-b"));
    client.StoreConsumerOffset(consumer_a, iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0, 1);
    client.StoreConsumerOffset(consumer_b, iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0, 3);

    auto offset_a =
        client.GetConsumerOffset(consumer_a, iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0);
    auto offset_b =
        client.GetConsumerOffset(consumer_b, iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0);

    EXPECT_EQ(offset_a.StoredOffset(), 1u);
    EXPECT_EQ(offset_b.StoredOffset(), 3u);
}

TEST_F(E2E_Client, StoreConsumerOffsetRejectsAnyPartitionId) {
    RecordProperty("description", "Storing an offset needs an explicit partition, unlike polling.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name,
                       iggy::TopicCreateOptions().SetPartitionsCount(1));

    const auto consumer = iggy::Consumer::Single(iggy::Identifier::String("offset-consumer"));
    ASSERT_THROW(client.StoreConsumerOffset(consumer, iggy::Identifier::Numeric(stream.Id()),
                                            iggy::Identifier::Numeric(0), iggy::kAnyPartitionId, 1),
                 iggy::IggyException);
}
