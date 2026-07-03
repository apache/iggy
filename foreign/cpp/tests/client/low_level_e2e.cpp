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

// TODO(slbotbm): Add tests for store_consumer_offset, get_consumer_offset, and delete_consumer_offset functions
// attached to client after implementing consumer group functions
#include <cstdint>
#include <string>
#include <unordered_set>

#include <gtest/gtest.h>

#include "lib.rs.h"
#include "tests/common/test_helpers.hpp"

class LowLevelE2E_Client : public E2ETestFixture {};

TEST_F(LowLevelE2E_Client, ConnectAndLogin) {
    RecordProperty("description", "Connects and logs in successfully using each supported connection string format.");
    const std::string username             = "iggy";
    const std::string password             = "iggy";
    const std::string connection_strings[] = {
        "iggy://iggy:iggy@127.0.0.1:8090",
        "iggy+tcp://iggy:iggy@127.0.0.1:8090",
        "iggy+http://iggy:iggy@127.0.0.1:3000",
        "",
    };

    for (const std::string &connection_string : connection_strings) {
        SCOPED_TRACE(connection_string);
        iggy::ffi::Client *client = nullptr;
        ASSERT_NO_THROW({ client = iggy::ffi::new_connection(connection_string); });
        ASSERT_NE(client, nullptr);
        TrackClient(client);

        ASSERT_NO_THROW(client->connect());
        ASSERT_NO_THROW(client->login_user(username, password));
    }
}

TEST_F(LowLevelE2E_Client, NewConnectionWithMalformedConnectionStringsThrow) {
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

TEST_F(LowLevelE2E_Client, LoginWithInvalidCredentialsThrows) {
    RecordProperty("description", "Throws when authentication uses invalid credentials after connecting.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);
    TrackClient(client);

    ASSERT_NO_THROW(client->connect());
    ASSERT_THROW(client->login_user("biggy", "biggy"), std::exception);
}

TEST_F(LowLevelE2E_Client, LoginTwiceWithDifferentCredentials) {
    RecordProperty("description", "Rejects a second login attempt that switches to invalid credentials.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);
    TrackClient(client);

    ASSERT_NO_THROW(client->connect());
    ASSERT_NO_THROW(client->login_user("iggy", "iggy"));
    ASSERT_THROW(client->login_user("biggy", "biggy"), std::exception);
}

TEST_F(LowLevelE2E_Client, LogoutWithoutLogin) {
    RecordProperty("description",
                   "Rejects logout before authentication, both before and after connect, then succeeds after login.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);
    TrackClient(client);

    ASSERT_THROW(client->logout_user(), std::exception);

    ASSERT_NO_THROW(client->connect());
    ASSERT_THROW(client->logout_user(), std::exception);

    ASSERT_NO_THROW(client->login_user("iggy", "iggy"));
    ASSERT_NO_THROW(client->logout_user());
}

TEST_F(LowLevelE2E_Client, ReloginOnSameClientAfterLogout) {
    RecordProperty("description", "Allows reauthenticating on the same connected client after a successful logout.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);
    TrackClient(client);

    iggy::ffi::ClientInfoDetails first_me{};
    iggy::ffi::ClientInfoDetails second_me{};
    ASSERT_NO_THROW(client->connect());
    ASSERT_NO_THROW(client->login_user("iggy", "iggy"));
    ASSERT_NO_THROW({ first_me = client->get_me(); });
    ASSERT_NO_THROW(client->logout_user());
    ASSERT_NO_THROW(client->login_user("iggy", "iggy"));
    ASSERT_NO_THROW({ second_me = client->get_me(); });

    EXPECT_EQ(second_me.client_id, first_me.client_id);
    EXPECT_EQ(second_me.user_id, first_me.user_id);
}

TEST_F(LowLevelE2E_Client, LogoutErrorsWhenCalledMoreThanOnce) {
    RecordProperty("description",
                   "Rejects repeated logout calls once the authenticated session has already logged out.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);
    TrackClient(client);

    ASSERT_NO_THROW(client->connect());
    ASSERT_NO_THROW(client->login_user("iggy", "iggy"));
    ASSERT_NO_THROW(client->logout_user());
    ASSERT_THROW(client->logout_user(), std::exception);
}

TEST_F(LowLevelE2E_Client, DeleteWhileUnauthenticatedAfterFailedLogin) {
    RecordProperty("description", "Allows client cleanup after a failed login leaves the connection unauthenticated.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);

    ASSERT_NO_THROW(client->connect());
    ASSERT_THROW(client->login_user("biggy", "biggy"), std::exception);
    ASSERT_NO_THROW(iggy::ffi::delete_client(client));
    client = nullptr;
}

TEST_F(LowLevelE2E_Client, ConnectLoginThenDisconnect) {
    RecordProperty("description",
                   "Connects, logs in, disconnects successfully, and rejects authenticated operations afterward.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);
    TrackClient(client);

    ASSERT_NO_THROW(client->connect());
    ASSERT_NO_THROW(client->login_user("iggy", "iggy"));
    ASSERT_NO_THROW(client->disconnect());
    ASSERT_THROW(client->get_me(), std::exception);
}

TEST_F(LowLevelE2E_Client, DisconnectWithoutConnect) {
    RecordProperty("description", "Allows disconnect to be called on a client that was never explicitly connected.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);
    TrackClient(client);

    ASSERT_NO_THROW(client->disconnect());
}

TEST_F(LowLevelE2E_Client, DisconnectWithoutLogin) {
    RecordProperty("description", "Allows disconnect after connect even when no user has authenticated.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);
    TrackClient(client);

    ASSERT_NO_THROW(client->connect());
    ASSERT_NO_THROW(client->disconnect());
    ASSERT_THROW(client->get_stats(), std::exception);
}

TEST_F(LowLevelE2E_Client, DisconnectThenReconnectWithoutRelogin) {
    RecordProperty("description",
                   "Requires logging in again after a disconnect and reconnect before authenticated operations work.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);
    TrackClient(client);

    ASSERT_NO_THROW(client->connect());
    ASSERT_NO_THROW(client->login_user("iggy", "iggy"));
    ASSERT_NO_THROW(client->disconnect());
    ASSERT_NO_THROW(client->connect());
    ASSERT_THROW(client->get_me(), std::exception);
}

TEST_F(LowLevelE2E_Client, DisconnectAfterFailedLogin) {
    RecordProperty("description", "Allows disconnect after a failed login attempt leaves the client unauthenticated.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);
    TrackClient(client);

    ASSERT_NO_THROW(client->connect());
    ASSERT_THROW(client->login_user("biggy", "biggy"), std::exception);
    ASSERT_NO_THROW(client->disconnect());
    ASSERT_THROW(client->get_me(), std::exception);
}

TEST_F(LowLevelE2E_Client, ConnectLoginThenShutdown) {
    RecordProperty("description",
                   "Connects, logs in, shuts down successfully, and rejects further operations afterward.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);
    TrackClient(client);

    ASSERT_NO_THROW(client->connect());
    ASSERT_NO_THROW(client->login_user("iggy", "iggy"));
    ASSERT_NO_THROW(client->ping());
    ASSERT_NO_THROW(client->shutdown());
    ASSERT_THROW(client->get_me(), std::exception);
    ASSERT_THROW(client->get_stats(), std::exception);
}

TEST_F(LowLevelE2E_Client, ShutdownWithoutConnect) {
    RecordProperty("description", "Allows shutdown to be called on a client that was never explicitly connected.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);
    TrackClient(client);

    ASSERT_NO_THROW(client->shutdown());
}

TEST_F(LowLevelE2E_Client, ShutdownWithoutLogin) {
    RecordProperty("description", "Allows shutdown after connect even when no user has authenticated.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);
    TrackClient(client);

    ASSERT_NO_THROW(client->connect());
    ASSERT_NO_THROW(client->shutdown());
    ASSERT_THROW(client->get_stats(), std::exception);
}

TEST_F(LowLevelE2E_Client, ShutdownAfterFailedLogin) {
    RecordProperty("description", "Allows shutdown after a failed login attempt leaves the client unauthenticated.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);
    TrackClient(client);

    ASSERT_NO_THROW(client->connect());
    ASSERT_THROW(client->login_user("biggy", "biggy"), std::exception);
    ASSERT_NO_THROW(client->shutdown());
    ASSERT_THROW(client->get_me(), std::exception);
}

TEST_F(LowLevelE2E_Client, RepeatedShutdownCallsHaveStableBehavior) {
    RecordProperty("description", "Keeps repeated shutdown calls stable across duplicate invocations.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);
    TrackClient(client);

    ASSERT_NO_THROW(client->connect());
    ASSERT_NO_THROW(client->login_user("iggy", "iggy"));
    ASSERT_NO_THROW(client->shutdown());
    ASSERT_NO_THROW(client->shutdown());
    ASSERT_THROW(client->get_me(), std::exception);
}

TEST_F(LowLevelE2E_Client, ShutdownThenConnectThrows) {
    RecordProperty("description", "Rejects reconnecting a client after shutdown transitions it to a terminal state.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);
    TrackClient(client);

    ASSERT_NO_THROW(client->connect());
    ASSERT_NO_THROW(client->login_user("iggy", "iggy"));
    ASSERT_NO_THROW(client->shutdown());
    ASSERT_THROW(client->connect(), std::exception);
}

TEST_F(LowLevelE2E_Client, ShutdownThenLoginThrows) {
    RecordProperty("description",
                   "Rejects logging in again after shutdown, even when login would normally auto-connect.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);
    TrackClient(client);

    ASSERT_NO_THROW(client->shutdown());
    ASSERT_THROW(client->login_user("iggy", "iggy"), std::exception);
}

TEST_F(LowLevelE2E_Client, GetClientsReflectsSessionRemovalAfterShutdown) {
    RecordProperty("description",
                   "Removes a shut down authenticated session from subsequent get_clients and get_client results.");
    iggy::ffi::Client *first_client  = GetLoggedInClient();
    iggy::ffi::Client *second_client = GetLoggedInClient();

    iggy::ffi::ClientInfoDetails first_me{};
    rust::Vec<iggy::ffi::ClientInfo> clients_after_shutdown;
    ASSERT_NO_THROW({ first_me = first_client->get_me(); });

    ASSERT_NO_THROW(first_client->shutdown());
    ASSERT_NO_THROW({ clients_after_shutdown = second_client->get_clients(); });

    bool found_first = false;
    for (const auto &client : clients_after_shutdown) {
        if (client.client_id == first_me.client_id) {
            found_first = true;
            break;
        }
    }

    EXPECT_FALSE(found_first);
    ASSERT_THROW(second_client->get_client(first_me.client_id), std::exception);
}

TEST_F(LowLevelE2E_Client, GetClientsReflectsSessionRemovalAfterDisconnect) {
    RecordProperty("description",
                   "Removes a disconnected authenticated session from subsequent get_clients and get_client results.");
    iggy::ffi::Client *first_client  = GetLoggedInClient();
    iggy::ffi::Client *second_client = GetLoggedInClient();

    iggy::ffi::ClientInfoDetails first_me{};
    rust::Vec<iggy::ffi::ClientInfo> clients_after_disconnect;
    ASSERT_NO_THROW({ first_me = first_client->get_me(); });

    ASSERT_NO_THROW(first_client->disconnect());
    ASSERT_NO_THROW({ clients_after_disconnect = second_client->get_clients(); });

    bool found_first = false;
    for (const auto &client : clients_after_disconnect) {
        if (client.client_id == first_me.client_id) {
            found_first = true;
            break;
        }
    }

    EXPECT_FALSE(found_first);
    ASSERT_THROW(second_client->get_client(first_me.client_id), std::exception);
}

TEST_F(LowLevelE2E_Client, GetClientsReflectsLoggedOutSessionAsUnauthenticated) {
    RecordProperty("description",
                   "Keeps a logged out session visible in get_clients and get_client, but marks it unauthenticated.");
    iggy::ffi::Client *first_client  = GetLoggedInClient();
    iggy::ffi::Client *second_client = GetLoggedInClient();

    iggy::ffi::ClientInfoDetails first_me{};
    iggy::ffi::ClientInfoDetails logged_out_client{};
    rust::Vec<iggy::ffi::ClientInfo> clients_after_logout;
    ASSERT_NO_THROW({ first_me = first_client->get_me(); });

    ASSERT_NO_THROW(first_client->logout_user());
    ASSERT_NO_THROW({
        clients_after_logout = second_client->get_clients();
        logged_out_client    = second_client->get_client(first_me.client_id);
    });

    bool found_first = false;
    for (const auto &client : clients_after_logout) {
        if (client.client_id != first_me.client_id) {
            continue;
        }

        found_first = true;
        EXPECT_FALSE(client.has_user_id);
        EXPECT_EQ(static_cast<std::string>(client.address), static_cast<std::string>(first_me.address));
        EXPECT_EQ(static_cast<std::string>(client.transport), static_cast<std::string>(first_me.transport));
        break;
    }

    EXPECT_TRUE(found_first);
    EXPECT_EQ(logged_out_client.client_id, first_me.client_id);
    EXPECT_FALSE(logged_out_client.has_user_id);
    EXPECT_EQ(static_cast<std::string>(logged_out_client.address), static_cast<std::string>(first_me.address));
    EXPECT_EQ(static_cast<std::string>(logged_out_client.transport), static_cast<std::string>(first_me.transport));
}

TEST_F(LowLevelE2E_Client, LoginWithoutConnect) {
    RecordProperty("description", "Supports login without an explicit prior connect call.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);
    TrackClient(client);

    ASSERT_NO_THROW(client->login_user("iggy", "iggy"));
}

TEST_F(LowLevelE2E_Client, ConnectWithoutLoginThenDelete) {
    RecordProperty("description", "Allows connecting without logging in and then deleting the client.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);

    ASSERT_NO_THROW(client->connect());
    ASSERT_NO_THROW(iggy::ffi::delete_client(client));
    client = nullptr;
}

TEST_F(LowLevelE2E_Client, DeleteWithoutDisconnect) {
    RecordProperty("description", "Allows deleting a connected and authenticated client without disconnecting first.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);

    ASSERT_NO_THROW(client->connect());
    ASSERT_NO_THROW(client->login_user("iggy", "iggy"));
    ASSERT_NO_THROW(iggy::ffi::delete_client(client));
    client = nullptr;
}

TEST_F(LowLevelE2E_Client, RepeatedClientMethodCallsHaveStableBehavior) {
    RecordProperty("description",
                   "Keeps repeated connect, login, and delete calls stable across duplicate invocations.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);

    ASSERT_NO_THROW(client->connect());
    ASSERT_NO_THROW(client->connect());
    ASSERT_NO_THROW(client->login_user("iggy", "iggy"));
    ASSERT_NO_THROW(client->login_user("iggy", "iggy"));
    ASSERT_NO_THROW(iggy::ffi::delete_client(client));
    client = nullptr;

    ASSERT_NO_THROW(iggy::ffi::delete_client(client));
}

TEST_F(LowLevelE2E_Client, RepeatedDisconnectCallsHaveStableBehavior) {
    RecordProperty("description", "Keeps repeated disconnect calls stable across duplicate invocations.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);
    TrackClient(client);

    ASSERT_NO_THROW(client->connect());
    ASSERT_NO_THROW(client->login_user("iggy", "iggy"));
    ASSERT_NO_THROW(client->disconnect());
    ASSERT_NO_THROW(client->disconnect());
    ASSERT_THROW(client->get_me(), std::exception);
}

TEST_F(LowLevelE2E_Client, DeleteNullConnectionIsNoop) {
    RecordProperty("description", "Treats deleting a null client pointer as a no-op.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW(iggy::ffi::delete_client(client));
}

TEST_F(LowLevelE2E_Client, GetStatsBeforeLoginThrows) {
    RecordProperty("description",
                   "Rejects get_stats before connect, after connect but before login, and after disconnect.");
    iggy::ffi::Client *client = GetLoggedOutClient();

    ASSERT_THROW(client->get_stats(), std::exception);
    ASSERT_NO_THROW(client->connect());
    ASSERT_THROW(client->get_stats(), std::exception);
    ASSERT_NO_THROW(client->login_user("iggy", "iggy"));
    ASSERT_NO_THROW(client->disconnect());
    ASSERT_THROW(client->get_stats(), std::exception);
}

TEST_F(LowLevelE2E_Client, FlushUnsavedBufferSucceedsForExistingPartition) {
    RecordProperty("description",
                   "Creates a stream and topic, sends one message, and flushes the partition buffer successfully.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    ASSERT_NO_THROW(client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire",
                                         0, "server_default"));

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    messages.push_back(iggy::ffi::make_message(to_payload("flush-me")));

    ASSERT_NO_THROW(client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0),
                                          "partition_id", partition_id_bytes(0), std::move(messages)));
    ASSERT_NO_THROW(
        client->flush_unsaved_buffer(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, true));
}

TEST_F(LowLevelE2E_Client, FlushUnsavedBufferSucceedsForExistingEmptyPartition) {
    RecordProperty("description",
                   "Succeeds when flush_unsaved_buffer is called for an existing partition with no unsaved messages.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    ASSERT_NO_THROW(client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire",
                                         0, "server_default"));

    ASSERT_NO_THROW(
        client->flush_unsaved_buffer(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, true));
}

TEST_F(LowLevelE2E_Client, FlushUnsavedBufferBeforeLoginThrows) {
    RecordProperty("description",
                   "Throws when flush_unsaved_buffer is called before connect, after connect but before login, and "
                   "after disconnect.");
    iggy::ffi::Client *client = GetLoggedOutClient();

    ASSERT_THROW(client->flush_unsaved_buffer(make_numeric_identifier(1), make_numeric_identifier(1), 0, true),
                 std::exception);
    ASSERT_NO_THROW(client->connect());
    ASSERT_THROW(client->flush_unsaved_buffer(make_numeric_identifier(1), make_numeric_identifier(1), 0, true),
                 std::exception);
    ASSERT_NO_THROW(client->login_user("iggy", "iggy"));
    ASSERT_NO_THROW(client->disconnect());
    ASSERT_THROW(client->flush_unsaved_buffer(make_numeric_identifier(1), make_numeric_identifier(1), 0, true),
                 std::exception);
}

TEST_F(LowLevelE2E_Client, FlushUnsavedBufferOnNonExistentStreamThrows) {
    RecordProperty("description", "Throws when flush_unsaved_buffer is called for a stream that does not exist.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0, "never_expire",
                                         0, "server_default"));

    ASSERT_THROW(
        client->flush_unsaved_buffer(make_string_identifier(GetRandomName()), make_numeric_identifier(0), 0, true),
        std::exception);
}

TEST_F(LowLevelE2E_Client, FlushUnsavedBufferOnNonExistentTopicThrows) {
    RecordProperty("description", "Throws when flush_unsaved_buffer is called for a topic that does not exist.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0, "never_expire",
                                         0, "server_default"));

    ASSERT_THROW(client->flush_unsaved_buffer(make_string_identifier(stream_name),
                                              make_string_identifier(GetRandomName()), 0, true),
                 std::exception);
}

TEST_F(LowLevelE2E_Client, FlushUnsavedBufferAfterStreamDeletedThrows) {
    RecordProperty("description", "Throws when flush_unsaved_buffer is called after the stream has been deleted.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    ASSERT_NO_THROW(client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire",
                                         0, "server_default"));

    const std::uint32_t saved_stream_id = stream.id;
    ASSERT_NO_THROW(client->delete_stream(make_numeric_identifier(saved_stream_id)));
    ForgetTrackedStream(saved_stream_id);

    ASSERT_THROW(
        client->flush_unsaved_buffer(make_numeric_identifier(saved_stream_id), make_numeric_identifier(0), 0, true),
        std::exception);
}

TEST_F(LowLevelE2E_Client, FlushUnsavedBufferAfterTopicDeletedThrows) {
    RecordProperty("description", "Throws when flush_unsaved_buffer is called after the topic has been deleted.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    ASSERT_NO_THROW(client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire",
                                         0, "server_default"));
    ASSERT_NO_THROW(client->delete_topic(make_numeric_identifier(stream.id), make_string_identifier(topic_name)));

    ASSERT_THROW(
        client->flush_unsaved_buffer(make_numeric_identifier(stream.id), make_string_identifier(topic_name), 0, true),
        std::exception);
}

TEST_F(LowLevelE2E_Client, FlushUnsavedBufferTwiceSucceeds) {
    RecordProperty("description", "Allows flush_unsaved_buffer to be called twice in a row for the same partition.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    ASSERT_NO_THROW(client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire",
                                         0, "server_default"));

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    messages.push_back(iggy::ffi::make_message(to_payload("flush-twice")));

    ASSERT_NO_THROW(client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0),
                                          "partition_id", partition_id_bytes(0), std::move(messages)));
    ASSERT_NO_THROW(
        client->flush_unsaved_buffer(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, true));
    ASSERT_NO_THROW(
        client->flush_unsaved_buffer(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, true));
}

TEST_F(LowLevelE2E_Client, FlushUnsavedBufferWithInvalidPartitionIdsThrows) {
    RecordProperty("description", "Throws when flush_unsaved_buffer is called for non-existent partition ids.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    ASSERT_NO_THROW(client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire",
                                         0, "server_default"));

    const std::uint32_t invalid_partition_ids[] = {1u, 9999u, static_cast<std::uint32_t>(-1)};
    for (const std::uint32_t invalid_partition_id : invalid_partition_ids) {
        SCOPED_TRACE(invalid_partition_id);
        ASSERT_THROW(client->flush_unsaved_buffer(make_numeric_identifier(stream.id), make_numeric_identifier(0),
                                                  invalid_partition_id, true),
                     std::exception);
    }
}

// TODO(slbotbm): add a test to create some streams, topics, partitions, and segments, send messages, and create
// consumer groups and verify it.
TEST_F(LowLevelE2E_Client, GetStatsReturnsServerStats) {
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
    iggy::ffi::Client *client                           = GetLoggedInClient();

    iggy::ffi::Client *second_client = nullptr;
    iggy::ffi::Client *third_client  = nullptr;

    iggy::ffi::Stats empty_stats{};
    iggy::ffi::Stats stats_after_create{};
    ASSERT_NO_THROW({
        empty_stats = client->get_stats();
        EXPECT_NE(empty_stats.process_id, 0u);
        EXPECT_GT(empty_stats.threads_count, 0u);
        EXPECT_GT(empty_stats.total_memory, 0u);
        EXPECT_LE(empty_stats.available_memory, empty_stats.total_memory);
        EXPECT_GE(empty_stats.total_disk_space, empty_stats.free_disk_space);
        EXPECT_FALSE(static_cast<std::string>(empty_stats.hostname).empty());
        EXPECT_FALSE(static_cast<std::string>(empty_stats.os_name).empty());
        EXPECT_FALSE(static_cast<std::string>(empty_stats.os_version).empty());
        EXPECT_FALSE(static_cast<std::string>(empty_stats.kernel_version).empty());
        EXPECT_FALSE(static_cast<std::string>(empty_stats.iggy_server_version).empty());
    });

    ASSERT_NO_THROW(client->create_stream(first_stream_name));
    TrackStream(first_stream_name);
    ASSERT_NO_THROW(client->create_stream(second_stream_name));
    TrackStream(second_stream_name);
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

    ASSERT_NO_THROW({ second_client = GetLoggedInClient(); });
    ASSERT_NE(second_client, nullptr);
    ASSERT_NO_THROW({ third_client = GetLoggedInClient(); });
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
        stats_after_create = client->get_stats();
        EXPECT_GE(stats_after_create.streams_count, empty_stats.streams_count + 2u);
        EXPECT_GE(stats_after_create.topics_count, empty_stats.topics_count + expected_topics_count);
        EXPECT_GE(stats_after_create.partitions_count, empty_stats.partitions_count + expected_partitions_count);
        EXPECT_GE(stats_after_create.segments_count, empty_stats.segments_count + expected_partitions_count);
        EXPECT_GE(stats_after_create.consumer_groups_count, empty_stats.consumer_groups_count + 3u);
        EXPECT_GE(stats_after_create.clients_count, empty_stats.clients_count + 2u);
        EXPECT_EQ(first_group.partitions_count, first_topic_partitions);
        EXPECT_EQ(second_group.partitions_count, second_topic_partitions);
        EXPECT_EQ(third_group.partitions_count, third_topic_partitions);
    });

    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(second_stream_name)));
    ForgetTrackedStream(second_stream_name);
    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(first_stream_name)));
    ForgetTrackedStream(first_stream_name);
    DeleteClient(third_client);
    DeleteClient(second_client);

    ASSERT_NO_THROW({
        const auto stats = client->get_stats();
        EXPECT_LE(stats.streams_count, stats_after_create.streams_count);
        EXPECT_LE(stats.topics_count, stats_after_create.topics_count);
        EXPECT_LE(stats.partitions_count, stats_after_create.partitions_count);
        EXPECT_LE(stats.segments_count, stats_after_create.segments_count);
        EXPECT_LE(stats.consumer_groups_count, stats_after_create.consumer_groups_count);
        EXPECT_LE(stats.clients_count, stats_after_create.clients_count);
    });
}

TEST_F(LowLevelE2E_Client, GetStatsIsStableAcrossBackToBackCalls) {
    RecordProperty(
        "description",
        "Returns sane invariant fields across back-to-back get_stats calls on an idle authenticated client.");
    iggy::ffi::Client *client = GetLoggedInClient();

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
    EXPECT_EQ(second_stats.has_server_semver, first_stats.has_server_semver);
    EXPECT_EQ(second_stats.iggy_server_semver, first_stats.iggy_server_semver);
    EXPECT_GE(first_stats.clients_count, 1u);
    EXPECT_GE(second_stats.clients_count, 1u);
}

TEST_F(LowLevelE2E_Client, GetMeBeforeLoginThrows) {
    RecordProperty("description",
                   "Rejects get_me before connect, after connect but before login, and after disconnect.");
    iggy::ffi::Client *client = GetLoggedOutClient();

    ASSERT_THROW(client->get_me(), std::exception);
    ASSERT_NO_THROW(client->connect());
    ASSERT_THROW(client->get_me(), std::exception);
    ASSERT_NO_THROW(client->login_user("iggy", "iggy"));
    ASSERT_NO_THROW(client->disconnect());
    ASSERT_THROW(client->get_me(), std::exception);
}

TEST_F(LowLevelE2E_Client, GetMeReturnsCurrentClientDetails) {
    RecordProperty("description", "Returns the current authenticated client details.");
    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW({
        const auto me = client->get_me();
        EXPECT_NE(me.client_id, 0u);
        EXPECT_TRUE(me.has_user_id);
        EXPECT_FALSE(static_cast<std::string>(me.address).empty());
        EXPECT_EQ(static_cast<std::string>(me.transport), "TCP");
        EXPECT_EQ(me.consumer_groups_count, 0u);
        EXPECT_TRUE(me.consumer_groups.empty());
    });
}

TEST_F(LowLevelE2E_Client, GetMeReflectsConsumerGroupMembershipChanges) {
    RecordProperty("description", "Reflects joined consumer groups in get_me and removes them again after leaving.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    const std::string group_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));

    const auto stream_details = client->get_stream(make_string_identifier(stream_name));
    ASSERT_EQ(stream_details.topics.size(), 1u);
    const auto created_group = client->create_consumer_group(make_string_identifier(stream_name),
                                                             make_string_identifier(topic_name), group_name);

    std::size_t baseline_groups_size    = 0;
    std::uint32_t baseline_groups_count = 0;
    ASSERT_NO_THROW({
        const auto me         = client->get_me();
        baseline_groups_count = me.consumer_groups_count;
        baseline_groups_size  = me.consumer_groups.size();
    });

    ASSERT_NO_THROW(client->join_consumer_group(make_numeric_identifier(stream_details.id),
                                                make_numeric_identifier(stream_details.topics[0].id),
                                                make_numeric_identifier(created_group.id)));

    ASSERT_NO_THROW({
        const auto me = client->get_me();
        EXPECT_GT(me.consumer_groups_count, baseline_groups_count);
        EXPECT_GT(me.consumer_groups.size(), baseline_groups_size);

        bool found_group = false;
        for (const auto &group : me.consumer_groups) {
            if (group.stream_id != stream_details.id || group.topic_id != stream_details.topics[0].id ||
                group.group_id != created_group.id) {
                continue;
            }
            found_group = true;
            break;
        }
        EXPECT_TRUE(found_group);
    });

    ASSERT_NO_THROW(client->leave_consumer_group(make_numeric_identifier(stream_details.id),
                                                 make_numeric_identifier(stream_details.topics[0].id),
                                                 make_numeric_identifier(created_group.id)));

    ASSERT_NO_THROW({
        const auto me = client->get_me();
        EXPECT_GE(me.consumer_groups_count, baseline_groups_count);
        EXPECT_GE(me.consumer_groups.size(), baseline_groups_size);

        bool found_group = false;
        for (const auto &group : me.consumer_groups) {
            if (group.stream_id != stream_details.id || group.topic_id != stream_details.topics[0].id ||
                group.group_id != created_group.id) {
                continue;
            }
            found_group = true;
            break;
        }
        EXPECT_FALSE(found_group);
    });
}

TEST_F(LowLevelE2E_Client, GetMeIsStableAcrossBackToBackCalls) {
    RecordProperty("description", "Returns stable current-client details across back-to-back get_me calls.");
    iggy::ffi::Client *client = GetLoggedInClient();

    iggy::ffi::ClientInfoDetails first_me{};
    iggy::ffi::ClientInfoDetails second_me{};
    ASSERT_NO_THROW({
        first_me  = client->get_me();
        second_me = client->get_me();
    });

    EXPECT_NE(first_me.client_id, 0u);
    EXPECT_TRUE(first_me.has_user_id);
    EXPECT_TRUE(second_me.has_user_id);
    EXPECT_EQ(second_me.client_id, first_me.client_id);
    EXPECT_EQ(second_me.has_user_id, first_me.has_user_id);
    EXPECT_EQ(second_me.user_id, first_me.user_id);
    EXPECT_EQ(static_cast<std::string>(second_me.address), static_cast<std::string>(first_me.address));
    EXPECT_EQ(static_cast<std::string>(first_me.transport), "TCP");
    EXPECT_EQ(static_cast<std::string>(second_me.transport), "TCP");
    EXPECT_EQ(static_cast<std::string>(second_me.transport), static_cast<std::string>(first_me.transport));
    EXPECT_EQ(second_me.consumer_groups_count, first_me.consumer_groups_count);
    EXPECT_EQ(second_me.consumer_groups.size(), first_me.consumer_groups.size());
}

TEST_F(LowLevelE2E_Client, GetMeReturnsDistinctClientIdsForDifferentSessions) {
    RecordProperty(
        "description",
        "Returns different client ids for separate authenticated sessions while keeping the same user identity.");
    iggy::ffi::Client *first_client  = GetLoggedInClient();
    iggy::ffi::Client *second_client = GetLoggedInClient();

    iggy::ffi::ClientInfoDetails first_me{};
    iggy::ffi::ClientInfoDetails second_me{};
    ASSERT_NO_THROW({
        first_me  = first_client->get_me();
        second_me = second_client->get_me();
    });

    EXPECT_NE(first_me.client_id, 0u);
    EXPECT_NE(second_me.client_id, 0u);
    EXPECT_TRUE(first_me.has_user_id);
    EXPECT_TRUE(second_me.has_user_id);
    EXPECT_NE(second_me.client_id, first_me.client_id);
    EXPECT_EQ(second_me.has_user_id, first_me.has_user_id);
    EXPECT_EQ(second_me.user_id, first_me.user_id);
    EXPECT_EQ(static_cast<std::string>(first_me.transport), "TCP");
    EXPECT_EQ(static_cast<std::string>(second_me.transport), "TCP");
}

TEST_F(LowLevelE2E_Client, GetMeReturnsValidDetailsAfterReconnect) {
    RecordProperty("description",
                   "Returns valid current-client details after reconnecting with a fresh authenticated session.");
    iggy::ffi::Client *first_client = GetLoggedInClient();

    iggy::ffi::ClientInfoDetails first_me{};
    ASSERT_NO_THROW({ first_me = first_client->get_me(); });
    EXPECT_NE(first_me.client_id, 0u);
    EXPECT_TRUE(first_me.has_user_id);

    DeleteClient(first_client);

    iggy::ffi::Client *second_client = GetLoggedInClient();

    iggy::ffi::ClientInfoDetails second_me{};
    ASSERT_NO_THROW({ second_me = second_client->get_me(); });
    EXPECT_NE(second_me.client_id, 0u);
    EXPECT_TRUE(second_me.has_user_id);
    EXPECT_EQ(second_me.has_user_id, first_me.has_user_id);
    EXPECT_EQ(second_me.user_id, first_me.user_id);
    EXPECT_EQ(static_cast<std::string>(first_me.transport), "TCP");
    EXPECT_EQ(static_cast<std::string>(second_me.transport), "TCP");
    EXPECT_EQ(static_cast<std::string>(second_me.transport), static_cast<std::string>(first_me.transport));
    EXPECT_FALSE(static_cast<std::string>(second_me.address).empty());
    EXPECT_EQ(second_me.consumer_groups_count, 0u);
    EXPECT_TRUE(second_me.consumer_groups.empty());
}

TEST_F(LowLevelE2E_Client, GetClientBeforeLoginThrows) {
    RecordProperty("description",
                   "Rejects get_client before connect, after connect but before login, and after disconnect.");
    iggy::ffi::Client *client = GetLoggedOutClient();

    ASSERT_THROW(client->get_client(1), std::exception);
    ASSERT_NO_THROW(client->connect());
    ASSERT_THROW(client->get_client(1), std::exception);
    ASSERT_NO_THROW(client->login_user("iggy", "iggy"));
    ASSERT_NO_THROW(client->disconnect());
    ASSERT_THROW(client->get_client(1), std::exception);
}

TEST_F(LowLevelE2E_Client, GetClientWithWrongClientIdThrows) {
    RecordProperty("description", "Rejects querying invalid or non-existent client ids.");
    iggy::ffi::Client *client = GetLoggedInClient();

    std::uint32_t non_existent_client_id = 1u;
    ASSERT_NO_THROW({
        const auto clients = client->get_clients();
        std::unordered_set<std::uint32_t> client_ids;
        for (const auto &entry : clients) {
            client_ids.insert(entry.client_id);
        }

        while (client_ids.find(non_existent_client_id) != client_ids.end()) {
            ++non_existent_client_id;
        }
    });

    const std::uint32_t wrong_client_ids[] = {0u, non_existent_client_id};
    for (const std::uint32_t wrong_client_id : wrong_client_ids) {
        SCOPED_TRACE(wrong_client_id);
        ASSERT_THROW(client->get_client(wrong_client_id), std::exception);
    }
}

TEST_F(LowLevelE2E_Client, GetClientReturnsDetailsForMatchingClientId) {
    RecordProperty("description", "Returns current client details when querying with the authenticated client id.");
    iggy::ffi::Client *client = GetLoggedInClient();

    iggy::ffi::ClientInfoDetails current_client{};
    iggy::ffi::ClientInfoDetails looked_up_client{};
    ASSERT_NO_THROW({
        current_client   = client->get_me();
        looked_up_client = client->get_client(current_client.client_id);
    });

    EXPECT_NE(current_client.client_id, 0u);
    EXPECT_TRUE(current_client.has_user_id);
    EXPECT_TRUE(looked_up_client.has_user_id);
    EXPECT_EQ(looked_up_client.client_id, current_client.client_id);
    EXPECT_EQ(looked_up_client.has_user_id, current_client.has_user_id);
    EXPECT_EQ(looked_up_client.user_id, current_client.user_id);
    EXPECT_EQ(static_cast<std::string>(looked_up_client.address), static_cast<std::string>(current_client.address));
    EXPECT_EQ(static_cast<std::string>(looked_up_client.transport), "TCP");
    EXPECT_EQ(static_cast<std::string>(looked_up_client.transport), static_cast<std::string>(current_client.transport));
    EXPECT_EQ(looked_up_client.consumer_groups_count, current_client.consumer_groups_count);
    EXPECT_EQ(looked_up_client.consumer_groups.size(), current_client.consumer_groups.size());
}

TEST_F(LowLevelE2E_Client, GetClientIsStableAcrossBackToBackCalls) {
    RecordProperty("description", "Returns stable client details across back-to-back get_client calls.");
    iggy::ffi::Client *client = GetLoggedInClient();

    iggy::ffi::ClientInfoDetails current_client{};
    iggy::ffi::ClientInfoDetails first_lookup{};
    iggy::ffi::ClientInfoDetails second_lookup{};
    ASSERT_NO_THROW({
        current_client = client->get_me();
        first_lookup   = client->get_client(current_client.client_id);
        second_lookup  = client->get_client(current_client.client_id);
    });

    EXPECT_NE(current_client.client_id, 0u);
    EXPECT_TRUE(current_client.has_user_id);
    EXPECT_TRUE(first_lookup.has_user_id);
    EXPECT_TRUE(second_lookup.has_user_id);
    EXPECT_EQ(first_lookup.client_id, current_client.client_id);
    EXPECT_EQ(second_lookup.client_id, first_lookup.client_id);
    EXPECT_EQ(first_lookup.has_user_id, current_client.has_user_id);
    EXPECT_EQ(second_lookup.has_user_id, first_lookup.has_user_id);
    EXPECT_EQ(second_lookup.user_id, first_lookup.user_id);
    EXPECT_EQ(static_cast<std::string>(second_lookup.address), static_cast<std::string>(first_lookup.address));
    EXPECT_EQ(static_cast<std::string>(first_lookup.transport), "TCP");
    EXPECT_EQ(static_cast<std::string>(second_lookup.transport), "TCP");
    EXPECT_EQ(static_cast<std::string>(second_lookup.transport), static_cast<std::string>(first_lookup.transport));
    EXPECT_EQ(second_lookup.consumer_groups_count, first_lookup.consumer_groups_count);
    EXPECT_EQ(second_lookup.consumer_groups.size(), first_lookup.consumer_groups.size());
}

TEST_F(LowLevelE2E_Client, GetClientsBeforeLoginThrows) {
    RecordProperty("description",
                   "Rejects get_clients before connect, after connect but before login, and after disconnect.");
    iggy::ffi::Client *client = GetLoggedOutClient();

    ASSERT_THROW(client->get_clients(), std::exception);
    ASSERT_NO_THROW(client->connect());
    ASSERT_THROW(client->get_clients(), std::exception);
    ASSERT_NO_THROW(client->login_user("iggy", "iggy"));
    ASSERT_NO_THROW(client->disconnect());
    ASSERT_THROW(client->get_clients(), std::exception);
}

TEST_F(LowLevelE2E_Client, GetClientsReturnsActiveClientSessions) {
    RecordProperty("description", "Returns the currently active authenticated client sessions.");
    iggy::ffi::Client *first_client  = GetLoggedInClient();
    iggy::ffi::Client *second_client = GetLoggedInClient();

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
            EXPECT_EQ(client.has_user_id, first_me.has_user_id);
            EXPECT_EQ(client.user_id, first_me.user_id);
            EXPECT_EQ(static_cast<std::string>(client.address), static_cast<std::string>(first_me.address));
            EXPECT_EQ(client.consumer_groups_count, first_me.consumer_groups_count);
        }

        if (client.client_id == second_me.client_id) {
            found_second = true;
            EXPECT_EQ(client.has_user_id, second_me.has_user_id);
            EXPECT_EQ(client.user_id, second_me.user_id);
            EXPECT_EQ(static_cast<std::string>(client.address), static_cast<std::string>(second_me.address));
            EXPECT_EQ(client.consumer_groups_count, second_me.consumer_groups_count);
        }
    }

    EXPECT_TRUE(found_first);
    EXPECT_TRUE(found_second);
}

TEST_F(LowLevelE2E_Client, GetClientsIsStableAcrossBackToBackCalls) {
    RecordProperty("description", "Returns stable client lists across back-to-back get_clients calls.");
    iggy::ffi::Client *first_client  = GetLoggedInClient();
    iggy::ffi::Client *second_client = GetLoggedInClient();
    ASSERT_NE(second_client, nullptr);

    iggy::ffi::ClientInfoDetails first_me{};
    iggy::ffi::ClientInfoDetails second_me{};
    rust::Vec<iggy::ffi::ClientInfo> first_clients;
    rust::Vec<iggy::ffi::ClientInfo> second_clients;
    ASSERT_NO_THROW({
        first_me       = first_client->get_me();
        second_me      = second_client->get_me();
        first_clients  = first_client->get_clients();
        second_clients = first_client->get_clients();
    });

    ASSERT_GE(first_clients.size(), 2u);
    ASSERT_GE(second_clients.size(), 2u);

    const auto expect_entry_matches = [](const rust::Vec<iggy::ffi::ClientInfo> &clients,
                                         const iggy::ffi::ClientInfoDetails &expected) {
        bool found = false;
        for (const auto &entry : clients) {
            if (entry.client_id != expected.client_id) {
                continue;
            }

            found = true;
            EXPECT_EQ(entry.has_user_id, expected.has_user_id);
            EXPECT_EQ(entry.user_id, expected.user_id);
            EXPECT_EQ(static_cast<std::string>(entry.address), static_cast<std::string>(expected.address));
            EXPECT_EQ(static_cast<std::string>(entry.transport), static_cast<std::string>(expected.transport));
            EXPECT_EQ(entry.consumer_groups_count, expected.consumer_groups_count);
            break;
        }

        EXPECT_TRUE(found);
    };

    expect_entry_matches(first_clients, first_me);
    expect_entry_matches(first_clients, second_me);
    expect_entry_matches(second_clients, first_me);
    expect_entry_matches(second_clients, second_me);
}

TEST_F(LowLevelE2E_Client, GetClientsMatchesGetClientForReturnedIds) {
    RecordProperty("description", "Returns list entries that agree with get_client for each returned client id.");
    iggy::ffi::Client *first_client  = GetLoggedInClient();
    iggy::ffi::Client *second_client = GetLoggedInClient();
    ASSERT_NE(second_client, nullptr);

    rust::Vec<iggy::ffi::ClientInfo> clients;
    ASSERT_NO_THROW({ clients = first_client->get_clients(); });
    ASSERT_GE(clients.size(), 2u);

    for (const auto &client : clients) {
        SCOPED_TRACE(client.client_id);
        iggy::ffi::ClientInfoDetails details{};
        ASSERT_NO_THROW({ details = first_client->get_client(client.client_id); });

        EXPECT_EQ(details.client_id, client.client_id);
        EXPECT_EQ(details.has_user_id, client.has_user_id);
        EXPECT_EQ(details.user_id, client.user_id);
        EXPECT_EQ(static_cast<std::string>(details.address), static_cast<std::string>(client.address));
        EXPECT_EQ(static_cast<std::string>(details.transport), static_cast<std::string>(client.transport));
        EXPECT_EQ(details.consumer_groups_count, client.consumer_groups_count);
    }
}

TEST_F(LowLevelE2E_Client, GetClientsReflectsAdditionalSession) {
    RecordProperty("description", "Reflects a newly added authenticated session in subsequent get_clients results.");
    iggy::ffi::Client *first_client = GetLoggedInClient();

    rust::Vec<iggy::ffi::ClientInfo> clients_before;
    ASSERT_NO_THROW({ clients_before = first_client->get_clients(); });

    iggy::ffi::Client *second_client = GetLoggedInClient();

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
        EXPECT_EQ(client.has_user_id, second_me.has_user_id);
        EXPECT_EQ(client.user_id, second_me.user_id);
        EXPECT_EQ(static_cast<std::string>(client.address), static_cast<std::string>(second_me.address));
        EXPECT_EQ(static_cast<std::string>(client.transport), "TCP");
        EXPECT_EQ(client.consumer_groups_count, second_me.consumer_groups_count);
        break;
    }
    EXPECT_TRUE(found_after);
}

TEST_F(LowLevelE2E_Client, PingSucceedsForNewConnection) {
    RecordProperty("description", "Successfully pings the server from a fresh unauthenticated client session.");
    iggy::ffi::Client *client = GetLoggedOutClient();

    ASSERT_NO_THROW(client->ping());
}

TEST_F(LowLevelE2E_Client, HeartbeatIntervalReturnsDefaultValueForNewConnection) {
    RecordProperty("description",
                   "Returns the default heartbeat interval in microseconds for a fresh unauthenticated client.");
    constexpr std::uint64_t default_heartbeat_micros = 5'000'000ull;
    iggy::ffi::Client *client                        = GetLoggedOutClient();

    const auto heartbeat_interval = client->heartbeat_interval();
    EXPECT_EQ(heartbeat_interval, default_heartbeat_micros);
}

TEST_F(LowLevelE2E_Client, HeartbeatIntervalReturnsConfiguredValueFromConnectionString) {
    RecordProperty("description",
                   "Returns the configured heartbeat interval in microseconds from the connection string.");
    constexpr std::uint64_t configured_heartbeat_micros = 10'000'000ull;
    iggy::ffi::Client *client                           = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection("iggy://iggy:iggy@127.0.0.1:8090?heartbeat_interval=10s"); });
    ASSERT_NE(client, nullptr);
    TrackClient(client);

    const auto heartbeat_interval = client->heartbeat_interval();
    EXPECT_EQ(heartbeat_interval, configured_heartbeat_micros);
}

TEST_F(LowLevelE2E_Client, SnapshotBeforeLoginThrows) {
    RecordProperty("description",
                   "Rejects snapshot before connect, after connect but before login, and after disconnect.");
    iggy::ffi::Client *client = GetLoggedOutClient();

    ASSERT_THROW(client->snapshot("deflated", make_snapshot_types({"test"})), std::exception);

    ASSERT_NO_THROW(client->connect());
    ASSERT_THROW(client->snapshot("deflated", make_snapshot_types({"test"})), std::exception);
    ASSERT_NO_THROW(client->login_user("iggy", "iggy"));
    ASSERT_NO_THROW(client->disconnect());
    ASSERT_THROW(client->snapshot("deflated", make_snapshot_types({"test"})), std::exception);
}

TEST_F(LowLevelE2E_Client, SnapshotAllCombinedWithOtherTypeThrows) {
    RecordProperty("description", "Rejects combining the all snapshot type with any other snapshot type.");
    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_THROW(client->snapshot("deflated", make_snapshot_types({"all", "test"})), std::exception);
}

TEST_F(LowLevelE2E_Client, SnapshotWithEmptySnapshotTypesThrows) {
    RecordProperty("description", "Rejects an empty snapshot type list in the wrapper before sending.");
    iggy::ffi::Client *client = GetLoggedInClient();

    rust::Vec<rust::String> snapshot_types;
    ASSERT_THROW(client->snapshot("deflated", snapshot_types), std::exception);
}

TEST_F(LowLevelE2E_Client, SnapshotReturnsNonEmptyBytes) {
    RecordProperty("description", "Returns a non-empty snapshot for a valid compression and snapshot type.");
    iggy::ffi::Client *client = GetLoggedInClient();

    rust::Vec<std::uint8_t> snapshot_bytes;
    ASSERT_NO_THROW({ snapshot_bytes = client->snapshot("deflated", make_snapshot_types({"test"})); });
    EXPECT_FALSE(snapshot_bytes.empty());
}

TEST_F(LowLevelE2E_Client, SnapshotWithInvalidCompressionThrows) {
    RecordProperty("description",
                   "Rejects empty or invalid snapshot compression values in the wrapper before sending.");
    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_THROW(client->snapshot("", make_snapshot_types({"test"})), std::exception);
    ASSERT_THROW(client->snapshot("invalid-compression", make_snapshot_types({"test"})), std::exception);
}

TEST_F(LowLevelE2E_Client, SnapshotWithInvalidSnapshotTypeThrows) {
    RecordProperty("description", "Rejects invalid snapshot type values in the wrapper before sending.");
    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_THROW(client->snapshot("deflated", make_snapshot_types({"not-a-real-type"})), std::exception);
}
