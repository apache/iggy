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
