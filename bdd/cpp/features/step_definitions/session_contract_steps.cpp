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

#define CUKE_OBJECT_PREFIX IggyBddSessionContract

#include <gtest/gtest.h>

#include <cucumber-cpp/autodetect.hpp>

#include <cstdint>
#include <exception>
#include <map>
#include <string>
#include <utility>

#include "world.hpp"

namespace {
// The bridge reports no error code for reads, so the check reads the error text.
const std::map<std::uint32_t, std::string> ERROR_MESSAGES = {
    {40, "Unauthenticated"},
    {58, "Request transiently not accepted"},
};
}  // namespace

GIVEN("^I have a cluster node that cannot reach quorum$") {
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    iggy::ffi::IggyClientConfig config{};
    config.server_address     = bdd::required_env("IGGY_TCP_ADDRESS_NO_QUORUM");
    iggy::ffi::Client *client = iggy::ffi::new_connection(std::move(config));
    ASSERT_NE(client, nullptr);
    context->client = client;
    context->client->connect();
}

WHEN("^I sign out$") {
    iggy::ffi::ErrorInfo error_info{};
    cucumber::ScenarioScope<bdd::GlobalContext> context;
    ASSERT_NE(context->client, nullptr);

    context->client->logout_user(error_info);
}

WHEN("^I request the stream with ID ([0-9]+)$") {
    REGEX_PARAM(std::uint32_t, stream_id);
    cucumber::ScenarioScope<bdd::GlobalContext> context;
    ASSERT_NE(context->client, nullptr);

    context->call_error.clear();
    try {
        context->client->get_stream(bdd::make_numeric_identifier(stream_id));
    } catch (const std::exception &error) {
        context->call_error = error.what();
    }
}

WHEN("^I sign in to that node as the root user$") {
    iggy::ffi::ErrorInfo error_info{};
    cucumber::ScenarioScope<bdd::GlobalContext> context;
    ASSERT_NE(context->client, nullptr);

    context->call_error.clear();
    try {
        context->client->login_user(bdd::required_env("IGGY_ROOT_USERNAME"), bdd::required_env("IGGY_ROOT_PASSWORD"),
                                    error_info);
    } catch (const std::exception &error) {
        context->call_error = error.what();
    }
}

THEN("^the call should fail with error code ([0-9]+)$") {
    REGEX_PARAM(std::uint32_t, code);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    EXPECT_NE(context->call_error.find(ERROR_MESSAGES.at(code)), std::string::npos) << context->call_error;
}
