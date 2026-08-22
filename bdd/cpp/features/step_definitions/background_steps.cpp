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

// cucumber-cpp names its generated step classes CukeObject<__COUNTER__>, and the counter
// restarts in every translation unit. A unique prefix per step file keeps those names from
// colliding when several step files are linked into one wire server.
#define CUKE_OBJECT_PREFIX IggyBddBackground

#include <gtest/gtest.h>

#include <cucumber-cpp/autodetect.hpp>

#include <cstdlib>
#include <stdexcept>
#include <string>

#include "world.hpp"

namespace {
std::string required_env(const char *name) {
    const char *value = std::getenv(name);
    if (value == nullptr || *value == '\0') {
        throw std::runtime_error(std::string(name) +
                                 " must be set; run the suite via scripts/run-bdd-tests.sh");
    }
    return std::string(value);
}
}  // namespace

GIVEN("^I have a running Iggy server$") {
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    const std::string address = required_env("IGGY_TCP_ADDRESS");
    iggy::ffi::Client *client = iggy::ffi::new_connection(address);
    ASSERT_NE(client, nullptr);
    context->client = client;
    context->client->connect();
}

GIVEN("^I am authenticated as the root user$") {
    cucumber::ScenarioScope<bdd::GlobalContext> context;
    ASSERT_NE(context->client, nullptr);

    const std::string username = required_env("IGGY_ROOT_USERNAME");
    const std::string password = required_env("IGGY_ROOT_PASSWORD");
    context->client->login_user(username, password);
}
