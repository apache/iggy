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
#include <string>

#include "world.hpp"

namespace {
std::string env_or(const char *name, const std::string &fallback) {
    const char *value = std::getenv(name);
    return value != nullptr ? std::string(value) : fallback;
}
}  // namespace

GIVEN("^I have a running Iggy server$") {
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    // Empty address makes the SDK fall back to its default TCP endpoint; in CI the address
    // is supplied via IGGY_TCP_ADDRESS (e.g. iggy-server:8090).
    const std::string address = env_or("IGGY_TCP_ADDRESS", "");
    iggy::ffi::Client *client = iggy::ffi::new_connection(address);
    ASSERT_NE(client, nullptr);
    context->client = client;
    context->client->connect();
}

GIVEN("^I am authenticated as the root user$") {
    cucumber::ScenarioScope<bdd::GlobalContext> context;
    ASSERT_NE(context->client, nullptr);

    const std::string username = env_or("IGGY_ROOT_USERNAME", "iggy");
    const std::string password = env_or("IGGY_ROOT_PASSWORD", "iggy");
    context->client->login_user(username, password);
}
