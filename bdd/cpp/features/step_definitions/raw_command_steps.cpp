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

#define CUKE_OBJECT_PREFIX IggyBddRawCommand

#include <gtest/gtest.h>

#include <cucumber-cpp/autodetect.hpp>

#include <cstdint>
#include <exception>
#include <string>

#include "world.hpp"

WHEN("^I send a raw command with code ([0-9]+) and an empty payload$") {
    REGEX_PARAM(std::uint32_t, code);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    context->raw_response.clear();
    context->raw_error.clear();
    try {
        const auto response = context->client->send_binary_request(code, rust::Vec<std::uint8_t>());
        context->raw_response.assign(response.begin(), response.end());
    } catch (const std::exception &error) {
        context->raw_error = error.what();
    }
}

THEN("^the raw command should succeed with an empty response$") {
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    EXPECT_TRUE(context->raw_error.empty());
    EXPECT_TRUE(context->raw_response.empty());
}

THEN("^the raw command should succeed with a non-empty response$") {
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    EXPECT_TRUE(context->raw_error.empty());
    EXPECT_FALSE(context->raw_response.empty());
}

THEN("^the raw command should fail with an invalid command error$") {
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    EXPECT_TRUE(context->raw_response.empty());
    EXPECT_NE(context->raw_error.find("Invalid command"), std::string::npos);
}
