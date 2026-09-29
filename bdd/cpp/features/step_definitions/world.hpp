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

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "iggy.hpp"

namespace bdd {

// Plain-C++ snapshot of a poll result so the scenario context never stores FFI types
// across steps.
struct PolledData {
    std::uint32_t count = 0;
    std::vector<std::uint64_t> offsets;
    std::vector<std::uint64_t> id_los;
    std::vector<std::string> payloads;
};

// Scenario-scoped state shared across cucumber-cpp steps. cucumber::ScenarioScope creates
// one instance per scenario (via a shared_ptr) and releases it when the scenario ends, so
// the destructor owns closing the Iggy connection. The client is held by value in an
// optional: destroying the context disconnects, and moving is never needed.
struct GlobalContext {
    std::optional<iggy::IggyBlockingClient> client;
    std::uint64_t last_sent_id_lo = 0;
    std::string last_sent_payload;
    PolledData polled;
    std::vector<std::uint8_t> raw_response;
    std::string raw_error;
};

// Payload for message `index` (0-based), matching the convention shared with the other SDK
// BDD suites so the feature's "expected payload content" assertion stays consistent.
inline std::string expected_payload(std::uint32_t index) {
    return "test message " + std::to_string(index);
}

}  // namespace bdd
