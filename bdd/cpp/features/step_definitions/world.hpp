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
#include <string>
#include <vector>

#include "lib.rs.h"

namespace bdd {

// Plain-C++ snapshot of a poll result so the scenario context never stores cxx types
// (rust::Vec) across steps.
struct PolledData {
    std::uint32_t count = 0;
    std::vector<std::uint64_t> offsets;
    std::vector<std::uint64_t> id_los;
    std::vector<std::string> payloads;
};

// Scenario-scoped state shared across cucumber-cpp steps. cucumber::ScenarioScope creates
// one instance per scenario (via a shared_ptr) and releases it when the scenario ends, so
// the destructor owns closing the Iggy connection. Copying is disabled to keep that single
// ownership unambiguous.
struct GlobalContext {
    iggy::ffi::Client *client     = nullptr;
    std::uint64_t last_sent_id_lo = 0;
    std::string last_sent_payload;
    PolledData polled;

    GlobalContext()                                 = default;
    GlobalContext(const GlobalContext &)            = delete;
    GlobalContext &operator=(const GlobalContext &) = delete;

    ~GlobalContext() {
        if (client != nullptr) {
            try {
                iggy::ffi::delete_client(client);
            } catch (...) {
            }
            client = nullptr;
        }
    }
};

inline iggy::ffi::Identifier make_string_identifier(const std::string &value) {
    iggy::ffi::Identifier identifier;
    identifier.set_string(value);
    return identifier;
}

inline iggy::ffi::Identifier make_numeric_identifier(const std::uint32_t value) {
    iggy::ffi::Identifier identifier;
    identifier.set_numeric(value);
    return identifier;
}

inline rust::Vec<std::uint8_t> to_payload(const std::string &value) {
    rust::Vec<std::uint8_t> bytes;
    for (const char c : value) {
        bytes.push_back(static_cast<std::uint8_t>(c));
    }
    return bytes;
}

inline std::string payload_to_string(const rust::Vec<std::uint8_t> &payload) {
    std::string value;
    value.reserve(payload.size());
    for (const std::uint8_t byte : payload) {
        value.push_back(static_cast<char>(byte));
    }
    return value;
}

inline rust::Vec<std::uint8_t> partition_id_bytes(std::uint32_t id) {
    rust::Vec<std::uint8_t> bytes;
    bytes.push_back(static_cast<std::uint8_t>(id & 0xFF));
    bytes.push_back(static_cast<std::uint8_t>((id >> 8) & 0xFF));
    bytes.push_back(static_cast<std::uint8_t>((id >> 16) & 0xFF));
    bytes.push_back(static_cast<std::uint8_t>((id >> 24) & 0xFF));
    return bytes;
}

// Payload for message `index` (0-based), matching the convention shared with the other SDK
// BDD suites so the feature's "expected payload content" assertion stays consistent.
inline std::string expected_payload(std::uint32_t index) {
    return "test message " + std::to_string(index);
}

}  // namespace bdd
