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

#pragma once

#include <cstdint>
#include <string>

#include "lib.rs.h"

inline iggy::ffi::Identifier make_string_identifier(const std::string &value) {
    iggy::ffi::Identifier identifier;
    identifier.kind   = "string";
    identifier.length = static_cast<std::uint8_t>(value.size());
    for (const char c : value) {
        identifier.value.push_back(static_cast<std::uint8_t>(c));
    }
    return identifier;
}

inline iggy::ffi::Identifier make_numeric_identifier(const std::uint32_t value) {
    iggy::ffi::Identifier identifier;
    identifier.kind   = "numeric";
    identifier.length = 4;
    identifier.value.push_back(static_cast<std::uint8_t>(value & 0xFF));
    identifier.value.push_back(static_cast<std::uint8_t>((value >> 8) & 0xFF));
    identifier.value.push_back(static_cast<std::uint8_t>((value >> 16) & 0xFF));
    identifier.value.push_back(static_cast<std::uint8_t>((value >> 24) & 0xFF));
    return identifier;
}

inline iggy::ffi::Client *login_to_server() {
    iggy::ffi::Client *client = iggy::ffi::new_connection("");
    client->connect();
    client->login_user("iggy", "iggy");
    return client;
}
