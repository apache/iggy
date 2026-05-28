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

#include <algorithm>
#include <cstdint>
#include <initializer_list>
#include <random>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "lib.rs.h"

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

inline rust::Vec<std::uint8_t> to_payload(const std::string &s) {
    rust::Vec<std::uint8_t> v;
    for (const char c : s) {
        v.push_back(static_cast<std::uint8_t>(c));
    }
    return v;
}

inline rust::Vec<std::uint8_t> partition_id_bytes(std::uint32_t id) {
    rust::Vec<std::uint8_t> v;
    v.push_back(static_cast<std::uint8_t>(id & 0xFF));
    v.push_back(static_cast<std::uint8_t>((id >> 8) & 0xFF));
    v.push_back(static_cast<std::uint8_t>((id >> 16) & 0xFF));
    v.push_back(static_cast<std::uint8_t>((id >> 24) & 0xFF));
    return v;
}

inline rust::Vec<rust::String> make_snapshot_types(std::initializer_list<const char *> values) {
    rust::Vec<rust::String> snapshot_types;
    for (const auto value : values) {
        snapshot_types.push_back(value);
    }
    return snapshot_types;
}
class E2ETestFixture : public ::testing::Test {
  public:
    ~E2ETestFixture() { Cleanup(); }
    void TearDown() override { Cleanup(); }

  protected:
    void TrackClient(iggy::ffi::Client *client) {
        ASSERT_NE(client, nullptr);
        clients_.push_back(client);
    }

    iggy::ffi::Client *GetLoggedOutClient() {
        iggy::ffi::Client *client = nullptr;
        EXPECT_NO_THROW({ client = iggy::ffi::new_connection(""); });
        EXPECT_NE(client, nullptr);
        if (client == nullptr) {
            return nullptr;
        }

        TrackClient(client);
        return client;
    }

    iggy::ffi::Client *GetLoggedInClient() {
        iggy::ffi::Client *client = GetLoggedOutClient();
        if (client == nullptr) {
            return nullptr;
        }

        EXPECT_NO_THROW(client->connect());
        EXPECT_NO_THROW(client->login_user("iggy", "iggy"));

        return client;
    }

    std::string GetRandomName() {
        static constexpr char alphabet[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        static thread_local std::mt19937 generator(std::random_device{}());
        std::uniform_int_distribution<std::size_t> length_distribution(8, 255);
        std::uniform_int_distribution<std::size_t> distribution(0, sizeof(alphabet) - 2);
        const std::size_t length = length_distribution(generator);

        std::string name;
        name.reserve(length);
        name.push_back('a');
        for (std::size_t i = 1; i < length; ++i) {
            name.push_back(alphabet[distribution(generator)]);
        }

        return name;
    }

    void TrackStream(const std::string &stream_name) { tracked_stream_names_.push_back(stream_name); }
    void TrackStream(const std::uint32_t stream_id) { tracked_stream_ids_.push_back(stream_id); }
    void ForgetTrackedStream(const std::string &stream_name) {
        tracked_stream_names_.erase(
            std::remove(tracked_stream_names_.begin(), tracked_stream_names_.end(), stream_name),
            tracked_stream_names_.end());
    }
    void ForgetTrackedStream(const std::uint32_t stream_id) {
        tracked_stream_ids_.erase(std::remove(tracked_stream_ids_.begin(), tracked_stream_ids_.end(), stream_id),
                                  tracked_stream_ids_.end());
    }

    void DeleteClient(iggy::ffi::Client *&client) {
        ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
        ForgetClient(client);
        client = nullptr;
    }

    void Cleanup() {
        CleanupStreams();
        CleanupClients();
    }

  private:
    void ForgetClient(iggy::ffi::Client *client) {
        const auto found = std::find(clients_.begin(), clients_.end(), client);
        if (found != clients_.end()) {
            *found = nullptr;
        }
    }

    void CleanupStreams() {
        if (tracked_stream_names_.empty() && tracked_stream_ids_.empty()) {
            return;
        }

        iggy::ffi::Client *cleanup_client = nullptr;
        ASSERT_NO_THROW({ cleanup_client = iggy::ffi::new_connection(""); });
        ASSERT_NE(cleanup_client, nullptr);
        ASSERT_NO_THROW(cleanup_client->connect());
        ASSERT_NO_THROW(cleanup_client->login_user("iggy", "iggy"));
        for (const auto &stream_name : tracked_stream_names_) {
            ASSERT_NO_THROW(cleanup_client->delete_stream(make_string_identifier(stream_name)));
        }
        for (const auto stream_id : tracked_stream_ids_) {
            ASSERT_NO_THROW(cleanup_client->delete_stream(make_numeric_identifier(stream_id)));
        }
        ASSERT_NO_THROW(iggy::ffi::delete_connection(cleanup_client));

        tracked_stream_names_.clear();
        tracked_stream_ids_.clear();
    }

    void CleanupClients() {
        for (iggy::ffi::Client *&client : clients_) {
            ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
            client = nullptr;
        }
        clients_.clear();
    }

    std::vector<iggy::ffi::Client *> clients_;
    std::vector<std::string> tracked_stream_names_;
    std::vector<std::uint32_t> tracked_stream_ids_;
};
