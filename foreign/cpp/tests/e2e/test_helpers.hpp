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

#include <algorithm>
#include <cstdint>
#include <initializer_list>
#include <random>
#include <string>
#include <utility>

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

inline iggy::ffi::HeaderField make_header_field(const iggy::ffi::HeaderKind kind, rust::Vec<std::uint8_t> value) {
    iggy::ffi::HeaderField field;
    field.kind  = static_cast<std::uint8_t>(kind);
    field.value = std::move(value);
    return field;
}

inline iggy::ffi::HeaderEntry make_header_entry(iggy::ffi::HeaderField key, iggy::ffi::HeaderField value) {
    iggy::ffi::HeaderEntry entry;
    entry.key   = std::move(key);
    entry.value = std::move(value);
    return entry;
}

inline bool has_header(const rust::Vec<iggy::ffi::HeaderEntry> &headers,
                       const std::uint8_t key_kind,
                       const rust::Vec<std::uint8_t> &key_value,
                       const std::uint8_t value_kind,
                       const rust::Vec<std::uint8_t> &value_value) {
    for (const auto &header : headers) {
        if (header.key.kind == key_kind && header.value.kind == value_kind &&
            header.key.value.size() == key_value.size() &&
            std::equal(header.key.value.begin(), header.key.value.end(), key_value.begin()) &&
            header.value.value.size() == value_value.size() &&
            std::equal(header.value.value.begin(), header.value.value.end(), value_value.begin())) {
            return true;
        }
    }

    return false;
}

struct TrackedConsumerGroup {
    std::string stream_name;
    std::string topic_name;
    std::string group_name;
};

class E2ETestFixture : public ::testing::Test {
  public:
    ~E2ETestFixture() { CleanupBestEffort(); }
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

    std::string GetRandomName(const std::size_t max_length = 255) {
        if (max_length == 0) {
            return {};
        }

        static constexpr char alphabet[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        static thread_local std::mt19937 generator(std::random_device{}());
        const std::size_t min_length = std::min<std::size_t>(8, max_length);
        std::uniform_int_distribution<std::size_t> length_distribution(min_length, max_length);
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

    iggy::ffi::UserInfoDetails CreateUser(iggy::ffi::Client *client,
                                          const std::string &username,
                                          const std::string &password,
                                          const std::uint8_t status,
                                          const bool has_permissions         = false,
                                          iggy::ffi::Permissions permissions = {}) {
        auto user = client->create_user(username, password, status, has_permissions, std::move(permissions));
        tracked_user_names_.push_back(username);
        return user;
    }

    void ForgetUser(const std::string &username) {
        tracked_user_names_.erase(std::remove(tracked_user_names_.begin(), tracked_user_names_.end(), username),
                                  tracked_user_names_.end());
    }

    void TrackStream(const std::string &stream_name) { tracked_stream_names_.push_back(stream_name); }
    void TrackStream(const std::uint32_t stream_id) { tracked_stream_ids_.push_back(stream_id); }
    void TrackConsumerGroup(const std::string &stream_name,
                            const std::string &topic_name,
                            const std::string &group_name) {
        tracked_consumer_groups_.push_back({stream_name, topic_name, group_name});
    }
    void ForgetTrackedStream(const std::string &stream_name) {
        tracked_stream_names_.erase(
            std::remove(tracked_stream_names_.begin(), tracked_stream_names_.end(), stream_name),
            tracked_stream_names_.end());
        for (std::size_t i = 0; i < tracked_consumer_groups_.size();) {
            const auto &group = tracked_consumer_groups_[i];
            if (group.stream_name == stream_name) {
                ForgetTrackedConsumerGroup(group.stream_name, group.topic_name, group.group_name);
                continue;
            }
            ++i;
        }
    }
    void ForgetTrackedStream(const std::uint32_t stream_id) {
        tracked_stream_ids_.erase(std::remove(tracked_stream_ids_.begin(), tracked_stream_ids_.end(), stream_id),
                                  tracked_stream_ids_.end());
    }
    void ForgetTrackedConsumerGroup(const std::string &stream_name,
                                    const std::string &topic_name,
                                    const std::string &group_name) {
        for (std::size_t i = 0; i < tracked_consumer_groups_.size();) {
            const auto &tracked_group = tracked_consumer_groups_[i];
            if (tracked_group.stream_name == stream_name && tracked_group.topic_name == topic_name &&
                tracked_group.group_name == group_name) {
                tracked_consumer_groups_.erase(tracked_consumer_groups_.begin() + i);
                continue;
            }
            ++i;
        }
    }

    void DeleteClient(iggy::ffi::Client *&client) {
        iggy::ffi::Client *client_to_delete = client;
        client                              = nullptr;
        ForgetClient(client_to_delete);
        EXPECT_NO_THROW(iggy::ffi::delete_client(client_to_delete));
    }

    void Cleanup() {
        CleanupConsumerGroups();
        CleanupStreams();
        CleanupClients();
        CleanupUsers();
    }

  private:
    void CleanupBestEffort() noexcept {
        CleanupConsumerGroupsBestEffort();
        CleanupStreamsBestEffort();
        CleanupClientsBestEffort();
        CleanupUsersBestEffort();
    }

    void ForgetClient(iggy::ffi::Client *client) {
        const auto found = std::find(clients_.begin(), clients_.end(), client);
        if (found != clients_.end()) {
            *found = nullptr;
        }
    }

    void CleanupUsers() {
        if (tracked_user_names_.empty()) {
            return;
        }

        iggy::ffi::Client *cleanup_client = nullptr;
        EXPECT_NO_THROW({ cleanup_client = iggy::ffi::new_connection(""); });
        EXPECT_NE(cleanup_client, nullptr);
        if (cleanup_client != nullptr) {
            EXPECT_NO_THROW(cleanup_client->connect());
            EXPECT_NO_THROW(cleanup_client->login_user("iggy", "iggy"));
            for (const auto &username : tracked_user_names_) {
                EXPECT_NO_THROW(cleanup_client->delete_user(make_string_identifier(username)));
            }
            EXPECT_NO_THROW(iggy::ffi::delete_client(cleanup_client));
        }

        tracked_user_names_.clear();
    }

    void CleanupUsersBestEffort() noexcept {
        if (tracked_user_names_.empty()) {
            return;
        }

        iggy::ffi::Client *cleanup_client = nullptr;
        try {
            cleanup_client = iggy::ffi::new_connection("");
            if (cleanup_client != nullptr) {
                cleanup_client->connect();
                cleanup_client->login_user("iggy", "iggy");
                for (const auto &username : tracked_user_names_) {
                    cleanup_client->delete_user(make_string_identifier(username));
                }
            }
        } catch (...) {
        }

        if (cleanup_client != nullptr) {
            try {
                iggy::ffi::delete_client(cleanup_client);
            } catch (...) {
            }
        }

        tracked_user_names_.clear();
    }

    void CleanupStreams() {
        if (tracked_stream_names_.empty() && tracked_stream_ids_.empty()) {
            return;
        }

        iggy::ffi::Client *cleanup_client = nullptr;
        EXPECT_NO_THROW({ cleanup_client = iggy::ffi::new_connection(""); });
        EXPECT_NE(cleanup_client, nullptr);
        if (cleanup_client != nullptr) {
            EXPECT_NO_THROW(cleanup_client->connect());
            EXPECT_NO_THROW(cleanup_client->login_user("iggy", "iggy"));
            for (const auto &stream_name : tracked_stream_names_) {
                EXPECT_NO_THROW(cleanup_client->delete_stream(make_string_identifier(stream_name)));
            }
            for (const auto stream_id : tracked_stream_ids_) {
                EXPECT_NO_THROW(cleanup_client->delete_stream(make_numeric_identifier(stream_id)));
            }
            EXPECT_NO_THROW(iggy::ffi::delete_client(cleanup_client));
        }

        tracked_stream_names_.clear();
        tracked_stream_ids_.clear();
    }

    void CleanupConsumerGroups() {
        if (tracked_consumer_groups_.empty()) {
            return;
        }

        iggy::ffi::Client *cleanup_client = nullptr;
        EXPECT_NO_THROW({ cleanup_client = iggy::ffi::new_connection(""); });
        EXPECT_NE(cleanup_client, nullptr);
        if (cleanup_client != nullptr) {
            EXPECT_NO_THROW(cleanup_client->connect());
            EXPECT_NO_THROW(cleanup_client->login_user("iggy", "iggy"));
            for (const auto &group : tracked_consumer_groups_) {
                EXPECT_NO_THROW(cleanup_client->delete_consumer_group(make_string_identifier(group.stream_name),
                                                                      make_string_identifier(group.topic_name),
                                                                      make_string_identifier(group.group_name)));
            }
            EXPECT_NO_THROW(iggy::ffi::delete_client(cleanup_client));
        }

        tracked_consumer_groups_.clear();
    }

    void CleanupStreamsBestEffort() noexcept {
        if (tracked_stream_names_.empty() && tracked_stream_ids_.empty()) {
            return;
        }

        iggy::ffi::Client *cleanup_client = nullptr;
        try {
            cleanup_client = iggy::ffi::new_connection("");
            if (cleanup_client != nullptr) {
                cleanup_client->connect();
                cleanup_client->login_user("iggy", "iggy");
                for (const auto &stream_name : tracked_stream_names_) {
                    cleanup_client->delete_stream(make_string_identifier(stream_name));
                }
                for (const auto stream_id : tracked_stream_ids_) {
                    cleanup_client->delete_stream(make_numeric_identifier(stream_id));
                }
            }
        } catch (...) {
        }

        if (cleanup_client != nullptr) {
            try {
                iggy::ffi::delete_client(cleanup_client);
            } catch (...) {
            }
        }

        tracked_stream_names_.clear();
        tracked_stream_ids_.clear();
    }

    void CleanupConsumerGroupsBestEffort() noexcept {
        if (tracked_consumer_groups_.empty()) {
            return;
        }

        iggy::ffi::Client *cleanup_client = nullptr;
        try {
            cleanup_client = iggy::ffi::new_connection("");
            if (cleanup_client != nullptr) {
                cleanup_client->connect();
                cleanup_client->login_user("iggy", "iggy");
                for (const auto &group : tracked_consumer_groups_) {
                    cleanup_client->delete_consumer_group(make_string_identifier(group.stream_name),
                                                          make_string_identifier(group.topic_name),
                                                          make_string_identifier(group.group_name));
                }
            }
        } catch (...) {
        }

        if (cleanup_client != nullptr) {
            try {
                iggy::ffi::delete_client(cleanup_client);
            } catch (...) {
            }
        }

        tracked_consumer_groups_.clear();
    }

    void CleanupClients() {
        for (iggy::ffi::Client *&client : clients_) {
            iggy::ffi::Client *client_to_delete = client;
            client                              = nullptr;
            EXPECT_NO_THROW(iggy::ffi::delete_client(client_to_delete));
        }
        clients_.clear();
    }

    void CleanupClientsBestEffort() noexcept {
        for (iggy::ffi::Client *&client : clients_) {
            iggy::ffi::Client *client_to_delete = client;
            client                              = nullptr;
            try {
                iggy::ffi::delete_client(client_to_delete);
            } catch (...) {
            }
        }
        clients_.clear();
    }

  protected:
    std::vector<iggy::ffi::Client *> clients_;
    std::vector<std::string> tracked_user_names_;
    std::vector<std::string> tracked_stream_names_;
    std::vector<std::uint32_t> tracked_stream_ids_;
    std::vector<TrackedConsumerGroup> tracked_consumer_groups_;
};
