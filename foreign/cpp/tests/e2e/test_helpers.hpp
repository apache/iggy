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
#include <random>
#include <string>
#include <utility>

#include <gtest/gtest.h>

#include "iggy.hpp"

struct TrackedConsumerGroup {
    std::string stream_name;
    std::string topic_name;
    std::string group_name;
};

class E2ETestFixture : public ::testing::Test {
  public:
    ~E2ETestFixture() override { CleanupBestEffort(); }
    void TearDown() override { Cleanup(); }

  protected:
    iggy::IggyBlockingClient GetLoggedOutHighLevelClient() { return iggy::IggyBlockingClient::Builder().Build(); }

    iggy::IggyBlockingClient GetLoggedInHighLevelClient(std::string username = "iggy", std::string password = "iggy") {
        auto client = GetLoggedOutHighLevelClient();
        client.Connect();
        client.Login(std::move(username), std::move(password));
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

    iggy::UserInfoDetails CreateUser(iggy::IggyBlockingClient &client,
                                     const std::string &username,
                                     const std::string &password,
                                     const iggy::UserStatus status,
                                     std::optional<iggy::Permissions> permissions = std::nullopt) {
        auto user = client.CreateUser(username, password, status, permissions);
        tracked_user_names_.push_back(username);
        return user;
    }

    void ForgetUser(const std::string &username) {
        tracked_user_names_.erase(std::remove(tracked_user_names_.begin(), tracked_user_names_.end(), username),
                                  tracked_user_names_.end());
    }

    void RenameTrackedUser(const std::string &from, const std::string &to) {
        const auto tracked_user = std::find(tracked_user_names_.begin(), tracked_user_names_.end(), from);
        ASSERT_NE(tracked_user, tracked_user_names_.end());
        *tracked_user = to;
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

    void Cleanup() {
        if (HasTrackedResources()) {
            RunAsRoot([this](iggy::IggyBlockingClient &client) {
                CleanupConsumerGroups(client);
                CleanupStreams(client);
                CleanupUsers(client);
            });
        }
    }

  private:
    void CleanupBestEffort() noexcept {
        if (HasTrackedResources()) {
            RunAsRootBestEffort([this](iggy::IggyBlockingClient &client) {
                CleanupConsumerGroupsBestEffort(client);
                CleanupStreamsBestEffort(client);
                CleanupUsersBestEffort(client);
            });
        }
    }

    void CleanupUsers(iggy::IggyBlockingClient &client) {
        for (const auto &username : tracked_user_names_) {
            EXPECT_NO_THROW(client.DeleteUser(iggy::Identifier::String(username)));
        }
        tracked_user_names_.clear();
    }

    void CleanupUsersBestEffort(iggy::IggyBlockingClient &client) noexcept {
        try {
            for (const auto &username : tracked_user_names_) {
                client.DeleteUser(iggy::Identifier::String(username));
            }
        } catch (...) {
        }

        tracked_user_names_.clear();
    }

    void CleanupStreams(iggy::IggyBlockingClient &client) {
        for (const auto &stream_name : tracked_stream_names_) {
            EXPECT_NO_THROW(client.DeleteStream(iggy::Identifier::String(stream_name)));
        }
        for (const auto stream_id : tracked_stream_ids_) {
            EXPECT_NO_THROW(client.DeleteStream(iggy::Identifier::Numeric(stream_id)));
        }
        tracked_stream_names_.clear();
        tracked_stream_ids_.clear();
    }

    void CleanupStreamsBestEffort(iggy::IggyBlockingClient &client) noexcept {
        try {
            for (const auto &stream_name : tracked_stream_names_) {
                client.DeleteStream(iggy::Identifier::String(stream_name));
            }
            for (const auto stream_id : tracked_stream_ids_) {
                client.DeleteStream(iggy::Identifier::Numeric(stream_id));
            }
        } catch (...) {
        }

        tracked_stream_names_.clear();
        tracked_stream_ids_.clear();
    }

    void CleanupConsumerGroups(iggy::IggyBlockingClient &client) {
        for (const auto &group : tracked_consumer_groups_) {
            EXPECT_NO_THROW(client.DeleteConsumerGroup(iggy::Identifier::String(group.stream_name),
                                                       iggy::Identifier::String(group.topic_name),
                                                       iggy::Identifier::String(group.group_name)));
        }
        tracked_consumer_groups_.clear();
    }

    void CleanupConsumerGroupsBestEffort(iggy::IggyBlockingClient &client) noexcept {
        try {
            for (const auto &group : tracked_consumer_groups_) {
                client.DeleteConsumerGroup(iggy::Identifier::String(group.stream_name),
                                           iggy::Identifier::String(group.topic_name),
                                           iggy::Identifier::String(group.group_name));
            }
        } catch (...) {
        }

        tracked_consumer_groups_.clear();
    }

    template <typename Cleanup>
    void RunAsRoot(Cleanup &&cleanup) {
        auto cleanup_client = iggy::IggyBlockingClient::Builder().Build();
        ASSERT_NO_THROW(cleanup_client.Connect());
        ASSERT_NO_THROW(cleanup_client.Login("iggy", "iggy"));
        cleanup(cleanup_client);
    }

    template <typename Cleanup>
    void RunAsRootBestEffort(Cleanup &&cleanup) noexcept {
        try {
            auto cleanup_client = iggy::IggyBlockingClient::Builder().Build();
            cleanup_client.Connect();
            cleanup_client.Login("iggy", "iggy");
            cleanup(cleanup_client);
        } catch (...) {
        }
    }

    bool HasTrackedResources() const {
        return !tracked_consumer_groups_.empty() || !tracked_stream_names_.empty() || !tracked_stream_ids_.empty() ||
               !tracked_user_names_.empty();
    }

    std::vector<std::string> tracked_user_names_;
    std::vector<std::string> tracked_stream_names_;
    std::vector<std::uint32_t> tracked_stream_ids_;
    std::vector<TrackedConsumerGroup> tracked_consumer_groups_;
};
