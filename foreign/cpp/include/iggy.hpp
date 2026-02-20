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
#include <stdexcept>
#include <string>
#include <utility>

namespace iggy {

class IggyException : public std::runtime_error {
  public:
    explicit IggyException(const char *message) : std::runtime_error(message) {}
    explicit IggyException(const std::string &message) : std::runtime_error(message) {}
};

class CompressionAlgorithm final {
  public:
    static CompressionAlgorithm none() { return CompressionAlgorithm("none"); }
    static CompressionAlgorithm gzip() { return CompressionAlgorithm("gzip"); }

    std::string algorithm() const { return algorithm_; }

  private:
    explicit CompressionAlgorithm(std::string algorithm) : algorithm_(std::move(algorithm)) {}

    std::string algorithm_;
};

class Expiry final {
  public:
    static Expiry server_default() { return Expiry("server_default", 0); }
    static Expiry never_expire() { return Expiry("never_expire", 0); }
    static Expiry duration(std::uint32_t micros) { return Expiry("duration", micros); }

  private:
    Expiry(std::string expiry_kind, std::uint32_t expiry_value)
        : expiry_kind_(std::move(expiry_kind)), expiry_value_(expiry_value) {}

    std::string expiry_type() const { return expiry_kind_; }
    std::uint32_t expiry_value() const { return expiry_value_; }

    std::uint32_t value() const { return expiry_value_; }

    std::string expiry_kind_;
    std::uint32_t expiry_value_;
};

class MaxTopicSize final {
  public:
    static MaxTopicSize server_default() { return MaxTopicSize("server_default"); }
    static MaxTopicSize unlimited() { return MaxTopicSize("unlimited"); }
    static MaxTopicSize from_bytes(std::uint64_t bytes) {
        return MaxTopicSize(std::to_string(bytes));
    }

    std::string max_topic_size() const { return max_topic_size_; }

  private:
    explicit MaxTopicSize(std::string max_topic_size)
        : max_topic_size_(std::move(max_topic_size)) {}

    std::string max_topic_size_;
};

class PollingStrategy final {
  public:
    static PollingStrategy offset(std::uint64_t value) { return PollingStrategy("offset", value); }
    static PollingStrategy timestamp(std::uint64_t value) {
        return PollingStrategy("timestamp", value);
    }
    static PollingStrategy first() { return PollingStrategy("first", 0); }
    static PollingStrategy last() { return PollingStrategy("last", 0); }
    static PollingStrategy next() { return PollingStrategy("next", 0); }

    std::string polling_strategy_kind() const { return polling_strategy_kind_; }
    std::uint64_t polling_strategy_value() const { return polling_strategy_value_; }

  private:
    PollingStrategy(std::string kind, std::uint64_t value)
        : polling_strategy_kind_(std::move(kind)), polling_strategy_value_(value) {}

    std::uint64_t value() const { return polling_strategy_value_; }

    std::string polling_strategy_kind_;
    std::uint64_t polling_strategy_value_;
};

}  // namespace iggy
