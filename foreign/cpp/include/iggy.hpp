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
#include <vector>

#include "absl/numeric/int128.h"
#include "lib.rs.h"
#include "rust/cxx.h"

namespace iggy {
class IggyClient;
class IggyMessage;
class StreamDetails;
class TopicDetails;

class IggyException : public std::runtime_error {
public:
  explicit IggyException(const char *message) : std::runtime_error(message) {}
  explicit IggyException(const std::string &message)
      : std::runtime_error(message) {}
};

class CompressionAlgorithm final {
public:
  static CompressionAlgorithm none() { return CompressionAlgorithm("none"); }
  static CompressionAlgorithm gzip() { return CompressionAlgorithm("gzip"); }

  std::string algorithm() const { return algorithm_; }

private:
  friend class iggy::IggyClient;

  explicit CompressionAlgorithm(std::string algorithm)
      : algorithm_(std::move(algorithm)) {}

  rust::String as_rust() const { return rust::String(algorithm_); }

  std::string algorithm_;
};

class Expiry final {
public:
  static Expiry server_default() { return Expiry("server_default", 0); }
  static Expiry never_expire() { return Expiry("never_expire", 0); }
  static Expiry duration(std::uint32_t micros) {
    return Expiry("duration", micros);
  }

private:
  friend class iggy::IggyClient;

  Expiry(std::string expiry_kind, std::uint32_t expiry_value)
      : expiry_kind_(std::move(expiry_kind)), expiry_value_(expiry_value) {}

  std::string expiry_type() const { return expiry_kind_; }
  std::uint32_t expiry_value() const { return expiry_value_; }

  rust::String type_as_rust() const { return rust::String(expiry_kind_); }

  std::uint32_t value() const { return expiry_value_; }

  std::string expiry_kind_;
  std::uint32_t expiry_value_;
};

class MaxTopicSize final {
public:
  static MaxTopicSize server_default() {
    return MaxTopicSize("server_default");
  }
  static MaxTopicSize unlimited() { return MaxTopicSize("unlimited"); }
  static MaxTopicSize from_bytes(std::uint64_t bytes) {
    return MaxTopicSize(std::to_string(bytes));
  }

  std::string max_topic_size() const { return max_topic_size_; }

private:
  friend class iggy::IggyClient;

  explicit MaxTopicSize(std::string max_topic_size)
      : max_topic_size_(std::move(max_topic_size)) {}

  rust::String as_rust() const { return rust::String(max_topic_size_); }

  std::string max_topic_size_;
};

class PollingStrategy final {
public:
  static PollingStrategy offset(std::uint64_t value) {
    return PollingStrategy("offset", value);
  }
  static PollingStrategy timestamp(std::uint64_t value) {
    return PollingStrategy("timestamp", value);
  }
  static PollingStrategy first() { return PollingStrategy("first", 0); }
  static PollingStrategy last() { return PollingStrategy("last", 0); }
  static PollingStrategy next() { return PollingStrategy("next", 0); }

  std::string polling_strategy_kind() const { return polling_strategy_kind_; }
  std::uint64_t polling_strategy_value() const {
    return polling_strategy_value_;
  }

private:
  friend class iggy::IggyClient;

  PollingStrategy(std::string kind, std::uint64_t value)
      : polling_strategy_kind_(std::move(kind)),
        polling_strategy_value_(value) {}

  rust::String kind_as_rust() const {
    return rust::String(polling_strategy_kind_);
  }

  std::uint64_t value() const { return polling_strategy_value_; }

  std::string polling_strategy_kind_;
  std::uint64_t polling_strategy_value_;
};

class IggyMessage final {
public:
  IggyMessage();

  void set_payload(const std::string &data);
  void set_payload(const std::vector<std::uint8_t> &data);

  const std::vector<std::uint8_t> &payload() const;

  std::uint64_t checksum() const;
  absl::uint128 id() const;
  std::uint64_t offset() const;
  std::uint64_t timestamp() const;
  std::uint64_t origin_timestamp() const;
  std::uint32_t user_headers_length() const;
  std::uint32_t payload_length() const;
  const std::vector<std::uint8_t> &user_headers() const;

private:
  friend class IggyClient;
  friend class PolledMessages;

  static IggyMessage from_ffi_message(const ffi::Message &message);
  static rust::Vec<std::uint8_t> to_rust_vec(const std::string &data);
  static rust::Vec<std::uint8_t>
  to_rust_vec(const std::vector<std::uint8_t> &data);
  ffi::Message to_ffi_message() const;

  bool send_message_{true};
  std::uint64_t checksum_{0};
  std::uint64_t id_high_{0};
  std::uint64_t id_low_{0};
  std::uint64_t offset_{0};
  std::uint64_t timestamp_{0};
  std::uint64_t origin_timestamp_{0};
  std::uint32_t user_headers_length_{0};
  std::uint32_t payload_length_{0};
  std::vector<std::uint8_t> payload_{};
  std::vector<std::uint8_t> user_headers_{};
};

class Identifier final {
public:
  Identifier() = delete;
  ~Identifier() noexcept;
  Identifier(const Identifier &) = delete;
  Identifier &operator=(const Identifier &) = delete;
  Identifier(Identifier &&other) noexcept;
  Identifier &operator=(Identifier &&other) noexcept;

  static Identifier from_string(const std::string &name);
  static Identifier from_uint32(std::uint32_t id);
  std::string value() const;

private:
  friend class iggy::IggyClient;

  explicit Identifier(iggy::ffi::Identifier *identifier);

  void destroy() noexcept;

  iggy::ffi::Identifier *identifier_ = nullptr;
};

class PolledMessages final {
public:
  std::uint32_t partition_id() const;
  std::uint64_t current_offset() const;
  std::uint32_t count() const;
  const std::vector<IggyMessage> &messages() const;

private:
  friend class iggy::IggyClient;

  explicit PolledMessages(const ffi::PolledMessages &messages);

  std::uint32_t partition_id_{0};
  std::uint64_t current_offset_{0};
  std::uint32_t count_{0};
  std::vector<IggyMessage> messages_{};
};

class IggyClient final {
public:
  IggyClient() = delete;
  ~IggyClient() noexcept;
  IggyClient(const IggyClient &) = delete;
  IggyClient &operator=(const IggyClient &) = delete;
  IggyClient(IggyClient &&other) noexcept;
  IggyClient &operator=(IggyClient &&other) noexcept;

  static IggyClient new_connection(const std::string &connection_string);
  void connect() const;
  void login_user(const std::string &username,
                  const std::string &password) const;
  void create_stream(const std::string &stream_name) const;
  StreamDetails get_stream(const iggy::Identifier &stream_id) const;
  void create_topic(const iggy::Identifier &stream_id,
                    const std::string &topic_name,
                    std::uint32_t partitions_count,
                    iggy::CompressionAlgorithm compression_algorithm,
                    std::uint8_t replication_factor, iggy::Expiry expiry,
                    iggy::MaxTopicSize max_topic_size) const;
  TopicDetails get_topic(const iggy::Identifier &stream_id,
                         const iggy::Identifier &topic_id) const;
  void send_messages(const std::vector<IggyMessage> &messages,
                     const iggy::Identifier &stream_id,
                     const iggy::Identifier &topic_id,
                     std::uint32_t partitioning) const;
  PolledMessages poll_messages(const iggy::Identifier &stream_id,
                               const iggy::Identifier &topic_id,
                               std::uint32_t partition_id,
                               const PollingStrategy &polling_strategy,
                               std::uint32_t count, bool auto_commit) const;

private:
  explicit IggyClient(ffi::Client *client);

  void destroy() noexcept;

  ffi::Client *client_ = nullptr;
};

class StreamDetails final {
public:
  StreamDetails() = delete;
  ~StreamDetails() noexcept;
  StreamDetails(const StreamDetails &) = delete;
  StreamDetails &operator=(const StreamDetails &) = delete;
  StreamDetails(StreamDetails &&other) noexcept;
  StreamDetails &operator=(StreamDetails &&other) noexcept;

  std::uint32_t stream_id() const;
  std::string name() const;
  std::uint64_t messages_count() const;
  std::uint32_t topics_count() const;

private:
  friend class iggy::IggyClient;

  explicit StreamDetails(iggy::ffi::StreamDetails *stream_details);

  void destroy() noexcept;

  iggy::ffi::StreamDetails *stream_details_ = nullptr;
};

class TopicDetails final {
public:
  TopicDetails() = delete;
  ~TopicDetails() noexcept;
  TopicDetails(const TopicDetails &) = delete;
  TopicDetails &operator=(const TopicDetails &) = delete;
  TopicDetails(TopicDetails &&other) noexcept;
  TopicDetails &operator=(TopicDetails &&other) noexcept;

  std::uint32_t topic_id() const;
  std::string name() const;
  std::uint64_t messages_count() const;
  std::uint32_t partitions_count() const;

private:
  friend class iggy::IggyClient;

  explicit TopicDetails(iggy::ffi::TopicDetails *p);

  void destroy() noexcept;

  iggy::ffi::TopicDetails *topic_details_ = nullptr;
};

} // namespace iggy
