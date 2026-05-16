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
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace iggy {

/// Exception raised by the C++ client when an operation fails.
class IggyException : public std::runtime_error {
  public:
    explicit IggyException(const char *message) : std::runtime_error(message) {}
    explicit IggyException(const std::string &message) : std::runtime_error(message) {}
};

namespace detail {

/// Internal helper used by string-backed option types in this header.
template <typename Tag>
class StringTag {
  protected:
    explicit StringTag(std::string value) : value_(std::move(value)) {}

    std::string_view value() const { return value_; }

  private:
    std::string value_;
};

}  // namespace detail

/// Compression setting for `Client::create_topic(...)`.
///
/// Use this type to choose whether messages in a topic are stored as-is or compressed with gzip.
///
/// Internally, this value is passed across the Rust FFI as a string.
/// Values outside the supported set are rejected by the Rust client.
/// Some Rust-side aliases may also be accepted for compatibility.
class CompressionAlgorithm final : private detail::StringTag<CompressionAlgorithm> {
  public:
    /// Store messages without compression.
    static CompressionAlgorithm none() { return CompressionAlgorithm("none"); }
    /// Compress messages with gzip.
    static CompressionAlgorithm gzip() { return CompressionAlgorithm("gzip"); }

    std::string_view compression_algorithm_value() const { return value(); }

  private:
    explicit CompressionAlgorithm(std::string algorithm)
        : detail::StringTag<CompressionAlgorithm>(std::move(algorithm)) {}
};

/// Compression setting for `Client::snapshot(...)`.
///
/// Use this type to choose how snapshot data is compressed in the generated archive.
///
/// Internally, this value is passed across the Rust FFI as a string.
/// Values outside the supported set are rejected by the Rust client.
/// Some Rust-side aliases may also be accepted for compatibility.
class SnapshotCompression final : private detail::StringTag<SnapshotCompression> {
  public:
    /// Store snapshot files without compression.
    static SnapshotCompression stored() { return SnapshotCompression("stored"); }
    /// Use standard deflate compression.
    static SnapshotCompression deflated() { return SnapshotCompression("deflated"); }
    /// Use bzip2 for a higher compression ratio at the cost of slower processing.
    static SnapshotCompression bzip2() { return SnapshotCompression("bzip2"); }
    /// Use Zstandard for fast compression and decompression.
    static SnapshotCompression zstd() { return SnapshotCompression("zstd"); }
    /// Use LZMA for high compression, especially for larger files.
    static SnapshotCompression lzma() { return SnapshotCompression("lzma"); }
    /// Use XZ, which is similar to LZMA and often faster to decompress.
    static SnapshotCompression xz() { return SnapshotCompression("xz"); }

    std::string_view snapshot_compression_value() const { return value(); }

  private:
    explicit SnapshotCompression(std::string snapshot_compression)
        : detail::StringTag<SnapshotCompression>(std::move(snapshot_compression)) {}
};

/// Snapshot selector for `Client::snapshot(...)`.
///
/// Use this type to choose which snapshot data the server should include.
///
/// Internally, each selected value is passed across the Rust FFI as a string.
/// Values outside the supported set are rejected by the Rust client.
/// Some Rust-side aliases may also be accepted for compatibility.
class SnapshotType final : private detail::StringTag<SnapshotType> {
  public:
    /// Include an overview of the filesystem structure.
    static SnapshotType filesystem_overview() { return SnapshotType("filesystem_overview"); }
    /// Include the list of currently running processes.
    static SnapshotType process_list() { return SnapshotType("process_list"); }
    /// Include CPU, memory, and other system resource usage statistics.
    static SnapshotType resource_usage() { return SnapshotType("resource_usage"); }
    /// Include the test snapshot used for development and testing.
    static SnapshotType test() { return SnapshotType("test"); }
    /// Include server logs from the configured logging directory.
    static SnapshotType server_logs() { return SnapshotType("server_logs"); }
    /// Include the server configuration.
    static SnapshotType server_config() { return SnapshotType("server_config"); }
    /// Include all available snapshot types.
    static SnapshotType all() { return SnapshotType("all"); }

    std::string_view snapshot_type_value() const { return value(); }

  private:
    explicit SnapshotType(std::string snapshot_type) : detail::StringTag<SnapshotType>(std::move(snapshot_type)) {}
};

/// Maximum retained size for a topic.
///
/// Use this type to choose whether a topic uses the server default limit, no limit, or an
/// explicit byte limit.
///
/// Internally, this value is passed across the Rust FFI as a string.
/// Values outside the supported set are rejected by the Rust client.
/// Some Rust-side aliases may also be accepted for compatibility.
class MaxTopicSize final : private detail::StringTag<MaxTopicSize> {
  public:
    /// Use the server's default maximum topic size.
    static MaxTopicSize server_default() { return MaxTopicSize("server_default"); }
    /// Disable the maximum topic size limit.
    static MaxTopicSize unlimited() { return MaxTopicSize("unlimited"); }
    /// Set an explicit maximum topic size in bytes.
    ///
    /// A value of `0` is treated as `server_default()`. A value of
    /// `std::numeric_limits<std::uint64_t>::max()` is treated as `unlimited()`.
    /// The configured limit cannot be lower than the server's segment size.
    static MaxTopicSize from_bytes(std::uint64_t bytes) {
        if (bytes == 0) {
            return server_default();
        }
        if (bytes == std::numeric_limits<std::uint64_t>::max()) {
            return unlimited();
        }
        return MaxTopicSize(std::to_string(bytes));
    }

    std::string_view max_topic_size() const { return value(); }

  private:
    explicit MaxTopicSize(std::string max_topic_size) : detail::StringTag<MaxTopicSize>(std::move(max_topic_size)) {}
};

// TODO(slbotbm): Add rust bindings for Identifier that will use IdKind
/// Describes whether an identifier is numeric or string-based.
///
/// Use this type to describe how an identifier is encoded.
///
/// This type is reserved for future identifier bindings and is not currently passed through the
/// Rust FFI.
class IdKind final : private detail::StringTag<IdKind> {
  public:
    /// A numeric identifier represented as a 32-bit integer.
    static IdKind numeric() { return IdKind("numeric"); }
    /// A string identifier represented by its text value.
    static IdKind string() { return IdKind("string"); }

    std::string_view id_kind_value() const { return value(); }

  private:
    explicit IdKind(std::string id_kind) : detail::StringTag<IdKind>(std::move(id_kind)) {}
};

/// Message expiry policy for `Client::create_topic(...)`.
///
/// Use this type to choose how long messages in a topic are retained.
///
/// `expiry_kind()` returns the selected mode. `expiry_value()` returns the associated payload:
/// the duration for `duration(micros)`, `0` for `server_default()`, or
/// `std::numeric_limits<std::uint64_t>::max()` for `never_expire()`.
///
/// Internally, this value is passed across the Rust FFI as a kind/value pair.
/// Unsupported kinds are rejected by the Rust client.
class Expiry final {
  public:
    /// Use the server's default message expiry policy.
    static Expiry server_default() { return Expiry("server_default", 0); }
    /// Keep messages until they are removed for some other reason, such as topic deletion.
    static Expiry never_expire() { return Expiry("never_expire", std::numeric_limits<std::uint64_t>::max()); }
    /// Expire messages after the given number of microseconds.
    static Expiry duration(std::uint64_t micros) { return Expiry("duration", micros); }

    std::string_view expiry_kind() const { return expiry_kind_; }
    std::uint64_t expiry_value() const { return expiry_value_; }

  private:
    explicit Expiry(std::string expiry_kind, std::uint64_t expiry_value)
        : expiry_kind_(std::move(expiry_kind)), expiry_value_(expiry_value) {}

    std::string expiry_kind_;
    std::uint64_t expiry_value_;
};

/// Starting position for `Client::poll_messages(...)`.
///
/// Use this type to choose where the server should begin reading messages.
///
/// `polling_strategy_kind()` returns the selected mode. `polling_strategy_value()` returns the
/// associated offset or timestamp for the parameterized modes and `0` for the others.
///
/// Internally, this value is passed across the Rust FFI as a kind/value pair.
/// Unsupported kinds are rejected by the Rust client.
class PollingStrategy final {
  public:
    /// Start polling from a specific message offset.
    static PollingStrategy offset(std::uint64_t value) { return PollingStrategy("offset", value); }
    /// Start polling from a specific timestamp.
    static PollingStrategy timestamp(std::uint64_t value) { return PollingStrategy("timestamp", value); }
    /// Start polling from the first message in the partition.
    static PollingStrategy first() { return PollingStrategy("first", 0); }
    /// Start polling from the last message currently available in the partition.
    static PollingStrategy last() { return PollingStrategy("last", 0); }
    /// Start polling from the next message after the stored consumer offset.
    ///
    /// This is typically used with automatic offset commits enabled.
    static PollingStrategy next() { return PollingStrategy("next", 0); }

    std::string_view polling_strategy_kind() const { return polling_strategy_kind_; }
    std::uint64_t polling_strategy_value() const { return polling_strategy_value_; }

  private:
    explicit PollingStrategy(std::string kind, std::uint64_t value)
        : polling_strategy_kind_(std::move(kind)), polling_strategy_value_(value) {}

    std::string polling_strategy_kind_;
    std::uint64_t polling_strategy_value_;
};

}  // namespace iggy
