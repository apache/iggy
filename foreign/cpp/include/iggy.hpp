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

#include <cstddef>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "lib.rs.h"

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
    ~StringTag() = default;

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

/// System snapshot selector for `Client::snapshot(...)`.
///
/// Use this type to choose which snapshot data the server should include.
///
/// Internally, each selected value is passed across the Rust FFI as a string.
/// Values outside the supported set are rejected by the Rust client.
class SystemSnapshotType final : private detail::StringTag<SystemSnapshotType> {
  public:
    /// Include an overview of the filesystem structure.
    static SystemSnapshotType filesystem_overview() { return SystemSnapshotType("filesystem_overview"); }
    /// Include the list of currently running processes.
    static SystemSnapshotType process_list() { return SystemSnapshotType("process_list"); }
    /// Include CPU, memory, and other system resource usage statistics.
    static SystemSnapshotType resource_usage() { return SystemSnapshotType("resource_usage"); }
    /// Include the test snapshot used for development and testing.
    static SystemSnapshotType test() { return SystemSnapshotType("test"); }
    /// Include server logs from the configured logging directory.
    static SystemSnapshotType server_logs() { return SystemSnapshotType("server_logs"); }
    /// Include the server configuration.
    static SystemSnapshotType server_config() { return SystemSnapshotType("server_config"); }
    /// Include all available snapshot types.
    static SystemSnapshotType all() { return SystemSnapshotType("all"); }

    std::string_view snapshot_type_value() const { return value(); }

  private:
    explicit SystemSnapshotType(std::string snapshot_type)
        : detail::StringTag<SystemSnapshotType>(std::move(snapshot_type)) {}
};

/// Maximum retained size for a topic.
///
/// Use this type to choose whether a topic uses the server default limit, no limit, or an
/// explicit byte limit.
///
/// Internally, this value is passed across the Rust FFI as a string.
/// Values outside the supported set are rejected by the Rust client.
/// In addition to `server_default` and `unlimited`, the Rust parser also accepts
/// numeric size strings. A value of `0` is treated as `server_default`, and a
/// value of `std::numeric_limits<std::uint64_t>::max()` is treated as `unlimited`.
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

namespace detail {

/// Numeric option values are little-endian on the wire. Encoded byte by byte so
/// a big-endian host produces the same block as a little-endian one.
template <typename Value>
rust::Vec<std::uint8_t> to_little_endian_bytes(const Value value) {
    rust::Vec<std::uint8_t> bytes;
    bytes.reserve(sizeof(Value));
    for (std::size_t index = 0; index < sizeof(Value); ++index) {
        bytes.push_back(static_cast<std::uint8_t>((value >> (index * 8)) & 0xFF));
    }

    return bytes;
}

inline rust::Vec<std::uint8_t> to_bool_bytes(const bool value) {
    rust::Vec<std::uint8_t> bytes;
    bytes.push_back(static_cast<std::uint8_t>(value ? 1 : 0));

    return bytes;
}

inline rust::Vec<std::uint8_t> to_key_bytes(const std::string_view key) {
    rust::Vec<std::uint8_t> bytes;
    bytes.reserve(key.size());
    for (const char character : key) {
        bytes.push_back(static_cast<std::uint8_t>(character));
    }

    return bytes;
}

inline iggy::ffi::HeaderField to_header_field(const iggy::ffi::HeaderKind kind, rust::Vec<std::uint8_t> value) {
    iggy::ffi::HeaderField field;
    field.kind  = static_cast<std::uint8_t>(kind);
    field.value = std::move(value);

    return field;
}

/// An option key is always `String`-kinded. Only the value kind varies per key.
inline iggy::ffi::HeaderEntry to_option_entry(const std::string_view key,
                                              const iggy::ffi::HeaderKind value_kind,
                                              rust::Vec<std::uint8_t> value) {
    iggy::ffi::HeaderEntry entry;
    entry.key   = to_header_field(iggy::ffi::HeaderKind::String, to_key_bytes(key));
    entry.value = to_header_field(value_kind, std::move(value));

    return entry;
}

}  // namespace detail

/// Topic option entries for the trailing options block of
/// `Client::create_topic(...)`.
///
/// Each factory names one key of the server's topic option catalog and encodes
/// its value under that key's catalog kind. Both halves matter: the server
/// refuses a key it does not list, and refuses a listed key whose value arrives
/// under a different kind than the catalog gives it.
/// `Client::describe_options("topic")` enumerates the catalog with each key's
/// kind and default, which is the discovery path over the binary transports:
/// they carry back an error code without naming the refused key.
///
/// These keys are create-time only. `Client::update_topic(...)` refuses every
/// one of them by name: they describe how a topic's partitions were laid down,
/// so changing one mid-life would leave earlier segments built to the old value.
class TopicOption final {
  public:
    /// Set the size at which this topic's segments rotate.
    ///
    /// Must be a multiple of 512 bytes, at least 1 MiB, and no larger than the
    /// server's segment ceiling.
    static iggy::ffi::HeaderEntry segment_size(const std::uint64_t bytes) {
        return detail::to_option_entry("segment_size", iggy::ffi::HeaderKind::Uint64,
                                       detail::to_little_endian_bytes(bytes));
    }

    /// Choose whether writes to this topic's partitions are fsynced.
    static iggy::ffi::HeaderEntry enforce_fsync(const bool enabled) {
        return detail::to_option_entry("enforce_fsync", iggy::ffi::HeaderKind::Bool, detail::to_bool_bytes(enabled));
    }

    /// Flush the journal once it holds this many messages.
    ///
    /// Must be non-zero. Paired with
    /// `size_of_messages_required_to_save(bytes)`: whichever threshold trips
    /// first flushes.
    static iggy::ffi::HeaderEntry messages_required_to_save(const std::uint32_t messages) {
        return detail::to_option_entry("messages_required_to_save", iggy::ffi::HeaderKind::Uint32,
                                       detail::to_little_endian_bytes(messages));
    }

    /// Flush the journal once it holds this many bytes.
    ///
    /// Capped at 1 GiB: a threshold above the largest a segment may be never
    /// trips, and the journal does not survive a crash.
    static iggy::ffi::HeaderEntry size_of_messages_required_to_save(const std::uint64_t bytes) {
        return detail::to_option_entry("size_of_messages_required_to_save", iggy::ffi::HeaderKind::Uint64,
                                       detail::to_little_endian_bytes(bytes));
    }

    /// Choose whether a segment's bytes are reserved on disk when it is created.
    ///
    /// Reserves `segment_size * partitions_count` up front, which the server
    /// caps at 64 GiB per topic.
    static iggy::ffi::HeaderEntry preallocate_segments(const bool enabled) {
        return detail::to_option_entry("preallocate_segments", iggy::ffi::HeaderKind::Bool,
                                       detail::to_bool_bytes(enabled));
    }

  private:
    TopicOption() = delete;
};

}  // namespace iggy
