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
/**
 * @file iggy.hpp
 * @brief Public C++ API for the Apache Iggy client.
 */

#include <chrono>
#include <cstdint>
#include <limits>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "lib.rs.h"

namespace iggy {

class IggyBlockingClient;
class LoginInfo;
class Partition;
class Topic;
class TopicDetails;
class Stream;
class StreamDetails;

namespace detail {
/** @brief Internal base for string-backed option types. */
template <typename Tag>
class StringTag {
  protected:
    explicit StringTag(std::string value) : value_(std::move(value)) {}
    ~StringTag() = default;

    [[nodiscard]] std::string_view Value() const { return value_; }

  private:
    std::string value_;
};

}  // namespace detail

/**
 * @brief Exception thrown when an Iggy client operation fails.
 */
class IggyException : public std::runtime_error {
  public:
    explicit IggyException(const char *message) : std::runtime_error(message) {}
    explicit IggyException(const std::string &message) : std::runtime_error(message) {}
};

/**
 * @brief Details returned after a successful login.
 *
 * Contains the authenticated user's ID. For HTTP connections, it also includes
 * the access token retained by the client for subsequent requests. Stateful
 * transports do not provide an access token. Treat the token as a credential:
 * do not write it to logs or expose it to untrusted code.
 */
class LoginInfo final {
  public:
    /**
     * @brief Returns the numeric ID of the authenticated user.
     * @return Numeric user ID.
     */
    [[nodiscard]] std::uint32_t UserId() const noexcept { return user_id_; }

    /**
     * @brief Returns the HTTP access token when the login returned one.
     * @return Reference to the owning optional token. Empty when the selected
     *         transport does not use an access token.
     */
    [[nodiscard]] const std::optional<std::string> &AccessToken() const noexcept { return access_token_; }

    /**
     * @brief Returns the access-token expiry when a token was returned.
     * @return Empty when no access token was returned; otherwise the
     *         server-provided expiry value.
     */
    [[nodiscard]] std::optional<std::uint64_t> AccessTokenExpiry() const noexcept { return access_token_expiry_; }

  private:
    LoginInfo(std::uint32_t user_id,
              std::optional<std::string> access_token,
              std::optional<std::uint64_t> access_token_expiry)
        : user_id_(user_id), access_token_(std::move(access_token)), access_token_expiry_(access_token_expiry) {}

    static LoginInfo FromFfi(ffi::LoginInfo login_info);

    friend class IggyBlockingClient;

    std::uint32_t user_id_;
    std::optional<std::string> access_token_;
    std::optional<std::uint64_t> access_token_expiry_;
};

/**
 * @brief Identifier for a server resource.
 *
 * Create an identifier from a server-assigned numeric ID or a resource name.
 * Resource names must contain between 1 and 255 bytes. A numeric ID of zero is
 * valid.
 */
class Identifier final {
  public:
    enum class Kind { Numeric, String };

    /**
     * @brief Creates a numeric identifier.
     * @param id Numeric server ID.
     * @return Identifier that addresses @p id.
     */
    static Identifier Numeric(std::uint32_t id) { return Identifier(Kind::Numeric, id); }

    /**
     * @brief Creates a name-based identifier.
     * @param name Resource name.
     * @return Identifier that addresses @p name.
     * @throws IggyException if @p name is empty or exceeds 255 bytes.
     */
    static Identifier String(std::string name) {
        if (name.empty() || name.size() > 255) {
            throw IggyException("Identifier name must contain 1 to 255 bytes");
        }
        return Identifier(Kind::String, std::move(name));
    }

    /**
     * @brief Returns this identifier's representation.
     * @return Kind::Numeric or Kind::String.
     */
    [[nodiscard]] Kind Type() const noexcept { return kind_; }

    /**
     * @brief Returns the identifier payload.
     * @return Reference to the owning payload containing the numeric ID for
     *         Kind::Numeric or the name for Kind::String. The reference
     *         remains valid while this Identifier remains alive.
     */
    [[nodiscard]] const std::variant<std::uint32_t, std::string> &Value() const noexcept { return value_; }

  private:
    Identifier(Kind kind, std::variant<std::uint32_t, std::string> value) : kind_(kind), value_(std::move(value)) {}

    static Identifier FromFfi(ffi::Identifier identifier);
    ffi::Identifier ToFfi() const;

    friend class IggyBlockingClient;

    Kind kind_;
    std::variant<std::uint32_t, std::string> value_;
};

/**
 * @brief Type tag for a HeaderField payload.
 *
 * Specifies how a HeaderField payload is encoded. Each field stores a type tag
 * and its corresponding bytes. Numeric payloads use little-endian byte order.
 */
enum class HeaderKind : std::uint8_t {
    Raw     = 1,
    String  = 2,
    Bool    = 3,
    Int8    = 4,
    Int16   = 5,
    Int32   = 6,
    Int64   = 7,
    Int128  = 8,
    Uint8   = 9,
    Uint16  = 10,
    Uint32  = 11,
    Uint64  = 12,
    Uint128 = 13,
    Float32 = 14,
    Float64 = 15,
};

/**
 * @brief One typed header key or value.
 *
 * Create() preserves the supplied bytes without validating that they match the
 * specified type. Invalid key or value encodings are rejected when the client
 * sends a request.
 */
class HeaderField final {
  public:
    /**
     * @brief Creates a typed header field from wire-encoded bytes.
     * @param kind Type tag for @p value.
     * @param value Payload encoded according to @p kind.
     * @return Header field containing the supplied type and bytes.
     */
    static HeaderField Create(HeaderKind kind, std::vector<std::uint8_t> value) {
        return HeaderField(kind, std::move(value));
    }

    /**
     * @brief Returns the wire type of Value().
     * @return Header type tag.
     */
    [[nodiscard]] HeaderKind Kind() const noexcept { return kind_; }

    /**
     * @brief Returns bytes owned by this field.
     * @return Payload encoded according to Kind().
     */
    [[nodiscard]] const std::vector<std::uint8_t> &Value() const noexcept { return value_; }

  private:
    HeaderField(HeaderKind kind, std::vector<std::uint8_t> value) : kind_(kind), value_(std::move(value)) {}

    static HeaderField FromFfi(ffi::HeaderField field);

    friend class HeaderEntry;

    HeaderKind kind_;
    std::vector<std::uint8_t> value_;
};

/**
 * @brief One typed header key-value pair.
 *
 * Topic options and message user headers use the same typed key-value format.
 */
class HeaderEntry final {
  public:
    /**
     * @brief Creates a header entry from its typed key and value.
     * @param key Typed entry key.
     * @param value Typed entry value.
     * @return Header entry containing @p key and @p value.
     */
    static HeaderEntry Create(HeaderField key, HeaderField value) {
        return HeaderEntry(std::move(key), std::move(value));
    }

    /**
     * @brief Returns the typed key.
     * @return Key owned by this entry.
     */
    [[nodiscard]] const HeaderField &Key() const noexcept { return key_; }

    /**
     * @brief Returns the typed value.
     * @return Value owned by this entry.
     */
    [[nodiscard]] const HeaderField &Value() const noexcept { return value_; }

  private:
    HeaderEntry(HeaderField key, HeaderField value) : key_(std::move(key)), value_(std::move(value)) {}

    static HeaderEntry FromFfi(ffi::HeaderEntry entry);

    friend class ResourceOptions;

    HeaderField key_;
    HeaderField value_;
};

/**
 * @brief Options recorded for a stream or topic.
 *
 * Explicit() contains values supplied when the resource was created. Derived()
 * contains values resolved from the server configuration at that time. Derived
 * values describe the resource's creation settings and can differ when the
 * resource is recreated with a different server configuration.
 *
 * This is a response-only model returned by Options(). Use TopicCreateOptions
 * to configure a new topic. Stream creation currently accepts only a name.
 */
class ResourceOptions final {
  public:
    /**
     * @brief Returns entries supplied explicitly at resource creation.
     * @return Explicit entries as map from option name to typed value.
     */
    [[nodiscard]] const std::map<std::string, HeaderField> &Explicit() const noexcept { return explicit_; }

    /**
     * @brief Returns entries derived from configured defaults at admission.
     * @return Derived entries as map from option name to typed value.
     * @note Stream responses currently expose explicit entries only, so this
     *       collection is empty for Stream and StreamDetails.
     */
    [[nodiscard]] const std::map<std::string, HeaderField> &Derived() const noexcept { return derived_; }

  private:
    ResourceOptions(std::map<std::string, HeaderField> explicit_entries,
                    std::map<std::string, HeaderField> derived_entries)
        : explicit_(std::move(explicit_entries)), derived_(std::move(derived_entries)) {}

    static ResourceOptions FromFfi(rust::Vec<ffi::HeaderEntry> explicit_entries,
                                   rust::Vec<ffi::HeaderEntry> derived_entries);

    friend class IggyBlockingClient;
    friend class Topic;
    friend class TopicDetails;
    friend class Stream;
    friend class StreamDetails;

    std::map<std::string, HeaderField> explicit_;
    std::map<std::string, HeaderField> derived_;
};

/**
 * @brief Snapshot of one topic's metadata and aggregate statistics.
 *
 * GetStream() returns one of these values for each observed topic. It owns its
 * name and option data.
 *
 * The value describes the topic state observed by the server for one request.
 * It is not a live view. SizeBytes(), MessagesCount(), and PartitionsCount()
 * can become stale immediately after the request completes when another client
 * changes the topic.
 *
 * Use GetTopic() to retrieve partition summaries. Topic IDs identify a topic
 * within its stream for its lifetime and remain stable when it is renamed.
 * CreatedAt() is the server timestamp, in microseconds, recorded when the
 * topic was created.
 */
class Topic final {
  public:
    /**
     * @brief Returns the numeric topic ID assigned within its stream.
     * @return Numeric topic ID.
     */
    [[nodiscard]] std::uint32_t Id() const noexcept { return id_; }

    /**
     * @brief Returns the server creation timestamp.
     * @return Timestamp in microseconds.
     */
    [[nodiscard]] std::uint64_t CreatedAt() const noexcept { return created_at_; }

    /**
     * @brief Returns the topic name.
     * @return Name owned by this value.
     */
    [[nodiscard]] const std::string &Name() const noexcept { return name_; }

    /**
     * @brief Returns the aggregate retained topic size.
     * @return Size in bytes.
     */
    [[nodiscard]] std::uint64_t SizeBytes() const noexcept { return size_bytes_; }

    /**
     * @brief Returns the server-encoded message retention value.
     * @return Retention value in microseconds or a protocol sentinel.
     */
    [[nodiscard]] std::uint64_t MessageExpiry() const noexcept { return message_expiry_; }

    /**
     * @brief Returns the server-selected storage compression algorithm.
     * @return Algorithm name owned by this value.
     */
    [[nodiscard]] const std::string &CompressionAlgorithm() const noexcept { return compression_algorithm_; }

    /**
     * @brief Returns the configured maximum retained topic size.
     * @return Maximum size in bytes.
     */
    [[nodiscard]] std::uint64_t MaxTopicSize() const noexcept { return max_topic_size_; }

    /**
     * @brief Returns the aggregate number of retained messages.
     * @return Message count.
     */
    [[nodiscard]] std::uint64_t MessagesCount() const noexcept { return messages_count_; }

    /**
     * @brief Returns the number of partitions belonging to this topic.
     * @return Partition count.
     */
    [[nodiscard]] std::uint32_t PartitionsCount() const noexcept { return partitions_count_; }

    /**
     * @brief Returns topic creation options and their admission provenance.
     * @return Options owned by this value.
     */
    [[nodiscard]] const ResourceOptions &Options() const noexcept { return options_; }

  private:
    Topic(std::uint32_t id,
          std::uint64_t created_at,
          std::string name,
          std::uint64_t size_bytes,
          std::uint64_t message_expiry,
          std::string compression_algorithm,
          std::uint64_t max_topic_size,
          std::uint64_t messages_count,
          std::uint32_t partitions_count,
          ResourceOptions options)
        : id_(id),
          created_at_(created_at),
          name_(std::move(name)),
          size_bytes_(size_bytes),
          message_expiry_(message_expiry),
          compression_algorithm_(std::move(compression_algorithm)),
          max_topic_size_(max_topic_size),
          messages_count_(messages_count),
          partitions_count_(partitions_count),
          options_(std::move(options)) {}

    static Topic FromFfi(ffi::Topic topic);

    friend class IggyBlockingClient;
    friend class StreamDetails;

    std::uint32_t id_;
    std::uint64_t created_at_;
    std::string name_;
    std::uint64_t size_bytes_;
    std::uint64_t message_expiry_;
    std::string compression_algorithm_;
    std::uint64_t max_topic_size_;
    std::uint64_t messages_count_;
    std::uint32_t partitions_count_;
    ResourceOptions options_;
};

/**
 * @brief Partition metadata returned within TopicDetails.
 *
 * Represents the state of a partition when its topic was retrieved. This is a
 * snapshot, not a live view, so offsets and statistics can change after
 * GetTopic() returns.
 */
class Partition final {
  public:
    /**
     * @brief Returns the numeric partition ID within its topic.
     * @return Numeric partition ID.
     */
    [[nodiscard]] std::uint32_t Id() const noexcept { return id_; }

    /**
     * @brief Returns the server creation timestamp.
     * @return Timestamp in microseconds.
     */
    [[nodiscard]] std::uint64_t CreatedAt() const noexcept { return created_at_; }

    /**
     * @brief Returns the number of retained storage segments.
     * @return Segment count.
     */
    [[nodiscard]] std::uint32_t SegmentsCount() const noexcept { return segments_count_; }

    /**
     * @brief Returns the current server-observed message offset.
     * @return Current message offset.
     */
    [[nodiscard]] std::uint64_t CurrentOffset() const noexcept { return current_offset_; }

    /**
     * @brief Returns the retained partition size.
     * @return Size in bytes.
     */
    [[nodiscard]] std::uint64_t SizeBytes() const noexcept { return size_bytes_; }

    /**
     * @brief Returns the number of retained messages.
     * @return Message count.
     */
    [[nodiscard]] std::uint64_t MessagesCount() const noexcept { return messages_count_; }

  private:
    Partition(std::uint32_t id,
              std::uint64_t created_at,
              std::uint32_t segments_count,
              std::uint64_t current_offset,
              std::uint64_t size_bytes,
              std::uint64_t messages_count)
        : id_(id),
          created_at_(created_at),
          segments_count_(segments_count),
          current_offset_(current_offset),
          size_bytes_(size_bytes),
          messages_count_(messages_count) {}

    static Partition FromFfi(ffi::Partition partition);

    friend class TopicDetails;

    std::uint32_t id_;
    std::uint64_t created_at_;
    std::uint32_t segments_count_;
    std::uint64_t current_offset_;
    std::uint64_t size_bytes_;
    std::uint64_t messages_count_;
};

/**
 * @brief Snapshot of one topic's metadata, aggregate statistics, and partitions.
 *
 * GetTopic() returns this value. It owns its name, partition summaries, and
 * option data.
 *
 * The value describes the topic state observed by the server for one request.
 * It is not a live view. Its metadata and partition summaries can become stale
 * immediately after the request completes when another client changes the
 * topic.
 *
 * Topic IDs identify a topic within its stream for its lifetime and remain
 * stable when it is renamed. CreatedAt() is the server timestamp, in
 * microseconds, recorded when the topic was created.
 */
class TopicDetails final {
  public:
    /**
     * @brief Returns the numeric topic ID within its stream.
     * @return Numeric topic ID.
     */
    [[nodiscard]] std::uint32_t Id() const noexcept { return id_; }

    /**
     * @brief Returns the server creation timestamp.
     * @return Timestamp in microseconds.
     */
    [[nodiscard]] std::uint64_t CreatedAt() const noexcept { return created_at_; }

    /**
     * @brief Returns the topic name.
     * @return Name owned by this value.
     */
    [[nodiscard]] const std::string &Name() const noexcept { return name_; }

    /**
     * @brief Returns the aggregate retained topic size.
     * @return Size in bytes.
     */
    [[nodiscard]] std::uint64_t SizeBytes() const noexcept { return size_bytes_; }

    /**
     * @brief Returns the server-encoded message retention value.
     * @return Retention value in microseconds or a protocol sentinel.
     */
    [[nodiscard]] std::uint64_t MessageExpiry() const noexcept { return message_expiry_; }

    /**
     * @brief Returns the storage compression algorithm selected for this topic.
     * @return Algorithm name owned by this value.
     */
    [[nodiscard]] const std::string &CompressionAlgorithm() const noexcept { return compression_algorithm_; }

    /**
     * @brief Returns the maximum retained size configured for this topic.
     * @return Maximum size in bytes.
     */
    [[nodiscard]] std::uint64_t MaxTopicSize() const noexcept { return max_topic_size_; }

    /**
     * @brief Returns the aggregate number of retained messages.
     * @return Message count.
     */
    [[nodiscard]] std::uint64_t MessagesCount() const noexcept { return messages_count_; }

    /**
     * @brief Returns the number of partitions belonging to this topic.
     * @return Partition count.
     */
    [[nodiscard]] std::uint32_t PartitionsCount() const noexcept { return partitions_count_; }

    /**
     * @brief Returns one summary for each partition in the topic.
     *
     * The summaries do not include segment metadata, messages, consumer
     * offsets, or consumer-group membership.
     * @return Partition summaries owned by this value.
     */
    [[nodiscard]] const std::vector<Partition> &Partitions() const noexcept { return partitions_; }

    /**
     * @brief Returns topic creation options and their admission provenance.
     * @return Options owned by this value.
     */
    [[nodiscard]] const ResourceOptions &Options() const noexcept { return options_; }

  private:
    TopicDetails(std::uint32_t id,
                 std::uint64_t created_at,
                 std::string name,
                 std::uint64_t size_bytes,
                 std::uint64_t message_expiry,
                 std::string compression_algorithm,
                 std::uint64_t max_topic_size,
                 std::uint64_t messages_count,
                 std::uint32_t partitions_count,
                 std::vector<Partition> partitions,
                 ResourceOptions options)
        : id_(id),
          created_at_(created_at),
          name_(std::move(name)),
          size_bytes_(size_bytes),
          message_expiry_(message_expiry),
          compression_algorithm_(std::move(compression_algorithm)),
          max_topic_size_(max_topic_size),
          messages_count_(messages_count),
          partitions_count_(partitions_count),
          partitions_(std::move(partitions)),
          options_(std::move(options)) {}

    static TopicDetails FromFfi(ffi::TopicDetails topic);

    friend class IggyBlockingClient;

    std::uint32_t id_;
    std::uint64_t created_at_;
    std::string name_;
    std::uint64_t size_bytes_;
    std::uint64_t message_expiry_;
    std::string compression_algorithm_;
    std::uint64_t max_topic_size_;
    std::uint64_t messages_count_;
    std::uint32_t partitions_count_;
    std::vector<Partition> partitions_;
    ResourceOptions options_;
};

/**
 * @brief Snapshot of one stream's metadata and aggregate statistics.
 *
 * CreateStream() and GetStream() return this value.
 *
 * The value describes the stream state observed by the server for one request.
 * It is not a live view or an atomic snapshot of later stream, topic, or
 * message activity. SizeBytes(), MessagesCount(), TopicsCount(), and Topics()
 * can become stale immediately after the request completes when another client
 * changes the stream.
 *
 * A newly created stream has no topics or messages, so CreateStream() returns
 * zero for SizeBytes(), MessagesCount(), and TopicsCount(), with an empty
 * Topics() collection. GetStream() returns the same aggregate fields and one
 * Topic summary for each observed topic.
 *
 * Stream IDs identify a stream for its lifetime and remain stable when it is
 * renamed. CreatedAt() is the server timestamp, in microseconds, recorded when
 * the stream was created.
 */
class StreamDetails final {
  public:
    /**
     * @brief Returns the numeric ID assigned by the server.
     *
     * This value can be passed to GetStream() while the stream exists. It is
     * unchanged by a stream rename.
     * @return Numeric stream ID.
     */
    [[nodiscard]] std::uint32_t Id() const noexcept { return id_; }

    /**
     * @brief Returns the server-recorded creation timestamp.
     * @return Timestamp in microseconds.
     */
    [[nodiscard]] std::uint64_t CreatedAt() const noexcept { return created_at_; }

    /**
     * @brief Returns the unique stream name observed by the server.
     * @return Reference owned by this value. It remains valid until this
     *         StreamDetails object is modified or destroyed.
     */
    [[nodiscard]] const std::string &Name() const noexcept { return name_; }

    /**
     * @brief Returns the aggregate retained size of all stream topics.
     * @return Size in bytes observed by the server for this request.
     */
    [[nodiscard]] std::uint64_t SizeBytes() const noexcept { return size_bytes_; }

    /**
     * @brief Returns the aggregate number of messages in all stream topics.
     * @return Message count observed by the server for this request.
     */
    [[nodiscard]] std::uint64_t MessagesCount() const noexcept { return messages_count_; }

    /**
     * @brief Returns the number of topics belonging to the stream.
     * @return Topic count observed by the server for this request.
     */
    [[nodiscard]] std::uint32_t TopicsCount() const noexcept { return topics_count_; }

    /**
     * @brief Returns the topic summaries observed by the server.
     * @return Topic values owned by this StreamDetails object.
     */
    [[nodiscard]] const std::vector<Topic> &Topics() const noexcept { return topics_; }

    /**
     * @brief Returns explicit stream creation options.
     * @return Options owned by this value.
     * @note The current bridge does not return derived stream options.
     */
    [[nodiscard]] const ResourceOptions &Options() const noexcept { return options_; }

  private:
    StreamDetails(std::uint32_t id,
                  std::uint64_t created_at,
                  std::string name,
                  std::uint64_t size_bytes,
                  std::uint64_t messages_count,
                  std::uint32_t topics_count,
                  std::vector<Topic> topics,
                  ResourceOptions options)
        : id_(id),
          created_at_(created_at),
          name_(std::move(name)),
          size_bytes_(size_bytes),
          messages_count_(messages_count),
          topics_count_(topics_count),
          topics_(std::move(topics)),
          options_(std::move(options)) {}

    static StreamDetails FromFfi(ffi::StreamDetails stream);

    friend class IggyBlockingClient;

    std::uint32_t id_;
    std::uint64_t created_at_;
    std::string name_;
    std::uint64_t size_bytes_;
    std::uint64_t messages_count_;
    std::uint32_t topics_count_;
    std::vector<Topic> topics_;
    ResourceOptions options_;
};

/**
 * @brief Snapshot of one stream's metadata and aggregate statistics.
 *
 * GetStreams() returns one of these values for each observed stream.
 *
 * The value describes the stream state observed by the server for one request.
 * It is not a live view. SizeBytes(), MessagesCount(), and TopicsCount() can
 * become stale immediately after the request completes when another client
 * changes the stream.
 *
 * Use GetStream() to retrieve topic summaries for a stream.
 */
class Stream final {
  public:
    /**
     * @brief Returns the numeric ID assigned by the server.
     * @return Numeric stream ID.
     */
    [[nodiscard]] std::uint32_t Id() const noexcept { return id_; }

    /**
     * @brief Returns the server-recorded creation timestamp.
     * @return Timestamp in microseconds.
     */
    [[nodiscard]] std::uint64_t CreatedAt() const noexcept { return created_at_; }

    /**
     * @brief Returns the stream name.
     * @return Name owned by this value.
     */
    [[nodiscard]] const std::string &Name() const noexcept { return name_; }

    /**
     * @brief Returns the aggregate retained stream size.
     * @return Size in bytes.
     */
    [[nodiscard]] std::uint64_t SizeBytes() const noexcept { return size_bytes_; }

    /**
     * @brief Returns the aggregate number of retained stream messages.
     * @return Message count.
     */
    [[nodiscard]] std::uint64_t MessagesCount() const noexcept { return messages_count_; }

    /**
     * @brief Returns the number of topics belonging to the stream.
     * @return Topic count.
     */
    [[nodiscard]] std::uint32_t TopicsCount() const noexcept { return topics_count_; }

    /**
     * @brief Returns explicit stream creation options.
     * @return Options owned by this value.
     * @note The current bridge does not return derived stream options.
     */
    [[nodiscard]] const ResourceOptions &Options() const noexcept { return options_; }

  private:
    Stream(std::uint32_t id,
           std::uint64_t created_at,
           std::string name,
           std::uint64_t size_bytes,
           std::uint64_t messages_count,
           std::uint32_t topics_count,
           ResourceOptions options)
        : id_(id),
          created_at_(created_at),
          name_(std::move(name)),
          size_bytes_(size_bytes),
          messages_count_(messages_count),
          topics_count_(topics_count),
          options_(std::move(options)) {}

    static Stream FromFfi(ffi::Stream stream);

    friend class IggyBlockingClient;

    std::uint32_t id_;
    std::uint64_t created_at_;
    std::string name_;
    std::uint64_t size_bytes_;
    std::uint64_t messages_count_;
    std::uint32_t topics_count_;
    ResourceOptions options_;
};

/**
 * @brief Compression algorithm used for topic messages.
 *
 * Selects whether messages in a topic are stored as-is or compressed with
 * gzip.
 *
 * @note The value is passed across the Rust FFI as a string. The Rust client
 *       rejects unsupported values.
 */
class CompressionAlgorithm final : private detail::StringTag<CompressionAlgorithm> {
  public:
    /** @brief Returns the uncompressed storage option. */
    static CompressionAlgorithm None() { return CompressionAlgorithm("none"); }

    /** @brief Returns the gzip compression option. */
    static CompressionAlgorithm Gzip() { return CompressionAlgorithm("gzip"); }

    /**
     * @brief Returns the compression algorithm name.
     * @return Compression algorithm name.
     */
    [[nodiscard]] std::string_view Value() const { return detail::StringTag<CompressionAlgorithm>::Value(); }

  private:
    explicit CompressionAlgorithm(std::string algorithm)
        : detail::StringTag<CompressionAlgorithm>(std::move(algorithm)) {}
};

/**
 * @brief Compression algorithm used for system snapshot archives.
 *
 * Selects how snapshot data is compressed in the generated archive.
 *
 * @note The value is passed across the Rust FFI as a string. The Rust client
 *       rejects unsupported values.
 */
class SnapshotCompression final : private detail::StringTag<SnapshotCompression> {
  public:
    /** @brief Returns the uncompressed storage option. */
    static SnapshotCompression Stored() { return SnapshotCompression("stored"); }

    /** @brief Returns the Deflate compression option. */
    static SnapshotCompression Deflated() { return SnapshotCompression("deflated"); }

    /** @brief Uses bzip2 for better compression with slower processing. */
    static SnapshotCompression Bzip2() { return SnapshotCompression("bzip2"); }

    /** @brief Uses Zstandard for fast compression and decompression. */
    static SnapshotCompression Zstd() { return SnapshotCompression("zstd"); }

    /** @brief Uses LZMA for high compression, especially for larger files. */
    static SnapshotCompression Lzma() { return SnapshotCompression("lzma"); }

    /** @brief Uses XZ for LZMA-like compression with faster decompression. */
    static SnapshotCompression Xz() { return SnapshotCompression("xz"); }

    /**
     * @brief Returns the snapshot compression algorithm name.
     * @return Snapshot compression algorithm name.
     */
    [[nodiscard]] std::string_view Value() const { return detail::StringTag<SnapshotCompression>::Value(); }

  private:
    explicit SnapshotCompression(std::string snapshot_compression)
        : detail::StringTag<SnapshotCompression>(std::move(snapshot_compression)) {}
};

/**
 * @brief Selects data to include in a system snapshot.
 */
class SystemSnapshotType final : private detail::StringTag<SystemSnapshotType> {
  public:
    /** @brief Includes an overview of the file-system structure. */
    static SystemSnapshotType FilesystemOverview() { return SystemSnapshotType("filesystem_overview"); }

    /** @brief Includes currently running processes. */
    static SystemSnapshotType ProcessList() { return SystemSnapshotType("process_list"); }

    /** @brief Includes CPU, memory, and other resource usage statistics. */
    static SystemSnapshotType ResourceUsage() { return SystemSnapshotType("resource_usage"); }

    /** @brief Includes the test snapshot used for development and testing. */
    static SystemSnapshotType Test() { return SystemSnapshotType("test"); }

    /** @brief Includes server logs from the configured logging directory. */
    static SystemSnapshotType ServerLogs() { return SystemSnapshotType("server_logs"); }

    /** @brief Includes server configuration. */
    static SystemSnapshotType ServerConfig() { return SystemSnapshotType("server_config"); }

    /** @brief Includes all available snapshot data. */
    static SystemSnapshotType All() { return SystemSnapshotType("all"); }

    /**
     * @brief Returns the value passed to the client implementation.
     * @return System snapshot type name.
     */
    [[nodiscard]] std::string_view SnapshotTypeValue() const { return Value(); }

  private:
    explicit SystemSnapshotType(std::string snapshot_type)
        : detail::StringTag<SystemSnapshotType>(std::move(snapshot_type)) {}
};

/**
 * @brief Maximum retained size of a topic.
 *
 * A topic may use the server default, have no size limit, or use an explicit
 * byte limit.
 *
 * Use ServerDefault(), Unlimited(), or FromBytes() to select the retention
 * limit.
 */
class MaxTopicSize final : private detail::StringTag<MaxTopicSize> {
  public:
    /** @brief Returns the server-default size option. */
    static MaxTopicSize ServerDefault() { return MaxTopicSize("server_default"); }

    /** @brief Returns the unlimited size option. */
    static MaxTopicSize Unlimited() { return MaxTopicSize("unlimited"); }

    /**
     * @brief Creates an explicit topic size limit.
     * @param bytes Maximum topic size in bytes.
     * @return Server-default size for zero, unlimited size for
     *         std::numeric_limits<std::uint64_t>::max(), or the requested limit.
     * @note The configured limit cannot be smaller than the server segment size.
     */
    static MaxTopicSize FromBytes(std::uint64_t bytes) {
        if (bytes == 0) {
            return ServerDefault();
        }
        if (bytes == std::numeric_limits<std::uint64_t>::max()) {
            return Unlimited();
        }
        return MaxTopicSize(std::to_string(bytes));
    }

    /**
     * @brief Returns the value passed to the client implementation.
     * @return Topic size option or decimal byte count.
     */
    [[nodiscard]] std::string_view Value() const { return detail::StringTag<MaxTopicSize>::Value(); }

  private:
    explicit MaxTopicSize(std::string max_topic_size) : detail::StringTag<MaxTopicSize>(std::move(max_topic_size)) {}
};

/**
 * @brief Message retention policy for a topic.
 *
 * Use ServerDefault(), NeverExpire(), or Duration() to select the retention
 * policy.
 */
class Expiry final {
  public:
    /** @brief Returns the server-default expiry policy. */
    static Expiry ServerDefault() { return Expiry("server_default", 0); }

    /**
     * @brief Keeps messages until another operation removes them, such as
     *        topic deletion.
     */
    static Expiry NeverExpire() { return Expiry("never_expire", std::numeric_limits<std::uint64_t>::max()); }

    /**
     * @brief Creates a time-based expiry policy.
     * @param micros Message lifetime in microseconds.
     * @return Time-based expiry policy.
     */
    static Expiry Duration(std::uint64_t micros) { return Expiry("duration", micros); }

    /**
     * @brief Returns the expiry policy kind.
     * @return One of server_default, never_expire, or duration.
     */
    [[nodiscard]] std::string_view Kind() const { return expiry_kind_; }

    /**
     * @brief Returns the value associated with the expiry policy.
     * @return Duration in microseconds for Duration(), zero for ServerDefault(),
     *         or std::numeric_limits<std::uint64_t>::max() for NeverExpire().
     */
    [[nodiscard]] std::uint64_t Value() const { return expiry_value_; }

  private:
    explicit Expiry(std::string expiry_kind, std::uint64_t expiry_value)
        : expiry_kind_(std::move(expiry_kind)), expiry_value_(expiry_value) {}

    std::string expiry_kind_;
    std::uint64_t expiry_value_;
};

/**
 * @brief Options for creating a topic.
 *
 * Use the typed setters to configure supported topic settings. Leave a setting
 * unset to use the server default. Use SetRawEntries() for supported options
 * that do not yet have a typed setter. When both specify the same option, the
 * typed setting takes precedence.
 */
class TopicCreateOptions final {
  public:
    TopicCreateOptions() = default;

    /**
     * @brief Returns the number of partitions to create.
     * @return Configured partition count, or `std::nullopt` to use the server
     *         default.
     */
    [[nodiscard]] std::optional<std::uint32_t> PartitionsCount() const noexcept { return partitions_count_; }

    /**
     * @brief Sets the number of partitions to create.
     * @param partitions_count Number of partitions, from 0 to 1,000 inclusive.
     * @return Reference to this options object.
     */
    TopicCreateOptions &SetPartitionsCount(std::uint32_t partitions_count) noexcept {
        partitions_count_ = partitions_count;
        return *this;
    }

    /**
     * @brief Returns the topic storage compression setting.
     * @return Configured compression algorithm, or `std::nullopt` to use the
     *         server default.
     */
    [[nodiscard]] const std::optional<::iggy::CompressionAlgorithm> &CompressionAlgorithm() const noexcept {
        return compression_algorithm_;
    }

    /**
     * @brief Sets the topic storage compression algorithm.
     * @param compression_algorithm Compression algorithm to use.
     * @return Reference to this options object.
     */
    TopicCreateOptions &SetCompressionAlgorithm(::iggy::CompressionAlgorithm compression_algorithm) {
        compression_algorithm_ = std::move(compression_algorithm);
        return *this;
    }

    /**
     * @brief Returns the message retention policy.
     * @return Configured expiry policy, or `std::nullopt` to use the server
     *         default.
     */
    [[nodiscard]] const std::optional<::iggy::Expiry> &MessageExpiry() const noexcept { return message_expiry_; }

    /**
     * @brief Sets the message retention policy.
     * @param message_expiry Expiry policy to apply. Expiry::ServerDefault()
     *        clears an explicitly configured policy.
     * @return Reference to this options object.
     */
    TopicCreateOptions &SetMessageExpiry(::iggy::Expiry message_expiry) {
        if (message_expiry.Kind() == "server_default") {
            message_expiry_.reset();
        } else {
            message_expiry_ = std::move(message_expiry);
        }
        return *this;
    }

    /**
     * @brief Returns the maximum retained topic size.
     * @return Configured size limit, or `std::nullopt` to use the server
     *         default.
     */
    [[nodiscard]] const std::optional<::iggy::MaxTopicSize> &MaxTopicSize() const noexcept { return max_topic_size_; }

    /**
     * @brief Sets the maximum retained topic size.
     * @param max_topic_size Maximum size to retain. The limit cannot be smaller
     *        than the configured segment size.
     * @return Reference to this options object.
     */
    TopicCreateOptions &SetMaxTopicSize(::iggy::MaxTopicSize max_topic_size) {
        if (max_topic_size.Value() == "server_default") {
            max_topic_size_.reset();
        } else {
            max_topic_size_ = std::move(max_topic_size);
        }
        return *this;
    }

    /**
     * @brief Returns the partition segment size.
     * @return Configured segment size in bytes, or `std::nullopt` to use the
     *         server default.
     */
    [[nodiscard]] std::optional<std::uint64_t> SegmentSize() const noexcept { return segment_size_; }

    /**
     * @brief Sets the size at which each partition segment rotates.
     * @param segment_size Segment size in bytes. Specify zero to use the server
     *        default; otherwise it must be a multiple of 512 between 1 MiB and
     *        1 GiB inclusive.
     * @return Reference to this options object.
     */
    TopicCreateOptions &SetSegmentSize(std::uint64_t segment_size) noexcept {
        segment_size_ = segment_size;
        return *this;
    }

    /**
     * @brief Returns whether partition writes are synchronously flushed to disk.
     * @return Configured setting, or `std::nullopt` to use the server default.
     */
    [[nodiscard]] std::optional<bool> EnforceFsync() const noexcept { return enforce_fsync_; }

    /**
     * @brief Sets whether partition writes are synchronously flushed to disk.
     * @param enforce_fsync `true` to fsync writes; `false` otherwise.
     * @return Reference to this options object.
     */
    TopicCreateOptions &SetEnforceFsync(bool enforce_fsync) noexcept {
        enforce_fsync_ = enforce_fsync;
        return *this;
    }

    /**
     * @brief Returns the message-count threshold for flushing the journal.
     * @return Configured threshold, or `std::nullopt` to use the server default.
     */
    [[nodiscard]] std::optional<std::uint32_t> MessagesRequiredToSave() const noexcept {
        return messages_required_to_save_;
    }

    /**
     * @brief Sets the message-count threshold for flushing the journal.
     *
     * The journal is flushed when this or the byte threshold is reached first.
     * @param messages_required_to_save Number of messages, from 1 to 16,777,216
     *        inclusive.
     * @return Reference to this options object.
     */
    TopicCreateOptions &SetMessagesRequiredToSave(std::uint32_t messages_required_to_save) noexcept {
        messages_required_to_save_ = messages_required_to_save;
        return *this;
    }

    /**
     * @brief Returns the byte threshold for flushing the journal.
     * @return Configured threshold in bytes, or `std::nullopt` to use the
     *         server default.
     */
    [[nodiscard]] std::optional<std::uint64_t> SizeOfMessagesRequiredToSave() const noexcept {
        return size_of_messages_required_to_save_;
    }

    /**
     * @brief Sets the byte threshold for flushing the journal.
     *
     * The journal is flushed when this or the message-count threshold is reached
     * first.
     * @param size_of_messages_required_to_save Size in bytes. Specify zero to
     *        use the server default; otherwise it must be between 1 and 1 GiB
     *        inclusive.
     * @return Reference to this options object.
     */
    TopicCreateOptions &SetSizeOfMessagesRequiredToSave(std::uint64_t size_of_messages_required_to_save) noexcept {
        size_of_messages_required_to_save_ = size_of_messages_required_to_save;
        return *this;
    }

    /**
     * @brief Returns whether partition segments are preallocated on disk.
     * @return Configured setting, or `std::nullopt` to use the server default.
     */
    [[nodiscard]] std::optional<bool> PreallocateSegments() const noexcept { return preallocate_segments_; }

    /**
     * @brief Sets whether partition segments are preallocated on disk.
     * @param preallocate_segments `true` to reserve segment space during topic
     *        creation; `false` otherwise.
     * @return Reference to this options object.
     * @note The total preallocated space cannot exceed 64 GiB.
     */
    TopicCreateOptions &SetPreallocateSegments(bool preallocate_segments) noexcept {
        preallocate_segments_ = preallocate_segments;
        return *this;
    }

    /**
     * @brief Returns additional topic settings as key-value pairs.
     *
     * Use this for supported settings that do not have a dedicated setter.
     * @return Ordered map of setting names and values.
     * @note A dedicated setter takes precedence when it configures the same
     *       setting.
     */
    [[nodiscard]] const std::map<std::string, std::string> &RawEntries() const noexcept { return raw_; }

    /**
     * @brief Adds or replaces additional topic settings.
     * @param entries Setting names and values to add.
     * @return Reference to this options object.
     * @note Unsupported names and invalid values are rejected when the topic is
     *       created. Use SetPartitionsCount() rather than an entry for the
     *       partition count.
     */
    TopicCreateOptions &SetRawEntries(const std::map<std::string, std::string> &entries) {
        for (const auto &entry : entries) {
            raw_.insert_or_assign(entry.first, entry.second);
        }
        return *this;
    }
    /**
     * @brief Adds or replaces additional topic settings.
     * @param entries Setting names and values to move into this options object.
     * @return Reference to this options object.
     * @see SetRawEntries(const std::map<std::string, std::string>&)
     */
    TopicCreateOptions &SetRawEntries(std::map<std::string, std::string> &&entries) {
        while (!entries.empty()) {
            auto node = entries.extract(entries.begin());
            raw_.erase(node.key());
            raw_.insert(std::move(node));
        }
        return *this;
    }

  private:
    std::optional<std::uint32_t> partitions_count_;
    std::optional<::iggy::CompressionAlgorithm> compression_algorithm_;
    std::optional<::iggy::Expiry> message_expiry_;
    std::optional<::iggy::MaxTopicSize> max_topic_size_;
    std::optional<std::uint64_t> segment_size_;
    std::optional<bool> enforce_fsync_;
    std::optional<std::uint32_t> messages_required_to_save_;
    std::optional<std::uint64_t> size_of_messages_required_to_save_;
    std::optional<bool> preallocate_segments_;
    std::map<std::string, std::string> raw_;

    friend class IggyBlockingClient;
};

/**
 * @brief Options for updating a topic.
 *
 * Use this class to change a topic's mutable settings. Leave a setting unset
 * to retain its current value. Topic creation settings, such as the partition
 * count and segment size, cannot be changed after the topic is created.
 *
 * Use the typed setters for supported settings. SetRawEntries() can configure
 * other supported mutable settings. When both configure the same setting, the
 * typed setting takes precedence.
 */
class TopicUpdateOptions final {
  public:
    TopicUpdateOptions() = default;

    /**
     * @brief Returns the requested storage compression update.
     * @return Compression algorithm to apply, or `std::nullopt` when this
     *         update leaves compression unchanged.
     */
    [[nodiscard]] const std::optional<::iggy::CompressionAlgorithm> &CompressionAlgorithm() const noexcept {
        return compression_algorithm_;
    }

    /**
     * @brief Sets the storage compression algorithm.
     * @param compression_algorithm Compression algorithm to apply.
     * @return Reference to this options object.
     */
    TopicUpdateOptions &SetCompressionAlgorithm(::iggy::CompressionAlgorithm compression_algorithm) {
        compression_algorithm_ = std::move(compression_algorithm);
        return *this;
    }

    /**
     * @brief Returns the requested message retention update.
     * @return Expiry policy to apply, or `std::nullopt` when this update leaves
     *         retention unchanged.
     */
    [[nodiscard]] const std::optional<::iggy::Expiry> &MessageExpiry() const noexcept { return message_expiry_; }

    /**
     * @brief Sets the message retention policy.
     * @param message_expiry Expiry policy to apply. Expiry::ServerDefault()
     *        leaves the current policy unchanged.
     * @return Reference to this options object.
     */
    TopicUpdateOptions &SetMessageExpiry(::iggy::Expiry message_expiry) {
        if (message_expiry.Kind() == "server_default") {
            message_expiry_.reset();
        } else {
            message_expiry_ = std::move(message_expiry);
        }
        return *this;
    }

    /**
     * @brief Returns the requested maximum retained-size update.
     * @return Size limit to apply, or `std::nullopt` when this update leaves
     *         the limit unchanged.
     */
    [[nodiscard]] const std::optional<::iggy::MaxTopicSize> &MaxTopicSize() const noexcept { return max_topic_size_; }

    /**
     * @brief Sets the maximum retained topic size.
     * @param max_topic_size Maximum size to retain. MaxTopicSize::ServerDefault()
     *        leaves the current limit unchanged.
     * @return Reference to this options object.
     */
    TopicUpdateOptions &SetMaxTopicSize(::iggy::MaxTopicSize max_topic_size) {
        if (max_topic_size.Value() == "server_default") {
            max_topic_size_.reset();
        } else {
            max_topic_size_ = std::move(max_topic_size);
        }
        return *this;
    }

    /**
     * @brief Returns additional mutable topic settings as key-value pairs.
     *
     * Use this for supported settings that do not have a dedicated setter.
     * @return Ordered map of setting names and values.
     * @note A dedicated setter takes precedence when it configures the same
     *       setting.
     */
    [[nodiscard]] const std::map<std::string, std::string> &RawEntries() const noexcept { return raw_; }

    /**
     * @brief Adds or replaces additional mutable topic settings.
     * @param entries Setting names and values to add.
     * @return Reference to this options object.
     * @note Unsupported, immutable, or invalid settings are rejected when the
     *       topic is updated.
     */
    TopicUpdateOptions &SetRawEntries(const std::map<std::string, std::string> &entries) {
        for (const auto &entry : entries) {
            raw_.insert_or_assign(entry.first, entry.second);
        }
        return *this;
    }
    /**
     * @brief Adds or replaces additional mutable topic settings.
     * @param entries Setting names and values to move into this options object.
     * @return Reference to this options object.
     * @see SetRawEntries(const std::map<std::string, std::string>&)
     */
    TopicUpdateOptions &SetRawEntries(std::map<std::string, std::string> &&entries) {
        while (!entries.empty()) {
            auto node = entries.extract(entries.begin());
            raw_.erase(node.key());
            raw_.insert(std::move(node));
        }
        return *this;
    }

  private:
    std::optional<::iggy::CompressionAlgorithm> compression_algorithm_;
    std::optional<::iggy::Expiry> message_expiry_;
    std::optional<::iggy::MaxTopicSize> max_topic_size_;
    std::map<std::string, std::string> raw_;

    friend class IggyBlockingClient;
};

/**
 * @brief Options for updating a stream.
 *
 * Use this class to supply stream settings to UpdateStream(). Currently, Iggy
 * does not support updating stream settings, so the server rejects every
 * supplied setting. The raw entries are retained for compatibility with future
 * server versions that add mutable stream settings.
 */
class StreamUpdateOptions final {
  public:
    StreamUpdateOptions() = default;

    /**
     * @brief Returns the requested stream settings as key-value pairs.
     * @return Ordered map of setting names and values.
     * @note The server currently rejects all stream settings.
     */
    [[nodiscard]] const std::map<std::string, std::string> &RawEntries() const noexcept { return raw_; }

    /**
     * @brief Adds or replaces requested stream settings.
     * @param entries Setting names and values to add.
     * @return Reference to this options object.
     * @note The server currently rejects all stream settings.
     */
    StreamUpdateOptions &SetRawEntries(const std::map<std::string, std::string> &entries) {
        for (const auto &entry : entries) {
            raw_.insert_or_assign(entry.first, entry.second);
        }
        return *this;
    }
    /**
     * @brief Adds or replaces requested stream settings.
     * @param entries Setting names and values to move into this options object.
     * @return Reference to this options object.
     * @see SetRawEntries(const std::map<std::string, std::string>&)
     * @note The server currently rejects all stream settings.
     */
    StreamUpdateOptions &SetRawEntries(std::map<std::string, std::string> &&entries) {
        while (!entries.empty()) {
            auto node = entries.extract(entries.begin());
            raw_.erase(node.key());
            raw_.insert(std::move(node));
        }
        return *this;
    }

  private:
    std::map<std::string, std::string> raw_;

    friend class IggyBlockingClient;
};

/**
 * @brief Starting position for polling messages.
 *
 * @note The strategy kind and value are passed across the Rust FFI as a pair.
 *       The Rust client rejects unsupported kinds.
 */
class PollingStrategy final {
  public:
    /**
     * @brief Starts polling at a message offset.
     * @param value Message offset.
     * @return Offset-based polling strategy.
     */
    static PollingStrategy Offset(std::uint64_t value) { return PollingStrategy("offset", value); }

    /**
     * @brief Starts polling at a timestamp.
     * @param value Timestamp value expected by the Iggy protocol.
     * @return Timestamp-based polling strategy.
     */
    static PollingStrategy Timestamp(std::uint64_t value) { return PollingStrategy("timestamp", value); }

    /** @brief Starts polling with the first message in the partition. */
    static PollingStrategy First() { return PollingStrategy("first", 0); }

    /** @brief Starts polling with the last available message in the partition. */
    static PollingStrategy Last() { return PollingStrategy("last", 0); }

    /**
     * @brief Returns a strategy that starts after the stored consumer offset.
     * @note Typically used with automatic offset commits enabled.
     */
    static PollingStrategy Next() { return PollingStrategy("next", 0); }

    /**
     * @brief Returns the polling strategy kind.
     * @return One of offset, timestamp, first, last, or next.
     */
    [[nodiscard]] std::string_view Kind() const { return polling_strategy_kind_; }

    /**
     * @brief Returns the value associated with the polling strategy.
     * @return Offset or timestamp for parameterized strategies; otherwise zero.
     */
    [[nodiscard]] std::uint64_t Value() const { return polling_strategy_value_; }

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
std::vector<std::uint8_t> to_little_endian_bytes(const Value value) {
    std::vector<std::uint8_t> bytes{};
    bytes.reserve(sizeof(Value));
    for (std::size_t index{}; index < sizeof(Value); ++index) {
        bytes.push_back(static_cast<std::uint8_t>((value >> (index * 8)) & 0xFF));
    }

    return bytes;
}

inline std::vector<std::uint8_t> to_bool_bytes(const bool value) {
    std::vector<std::uint8_t> bytes{};
    bytes.push_back(static_cast<std::uint8_t>(value ? 1 : 0));

    return bytes;
}

inline std::vector<std::uint8_t> to_key_bytes(const std::string_view key) {
    std::vector<std::uint8_t> bytes{};
    bytes.reserve(key.size());
    for (const char character : key) {
        bytes.push_back(static_cast<std::uint8_t>(character));
    }

    return bytes;
}

/// An option key is always `String`-kinded. Only the value kind varies per key.
inline HeaderEntry to_option_entry(const std::string_view key,
                                   const HeaderKind value_kind,
                                   std::vector<std::uint8_t> value) {
    return HeaderEntry::Create(HeaderField::Create(HeaderKind::String, to_key_bytes(key)),
                               HeaderField::Create(value_kind, std::move(value)));
}

}  // namespace detail

enum class Durability { Replicated, Persisted };

constexpr std::string_view to_string(const Durability durability) {
    switch (durability) {
        case Durability::Replicated:
            return "replicated";
        case Durability::Persisted:
            return "persisted";
    }
    throw std::invalid_argument("Unknown durability");
}

/**
 * @brief Creates catalog entries for ResourceOptions::Explicit().
 *
 * Each factory encodes one key from the server's topic option catalog using
 * that key's required value kind. The server rejects unknown keys and values
 * encoded with a different kind.
 *
 * For example, pass
 * `ResourceOptions::Explicit({TopicOption::SegmentSize(1024 * 1024)})` to
 * CreateTopic(). The server rejects unknown keys and values encoded with a
 * different kind.
 *
 * @note These options are accepted only during topic creation. UpdateTopic()
 *       rejects them because they define how partition storage is created.
 *       Changing them later could leave existing and new segments with
 *       different storage settings.
 */
class TopicOption final {
  public:
    /**
     * @brief Set the size at which this topic's segments rotate.
     *
     * Must be a multiple of 512 bytes, at least 1 MiB, and no larger than the
     * server's segment ceiling.
     *
     * @param bytes Segment size in bytes.
     * @return Encoded topic option entry.
     */
    static HeaderEntry SegmentSize(const std::uint64_t bytes) {
        return detail::to_option_entry("segment_size", HeaderKind::Uint64, detail::to_little_endian_bytes(bytes));
    }

    /**
     * @brief Choose the message completion policy.
     *
     * @param value The policy, defaulting to Replicated.
     * @return Encoded topic option entry.
     */
    static HeaderEntry Durability(const ::iggy::Durability value = ::iggy::Durability::Replicated) {
        return detail::to_option_entry("durability", HeaderKind::String, detail::to_key_bytes(to_string(value)));
    }

    /**
     * @brief Choose explicit offset completion independently of message durability.
     * @param value The policy, defaulting to Replicated.
     * @return Encoded topic option entry.
     */
    static HeaderEntry ConsumerOffsetDurability(const ::iggy::Durability value = ::iggy::Durability::Replicated) {
        return detail::to_option_entry("consumer_offset_durability", HeaderKind::String,
                                       detail::to_key_bytes(to_string(value)));
    }

    /**
     * @brief Flush the journal once it holds this many messages.
     *
     * Must be non-zero. Paired with
     * `SizeOfMessagesRequiredToSave(bytes)`: whichever threshold trips
     * first flushes.
     *
     * @param messages Message count at which to flush the journal.
     * @return Encoded topic option entry.
     */
    static HeaderEntry MessagesRequiredToSave(const std::uint32_t messages) {
        return detail::to_option_entry("messages_required_to_save", HeaderKind::Uint32,
                                       detail::to_little_endian_bytes(messages));
    }

    /**
     * @brief Flush the journal once it holds this many bytes.
     *
     * Capped at 1 GiB: a threshold above the largest a segment may be never
     * trips, and the journal does not survive a crash.
     *
     * @param bytes Byte count at which to flush the journal.
     * @return Encoded topic option entry.
     */
    static HeaderEntry SizeOfMessagesRequiredToSave(const std::uint64_t bytes) {
        return detail::to_option_entry("size_of_messages_required_to_save", HeaderKind::Uint64,
                                       detail::to_little_endian_bytes(bytes));
    }

    /**
     * @brief Choose whether a segment's bytes are reserved on disk when it is created.
     *
     * Reserves `segment_size * partitions_count` up front, which the server
     * caps at 64 GiB per topic.
     *
     * @param enabled Whether to reserve segment storage on disk.
     * @return Encoded topic option entry.
     */
    static HeaderEntry PreallocateSegments(const bool enabled) {
        return detail::to_option_entry("preallocate_segments", HeaderKind::Bool, detail::to_bool_bytes(enabled));
    }

  private:
    TopicOption() = delete;
};

/**
 * @brief Owning client connection to an Apache Iggy server.
 *
 * Create instances with Builder or FromConnectionString(). The client owns a
 * handle to the underlying Rust client. Destroying the C++ object releases that
 * handle. The Rust client aborts its heartbeat task when it is dropped.
 *
 * Builder initializes a TCP client. To use QUIC, HTTP, or WebSocket, create the
 * client with FromConnectionString().
 *
 * @code{.cpp}
 * auto client{iggy::IggyBlockingClient::Builder()
 *                 .WithServerAddress("127.0.0.1:8090")
 *                 .Build()};
 * client.Connect();
 * client.Login("iggy", "iggy");
 * client.Shutdown();
 * @endcode
 */
class IggyBlockingClient final {
  public:
    class Builder;

    /** @brief IggyBlockingClient is move-only. */
    IggyBlockingClient(const IggyBlockingClient &)            = delete;
    IggyBlockingClient &operator=(const IggyBlockingClient &) = delete;

    /**
     * @brief Transfers ownership of a client.
     * @param other Client whose connection ownership is transferred.
     *
     * The moved-from client may be destroyed or assigned a new value, but must
     * not be used for client operations.
     */
    IggyBlockingClient(IggyBlockingClient &&other) noexcept;

    /**
     * @brief Replaces this client by taking ownership from another client.
     * @param other Client whose connection ownership is transferred.
     * @return Reference to this client.
     *
     * Any Rust client handle currently owned by this object is released first.
     * Call Shutdown() before replacing a connected client. The moved-from
     * client must not be used for client operations.
     */
    IggyBlockingClient &operator=(IggyBlockingClient &&other) noexcept;

    /**
     * @brief Releases the handle to the underlying Rust client.
     *
     * Dropping the underlying Rust client aborts its heartbeat task. Cleanup
     * errors cannot be reported from the destructor.
     */
    ~IggyBlockingClient();

    /**
     * @brief Creates a client from an Iggy connection string.
     *
     * Connection strings use one of these forms:
     *
     * - `iggy://<credentials>@<host>:<port>[?<options>]` for TCP.
     * - `iggy+tcp://<credentials>@<host>:<port>[?<options>]` for TCP.
     * - `iggy+quic://<credentials>@<host>:<port>[?<options>]` for QUIC.
     * - `iggy+http://<credentials>@<host>:<port>[?<options>]` for HTTP.
     * - `iggy+ws://<credentials>@<host>:<port>[?<options>]` for WebSocket.
     *
     * Credentials are either `<username>:<password>` or a personal access
     * token. Multiple query parameters are separated with `&`.
     *
     * Connection string examples:
     *
     * - Username and password:
     *   `iggy+tcp://iggy:iggy@127.0.0.1:8090`
     * - Personal access token:
     *   `iggy+tcp://iggypat-1234567890abcdef@127.0.0.1:8090`
     * - TCP with TLS:
     *   `iggy+tcp://iggy:iggy@localhost:8090?tls=true&tls_domain=localhost`
     *
     * TCP accepts these query parameters:
     *
     * - `tls=<bool>`
     * - `tls_domain=<string>`
     * - `tls_ca_file=<path>`
     * - `reconnection_retries=<uint32|unlimited>`
     * - `reconnection_interval=<duration>`
     * - `reestablish_after=<duration>`
     * - `heartbeat_interval=<duration>`
     * - `nodelay=<bool>`
     *
     * QUIC accepts these query parameters:
     *
     * - `response_buffer_size=<uint64>`
     * - `max_concurrent_bidi_streams=<uint64>`
     * - `datagram_send_buffer_size=<uint64>`
     * - `initial_mtu=<uint16>`
     * - `send_window=<uint64>`
     * - `receive_window=<uint64>`
     * - `keep_alive_interval=<uint64>`
     * - `max_idle_timeout=<uint64>`
     * - `validate_certificate=<bool>`
     * - `heartbeat_interval=<duration>`
     * - `reconnection_max_retries=<uint32|unlimited>`
     * - `reconnection_interval=<duration>`
     * - `reconnection_reestablish_after=<duration>`
     *
     * HTTP accepts these query parameters:
     *
     * - `heartbeat_interval=<duration>`
     * - `retries=<uint32>`
     *
     * WebSocket accepts these query parameters:
     *
     * - `heartbeat_interval=<duration>`
     * - `reconnection_retries=<uint32|unlimited>`
     * - `reconnection_interval=<duration>`
     * - `reestablish_after=<duration>`
     * - `read_buffer_size=<unsigned integer>`
     * - `write_buffer_size=<unsigned integer>`
     * - `max_write_buffer_size=<unsigned integer>`
     * - `max_message_size=<unsigned integer>`
     * - `max_frame_size=<unsigned integer>`
     * - `accept_unmasked_frames=<bool>`
     * - `tls=<bool>`
     * - `tls_domain=<string>`
     * - `tls_ca_file=<path>`
     * - `tls_validate_certificate=<bool>`
     *
     * Durations use Iggy duration syntax, such as `500ms`, `5s`, or `1min`.
     * Boolean values are `true` or `false`.
     *
     * Credentials embedded in the connection string configure automatic login
     * for Connect() and later reconnections. This method parses configuration
     * but does not establish a network connection.
     *
     * @param connection_string Connection string containing client configuration.
     * @return Configured, disconnected client.
     * @throws IggyException if the connection string is invalid or the client
     *         cannot be created.
     */
    static IggyBlockingClient FromConnectionString(std::string connection_string);

    /**
     * @brief Connects to the configured Iggy server.
     *
     * Establishes the configured transport connection and starts heartbeat
     * processing. If automatic login was configured, authentication is also
     * performed.
     *
     * @note HTTP is stateless; connecting initializes heartbeat processing but
     *       does not open a persistent transport connection.
     * @note Repeated calls do not start additional heartbeat tasks. An existing
     *       heartbeat task is reused while it is still running.
     * @note The default reconnection limit is unlimited. If the server remains
     *       unavailable, this method keeps retrying and blocks the caller. Use
     *       WithReconnectionMaxRetries() to bound the wait.
     * @throws IggyException if automatic authentication fails, or if a finite
     *         reconnection limit is configured and exhausted.
     */
    void Connect();

    /**
     * @brief Disconnects from the configured Iggy server.
     *
     * Disconnect is temporary. It drops the active transport connection and
     * changes the client state to disconnected, but keeps the client reusable.
     * Call Connect() to establish a new connection. Configured automatic login
     * is applied when reconnecting.
     *
     * @note Disconnect() does not stop the existing heartbeat task. With
     *       automatic login configured, a heartbeat may reconnect and
     *       authenticate the client in the background.
     * @note The HTTP transport is stateless and treats this operation as a
     *       no-op.
     * @throws IggyException if the client cannot disconnect cleanly.
     * @see Shutdown()
     */
    void Disconnect();

    /**
     * @brief Shuts down the client and its background tasks.
     *
     * Shutdown is terminal for stateful transports. It gracefully closes the
     * active transport where supported, releases transport resources, and
     * changes the client state to shutdown. Binary operations then fail with a
     * client-shutdown error. The background heartbeat task stops when it next
     * observes that error. Create a new client instead of reusing a shut-down
     * client.
     *
     * @note The HTTP transport is stateless and treats this operation as a
     *       no-op.
     * @throws IggyException if shutdown fails.
     * @see Disconnect()
     */
    void Shutdown();

    /**
     * @brief Authenticates with a username and password.
     *
     * For TCP, QUIC, and WebSocket, call Connect() first. A successful login
     * leaves the transport connected and marks the session authenticated. For
     * HTTP, the returned access token is stored by the client and used for
     * subsequent authenticated requests.
     *
     * @param username Iggy user name.
     * @param password Iggy user password.
     * @return Information about the authenticated session.
     * @throws IggyException if authentication fails.
     */
    LoginInfo Login(std::string username, std::string password);

    /**
     * @brief Ends the current authenticated session.
     *
     * Logout does not disconnect the transport. For binary transports, the
     * client returns to the connected but unauthenticated state. For HTTP, the
     * stored access token is cleared after the server accepts the logout.
     * Protected operations require another successful Login() or an automatic
     * login during reconnection.
     *
     * @throws IggyException if logout fails.
     * @see Disconnect()
     */
    void Logout();

    /**
     * @brief Creates a top-level stream in the cluster metadata.
     *
     * A stream is the top-level namespace for topics. This creates no topics,
     * partitions, or messages. Its name must be unique, non-empty, and no more
     * than 255 UTF-8 bytes.
     *
     * A transport failure after submission can leave the stream created. Look
     * it up by name before retrying or choosing another name.
     *
     * @param name Unique stream name.
     * @return Details of the newly created, topic-less stream.
     * @throws IggyException if the client is unavailable or unauthenticated;
     *         the name is invalid or already in use; the caller lacks
     *         stream-management permission; or the request fails.
     */
    StreamDetails CreateStream(std::string name);

    /**
     * @brief Renames a stream.
     *
     * @param stream Stream to rename, addressed by numeric ID or name.
     * @param name New unique stream name.
     * @param options Stream update options (currently no updatable keys; `raw`
     *        carries forward-compatible keys, each rejected until catalogued).
     * @throws IggyException if the client is unavailable, the caller lacks
     *         stream-management permission, either value is invalid, the stream
     *         does not exist, the name is already taken, or the request fails.
     */
    void UpdateStream(const Identifier &stream, std::string name, const StreamUpdateOptions &options = {});

    /**
     * @brief Lists stream summaries visible to the authenticated user.
     *
     * The summaries exclude per-topic details. Use GetStream() for those.
     * @return Stream summaries visible to the authenticated user.
     * @throws IggyException if the client is unavailable, the caller lacks
     *         permission to read streams, or the request fails.
     */
    std::vector<Stream> GetStreams();

    /**
     * @brief Retrieves one stream by numeric ID or name.
     *
     * The result includes observed aggregate statistics and topic summaries.
     * It does not include partition details or messages, and its statistics can
     * become stale immediately after the request completes.
     *
     * @param stream Stream to retrieve. Numeric IDs remain stable if a stream
     *        is renamed.
     * @return Details for the requested stream.
     * @throws IggyException if the client is unavailable or unauthenticated;
     *         the stream does not exist; the caller lacks read permission; or
     *         the metadata read fails.
     */
    StreamDetails GetStream(const Identifier &stream);

    /**
     * @brief Deletes a stream and all of its topics, partitions, and messages.
     *
     * This is irreversible. A transport failure after submission can leave the
     * deletion committed, so query the stream before retrying this request.
     *
     * @param stream Stream to delete, addressed by numeric ID or name.
     * @throws IggyException if the client is unavailable, the caller lacks
     *         stream-management permission, the stream does not exist, or the
     *         request fails.
     */
    void DeleteStream(const Identifier &stream);

    /**
     * @brief Removes all messages from every topic in a stream.
     *
     * The stream, its topics, and topic configuration remain available. A
     * transport failure after submission can still leave the purge committed.
     * @param stream Stream to purge, addressed by numeric ID or name.
     * @throws IggyException if the client is unavailable, the caller lacks
     *         stream-management permission, the stream does not exist, or the
     *         request fails.
     */
    void PurgeStream(const Identifier &stream);

    /**
     * @brief Creates a topic and its initial partitions in a stream.
     *
     * The server creates the topic's initial partitions and applies the
     * supplied TopicCreateOptions. Settings left unset use the server default.
     * Use SetRawEntries() for supported settings without a dedicated setter.
     * A dedicated setter takes precedence when it configures the same setting.
     *
     * @param stream Parent stream, addressed by numeric ID or name.
     * @param name Unique topic name within @p stream.
     * @param options Topic creation options.
     * @return Metadata and initial partition summaries for the created topic.
     * @throws IggyException if the client is unavailable or unauthenticated;
     *         an identifier, name, partition count, or option is invalid; the
     *         stream does not exist; the caller lacks topic-management
     *         permission; or the server rejects or cannot commit the write.
     */
    TopicDetails CreateTopic(const Identifier &stream, std::string name, const TopicCreateOptions &options = {});

    /**
     * @brief Renames a topic and updates its mutable configuration.
     *
     * The supplied TopicUpdateOptions changes only the settings it contains;
     * settings left unset retain their current values. Topic creation settings,
     * such as the partition count and segment size, cannot be changed after the
     * topic is created. Use SetRawEntries() for supported mutable settings
     * without a dedicated setter.
     *
     * @param stream Parent stream, addressed by numeric ID or name.
     * @param topic Topic to update, addressed by numeric ID or name.
     * @param name New unique topic name within @p stream.
     * @param options Topic update options.
     * @throws IggyException if the client is unavailable or unauthenticated;
     *         an identifier, name, setting, or option is invalid; the stream or
     *         topic does not exist; the caller lacks permission; or the server
     *         rejects or cannot commit the write.
     */
    void UpdateTopic(const Identifier &stream,
                     const Identifier &topic,
                     std::string name,
                     const TopicUpdateOptions &options = {});

    /**
     * @brief Lists topic summaries in a stream.
     *
     * The returned summaries do not include partition details. Use GetTopic()
     * when partition offsets, sizes, and segment counts are needed.
     *
     * @param stream Parent stream, addressed by numeric ID or name.
     * @return Topic summaries visible to the authenticated user.
     * @throws IggyException if the client is unavailable or unauthenticated;
     *         the stream does not exist; the caller lacks read permission; or
     *         the metadata read fails.
     */
    std::vector<Topic> GetTopics(const Identifier &stream);

    /**
     * @brief Retrieves one topic and its partition summaries.
     *
     * The result is an observed metadata read. Partition offsets and retained
     * statistics can change immediately after this call returns.
     *
     * @param stream Parent stream, addressed by numeric ID or name.
     * @param topic Topic to retrieve, addressed by numeric ID or name.
     * @return Topic metadata and one summary per partition.
     * @throws IggyException if the client is unavailable or unauthenticated;
     *         the stream or topic does not exist; the caller lacks read
     *         permission; or the metadata read fails.
     */
    TopicDetails GetTopic(const Identifier &stream, const Identifier &topic);

    /**
     * @brief Deletes a topic, its partitions, and retained messages.
     *
     * A failed or unknown transport outcome can leave the deletion committed.
     * Query the topic before retrying a destructive request.
     *
     * @param stream Parent stream, addressed by numeric ID or name.
     * @param topic Topic to delete, addressed by numeric ID or name.
     * @throws IggyException if the client is unavailable or unauthenticated;
     *         the stream or topic does not exist; the caller lacks
     *         topic-management permission; or the server rejects or cannot
     *         commit the write.
     */
    void DeleteTopic(const Identifier &stream, const Identifier &topic);

    /**
     * @brief Removes retained messages from every partition of a topic.
     *
     * The topic, its partitions, names, and configuration remain. New messages
     * can be sent after a purge. A failed or unknown transport outcome can
     * still leave the purge committed.
     *
     * @param stream Parent stream, addressed by numeric ID or name.
     * @param topic Topic to purge, addressed by numeric ID or name.
     * @throws IggyException if the client is unavailable or unauthenticated;
     *         the stream or topic does not exist; the caller lacks
     *         topic-management permission; or the server rejects or cannot
     *         commit the write.
     */
    void PurgeTopic(const Identifier &stream, const Identifier &topic);

    /**
     * @brief Adds partitions to a topic.
     *
     * New partitions receive IDs after the topic's existing partitions. The
     * requested count must be between 1 and 1000. A transport failure after
     * submission can still leave the partitions created.
     *
     * @param stream Parent stream, addressed by numeric ID or name.
     * @param topic Topic to extend, addressed by numeric ID or name.
     * @param partitions_count Number of partitions to add.
     * @throws IggyException if the client is unavailable or unauthenticated;
     *         an identifier or count is invalid; the stream or topic does not
     *         exist; the caller lacks topic-management permission; or the
     *         request fails.
     */
    void CreatePartitions(const Identifier &stream, const Identifier &topic, std::uint32_t partitions_count);

    /**
     * @brief Deletes the highest-numbered partitions from a topic.
     *
     * The deleted partitions and their retained messages are removed. The
     * requested count must be between 1 and 1000. A transport failure after
     * submission can still leave the deletion committed.
     *
     * @param stream Parent stream, addressed by numeric ID or name.
     * @param topic Topic to shrink, addressed by numeric ID or name.
     * @param partitions_count Number of partitions to delete.
     * @throws IggyException if the client is unavailable or unauthenticated;
     *         an identifier or count is invalid; the stream or topic does not
     *         exist; the caller lacks topic-management permission; or the
     *         request fails.
     */
    void DeletePartitions(const Identifier &stream, const Identifier &topic, std::uint32_t partitions_count);

  private:
    explicit IggyBlockingClient(ffi::Client *client);

    template <typename Operation>
    static decltype(auto) RethrowAsIggyException(Operation &&operation) {
        try {
            return std::forward<Operation>(operation)();
        } catch (const std::exception &error) {
            throw IggyException(error.what());
        }
    }

    [[nodiscard]] ffi::Client *Handle() const;
    void Reset() noexcept;

    ffi::Client *client_;
};

/**
 * @brief Fluent builder for IggyBlockingClient.
 *
 * The builder creates TCP clients only. Use
 * IggyBlockingClient::FromConnectionString() to select another transport.
 * Configuration methods return the builder by reference and may be chained.
 * Unless documented otherwise, settings are validated and applied by Build().
 */
class IggyBlockingClient::Builder final {
  public:
    /**
     * @brief Creates a builder with the default TCP endpoint, 127.0.0.1:8090.
     *
     * Automatic login and TLS are disabled. Reconnection is enabled with
     * unlimited retries, a one-second retry interval, and a five-second delay
     * before reestablishing a previously working connection. The heartbeat
     * interval is five seconds. TCP_NODELAY is disabled. Build() always returns
     * a disconnected client.
     */
    Builder();

    /**
     * @brief Sets the TCP server address.
     *
     * The address is trimmed and validated during Build(). Host names, IPv4,
     * and bracketed IPv6 are accepted. A non-zero port is required.
     *
     * @param server_address Server address in host:port form.
     * @return Reference to this builder.
     * @throws IggyException if @p server_address is empty.
     * @note Build() throws IggyException if the address is invalid.
     */
    Builder &WithServerAddress(std::string server_address);

    /**
     * @brief Enables automatic authentication with user credentials.
     *
     * The credentials are used whenever Connect() establishes a connection,
     * including reconnections. This replaces a previously configured personal
     * access token.
     *
     * @param username Iggy user name.
     * @param password Iggy user password.
     * @return Reference to this builder.
     * @throws IggyException if either credential is empty.
     * @see IggyBlockingClient::Connect()
     * @see IggyBlockingClient::Login()
     */
    Builder &WithAutoLogin(std::string username, std::string password);

    /**
     * @brief Enables automatic authentication with a personal access token.
     *
     * The token is used whenever Connect() establishes a connection, including
     * reconnections. This replaces previously configured username and password
     * credentials.
     *
     * @param token Personal access token.
     * @return Reference to this builder.
     * @throws IggyException if the token is empty.
     * @see IggyBlockingClient::Connect()
     */
    Builder &WithPersonalAccessToken(std::string token);

    /**
     * @brief Sets the maximum number of reconnection attempts.
     *
     * Reconnection is enabled by default. A value of zero disables retries
     * after the initial connection attempt. This replaces a previous call to
     * WithoutReconnectionLimit().
     *
     * @param retries Maximum number of attempts.
     * @return Reference to this builder.
     */
    Builder &WithReconnectionMaxRetries(std::uint32_t retries);

    /**
     * @brief Removes the limit on reconnection attempts.
     *
     * This is the default and replaces a previous finite retry limit.
     *
     * @return Reference to this builder.
     */
    Builder &WithoutReconnectionLimit();

    /**
     * @brief Sets the delay between reconnection attempts.
     *
     * The default interval is one second. This interval applies between failed
     * connection attempts.
     *
     * @param interval Non-negative reconnection interval.
     * @return Reference to this builder.
     * @throws IggyException if @p interval is negative.
     */
    Builder &WithReconnectionInterval(std::chrono::microseconds interval);

    /**
     * @brief Sets the delay before restoring a lost established connection.
     *
     * The default delay is five seconds. This cooldown is distinct from the
     * interval between failed connection attempts.
     *
     * @param duration Non-negative delay.
     * @return Reference to this builder.
     * @throws IggyException if @p duration is negative.
     */
    Builder &WithReestablishAfter(std::chrono::microseconds duration);

    /**
     * @brief Enables or disables TLS.
     *
     * TLS is disabled by default. TLS domain, CA file, and certificate
     * validation settings require TLS to be enabled.
     *
     * @param enabled Whether TLS is enabled.
     * @return Reference to this builder.
     */
    Builder &WithTlsEnabled(bool enabled = true);

    /**
     * @brief Sets the domain used for TLS server-name verification.
     *
     * When omitted, the domain is derived from the configured server address.
     * Build() throws IggyException if this is set while TLS is disabled.
     *
     * @param domain TLS domain name.
     * @return Reference to this builder.
     * @throws IggyException if @p domain is empty.
     */
    Builder &WithTlsDomain(std::string domain);

    /**
     * @brief Sets the certificate-authority file used by TLS.
     *
     * When omitted, system root certificates are used. This setting has no
     * effect unless TLS is enabled. Build() throws IggyException if a path is
     * set while TLS is disabled.
     *
     * @param path Path to a PEM-encoded certificate-authority file.
     * @return Reference to this builder.
     * @throws IggyException if @p path is empty.
     */
    Builder &WithTlsCaFile(std::string path);

    /**
     * @brief Enables or disables TLS certificate validation.
     *
     * Certificate validation is enabled by default. Disabling it accepts
     * certificates without verifying their trust chain or server identity and
     * should be limited to controlled development environments. This setting
     * requires TLS; Build() throws IggyException if certificate validation is
     * configured while TLS is disabled.
     *
     * @param enabled Whether the server certificate is validated.
     * @return Reference to this builder.
     */
    Builder &WithTlsCertificateValidation(bool enabled = true);

    /**
     * @brief Enables TCP_NODELAY on the client socket.
     *
     * TCP_NODELAY disables Nagle's algorithm to reduce latency for small
     * writes, potentially increasing packet count. It is disabled by default.
     *
     * @return Reference to this builder.
     */
    Builder &WithNoDelay();

    /**
     * @brief Builds an owning Iggy blocking client.
     *
     * Build() validates the TCP configuration and creates an independent
     * client. The builder is not consumed and may be reused. The returned
     * client is always disconnected; call IggyBlockingClient::Connect()
     * explicitly before using operations that require a connection.
     *
     * @return Configured client.
     * @throws IggyException if validation or client creation fails.
     */
    IggyBlockingClient Build() const;

  private:
    std::string server_address_{};
    ffi::AutoLoginKind auto_login_kind_{ffi::AutoLoginKind::Disabled};
    std::string auto_login_username_{};
    std::string auto_login_password_{};
    std::string personal_access_token_{};
    std::optional<std::uint32_t> reconnection_max_retries_{};
    std::optional<std::uint64_t> reconnection_interval_micros_{};
    std::optional<std::uint64_t> reestablish_after_micros_{};
    bool tls_enabled_{};
    std::string tls_domain_{};
    std::string tls_ca_file_{};
    std::optional<bool> tls_validate_certificate_{};
    bool no_delay_{};
};

}  // namespace iggy
