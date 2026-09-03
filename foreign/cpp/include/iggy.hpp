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
 * The value identifies the authenticated user. HTTP logins also return the
 * access token retained by the client for later requests. Stateful transports
 * do not return an access token. The token is a credential: callers must not
 * write it to logs or expose it to untrusted code.
 */
class LoginInfo final {
  public:
    /**
     * @brief Returns the numeric ID of the authenticated user.
     * @return Numeric user ID.
     */
    std::uint32_t UserId() const noexcept { return user_id_; }

    /**
     * @brief Returns the HTTP access token when the login returned one.
     * @return Empty when the selected transport does not use an access token.
     *         A returned string view remains valid while this LoginInfo object
     *         is not modified or destroyed.
     */
    std::optional<std::string_view> AccessToken() const noexcept {
        if (!access_token_) {
            return std::nullopt;
        }
        return *access_token_;
    }

    /**
     * @brief Returns the access-token expiry when a token was returned.
     * @return Empty when no access token was returned; otherwise the
     *         server-provided expiry value.
     */
    std::optional<std::uint64_t> AccessTokenExpiry() const noexcept { return access_token_expiry_; }

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
 * An identifier is either a numeric server ID or a name. The factories enforce
 * the protocol's non-empty, 255-byte name limit. Numeric zero is valid because
 * the server can assign zero as a resource ID.
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
    Kind Type() const noexcept { return kind_; }

    /**
     * @brief Returns the identifier payload.
     * @return Numeric ID for Kind::Numeric, or a view of the name for
     *         Kind::String. The string view remains valid while this
     *         Identifier object is not modified or destroyed.
     */
    std::variant<std::uint32_t, std::string_view> Value() const noexcept {
        if (kind_ == Kind::Numeric) {
            return std::get<std::uint32_t>(value_);
        }
        return std::string_view(std::get<std::string>(value_));
    }

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
 * The tag and bytes use Iggy's header codec. Numeric payloads are little
 * endian. Create TopicOption values instead of encoding catalog options by
 * hand; use HeaderField directly only for application-defined headers.
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
 * HeaderField maps directly to Rust's header-field representation. Create()
 * preserves the supplied bytes and does not verify that they match @p kind.
 * Invalid key or value encodings are rejected when a request is sent.
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
    HeaderKind Kind() const noexcept { return kind_; }

    /**
     * @brief Returns bytes owned by this field.
     * @return Payload encoded according to Kind().
     */
    const std::vector<std::uint8_t> &Value() const noexcept { return value_; }

  private:
    HeaderField(HeaderKind kind, std::vector<std::uint8_t> value) : kind_(kind), value_(std::move(value)) {}

    static HeaderField FromFfi(ffi::HeaderField field);
    static ffi::HeaderField ToFfi(HeaderField field);

    friend class HeaderEntry;

    HeaderKind kind_;
    std::vector<std::uint8_t> value_;
};

/**
 * @brief One typed header key-value pair.
 *
 * This maps directly to Rust's HeaderEntry. Topic options cross the C++ bridge
 * as header entries because their keys and values use the same typed codec as
 * message user headers.
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
    const HeaderField &Key() const noexcept { return key_; }

    /**
     * @brief Returns the typed value.
     * @return Value owned by this entry.
     */
    const HeaderField &Value() const noexcept { return value_; }

  private:
    HeaderEntry(HeaderField key, HeaderField value) : key_(std::move(key)), value_(std::move(value)) {}

    static HeaderEntry FromFfi(ffi::HeaderEntry entry);
    static ffi::HeaderEntry ToFfi(HeaderEntry entry);

    friend class ResourceOptions;

    HeaderField key_;
    HeaderField value_;
};

/**
 * @brief Creation options attached to a stream or topic.
 *
 * This maps to Rust's ResourceOptions. Explicit() contains entries supplied by
 * the creating client. Derived() contains values the server resolved from its
 * configuration at admission time. Derived values describe that resource's
 * history and may differ if the resource is recreated under another server
 * configuration.
 *
 * Only explicit entries are sent; derived entries returned by Options() are
 * not resubmitted. For topic creation prefer `TopicCreateOptions`.
 */
class ResourceOptions final {
  public:
    /**
     * @brief Creates an empty option collection.
     * @return Resource options with no explicit or derived entries.
     */
    static ResourceOptions Empty() { return ResourceOptions({}); }

    /**
     * @brief Creates request options from entries selected by the caller.
     * @param entries Explicit option entries.
     * @return Resource options that will submit @p entries.
     */
    static ResourceOptions Explicit(std::vector<HeaderEntry> entries);

    /**
     * @brief Returns entries supplied explicitly at resource creation.
     * @return Explicit entries owned by this option collection.
     */
    const std::vector<HeaderEntry> &Explicit() const noexcept { return explicit_; }

    /**
     * @brief Returns entries derived from configured defaults at admission.
     * @return Derived entries owned by this option collection.
     * @note Stream responses currently expose explicit entries only, so this
     *       collection is empty for Stream and StreamDetails.
     */
    const std::vector<HeaderEntry> &Derived() const noexcept { return derived_; }

  private:
    explicit ResourceOptions(std::vector<HeaderEntry> explicit_entries) : explicit_(std::move(explicit_entries)) {}
    ResourceOptions(std::vector<HeaderEntry> explicit_entries, std::vector<HeaderEntry> derived_entries)
        : explicit_(std::move(explicit_entries)), derived_(std::move(derived_entries)) {}

    static ResourceOptions FromFfi(rust::Vec<ffi::HeaderEntry> explicit_entries,
                                   rust::Vec<ffi::HeaderEntry> derived_entries);
    static rust::Vec<ffi::HeaderEntry> ToFfi(ResourceOptions options);

    friend class IggyBlockingClient;
    friend class Topic;
    friend class TopicDetails;
    friend class Stream;
    friend class StreamDetails;

    std::vector<HeaderEntry> explicit_;
    std::vector<HeaderEntry> derived_;
};

/**
 * @brief Topic summary returned within StreamDetails.
 *
 * A Topic owns its string fields and contains no Rust bridge values. It
 * describes server state observed during the enclosing stream read, not a live
 * view. Its aggregate statistics and partition count can change immediately
 * after that request completes.
 *
 * Options() distinguishes values selected by the creating client from values
 * the server derived during admission. Partition details require GetTopic().
 */
class Topic final {
  public:
    /**
     * @brief Returns the numeric topic ID assigned within its stream.
     * @return Numeric topic ID.
     */
    std::uint32_t Id() const noexcept { return id_; }

    /**
     * @brief Returns the server creation timestamp.
     * @return Timestamp in microseconds.
     */
    std::uint64_t CreatedAt() const noexcept { return created_at_; }

    /**
     * @brief Returns the topic name.
     * @return Name owned by this value.
     */
    const std::string &Name() const noexcept { return name_; }

    /**
     * @brief Returns the aggregate retained topic size.
     * @return Size in bytes.
     */
    std::uint64_t SizeBytes() const noexcept { return size_bytes_; }

    /**
     * @brief Returns the server-encoded message retention value.
     * @return Retention value in microseconds or a protocol sentinel.
     */
    std::uint64_t MessageExpiry() const noexcept { return message_expiry_; }

    /**
     * @brief Returns the server-selected storage compression algorithm.
     * @return Algorithm name owned by this value.
     */
    const std::string &CompressionAlgorithm() const noexcept { return compression_algorithm_; }

    /**
     * @brief Returns the configured maximum retained topic size.
     * @return Maximum size in bytes.
     */
    std::uint64_t MaxTopicSize() const noexcept { return max_topic_size_; }

    /**
     * @brief Returns the aggregate number of retained messages.
     * @return Message count.
     */
    std::uint64_t MessagesCount() const noexcept { return messages_count_; }

    /**
     * @brief Returns the number of partitions belonging to this topic.
     * @return Partition count.
     */
    std::uint32_t PartitionsCount() const noexcept { return partitions_count_; }

    /**
     * @brief Returns topic creation options and their admission provenance.
     * @return Options owned by this value.
     */
    const ResourceOptions &Options() const noexcept { return options_; }

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
 * This is an observed summary, not a live partition handle. Offsets and
 * statistics can change immediately after GetTopic() returns.
 */
class Partition final {
  public:
    /**
     * @brief Returns the numeric partition ID within its topic.
     * @return Numeric partition ID.
     */
    std::uint32_t Id() const noexcept { return id_; }

    /**
     * @brief Returns the server creation timestamp.
     * @return Timestamp in microseconds.
     */
    std::uint64_t CreatedAt() const noexcept { return created_at_; }

    /**
     * @brief Returns the number of retained storage segments.
     * @return Segment count.
     */
    std::uint32_t SegmentsCount() const noexcept { return segments_count_; }

    /**
     * @brief Returns the current server-observed message offset.
     * @return Current message offset.
     */
    std::uint64_t CurrentOffset() const noexcept { return current_offset_; }

    /**
     * @brief Returns the retained partition size.
     * @return Size in bytes.
     */
    std::uint64_t SizeBytes() const noexcept { return size_bytes_; }

    /**
     * @brief Returns the number of retained messages.
     * @return Message count.
     */
    std::uint64_t MessagesCount() const noexcept { return messages_count_; }

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
 * @brief Topic metadata and partition summaries returned by topic detail calls.
 *
 * Partitions() contains one observed summary per partition. It does not expose
 * segment metadata, messages, consumer offsets, or consumer-group membership.
 */
class TopicDetails final {
  public:
    /**
     * @brief Returns the numeric topic ID within its stream.
     * @return Numeric topic ID.
     */
    std::uint32_t Id() const noexcept { return id_; }

    /**
     * @brief Returns the server creation timestamp.
     * @return Timestamp in microseconds.
     */
    std::uint64_t CreatedAt() const noexcept { return created_at_; }

    /**
     * @brief Returns the topic name.
     * @return Name owned by this value.
     */
    const std::string &Name() const noexcept { return name_; }

    /**
     * @brief Returns the aggregate retained topic size.
     * @return Size in bytes.
     */
    std::uint64_t SizeBytes() const noexcept { return size_bytes_; }

    /**
     * @brief Returns the server-encoded message retention value.
     * @return Retention value in microseconds or a protocol sentinel.
     */
    std::uint64_t MessageExpiry() const noexcept { return message_expiry_; }

    /**
     * @brief Returns the storage compression algorithm selected for this topic.
     * @return Algorithm name owned by this value.
     */
    const std::string &CompressionAlgorithm() const noexcept { return compression_algorithm_; }

    /**
     * @brief Returns the maximum retained size configured for this topic.
     * @return Maximum size in bytes.
     */
    std::uint64_t MaxTopicSize() const noexcept { return max_topic_size_; }

    /**
     * @brief Returns the aggregate number of retained messages.
     * @return Message count.
     */
    std::uint64_t MessagesCount() const noexcept { return messages_count_; }

    /**
     * @brief Returns the number of partitions belonging to this topic.
     * @return Partition count.
     */
    std::uint32_t PartitionsCount() const noexcept { return partitions_count_; }

    /**
     * @brief Returns partition summaries.
     * @return Summaries owned by this value.
     */
    const std::vector<Partition> &Partitions() const noexcept { return partitions_; }

    /**
     * @brief Returns topic creation options and their admission provenance.
     * @return Options owned by this value.
     */
    const ResourceOptions &Options() const noexcept { return options_; }

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

namespace detail {
/** @brief Internal base for string-backed option types. */
template <typename Tag>
class StringTag {
  protected:
    explicit StringTag(std::string value) : value_(std::move(value)) {}
    ~StringTag() = default;

    std::string_view Value() const { return value_; }

  private:
    std::string value_;
};

}  // namespace detail

/**
 * @brief Snapshot of one stream's metadata and aggregate statistics.
 *
 * CreateStream() and GetStream() return this value after converting the Rust
 * bridge response to ordinary C++ fields. It owns its name and topic
 * collection, so no bridge-owned string or container leaks through the public
 * API.
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
     */
    std::uint32_t Id() const noexcept { return id_; }

    /**
     * @brief Returns the server-recorded creation timestamp.
     * @return Timestamp in microseconds.
     */
    std::uint64_t CreatedAt() const noexcept { return created_at_; }

    /**
     * @brief Returns the unique stream name observed by the server.
     * @return Reference owned by this value. It remains valid until this
     *         StreamDetails object is modified or destroyed.
     */
    const std::string &Name() const noexcept { return name_; }

    /**
     * @brief Returns the aggregate retained size of all stream topics.
     * @return Size in bytes observed by the server for this request.
     */
    std::uint64_t SizeBytes() const noexcept { return size_bytes_; }

    /**
     * @brief Returns the aggregate number of messages in all stream topics.
     * @return Message count observed by the server for this request.
     */
    std::uint64_t MessagesCount() const noexcept { return messages_count_; }

    /**
     * @brief Returns the number of topics belonging to the stream.
     * @return Topic count observed by the server for this request.
     */
    std::uint32_t TopicsCount() const noexcept { return topics_count_; }

    /**
     * @brief Returns the topic summaries observed by the server.
     * @return Topic values owned by this StreamDetails object.
     */
    const std::vector<Topic> &Topics() const noexcept { return topics_; }

    /**
     * @brief Returns explicit stream creation options.
     * @return Options owned by this value.
     * @note The current bridge does not return derived stream options.
     */
    const ResourceOptions &Options() const noexcept { return options_; }

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
 * @brief Summary of a stream returned by GetStreams().
 *
 * Unlike StreamDetails, this value does not include topic summaries. Its
 * aggregate statistics describe the state observed by the server for one
 * request and can become stale when another client modifies the stream.
 */
class Stream final {
  public:
    /**
     * @brief Returns the numeric ID assigned by the server.
     * @return Numeric stream ID.
     */
    std::uint32_t Id() const noexcept { return id_; }

    /**
     * @brief Returns the server-recorded creation timestamp.
     * @return Timestamp in microseconds.
     */
    std::uint64_t CreatedAt() const noexcept { return created_at_; }

    /**
     * @brief Returns the stream name.
     * @return Name owned by this value.
     */
    const std::string &Name() const noexcept { return name_; }

    /**
     * @brief Returns the aggregate retained stream size.
     * @return Size in bytes.
     */
    std::uint64_t SizeBytes() const noexcept { return size_bytes_; }

    /**
     * @brief Returns the aggregate number of retained stream messages.
     * @return Message count.
     */
    std::uint64_t MessagesCount() const noexcept { return messages_count_; }

    /**
     * @brief Returns the number of topics belonging to the stream.
     * @return Topic count.
     */
    std::uint32_t TopicsCount() const noexcept { return topics_count_; }

    /**
     * @brief Returns explicit stream creation options.
     * @return Options owned by this value.
     * @note The current bridge does not return derived stream options.
     */
    const ResourceOptions &Options() const noexcept { return options_; }

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
     * @brief Returns the value passed to the client implementation.
     * @return Compression algorithm name.
     */
    std::string_view CompressionAlgorithmValue() const { return Value(); }

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
     * @brief Returns the value passed to the client implementation.
     * @return Snapshot compression algorithm name.
     */
    std::string_view SnapshotCompressionValue() const { return Value(); }

  private:
    explicit SnapshotCompression(std::string snapshot_compression)
        : detail::StringTag<SnapshotCompression>(std::move(snapshot_compression)) {}
};

/**
 * @brief Selects data to include in a system snapshot.
 *
 * @note Each selected value is passed across the Rust FFI as a string. The
 *       Rust client rejects unsupported values.
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
    std::string_view SnapshotTypeValue() const { return Value(); }

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
 * @note The value is passed across the Rust FFI as a string. The Rust parser
 *       accepts server_default, unlimited, and decimal byte counts. Zero maps
 *       to server_default, and std::numeric_limits<std::uint64_t>::max() maps
 *       to unlimited. The Rust client rejects unsupported values.
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
    std::string_view MaxTopicSizeValue() const { return Value(); }

  private:
    explicit MaxTopicSize(std::string max_topic_size) : detail::StringTag<MaxTopicSize>(std::move(max_topic_size)) {}
};

/**
 * @brief Message retention policy for a topic.
 *
 * @note The expiry kind and value are passed across the Rust FFI as a pair.
 *       The Rust client rejects unsupported kinds.
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
    std::string_view ExpiryKind() const { return expiry_kind_; }

    /**
     * @brief Returns the value associated with the expiry policy.
     * @return Duration in microseconds for Duration(), zero for ServerDefault(),
     *         or std::numeric_limits<std::uint64_t>::max() for NeverExpire().
     */
    std::uint64_t ExpiryValue() const { return expiry_value_; }

  private:
    explicit Expiry(std::string expiry_kind, std::uint64_t expiry_value)
        : expiry_kind_(std::move(expiry_kind)), expiry_value_(expiry_value) {}

    std::string expiry_kind_;
    std::uint64_t expiry_value_;
};

/**
 * @brief Options for creating a topic.
 *
 * Mirrors Rust `TopicCreateOptions` (`core/common/src/types/options/mod.rs:698`).
 * Each `std::optional` field corresponds to a catalog key; `std::nullopt`
 * means the server default is used and the key is omitted from the request.
 * `Raw` carries forward-compatible string keys; a typed field wins on
 * collision when encoded. The wire form is the topic options TLV block
 * (`core/binary_protocol/src/primitives/options.rs:18`), the same encoding
 * used for message user headers.
 */
class TopicCreateOptions final {
  public:
    TopicCreateOptions() = default;

    /**
     * @brief Number of partitions to create with the topic.
     * @return Partition count when set; `nullopt` uses the server default
     *         `DEFAULT_PARTITIONS_COUNT` (1).
     * @note Not an option key. Fills the `CreateTopic` command's fixed field;
     *       it is consumed to compute assignments and is not stored as a topic
     *       option. Must be `1..=1000` when set; server rejects `0`.
     */
    std::optional<std::uint32_t> PartitionsCount() const noexcept { return partitions_count_; }
    TopicCreateOptions &SetPartitionsCount(std::uint32_t partitions_count) noexcept {
        partitions_count_ = partitions_count;
        return *this;
    }

    /**
     * @brief Storage compression for the topic.
     * @return Compression algorithm when set; `nullopt` uses the server default
     *         (`none`).
     * @note Catalog key `compression_algorithm` (`HeaderKind::String`, values
     *       `none`, `gzip`). Also updatable via `TopicUpdateOptions`.
     */
    std::optional<::iggy::CompressionAlgorithm> CompressionAlgorithm() const noexcept { return compression_algorithm_; }
    TopicCreateOptions &SetCompressionAlgorithm(::iggy::CompressionAlgorithm compression_algorithm) {
        compression_algorithm_ = std::move(compression_algorithm);
        return *this;
    }

    /**
     * @brief Message retention policy for the topic.
     * @return Expiry when set; `nullopt` uses the server default
     *         `IggyExpiry::ServerDefault` (alias `never_expire` with sentinel
     *         `u64::MAX` on the wire as `Uint64`).
     * @note Catalog key `message_expiry` (`Uint64` micros, or `String` like
     *       `"7 days"` via `Raw`). `0` normalizes to `nullopt`. Also updatable.
     */
    std::optional<::iggy::Expiry> MessageExpiry() const noexcept { return message_expiry_; }
    TopicCreateOptions &SetMessageExpiry(::iggy::Expiry message_expiry) {
        message_expiry_ = std::move(message_expiry);
        return *this;
    }

    /**
     * @brief Maximum retained topic size.
     * @return Max size when set; `nullopt` uses the server default
     *         `MaxTopicSize::ServerDefault` (`unlimited`, `u64::MAX` on wire).
     * @note Catalog key `max_topic_size` (`Uint64` bytes or `String` like
     *       `"1 GiB"` via `Raw`). `0` normalizes to `nullopt`. Must be `>=`
     *       the resolved segment size when both are set. Also updatable.
     */
    std::optional<::iggy::MaxTopicSize> MaxTopicSize() const noexcept { return max_topic_size_; }
    TopicCreateOptions &SetMaxTopicSize(::iggy::MaxTopicSize max_topic_size) {
        max_topic_size_ = std::move(max_topic_size);
        return *this;
    }

    /**
     * @brief Size at which a partition segment rotates.
     * @return Segment size in bytes when set; `nullopt` uses the server default
     *         `DEFAULT_SEGMENT_SIZE` (1 GiB).
     * @note Catalog key `segment_size` (`Uint64` bytes, `String` `"128MiB"` via
     *       `Raw`). Constraints: multiple of `512`, `1 MiB` (`MIN_TOPIC_SEGMENT_SIZE`)
     *       `..=` `1 GiB` (`MAX_TOPIC_SEGMENT_SIZE`, the server segment ceiling
     *       `core/common/src/types/options/mod.rs:310`). `0` normalizes to
     *       `nullopt`. Creation-only; `UpdateTopic` rejects it.
     */
    std::optional<std::uint64_t> SegmentSize() const noexcept { return segment_size_; }
    TopicCreateOptions &SetSegmentSize(std::uint64_t segment_size) noexcept {
        segment_size_ = segment_size;
        return *this;
    }

    /**
     * @brief Whether partition writes are fsynced.
     * @return Fsync flag when set; `nullopt` uses the server default
     *         `DEFAULT_ENFORCE_FSYNC` (`false`).
     * @note Catalog key `enforce_fsync` (`Bool` `0`/`1`, or `String`
     *       `"true"`/`"false"` via `Raw`). Creation-only.
     */
    std::optional<bool> EnforceFsync() const noexcept { return enforce_fsync_; }
    TopicCreateOptions &SetEnforceFsync(bool enforce_fsync) noexcept {
        enforce_fsync_ = enforce_fsync;
        return *this;
    }

    /**
     * @brief Journal flush threshold in message count.
     * @return Count when set; `nullopt` uses the server default
     *         `DEFAULT_MESSAGES_REQUIRED_TO_SAVE` (`1024`).
     * @note Catalog key `messages_required_to_save` (`Uint32`). Must be
     *       `1..=16_777_216` (`MAX_MESSAGES_REQUIRED_TO_SAVE`). `0` is rejected.
     *       Paired with `SizeOfMessagesRequiredToSave`; whichever trips first
     *       flushes. Creation-only.
     */
    std::optional<std::uint32_t> MessagesRequiredToSave() const noexcept { return messages_required_to_save_; }
    TopicCreateOptions &SetMessagesRequiredToSave(std::uint32_t messages_required_to_save) noexcept {
        messages_required_to_save_ = messages_required_to_save;
        return *this;
    }

    /**
     * @brief Journal flush threshold in bytes.
     * @return Byte threshold when set; `nullopt` uses the server default
     *         `DEFAULT_SIZE_OF_MESSAGES_REQUIRED_TO_SAVE` (1 MiB).
     * @note Catalog key `size_of_messages_required_to_save` (`Uint64` bytes,
     *       `String` like `"4KiB"` via `Raw`). Must be `1..=1 GiB`
     *       (`MAX_SIZE_OF_MESSAGES_REQUIRED_TO_SAVE`). `0` normalizes to
     *       `nullopt`. Paired with `MessagesRequiredToSave`. Creation-only.
     */
    std::optional<std::uint64_t> SizeOfMessagesRequiredToSave() const noexcept {
        return size_of_messages_required_to_save_;
    }
    TopicCreateOptions &SetSizeOfMessagesRequiredToSave(std::uint64_t size_of_messages_required_to_save) noexcept {
        size_of_messages_required_to_save_ = size_of_messages_required_to_save;
        return *this;
    }

    /**
     * @brief Whether segment bytes are reserved on disk at creation.
     * @return Flag when set; `nullopt` uses the server default
     *         `DEFAULT_PREALLOCATE_SEGMENTS` (`false`).
     * @note Catalog key `preallocate_segments` (`Bool`). Creation-only.
     *       Admission rejects `segment_size * partitions_count > 64 GiB`
     *       (`MAX_PREALLOCATED_TOPIC_BYTES`).
     */
    std::optional<bool> PreallocateSegments() const noexcept { return preallocate_segments_; }
    TopicCreateOptions &SetPreallocateSegments(bool preallocate_segments) noexcept {
        preallocate_segments_ = preallocate_segments;
        return *this;
    }

    /**
     * @brief Forward-compatible string keys not yet covered by a typed field.
     * @return Ordered map of `key -> value` strings; values are parsed server-side
     *         via the same `FromStr` as config file entries. An unknown key is
     *         rejected with `UnsupportedOptionKey`; a bad value with
     *         `InvalidOptionValue`.
     * @note A typed field wins on collision when both `raw` and the typed setter
     *       name the same key (`mod.rs:1374` `typed_field_wins_over_raw_entry_for_the_same_key`).
     *       `partitions_count` is not a key and is rejected if placed in `raw`.
     */
    const std::map<std::string, std::string> &Raw() const noexcept { return raw_; }
    TopicCreateOptions &SetRaw(std::map<std::string, std::string> raw) {
        raw_ = std::move(raw);
        return *this;
    }
    TopicCreateOptions &SetRawEntry(std::string key, std::string value) {
        raw_.emplace(std::move(key), std::move(value));
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
 * Mirrors Rust `TopicUpdateOptions` (`core/common/src/types/options/mod.rs:640`).
 * Separate from `TopicCreateOptions` so creation-only keys cannot be set on
 * update. Patch semantics: `std::nullopt` keeps the topic's current value;
 * only the keys present are changed. Only `compression_algorithm`,
 * `message_expiry`, `max_topic_size` and `raw` keys in
 * `UPDATABLE_TOPIC_OPTION_KEYS` (`core/common/src/types/options/mod.rs:476`)
 * are accepted. Creation-only keys (`segment_size`, `enforce_fsync`,
 * `messages_required_to_save`, `size_of_messages_required_to_save`,
 * `preallocate_segments`) and unknown keys are rejected with
 * `UnsupportedOptionKey`.
 */
class TopicUpdateOptions final {
  public:
    TopicUpdateOptions() = default;

    /**
     * @brief New storage compression.
     * @return Algorithm when set; `nullopt` keeps the current value.
     * @note Catalog key `compression_algorithm` (`HeaderKind::String`, `none`/`gzip`).
     */
    std::optional<::iggy::CompressionAlgorithm> CompressionAlgorithm() const noexcept { return compression_algorithm_; }
    TopicUpdateOptions &SetCompressionAlgorithm(::iggy::CompressionAlgorithm compression_algorithm) {
        compression_algorithm_ = std::move(compression_algorithm);
        return *this;
    }

    /**
     * @brief New message retention policy.
     * @return Expiry when set; `nullopt` keeps the current value.
     * @note Catalog key `message_expiry` (`Uint64` micros or `String` like
     *       `"7 days"` via `Raw`). `0` is treated as `nullopt` on the Rust side.
     */
    std::optional<::iggy::Expiry> MessageExpiry() const noexcept { return message_expiry_; }
    TopicUpdateOptions &SetMessageExpiry(::iggy::Expiry message_expiry) {
        message_expiry_ = std::move(message_expiry);
        return *this;
    }

    /**
     * @brief New maximum retained topic size.
     * @return Max size when set; `nullopt` keeps the current value.
     * @note Catalog key `max_topic_size` (`Uint64` bytes or `String` like
     *       `"1 GiB"` via `Raw`). `0` is treated as `nullopt` on the Rust side.
     */
    std::optional<::iggy::MaxTopicSize> MaxTopicSize() const noexcept { return max_topic_size_; }
    TopicUpdateOptions &SetMaxTopicSize(::iggy::MaxTopicSize max_topic_size) {
        max_topic_size_ = std::move(max_topic_size);
        return *this;
    }

    /**
     * @brief Forward-compatible updatable string keys.
     * @return Ordered map of `key -> value`; only keys in the updatable catalog
     *         are accepted. A create-only or unknown key is rejected with
     *         `UnsupportedOptionKey`; a bad value with `InvalidOptionValue`.
     * @note Typed field wins on collision.
     */
    const std::map<std::string, std::string> &Raw() const noexcept { return raw_; }
    TopicUpdateOptions &SetRaw(std::map<std::string, std::string> raw) {
        raw_ = std::move(raw);
        return *this;
    }
    TopicUpdateOptions &SetRawEntry(std::string key,
                                    std::string value) {  // NOLINT(bugprone-easily-swappable-parameters)
        raw_.emplace(std::move(key), std::move(value));
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
 * Mirrors Rust `StreamUpdateOptions` (`core/common/src/types/options/mod.rs:547`).
 * Separate from creation because streams have no catalog keys yet. Patch
 * semantics: `std::nullopt` keeps current value; only keys present are
 * changed. Currently `UPDATABLE_STREAM_OPTION_KEYS` (`mod.rs:486`) is empty,
 * so every key is rejected with `UnsupportedOptionKey` until the first stream
 * option lands. `raw` carries forward-compatible string keys.
 */
class StreamUpdateOptions final {
  public:
    StreamUpdateOptions() = default;

    /**
     * @brief Forward-compatible string keys for stream update.
     * @return Ordered map of `key -> value` strings; values are parsed
     *         server-side via `FromStr`. Unknown keys rejected with
     *         `UnsupportedOptionKey`.
     */
    const std::map<std::string, std::string> &Raw() const noexcept { return raw_; }
    StreamUpdateOptions &SetRaw(std::map<std::string, std::string> raw) {
        raw_ = std::move(raw);
        return *this;
    }
    StreamUpdateOptions &SetRawEntry(std::string key, std::string value) {
        raw_.emplace(std::move(key), std::move(value));
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
    std::string_view PollingStrategyKind() const { return polling_strategy_kind_; }

    /**
     * @brief Returns the value associated with the polling strategy.
     * @return Offset or timestamp for parameterized strategies; otherwise zero.
     */
    std::uint64_t PollingStrategyValue() const { return polling_strategy_value_; }

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
     * Mirrors Rust `TopicClient::create_topic` (`core/common/src/traits/topic_client.rs:43`)
     * with `TopicCreateOptions` (`core/common/src/types/options/mod.rs:698`).
     * Each `std::optional` corresponds to a catalog key; `nullopt` uses the
     * server default. `raw` carries forward string keys; a typed field wins
     * on collision.
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
     * Mirrors Rust `TopicClient::update_topic` with `TopicUpdateOptions`
     * (`core/common/src/types/options/mod.rs:640`). Only `compression_algorithm`,
     * `message_expiry`, `max_topic_size` and `raw` keys in
     * `UPDATABLE_TOPIC_OPTION_KEYS` are accepted; `nullopt` keeps the current
     * value. Creation-only keys (`segment_size`, `enforce_fsync`, etc.) are
     * rejected.
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

    ffi::Client *Handle() const;
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
