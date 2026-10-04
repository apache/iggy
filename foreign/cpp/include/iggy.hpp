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

#if defined(__GNUC__)
#    pragma GCC diagnostic push
#    pragma GCC diagnostic ignored "-Wpedantic"
#endif
#include "absl/numeric/int128.h"
#if defined(__GNUC__)
#    pragma GCC diagnostic pop
#endif

#include "lib.rs.h"

namespace iggy {

class Consumer;
class ConsumerOffsetInfo;
class ClientInfo;
class ClientInfoDetails;
class CacheMetricEntry;
class Stats;
class ConsumerGroupInfo;
class IggyBlockingClient;
class LoginInfo;
class Partition;
class Topic;
class TopicDetails;
class Stream;
class StreamDetails;
class GlobalPermissions;
class Permissions;
class StreamPermissions;
class TopicPermissions;
class UserInfo;
class UserInfoDetails;
class ConsumerGroup;
class ConsumerGroupDetails;
class ConsumerGroupMember;
class IggyMessagePolled;
class IggyMessageToSend;
class ClusterMetadata;
class ClusterNode;
class TransportEndpoints;
class OptionSpec;
class Partitioning;
class PolledMessages;
class SendMessagesConfirmation;
class SendMessagesResponse;

namespace detail {
/** @brief Internal base for string-backed option types. */
template <typename Tag>
class StringTag {
  protected:
    explicit StringTag(std::string value) : value_(std::move(value)) {}
    ~StringTag()                            = default;
    StringTag(const StringTag &)            = default;
    StringTag(StringTag &&)                 = default;
    StringTag &operator=(const StringTag &) = default;
    StringTag &operator=(StringTag &&)      = default;

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
    static constexpr std::size_t kMaxIdentifierLength = 255;

    /** @brief Selects the representation stored by an Identifier. */
    enum class Kind : std::uint8_t { Numeric, String };

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
        if (name.empty() || name.size() > kMaxIdentifierLength) {
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

    [[nodiscard]] ffi::Identifier ToFfi() const;

    friend class IggyBlockingClient;

    Kind kind_;
    std::variant<std::uint32_t, std::string> value_;
};

/**
 * @brief Controls whether a user may authenticate.
 */
enum class UserStatus : std::uint8_t {
    Active   = 1,  ///< The user may authenticate.
    Inactive = 2,  ///< Authentication for the user is rejected.
};

/**
 * @brief Cluster-wide permissions assigned to a user.
 *
 * Global grants apply without naming individual streams or topics. Management
 * grants include the corresponding read grants. Stream and topic grants form a
 * hierarchy: managing streams includes managing topics, reading streams
 * includes reading topics, and reading topics includes polling messages and
 * managing consumer groups. Managing streams or topics also authorizes sending
 * messages; SendMessages() can grant sending without management permission.
 *
 * A default-constructed value has every flag disabled. The setters record the
 * supplied flags without expanding implied grants; the server applies the
 * hierarchy when authorizing a request.
 */
class GlobalPermissions final {
  public:
    /**
     * @brief Returns the configured cluster-management flag.
     * @return Configured flag value.
     */
    [[nodiscard]] bool ManageServers() const noexcept { return manage_servers_; }

    /**
     * @brief Returns the configured server-information read flag.
     * @return Configured flag value.
     */
    [[nodiscard]] bool ReadServers() const noexcept { return read_servers_; }

    /**
     * @brief Returns the configured user-management flag.
     * @return Configured flag value.
     */
    [[nodiscard]] bool ManageUsers() const noexcept { return manage_users_; }

    /**
     * @brief Returns the configured user-information read flag.
     * @return Configured flag value.
     */
    [[nodiscard]] bool ReadUsers() const noexcept { return read_users_; }

    /**
     * @brief Returns the configured all-stream management flag.
     * @return Configured flag value.
     */
    [[nodiscard]] bool ManageStreams() const noexcept { return manage_streams_; }

    /**
     * @brief Returns the configured all-stream read flag.
     * @return Configured flag value.
     */
    [[nodiscard]] bool ReadStreams() const noexcept { return read_streams_; }

    /**
     * @brief Returns the configured all-topic management flag.
     * @return Configured flag value.
     */
    [[nodiscard]] bool ManageTopics() const noexcept { return manage_topics_; }

    /**
     * @brief Returns the configured all-topic read flag.
     * @return Configured flag value.
     */
    [[nodiscard]] bool ReadTopics() const noexcept { return read_topics_; }

    /**
     * @brief Returns the configured all-topic message-polling flag.
     * @return Configured flag value.
     */
    [[nodiscard]] bool PollMessages() const noexcept { return poll_messages_; }

    /**
     * @brief Returns the configured all-topic message-sending flag.
     * @return Configured flag value.
     */
    [[nodiscard]] bool SendMessages() const noexcept { return send_messages_; }

    /**
     * @brief Enables or disables cluster-management permission.
     * @param enabled Requested flag value.
     * @return Reference to this permissions object.
     */
    GlobalPermissions &SetManageServers(bool enabled) {
        manage_servers_ = enabled;
        return *this;
    }

    /**
     * @brief Enables or disables permission to read server information.
     * @param enabled Requested flag value.
     * @return Reference to this permissions object.
     */
    GlobalPermissions &SetReadServers(bool enabled) {
        read_servers_ = enabled;
        return *this;
    }

    /**
     * @brief Enables or disables user-management permission.
     * @param enabled Requested flag value.
     * @return Reference to this permissions object.
     */
    GlobalPermissions &SetManageUsers(bool enabled) {
        manage_users_ = enabled;
        return *this;
    }

    /**
     * @brief Enables or disables permission to read user information.
     * @param enabled Requested flag value.
     * @return Reference to this permissions object.
     */
    GlobalPermissions &SetReadUsers(bool enabled) {
        read_users_ = enabled;
        return *this;
    }

    /**
     * @brief Enables or disables management permission for every stream.
     * @param enabled Requested flag value.
     * @return Reference to this permissions object.
     */
    GlobalPermissions &SetManageStreams(bool enabled) {
        manage_streams_ = enabled;
        return *this;
    }

    /**
     * @brief Enables or disables read permission for every stream.
     * @param enabled Requested flag value.
     * @return Reference to this permissions object.
     */
    GlobalPermissions &SetReadStreams(bool enabled) {
        read_streams_ = enabled;
        return *this;
    }

    /**
     * @brief Enables or disables management permission for every topic.
     * @param enabled Requested flag value.
     * @return Reference to this permissions object.
     */
    GlobalPermissions &SetManageTopics(bool enabled) {
        manage_topics_ = enabled;
        return *this;
    }

    /**
     * @brief Enables or disables read permission for every topic.
     * @param enabled Requested flag value.
     * @return Reference to this permissions object.
     */
    GlobalPermissions &SetReadTopics(bool enabled) {
        read_topics_ = enabled;
        return *this;
    }

    /**
     * @brief Enables or disables polling permission for every topic.
     * @param enabled Requested flag value.
     * @return Reference to this permissions object.
     */
    GlobalPermissions &SetPollMessages(bool enabled) {
        poll_messages_ = enabled;
        return *this;
    }

    /**
     * @brief Enables or disables sending permission for every topic.
     * @param enabled Requested flag value.
     * @return Reference to this permissions object.
     */
    GlobalPermissions &SetSendMessages(bool enabled) {
        send_messages_ = enabled;
        return *this;
    }

  private:
    [[nodiscard]] ffi::GlobalPermissions ToFfi() const;
    static GlobalPermissions FromFfi(ffi::GlobalPermissions permissions);

    friend class Permissions;

    bool manage_servers_{};
    bool read_servers_{};
    bool manage_users_{};
    bool read_users_{};
    bool manage_streams_{};
    bool read_streams_{};
    bool manage_topics_{};
    bool read_topics_{};
    bool poll_messages_{};
    bool send_messages_{};
};

/**
 * @brief Permissions extending a user's access to one topic.
 *
 * These flags grant access in addition to enclosing stream and global grants;
 * a disabled flag does not revoke access granted at a broader scope. Managing
 * a topic includes reading it. Reading a topic includes polling messages and
 * managing its consumer groups. Managing a topic also authorizes sending;
 * SetSendMessages() can grant sending without management permission.
 *
 * A default-constructed value has every flag disabled.
 */
class TopicPermissions final {
  public:
    /**
     * @brief Returns the configured topic-management flag.
     * @return Configured flag value.
     */
    [[nodiscard]] bool ManageTopic() const noexcept { return manage_topic_; }

    /**
     * @brief Returns the configured topic-read flag.
     * @return Configured flag value.
     */
    [[nodiscard]] bool ReadTopic() const noexcept { return read_topic_; }

    /**
     * @brief Returns the configured message-polling flag.
     * @return Configured flag value.
     */
    [[nodiscard]] bool PollMessages() const noexcept { return poll_messages_; }

    /**
     * @brief Returns the configured message-sending flag.
     * @return Configured flag value.
     */
    [[nodiscard]] bool SendMessages() const noexcept { return send_messages_; }

    /**
     * @brief Enables or disables topic-management permission.
     * @param enabled Requested flag value.
     * @return Reference to this permissions object.
     */
    TopicPermissions &SetManageTopic(bool enabled) {
        manage_topic_ = enabled;
        return *this;
    }

    /**
     * @brief Enables or disables topic-read permission.
     * @param enabled Requested flag value.
     * @return Reference to this permissions object.
     */
    TopicPermissions &SetReadTopic(bool enabled) {
        read_topic_ = enabled;
        return *this;
    }

    /**
     * @brief Enables or disables message-polling permission.
     * @param enabled Requested flag value.
     * @return Reference to this permissions object.
     */
    TopicPermissions &SetPollMessages(bool enabled) {
        poll_messages_ = enabled;
        return *this;
    }

    /**
     * @brief Enables or disables message-sending permission.
     * @param enabled Requested flag value.
     * @return Reference to this permissions object.
     */
    TopicPermissions &SetSendMessages(bool enabled) {
        send_messages_ = enabled;
        return *this;
    }

  private:
    [[nodiscard]] ffi::TopicPermissions ToFfi() const;
    static TopicPermissions FromFfi(ffi::TopicPermissions permissions);

    friend class StreamPermissions;

    bool manage_topic_{};
    bool read_topic_{};
    bool poll_messages_{};
    bool send_messages_{};
};

/**
 * @brief Permissions extending a user's access to one stream.
 *
 * Stream grants apply to the stream identified by the containing Permissions
 * map. Topic-specific grants are keyed by numeric topic ID. These flags extend
 * broader grants and cannot revoke permissions granted globally. Managing a
 * stream includes reading it and managing its topics; reading a stream includes
 * reading its topics. Managing a stream or its topics also authorizes sending;
 * SetSendMessages() can grant sending without management permission.
 *
 * A default-constructed value has every flag disabled and no topic entries.
 */
class StreamPermissions final {
  public:
    /**
     * @brief Returns the configured stream-management flag.
     * @return Configured flag value.
     */
    [[nodiscard]] bool ManageStream() const noexcept { return manage_stream_; }

    /**
     * @brief Returns the configured stream-read flag.
     * @return Configured flag value.
     */
    [[nodiscard]] bool ReadStream() const noexcept { return read_stream_; }

    /**
     * @brief Returns the configured management flag for all stream topics.
     * @return Configured flag value.
     */
    [[nodiscard]] bool ManageTopics() const noexcept { return manage_topics_; }

    /**
     * @brief Returns the configured read flag for all stream topics.
     * @return Configured flag value.
     */
    [[nodiscard]] bool ReadTopics() const noexcept { return read_topics_; }

    /**
     * @brief Returns the configured polling flag for all stream topics.
     * @return Configured flag value.
     */
    [[nodiscard]] bool PollMessages() const noexcept { return poll_messages_; }

    /**
     * @brief Returns the configured sending flag for all stream topics.
     * @return Configured flag value.
     */
    [[nodiscard]] bool SendMessages() const noexcept { return send_messages_; }

    /**
     * @brief Returns topic-specific grants keyed by numeric topic ID.
     * @return Map owned by this value.
     */
    [[nodiscard]] const std::map<std::uint32_t, TopicPermissions> &Topics() const noexcept { return topics_; }

    /**
     * @brief Enables or disables stream-management permission.
     * @param enabled Requested flag value.
     * @return Reference to this permissions object.
     */
    StreamPermissions &SetManageStream(bool enabled) {
        manage_stream_ = enabled;
        return *this;
    }

    /**
     * @brief Enables or disables stream-read permission.
     * @param enabled Requested flag value.
     * @return Reference to this permissions object.
     */
    StreamPermissions &SetReadStream(bool enabled) {
        read_stream_ = enabled;
        return *this;
    }

    /**
     * @brief Enables or disables management permission for all stream topics.
     * @param enabled Requested flag value.
     * @return Reference to this permissions object.
     */
    StreamPermissions &SetManageTopics(bool enabled) {
        manage_topics_ = enabled;
        return *this;
    }

    /**
     * @brief Enables or disables read permission for all stream topics.
     * @param enabled Requested flag value.
     * @return Reference to this permissions object.
     */
    StreamPermissions &SetReadTopics(bool enabled) {
        read_topics_ = enabled;
        return *this;
    }

    /**
     * @brief Enables or disables polling permission for all stream topics.
     * @param enabled Requested flag value.
     * @return Reference to this permissions object.
     */
    StreamPermissions &SetPollMessages(bool enabled) {
        poll_messages_ = enabled;
        return *this;
    }

    /**
     * @brief Enables or disables sending permission for all stream topics.
     * @param enabled Requested flag value.
     * @return Reference to this permissions object.
     */
    StreamPermissions &SetSendMessages(bool enabled) {
        send_messages_ = enabled;
        return *this;
    }

    /**
     * @brief Replaces the topic-specific permission map.
     * @param topics Grants keyed by numeric topic ID.
     * @return Reference to this permissions object.
     */
    StreamPermissions &SetTopics(std::map<std::uint32_t, TopicPermissions> topics) {
        topics_ = std::move(topics);
        return *this;
    }

  private:
    [[nodiscard]] ffi::StreamPermissions ToFfi() const;
    static StreamPermissions FromFfi(const ffi::StreamPermissions &permissions);

    friend class Permissions;

    bool manage_stream_{};
    bool read_stream_{};
    bool manage_topics_{};
    bool read_topics_{};
    bool poll_messages_{};
    bool send_messages_{};
    std::map<std::uint32_t, TopicPermissions> topics_;
};

/**
 * @brief Complete permission assignment for a user.
 *
 * Global permissions apply cluster-wide. Stream entries are keyed by numeric
 * stream ID and add narrower grants, including optional topic-specific grants.
 * Narrower scopes extend broader scopes and do not deny an inherited grant.
 *
 * A default-constructed value contains no grants. Passing such a value to
 * CreateUser() assigns an explicit but empty permission set; passing
 * `std::nullopt` assigns no permission object.
 */
class Permissions final {
  public:
    /**
     * @brief Returns the cluster-wide grants.
     * @return Global permissions owned by this value.
     */
    [[nodiscard]] const GlobalPermissions &Global() const noexcept { return global_; }

    /**
     * @brief Returns stream-specific grants keyed by numeric stream ID.
     * @return Map owned by this value.
     */
    [[nodiscard]] const std::map<std::uint32_t, StreamPermissions> &Streams() const noexcept { return streams_; }

    /**
     * @brief Replaces the cluster-wide grants.
     * @param global New global permissions.
     * @return Reference to this permissions object.
     */
    Permissions &SetGlobal(GlobalPermissions global) {
        global_ = global;
        return *this;
    }

    /**
     * @brief Replaces the stream-specific grants.
     * @param streams Grants keyed by numeric stream ID.
     * @return Reference to this permissions object.
     */
    Permissions &SetStreams(std::map<std::uint32_t, StreamPermissions> streams) {
        streams_ = std::move(streams);
        return *this;
    }

  private:
    [[nodiscard]] ffi::Permissions ToFfi() const;
    static Permissions FromFfi(const ffi::Permissions &permissions);

    friend class IggyBlockingClient;
    friend class UserInfoDetails;

    GlobalPermissions global_;
    std::map<std::uint32_t, StreamPermissions> streams_;
};

/**
 * @brief Identifies the owner of a stored consumer offset.
 *
 * A consumer offset belongs either to an individual consumer or to a consumer
 * group. Create a value with Single() or Group(), then pass it to the consumer
 * offset operations on IggyBlockingClient.
 */
class Consumer final {
  public:
    /** @brief Selects an individual consumer or consumer-group identity. */
    enum class Kind : std::uint8_t { Single, Group };

    /**
     * @brief Identifies an individual consumer.
     * @param id Consumer ID or name.
     * @return Individual consumer identity.
     */
    static Consumer Single(Identifier id) { return Consumer(Kind::Single, std::move(id)); }

    /**
     * @brief Identifies a consumer group.
     * @param id Consumer group ID or name.
     * @return Consumer group identity.
     */
    static Consumer Group(Identifier id) { return Consumer(Kind::Group, std::move(id)); }

    /**
     * @brief Returns the kind of consumer represented by this value.
     * @return Kind::Single for an individual consumer or Kind::Group for a
     *         consumer group.
     */
    [[nodiscard]] Kind Type() const noexcept { return kind_; }

    /**
     * @brief Returns the consumer or consumer group identifier.
     * @return Identifier owned by this value. The reference remains valid while
     *         this Consumer remains alive.
     */
    [[nodiscard]] const Identifier &Id() const noexcept { return id_; }

  private:
    Consumer(Kind kind, Identifier id) : kind_(kind), id_(std::move(id)) {}

    [[nodiscard]] std::string_view KindName() const noexcept {
        return kind_ == Kind::Single ? "consumer" : "consumer_group";
    }

    friend class IggyBlockingClient;

    Kind kind_;
    Identifier id_;
};

/**
 * @brief Snapshot of a consumer offset and its partition state.
 *
 * GetConsumerOffset() returns this value for an individual consumer or a
 * consumer group. The partition's current offset can advance immediately after
 * the request completes, while the stored offset changes only when explicitly
 * stored or deleted.
 */
class ConsumerOffsetInfo final {
  public:
    /**
     * @brief Returns the partition associated with the stored offset.
     * @return Numeric partition ID.
     */
    [[nodiscard]] std::uint32_t PartitionId() const noexcept { return partition_id_; }

    /**
     * @brief Returns the partition's current message offset.
     * @return Current message offset observed by the server for this request.
     */
    [[nodiscard]] std::uint64_t CurrentOffset() const noexcept { return current_offset_; }

    /**
     * @brief Returns the offset stored for the consumer identity.
     * @return Stored consumer offset observed by the server for this request.
     */
    [[nodiscard]] std::uint64_t StoredOffset() const noexcept { return stored_offset_; }

  private:
    ConsumerOffsetInfo(std::uint32_t partition_id, std::uint64_t current_offset, std::uint64_t stored_offset)
        : partition_id_(partition_id), current_offset_(current_offset), stored_offset_(stored_offset) {}

    static ConsumerOffsetInfo FromFfi(ffi::ConsumerOffsetInfo offset);

    friend class IggyBlockingClient;

    std::uint32_t partition_id_;
    std::uint64_t current_offset_;
    std::uint64_t stored_offset_;
};

/**
 * @brief Type tag for a HeaderField payload.
 *
 * Specifies how a HeaderField payload is encoded. Each field stores a type tag
 * and its corresponding bytes. Raw and string payloads contain between 1 and
 * 255 bytes, and string payloads must contain valid UTF-8. A boolean is one
 * byte containing either 0 or 1. Integers use their exact natural width and
 * little-endian byte order; signed integers use two's-complement
 * representation. Floating-point values use little-endian IEEE 754 binary32
 * or binary64 representation.
 */
enum class HeaderKind : std::uint8_t {
    Raw     = 1,   ///< Uninterpreted byte sequence.
    String  = 2,   ///< UTF-8 encoded text.
    Bool    = 3,   ///< Boolean encoded as one byte: 0 for false or 1 for true.
    Int8    = 4,   ///< One-byte signed integer.
    Int16   = 5,   ///< Two-byte signed integer.
    Int32   = 6,   ///< Four-byte signed integer.
    Int64   = 7,   ///< Eight-byte signed integer.
    Int128  = 8,   ///< Sixteen-byte signed integer.
    Uint8   = 9,   ///< One-byte unsigned integer.
    Uint16  = 10,  ///< Two-byte unsigned integer.
    Uint32  = 11,  ///< Four-byte unsigned integer.
    Uint64  = 12,  ///< Eight-byte unsigned integer.
    Uint128 = 13,  ///< Sixteen-byte unsigned integer.
    Float32 = 14,  ///< Four-byte IEEE 754 binary32 value.
    Float64 = 15,  ///< Eight-byte IEEE 754 binary64 value.
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
     * @brief Creates a typed header field by copying bytes from a string view.
     * @param kind Type tag for @p value.
     * @param value Bytes to copy into the field.
     * @return Header field containing the supplied type and bytes.
     * @note This overload does not validate that the bytes match @p kind or
     *       that a HeaderKind::String value contains valid UTF-8.
     */
    static HeaderField Create(HeaderKind kind, std::string_view value) {
        return HeaderField(kind, std::vector<std::uint8_t>(value.begin(), value.end()));
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

    friend class IggyMessagePolled;
    friend class ResourceOptions;

    HeaderField key_;
    HeaderField value_;
};

/**
 * @brief Message payload and user headers prepared for sending.
 *
 * Create() owns the supplied payload and headers. Validation is deferred until
 * the message is sent. A valid payload contains between 1 and 64,000,000 bytes,
 * and the encoded user headers occupy no more than 100,000 bytes. Header keys
 * must be unique. Header insertion order is not preserved during transmission;
 * headers are ordered by their typed keys.
 *
 * The message ID is application-defined and defaults to zero. IDs do not need
 * to be unique.
 */
class IggyMessageToSend final {
  public:
    /**
     * @brief Creates a message for a send operation.
     * @param payload Binary message payload.
     * @param user_headers Optional typed user headers. Keys must be unique.
     * @param id Application-defined message ID.
     * @return Message owning @p payload and @p user_headers.
     * @note Payload and header constraints are validated when the message is
     *       sent, not by this function.
     */
    static IggyMessageToSend Create(std::string_view payload,
                                    std::vector<HeaderEntry> user_headers = {},
                                    absl::uint128 id                      = 0) {
        return IggyMessageToSend(id, std::vector<std::uint8_t>(payload.begin(), payload.end()),
                                 std::move(user_headers));
    }

    /**
     * @brief Returns the application-defined message ID.
     * @return Message ID supplied to Create(), or zero when omitted.
     */
    [[nodiscard]] absl::uint128 Id() const noexcept { return id_; }

    /**
     * @brief Returns the binary message payload.
     * @return Payload owned by this value. The reference remains valid while
     *         this IggyMessageToSend remains alive.
     */
    [[nodiscard]] const std::vector<std::uint8_t> &Payload() const noexcept { return payload_; }

    /**
     * @brief Returns the typed user headers.
     * @return Headers owned by this value in their original insertion order.
     *         The reference remains valid while this IggyMessageToSend remains
     *         alive.
     */
    [[nodiscard]] const std::vector<HeaderEntry> &UserHeaders() const noexcept { return user_headers_; }

  private:
    IggyMessageToSend(absl::uint128 id, std::vector<std::uint8_t> payload, std::vector<HeaderEntry> user_headers)
        : id_(id), payload_(std::move(payload)), user_headers_(std::move(user_headers)) {}

    [[nodiscard]] ffi::IggyMessageToSend ToFfi() const;

    friend class IggyBlockingClient;

    absl::uint128 id_;
    std::vector<std::uint8_t> payload_;
    std::vector<HeaderEntry> user_headers_;
};

/**
 * @brief Message and metadata returned by a poll operation.
 *
 * This value owns its payload and decoded user headers. Header entries are
 * returned in their encoded order. Malformed encoded headers are reported as
 * an empty collection rather than making the message unreadable.
 */
class IggyMessagePolled final {
  public:
    /**
     * @brief Returns the stored message checksum.
     * @return Checksum covering the message fields after the checksum field.
     */
    [[nodiscard]] std::uint64_t Checksum() const noexcept { return checksum_; }

    /**
     * @brief Returns the application-defined message ID.
     * @return Message ID supplied when the message was sent.
     */
    [[nodiscard]] absl::uint128 Id() const noexcept { return id_; }

    /**
     * @brief Returns the message offset within its partition.
     * @return Offset assigned by the server.
     */
    [[nodiscard]] std::uint64_t Offset() const noexcept { return offset_; }

    /**
     * @brief Returns the timestamp assigned when the message was stored.
     * @return Server timestamp in microseconds since the Unix epoch.
     */
    [[nodiscard]] std::uint64_t Timestamp() const noexcept { return timestamp_; }

    /**
     * @brief Returns the timestamp recorded when the message was created.
     * @return Origin timestamp in microseconds since the Unix epoch.
     */
    [[nodiscard]] std::uint64_t OriginTimestamp() const noexcept { return origin_timestamp_; }

    /**
     * @brief Returns the encoded size of the user-header section.
     * @return Encoded user-header length in bytes.
     */
    [[nodiscard]] std::uint32_t UserHeadersLength() const noexcept { return user_headers_length_; }

    /**
     * @brief Returns the payload length recorded in the message header.
     * @return Payload length in bytes.
     */
    [[nodiscard]] std::uint32_t PayloadLength() const noexcept { return payload_length_; }

    /**
     * @brief Returns the message header's reserved field.
     * @return Reserved value, currently zero.
     */
    [[nodiscard]] std::uint64_t Reserved() const noexcept { return reserved_; }

    /**
     * @brief Returns the binary message payload.
     * @return Payload owned by this value. The reference remains valid while
     *         this IggyMessagePolled remains alive.
     */
    [[nodiscard]] const std::vector<std::uint8_t> &Payload() const noexcept { return payload_; }

    /**
     * @brief Returns the decoded typed user headers.
     * @return Headers owned by this value in their encoded order. The reference
     *         remains valid while this IggyMessagePolled remains alive.
     */
    [[nodiscard]] const std::vector<HeaderEntry> &UserHeaders() const noexcept { return user_headers_; }

  private:
    IggyMessagePolled(std::uint64_t checksum,
                      absl::uint128 id,
                      std::uint64_t offset,
                      std::uint64_t timestamp,
                      std::uint64_t origin_timestamp,
                      std::uint32_t user_headers_length,
                      std::uint32_t payload_length,
                      std::uint64_t reserved,
                      std::vector<std::uint8_t> payload,
                      std::vector<HeaderEntry> user_headers)
        : checksum_(checksum),
          id_(id),
          offset_(offset),
          timestamp_(timestamp),
          origin_timestamp_(origin_timestamp),
          user_headers_length_(user_headers_length),
          payload_length_(payload_length),
          reserved_(reserved),
          payload_(std::move(payload)),
          user_headers_(std::move(user_headers)) {}

    static IggyMessagePolled FromFfi(ffi::IggyMessagePolled message);

    friend class IggyBlockingClient;
    friend class PolledMessages;

    std::uint64_t checksum_;
    absl::uint128 id_;
    std::uint64_t offset_;
    std::uint64_t timestamp_;
    std::uint64_t origin_timestamp_;
    std::uint32_t user_headers_length_;
    std::uint32_t payload_length_;
    std::uint64_t reserved_;
    std::vector<std::uint8_t> payload_;
    std::vector<HeaderEntry> user_headers_;
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
 * to configure a new topic. Stream creation accepts only a name.
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
     * @note The server returns only explicit entries for streams, so this
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
    friend class UserInfo;
    friend class UserInfoDetails;

    std::map<std::string, HeaderField> explicit_;
    std::map<std::string, HeaderField> derived_;
};

/**
 * @brief Snapshot of basic user metadata.
 *
 * GetUsers() returns one value for each user visible to the caller. This
 * summary omits permissions. User IDs remain assigned to the same user until
 * that user is deleted. CreatedAt() is expressed in microseconds since the
 * Unix epoch.
 */
class UserInfo final {
  public:
    /**
     * @brief Returns the server-assigned numeric user ID.
     * @return Numeric user ID.
     */
    [[nodiscard]] std::uint32_t Id() const noexcept { return id_; }

    /**
     * @brief Returns the creation timestamp.
     * @return Timestamp in microseconds since the Unix epoch.
     */
    [[nodiscard]] std::uint64_t CreatedAt() const noexcept { return created_at_; }

    /**
     * @brief Returns whether the user is active or inactive.
     * @return Current user status observed for this request.
     */
    [[nodiscard]] UserStatus Status() const noexcept { return status_; }

    /**
     * @brief Returns the unique user name.
     * @return Name owned by this value.
     */
    [[nodiscard]] const std::string &Username() const noexcept { return username_; }

    /**
     * @brief Returns explicit user creation options.
     * @return Options owned by this value.
     */
    [[nodiscard]] const ResourceOptions &Options() const noexcept { return options_; }

  private:
    UserInfo(std::uint32_t id,
             std::uint64_t created_at,
             UserStatus status,
             std::string username,
             ResourceOptions options)
        : id_(id),
          created_at_(created_at),
          status_(status),
          username_(std::move(username)),
          options_(std::move(options)) {}

    static UserInfo FromFfi(ffi::UserInfo user);

    friend class IggyBlockingClient;

    std::uint32_t id_;
    std::uint64_t created_at_;
    UserStatus status_;
    std::string username_;
    ResourceOptions options_;
};

/**
 * @brief Snapshot of user metadata and assigned permissions.
 *
 * GetUser() and CreateUser() return this detailed form. Permissions() is empty
 * when the user has no permission object. An engaged Permissions value can
 * still contain no enabled grants.
 */
class UserInfoDetails final {
  public:
    /**
     * @brief Returns the server-assigned numeric user ID.
     * @return Numeric user ID.
     */
    [[nodiscard]] std::uint32_t Id() const noexcept { return id_; }

    /**
     * @brief Returns the creation timestamp.
     * @return Timestamp in microseconds since the Unix epoch.
     */
    [[nodiscard]] std::uint64_t CreatedAt() const noexcept { return created_at_; }

    /**
     * @brief Returns whether the user is active or inactive.
     * @return Current user status observed for this request.
     */
    [[nodiscard]] UserStatus Status() const noexcept { return status_; }

    /**
     * @brief Returns the unique user name.
     * @return Name owned by this value.
     */
    [[nodiscard]] const std::string &Username() const noexcept { return username_; }

    /**
     * @brief Returns the user's explicit permission assignment.
     * @return Empty when no permission object is assigned; otherwise the
     *         permissions owned by this value.
     */
    [[nodiscard]] const std::optional<::iggy::Permissions> &Permissions() const noexcept { return permissions_; }

    /**
     * @brief Returns explicit user creation options.
     * @return Options owned by this value.
     */
    [[nodiscard]] const ResourceOptions &Options() const noexcept { return options_; }

  private:
    UserInfoDetails(std::uint32_t id,
                    std::uint64_t created_at,
                    UserStatus status,
                    std::string username,
                    std::optional<::iggy::Permissions> permissions,
                    ResourceOptions options)
        : id_(id),
          created_at_(created_at),
          status_(status),
          username_(std::move(username)),
          permissions_(std::move(permissions)),
          options_(std::move(options)) {}

    static UserInfoDetails FromFfi(ffi::UserInfoDetails user);

    friend class IggyBlockingClient;

    std::uint32_t id_;
    std::uint64_t created_at_;
    UserStatus status_;
    std::string username_;
    std::optional<::iggy::Permissions> permissions_;
    ResourceOptions options_;
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
     * @note The server does not return derived stream options.
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
     * @note The server does not return derived stream options.
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
 * @brief Snapshot of a consumer group member and its partition assignments.
 *
 * ConsumerGroupDetails contains one of these values for every member observed
 * by the server. Membership and partition assignments can change immediately
 * after the request completes.
 */
class ConsumerGroupMember final {
  public:
    /**
     * @brief Returns the numeric ID of the consumer group member.
     * @return Numeric member ID assigned by the server.
     */
    [[nodiscard]] std::uint32_t Id() const noexcept { return id_; }

    /**
     * @brief Returns the server-reported number of partitions assigned to this member.
     * @return Partition count reported by the server.
     */
    [[nodiscard]] std::uint32_t PartitionsCount() const noexcept { return partitions_count_; }

    /**
     * @brief Returns the partitions assigned to this member.
     * @return Partition IDs owned by this value. The reference remains valid
     *         while this ConsumerGroupMember remains alive.
     */
    [[nodiscard]] const std::vector<std::uint32_t> &Partitions() const noexcept { return partitions_; }

  private:
    ConsumerGroupMember(std::uint32_t id, std::uint32_t partitions_count, std::vector<std::uint32_t> partitions)
        : id_(id), partitions_count_(partitions_count), partitions_(std::move(partitions)) {}

    static ConsumerGroupMember FromFfi(ffi::ConsumerGroupMember member);

    friend class ConsumerGroupDetails;

    std::uint32_t id_;
    std::uint32_t partitions_count_;
    std::vector<std::uint32_t> partitions_;
};

/**
 * @brief Snapshot of consumer group metadata.
 *
 * GetConsumerGroups() returns one summary for each consumer group observed in
 * a topic. Use GetConsumerGroup() when individual member and partition
 * assignment details are needed.
 */
class ConsumerGroup final {
  public:
    /**
     * @brief Returns the numeric ID assigned to the consumer group.
     * @return Numeric consumer group ID.
     */
    [[nodiscard]] std::uint32_t Id() const noexcept { return id_; }

    /**
     * @brief Returns the consumer group name.
     * @return Name owned by this value.
     */
    [[nodiscard]] const std::string &Name() const noexcept { return name_; }

    /**
     * @brief Returns the number of partitions consumed by the group.
     * @return Partition count observed by the server for this request.
     */
    [[nodiscard]] std::uint32_t PartitionsCount() const noexcept { return partitions_count_; }

    /**
     * @brief Returns the number of members in the group.
     * @return Member count observed by the server for this request.
     */
    [[nodiscard]] std::uint32_t MembersCount() const noexcept { return members_count_; }

  private:
    ConsumerGroup(std::uint32_t id, std::string name, std::uint32_t partitions_count, std::uint32_t members_count)
        : id_(id), name_(std::move(name)), partitions_count_(partitions_count), members_count_(members_count) {}

    static ConsumerGroup FromFfi(ffi::ConsumerGroup group);

    friend class IggyBlockingClient;

    std::uint32_t id_;
    std::string name_;
    std::uint32_t partitions_count_;
    std::uint32_t members_count_;
};

/**
 * @brief Snapshot of consumer group metadata and member details.
 *
 * CreateConsumerGroup() and GetConsumerGroup() return this value. Membership
 * and partition assignments can change immediately after the request
 * completes.
 */
class ConsumerGroupDetails final {
  public:
    /**
     * @brief Returns the numeric ID assigned to the consumer group.
     * @return Numeric consumer group ID.
     */
    [[nodiscard]] std::uint32_t Id() const noexcept { return id_; }

    /**
     * @brief Returns the consumer group name.
     * @return Name owned by this value.
     */
    [[nodiscard]] const std::string &Name() const noexcept { return name_; }

    /**
     * @brief Returns the number of partitions consumed by the group.
     * @return Partition count observed by the server for this request.
     */
    [[nodiscard]] std::uint32_t PartitionsCount() const noexcept { return partitions_count_; }

    /**
     * @brief Returns the server-reported number of members in the group.
     * @return Member count reported by the server.
     */
    [[nodiscard]] std::uint32_t MembersCount() const noexcept { return members_count_; }

    /**
     * @brief Returns the consumer group members and their partition assignments.
     * @return Member details owned by this value. The reference remains valid
     *         while this ConsumerGroupDetails remains alive.
     */
    [[nodiscard]] const std::vector<ConsumerGroupMember> &Members() const noexcept { return members_; }

  private:
    ConsumerGroupDetails(std::uint32_t id,
                         std::string name,
                         std::uint32_t partitions_count,
                         std::uint32_t members_count,
                         std::vector<ConsumerGroupMember> members)
        : id_(id),
          name_(std::move(name)),
          partitions_count_(partitions_count),
          members_count_(members_count),
          members_(std::move(members)) {}

    static ConsumerGroupDetails FromFfi(ffi::ConsumerGroupDetails group);

    friend class IggyBlockingClient;

    std::uint32_t id_;
    std::string name_;
    std::uint32_t partitions_count_;
    std::uint32_t members_count_;
    std::vector<ConsumerGroupMember> members_;
};

/**
 * @brief Identifies one consumer-group membership of a connected client.
 *
 * ClientInfoDetails returns these numeric identifiers for each membership
 * observed by the server. The membership can change immediately after the
 * client information is retrieved.
 */
class ConsumerGroupInfo final {
  public:
    /**
     * @brief Returns the numeric ID of the member group's stream.
     * @return Numeric stream ID.
     */
    [[nodiscard]] std::uint32_t StreamId() const noexcept { return stream_id_; }

    /**
     * @brief Returns the numeric ID of the member group's topic.
     * @return Numeric topic ID.
     */
    [[nodiscard]] std::uint32_t TopicId() const noexcept { return topic_id_; }

    /**
     * @brief Returns the numeric consumer group ID.
     * @return Numeric consumer group ID.
     */
    [[nodiscard]] std::uint32_t GroupId() const noexcept { return group_id_; }

  private:
    ConsumerGroupInfo(std::uint32_t stream_id, std::uint32_t topic_id, std::uint32_t group_id)
        : stream_id_(stream_id), topic_id_(topic_id), group_id_(group_id) {}

    static ConsumerGroupInfo FromFfi(ffi::ConsumerGroupInfo info);

    friend class ClientInfoDetails;

    std::uint32_t stream_id_;
    std::uint32_t topic_id_;
    std::uint32_t group_id_;
};

/**
 * @brief Snapshot summary of a client connection known to the server.
 *
 * GetClients() returns one summary for each connection observed by the server.
 * A client is a transport connection, not an Iggy user. Connections can close,
 * authenticate, or change consumer-group membership immediately after the
 * request completes.
 */
class ClientInfo final {
  public:
    /**
     * @brief Returns the server-assigned connection ID.
     * @return Numeric client ID accepted by GetClient() while the connection
     *         remains known to the server.
     */
    [[nodiscard]] std::uint32_t ClientId() const noexcept { return client_id_; }

    /**
     * @brief Returns the authenticated user ID for this connection.
     * @return Empty when the client has not authenticated.
     */
    [[nodiscard]] const std::optional<std::uint32_t> &UserId() const noexcept { return user_id_; }

    /**
     * @brief Returns the remote address reported by the server.
     * @return Address owned by this value.
     */
    [[nodiscard]] const std::string &Address() const noexcept { return address_; }

    /**
     * @brief Returns the transport name reported by the server.
     * @return Transport name owned by this value.
     */
    [[nodiscard]] const std::string &Transport() const noexcept { return transport_; }

    /**
     * @brief Returns the number of consumer groups joined by this client.
     * @return Membership count observed for this request.
     */
    [[nodiscard]] std::uint32_t ConsumerGroupsCount() const noexcept { return consumer_groups_count_; }

  private:
    ClientInfo(std::uint32_t client_id,
               std::optional<std::uint32_t> user_id,
               std::string address,
               std::string transport,
               std::uint32_t consumer_groups_count)
        : client_id_(client_id),
          user_id_(user_id),
          address_(std::move(address)),
          transport_(std::move(transport)),
          consumer_groups_count_(consumer_groups_count) {}

    static ClientInfo FromFfi(ffi::ClientInfo info);

    friend class IggyBlockingClient;

    std::uint32_t client_id_;
    std::optional<std::uint32_t> user_id_;
    std::string address_;
    std::string transport_;
    std::uint32_t consumer_groups_count_;
};

/**
 * @brief Snapshot of a client connection and its consumer-group memberships.
 *
 * GetMe() and GetClient() return this detailed form. The connection state and
 * memberships are not live and can change immediately after retrieval.
 */
class ClientInfoDetails final {
  public:
    /**
     * @brief Returns the server-assigned connection ID.
     * @return Numeric client ID.
     */
    [[nodiscard]] std::uint32_t ClientId() const noexcept { return client_id_; }

    /**
     * @brief Returns the authenticated user ID for this connection.
     * @return Empty when the client has not authenticated.
     */
    [[nodiscard]] const std::optional<std::uint32_t> &UserId() const noexcept { return user_id_; }

    /**
     * @brief Returns the remote address reported by the server.
     * @return Address owned by this value.
     */
    [[nodiscard]] const std::string &Address() const noexcept { return address_; }

    /**
     * @brief Returns the transport name reported by the server.
     * @return Transport name owned by this value.
     */
    [[nodiscard]] const std::string &Transport() const noexcept { return transport_; }

    /**
     * @brief Returns the server-reported consumer-group membership count.
     * @return Membership count observed for this request.
     */
    [[nodiscard]] std::uint32_t ConsumerGroupsCount() const noexcept { return consumer_groups_count_; }

    /**
     * @brief Returns the observed consumer-group memberships.
     * @return Membership identifiers owned by this value.
     */
    [[nodiscard]] const std::vector<ConsumerGroupInfo> &ConsumerGroups() const noexcept { return consumer_groups_; }

  private:
    ClientInfoDetails(std::uint32_t client_id,
                      std::optional<std::uint32_t> user_id,
                      std::string address,
                      std::string transport,
                      std::uint32_t consumer_groups_count,
                      std::vector<ConsumerGroupInfo> consumer_groups)
        : client_id_(client_id),
          user_id_(user_id),
          address_(std::move(address)),
          transport_(std::move(transport)),
          consumer_groups_count_(consumer_groups_count),
          consumer_groups_(std::move(consumer_groups)) {}

    static ClientInfoDetails FromFfi(ffi::ClientInfoDetails info);

    friend class IggyBlockingClient;

    std::uint32_t client_id_;
    std::optional<std::uint32_t> user_id_;
    std::string address_;
    std::string transport_;
    std::uint32_t consumer_groups_count_;
    std::vector<ConsumerGroupInfo> consumer_groups_;
};

/**
 * @brief Cache counters for one stream, topic, and partition.
 *
 * Stats::CacheMetrics() contains these entries when partition cache metrics
 * are available. The server returns an empty cache-metrics collection.
 */
class CacheMetricEntry final {
  public:
    /**
     * @brief Returns the numeric stream ID for this cache entry.
     * @return Numeric stream ID.
     */
    [[nodiscard]] std::uint32_t StreamId() const noexcept { return stream_id_; }

    /**
     * @brief Returns the numeric topic ID for this cache entry.
     * @return Numeric topic ID.
     */
    [[nodiscard]] std::uint32_t TopicId() const noexcept { return topic_id_; }

    /**
     * @brief Returns the numeric partition ID for this cache entry.
     * @return Numeric partition ID.
     */
    [[nodiscard]] std::uint32_t PartitionId() const noexcept { return partition_id_; }

    /**
     * @brief Returns the cumulative number of cache hits reported by the server.
     * @return Cache hit count.
     */
    [[nodiscard]] std::uint64_t Hits() const noexcept { return hits_; }

    /**
     * @brief Returns the cumulative number of cache misses reported by the server.
     * @return Cache miss count.
     */
    [[nodiscard]] std::uint64_t Misses() const noexcept { return misses_; }

    /**
     * @brief Returns the server-reported ratio of hits to total cache lookups.
     * @return Cache hit ratio.
     */
    [[nodiscard]] float HitRatio() const noexcept { return hit_ratio_; }

  private:
    CacheMetricEntry(std::uint32_t stream_id,
                     std::uint32_t topic_id,
                     std::uint32_t partition_id,
                     std::uint64_t hits,
                     std::uint64_t misses,
                     float hit_ratio)
        : stream_id_(stream_id),
          topic_id_(topic_id),
          partition_id_(partition_id),
          hits_(hits),
          misses_(misses),
          hit_ratio_(hit_ratio) {}

    static CacheMetricEntry FromFfi(ffi::CacheMetricEntry entry);

    friend class Stats;

    std::uint32_t stream_id_;
    std::uint32_t topic_id_;
    std::uint32_t partition_id_;
    std::uint64_t hits_;
    std::uint64_t misses_;
    float hit_ratio_;
};

/**
 * @brief Snapshot of server process, storage, and resource statistics.
 *
 * GetStats() returns process and host measurements from the serving server,
 * together with metadata totals observed for one request. Values are not a
 * live or transactional view. CPU measurements depend on the server's sampling
 * history, and the first sample on a serving thread can report zero. Memory
 * totals honor an effective cgroup limit when one applies. Disk-space values
 * describe the volume containing the configured data directory and can be zero
 * when the server cannot probe that volume.
 */
class Stats final {
  public:
    /**
     * @brief Returns the operating-system process ID of the server.
     * @return Numeric process ID.
     */
    [[nodiscard]] std::uint32_t ProcessId() const noexcept { return process_id_; }

    /**
     * @brief Returns the server process CPU usage.
     * @return Process CPU usage as a percentage.
     */
    [[nodiscard]] float CpuUsage() const noexcept { return cpu_usage_; }

    /**
     * @brief Returns total CPU usage for the available CPU set.
     * @return Total CPU usage as a percentage.
     */
    [[nodiscard]] float TotalCpuUsage() const noexcept { return total_cpu_usage_; }

    /**
     * @brief Returns server process memory usage.
     * @return Process memory usage in bytes.
     */
    [[nodiscard]] std::uint64_t MemoryUsage() const noexcept { return memory_usage_; }

    /**
     * @brief Returns total host or effective cgroup memory.
     * @return Total memory in bytes.
     */
    [[nodiscard]] std::uint64_t TotalMemory() const noexcept { return total_memory_; }

    /**
     * @brief Returns available host or effective cgroup memory.
     * @return Available memory in bytes.
     */
    [[nodiscard]] std::uint64_t AvailableMemory() const noexcept { return available_memory_; }

    /**
     * @brief Returns server process uptime.
     * @return Process uptime in microseconds.
     */
    [[nodiscard]] std::uint64_t RunTimeMicros() const noexcept { return run_time_micros_; }

    /**
     * @brief Returns the server process start time.
     * @return Timestamp in microseconds since the Unix epoch.
     */
    [[nodiscard]] std::uint64_t StartTimeEpochMicros() const noexcept { return start_time_epoch_micros_; }

    /**
     * @brief Returns the server process read-byte count.
     * @return Number of bytes read by the process.
     */
    [[nodiscard]] std::uint64_t ReadBytes() const noexcept { return read_bytes_; }

    /**
     * @brief Returns the server process written-byte count.
     * @return Number of bytes written by the process.
     */
    [[nodiscard]] std::uint64_t WrittenBytes() const noexcept { return written_bytes_; }

    /**
     * @brief Returns the aggregate retained message size.
     * @return Retained message size in bytes.
     */
    [[nodiscard]] std::uint64_t MessagesSizeBytes() const noexcept { return messages_size_bytes_; }

    /**
     * @brief Returns the observed number of streams.
     * @return Stream count.
     */
    [[nodiscard]] std::uint32_t StreamsCount() const noexcept { return streams_count_; }

    /**
     * @brief Returns the observed number of topics.
     * @return Topic count.
     */
    [[nodiscard]] std::uint32_t TopicsCount() const noexcept { return topics_count_; }

    /**
     * @brief Returns the observed number of partitions.
     * @return Partition count.
     */
    [[nodiscard]] std::uint32_t PartitionsCount() const noexcept { return partitions_count_; }

    /**
     * @brief Returns the observed number of partition segments.
     * @return Segment count.
     */
    [[nodiscard]] std::uint32_t SegmentsCount() const noexcept { return segments_count_; }

    /**
     * @brief Returns the observed number of retained messages.
     * @return Message count.
     */
    [[nodiscard]] std::uint64_t MessagesCount() const noexcept { return messages_count_; }

    /**
     * @brief Returns the number of client connections observed by the server.
     * @return Client connection count.
     */
    [[nodiscard]] std::uint32_t ClientsCount() const noexcept { return clients_count_; }

    /**
     * @brief Returns the observed number of consumer groups.
     * @return Consumer group count.
     */
    [[nodiscard]] std::uint32_t ConsumerGroupsCount() const noexcept { return consumer_groups_count_; }

    /**
     * @brief Returns the server host name.
     * @return Host name owned by this value.
     */
    [[nodiscard]] const std::string &Hostname() const noexcept { return hostname_; }

    /**
     * @brief Returns the server operating-system name.
     * @return Operating-system name owned by this value.
     */
    [[nodiscard]] const std::string &OsName() const noexcept { return os_name_; }

    /**
     * @brief Returns the server operating-system version.
     * @return Operating-system version owned by this value.
     */
    [[nodiscard]] const std::string &OsVersion() const noexcept { return os_version_; }

    /**
     * @brief Returns the server kernel version.
     * @return Kernel version owned by this value.
     */
    [[nodiscard]] const std::string &KernelVersion() const noexcept { return kernel_version_; }

    /**
     * @brief Returns the human-readable Iggy server version.
     * @return Version string owned by this value.
     */
    [[nodiscard]] const std::string &IggyServerVersion() const noexcept { return iggy_server_version_; }

    /**
     * @brief Returns the numeric semantic version when reported by the server.
     * @return `major * 1,000,000 + minor * 1,000 + patch`, or `std::nullopt`
     *         when the server does not report a numeric version.
     */
    [[nodiscard]] const std::optional<std::uint32_t> &ServerSemver() const noexcept { return server_semver_; }

    /**
     * @brief Returns partition cache metrics reported by the server.
     * @return Entries owned by this value. The server returns an empty
     *         collection.
     */
    [[nodiscard]] const std::vector<CacheMetricEntry> &CacheMetrics() const noexcept { return cache_metrics_; }

    /**
     * @brief Returns the number of threads in the server process.
     * @return Process thread count.
     */
    [[nodiscard]] std::uint32_t ThreadsCount() const noexcept { return threads_count_; }

    /**
     * @brief Returns free space on the server data-directory volume.
     * @return Free space in bytes, or zero when the probe is unavailable.
     */
    [[nodiscard]] std::uint64_t FreeDiskSpace() const noexcept { return free_disk_space_; }

    /**
     * @brief Returns total space on the server data-directory volume.
     * @return Total space in bytes, or zero when the probe is unavailable.
     */
    [[nodiscard]] std::uint64_t TotalDiskSpace() const noexcept { return total_disk_space_; }

    /**
     * @brief Returns the number of file descriptors the server process holds open.
     * @return Open descriptor count, or zero when the server cannot count them.
     */
    [[nodiscard]] std::uint64_t OpenFilesCount() const noexcept { return open_files_count_; }

    /**
     * @brief Returns the server's soft `RLIMIT_NOFILE`, the count at which opens fail.
     * @return Descriptor limit, or zero when the server cannot read it.
     */
    [[nodiscard]] std::uint64_t OpenFilesLimit() const noexcept { return open_files_limit_; }

  private:
    Stats(std::uint32_t process_id,
          float cpu_usage,
          float total_cpu_usage,
          std::uint64_t memory_usage,
          std::uint64_t total_memory,
          std::uint64_t available_memory,
          std::uint64_t run_time_micros,
          std::uint64_t start_time_epoch_micros,
          std::uint64_t read_bytes,
          std::uint64_t written_bytes,
          std::uint64_t messages_size_bytes,
          std::uint32_t streams_count,
          std::uint32_t topics_count,
          std::uint32_t partitions_count,
          std::uint32_t segments_count,
          std::uint64_t messages_count,
          std::uint32_t clients_count,
          std::uint32_t consumer_groups_count,
          std::string hostname,
          std::string os_name,
          std::string os_version,
          std::string kernel_version,
          std::string iggy_server_version,
          std::optional<std::uint32_t> server_semver,
          std::vector<CacheMetricEntry> cache_metrics,
          std::uint32_t threads_count,
          std::uint64_t free_disk_space,
          std::uint64_t total_disk_space,
          std::uint64_t open_files_count,
          std::uint64_t open_files_limit)
        : process_id_(process_id),
          cpu_usage_(cpu_usage),
          total_cpu_usage_(total_cpu_usage),
          memory_usage_(memory_usage),
          total_memory_(total_memory),
          available_memory_(available_memory),
          run_time_micros_(run_time_micros),
          start_time_epoch_micros_(start_time_epoch_micros),
          read_bytes_(read_bytes),
          written_bytes_(written_bytes),
          messages_size_bytes_(messages_size_bytes),
          streams_count_(streams_count),
          topics_count_(topics_count),
          partitions_count_(partitions_count),
          segments_count_(segments_count),
          messages_count_(messages_count),
          clients_count_(clients_count),
          consumer_groups_count_(consumer_groups_count),
          hostname_(std::move(hostname)),
          os_name_(std::move(os_name)),
          os_version_(std::move(os_version)),
          kernel_version_(std::move(kernel_version)),
          iggy_server_version_(std::move(iggy_server_version)),
          server_semver_(server_semver),
          cache_metrics_(std::move(cache_metrics)),
          threads_count_(threads_count),
          free_disk_space_(free_disk_space),
          total_disk_space_(total_disk_space),
          open_files_count_(open_files_count),
          open_files_limit_(open_files_limit) {}

    static Stats FromFfi(ffi::Stats stats);

    friend class IggyBlockingClient;

    std::uint32_t process_id_;
    float cpu_usage_;
    float total_cpu_usage_;
    std::uint64_t memory_usage_;
    std::uint64_t total_memory_;
    std::uint64_t available_memory_;
    std::uint64_t run_time_micros_;
    std::uint64_t start_time_epoch_micros_;
    std::uint64_t read_bytes_;
    std::uint64_t written_bytes_;
    std::uint64_t messages_size_bytes_;
    std::uint32_t streams_count_;
    std::uint32_t topics_count_;
    std::uint32_t partitions_count_;
    std::uint32_t segments_count_;
    std::uint64_t messages_count_;
    std::uint32_t clients_count_;
    std::uint32_t consumer_groups_count_;
    std::string hostname_;
    std::string os_name_;
    std::string os_version_;
    std::string kernel_version_;
    std::string iggy_server_version_;
    std::optional<std::uint32_t> server_semver_;
    std::vector<CacheMetricEntry> cache_metrics_;
    std::uint32_t threads_count_;
    std::uint64_t free_disk_space_;
    std::uint64_t total_disk_space_;
    std::uint64_t open_files_count_;
    std::uint64_t open_files_limit_;
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
     * @throws std::invalid_argument if @p micros is zero.
     */
    static Expiry Duration(std::uint64_t micros) {
        if (micros == 0) {
            throw std::invalid_argument("Expiry duration must be greater than zero");
        }
        return Expiry("duration", micros);
    }

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
 * @brief Storage guarantee required before an operation reports completion.
 *
 * Both policies persist data through the replicated journal. Replicated waits
 * for quorum commit without an additional stable-storage barrier. Persisted
 * also requires recoverable stable-storage copies on the quorum. Topic message
 * durability and consumer-offset durability are configured independently.
 */
enum class Durability : std::uint8_t {
    Replicated,  ///< Wait for quorum commit.
    Persisted,   ///< Wait for quorum commit backed by stable storage.
};

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
     * @return Configured partition count, or `std::nullopt` to default to 1.
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
     * @brief Returns the message completion policy.
     * @return Configured policy, or `std::nullopt` to use the server default
     *         (`replicated`).
     */
    [[nodiscard]] std::optional<::iggy::Durability> Durability() const noexcept { return durability_; }

    /**
     * @brief Sets the message completion policy.
     * @param durability `replicated` or `persisted`, independent of the
     *        consumer-offset policy.
     * @return Reference to this options object.
     */
    TopicCreateOptions &SetDurability(::iggy::Durability durability) noexcept {
        durability_ = durability;
        return *this;
    }

    /**
     * @brief Returns the consumer-offset completion policy.
     * @return Configured policy, or `std::nullopt` to use the server default
     *         (`replicated`).
     */
    [[nodiscard]] std::optional<::iggy::Durability> ConsumerOffsetDurability() const noexcept {
        return consumer_offset_durability_;
    }

    /**
     * @brief Sets the consumer-offset completion policy.
     * @param durability `replicated` or `persisted`, independent of the
     *        message policy.
     * @return Reference to this options object.
     */
    TopicCreateOptions &SetConsumerOffsetDurability(::iggy::Durability durability) noexcept {
        consumer_offset_durability_ = durability;
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
    std::optional<::iggy::Durability> durability_;
    std::optional<::iggy::Durability> consumer_offset_durability_;
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
 * Use this class to supply stream settings to UpdateStream(). The server does
 * not support updating stream settings and rejects every supplied setting. The
 * raw entries allow settings to be passed without changing this C++ API when
 * the server adds mutable stream settings.
 */
class StreamUpdateOptions final {
  public:
    StreamUpdateOptions() = default;

    /**
     * @brief Returns the requested stream settings as key-value pairs.
     * @return Ordered map of setting names and values.
     * @note The server rejects all stream settings.
     */
    [[nodiscard]] const std::map<std::string, std::string> &RawEntries() const noexcept { return raw_; }

    /**
     * @brief Adds or replaces requested stream settings.
     * @param entries Setting names and values to add.
     * @return Reference to this options object.
     * @note The server rejects all stream settings.
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
     * @note The server rejects all stream settings.
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
 * @brief Options for updating a user.
 *
 * Use this class to supply user settings to UpdateUser(). Updating a user
 * patches only the supplied settings; omitted settings remain unchanged.
 * The server does not support updating user settings and rejects every
 * supplied setting. The raw entries allow settings to be passed without
 * changing this C++ API when the server adds mutable user settings.
 */
class UserUpdateOptions final {
  public:
    /** @brief Creates an update with no requested user settings. */
    UserUpdateOptions() = default;

    /**
     * @brief Returns the requested user settings as key-value pairs.
     * @return Ordered map of setting names and values.
     * @note The server rejects all user settings.
     */
    [[nodiscard]] const std::map<std::string, std::string> &RawEntries() const noexcept { return raw_; }

    /**
     * @brief Adds or replaces requested user settings.
     * @param entries Setting names and values to add.
     * @return Reference to this options object.
     * @note The server rejects all user settings.
     */
    UserUpdateOptions &SetRawEntries(const std::map<std::string, std::string> &entries) {
        for (const auto &entry : entries) {
            raw_.insert_or_assign(entry.first, entry.second);
        }
        return *this;
    }
    /**
     * @brief Adds or replaces requested user settings.
     * @param entries Setting names and values to move into this options object.
     * @return Reference to this options object.
     * @see SetRawEntries(const std::map<std::string, std::string>&)
     * @note The server rejects all user settings.
     */
    UserUpdateOptions &SetRawEntries(std::map<std::string, std::string> &&entries) {
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

/**
 * @brief Selects the destination partition for a batch of messages.
 *
 * Balanced() distributes batches across the topic's partitions. PartitionId()
 * selects one partition explicitly. MessagesKey() hashes the supplied bytes
 * modulo the topic's partition count, so changing that count can change the
 * destination for a key.
 *
 * The selected strategy applies to the entire SendMessages() call. Validation
 * is deferred until the batch is sent. In particular, a message key must
 * contain between 1 and 255 bytes.
 */
class Partitioning final {
  public:
    /**
     * @brief Selects partitions using the client's balanced strategy.
     * @return Balanced partitioning with no value payload.
     */
    static Partitioning Balanced() { return Partitioning("balanced", {}); }

    /**
     * @brief Selects a partition by numeric ID.
     * @param partition_id Destination partition ID.
     * @return Explicit partitioning whose value is the ID encoded as four
     *         little-endian bytes.
     */
    static Partitioning PartitionId(std::uint32_t partition_id) {
        constexpr std::size_t kPartitionIdBytes = 4;
        constexpr std::uint32_t kBitsPerByte    = 8;
        std::vector<std::uint8_t> partitioning_value(kPartitionIdBytes);
        for (std::size_t index = 0; index < partitioning_value.size(); ++index) {
            partitioning_value[index] = static_cast<std::uint8_t>(partition_id >> (index * kBitsPerByte));
        }
        return Partitioning("partition_id", std::move(partitioning_value));
    }

    /**
     * @brief Selects a partition by hashing a message key.
     * @param key Binary key used to select the destination partition.
     * @return Key-based partitioning owning @p key.
     * @note The key length is validated by SendMessages() and must be between
     *       1 and 255 bytes.
     */
    static Partitioning MessagesKey(std::vector<std::uint8_t> key) {
        return Partitioning("messages_key", std::move(key));
    }

    /**
     * @brief Returns the partitioning strategy name.
     * @return `balanced`, `partition_id`, or `messages_key`.
     */
    [[nodiscard]] std::string_view Kind() const { return partitioning_kind_; }

    /**
     * @brief Returns the strategy's encoded value.
     * @return Empty bytes for balanced partitioning, four little-endian bytes
     *         for an explicit partition ID, or the message key bytes.
     */
    [[nodiscard]] const std::vector<std::uint8_t> &Value() const noexcept { return partitioning_value_; }

  private:
    explicit Partitioning(std::string kind, std::vector<std::uint8_t> value)
        : partitioning_kind_(std::move(kind)), partitioning_value_(std::move(value)) {}

    std::string partitioning_kind_;
    std::vector<std::uint8_t> partitioning_value_;
};

/**
 * @brief Commit information for one partition written by SendMessages().
 *
 * BaseOffset() identifies where the first message from the committed batch
 * landed in this partition. Sending is at least once, so a retry may have
 * committed the same batch at an earlier offset. The confirmation therefore
 * does not establish uniqueness. It reports quorum commit; recoverable
 * stable-storage durability depends on the topic's durability policy.
 */
class SendMessagesConfirmation final {
  public:
    /**
     * @brief Returns the numeric stream ID resolved by the server.
     * @return Stream ID containing the committed batch.
     */
    [[nodiscard]] std::uint32_t StreamId() const noexcept { return stream_id_; }

    /**
     * @brief Returns the numeric topic ID resolved by the server.
     * @return Topic ID containing the committed batch.
     */
    [[nodiscard]] std::uint32_t TopicId() const noexcept { return topic_id_; }

    /**
     * @brief Returns the partition that received the batch.
     * @return Numeric partition ID.
     */
    [[nodiscard]] std::uint32_t PartitionId() const noexcept { return partition_id_; }

    /**
     * @brief Returns the offset assigned to the batch's first message.
     * @return First committed message offset in this partition.
     */
    [[nodiscard]] std::uint64_t BaseOffset() const noexcept { return base_offset_; }

  private:
    SendMessagesConfirmation(std::uint32_t stream_id,
                             std::uint32_t topic_id,
                             std::uint32_t partition_id,
                             std::uint64_t base_offset)
        : stream_id_(stream_id), topic_id_(topic_id), partition_id_(partition_id), base_offset_(base_offset) {}

    static SendMessagesConfirmation FromFfi(ffi::SendMessagesConfirmation confirmation);

    friend class SendMessagesResponse;

    std::uint32_t stream_id_;
    std::uint32_t topic_id_;
    std::uint32_t partition_id_;
    std::uint64_t base_offset_;
};

/**
 * @brief Result of a successful SendMessages() operation.
 *
 * Confirmations are returned per partition. The collection may be empty when
 * the server reports no offsets. An empty collection does not mean the send
 * failed.
 */
class SendMessagesResponse final {
  public:
    /**
     * @brief Returns the available per-partition commit information.
     * @return Confirmations owned by this response. The reference remains
     *         valid while this SendMessagesResponse remains alive.
     */
    [[nodiscard]] const std::vector<SendMessagesConfirmation> &Confirmations() const noexcept { return confirmations_; }

  private:
    explicit SendMessagesResponse(std::vector<SendMessagesConfirmation> confirmations)
        : confirmations_(std::move(confirmations)) {}

    static SendMessagesResponse FromFfi(ffi::SendMessagesResponse response);

    friend class IggyBlockingClient;

    std::vector<SendMessagesConfirmation> confirmations_;
};

/**
 * @brief Messages and partition state returned by PollMessages().
 *
 * CurrentOffset() is the partition's current offset observed for this request,
 * not the offset of the last returned message. For a consumer-group member
 * with no assigned partitions, an empty result can use
 * `std::numeric_limits<std::uint32_t>::max() - 1` as its partition ID.
 */
class PolledMessages final {
  public:
    /**
     * @brief Returns the partition selected for this poll.
     * @return Numeric partition ID, or the no-assignment sentinel described by
     *         PolledMessages when no consumer-group partition is available.
     */
    [[nodiscard]] std::uint32_t PartitionId() const noexcept { return partition_id_; }

    /**
     * @brief Returns the partition's current message offset.
     * @return Current offset observed by the server for this request.
     */
    [[nodiscard]] std::uint64_t CurrentOffset() const noexcept { return current_offset_; }

    /**
     * @brief Returns the number of messages in this result.
     * @return Message count reported by the server.
     */
    [[nodiscard]] std::uint32_t Count() const noexcept { return count_; }

    /**
     * @brief Returns the polled messages in partition order.
     * @return Messages owned by this result. The reference remains valid while
     *         this PolledMessages remains alive.
     */
    [[nodiscard]] const std::vector<IggyMessagePolled> &Messages() const noexcept { return messages_; }

  private:
    PolledMessages(std::uint32_t partition_id,
                   std::uint64_t current_offset,
                   std::uint32_t count,
                   std::vector<IggyMessagePolled> messages)
        : partition_id_(partition_id), current_offset_(current_offset), count_(count), messages_(std::move(messages)) {}

    static PolledMessages FromFfi(ffi::PolledMessages polled);

    friend class IggyBlockingClient;

    std::uint32_t partition_id_;
    std::uint64_t current_offset_;
    std::uint32_t count_;
    std::vector<IggyMessagePolled> messages_;
};

/**
 * @brief One server-supported resource option returned by DescribeOptions().
 *
 * The default value is encoded according to Kind(), using the same HeaderKind
 * codes as typed header fields. An empty default value means that the option
 * has no default.
 */
class OptionSpec final {
  public:
    /**
     * @brief Returns the option name accepted by the resource command.
     * @return Option key owned by this value.
     */
    [[nodiscard]] const std::string &Key() const noexcept { return key_; }

    /**
     * @brief Returns the HeaderKind code for the option's default value.
     * @return Numeric code corresponding to a HeaderKind enumerator.
     */
    [[nodiscard]] std::uint8_t Kind() const noexcept { return kind_; }

    /**
     * @brief Returns the server default encoded according to Kind().
     * @return Encoded default value, or an empty vector when none is defined.
     */
    [[nodiscard]] const std::vector<std::uint8_t> &DefaultValue() const noexcept { return default_value_; }

    /**
     * @brief Returns the server-provided option description.
     * @return Human-readable description owned by this value.
     */
    [[nodiscard]] const std::string &Description() const noexcept { return description_; }

  private:
    OptionSpec(std::string key, std::uint8_t kind, std::vector<std::uint8_t> default_value, std::string description)
        : key_(std::move(key)),
          kind_(kind),
          default_value_(std::move(default_value)),
          description_(std::move(description)) {}

    static OptionSpec FromFfi(ffi::OptionSpec spec);

    friend class IggyBlockingClient;

    std::string key_;
    std::uint8_t kind_;
    std::vector<std::uint8_t> default_value_;
    std::string description_;
};

/**
 * @brief Client-facing transport ports advertised for a cluster node.
 *
 * A port value of zero means that the corresponding transport is not
 * advertised for the node.
 */
class TransportEndpoints final {
  public:
    /**
     * @brief Returns the advertised TCP port.
     * @return TCP port, or zero when TCP is not advertised.
     */
    [[nodiscard]] std::uint16_t Tcp() const noexcept { return tcp_; }

    /**
     * @brief Returns the advertised QUIC port.
     * @return QUIC port, or zero when QUIC is not advertised.
     */
    [[nodiscard]] std::uint16_t Quic() const noexcept { return quic_; }

    /**
     * @brief Returns the advertised HTTP port.
     * @return HTTP port, or zero when HTTP is not advertised.
     */
    [[nodiscard]] std::uint16_t Http() const noexcept { return http_; }

    /**
     * @brief Returns the advertised WebSocket port.
     * @return WebSocket port, or zero when WebSocket is not advertised.
     */
    [[nodiscard]] std::uint16_t Websocket() const noexcept { return websocket_; }

  private:
    TransportEndpoints(std::uint16_t tcp, std::uint16_t quic, std::uint16_t http, std::uint16_t websocket)
        : tcp_(tcp), quic_(quic), http_(http), websocket_(websocket) {}

    static TransportEndpoints FromFfi(ffi::TransportEndpoints endpoints);

    friend class ClusterNode;

    std::uint16_t tcp_;
    std::uint16_t quic_;
    std::uint16_t http_;
    std::uint16_t websocket_;
};

/**
 * @brief One node in the server's cluster topology.
 *
 * The address and ports are client-facing endpoints selected for the requesting
 * client's network. Role and status are lowercase server values. Current role
 * values are `leader` and `follower`; status values include `healthy`,
 * `starting`, `stopping`, `unreachable`, `maintenance`, and `unknown`.
 */
class ClusterNode final {
  public:
    /**
     * @brief Returns the configured node name.
     * @return Node name owned by this value.
     */
    [[nodiscard]] const std::string &Name() const noexcept { return name_; }

    /**
     * @brief Returns the client-facing node address.
     * @return IP address or host name owned by this value, without a port.
     */
    [[nodiscard]] const std::string &Ip() const noexcept { return ip_; }

    /**
     * @brief Returns the node's advertised client transport ports.
     * @return Transport endpoints owned by this value.
     */
    [[nodiscard]] const TransportEndpoints &Endpoints() const noexcept { return endpoints_; }

    /**
     * @brief Returns the node's current cluster role.
     * @return Lowercase role name supplied by the server.
     */
    [[nodiscard]] const std::string &Role() const noexcept { return role_; }

    /**
     * @brief Returns the node's current status.
     * @return Lowercase status name supplied by the server.
     */
    [[nodiscard]] const std::string &Status() const noexcept { return status_; }

  private:
    ClusterNode(std::string name, std::string ip, TransportEndpoints endpoints, std::string role, std::string status)
        : name_(std::move(name)),
          ip_(std::move(ip)),
          endpoints_(std::move(endpoints)),
          role_(std::move(role)),
          status_(std::move(status)) {}

    static ClusterNode FromFfi(ffi::ClusterNode node);

    friend class ClusterMetadata;

    std::string name_;
    std::string ip_;
    TransportEndpoints endpoints_;
    std::string role_;
    std::string status_;
};

/**
 * @brief Snapshot of the server's advertised cluster topology.
 *
 * The result contains one entry per configured cluster node. A server without
 * an enabled cluster reports a synthesized single-node cluster. Leadership and
 * status can change immediately after the metadata is returned.
 */
class ClusterMetadata final {
  public:
    /**
     * @brief Returns the advertised cluster name.
     * @return Cluster name owned by this value.
     */
    [[nodiscard]] const std::string &Name() const noexcept { return name_; }

    /**
     * @brief Returns the advertised cluster nodes.
     * @return Nodes owned by this value. The reference remains valid while
     *         this ClusterMetadata remains alive.
     */
    [[nodiscard]] const std::vector<ClusterNode> &Nodes() const noexcept { return nodes_; }

  private:
    ClusterMetadata(std::string name, std::vector<ClusterNode> nodes)
        : name_(std::move(name)), nodes_(std::move(nodes)) {}

    static ClusterMetadata FromFfi(ffi::ClusterMetadata metadata);

    friend class IggyBlockingClient;

    std::string name_;
    std::vector<ClusterNode> nodes_;
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
     * @brief Retrieves one user by numeric ID or name.
     *
     * An authenticated user may retrieve its own account without the global
     * read-users grant. Reading another account requires read-users or
     * manage-users permission. The result is a snapshot and includes the
     * target user's optional permission assignment.
     *
     * @param user User to retrieve, addressed by numeric ID or name.
     * @return Details for the requested user.
     * @throws IggyException if the client is unavailable or unauthenticated;
     *         the identifier is invalid; the user does not exist; the caller
     *         lacks permission; or the request fails.
     */
    UserInfoDetails GetUser(const Identifier &user);

    /**
     * @brief Lists user summaries visible to the authenticated caller.
     *
     * The summaries omit permissions. Use GetUser() to retrieve one user's
     * permission assignment.
     *
     * @return User summaries observed by the server for this request.
     * @throws IggyException if the client is unavailable or unauthenticated;
     *         the caller lacks read-users or manage-users permission; or the
     *         request fails.
     */
    std::vector<UserInfo> GetUsers();

    /**
     * @brief Creates a user account.
     *
     * User names must contain between 3 and 50 bytes and be unique. Passwords
     * must contain between 3 and 100 bytes. An inactive account is created but
     * cannot authenticate. Passing `std::nullopt` assigns no permission object;
     * passing a default-constructed Permissions assigns an explicit permission
     * object with no enabled grants.
     *
     * A failed or unknown transport outcome can leave the user created. Query
     * the account by name before retrying this request.
     *
     * @param username Unique user name.
     * @param password Initial user password.
     * @param status Initial authentication status.
     * @param permissions Optional permission assignment.
     * @return Details of the newly created user.
     * @throws IggyException if the client is unavailable or unauthenticated;
     *         the name, password, status, or permissions are invalid; the name
     *         is already in use; the caller lacks manage-users permission; or
     *         the request fails.
     */
    UserInfoDetails CreateUser(std::string username,
                               std::string password,
                               UserStatus status,
                               const std::optional<Permissions> &permissions = std::nullopt);

    /**
     * @brief Deletes a user account.
     *
     * The root user cannot be deleted. Deleting another user also removes its
     * permission assignment and personal access tokens. A failed or unknown
     * transport outcome can leave the deletion committed; look up the user
     * before retrying.
     *
     * @param user User to delete, addressed by numeric ID or name.
     * @throws IggyException if the client is unavailable or unauthenticated;
     *         the identifier is invalid; the user does not exist or is the
     *         root user; the caller lacks manage-users permission; or the
     *         request fails.
     */
    void DeleteUser(const Identifier &user);

    /**
     * @brief Changes a user's name, status, and mutable settings.
     *
     * A supplied name must contain between 3 and 50 bytes and remain unique.
     * Setting the status to inactive prevents subsequent authentication.
     * Passing `std::nullopt` for the name or status leaves that field
     * unchanged. The supplied UserUpdateOptions changes only the settings it
     * contains; settings left unset retain their current values. User settings
     * are currently not mutable, so the options object must be empty. Passing
     * `std::nullopt` for both fields and an empty options object is accepted as
     * a no-op.
     *
     * A failed or unknown transport outcome can leave the update committed.
     * Retrieve the user before retrying with different values.
     *
     * @param user User to update, addressed by numeric ID or name.
     * @param username New unique name, or `std::nullopt` to retain the name.
     * @param status New authentication status, or `std::nullopt` to retain the
     *        status.
     * @param options User update options.
     * @throws IggyException if the client is unavailable or unauthenticated;
     *         an identifier, name, status, or option is invalid; an option is
     *         unsupported; the user does not exist; the name is already in
     *         use; the caller lacks manage-users permission; or the request
     *         fails.
     */
    void UpdateUser(const Identifier &user,
                    std::optional<std::string> username,
                    std::optional<UserStatus> status,
                    const UserUpdateOptions &options = {});

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

    /**
     * @brief Deletes the oldest sealed segments from one partition.
     *
     * The active segment is never deleted. If fewer sealed segments exist than
     * requested, every sealed segment is selected. A count of zero, or a
     * partition with no sealed segments, succeeds without deleting data. The
     * server commits a truncation watermark before local replicas remove the
     * selected segment files.
     *
     * A failed or unknown transport outcome can leave the truncation committed.
     * Inspect the partition before retrying this destructive request.
     *
     * @param stream Parent stream, addressed by numeric ID or name.
     * @param topic Parent topic, addressed by numeric ID or name.
     * @param partition_id Numeric partition ID.
     * @param segments_count Maximum number of oldest sealed segments to delete.
     * @throws IggyException if the client is unavailable or unauthenticated;
     *         an identifier or partition is invalid; the stream, topic, or
     *         partition does not exist; the caller lacks topic-management
     *         permission; or the request fails.
     */
    void DeleteSegments(const Identifier &stream,
                        const Identifier &topic,
                        std::uint32_t partition_id,
                        std::uint32_t segments_count);

    /**
     * @brief Creates a consumer group for a topic.
     *
     * The group name must be unique within the topic, non-empty, and no more
     * than 255 UTF-8 bytes. The new group initially has no members.
     *
     * The server assigns consumer group IDs monotonically. Deleting a group
     * and recreating it with the same name is allowed, but the recreated group
     * receives a new ID rather than reusing the deleted group's ID.
     *
     * @param stream Parent stream, addressed by numeric ID or name.
     * @param topic Parent topic, addressed by numeric ID or name.
     * @param name Unique consumer group name within @p topic.
     * @return Details of the newly created consumer group.
     * @throws IggyException if the client is unavailable or unauthenticated;
     *         an identifier or the name is invalid; the stream or topic does
     *         not exist; the name is already in use; the caller lacks
     *         stream- or topic-management permission; or the request fails.
     */
    ConsumerGroupDetails CreateConsumerGroup(const Identifier &stream, const Identifier &topic, std::string name);

    /**
     * @brief Retrieves one consumer group and its current members.
     *
     * The returned details are a snapshot. Membership and partition
     * assignments can change immediately after this call returns.
     *
     * @param stream Parent stream, addressed by numeric ID or name.
     * @param topic Parent topic, addressed by numeric ID or name.
     * @param group Consumer group to retrieve, addressed by numeric ID or name.
     * @return Consumer group metadata and member details.
     * @throws IggyException if the client is unavailable or unauthenticated;
     *         an identifier is invalid; the stream, topic, or consumer group
     *         does not exist; the caller lacks read permission; or the
     *         metadata read fails.
     */
    ConsumerGroupDetails GetConsumerGroup(const Identifier &stream, const Identifier &topic, const Identifier &group);

    /**
     * @brief Lists consumer group summaries for a topic.
     *
     * The summaries include member and partition counts but omit individual
     * member details. Use GetConsumerGroup() to retrieve those details.
     *
     * The server reports a missing parent stream or topic as an error. An empty
     * result means both parent resources existed when the request was
     * evaluated.
     *
     * @param stream Parent stream, addressed by numeric ID or name.
     * @param topic Parent topic, addressed by numeric ID or name.
     * @return Consumer group summaries for the requested topic.
     * @throws IggyException if the client is unavailable or unauthenticated;
     *         an identifier is invalid; the stream or topic does not exist;
     *         the caller lacks read permission; or the metadata read fails.
     */
    std::vector<ConsumerGroup> GetConsumerGroups(const Identifier &stream, const Identifier &topic);

    /**
     * @brief Deletes a consumer group from a topic.
     *
     * A failed or unknown transport outcome can leave the deletion committed.
     * Query the topic's consumer groups before retrying this request.
     *
     * @param stream Parent stream, addressed by numeric ID or name.
     * @param topic Parent topic, addressed by numeric ID or name.
     * @param group Consumer group to delete, addressed by numeric ID or name.
     * @throws IggyException if the client is unavailable or unauthenticated;
     *         an identifier is invalid; the stream, topic, or consumer group
     *         does not exist; the caller lacks stream- or topic-management
     *         permission; or the request fails.
     */
    void DeleteConsumerGroup(const Identifier &stream, const Identifier &topic, const Identifier &group);

    /**
     * @brief Joins the current client to a consumer group.
     *
     * The server assigns topic partitions among the group's members. Joining
     * the same group again does not add a second membership for this client.
     * Joining consumer groups over HTTP is not supported.
     *
     * @param stream Parent stream, addressed by numeric ID or name.
     * @param topic Parent topic, addressed by numeric ID or name.
     * @param group Consumer group to join, addressed by numeric ID or name.
     * @throws IggyException if the client is unavailable or unauthenticated;
     *         an identifier is invalid; the stream, topic, or consumer group
     *         does not exist; the caller lacks read permission; the transport
     *         does not support group membership; or the request fails.
     */
    void JoinConsumerGroup(const Identifier &stream, const Identifier &topic, const Identifier &group);

    /**
     * @brief Removes the current client from a consumer group.
     *
     * The server reassigns partitions among the remaining group members.
     * The client must currently belong to the group; leaving twice or leaving
     * without first joining fails. Leaving consumer groups over HTTP is not
     * supported.
     *
     * @param stream Parent stream, addressed by numeric ID or name.
     * @param topic Parent topic, addressed by numeric ID or name.
     * @param group Consumer group to leave, addressed by numeric ID or name.
     * @throws IggyException if the client is unavailable or unauthenticated;
     *         an identifier is invalid; the stream, topic, or consumer group
     *         does not exist; this client is not a member; the caller lacks
     *         read permission; the transport does not support group
     *         membership; or the request fails.
     */
    void LeaveConsumerGroup(const Identifier &stream, const Identifier &topic, const Identifier &group);

    /**
     * @brief Stores an offset for a consumer or consumer group.
     *
     * The server accepts offsets from zero through the partition's current
     * offset, inclusive. It rejects every offset for an empty partition and
     * any offset beyond the current offset. Storing another value for the same
     * consumer and partition replaces the previous value.
     *
     * For a consumer group, the group must exist and the current client must
     * own @p partition_id in that group. This ownership fence does not apply to
     * individual consumers.
     *
     * @param consumer Consumer identity that owns the offset.
     * @param stream Parent stream, addressed by numeric ID or name.
     * @param topic Parent topic, addressed by numeric ID or name.
     * @param partition_id Partition whose offset is stored, or `std::nullopt`
     *        to omit the partition from the request. The maximum
     *        `std::uint32_t` value is rejected because it is reserved by the
     *        FFI representation.
     * @param offset Message offset to store.
     * @throws IggyException if an identifier, partition, or offset is invalid;
     *         the resource does not exist; the client is unauthenticated; the
     *         caller lacks permission; or the request fails.
     */
    void StoreConsumerOffset(const Consumer &consumer,
                             const Identifier &stream,
                             const Identifier &topic,
                             std::optional<std::uint32_t> partition_id,
                             std::uint64_t offset);

    /**
     * @brief Retrieves the stored offset for a consumer or consumer group.
     *
     * This method throws IggyException when no offset has been stored. A
     * consumer group offset can be read by an authenticated caller with poll
     * permission even when that client is not a member of the group.
     *
     * An offset created by an auto-commit poll is visible through this method.
     * A local auto-commit cursor can be visible before its durable store has
     * committed.
     *
     * @param consumer Consumer identity that owns the offset.
     * @param stream Parent stream, addressed by numeric ID or name.
     * @param topic Parent topic, addressed by numeric ID or name.
     * @param partition_id Partition whose offset is retrieved, or
     *        `std::nullopt` to omit the partition from the request. The
     *        maximum `std::uint32_t` value is rejected because it is reserved
     *        by the FFI representation.
     * @return Partition state and the stored consumer offset.
     * @throws IggyException if an identifier or partition is invalid; the
     *         resource or stored offset does not exist; the client is
     *         unauthenticated; the caller lacks permission; or the request
     *         fails.
     */
    ConsumerOffsetInfo GetConsumerOffset(const Consumer &consumer,
                                         const Identifier &stream,
                                         const Identifier &topic,
                                         std::optional<std::uint32_t> partition_id = std::nullopt);

    /**
     * @brief Deletes the stored offset for a consumer or consumer group.
     *
     * Deletion is not idempotent: deleting an offset that was never stored, or
     * deleting the same offset again, fails. A failed or unknown transport
     * outcome can leave the deletion committed, in which case a retry can fail
     * because the offset is already absent.
     *
     * For a consumer group, the group must exist and the current client must
     * own @p partition_id in that group. This ownership fence does not apply to
     * individual consumers.
     *
     * @param consumer Consumer identity that owns the offset.
     * @param stream Parent stream, addressed by numeric ID or name.
     * @param topic Parent topic, addressed by numeric ID or name.
     * @param partition_id Partition whose offset is deleted, or `std::nullopt`
     *        to omit the partition from the request. The maximum
     *        `std::uint32_t` value is rejected because it is reserved by the
     *        FFI representation.
     * @throws IggyException if an identifier or partition is invalid; the
     *         resource or stored offset does not exist; the client is
     *         unauthenticated; the caller lacks permission; or the request
     *         fails.
     */
    void DeleteConsumerOffset(const Consumer &consumer,
                              const Identifier &stream,
                              const Identifier &topic,
                              std::optional<std::uint32_t> partition_id = std::nullopt);

    /**
     * @brief Retrieves details for this client connection.
     *
     * The result includes consumer-group memberships observed for the current
     * authenticated connection. HTTP is stateless and does not expose a
     * persistent current connection, so the HTTP transport reports this
     * operation as unavailable.
     *
     * @return Details for the current client connection.
     * @throws IggyException if the client is unavailable or unauthenticated;
     *         the transport does not support the operation; or the request
     *         fails.
     */
    ClientInfoDetails GetMe();

    /**
     * @brief Retrieves one currently connected client by numeric ID.
     *
     * The result is a snapshot containing the connection's observed consumer-
     * group memberships. The connection can disappear immediately after the
     * request completes.
     *
     * @param client_id Server-assigned connection ID.
     * @return Details for the requested client connection.
     * @throws IggyException if the client is unavailable or unauthenticated;
     *         the requested connection does not exist; the caller lacks
     *         read-servers or manage-servers permission; or the request fails.
     */
    ClientInfoDetails GetClient(std::uint32_t client_id);

    /**
     * @brief Lists client connections currently known to the server.
     *
     * Each entry is a snapshot summary and omits individual consumer-group
     * identifiers. Use GetClient() for membership details.
     *
     * @return Client connection summaries observed for this request.
     * @throws IggyException if the client is unavailable or unauthenticated;
     *         the caller lacks read-servers or manage-servers permission; or
     *         the request fails.
     */
    std::vector<ClientInfo> GetClients();

    /**
     * @brief Retrieves server process, storage, and resource statistics.
     *
     * The returned fields are an observed snapshot and can change immediately.
     * Process and host measurements describe the server that handles the
     * request; metadata totals describe the state visible to that server.
     *
     * @return Statistics observed for this request.
     * @throws IggyException if the client is unavailable or unauthenticated;
     *         the caller lacks read-servers or manage-servers permission; or
     *         the request fails.
     */
    Stats GetStats();

    /**
     * @brief Sends a non-empty batch of messages to one topic.
     *
     * The partitioning strategy selects one destination for the entire batch.
     * Message and partitioning constraints are validated before the request is
     * sent. The Rust SDK assigns generated IDs to messages whose ID is zero;
     * the C++ input objects are not modified.
     *
     * Delivery is at least once. A failed or unknown transport outcome can
     * leave the batch committed, so retrying may append duplicates. Successful
     * responses can contain no confirmations; see SendMessagesResponse.
     *
     * @param stream Parent stream, addressed by numeric ID or name.
     * @param topic Destination topic, addressed by numeric ID or name.
     * @param partitioning Strategy used to select the destination partition.
     * @param messages Messages to send. The collection must not be empty.
     * @return Available per-partition commit confirmations.
     * @throws IggyException if the client is unavailable or unauthenticated;
     *         an identifier, partitioning strategy, message, or batch is
     *         invalid; the stream, topic, or partition does not exist; the
     *         caller lacks send permission; or the request fails.
     */
    SendMessagesResponse SendMessages(const Identifier &stream,
                                      const Identifier &topic,
                                      const Partitioning &partitioning,
                                      const std::vector<IggyMessageToSend> &messages);

    /**
     * @brief Polls messages for an individual consumer or consumer group.
     *
     * For an individual consumer, an omitted partition selects partition zero.
     * For a consumer group, an omitted partition lets the stateful client
     * select one of this connection's assigned partitions. An explicit group
     * partition must belong to the current connection. Group membership is not
     * supported by the HTTP transport.
     *
     * With @p auto_commit enabled, the server can advance the consumer cursor
     * before the response reaches the caller. Retrying a missing response with
     * PollingStrategy::Next() can therefore skip messages. Applications that
     * require controlled recovery should checkpoint processed message offsets
     * and resume with PollingStrategy::Offset(). CurrentOffset() is not such a
     * checkpoint because it describes the partition rather than the last
     * message returned.
     *
     * @param stream Parent stream, addressed by numeric ID or name.
     * @param topic Topic to poll, addressed by numeric ID or name.
     * @param partition_id Partition to poll, or `std::nullopt` for the default
     *        individual partition or client-selected group partition. The
     *        maximum `std::uint32_t` value is reserved by the FFI and rejected.
     * @param consumer Consumer identity that owns the polling cursor.
     * @param strategy Starting position for this poll.
     * @param count Maximum number of messages to return. Must be greater than
     *        zero.
     * @param auto_commit Whether to advance the consumer offset automatically.
     * @return Selected partition state and the messages read from it.
     * @throws IggyException if the client is unavailable or unauthenticated;
     *         an identifier, partition, strategy, or count is invalid; a
     *         resource does not exist; group membership or partition ownership
     *         is missing; the caller lacks poll permission; or the request
     *         fails.
     */
    PolledMessages PollMessages(const Identifier &stream,
                                const Identifier &topic,
                                std::optional<std::uint32_t> partition_id,
                                const Consumer &consumer,
                                const PollingStrategy &strategy,
                                std::uint32_t count,
                                bool auto_commit);

    /**
     * @brief Retrieves the server's option catalog for one resource type.
     *
     * The catalog describes accepted create-option keys, their HeaderKind
     * encodings, defaults, and descriptions. A supported scope with no option
     * keys returns an empty collection. This is currently the case for the
     * `stream` and `user` scopes.
     *
     * @param scope Resource scope. Must be `topic`, `stream`, or `user`.
     * @return Option specifications for the requested scope.
     * @throws IggyException if the client is unavailable or unauthenticated;
     *         @p scope is invalid; or the request fails.
     */
    std::vector<OptionSpec> DescribeOptions(std::string scope);

    /**
     * @brief Checks whether the configured server can answer a request.
     *
     * Ping does not require authentication and returns no server data.
     *
     * @throws IggyException if the client is unavailable or the request fails.
     */
    void Ping();

    /**
     * @brief Returns the client's configured heartbeat interval.
     * @return Non-zero interval in microseconds.
     * @note This reads client configuration and does not contact the server.
     * @throws IggyException if this client has been moved from.
     */
    std::chrono::microseconds HeartbeatInterval();

    /**
     * @brief Captures server diagnostics as a ZIP archive.
     *
     * Duplicate snapshot types are removed. SystemSnapshotType::All() must be
     * the only requested type and expands to all production diagnostic
     * sections. The server runs only one snapshot collection at a time; a
     * concurrent request fails. A section that cannot be captured can be
     * omitted while the remaining archive is still returned.
     *
     * @param compression Compression method used for ZIP entries.
     * @param types Non-empty collection of diagnostic sections to capture.
     * @return Complete ZIP archive bytes owned by the caller.
     * @throws IggyException if the client is unavailable or unauthenticated;
     *         the caller lacks read-servers or manage-servers permission; the
     *         requested types are invalid; another snapshot is in progress;
     *         archive creation fails; or the request fails.
     */
    std::vector<std::uint8_t> Snapshot(const SnapshotCompression &compression,
                                       const std::vector<SystemSnapshotType> &types);

    /**
     * @brief Sends one command through the raw Iggy binary protocol.
     *
     * The caller supplies only the command body and receives only the response
     * body; the configured transport adds and removes protocol framing. The
     * command keeps its normal authentication, authorization, and mutation
     * semantics. Session-control commands for login, logout, and registration
     * are rejected so that the SDK's session state cannot be bypassed.
     *
     * @param code Iggy binary-protocol command code.
     * @param payload Command body encoded for @p code.
     * @return Raw response body without protocol framing.
     * @throws IggyException if the transport is HTTP; @p code is a
     *         session-control or unsupported command; the payload is invalid;
     *         the command's access checks fail; or the request fails.
     */
    std::vector<std::uint8_t> SendBinaryRequest(std::uint32_t code, const std::vector<std::uint8_t> &payload);

    /**
     * @brief Replaces or removes a user's complete permission assignment.
     *
     * Passing `std::nullopt` removes the permission object. Passing a
     * default-constructed Permissions assigns an explicit permission object
     * with no enabled grants. The root user's permissions are immutable.
     *
     * A failed or unknown transport outcome can leave the replacement
     * committed. Retrieve the user before retrying with a different value.
     *
     * @param user User to update, addressed by numeric ID or name.
     * @param permissions Replacement assignment, or `std::nullopt` to remove
     *        the current assignment.
     * @throws IggyException if the client is unavailable or unauthenticated;
     *         the identifier or permissions are invalid; the user does not
     *         exist or is the root user; the caller lacks manage-users
     *         permission; or the request fails.
     */
    void UpdatePermissions(const Identifier &user, const std::optional<Permissions> &permissions);

    /**
     * @brief Changes a user's password after verifying its current value.
     *
     * Both passwords must contain between 3 and 100 bytes. The current password
     * is checked against the target user even when an administrator changes
     * another account. A user may change its own password without manage-users
     * permission; changing another user's password requires that permission.
     *
     * A failed or unknown transport outcome can leave the password changed.
     * Verify which credential works before retrying.
     *
     * @param user User whose password is changed, addressed by numeric ID or
     *        name.
     * @param current_password Target user's current password.
     * @param new_password Replacement password.
     * @throws IggyException if the client is unavailable or unauthenticated;
     *         the identifier or password is invalid; the user does not exist;
     *         the current password does not match; the caller lacks permission;
     *         or the request fails.
     */
    void ChangePassword(const Identifier &user, std::string current_password, std::string new_password);

    /**
     * @brief Retrieves the server's current cluster topology.
     *
     * The result includes client-facing addresses, transport ports, roles, and
     * statuses for the configured nodes. No cluster-wide read grant is needed,
     * but the caller must be authenticated. A server without an enabled cluster
     * reports a synthesized single-node topology.
     *
     * @return Cluster metadata observed for this request.
     * @throws IggyException if the client is unavailable or unauthenticated;
     *         the response is invalid; or the request fails.
     */
    ClusterMetadata GetClusterMetadata();

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
    [[nodiscard]] IggyBlockingClient Build() const;

  private:
    std::string server_address_;
    ffi::AutoLoginKind auto_login_kind_{ffi::AutoLoginKind::Disabled};
    std::string auto_login_username_;
    std::string auto_login_password_;
    std::string personal_access_token_;
    std::optional<std::uint32_t> reconnection_max_retries_;
    std::optional<std::uint64_t> reconnection_interval_micros_;
    std::optional<std::uint64_t> reestablish_after_micros_;
    bool tls_enabled_{};
    std::string tls_domain_;
    std::string tls_ca_file_;
    std::optional<bool> tls_validate_certificate_;
    bool no_delay_{};
};

}  // namespace iggy
