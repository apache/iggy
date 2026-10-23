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

#include "iggy.hpp"

namespace iggy {

namespace {

rust::Vec<ffi::HeaderEntry> ToFfiRawOptions(const std::map<std::string, std::string> &raw_options) {
    rust::Vec<ffi::HeaderEntry> ffi_options;
    ffi_options.reserve(raw_options.size());
    for (const auto &[key, value] : raw_options) {
        ffi::HeaderEntry ffi_entry;
        ffi_entry.key.kind = static_cast<std::uint8_t>(HeaderKind::String);
        ffi_entry.key.value.reserve(key.size());
        for (const char character : key) {
            ffi_entry.key.value.push_back(static_cast<std::uint8_t>(character));
        }
        ffi_entry.value.kind = static_cast<std::uint8_t>(HeaderKind::String);
        ffi_entry.value.value.reserve(value.size());
        for (const char character : value) {
            ffi_entry.value.value.push_back(static_cast<std::uint8_t>(character));
        }
        ffi_options.push_back(std::move(ffi_entry));
    }
    return ffi_options;
}

template <typename Key>
rust::Vec<rust::String> ToFfiResetKeys(const std::set<Key> &keys) {
    rust::Vec<rust::String> ffi_keys;
    ffi_keys.reserve(keys.size());
    for (const auto key : keys) {
        const auto name = to_string(key);
        ffi_keys.push_back(rust::String(name.data(), name.size()));
    }
    return ffi_keys;
}

rust::Vec<rust::String> ToFfiRawResetKeys(const std::set<std::string> &keys) {
    rust::Vec<rust::String> ffi_keys;
    ffi_keys.reserve(keys.size());
    for (const auto &key : keys) {
        ffi_keys.push_back(rust::String(key));
    }
    return ffi_keys;
}

rust::Vec<std::uint8_t> ToFfiBytes(const std::vector<std::uint8_t> &bytes) {
    rust::Vec<std::uint8_t> ffi_bytes;
    ffi_bytes.reserve(bytes.size());
    for (const auto byte : bytes) {
        ffi_bytes.push_back(byte);
    }
    return ffi_bytes;
}

template <typename FfiOptions, typename Options>
void SetMutableTopicOptions(FfiOptions &ffi_options, const Options &options) {
    if (const auto &value = options.MessageExpiry()) {
        ffi_options.has_message_expiry   = true;
        ffi_options.message_expiry_kind  = std::string(value->Kind());
        ffi_options.message_expiry_value = value->Value();
    }
    if (const auto &value = options.MaxTopicSize()) {
        ffi_options.has_max_topic_size = true;
        ffi_options.max_topic_size     = std::string(value->Value());
    }
    ffi_options.raw_options = ToFfiRawOptions(options.RawEntries());
}

}  // namespace

IggyBlockingClient::IggyBlockingClient(IggyBlockingClient &&other) noexcept
    : client_(std::exchange(other.client_, nullptr)) {}

IggyBlockingClient &IggyBlockingClient::operator=(IggyBlockingClient &&other) noexcept {
    if (this != &other) {
        Reset();
        client_ = std::exchange(other.client_, nullptr);
    }
    return *this;
}

IggyBlockingClient::~IggyBlockingClient() {
    Reset();
}

IggyBlockingClient IggyBlockingClient::FromConnectionString(std::string connection_string) {
    return RethrowAsIggyException(
        [&connection_string] { return IggyBlockingClient(ffi::from_connection_string(connection_string)); });
}

void IggyBlockingClient::Connect() {
    RethrowAsIggyException([this] { Handle()->connect(); });
}

void IggyBlockingClient::Disconnect() {
    RethrowAsIggyException([this] { Handle()->disconnect(); });
}

void IggyBlockingClient::Shutdown() {
    RethrowAsIggyException([this] { Handle()->shutdown(); });
}

void IggyBlockingClient::ResolveOperation(std::uint64_t operation_id, std::chrono::microseconds timeout) {
    if (timeout.count() <= 0) {
        throw IggyException("Resolution timeout must be positive");
    }
    RethrowAsIggyException([this, operation_id, timeout](ffi::ErrorInfo &error_info) {
        Handle()->resolve_operation(operation_id, static_cast<std::uint64_t>(timeout.count()), error_info);
    });
}

void IggyBlockingClient::AbandonOperation(std::uint64_t operation_id) {
    RethrowAsIggyException(
        [this, operation_id](ffi::ErrorInfo &error_info) { Handle()->abandon_operation(operation_id, error_info); });
}

LoginInfo IggyBlockingClient::Login(std::string username, std::string password) {
    return RethrowAsIggyException([this, &username, &password](ffi::ErrorInfo &error_info) {
        return LoginInfo::FromFfi(Handle()->login_user(username, password, error_info));
    });
}

void IggyBlockingClient::Logout() {
    RethrowAsIggyException([this](ffi::ErrorInfo &error_info) { Handle()->logout_user(error_info); });
}

UserInfoDetails IggyBlockingClient::GetUser(const Identifier &user) {
    return RethrowAsIggyException([this, &user] { return UserInfoDetails::FromFfi(Handle()->get_user(user.ToFfi())); });
}

std::vector<UserInfo> IggyBlockingClient::GetUsers() {
    return RethrowAsIggyException([this] {
        std::vector<UserInfo> users;
        auto ffi_users = Handle()->get_users();
        users.reserve(ffi_users.size());
        for (auto &user : ffi_users) {
            users.push_back(UserInfo::FromFfi(std::move(user)));
        }
        return users;
    });
}

UserInfoDetails IggyBlockingClient::CreateUser(std::string username,
                                               std::string password,
                                               UserStatus status,
                                               const std::optional<Permissions> &permissions,
                                               const UserCreateOptions &options) {
    return RethrowAsIggyException([this, &username, &password, status, &permissions,
                                   &options](ffi::ErrorInfo &error_info) {
        auto ffi_permissions = permissions ? permissions->ToFfi() : ffi::Permissions{};
        ffi::UserCreateOptions ffi_options{};
        ffi_options.raw_options = ToFfiRawOptions(options.RawEntries());
        return UserInfoDetails::FromFfi(Handle()->create_user(username, password, static_cast<ffi::UserStatus>(status),
                                                              permissions.has_value(), std::move(ffi_permissions),
                                                              std::move(ffi_options), error_info));
    });
}

void IggyBlockingClient::DeleteUser(const Identifier &user) {
    RethrowAsIggyException(
        [this, &user](ffi::ErrorInfo &error_info) { Handle()->delete_user(user.ToFfi(), error_info); });
}

void IggyBlockingClient::UpdateUser(const Identifier &user, const UserUpdateOptions &options) {
    RethrowAsIggyException([this, &user, &options](ffi::ErrorInfo &error_info) {
        ffi::UserUpdateOptions ffi_options{};
        ffi_options.has_username = options.Username().has_value();
        ffi_options.username     = options.Username().value_or("");
        ffi_options.has_status   = options.Status().has_value();
        ffi_options.status       = static_cast<ffi::UserStatus>(options.Status().value_or(UserStatus::Active));
        ffi_options.reset        = ToFfiResetKeys(options.ResetKeys());
        ffi_options.raw_reset    = ToFfiRawResetKeys(options.RawResetKeys());
        ffi_options.raw_options  = ToFfiRawOptions(options.RawEntries());
        Handle()->update_user(user.ToFfi(), std::move(ffi_options), error_info);
    });
}

StreamDetails IggyBlockingClient::CreateStream(std::string name, const StreamCreateOptions &options) {
    return RethrowAsIggyException([this, &name, &options](ffi::ErrorInfo &error_info) {
        ffi::StreamCreateOptions ffi_options{};
        if (options.encryption_key_) {
            ffi_options.has_encryption_key = true;
            ffi_options.encryption_key     = ToFfiBytes(*options.encryption_key_);
        }
        if (options.encryption_key_base64_) {
            ffi_options.has_encryption_key_base64 = true;
            ffi_options.encryption_key_base64     = *options.encryption_key_base64_;
        }
        ffi_options.raw_options = ToFfiRawOptions(options.RawEntries());
        return StreamDetails::FromFfi(Handle()->create_stream(name, std::move(ffi_options), error_info));
    });
}

void IggyBlockingClient::UpdateStream(const Identifier &stream, const StreamUpdateOptions &options) {
    RethrowAsIggyException([this, &stream, &options](ffi::ErrorInfo &error_info) {
        ffi::StreamUpdateOptions ffi_options{};
        ffi_options.has_name    = options.Name().has_value();
        ffi_options.name        = options.Name().value_or("");
        ffi_options.reset       = ToFfiResetKeys(options.ResetKeys());
        ffi_options.raw_reset   = ToFfiRawResetKeys(options.RawResetKeys());
        ffi_options.raw_options = ToFfiRawOptions(options.RawEntries());
        Handle()->update_stream(stream.ToFfi(), std::move(ffi_options), error_info);
    });
}

std::vector<Stream> IggyBlockingClient::GetStreams() {
    return RethrowAsIggyException([this] {
        std::vector<Stream> streams;
        auto ffi_streams = Handle()->get_streams();
        streams.reserve(ffi_streams.size());
        for (auto &stream : ffi_streams) {
            streams.push_back(Stream::FromFfi(std::move(stream)));
        }
        return streams;
    });
}

StreamDetails IggyBlockingClient::GetStream(const Identifier &stream) {
    return RethrowAsIggyException(
        [this, &stream] { return StreamDetails::FromFfi(Handle()->get_stream(stream.ToFfi())); });
}

void IggyBlockingClient::DeleteStream(const Identifier &stream) {
    RethrowAsIggyException(
        [this, &stream](ffi::ErrorInfo &error_info) { Handle()->delete_stream(stream.ToFfi(), error_info); });
}

TopicDetails IggyBlockingClient::CreateTopic(const Identifier &stream,
                                             std::string name,
                                             const TopicCreateOptions &options) {
    return RethrowAsIggyException([this, &stream, &name, &options](ffi::ErrorInfo &error_info) {
        ffi::TopicCreateOptions ffi_options{};
        SetMutableTopicOptions(ffi_options, options);
        if (const auto &value = options.CompressionAlgorithm()) {
            ffi_options.has_compression_algorithm = true;
            ffi_options.compression_algorithm     = std::string(value->Value());
        }
        if (auto value = options.PartitionsCount()) {
            ffi_options.has_partitions_count = true;
            ffi_options.partitions_count     = *value;
        } else {
            ffi_options.has_partitions_count = false;
            ffi_options.partitions_count     = 0;
        }
        if (auto value = options.SegmentSize()) {
            ffi_options.has_segment_size = true;
            ffi_options.segment_size     = *value;
        } else {
            ffi_options.has_segment_size = false;
            ffi_options.segment_size     = 0;
        }
        if (auto value = options.Durability()) {
            ffi_options.has_durability = true;
            ffi_options.durability     = std::string(iggy::to_string(*value));
        } else {
            ffi_options.has_durability = false;
            ffi_options.durability     = "";
        }
        if (auto value = options.ConsumerOffsetDurability()) {
            ffi_options.has_consumer_offset_durability = true;
            ffi_options.consumer_offset_durability     = std::string(iggy::to_string(*value));
        } else {
            ffi_options.has_consumer_offset_durability = false;
            ffi_options.consumer_offset_durability     = "";
        }
        if (auto value = options.PartitionResizePolicy()) {
            ffi_options.has_partition_resize_policy = true;
            ffi_options.partition_resize_policy     = std::string(iggy::to_string(*value));
        }
        if (auto value = options.MessagesRequiredToSave()) {
            ffi_options.has_messages_required_to_save = true;
            ffi_options.messages_required_to_save     = *value;
        } else {
            ffi_options.has_messages_required_to_save = false;
            ffi_options.messages_required_to_save     = 0;
        }
        if (auto value = options.SizeOfMessagesRequiredToSave()) {
            ffi_options.has_size_of_messages_required_to_save = true;
            ffi_options.size_of_messages_required_to_save     = *value;
        } else {
            ffi_options.has_size_of_messages_required_to_save = false;
            ffi_options.size_of_messages_required_to_save     = 0;
        }
        if (auto value = options.PreallocateSegments()) {
            ffi_options.has_preallocate_segments = true;
            ffi_options.preallocate_segments     = *value;
        } else {
            ffi_options.has_preallocate_segments = false;
            ffi_options.preallocate_segments     = false;
        }
        return TopicDetails::FromFfi(Handle()->create_topic(stream.ToFfi(), name, std::move(ffi_options), error_info));
    });
}

void IggyBlockingClient::UpdateTopic(const Identifier &stream,
                                     const Identifier &topic,
                                     const TopicUpdateOptions &options) {
    RethrowAsIggyException([this, &stream, &topic, &options](ffi::ErrorInfo &error_info) {
        ffi::TopicUpdateOptions ffi_options{};
        SetMutableTopicOptions(ffi_options, options);
        ffi_options.has_name  = options.Name().has_value();
        ffi_options.name      = options.Name().value_or("");
        ffi_options.reset     = ToFfiResetKeys(options.ResetKeys());
        ffi_options.raw_reset = ToFfiRawResetKeys(options.RawResetKeys());

        Handle()->update_topic(stream.ToFfi(), topic.ToFfi(), std::move(ffi_options), error_info);
    });
}

std::vector<Topic> IggyBlockingClient::GetTopics(const Identifier &stream) {
    return RethrowAsIggyException([this, &stream] {
        std::vector<Topic> topics;
        auto ffi_topics = Handle()->get_topics(stream.ToFfi());
        topics.reserve(ffi_topics.size());
        for (auto &topic : ffi_topics) {
            topics.push_back(Topic::FromFfi(std::move(topic)));
        }
        return topics;
    });
}

TopicDetails IggyBlockingClient::GetTopic(const Identifier &stream, const Identifier &topic) {
    return RethrowAsIggyException(
        [this, &stream, &topic] { return TopicDetails::FromFfi(Handle()->get_topic(stream.ToFfi(), topic.ToFfi())); });
}

void IggyBlockingClient::DeleteTopic(const Identifier &stream, const Identifier &topic) {
    RethrowAsIggyException([this, &stream, &topic](ffi::ErrorInfo &error_info) {
        Handle()->delete_topic(stream.ToFfi(), topic.ToFfi(), error_info);
    });
}

void IggyBlockingClient::CreatePartitions(const Identifier &stream,
                                          const Identifier &topic,
                                          const std::uint32_t partitions_count) {
    RethrowAsIggyException([this, &stream, &topic, partitions_count](ffi::ErrorInfo &error_info) {
        Handle()->create_partitions(stream.ToFfi(), topic.ToFfi(), partitions_count, error_info);
    });
}

void IggyBlockingClient::DeletePartitions(const Identifier &stream,
                                          const Identifier &topic,
                                          const std::uint32_t partitions_count) {
    RethrowAsIggyException([this, &stream, &topic, partitions_count](ffi::ErrorInfo &error_info) {
        Handle()->delete_partitions(stream.ToFfi(), topic.ToFfi(), partitions_count, error_info);
    });
}

void IggyBlockingClient::DeleteSegments(const Identifier &stream,
                                        const Identifier &topic,
                                        const std::uint32_t partition_id,
                                        const std::uint32_t segments_count) {
    RethrowAsIggyException([this, &stream, &topic, partition_id, segments_count](ffi::ErrorInfo &error_info) {
        Handle()->delete_segments(stream.ToFfi(), topic.ToFfi(), partition_id, segments_count, error_info);
    });
}

ConsumerGroupDetails IggyBlockingClient::CreateConsumerGroup(const Identifier &stream,
                                                             const Identifier &topic,
                                                             std::string name) {
    return RethrowAsIggyException([this, &stream, &topic, &name](ffi::ErrorInfo &error_info) {
        return ConsumerGroupDetails::FromFfi(
            Handle()->create_consumer_group(stream.ToFfi(), topic.ToFfi(), name, error_info));
    });
}

ConsumerGroupDetails IggyBlockingClient::GetConsumerGroup(const Identifier &stream,
                                                          const Identifier &topic,
                                                          const Identifier &group) {
    return RethrowAsIggyException([this, &stream, &topic, &group] {
        return ConsumerGroupDetails::FromFfi(
            Handle()->get_consumer_group(stream.ToFfi(), topic.ToFfi(), group.ToFfi()));
    });
}

std::vector<ConsumerGroup> IggyBlockingClient::GetConsumerGroups(const Identifier &stream, const Identifier &topic) {
    return RethrowAsIggyException([this, &stream, &topic] {
        std::vector<ConsumerGroup> groups;
        auto ffi_groups = Handle()->get_consumer_groups(stream.ToFfi(), topic.ToFfi());
        groups.reserve(ffi_groups.size());
        for (auto &group : ffi_groups) {
            groups.push_back(ConsumerGroup::FromFfi(std::move(group)));
        }
        return groups;
    });
}

void IggyBlockingClient::DeleteConsumerGroup(const Identifier &stream,
                                             const Identifier &topic,
                                             const Identifier &group) {
    RethrowAsIggyException([this, &stream, &topic, &group](ffi::ErrorInfo &error_info) {
        Handle()->delete_consumer_group(stream.ToFfi(), topic.ToFfi(), group.ToFfi(), error_info);
    });
}

void IggyBlockingClient::JoinConsumerGroup(const Identifier &stream, const Identifier &topic, const Identifier &group) {
    RethrowAsIggyException([this, &stream, &topic, &group](ffi::ErrorInfo &error_info) {
        Handle()->join_consumer_group(stream.ToFfi(), topic.ToFfi(), group.ToFfi(), error_info);
    });
}

void IggyBlockingClient::LeaveConsumerGroup(const Identifier &stream,
                                            const Identifier &topic,
                                            const Identifier &group) {
    RethrowAsIggyException([this, &stream, &topic, &group](ffi::ErrorInfo &error_info) {
        Handle()->leave_consumer_group(stream.ToFfi(), topic.ToFfi(), group.ToFfi(), error_info);
    });
}

void IggyBlockingClient::StoreConsumerOffset(const Consumer &consumer,
                                             const Identifier &stream,
                                             const Identifier &topic,
                                             const std::optional<std::uint32_t> partition_id,
                                             const std::uint64_t offset) {
    RethrowAsIggyException([this, &consumer, &stream, &topic, offset, partition_id](ffi::ErrorInfo &error_info) {
        constexpr auto unspecified_partition_id = std::numeric_limits<std::uint32_t>::max();
        if (partition_id == unspecified_partition_id) {
            throw std::invalid_argument("partition_id cannot be the maximum std::uint32_t value");
        }
        const auto ffi_partition_id = partition_id.value_or(unspecified_partition_id);
        Handle()->store_consumer_offset(stream.ToFfi(), topic.ToFfi(), ffi_partition_id,
                                        std::string(consumer.KindName()), consumer.Id().ToFfi(), offset, error_info);
    });
}

void IggyBlockingClient::StoreConsumerPosition(const Consumer &consumer,
                                               const Identifier &stream,
                                               const Identifier &topic,
                                               const std::uint32_t partition_id,
                                               const std::uint64_t offset,
                                               const PartitionContext &context) {
    RethrowAsIggyException([this, &consumer, &stream, &topic, partition_id, offset,
                            &context](ffi::ErrorInfo &error_info) {
        Handle()->store_consumer_position(stream.ToFfi(), topic.ToFfi(), partition_id, std::string(consumer.KindName()),
                                          consumer.Id().ToFfi(), offset, context.ToFfi(), error_info);
    });
}

ConsumerOffsetInfo IggyBlockingClient::GetConsumerOffset(const Consumer &consumer,
                                                         const Identifier &stream,
                                                         const Identifier &topic,
                                                         const std::optional<std::uint32_t> partition_id) {
    return RethrowAsIggyException([this, &consumer, &stream, &topic, partition_id] {
        constexpr auto unspecified_partition_id = std::numeric_limits<std::uint32_t>::max();
        if (partition_id == unspecified_partition_id) {
            throw std::invalid_argument("partition_id cannot be the maximum std::uint32_t value");
        }
        const auto ffi_partition_id = partition_id.value_or(unspecified_partition_id);
        return ConsumerOffsetInfo::FromFfi(Handle()->get_consumer_offset(
            stream.ToFfi(), topic.ToFfi(), ffi_partition_id, std::string(consumer.KindName()), consumer.Id().ToFfi()));
    });
}

void IggyBlockingClient::DeleteConsumerOffset(const Consumer &consumer,
                                              const Identifier &stream,
                                              const Identifier &topic,
                                              const std::optional<std::uint32_t> partition_id) {
    RethrowAsIggyException([this, &consumer, &stream, &topic, partition_id](ffi::ErrorInfo &error_info) {
        constexpr auto unspecified_partition_id = std::numeric_limits<std::uint32_t>::max();
        if (partition_id == unspecified_partition_id) {
            throw std::invalid_argument("partition_id cannot be the maximum std::uint32_t value");
        }
        const auto ffi_partition_id = partition_id.value_or(unspecified_partition_id);
        Handle()->delete_consumer_offset(stream.ToFfi(), topic.ToFfi(), ffi_partition_id,
                                         std::string(consumer.KindName()), consumer.Id().ToFfi(), error_info);
    });
}

ClientInfoDetails IggyBlockingClient::GetMe() {
    return RethrowAsIggyException([this] { return ClientInfoDetails::FromFfi(Handle()->get_me()); });
}

ClientInfoDetails IggyBlockingClient::GetClient(std::uint32_t client_id) {
    return RethrowAsIggyException(
        [this, client_id] { return ClientInfoDetails::FromFfi(Handle()->get_client(client_id)); });
}

std::vector<ClientInfo> IggyBlockingClient::GetClients() {
    return RethrowAsIggyException([this] {
        std::vector<ClientInfo> clients;
        auto ffi_clients = Handle()->get_clients();
        clients.reserve(ffi_clients.size());
        for (auto &client : ffi_clients) {
            clients.push_back(ClientInfo::FromFfi(std::move(client)));
        }
        return clients;
    });
}

std::vector<OptionSpec> IggyBlockingClient::DescribeOptions(OptionsScope scope) {
    return RethrowAsIggyException([this, scope] {
        const auto scope_name = to_string(scope);
        std::vector<OptionSpec> specs;
        auto ffi_specs = Handle()->describe_options(rust::String(scope_name.data(), scope_name.size()));
        specs.reserve(ffi_specs.size());
        for (auto &spec : ffi_specs) {
            specs.push_back(OptionSpec::FromFfi(std::move(spec)));
        }
        return specs;
    });
}

Stats IggyBlockingClient::GetStats() {
    return RethrowAsIggyException([this] { return Stats::FromFfi(Handle()->get_stats()); });
}

IggyBlockingClient::IggyBlockingClient(ffi::Client *client) : client_(client) {
    if (client_ == nullptr) {
        throw IggyException("Could not create Iggy client");
    }
}

ffi::Client *IggyBlockingClient::Handle() const {
    if (client_ == nullptr) {
        throw IggyException("Cannot use a moved-from IggyBlockingClient");
    }
    return client_;
}

void IggyBlockingClient::Reset() noexcept {
    if (client_ == nullptr) {
        return;
    }

    ffi::Client *client{std::exchange(client_, nullptr)};
    ffi::delete_client(client);
}

std::string EncryptionKeyFingerprint(const std::vector<std::uint8_t> &key) {
    ffi::ErrorInfo error_info{};
    try {
        const auto fingerprint =
            ffi::encryption_key_fingerprint(rust::Slice<const std::uint8_t>(key.data(), key.size()), error_info);
        return std::string(fingerprint.data(), fingerprint.size());
    } catch (const std::exception &error) {
        throw IggyException(error.what(), error_info);
    }
}

std::string EncryptionKeyFingerprint(const std::string &key_base64) {
    ffi::ErrorInfo error_info{};
    try {
        const auto fingerprint =
            ffi::encryption_key_fingerprint_base64(rust::Str(key_base64.data(), key_base64.size()), error_info);
        return std::string(fingerprint.data(), fingerprint.size());
    } catch (const std::exception &error) {
        throw IggyException(error.what(), error_info);
    }
}

IggyBlockingClient::Builder::Builder() = default;

IggyBlockingClient::Builder &IggyBlockingClient::Builder::WithServerAddress(std::string server_address) {
    if (server_address.empty()) {
        throw IggyException("Server address cannot be empty");
    }
    server_address_ = std::move(server_address);
    return *this;
}

IggyBlockingClient::Builder &IggyBlockingClient::Builder::WithAutoLogin(std::string username, std::string password) {
    if (username.empty() || password.empty()) {
        throw IggyException("Automatic login username and password cannot be empty");
    }
    auto_login_kind_     = ffi::AutoLoginKind::UsernamePassword;
    auto_login_username_ = std::move(username);
    auto_login_password_ = std::move(password);
    personal_access_token_.clear();
    return *this;
}

IggyBlockingClient::Builder &IggyBlockingClient::Builder::WithPersonalAccessToken(std::string token) {
    if (token.empty()) {
        throw IggyException("Personal access token cannot be empty");
    }
    auto_login_kind_       = ffi::AutoLoginKind::PersonalAccessToken;
    personal_access_token_ = std::move(token);
    auto_login_username_.clear();
    auto_login_password_.clear();
    return *this;
}

IggyBlockingClient::Builder &IggyBlockingClient::Builder::WithReconnectionMaxRetries(std::uint32_t retries) {
    reconnection_max_retries_ = retries;
    return *this;
}

IggyBlockingClient::Builder &IggyBlockingClient::Builder::WithoutReconnectionLimit() {
    reconnection_max_retries_.reset();
    return *this;
}

IggyBlockingClient::Builder &IggyBlockingClient::Builder::WithReconnectionInterval(std::chrono::microseconds interval) {
    if (interval.count() < 0) {
        throw IggyException("Reconnection interval cannot be negative");
    }
    reconnection_interval_micros_ = static_cast<std::uint64_t>(interval.count());
    return *this;
}

IggyBlockingClient::Builder &IggyBlockingClient::Builder::WithReestablishAfter(std::chrono::microseconds duration) {
    if (duration.count() < 0) {
        throw IggyException("Reestablish duration cannot be negative");
    }
    reestablish_after_micros_ = static_cast<std::uint64_t>(duration.count());
    return *this;
}

IggyBlockingClient::Builder &IggyBlockingClient::Builder::WithTlsEnabled(bool enabled) {
    tls_enabled_ = enabled;
    return *this;
}

IggyBlockingClient::Builder &IggyBlockingClient::Builder::WithTlsDomain(std::string domain) {
    if (domain.empty()) {
        throw IggyException("TLS domain cannot be empty");
    }
    tls_domain_ = std::move(domain);
    return *this;
}

IggyBlockingClient::Builder &IggyBlockingClient::Builder::WithTlsCaFile(std::string path) {
    if (path.empty()) {
        throw IggyException("TLS CA file cannot be empty");
    }
    tls_ca_file_ = std::move(path);
    return *this;
}

IggyBlockingClient::Builder &IggyBlockingClient::Builder::WithTlsCertificateValidation(bool enabled) {
    tls_validate_certificate_ = enabled;
    return *this;
}

IggyBlockingClient::Builder &IggyBlockingClient::Builder::WithNoDelay() {
    no_delay_ = true;
    return *this;
}

IggyBlockingClient::Builder &IggyBlockingClient::Builder::WithPendingOperations(std::uint64_t max_operations,
                                                                                std::uint64_t max_bytes) {
    if (max_operations == 0 || max_bytes == 0) {
        throw IggyException("Pending operation limits must be positive");
    }
    has_pending_operations_ = true;
    pending_max_operations_ = max_operations;
    pending_max_bytes_      = max_bytes;
    return *this;
}

IggyBlockingClient IggyBlockingClient::Builder::Build() const {
    return IggyBlockingClient::RethrowAsIggyException([this] {
        ffi::IggyClientConfig config{};
        config.server_address               = server_address_;
        config.auto_login_kind              = auto_login_kind_;
        config.username                     = auto_login_username_;
        config.password                     = auto_login_password_;
        config.personal_access_token        = personal_access_token_;
        config.has_reconnection_max_retries = reconnection_max_retries_.has_value();
        config.reconnection_max_retries     = reconnection_max_retries_.value_or(0);
        config.has_reconnection_interval    = reconnection_interval_micros_.has_value();
        config.reconnection_interval_micros = reconnection_interval_micros_.value_or(0);
        config.has_reestablish_after        = reestablish_after_micros_.has_value();
        config.reestablish_after_micros     = reestablish_after_micros_.value_or(0);
        config.tls_enabled                  = tls_enabled_;
        config.tls_domain                   = tls_domain_;
        config.tls_ca_file                  = tls_ca_file_;
        config.has_tls_validate_certificate = tls_validate_certificate_.has_value();
        config.tls_validate_certificate     = tls_validate_certificate_.value_or(false);
        config.no_delay                     = no_delay_;
        config.has_pending_operations       = has_pending_operations_;
        config.pending_max_operations       = pending_max_operations_;
        config.pending_max_bytes            = pending_max_bytes_;
        return IggyBlockingClient(ffi::new_connection(std::move(config)));
    });
}

}  // namespace iggy
