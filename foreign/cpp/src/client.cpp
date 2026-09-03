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

#include <utility>

#include "iggy.hpp"

namespace iggy {

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

LoginInfo IggyBlockingClient::Login(std::string username, std::string password) {
    return RethrowAsIggyException([this, &username, &password] {
        return LoginInfo::FromFfi(Handle()->login_user(std::move(username), std::move(password)));
    });
}

void IggyBlockingClient::Logout() {
    RethrowAsIggyException([this] { Handle()->logout_user(); });
}

StreamDetails IggyBlockingClient::CreateStream(std::string name) {
    return RethrowAsIggyException(
        [this, &name] { return StreamDetails::FromFfi(Handle()->create_stream(std::move(name))); });
}

void IggyBlockingClient::UpdateStream(const Identifier &stream, std::string name) {
    return RethrowAsIggyException([this, &stream, &name] { Handle()->update_stream(stream.ToFfi(), std::move(name)); });
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
    return RethrowAsIggyException([this, &stream] { Handle()->delete_stream(stream.ToFfi()); });
}

void IggyBlockingClient::PurgeStream(const Identifier &stream) {
    return RethrowAsIggyException([this, &stream] { Handle()->purge_stream(stream.ToFfi()); });
}

TopicDetails IggyBlockingClient::CreateTopic(const Identifier &stream,
                                             std::string name,
                                             const std::uint32_t partitions_count,
                                             const CompressionAlgorithm compression_algorithm,
                                             const Expiry message_expiry,
                                             const MaxTopicSize max_topic_size,
                                             ResourceOptions options) {
    return RethrowAsIggyException(
        [this, &stream, &name, partitions_count, &compression_algorithm, &message_expiry, &max_topic_size, &options] {
            return TopicDetails::FromFfi(Handle()->create_topic(
                stream.ToFfi(), std::move(name), partitions_count,
                std::string(compression_algorithm.CompressionAlgorithmValue()),
                std::string(message_expiry.ExpiryKind()), message_expiry.ExpiryValue(),
                std::string(max_topic_size.MaxTopicSizeValue()), ResourceOptions::ToFfi(std::move(options))));
        });
}

void IggyBlockingClient::UpdateTopic(const Identifier &stream,
                                     const Identifier &topic,
                                     std::string name,
                                     const CompressionAlgorithm compression_algorithm,
                                     const Expiry message_expiry,
                                     const MaxTopicSize max_topic_size,
                                     ResourceOptions options) {
    return RethrowAsIggyException(
        [this, &stream, &topic, &name, &compression_algorithm, &message_expiry, &max_topic_size, &options] {
            Handle()->update_topic(stream.ToFfi(), topic.ToFfi(), std::move(name),
                                   std::string(compression_algorithm.CompressionAlgorithmValue()),
                                   std::string(message_expiry.ExpiryKind()), message_expiry.ExpiryValue(),
                                   std::string(max_topic_size.MaxTopicSizeValue()),
                                   ResourceOptions::ToFfi(std::move(options)));
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
    return RethrowAsIggyException([this, &stream, &topic] { Handle()->delete_topic(stream.ToFfi(), topic.ToFfi()); });
}

void IggyBlockingClient::PurgeTopic(const Identifier &stream, const Identifier &topic) {
    return RethrowAsIggyException([this, &stream, &topic] { Handle()->purge_topic(stream.ToFfi(), topic.ToFfi()); });
}

void IggyBlockingClient::CreatePartitions(const Identifier &stream,
                                          const Identifier &topic,
                                          const std::uint32_t partitions_count) {
    return RethrowAsIggyException([this, &stream, &topic, partitions_count] {
        Handle()->create_partitions(stream.ToFfi(), topic.ToFfi(), partitions_count);
    });
}

void IggyBlockingClient::DeletePartitions(const Identifier &stream,
                                          const Identifier &topic,
                                          const std::uint32_t partitions_count) {
    return RethrowAsIggyException([this, &stream, &topic, partitions_count] {
        Handle()->delete_partitions(stream.ToFfi(), topic.ToFfi(), partitions_count);
    });
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
        return IggyBlockingClient(ffi::new_connection(std::move(config)));
    });
}

}  // namespace iggy
