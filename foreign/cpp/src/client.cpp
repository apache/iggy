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
    try {
        return IggyBlockingClient(ffi::from_connection_string(connection_string));
    } catch (const std::exception &error) {
        throw IggyException(error.what());
    }
}

void IggyBlockingClient::Connect() const {
    try {
        client_->connect();
    } catch (const std::exception &error) {
        throw IggyException(error.what());
    }
}

void IggyBlockingClient::Disconnect() const {
    try {
        client_->disconnect();
    } catch (const std::exception &error) {
        throw IggyException(error.what());
    }
}

void IggyBlockingClient::Shutdown() const {
    try {
        client_->shutdown();
    } catch (const std::exception &error) {
        throw IggyException(error.what());
    }
}

LoginInfo IggyBlockingClient::Login(std::string username, std::string password) const {
    try {
        return client_->login_user(std::move(username), std::move(password));
    } catch (const std::exception &error) {
        throw IggyException(error.what());
    }
}

void IggyBlockingClient::Logout() const {
    try {
        client_->logout_user();
    } catch (const std::exception &error) {
        throw IggyException(error.what());
    }
}

IggyBlockingClient::IggyBlockingClient(ffi::Client *client) : client_(client) {
    if (client_ == nullptr) {
        throw IggyException("Could not create Iggy client");
    }
}

void IggyBlockingClient::Reset() noexcept {
    if (client_ == nullptr) {
        return;
    }

    ffi::Client *client = std::exchange(client_, nullptr);
    try {
        ffi::delete_client(client);
    } catch (...) {
    }
}

IggyBlockingClient::Builder::Builder() = default;

IggyBlockingClient::Builder &IggyBlockingClient::Builder::WithServerAddress(std::string server_address) {
    server_address_ = std::move(server_address);
    return *this;
}

IggyBlockingClient::Builder &IggyBlockingClient::Builder::WithAutoLogin(std::string username, std::string password) {
    auto_login_kind_     = "username_password";
    auto_login_username_ = std::move(username);
    auto_login_password_ = std::move(password);
    personal_access_token_.clear();
    return *this;
}

IggyBlockingClient::Builder &IggyBlockingClient::Builder::WithPersonalAccessToken(std::string token) {
    auto_login_kind_       = "personal_access_token";
    personal_access_token_ = std::move(token);
    auto_login_username_.clear();
    auto_login_password_.clear();
    return *this;
}

IggyBlockingClient::Builder &IggyBlockingClient::Builder::WithReconnectionMaxRetries(std::uint32_t retries) {
    set_reconnection_max_retries_ = true;
    reconnection_max_retries_     = retries;
    return *this;
}

IggyBlockingClient::Builder &IggyBlockingClient::Builder::WithoutReconnectionLimit() {
    set_reconnection_max_retries_ = true;
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
    tls_domain_ = std::move(domain);
    return *this;
}

IggyBlockingClient::Builder &IggyBlockingClient::Builder::WithTlsCaFile(std::string path) {
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
    ffi::Client *client = nullptr;
    try {
        ffi::IggyClientConfig config{};
        config.server_address               = server_address_;
        config.auto_login_kind              = auto_login_kind_;
        config.username                     = auto_login_username_;
        config.password                     = auto_login_password_;
        config.personal_access_token        = personal_access_token_;
        config.set_reconnection_max_retries = set_reconnection_max_retries_;
        config.has_reconnection_max_retries = reconnection_max_retries_.has_value();
        config.reconnection_max_retries     = reconnection_max_retries_.value_or(0);
        config.has_reconnection_interval    = reconnection_interval_micros_.has_value();
        config.reconnection_interval_micros = reconnection_interval_micros_.value_or(0);
        config.has_reestablish_after        = reestablish_after_micros_.has_value();
        config.reestablish_after_micros     = reestablish_after_micros_.value_or(0);
        config.has_tls_enabled              = tls_enabled_.has_value();
        config.tls_enabled                  = tls_enabled_.value_or(false);
        config.tls_domain                   = tls_domain_;
        config.tls_ca_file                  = tls_ca_file_;
        config.has_tls_validate_certificate = tls_validate_certificate_.has_value();
        config.tls_validate_certificate     = tls_validate_certificate_.value_or(false);
        config.no_delay                     = no_delay_;
        client                              = ffi::new_connection(std::move(config));
        if (client == nullptr) {
            throw IggyException("Could not create Iggy client");
        }
        return IggyBlockingClient(client);
    } catch (const std::exception &error) {
        if (client != nullptr) {
            try {
                ffi::delete_client(client);
            } catch (...) {
            }
        }
        throw IggyException(error.what());
    }
}

}  // namespace iggy
