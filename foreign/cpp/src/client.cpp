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

#include "iggy.hpp"
#include "lib.rs.h"
#include "rust/cxx.h"

namespace iggy {

IggyClient::~IggyClient() noexcept { destroy(); }
IggyClient::IggyClient(ffi::Client *client) : client_(client) {}

IggyClient::IggyClient(IggyClient &&other) noexcept
    : client_(std::exchange(other.client_, nullptr)) {}

IggyClient &IggyClient::operator=(IggyClient &&other) noexcept {
  if (this != &other) {
    destroy();
    client_ = std::exchange(other.client_, nullptr);
  }
  return *this;
}

void IggyClient::destroy() noexcept {
  if (!client_)
    return;
  iggy::ffi::delete_connection(client_);
  client_ = nullptr;
}

IggyClient IggyClient::new_connection(const std::string &connection_string) {
  try {
    rust::String s(connection_string);
    ffi::Client *p = iggy::ffi::new_connection(std::move(s));
    if (!p)
      throw IggyException("iggy::new_connection returned null");
    return IggyClient(p);
  } catch (const rust::Error &err) {
    throw IggyException(err.what());
  } catch (const std::exception &ex) {
    throw IggyException(ex.what());
  }
}

void IggyClient::connect() const {
  try {
    client_->connect();
  } catch (const rust::Error &err) {
    throw IggyException(err.what());
  } catch (const std::exception &ex) {
    throw IggyException(ex.what());
  }
}

void IggyClient::login_user(const std::string &username,
                            const std::string &password) const {
  try {
    rust::String u(username);
    rust::String p(password);
    client_->login_user(std::move(u), std::move(p));
  } catch (const rust::Error &err) {
    throw IggyException(err.what());
  } catch (const std::exception &ex) {
    throw IggyException(ex.what());
  }
}

void IggyClient::create_stream(const std::string &stream_name) const {
  try {
    rust::String s(stream_name);
    client_->create_stream(std::move(s));
  } catch (const rust::Error &err) {
    throw IggyException(err.what());
  } catch (const std::exception &ex) {
    throw IggyException(ex.what());
  }
}

StreamDetails IggyClient::get_stream(const iggy::Identifier &stream_id) const {
  try {
    iggy::ffi::StreamDetails *details =
        client_->get_stream(*stream_id.identifier_);
    return StreamDetails(details);
  } catch (const rust::Error &err) {
    throw IggyException(err.what());
  } catch (const std::exception &ex) {
    throw IggyException(ex.what());
  }
}

void IggyClient::create_topic(const iggy::Identifier &stream_id,
                              const std::string &topic_name,
                              std::uint32_t partitions_count,
                              iggy::CompressionAlgorithm compression_algorithm,
                              std::uint8_t replication_factor,
                              iggy::Expiry expiry,
                              iggy::MaxTopicSize max_topic_size) const {
  try {
    rust::String name(topic_name);
    client_->create_topic(*stream_id.identifier_, std::move(name),
                          partitions_count, compression_algorithm.as_rust(),
                          replication_factor, expiry.type_as_rust(),
                          expiry.value(), max_topic_size.as_rust());
  } catch (const rust::Error &err) {
    throw IggyException(err.what());
  } catch (const std::exception &ex) {
    throw IggyException(ex.what());
  }
}

TopicDetails IggyClient::get_topic(const iggy::Identifier &stream_id,
                                   const iggy::Identifier &topic_id) const {
  try {
    iggy::ffi::TopicDetails *details =
        client_->get_topic(*stream_id.identifier_, *topic_id.identifier_);
    return TopicDetails(details);
  } catch (const rust::Error &err) {
    throw IggyException(err.what());
  } catch (const std::exception &ex) {
    throw IggyException(ex.what());
  }
}

void IggyClient::send_messages(const std::vector<IggyMessage> &messages,
                               const iggy::Identifier &stream_id,
                               const iggy::Identifier &topic_id,
                               std::uint32_t partitioning) const {
  try {
    rust::Vec<ffi::Message> ffi_messages;
    ffi_messages.reserve(messages.size());
    for (const auto &message : messages)
      ffi_messages.push_back(message.to_ffi_message());
    client_->send_messages(std::move(ffi_messages), *stream_id.identifier_,
                           *topic_id.identifier_, partitioning);
  } catch (const rust::Error &err) {
    throw IggyException(err.what());
  } catch (const std::exception &ex) {
    throw IggyException(ex.what());
  }
}

PolledMessages IggyClient::poll_messages(
    const iggy::Identifier &stream_id, const iggy::Identifier &topic_id,
    std::uint32_t partition_id, const PollingStrategy &polling_strategy,
    std::uint32_t count, bool auto_commit) const {
  try {
    ffi::PolledMessages polled =
        client_->poll_messages(*stream_id.identifier_, *topic_id.identifier_,
                               partition_id, polling_strategy.kind_as_rust(),
                               polling_strategy.value(), count, auto_commit);
    return PolledMessages(polled);
  } catch (const rust::Error &err) {
    throw IggyException(err.what());
  } catch (const std::exception &ex) {
    throw IggyException(ex.what());
  }
}

} // namespace iggy
