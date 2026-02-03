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

namespace iggy {

IggyMessage::IggyMessage() = default;

void IggyMessage::set_payload(const std::string &data) {
  try {
    if (!send_message_) {
      throw std::runtime_error("Cannot modify payload on a received message.");
    }
    payload_.assign(data.begin(), data.end());
    payload_length_ = static_cast<std::uint32_t>(payload_.size());
  } catch (const rust::Error &err) {
    throw IggyException(err.what());
  } catch (const std::exception &ex) {
    throw IggyException(ex.what());
  }
}

void IggyMessage::set_payload(const std::vector<std::uint8_t> &data) {
  try {
    if (!send_message_) {
      throw std::runtime_error("Cannot modify payload on a received message.");
    }
    payload_ = data;
    payload_length_ = static_cast<std::uint32_t>(payload_.size());
  } catch (const rust::Error &err) {
    throw IggyException(err.what());
  } catch (const std::exception &ex) {
    throw IggyException(ex.what());
  }
}

const std::vector<std::uint8_t> &IggyMessage::payload() const {
  return payload_;
}

std::uint64_t IggyMessage::checksum() const {
  try {
    if (send_message_) {
      throw std::runtime_error(
          "Field 'checksum' is not available on outbound messages.");
    }
    return checksum_;
  } catch (const rust::Error &err) {
    throw IggyException(err.what());
  } catch (const std::exception &ex) {
    throw IggyException(ex.what());
  }
}

absl::uint128 IggyMessage::id() const {
  try {
    if (send_message_) {
      throw std::runtime_error(
          "Field 'id' is not available on outbound messages.");
    }
    return absl::MakeUint128(id_high_, id_low_);
  } catch (const rust::Error &err) {
    throw IggyException(err.what());
  } catch (const std::exception &ex) {
    throw IggyException(ex.what());
  }
}

std::uint64_t IggyMessage::offset() const {
  try {
    if (send_message_) {
      throw std::runtime_error(
          "Field 'offset' is not available on outbound messages.");
    }
    return offset_;
  } catch (const rust::Error &err) {
    throw IggyException(err.what());
  } catch (const std::exception &ex) {
    throw IggyException(ex.what());
  }
}

std::uint64_t IggyMessage::timestamp() const {
  try {
    if (send_message_) {
      throw std::runtime_error(
          "Field 'timestamp' is not available on outbound messages.");
    }
    return timestamp_;
  } catch (const rust::Error &err) {
    throw IggyException(err.what());
  } catch (const std::exception &ex) {
    throw IggyException(ex.what());
  }
}

std::uint64_t IggyMessage::origin_timestamp() const {
  try {
    if (send_message_) {
      throw std::runtime_error(
          "Field 'origin_timestamp' is not available on outbound messages.");
    }
    return origin_timestamp_;
  } catch (const rust::Error &err) {
    throw IggyException(err.what());
  } catch (const std::exception &ex) {
    throw IggyException(ex.what());
  }
}

std::uint32_t IggyMessage::user_headers_length() const {
  try {
    if (send_message_) {
      throw std::runtime_error(
          "Field 'user_headers_length' is not available on outbound messages.");
    }
    return user_headers_length_;
  } catch (const rust::Error &err) {
    throw IggyException(err.what());
  } catch (const std::exception &ex) {
    throw IggyException(ex.what());
  }
}

std::uint32_t IggyMessage::payload_length() const {
  try {
    if (send_message_) {
      throw std::runtime_error(
          "Field 'payload_length' is not available on outbound messages.");
    }
    return payload_length_;
  } catch (const rust::Error &err) {
    throw IggyException(err.what());
  } catch (const std::exception &ex) {
    throw IggyException(ex.what());
  }
}

const std::vector<std::uint8_t> &IggyMessage::user_headers() const {
  try {
    if (send_message_) {
      throw std::runtime_error(
          "Field 'user_headers' is not available on outbound messages.");
    }
    return user_headers_;
  } catch (const rust::Error &err) {
    throw IggyException(err.what());
  } catch (const std::exception &ex) {
    throw IggyException(ex.what());
  }
}

IggyMessage IggyMessage::from_ffi_message(const ffi::Message &message) {
  try {
    IggyMessage result;
    result.send_message_ = false;
    result.checksum_ = message.checksum;
    result.id_high_ = message.id_high;
    result.id_low_ = message.id_low;
    result.offset_ = message.offset;
    result.timestamp_ = message.timestamp;
    result.origin_timestamp_ = message.origin_timestamp;
    result.user_headers_length_ = message.user_headers_length;
    result.payload_length_ = message.payload_length;
    result.payload_.assign(message.payload.begin(), message.payload.end());
    result.user_headers_.assign(message.user_headers.begin(),
                                message.user_headers.end());
    return result;
  } catch (const rust::Error &err) {
    throw IggyException(err.what());
  } catch (const std::exception &ex) {
    throw IggyException(ex.what());
  }
}

rust::Vec<std::uint8_t> IggyMessage::to_rust_vec(const std::string &data) {
  try {
    rust::Vec<std::uint8_t> rust_vec;
    rust_vec.reserve(data.size());
    for (unsigned char byte : data) {
      rust_vec.push_back(byte);
    }
    return rust_vec;
  } catch (const rust::Error &err) {
    throw IggyException(err.what());
  } catch (const std::exception &ex) {
    throw IggyException(ex.what());
  }
}

rust::Vec<std::uint8_t>
IggyMessage::to_rust_vec(const std::vector<std::uint8_t> &data) {
  try {
    rust::Vec<std::uint8_t> rust_vec;
    rust_vec.reserve(data.size());
    for (std::uint8_t byte : data) {
      rust_vec.push_back(byte);
    }
    return rust_vec;
  } catch (const rust::Error &err) {
    throw IggyException(err.what());
  } catch (const std::exception &ex) {
    throw IggyException(ex.what());
  }
}

ffi::Message IggyMessage::to_ffi_message() const {
  try {
    if (!send_message_) {
      throw std::runtime_error("Cannot send a received message instance.");
    }
    ffi::Message message;
    message.checksum = 0;
    message.id_high = 0;
    message.id_low = 0;
    message.offset = 0;
    message.timestamp = 0;
    message.origin_timestamp = 0;
    message.user_headers_length = 0;
    message.payload_length = static_cast<std::uint32_t>(payload_.size());
    message.payload = to_rust_vec(payload_);
    message.user_headers = rust::Vec<std::uint8_t>();
    return message;
  } catch (const rust::Error &err) {
    throw IggyException(err.what());
  } catch (const std::exception &ex) {
    throw IggyException(ex.what());
  }
}

PolledMessages::PolledMessages(const ffi::PolledMessages &messages)
    : partition_id_(messages.partition_id),
      current_offset_(messages.current_offset), count_(messages.count) {
  try {
    messages_.reserve(messages.messages.size());
    for (const auto &message : messages.messages) {
      messages_.push_back(IggyMessage::from_ffi_message(message));
    }
  } catch (const rust::Error &err) {
    throw IggyException(err.what());
  } catch (const std::exception &ex) {
    throw IggyException(ex.what());
  }
}

std::uint32_t PolledMessages::partition_id() const { return partition_id_; }

std::uint64_t PolledMessages::current_offset() const { return current_offset_; }

std::uint32_t PolledMessages::count() const { return count_; }

const std::vector<IggyMessage> &PolledMessages::messages() const {
  try {
    return messages_;
  } catch (const rust::Error &err) {
    throw IggyException(err.what());
  } catch (const std::exception &ex) {
    throw IggyException(ex.what());
  }
}

} // namespace iggy
