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

#include <string>

#include "iggy.hpp"

namespace iggy {

LoginInfo LoginInfo::FromFfi(ffi::LoginInfo login_info) {
    std::optional<std::string> access_token;
    std::optional<std::uint64_t> access_token_expiry;
    if (login_info.has_access_token) {
        access_token.emplace(login_info.access_token.c_str(), login_info.access_token.size());
        access_token_expiry = login_info.access_token_expiry;
    }

    return LoginInfo(login_info.user_id, std::move(access_token), access_token_expiry);
}

Identifier Identifier::FromFfi(ffi::Identifier identifier) {
    const std::string kind(identifier.kind.c_str(), identifier.kind.size());
    if (kind == "numeric") {
        if (identifier.length != sizeof(std::uint32_t) || identifier.value.size() != sizeof(std::uint32_t)) {
            throw IggyException("Invalid numeric identifier returned by Rust");
        }
        const auto id = static_cast<std::uint32_t>(identifier.value[0]) |
                        (static_cast<std::uint32_t>(identifier.value[1]) << 8U) |
                        (static_cast<std::uint32_t>(identifier.value[2]) << 16U) |
                        (static_cast<std::uint32_t>(identifier.value[3]) << 24U);
        return Numeric(id);
    }
    if (kind == "string") {
        if (identifier.length != identifier.value.size()) {
            throw IggyException("Invalid string identifier returned by Rust");
        }
        return String(std::string(identifier.value.begin(), identifier.value.end()));
    }
    throw IggyException("Invalid identifier kind returned by Rust");
}

ffi::Identifier Identifier::ToFfi() const {
    ffi::Identifier identifier{};
    if (kind_ == Kind::Numeric) {
        identifier.set_numeric(std::get<std::uint32_t>(value_));
    } else {
        identifier.set_string(std::get<std::string>(value_));
    }
    return identifier;
}

HeaderField HeaderField::FromFfi(ffi::HeaderField field) {
    return HeaderField(static_cast<HeaderKind>(field.kind),
                       std::vector<std::uint8_t>(field.value.begin(), field.value.end()));
}

ffi::HeaderField HeaderField::ToFfi(HeaderField field) {
    ffi::HeaderField ffi_field{};
    ffi_field.kind = static_cast<std::uint8_t>(field.kind_);
    ffi_field.value.reserve(field.value_.size());
    for (const auto byte : field.value_) {
        ffi_field.value.push_back(byte);
    }
    return ffi_field;
}

HeaderEntry HeaderEntry::FromFfi(ffi::HeaderEntry entry) {
    return HeaderEntry(HeaderField::FromFfi(std::move(entry.key)), HeaderField::FromFfi(std::move(entry.value)));
}

ffi::HeaderEntry HeaderEntry::ToFfi(HeaderEntry entry) {
    ffi::HeaderEntry ffi_entry{};
    ffi_entry.key   = HeaderField::ToFfi(std::move(entry.key_));
    ffi_entry.value = HeaderField::ToFfi(std::move(entry.value_));
    return ffi_entry;
}

ResourceOptions ResourceOptions::Explicit(std::vector<HeaderEntry> entries) {
    return ResourceOptions(std::move(entries));
}

ResourceOptions ResourceOptions::FromFfi(rust::Vec<ffi::HeaderEntry> explicit_entries,
                                         rust::Vec<ffi::HeaderEntry> derived_entries) {
    std::vector<HeaderEntry> explicit_options;
    explicit_options.reserve(explicit_entries.size());
    for (auto &entry : explicit_entries) {
        explicit_options.push_back(HeaderEntry::FromFfi(std::move(entry)));
    }
    std::vector<HeaderEntry> derived_options;
    derived_options.reserve(derived_entries.size());
    for (auto &entry : derived_entries) {
        derived_options.push_back(HeaderEntry::FromFfi(std::move(entry)));
    }
    return ResourceOptions(std::move(explicit_options), std::move(derived_options));
}

rust::Vec<ffi::HeaderEntry> ResourceOptions::ToFfi(ResourceOptions options) {
    rust::Vec<ffi::HeaderEntry> headers{};
    headers.reserve(options.explicit_.size());
    for (auto &entry : options.explicit_) {
        headers.push_back(HeaderEntry::ToFfi(std::move(entry)));
    }
    return headers;
}

Topic Topic::FromFfi(ffi::Topic topic) {
    return Topic(topic.id, topic.created_at, std::string(topic.name.c_str(), topic.name.size()), topic.size_bytes,
                 topic.message_expiry,
                 std::string(topic.compression_algorithm.c_str(), topic.compression_algorithm.size()),
                 topic.max_topic_size, topic.messages_count, topic.partitions_count,
                 ResourceOptions::FromFfi(std::move(topic.options), std::move(topic.derived_options)));
}

Partition Partition::FromFfi(ffi::Partition partition) {
    return Partition(partition.id, partition.created_at, partition.segments_count, partition.current_offset,
                     partition.size_bytes, partition.messages_count);
}

TopicDetails TopicDetails::FromFfi(ffi::TopicDetails topic) {
    std::vector<Partition> partitions;
    partitions.reserve(topic.partitions.size());
    for (auto &partition : topic.partitions) {
        partitions.push_back(Partition::FromFfi(std::move(partition)));
    }

    return TopicDetails(topic.id, topic.created_at, std::string(topic.name.c_str(), topic.name.size()),
                        topic.size_bytes, topic.message_expiry,
                        std::string(topic.compression_algorithm.c_str(), topic.compression_algorithm.size()),
                        topic.max_topic_size, topic.messages_count, topic.partitions_count, std::move(partitions),
                        ResourceOptions::FromFfi(std::move(topic.options), std::move(topic.derived_options)));
}

StreamDetails StreamDetails::FromFfi(ffi::StreamDetails stream) {
    std::vector<Topic> topics;
    topics.reserve(stream.topics.size());
    for (auto &topic : stream.topics) {
        topics.push_back(Topic::FromFfi(std::move(topic)));
    }

    return StreamDetails(stream.id, stream.created_at, std::string(stream.name.c_str(), stream.name.size()),
                         stream.size_bytes, stream.messages_count, stream.topics_count, std::move(topics),
                         ResourceOptions::FromFfi(std::move(stream.options), rust::Vec<ffi::HeaderEntry>{}));
}

Stream Stream::FromFfi(ffi::Stream stream) {
    return Stream(stream.id, stream.created_at, std::string(stream.name.c_str(), stream.name.size()), stream.size_bytes,
                  stream.messages_count, stream.topics_count,
                  ResourceOptions::FromFfi(std::move(stream.options), rust::Vec<ffi::HeaderEntry>{}));
}

}  // namespace iggy
