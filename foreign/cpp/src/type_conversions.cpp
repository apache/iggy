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

LoginInfo LoginInfo::FromFfi(ffi::LoginInfo login_info) {
    std::optional<std::string> access_token;
    std::optional<std::uint64_t> access_token_expiry;
    if (login_info.has_access_token) {
        access_token.emplace(login_info.access_token.c_str(), login_info.access_token.size());
        access_token_expiry = login_info.access_token_expiry;
    }

    return LoginInfo(login_info.user_id, std::move(access_token), access_token_expiry);
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

ConsumerOffsetInfo ConsumerOffsetInfo::FromFfi(ffi::ConsumerOffsetInfo offset) {
    return ConsumerOffsetInfo(offset.partition_id, offset.current_offset, offset.stored_offset);
}

HeaderField HeaderField::FromFfi(ffi::HeaderField field) {
    return HeaderField(static_cast<HeaderKind>(field.kind),
                       std::vector<std::uint8_t>(field.value.begin(), field.value.end()));
}

HeaderEntry HeaderEntry::FromFfi(ffi::HeaderEntry entry) {
    return HeaderEntry(HeaderField::FromFfi(std::move(entry.key)), HeaderField::FromFfi(std::move(entry.value)));
}

ffi::IggyMessageToSend IggyMessageToSend::ToFfi() const {
    ffi::IggyMessageToSend ffi_message;
    ffi_message.id_lo = absl::Uint128Low64(id_);
    ffi_message.id_hi = absl::Uint128High64(id_);
    ffi_message.payload.reserve(payload_.size());
    for (const auto byte : payload_) {
        ffi_message.payload.push_back(byte);
    }
    ffi_message.user_headers.reserve(user_headers_.size());
    for (const auto &entry : user_headers_) {
        const auto &key   = entry.Key();
        const auto &value = entry.Value();
        ffi::HeaderEntry ffi_entry;
        ffi_entry.key.kind = static_cast<std::uint8_t>(key.Kind());
        ffi_entry.key.value.reserve(key.Value().size());
        for (const auto byte : key.Value()) {
            ffi_entry.key.value.push_back(byte);
        }
        ffi_entry.value.kind = static_cast<std::uint8_t>(value.Kind());
        ffi_entry.value.value.reserve(value.Value().size());
        for (const auto byte : value.Value()) {
            ffi_entry.value.value.push_back(byte);
        }
        ffi_message.user_headers.push_back(std::move(ffi_entry));
    }
    return ffi_message;
}

IggyMessagePolled IggyMessagePolled::FromFfi(ffi::IggyMessagePolled message) {
    std::vector<HeaderEntry> user_headers;
    user_headers.reserve(message.user_headers.size());
    for (auto &entry : message.user_headers) {
        user_headers.push_back(HeaderEntry::FromFfi(std::move(entry)));
    }

    return IggyMessagePolled(
        message.checksum, absl::MakeUint128(message.id_hi, message.id_lo), message.offset, message.timestamp,
        message.origin_timestamp, message.user_headers_length, message.payload_length, message.reserved,
        std::vector<std::uint8_t>(message.payload.begin(), message.payload.end()), std::move(user_headers));
}

ResourceOptions ResourceOptions::FromFfi(rust::Vec<ffi::HeaderEntry> explicit_entries,
                                         rust::Vec<ffi::HeaderEntry> derived_entries) {
    std::map<std::string, HeaderField> explicit_options;
    for (auto &entry : explicit_entries) {
        HeaderEntry header_entry = HeaderEntry::FromFfi(std::move(entry));
        const auto &key_bytes    = header_entry.Key().Value();
        std::string key(key_bytes.begin(), key_bytes.end());
        explicit_options.emplace(std::move(key), std::move(header_entry.value_));
    }
    std::map<std::string, HeaderField> derived_options;
    for (auto &entry : derived_entries) {
        HeaderEntry header_entry = HeaderEntry::FromFfi(std::move(entry));
        const auto &key_bytes    = header_entry.Key().Value();
        std::string key(key_bytes.begin(), key_bytes.end());
        derived_options.emplace(std::move(key), std::move(header_entry.value_));
    }
    return ResourceOptions(std::move(explicit_options), std::move(derived_options));
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
        partitions.push_back(Partition::FromFfi(partition));
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

ConsumerGroupMember ConsumerGroupMember::FromFfi(ffi::ConsumerGroupMember member) {
    return ConsumerGroupMember(member.id, member.partitions_count,
                               std::vector<std::uint32_t>(member.partitions.begin(), member.partitions.end()));
}

ConsumerGroup ConsumerGroup::FromFfi(ffi::ConsumerGroup group) {
    return ConsumerGroup(group.id, std::string(group.name.c_str(), group.name.size()), group.partitions_count,
                         group.members_count);
}

ConsumerGroupDetails ConsumerGroupDetails::FromFfi(ffi::ConsumerGroupDetails group) {
    std::vector<ConsumerGroupMember> members;
    members.reserve(group.members.size());
    for (auto &member : group.members) {
        members.push_back(ConsumerGroupMember::FromFfi(std::move(member)));
    }

    return ConsumerGroupDetails(group.id, std::string(group.name.c_str(), group.name.size()), group.partitions_count,
                                group.members_count, std::move(members));
}

}  // namespace iggy
