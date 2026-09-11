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

HeaderField HeaderField::FromFfi(ffi::HeaderField field) {
    return HeaderField(static_cast<HeaderKind>(field.kind),
                       std::vector<std::uint8_t>(field.value.begin(), field.value.end()));
}

HeaderEntry HeaderEntry::FromFfi(ffi::HeaderEntry entry) {
    return HeaderEntry(HeaderField::FromFfi(std::move(entry.key)), HeaderField::FromFfi(std::move(entry.value)));
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
