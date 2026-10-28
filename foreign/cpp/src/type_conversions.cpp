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

std::optional<StreamEncryptionSource> EncryptionKeySourceFromFfi(bool has_source, ffi::StreamEncryptionSource source) {
    if (!has_source) {
        return std::nullopt;
    }
    return static_cast<StreamEncryptionSource>(source);
}

std::optional<std::string> TextFromFfi(bool has_text, const rust::String &text) {
    if (!has_text) {
        return std::nullopt;
    }
    return std::string(text.data(), text.size());
}

std::vector<std::string> TextsFromFfi(const rust::Vec<rust::String> &texts) {
    std::vector<std::string> result;
    result.reserve(texts.size());
    for (const auto &text : texts) {
        result.emplace_back(text.data(), text.size());
    }
    return result;
}

}  // namespace

LoginInfo LoginInfo::FromFfi(ffi::LoginInfo login_info) {
    std::optional<std::string> access_token;
    std::optional<std::uint64_t> access_token_expiry;
    if (login_info.has_access_token) {
        access_token.emplace(login_info.access_token.c_str(), login_info.access_token.size());
        access_token_expiry = login_info.access_token_expiry;
    }

    return LoginInfo(login_info.user_id, std::move(access_token), access_token_expiry);
}

ffi::GlobalPermissions GlobalPermissions::ToFfi() const {
    ffi::GlobalPermissions permissions{};
    permissions.manage_servers = manage_servers_;
    permissions.read_servers   = read_servers_;
    permissions.manage_users   = manage_users_;
    permissions.read_users     = read_users_;
    permissions.manage_streams = manage_streams_;
    permissions.read_streams   = read_streams_;
    permissions.manage_topics  = manage_topics_;
    permissions.read_topics    = read_topics_;
    permissions.poll_messages  = poll_messages_;
    permissions.send_messages  = send_messages_;
    return permissions;
}

GlobalPermissions GlobalPermissions::FromFfi(ffi::GlobalPermissions permissions) {
    GlobalPermissions result;
    result.manage_servers_ = permissions.manage_servers;
    result.read_servers_   = permissions.read_servers;
    result.manage_users_   = permissions.manage_users;
    result.read_users_     = permissions.read_users;
    result.manage_streams_ = permissions.manage_streams;
    result.read_streams_   = permissions.read_streams;
    result.manage_topics_  = permissions.manage_topics;
    result.read_topics_    = permissions.read_topics;
    result.poll_messages_  = permissions.poll_messages;
    result.send_messages_  = permissions.send_messages;
    return result;
}

ffi::TopicPermissions TopicPermissions::ToFfi() const {
    ffi::TopicPermissions permissions{};
    permissions.manage_topic  = manage_topic_;
    permissions.read_topic    = read_topic_;
    permissions.poll_messages = poll_messages_;
    permissions.send_messages = send_messages_;
    return permissions;
}

TopicPermissions TopicPermissions::FromFfi(ffi::TopicPermissions permissions) {
    TopicPermissions result;
    result.manage_topic_  = permissions.manage_topic;
    result.read_topic_    = permissions.read_topic;
    result.poll_messages_ = permissions.poll_messages;
    result.send_messages_ = permissions.send_messages;
    return result;
}

ffi::StreamPermissions StreamPermissions::ToFfi() const {
    ffi::StreamPermissions permissions{};
    permissions.manage_stream = manage_stream_;
    permissions.read_stream   = read_stream_;
    permissions.manage_topics = manage_topics_;
    permissions.read_topics   = read_topics_;
    permissions.poll_messages = poll_messages_;
    permissions.send_messages = send_messages_;
    permissions.topics.reserve(topics_.size());
    for (const auto &[topic_id, topic_permissions] : topics_) {
        ffi::TopicPermissionEntry entry{};
        entry.topic_id    = topic_id;
        entry.permissions = topic_permissions.ToFfi();
        permissions.topics.push_back(entry);
    }
    return permissions;
}

StreamPermissions StreamPermissions::FromFfi(const ffi::StreamPermissions &permissions) {
    StreamPermissions result;
    result.manage_stream_ = permissions.manage_stream;
    result.read_stream_   = permissions.read_stream;
    result.manage_topics_ = permissions.manage_topics;
    result.read_topics_   = permissions.read_topics;
    result.poll_messages_ = permissions.poll_messages;
    result.send_messages_ = permissions.send_messages;
    for (const auto &entry : permissions.topics) {
        result.topics_.insert_or_assign(entry.topic_id, TopicPermissions::FromFfi(entry.permissions));
    }
    return result;
}

ffi::Permissions Permissions::ToFfi() const {
    ffi::Permissions permissions{};
    permissions.global = global_.ToFfi();
    permissions.streams.reserve(streams_.size());
    for (const auto &[stream_id, stream_permissions] : streams_) {
        ffi::StreamPermissionEntry entry{};
        entry.stream_id   = stream_id;
        entry.permissions = stream_permissions.ToFfi();
        permissions.streams.push_back(std::move(entry));
    }
    return permissions;
}

Permissions Permissions::FromFfi(const ffi::Permissions &permissions) {
    Permissions result;
    result.global_ = GlobalPermissions::FromFfi(permissions.global);
    for (const auto &entry : permissions.streams) {
        result.streams_.insert_or_assign(entry.stream_id, StreamPermissions::FromFfi(entry.permissions));
    }
    return result;
}

UserInfo UserInfo::FromFfi(ffi::UserInfo user) {
    return UserInfo(user.id, user.created_at, static_cast<UserStatus>(user.status),
                    std::string(user.username.c_str(), user.username.size()),
                    ResourceOptions::FromFfi(std::move(user.options), std::move(user.derived_options)));
}

UserInfoDetails UserInfoDetails::FromFfi(ffi::UserInfoDetails user) {
    std::optional<::iggy::Permissions> permissions;
    if (user.has_permissions) {
        permissions = ::iggy::Permissions::FromFfi(user.permissions);
    }
    return UserInfoDetails(user.id, user.created_at, static_cast<UserStatus>(user.status),
                           std::string(user.username.c_str(), user.username.size()), std::move(permissions),
                           ResourceOptions::FromFfi(std::move(user.options), std::move(user.derived_options)));
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

ffi::PartitionContext PartitionContext::ToFfi() const {
    ffi::PartitionContext context{};
    context.incarnation      = incarnation_;
    context.owner_generation = owner_generation_;
    context.metadata_op      = metadata_op_;
    return context;
}

PartitionContext PartitionContext::FromFfi(ffi::PartitionContext context) {
    return PartitionContext(context.incarnation, context.owner_generation, context.metadata_op);
}

Partition Partition::FromFfi(ffi::Partition partition) {
    return Partition(partition.id, partition.created_at, partition.segments_count, partition.current_offset,
                     partition.size_bytes, partition.messages_count, PartitionContext::FromFfi(partition.context));
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
                         ResourceOptions::FromFfi(std::move(stream.options), std::move(stream.derived_options)),
                         EncryptionKeySourceFromFfi(stream.has_encryption_key_source, stream.encryption_key_source),
                         TextFromFfi(stream.has_encryption_key_fingerprint, stream.encryption_key_fingerprint),
                         TextsFromFfi(stream.encryption_kek_fingerprints));
}

Stream Stream::FromFfi(ffi::Stream stream) {
    return Stream(stream.id, stream.created_at, std::string(stream.name.c_str(), stream.name.size()), stream.size_bytes,
                  stream.messages_count, stream.topics_count,
                  ResourceOptions::FromFfi(std::move(stream.options), std::move(stream.derived_options)),
                  EncryptionKeySourceFromFfi(stream.has_encryption_key_source, stream.encryption_key_source),
                  TextFromFfi(stream.has_encryption_key_fingerprint, stream.encryption_key_fingerprint),
                  TextsFromFfi(stream.encryption_kek_fingerprints));
}

OptionSpec OptionSpec::FromFfi(ffi::OptionSpec spec) {
    return OptionSpec(std::string(spec.key.c_str(), spec.key.size()), static_cast<HeaderKind>(spec.kind),
                      std::vector<std::uint8_t>(spec.default_value.begin(), spec.default_value.end()),
                      std::string(spec.description.c_str(), spec.description.size()), spec.updatable, spec.resettable,
                      spec.secret, spec.read_only, spec.critical);
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

ConsumerGroupInfo ConsumerGroupInfo::FromFfi(ffi::ConsumerGroupInfo info) {
    return ConsumerGroupInfo(info.stream_id, info.topic_id, info.group_id);
}

ClientInfo ClientInfo::FromFfi(ffi::ClientInfo info) {
    std::optional<std::uint32_t> user_id;
    if (info.has_user_id) {
        user_id = info.user_id;
    }
    return ClientInfo(info.client_id, user_id, std::string(info.address.c_str(), info.address.size()),
                      std::string(info.transport.c_str(), info.transport.size()), info.consumer_groups_count);
}

ClientInfoDetails ClientInfoDetails::FromFfi(ffi::ClientInfoDetails info) {
    std::optional<std::uint32_t> user_id;
    if (info.has_user_id) {
        user_id = info.user_id;
    }
    std::vector<ConsumerGroupInfo> consumer_groups;
    consumer_groups.reserve(info.consumer_groups.size());
    for (auto &group : info.consumer_groups) {
        consumer_groups.push_back(ConsumerGroupInfo::FromFfi(group));
    }
    return ClientInfoDetails(info.client_id, user_id, std::string(info.address.c_str(), info.address.size()),
                             std::string(info.transport.c_str(), info.transport.size()), info.consumer_groups_count,
                             std::move(consumer_groups));
}

CacheMetricEntry CacheMetricEntry::FromFfi(ffi::CacheMetricEntry entry) {
    return CacheMetricEntry(entry.stream_id, entry.topic_id, entry.partition_id, entry.hits, entry.misses,
                            entry.hit_ratio);
}

Stats Stats::FromFfi(ffi::Stats stats) {
    std::optional<std::uint32_t> server_semver;
    if (stats.has_server_semver) {
        server_semver = stats.iggy_server_semver;
    }
    std::vector<CacheMetricEntry> cache_metrics;
    cache_metrics.reserve(stats.cache_metrics.size());
    for (auto &entry : stats.cache_metrics) {
        cache_metrics.push_back(CacheMetricEntry::FromFfi(entry));
    }
    return Stats(stats.process_id, stats.cpu_usage, stats.total_cpu_usage, stats.memory_usage, stats.total_memory,
                 stats.available_memory, stats.run_time_micros, stats.start_time_epoch_micros, stats.read_bytes,
                 stats.written_bytes, stats.messages_size_bytes, stats.streams_count, stats.topics_count,
                 stats.partitions_count, stats.segments_count, stats.messages_count, stats.clients_count,
                 stats.consumer_groups_count, std::string(stats.hostname.c_str(), stats.hostname.size()),
                 std::string(stats.os_name.c_str(), stats.os_name.size()),
                 std::string(stats.os_version.c_str(), stats.os_version.size()),
                 std::string(stats.kernel_version.c_str(), stats.kernel_version.size()),
                 std::string(stats.iggy_server_version.c_str(), stats.iggy_server_version.size()), server_semver,
                 std::move(cache_metrics), stats.threads_count, stats.free_disk_space, stats.total_disk_space,
                 stats.open_files_count, stats.open_files_limit);
}

MetadataChangeCursor MetadataChangeCursor::FromFfi(ffi::MetadataChangeCursor cursor) {
    MetadataChangeCursor result;
    result.log_id_   = absl::MakeUint128(cursor.log_id_hi, cursor.log_id_lo);
    result.op_       = cursor.op;
    result.checksum_ = absl::MakeUint128(cursor.checksum_hi, cursor.checksum_lo);
    result.ordinal_  = cursor.ordinal;
    result.text_     = std::string(cursor.text.data(), cursor.text.size());
    return result;
}

MetadataCurrentPageToken MetadataCurrentPageToken::FromFfi(ffi::MetadataCurrentPageToken token) {
    MetadataCurrentPageToken result;
    result.text_          = std::string(token.text.data(), token.text.size());
    result.anchor_        = MetadataChangeCursor::FromFfi(std::move(token.anchor));
    result.required_head_ = token.required_head;
    result.groups_mask_   = token.groups_mask;
    result.last_group_    = static_cast<MetadataChangeGroup>(token.last_group);
    result.last_key_.assign(token.last_key.begin(), token.last_key.end());
    return result;
}

ffi::MetadataChangesQuery MetadataChangesQuery::ToFfi() const {
    ffi::MetadataChangesQuery query{};
    query.start = static_cast<ffi::MetadataChangeStart>(start_);
    if (cursor_) {
        query.cursor = cursor_->ToString();
    }
    if (page_token_) {
        query.page_token = page_token_->ToString();
    }
    query.groups.reserve(groups_.size());
    for (const auto group : groups_) {
        query.groups.push_back(static_cast<std::uint8_t>(group));
    }
    query.has_since = since_.has_value();
    query.since     = since_.value_or(0);
    query.has_until = until_.has_value();
    query.until     = until_.value_or(0);
    query.limit     = limit_;
    return query;
}

MetadataStreamChange MetadataStreamChange::FromFfi(ffi::MetadataChangeBody &body) {
    MetadataStreamChange change;
    change.stream_id_         = body.stream_id;
    change.stream_created_op_ = body.stream_created_op;
    change.created_at_        = body.created_at;
    change.name_              = std::string(body.name.data(), body.name.size());
    if (body.options_readable) {
        change.options_ = ResourceOptions::FromFfi(std::move(body.options), std::move(body.derived_options));
    }
    return change;
}

MetadataStreamDeleted MetadataStreamDeleted::FromFfi(ffi::MetadataChangeBody &body) {
    MetadataStreamDeleted change;
    change.stream_id_         = body.stream_id;
    change.stream_created_op_ = body.stream_created_op;
    change.created_at_        = body.created_at;
    change.name_              = std::string(body.name.data(), body.name.size());
    if (body.has_requested_op) {
        change.requested_op_ = body.requested_op;
    }
    return change;
}

MetadataTopicChange MetadataTopicChange::FromFfi(ffi::MetadataChangeBody &body) {
    MetadataTopicChange change;
    change.stream_id_         = body.stream_id;
    change.stream_created_op_ = body.stream_created_op;
    change.topic_id_          = body.topic_id;
    change.topic_created_op_  = body.topic_created_op;
    change.created_at_        = body.created_at;
    change.name_              = std::string(body.name.data(), body.name.size());
    change.partitions_count_  = body.partitions_count;
    if (body.options_readable) {
        change.options_ = ResourceOptions::FromFfi(std::move(body.options), std::move(body.derived_options));
    }
    return change;
}

MetadataTopicItem MetadataTopicItem::FromFfi(ffi::MetadataChangeBody &body) {
    MetadataTopicItem item;
    item.stream_id_         = body.stream_id;
    item.stream_created_op_ = body.stream_created_op;
    item.topic_id_          = body.topic_id;
    item.topic_created_op_  = body.topic_created_op;
    item.created_at_        = body.created_at;
    item.name_              = std::string(body.name.data(), body.name.size());
    if (body.options_readable) {
        item.options_ = ResourceOptions::FromFfi(std::move(body.options), std::move(body.derived_options));
    }
    return item;
}

MetadataTopicDeleted MetadataTopicDeleted::FromFfi(ffi::MetadataChangeBody &body) {
    MetadataTopicDeleted change;
    change.stream_id_         = body.stream_id;
    change.stream_created_op_ = body.stream_created_op;
    change.topic_id_          = body.topic_id;
    change.topic_created_op_  = body.topic_created_op;
    change.created_at_        = body.created_at;
    change.name_              = std::string(body.name.data(), body.name.size());
    if (body.has_requested_op) {
        change.requested_op_ = body.requested_op;
    }
    return change;
}

MetadataPartitionsChanged MetadataPartitionsChanged::FromFfi(ffi::MetadataChangeBody &body) {
    MetadataPartitionsChanged change;
    change.stream_id_         = body.stream_id;
    change.stream_created_op_ = body.stream_created_op;
    change.topic_id_          = body.topic_id;
    change.topic_created_op_  = body.topic_created_op;
    change.partitions_count_  = body.partitions_count;
    change.removed_partition_ids_.assign(body.partition_ids.begin(), body.partition_ids.end());
    if (body.has_requested_op) {
        change.requested_op_ = body.requested_op;
    }
    return change;
}

MetadataPartitionsItem MetadataPartitionsItem::FromFfi(ffi::MetadataChangeBody &body) {
    MetadataPartitionsItem item;
    item.stream_id_         = body.stream_id;
    item.stream_created_op_ = body.stream_created_op;
    item.topic_id_          = body.topic_id;
    item.topic_created_op_  = body.topic_created_op;
    item.partitions_count_  = body.partitions_count;
    return item;
}

MetadataConsumerGroupCreated MetadataConsumerGroupCreated::FromFfi(ffi::MetadataChangeBody &body) {
    MetadataConsumerGroupCreated change;
    change.stream_id_             = body.stream_id;
    change.stream_created_op_     = body.stream_created_op;
    change.topic_id_              = body.topic_id;
    change.topic_created_op_      = body.topic_created_op;
    change.group_id_              = body.group_id;
    change.name_                  = std::string(body.name.data(), body.name.size());
    change.consumer_groups_count_ = body.consumer_groups_count;
    return change;
}

MetadataConsumerGroupDeleted MetadataConsumerGroupDeleted::FromFfi(ffi::MetadataChangeBody &body) {
    MetadataConsumerGroupDeleted change;
    change.stream_id_             = body.stream_id;
    change.stream_created_op_     = body.stream_created_op;
    change.topic_id_              = body.topic_id;
    change.topic_created_op_      = body.topic_created_op;
    change.group_id_              = body.group_id;
    change.name_                  = std::string(body.name.data(), body.name.size());
    change.consumer_groups_count_ = body.consumer_groups_count;
    if (body.has_requested_op) {
        change.requested_op_ = body.requested_op;
    }
    return change;
}

MetadataConsumerGroupItem MetadataConsumerGroupItem::FromFfi(ffi::MetadataChangeBody &body) {
    MetadataConsumerGroupItem item;
    item.stream_id_         = body.stream_id;
    item.stream_created_op_ = body.stream_created_op;
    item.topic_id_          = body.topic_id;
    item.topic_created_op_  = body.topic_created_op;
    item.group_id_          = body.group_id;
    item.name_              = std::string(body.name.data(), body.name.size());
    return item;
}

MetadataMemberJoined MetadataMemberJoined::FromFfi(ffi::MetadataChangeBody &body) {
    MetadataMemberJoined change;
    change.stream_id_         = body.stream_id;
    change.stream_created_op_ = body.stream_created_op;
    change.topic_id_          = body.topic_id;
    change.topic_created_op_  = body.topic_created_op;
    change.group_id_          = body.group_id;
    change.member_id_         = body.member_id;
    change.client_id_         = absl::MakeUint128(body.client_id_hi, body.client_id_lo);
    change.members_count_     = body.members_count;
    change.partition_ids_.assign(body.partition_ids.begin(), body.partition_ids.end());
    return change;
}

MetadataMemberLeft MetadataMemberLeft::FromFfi(ffi::MetadataChangeBody &body) {
    MetadataMemberLeft change;
    change.stream_id_         = body.stream_id;
    change.stream_created_op_ = body.stream_created_op;
    change.topic_id_          = body.topic_id;
    change.topic_created_op_  = body.topic_created_op;
    change.group_id_          = body.group_id;
    change.member_id_         = body.member_id;
    change.client_id_         = absl::MakeUint128(body.client_id_hi, body.client_id_lo);
    change.members_count_     = body.members_count;
    change.reason_            = static_cast<MetadataMemberLeftReason>(body.reason);
    return change;
}

MetadataPartitionHandedOver MetadataPartitionHandedOver::FromFfi(ffi::MetadataChangeBody &body) {
    MetadataPartitionHandedOver change;
    change.stream_id_         = body.stream_id;
    change.stream_created_op_ = body.stream_created_op;
    change.topic_id_          = body.topic_id;
    change.topic_created_op_  = body.topic_created_op;
    change.group_id_          = body.group_id;
    change.partition_id_      = body.partition_id;
    if (body.has_member_id) {
        change.member_id_ = body.member_id;
    }
    return change;
}

MetadataMemberItem MetadataMemberItem::FromFfi(ffi::MetadataChangeBody &body) {
    MetadataMemberItem item;
    item.stream_id_         = body.stream_id;
    item.stream_created_op_ = body.stream_created_op;
    item.topic_id_          = body.topic_id;
    item.topic_created_op_  = body.topic_created_op;
    item.group_id_          = body.group_id;
    item.member_id_         = body.member_id;
    item.client_id_         = absl::MakeUint128(body.client_id_hi, body.client_id_lo);
    item.partition_ids_.assign(body.partition_ids.begin(), body.partition_ids.end());
    return item;
}

MetadataUserChange MetadataUserChange::FromFfi(ffi::MetadataChangeBody &body) {
    MetadataUserChange change;
    change.user_id_         = body.user_id;
    change.user_created_at_ = body.user_created_at;
    change.username_        = std::string(body.name.data(), body.name.size());
    change.status_          = static_cast<UserStatus>(body.status);
    if (body.has_permissions) {
        change.permissions_ = ::iggy::Permissions::FromFfi(body.permissions);
    }
    if (body.options_readable) {
        change.options_ = ResourceOptions::FromFfi(std::move(body.options), std::move(body.derived_options));
    }
    return change;
}

MetadataUserIdentity MetadataUserIdentity::FromFfi(ffi::MetadataChangeBody &body) {
    MetadataUserIdentity change;
    change.user_id_         = body.user_id;
    change.user_created_at_ = body.user_created_at;
    change.username_        = std::string(body.name.data(), body.name.size());
    return change;
}

MetadataUserPermissionsUpdated MetadataUserPermissionsUpdated::FromFfi(ffi::MetadataChangeBody &body) {
    MetadataUserPermissionsUpdated change;
    change.user_id_         = body.user_id;
    change.user_created_at_ = body.user_created_at;
    change.username_        = std::string(body.name.data(), body.name.size());
    if (body.has_permissions) {
        change.permissions_ = ::iggy::Permissions::FromFfi(body.permissions);
    }
    change.reason_ = static_cast<MetadataPermissionsUpdatedReason>(body.reason);
    return change;
}

MetadataTokenChange MetadataTokenChange::FromFfi(ffi::MetadataChangeBody &body) {
    MetadataTokenChange change;
    change.user_id_         = body.user_id;
    change.user_created_at_ = body.user_created_at;
    change.name_            = std::string(body.name.data(), body.name.size());
    if (body.has_expiry) {
        change.expiry_ = body.expiry;
    }
    return change;
}

MetadataTokenDeleted MetadataTokenDeleted::FromFfi(ffi::MetadataChangeBody &body) {
    MetadataTokenDeleted change;
    change.user_id_         = body.user_id;
    change.user_created_at_ = body.user_created_at;
    change.name_            = std::string(body.name.data(), body.name.size());
    change.reason_          = static_cast<MetadataTokenDeletedReason>(body.reason);
    return change;
}

MetadataSession MetadataSession::FromFfi(ffi::MetadataChangeBody &body) {
    MetadataSession change;
    change.user_id_         = body.user_id;
    change.user_created_at_ = body.user_created_at;
    change.client_id_       = absl::MakeUint128(body.client_id_hi, body.client_id_lo);
    change.session_         = body.session;
    return change;
}

MetadataSessionClosed MetadataSessionClosed::FromFfi(ffi::MetadataChangeBody &body) {
    MetadataSessionClosed change;
    change.user_id_         = body.user_id;
    change.user_created_at_ = body.user_created_at;
    change.client_id_       = absl::MakeUint128(body.client_id_hi, body.client_id_lo);
    change.session_         = body.session;
    change.reason_          = static_cast<MetadataSessionClosedReason>(body.reason);
    return change;
}

MetadataChangeRecord MetadataChangeRecord::FromFfi(ffi::MetadataChangeRecord record) {
    const auto checksum = absl::MakeUint128(record.checksum_hi, record.checksum_lo);
    std::optional<std::uint32_t> actor_user_id;
    if (record.has_actor_user_id) {
        actor_user_id = record.actor_user_id;
    }
    switch (record.form) {
        case ffi::MetadataChangeForm::Known: {
            const auto kind = static_cast<MetadataChangeKind>(record.kind);
            return MetadataChangeRecord(record.op, record.ordinal, checksum, record.timestamp, actor_user_id, kind,
                                        static_cast<MetadataChangeGroup>(record.group), BodyFromFfi(kind, record.body));
        }
        case ffi::MetadataChangeForm::Unknown: {
            MetadataUnknownChange change;
            change.group_code_ = record.group;
            change.kind_code_  = record.kind;
            change.body_.assign(record.body.unknown_body.begin(), record.body.unknown_body.end());
            return MetadataChangeRecord(record.op, record.ordinal, checksum, record.timestamp, actor_user_id,
                                        std::nullopt, std::nullopt, std::move(change));
        }
        case ffi::MetadataChangeForm::UnknownJson: {
            MetadataUnknownJsonChange change;
            change.group_ = std::string(record.group_name.data(), record.group_name.size());
            change.kind_  = std::string(record.kind_name.data(), record.kind_name.size());
            change.json_.assign(record.body.unknown_body.begin(), record.body.unknown_body.end());
            return MetadataChangeRecord(record.op, record.ordinal, checksum, record.timestamp, actor_user_id,
                                        std::nullopt, std::nullopt, std::move(change));
        }
    }
    throw IggyException("Unknown metadata change record form");
}

MetadataChangeRecord::Body MetadataChangeRecord::BodyFromFfi(MetadataChangeKind kind, ffi::MetadataChangeBody &body) {
    switch (kind) {
        case MetadataChangeKind::StreamCreated:
        case MetadataChangeKind::StreamUpdated:
            return MetadataStreamChange::FromFfi(body);
        case MetadataChangeKind::StreamDeleted:
            return MetadataStreamDeleted::FromFfi(body);
        case MetadataChangeKind::TopicCreated:
        case MetadataChangeKind::TopicUpdated:
            return MetadataTopicChange::FromFfi(body);
        case MetadataChangeKind::TopicDeleted:
            return MetadataTopicDeleted::FromFfi(body);
        case MetadataChangeKind::PartitionsChanged:
            return MetadataPartitionsChanged::FromFfi(body);
        case MetadataChangeKind::ConsumerGroupCreated:
            return MetadataConsumerGroupCreated::FromFfi(body);
        case MetadataChangeKind::ConsumerGroupDeleted:
            return MetadataConsumerGroupDeleted::FromFfi(body);
        case MetadataChangeKind::ConsumerGroupMemberJoined:
            return MetadataMemberJoined::FromFfi(body);
        case MetadataChangeKind::ConsumerGroupMemberLeft:
            return MetadataMemberLeft::FromFfi(body);
        case MetadataChangeKind::ConsumerGroupPartitionHandedOver:
            return MetadataPartitionHandedOver::FromFfi(body);
        case MetadataChangeKind::UserCreated:
        case MetadataChangeKind::UserUpdated:
            return MetadataUserChange::FromFfi(body);
        case MetadataChangeKind::UserDeleted:
        case MetadataChangeKind::UserPasswordChanged:
            return MetadataUserIdentity::FromFfi(body);
        case MetadataChangeKind::UserPermissionsUpdated:
            return MetadataUserPermissionsUpdated::FromFfi(body);
        case MetadataChangeKind::PersonalAccessTokenCreated:
            return MetadataTokenChange::FromFfi(body);
        case MetadataChangeKind::PersonalAccessTokenDeleted:
            return MetadataTokenDeleted::FromFfi(body);
        case MetadataChangeKind::SessionRegistered:
            return MetadataSession::FromFfi(body);
        case MetadataChangeKind::SessionClosed:
            return MetadataSessionClosed::FromFfi(body);
    }
    throw IggyException("Unknown metadata change kind");
}

MetadataCurrentItem MetadataCurrentItem::FromFfi(ffi::MetadataCurrentItem item) {
    switch (item.form) {
        case ffi::MetadataChangeForm::Known: {
            const auto group = static_cast<MetadataChangeGroup>(item.group);
            return MetadataCurrentItem(group, BodyFromFfi(group, item.body));
        }
        case ffi::MetadataChangeForm::Unknown: {
            MetadataUnknownItem value;
            value.group_code_ = item.group;
            value.body_.assign(item.body.unknown_body.begin(), item.body.unknown_body.end());
            return MetadataCurrentItem(std::nullopt, std::move(value));
        }
        case ffi::MetadataChangeForm::UnknownJson: {
            MetadataUnknownJsonItem value;
            value.group_ = std::string(item.group_name.data(), item.group_name.size());
            value.json_.assign(item.body.unknown_body.begin(), item.body.unknown_body.end());
            return MetadataCurrentItem(std::nullopt, std::move(value));
        }
    }
    throw IggyException("Unknown metadata current item form");
}

MetadataCurrentItem::Body MetadataCurrentItem::BodyFromFfi(MetadataChangeGroup group, ffi::MetadataChangeBody &body) {
    switch (group) {
        case MetadataChangeGroup::Streams:
            return MetadataStreamChange::FromFfi(body);
        case MetadataChangeGroup::Topics:
            return MetadataTopicItem::FromFfi(body);
        case MetadataChangeGroup::Partitions:
            return MetadataPartitionsItem::FromFfi(body);
        case MetadataChangeGroup::ConsumerGroups:
            return MetadataConsumerGroupItem::FromFfi(body);
        case MetadataChangeGroup::ConsumerGroupMembers:
            return MetadataMemberItem::FromFfi(body);
        case MetadataChangeGroup::Users:
            return MetadataUserChange::FromFfi(body);
        case MetadataChangeGroup::PersonalAccessTokens:
            return MetadataTokenChange::FromFfi(body);
        case MetadataChangeGroup::Sessions:
            return MetadataSession::FromFfi(body);
    }
    throw IggyException("Unknown metadata change group");
}

MetadataChanges MetadataChanges::FromFfi(ffi::MetadataChanges changes) {
    MetadataChanges result;
    result.status_ = static_cast<MetadataChangeStatus>(changes.status);
    result.phase_  = static_cast<MetadataChangePhase>(changes.phase);
    if (changes.has_log_id) {
        result.log_id_ = absl::MakeUint128(changes.log_id_hi, changes.log_id_lo);
    }
    result.oldest_op_      = changes.oldest_op;
    result.retained_since_ = changes.retained_since;
    result.head_op_        = changes.head_op;
    if (changes.has_next) {
        result.next_ = MetadataChangeCursor::FromFfi(std::move(changes.next));
    }
    result.has_more_ = changes.has_more;
    result.records_.reserve(changes.records.size());
    for (auto &record : changes.records) {
        result.records_.push_back(MetadataChangeRecord::FromFfi(std::move(record)));
    }
    result.items_.reserve(changes.items.size());
    for (auto &item : changes.items) {
        result.items_.push_back(MetadataCurrentItem::FromFfi(std::move(item)));
    }
    if (changes.has_current_page_token) {
        result.current_page_token_ = MetadataCurrentPageToken::FromFfi(std::move(changes.current_page_token));
    }
    return result;
}

}  // namespace iggy
