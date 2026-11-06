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

// The methods: decode the arguments, call the SDK through its generated bridge, and return the
// canonical form of the result. The agent uses the bridge, iggy::ffi, because the IggyBlockingClient
// of iggy.hpp has no SendMessages, Ping or raw requests, and the two cannot share a connection.

#include <map>
#include <string>
#include <utility>

#include "args.hpp"
#include "canonical.hpp"

namespace agent {

namespace {

// The budget of a resolution that names none: IggyBlockingClient::kDefaultResolutionTimeout.
constexpr std::uint64_t kDefaultResolveMillis = 30'000;
// MetadataChangesQuery::kDefaultLimit of the SDK.
constexpr std::uint32_t kDefaultMetadataLimit = 100;

using Method = Json (*)(const ClientCall &call, Args &args);

ffi::PollOptions PollOptionsOf(Args &args) {
    auto options = ffi::default_poll_options();
    SetCommit(options, args.Optional("commit"));
    if (const auto *max_wait = args.Optional("max_wait_ms")) {
        options.max_wait_micros = Unsigned(*max_wait, "max_wait_ms") * 1000;
    }
    return options;
}

// A poll runs as a PendingPoll, so a cancelled task drops its request as the SDK documents, which
// also cancels a poll that waits on the server.
ffi::PolledMessages WaitForPoll(const ffi::PendingPoll &poll, const std::atomic<bool> &cancelled) {
    while (!cancelled) {
        auto outcome = Sdk([&](ffi::ErrorInfo &info) { return poll.wait(kWaitSliceMicros, info); });
        if (outcome.ready) {
            return std::move(outcome.batch);
        }
    }
    poll.cancel();
    throw AgentFailure("the call was cancelled");
}

Json Connect(const ClientCall &call, Args &args) {
    args.Finish();
    Sdk([&](ffi::ErrorInfo &) { call.client.connect(); });
    return nullptr;
}

Json Disconnect(const ClientCall &call, Args &args) {
    args.Finish();
    Sdk([&](ffi::ErrorInfo &) { call.client.disconnect(); });
    return nullptr;
}

Json Ping(const ClientCall &call, Args &args) {
    args.Finish();
    Sdk([&](ffi::ErrorInfo &) { call.client.ping(); });
    return nullptr;
}

Json LoginUser(const ClientCall &call, Args &args) {
    const auto username = Text(args.Required("username"), "username");
    const auto password = Text(args.Required("password"), "password");
    args.Finish();
    const auto login = Sdk([&](ffi::ErrorInfo &info) { return call.client.login_user(username, password, info); });
    return {{"user_id", login.user_id}};
}

Json LogoutUser(const ClientCall &call, Args &args) {
    args.Finish();
    Sdk([&](ffi::ErrorInfo &info) { call.client.logout_user(info); });
    return nullptr;
}

Json GetStream(const ClientCall &call, Args &args) {
    auto stream = Identifier(args.Required("stream_id"));
    args.Finish();
    return StreamDetailsJson(Sdk([&](ffi::ErrorInfo &) { return call.client.get_stream(std::move(stream)); }));
}

Json GetStreams(const ClientCall &call, Args &args) {
    args.Finish();
    const auto streams = Sdk([&](ffi::ErrorInfo &) { return call.client.get_streams(); });
    auto view          = Json::array();
    for (const auto &stream : streams) {
        view.push_back(StreamJson(stream));
    }
    return view;
}

Json CreateStream(const ClientCall &call, Args &args) {
    const auto name = Text(args.Required("name"), "name");
    ffi::StreamCreateOptions options{};
    if (const auto *key = args.Optional("encryption_key_hex")) {
        options.has_encryption_key = true;
        options.encryption_key     = RustBytes(FromHex(Text(*key, "encryption_key_hex")));
    }
    args.Finish();
    return StreamDetailsJson(
        Sdk([&](ffi::ErrorInfo &info) { return call.client.create_stream(name, std::move(options), info); }));
}

Json UpdateStream(const ClientCall &call, Args &args) {
    auto stream = Identifier(args.Required("stream_id"));
    ffi::StreamUpdateOptions options{};
    if (const auto *name = args.Optional("name")) {
        options.has_name = true;
        options.name     = Text(*name, "name");
    }
    // Raw reset names go to the server, which decides, as in the Rust and Go agents.
    options.raw_reset = ResetKeys(args.Optional("reset"));
    args.Finish();
    Sdk([&](ffi::ErrorInfo &info) { call.client.update_stream(std::move(stream), std::move(options), info); });
    return nullptr;
}

Json DeleteStream(const ClientCall &call, Args &args) {
    auto stream = Identifier(args.Required("stream_id"));
    args.Finish();
    Sdk([&](ffi::ErrorInfo &info) { call.client.delete_stream(std::move(stream), info); });
    return nullptr;
}

Json GetTopic(const ClientCall &call, Args &args) {
    auto stream = Identifier(args.Required("stream_id"));
    auto topic  = Identifier(args.Required("topic_id"));
    args.Finish();
    return TopicDetailsJson(
        Sdk([&](ffi::ErrorInfo &) { return call.client.get_topic(std::move(stream), std::move(topic)); }));
}

Json GetTopics(const ClientCall &call, Args &args) {
    auto stream = Identifier(args.Required("stream_id"));
    args.Finish();
    const auto topics = Sdk([&](ffi::ErrorInfo &) { return call.client.get_topics(std::move(stream)); });
    auto view         = Json::array();
    for (const auto &topic : topics) {
        view.push_back(TopicJson(topic));
    }
    return view;
}

template <typename Options>
void SetTopicSettings(Options &options, Args &args) {
    if (const auto *expiry = args.Optional("message_expiry")) {
        const auto [kind, value]     = Expiry(*expiry);
        options.has_message_expiry   = true;
        options.message_expiry_kind  = kind;
        options.message_expiry_value = value;
    }
    if (const auto *size = args.Optional("max_topic_size")) {
        options.has_max_topic_size = true;
        options.max_topic_size     = TopicSize(*size);
    }
}

Json CreateTopic(const ClientCall &call, Args &args) {
    auto stream     = Identifier(args.Required("stream_id"));
    const auto name = Text(args.Required("name"), "name");
    ffi::TopicCreateOptions options{};
    if (const auto *count = args.Optional("partitions_count")) {
        options.has_partitions_count = true;
        options.partitions_count     = Unsigned32(*count, "partitions_count");
    }
    SetTopicSettings(options, args);
    args.Finish();
    return TopicDetailsJson(Sdk([&](ffi::ErrorInfo &info) {
        return call.client.create_topic(std::move(stream), name, std::move(options), info);
    }));
}

Json UpdateTopic(const ClientCall &call, Args &args) {
    auto stream = Identifier(args.Required("stream_id"));
    auto topic  = Identifier(args.Required("topic_id"));
    ffi::TopicUpdateOptions options{};
    if (const auto *name = args.Optional("name")) {
        options.has_name = true;
        options.name     = Text(*name, "name");
    }
    SetTopicSettings(options, args);
    options.raw_reset = ResetKeys(args.Optional("reset"));
    args.Finish();
    Sdk([&](ffi::ErrorInfo &info) {
        call.client.update_topic(std::move(stream), std::move(topic), std::move(options), info);
    });
    return nullptr;
}

Json DeleteTopic(const ClientCall &call, Args &args) {
    auto stream = Identifier(args.Required("stream_id"));
    auto topic  = Identifier(args.Required("topic_id"));
    args.Finish();
    Sdk([&](ffi::ErrorInfo &info) { call.client.delete_topic(std::move(stream), std::move(topic), info); });
    return nullptr;
}

Json SendMessages(const ClientCall &call, Args &args) {
    auto stream       = Identifier(args.Required("stream_id"));
    auto topic        = Identifier(args.Required("topic_id"));
    auto partitioning = Partitioning(args.Required("partitioning"));
    auto messages     = Messages(args.Required("messages"));
    args.Finish();
    Sdk([&](ffi::ErrorInfo &info) {
        call.client.send_messages(std::move(stream), std::move(topic), partitioning.kind, std::move(partitioning.value),
                                  std::move(messages), info);
    });
    return nullptr;
}

Json PollMessages(const ClientCall &call, Args &args) {
    auto stream          = Identifier(args.Required("stream_id"));
    auto topic           = Identifier(args.Required("topic_id"));
    const auto partition = Unsigned32(args.Required("partition_id"), "partition_id");
    auto consumer        = Consumer(args.Optional("consumer"));
    auto strategy        = Strategy(args.Required("strategy"));
    auto options         = PollOptionsOf(args);
    options.count        = Unsigned32(args.Required("count"), "count");
    if (const auto *min_count = args.Optional("min_count")) {
        options.min_count = Unsigned32(*min_count, "min_count");
    }
    args.Finish();
    const auto poll = Sdk([&](ffi::ErrorInfo &) {
        return call.client.start_poll_messages(std::move(stream), std::move(topic), partition, consumer.kind,
                                               std::move(consumer.id), std::move(strategy), options);
    });
    return PolledJson(WaitForPoll(*poll, call.cancelled));
}

Json CreateConsumerGroup(const ClientCall &call, Args &args) {
    auto stream     = Identifier(args.Required("stream_id"));
    auto topic      = Identifier(args.Required("topic_id"));
    const auto name = Text(args.Required("name"), "name");
    args.Finish();
    return GroupJson(Sdk([&](ffi::ErrorInfo &info) {
        return call.client.create_consumer_group(std::move(stream), std::move(topic), name, info);
    }));
}

Json GetConsumerGroup(const ClientCall &call, Args &args) {
    auto stream = Identifier(args.Required("stream_id"));
    auto topic  = Identifier(args.Required("topic_id"));
    auto group  = Identifier(args.Required("group_id"));
    args.Finish();
    return GroupJson(Sdk([&](ffi::ErrorInfo &) {
        return call.client.get_consumer_group(std::move(stream), std::move(topic), std::move(group));
    }));
}

Json DeleteConsumerGroup(const ClientCall &call, Args &args) {
    auto stream = Identifier(args.Required("stream_id"));
    auto topic  = Identifier(args.Required("topic_id"));
    auto group  = Identifier(args.Required("group_id"));
    args.Finish();
    Sdk([&](ffi::ErrorInfo &info) {
        call.client.delete_consumer_group(std::move(stream), std::move(topic), std::move(group), info);
    });
    return nullptr;
}

Json BuildReader(const ClientCall &call, Args &args) {
    auto stream   = Identifier(args.Required("stream_id"));
    auto topic    = Identifier(args.Required("topic_id"));
    auto consumer = Consumer(args.Optional("consumer"));
    ffi::ReaderOptions options{};
    if (const auto *partitions = args.Optional("partitions")) {
        if (!partitions->is_array()) {
            throw AgentFailure("arguments: partitions cannot be " + partitions->dump());
        }
        for (const auto &partition : *partitions) {
            options.partitions.push_back(Unsigned32(partition, "partition"));
        }
    }
    if (const auto *start = args.Optional("start")) {
        options.has_start = true;
        options.start     = Strategy(*start);
    }
    options.has_poll_options = true;
    options.poll_options     = PollOptionsOf(args);
    args.Finish();
    if (call.save_as.empty()) {
        throw AgentFailure("build_reader needs save_as");
    }
    auto reader = Sdk([&](ffi::ErrorInfo &info) {
        return call.client.reader(std::move(stream), std::move(topic), consumer.kind, std::move(consumer.id),
                                  std::move(options), info);
    });
    call.agent.KeepReader(call.save_as, std::move(reader));
    return nullptr;
}

Json BuildConsumer(const ClientCall &call, Args &args) {
    auto stream      = Identifier(args.Required("stream_id"));
    auto topic       = Identifier(args.Required("topic_id"));
    const auto group = Text(args.Required("group"), "group");
    ffi::ConsumerOptions options{};
    options.has_poll_options = true;
    options.poll_options     = PollOptionsOf(args);
    if (const auto *count = args.Optional("count")) {
        options.poll_options.count = Unsigned32(*count, "count");
    }
    args.Finish();
    if (call.save_as.empty()) {
        throw AgentFailure("build_consumer needs save_as");
    }
    auto consumer = Sdk([&](ffi::ErrorInfo &info) {
        return call.client.consumer(group, std::move(stream), std::move(topic), true, std::move(options), info);
    });
    call.agent.KeepConsumer(call.save_as, std::move(consumer));
    return nullptr;
}

Json ResolveOperation(const ClientCall &call, Args &args) {
    const auto operation = Unsigned(args.Required("operation_id"), "operation_id");
    const auto *deadline = args.Optional("deadline_ms");
    const auto millis    = deadline != nullptr ? Unsigned(*deadline, "deadline_ms") : kDefaultResolveMillis;
    args.Finish();
    Sdk([&](ffi::ErrorInfo &info) { call.client.resolve_operation(operation, millis * 1000, info); });
    return nullptr;
}

Json AbandonOperation(const ClientCall &call, Args &args) {
    const auto operation = Unsigned(args.Required("operation_id"), "operation_id");
    args.Finish();
    Sdk([&](ffi::ErrorInfo &info) { call.client.abandon_operation(operation, info); });
    return nullptr;
}

rust::Vec<std::uint8_t> RawPayload(Args &args) {
    const auto *payload = args.Optional("payload_hex");
    return RustBytes(payload != nullptr ? FromHex(Text(*payload, "payload_hex")) : std::vector<std::uint8_t>());
}

Json SendBinaryRequest(const ClientCall &call, Args &args) {
    const auto code = Unsigned32(args.Required("code"), "code");
    auto payload    = RawPayload(args);
    args.Finish();
    const auto reply =
        Sdk([&](ffi::ErrorInfo &info) { return call.client.send_binary_request(code, std::move(payload), info); });
    return {{"payload_hex", Hex(reply)}};
}

Json SendBinaryRequestToReplica(const ClientCall &call, Args &args) {
    const auto code    = Unsigned32(args.Required("code"), "code");
    auto payload       = RawPayload(args);
    const auto replica = Unsigned8(args.Required("replica"), "replica");
    args.Finish();
    const auto reply = Sdk([&](ffi::ErrorInfo &info) {
        return call.client.send_binary_request_to_replica(code, std::move(payload), replica, info);
    });
    return {{"payload_hex", Hex(reply)}};
}

Json DescribeOptions(const ClientCall &call, Args &args) {
    const auto scope = Text(args.Required("scope"), "scope");
    args.Finish();
    if (scope != "stream" && scope != "topic" && scope != "user") {
        throw AgentFailure("arguments: an unknown scope " + scope);
    }
    const auto specs = Sdk([&](ffi::ErrorInfo &) { return call.client.describe_options(scope); });
    auto view        = Json::array();
    for (const auto &spec : specs) {
        view.push_back(OptionSpecJson(spec));
    }
    return view;
}

Json CreateUser(const ClientCall &call, Args &args) {
    const auto username    = Text(args.Required("username"), "username");
    const auto password    = Text(args.Required("password"), "password");
    const auto *status     = args.Optional("status");
    const auto *granted    = args.Optional("permissions");
    const auto user_status = status != nullptr ? UserStatus(*status) : ffi::UserStatus::Active;
    auto permissions       = granted != nullptr ? Permissions(*granted) : ffi::Permissions{};
    args.Finish();
    ffi::UserCreateOptions options{};
    return UserJson(Sdk([&](ffi::ErrorInfo &info) {
        return call.client.create_user(username, password, user_status, granted != nullptr, std::move(permissions),
                                       std::move(options), info);
    }));
}

Json GetUser(const ClientCall &call, Args &args) {
    auto user = Identifier(args.Required("user_id"));
    args.Finish();
    return UserJson(Sdk([&](ffi::ErrorInfo &) { return call.client.get_user(std::move(user)); }));
}

Json UpdateUser(const ClientCall &call, Args &args) {
    auto user = Identifier(args.Required("user_id"));
    ffi::UserUpdateOptions options{};
    options.status = ffi::UserStatus::Active;
    if (const auto *username = args.Optional("username")) {
        options.has_username = true;
        options.username     = Text(*username, "username");
    }
    if (const auto *status = args.Optional("status")) {
        options.has_status = true;
        options.status     = UserStatus(*status);
    }
    args.Finish();
    Sdk([&](ffi::ErrorInfo &info) { call.client.update_user(std::move(user), std::move(options), info); });
    return nullptr;
}

Json DeleteUser(const ClientCall &call, Args &args) {
    auto user = Identifier(args.Required("user_id"));
    args.Finish();
    Sdk([&](ffi::ErrorInfo &info) { call.client.delete_user(std::move(user), info); });
    return nullptr;
}

Json GetMetadataChanges(const ClientCall &call, Args &args) {
    ffi::MetadataChangesQuery query{};
    SetStart(query, args.Required("start"));
    query.groups      = Groups(args.Optional("groups"));
    const auto *limit = args.Optional("limit");
    query.limit       = limit != nullptr ? Unsigned32(*limit, "limit") : kDefaultMetadataLimit;
    args.Finish();
    return MetadataChangesJson(
        Sdk([&](ffi::ErrorInfo &info) { return call.client.get_metadata_changes(std::move(query), info); }));
}

const std::map<std::string, Method, std::less<>> &Methods() {
    static const std::map<std::string, Method, std::less<>> kMethods = {
        {"connect", Connect},
        {"disconnect", Disconnect},
        {"ping", Ping},
        {"login_user", LoginUser},
        {"logout_user", LogoutUser},
        {"get_stream", GetStream},
        {"get_streams", GetStreams},
        {"create_stream", CreateStream},
        {"update_stream", UpdateStream},
        {"delete_stream", DeleteStream},
        {"get_topic", GetTopic},
        {"get_topics", GetTopics},
        {"create_topic", CreateTopic},
        {"update_topic", UpdateTopic},
        {"delete_topic", DeleteTopic},
        {"send_messages", SendMessages},
        {"poll_messages", PollMessages},
        {"create_consumer_group", CreateConsumerGroup},
        {"get_consumer_group", GetConsumerGroup},
        {"delete_consumer_group", DeleteConsumerGroup},
        {"build_reader", BuildReader},
        {"build_consumer", BuildConsumer},
        {"resolve_operation", ResolveOperation},
        {"abandon_operation", AbandonOperation},
        {"send_binary_request", SendBinaryRequest},
        {"send_binary_request_to_replica", SendBinaryRequestToReplica},
        {"describe_options", DescribeOptions},
        {"create_user", CreateUser},
        {"get_user", GetUser},
        {"update_user", UpdateUser},
        {"delete_user", DeleteUser},
        {"get_metadata_changes", GetMetadataChanges},
    };
    return kMethods;
}

}  // namespace

Json CallClient(const ClientCall &call, const std::string &method, const Json &args) {
    const auto &methods = Methods();
    const auto found    = methods.find(method);
    if (found == methods.end()) {
        throw Unsupported("the method " + method);
    }
    Args decoded(args);
    return found->second(call, decoded);
}

// The SDK waits for a batch up to a timeout and returns an empty batch when the time runs out. A
// reader never hands over an empty batch otherwise, so the agent waits on, as the Rust reader does.
Json NextBatch(const ffi::PartitionReader &reader, const Json &args, const std::atomic<bool> &cancelled) {
    Args(args).Finish();
    while (!cancelled) {
        const auto batch = SdkRead([&](ffi::ReadErrorInfo &info) { return reader.next(kWaitSliceMicros, info); });
        if (!batch.messages.empty()) {
            return PolledJson(batch);
        }
    }
    throw AgentFailure("the call was cancelled");
}

Json NextMessage(const ffi::Consumer &consumer, const Json &args, const std::atomic<bool> &cancelled) {
    Args(args).Finish();
    while (!cancelled) {
        const auto received = SdkRead([&](ffi::ReadErrorInfo &info) { return consumer.next(kWaitSliceMicros, info); });
        if (!received.empty()) {
            return {{"partition_id", received[0].partition_id}, {"message", MessageJson(received[0].message)}};
        }
    }
    throw AgentFailure("the call was cancelled");
}

}  // namespace agent
