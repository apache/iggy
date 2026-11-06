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

package org.apache.iggy.conformance;

import org.apache.iggy.client.async.reader.IggyConsumer;
import org.apache.iggy.client.async.reader.PartitionReader;
import org.apache.iggy.client.async.reader.PartitionReaderBuilder;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.conformance.Entities.Guarded;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.identifier.UserId;
import org.apache.iggy.message.PollOptions;
import org.apache.iggy.stream.StreamOptions;
import org.apache.iggy.stream.StreamUpdate;
import org.apache.iggy.system.MetadataChangesQuery;
import org.apache.iggy.system.OptionsScope;
import org.apache.iggy.topic.TopicOptions;
import org.apache.iggy.topic.TopicUpdate;
import org.apache.iggy.user.UserUpdate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

import java.time.Duration;
import java.util.HexFormat;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/** The methods of conformance/README.md: read the arguments, call the SDK, return the canonical form of the result. */
final class Calls {

    /** The budget of a resolution that names none. The Java SDK has no default, so this is that of Rust and Go. */
    private static final Duration DEFAULT_RESOLVE_DEADLINE = Duration.ofSeconds(30);

    private final Entities entities;

    Calls(Entities entities) {
        this.entities = entities;
    }

    /** The result of a call, or null for a method without one. */
    JsonNode call(Call call, String entity, String method, JsonNode raw, String saveAs) {
        if (method.equals("next")) {
            Args.of(raw);
            return next(call, entity);
        }
        AsyncIggyTcpClient client = entities.client(entity);
        if (client == null) {
            throw new IllegalArgumentException("no client " + entity);
        }
        return switch (method) {
            case "connect" -> noArgsCall(raw, () -> call.await(client.connect()));
            case "disconnect" -> noArgsCall(raw, () -> call.await(client.disconnect()));
            case "ping" -> noArgsCall(raw, () -> call.await(client.system().ping()));
            case "logout_user" ->
                noArgsCall(raw, () -> call.await(client.users().logout()));
            case "login_user" -> {
                Args args = Args.of(raw, "username", "password");
                var identity = call.await(client.users().login(args.text("username"), args.text("password")));
                yield Canonical.object().put("user_id", identity.userId());
            }
            case "get_stream" -> {
                Args args = Args.of(raw, "stream_id");
                yield call.await(client.streams().getStream(streamId(args)))
                        .<JsonNode>map(Canonical::streamDetails)
                        .orElse(null);
            }
            case "get_streams" -> {
                Args.of(raw);
                yield Canonical.list(call.await(client.streams().getStreams()), Canonical::stream);
            }
            case "create_stream" -> createStream(call, client, Args.of(raw, "name", "encryption_key_hex"));
            case "update_stream" -> updateStream(call, client, Args.of(raw, "stream_id", "name", "reset"));
            case "delete_stream" -> {
                Args args = Args.of(raw, "stream_id");
                call.await(client.streams().deleteStream(streamId(args)));
                yield null;
            }
            case "get_topic" -> {
                Args args = Args.of(raw, "stream_id", "topic_id");
                yield call.await(client.topics().getTopic(streamId(args), topicId(args)))
                        .<JsonNode>map(Canonical::topicDetails)
                        .orElse(null);
            }
            case "get_topics" -> {
                Args args = Args.of(raw, "stream_id");
                yield Canonical.list(call.await(client.topics().getTopics(streamId(args))), Canonical::topic);
            }
            case "create_topic" ->
                createTopic(
                        call,
                        client,
                        Args.of(raw, "stream_id", "name", "partitions_count", "message_expiry", "max_topic_size"));
            case "update_topic" ->
                updateTopic(
                        call,
                        client,
                        Args.of(raw, "stream_id", "topic_id", "name", "message_expiry", "max_topic_size", "reset"));
            case "delete_topic" -> {
                Args args = Args.of(raw, "stream_id", "topic_id");
                call.await(client.topics().deleteTopic(streamId(args), topicId(args)));
                yield null;
            }
            case "send_messages" -> {
                Args args = Args.of(raw, "stream_id", "topic_id", "partitioning", "messages");
                call.await(client.messages()
                        .sendMessages(
                                streamId(args),
                                topicId(args),
                                args.partitioning("partitioning"),
                                args.messages("messages")));
                yield null;
            }
            case "poll_messages" ->
                pollMessages(
                        call,
                        client,
                        Args.of(
                                raw,
                                "stream_id",
                                "topic_id",
                                "partition_id",
                                "consumer",
                                "strategy",
                                "count",
                                "commit",
                                "max_wait_ms",
                                "min_count"));
            case "create_consumer_group" -> {
                Args args = Args.of(raw, "stream_id", "topic_id", "name");
                yield Canonical.consumerGroup(call.await(
                        client.consumerGroups().createConsumerGroup(streamId(args), topicId(args), args.text("name"))));
            }
            case "get_consumer_group" -> {
                Args args = Args.of(raw, "stream_id", "topic_id", "group_id");
                yield call.await(client.consumerGroups().getConsumerGroup(streamId(args), topicId(args), groupId(args)))
                        .<JsonNode>map(Canonical::consumerGroup)
                        .orElse(null);
            }
            case "delete_consumer_group" -> {
                Args args = Args.of(raw, "stream_id", "topic_id", "group_id");
                call.await(client.consumerGroups().deleteConsumerGroup(streamId(args), topicId(args), groupId(args)));
                yield null;
            }
            case "build_reader" ->
                buildReader(
                        call,
                        client,
                        Args.of(
                                raw,
                                "stream_id",
                                "topic_id",
                                "consumer",
                                "partitions",
                                "start",
                                "max_wait_ms",
                                "commit"),
                        saveAs);
            case "build_consumer" ->
                buildConsumer(
                        call,
                        client,
                        Args.of(raw, "stream_id", "topic_id", "group", "max_wait_ms", "count", "commit"),
                        saveAs);
            case "resolve_operation" -> {
                Args args = Args.of(raw, "operation_id", "deadline_ms");
                Duration budget = args.milliseconds("deadline_ms").orElse(DEFAULT_RESOLVE_DEADLINE);
                call.await(client.resolveOperation(args.u64("operation_id"), budget));
                yield null;
            }
            case "abandon_operation" -> {
                Args args = Args.of(raw, "operation_id");
                call.await(client.abandonOperation(args.u64("operation_id")));
                yield null;
            }
            case "send_binary_request" -> {
                Args args = Args.of(raw, "code", "payload_hex");
                yield rawReply(call.await(client.sendBinaryRequest((int) args.u32("code"), args.hex("payload_hex"))));
            }
            case "send_binary_request_to_replica" -> {
                Args args = Args.of(raw, "code", "payload_hex", "replica");
                yield rawReply(call.await(client.sendBinaryRequestToReplica(
                        (int) args.u32("code"), args.hex("payload_hex"), (int) args.u32("replica"))));
            }
            case "describe_options" -> {
                Args args = Args.of(raw, "scope");
                yield Canonical.list(
                        call.await(client.system().describeOptions(scope(args.text("scope")))), Canonical::optionSpec);
            }
            case "create_user" -> {
                Args args = Args.of(raw, "username", "password", "status", "permissions");
                yield Canonical.user(call.await(client.users()
                        .createUser(
                                args.text("username"),
                                args.text("password"),
                                args.status("status"),
                                args.permissions("permissions"))));
            }
            case "get_user" -> {
                Args args = Args.of(raw, "user_id");
                yield call.await(client.users().getUser(userId(args)))
                        .<JsonNode>map(Canonical::user)
                        .orElse(null);
            }
            case "update_user" -> {
                Args args = Args.of(raw, "user_id", "username", "status");
                UserUpdate.Builder update = UserUpdate.builder();
                args.optionalText("username").ifPresent(update::username);
                if (args.has("status")) {
                    update.status(args.status("status"));
                }
                call.await(client.users().updateUser(userId(args), update.build()));
                yield null;
            }
            case "delete_user" -> {
                Args args = Args.of(raw, "user_id");
                call.await(client.users().deleteUser(userId(args)));
                yield null;
            }
            case "get_metadata_changes" -> {
                Args args = Args.of(raw, "start", "groups", "limit");
                MetadataChangesQuery.Builder query = MetadataChangesQuery.builder()
                        .start(args.metadataStart("start"))
                        .groups(args.metadataGroups("groups"));
                args.optionalU32("limit").ifPresent(query::limit);
                yield MetadataView.changes(call.await(client.system().getMetadataChanges(query.build())));
            }
            case "get_connection_info" -> {
                Args.of(raw);
                yield Canonical.object()
                        .put("server_address", client.getConnectionInfo().serverAddress());
            }
            default -> throw new Reply.NotSupported("the method " + method);
        };
    }

    private JsonNode next(Call call, String entity) {
        Guarded<PartitionReader> reader = entities.reader(entity);
        if (reader != null) {
            return locked(
                    reader.lock(),
                    () -> Canonical.polled(call.await(reader.value().next())));
        }
        Guarded<IggyConsumer> consumer = entities.consumer(entity);
        if (consumer != null) {
            return locked(
                    consumer.lock(),
                    () -> Canonical.received(call.await(consumer.value().next())));
        }
        throw new IllegalArgumentException("no reader or consumer " + entity);
    }

    private static JsonNode createStream(Call call, AsyncIggyTcpClient client, Args args) {
        StreamOptions options = args.has("encryption_key_hex")
                ? StreamOptions.builder()
                        .encryptionKey(args.hex("encryption_key_hex"))
                        .build()
                : StreamOptions.empty();
        return Canonical.streamDetails(call.await(client.streams().createStream(args.text("name"), options)));
    }

    private static JsonNode updateStream(Call call, AsyncIggyTcpClient client, Args args) {
        StreamUpdate.Builder update = StreamUpdate.builder();
        args.optionalText("name").ifPresent(update::name);
        args.texts("reset").forEach(update::rawReset);
        call.await(client.streams().updateStream(streamId(args), update.build()));
        return null;
    }

    private static JsonNode createTopic(Call call, AsyncIggyTcpClient client, Args args) {
        long partitionsCount = args.optionalU32("partitions_count")
                .orElseThrow(() -> new Reply.NotSupported(
                        "create_topic without partitions_count: the Java SDK has no default partition count"));
        TopicOptions.Builder options = TopicOptions.builder();
        args.expiry("message_expiry").ifPresent(options::messageExpiry);
        args.topicSize("max_topic_size").ifPresent(options::maxTopicSize);
        return Canonical.topicDetails(call.await(
                client.topics().createTopic(streamId(args), args.text("name"), partitionsCount, options.build())));
    }

    private static JsonNode updateTopic(Call call, AsyncIggyTcpClient client, Args args) {
        TopicUpdate.Builder update = TopicUpdate.builder();
        args.optionalText("name").ifPresent(update::name);
        args.expiry("message_expiry").ifPresent(update::messageExpiry);
        args.topicSize("max_topic_size").ifPresent(update::maxTopicSize);
        args.texts("reset").forEach(update::rawReset);
        call.await(client.topics().updateTopic(streamId(args), topicId(args), update.build()));
        return null;
    }

    private static JsonNode pollMessages(Call call, AsyncIggyTcpClient client, Args args) {
        PollOptions options =
                PollOptions.defaults().withCount(args.u32("count")).withCommit(args.commit("commit"));
        options = args.milliseconds("max_wait_ms").map(options::withMaxWait).orElse(options);
        options = args.optionalU32("min_count").map(options::withMinCount).orElse(options);
        return Canonical.polled(call.await(client.messages()
                .pollMessages(
                        streamId(args),
                        topicId(args),
                        args.u32("partition_id"),
                        args.consumer("consumer"),
                        args.strategy("strategy"),
                        options)));
    }

    private JsonNode buildReader(Call call, AsyncIggyTcpClient client, Args args, String saveAs) {
        String entity = requireSaveAs("build_reader", saveAs);
        PartitionReaderBuilder builder = client.reader(streamId(args), topicId(args), args.consumer("consumer"));
        args.partitionIds("partitions").ifPresent(builder::partitions);
        if (args.has("start")) {
            builder.start(args.strategy("start"));
        }
        entities.keepReader(
                entity, call.await(builder.options(readerOptions(args)).build()));
        return null;
    }

    private JsonNode buildConsumer(Call call, AsyncIggyTcpClient client, Args args, String saveAs) {
        String entity = requireSaveAs("build_consumer", saveAs);
        PollOptions options = readerOptions(args);
        options = args.optionalU32("count").map(options::withCount).orElse(options);
        Consumer group = Consumer.group(ConsumerId.of(args.text("group")));
        PartitionReaderBuilder builder =
                client.reader(streamId(args), topicId(args), group).options(options);
        entities.keepConsumer(entity, call.await(IggyConsumer.build(builder)));
        return null;
    }

    /** The options of a reader, with the wait of the SDK unless the call names one. */
    private static PollOptions readerOptions(Args args) {
        Duration maxWait = args.milliseconds("max_wait_ms").orElse(PartitionReaderBuilder.DEFAULT_MAX_WAIT);
        return PollOptions.waitFor(maxWait).withCommit(args.commit("commit"));
    }

    private static JsonNode noArgsCall(JsonNode raw, Runnable call) {
        Args.of(raw);
        call.run();
        return null;
    }

    private static <T> T locked(ReentrantLock lock, Supplier<T> action) {
        lock.lock();
        try {
            return action.get();
        } finally {
            lock.unlock();
        }
    }

    private static ObjectNode rawReply(byte[] payload) {
        return Canonical.object().put("payload_hex", HexFormat.of().formatHex(payload));
    }

    private static String requireSaveAs(String method, String saveAs) {
        if (saveAs == null || saveAs.isEmpty()) {
            throw new IllegalArgumentException(method + " needs save_as");
        }
        return saveAs;
    }

    private static OptionsScope scope(String scope) {
        return switch (scope) {
            case "stream" -> OptionsScope.Stream;
            case "topic" -> OptionsScope.Topic;
            case "user" -> OptionsScope.User;
            default -> throw new IllegalArgumentException("unknown options scope " + scope);
        };
    }

    private static StreamId streamId(Args args) {
        return args.id("stream_id", StreamId::of, StreamId::of);
    }

    private static TopicId topicId(Args args) {
        return args.id("topic_id", TopicId::of, TopicId::of);
    }

    private static ConsumerId groupId(Args args) {
        return args.id("group_id", ConsumerId::of, ConsumerId::of);
    }

    private static UserId userId(Args args) {
        return args.id("user_id", UserId::of, UserId::of);
    }
}
