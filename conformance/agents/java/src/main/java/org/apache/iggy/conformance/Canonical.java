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

import org.apache.iggy.client.async.reader.ReceivedMessage;
import org.apache.iggy.consumergroup.ConsumerGroupDetails;
import org.apache.iggy.consumergroup.ConsumerGroupMember;
import org.apache.iggy.message.HeaderKey;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.partition.Partition;
import org.apache.iggy.stream.StreamBase;
import org.apache.iggy.stream.StreamDetails;
import org.apache.iggy.system.OptionSpec;
import org.apache.iggy.topic.Topic;
import org.apache.iggy.topic.TopicDetails;
import org.apache.iggy.user.GlobalPermissions;
import org.apache.iggy.user.UserInfoDetails;
import org.apache.iggy.user.UserStatus;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.NullNode;
import tools.jackson.databind.node.ObjectNode;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * The canonical result forms of core/conformance/src/canonical.rs. The orchestrator decodes each result into its form
 * and refuses any field that the form does not have.
 */
final class Canonical {

    private Canonical() {}

    static ObjectNode object() {
        return JsonNodeFactory.instance.objectNode();
    }

    static ArrayNode array() {
        return JsonNodeFactory.instance.arrayNode();
    }

    static <T> ArrayNode list(List<T> values, Function<T, ObjectNode> form) {
        ArrayNode list = array();
        values.forEach(value -> list.add(form.apply(value)));
        return list;
    }

    static ObjectNode stream(StreamBase stream) {
        return streamFields(
                stream.id(),
                stream.name(),
                stream.createdAt(),
                stream.size(),
                stream.messagesCount(),
                stream.topicsCount(),
                options(stream.options(), stream.derivedOptions()));
    }

    static ObjectNode streamDetails(StreamDetails stream) {
        ObjectNode details = streamFields(
                stream.id(),
                stream.name(),
                stream.createdAt(),
                stream.size(),
                stream.messagesCount(),
                stream.topicsCount(),
                options(stream.options(), stream.derivedOptions()));
        details.set("topics", list(stream.topics(), Canonical::topic));
        return details;
    }

    static ObjectNode topic(Topic topic) {
        return topicFields(
                topic.id(),
                topic.name(),
                topic.createdAt(),
                topic.size(),
                topic.messagesCount(),
                topic.partitionsCount(),
                options(topic.options(), topic.derivedOptions()));
    }

    static ObjectNode topicDetails(TopicDetails topic) {
        ObjectNode details = topicFields(
                topic.id(),
                topic.name(),
                topic.createdAt(),
                topic.size(),
                topic.messagesCount(),
                topic.partitionsCount(),
                options(topic.options(), topic.derivedOptions()));
        details.set("partitions", list(topic.partitions(), Canonical::partition));
        return details;
    }

    static ObjectNode polled(PolledMessages polled) {
        ObjectNode result = object();
        result.put("partition_id", polled.partitionId());
        result.put("current_offset", polled.currentOffset());
        result.put("count", polled.count());
        result.set("messages", list(polled.messages(), Canonical::message));
        return result;
    }

    static ObjectNode received(ReceivedMessage received) {
        ObjectNode result = object();
        result.put("partition_id", received.partitionId());
        result.set("message", message(received.message()));
        return result;
    }

    static ObjectNode consumerGroup(ConsumerGroupDetails group) {
        ObjectNode result = object();
        result.put("id", group.id());
        result.put("name", group.name());
        result.put("partitions_count", group.partitionsCount());
        result.put("members_count", group.membersCount());
        result.set("members", list(group.members(), Canonical::member));
        return result;
    }

    static ObjectNode user(UserInfoDetails user) {
        ObjectNode result = object();
        result.put("id", user.id());
        result.put("username", user.username());
        result.put("status", status(user.status()));
        result.put("created_at", user.createdAt());
        // Format 1 has global permissions only.
        result.set(
                "permissions",
                user.permissions()
                        .<JsonNode>map(granted -> object().set("global", globalPermissions(granted.global())))
                        .orElse(NullNode.getInstance()));
        result.set("options", options(user.options(), user.derivedOptions()));
        return result;
    }

    static ObjectNode optionSpec(OptionSpec spec) {
        ObjectNode result = object();
        result.put("key", spec.key());
        result.put("kind", ValueText.kind(spec.defaultValue().kind()));
        result.put("updatable", spec.updatable());
        result.put("resettable", spec.resettable());
        result.put("secret", spec.secret());
        result.put("read_only", spec.readOnly());
        result.put("critical", spec.critical());
        return result;
    }

    static String status(UserStatus status) {
        return status == UserStatus.Inactive ? "inactive" : "active";
    }

    static ObjectNode globalPermissions(GlobalPermissions global) {
        ObjectNode result = object();
        result.put("manage_servers", global.manageServers());
        result.put("read_servers", global.readServers());
        result.put("manage_users", global.manageUsers());
        result.put("read_users", global.readUsers());
        result.put("manage_streams", global.manageStreams());
        result.put("read_streams", global.readStreams());
        result.put("manage_topics", global.manageTopics());
        result.put("read_topics", global.readTopics());
        result.put("poll_messages", global.pollMessages());
        result.put("send_messages", global.sendMessages());
        return result;
    }

    /**
     * The keys that the client set and the keys that the server derived, in one map with the explicit flag of each.
     */
    static ObjectNode options(Map<String, HeaderValue> explicit, Map<String, HeaderValue> derived) {
        ObjectNode options = object();
        derived.forEach((key, value) -> options.set(key, option(value, false)));
        explicit.forEach((key, value) -> options.set(key, option(value, true)));
        return options;
    }

    private static ObjectNode option(HeaderValue value, boolean explicit) {
        ObjectNode option = object();
        option.put("value", ValueText.text(value));
        option.put("kind", ValueText.kind(value.kind()));
        option.put("explicit", explicit);
        return option;
    }

    private static ObjectNode streamFields(
            long id,
            String name,
            BigInteger createdAt,
            String size,
            BigInteger messagesCount,
            long topicsCount,
            ObjectNode options) {
        ObjectNode stream = object();
        stream.put("id", id);
        stream.put("name", name);
        stream.put("created_at", createdAt);
        stream.put("size_bytes", new BigInteger(size));
        stream.put("messages_count", messagesCount);
        stream.put("topics_count", topicsCount);
        stream.set("options", options);
        return stream;
    }

    private static ObjectNode topicFields(
            long id,
            String name,
            BigInteger createdAt,
            String size,
            BigInteger messagesCount,
            long partitionsCount,
            ObjectNode options) {
        ObjectNode topic = object();
        topic.put("id", id);
        topic.put("name", name);
        topic.put("created_at", createdAt);
        topic.put("size_bytes", new BigInteger(size));
        topic.put("messages_count", messagesCount);
        topic.put("partitions_count", partitionsCount);
        topic.set("options", options);
        return topic;
    }

    private static ObjectNode partition(Partition partition) {
        ObjectNode result = object();
        result.put("id", partition.id());
        result.put("created_at", partition.createdAt());
        result.put("segments_count", partition.segmentsCount());
        result.put("current_offset", partition.currentOffset());
        result.put("size_bytes", new BigInteger(partition.size()));
        result.put("messages_count", partition.messagesCount());
        return result;
    }

    private static ObjectNode message(Message message) {
        ObjectNode result = object();
        result.put("offset", message.header().offset());
        result.put("id", message.header().id().toBigInteger().toString());
        result.put("timestamp", message.header().timestamp());
        result.put("origin_timestamp", message.header().originTimestamp());
        result.put("checksum", message.header().checksum());
        String text = utf8(message.payload());
        if (text != null) {
            result.put("payload", text);
        }
        result.put("payload_hex", HexFormat.of().formatHex(message.payload()));
        if (!message.userHeaders().isEmpty()) {
            ObjectNode headers = result.putObject("headers");
            for (Map.Entry<HeaderKey, HeaderValue> header :
                    message.userHeaders().entrySet()) {
                ObjectNode entry = headers.putObject(new String(header.getKey().value(), StandardCharsets.UTF_8));
                entry.put("kind", ValueText.kind(header.getValue().kind()));
                entry.put("value", ValueText.text(header.getValue()));
            }
        }
        return result;
    }

    private static ObjectNode member(ConsumerGroupMember member) {
        ObjectNode result = object();
        result.put("id", member.id());
        result.put("partitions_count", member.partitionsCount());
        ArrayNode partitions = result.putArray("partitions");
        if (member.partitions() != null) {
            member.partitions().forEach(partitions::add);
        }
        return result;
    }

    /** The payload as text, or null when it is not valid UTF-8. */
    private static String utf8(byte[] payload) {
        try {
            return StandardCharsets.UTF_8
                    .newDecoder()
                    .decode(ByteBuffer.wrap(payload))
                    .toString();
        } catch (CharacterCodingException notText) {
            return null;
        }
    }
}
