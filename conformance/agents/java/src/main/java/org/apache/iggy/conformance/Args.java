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

import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.message.AutoCommit;
import org.apache.iggy.message.CommitAfter;
import org.apache.iggy.message.CommitPolicy;
import org.apache.iggy.message.HeaderKey;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.MessageHeader;
import org.apache.iggy.message.MessageId;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.system.MetadataChangeCursor;
import org.apache.iggy.system.MetadataChangeGroup;
import org.apache.iggy.system.MetadataChangeStart;
import org.apache.iggy.system.MetadataCurrentPageToken;
import org.apache.iggy.user.GlobalPermissions;
import org.apache.iggy.user.Permissions;
import org.apache.iggy.user.UserStatus;
import tools.jackson.databind.JsonNode;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * The arguments of one call. It refuses a field that the agent does not know, so that a new argument shows up as a
 * failure instead of being ignored. The forms are those of core/conformance/src/api.rs.
 */
final class Args {

    /** The consumer of a call without one. The Java SDK has no default, so this is the default of Rust and Go. */
    private static final Consumer DEFAULT_CONSUMER = Consumer.of(0L);

    /** Never expires, and no size cap: the largest unsigned 64-bit value. */
    private static final BigInteger UNSIGNED_64_MAX =
            BigInteger.ONE.shiftLeft(Long.SIZE).subtract(BigInteger.ONE);

    private final JsonNode node;

    private Args(JsonNode node) {
        this.node = node;
    }

    static Args of(JsonNode raw, String... fields) {
        JsonNode node = raw == null || raw.isNull() ? Canonical.object() : raw;
        if (!node.isObject()) {
            throw new IllegalArgumentException("the arguments are not an object: " + node);
        }
        Set<String> known = Set.of(fields);
        for (String name : node.propertyNames()) {
            if (!known.contains(name)) {
                throw new IllegalArgumentException("unknown argument " + name);
            }
        }
        return new Args(node);
    }

    /** The arguments of an object field, such as the login of a client, or empty when it is absent or null. */
    Optional<Args> object(String field, String... fields) {
        return has(field) ? Optional.of(Args.of(required(field), fields)) : Optional.empty();
    }

    boolean has(String field) {
        JsonNode value = node.get(field);
        return value != null && !value.isNull();
    }

    String text(String field) {
        return text(required(field), field);
    }

    Optional<String> optionalText(String field) {
        return has(field) ? Optional.of(text(field)) : Optional.empty();
    }

    long u32(String field) {
        return unsigned(required(field), Integer.SIZE, field).longValueExact();
    }

    Optional<Long> optionalU32(String field) {
        return has(field) ? Optional.of(u32(field)) : Optional.empty();
    }

    long u64(String field) {
        return unsigned(required(field), Long.SIZE, field).longValue();
    }

    Optional<Duration> milliseconds(String field) {
        return has(field) ? Optional.of(Duration.ofMillis(u64(field))) : Optional.empty();
    }

    byte[] hex(String field) {
        return has(field) ? HexFormat.of().parseHex(text(field)) : new byte[0];
    }

    Optional<List<Long>> partitionIds(String field) {
        if (!has(field)) {
            return Optional.empty();
        }
        List<Long> ids = new ArrayList<>();
        required(field)
                .forEach(value -> ids.add(unsigned(value, Integer.SIZE, field).longValueExact()));
        return Optional.of(ids);
    }

    List<String> texts(String field) {
        List<String> texts = new ArrayList<>();
        if (has(field)) {
            required(field).forEach(value -> texts.add(text(value, field)));
        }
        return texts;
    }

    /** An identifier: a number for an id, text for a name. */
    <T> T id(String field, Function<Long, T> numeric, Function<String, T> named) {
        return id(required(field), field, numeric, named);
    }

    /** {@code first}, {@code last}, {@code next}, {@code {offset: N}} or {@code {timestamp: microseconds}}. */
    PollingStrategy strategy(String field) {
        Keyed keyed = keyed(field);
        return switch (keyed.key()) {
            case "first" -> PollingStrategy.first();
            case "last" -> PollingStrategy.last();
            case "next" -> PollingStrategy.next();
            case "offset" -> PollingStrategy.offset(unsigned(keyed.value(), Long.SIZE, field));
            case "timestamp" -> PollingStrategy.timestamp(unsigned(keyed.value(), Long.SIZE, field));
            default -> throw new IllegalArgumentException("unknown strategy " + keyed.key());
        };
    }

    /** {@code balanced}, {@code {partition_id: N}} or {@code {messages_key: text}}. */
    Partitioning partitioning(String field) {
        Keyed keyed = keyed(field);
        return switch (keyed.key()) {
            case "balanced" -> Partitioning.balanced();
            case "partition_id" ->
                Partitioning.partitionId(
                        unsigned(keyed.value(), Integer.SIZE, field).longValueExact());
            case "messages_key" -> Partitioning.messagesKey(text(keyed.value(), field));
            default -> throw new IllegalArgumentException("unknown partitioning " + keyed.key());
        };
    }

    /** {@code {consumer: id}} or {@code {group: id}}, and {@link #DEFAULT_CONSUMER} without one. */
    Consumer consumer(String field) {
        if (!has(field)) {
            return DEFAULT_CONSUMER;
        }
        Keyed keyed = keyed(field);
        ConsumerId consumerId = id(keyed.value(), field, ConsumerId::of, ConsumerId::of);
        return switch (keyed.key()) {
            case "consumer" -> Consumer.of(consumerId);
            case "group" -> Consumer.group(consumerId);
            default -> throw new IllegalArgumentException("unknown consumer kind " + keyed.key());
        };
    }

    /** {@code manual}, the default, {@code on_read} or {@code after_processing}. */
    CommitPolicy commit(String field) {
        String commit = optionalText(field).orElse("manual");
        return switch (commit) {
            case "manual" -> CommitPolicy.manual();
            case "on_read" -> CommitPolicy.onRead();
            case "after_processing" -> CommitPolicy.auto(AutoCommit.defaults().withAfter(CommitAfter.PROCESSING));
            default -> throw new IllegalArgumentException("unknown commit policy " + commit);
        };
    }

    /**
     * {@code never}, {@code server_default} or microseconds, in the form of the SDK: the largest unsigned 64-bit
     * value means never, and zero leaves the server default.
     */
    Optional<BigInteger> expiry(String field) {
        return keywordOrNumber(field, "never", "server_default");
    }

    /** {@code unlimited}, {@code server_default} or bytes, in the form of the SDK, as for {@link #expiry}. */
    Optional<BigInteger> topicSize(String field) {
        return keywordOrNumber(field, "unlimited", "server_default");
    }

    UserStatus status(String field) {
        String status = optionalText(field).orElse("active");
        return switch (status) {
            case "active" -> UserStatus.Active;
            case "inactive" -> UserStatus.Inactive;
            default -> throw new IllegalArgumentException("unknown user status " + status);
        };
    }

    /** Global permissions only, each flag false unless set. Without them the user has no permissions. */
    Optional<Permissions> permissions(String field) {
        if (!has(field)) {
            return Optional.empty();
        }
        Args permissions = Args.of(required(field), "global");
        Args global = Args.of(
                permissions.has("global") ? permissions.required("global") : null,
                "manage_servers",
                "read_servers",
                "manage_users",
                "read_users",
                "manage_streams",
                "read_streams",
                "manage_topics",
                "read_topics",
                "poll_messages",
                "send_messages");
        return Optional.of(new Permissions(
                new GlobalPermissions(
                        global.flag("manage_servers"),
                        global.flag("read_servers"),
                        global.flag("manage_users"),
                        global.flag("read_users"),
                        global.flag("manage_streams"),
                        global.flag("read_streams"),
                        global.flag("manage_topics"),
                        global.flag("read_topics"),
                        global.flag("poll_messages"),
                        global.flag("send_messages")),
                Map.of()));
    }

    /** {@code earliest}, {@code latest}, {@code {after: cursor}} or {@code {current: page token or null}}. */
    MetadataChangeStart metadataStart(String field) {
        Keyed keyed = keyed(field);
        return switch (keyed.key()) {
            case "earliest" -> MetadataChangeStart.earliest();
            case "latest" -> MetadataChangeStart.latest();
            case "after" -> MetadataChangeStart.after(MetadataChangeCursor.parse(text(keyed.value(), field)));
            case "current" ->
                keyed.value() == null || keyed.value().isNull()
                        ? MetadataChangeStart.current()
                        : MetadataChangeStart.current(MetadataCurrentPageToken.parse(text(keyed.value(), field)));
            default -> throw new IllegalArgumentException("unknown metadata start " + keyed.key());
        };
    }

    List<MetadataChangeGroup> metadataGroups(String field) {
        List<MetadataChangeGroup> groups = new ArrayList<>();
        for (String name : texts(field)) {
            groups.add(MetadataChangeGroup.fromValue(name)
                    .orElseThrow(() -> new IllegalArgumentException("unknown metadata change group " + name)));
        }
        return groups;
    }

    /** A list of {@code {id, payload}} or {@code {id, payload_hex}}, with optional headers. */
    List<Message> messages(String field) {
        List<Message> messages = new ArrayList<>();
        required(field)
                .forEach(value -> messages.add(Args.of(value, "id", "payload", "payload_hex", "headers")
                        .message()));
        return messages;
    }

    private Message message() {
        if (has("payload") == has("payload_hex")) {
            throw new IllegalArgumentException("a message needs exactly one of payload and payload_hex");
        }
        byte[] payload = has("payload") ? text("payload").getBytes(StandardCharsets.UTF_8) : hex("payload_hex");
        // Id 0 lets the SDK pick the id.
        BigInteger id = has("id") ? u128(text("id")) : BigInteger.ZERO;
        MessageHeader header = new MessageHeader(
                BigInteger.ZERO,
                MessageId.from(id),
                BigInteger.ZERO,
                BigInteger.ZERO,
                BigInteger.ZERO,
                0L,
                (long) payload.length,
                BigInteger.ZERO);
        Message message = new Message(header, payload, Map.of());
        Map<HeaderKey, HeaderValue> headers = headers("headers");
        return headers.isEmpty() ? message : message.withUserHeaders(headers);
    }

    private Map<HeaderKey, HeaderValue> headers(String field) {
        Map<HeaderKey, HeaderValue> headers = new LinkedHashMap<>();
        if (has(field)) {
            for (Map.Entry<String, JsonNode> header : required(field).properties()) {
                Args entry = Args.of(header.getValue(), "kind", "value");
                headers.put(
                        HeaderKey.fromString(header.getKey()),
                        ValueText.fromText(entry.text("kind"), entry.text("value")));
            }
        }
        return headers;
    }

    private boolean flag(String field) {
        if (!has(field)) {
            return false;
        }
        JsonNode value = required(field);
        if (!value.isBoolean()) {
            throw new IllegalArgumentException(field + " is not a boolean");
        }
        return value.booleanValue();
    }

    private Optional<BigInteger> keywordOrNumber(String field, String largest, String zero) {
        if (!has(field)) {
            return Optional.empty();
        }
        JsonNode value = required(field);
        if (!value.isString()) {
            return Optional.of(unsigned(value, Long.SIZE, field));
        }
        if (value.stringValue().equals(largest)) {
            return Optional.of(UNSIGNED_64_MAX);
        }
        if (value.stringValue().equals(zero)) {
            return Optional.of(BigInteger.ZERO);
        }
        throw new IllegalArgumentException("unknown " + field + " " + value.stringValue());
    }

    /** A bare keyword such as {@code first}, or a one-key object such as {@code {offset: 3}}. */
    private Keyed keyed(String field) {
        JsonNode value = required(field);
        if (value.isString()) {
            return new Keyed(value.stringValue(), null);
        }
        if (!value.isObject() || value.size() != 1) {
            throw new IllegalArgumentException(field + " is not a keyword or a one-key object: " + value);
        }
        Map.Entry<String, JsonNode> entry = value.properties().iterator().next();
        return new Keyed(entry.getKey(), entry.getValue());
    }

    private JsonNode required(String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            throw new IllegalArgumentException("missing argument " + field);
        }
        return value;
    }

    private static <T> T id(JsonNode value, String field, Function<Long, T> numeric, Function<String, T> named) {
        return value != null && value.isString()
                ? named.apply(value.stringValue())
                : numeric.apply(unsigned(value, Integer.SIZE, field).longValueExact());
    }

    private static String text(JsonNode value, String field) {
        if (value == null || !value.isString()) {
            throw new IllegalArgumentException(field + " is not text: " + value);
        }
        return value.stringValue();
    }

    private static BigInteger unsigned(JsonNode value, int bits, String field) {
        if (value == null || !value.isIntegralNumber()) {
            throw new IllegalArgumentException(field + " is not an integer: " + value);
        }
        BigInteger number = value.bigIntegerValue();
        if (number.signum() < 0 || number.bitLength() > bits) {
            throw new IllegalArgumentException(field + " is not an unsigned " + bits + "-bit integer: " + number);
        }
        return number;
    }

    private static BigInteger u128(String text) {
        BigInteger id = new BigInteger(text);
        if (id.signum() < 0 || id.bitLength() > 128) {
            throw new IllegalArgumentException(text + " is not a u128 message id");
        }
        return id;
    }

    private record Keyed(String key, JsonNode value) {}
}
