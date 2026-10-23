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

package org.apache.iggy.serde;

import com.dynatrace.hash4j.hashing.Hashing;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.apache.iggy.identifier.Identifier;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.identifier.UserId;
import org.apache.iggy.message.HeaderKey;
import org.apache.iggy.message.HeaderKind;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.MessageHeader;
import org.apache.iggy.message.MessageId;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.stream.StreamUpdate;
import org.apache.iggy.system.OptionsScope;
import org.apache.iggy.topic.TopicUpdate;
import org.apache.iggy.user.GlobalPermissions;
import org.apache.iggy.user.Permissions;
import org.apache.iggy.user.StreamPermissions;
import org.apache.iggy.user.TopicPermissions;
import org.apache.iggy.user.UserStatus;
import org.apache.iggy.user.UserUpdate;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Unified serializer for both blocking and async clients.
 * Provides serialization of domain objects to ByteBuf according to Iggy wire protocol.
 */
public final class BytesSerializer {

    /**
     * Credential bounds the server enforces, in UTF-8 bytes. Checked before the round trip so a bad value
     * fails here instead of as an opaque server error.
     */
    public static final int MIN_USERNAME_LENGTH = 3;

    public static final int MAX_USERNAME_LENGTH = 50;

    public static final int MIN_PASSWORD_LENGTH = 3;

    public static final int MAX_PASSWORD_LENGTH = 100;

    /** Size of the batch header on the wire; bytes past the stamped fields stay zero. */
    static final int BATCH_HEADER_SIZE = 256;

    /**
     * Key and value length bound, in encoded bytes rather than characters. Belongs to the
     * header-field codec that both user headers and resource options ride, so the server refuses a
     * block carrying a field outside this range.
     */
    private static final int MAX_HEADER_FIELD_LENGTH = 255;

    /** Bound on a u8-length-prefixed wire string, in encoded bytes. */
    private static final int MAX_U8_STRING_LENGTH = 255;

    /** The timestamp delta is a u32 microsecond offset from the batch origin timestamp. */
    private static final BigInteger MAX_TIMESTAMP_DELTA_MICROS = BigInteger.valueOf(0xFFFF_FFFFL);

    /** Encoded user headers of a message that carries none. */
    private static final byte[] EMPTY_USER_HEADERS = new byte[0];

    /** Batch checksum input: five u64 header fields plus the u32 message count. */
    private static final int BATCH_CHECKSUM_FIXED_INPUT_BYTES = 5 * Long.BYTES + Integer.BYTES;

    private BytesSerializer() {}

    public static ByteBuf toBytes(Consumer consumer) {
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeByte(consumer.kind().asCode());
        buffer.writeBytes(toBytes(consumer.id()));
        return buffer;
    }

    public static ByteBuf toBytes(Identifier identifier) {
        return identifier.toBytes();
    }

    public static ByteBuf toBytes(Partitioning partitioning) {
        ByteBuf buffer = Unpooled.buffer(2 + partitioning.value().length);
        buffer.writeByte(partitioning.kind().asCode());
        buffer.writeByte(partitioning.value().length);
        buffer.writeBytes(partitioning.value());
        return buffer;
    }

    public static ByteBuf toBytes(PollingStrategy strategy) {
        var buffer = Unpooled.buffer(9);
        buffer.writeByte(strategy.kind().asCode());
        buffer.writeBytes(toBytesAsU64(strategy.value()));
        return buffer;
    }

    public static ByteBuf toBytes(Optional<Long> optionalLong) {
        var buffer = Unpooled.buffer(5);
        if (optionalLong.isPresent()) {
            buffer.writeByte(1);
            buffer.writeIntLE(optionalLong.get().intValue());
        } else {
            buffer.writeByte(0);
            buffer.writeIntLE(0);
        }
        return buffer;
    }

    public static ByteBuf toBytes(Map<HeaderKey, HeaderValue> headers) {
        if (headers.isEmpty()) {
            return Unpooled.EMPTY_BUFFER;
        }
        var buffer = Unpooled.buffer();
        for (Map.Entry<HeaderKey, HeaderValue> entry : headers.entrySet()) {
            HeaderKey key = entry.getKey();
            checkFieldLength(key.value().length, "key", key);
            buffer.writeByte(key.kind().asCode());
            buffer.writeIntLE(key.value().length);
            buffer.writeBytes(key.value());

            HeaderValue value = entry.getValue();
            checkFieldLength(value.value().length, "value for key", key);
            buffer.writeByte(value.kind().asCode());
            buffer.writeIntLE(value.value().length);
            buffer.writeBytes(value.value());
        }
        return buffer;
    }

    public static ByteBuf toBytes(Permissions permissions) {
        var buffer = Unpooled.buffer();
        buffer.writeBytes(toBytes(permissions.global()));
        if (permissions.streams().isEmpty()) {
            buffer.writeByte(0);
        } else {
            for (Map.Entry<Long, StreamPermissions> entry :
                    permissions.streams().entrySet()) {
                buffer.writeByte(1);
                buffer.writeIntLE(entry.getKey().intValue());
                buffer.writeBytes(toBytes(entry.getValue()));
            }
            buffer.writeByte(0);
        }

        return buffer;
    }

    public static ByteBuf toBytes(GlobalPermissions permissions) {
        var buffer = Unpooled.buffer();
        buffer.writeBoolean(permissions.manageServers());
        buffer.writeBoolean(permissions.readServers());
        buffer.writeBoolean(permissions.manageUsers());
        buffer.writeBoolean(permissions.readUsers());
        buffer.writeBoolean(permissions.manageStreams());
        buffer.writeBoolean(permissions.readStreams());
        buffer.writeBoolean(permissions.manageTopics());
        buffer.writeBoolean(permissions.readTopics());
        buffer.writeBoolean(permissions.pollMessages());
        buffer.writeBoolean(permissions.sendMessages());
        return buffer;
    }

    public static ByteBuf toBytes(StreamPermissions permissions) {
        var buffer = Unpooled.buffer();
        buffer.writeBoolean(permissions.manageStream());
        buffer.writeBoolean(permissions.readStream());
        buffer.writeBoolean(permissions.manageTopics());
        buffer.writeBoolean(permissions.readTopics());
        buffer.writeBoolean(permissions.pollMessages());
        buffer.writeBoolean(permissions.sendMessages());

        if (permissions.topics().isEmpty()) {
            buffer.writeByte(0);
        } else {
            for (Map.Entry<Long, TopicPermissions> entry : permissions.topics().entrySet()) {
                buffer.writeByte(1);
                buffer.writeIntLE(entry.getKey().intValue());
                buffer.writeBytes(toBytes(entry.getValue()));
            }
            buffer.writeByte(0);
        }

        return buffer;
    }

    public static ByteBuf toBytes(TopicPermissions permissions) {
        var buffer = Unpooled.buffer();
        buffer.writeBoolean(permissions.manageTopic());
        buffer.writeBoolean(permissions.readTopic());
        buffer.writeBoolean(permissions.pollMessages());
        buffer.writeBoolean(permissions.sendMessages());
        return buffer;
    }

    /** A u8-length-prefixed wire string; {@code field} names it in the error when it does not fit. */
    public static ByteBuf toBytes(String value, String field) {
        return toBytes(value, field, 1, MAX_U8_STRING_LENGTH);
    }

    /**
     * A u8-length-prefixed wire string bounded to {@code [minLength, maxLength]} UTF-8 bytes, for
     * fields the server holds to a tighter range than the prefix allows.
     */
    public static ByteBuf toBytes(String value, String field, int minLength, int maxLength) {
        byte[] stringBytes = value.getBytes(StandardCharsets.UTF_8);
        if (stringBytes.length < minLength || stringBytes.length > maxLength) {
            throw new IggyInvalidArgumentException("Invalid " + field + " length: " + stringBytes.length
                    + " bytes when UTF-8 encoded, must be between " + minLength + " and " + maxLength);
        }
        ByteBuf buffer = Unpooled.buffer(1 + stringBytes.length);
        buffer.writeByte(stringBytes.length);
        buffer.writeBytes(stringBytes);
        return buffer;
    }

    /**
     * An options block: the user-headers TLV with string keys, written in ascending key byte order so one
     * set of options has one encoding. A request carries it to the end of its payload.
     */
    public static ByteBuf toOptionsBlock(Map<String, HeaderValue> options) {
        if (options.isEmpty()) {
            return Unpooled.buffer(0);
        }
        Map<byte[], HeaderValue> sorted = new TreeMap<>(Arrays::compareUnsigned);
        options.forEach((key, value) -> sorted.put(key.getBytes(StandardCharsets.UTF_8), value));
        var buffer = Unpooled.buffer();
        for (Map.Entry<byte[], HeaderValue> entry : sorted.entrySet()) {
            HeaderKey key = new HeaderKey(HeaderKind.String, entry.getKey());
            checkFieldLength(key.value().length, "key", key);
            buffer.writeByte(HeaderKind.String.asCode());
            buffer.writeIntLE(key.value().length);
            buffer.writeBytes(key.value());

            HeaderValue value = entry.getValue();
            checkFieldLength(value.value().length, "value for key", key);
            buffer.writeByte(value.kind().asCode());
            buffer.writeIntLE(value.value().length);
            buffer.writeBytes(value.value());
        }
        return buffer;
    }

    /** {@code CreateStream}: {@code [name][options to end]}. */
    public static ByteBuf toCreateStream(String name, Map<String, HeaderValue> options) {
        var payload = toBytes(name, "name");
        writeAndRelease(payload, toOptionsBlock(options));
        return payload;
    }

    /**
     * {@code CreateTopic}: {@code [stream_id][partitions_count:u32][name][options to end]}.
     * {@code partitions_count} is an argument of the command, never an option: admission consumes it to
     * compute partition assignments.
     */
    public static ByteBuf toCreateTopic(
            StreamId streamId, String name, long partitionsCount, Map<String, HeaderValue> options) {
        var payload = Unpooled.buffer();
        writeAndRelease(payload, toBytes(streamId));
        payload.writeIntLE((int) partitionsCount);
        writeAndRelease(payload, toBytes(name, "name"));
        writeAndRelease(payload, toOptionsBlock(options));
        return payload;
    }

    /**
     * {@code CreateUser}: {@code [username][password][status:u8][has_permissions:u8]
     * [permissions_len:u32 permissions]?[options to end]}.
     */
    public static ByteBuf toCreateUser(
            String username,
            String password,
            UserStatus status,
            Optional<Permissions> permissions,
            Map<String, HeaderValue> options) {
        var payload = Unpooled.buffer();
        writeAndRelease(payload, toBytes(username, "username", MIN_USERNAME_LENGTH, MAX_USERNAME_LENGTH));
        writeAndRelease(payload, toBytes(password, "password", MIN_PASSWORD_LENGTH, MAX_PASSWORD_LENGTH));
        payload.writeByte(status.asCode());
        permissions.ifPresentOrElse(
                granted -> {
                    payload.writeByte(1);
                    var permissionBytes = toBytes(granted);
                    payload.writeIntLE(permissionBytes.readableBytes());
                    writeAndRelease(payload, permissionBytes);
                },
                () -> payload.writeByte(0));
        writeAndRelease(payload, toOptionsBlock(options));
        return payload;
    }

    /** {@code UpdateResource} on a stream. */
    public static ByteBuf toUpdateResource(StreamId streamId, StreamUpdate update) {
        var target = Unpooled.buffer();
        target.writeByte(OptionsScope.Stream.asCode());
        writeAndRelease(target, toBytes(streamId));
        return updateResource(target, update.resetKeys(), update.entries());
    }

    /** {@code UpdateResource} on a topic. */
    public static ByteBuf toUpdateResource(StreamId streamId, TopicId topicId, TopicUpdate update) {
        var target = Unpooled.buffer();
        target.writeByte(OptionsScope.Topic.asCode());
        writeAndRelease(target, toBytes(streamId));
        writeAndRelease(target, toBytes(topicId));
        return updateResource(target, update.resetKeys(), update.entries());
    }

    /** {@code UpdateResource} on a user. */
    public static ByteBuf toUpdateResource(UserId userId, UserUpdate update) {
        var target = Unpooled.buffer();
        target.writeByte(OptionsScope.User.asCode());
        writeAndRelease(target, toBytes(userId));
        return updateResource(target, update.resetKeys(), update.entries());
    }

    public static ByteBuf toBytesAsU64(BigInteger value) {
        if (value.signum() == -1) {
            throw new IggyInvalidArgumentException("Negative value cannot be serialized to unsigned 64: " + value);
        }
        ByteBuf buffer = Unpooled.buffer(8, 8);
        byte[] valueAsBytes = value.toByteArray();
        if (valueAsBytes.length > 9 || (valueAsBytes.length == 9 && valueAsBytes[0] != 0)) {
            throw new IggyInvalidArgumentException("Value too large for U64: " + value);
        }
        ArrayUtils.reverse(valueAsBytes);
        buffer.writeBytes(valueAsBytes, 0, Math.min(8, valueAsBytes.length));
        if (valueAsBytes.length < 8) {
            buffer.writeZero(8 - valueAsBytes.length);
        }
        return buffer;
    }

    public static ByteBuf toBytesAsU128(BigInteger value) {
        if (value.signum() == -1) {
            throw new IggyInvalidArgumentException("Negative value cannot be serialized to unsigned 128: " + value);
        }
        ByteBuf buffer = Unpooled.buffer(16, 16);
        byte[] valueAsBytes = value.toByteArray();
        if (valueAsBytes.length > 17 || (valueAsBytes.length == 17 && valueAsBytes[0] != 0)) {
            throw new IggyInvalidArgumentException("Value too large for U128: " + value);
        }
        ArrayUtils.reverse(valueAsBytes);
        buffer.writeBytes(valueAsBytes, 0, Math.min(16, valueAsBytes.length));
        if (valueAsBytes.length < 16) {
            buffer.writeZero(16 - valueAsBytes.length);
        }
        return buffer;
    }

    /**
     * Encodes messages as one batch record: a batch header followed by per-message frames.
     * The server stamps {@code partition_id}, {@code base_offset}, and {@code base_timestamp},
     * so they are encoded as zero here.
     */
    public static ByteBuf toMessagesBatch(List<Message> messages) {
        var rawMessages = toRawMessages(messages);
        return encodeBatch(rawMessages);
    }

    /**
     * Appends the same batch record to {@code out} at its current writer index. A caller that
     * has already written the bytes preceding the batch encodes it straight into their buffer
     * instead of filling a second one and copying it over, which is the whole payload once per
     * request.
     */
    public static void encodeMessagesBatchInto(ByteBuf out, List<Message> messages) {
        encodeBatchInto(out, toRawMessages(messages));
    }

    private static List<RawMessage> toRawMessages(List<Message> messages) {
        if (messages.isEmpty()) {
            throw new IggyInvalidArgumentException("Cannot encode an empty message batch");
        }
        List<RawMessage> rawMessages = new ArrayList<>(messages.size());
        for (Message message : messages) {
            rawMessages.add(new RawMessage(
                    encodedMessageId(message.header().id()),
                    message.header().originTimestamp(),
                    message.payload(),
                    encodedUserHeaders(message.userHeaders())));
        }
        return rawMessages;
    }

    static ByteBuf encodeBatch(List<RawMessage> messages) {
        var batch = Unpooled.buffer(BATCH_HEADER_SIZE);
        try {
            encodeBatchInto(batch, messages);
            return batch;
        } catch (RuntimeException | Error error) {
            batch.release();
            throw error;
        }
    }

    private static BatchExtent measureBatch(List<RawMessage> messages, long capacityAllowance) {
        var originTimestamp = messages.get(0).originTimestamp();
        var latestTimestamp = originTimestamp;
        var latestIndex = 0;
        long length = BATCH_HEADER_SIZE;
        for (int index = 0; index < messages.size(); index++) {
            RawMessage message = messages.get(index);
            var timestamp = message.originTimestamp();
            if (timestamp.signum() < 0 || timestamp.bitLength() > Long.SIZE) {
                throw new IggyInvalidArgumentException("Message " + index + " origin timestamp " + timestamp
                        + " is outside the unsigned 64-bit range");
            }
            originTimestamp = originTimestamp.min(timestamp);
            if (timestamp.compareTo(latestTimestamp) > 0) {
                latestTimestamp = timestamp;
                latestIndex = index;
            }
            length += (long) MessageHeader.SIZE + message.payload().length + message.userHeaders().length;
            if (length > capacityAllowance) {
                throw new IggyInvalidArgumentException("Message batch exceeds the output buffer capacity");
            }
        }
        // Name the offending message and its delta, the way the server's own
        // InvalidMessageTimestampDelta does: the batch origin is whichever
        // message is oldest, so neither is obvious from the caller's input.
        var delta = latestTimestamp.subtract(originTimestamp);
        if (delta.compareTo(MAX_TIMESTAMP_DELTA_MICROS) > 0) {
            throw new IggyInvalidArgumentException("Message " + latestIndex
                    + " origin timestamp exceeds the batch origin by " + delta
                    + " microseconds, more than the timestamp delta field can hold");
        }
        return new BatchExtent(originTimestamp, length);
    }

    static void encodeBatchInto(ByteBuf out, List<RawMessage> messages) {
        if (messages.isEmpty()) {
            throw new IggyInvalidArgumentException("Cannot encode an empty message batch");
        }
        var batchStart = out.writerIndex();
        var extent = measureBatch(messages, (long) out.maxCapacity() - batchStart);
        var batchOriginTimestamp = extent.originTimestamp();
        var batchLength = extent.length();
        // Size to the exact total before the first batch byte. Letting the
        // writes grow the buffer instead rounds up to the next power of two,
        // which on a batch just over a megabyte reserves two.
        var required = batchStart + (int) batchLength;
        if (out.capacity() < required) {
            out.capacity(required);
        }
        try {
            out.writeZero(BATCH_HEADER_SIZE);
            for (int index = 0; index < messages.size(); index++) {
                RawMessage message = messages.get(index);
                var timestampDelta = message.originTimestamp().subtract(batchOriginTimestamp);
                var frameStart = out.writerIndex();
                out.writeLongLE(0);
                out.writeBytes(message.id());
                out.writeIntLE(index);
                out.writeIntLE(timestampDelta.intValue());
                out.writeIntLE(message.userHeaders().length);
                out.writeIntLE(message.payload().length);
                out.writeLongLE(0);
                out.writeBytes(message.payload());
                out.writeBytes(message.userHeaders());
                out.setLongLE(
                        frameStart, xxHash3(out, frameStart + Long.BYTES, out.writerIndex() - frameStart - Long.BYTES));
            }
            out.setLongLE(batchStart + 24, batchOriginTimestamp.longValue());
            out.setLongLE(batchStart + 32, batchLength);
            out.setLongLE(batchStart + 40, batchChecksum(out, batchStart, batchOriginTimestamp, batchLength, messages));
            out.setIntLE(batchStart + 48, messages.size());
        } catch (RuntimeException | Error error) {
            out.writerIndex(batchStart);
            throw error;
        }
    }

    /**
     * The batch checksum covers the header meta fields and each frame's checksum field, not the
     * message bodies; bodies are bound through the per-frame checksums.
     */
    private static long batchChecksum(
            ByteBuf batch,
            int batchStart,
            BigInteger batchOriginTimestamp,
            long batchLength,
            List<RawMessage> messages) {
        var input = Unpooled.buffer(BATCH_CHECKSUM_FIXED_INPUT_BYTES + Long.BYTES * messages.size());
        try {
            input.writeLongLE(0);
            input.writeLongLE(0);
            input.writeLongLE(0);
            input.writeLongLE(batchOriginTimestamp.longValue());
            input.writeLongLE(batchLength);
            input.writeIntLE(messages.size());
            var frameStart = batchStart + BATCH_HEADER_SIZE;
            for (RawMessage message : messages) {
                input.writeLongLE(batch.getLongLE(frameStart));
                frameStart += MessageHeader.SIZE + message.payload().length + message.userHeaders().length;
            }
            return xxHash3(input, 0, input.readableBytes());
        } finally {
            input.release();
        }
    }

    /**
     * The frame checksum covers the id, so a zero id is minted client-side before encoding
     * rather than assigned by the server.
     */
    private static byte[] encodedMessageId(MessageId id) {
        if (id.toBigInteger().signum() == 0) {
            return MessageIdGenerator.mint();
        }
        return readAllBytes(id.toBytes());
    }

    private static long xxHash3(ByteBuf buffer, int index, int length) {
        if (buffer.hasArray()) {
            return Hashing.xxh3_64().hashBytesToLong(buffer.array(), buffer.arrayOffset() + index, length);
        }
        var bytes = new byte[length];
        buffer.getBytes(index, bytes);
        return Hashing.xxh3_64().hashBytesToLong(bytes);
    }

    /**
     * Encoded user headers, or an empty array when there are none.
     *
     * <p>Returns before a buffer exists for the empty case. {@link #toBytes(Map)} answers that case
     * with the shared {@link Unpooled#EMPTY_BUFFER}, which {@link #readAllBytes(ByteBuf)} would then
     * release. That release happens to be a no-op on the singleton, which is the only reason the
     * previous shape was safe.
     */
    private static byte[] encodedUserHeaders(Map<HeaderKey, HeaderValue> userHeaders) {
        if (userHeaders == null || userHeaders.isEmpty()) {
            return EMPTY_USER_HEADERS;
        }
        return readAllBytes(toBytes(userHeaders));
    }

    /**
     * Completes an {@code UpdateResource} body after its scope and identifiers:
     * {@code [reset_count:u8][reset_key]*[options to end]}.
     */
    private static ByteBuf updateResource(ByteBuf payload, List<String> reset, Map<String, HeaderValue> set) {
        payload.writeByte(reset.size());
        for (String key : reset) {
            writeAndRelease(payload, toBytes(key, "reset key"));
        }
        writeAndRelease(payload, toOptionsBlock(set));
        return payload;
    }

    private static void writeAndRelease(ByteBuf payload, ByteBuf part) {
        try {
            payload.writeBytes(part);
        } finally {
            part.release();
        }
    }

    private static byte[] readAllBytes(ByteBuf buffer) {
        try {
            var bytes = new byte[buffer.readableBytes()];
            buffer.readBytes(bytes);
            return bytes;
        } finally {
            buffer.release();
        }
    }

    /**
     * Rejects a key or value the TLV codec cannot express.
     *
     * <p>The {@code HeaderKey} / {@code HeaderValue} factories bound what they build, but both are
     * records whose canonical constructor is public and unchecked. A field out of range would
     * encode here and come back as a generic server error naming neither the key nor the bound it
     * broke.
     *
     * <p>The message names the key, so it is built only on failure: this runs for every header of
     * every message sent.
     */
    private static void checkFieldLength(int length, String field, HeaderKey key) {
        if (length < 1 || length > MAX_HEADER_FIELD_LENGTH) {
            throw new IggyInvalidArgumentException("Invalid header " + field + " '" + key + "' length: " + length
                    + " bytes, must be between 1 and " + MAX_HEADER_FIELD_LENGTH);
        }
    }

    /**
     * The batch-header values a set of messages implies. Measured before a byte is written so an
     * input the wire cannot carry is refused with the output buffer untouched.
     */
    private record BatchExtent(BigInteger originTimestamp, long length) {}

    /**
     * One message as it enters the batch encoder: the id already encoded to its 16 wire bytes
     * and the user headers already encoded to their opaque bytes.
     */
    record RawMessage(byte[] id, BigInteger originTimestamp, byte[] payload, byte[] userHeaders) {
        RawMessage {
            if (id.length != 16) {
                throw new IggyInvalidArgumentException("Message id must have 16 bytes");
            }
        }
    }
}
