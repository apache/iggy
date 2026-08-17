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
import org.apache.iggy.message.HeaderKey;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.MessageHeader;
import org.apache.iggy.message.MessageId;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.message.UuidMessageId;
import org.apache.iggy.user.GlobalPermissions;
import org.apache.iggy.user.Permissions;
import org.apache.iggy.user.StreamPermissions;
import org.apache.iggy.user.TopicPermissions;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Unified serializer for both blocking and async clients.
 * Provides serialization of domain objects to ByteBuf according to Iggy wire protocol.
 */
public final class BytesSerializer {

    /** Size of the batch header on the wire; bytes past the stamped fields stay zero. */
    static final int BATCH_HEADER_SIZE = 256;

    /**
     * Key and value length bound, in encoded bytes rather than characters. Belongs to the
     * header-field codec that both user headers and resource options ride, so the server refuses a
     * block carrying a field outside this range.
     */
    private static final int MAX_HEADER_FIELD_LENGTH = 255;

    /** The timestamp delta is a u32 microsecond offset from the batch origin timestamp. */
    private static final BigInteger MAX_TIMESTAMP_DELTA_MICROS = BigInteger.valueOf(0xFFFF_FFFFL);

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
        if (identifier.getKind() == 1) {
            ByteBuf buffer = Unpooled.buffer(6);
            buffer.writeByte(1);
            buffer.writeByte(4);
            buffer.writeIntLE(identifier.getId().intValue());
            return buffer;
        } else if (identifier.getKind() == 2) {
            ByteBuf buffer = Unpooled.buffer(2 + identifier.getName().length());
            buffer.writeByte(2);
            buffer.writeByte(identifier.getName().length());
            buffer.writeBytes(identifier.getName().getBytes());
            return buffer;
        } else {
            throw new IggyInvalidArgumentException("Unknown identifier kind: " + identifier.getKind());
        }
    }

    public static ByteBuf toBytes(Partitioning partitioning) {
        ByteBuf buffer = Unpooled.buffer(2 + partitioning.value().length);
        buffer.writeByte(partitioning.kind().asCode());
        buffer.writeByte(partitioning.value().length);
        buffer.writeBytes(partitioning.value());
        return buffer;
    }

    /**
     * Encodes messages as one batch record: a batch header followed by per-message frames.
     * The server stamps {@code partition_id}, {@code base_offset}, and {@code base_timestamp},
     * so they are encoded as zero here.
     */
    public static ByteBuf toMessagesBatch(List<Message> messages) {
        if (messages.isEmpty()) {
            throw new IggyInvalidArgumentException("Cannot encode an empty message batch");
        }
        List<RawMessage> rawMessages = new ArrayList<>(messages.size());
        for (Message message : messages) {
            rawMessages.add(new RawMessage(
                    encodedMessageId(message.header().id()),
                    message.header().originTimestamp(),
                    message.payload(),
                    readAllBytes(toBytes(message.userHeaders()))));
        }
        return encodeBatch(rawMessages);
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
            checkFieldLength(key.value().length, "key '" + key + "'");
            buffer.writeByte(key.kind().asCode());
            buffer.writeIntLE(key.value().length);
            buffer.writeBytes(key.value());

            HeaderValue value = entry.getValue();
            checkFieldLength(value.value().length, "value for key '" + key + "'");
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

    public static ByteBuf toBytes(String value) {
        int bufferLength = 1 + value.length();
        ByteBuf buffer = Unpooled.buffer(bufferLength);
        byte[] stringBytes = value.getBytes(StandardCharsets.UTF_8);
        buffer.writeByte(stringBytes.length);
        buffer.writeBytes(stringBytes);
        return buffer;
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

    static ByteBuf encodeBatch(List<RawMessage> messages) {
        var batchOriginTimestamp = messages.stream()
                .map(RawMessage::originTimestamp)
                .min(BigInteger::compareTo)
                .orElseThrow(() -> new IggyInvalidArgumentException("Cannot encode an empty message batch"));
        var blobLength = 0;
        for (RawMessage message : messages) {
            blobLength += MessageHeader.SIZE + message.payload().length + message.userHeaders().length;
        }

        var batch = Unpooled.buffer(BATCH_HEADER_SIZE + blobLength);
        batch.writeZero(BATCH_HEADER_SIZE);
        for (int index = 0; index < messages.size(); index++) {
            RawMessage message = messages.get(index);
            var timestampDelta = message.originTimestamp().subtract(batchOriginTimestamp);
            if (timestampDelta.compareTo(MAX_TIMESTAMP_DELTA_MICROS) > 0) {
                throw new IggyInvalidArgumentException("Message origin timestamp exceeds the batch origin by "
                        + timestampDelta + " microseconds, more than the timestamp delta field can hold");
            }
            var frameStart = batch.writerIndex();
            batch.writeLongLE(0); // checksum, backpatched below
            batch.writeBytes(message.id());
            batch.writeIntLE(index); // offset_delta
            batch.writeIntLE(timestampDelta.intValue());
            batch.writeIntLE(message.userHeaders().length);
            batch.writeIntLE(message.payload().length);
            batch.writeLongLE(0); // reserved
            batch.writeBytes(message.payload());
            batch.writeBytes(message.userHeaders());
            batch.setLongLE(
                    frameStart, xxHash3(batch, frameStart + Long.BYTES, batch.writerIndex() - frameStart - Long.BYTES));
        }

        long batchLength = BATCH_HEADER_SIZE + blobLength;
        batch.setBytes(24, toBytesAsU64(batchOriginTimestamp));
        batch.setLongLE(32, batchLength);
        batch.setLongLE(40, batchChecksum(batch, batchOriginTimestamp, batchLength, messages));
        batch.setIntLE(48, messages.size());
        return batch;
    }

    /**
     * The batch checksum covers the header meta fields and each frame's checksum field, not the
     * message bodies; bodies are bound through the per-frame checksums.
     */
    private static long batchChecksum(
            ByteBuf batch, BigInteger batchOriginTimestamp, long batchLength, List<RawMessage> messages) {
        var input = Unpooled.buffer(BATCH_CHECKSUM_FIXED_INPUT_BYTES + Long.BYTES * messages.size());
        input.writeLongLE(0); // partition_id
        input.writeLongLE(0); // base_offset
        input.writeLongLE(0); // base_timestamp
        input.writeBytes(toBytesAsU64(batchOriginTimestamp));
        input.writeLongLE(batchLength);
        input.writeIntLE(messages.size());
        var frameStart = BATCH_HEADER_SIZE;
        for (RawMessage message : messages) {
            input.writeLongLE(batch.getLongLE(frameStart));
            frameStart += MessageHeader.SIZE + message.payload().length + message.userHeaders().length;
        }
        return xxHash3(input, 0, input.readableBytes());
    }

    /**
     * The frame checksum covers the id, so a zero id is minted client-side before encoding
     * rather than assigned by the server.
     */
    private static byte[] encodedMessageId(MessageId id) {
        if (id.toBigInteger().signum() == 0) {
            return readAllBytes(new UuidMessageId(UUID.randomUUID()).toBytes());
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

    private static byte[] readAllBytes(ByteBuf buffer) {
        var bytes = new byte[buffer.readableBytes()];
        buffer.readBytes(bytes);
        return bytes;
    }

    /**
     * Rejects a key or value the TLV codec cannot express.
     *
     * <p>The {@code HeaderKey} / {@code HeaderValue} factories bound what they build, but both are
     * records whose canonical constructor is public and unchecked. A field out of range would
     * encode here and come back as a generic server error naming neither the key nor the bound it
     * broke.
     */
    private static void checkFieldLength(int length, String field) {
        if (length < 1 || length > MAX_HEADER_FIELD_LENGTH) {
            throw new IggyInvalidArgumentException("Invalid header " + field + " length: " + length
                    + " bytes, must be between 1 and " + MAX_HEADER_FIELD_LENGTH);
        }
    }
}
