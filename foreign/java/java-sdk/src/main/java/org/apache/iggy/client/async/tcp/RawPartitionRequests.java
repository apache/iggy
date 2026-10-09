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

package org.apache.iggy.client.async.tcp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.message.PartitioningKind;
import org.apache.iggy.serde.CommandCode;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Stamps raw partition commands with the partition context the server fences
 * them by, as Rust's raw API does: a send to an explicit partition shares the
 * typed sends' context cache, and polls and offset writes take the routed path
 * of a typed poll or offset write. Other commands go out as they are.
 */
final class RawPartitionRequests {
    private static final int SEND_MESSAGES = CommandCode.Messages.SEND.getValue();
    private static final int POLL_MESSAGES = CommandCode.Messages.POLL.getValue();
    private static final int STORE_CONSUMER_OFFSET = CommandCode.ConsumerOffset.STORE.getValue();
    private static final int DELETE_CONSUMER_OFFSET = CommandCode.ConsumerOffset.DELETE.getValue();
    private static final int SEND_METADATA_LENGTH_BYTES = Integer.BYTES;
    // An identifier and a partitioning both lead with a kind byte and a length byte.
    private static final int KIND_AND_LENGTH_BYTES = 2;
    private static final int NUMERIC_IDENTIFIER_KIND = 1;
    private static final int STRING_IDENTIFIER_KIND = 2;
    private static final int PARTITION_ID_BYTES = Integer.BYTES;
    private static final int OPTIONAL_PARTITION_BYTES = 1 + PARTITION_ID_BYTES;
    private static final int STORE_OFFSET_SUFFIX_BYTES = Long.BYTES + 1;
    private static final int DELETE_OFFSET_SUFFIX_BYTES = 1;
    private static final String UNPARTITIONED_SEND = "A raw SendMessages request needs an explicit partition id."
            + " Send balanced or messages-key batches with MessagesClient.sendMessages.";

    private final Supplier<AsyncTcpConnection> connection;
    private final PartitionContexts sendContexts;
    private final MessagesTcpClient messages;
    private final ConsumerOffsetsTcpClient offsets;

    RawPartitionRequests(
            Supplier<AsyncTcpConnection> connection,
            PartitionContexts sendContexts,
            MessagesTcpClient messages,
            ConsumerOffsetsTcpClient offsets) {
        this.connection = connection;
        this.sendContexts = sendContexts;
        this.messages = messages;
        this.offsets = offsets;
    }

    /**
     * Fails a partition command whose payload does not follow the layout the
     * typed API writes, and a send without an explicit partition, before
     * anything is sent: either would otherwise go out with no context.
     */
    CompletableFuture<ByteBuf> send(int code, byte[] payload) {
        try {
            if (code == SEND_MESSAGES) {
                return sendMessages(payload);
            }
            if (code == POLL_MESSAGES) {
                return pollMessages(payload);
            }
            if (code == STORE_CONSUMER_OFFSET || code == DELETE_CONSUMER_OFFSET) {
                return writeOffset(code, payload);
            }
        } catch (IggyInvalidArgumentException | IggyServerException refused) {
            return CompletableFuture.failedFuture(refused);
        }
        return connection.get().send(code, Unpooled.copiedBuffer(payload));
    }

    private CompletableFuture<ByteBuf> sendMessages(byte[] payload) {
        int streamStart = SEND_METADATA_LENGTH_BYTES;
        int partitioning = identifierEnd(payload, identifierEnd(payload, streamStart, SEND_MESSAGES), SEND_MESSAGES);
        int kind = unsignedByteAt(payload, partitioning, SEND_MESSAGES, "partitioning");
        if (kind == PartitioningKind.Balanced.asCode() || kind == PartitioningKind.MessagesKey.asCode()) {
            throw IggyServerException.fromTcpResponse(
                    IggyErrorCode.FEATURE_UNAVAILABLE.getCode(), UNPARTITIONED_SEND.getBytes(StandardCharsets.UTF_8));
        }
        int partitionId = partitioning + KIND_AND_LENGTH_BYTES;
        if (kind != PartitioningKind.PartitionId.asCode()
                || unsignedByteAt(payload, partitioning + 1, SEND_MESSAGES, "partitioning") != PARTITION_ID_BYTES
                || partitionId + PARTITION_ID_BYTES > payload.length) {
            throw malformed(SEND_MESSAGES, "partitioning", partitioning);
        }
        ByteBuf discovery = Unpooled.buffer(partitioning - streamStart + PARTITION_ID_BYTES)
                .writeBytes(payload, streamStart, partitioning - streamStart)
                .writeBytes(payload, partitionId, PARTITION_ID_BYTES);
        return sendContexts.send(SEND_MESSAGES, Unpooled.copiedBuffer(payload), discovery, discovery.readableBytes());
    }

    private CompletableFuture<ByteBuf> pollMessages(byte[] payload) {
        int parameters = targetEnd(payload, POLL_MESSAGES);
        if (payload.length - parameters != PollRouter.POLL_PARAMETERS_BYTES) {
            throw malformed(POLL_MESSAGES, "polling parameters", parameters);
        }
        return messages.sendPoll(Unpooled.copiedBuffer(payload), Optional.empty());
    }

    private CompletableFuture<ByteBuf> writeOffset(int code, byte[] payload) {
        int target = targetEnd(payload, code);
        int suffix = code == STORE_CONSUMER_OFFSET ? STORE_OFFSET_SUFFIX_BYTES : DELETE_OFFSET_SUFFIX_BYTES;
        if (payload.length - target != suffix) {
            throw malformed(code, "offset fields", target);
        }
        return offsets.write(code, Unpooled.copiedBuffer(payload), target, Optional.empty());
    }

    /** Where the consumer, stream, topic and optional partition that lead a poll or offset write end. */
    private static int targetEnd(byte[] payload, int code) {
        int consumerKind = unsignedByteAt(payload, 0, code, "consumer");
        if (consumerKind != Consumer.Kind.Consumer.asCode() && consumerKind != Consumer.Kind.ConsumerGroup.asCode()) {
            throw malformed(code, "consumer", 0);
        }
        int partition = identifierEnd(payload, identifierEnd(payload, identifierEnd(payload, 1, code), code), code);
        if (partition + OPTIONAL_PARTITION_BYTES > payload.length) {
            throw malformed(code, "partition id", partition);
        }
        return partition + OPTIONAL_PARTITION_BYTES;
    }

    private static int identifierEnd(byte[] payload, int start, int code) {
        int kind = unsignedByteAt(payload, start, code, "identifier");
        int length = unsignedByteAt(payload, start + 1, code, "identifier");
        int end = start + KIND_AND_LENGTH_BYTES + length;
        boolean valid = (kind == NUMERIC_IDENTIFIER_KIND && length == Integer.BYTES)
                || (kind == STRING_IDENTIFIER_KIND && length > 0);
        if (!valid || end > payload.length) {
            throw malformed(code, "identifier", start);
        }
        return end;
    }

    private static int unsignedByteAt(byte[] payload, int index, int code, String field) {
        if (index >= payload.length) {
            throw malformed(code, field, index);
        }
        return Byte.toUnsignedInt(payload[index]);
    }

    private static IggyInvalidArgumentException malformed(int code, String field, int index) {
        return new IggyInvalidArgumentException(
                "Raw command " + code + " has no valid " + field + " at byte " + index + " of its payload");
    }
}
