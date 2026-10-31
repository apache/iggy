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
import org.apache.iggy.message.DeferredPollOptions;
import org.apache.iggy.message.PartitioningKind;
import org.apache.iggy.message.PollingKind;
import org.apache.iggy.serde.BytesDeserializer;
import org.apache.iggy.serde.CommandCode;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Stamps raw partition commands with the partition context the server fences
 * them by, as Rust's raw API does: a send to an explicit partition shares the
 * typed sends' context cache, and polls, deferred polls and
 * offset writes take the routed path of the typed call. A fetch keeps the
 * fences of its targets and goes to the primary of its first target, as a
 * typed fetch goes. Deferred-request cancels are refused. Other commands go
 * out as they are.
 */
final class RawPartitionRequests {
    private static final int SEND_MESSAGES = CommandCode.Messages.SEND.getValue();
    private static final int POLL_MESSAGES = CommandCode.Messages.POLL.getValue();
    private static final int POLL_MESSAGES_DEFERRED = CommandCode.Messages.POLL_DEFERRED.getValue();
    private static final int FETCH_MESSAGES = CommandCode.Messages.FETCH_MESSAGES.getValue();
    private static final int CANCEL_DEFERRED_REQUEST = CommandCode.Messages.CANCEL_DEFERRED_REQUEST.getValue();
    private static final int STORE_CONSUMER_OFFSET = CommandCode.ConsumerOffset.STORE.getValue();
    private static final int DELETE_CONSUMER_OFFSET = CommandCode.ConsumerOffset.DELETE.getValue();
    // A poll's count and auto-commit flag end its polling parameters.
    private static final int COUNT_AND_AUTO_COMMIT_BYTES = Integer.BYTES + 1;
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
    private final PollRouter pollRouter;
    private final MessagesTcpClient messages;
    private final ConsumerOffsetsTcpClient offsets;

    RawPartitionRequests(
            Supplier<AsyncTcpConnection> connection,
            PollRouter pollRouter,
            MessagesTcpClient messages,
            ConsumerOffsetsTcpClient offsets) {
        this.connection = connection;
        this.pollRouter = pollRouter;
        this.messages = messages;
        this.offsets = offsets;
    }

    /**
     * Fails a partition command whose payload does not follow the layout the
     * typed API writes, and a send without an explicit partition, before
     * anything is sent: either would otherwise go out with no context. A
     * deferred poll with limits that the server's decoder refuses fails the
     * same way.
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    CompletableFuture<ByteBuf> send(int code, byte[] payload) {
        try {
            if (code == SEND_MESSAGES) {
                return sendMessages(payload);
            }
            if (code == POLL_MESSAGES) {
                return pollMessages(payload);
            }
            if (code == POLL_MESSAGES_DEFERRED) {
                return pollMessagesDeferred(payload);
            }
            if (code == FETCH_MESSAGES) {
                return fetchMessages(payload);
            }
            if (code == CANCEL_DEFERRED_REQUEST) {
                // A cancel names a pending request by its id, and the server answers no cancel,
                // so a new raw request could only wait for its deadline.
                throw IggyServerException.fromTcpResponse(IggyErrorCode.INVALID_COMMAND.getCode(), new byte[0]);
            }
            if (code == CommandCode.ConsumerOffset.GET.getValue()) {
                targetEnd(payload, code);
                return pollRouter.partitionRequest(code, Unpooled.copiedBuffer(payload), Optional.empty());
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
        return pollRouter.sendMessages(Unpooled.copiedBuffer(payload), discovery);
    }

    private CompletableFuture<ByteBuf> pollMessages(byte[] payload) {
        int parameters = targetEnd(payload, POLL_MESSAGES);
        if (payload.length - parameters != PollRouter.POLL_PARAMETERS_BYTES) {
            throw malformed(POLL_MESSAGES, "polling parameters", parameters);
        }
        return messages.sendPoll(Unpooled.copiedBuffer(payload), Optional.empty());
    }

    private CompletableFuture<ByteBuf> pollMessagesDeferred(byte[] payload) {
        int parameters = targetEnd(payload, POLL_MESSAGES_DEFERRED);
        int limits = parameters + PollRouter.POLL_PARAMETERS_BYTES;
        if (payload.length - limits != PollRouter.DEFERRED_PARAMETERS_BYTES) {
            throw malformed(POLL_MESSAGES_DEFERRED, "polling parameters", parameters);
        }
        ByteBuf request = Unpooled.copiedBuffer(payload);
        DeferredPollOptions options;
        try {
            options = PollRouter.deferredOptions(request);
            options.validate(request.getUnsignedIntLE(limits - COUNT_AND_AUTO_COMMIT_BYTES));
        } catch (IggyInvalidArgumentException refused) {
            request.release();
            throw refused;
        }
        // As for a typed deferred poll, the request's own budget sets the deadline,
        // and each attempt sends what is left of that budget.
        return pollRouter.partitionRequest(
                POLL_MESSAGES_DEFERRED,
                request,
                Optional.empty(),
                System.nanoTime() + options.requestTimeout().toNanos());
    }

    /** Refuses a fetch that the server would refuse to decode, see FetchMessagesRequest in core/binary_protocol. */
    private CompletableFuture<ByteBuf> fetchMessages(byte[] payload) {
        int topic = consumerEnd(payload, FETCH_MESSAGES);
        int limits = identifierEnd(payload, identifierEnd(payload, topic, FETCH_MESSAGES), FETCH_MESSAGES);
        int targets = limits + FetchMessages.LIMITS_BYTES;
        if (targets > payload.length) {
            throw malformed(FETCH_MESSAGES, "fetch limits", limits);
        }
        ByteBuf fields = Unpooled.wrappedBuffer(payload);
        long count = fields.getUnsignedIntLE(targets - Integer.BYTES);
        if (count == 0
                || count > FetchMessages.MAX_PARTITIONS_PER_REQUEST
                || payload.length - targets != count * FetchMessages.TARGET_BYTES) {
            throw malformed(FETCH_MESSAGES, "targets", targets - Integer.BYTES);
        }
        DeferredPollOptions options = new DeferredPollOptions(
                Duration.of(fields.getLongLE(limits), ChronoUnit.MICROS),
                fields.getUnsignedIntLE(limits + FetchMessages.MIN_COUNT_OFFSET),
                fields.getUnsignedIntLE(limits + FetchMessages.MAX_BYTES_OFFSET),
                Duration.of(fields.getLongLE(limits + Long.BYTES), ChronoUnit.MICROS));
        options.validate(fields.getUnsignedIntLE(limits + FetchMessages.COUNT_OFFSET));
        if (!FetchMessages.canStart(options)) {
            throw malformed(FETCH_MESSAGES, "request timeout", limits + Long.BYTES);
        }
        List<Long> partitions = fetchPartitions(fields, targets, options.maxBytes());
        byte[] topicBytes = Arrays.copyOfRange(payload, topic, limits);
        CompletableFuture<ByteBuf> reply = FetchMessages.send(
                pollRouter,
                Unpooled.copiedBuffer(payload),
                topicBytes,
                partitions,
                System.nanoTime() + options.requestTimeout().toNanos(),
                pollRouter.startCall());
        return AsyncTcpConnection.mapResponse(reply, body -> {
            try {
                FetchMessages.markMoved(
                        pollRouter, topicBytes, BytesDeserializer.readFetchedMessages(body.duplicate()));
            } catch (RuntimeException malformedReply) {
                body.release();
                throw malformedReply;
            }
            return body;
        });
    }

    private CompletableFuture<ByteBuf> writeOffset(int code, byte[] payload) {
        int target = targetEnd(payload, code);
        int suffix = code == STORE_CONSUMER_OFFSET ? STORE_OFFSET_SUFFIX_BYTES : DELETE_OFFSET_SUFFIX_BYTES;
        if (payload.length - target != suffix) {
            throw malformed(code, "offset fields", target);
        }
        return offsets.write(code, Unpooled.copiedBuffer(payload), Optional.empty());
    }

    /** The partitions of the fetch targets that start at {@code targets} and end {@code fields}, in their order. */
    private static List<Long> fetchPartitions(ByteBuf fields, int targets, long maxBytes) {
        Set<Long> partitions = new LinkedHashSet<>();
        for (int target = targets; target < fields.writerIndex(); target += FetchMessages.TARGET_BYTES) {
            int kind = fields.getUnsignedByte(target + FetchMessages.STRATEGY_KIND_OFFSET);
            long targetMaxBytes = fields.getUnsignedIntLE(target + FetchMessages.TARGET_MAX_BYTES_OFFSET);
            if ((kind != PollingKind.Offset.asCode() && kind != PollingKind.Next.asCode())
                    || targetMaxBytes < DeferredPollOptions.MIN_MAX_BYTES
                    || targetMaxBytes > maxBytes) {
                throw malformed(FETCH_MESSAGES, "target", target);
            }
            if (!partitions.add(fields.getUnsignedIntLE(target))) {
                throw malformed(FETCH_MESSAGES, "unique partition id", target);
            }
        }
        return List.copyOf(partitions);
    }

    /** Where the consumer, stream, topic and optional partition that lead a poll or offset write end. */
    private static int targetEnd(byte[] payload, int code) {
        int partition = identifierEnd(payload, identifierEnd(payload, consumerEnd(payload, code), code), code);
        if (partition + OPTIONAL_PARTITION_BYTES > payload.length) {
            throw malformed(code, "partition id", partition);
        }
        return partition + OPTIONAL_PARTITION_BYTES;
    }

    private static int consumerEnd(byte[] payload, int code) {
        int consumerKind = unsignedByteAt(payload, 0, code, "consumer");
        if (consumerKind != Consumer.Kind.Consumer.asCode() && consumerKind != Consumer.Kind.ConsumerGroup.asCode()) {
            throw malformed(code, "consumer", 0);
        }
        return identifierEnd(payload, 1, code);
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
