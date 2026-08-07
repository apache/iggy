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

package org.apache.iggy.client.async.tcp.vsr;

import io.netty.buffer.ByteBuf;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.apache.iggy.exception.IggyServerException;

/**
 * Partition-plane namespace packing and payload peeking, mirroring
 * {@code core/binary_protocol/src/namespace.rs} and the request inspection in
 * {@code core/sdk/src/vsr.rs}.
 *
 * <p>The packed namespace routes a request to its partition's consensus
 * group: stream id in bits 32..43, topic id in bits 20..31, partition id in
 * bits 0..19. Control-plane requests (Register/Logout) use the metadata
 * sentinel {@code 1 << 63}; non-replicated and metadata ops use zero.
 */
final class VsrNamespace {

    static final long METADATA_CONSENSUS_NAMESPACE = 1L << 63;

    static final int MAX_STREAMS = 4096;
    static final int MAX_TOPICS = 4096;
    static final int MAX_PARTITIONS = 1_000_000;

    static final int STREAM_SHIFT = 32;
    static final int TOPIC_SHIFT = 20;

    private static final int SEND_MESSAGES_CODE = 101;
    private static final int STORE_CONSUMER_OFFSET_CODE = 121;
    private static final int DELETE_CONSUMER_OFFSET_CODE = 122;
    private static final int STORE_CONSUMER_OFFSET_2_CODE = 123;
    private static final int DELETE_CONSUMER_OFFSET_2_CODE = 124;

    private static final int IDENTIFIER_KIND_NUMERIC = 1;
    private static final int PARTITIONING_KIND_PARTITION_ID = 2;

    private VsrNamespace() {}

    static long namespaceFor(int operation, int commandCode, ByteBuf payload) {
        if (operation == VsrOperation.REGISTER || operation == VsrOperation.LOGOUT) {
            return METADATA_CONSENSUS_NAMESPACE;
        }
        if (operation == VsrOperation.NON_REPLICATED || VsrOperation.isMetadata(operation)) {
            return 0;
        }
        return switch (commandCode) {
            case SEND_MESSAGES_CODE -> fromSendMessages(payload);
            case STORE_CONSUMER_OFFSET_CODE,
                    DELETE_CONSUMER_OFFSET_CODE,
                    STORE_CONSUMER_OFFSET_2_CODE,
                    DELETE_CONSUMER_OFFSET_2_CODE -> fromConsumerOffset(payload);
            default -> throw IggyServerException.fromTcpResponse(VsrHeaders.ERROR_FEATURE_UNAVAILABLE, new byte[0]);
        };
    }

    /**
     * Payload: {@code [metadata_len:u32][stream id][topic id][partitioning]}.
     * Only explicit partition-id partitioning can be routed. Balanced and
     * key-based partitioning are resolved to a partition id by
     * {@code MessagesTcpClient} before the frame is encoded, so this check
     * is defense-in-depth for callers bypassing that resolution.
     */
    private static long fromSendMessages(ByteBuf payload) {
        ByteBuf in = payload.slice();
        if (in.readableBytes() < 4) {
            throw new IggyInvalidArgumentException("Send messages payload is missing its metadata section");
        }
        long metadataLength = in.readUnsignedIntLE();
        if (in.readableBytes() < metadataLength) {
            throw new IggyInvalidArgumentException("Send messages metadata section is truncated");
        }
        Long streamId = readNumericIdentifier(in);
        Long topicId = readNumericIdentifier(in);
        int partitioningKind = in.readUnsignedByte();
        int partitioningLength = in.readUnsignedByte();
        if (partitioningKind != PARTITIONING_KIND_PARTITION_ID || partitioningLength != 4) {
            throw IggyServerException.fromTcpResponse(VsrHeaders.ERROR_FEATURE_UNAVAILABLE, new byte[0]);
        }
        long partitionId = in.readUnsignedIntLE();
        return pack(streamId, topicId, partitionId);
    }

    /**
     * Payload: {@code [consumer kind:u8][consumer id][stream id][topic id]
     * [has_partition:u8][partition_id:u32]...}.
     */
    private static long fromConsumerOffset(ByteBuf payload) {
        ByteBuf in = payload.slice();
        in.skipBytes(1);
        skipIdentifier(in);
        Long streamId = readNumericIdentifier(in);
        Long topicId = readNumericIdentifier(in);
        int hasPartition = in.readUnsignedByte();
        long partitionId = in.readUnsignedIntLE();
        if (hasPartition == 0) {
            throw new IggyInvalidArgumentException("Consumer offset requests require an explicit partition id");
        }
        return pack(streamId, topicId, partitionId);
    }

    /** Named identifiers resolve server-side; they defer routing with namespace zero. */
    private static long pack(Long streamId, Long topicId, long partitionId) {
        if (streamId == null || topicId == null) {
            return 0;
        }
        validate(streamId, MAX_STREAMS, "stream id");
        validate(topicId, MAX_TOPICS, "topic id");
        validate(partitionId, MAX_PARTITIONS, "partition id");
        return (streamId << STREAM_SHIFT) | (topicId << TOPIC_SHIFT) | partitionId;
    }

    private static void validate(long value, int exclusiveMax, String field) {
        if (value >= exclusiveMax) {
            throw new IggyInvalidArgumentException(
                    "The " + field + " " + value + " exceeds the VSR namespace limit of " + (exclusiveMax - 1));
        }
    }

    private static Long readNumericIdentifier(ByteBuf in) {
        int kind = in.readUnsignedByte();
        int length = in.readUnsignedByte();
        if (kind == IDENTIFIER_KIND_NUMERIC && length == 4) {
            return in.readUnsignedIntLE();
        }
        in.skipBytes(length);
        return null;
    }

    private static void skipIdentifier(ByteBuf in) {
        in.skipBytes(1);
        int length = in.readUnsignedByte();
        in.skipBytes(length);
    }
}
