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

package org.apache.iggy.topic;

import org.apache.iggy.message.HeaderValue;

import java.math.BigInteger;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Typed builder for the topic option keys that have no parameter of their own on
 * {@code createTopic}.
 *
 * <p>Compression, message expiry and max topic size are named parameters already. The five keys here
 * would otherwise have to be spelled as raw strings paired with hand-encoded values, where a typo in
 * the key or the wrong kind for the value is only caught by the server refusing the create.
 *
 * <p>These are create-time keys: an update refuses them by name, because they describe how a
 * partition's storage was laid down. {@code describeOptions} enumerates what a given server accepts.
 *
 * <pre>{@code
 * var options = TopicOptions.builder()
 *         .segmentSize(BigInteger.valueOf(134_217_728))
 *         .enforceFsync(true)
 *         .build();
 * topicsClient.createTopic(streamId, 1L, CompressionAlgorithm.None,
 *         BigInteger.ZERO, BigInteger.ZERO, "orders", options);
 * }</pre>
 */
public final class TopicOptions {

    private TopicOptions() {}

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private final Map<String, HeaderValue> options = new LinkedHashMap<>();

        private Builder() {}

        /** Per-topic segment size in bytes: a 512-byte multiple within the server's bounds. */
        public Builder segmentSize(BigInteger bytes) {
            options.put("segment_size", HeaderValue.fromUint64(bytes));
            return this;
        }

        /** Whether writes to this topic's partitions fsync. */
        public Builder enforceFsync(boolean enabled) {
            options.put("enforce_fsync", HeaderValue.fromBool(enabled));
            return this;
        }

        /** Flush the journal once it holds this many messages. Must be non-zero. */
        public Builder messagesRequiredToSave(long messages) {
            options.put("messages_required_to_save", HeaderValue.fromUint32(messages));
            return this;
        }

        /** Flush the journal once it holds this many bytes. */
        public Builder sizeOfMessagesRequiredToSave(BigInteger bytes) {
            options.put("size_of_messages_required_to_save", HeaderValue.fromUint64(bytes));
            return this;
        }

        /**
         * Reserve each segment's bytes up front where the filesystem supports it.
         *
         * <p>The reservation is real disk and runs inline on the owning shard, so the server caps
         * {@code segment_size * partitions_count} at admission.
         */
        public Builder preallocateSegments(boolean enabled) {
            options.put("preallocate_segments", HeaderValue.fromBool(enabled));
            return this;
        }

        /**
         * The keys set on this builder, in the order they were set.
         *
         * <p>Wrapping a {@link LinkedHashMap} rather than calling {@code Map.copyOf}, whose
         * iteration order is salted per JVM run: the encoder walks this map in order, so a
         * randomized one would give the same builder a different block on every run.
         */
        public Map<String, HeaderValue> build() {
            return Collections.unmodifiableMap(new LinkedHashMap<>(options));
        }
    }
}
