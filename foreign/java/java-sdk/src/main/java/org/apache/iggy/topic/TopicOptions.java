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
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * The options a topic is created with, one typed setter per catalog key.
 *
 * <p>Every key here is fixed at create except {@code message_expiry} and {@code max_topic_size}, which a
 * {@link TopicUpdate} may change later. A key left unset is resolved by the server and reported in the
 * topic's derived options. {@code describeOptions} enumerates what a given server accepts.
 *
 * <pre>{@code
 * var options = TopicOptions.builder()
 *         .messageExpiry(BigInteger.valueOf(604_800_000_000L))
 *         .segmentSize(BigInteger.valueOf(134_217_728))
 *         .durability(Durability.PERSISTED)
 *         .build();
 * topicsClient.createTopic(streamId, "orders", 1, options);
 * }</pre>
 */
public final class TopicOptions {

    private final Map<String, HeaderValue> entries;
    private final Set<String> rawKeys;

    private TopicOptions(Map<String, HeaderValue> entries, Set<String> rawKeys) {
        this.entries = entries;
        this.rawKeys = rawKeys;
    }

    public static Map<String, HeaderValue> withDurabilityDefaults(Map<String, HeaderValue> source) {
        Map<String, HeaderValue> resolved = new LinkedHashMap<>();
        if (source != null) {
            resolved.putAll(source);
        }
        for (String key : new String[] {"durability", "consumer_offset_durability"}) {
            var value = resolved.get(key);
            if (!resolved.containsKey(key)) {
                resolved.put(key, HeaderValue.fromString(Durability.REPLICATED.value()));
            } else if (value == null
                    || value.kind() != org.apache.iggy.message.HeaderKind.String
                    || !(value.toStringValue().equals("replicated")
                            || value.toStringValue().equals("persisted"))) {
                throw new IllegalArgumentException("Invalid " + key);
            }
        }
        return resolved;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Every option this value sends, keyed by option name. A typed setting wins over a raw entry of the
     * same key. The encoder writes them in ascending key order, so the order here carries no meaning.
     */
    public Map<String, HeaderValue> entries() {
        return entries;
    }

    /** Raw entries print by name only, because this build cannot tell which newer keys are secret. */
    @Override
    public String toString() {
        Map<String, HeaderValue> typed = new LinkedHashMap<>(entries);
        typed.keySet().removeAll(rawKeys);
        return "TopicOptions[options=" + typed + ", raw=" + rawKeys + "]";
    }

    public static final class Builder {

        private final Map<String, HeaderValue> typed = new LinkedHashMap<>();
        private final Map<String, HeaderValue> raw = new LinkedHashMap<>();

        private Builder() {
            durability(Durability.REPLICATED);
            consumerOffsetDurability(Durability.REPLICATED);
        }

        /** The stored compression label. The server never applies it to messages. */
        public Builder compressionAlgorithm(CompressionAlgorithm algorithm) {
            return set(
                    TopicOptionKey.COMPRESSION_ALGORITHM,
                    HeaderValue.fromString(TopicSettings.compressionName(algorithm)));
        }

        /**
         * Message expiry in microseconds, {@code 2^64 - 1} meaning never. Zero leaves the key to the server
         * default.
         */
        public Builder messageExpiry(BigInteger micros) {
            return setUnlessZero(TopicOptionKey.MESSAGE_EXPIRY, micros);
        }

        /**
         * Topic size cap in bytes, {@code 2^64 - 1} meaning unlimited. Zero leaves the key to the server
         * default.
         */
        public Builder maxTopicSize(BigInteger bytes) {
            return setUnlessZero(TopicOptionKey.MAX_TOPIC_SIZE, bytes);
        }

        /** Per-topic segment size in bytes: a 512-byte multiple within the server's bounds. */
        public Builder segmentSize(BigInteger bytes) {
            return set(TopicOptionKey.SEGMENT_SIZE, HeaderValue.fromUint64(bytes));
        }

        /** Message completion policy, independently defaulting to replicated. */
        public Builder durability(Durability durability) {
            return set(TopicOptionKey.DURABILITY, HeaderValue.fromString(durability.value()));
        }

        public Builder consumerOffsetDurability(Durability durability) {
            return set(TopicOptionKey.CONSUMER_OFFSET_DURABILITY, HeaderValue.fromString(durability.value()));
        }

        /** Whether the partition set may change after create. */
        public Builder partitionResizePolicy(PartitionResizePolicy policy) {
            return set(TopicOptionKey.PARTITION_RESIZE_POLICY, HeaderValue.fromString(policy.value()));
        }

        /** Flush the journal once it holds this many messages. Must be non-zero. */
        public Builder messagesRequiredToSave(long messages) {
            return set(TopicOptionKey.MESSAGES_REQUIRED_TO_SAVE, HeaderValue.fromUint32(messages));
        }

        /** Flush the journal once it holds this many bytes. */
        public Builder sizeOfMessagesRequiredToSave(BigInteger bytes) {
            return set(TopicOptionKey.SIZE_OF_MESSAGES_REQUIRED_TO_SAVE, HeaderValue.fromUint64(bytes));
        }

        /**
         * Reserve each segment's bytes up front where the filesystem supports it.
         *
         * <p>The reservation is real disk and runs inline on the owning shard, so the server caps
         * {@code segment_size * partitions_count} at admission.
         */
        public Builder preallocateSegments(boolean enabled) {
            return set(TopicOptionKey.PREALLOCATE_SEGMENTS, HeaderValue.fromBool(enabled));
        }

        /** An option this build has no setter for. A typed setter wins on collision. */
        public Builder raw(String key, HeaderValue value) {
            raw.put(key, value);
            return this;
        }

        public TopicOptions build() {
            Map<String, HeaderValue> entries = new LinkedHashMap<>(raw);
            entries.putAll(typed);
            Set<String> rawKeys = new LinkedHashSet<>(raw.keySet());
            rawKeys.removeAll(typed.keySet());
            return new TopicOptions(Collections.unmodifiableMap(entries), Collections.unmodifiableSet(rawKeys));
        }

        private Builder set(TopicOptionKey key, HeaderValue value) {
            typed.put(key.value(), value);
            return this;
        }

        private Builder setUnlessZero(TopicOptionKey key, BigInteger value) {
            if (value.signum() == 0) {
                typed.remove(key.value());
                return this;
            }
            return set(key, HeaderValue.fromUint64(value));
        }
    }
}
