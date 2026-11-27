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

import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.system.OptionKey;

import java.math.BigInteger;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.iggy.system.OptionKey.MAX_NAME_LENGTH;

/**
 * What an update changes on a topic. Every key left out keeps its stored value.
 *
 * <p>Only the name, the message expiry, the size cap and the two local retention limits change after
 * create, and the last four can be reset to their defaults. Tiered storage can be turned on, never off
 * and never reset, because the local segments it removed cannot come back. Compression and the other
 * settings are fixed at create, so they have no field here. A zero expiry, size cap or local retention
 * limit changes nothing, {@code reset} is what returns a key to its default.
 *
 * <p>The six durations from {@code freshUploadInterval} to {@code longTermRewriteMaxInterval} change only on a
 * geo topic, and the server refuses them on a normal topic. {@code readCacheSize} and {@code readAhead}
 * change on any topic. On a geo topic these eight are the only settings an update changes. Zero is a value
 * for them and is sent. They have no default, so they cannot be reset.
 *
 * <p>An update the server would refuse for its shape is refused before anything is sent, with the server's
 * error code. An update with a raw key or a raw reset name gets only the structural checks, the server
 * decides the rest.
 *
 * @param name the new name, or empty to keep it
 * @param messageExpiry the new expiry in microseconds, {@code 2^64 - 1} meaning never
 * @param maxTopicSize the new size cap in bytes, {@code 2^64 - 1} meaning unlimited
 * @param reset keys to return to their defaults
 * @param raw keys this build has no field for, a field wins on collision
 * @param rawReset reset names this build has no key constant for
 * @param localRetentionAge how long an uploaded segment stays on local disk in microseconds,
 *     {@code 2^64 - 1} meaning never removed
 * @param localRetentionSize the cap in bytes on the uploaded segments kept on local disk,
 *     {@code 2^64 - 1} meaning unlimited
 * @param tieredStorage {@code true} to turn tiered storage on
 * @param freshUploadInterval the longest time a send of the geo topic waits for its upload to start, in
 *     microseconds, at most ten seconds
 * @param consumerOffsetUploadInterval the shortest time between two uploads of the positions that the offsets
 *     the geo topic's consumer groups store wait for, in microseconds, at most one second
 * @param positionsPublishInterval how often a node uploads the geo topic's changed positions, in microseconds
 * @param tombstoneGrace how long the deleted geo topic's tombstone and objects stay, in microseconds
 * @param positionsResumeTimeout how long the geo topic's group polls wait for the peers' positions after a
 *     node starts, in microseconds
 * @param longTermRewriteMaxInterval the longest wait between two long-term rewrites of the geo topic's
 *     partition, in microseconds, at least one second
 * @param readCacheSize the memory in bytes that each node keeps for what the topic's reads fetch from the
 *     object store, zero keeping nothing, otherwise at least 512 KiB
 * @param readAhead the most bytes that readahead fetches in front of a sequential reader of the topic, zero
 *     turning readahead off, otherwise at least 128 KiB
 */
public record TopicUpdate(
        Optional<String> name,
        Optional<BigInteger> messageExpiry,
        Optional<BigInteger> maxTopicSize,
        Set<TopicOptionKey> reset,
        Map<String, HeaderValue> raw,
        Set<String> rawReset,
        Optional<BigInteger> localRetentionAge,
        Optional<BigInteger> localRetentionSize,
        Optional<Boolean> tieredStorage,
        Optional<BigInteger> freshUploadInterval,
        Optional<BigInteger> consumerOffsetUploadInterval,
        Optional<BigInteger> positionsPublishInterval,
        Optional<BigInteger> tombstoneGrace,
        Optional<BigInteger> positionsResumeTimeout,
        Optional<BigInteger> longTermRewriteMaxInterval,
        Optional<BigInteger> readCacheSize,
        Optional<BigInteger> readAhead) {

    public TopicUpdate {
        reset = copyOf(reset);
        raw = Collections.unmodifiableMap(new LinkedHashMap<>(raw));
        rawReset = Set.copyOf(rawReset);
        boolean typedOnly = raw.isEmpty() && rawReset.isEmpty();
        OptionKey.requireValidUpdate(
                entries(
                                name,
                                messageExpiry,
                                maxTopicSize,
                                localRetentionAge,
                                localRetentionSize,
                                tieredStorage,
                                freshUploadInterval,
                                consumerOffsetUploadInterval,
                                positionsPublishInterval,
                                tombstoneGrace,
                                positionsResumeTimeout,
                                longTermRewriteMaxInterval,
                                readCacheSize,
                                readAhead,
                                raw)
                        .keySet(),
                reset,
                rawReset,
                typedOnly,
                EnumSet.allOf(TopicOptionKey.class));
        if (typedOnly) {
            name.ifPresent(value -> OptionKey.requireTextLength(
                    TopicOptionKey.NAME.value(), value, 1, MAX_NAME_LENGTH, IggyErrorCode.INVALID_TOPIC_NAME));
            tieredStorage.ifPresent(enabled -> OptionKey.requireEnabled(TopicOptionKey.TIERED_STORAGE, enabled));
        }
    }

    /** An update of the keys that predate the read cache sizes. */
    @SuppressWarnings("checkstyle:ParameterNumber")
    public TopicUpdate(
            Optional<String> name,
            Optional<BigInteger> messageExpiry,
            Optional<BigInteger> maxTopicSize,
            Set<TopicOptionKey> reset,
            Map<String, HeaderValue> raw,
            Set<String> rawReset,
            Optional<BigInteger> localRetentionAge,
            Optional<BigInteger> localRetentionSize,
            Optional<Boolean> tieredStorage,
            Optional<BigInteger> freshUploadInterval,
            Optional<BigInteger> consumerOffsetUploadInterval,
            Optional<BigInteger> positionsPublishInterval,
            Optional<BigInteger> tombstoneGrace,
            Optional<BigInteger> positionsResumeTimeout,
            Optional<BigInteger> longTermRewriteMaxInterval) {
        this(
                name,
                messageExpiry,
                maxTopicSize,
                reset,
                raw,
                rawReset,
                localRetentionAge,
                localRetentionSize,
                tieredStorage,
                freshUploadInterval,
                consumerOffsetUploadInterval,
                positionsPublishInterval,
                tombstoneGrace,
                positionsResumeTimeout,
                longTermRewriteMaxInterval,
                Optional.empty(),
                Optional.empty());
    }

    /** An update of the keys that predate the durations of a geo topic. */
    @SuppressWarnings("checkstyle:ParameterNumber")
    public TopicUpdate(
            Optional<String> name,
            Optional<BigInteger> messageExpiry,
            Optional<BigInteger> maxTopicSize,
            Set<TopicOptionKey> reset,
            Map<String, HeaderValue> raw,
            Set<String> rawReset,
            Optional<BigInteger> localRetentionAge,
            Optional<BigInteger> localRetentionSize,
            Optional<Boolean> tieredStorage) {
        this(
                name,
                messageExpiry,
                maxTopicSize,
                reset,
                raw,
                rawReset,
                localRetentionAge,
                localRetentionSize,
                tieredStorage,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    /** An update of the keys that predate tiered storage. */
    public TopicUpdate(
            Optional<String> name,
            Optional<BigInteger> messageExpiry,
            Optional<BigInteger> maxTopicSize,
            Set<TopicOptionKey> reset,
            Map<String, HeaderValue> raw,
            Set<String> rawReset) {
        this(
                name,
                messageExpiry,
                maxTopicSize,
                reset,
                raw,
                rawReset,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    public static Builder builder() {
        return new Builder();
    }

    /** The keys this update sets, keyed by option name. */
    public Map<String, HeaderValue> entries() {
        return entries(
                name,
                messageExpiry,
                maxTopicSize,
                localRetentionAge,
                localRetentionSize,
                tieredStorage,
                freshUploadInterval,
                consumerOffsetUploadInterval,
                positionsPublishInterval,
                tombstoneGrace,
                positionsResumeTimeout,
                longTermRewriteMaxInterval,
                readCacheSize,
                readAhead,
                raw);
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    private static Map<String, HeaderValue> entries(
            Optional<String> name,
            Optional<BigInteger> messageExpiry,
            Optional<BigInteger> maxTopicSize,
            Optional<BigInteger> localRetentionAge,
            Optional<BigInteger> localRetentionSize,
            Optional<Boolean> tieredStorage,
            Optional<BigInteger> freshUploadInterval,
            Optional<BigInteger> consumerOffsetUploadInterval,
            Optional<BigInteger> positionsPublishInterval,
            Optional<BigInteger> tombstoneGrace,
            Optional<BigInteger> positionsResumeTimeout,
            Optional<BigInteger> longTermRewriteMaxInterval,
            Optional<BigInteger> readCacheSize,
            Optional<BigInteger> readAhead,
            Map<String, HeaderValue> raw) {
        Map<String, HeaderValue> entries = new LinkedHashMap<>(raw);
        name.ifPresent(
                value -> entries.put(TopicOptionKey.NAME.value(), OptionKey.textValue(value, 1, MAX_NAME_LENGTH)));
        messageExpiry
                .filter(value -> value.signum() != 0)
                .ifPresent(value -> entries.put(TopicOptionKey.MESSAGE_EXPIRY.value(), HeaderValue.fromUint64(value)));
        maxTopicSize
                .filter(value -> value.signum() != 0)
                .ifPresent(value -> entries.put(TopicOptionKey.MAX_TOPIC_SIZE.value(), HeaderValue.fromUint64(value)));
        localRetentionAge
                .filter(value -> value.signum() != 0)
                .ifPresent(value ->
                        entries.put(TopicOptionKey.LOCAL_RETENTION_AGE.value(), HeaderValue.fromUint64(value)));
        localRetentionSize
                .filter(value -> value.signum() != 0)
                .ifPresent(value ->
                        entries.put(TopicOptionKey.LOCAL_RETENTION_SIZE.value(), HeaderValue.fromUint64(value)));
        tieredStorage.ifPresent(
                enabled -> entries.put(TopicOptionKey.TIERED_STORAGE.value(), HeaderValue.fromBool(enabled)));
        putDuration(entries, TopicOptionKey.FRESH_UPLOAD_INTERVAL, freshUploadInterval);
        putDuration(entries, TopicOptionKey.CONSUMER_OFFSET_UPLOAD_INTERVAL, consumerOffsetUploadInterval);
        putDuration(entries, TopicOptionKey.POSITIONS_PUBLISH_INTERVAL, positionsPublishInterval);
        putDuration(entries, TopicOptionKey.TOMBSTONE_GRACE, tombstoneGrace);
        putDuration(entries, TopicOptionKey.POSITIONS_RESUME_TIMEOUT, positionsResumeTimeout);
        putDuration(entries, TopicOptionKey.LONG_TERM_REWRITE_MAX_INTERVAL, longTermRewriteMaxInterval);
        readCacheSize.ifPresent(
                bytes -> entries.put(TopicOptionKey.READ_CACHE_SIZE.value(), HeaderValue.fromUint64(bytes)));
        readAhead.ifPresent(bytes -> entries.put(TopicOptionKey.READ_AHEAD.value(), HeaderValue.fromUint64(bytes)));
        return Collections.unmodifiableMap(entries);
    }

    /** The names this update resets, typed and raw together in ascending order. */
    public List<String> resetKeys() {
        return OptionKey.resetNames(reset, rawReset);
    }

    /** Raw entries print by name only, because this build cannot tell which newer keys are secret. */
    @Override
    public String toString() {
        return "TopicUpdate[name=" + name + ", messageExpiry=" + messageExpiry + ", maxTopicSize=" + maxTopicSize
                + ", reset=" + reset + ", raw=" + raw.keySet() + ", rawReset=" + rawReset + ", localRetentionAge="
                + localRetentionAge + ", localRetentionSize=" + localRetentionSize + ", tieredStorage="
                + tieredStorage + ", freshUploadInterval=" + freshUploadInterval + ", consumerOffsetUploadInterval="
                + consumerOffsetUploadInterval + ", positionsPublishInterval=" + positionsPublishInterval
                + ", tombstoneGrace=" + tombstoneGrace + ", positionsResumeTimeout=" + positionsResumeTimeout
                + ", longTermRewriteMaxInterval=" + longTermRewriteMaxInterval + ", readCacheSize=" + readCacheSize
                + ", readAhead=" + readAhead + "]";
    }

    private static Set<TopicOptionKey> copyOf(Set<TopicOptionKey> keys) {
        Set<TopicOptionKey> copy = EnumSet.noneOf(TopicOptionKey.class);
        copy.addAll(keys);
        return Collections.unmodifiableSet(copy);
    }

    /** Zero is a duration of a geo topic, not a sentinel to drop, so only an absent one stays out. */
    private static void putDuration(Map<String, HeaderValue> entries, TopicOptionKey key, Optional<BigInteger> micros) {
        micros.ifPresent(value -> entries.put(key.value(), HeaderValue.fromUint64(value)));
    }

    public static final class Builder {

        private final Set<TopicOptionKey> reset = EnumSet.noneOf(TopicOptionKey.class);
        private final Map<String, HeaderValue> raw = new LinkedHashMap<>();
        private final Set<String> rawReset = new LinkedHashSet<>();
        private Optional<String> name = Optional.empty();
        private Optional<BigInteger> messageExpiry = Optional.empty();
        private Optional<BigInteger> maxTopicSize = Optional.empty();
        private Optional<BigInteger> localRetentionAge = Optional.empty();
        private Optional<BigInteger> localRetentionSize = Optional.empty();
        private Optional<Boolean> tieredStorage = Optional.empty();
        private Optional<BigInteger> freshUploadInterval = Optional.empty();
        private Optional<BigInteger> consumerOffsetUploadInterval = Optional.empty();
        private Optional<BigInteger> positionsPublishInterval = Optional.empty();
        private Optional<BigInteger> tombstoneGrace = Optional.empty();
        private Optional<BigInteger> positionsResumeTimeout = Optional.empty();
        private Optional<BigInteger> longTermRewriteMaxInterval = Optional.empty();
        private Optional<BigInteger> readCacheSize = Optional.empty();
        private Optional<BigInteger> readAhead = Optional.empty();

        private Builder() {}

        public Builder name(String name) {
            this.name = Optional.of(name);
            return this;
        }

        public Builder messageExpiry(BigInteger micros) {
            this.messageExpiry = Optional.of(micros);
            return this;
        }

        public Builder maxTopicSize(BigInteger bytes) {
            this.maxTopicSize = Optional.of(bytes);
            return this;
        }

        public Builder localRetentionAge(BigInteger micros) {
            this.localRetentionAge = Optional.of(micros);
            return this;
        }

        public Builder localRetentionSize(BigInteger bytes) {
            this.localRetentionSize = Optional.of(bytes);
            return this;
        }

        /** Only {@code true} is accepted: an update never turns tiered storage off. */
        public Builder tieredStorage(boolean enabled) {
            this.tieredStorage = Optional.of(enabled);
            return this;
        }

        /** Sent as written, zero included. This and the five duration setters below act only on a geo topic. */
        public Builder freshUploadInterval(BigInteger micros) {
            this.freshUploadInterval = Optional.of(micros);
            return this;
        }

        public Builder consumerOffsetUploadInterval(BigInteger micros) {
            this.consumerOffsetUploadInterval = Optional.of(micros);
            return this;
        }

        public Builder positionsPublishInterval(BigInteger micros) {
            this.positionsPublishInterval = Optional.of(micros);
            return this;
        }

        public Builder tombstoneGrace(BigInteger micros) {
            this.tombstoneGrace = Optional.of(micros);
            return this;
        }

        public Builder positionsResumeTimeout(BigInteger micros) {
            this.positionsResumeTimeout = Optional.of(micros);
            return this;
        }

        public Builder longTermRewriteMaxInterval(BigInteger micros) {
            this.longTermRewriteMaxInterval = Optional.of(micros);
            return this;
        }

        /** Sent as written, zero included. This and {@link #readAhead} act on a normal or a geo topic. */
        public Builder readCacheSize(BigInteger bytes) {
            this.readCacheSize = Optional.of(bytes);
            return this;
        }

        public Builder readAhead(BigInteger bytes) {
            this.readAhead = Optional.of(bytes);
            return this;
        }

        public Builder reset(TopicOptionKey key) {
            reset.add(key);
            return this;
        }

        public Builder raw(String key, HeaderValue value) {
            raw.put(key, value);
            return this;
        }

        /** Resets a key this build has no constant for. The server decides whether the reset is allowed. */
        public Builder rawReset(String key) {
            rawReset.add(key);
            return this;
        }

        /**
         * @throws org.apache.iggy.exception.IggyValidationException when the server would refuse the update
         *     for its shape
         */
        public TopicUpdate build() {
            return new TopicUpdate(
                    name,
                    messageExpiry,
                    maxTopicSize,
                    reset,
                    raw,
                    rawReset,
                    localRetentionAge,
                    localRetentionSize,
                    tieredStorage,
                    freshUploadInterval,
                    consumerOffsetUploadInterval,
                    positionsPublishInterval,
                    tombstoneGrace,
                    positionsResumeTimeout,
                    longTermRewriteMaxInterval,
                    readCacheSize,
                    readAhead);
        }
    }
}
