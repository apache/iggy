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
 * <p>Only the name, the message expiry and the size cap change after create, and the last two can be
 * reset to their defaults. Compression and the other settings are fixed at create, so they have no field
 * here. A zero expiry or size cap changes nothing, {@code reset} is what returns a key to its default.
 * An update the server would refuse for its shape is refused before anything is sent, with the server's
 * error code. An update with a raw key or a raw reset name gets only the structural checks, the server
 * decides the rest.
 *
 * @param name the new name, or empty to keep it
 * @param messageExpiry the new expiry in microseconds, {@code 2^64 - 1} meaning never
 * @param maxTopicSize the new size cap in bytes, {@code 2^64 - 1} meaning unlimited
 * @param reset keys to return to their defaults
 * @param raw keys this build has no field for, a field wins on collision
 * @param rawReset reset names this build has no key constant for
 */
public record TopicUpdate(
        Optional<String> name,
        Optional<BigInteger> messageExpiry,
        Optional<BigInteger> maxTopicSize,
        Set<TopicOptionKey> reset,
        Map<String, HeaderValue> raw,
        Set<String> rawReset) {

    public TopicUpdate {
        reset = copyOf(reset);
        raw = Collections.unmodifiableMap(new LinkedHashMap<>(raw));
        rawReset = Set.copyOf(rawReset);
        boolean typedOnly = raw.isEmpty() && rawReset.isEmpty();
        OptionKey.requireValidUpdate(
                entries(name, messageExpiry, maxTopicSize, raw).keySet(),
                reset,
                rawReset,
                typedOnly,
                EnumSet.allOf(TopicOptionKey.class));
        if (typedOnly) {
            name.ifPresent(value -> OptionKey.requireTextLength(
                    TopicOptionKey.NAME.value(), value, 1, MAX_NAME_LENGTH, IggyErrorCode.INVALID_TOPIC_NAME));
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /** The keys this update sets, keyed by option name. */
    public Map<String, HeaderValue> entries() {
        return entries(name, messageExpiry, maxTopicSize, raw);
    }

    private static Map<String, HeaderValue> entries(
            Optional<String> name,
            Optional<BigInteger> messageExpiry,
            Optional<BigInteger> maxTopicSize,
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
                + ", reset=" + reset + ", raw=" + raw.keySet() + ", rawReset=" + rawReset + "]";
    }

    private static Set<TopicOptionKey> copyOf(Set<TopicOptionKey> keys) {
        Set<TopicOptionKey> copy = EnumSet.noneOf(TopicOptionKey.class);
        copy.addAll(keys);
        return Collections.unmodifiableSet(copy);
    }

    public static final class Builder {

        private final Set<TopicOptionKey> reset = EnumSet.noneOf(TopicOptionKey.class);
        private final Map<String, HeaderValue> raw = new LinkedHashMap<>();
        private final Set<String> rawReset = new LinkedHashSet<>();
        private Optional<String> name = Optional.empty();
        private Optional<BigInteger> messageExpiry = Optional.empty();
        private Optional<BigInteger> maxTopicSize = Optional.empty();

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
            return new TopicUpdate(name, messageExpiry, maxTopicSize, reset, raw, rawReset);
        }
    }
}
