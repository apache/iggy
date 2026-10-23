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
import java.util.Map;
import java.util.Optional;

/**
 * A topic as the server reports it.
 *
 * <p>{@code options} carries the keys the creating client set explicitly, {@code derivedOptions} the
 * values admission resolved for the keys it did not. Both are keyed by option name, and the split is the
 * provenance: a value in {@code derivedOptions} would have resolved differently under another server
 * config. Values keep the kind the server stored on every transport. Message expiry, compression and the
 * size cap live in those blocks only, read them through the accessors below.
 */
public record Topic(
        Long id,
        BigInteger createdAt,
        String name,
        String size,
        BigInteger messagesCount,
        Long partitionsCount,
        Map<String, HeaderValue> options,
        Map<String, HeaderValue> derivedOptions) {
    public Topic {
        options = options == null ? Map.of() : options;
        derivedOptions = derivedOptions == null ? Map.of() : derivedOptions;
    }

    /** Message expiry in microseconds, {@code 2^64 - 1} meaning never. */
    public Optional<BigInteger> messageExpiry() {
        return TopicSettings.uint64(options, derivedOptions, TopicOptionKey.MESSAGE_EXPIRY);
    }

    /** The stored compression label. */
    public Optional<CompressionAlgorithm> compressionAlgorithm() {
        return TopicSettings.compression(options, derivedOptions);
    }

    /** Topic size cap in bytes, {@code 2^64 - 1} meaning unlimited. */
    public Optional<BigInteger> maxTopicSize() {
        return TopicSettings.uint64(options, derivedOptions, TopicOptionKey.MAX_TOPIC_SIZE);
    }
}
