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

import org.apache.iggy.message.HeaderKind;
import org.apache.iggy.message.HeaderValue;

import java.math.BigInteger;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/** Reads a topic's settings from its option blocks, the explicit value first. */
final class TopicSettings {

    private TopicSettings() {}

    static Optional<BigInteger> uint64(
            Map<String, HeaderValue> options, Map<String, HeaderValue> derived, TopicOptionKey key) {
        return value(options, derived, key)
                .filter(value -> value.kind() == HeaderKind.Uint64)
                .map(HeaderValue::asUint64);
    }

    static Optional<CompressionAlgorithm> compression(
            Map<String, HeaderValue> options, Map<String, HeaderValue> derived) {
        return value(options, derived, TopicOptionKey.COMPRESSION_ALGORITHM)
                .filter(value -> value.kind() == HeaderKind.String)
                .flatMap(value -> compressionNamed(value.asString()));
    }

    static String compressionName(CompressionAlgorithm algorithm) {
        return algorithm.name().toLowerCase(Locale.ROOT);
    }

    private static Optional<CompressionAlgorithm> compressionNamed(String name) {
        for (CompressionAlgorithm algorithm : CompressionAlgorithm.values()) {
            if (compressionName(algorithm).equals(name)) {
                return Optional.of(algorithm);
            }
        }
        return Optional.empty();
    }

    private static Optional<HeaderValue> value(
            Map<String, HeaderValue> options, Map<String, HeaderValue> derived, TopicOptionKey key) {
        HeaderValue value = options.get(key.value());
        return Optional.ofNullable(value == null ? derived.get(key.value()) : value);
    }
}
