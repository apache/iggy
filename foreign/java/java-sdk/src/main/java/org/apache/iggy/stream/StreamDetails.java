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

package org.apache.iggy.stream;

import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.topic.Topic;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A stream with its topics, as the server reports it. See {@link StreamBase} for what {@code options} and
 * {@code derivedOptions} carry.
 */
public record StreamDetails(
        Long id,
        BigInteger createdAt,
        String name,
        String size,
        BigInteger messagesCount,
        Long topicsCount,
        List<Topic> topics,
        Map<String, HeaderValue> options,
        Map<String, HeaderValue> derivedOptions) {
    public StreamDetails {
        options = options == null ? Map.of() : options;
        derivedOptions = derivedOptions == null ? Map.of() : derivedOptions;
    }

    public StreamDetails(StreamBase base, List<Topic> topics) {
        this(
                base.id(),
                base.createdAt(),
                base.name(),
                base.size(),
                base.messagesCount(),
                base.topicsCount(),
                topics,
                base.options(),
                base.derivedOptions());
    }

    /** Who chose the stream's data key, empty when the server reported no source. */
    public Optional<EncryptionKeySource> encryptionKeySource() {
        return EncryptionProjection.source(options, derivedOptions);
    }

    /** Fingerprint of the stream's data key, empty for a plaintext stream. */
    public Optional<String> encryptionKeyFingerprint() {
        return EncryptionProjection.keyFingerprint(options, derivedOptions);
    }

    /** Fingerprints of the server keys that wrap the stream's key, empty for a plaintext stream. */
    public List<String> encryptionKekFingerprints() {
        return EncryptionProjection.kekFingerprints(options, derivedOptions);
    }
}
