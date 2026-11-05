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

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A stream as the server reports it.
 *
 * <p>{@code options} carries the keys the creating client set, {@code derivedOptions} the values the
 * server filled or computes, such as the encryption key source and fingerprints. Both are keyed by option
 * name and keep the kind the server reports, on every transport. The stream key itself is never returned.
 */
public record StreamBase(
        Long id,
        BigInteger createdAt,
        String name,
        String size,
        BigInteger messagesCount,
        Long topicsCount,
        Map<String, HeaderValue> options,
        Map<String, HeaderValue> derivedOptions) {
    public StreamBase {
        options = options == null ? Map.of() : options;
        derivedOptions = derivedOptions == null ? Map.of() : derivedOptions;
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
