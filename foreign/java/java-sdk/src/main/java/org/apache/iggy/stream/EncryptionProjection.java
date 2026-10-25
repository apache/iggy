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

import org.apache.iggy.message.HeaderKind;
import org.apache.iggy.message.HeaderValue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Reads the encryption projections a server reports in a stream's derived options. */
public final class EncryptionProjection {

    private EncryptionProjection() {}

    public static Optional<EncryptionKeySource> source(
            Map<String, HeaderValue> options, Map<String, HeaderValue> derived) {
        return text(options, derived, StreamOptionKey.ENCRYPTION_KEY_SOURCE).flatMap(EncryptionKeySource::fromValue);
    }

    public static Optional<String> keyFingerprint(Map<String, HeaderValue> options, Map<String, HeaderValue> derived) {
        return text(options, derived, StreamOptionKey.ENCRYPTION_KEY_FINGERPRINT);
    }

    static List<String> kekFingerprints(Map<String, HeaderValue> options, Map<String, HeaderValue> derived) {
        return text(options, derived, StreamOptionKey.ENCRYPTION_KEK_FINGERPRINT)
                .map(text -> Arrays.asList(text.split(",")))
                .map(List::copyOf)
                .orElse(List.of());
    }

    private static Optional<String> text(
            Map<String, HeaderValue> options, Map<String, HeaderValue> derived, StreamOptionKey key) {
        HeaderValue value = derived.get(key.value());
        if (value == null) {
            value = options.get(key.value());
        }
        return Optional.ofNullable(value)
                .filter(found -> found.kind() == HeaderKind.String)
                .map(HeaderValue::asString);
    }
}
